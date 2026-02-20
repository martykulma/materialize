// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Combined Raft storage (v1 API) for persist consensus.
//!
//! In openraft 0.9, `RaftLogStorage` and `RaftStateMachine` are sealed traits.
//! We implement the v1 `RaftStorage` trait instead and use `openraft::storage::Adaptor`
//! to convert to the v2 types that `Raft::new()` expects.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::io::Cursor;
use std::ops::RangeBounds;
use std::sync::{Arc, Mutex};

use mz_persist::location::VersionedData;
use openraft::storage::{LogState, RaftLogReader, RaftSnapshotBuilder, RaftStorage};
use openraft::{
    Entry, EntryPayload, LogId, Snapshot, SnapshotMeta, StorageError, StoredMembership, Vote,
};

use crate::raft_types::{ConsensusResponse, NodeInfo, TypeConfig};
use crate::state_machine::{StateMachineData, StateMachineStore, StoredSnapshot, apply_request};

/// Combined Raft storage for log and state machine data.
///
/// Implements the openraft v1 `RaftStorage` trait. The state machine data
/// is shared via `Arc<Mutex<>>` so the gRPC server can read it directly.
#[derive(Clone)]
pub struct Storage {
    vote: Arc<Mutex<Option<Vote<u64>>>>,
    log: Arc<Mutex<BTreeMap<u64, Entry<TypeConfig>>>>,
    last_purged_log_id: Arc<Mutex<Option<LogId<u64>>>>,
    state_machine: Arc<Mutex<StateMachineData>>,
    snapshot: Arc<Mutex<Option<StoredSnapshot>>>,
}

impl Storage {
    /// Creates a new Storage and a StateMachineStore for external reads.
    pub fn new() -> (Self, StateMachineStore) {
        let sm_data = Arc::new(Mutex::new(StateMachineData::default()));
        let storage = Self {
            vote: Arc::new(Mutex::new(None)),
            log: Arc::new(Mutex::new(BTreeMap::new())),
            last_purged_log_id: Arc::new(Mutex::new(None)),
            state_machine: sm_data.clone(),
            snapshot: Arc::new(Mutex::new(None)),
        };
        let sm_store = StateMachineStore(sm_data);
        (storage, sm_store)
    }
}

impl RaftLogReader<TypeConfig> for Storage {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + Send>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<TypeConfig>>, StorageError<u64>> {
        let log = self.log.lock().expect("lock poisoned");
        Ok(log.range(range).map(|(_, v)| v.clone()).collect())
    }
}

impl RaftSnapshotBuilder<TypeConfig> for Storage {
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let sm = self.state_machine.lock().expect("lock poisoned");

        let data_bytes = serde_json::to_vec(&sm.data).map_err(|e| {
            StorageError::from_io_error(
                openraft::ErrorSubject::StateMachine,
                openraft::ErrorVerb::Read,
                std::io::Error::new(std::io::ErrorKind::Other, e),
            )
        })?;

        let last_applied_log = sm.last_applied_log;
        let last_membership = sm.last_membership.clone();

        let snapshot_id = last_applied_log
            .map(|id| format!("{}-{}", id.leader_id, id.index))
            .unwrap_or_default();

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let mut snap = self.snapshot.lock().expect("lock poisoned");
        *snap = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data_bytes.clone(),
        });

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data_bytes)),
        })
    }
}

impl RaftStorage<TypeConfig> for Storage {
    type LogReader = Self;
    type SnapshotBuilder = Self;

    async fn save_vote(&mut self, vote: &Vote<u64>) -> Result<(), StorageError<u64>> {
        let mut v = self.vote.lock().expect("lock poisoned");
        *v = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<u64>>, StorageError<u64>> {
        let v = self.vote.lock().expect("lock poisoned");
        Ok(*v)
    }

    async fn get_log_state(&mut self) -> Result<LogState<TypeConfig>, StorageError<u64>> {
        let log = self.log.lock().expect("lock poisoned");
        let purged = self.last_purged_log_id.lock().expect("lock poisoned");
        let last_log_id = log.last_key_value().map(|(_, e)| e.log_id);
        Ok(LogState {
            last_purged_log_id: *purged,
            last_log_id: last_log_id.or(*purged),
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn append_to_log<I: IntoIterator<Item = Entry<TypeConfig>> + Send>(
        &mut self,
        entries: I,
    ) -> Result<(), StorageError<u64>> {
        let mut log = self.log.lock().expect("lock poisoned");
        for entry in entries {
            log.insert(entry.log_id.index, entry);
        }
        Ok(())
    }

    async fn delete_conflict_logs_since(
        &mut self,
        log_id: LogId<u64>,
    ) -> Result<(), StorageError<u64>> {
        let mut log = self.log.lock().expect("lock poisoned");
        let to_remove: Vec<u64> = log.range(log_id.index..).map(|(k, _)| *k).collect();
        for k in to_remove {
            log.remove(&k);
        }
        Ok(())
    }

    async fn purge_logs_upto(&mut self, log_id: LogId<u64>) -> Result<(), StorageError<u64>> {
        let mut log = self.log.lock().expect("lock poisoned");
        let to_remove: Vec<u64> = log.range(..=log_id.index).map(|(k, _)| *k).collect();
        for k in to_remove {
            log.remove(&k);
        }
        let mut purged = self.last_purged_log_id.lock().expect("lock poisoned");
        *purged = Some(log_id);
        Ok(())
    }

    async fn last_applied_state(
        &mut self,
    ) -> Result<
        (
            Option<LogId<u64>>,
            StoredMembership<u64, NodeInfo>,
        ),
        StorageError<u64>,
    > {
        let sm = self.state_machine.lock().expect("lock poisoned");
        Ok((sm.last_applied_log, sm.last_membership.clone()))
    }

    async fn apply_to_state_machine(
        &mut self,
        entries: &[Entry<TypeConfig>],
    ) -> Result<Vec<ConsensusResponse>, StorageError<u64>> {
        let mut sm = self.state_machine.lock().expect("lock poisoned");
        let mut responses = Vec::new();

        for entry in entries {
            sm.last_applied_log = Some(entry.log_id);

            match &entry.payload {
                EntryPayload::Blank => {
                    responses.push(ConsensusResponse::CompareAndSet { committed: false });
                }
                EntryPayload::Normal(req) => {
                    let resp = apply_request(&mut sm.data, req.clone());
                    responses.push(resp);
                }
                EntryPayload::Membership(m) => {
                    sm.last_membership = StoredMembership::new(Some(entry.log_id), m.clone());
                    responses.push(ConsensusResponse::CompareAndSet { committed: false });
                }
            }
        }

        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, NodeInfo>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        let data_bytes = snapshot.into_inner();
        let sm_data: BTreeMap<String, Vec<VersionedData>> =
            serde_json::from_slice(&data_bytes).map_err(|e| {
                StorageError::from_io_error(
                    openraft::ErrorSubject::StateMachine,
                    openraft::ErrorVerb::Read,
                    std::io::Error::new(std::io::ErrorKind::Other, e),
                )
            })?;

        let mut sm = self.state_machine.lock().expect("lock poisoned");
        sm.last_applied_log = meta.last_log_id;
        sm.last_membership = meta.last_membership.clone();
        sm.data = sm_data;

        let mut snap = self.snapshot.lock().expect("lock poisoned");
        *snap = Some(StoredSnapshot {
            meta: meta.clone(),
            data: data_bytes,
        });

        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        let snap = self.snapshot.lock().expect("lock poisoned");
        Ok(snap.as_ref().map(|s| Snapshot {
            meta: s.meta.clone(),
            snapshot: Box::new(Cursor::new(s.data.clone())),
        }))
    }
}
