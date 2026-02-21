// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! Raft state machine data and apply logic for persist consensus.
//!
//! The state machine mirrors the logic in `MemConsensus` from `mz_persist::mem`,
//! maintaining a `BTreeMap<String, Vec<VersionedData>>` as its state.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use mz_persist::location::{SeqNo, VersionedData};
use openraft::{LogId, SnapshotMeta, StoredMembership};

use crate::raft_types::{ConsensusRequest, ConsensusResponse, NodeInfo};

/// Snapshot data stored by the state machine.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<u64, NodeInfo>,
    pub data: Vec<u8>,
}

/// The inner state of the state machine.
#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, NodeInfo>,
    pub data: BTreeMap<String, Vec<VersionedData>>,
}

/// Provides direct read access to the state machine data (no Raft round-trip).
///
/// Wraps `Arc<Mutex<StateMachineData>>` so it can be shared between the
/// `Storage` (which writes via Raft) and the gRPC server (which reads directly).
#[derive(Debug, Clone)]
pub struct StateMachineStore(pub Arc<Mutex<StateMachineData>>);

impl StateMachineStore {
    /// Returns the latest versioned data for the given key.
    pub fn head(&self, key: &str) -> Option<VersionedData> {
        let data = self.0.lock().expect("lock poisoned");
        data.data.get(key).and_then(|v| v.last().cloned())
    }

    /// Returns up to `limit` versions for `key` with seqno >= `from`.
    pub fn scan(&self, key: &str, from: SeqNo, limit: usize) -> Vec<VersionedData> {
        let data = self.0.lock().expect("lock poisoned");
        if let Some(values) = data.data.get(key) {
            let from_idx = values.partition_point(|x| x.seqno < from);
            let from_values = &values[from_idx..];
            from_values[..usize::min(limit, from_values.len())].to_vec()
        } else {
            Vec::new()
        }
    }

    /// Returns all keys in the store.
    pub fn list_keys(&self) -> Vec<String> {
        let data = self.0.lock().expect("lock poisoned");
        data.data.keys().cloned().collect()
    }
}

// --- Apply logic (mirrors MemConsensus) ---

pub fn apply_request(
    store: &mut BTreeMap<String, Vec<VersionedData>>,
    req: ConsensusRequest,
) -> ConsensusResponse {
    match req {
        ConsensusRequest::CompareAndSet {
            key,
            expected,
            new_seqno,
            new_data,
        } => {
            let expected_seqno = expected.map(SeqNo);
            let new = VersionedData {
                seqno: SeqNo(new_seqno),
                data: Bytes::from(new_data),
            };

            // Validate: new seqno must be strictly greater than expected.
            if let Some(exp) = expected_seqno {
                if new.seqno <= exp {
                    return ConsensusResponse::Error {
                        message: format!(
                            "new seqno must be strictly greater than expected. Got new: {:?} expected: {:?}",
                            new.seqno, exp
                        ),
                    };
                }
            }

            // Validate: seqno must fit in [0, i64::MAX].
            let max_seqno: u64 = i64::MAX as u64;
            if new.seqno.0 > max_seqno {
                return ConsensusResponse::Error {
                    message: format!(
                        "sequence numbers must fit within [0, i64::MAX], received: {:?}",
                        new.seqno
                    ),
                };
            }

            // Check current head matches expected.
            let current_seqno = store.get(&key).and_then(|v| v.last()).map(|d| d.seqno);

            if current_seqno != expected_seqno {
                return ConsensusResponse::CompareAndSet { committed: false };
            }

            store.entry(key).or_default().push(new);
            ConsensusResponse::CompareAndSet { committed: true }
        }
        ConsensusRequest::Batch(requests) => {
            let responses = requests
                .into_iter()
                .map(|req| apply_request(store, req))
                .collect();
            ConsensusResponse::Batch(responses)
        }
        ConsensusRequest::Truncate { key, seqno } => {
            let seqno = SeqNo(seqno);

            // Check that the key exists and seqno <= current head.
            let current_head = store.get(&key).and_then(|v| v.last()).map(|d| d.seqno);

            if current_head.map_or(true, |head| head < seqno) {
                return ConsensusResponse::Error {
                    message: format!("upper bound too high for truncate: {:?}", seqno),
                };
            }

            let mut deleted = 0;
            if let Some(values) = store.get_mut(&key) {
                let count_before = values.len();
                values.retain(|val| val.seqno >= seqno);
                deleted = count_before - values.len();
            }

            ConsensusResponse::Truncate {
                deleted: Some(deleted),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_compare_and_set_basic() {
        let mut store = BTreeMap::new();

        let resp = apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: None,
                new_seqno: 5,
                new_data: b"abc".to_vec(),
            },
        );
        assert!(matches!(
            resp,
            ConsensusResponse::CompareAndSet { committed: true }
        ));

        let head = store.get("k1").and_then(|v| v.last()).unwrap();
        assert_eq!(head.seqno, SeqNo(5));
        assert_eq!(head.data, Bytes::from("abc"));
    }

    #[test]
    fn test_compare_and_set_mismatch() {
        let mut store = BTreeMap::new();

        apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: None,
                new_seqno: 5,
                new_data: b"abc".to_vec(),
            },
        );

        let resp = apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: Some(3),
                new_seqno: 10,
                new_data: b"def".to_vec(),
            },
        );
        assert!(matches!(
            resp,
            ConsensusResponse::CompareAndSet { committed: false }
        ));
    }

    #[test]
    fn test_truncate() {
        let mut store = BTreeMap::new();

        apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: None,
                new_seqno: 5,
                new_data: b"abc".to_vec(),
            },
        );
        apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: Some(5),
                new_seqno: 10,
                new_data: b"def".to_vec(),
            },
        );

        let resp = apply_request(
            &mut store,
            ConsensusRequest::Truncate {
                key: "k1".to_string(),
                seqno: 6,
            },
        );
        assert!(matches!(
            resp,
            ConsensusResponse::Truncate {
                deleted: Some(1),
            }
        ));

        let values = store.get("k1").unwrap();
        assert_eq!(values.len(), 1);
        assert_eq!(values[0].seqno, SeqNo(10));
    }

    #[test]
    fn test_batch_apply() {
        let mut store = BTreeMap::new();

        // Batch: insert k1, then insert k2, then insert k1 again (with correct expected).
        let resp = apply_request(
            &mut store,
            ConsensusRequest::Batch(vec![
                ConsensusRequest::CompareAndSet {
                    key: "k1".to_string(),
                    expected: None,
                    new_seqno: 1,
                    new_data: b"a".to_vec(),
                },
                ConsensusRequest::CompareAndSet {
                    key: "k2".to_string(),
                    expected: None,
                    new_seqno: 1,
                    new_data: b"b".to_vec(),
                },
                ConsensusRequest::CompareAndSet {
                    key: "k1".to_string(),
                    expected: Some(1),
                    new_seqno: 2,
                    new_data: b"c".to_vec(),
                },
            ]),
        );

        match resp {
            ConsensusResponse::Batch(responses) => {
                assert_eq!(responses.len(), 3);
                assert!(matches!(
                    responses[0],
                    ConsensusResponse::CompareAndSet { committed: true }
                ));
                assert!(matches!(
                    responses[1],
                    ConsensusResponse::CompareAndSet { committed: true }
                ));
                assert!(matches!(
                    responses[2],
                    ConsensusResponse::CompareAndSet { committed: true }
                ));
            }
            other => panic!("expected Batch response, got: {other:?}"),
        }

        // Verify state: k1 has seqno 2, k2 has seqno 1.
        assert_eq!(store.get("k1").unwrap().last().unwrap().seqno, SeqNo(2));
        assert_eq!(store.get("k2").unwrap().last().unwrap().seqno, SeqNo(1));
    }

    #[test]
    fn test_batch_partial_failure() {
        let mut store = BTreeMap::new();

        // Insert k1 first.
        apply_request(
            &mut store,
            ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: None,
                new_seqno: 5,
                new_data: b"abc".to_vec(),
            },
        );

        // Batch where second request has wrong expected seqno (3 instead of 5).
        // Note: new_seqno must be > expected for the request to be valid,
        // so we use expected=3, new_seqno=10 to get a CAS mismatch (not an error).
        let resp = apply_request(
            &mut store,
            ConsensusRequest::Batch(vec![
                ConsensusRequest::CompareAndSet {
                    key: "k2".to_string(),
                    expected: None,
                    new_seqno: 1,
                    new_data: b"ok".to_vec(),
                },
                ConsensusRequest::CompareAndSet {
                    key: "k1".to_string(),
                    expected: Some(3), // wrong: actual head is 5
                    new_seqno: 10,
                    new_data: b"fail".to_vec(),
                },
            ]),
        );

        match resp {
            ConsensusResponse::Batch(responses) => {
                assert_eq!(responses.len(), 2);
                // First succeeds.
                assert!(matches!(
                    responses[0],
                    ConsensusResponse::CompareAndSet { committed: true }
                ));
                // Second fails (expectation mismatch).
                assert!(matches!(
                    responses[1],
                    ConsensusResponse::CompareAndSet { committed: false }
                ));
            }
            other => panic!("expected Batch response, got: {other:?}"),
        }

        // k2 was created, k1 unchanged.
        assert_eq!(store.get("k2").unwrap().last().unwrap().seqno, SeqNo(1));
        assert_eq!(store.get("k1").unwrap().last().unwrap().seqno, SeqNo(5));
    }

    #[test]
    fn test_batch_single_element() {
        let mut store = BTreeMap::new();

        let resp = apply_request(
            &mut store,
            ConsensusRequest::Batch(vec![ConsensusRequest::CompareAndSet {
                key: "k1".to_string(),
                expected: None,
                new_seqno: 1,
                new_data: b"solo".to_vec(),
            }]),
        );

        match resp {
            ConsensusResponse::Batch(responses) => {
                assert_eq!(responses.len(), 1);
                assert!(matches!(
                    responses[0],
                    ConsensusResponse::CompareAndSet { committed: true }
                ));
            }
            other => panic!("expected Batch response, got: {other:?}"),
        }
    }

    #[test]
    fn test_batch_empty() {
        let mut store = BTreeMap::new();

        let resp = apply_request(&mut store, ConsensusRequest::Batch(vec![]));

        match resp {
            ConsensusResponse::Batch(responses) => {
                assert!(responses.is_empty());
            }
            other => panic!("expected Batch response, got: {other:?}"),
        }
    }
}
