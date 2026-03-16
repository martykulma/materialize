// Copyright Materialize, Inc. and contributors. All rights reserved.
//
// Use of this software is governed by the Business Source License
// included in the LICENSE file.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0.

//! A direct (non-timely) async pipeline for PostgreSQL sources.
//!
//! The pipeline is structured as a DAG of tokio tasks connected by unbounded channels:
//!
//! ```text
//!  ┌──────────────────────────────────┐
//!  │ Source Task (snapshot → repl)    │──── SourceEvent ────→ Decode Task
//!  └──────────────────────────────────┘                          │
//!            │                                               DecodedEvent
//!            │ MintRequest (initial)                             │
//!            ▼                                                   ▼
//!       Remap Task ◄── MintRequest ── LSN Poller        Reclock Task
//!            │                                          ↑       │
//!       RemapBinding ───────────────────────────────────┘  PersistEvent
//!                                                               │
//!                                                    ┌──────────┼──────────┐
//!                                                    ▼          ▼          ▼
//!                                              Persist[0]  Persist[1]  Persist[N]
//! ```
//!
//! Persist tasks build batches incrementally as data arrives, then `compare_and_append`
//! the completed batch when the frontier advances.

use std::collections::{BTreeMap, BTreeSet};
use std::pin::pin;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use differential_dataflow::consolidation;
use futures::{StreamExt, TryStreamExt};
use mz_ore::future::InTask;
use mz_ore::task;
use mz_persist_client::Diagnostics;
use mz_persist_client::cache::PersistClientCache;
use mz_persist_client::write::WriteHandle;
use mz_persist_types::codec_impls::UnitSchema;
use mz_repr::{Datum, Diff, GlobalId, Row};
use mz_sql_parser::ast::Ident;
use mz_sql_parser::ast::display::{AstDisplay, escaped_string_literal};
use mz_storage_types::StorageDiff;
use mz_storage_types::configuration::StorageConfiguration;
use mz_storage_types::controller::CollectionMetadata;
use mz_storage_types::sources::{
    IngestionDescription, MzOffset, PostgresSourceConnection, SourceData, SourceExportDetails,
    SourceTimestamp,
};
use postgres_replication::LogicalReplicationStream;
use postgres_replication::protocol::{LogicalReplicationMessage, ReplicationMessage, TupleData};
use timely::order::PartialOrder;
use timely::progress::Antichain;
use timely::progress::Timestamp as _;
use tokio::sync::mpsc;
use tokio_postgres::types::PgLsn;
use tracing::{info, trace, warn};

use mz_storage_client::client::{Status, StatusUpdate};

use crate::source::postgres::replication::unpack_tuple;
use crate::source::postgres::snapshot::{decode_copy_row, export_snapshot};
use crate::source::postgres::{
    DefiniteError, SourceOutputInfo, cast_row, ensure_replication_slot, fetch_max_lsn,
    fetch_slot_metadata, verify_schema,
};
use crate::storage_state::StorageState;

// ===========================================================================
// Message types for inter-task communication
// ===========================================================================

/// Messages from the Source task (snapshot + replication) to the Decode task.
/// Row data is pre-unpacked to `Row` to avoid TupleData (which is not Clone/Send-friendly).
enum SourceEvent {
    /// A decoded row for a specific table.
    Row {
        oid: u32,
        row: Row,
        diff: i64,
        lsn: MzOffset,
    },
    /// An error for a specific table OID (e.g., missing REPLICA IDENTITY).
    TableError {
        oid: u32,
        err: DefiniteError,
        lsn: MzOffset,
    },
    /// A Relation message from the replication stream — decode must update projections.
    Relation {
        rel_id: u32,
        columns: Vec<(String, usize)>,
    },
    /// A TRUNCATE of the given table OIDs.
    Truncate { oids: Vec<u32>, lsn: MzOffset },
    /// Source has consumed all data up to (but not including) this offset.
    Progress(MzOffset),
    /// Snapshot phase is complete at this LSN.
    SnapshotComplete { snapshot_lsn: MzOffset },
}

/// Messages from the Decode task to the Reclock task.
enum DecodedEvent {
    /// Decoded data for a specific export.
    Data {
        export_id: GlobalId,
        data: SourceData,
        diff: i64,
        lsn: MzOffset,
    },
    /// Source has consumed all data up to this offset.
    Progress(MzOffset),
    /// Snapshot phase is complete.
    SnapshotComplete { snapshot_lsn: MzOffset },
}

/// Requests to the Remap task to mint a new binding.
struct MintRequest {
    /// The new source upper (data up to this offset is covered).
    source_upper: MzOffset,
    /// The output timestamp to bind to.
    binding_ts: mz_repr::Timestamp,
}

/// A binding read back from the remap persist shard by the Reclock task.
struct RemapBinding {
    /// The output timestamp assigned.
    binding_ts: mz_repr::Timestamp,
    /// Source data below this offset maps to `binding_ts`.
    source_upper: MzOffset,
    /// The new upper of the remap shard (binding_ts + 1).
    new_ts_upper: Antichain<mz_repr::Timestamp>,
}

/// Messages from the Reclock task to per-export Persist tasks.
enum PersistEvent {
    /// A timestamped row to add to the current batch.
    Data(SourceData, mz_repr::Timestamp, StorageDiff),
    /// Seal the current batch up to this frontier and append it.
    Seal(Antichain<mz_repr::Timestamp>),
}

// ===========================================================================
// Shared state and guard
// ===========================================================================

/// Shared frontier state between the async tasks and worker 0's timely thread.
type SharedUppers = Arc<Mutex<BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>>>;

/// Shared status updates from async tasks to the timely thread.
type SharedStatusUpdates = Arc<Mutex<Vec<StatusUpdate>>>;

/// Guard for a direct (non-timely) PostgreSQL source pipeline.
/// Only created on worker 0. Dropping the guard aborts all async tasks.
pub struct DirectSourceGuard {
    shared_uppers: SharedUppers,
    shared_status_updates: SharedStatusUpdates,
    source_uppers:
        BTreeMap<GlobalId, std::rc::Rc<std::cell::RefCell<Antichain<mz_repr::Timestamp>>>>,
    _task: task::AbortOnDropHandle<()>,
}

impl DirectSourceGuard {
    /// Copy the latest frontier values from the shared state into this worker's source_uppers.
    /// Also drain any pending status updates into the provided vec.
    pub fn update_source_uppers(&self, status_updates_out: &mut Vec<StatusUpdate>) {
        let shared = self
            .shared_uppers
            .lock()
            .expect("shared_uppers lock poisoned");
        for (id, upper_rc) in &self.source_uppers {
            if let Some(observed) = shared.get(id) {
                let mut current = upper_rc.borrow_mut();
                if PartialOrder::less_than(&*current, observed) {
                    current.clone_from(observed);
                }
            }
        }
        drop(shared);

        let mut status = self
            .shared_status_updates
            .lock()
            .expect("shared_status_updates lock poisoned");
        status_updates_out.append(&mut *status);
    }
}

// ===========================================================================
// Entry point
// ===========================================================================

/// Spawn the direct PostgreSQL source pipeline as a DAG of tokio tasks.
/// Only called from worker 0.
pub fn spawn_direct_pg_source(
    primary_source_id: GlobalId,
    connection: PostgresSourceConnection,
    description: IngestionDescription<CollectionMetadata>,
    as_of: Antichain<mz_repr::Timestamp>,
    _resume_uppers: BTreeMap<GlobalId, Antichain<mz_repr::Timestamp>>,
    source_resume_uppers: BTreeMap<GlobalId, Vec<Row>>,
    storage_state: &StorageState,
) -> DirectSourceGuard {
    info!("spawn_direct_pg_source");
    let persist_clients = Arc::clone(&storage_state.persist_clients);
    let storage_configuration = storage_state.storage_configuration.clone();
    let now_fn = storage_state.now.clone();
    let remap_metadata = description.remap_metadata.clone();
    let remap_collection_id = description.remap_collection_id;

    // Build table_info and output_map.
    let mut table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>> = BTreeMap::new();
    let mut output_map: BTreeMap<usize, GlobalId> = BTreeMap::new();
    for (idx, (id, export)) in description.source_exports.iter().enumerate() {
        let details = match &export.details {
            SourceExportDetails::Postgres(details) => details,
            SourceExportDetails::None => continue,
            _ => panic!("unexpected source export details: {:?}", export.details),
        };
        let resume_upper = Antichain::from_iter(
            source_resume_uppers
                .get(id)
                .expect("all source exports must be present in source resume uppers")
                .iter()
                .map(MzOffset::decode_row),
        );
        let output = SourceOutputInfo {
            desc: details.table.clone(),
            projection: None,
            casts: details.column_casts.clone(),
            resume_upper,
            export_id: *id,
        };
        table_info
            .entry(output.desc.oid)
            .or_insert_with(BTreeMap::new)
            .insert(idx, output);
        output_map.insert(idx, *id);
    }

    // Capture Rc handles to source_uppers for the guard.
    let guard_source_uppers: BTreeMap<
        GlobalId,
        std::rc::Rc<std::cell::RefCell<Antichain<mz_repr::Timestamp>>>,
    > = description
        .collection_ids()
        .filter_map(|id| {
            storage_state
                .source_uppers
                .get(&id)
                .map(|rc| (id, rc.clone()))
        })
        .collect();

    // Report status.
    {
        let now = chrono::Utc::now();
        let mut updates = storage_state.shared_status_updates.borrow_mut();
        for id in
            std::iter::once(primary_source_id).chain(description.source_exports.keys().copied())
        {
            updates.push(StatusUpdate::new(id, now, Status::Starting));
            updates.push(StatusUpdate::new(id, now, Status::Running));
        }
    }

    let shared_uppers: SharedUppers = Arc::new(Mutex::new(BTreeMap::new()));
    let shared_status: SharedStatusUpdates = Arc::new(Mutex::new(Vec::new()));
    let source_exports = description.source_exports.clone();
    let task_shared_uppers = Arc::clone(&shared_uppers);
    let task_shared_status = Arc::clone(&shared_status);
    let all_ids: Vec<GlobalId> = std::iter::once(primary_source_id)
        .chain(description.source_exports.keys().copied())
        .collect();

    let handle = task::spawn(
        || format!("direct_pg_source:{primary_source_id}"),
        async move {
            loop {
                let result = run_pipeline(
                    primary_source_id,
                    &connection,
                    &source_exports,
                    &table_info,
                    &output_map,
                    &as_of,
                    &persist_clients,
                    &storage_configuration,
                    &now_fn,
                    &remap_metadata,
                    remap_collection_id,
                    &task_shared_uppers,
                )
                .await;

                match result {
                    Ok(()) => {
                        info!(%primary_source_id, "direct pg source completed cleanly");
                        return;
                    }
                    Err(err) => {
                        warn!(%primary_source_id, "direct pg source error, retrying: {err:#}");
                        // Report the error as a Stalled status for all source objects.
                        {
                            let now = chrono::Utc::now();
                            let error_str = format!("{err:#}");
                            let mut updates = task_shared_status.lock().expect("lock");
                            for &id in &all_ids {
                                updates.push(StatusUpdate {
                                    id,
                                    timestamp: now,
                                    status: Status::Stalled,
                                    error: Some(error_str.clone()),
                                    hints: Default::default(),
                                    namespaced_errors: Default::default(),
                                    replica_id: None,
                                });
                            }
                        }
                        tokio::time::sleep(Duration::from_secs(5)).await;
                    }
                }
            }
        },
    );

    DirectSourceGuard {
        shared_uppers,
        shared_status_updates: shared_status,
        source_uppers: guard_source_uppers,
        _task: handle.abort_on_drop(),
    }
}

// ===========================================================================
// Pipeline orchestrator
// ===========================================================================

/// Set up channels, spawn all tasks, and wait for completion.
#[allow(clippy::too_many_arguments)]
async fn run_pipeline(
    primary_source_id: GlobalId,
    connection: &PostgresSourceConnection,
    source_exports: &BTreeMap<
        GlobalId,
        mz_storage_types::sources::SourceExport<CollectionMetadata>,
    >,
    table_info: &BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_map: &BTreeMap<usize, GlobalId>,
    as_of: &Antichain<mz_repr::Timestamp>,
    persist_clients: &Arc<PersistClientCache>,
    storage_configuration: &StorageConfiguration,
    now_fn: &mz_ore::now::NowFn,
    remap_metadata: &CollectionMetadata,
    remap_collection_id: GlobalId,
    shared_uppers: &SharedUppers,
) -> Result<(), anyhow::Error> {
    // --- Channels ---
    let (source_tx, source_rx) = mpsc::unbounded_channel::<SourceEvent>();
    let (decoded_tx, decoded_rx) = mpsc::unbounded_channel::<DecodedEvent>();
    let (mint_tx, mint_rx) = mpsc::unbounded_channel::<MintRequest>();

    // Shared semaphore to bound concurrent compare_and_append operations across
    // all persist tasks, reserving connection pool capacity for remap, GC, and compaction.
    let max_appends = mz_storage_types::dyncfgs::PG_DIRECT_MAX_CONCURRENT_APPENDS
        .get(storage_configuration.config_set());
    info!(%primary_source_id, %max_appends, "configuring append concurrency");
    let append_semaphore = Arc::new(tokio::sync::Semaphore::new(max_appends));

    // Per-export persist channels (reclock → persist).
    let mut persist_txs: BTreeMap<GlobalId, mpsc::UnboundedSender<PersistEvent>> = BTreeMap::new();
    let mut persist_rxs: BTreeMap<GlobalId, mpsc::UnboundedReceiver<PersistEvent>> =
        BTreeMap::new();
    for id in source_exports.keys() {
        let (tx, rx) = mpsc::unbounded_channel::<PersistEvent>();
        persist_txs.insert(*id, tx);
        persist_rxs.insert(*id, rx);
    }

    // Direct channel from remap task → reclock task for bindings (bypasses persist).
    let (binding_tx, binding_rx) = mpsc::unbounded_channel::<RemapBinding>();

    // --- Connections ---
    let connection_config = connection
        .connection
        .config(
            &storage_configuration.connection_context.secrets_reader,
            storage_configuration,
            InTask::Yes,
        )
        .await?;

    let slot = connection.publication_details.slot.clone();
    let publication = connection.publication.clone();

    // Replication connection for slot management.
    let replication_client = connection_config
        .connect_replication(&storage_configuration.connection_context.ssh_tunnel_manager)
        .await?;
    ensure_replication_slot(&replication_client, &slot).await?;

    // Metadata connections.
    let metadata_client = connection_config
        .connect(
            "direct_pg metadata",
            &storage_configuration.connection_context.ssh_tunnel_manager,
        )
        .await?;
    let decode_metadata_client = connection_config
        .connect(
            "direct_pg decode_metadata",
            &storage_configuration.connection_context.ssh_tunnel_manager,
        )
        .await?;

    // --- Open persist writers, check shard uppers, and spawn persist tasks ---
    // We open writers first so we can check each shard's actual upper to determine
    // which tables truly need snapshotting. The source_resume_uppers from the
    // coordinator may be stale (e.g., pipeline was torn down before the controller
    // processed our FrontierUpper update), so we use the persist shard state as the
    // authoritative source of truth.
    let mut _persist_handles = Vec::new();
    // Track which export_ids already have data (upper > minimum).
    let mut exports_with_data: BTreeSet<GlobalId> = BTreeSet::new();
    for (id, export) in source_exports {
        let meta = &export.storage_metadata;
        let persist_client = persist_clients.open(meta.persist_location.clone()).await?;
        let mut write_handle = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                meta.data_shard,
                Arc::new(meta.relation_desc.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: id.to_string(),
                    handle_purpose: format!("direct_pg_source::export {}", id),
                },
            )
            .await?;

        let shard_upper = write_handle.fetch_recent_upper().await.clone();
        if shard_upper != Antichain::from_elem(mz_repr::Timestamp::minimum()) {
            info!(%id, ?shard_upper, "export shard already has data, skipping snapshot");
            exports_with_data.insert(*id);
        }

        let rx = persist_rxs.remove(id).unwrap();
        let uppers = Arc::clone(shared_uppers);
        let eid = *id;
        let sem = Arc::clone(&append_semaphore);
        _persist_handles.push(
            task::spawn(
                || format!("direct_pg_persist:{eid}"),
                persist_task(eid, write_handle, rx, uppers, sem),
            )
            .abort_on_drop(),
        );
    }

    // --- Determine tables needing snapshot ---
    // A table needs snapshotting only if at least one of its exports does NOT already
    // have data in its persist shard.
    let tables_to_snapshot: BTreeSet<u32> = table_info
        .iter()
        .filter(|(_oid, outputs)| {
            outputs
                .values()
                .any(|info| !exports_with_data.contains(&info.export_id))
        })
        .map(|(oid, _)| *oid)
        .collect();
    info!(%primary_source_id, ?tables_to_snapshot, "tables needing snapshot");

    // --- Remap writer ---
    let reclock = DirectReclockWriter::new(
        persist_clients,
        remap_metadata,
        remap_collection_id,
        primary_source_id,
    )
    .await?;
    // Capture the remap shard's current upper for the reclock task subscription start point.
    let remap_upper_at_start = reclock.upper.clone();

    // --- Spawn Remap task ---
    let remap_shared = Arc::clone(shared_uppers);
    let _remap_handle = task::spawn(
        || format!("direct_pg_remap:{primary_source_id}"),
        remap_task(reclock, mint_rx, remap_collection_id, remap_shared, binding_tx),
    )
    .abort_on_drop();

    // --- Spawn Decode task ---
    let decode_table_info = table_info.clone();
    let decode_output_map = output_map.clone();
    let decode_pub = publication.clone();
    let _decode_handle = task::spawn(
        || format!("direct_pg_decode:{primary_source_id}"),
        decode_task(
            source_rx,
            decoded_tx,
            decode_table_info,
            decode_output_map,
            decode_metadata_client,
            decode_pub,
        ),
    )
    .abort_on_drop();

    // --- Recover remap state from persist (one-time startup snapshot) ---
    // Read the accumulated remap bindings from the remap shard so we can
    // reconstruct the source_upper state from previous pipeline runs.
    // After this, bindings arrive directly from the remap task via channel.
    let startup_remap_state = {
        let persist_client = persist_clients
            .open(remap_metadata.persist_location.clone())
            .await?;
        let remap_desc =
            Arc::new(mz_storage_types::sources::postgres::PG_PROGRESS_DESC.clone());
        let mut read_handle = persist_client
            .open_leased_reader::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                remap_metadata.data_shard,
                remap_desc,
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: remap_collection_id.to_string(),
                    handle_purpose: format!("direct_pg_reclock_startup:{}", primary_source_id),
                },
                false,
            )
            .await
            .map_err(|e| anyhow::anyhow!("open remap reader for startup failed: {e}"))?;

        if remap_upper_at_start != Antichain::from_elem(mz_repr::Timestamp::minimum())
            && PartialOrder::less_than(as_of, &remap_upper_at_start)
        {
            info!(
                %primary_source_id,
                ?as_of,
                ?remap_upper_at_start,
                "recovering remap state from persist"
            );
            let snapshot = read_handle
                .snapshot_and_fetch(as_of.clone())
                .await
                .map_err(|since| {
                    anyhow::anyhow!("remap startup snapshot failed: since {:?}", since)
                })?;

            // Accumulate the differential state to recover the current source_upper.
            let mut accumulated: Vec<(MzOffset, Diff)> = Vec::new();
            for ((source_data, _), _ts, diff) in snapshot {
                if let SourceData(Ok(row)) = source_data {
                    let offset = MzOffset::decode_row(&row);
                    accumulated.push((offset, diff.into()));
                }
            }
            differential_dataflow::consolidation::consolidate(&mut accumulated);
            accumulated
        } else {
            // Remap shard is empty or not yet readable at as_of — nothing to recover.
            Vec::new()
        }
        // read_handle is dropped here, releasing the lease
    };

    // --- Spawn Reclock task ---
    // Receives bindings directly from the remap task via channel.
    // No persist subscribe, no critical since handle — the controller manages since.
    let reclock_persist_txs = persist_txs.clone();
    let _reclock_handle = task::spawn(
        || format!("direct_pg_reclock:{primary_source_id}"),
        reclock_task(
            primary_source_id,
            decoded_rx,
            reclock_persist_txs,
            binding_rx,
            startup_remap_state,
            exports_with_data,
        ),
    )
    .abort_on_drop();

    // --- Spawn Source task ---
    // The source task needs the connection_config to create new connections for
    // snapshot and replication. It also gets the mint_tx for the initial snapshot binding.
    let source_table_info = table_info.clone();
    let source_handle = task::spawn(
        || format!("direct_pg_source:{primary_source_id}"),
        source_task(
            primary_source_id,
            connection_config,
            storage_configuration.clone(),
            slot.clone(),
            publication.clone(),
            source_tx,
            mint_tx.clone(),
            now_fn.clone(),
            source_table_info,
            tables_to_snapshot,
            replication_client,
            metadata_client,
        ),
    );

    // Wait for the source task to complete (or error). We use abort_on_drop
    // so the source task is cancelled when run_pipeline is dropped.
    let source_handle = source_handle.abort_on_drop();
    let result = source_handle.await;

    // Drop the mint sender so the remap task knows no more mints are coming
    // (the LSN poller, if spawned, holds its own clone of mint_tx).
    drop(mint_tx);

    // Propagate the source error if any.
    result
}

// ===========================================================================
// Source task: snapshot + replication
// ===========================================================================

#[allow(clippy::too_many_arguments)]
async fn source_task(
    primary_source_id: GlobalId,
    connection_config: mz_postgres_util::tunnel::Config,
    storage_configuration: StorageConfiguration,
    slot: String,
    publication: String,
    source_tx: mpsc::UnboundedSender<SourceEvent>,
    mint_tx: mpsc::UnboundedSender<MintRequest>,
    now_fn: mz_ore::now::NowFn,
    table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    tables_to_snapshot: BTreeSet<u32>,
    replication_client: mz_postgres_util::Client,
    metadata_client: mz_postgres_util::Client,
) -> Result<(), anyhow::Error> {
    let mut snapshot_lsn = MzOffset::minimum();

    // =================== SNAPSHOT PHASE ===================
    if !tables_to_snapshot.is_empty() {
        info!(
            %primary_source_id,
            tables = ?tables_to_snapshot,
            "starting snapshot phase"
        );

        let tmp_slot = format!("mzsnapshot_{}", uuid::Uuid::new_v4()).replace('-', "");
        let (snapshot_id, snap_lsn) = export_snapshot(&replication_client, &tmp_slot, true).await?;
        snapshot_lsn = snap_lsn;
        info!(%primary_source_id, %snapshot_lsn, "got snapshot at LSN");

        let snapshot_client = connection_config
            .connect(
                "direct_pg snapshot",
                &storage_configuration.connection_context.ssh_tunnel_manager,
            )
            .await?;
        crate::source::postgres::snapshot::use_snapshot(&snapshot_client, &snapshot_id).await?;

        for &oid in &tables_to_snapshot {
            let outputs = match table_info.get(&oid) {
                Some(o) => o,
                None => continue,
            };

            let any_output = outputs.values().next().unwrap();
            let table_desc = &any_output.desc;

            let column_list = table_desc
                .columns
                .iter()
                .map(|c| Ident::new_unchecked(&c.name).to_ast_string_simple())
                .collect::<Vec<_>>()
                .join(", ");

            let schema = Ident::new_unchecked(&table_desc.namespace).to_ast_string_simple();
            let table = Ident::new_unchecked(&table_desc.name).to_ast_string_simple();
            let query = format!(
                "COPY (SELECT {column_list} FROM {schema}.{table}) TO STDOUT (FORMAT TEXT)"
            );

            let copy_stream = snapshot_client.copy_out(&*query).await?;
            let mut stream = pin!(copy_stream);
            let mut row_buf = Row::default();

            while let Some(chunk) = stream.try_next().await? {
                let data = &*chunk;
                let col_len = table_desc.columns.len();
                if let Err(err) = decode_copy_row(data, col_len, &mut row_buf) {
                    let _ = source_tx.send(SourceEvent::TableError {
                        oid,
                        err: err.into(),
                        lsn: snapshot_lsn,
                    });
                    continue;
                }

                let _ = source_tx.send(SourceEvent::Row {
                    oid,
                    row: row_buf.clone(),
                    diff: 1,
                    lsn: snapshot_lsn,
                });
            }
        }

        // Mint the initial binding: source_upper = snapshot_lsn + 1.
        // Use NOW() as the binding timestamp so the mint succeeds even when the
        // remap shard already has a high upper (e.g., from a previous pipeline run
        // that was torn down and restarted when new exports were added).
        let snapshot_binding_ts = mz_repr::Timestamp::from((now_fn)());
        info!(
            %primary_source_id,
            %snapshot_binding_ts,
            snapshot_source_upper = %(snapshot_lsn.offset + 1),
            "minting snapshot binding"
        );
        let _ = mint_tx.send(MintRequest {
            source_upper: MzOffset::from(snapshot_lsn.offset + 1),
            binding_ts: snapshot_binding_ts,
        });

        // Tell downstream that snapshot is done.
        let _ = source_tx.send(SourceEvent::Progress(MzOffset::from(
            snapshot_lsn.offset + 1,
        )));
        let _ = source_tx.send(SourceEvent::SnapshotComplete { snapshot_lsn });

        info!(%primary_source_id, "snapshot phase complete");
    }

    // =================== REPLICATION PHASE ===================
    info!(%primary_source_id, "starting replication phase");

    let slot_metadata = fetch_slot_metadata(
        &metadata_client,
        &slot,
        mz_storage_types::dyncfgs::PG_FETCH_SLOT_RESUME_LSN_INTERVAL
            .get(storage_configuration.config_set()),
    )
    .await?;

    // Kill stale connections to the replication slot.
    if let Some(active_pid) = slot_metadata.active_pid {
        warn!(
            %primary_source_id, %active_pid,
            "replication slot already in use; killing existing connection",
        );
        let _ = metadata_client
            .execute("SELECT pg_terminate_backend($1)", &[&active_pid])
            .await;
    }

    let resume_lsn = slot_metadata.confirmed_flush_lsn;
    info!(%primary_source_id, %resume_lsn, "starting replication from LSN");

    // Open a fresh replication connection for START_REPLICATION.
    let repl_client = connection_config
        .connect_replication(&storage_configuration.connection_context.ssh_tunnel_manager)
        .await?;

    let lsn = PgLsn::from(resume_lsn.offset);
    let query = format!(
        r#"START_REPLICATION SLOT "{}" LOGICAL {} ("proto_version" '1', "publication_names" {})"#,
        Ident::new_unchecked(&slot).to_ast_string_simple(),
        lsn,
        escaped_string_literal(&publication),
    );
    let copy_stream = repl_client.copy_both_simple(&query).await?;
    let mut stream = pin!(LogicalReplicationStream::new(copy_stream));

    // Track the data upper (latest LSN we've seen).
    let mut data_upper = resume_lsn;
    let mut last_committed_lsn = resume_lsn;

    // Spawn the LSN poller now that replication is active.
    // It uses the metadata_client to query pg_current_wal_lsn() and sends MintRequests.
    let lsn_poller_mint_tx = mint_tx.clone();
    let lsn_poller_now = now_fn.clone();
    let _lsn_poller_handle = task::spawn(
        || format!("direct_pg_lsn_poller:{primary_source_id}"),
        lsn_poller_task(metadata_client, lsn_poller_mint_tx, lsn_poller_now),
    );

    // Feedback timer for proactive standby status updates.
    let mut feedback_timer = tokio::time::interval(Duration::from_secs(1));
    feedback_timer.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    info!(%primary_source_id, "entering replication loop");

    loop {
        tokio::select! {
            msg = stream.next() => {
                let msg = match msg {
                    Some(Ok(msg)) => msg,
                    Some(Err(err)) => return Err(err.into()),
                    None => return Err(anyhow::anyhow!("replication stream ended")),
                };

                match msg {
                    ReplicationMessage::XLogData(xlog_data) => {
                        let message = xlog_data.into_data();
                        match message {
                            LogicalReplicationMessage::Begin(begin) => {
                                let commit_lsn =
                                    MzOffset::from(u64::from(begin.final_lsn()));
                                trace!(
                                    %primary_source_id, %commit_lsn,
                                    "received Begin"
                                );
                                // Process transaction inline, sending events to source_tx.
                                process_replication_transaction(
                                    &mut stream,
                                    &source_tx,
                                    commit_lsn,
                                    &tables_to_snapshot,
                                    snapshot_lsn,
                                )
                                .await?;
                                if commit_lsn > data_upper {
                                    data_upper = commit_lsn;
                                }
                                if commit_lsn > last_committed_lsn {
                                    last_committed_lsn = commit_lsn;
                                }
                                // Report progress after each transaction.
                                let _ = source_tx.send(SourceEvent::Progress(
                                    MzOffset::from(data_upper.offset + 1),
                                ));
                            }
                            LogicalReplicationMessage::Commit(_) => {}
                            _ => {}
                        }
                    }
                    ReplicationMessage::PrimaryKeepAlive(keepalive) => {
                        let server_lsn =
                            MzOffset::from(u64::from(keepalive.wal_end()));
                        if server_lsn > data_upper {
                            data_upper = server_lsn;
                        }
                        // Report progress so reclock can advance.
                        let _ = source_tx.send(SourceEvent::Progress(
                            MzOffset::from(data_upper.offset + 1),
                        ));

                        // Reply if requested.
                        if keepalive.reply() == 1 {
                            let ts: i64 = pg_epoch()
                                .elapsed()
                                .unwrap()
                                .as_micros()
                                .try_into()
                                .unwrap();
                            let lsn = PgLsn::from(last_committed_lsn.offset);
                            stream
                                .as_mut()
                                .standby_status_update(lsn, lsn, lsn, ts, 0)
                                .await?;
                        }
                    }
                    _ => {
                        return Err(anyhow::anyhow!("unexpected replication message"));
                    }
                }
            }
            _ = feedback_timer.tick() => {
                // Proactively send standby status with reply=1 to get PrimaryKeepAlive
                // messages back promptly. This drives frontier advancement.
                let ts: i64 = pg_epoch().elapsed().unwrap().as_micros().try_into().unwrap();
                let lsn = PgLsn::from(last_committed_lsn.offset);
                stream
                    .as_mut()
                    .standby_status_update(lsn, lsn, lsn, ts, 1)
                    .await?;
            }
        }
    }
}

/// Process a single transaction from the replication stream, sending events to the source channel.
async fn process_replication_transaction(
    stream: &mut std::pin::Pin<&mut LogicalReplicationStream>,
    source_tx: &mpsc::UnboundedSender<SourceEvent>,
    commit_lsn: MzOffset,
    tables_to_snapshot: &BTreeSet<u32>,
    snapshot_lsn: MzOffset,
) -> Result<(), anyhow::Error> {
    let mut row_buf = Row::default();

    loop {
        let msg = match stream.next().await {
            Some(Ok(msg)) => msg,
            Some(Err(err)) => return Err(err.into()),
            None => {
                return Err(anyhow::anyhow!(
                    "replication stream ended during transaction"
                ));
            }
        };

        match msg {
            ReplicationMessage::XLogData(xlog_data) => {
                let message = xlog_data.into_data();
                match message {
                    LogicalReplicationMessage::Insert(body) => {
                        let rel_id = body.rel_id();
                        if tables_to_snapshot.contains(&rel_id) && commit_lsn <= snapshot_lsn {
                            continue;
                        }
                        let tuple_data = body.tuple().tuple_data();
                        match unpack_tuple(tuple_data.iter(), &mut row_buf) {
                            Ok(row) => {
                                let _ = source_tx.send(SourceEvent::Row {
                                    oid: rel_id,
                                    row,
                                    diff: 1,
                                    lsn: commit_lsn,
                                });
                            }
                            Err(err) => {
                                let _ = source_tx.send(SourceEvent::TableError {
                                    oid: rel_id,
                                    err: err.into(),
                                    lsn: commit_lsn,
                                });
                            }
                        }
                    }
                    LogicalReplicationMessage::Update(body) => {
                        let rel_id = body.rel_id();
                        if tables_to_snapshot.contains(&rel_id) && commit_lsn <= snapshot_lsn {
                            continue;
                        }
                        match body.old_tuple() {
                            Some(old_tuple) => {
                                // Retract old row.
                                match unpack_tuple(old_tuple.tuple_data().iter(), &mut row_buf) {
                                    Ok(row) => {
                                        let _ = source_tx.send(SourceEvent::Row {
                                            oid: rel_id,
                                            row,
                                            diff: -1,
                                            lsn: commit_lsn,
                                        });
                                    }
                                    Err(err) => {
                                        let _ = source_tx.send(SourceEvent::TableError {
                                            oid: rel_id,
                                            err: err.into(),
                                            lsn: commit_lsn,
                                        });
                                        continue;
                                    }
                                }
                                // Insert new row with Toast resolution.
                                let new_tuple = body.new_tuple();
                                let resolved: Vec<&TupleData> = new_tuple
                                    .tuple_data()
                                    .iter()
                                    .zip(old_tuple.tuple_data().iter())
                                    .map(|(new, old)| match new {
                                        TupleData::UnchangedToast => old,
                                        _ => new,
                                    })
                                    .collect();
                                match unpack_tuple(resolved.into_iter(), &mut row_buf) {
                                    Ok(row) => {
                                        let _ = source_tx.send(SourceEvent::Row {
                                            oid: rel_id,
                                            row,
                                            diff: 1,
                                            lsn: commit_lsn,
                                        });
                                    }
                                    Err(err) => {
                                        let _ = source_tx.send(SourceEvent::TableError {
                                            oid: rel_id,
                                            err: err.into(),
                                            lsn: commit_lsn,
                                        });
                                    }
                                }
                            }
                            None => {
                                let _ = source_tx.send(SourceEvent::TableError {
                                    oid: rel_id,
                                    err: DefiniteError::DefaultReplicaIdentity,
                                    lsn: commit_lsn,
                                });
                            }
                        }
                    }
                    LogicalReplicationMessage::Delete(body) => {
                        let rel_id = body.rel_id();
                        if tables_to_snapshot.contains(&rel_id) && commit_lsn <= snapshot_lsn {
                            continue;
                        }
                        match body.old_tuple() {
                            Some(old_tuple) => {
                                match unpack_tuple(old_tuple.tuple_data().iter(), &mut row_buf) {
                                    Ok(row) => {
                                        let _ = source_tx.send(SourceEvent::Row {
                                            oid: rel_id,
                                            row,
                                            diff: -1,
                                            lsn: commit_lsn,
                                        });
                                    }
                                    Err(err) => {
                                        let _ = source_tx.send(SourceEvent::TableError {
                                            oid: rel_id,
                                            err: err.into(),
                                            lsn: commit_lsn,
                                        });
                                    }
                                }
                            }
                            None => {
                                let _ = source_tx.send(SourceEvent::TableError {
                                    oid: rel_id,
                                    err: DefiniteError::DefaultReplicaIdentity,
                                    lsn: commit_lsn,
                                });
                            }
                        }
                    }
                    LogicalReplicationMessage::Relation(body) => {
                        let columns: Vec<(String, usize)> = body
                            .columns()
                            .iter()
                            .enumerate()
                            .map(|(idx, col)| (col.name().unwrap().to_string(), idx))
                            .collect();
                        let _ = source_tx.send(SourceEvent::Relation {
                            rel_id: body.rel_id(),
                            columns,
                        });
                    }
                    LogicalReplicationMessage::Truncate(body) => {
                        let _ = source_tx.send(SourceEvent::Truncate {
                            oids: body.rel_ids().to_vec(),
                            lsn: commit_lsn,
                        });
                    }
                    LogicalReplicationMessage::Commit(body) => {
                        if commit_lsn != body.commit_lsn().into() {
                            return Err(anyhow::anyhow!(
                                "LSN mismatch: expected {} got {}",
                                commit_lsn,
                                MzOffset::from(u64::from(body.commit_lsn()))
                            ));
                        }
                        return Ok(());
                    }
                    LogicalReplicationMessage::Origin(_) | LogicalReplicationMessage::Type(_) => {}
                    LogicalReplicationMessage::Begin(_) => {
                        return Err(anyhow::anyhow!("nested BEGIN in transaction"));
                    }
                    _ => {
                        return Err(anyhow::anyhow!("unknown logical replication message"));
                    }
                }
            }
            ReplicationMessage::PrimaryKeepAlive(_) => continue,
            _ => {
                return Err(anyhow::anyhow!(
                    "unexpected replication message in transaction"
                ));
            }
        }
    }
}

// ===========================================================================
// Decode task
// ===========================================================================

async fn decode_task(
    mut source_rx: mpsc::UnboundedReceiver<SourceEvent>,
    decoded_tx: mpsc::UnboundedSender<DecodedEvent>,
    mut table_info: BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>,
    output_map: BTreeMap<usize, GlobalId>,
    metadata_client: mz_postgres_util::Client,
    publication: String,
) -> Result<(), anyhow::Error> {
    while let Some(event) = source_rx.recv().await {
        match event {
            SourceEvent::Row {
                oid,
                row,
                diff,
                lsn,
            } => {
                let outputs = match table_info.get(&oid) {
                    Some(o) => o,
                    None => continue,
                };

                let all_datums: Vec<Datum<'_>> = row.iter().collect();

                for (output_idx, info) in outputs {
                    let Some(export_id) = output_map.get(output_idx) else {
                        continue;
                    };

                    // Apply projection if present (replication Relation reordering).
                    let datums: Vec<Datum<'_>> = if let Some(ref projection) = info.projection {
                        projection.iter().map(|idx| all_datums[*idx]).collect()
                    } else {
                        all_datums.clone()
                    };

                    let mut output_row = Row::default();
                    match cast_row(&info.casts, &datums, &mut output_row) {
                        Ok(()) => {
                            let _ = decoded_tx.send(DecodedEvent::Data {
                                export_id: *export_id,
                                data: SourceData(Ok(output_row)),
                                diff,
                                lsn,
                            });
                        }
                        Err(err) => {
                            let _ = decoded_tx.send(DecodedEvent::Data {
                                export_id: *export_id,
                                data: SourceData(Err(err.into())),
                                diff: 1,
                                lsn,
                            });
                        }
                    }
                }
            }
            SourceEvent::TableError { oid, err, lsn } => {
                if let Some(outputs) = table_info.get(&oid) {
                    for (output_idx, _) in outputs {
                        if let Some(export_id) = output_map.get(output_idx) {
                            let _ = decoded_tx.send(DecodedEvent::Data {
                                export_id: *export_id,
                                data: SourceData(Err(err.clone().into())),
                                diff: 1,
                                lsn,
                            });
                        }
                    }
                }
            }
            SourceEvent::Relation { rel_id, columns } => {
                if let Some(outputs) = table_info.get_mut(&rel_id) {
                    // Verify schema against upstream.
                    let upstream_info = mz_postgres_util::publication_info(
                        &metadata_client,
                        &publication,
                        Some(&[rel_id]),
                    )
                    .await?;

                    outputs.retain(|_output_index, info| {
                        verify_schema(rel_id, info, &upstream_info).is_ok()
                    });

                    // Recalculate projections from the new column ordering.
                    let column_positions: BTreeMap<_, _> = columns
                        .iter()
                        .map(|(name, idx)| (name.clone(), *idx))
                        .collect();
                    for info in outputs.values_mut() {
                        let mut projection = vec![];
                        for col in info.desc.columns.iter() {
                            projection.push(column_positions[&*col.name]);
                        }
                        info.projection = Some(projection);
                    }
                }
            }
            SourceEvent::Truncate { oids, lsn } => {
                for oid in oids {
                    if let Some(outputs) = table_info.get_mut(&oid) {
                        for (output_idx, _) in std::mem::take(outputs) {
                            if let Some(export_id) = output_map.get(&output_idx) {
                                let _ = decoded_tx.send(DecodedEvent::Data {
                                    export_id: *export_id,
                                    data: SourceData(Err(DefiniteError::TableTruncated.into())),
                                    diff: 1,
                                    lsn,
                                });
                            }
                        }
                    }
                }
            }
            SourceEvent::Progress(offset) => {
                let _ = decoded_tx.send(DecodedEvent::Progress(offset));
            }
            SourceEvent::SnapshotComplete { snapshot_lsn } => {
                let _ = decoded_tx.send(DecodedEvent::SnapshotComplete { snapshot_lsn });
            }
        }
    }
    Ok(())
}

// ===========================================================================
// Remap task
// ===========================================================================

async fn remap_task(
    mut reclock: DirectReclockWriter,
    mut mint_rx: mpsc::UnboundedReceiver<MintRequest>,
    remap_collection_id: GlobalId,
    shared_uppers: SharedUppers,
    binding_tx: mpsc::UnboundedSender<RemapBinding>,
) -> Result<(), anyhow::Error> {
    while let Some(req) = mint_rx.recv().await {
        let new_source_upper = Antichain::from_elem(req.source_upper);
        let new_into_upper = Antichain::from_elem(req.binding_ts.step_forward());

        reclock
            .mint(req.binding_ts, new_into_upper.clone(), &new_source_upper)
            .await?;

        // Report remap frontier progress.
        shared_uppers
            .lock()
            .expect("poisoned")
            .insert(remap_collection_id, new_into_upper.clone());

        // Send binding directly to the reclock task (bypasses persist subscribe).
        let _ = binding_tx.send(RemapBinding {
            binding_ts: req.binding_ts,
            source_upper: req.source_upper,
            new_ts_upper: new_into_upper,
        });
    }
    Ok(())
}

// ===========================================================================
// Reclock task
// ===========================================================================

async fn reclock_task(
    primary_source_id: GlobalId,
    mut decoded_rx: mpsc::UnboundedReceiver<DecodedEvent>,
    persist_txs: BTreeMap<GlobalId, mpsc::UnboundedSender<PersistEvent>>,
    mut binding_rx: mpsc::UnboundedReceiver<RemapBinding>,
    startup_remap_state: Vec<(MzOffset, Diff)>,
    exports_with_data: BTreeSet<GlobalId>,
) -> Result<(), anyhow::Error> {
    info!(
        %primary_source_id,
        startup_entries = startup_remap_state.len(),
        "reclock task starting"
    );

    // --- State ---
    // Buffer of decoded data not yet assigned a timestamp.
    let mut buffered: Vec<(GlobalId, SourceData, i64, MzOffset)> = Vec::new();
    // Source progress: we've received all source data below this offset.
    let mut source_progress = MzOffset::minimum();
    // Pending bindings waiting to be applied.
    let mut pending_bindings: Vec<RemapBinding> = Vec::new();
    // Exports that have received at least one Data event. Only these get Seal events.
    let mut activated_exports: BTreeSet<GlobalId> = exports_with_data;
    // Accumulated remap state from startup snapshot. New bindings from the
    // remap task arrive pre-computed (no differential decoding needed).
    let mut _remap_accumulated: Vec<(MzOffset, Diff)> = startup_remap_state;

    loop {
        tokio::select! {
            // Read decoded source data.
            event = decoded_rx.recv() => {
                match event {
                    Some(DecodedEvent::Data { export_id, data, diff, lsn }) => {
                        buffered.push((export_id, data, diff, lsn));
                    }
                    Some(DecodedEvent::Progress(offset)) => {
                        if offset > source_progress {
                            source_progress = offset;
                        }
                        apply_bindings(
                            &mut buffered,
                            &mut pending_bindings,
                            source_progress,
                            &persist_txs,
                            &mut activated_exports,
                        );
                    }
                    Some(DecodedEvent::SnapshotComplete { .. }) => {
                        for id in persist_txs.keys() {
                            activated_exports.insert(*id);
                        }
                        info!(
                            %primary_source_id,
                            "snapshot complete, activated all exports for frontier advancement"
                        );
                    }
                    None => return Ok(()),
                }
            }
            // Receive bindings directly from the remap task (no persist round-trip).
            binding = binding_rx.recv() => {
                match binding {
                    Some(binding) => {
                        pending_bindings.push(binding);
                        apply_bindings(
                            &mut buffered,
                            &mut pending_bindings,
                            source_progress,
                            &persist_txs,
                            &mut activated_exports,
                        );
                    }
                    None => {
                        // Remap task is gone (pipeline shutting down).
                        return Ok(());
                    }
                }
            }
        }
    }
}

/// Apply any pending bindings whose source_upper <= source_progress.
/// Only sends Seals to exports that have had at least one Data event emitted,
/// preventing empty Seals from advancing new exports' frontiers prematurely.
fn apply_bindings(
    buffered: &mut Vec<(GlobalId, SourceData, i64, MzOffset)>,
    pending_bindings: &mut Vec<RemapBinding>,
    source_progress: MzOffset,
    persist_txs: &BTreeMap<GlobalId, mpsc::UnboundedSender<PersistEvent>>,
    activated_exports: &mut BTreeSet<GlobalId>,
) {
    // Sort bindings by timestamp so we apply them in order.
    pending_bindings.sort_by_key(|b| b.binding_ts);

    let mut applied = Vec::new();
    for (idx, binding) in pending_bindings.iter().enumerate() {
        // We can only apply this binding if the source has delivered all data
        // up to the binding's source_upper.
        if source_progress < binding.source_upper {
            trace!(
                %source_progress,
                source_upper = %binding.source_upper,
                binding_ts = %binding.binding_ts,
                "apply_bindings: source not caught up, waiting"
            );
            break;
        }

        info!(
            binding_ts = %binding.binding_ts,
            source_upper = %binding.source_upper,
            buffered_count = buffered.len(),
            "apply_bindings: applying binding"
        );

        // Emit all buffered data with lsn < source_upper at this binding's timestamp.
        let mut remaining = Vec::new();
        let mut emitted = 0usize;
        for (export_id, data, diff, lsn) in buffered.drain(..) {
            if lsn < binding.source_upper {
                if let Some(tx) = persist_txs.get(&export_id) {
                    let _ = tx.send(PersistEvent::Data(data, binding.binding_ts, diff));
                    emitted += 1;
                    activated_exports.insert(export_id);
                }
            } else {
                remaining.push((export_id, data, diff, lsn));
            }
        }
        *buffered = remaining;

        info!(
            binding_ts = %binding.binding_ts,
            %emitted,
            remaining = buffered.len(),
            "apply_bindings: emitted data, sending Seal"
        );

        // Only seal exports that have been activated (received at least one Data event).
        // Sealing exports that have no data would advance their frontier prematurely,
        // causing data loss if the pipeline is torn down before the actual snapshot
        // data arrives at a later binding.
        for (id, tx) in persist_txs {
            if activated_exports.contains(id) {
                let _ = tx.send(PersistEvent::Seal(binding.new_ts_upper.clone()));
            }
        }

        applied.push(idx);
    }

    // Remove applied bindings (in reverse to preserve indices).
    for idx in applied.into_iter().rev() {
        pending_bindings.remove(idx);
    }
}

// ===========================================================================
// Persist task (one per export)
// ===========================================================================

async fn persist_task(
    export_id: GlobalId,
    mut write_handle: WriteHandle<SourceData, (), mz_repr::Timestamp, StorageDiff>,
    mut rx: mpsc::UnboundedReceiver<PersistEvent>,
    shared_uppers: SharedUppers,
    append_semaphore: Arc<tokio::sync::Semaphore>,
) -> Result<(), anyhow::Error> {
    let mut current_upper = write_handle.fetch_recent_upper().await.clone();
    info!(%export_id, ?current_upper, "persist_task starting");

    // If the shard has already been finalized (empty upper), nothing to do.
    if current_upper.is_empty() {
        info!(%export_id, "persist shard already finalized, exiting");
        return Ok(());
    }

    // We accumulate data in a batch builder. When a Seal arrives, we finish
    // the batch and compare_and_append it.
    let mut builder: Option<
        mz_persist_client::batch::BatchBuilder<SourceData, (), mz_repr::Timestamp, StorageDiff>,
    > = None;
    let mut batch_has_data = false;

    while let Some(event) = rx.recv().await {
        // If current_upper has been driven to empty (shard finalized), stop.
        if current_upper.is_empty() {
            info!(%export_id, "persist shard finalized during operation, exiting");
            return Ok(());
        }

        match event {
            PersistEvent::Data(data, ts, diff) => {
                // Lazily create a batch builder.
                if builder.is_none() {
                    builder = Some(write_handle.builder(current_upper.clone()));
                }
                let b = builder.as_mut().unwrap();
                b.add(&data, &(), &ts, &diff)
                    .await
                    .map_err(|e| anyhow::anyhow!("batch add failed for {}: {}", export_id, e))?;
                batch_has_data = true;
            }
            PersistEvent::Seal(mut new_upper) => {
                if !PartialOrder::less_than(&current_upper, &new_upper) {
                    continue;
                }

                // Coalesce: drain any additional Seal/Data events that have
                // queued up. This is especially effective when many tasks are
                // blocked on the semaphore — seals accumulate and we skip
                // straight to the latest one, reducing total appends.
                while let Ok(event) = rx.try_recv() {
                    match event {
                        PersistEvent::Data(data, ts, diff) => {
                            if builder.is_none() {
                                builder = Some(write_handle.builder(current_upper.clone()));
                            }
                            let b = builder.as_mut().unwrap();
                            b.add(&data, &(), &ts, &diff).await.map_err(|e| {
                                anyhow::anyhow!("batch add failed for {}: {}", export_id, e)
                            })?;
                            batch_has_data = true;
                        }
                        PersistEvent::Seal(newer_upper) => {
                            if PartialOrder::less_than(&new_upper, &newer_upper) {
                                new_upper = newer_upper;
                            }
                        }
                    }
                }

                // Acquire a semaphore permit before hitting consensus.
                let _permit = append_semaphore.acquire().await.map_err(|_| {
                    anyhow::anyhow!("append semaphore closed for {}", export_id)
                })?;

                if let Some(b) = builder.take() {
                    let mut batch = b.finish(new_upper.clone()).await.map_err(|e| {
                        anyhow::anyhow!("batch finish failed for {}: {}", export_id, e)
                    })?;
                    match write_handle
                        .compare_and_append_batch(
                            &mut [&mut batch],
                            current_upper.clone(),
                            new_upper.clone(),
                            true,
                        )
                        .await
                    {
                        Ok(Ok(())) => {
                            if batch_has_data {
                                info!(%export_id, "batch appended");
                            }
                            current_upper = new_upper.clone();
                            shared_uppers
                                .lock()
                                .expect("poisoned")
                                .insert(export_id, new_upper.clone());
                            // No confirmation needed — controller manages remap since.
                        }
                        Ok(Err(mismatch)) => {
                            warn!(
                                %export_id,
                                "compare_and_append upper mismatch: {:?}",
                                mismatch,
                            );
                            current_upper = mismatch.current;
                        }
                        Err(err) => {
                            return Err(anyhow::anyhow!(
                                "compare_and_append failed for {}: {}",
                                export_id,
                                err,
                            ));
                        }
                    }
                } else {
                    // No data — empty batch to advance the upper.
                    let b = write_handle.builder(current_upper.clone());
                    let mut batch = b.finish(new_upper.clone()).await.map_err(|e| {
                        anyhow::anyhow!("empty batch finish failed for {}: {}", export_id, e)
                    })?;
                    match write_handle
                        .compare_and_append_batch(
                            &mut [&mut batch],
                            current_upper.clone(),
                            new_upper.clone(),
                            true,
                        )
                        .await
                    {
                        Ok(Ok(())) => {
                            current_upper = new_upper.clone();
                            shared_uppers
                                .lock()
                                .expect("poisoned")
                                .insert(export_id, new_upper.clone());
                            // No confirmation needed — controller manages remap since.
                        }
                        Ok(Err(mismatch)) => {
                            current_upper = mismatch.current;
                        }
                        Err(err) => {
                            return Err(anyhow::anyhow!(
                                "empty compare_and_append failed for {}: {}",
                                export_id,
                                err,
                            ));
                        }
                    }
                }
                batch_has_data = false;
            }
        }
    }
    Ok(())
}

// ===========================================================================
// LSN poller task
// ===========================================================================

async fn lsn_poller_task(
    metadata_client: mz_postgres_util::Client,
    mint_tx: mpsc::UnboundedSender<MintRequest>,
    now_fn: mz_ore::now::NowFn,
) -> Result<(), anyhow::Error> {
    let mut interval = tokio::time::interval(Duration::from_secs(1));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);

    loop {
        interval.tick().await;

        let max_lsn = match fetch_max_lsn(&metadata_client).await {
            Ok(lsn) => lsn,
            Err(e) => {
                warn!("lsn poller: failed to fetch max LSN: {e}");
                continue;
            }
        };

        let binding_ts = mz_repr::Timestamp::from((now_fn)());
        if mint_tx
            .send(MintRequest {
                source_upper: max_lsn,
                binding_ts,
            })
            .is_err()
        {
            // Remap task is gone.
            return Ok(());
        }
    }
}

// ===========================================================================
// DirectReclockWriter (remap shard writer)
// ===========================================================================

struct DirectReclockWriter {
    write_handle: WriteHandle<SourceData, (), mz_repr::Timestamp, StorageDiff>,
    upper: Antichain<mz_repr::Timestamp>,
    source_upper: Vec<MzOffset>,
}

impl DirectReclockWriter {
    async fn new(
        persist_clients: &Arc<PersistClientCache>,
        remap_metadata: &CollectionMetadata,
        remap_collection_id: GlobalId,
        primary_source_id: GlobalId,
    ) -> Result<Self, anyhow::Error> {
        let persist_client = persist_clients
            .open(remap_metadata.persist_location.clone())
            .await?;

        let write_handle = persist_client
            .open_writer::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                remap_metadata.data_shard,
                Arc::new(mz_storage_types::sources::postgres::PG_PROGRESS_DESC.clone()),
                Arc::new(UnitSchema),
                Diagnostics {
                    shard_name: remap_collection_id.to_string(),
                    handle_purpose: format!("direct_pg_source::reclock {}", primary_source_id),
                },
            )
            .await?;

        let upper = write_handle.upper().clone();

        // If the remap shard already has data, read back the current source_upper.
        let source_upper = if upper != Antichain::from_elem(mz_repr::Timestamp::minimum()) {
            let mut read_handle = persist_client
                .open_leased_reader::<SourceData, (), mz_repr::Timestamp, StorageDiff>(
                    remap_metadata.data_shard,
                    Arc::new(mz_storage_types::sources::postgres::PG_PROGRESS_DESC.clone()),
                    Arc::new(UnitSchema),
                    Diagnostics {
                        shard_name: remap_collection_id.to_string(),
                        handle_purpose: format!(
                            "direct_pg_source::reclock_read {}",
                            primary_source_id
                        ),
                    },
                    false,
                )
                .await
                .map_err(|e| anyhow::anyhow!("open remap reader failed: {e}"))?;

            let as_of = upper
                .as_option()
                .map(|ts| {
                    let t = ts.step_back().unwrap_or(mz_repr::Timestamp::minimum());
                    Antichain::from_elem(t)
                })
                .unwrap_or_else(|| Antichain::from_elem(mz_repr::Timestamp::minimum()));

            let mut entries: Vec<(MzOffset, Diff)> = Vec::new();
            let cursor = read_handle
                .snapshot_and_fetch(as_of)
                .await
                .map_err(|since| anyhow::anyhow!("remap since {:?} is beyond our as_of", since))?;

            for ((source_data, _), _ts, diff) in cursor {
                if let SourceData(Ok(row)) = source_data {
                    let offset = MzOffset::decode_row(&row);
                    entries.push((offset, diff.into()));
                }
            }
            differential_dataflow::consolidation::consolidate(&mut entries);
            let result: Vec<MzOffset> = entries
                .into_iter()
                .filter(|(_, diff)| *diff > Diff::ZERO)
                .map(|(offset, _)| offset)
                .collect();
            if result.is_empty() {
                vec![MzOffset::minimum()]
            } else {
                result
            }
        } else {
            // Empty remap shard — nothing to retract on first mint.
            vec![]
        };

        info!(
            %primary_source_id,
            ?upper,
            ?source_upper,
            "initialized reclock writer"
        );

        Ok(Self {
            write_handle,
            upper,
            source_upper,
        })
    }

    /// Mint a reclock binding: record that `new_source_upper` maps to `binding_ts`.
    async fn mint(
        &mut self,
        binding_ts: mz_repr::Timestamp,
        new_into_upper: Antichain<mz_repr::Timestamp>,
        new_source_upper: &Antichain<MzOffset>,
    ) -> Result<(), anyhow::Error> {
        let mut updates: Vec<(MzOffset, mz_repr::Timestamp, Diff)> = Vec::new();
        for src_ts in &self.source_upper {
            updates.push((src_ts.clone(), binding_ts, Diff::MINUS_ONE));
        }
        for src_ts in new_source_upper.elements() {
            updates.push((src_ts.clone(), binding_ts, Diff::ONE));
        }
        consolidation::consolidate_updates(&mut updates);

        let row_updates: Vec<_> = updates
            .iter()
            .map(|(from_ts, into_ts, diff)| {
                (
                    (SourceData(Ok(from_ts.encode_row())), ()),
                    into_ts.clone(),
                    diff.into_inner(),
                )
            })
            .collect();

        loop {
            match self
                .write_handle
                .compare_and_append(&row_updates, self.upper.clone(), new_into_upper.clone())
                .await
            {
                Ok(Ok(())) => {
                    self.upper = new_into_upper.clone();
                    self.source_upper = new_source_upper.elements().to_vec();
                    return Ok(());
                }
                Ok(Err(mismatch)) => {
                    self.upper = mismatch.current;
                    if !PartialOrder::less_than(&self.upper, &new_into_upper) {
                        self.source_upper = new_source_upper.elements().to_vec();
                        return Ok(());
                    }
                }
                Err(err) => {
                    return Err(anyhow::anyhow!(
                        "reclock compare_and_append failed: {}",
                        err
                    ));
                }
            }
        }
    }
}

// ===========================================================================
// Helpers
// ===========================================================================

/// Postgres epoch is 2000-01-01T00:00:00Z
fn pg_epoch() -> std::time::SystemTime {
    std::time::UNIX_EPOCH + Duration::from_secs(946_684_800)
}
