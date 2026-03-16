# Skunkworks: Timely-free PostgreSQL Source

## Context

Replace the timely-based PostgreSQL source pipeline with a pure tokio async implementation.
The new source is started from `build_ingestion_dataflow`, bypasses all timely/differential
operators, and writes directly to persist. The dyncfg `DIRECT_PG_SOURCE` (default `true`)
controls whether new PG sources use this path.

**Status: Working.** The `test/pg-cdc/pg-cdc.td` integration test passes end-to-end, covering
snapshot, replication, schema changes, publication drops (error reporting), multi-table sources,
and pipeline restarts when new exports are added.

## Architecture

The pipeline is a DAG of tokio tasks connected by unbounded mpsc channels. Only worker 0
spawns the pipeline; all other workers skip. The outer task retries the entire pipeline on
transient errors.

```
build_ingestion_dataflow (render.rs, worker 0 only)
  └─ spawn_direct_pg_source() → DirectSourceGuard
       └─ retry loop → run_pipeline()
            │
            ├─ Source Task (snapshot → replication)
            │     │
            │     ├── SourceEvent ────→ Decode Task
            │     │                          │
            │     │                      DecodedEvent
            │     │ MintRequest (initial)    │
            │     ▼                          ▼
            │  Remap Task ──► Remap Shard    Reclock Task ◄── Remap Shard (subscribe)
            │     │       (write bindings)   ↑  (critical since handle)
            │     │                          │       │
            │     │              persist confirm     PersistEvent
            │     │                          │       │
            │     │                      ┌───┼───────┼──────────┐
            │     │                      │   │       ▼          │
            │     │                      │ Persist[0]  Persist[1]  Persist[N]
            │     │                      │   │                      │
            │     │                      │   └── confirm ──────────►│
            │     │                      │                          │
            │
            └─ LSN Poller Task (polls pg_current_wal_lsn every 1s)
```

**Key data flow for bindings**: The Remap task writes bindings to the remap persist shard.
The Reclock task reads them back via a persist subscribe (snapshot + listen). This ensures
bindings are durable. The Reclock task holds a critical since handle on the remap shard to
prevent compaction until the corresponding data has been durably written to all export shards.

All inner tasks use `abort_on_drop` so they are cancelled when the pipeline is torn down
(e.g., DropDataflow → guard dropped → outer task aborted → inner handles dropped).

## Files

### `src/storage/src/render/postgres_direct.rs` (~1850 lines)
The entire async pipeline implementation.

### `src/storage/src/render.rs`
Integration point: checks `DIRECT_PG_SOURCE` dyncfg, worker 0 spawns direct source,
other workers log and return.

### `src/storage/src/storage_state.rs`
- `direct_source_guards: BTreeMap<GlobalId, DirectSourceGuard>` field
- DropDataflow handler removes the guard
- `report_frontier_progress()` calls `guard.update_source_uppers()` to copy frontiers
  from shared state into the Rc<RefCell<>> entries that the standard reporting loop reads,
  and drains pending status updates

### `src/storage-types/src/dyncfgs.rs`
`DIRECT_PG_SOURCE: Config<bool>` (default `true`), registered in `all_dyncfgs()`.

### Reused from existing postgres source (made `pub(crate)`)
- `source/postgres.rs`: `SourceOutputInfo`, `cast_row`, `verify_schema`, `ensure_replication_slot`, `fetch_slot_metadata`, `fetch_max_lsn`, `DefiniteError`
- `source/postgres/snapshot.rs`: `export_snapshot`, `use_snapshot`, `decode_copy_row`
- `source/postgres/replication.rs`: `unpack_tuple`

## Detailed Design

### 1. Multi-table data model

```rust
/// Maps table OID → output_index → per-output metadata.
type TableInfo = BTreeMap<u32, BTreeMap<usize, SourceOutputInfo>>;

/// Maps output_index → export_id (GlobalId) for routing decoded rows.
type OutputMap = BTreeMap<usize, GlobalId>;
```

Built from `description.source_exports` at spawn time. Both snapshot and replication route
rows by OID through `table_info`.

### 2. Message types

Six inter-task message types, each an enum:

- **`SourceEvent`** (Source → Decode): `Row`, `TableError`, `Relation`, `Truncate`, `Progress`, `SnapshotComplete`
- **`DecodedEvent`** (Decode → Reclock): `Data` (with `SourceData` after cast), `Progress`, `SnapshotComplete`
- **`MintRequest`** (Source/LSN Poller → Remap): `source_upper`, `binding_ts`
- **`PersistEvent`** (Reclock → Persist): `Data(SourceData, timestamp, diff)`, `Seal(frontier)`

### 3. Guard and shared state

```rust
pub struct DirectSourceGuard {
    shared_uppers: Arc<Mutex<BTreeMap<GlobalId, Antichain<Timestamp>>>>,
    shared_status_updates: Arc<Mutex<Vec<StatusUpdate>>>,
    source_uppers: BTreeMap<GlobalId, Rc<RefCell<Antichain<Timestamp>>>>,
    _task: AbortOnDropHandle<()>,
}
```

- **`shared_uppers`**: Persist tasks write frontier updates here after successful `compare_and_append`. The remap task also writes the remap collection's frontier.
- **`shared_status_updates`**: The outer retry loop writes `Status::Stalled` errors here when the pipeline fails. Drained by the timely thread in `report_frontier_progress()`.
- **`source_uppers`**: Rc handles to the standard source_uppers entries. `update_source_uppers()` copies from shared_uppers into these, which the existing frontier reporting loop then picks up and sends as `StorageResponse::FrontierUpper`.
- **`_task`**: `AbortOnDropHandle` — dropping the guard aborts the outer task and all inner tasks.

### 4. Pipeline orchestration (`run_pipeline`)

1. Open channels between all tasks
2. Connect to PostgreSQL (replication + 2 metadata connections)
3. Ensure replication slot exists
4. **Open persist writers and check shard uppers**: For each export, open a `WriteHandle` and call `fetch_recent_upper()`. If the shard upper > minimum, the export already has data — skip snapshotting that table's OID. This prevents duplicate data when the pipeline restarts before the controller has updated `source_resume_uppers`.
5. Determine `tables_to_snapshot` from the shard upper check (not from `source_resume_uppers`)
6. Create `DirectReclockWriter` for the remap shard; capture remap upper for reclock task
7. Spawn all tasks (Persist per export, Remap, Decode, Reclock, Source)
8. Wait for Source task to complete or error

### 5. Source task

**Snapshot phase** (if `tables_to_snapshot` is non-empty):
1. `export_snapshot()` → create temp replication slot, get `(snapshot_id, snapshot_lsn)`
2. `use_snapshot()` → SET TRANSACTION SNAPSHOT on a separate connection
3. For each table OID: `COPY ... TO STDOUT (FORMAT TEXT)`, decode rows via `decode_copy_row`, send `SourceEvent::Row` at `snapshot_lsn`
4. Mint binding at `NOW()` with `source_upper = snapshot_lsn + 1`. Using NOW() (not Timestamp::minimum) is critical — ensures the mint succeeds even when the remap shard already has a high upper from a previous pipeline run.
5. Send `Progress(snapshot_lsn + 1)` and `SnapshotComplete`

**Replication phase**:
1. Fetch slot metadata, kill stale connections if slot is in use
2. `START_REPLICATION SLOT ... LOGICAL resume_lsn`
3. Spawn LSN poller (polls `pg_current_wal_lsn()` every 1s, sends `MintRequest` with NOW())
4. Main select loop:
   - `XLogData(Begin)` → `process_replication_transaction()` which reads messages until `Commit`, sending `Row`/`TableError`/`Relation`/`Truncate` events. Skips rows for snapshotted tables where `commit_lsn <= snapshot_lsn` (rewind handling).
   - `PrimaryKeepAlive` → update `data_upper`, send `Progress`, reply if requested
   - `feedback_timer` (1s) → proactive standby status update to drive frontier advancement

### 6. Decode task

Receives `SourceEvent`, applies type casts and projections, emits `DecodedEvent`:
- `Row` → for each output of the row's OID: apply projection, `cast_row()`, emit `DecodedEvent::Data`
- `TableError` → emit error `SourceData` for all outputs of that OID
- `Relation` → verify schema via `publication_info()` + `verify_schema()`, recalculate column projections
- `Truncate` → emit `DefiniteError::TableTruncated` for affected outputs, remove them from `table_info`

### 7. Remap task

Receives `MintRequest`, writes bindings to the remap persist shard via `DirectReclockWriter::mint()`.
Does NOT send bindings via channel — the reclock task reads them from persist.

**`DirectReclockWriter`**: Custom remap shard writer that maintains the remap collection as a
differential collection of `(MzOffset, Timestamp, Diff)`. On initialization, reads back the
current source_upper from the remap shard (snapshot + consolidate). On mint, retracts the old
source_upper and inserts the new one at the binding timestamp, then `compare_and_append`.
Handles upper mismatches (another writer already advanced) by accepting the mismatch if
`current >= new_into_upper`.

### 8. Reclock task

Reads bindings from the remap persist shard and holds a critical since handle.

**Initialization:**
1. Open a leased reader on the remap shard
2. Open a critical since handle (`CriticalReaderId` is deterministic from source ID, survives restarts)
3. Subscribe from `remap_upper - 1` (not from since) to avoid replaying historical bindings
4. `snapshot_and_fetch` at `remap_upper - 1` for accumulated state, `listen` for new bindings
5. Bridge snapshot + listen through an mpsc channel (for Unpin/Send compatibility)

**Binding reconstruction from differential updates:**
The remap shard stores a differential collection. The reclock task maintains a running
accumulated state (`remap_accumulated`). As updates arrive, they're added to the accumulated
state and consolidated. For each binding timestamp, the source_upper is the max positive offset
in the accumulated state. This correctly handles the case where the source_upper doesn't change
between mints (retraction and insertion of the same offset cancel in per-timestamp view, but
the accumulated state preserves the value).

**Main loop (select! over three channels):**
- `decoded_rx`: Buffer data events; on Progress, try to apply bindings
- `remap_event_rx`: Accumulate remap updates; on Progress, create bindings and try to apply
- `persist_confirm_rx`: Track confirmed persist uppers; downgrade remap since handle

**Binding application:** When a binding's `source_upper <= source_progress`:
1. Emit all buffered data with `lsn < source_upper` at `binding_ts` → `PersistEvent::Data`
2. Send `PersistEvent::Seal(new_ts_upper)` only to **activated exports** (exports that have
   received at least one Data event)
3. Remove applied binding

**Activated exports:** Only exports that have received at least one Data event get Seal events.
This prevents empty Seals from advancing new exports' frontiers before their snapshot data is
written — critical for pipeline restart correctness. On initialization, exports that already
had data (shard upper > minimum) are pre-activated.

**Since downgrade:** When the minimum confirmed upper across all exports advances, the reclock
task downgrades the remap shard's since handle via `maybe_compare_and_downgrade_since`. This
allows compaction of remap bindings that are no longer needed.

### 9. Persist task (one per export)

Maintains a lazy `BatchBuilder`. On `Data`, adds to builder. On `Seal`:
1. Finish the batch with the new upper
2. `compare_and_append_batch` with current_upper → new_upper
3. On success: update `shared_uppers`, advance `current_upper`, send confirmation to reclock
4. On upper mismatch: accept the mismatch's current upper (another writer won)
5. Guards against empty upper (finalized shard) at startup and in the event loop

### 10. Rewind handling

Skip-based approach: during the rewind window (`commit_lsn <= snapshot_lsn`), skip WAL rows
for snapshotted tables entirely. The snapshot captured the full table state at `snapshot_lsn`,
so any WAL events at or before that LSN are already reflected. No retractions needed.

### 11. Error and status reporting

Pipeline errors (e.g., publication dropped, connection lost) surface as:
1. The outer retry loop catches the error from `run_pipeline()`
2. Writes `Status::Stalled` with the error message to `shared_status_updates` for all source objects
3. Sleeps 5s and retries
4. The timely thread drains `shared_status_updates` during `report_frontier_progress()` and writes them to `mz_internal.mz_source_statuses`

### 12. Pipeline restart correctness

When a new `CREATE TABLE ... FROM SOURCE` is issued, the coordinator sends DropDataflow + CreateSources,
re-rendering the entire ingestion. Key correctness invariants:

1. **No duplicate snapshots**: `tables_to_snapshot` is determined by checking each export's persist shard upper directly (`fetch_recent_upper()`), not from `source_resume_uppers` which may be stale.
2. **No leaked tasks**: All inner tasks use `abort_on_drop`. Dropping the guard aborts the outer task which drops all inner handles.
3. **Remap continuity**: Snapshot binding uses NOW() so it writes a real remap entry that advances the remap shard, even when restarting with a high existing upper.
4. **No premature frontier advancement**: The reclock task only sends Seal events to "activated" exports (those that have received data). New exports don't get Seals until their snapshot data has been emitted through a binding. This prevents the race where empty Seals advance a new export's frontier past minimum, causing the next pipeline restart to skip its snapshot (because `upper > minimum`).
5. **Remap subscribe from upper**: The reclock task subscribes from the remap shard's current upper (not since), avoiding replay of historical bindings that would trigger empty Seals for new exports.
6. **Critical since handle**: The reclock task holds a critical since on the remap shard, preventing compaction until all data associated with each binding has been durably written to export shards.

## Out of Scope (Future Work)

- Per-table health status (currently reports Stalled for all exports on any error)
- Statistics reporting to `mz_source_statistics`
- Multi-worker parallelism (currently single-worker on worker 0)
- Graceful handling of definite errors (currently retries everything)
- SSH tunnel support testing
