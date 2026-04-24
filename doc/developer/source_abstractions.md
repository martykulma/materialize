# Source implementation: abstractions and invariants

Scope: `mz_storage::source`, `mz_storage::render::sources`, `mz_storage::render::persist_sink`,
`mz_storage::upsert`, `mz_storage::healthcheck`, relevant types in `mz_storage_types::sources`.

Purpose: enumerate the load-bearing abstractions and invariants so implementation changes can be
evaluated against them. Not a tutorial.

## 1. Time domains

- **`FromTime` (source time domain)**: connector-native progress coordinate. Implements
  `SourceTimestamp: Timestamp + Columnation + Refines<()> + Display + Sync` plus `Codec64` and
  `encode_row`/`decode_row` for persistence.
  - Kafka: `Partitioned<RangeBound<PartitionId>, MzOffset>` (partially ordered).
  - Postgres: LSN via `MzOffset`.
  - MySQL: `GtidPartition` (GTID set, partially ordered).
  - SQL Server: per-capture-instance LSN (partitioned).
  - Load generator: `MzOffset`.
- **`IntoTime` (Materialize time domain)**: `mz_repr::Timestamp` (total order, epoch-ms). This is
  what persist shards and downstream dataflows see.
- **Timeline** (`mz_storage_types::sources::Timeline`): `EpochMilliseconds | External(name) |
  User(name)`. Cross-source joinability requires equal timeline.

Invariants:
- Every source's `FromTime` must refine `()` so it can live in a child scope rooted at `()`.
- `FromTime` must be encodable to a single-column `Row` (see `timestamp_desc`) — this is the
  schema of the remap/progress shard.

## 2. Core traits

### `SourceConnection` (`mz_storage_types::sources`)
Static metadata describing the external system:
- `name()`, `external_reference()`, `connection_id()`.
- `default_key_desc()`, `default_value_desc()`, `timestamp_desc()` (= remap shard schema).
- `supports_read_only()`, `prefers_single_replica()`.

### `SourceRender` (`mz_storage::source::types`)
The primary connector contract:
```
type Time: SourceTimestamp;
const STATUS_NAMESPACE: StatusNamespace;
fn render(scope, config, resume_uppers, start_signal)
    -> (exports: BTreeMap<GlobalId, StackedCollection<Time, Result<SourceMessage, DataflowError>>>,
        health: StreamVec<Time, HealthStatusMessage>,
        probes: StreamVec<Time, Probe<Time>>,
        tokens: Vec<PressOnDropButton>)
```

Render-time contract (enforced by reviewers, not the type system):
1. **Definiteness**: emitted data must be definite for all times beyond the resumption frontier.
   (definition: `doc/developer/design/20210831_correctness.md`)
2. **Health stream** must reflect transient failures (`Stalled{should_halt: true}` → restart).
3. **Probe stream** must periodically emit the current upstream frontier so the remap operator
   can mint bindings.
4. **Tokens**: dropping must immediately drop all capabilities and advance to the empty antichain.
5. **`resume_uppers`** is advisory — safe to ignore — but used to release upstream resources
   (commit offsets, advance replication slot).
6. **`start_signal`** blocks the source until upstream (envelope) rehydration completes.

## 3. Message types

- **`SourceMessage { key: Row, value: Row, metadata: Row }`** — connector-agnostic record.
- **`SourceOutput<FromTime>`** — adds `from_time`; emitted by reclock consumer side.
- **`DecodeResult<FromTime>`** — decoder output; `key`/`value` are `Option<Result<Row, DecodeError>>`.
- **`Probe<T> { probe_ts: mz_repr::Timestamp, upstream_frontier: Antichain<T> }`**.
- **`ProgressStatisticsUpdate`** — either `SteadyState { offset_known, offset_committed }` or
  `Snapshot { records_known, records_staged }`. Units are connector-defined u64.

## 4. Raw-source pipeline (`create_raw_source`)

Lives in `mz_storage::source::source_reader_pipeline`. Structure:

```
root_scope(())
 └── child_scope(FromTime)
       ├── connector.render() → per-export Collection<FromTime, Result<SourceMessage, DataflowError>>
       │                        + health + probe + tokens
       └── PusherCapture crosses scope boundary
scope(mz_repr::Timestamp)
 ├── remap_operator       (single writer, writes remap shard)
 ├── reclock per export   (FromTime stream + bindings → IntoTime stream)
 └── reclock_committed_upper (feedback: IntoTime committed_upper → FromTime resume_upper)
```

### `RawSourceCreationConfig`
The bundle passed to every raw source. Key fields:
- `id: GlobalId`, `source_exports: BTreeMap<GlobalId, SourceExport<CollectionMetadata>>`.
- `worker_id`, `worker_count`.
- `as_of: Antichain<mz_repr::Timestamp>` — downstream snapshot point.
- `resume_uppers` (IntoTime, per-export), `source_resume_uppers` (FromTime-encoded `Vec<Row>`, per-export).
- `timestamp_interval`, `now_fn`.
- `remap_metadata`, `remap_collection_id`, `shared_remap_upper: Rc<RefCell<Antichain>>`.
- `persist_clients`, `metrics`, `statistics`, `config`.
- `busy_signal: Arc<Semaphore>` — upstream-slowdown backpressure.

### Worker partitioning
`responsible_worker(p) = hash((id, p)) % worker_count`. Use `responsible_for` for deterministic
partition→worker assignment. Single-writer roles (remap, PG replication reader) use
`id.hashed() % worker_count` (i.e. partition = `()`).

## 5. Remap / reclock

### `ReclockOperator<FromTime, IntoTime, Handle>`
Maintains two frontiers: `upper: Antichain<IntoTime>` (written-to-shard upper) and
`source_upper: MutableAntichain<FromTime>` (accumulated from bindings).

**`mint(binding_ts, new_into_upper, new_from_upper)`** mints `(FromTime, IntoTime, Diff)` triples.

### `RemapHandle` (`mz_storage_client::util::remap_handle`)
Duplex interface over the remap persist shard: `next()` (tail) + `compare_and_append` (write).
Compat impl: `mz_storage::source::reclock::compat::PersistHandle`.

### Invariants
1. **Well-formedness**: the remap collection accumulated at any `IntoTime` must describe a
   well-formed `Antichain<FromTime>` — every `FromTime` element has accumulated frequency 1.
2. **Single writer**: only one worker (chosen by `id.hashed() % worker_count`) writes the remap
   shard; others clear `shared_remap_upper` and exit.
3. **First binding at minimum**: the very first minted binding uses `IntoTime::minimum()` so the
   shard is never empty at any timestamp (required for reads at arbitrary `as_of`).
4. **Probe-gated minting**: new bindings are minted only after a probe with strictly advancing
   `probe_ts`. `binding_ts = probe.probe_ts`, `new_into_upper = binding_ts.step_forward()`.
5. **Closure**: if `new_from_upper` is empty (source closed), `new_into_upper` also closes.
6. **Broadcast**: the remap collection is broadcast to all workers — every worker reclocks its
   own export.
7. **Read-only**: in read-only replicas the remap operator must not write. It degrades to pure
   consumer (see `read_only_rx` in `remap_operator`).
8. **`shared_remap_upper`** exists so `storage_state` can surface the remap write frontier for
   resumption / statistics without coupling through the shard.

## 6. Probes (`source::probe::Ticker`)

- Time-interval scheduler; `probe_ts` rounded down to nearest multiple of interval to reduce
  time-series churn.
- Interval re-read each tick → dynamic reconfig via `StorageConfiguration`.
- Missed ticks are skipped, never queued.
- Probes are broadcast from the source scope to the remap operator via `tokio::sync::watch`.

## 7. Source exports (subsources)

- A single ingestion can have multiple exports (MySQL/PG CDC tables, Kafka with
  upsert+unenveloped, etc.). Each has its own `GlobalId`, `CollectionMetadata` (persist shard),
  and `SourceExportDataConfig { encoding, envelope }`.
- Reclocking is per-export; errors (definite) are per-export.
- Resume uppers are tracked per-export.
- Primary export uses `default_key_desc` / `default_value_desc`; subsource exports carry their
  own `SourceExportDetails` describing the remote object + decoder.

## 8. Errors

Two classes:
- **Definite errors** (`DataflowError`): deterministic functions of the data at a specific
  `FromTime`. They flow into the per-export error `Collection` and persist forever. A retraction
  must be observed to resume the stream; surfaced as a non-halting `Stalled` health status.
- **Transient errors**: connection, auth, protocol. They are surfaced via the health stream
  with `should_halt: true`, triggering a dataflow restart. They must not appear as diffs in the
  data collection.

Invariants:
- Operators that can emit transient errors (`TableReader`, `ReplicationReader`, ...) use
  `AsyncOperatorBuilder::build_fallible` so `?` propagation cannot leave capabilities downgraded
  with bogus frontiers.
- Any error delivered into the data stream by `source_render_operator` is treated as definite
  and surfaces a stalled health message.

## 9. Health

- **`HealthStatusUpdate`**: `Running | Stalled { error, hint, should_halt } | Ceased { error }`.
  `Ord` is designed so the worst status wins on aggregation — used by `healthcheck.rs` to
  compute `OverallStatus`.
- **`HealthStatusMessage { id: Option<GlobalId>, namespace: StatusNamespace, update }`**.
- **`StatusNamespace`**: per-connector (`Kafka`, `Postgres`, `MySql`, `SqlServer`, `Generator`,
  `Ssh`, ...). Allows layered reporting (e.g. SSH tunnel state separately from Kafka).
- Halting statuses trigger storage cluster restart via `internal_control`.

## 10. Snapshot ↔ replication rewind (PG / MySQL / SQL Server)

Snapshot and replication must agree on the same `FromTime` so the resulting collection is
definite. Pattern:

1. Snapshot operator opens a transaction at some upstream snapshot point `t_snapshot` and reads
   the rows. For PG this requires a temporary replication slot (only way to obtain a consistent
   LSN-addressable point). For MySQL: table lock + current GTID. For SQL Server: CDC boundary LSN.
2. Snapshot emits a **rewind request** on a broadcast edge to the replication reader carrying
   `t_slot` (the real slot start) and `t_snapshot`.
3. Replication reader subtracts all updates in `(t_slot, t_snapshot]` from the snapshot
   (rewind equation in `postgres/snapshot.rs`):
   `sum(t ≤ t_slot) = sum(t ≤ t_snapshot) − sum(t_slot < t ≤ t_snapshot)`.
4. Snapshot resumes correctly: already-snapshotted exports are skipped on restart based on
   their per-export resume upper.

Invariants:
- Snapshot and rewind must use the same `FromTime` values — mis-aligned rewind corrupts data.
- Snapshot leader commits its transaction **last** (after all followers) to keep the shared
  consistent point open.
- Every export that is snapshotted emits exactly one rewind request.

## 11. `persist_sink` (storage side)

Three-stage pattern (`render/persist_sink.rs`):
```
mint_batch_descriptions (one worker) → write_batches (all workers) → append_batches (one worker)
```

Differs from compute's persist_sink:
- **No self-correction**. Sources are definite but not reliably re-producible. Re-reading the
  persist shard just to diff against the input stream would double load and is unnecessary.
- **Bounded memory at a single timestamp**: snapshots can represent huge data, so writes are
  chunked and streamed rather than buffered per-timestamp.

Invariants:
- Only one worker mints batch descriptions per (shard, ts-range); all workers may write batches
  for that description; one worker appends.
- Resume upper is read from the shard's `upper` on dataflow start.

## 12. Envelopes (in `render_source_stream`)

Source export data goes through decode → envelope:
- **None** (append-only): emitted rows + definite errors.
- **Upsert**: keyed; driven by `mz_storage::upsert` or `upsert_continual_feedback_v2`. Pluggable
  backend: memory / RocksDB / differential-dataflow collection.
  - `UpsertKey([u8; 32])` = SHA-256 of key row — collision-resistant id.
  - `UpsertValue = Result<Row, UpsertError>`.
  - **Rehydration**: on startup the upsert operator reloads state from persist; `start_signal`
    is fired when complete so the raw source may begin producing. Backpressure is via
    `busy_signal` (`SignaledFuture`).
- **CDC v2** (`render_decode_cdcv2`): source produces its own timestamped updates + progress.

Invariants:
- Upsert state is keyed by `UpsertKey`, not by the raw key row (collision probability handled
  by SHA-256 width).
- Upsert output is either a well-formed retraction/insertion pair or an `UpsertValue::Err`
  carrying a `DecodeError`/`UpsertError`/`EnvelopeError`.
- Rehydration must complete before any new data is applied — otherwise retractions against
  pre-existing keys would be missed.

## 13. Backpressure

- **`busy_signal: Arc<Semaphore>`** — zero permits = pause; held by the downstream sink while
  persist is saturated. Wrap any upstream async future with `SignaledFuture::new(sem, fut)`;
  the future polls only when a permit can be acquired.
- **`StorageMaxInflightBytesConfig`** — caps per-source in-flight bytes for the persist source
  reader.
- **Decoder backpressure**: decoded stream exchanges on `from_time` hash so parallel decoders
  can be throttled independently.

## 14. Resume state

Three layers of resumption, each with its own frontier:
1. **Persist shard upper** (IntoTime) — authoritative for what's durably committed.
2. **Remap shard contents** — authoritative for the FromTime ↔ IntoTime correspondence.
3. **Per-export resume upper** (IntoTime) — computed from shard uppers; reclocked back through
   the remap collection (`reclock_committed_upper`) into a FromTime resume upper that the
   connector uses to skip already-ingested data.

Invariants:
- On restart, a source must read from a `FromTime` ≤ the resume upper so definite output is
  preserved.
- The reported ingestion `resume_upper` is the element-wise meet of per-export resume uppers.
- Kafka/PG/MySQL/SS offset committers use the *source-domain* resume upper (from
  `reclock_committed_upper`) to advance upstream consumer groups / slots / cleanup.

## 15. Lifecycle

1. `mz_storage_controller` sends `RunIngestionCommand` → `storage_state::Worker`.
2. Worker calls `render::sources::render_source` → `create_raw_source` → connector's
   `SourceRender::render`.
3. Data flows: raw FromTime stream → reclock → IntoTime stream → decode → envelope →
   `persist_sink`.
4. Health flows: connector health + generic stats health → namespace-aggregated → storage
   controller's `StatusUpdate` stream → MZ_INTERNAL status tables.
5. Drop: dropping `PressOnDropButton` tokens closes the dataflow; controller sends
   `DropCollections`.

## 16. Constraints to preserve when modifying

- Do not introduce self-correction in `render::persist_sink`.
- Do not emit transient errors into the data collection.
- Do not mint remap bindings without a strictly-advancing probe timestamp.
- Do not write remap shard from more than one worker; do not write from read-only replicas.
- Do not break the rewind equation: `t_slot` and `t_snapshot` must be drawn from the same
  time domain the replication reader uses.
- Do not assume `FromTime` is totally ordered — Kafka and MySQL are partially ordered.
- Do not violate definite-data: any non-deterministic function of the data must be relocated
  behind the reclock boundary (at a specific IntoTime) or excluded.
- Do not bypass `busy_signal` / `start_signal` on connector rewrites — rehydration correctness
  depends on them.
- Do not change `timestamp_desc` for an existing source without a migration — it is the persist
  shard schema of the remap collection.

## 17. Pointers to canonical references

- Design: `doc/developer/design/20210714_reclocking.md`,
  `doc/developer/design/20220411_reclocking_implementation.md`,
  `doc/developer/design/20210831_correctness.md`.
- Architecture: `doc/developer/platform/architecture-storage.md`.
- Flow index: `doc/developer/generated/flows.md` §"Source ingestion".
- Module docs: `doc/developer/generated/storage/source/*`.
