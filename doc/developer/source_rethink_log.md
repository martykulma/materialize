# Source rethink log

Append-only working log of proposals, validations, and decisions for simplifying the source
implementation. Newest entries at the bottom. Do not rewrite history; supersede with a new
entry that references the prior one.

Companion to `doc/developer/source_abstractions.md` (current invariants).

Format: `## YYYY-MM-DD — <slug>` per entry. Sub-headings free-form.

---

## 2026-04-29 — initial alternatives sketch

Goals (from session):
- Primary: reduce complexity of adding a new source.
- Secondary: avoid sacrificing performance where possible.
- Secondary: simplify error reporting.
- Secondary: improve ability to identify and isolate failures.

### Identified complexity hot-spots

1. **Scope gymnastics.** Three nested scopes (`()` → `IntoTime` → `FromTime`) plus
   `PusherCapture` to cross the boundary. Every connector must reason about which scope its
   operators live in.
2. **Skeleton duplication.** Each connector re-implements: probe loop, partition assignment,
   snapshot↔replication rewind, definite/transient error split, health emission, offset
   committer feedback. ~80% copy between PG/MySQL/SS.
3. **Control-plane sprawl.** Seven channels feed one source: `busy_signal`, `start_signal`,
   `resume_uppers`, `source_resume_uppers`, `shared_remap_upper`, `committed_upper`, plus the
   probe stream and health stream as data edges.
4. **Coarse failure granularity.** Any transient error halts the whole dataflow. Per-export
   rewind/snapshot logic is hand-rolled. Subsource failures cross-contaminate.
5. **Duplicated machinery.** Two upsert implementations (v1 + continual-feedback v2). Storage
   and compute each have their own `persist_sink`.

### Direction A — Connector Kernel: shrink `SourceRender` to a non-Timely async trait

Replace `SourceRender::render(...) -> (collections, health, probes, tokens)` with a small
async trait. Framework owns all Timely.

```rust
trait SourceKernel {
    type Time: SourceTimestamp;
    type Cursor: Serialize + DeserializeOwned;     // resumable position

    async fn probe(&mut self) -> Result<Antichain<Self::Time>, Transient>;

    // Optional. Framework partitions; connector reads its slice.
    async fn snapshot(&mut self, partition: PartitionAssignment)
        -> Result<SnapshotStream<Self::Time>, Transient>;

    // Streaming reader; framework calls once per worker that owns partitions.
    async fn stream(&mut self, from: Antichain<Self::Time>, partitions: PartitionAssignment)
        -> Result<EventStream<Self::Time>, Transient>;

    async fn commit(&mut self, upper: Antichain<Self::Time>);   // optional
}
```

Framework provides:
- Probe loop + `Ticker`.
- Reclock + remap shard + broadcast.
- Snapshot↔replication rewind (a reusable `RewindCoordinator` that consumes a
  `SnapshotStream` + `EventStream` and emits the rewound collection).
- Worker assignment (today's `responsible_for` becomes a typed `PartitionAssignment`).
- Backpressure (`busy_signal`) and rehydration gating (`start_signal`) become invisible —
  connector just awaits on framework-provided handles whose `poll` already integrates these.
- Health: any returned `Transient` becomes a halting status; any per-record `Definite` flows
  to that export's error collection. Connector never touches `HealthStatusUpdate`.

Tradeoffs:
- Loses raw Timely flexibility. Kafka's per-partition queue distribution must be expressible
  via `PartitionAssignment`.
- Win: new source ≈ 300 LOC instead of ≈ 2000. PG/MySQL/SS rewind logic deduplicates.
- Cost: PG/MySQL/SS need careful surface design — schema-change handling, GTID partitioning,
  SQL Server LOD-deferral across batches, capture-instance lifecycle.

### Direction B — Collapse the scope hierarchy

Today: data is generated in a `FromTime` scope, captured across boundary, reclocked in
`IntoTime`. Reason: `Antichain<FromTime>` is partially ordered for Kafka/MySQL and we want
the connector to use Timely capabilities natively.

Alternative: a single scope (`mz_repr::Timestamp`). Connectors emit `(record, FromTime)` pairs
as data; reclock is an inline stateful operator (already mostly true via the `reclock(...)`
utility). `Probe<FromTime>` is just data. No `PusherCapture`, no scope nesting.

Tradeoffs:
- Loses the property that connector capabilities track upstream progress in a typed way.
  Framework would have to enforce that connector emissions are paired with frontier advances.
- Mostly a sideways move on its own. *Enabled* by (A) — once connectors don't write Timely
  directly, the multi-scope structure has no consumer.

### Direction D — Per-export fault isolation

Today: one transient error → entire ingestion restarts → all exports may re-snapshot.
Subsource failures cross-contaminate.

Alternative: **export groups** as the unit of restart.
- `SourceKernel` returns a `RestartUnit` ID per export at render time.
- Health aggregation tracks status per `RestartUnit`, not per source.
- Definite errors stay scoped to the failing export's persist shard.
- Transient errors propagate only within the failing `RestartUnit`.

Tradeoffs:
- More state in the supervisor.
- Some upstream resources genuinely can't be split (a single PG slot serving N tables).
  User-visible model becomes "the slot is the shared fate" — which is the truth.
- Cleaner if (A) lands first; the framework already owns restart.

### Direction E — Type the error model

Replace the implicit "definite via data stream / transient via health stream" split with one
typed result:

```rust
enum SourceEvent<T, R> {
    Record { time: T, value: Result<R, DefiniteError> },
    Frontier(Antichain<T>),
    Transient(anyhow::Error),
}
```

Framework demuxes. Connectors cannot accidentally surface a transient error as data
(`build_fallible` exists today specifically to prevent that — typed events make it
impossible by construction).

Tradeoffs:
- Mostly upside. Small. Independent of (A).
- Stepping stone to (A).

### Suggested order

1. **(E)** typed event enum — small, independent, immediate clarity win.
2. **(A)** kernel trait — biggest complexity reduction; absorbs (E).
3. **(D)** restart-unit supervision — buys fault isolation.
4. **(B)** collapse scopes — sweep up dead complexity once (A) means no connector cares.

### Open questions for next entry

- Does (A)'s `SourceKernel` cover Kafka's per-partition queue model without performance loss?
- Does `RewindCoordinator` cover SQL Server's per-capture-instance snapshot boundary and
  LOD-deferral pattern?
- Can MySQL's GTID-set partial-order semantics fit `Antichain<Self::Time>` without leaking
  GTID-set merge semantics into the trait?
- Where does schema validation (PG `replication.rs`, MySQL `schemas.rs`, SS restore-history)
  live in the kernel model? Probe-side hook? Per-event policy?
- How does (A) interact with read-only replicas (must not write remap shard)?

Next: validate (A) against current connectors — Kafka, Postgres, MySQL, SQL Server, load
generator. Goal is to either confirm the surface or surface concrete blockers.

---

## 2026-04-29 — validation of (A) against current connectors

Read the actual source of all five connectors to test the proposed `SourceKernel` surface.

### Per-connector observations

#### Kafka (`source/kafka.rs`, 1780 LOC)

Today: two operators.
- **Metadata fetcher** (single worker): runs a `Ticker`, fetches partition list + high
  watermarks, emits `(probe_ts, MetadataUpdate)` and a separate `Probe` stream. Detects topic
  recreation by frontier regression and emits `DefiniteError`. Also produces both `Kafka` and
  `Ssh` health namespaces.
- **Reader** (multi-worker, partition round-robin via `responsible_for_pid`): single rdkafka
  `BaseConsumer` per worker, `partition_queue` split per partition. Holds `data_cap_set` with
  per-partition `PartitionCapability`. Consumes `MetadataUpdate` to manage subscriptions.
  Concurrently runs `KafkaResumeUpperProcessor` to commit offsets back to Kafka.

Key constraints not in (A) draft:
1. **Per-partition queues are perf-critical.** rdkafka splits the consumer's incoming queue
   into partition queues to keep one slow partition from starving others. An `EventStream`
   trait object that re-multiplexes adds copies/allocs.
2. **Multiple exports share data.** Kafka exports differ by envelope/encoding, not by content
   — every export gets every record. Today: `outputs.iter().repeat_clone(error)`.
3. **Definite-poisoning from the prober.** Topic-recreation is detected on the metadata side,
   not the data side, and must terminate the source.
4. **Two health namespaces** (Kafka + Ssh) per worker, separately driven.

No snapshot phase. No commit on advancing IntoTime — commit happens via `resume_uppers`
arriving as `Antichain<KafkaTimestamp>`.

#### Postgres (`source/postgres/*`, ~2700 LOC)

Today: snapshot operator + replication operator + per-export schema verify.
- **Snapshot**: leader-broadcast pattern. Leader creates temporary slot in a transaction,
  exports `snapshot_id`, queries `pg_class.relpages` for block estimates, broadcasts
  `SnapshotInfo { snapshot_id, snapshot_lsn, table_block_counts, upstream_info }`. Followers
  join exported transaction, run ctid-range `COPY` queries. PG ≥ 14 ctid-range scan; older
  falls back to single-worker-per-table.
- **Replication**: single worker, broadcasts raw replication messages to all workers for
  parallel decode. Keepalive LSNs drive `Probe` stream and capability advancement. Slot
  advancement uses `resume_uppers`.
- **Rewind**: snapshot emits `RewindRequest { t_slot, t_snapshot }` per output via broadcast
  feedback edge; replication reader emits negated diffs at LSN 0 for `(t_slot, t_snapshot]`.
- **Schema verify**: `verify_schema` runs per-event in the binlog; mismatches → DefiniteError.

#### MySQL (`source/mysql/*`, ~2000 LOC)

Today: snapshot + replication + statistics.
- **Snapshot**: per-table single-worker assignment. No within-table parallelism. Worker locks
  tables, captures GTID frontier, runs `SELECT *`, emits rewind requests with GTID-set
  frontier.
- **Replication**: single worker reads binlog, distributes raw events for parallel decode.
- **Statistics** (separate operator): probes server for upstream offset, drives Probe stream
  + offset_known/committed.
- **Schema verify**: per-event, MySQL-specific (`schemas.rs`).
- `GtidPartition` is *partially ordered* and *partitioned by source-uuid*.

#### SQL Server (`source/sql_server/*`, ~1100 LOC)

Today: replication operator + progress operator.
- **Replication** (single worker, hardcoded `REPL_READER` partition): does snapshot AND
  streaming in one async function. Per-capture-instance snapshot LSN. Tracks **deferred LOD
  updates** across CDC batches (varchar(max) NULL-in-old-row pattern). Validates upstream
  `restore_history_id` at start (mismatch → DefiniteError). Handles inline schema-change
  events from CDC stream → per-export `IncompatibleSchemaChange` definite error.
- **Progress** (single worker): probes max LSN, drives `offset_known`, optionally cleans up
  already-ingested rows from upstream change tables (`CDC_CLEANUP_CHANGE_TABLE` flag).
- Rewind uses `(initial_lsn, snapshot_lsn)` pair per capture instance.

Notable: today there is **no within-source parallelism at all** for SQL Server — single
worker handles every export's data. Comment in code flags this as future work.

#### Load generator (`source/generator.rs`, 538 LOC)

Two flavors. `Simple` runs one `Generator` trait on a tick. `KeyValue` is bespoke and
distributed. Multiple exports may receive **different subsets** of events keyed by
`LoadGeneratorOutput` discriminator (e.g. `auctions` vs `bids`). No upstream probe — frontier
is computed locally from the generator's deterministic schedule.

### Consequences for the kernel surface

The four-method draft from the prior entry does not survive. Concrete revisions needed.

#### Revision 1 — `probe()` returns richer `UpstreamStatus`

```rust
enum UpstreamStatus<T> {
    Frontier(Antichain<T>),
    DefiniteError(DataflowError),     // Kafka topic recreation, etc.
}
```

`PartitionsChanged` is *not* needed at this level — partition state can be derived by the
connector internally between probe calls and surfaced via a partition-aware
`PartitionAssignment`. (Alternative: extend `UpstreamStatus` with a partition-delta variant.
Open.)

#### Revision 2 — `EventStream` items must carry a per-export discriminator

```rust
struct EventItem<T> {
    target: ExportTarget,                 // see below
    time: T,
    payload: Result<Row, DefiniteError>,
}

enum ExportTarget {
    AllExports,                            // Kafka: same record into all exports
    OneExport(GlobalId),                   // PG/MySQL/SS: per-table
    Subset(SmallVec<[GlobalId; 4]>),       // Generator: discriminator-based subset
}
```

`AllExports` avoids the per-export `clone()` cost in the Kafka common case (no
`outputs.iter().repeat_clone(...)` in connector code; framework owns the fan-out and can
keep it as a single broadcast in Timely terms).

#### Revision 3 — Snapshot needs a leader plan/broadcast phase

```rust
async fn plan_snapshot(&mut self)
    -> Result<Option<SnapshotPlan<Self>>, Transient>;        // single worker (leader)

struct SnapshotPlan<K: SourceKernel> {
    upper: Antichain<K::Time>,                // t_snapshot
    plan: K::Plan,                            // broadcast payload
    partitions: Vec<K::Partition>,            // per-worker assignment list
}

async fn snapshot(&mut self, plan: K::Plan, partition: K::Partition)
    -> Result<EventStream<Self::Time>, Transient>;            // multi-worker
```

Covers PG (`snapshot_id`+`snapshot_lsn`+`table_block_counts`+`upstream_info` is `Plan`;
ctid-range is `Partition`), MySQL (`initial_gtid_set` is `Plan`; table is `Partition`), SQL
Server (`initial_lsn` per capture instance is `Plan`; capture-instance is `Partition` —
today single-worker, but kernel API doesn't preclude multi-worker), Kafka (returns `None`),
load gen (returns `None`).

#### Revision 4 — Snapshot stream and replication stream may share resources

SQL Server and PG read from the same logical connection for snapshot then stream. Two
options:

- **Option A (simpler API, performance cost):** require connector to open separate sessions.
- **Option B (richer API):** allow `snapshot` to return an `EventStream` that, when
  exhausted, *transitions into* a stream readable via `stream(from = snapshot.upper)`. The
  framework promises to call `stream` immediately after exhausting the snapshot stream.

Recommend B — two SQL Server CDC connections is a real operational concern (max-connection
limits), and the API is identical from the connector's perspective (it just returns a
stream that internally switches modes).

#### Revision 5 — Health namespaces

The connector wants more than one namespace (Kafka has Kafka + Ssh; PG has Postgres + Ssh
for SSH-classified errors). `Transient` should carry a `StatusNamespace` so the framework
can route correctly:

```rust
struct Transient {
    namespace: StatusNamespace,
    error: anyhow::Error,
    hint: Option<String>,
}
```

#### Revision 6 — Rewind is framework-owned but needs reader replay-from-LSN

Framework's `RewindCoordinator` subtracts `(t_slot, t_snapshot]` by replay-and-negate. This
requires `stream(from = t_slot)` to actually work — i.e. the connector must accept a
historical resumption point that's been kept by upstream retention.

| Connector  | Replay supported? | Notes                                                          |
|------------|-------------------|----------------------------------------------------------------|
| Kafka      | N/A (no snapshot) |                                                                |
| Postgres   | Yes               | Slot retains until `confirmed_flush_lsn` advances.             |
| MySQL      | Yes               | Binlog retention (time/size based; matches today's assumption).|
| SQL Server | Yes               | CDC change-table retention.                                    |
| Generator  | N/A               |                                                                |

OK across the board.

### Final revised kernel surface

```rust
trait SourceKernel: Send + 'static {
    type Time: SourceTimestamp;
    type Plan: Serialize + DeserializeOwned + Clone + Send + 'static;
    type Partition: Serialize + DeserializeOwned + Clone + Send + 'static;

    const STATUS_NAMESPACE: StatusNamespace;

    /// Single-worker, periodic. Framework owns the Ticker.
    async fn probe(&mut self) -> Result<UpstreamStatus<Self::Time>, Transient>;

    /// Single-worker (snapshot leader). Returns `None` if no snapshot is needed
    /// (Kafka, generator, or all exports already past minimum).
    async fn plan_snapshot(&mut self)
        -> Result<Option<SnapshotPlan<Self>>, Transient>;

    /// Multi-worker. Each worker is invoked once per assigned partition.
    /// EventStream may transition into the streaming reader (see commentary).
    async fn snapshot(&mut self, plan: Self::Plan, partition: Self::Partition)
        -> Result<EventStream<Self::Time>, Transient>;

    /// Single-worker. Reads from `from` forward. May yield definite errors per export.
    async fn stream(&mut self, from: Antichain<Self::Time>)
        -> Result<EventStream<Self::Time>, Transient>;

    /// Single-worker, periodic. Best-effort upstream resource release.
    async fn commit(&mut self, upper: Antichain<Self::Time>) -> Result<(), Transient>;
}

enum UpstreamStatus<T> {
    Frontier(Antichain<T>),
    DefiniteError(DataflowError),
}

enum ExportTarget {
    AllExports,
    OneExport(GlobalId),
    Subset(SmallVec<[GlobalId; 4]>),
}

struct EventItem<T> {
    target: ExportTarget,
    time: T,
    payload: Result<Row, DefiniteError>,    // metadata/key carried inside Row pack
}

struct Transient { namespace: StatusNamespace, error: anyhow::Error, hint: Option<String> }
```

### Concrete blockers identified

1. **Kafka per-partition queue throughput.** Wrapping rdkafka's per-queue poll in
   `EventStream` risks 1 alloc/message overhead on a hot path. **Mitigation:** specify
   `EventStream` as `impl futures::Stream<Item = EventItem<T>> + Send` so the connector can
   yield from the queue without intermediate buffering. Validate with a microbenchmark
   before committing the trait.
2. **SQL Server single-worker constraint.** Kernel API supports per-partition multi-worker
   replication, but today's implementation is single-worker because the SS CDC client is
   not yet sharded. Acceptable: kernel allows future parallelism, current SS impl just
   returns one partition.
3. **Probe-side definite errors.** Framework must be able to terminate the source with a
   definite error visible in *all* exports' error collections. Today Kafka does this by
   stalling forever after writing the error. Framework needs an explicit
   `terminate_with_definite(err)` primitive. Doable.
4. **Per-event schema validation.** PG/MySQL/SS validate schema mid-stream; on mismatch
   they emit a per-export DefiniteError. Fits `EventItem.payload = Err(DefiniteError)` with
   `target = OneExport(id)`. No API change needed.
5. **`SnapshotPlan` size.** PG broadcasts `BTreeMap<u32, PostgresTableDesc>` plus block
   counts. This is small (~tens of KB worst case) but must be `Serialize`. Keep `Plan`
   bounded; document that it must fit in a single broadcast event.

### What this saves vs. today

Per-connector, code that disappears into the framework:
- Health stream construction + namespace routing.
- `repeat_clone` across exports for fan-out.
- `output_index` partitioning at end-of-render.
- `responsible_for` dispatch glue.
- Transient/definite error error type definition + `build_fallible` plumbing.
- Probe stream wiring + `Ticker` setup.
- Snapshot leader broadcast/exchange bookkeeping (`SnapshotInfo` distribution).
- Rewind feedback edge plumbing (`RewindRequest` types, broadcast wiring).
- Resume-upper decoding from `Vec<Row>` → `Antichain<FromTime>`.
- Health-`Running` initial emission.
- `PusherCapture` / scope-crossing.

Estimated LOC reduction (rough): Kafka 1780 → ~700, PG 2700 → ~900, MySQL 2000 → ~700,
SQL Server 1100 → ~600, generator 538 → ~200. Total ≈ 8000 → ≈ 3100 (~60% reduction in
connector-side code), with the framework gaining ~1000 LOC of new infrastructure.

### Verdict

(A) is implementable across all five connectors with the revisions above. No connector
imposes a constraint that breaks the API. The remaining risks are performance (Kafka
hot-path) and surface-area discipline (`Plan` size, `EventStream` non-buffering) — both
addressable by careful trait design and microbenchmark validation, not architectural
changes.

Recommended next step: prototype the framework + migrate the **load generator** first
(smallest, no upstream complexity). Then **Postgres** (most representative of CDC pattern).
Kafka last (perf-sensitive; needs the microbenchmark).

### Open questions deferred to next entry

- Should `SnapshotPlan` be one-shot (sent once at startup) or replayable across restarts?
  Today PG re-creates the temp slot on every restart; making `Plan` replayable would need a
  durable snapshot-coordination shard.
- How does (D) per-export fault isolation interact with the kernel? `RestartUnit` becomes
  a method on `SourceKernel` returning per-export grouping?
- Does the kernel run inside a Timely operator at all, or as a pure tokio task whose
  outputs the framework feeds into Timely via channels? Tokio-only would eliminate the
  scope hierarchy entirely (achieves goal of (B)) but couples the supervisor to tokio
  concurrency primitives.

---

## 2026-04-29 — split: `MultiplexedSource` vs `DemuxedSource`

Proposal: replace the unified `SourceKernel` trait with two traits matching the two natural
shapes the connectors already exhibit. The unified trait was accumulating optional methods
(snapshot, plan, fan-out target) that exist only because of disagreements between the
shapes. Split removes the disagreements.

### The two shapes

| Dimension             | Multiplexed (PG, MySQL, SS today)         | Demuxed (Kafka, SS later)            |
|-----------------------|-------------------------------------------|--------------------------------------|
| Upstream cursor       | One global (LSN / GTID)                   | N independent (per-partition offset) |
| Snapshot              | Yes — leader plan + broadcast + rewind    | None                                 |
| Event → export        | One-to-one (record belongs to one table)  | One-to-many (record into all exports)|
| Worker model          | Snapshot: leader+followers. Stream: single| Per-partition multi-worker           |
| Mid-stream schema     | Per-event validation                      | N/A                                  |
| Definite poisoning    | From data path                            | From probe path (e.g. topic recreation)|
| Restart unit          | Whole source (slot is shared)             | Per-partition (independent)          |
| Time order            | Total or near-total                       | Partition-order (partial)            |

### Trait surfaces

```rust
/// Connector with a single global cursor and snapshot phase.
/// Suits CDC over a logical-replication slot / binlog / CDC stream.
trait MultiplexedSource: Send + 'static {
    type Time: SourceTimestamp;
    type Plan: Plan;                      // leader-broadcast payload
    type TablePartition: Partition;       // per-export-group assignment for snapshot

    const STATUS_NAMESPACE: StatusNamespace;

    /// Single-worker, periodic. Framework owns the Ticker.
    async fn probe(&mut self) -> Result<Antichain<Self::Time>, Transient>;

    /// Single-worker (snapshot leader). `None` if all exports are past minimum.
    async fn plan_snapshot(&mut self, exports: &[ExportRequest])
        -> Result<Option<SnapshotPlan<Self>>, Transient>;

    /// Multi-worker (followers). One call per assigned partition.
    async fn snapshot_table(&mut self, plan: Self::Plan, partition: Self::TablePartition)
        -> Result<TableEventStream<Self::Time>, Transient>;

    /// Single-worker. Framework opens one stream per source.
    /// Items target one export each. May yield definite errors per-export.
    async fn stream(&mut self, from: Antichain<Self::Time>)
        -> Result<MultiplexedStream<Self::Time>, Transient>;

    /// Single-worker, periodic.
    async fn commit(&mut self, upper: Antichain<Self::Time>) -> Result<(), Transient>;
}

struct TableEvent<T> {
    export_id: GlobalId,                  // exactly one
    time: T,
    payload: Result<Row, DefiniteError>,
}

/// Connector with N independent partitions, each with its own cursor.
/// Suits log-style sources where partitions are the unit of parallelism and failure.
trait DemuxedSource: Send + 'static {
    type Time: SourceTimestamp;           // typically Partitioned<P, MzOffset>
    type Partition: Partition;

    const STATUS_NAMESPACE: StatusNamespace;

    /// Single-worker. Periodic. Returns the current partition list, or DefiniteError to
    /// poison the source (e.g. topic recreation detected).
    async fn discover_partitions(&mut self)
        -> Result<DiscoveryResult<Self::Partition, Self::Time>, Transient>;

    /// Per-partition. Framework opens one task per assigned partition per worker.
    /// Items go to all exports; framework owns fan-out / envelope.
    async fn stream_partition(
        &mut self,
        partition: Self::Partition,
        from: Self::Time,
    ) -> Result<PartitionStream<Self::Time>, Transient>;

    /// Per-partition, periodic.
    async fn commit_partition(&mut self, partition: &Self::Partition, upper: Self::Time)
        -> Result<(), Transient>;
}

enum DiscoveryResult<P, T> {
    Partitions { partitions: Vec<(P, T /* high watermark */)> },
    DefiniteError(DataflowError),
}

struct PartitionEvent<T> {
    time: T,
    payload: Result<Row, DefiniteError>,  // Row already contains key/value/metadata
}
```

`MultiplexedStream` and `TableEventStream` are `impl Stream<Item = TableEvent<T>>`;
`PartitionStream` is `impl Stream<Item = PartitionEvent<T>>`. All non-boxed to preserve the
no-alloc-per-message hot path (Revision 1 from prior entry).

### Shared infrastructure (one layer below both traits)

Both frameworks build on the same primitives, which is where the actual reuse lives:

- **Reclock + remap shard.** Unchanged from today. Independent of source shape.
- **`persist_sink`.** Already shape-agnostic.
- **Decode + envelope.** Already shape-agnostic.
- **Health aggregation.** `Transient` is the same type for both traits.
- **Definite-error fan-out.** Multiplexed uses `TableEvent.export_id`; demuxed broadcasts
  to all exports of the partition's source. Both delegate to the same inner primitive.
- **Backpressure (`busy_signal`) and rehydration (`start_signal`).** Same primitives.

In other words: the *framework* code shared between the two paths is reclock + persist +
decode + envelope + health (most of today's `source_reader_pipeline.rs` and
`render/sources.rs`). The *trait-specific* code is the supervisor — leader/follower
choreography for multiplexed, partition-assignment loop for demuxed.

### Mapping today's connectors

| Connector       | Trait              | Notes                                                                |
|-----------------|--------------------|----------------------------------------------------------------------|
| Postgres        | `MultiplexedSource`| Plan = `SnapshotInfo`; TablePartition = `(oid, ctid_range)`.         |
| MySQL           | `MultiplexedSource`| Plan = `initial_gtid_set`; TablePartition = table name.              |
| SQL Server      | `MultiplexedSource`| Plan = per-capture-instance `initial_lsn`; TablePartition = capture-instance. Single worker today; the multiplexed framework supports this trivially. |
| SQL Server v2   | `DemuxedSource` *(future)* | Once the CDC client is shardable, capture-instances become `Partition`. Migration is a re-impl, not an API change. |
| Kafka           | `DemuxedSource`    | Partition = `PartitionId`; `discover_partitions` replaces the metadata fetcher. |
| Generator/Simple| `MultiplexedSource`| Plan = empty; no snapshot phase (`plan_snapshot` returns `None`).    |
| Generator/KV    | `DemuxedSource`    | Already partition-distributed.                                       |

### What the split removes that the unified trait carried

- `ExportTarget` enum (`AllExports` / `OneExport` / `Subset`). Each trait has exactly one
  fan-out semantics — no enum, no per-event branching.
- `Option<SnapshotPlan>` from the multiplexed path's signature — `plan_snapshot` returns
  `Option<...>` only for the "everyone past minimum" case, not because the source has no
  concept of snapshot.
- The whole snapshot/rewind apparatus from the demuxed path. A Kafka connector author
  reads `DemuxedSource` and never sees `Plan`, `TablePartition`, `snapshot_table`, or
  rewind. ~3 methods, all aligned with the connector's natural shape.
- `UpstreamStatus<T>::DefiniteError` from the multiplexed path: definite-error poisoning
  in CDC sources comes from the data path (mid-event), not the probe path. The
  multiplexed `probe()` returns just the frontier. Demuxed keeps it because Kafka detects
  topic recreation in metadata fetches.

### What the split adds

- Two supervisor implementations instead of one. Mostly mechanical (~500 LOC each on top
  of the shared infrastructure layer).
- A small risk of duplication if both supervisors evolve independently. Mitigation: keep
  the shared layer aggressive — anything that isn't *uniquely* about leader broadcasting
  vs partition discovery belongs below the trait.

### Restart-unit isolation (Direction D) becomes trait-implicit

- `MultiplexedSource`: restart unit is the source. The slot/binlog/CDC stream is shared
  state; you can't restart half of it.
- `DemuxedSource`: restart unit is the partition (or partition group sharing a
  connection). A failed Kafka partition reconnect doesn't disturb the others.

No separate `RestartUnit` API. The trait choice *is* the answer.

### What this clarifies in the implementer's experience

A new connector author sees one of two short, focused traits:
- "I have a CDC stream with snapshot semantics" → `MultiplexedSource`. ~5 methods, all
  about the cursor and the snapshot.
- "I have a partitioned log" → `DemuxedSource`. ~3 methods, all about partitions.

Today the author has to read Kafka *and* Postgres, internalize that they're solving
different problems with the same operator-graph framework, and then figure out which 30%
of the existing patterns apply to their case. The split makes this an obvious fork.

### Verdict

Yes. Split. The two-trait surface is meaningfully simpler than the unified kernel for both
implementers and maintainers, and the case where SQL Server might migrate from one to the
other is supported (trait swap, not API change).

### Updated plan order

1. **(E)** typed event enum — still independent, still first.
2. **Extract shared infrastructure layer** — reclock+persist+decode+envelope+health glue
   that both supervisors will use. This is mostly today's
   `source_reader_pipeline::create_raw_source` minus the connector-specific portions, plus
   the per-export error fan-out primitive.
3. **`MultiplexedSource` trait + supervisor.** Migrate load generator/Simple → Postgres →
   MySQL → SQL Server.
4. **`DemuxedSource` trait + supervisor.** Migrate generator/KV → Kafka.
5. **(B)** collapse scope hierarchy — once both supervisors own Timely, neither connector
   trait deals with scopes.

### Open questions deferred to next entry

- Is `MultiplexedSource::stream` returning *exactly one* stream the right shape, or should
  it return a stream-of-streams keyed by `export_id` to permit future per-export
  parallelism in the multiplexed case? (Probably not — one stream is the truth of the
  upstream cursor; per-export parallelism is a decode-stage concern.)
- For demuxed, should `discover_partitions` return *deltas* (added/removed) or absolute
  state? Absolute is simpler; framework computes the delta. Kafka's metadata fetcher
  returns absolute today.
- Is the shared infrastructure layer best expressed as a Rust crate (`mz-source-framework`)
  separate from `mz-storage`, so connectors can depend on it without pulling in the rest
  of storage? Probably yes once stable, not yet.

---

## 2026-04-29 — supervisor model: Timely vs tokio

**Note**: a prototype exists that has eliminated the `FromTime` scope (Direction B), and is
*partially* working. Specific gaps not yet enumerated in this log; recommendations below
should be reconciled with the prototype's actual shape before being treated as decisions.

### The question

Where do connector trait methods (`stream`, `snapshot_table`, `probe`, ...) execute, and
how do their outputs reach the rest of the storage dataflow?

Three viable answers:

1. **Timely-operator supervisor.** Trait methods are called from inside an
   `AsyncOperatorBuilder` operator. The supervisor *is* a Timely operator and holds
   capabilities. Closest to today.
2. **Pure tokio supervisor + Timely sink.** The supervisor is a tokio task. It runs the
   trait methods and writes results into a channel. A separate Timely operator reads the
   channel and downgrades capabilities. Connector never sees Timely.
3. **Hybrid: tokio connector task + thin Timely shim.** The trait runs as a tokio task
   that yields streams. A small Timely operator consumes those streams and is the only
   place capabilities live. Cross-worker coordination (broadcast, exchange) stays in
   Timely.

### Evaluation against goals

| Goal                                     | (1) Timely op | (2) Pure tokio | (3) Hybrid |
|------------------------------------------|:-------------:|:--------------:|:----------:|
| Connector author writes only `async fn`  | ✗ (capabilities visible) | ✓ | ✓ |
| Achieves (B) — no `FromTime` scope       | ⚠ (possible but awkward) | ✓ | ✓ |
| Cross-worker coordination is ergonomic   | ✓ (Timely native) | ✗ (re-implement broadcast in tokio) | ✓ |
| Backpressure is composable               | ✓ | ⚠ (needs explicit primitives) | ✓ |
| Connector unit-testable without Timely   | ✗ | ✓ | ✓ |
| Cancellation/restart hygiene             | ⚠ (capability/cancel races) | ✓ | ✓ |
| Refactor size                            | small | large | medium |
| Failure isolation (D)                    | ⚠ (operator restart granularity) | ✓ | ✓ |

### Why (1) Timely-operator supervisor falls short

Today's `AsyncOperatorBuilder` already exhibits the failure modes:

- The Kafka metadata fetcher uses `std::future::pending::<()>().await` after emitting a
  transient error, "to wedge forever" so it doesn't accidentally downgrade to the empty
  frontier. That's a workaround for the trait's leakage of capability semantics into the
  connector.
- PG/MySQL/SS use `build_fallible` so `?` doesn't accidentally drop capabilities. The
  framework would still need this if the connector trait ran inside a Timely operator and
  could `?`-bail.
- Every connector reasons about whether to clear `data_cap_set` on shutdown vs on error.
  Get it wrong and frontier reports nonsense.

These are the same hazards that the trait redesign is trying to remove. Putting the trait
calls inside a Timely operator preserves them.

### Why (2) Pure tokio supervisor is too far

The multiplexed snapshot plan must be broadcast from one worker to all workers. Today this
uses Timely broadcast. Doing it in tokio requires either:

- A new in-cluster gossip primitive between worker tasks (mz-cluster doesn't have a
  cross-worker async channel that respects backpressure and lifetime).
- Routing the plan through `internal_control` (heavyweight; designed for storage commands).
- Routing through a persist shard (durable, but adds a write-then-read latency at every
  source startup, which is on the snapshot critical path).

Reimplementing what Timely already provides is the wrong trade. The connector author
benefit (no Timely visible) is captured by (3) without paying this cost.

### Why (3) Hybrid is the right shape

```text
                      ┌─────────────────────┐
                      │  Connector tokio    │
                      │  task (per worker)  │
   trait methods ◀─── │                     │
                      │  yields:            │
                      │  - PartitionStream  │
                      │  - probe results    │
                      │  - errors           │
                      └─────────┬───────────┘
                                │ bounded channel
                                ▼
                      ┌─────────────────────┐
                      │  Timely shim        │   ← only this layer touches
                      │  (per worker)       │     capabilities, scopes,
                      │                     │     broadcast/exchange
                      │  - downgrades caps  │
                      │  - emits FromTime,  │
                      │    payload pairs    │
                      └─────────┬───────────┘
                                │
                                ▼
                       reclock (inline)
                                │
                                ▼
                       decode → envelope → persist_sink
```

Properties:

- **Connector trait is pure async.** No `Capability`, no `Scope`, no `AsyncOperatorBuilder`
  in trait method signatures. Implementations look like ordinary tokio code.
- **Capabilities live in exactly one place.** The shim. It owns the downgrade rules tied
  to the trait's reported frontier. No `pending::<()>` workarounds.
- **Cross-worker coordination keeps Timely's primitives.** Snapshot leader broadcast for
  multiplexed = a broadcast operator that reads from the leader's connector task. Demuxed
  partition assignment = a discovery operator that exchanges partition lists.
- **Backpressure is a bounded channel.** `busy_signal` becomes "the channel is full." No
  `SignaledFuture` semaphore-poll trick.
- **(B) is achieved by construction.** Connector emits `(FromTime, payload)` as data
  records inside the IntoTime scope; reclock is the same inline operator we already have.
- **Restart isolation (D) is natural.** The connector tokio task is the unit of restart.
  Demuxed sources spawn one task per partition (or per partition group); a failed task
  restarts independently. Multiplexed sources are one task = one restart unit, which is
  the truth.
- **Unit-testable.** A connector author writes a `MultiplexedSource` impl and tests it
  against a mock harness in pure tokio. No Timely runtime in tests.

### What the shim must do

This is the piece that subsumes most of today's `source_reader_pipeline.rs` plus the
per-connector wrappers:

1. Drive the connector task lifecycle: spawn, watch for panic/exit, restart per the
   restart-unit policy.
2. Receive items from the bounded channel; downgrade `data_cap` to the reported frontier.
3. For multiplexed: handle the snapshot phase — leader runs `plan_snapshot`, broadcasts
   the `Plan`, all workers run `snapshot_table` on their assigned partitions; rewind
   coordinator subtracts `(t_slot, t_snapshot]` by negating the stream's diffs in that
   range; transition into `stream` afterwards.
4. For demuxed: run a discovery operator (single-worker), exchange partition lists,
   spawn per-partition connector tasks per worker, manage their lifecycles.
5. Integrate `commit` callback with `resume_uppers` arrival.
6. Route `Transient` errors → halting health; route per-event `DefiniteError` → per-export
   error collection.

### Open hazards

- **Channel sizing.** Bounded channels backpressure cleanly but a too-small channel
  starves the consumer and a too-large one defeats memory bounds. Recommend a single
  knob: `STORAGE_SOURCE_CHANNEL_CAPACITY` dyncfg, defaulted by source kind.
- **Cancellation semantics.** Dropping the connector task must release upstream resources
  (close PG slot session, drop rdkafka consumer). Connector trait should make this
  explicit via `Drop` semantics on whatever the connector returns; the shim guarantees
  `drop` order.
- **Frontier reporting frequency.** A pure-data-channel approach loses Timely's natural
  per-batch frontier. Connector must explicitly send `Frontier(antichain)` items between
  data items; the shim downgrades on each. Cost: one extra channel item per probe tick.
  Acceptable.
- **Multi-stream connectors (snapshot transition into stream).** The multiplexed kernel
  spec already requires `snapshot_table`'s stream to potentially transition into the
  stream reader. In tokio this is just two awaits in sequence; in (1) it would have been
  awkward across operator boundaries.

### Recommendation

**Adopt (3) Hybrid.** Specifically:

- Connector traits (`MultiplexedSource`, `DemuxedSource`) are pure async traits. No
  Timely types in their signatures.
- Each worker spawns a tokio task per active connector instance; multiplexed sources have
  one task, demuxed sources have one task per assigned partition.
- A per-worker Timely shim operator consumes the bounded channel, owns capabilities,
  emits `(FromTime, payload)` data records.
- Cross-worker coordination (snapshot leader broadcast, demuxed partition discovery) uses
  small Timely operators that exchange/broadcast over the connector tasks' channels.
- The `FromTime` scope is gone; reclock is inline in the IntoTime scope. (B) is achieved.

### Reconciling with the prototype

Things to learn from the existing prototype before locking this in:

- **What's the gap in "partially working"?** If it's coordination (snapshot plan
  broadcast, capability downgrade order, exchange of partition lists), the hybrid model's
  Timely-shim layer is the natural place to fix it.
- **Where does the prototype put capability ownership?** If the connector still holds a
  `Capability`, that's a sign of leakage; we want to move it into the shim.
- **How is backpressure expressed?** If still using `SignaledFuture`/`busy_signal`,
  consider whether a bounded channel can subsume both.
- **Is there a probe/frontier channel separate from the data channel?** The hybrid spec
  says one channel with `Frontier` items interleaved. If the prototype has separate
  channels, that's also valid; pick whichever is simpler in the shim.

### Now closeable

- "Does the kernel run inside a Timely operator at all, or as a pure tokio task...?" —
  answered: pure tokio task; thin Timely shim.
- (B) "collapse scopes" — subsumed; achieved by construction in the hybrid model.

### Still open

- `SnapshotPlan` replayability across restarts (multiplexed). With the hybrid model the
  shim is restartable — the connector task gets a fresh `plan_snapshot` call. Today's
  PG behavior (recreate temp slot every restart) keeps working. The question is whether
  *durable* plans (saved to a coordination shard) buy enough to justify their cost. Lean
  no for v1.
- Multiplexed `stream` shape: one stream vs stream-of-streams.
- Demuxed `discover_partitions`: deltas vs absolute. Lean absolute (let the shim diff).
- Whether to spin out `mz-source-framework` as a separate crate. Defer until v1 lands.

---

## 2026-04-29 — prototype review: maz-sk-single-time-domain

Branch `maz-sk-single-time-domain` (9 commits beyond main) implements the hybrid
model for Postgres. Reviewed against the recommendation in the prior entry.

### Architecture matches the recommendation

- `SourceTask` trait in `types.rs` is pure async: `spawn(config, resume_rx) ->
  (SourceTaskOutputs, AbortOnDropHandle)`.
- `SourceTaskOutputs { data_rx, probe_rx, health_rx }` — three tokio channels (mpsc, watch,
  mpsc). `SourceTaskInputs { resume_rx }` — one watch.
- `channel_reclock.rs` is the per-worker Timely shim: consumes `data_rx`, owns
  capabilities, performs reclock inline against the remap collection's broadcast stream.
  Single time domain — no `FromTime` scope.
- `create_raw_source_from_task` orchestrates remap + reclock + per-export partition.
- `render_task_source` (in `render/sources.rs`) wraps the above and bridges `health_rx`
  via a `TaskHealthBridge` operator.
- The legacy `SourceRender` path is preserved alongside; PG is the only migrated
  connector. Other connectors keep using `create_raw_source` via `source_render_operator`.

This is the recommended shape. Below are the bugs against it.

### Bugs found that match "error propagation" CI failures

**EP-1. Definite errors miss-routed as transient.**

`pg_source_task_inner` wraps everything in `Result<(), TransientError>`, including:
- `TransientError::BareTransactionEvent`
- `TransientError::NestedTransaction`
- `TransientError::UnknownReplicationMessage`
- `TransientError::ReplicationEOF`

These represent protocol-level violations at a *specific* LSN — they are deterministic
functions of the upstream stream and are definite. Today's PG (`replication.rs` in main)
emits them as `DefiniteError` into the per-export error collection. Wrapping them as
transient triggers a dataflow restart loop on bad replication traffic instead of poisoning
the affected exports.

Fix: classify each error site. Decoding/protocol errors at known LSN → definite (emit as
`Err(DataflowError)` in the data channel at that LSN). Connection/auth/network → transient
(propagate via health channel).

**EP-2. Task error → halting health, but no retraction of in-flight data.**

When `pg_source_task_inner` returns `Err`, the wrapper sends `HealthStatusUpdate::halting`
and exits. `data_tx` drops, `channel_reclock` sees `recv() → None`, advances frontier to
empty, persist commits whatever data the task already pushed.

This is *correct* for committed transactions (definite data preserved) but the path has
two gaps:
- `data_upper = commit_lsn + 1` is set in `Begin(...)` *before* `process_transaction`
  runs. If `process_transaction` errors before a `flush_replication_batch` call, the
  in-flight `batch_updates` is dropped (good), but `data_upper` was never observed by the
  shim because no flush carried it. So no data is lost — also good.
- However, `flush_replication_batch` always runs after `process_transaction` returns Ok,
  so a successful transaction at LSN L *will* publish frontier `L+1`. If the *next*
  transaction errors and the task dies, the frontier is correctly at `L+1`, persist
  commits up to L. OK.

So EP-2 is not an active bug, but the invariant is fragile. Recommend: never advance
`data_upper` before the data for that LSN is in `batch_updates` *and* about to be flushed.
Currently `data_upper` advances on `Begin(...)` which runs first. Move the advance to
after `process_transaction` returns successfully.

**EP-3. Receiver-dropped silently swallowed.**

`flush_replication_batch` and `send_batch` both swallow `data_tx.send(...).is_err()`:

```rust
if data_tx.send(batch).is_err() {
    return Ok(());  // receiver dropped, shut down gracefully
}
```

This is fine for shutdown but conflates *intentional* (dataflow shutdown) with
*pathological* (channel_reclock panicked, channel closed unexpectedly) cases. The task
keeps running, processing replication messages, allocating, and discarding them. Should
exit promptly when the receiver is gone.

Fix: return a typed `ChannelClosed` and exit the task loop.

**EP-4. `compute_resume_lsn → None` hangs the task forever.**

```rust
let Some(resume_lsn) = resume_lsn else {
    std::future::pending::<()>().await;
    return Ok(());
};
```

If all exports legitimately advance past everything, `compute_resume_lsn` may return
`None` (no remaining exports? all empty antichains?). The task wedges instead of cleanly
shutting down. The shim's `data_tx` drop never happens (probe_tx, health_tx also held),
the dataflow can't terminate.

Fix: when resume_lsn is `None`, drop all senders and exit cleanly.

**EP-5. Health is not aggregated through the same operator as legacy sources.**

`render_task_source` builds a `TaskHealthBridge` operator that converts `health_rx`
messages into a Timely stream. Good. But this stream is added to `health_streams`
*before* the per-export decode/envelope health is appended — same as legacy. The
aggregation logic in `healthcheck.rs` should handle both. Verify with a test that has
both PG (task path) and Kafka (legacy path) running: they must compose, not collide.

This isn't strictly a bug but a likely CI failure mode if the test framework expects
specific health namespaces in specific orders.

### Bugs found that match "slot management" CI failures

**SM-1. `ensure_replication_slot` may create a fresh slot on cold start, losing data.**

`pg_source_task_inner` calls `ensure_replication_slot(&replication_client, slot)` before
fetching slot metadata. If the slot doesn't exist (because someone deleted it, or because
of a test scenario that drops it between runs), the function creates a *new* slot at the
current WAL position. Any events in the WAL from before slot creation are unrecoverable.
Today's `replication.rs` in main does *not* call `ensure_replication_slot` — the slot is
expected to exist (created during purification) and a missing slot is a definite error
("replication slot mysteriously missing"). The prototype silently papering over this
masks operator errors and *introduces data loss*.

Fix: do not call `ensure_replication_slot` from the task. If the slot is missing, return
`TransientError::MissingReplicationSlot` (today's behavior).

**SM-2. Snapshot temp slot creation order vs. main slot's `confirmed_flush_lsn`.**

The order is:

1. `ensure_replication_slot` (creates main slot if missing — see SM-1).
2. `fetch_slot_metadata` (reads `confirmed_flush_lsn`).
3. `compute_resume_lsn` — for exports with `resume_upper == 0`, uses `confirmed_flush_lsn`.
4. `run_snapshot` — creates temp slot, captures `snapshot_lsn = consistent_point - 1`.
5. `run_replication` — `START_REPLICATION SLOT main_slot AT resume_lsn`.

In a normal cold-start with a *just-created* main slot, `confirmed_flush_lsn` equals
`restart_lsn` equals "where the slot was created". Then `snapshot_lsn = consistent_point - 1`
is at-or-just-after `confirmed_flush_lsn`. Replication starts at the smaller of these and
the rewind logic retracts snapshot rows whose LSN is `<= snapshot_lsn`. OK in theory.

But there's a subtle race: between step 1 (slot creation at `restart_lsn = X`) and step 4
(temp slot at `consistent_point = Y`), `Y > X` because WAL advanced. If `confirmed_flush_lsn`
in step 2 is read *before* the main slot has accepted any data (cold start), it equals
`restart_lsn = X`. Resume_lsn = X. Snapshot_lsn = Y - 1, with X < Y - 1. Replication
streams from X onward. Events in `(X, Y - 1]` are *not* in the snapshot (snapshot was at
Y) but *are* in the replication stream. Rewind retracts snapshot rows for LSN ≤ Y - 1,
which is all of them — including the legitimate snapshot rows at LSN 0. Result: snapshot
rows are double-retracted (once by the rewind, never re-emitted because the snapshot
events were emitted at LSN 0 with Diff::ONE and rewind emits at LSN 0 with Diff::-1).

Actually wait — re-reading `emit_row`:
```rust
if let Some(req) = rewinds.get(&output_index) {
    if commit_lsn <= req.snapshot_lsn {
        batch.push(((export_id, row.clone(), 0), 0, -diff));  // rewind at LSN 0
    }
}
batch.push(((export_id, row, commit_lsn), commit_lsn, diff));  // event at real LSN
```

The rewind emits the *same row* at LSN 0 with negated diff, plus the original at
`commit_lsn`. Snapshot rows are sent separately by `snapshot_table` at LSN 0 with `Diff::ONE`.
So at LSN 0, we have: snapshot row (+1) + rewind retraction (-1) for events whose
commit_lsn ≤ snapshot_lsn. At commit_lsn we have: the original event (+1).

This is the same algebra as today's PG. The implementation looks correct *if* the
snapshot rows and the rewind retractions actually pair up — i.e. for every row R that
existed in the table at snapshot_lsn but was modified at LSN ≤ snapshot_lsn, the snapshot
emitted R and the rewind emits the modification. **But snapshot is just `COPY (SELECT *)`,
which gives the *current* state at snapshot_lsn, not a log of changes.** The rewind
algebra requires the *changes* between t_slot (the resume LSN) and t_snapshot to be
subtractable from the snapshot. Today's PG handles this by emitting the replication
events at LSN 0 with negated diffs **only** for events whose `commit_lsn ∈ (t_slot,
t_snapshot]`. The prototype's check is `commit_lsn ≤ req.snapshot_lsn` — there's no
`> t_slot` lower bound.

**This is a real bug.** Events with `commit_lsn ≤ resume_lsn` (which equals
`confirmed_flush_lsn` for snapshot-needing exports) are *also* rewound, producing
double-counting. On a freshly-created slot, `confirmed_flush_lsn = restart_lsn = X` and
no events have commit_lsn ≤ X (the slot didn't exist before X), so this is benign at cold
start. But on resumption with some exports needing snapshot and others past minimum, the
new snapshot's `snapshot_lsn` may be far past `confirmed_flush_lsn`, and replication
events in `[earliest_resume_lsn, t_slot]` are erroneously rewound for the snapshotting
exports.

Wait — `RewindRequest` is keyed by `output_index`. So the rewind is per-export. For
export E that needs snapshot, the rewind retracts events at `commit_lsn ≤ snapshot_lsn`.
But the replication stream starts at `min(resume_lsn over all exports)`. For export E
specifically, since its `resume_upper == 0`, the relevant range is `[0, snapshot_lsn]`.
Today's behavior in `replication.rs::process_event`:
```
events at commit_lsn ≤ snapshot_lsn → emit at LSN 0 with negated diff (rewind)
events at commit_lsn > snapshot_lsn → emit at commit_lsn (normal)
```
The prototype matches this. **My alarm was misread.** Let me back off — the prototype's
rewind algebra is correct.

So what *is* the slot management bug? Re-reading:

**SM-3. Slot is killed if active. Aggressive.**

```rust
if let Some(active_pid) = slot_metadata.active_pid {
    tracing::warn!(%id, %active_pid, "replication slot in use; killing existing connection");
    let _ = metadata_client.execute("SELECT pg_terminate_backend($1)", &[&active_pid]).await;
}
```

If two storage replicas race for the same slot (which can happen during failover or in
specific multi-replica configurations), they will repeatedly terminate each other. This
also makes single-replica restart racy: the *outgoing* dataflow's slot-holder may not be
fully gone when the new dataflow starts; killing it can race with PG's own cleanup.

Today's `replication.rs` in main does not unconditionally kill — it errors with
`SlotInUse` and lets the controller decide. The prototype should restore that behavior.

**SM-4. Snapshot's temp slot may leak on task cancellation.**

`run_snapshot` creates a temp slot named `mzsnapshot_<uuid>` and `BEGIN`s a transaction.
On `AbortOnDropHandle::abort()`, the futures are cancelled mid-await; the `replication_client`
is dropped, which closes the session, which (per PG) drops temporary slots. Should be
fine *if* PG actually closes the session promptly — but on network hangs this may take
the keepalive timeout. Not a correctness issue, but a resource leak that may surface in
CI under repeated start/stop.

Fix: explicit `ROLLBACK; DROP_REPLICATION_SLOT mzsnapshot_<uuid>` in a `Drop` shim before
the client drops.

**SM-5. `last_committed_upper` only advances when `resume_rx.changed()` fires.**

The keepalive sends `last_committed_upper` to PG every second (good — drives
`confirmed_flush_lsn` advancement). `last_committed_upper` initializes to `resume_lsn`
and advances only when the framework's `resume_tx` watch channel updates.

The framework's `resume_upper_forward` task reads from the reclocked `committed_upper`
stream. If the persist sink is making progress, this works. If something *upstream* of
the reclock stalls (e.g. the channel_reclock operator wedges, or the persist sink
stalls), `confirmed_flush_lsn` never advances, the slot grows unbounded, and PG
eventually exhausts WAL retention. This is the same risk the legacy code had — but the
*detection* path is different. Legacy path: stalled health from the persist sink. Task
path: nothing surfaces until PG starts complaining.

Fix: add a watchdog that emits a warning health if `last_committed_upper` hasn't advanced
in N seconds while data is being processed.

### Other prototype gaps that may bite CI separately

**G-1. Hardcoded single-worker.** `is_active_worker = worker_id == 0`. Multi-worker tests
will only see worker 0 doing work. Today's PG's snapshot is multi-worker via ctid ranges;
prototype is single-worker. Performance regression on snapshot of large tables; tests
asserting parallelism will fail.

**G-2. Remap operator forced to worker 0.** `remap_operator(..., Some(0))` overrides the
default `id.hashed() % worker_count` placement. If two PG sources land on worker 0, they
share a worker for both their tasks *and* their remap writes. Workload imbalance.

**G-3. `start_signal` is dropped.** `_start_signal` is never awaited. Upsert sources
that depend on rehydration completion before snapshot will start producing data
prematurely. Probably no upsert PG test exists yet; will fail when one is added.

**G-4. `_metadata_client` unused in `process_transaction`.** Schema validation is
missing — the prototype does not call `verify_schema` or any equivalent during
replication. Schema-change testdrive scenarios will fail.

**G-5. `tracing::warn!("ENTERED")` debug logs.** Cosmetic but pollutes test logs; some
test infrastructure greps for warnings as failures.

### Mapping prototype to the proposed `MultiplexedSource` trait

The prototype's `SourceTask` is essentially the proposed trait but flatter:
- `spawn(config, resume_rx)` ≈ what would become a thin shim that calls
  `plan_snapshot` + `snapshot_table` + `stream` + `commit` from a single `async fn run`.
- `data_tx` carries all three of: snapshot data, stream data, errors, frontier batches.
  Proposed split: `TableEventStream` for snapshot, `MultiplexedStream` for stream.
- `probe_tx` = direct watch channel; proposed: from `probe()` calls polled by shim.
- `health_tx` = direct mpsc; proposed: framework converts `Transient` to halting health
  and routes `DefiniteError`s into the data path.

The prototype is a *useful intermediate*: it proves the channel-based supervisor works.
The proposed trait split (probe / plan_snapshot / snapshot_table / stream / commit) is
the next step — break `pg_source_task_inner` into those methods.

### Recommendations for the prototype

Order of fixes to unblock CI, then refactor:

1. **SM-1**: stop calling `ensure_replication_slot`. Treat missing slot as definite.
2. **SM-3**: stop killing slot holders. Surface `SlotInUse` as transient health.
3. **EP-1**: re-classify `BareTransactionEvent` / `NestedTransaction` /
   `UnknownReplicationMessage` / `ReplicationEOF` as definite where appropriate.
4. **EP-3**: exit task on `data_tx.send` failure with a typed error.
5. **EP-4**: when `compute_resume_lsn` is `None`, drop senders and exit.
6. **G-3**: honor `start_signal` (await before snapshot/replication starts).
7. **G-4**: add per-event schema verification (port `verify_schema` into the task).
8. **SM-5**: watchdog on `last_committed_upper`.
9. **G-1, G-2**: keep single-worker for v1 but file follow-ups for snapshot parallelism
   and proper worker placement.
10. **Refactor**: split `pg_source_task_inner` into `MultiplexedSource` trait methods.
   This is no longer a fix, it's the migration to the proposed design.

### Closing now-answered questions

- "What's the gap in 'partially working'?" — Items SM-1, SM-3, EP-1 are the most likely
  CI-failure causes. EP-4, G-3, G-4 are runner-up suspects.
- "Where does the prototype put capability ownership?" — Entirely in `channel_reclock`
  (the shim). Connector task never touches `Capability`. Confirms the hybrid model.
- "How is backpressure expressed?" — Currently it isn't. `data_tx` is unbounded mpsc.
  No bound, no `busy_signal`-like mechanism. **This is a v1 gap.** Should be a bounded
  channel keyed on `STORAGE_SOURCE_CHANNEL_CAPACITY`.
- "Is there a probe/frontier channel separate from the data channel?" — Yes, three
  channels: `data_rx` (mpsc, batches with frontier), `probe_rx` (watch, single latest),
  `health_rx` (mpsc, all health). The frontier travels embedded in `SourceBatch.frontier`,
  not as a separate item. Reasonable.

---

## 2026-04-29 — backpressure correction

Prior entry said "backpressure is missing." Inaccurate. The design intent is
**`resume_upper`-based backpressure**: the task receives the reclocked committed upper
via `resume_rx` and slows itself when ingestion falls too far behind. The wiring is in
place; the gating logic isn't.

### What's wired vs what's missing

Wired (works):
- `SourceTask::spawn` accepts a `watch::Receiver<Antichain<FromTime>>`.
- `create_raw_source_from_task` spawns `resume_upper_forward` to feed the reclocked
  committed_upper stream into `resume_tx`.
- `pg_source_task`'s `tokio::select!` loop arm `Ok(()) = resume_rx.changed() => { ... }`
  updates `last_committed_upper` (used immediately for PG standby keepalives).

Missing: the actual *pause*. Today's loop unconditionally awaits `stream.next()` and
processes whatever PG sends. There is no check like "if `data_upper -
last_committed_upper > threshold`, stop reading from the replication stream and only
wait for `resume_rx.changed()`."

### Evaluating the design

`resume_upper`-as-backpressure has real advantages over a bounded channel:

- **Natural unit.** LSN/GTID lag is meaningful to operators and observable in metrics.
  Bytes-in-channel is meaningful only to engineers.
- **No tuning of channel size per source.** The threshold is in source-domain time,
  which is intrinsic.
- **Composes with upstream slot management.** PG's `confirmed_flush_lsn` advancement
  *already* tracks committed upper; the backpressure mechanism reuses that signal.
- **Single source of truth.** The same number that determines slot advancement
  determines task pacing.
- **Doesn't conflate buffering with progress.** A bounded channel saturating is ambiguous
  (slow consumer? big record?); LSN lag is unambiguous.

The disadvantages, with the recommendation for each:

- **Memory not directly bounded.** A burst of large transactions can fill memory while
  LSN lag is small. *Mitigation:* still use a bounded channel as a floor, sized
  generously (e.g. 64MiB worth of batches). The bounded channel exists only as a
  memory-safety guarantee, not as the primary rate control. If it ever saturates, that's
  diagnostic — the persist sink is genuinely stuck and the task should also halt.
- **Higher latency to detect saturation.** Resume_upper updates lag the actual commit by
  reclock + persist round-trip time. *Mitigation:* set the lag threshold high enough
  that the round-trip is small relative to it (e.g. tens of seconds of LSN delta).
- **Restart races.** On restart, `resume_rx` initial value is `Antichain::from_elem(min)`
  until the first feedback arrives. The task would see infinite lag and pause forever.
  *Mitigation:* initialize `last_committed_upper` from the resume LSN computed at task
  start, not from the watch channel. The prototype already does this:
  `let mut last_committed_upper = resume_lsn;`.

### Where the gate should sit in the loop

The PG task's main loop today:

```rust
loop {
    tokio::select! {
        biased;
        _ = feedback_timer.tick() => { send_standby_keepalive(...) }
        Ok(()) = resume_rx.changed() => { last_committed_upper = ...; }
        msg = stream.next() => { ... process ... }
    }
}
```

To gate on lag, transform into a small state machine:

```rust
loop {
    // Decide whether we're allowed to read more data.
    let lagging = data_upper.lag_beyond(&last_committed_upper, lag_threshold);

    tokio::select! {
        biased;
        _ = feedback_timer.tick() => { send_standby_keepalive(...) }
        Ok(()) = resume_rx.changed() => { last_committed_upper = ...; }
        msg = stream.next(), if !lagging => { ... process ... }
        // When `lagging` is true, no progress on the stream until resume_rx updates.
    }
}
```

The `if !lagging` guard on the stream arm makes `tokio::select!` skip that branch when
lagging. Keepalive and feedback continue to flow.

### Threshold semantics

For PG (LSN, totally ordered): `data_upper - last_committed_upper > N_BYTES_OF_WAL`.
Configurable via dyncfg (e.g. `STORAGE_PG_SOURCE_BACKPRESSURE_LAG_BYTES`, default 256MB).

For MySQL (GtidPartition, partially ordered): trickier. The "lag" between two
`Antichain<GtidPartition>` is not a scalar. Options:

- Use *count* of source-uuids that have advanced past the committed antichain. Coarse
  but well-defined.
- Use the maximum scalar lag across source-uuids that are present in both frontiers.
  Approximates "the most lagging partition."

For SQL Server (Lsn): similar to PG; LSN difference.

For Kafka (`Partitioned<RangeBound<PartitionId>, MzOffset>`): demuxed source — pause is
*per-partition* and is a per-partition offset delta. That's natural in the demuxed kernel
and doesn't need a global aggregation.

This argues for the threshold being part of the connector's responsibility (it knows the
shape of its time domain), with framework primitives provided. Suggested trait method:

```rust
trait MultiplexedSource {
    /// Returns true if data_upper is too far ahead of committed_upper to safely read more.
    /// Default impl returns false (no backpressure).
    fn should_pause(
        data_upper: &Antichain<Self::Time>,
        committed_upper: &Antichain<Self::Time>,
    ) -> bool;
}
```

Or, more minimally: connector knows its own type and gates internally. Keep the framework
out of it.

### What changes in the prior recommendations

Strike "Backpressure is missing." from the prototype review. Replace with:

> Backpressure is **designed but unfinished**. The `resume_rx` watch channel and the
> tracking of `last_committed_upper` are wired through the supervisor and the PG task.
> The lag-based gate on `stream.next()` is not yet implemented. Add the gate before
> moving past CI.

The earlier recommendation to introduce a bounded `data_tx` channel still stands, but
re-cast: the bounded channel is a **memory-safety floor**, not the primary rate-control
mechanism. Size it generously (e.g. 1024 batches or 64MiB equivalent); under healthy
operation the lag-gate keeps the task within the bound, and the channel only saturates
in pathological cases.

### Updated v1 plan

In addition to the prior fix list (SM-1, SM-3, EP-1, EP-3, EP-4, G-3, G-4, SM-5):

11. **Add the `should_pause` gate** in `pg_source_task`'s `tokio::select!` and a dyncfg
    `STORAGE_PG_SOURCE_BACKPRESSURE_LAG_BYTES`. Test: assert that when the persist sink
    is held back, the task's CPU/network drops correspondingly.
12. **Add a bounded `data_tx`** sized to a configurable byte budget. Saturation emits a
    halting health (memory-safety violation = configuration bug or persist hang).

### Now-closeable

- "How is backpressure expressed?" — Answered: by `resume_upper` lag feedback. Not
  bounded-channel. The prior entry's claim was wrong.
- The proposed kernel design (this log's middle entries) should reflect that
  `should_pause` (or equivalent) is a connector-driven decision, not a framework one.

---

## 2026-04-29 — closing remaining design questions

The remaining open questions all collapse once examined against actual connector
behavior. Closing them as a batch.

### Q1: SnapshotPlan replayability across restarts

**Closed: not needed.**

PG cannot resume a partially-completed snapshot. If `COPY` fails at 50% of a table, we
redo the whole table. What survives a restart is **per-export completion**: in a source
with 3 tables, if A and B finish before C fails, on restart only C is re-snapshotted.
This already works because `source_resume_uppers` per export is consulted at task start,
and exports past minimum skip the snapshot phase.

The temp slot's `consistent_point` is only useful *during* the snapshot phase. Once the
snapshot commits, the temp slot is dropped and the LSN it referenced is unreachable
anyway. There's nothing left to persist.

Generalizes:

- **MySQL**: per-table `SELECT *`; same model.
- **SQL Server**: per-capture-instance snapshot within a CDC transaction; same model.
- **Kafka / generator**: no snapshot.

The unit of snapshot durability is **the export's persist shard upper**. Already durable.
Plan replayability across restarts buys nothing.

### Q2: Multiplexed `stream` shape — one stream vs stream-of-streams

**Closed: one stream.**

PG/MySQL/SS all read *one* upstream logical stream (replication slot / binlog / CDC
handle). The connector cannot read it in parallel by export — there is no per-export
upstream stream to read.

Returning a stream-of-streams from the connector would require the connector to buffer
and demux internally. That's strictly worse than handing the framework one stream and
letting the framework route by `export_id`. One stream is the only design that maps to
the upstream reality.

Per-export *parallelism* (in decode/envelope) is already a framework concern downstream
of the connector and doesn't need to be exposed in the trait.

### Q3: Demuxed `discover_partitions` — deltas vs absolute

**Closed: absolute.**

Kafka's metadata fetcher today returns absolute partition list with high watermarks.
Computing deltas in the connector is redundant: the framework already needs to track
previous state to detect topic-recreation regressions (frontier going backwards). Absolute
is simpler, framework diffs.

### Q4: Separate `mz-source-framework` crate

**Deferred. Not a design question — packaging.** Revisit after v1 lands and the API has
stabilized through real use.

### Design phase status

All design questions raised in this log are now either answered or deferred to packaging.
The remaining work is implementation:

1. Unblock the prototype's CI failures (SM-1, SM-3, EP-1, EP-3, EP-4 from the prototype
   review).
2. Add the backpressure gate (lag-based pause on `resume_rx`).
3. Restore feature parity for PG (schema verification, `start_signal`).
4. Refactor `pg_source_task_inner` into `MultiplexedSource` trait methods.
5. Migrate MySQL → MultiplexedSource.
6. Migrate SQL Server → MultiplexedSource.
7. Build `DemuxedSource` trait + supervisor.
8. Migrate Kafka → DemuxedSource.
9. Migrate load generator (Simple → Multiplexed, KV → Demuxed).

Steps 1–4 establish the multiplexed framework against a working connector. Steps 5–6
validate it against two more. Step 7 forks the demuxed framework. Steps 8–9 finish
migration. Direction (B) "collapse scopes" falls out automatically — the prototype
already demonstrates this.

Future log entries should be implementation observations, not design decisions, unless
something surfaced during implementation forces a redesign.

---

## 2026-04-29 — `MultiplexedSource` trait sketched against PG prototype

Concrete trait surface, validated by walking through what each `pg_source_task_inner`
step would become. Bias toward the prototype's actual shape (single-worker, single-LSN
domain) with explicit hooks for future per-table parallelism.

### Trait surface

```rust
use std::pin::Pin;
use futures::Stream;
use serde::{Deserialize, Serialize};

pub trait MultiplexedSource: Send + 'static {
    type Time: SourceTimestamp;

    /// Snapshot plan broadcast from leader to followers. Must be small and serializable.
    type Plan: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;

    /// One unit of snapshot work. Distributed across workers.
    type Partition: Serialize + DeserializeOwned + Send + 'static;

    /// Stream returned from `snapshot_partition`.
    type SnapshotStream: Stream<Item = TableEvent<Self::Time>> + Send + Unpin + 'static;

    /// Stream returned from `stream`.
    type ChangeStream: Stream<Item = TableEvent<Self::Time>> + Send + Unpin + 'static;

    const STATUS_NAMESPACE: StatusNamespace;

    // -- Construction --

    /// Construct a per-worker connector instance. Receives framework-provided handles
    /// for committed-upper feedback (used for backpressure + upstream acknowledgment)
    /// and configuration.
    fn new(config: ConnectorConfig<Self::Time>) -> Self;

    // -- Probe (single-worker, periodic) --

    /// Read the current upstream write frontier. Framework owns the Ticker and calls
    /// this every `timestamp_interval`.
    async fn probe(&mut self) -> Result<Antichain<Self::Time>, Transient>;

    // -- Snapshot (leader/followers) --

    /// Single-worker (leader). Determines whether a snapshot is needed and, if so,
    /// establishes the consistency point that followers will share.
    ///
    /// Returns `None` when no export needs a snapshot. Otherwise the connector must
    /// hold any coordination state (e.g. an open transaction containing an exported
    /// snapshot) until `finalize_snapshot` is called.
    async fn plan_snapshot(
        &mut self,
        exports: &[ExportRequest<Self::Time>],
    ) -> Result<Option<SnapshotPlan<Self>>, Transient>;

    /// Per-worker. Snapshot one partition using the broadcast `plan`.
    /// Returns a stream that ends when the partition is fully snapshotted.
    async fn snapshot_partition(
        &mut self,
        plan: Self::Plan,
        partition: Self::Partition,
    ) -> Result<Self::SnapshotStream, Transient>;

    /// Single-worker (leader). Called after every worker has consumed every
    /// `snapshot_partition` stream to completion. Releases coordination state.
    async fn finalize_snapshot(&mut self) -> Result<(), Transient>;

    // -- Stream (single-worker) --

    /// Begin streaming changes from `from` forward. Framework calls this once after
    /// `finalize_snapshot` (or directly, if no snapshot was needed).
    ///
    /// The connector internally observes `ConnectorConfig::committed_upper_rx` for
    /// upstream acknowledgment (e.g. PG standby keepalives) and for backpressure (gate
    /// reading the upstream stream when the connector's `data_upper` is too far ahead
    /// of `committed_upper`).
    async fn stream(
        &mut self,
        from: Antichain<Self::Time>,
    ) -> Result<Self::ChangeStream, Transient>;
}

pub struct SnapshotPlan<S: MultiplexedSource + ?Sized> {
    pub plan: S::Plan,
    pub upper: Antichain<S::Time>,         // t_snapshot — used for rewind subtraction
    pub partitions: Vec<S::Partition>,
}

pub struct ExportRequest<T> {
    pub export_id: GlobalId,
    pub resume_upper: Antichain<T>,
    pub details: SourceExportDetails,
}

pub struct TableEvent<T> {
    pub export_id: GlobalId,
    pub time: T,
    pub payload: Result<Row, DefiniteError>,
}

pub struct ConnectorConfig<T: SourceTimestamp> {
    pub source_connection: ...,            // connection-specific configuration
    pub committed_upper_rx: watch::Receiver<Antichain<T>>,
    pub storage_config: StorageConfiguration,
    pub now_fn: NowFn,
    pub timestamp_interval: Duration,
    pub source_id: GlobalId,
    pub worker_id: usize,
    pub worker_count: usize,
}

pub enum Transient {
    /// Framework should restart the dataflow.
    Halting { error: anyhow::Error, namespace: StatusNamespace },
}
```

### PG implementation against the trait

```rust
pub struct PostgresMultiplexed {
    cfg: ConnectorConfig<MzOffset>,
    pg_conn: PostgresSourceConnection,

    /// Lazy: opened on first `probe` call.
    metadata_client: tokio::sync::OnceCell<Arc<Client>>,

    /// Held between `plan_snapshot` and `finalize_snapshot`. Owns the BEGIN; CREATE
    /// TEMPORARY REPLICATION SLOT; pg_export_snapshot transaction.
    snapshot_client: Option<Client>,
}

#[derive(Clone, Serialize, Deserialize)]
pub struct PgPlan {
    snapshot_id: String,
    snapshot_lsn: MzOffset,
}

#[derive(Serialize, Deserialize)]
pub struct PgPartition {
    table_oid: u32,
    output_index: usize,
    export_id: GlobalId,
    desc: PostgresTableDesc,
    casts: Vec<(CastType, MirScalarExpr)>,
    /// `None` means "scan whole table" (single-worker case). Future: ctid range.
    ctid_range: Option<(u64, Option<u64>)>,
}

impl MultiplexedSource for PostgresMultiplexed {
    type Time = MzOffset;
    type Plan = PgPlan;
    type Partition = PgPartition;
    type SnapshotStream = PgCopyStream;
    type ChangeStream = PgReplicationStream;
    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::Postgres;

    fn new(cfg: ConnectorConfig<MzOffset>) -> Self {
        let pg_conn = cfg.source_connection.clone().expect_postgres();
        Self { cfg, pg_conn, metadata_client: OnceCell::new(), snapshot_client: None }
    }

    async fn probe(&mut self) -> Result<Antichain<MzOffset>, Transient> {
        let client = self.ensure_metadata_client().await?;
        let lsn = fetch_max_lsn(&*client).await
            .map_err(|e| Transient::halting(e, StatusNamespace::Postgres))?;
        Ok(Antichain::from_elem(lsn))
    }

    async fn plan_snapshot(
        &mut self,
        exports: &[ExportRequest<MzOffset>],
    ) -> Result<Option<SnapshotPlan<Self>>, Transient> {
        // Filter to exports needing snapshot.
        let snapshotting: Vec<_> = exports
            .iter()
            .filter(|e| *e.resume_upper == [MzOffset::from(0u64)])
            .collect();
        if snapshotting.is_empty() { return Ok(None); }

        // Open replication client; BEGIN; CREATE TEMP SLOT; export snapshot.
        let mut client = self.connect_replication().await?;
        let (snapshot_lsn, snapshot_id) = export_snapshot_for_copy(&client).await?;

        let partitions = snapshotting.iter().map(|e| {
            let pg = e.details.expect_postgres();
            PgPartition {
                table_oid: pg.table.oid,
                output_index: e.output_index,
                export_id: e.export_id,
                desc: pg.table.clone(),
                casts: pg.column_casts.clone(),
                ctid_range: None,    // single-worker for v1
            }
        }).collect();

        self.snapshot_client = Some(client);

        Ok(Some(SnapshotPlan {
            plan: PgPlan { snapshot_id, snapshot_lsn },
            upper: Antichain::from_elem(snapshot_lsn + MzOffset::from(1)),
            partitions,
        }))
    }

    async fn snapshot_partition(
        &mut self,
        plan: Self::Plan,
        partition: Self::Partition,
    ) -> Result<Self::SnapshotStream, Transient> {
        // Single-worker v1: snapshot_client is the leader's; we are the leader.
        // Multi-worker future: open a fresh client, SET TRANSACTION SNAPSHOT plan.snapshot_id.
        let client = self.snapshot_client.as_ref()
            .ok_or_else(|| Transient::halting_msg("snapshot client missing", Self::STATUS_NAMESPACE))?;

        let copy_stream = open_copy_query(client, &partition).await?;
        Ok(PgCopyStream::new(copy_stream, partition))
    }

    async fn finalize_snapshot(&mut self) -> Result<(), Transient> {
        if let Some(client) = self.snapshot_client.take() {
            client.simple_query("COMMIT;").await
                .map_err(|e| Transient::halting(e.into(), Self::STATUS_NAMESPACE))?;
        }
        Ok(())
    }

    async fn stream(
        &mut self,
        from: Antichain<MzOffset>,
    ) -> Result<Self::ChangeStream, Transient> {
        let from_lsn = from.into_option()
            .ok_or_else(|| Transient::halting_msg("empty resume frontier", Self::STATUS_NAMESPACE))?;
        let client = self.connect_replication().await?;
        let pg_stream = start_replication(&client, &self.pg_conn, from_lsn).await?;
        let table_info = self.build_table_info();
        let committed_upper_rx = self.cfg.committed_upper_rx.clone();
        let lag_threshold = pg_lag_threshold(&self.cfg);

        // Spawn the internal task that owns the LogicalReplicationStream and shares
        // it between data-read and standby-write paths. It exposes a Stream of
        // TableEvents to the caller.
        Ok(PgReplicationStream::spawn(
            pg_stream, table_info, committed_upper_rx, lag_threshold,
        ))
    }
}
```

### Where each piece of `pg_source_task_inner` lands

| Today's prototype                          | Trait location                                |
|--------------------------------------------|-----------------------------------------------|
| Connect replication+metadata clients       | Lazy in `probe` / `plan_snapshot` / `stream`  |
| `ensure_replication_slot` (SM-1 bug)       | **Removed.** Missing slot → definite error    |
| `fetch_slot_metadata` + kill active pid    | In `stream` setup; kill removed (SM-3)        |
| `compute_resume_lsn`                       | Framework-side from `ExportRequest.resume_upper` |
| Initial probe                              | Framework calls `probe()` before plan         |
| `run_snapshot` outer loop                  | Framework drives `snapshot_partition` per table|
| `export_snapshot_for_copy`                 | `plan_snapshot`                               |
| Per-table COPY                             | `snapshot_partition` returns `SnapshotStream` |
| `COMMIT;` after all tables                 | `finalize_snapshot`                           |
| `START_REPLICATION` + main loop            | `stream` returns `ChangeStream`               |
| `feedback_timer` standby keepalive         | Inside `PgReplicationStream` task             |
| `resume_rx.changed()` tracking             | Inside `PgReplicationStream` task             |
| `process_transaction` event decode         | Inside `PgReplicationStream` task             |
| `flush_replication_batch` frontier mgmt    | **Framework owns** — connector emits `TableEvent` items only |
| `emit_row` rewind subtraction              | Framework's `RewindCoordinator` (driven by `SnapshotPlan.upper`) |
| `send_batch` batching                      | **Removed** — framework batches              |
| Health "Running" emission                  | Framework emits at construction               |
| Halting on transient                       | Framework converts `Transient` to halting health |
| Per-record `DataflowError` in stream       | `TableEvent.payload = Err(DefiniteError)`     |

### What the framework owns

For the multiplexed shape:

1. **Probe loop.** `Ticker` calls `probe()`, wraps in `Probe<T>`, feeds the remap operator.
2. **Snapshot orchestration.**
   - On startup, ask connector for `ExportRequest`s from each export's resume_upper.
   - Call `plan_snapshot(exports)` on the leader worker.
   - Broadcast the `Plan` to all workers.
   - Distribute `partitions` to workers (initially: all to one worker; later: ctid-range
     across workers for parallel snapshot).
   - Each worker calls `snapshot_partition` and feeds the resulting stream into the
     channel reclock (with rewind subtraction at LSN ≤ `SnapshotPlan.upper`).
   - When all snapshot streams end, leader calls `finalize_snapshot`.
3. **Streaming.** Single worker calls `stream(resume_upper)`; framework feeds the
   `ChangeStream` items into the same channel reclock.
4. **Rewind coordinator.** Framework subtracts events at LSN ≤ `SnapshotPlan.upper` from
   the snapshot at LSN 0 with negated diffs. Connector never sees rewind; it just emits
   events at their natural LSN.
5. **Frontier batching.** Framework groups `TableEvent`s into `SourceBatch`es and
   computes the FromTime frontier from the connector's emitted timestamps.
6. **Backpressure feedback.** Framework feeds `committed_upper` into
   `ConnectorConfig::committed_upper_rx`; connector self-throttles.
7. **Error routing.** `Transient` → halting health. `TableEvent::payload = Err(...)` →
   per-export error collection.

### What does *not* fit cleanly

A few things had to be smudged:

1. **`stream` returning a `ChangeStream` that the connector pre-spawns a task for.**
   The replication connection is bidirectional (read XLogData, write standby keepalives).
   The trait would prefer a pure data stream, but PG's connection can't be split into
   independent read/write halves while keeping the keepalive cadence aligned with the
   stream's progress. So the connector internally spawns a task and returns a
   `mpsc::Receiver`-flavored stream. Acceptable: it's encapsulated in the connector and
   doesn't leak into the trait surface.

2. **`finalize_snapshot` exists only because PG's snapshot transaction can't be
   committed inside a per-partition method without breaking the multi-worker case.**
   In single-worker (today's prototype) it's a one-line `COMMIT;`. Future multi-worker
   makes it the place where the leader synchronizes with followers. Worth keeping even
   though it's a no-op for some connectors (KV generator's `finalize_snapshot` returns
   `Ok(())` immediately).

3. **`Plan` size.** PG's `PgPlan` is small (a uuid + an LSN). MySQL's plan would be
   larger (initial GTID set per table). SQL Server's plan is a per-capture-instance
   `initial_lsn` map. All within reason for a single broadcast event. Document the
   size cap (e.g. ≤ 64KiB) so connector authors don't accidentally smuggle whole
   schemas through.

4. **Lazy `metadata_client` in PG.** First `probe` call connects; subsequent calls
   reuse. If `probe` hits a transient, the connector clears the cell and re-tries on
   next call. Acceptable pattern for the trait.

### What remains in connector code (PG)

After this refactor, `postgres.rs` shrinks but keeps:

- All PG-specific protocol code (replication parsing, COPY decoding, type casts).
- The `replication_task` internal helper (essentially today's `run_replication` loop,
  but feeding a channel rather than `data_tx` directly).
- Schema verification (still TODO from prototype G-4).
- Error classification (definite vs transient, per EP-1).
- Lag-based backpressure gate (per the backpressure correction entry).

What disappears:

- `pg_source_task` outer wrapper.
- `pg_source_task_inner` — split into `probe` / `plan_snapshot` / `snapshot_partition` /
  `finalize_snapshot` / `stream`.
- All `data_tx`/`probe_tx`/`health_tx` channel construction and sender management.
- `compute_resume_lsn` — framework computes from ExportRequests.
- `flush_replication_batch`, `send_batch` — framework batches.
- The big `tokio::select!` mixing feedback timer, resume_rx, and stream events — split
  between `stream`'s internal task (which still has a select, but is purely connector-
  internal) and framework-owned timers.

Estimated PG LOC after refactor: ~600–700 (today's prototype is ~1100, today's main is
~2700). Most of the savings come from removing channel plumbing and the snapshot leader
broadcast scaffolding (which framework now owns).

### Open implementation questions surfaced by the sketch

(These are implementation, not design — listing them here for the next implementer.)

1. **Does `OnceCell` on a non-Sync `Client` cause issues across the connector's method
   calls?** Probably need `tokio::sync::OnceCell` or `Mutex<Option<Arc<Client>>>`.
2. **Where does the `resume_holder` token (today in `create_raw_source_from_task`) live?**
   The framework still needs to keep the resume-forwarding task alive for the lifetime
   of the dataflow — same dummy-operator pattern as today.
3. **Is `Self::SnapshotStream`/`Self::ChangeStream` as associated types worth the
   complexity vs. `Pin<Box<dyn Stream + Send>>`?** Heap allocation per *stream* (not
   per item) is once per source startup; box-dyn is fine and simpler. Recommend
   `Pin<Box<dyn Stream<Item = TableEvent<Self::Time>> + Send>>` and forget associated
   types.
4. **`ConnectorConfig` vs threading individual fields?** Bag-of-fields is ergonomic;
   start there, factor when the bag grows past ~10 fields.
5. **Per-export schema verification timing.** PG validates schema on each replication
   event. The `ChangeStream` task's loop is the right place for this. Verification
   failure → emit `TableEvent { payload: Err(DefiniteError::IncompatibleSchema), ... }`
   and ignore that export's events thereafter, but keep streaming for other exports.

### Verdict

The trait holds up against PG. The smudged points (`finalize_snapshot`, internally-
spawned stream task) are acceptable encapsulation costs, not API breaks. The framework's
share of work (probe loop, snapshot orchestration, rewind coordinator, frontier
batching, backpressure feedback) is exactly the shared-infrastructure layer identified
in earlier entries — confirming that layer is real and reusable.

Recommend proceeding to implementation: refactor `pg_source_task_inner` into the trait,
keep the prototype's channel-based supervisor as the framework, then validate against
MySQL.

---

## 2026-04-29 — `MultiplexedSource` validated against MySQL; refinements applied

Read `mysql.rs`, `mysql/snapshot.rs`, `mysql/replication.rs`, `mysql/schemas.rs`,
`mysql/statistics.rs` (today's main, ~2000 LOC). Mapped each piece against the trait
sketched in the prior entry. Three concrete refinements emerged.

### Where MySQL maps cleanly

- **`probe()`**: `query_sys_var("global.gtid_executed")` → `gtid_set_frontier()`. Single
  client. Today's MySQL has this in a dedicated `statistics.rs` operator; collapses into
  the trait's `probe()`.
- **`stream(from)`**: same shape as PG. Single worker, binlog reader. Inside, schema
  validation events trigger `DefiniteError::IncompatibleSchema` per affected export.
  Same internal-task pattern as PG (binlog stream is a unidirectional read; no keepalive
  back-channel like PG's).
- **`finalize_snapshot()`**: trivial — MySQL has no leader-side state to release. Returns
  `Ok(())`.
- **`Partition` = `(table_name, exports_for_table)`**. Today's MySQL distributes tables
  across workers via `responsible_for(table_name)`. Framework distribution of partitions
  matches naturally.
- **Definite vs transient classification**: today's MySQL already has clean separation
  (`DefiniteError` / `TransientError`); maps directly to `TableEvent.payload = Err(...)`
  vs trait method `Result<_, Transient>`.

### Where MySQL forces refinements to the trait

#### Refinement 1: `Plan` can be empty; per-partition snapshot upper is the norm

PG: one worker creates a temp slot, `consistent_point` is *the* snapshot upper for all
partitions, broadcast to followers via `Plan { snapshot_id, snapshot_lsn }`.

MySQL: each worker **independently** locks its assigned tables, reads `@@gtid_executed`,
starts its own REPEATABLE READ + CONSISTENT SNAPSHOT transaction, unlocks, then reads.
**Each worker has its own `snapshot_upper`**. There is no leader broadcast.

This breaks the trait's "one Plan, one upper" assumption.

**Refinement to the trait:**
- `Plan` is allowed to be `()` (empty). MySQL: `type Plan = ()`. PG keeps `PgPlan`.
- `snapshot_partition` returns a `SnapshotChunk<S>` carrying both the stream **and** the
  per-partition snapshot upper:

  ```rust
  pub struct SnapshotChunk<S: MultiplexedSource + ?Sized> {
      pub stream: S::SnapshotStream,
      pub upper: Antichain<S::Time>,    // per-partition snapshot upper, for rewind
  }
  ```

- `SnapshotPlan` no longer carries `upper`. The framework collects each
  `SnapshotChunk.upper` per partition.

This isn't a downgrade for PG. PG's per-partition upper is just `Plan.snapshot_lsn` for
every partition; the framework still gets the same number, just per-partition.

#### Refinement 2: `t_slot` (rewind lower bound) is per-export, not per-source

The rewind subtraction range is `(t_slot, t_snapshot]`:
- `t_slot`: the LSN/GTID at which the export was first attached to the source. Below
  this, the export's data is durable (in persist) or is being snapshotted now.
- `t_snapshot`: the consistency point at which the snapshot was actually taken (per
  partition, per Refinement 1).

For MySQL, `initial_gtid_set` is **stored per export** in `SourceExportDetails::MySql`:

```
SourceOutputInfo {
    initial_gtid_set: Antichain<GtidPartition>,    // ← t_slot for this export
    resume_upper: Antichain<GtidPartition>,        // ← progress in persist
    ...
}
```

For PG, all exports added at source creation share `t_slot = slot's confirmed_flush_lsn`,
but exports added later via `ALTER SOURCE ... ADD SUBSOURCE` get their own `t_slot`.

**Refinement to the trait:** `ExportRequest` carries an `initial_upper` (the per-export
`t_slot`):

```rust
pub struct ExportRequest<T> {
    pub export_id: GlobalId,
    pub resume_upper: Antichain<T>,        // progress in persist (today's name)
    pub initial_upper: Antichain<T>,       // per-export t_slot (NEW)
    pub details: SourceExportDetails,
}
```

Framework's `RewindCoordinator` then computes per-(export, partition) rewind range:
`(export.initial_upper, partition.snapshot_chunk.upper]`.

Already true in MySQL (`initial_gtid_set` per export). For PG today: the prototype uses
the slot's `confirmed_flush_lsn` for all snapshotting exports; this needs to become
per-export to match MySQL's model and to support the late-added-subsource case correctly.
Today's main PG `replication.rs` already does this work via the rewind feedback edge.

#### Refinement 3: `stream(from)` may fail with a definite error

MySQL's `stream` startup queries `@@GTID_PURGED`; if the requested GTID set is no longer
in the binlog (server has purged), this is `DefiniteError::BinlogNotAvailable` — poisons
the source for every export, never recoverable by retry.

PG has analogous cases too (slot invalidated because oversized — `SlotInvalidated`
is already in the prototype's `DefiniteError` enum).

The trait sketch had `stream(from) -> Result<ChangeStream, Transient>`. Insufficient.

**Refinement to the trait:**

```rust
async fn stream(
    &mut self,
    from: Antichain<Self::Time>,
) -> Result<Self::ChangeStream, StreamStartError>;

pub enum StreamStartError {
    /// Restart the dataflow.
    Transient(Transient),
    /// Poison the source. Framework emits the error to every export's error collection
    /// and terminates the dataflow.
    Definite(DataflowError),
}
```

Same treatment for `plan_snapshot` and `snapshot_partition` if any startup-time error
should be definite. (PG's snapshot consistency-point capture can fail in ways that are
not deterministic, so leave those as `Transient` for now. MySQL's snapshot-side definite
errors come from schema mismatches mid-snapshot, which travel as `TableEvent.payload`,
not from method-startup.)

### Refined trait surface (incorporating both connectors)

```rust
pub trait MultiplexedSource: Send + 'static {
    type Time: SourceTimestamp;
    type Plan: Serialize + DeserializeOwned + Clone + Send + Sync + 'static;
    type Partition: Serialize + DeserializeOwned + Send + 'static;
    type SnapshotStream: Stream<Item = TableEvent<Self::Time>> + Send + Unpin + 'static;
    type ChangeStream: Stream<Item = TableEvent<Self::Time>> + Send + Unpin + 'static;

    const STATUS_NAMESPACE: StatusNamespace;

    fn new(config: ConnectorConfig<Self::Time>) -> Self;

    async fn probe(&mut self) -> Result<Antichain<Self::Time>, Transient>;

    async fn plan_snapshot(
        &mut self,
        exports: &[ExportRequest<Self::Time>],
    ) -> Result<Option<SnapshotPlan<Self>>, StreamStartError>;

    async fn snapshot_partition(
        &mut self,
        plan: Self::Plan,
        partition: Self::Partition,
    ) -> Result<SnapshotChunk<Self>, StreamStartError>;

    async fn finalize_snapshot(&mut self) -> Result<(), Transient>;

    async fn stream(
        &mut self,
        from: Antichain<Self::Time>,
    ) -> Result<Self::ChangeStream, StreamStartError>;
}

pub struct SnapshotPlan<S: MultiplexedSource + ?Sized> {
    pub plan: S::Plan,
    pub partitions: Vec<S::Partition>,
}

pub struct SnapshotChunk<S: MultiplexedSource + ?Sized> {
    pub stream: S::SnapshotStream,
    pub upper: Antichain<S::Time>,
}

pub struct ExportRequest<T> {
    pub export_id: GlobalId,
    pub resume_upper: Antichain<T>,
    pub initial_upper: Antichain<T>,
    pub details: SourceExportDetails,
}

pub enum StreamStartError {
    Transient(Transient),
    Definite(DataflowError),
}

pub struct TableEvent<T> {
    pub export_id: GlobalId,
    pub time: T,
    pub payload: Result<Row, DefiniteError>,
}
```

### MySQL implementation skeleton

```rust
pub struct MySqlMultiplexed {
    cfg: ConnectorConfig<GtidPartition>,
    my_conn: MySqlSourceConnection,
    /// Lazy connection for probe/replication-startup checks.
    probe_conn: tokio::sync::OnceCell<MySqlConn>,
}

impl MultiplexedSource for MySqlMultiplexed {
    type Time = GtidPartition;
    type Plan = ();    // No leader broadcast needed
    type Partition = MySqlPartition;
    type SnapshotStream = MySqlSnapshotStream;
    type ChangeStream = MySqlBinlogStream;
    const STATUS_NAMESPACE: StatusNamespace = StatusNamespace::MySql;

    fn new(cfg: ConnectorConfig<GtidPartition>) -> Self { ... }

    async fn probe(&mut self) -> Result<Antichain<GtidPartition>, Transient> {
        let conn = self.ensure_probe_conn().await?;
        let gtid_executed = query_sys_var(conn, "global.gtid_executed").await?;
        gtid_set_frontier(&gtid_executed)
            .map_err(|e| Transient::halting(e.into(), Self::STATUS_NAMESPACE))
    }

    async fn plan_snapshot(
        &mut self,
        exports: &[ExportRequest<GtidPartition>],
    ) -> Result<Option<SnapshotPlan<Self>>, StreamStartError> {
        let snapshotting: Vec<_> = exports.iter()
            .filter(|e| *e.resume_upper == [GtidPartition::minimum()])
            .collect();
        if snapshotting.is_empty() { return Ok(None); }

        // Group exports by table name. Each table becomes one Partition.
        let partitions = group_by_table(&snapshotting)
            .into_iter()
            .map(|(table, outputs)| MySqlPartition { table, outputs })
            .collect();

        // No leader work — MySQL workers self-coordinate via per-worker locks.
        Ok(Some(SnapshotPlan { plan: (), partitions }))
    }

    async fn snapshot_partition(
        &mut self,
        _plan: (),
        partition: MySqlPartition,
    ) -> Result<SnapshotChunk<Self>, StreamStartError> {
        // Per-worker dance: connect → LOCK TABLES → read @@gtid_executed → BEGIN
        // REPEATABLE READ + CONSISTENT SNAPSHOT → UNLOCK → SELECT.
        let mut lock_conn = self.connect("snapshot-lock").await?;
        lock_tables(&mut lock_conn, &partition.table).await?;
        let snapshot_upper = read_gtid_executed_as_frontier(&mut lock_conn).await?;
        let mut data_conn = self.connect("snapshot-data").await?;
        begin_consistent_snapshot(&mut data_conn).await?;
        unlock_tables(&mut lock_conn).await?;
        drop(lock_conn);

        let stream = MySqlSnapshotStream::spawn(data_conn, partition);
        Ok(SnapshotChunk { stream, upper: snapshot_upper })
    }

    async fn finalize_snapshot(&mut self) -> Result<(), Transient> {
        Ok(())   // No-op for MySQL
    }

    async fn stream(
        &mut self,
        from: Antichain<GtidPartition>,
    ) -> Result<Self::ChangeStream, StreamStartError> {
        let mut conn = self.connect("repl").await?;
        validate_mysql_repl_settings(&mut conn).await
            .map_err(|e| StreamStartError::Transient(...))?;

        // Check @@GTID_PURGED — if the requested frontier is no longer in the binlog,
        // emit a definite error.
        let purged = query_sys_var(&mut conn, "global.gtid_purged").await
            .map_err(|e| StreamStartError::Transient(...))?;
        if purged_dominates(&purged, &from) {
            return Err(StreamStartError::Definite(
                DefiniteError::BinlogNotAvailable.into()));
        }

        let binlog = open_binlog_stream(&mut conn, &from).await?;
        let committed_upper_rx = self.cfg.committed_upper_rx.clone();
        Ok(MySqlBinlogStream::spawn(binlog, committed_upper_rx, ...))
    }
}
```

### Mapping each MySQL piece to the trait

| Today's MySQL                        | Trait location                                |
|--------------------------------------|-----------------------------------------------|
| `mysql.rs::render`                    | Eliminated — framework owns dataflow assembly |
| `validate_mysql_repl_settings`        | `stream` startup                              |
| `mysql/statistics.rs` probe loop      | Framework calls `probe()`                     |
| `mysql/statistics.rs` resume_uppers loop | Framework feeds `committed_upper_rx`       |
| Per-table worker assignment via `responsible_for(table_name)` | Framework distributes `Vec<Partition>` |
| Table lock + `@@gtid_executed` + REPEATABLE READ | `snapshot_partition`                |
| `RewindRequest { output_index, snapshot_upper }` | `SnapshotChunk { stream, upper }` per partition + `ExportRequest::initial_upper` per export |
| `verify_schemas` at snapshot          | Inside `MySqlSnapshotStream` task             |
| `verify_schemas` mid-replication      | Inside `MySqlBinlogStream` task               |
| `BinlogNotAvailable` definite error   | `StreamStartError::Definite` from `stream`    |
| `IncompatibleSchema` definite error per-table | `TableEvent.payload = Err(...)` for that export |
| Health "Running" emission             | Framework emits at construction               |
| SSH-namespaced transient errors       | `Transient.namespace = StatusNamespace::Ssh`  |

### What disappears from MySQL connector code

- `mysql.rs::render` (~225 LOC of dataflow assembly).
- `mysql/statistics.rs` whole module (171 LOC) — collapses into `probe()`.
- `RewindRequest` broadcast feedback edge plumbing.
- Per-export error collection partitioning at end-of-render.
- Health namespace routing.
- Definite-error fan-out via `return_definite_error`.
- `responsible_for(table_name)` dispatch glue.

What stays:
- `mysql/snapshot.rs` per-table snapshot logic (lock dance, COPY, decode).
- `mysql/replication.rs::context.rs / events.rs / partitions.rs` — protocol parsing.
- `mysql/schemas.rs` schema verification.
- `validate_mysql_repl_settings`.
- All `DefiniteError` / `TransientError` variants.

Estimated MySQL LOC after refactor: ~700 (today's main is ~2000).

### Findings for PG (revisions to prior entry)

Triggered by Refinements 1–3 from MySQL:

#### PG-A: `SnapshotChunk { stream, upper }` instead of `SnapshotPlan { upper }`

PG's `Plan { snapshot_id, snapshot_lsn }` already carries the upper; with the
refinement, every `snapshot_partition` returns the same `snapshot_lsn`. Trivial change.

#### PG-B: Per-export `initial_upper`

The prototype's `compute_resume_lsn` uses the slot's `confirmed_flush_lsn` as the
rewind lower bound for *all* snapshotting exports. This is correct for the cold-start
case (only one t_slot in play). For the late-added-subsource case (export added via
`ALTER SOURCE ... ADD SUBSOURCE` after the slot has advanced), each new subsource needs
its own `initial_upper`.

**Action**: thread `initial_upper` per `ExportRequest` from the storage controller.
Today's main reads this from `SourceExportDetails::Postgres` (slot LSN at export
creation), the prototype dropped this — restore it.

#### PG-C: `StreamStartError::Definite` for slot-invalidation

The prototype's `DefiniteError` enum already includes `SlotCompacted` and
`SlotInvalidated`. Today's prototype doesn't surface them as definite errors that
poison the source — they get wrapped in `TransientError`. Use `StreamStartError::Definite`
in the `stream()` method.

### Now-confirmed across two connectors

The trait holds. The three refinements (per-partition `SnapshotChunk.upper`, per-export
`initial_upper`, `StreamStartError`) are minor mechanical changes, not structural. The
framework's responsibilities (probe loop, snapshot orchestration, rewind coordinator,
frontier batching, backpressure feedback, error routing) are the same shared layer
identified earlier.

Estimated total LOC reduction across PG + MySQL after refactor: from ~4700 (today's
main) to ~1300–1400 — roughly 70%. Most savings come from eliminating the per-connector
dataflow-assembly + channel-plumbing + error-routing boilerplate.

### Implementation gotchas surfaced by MySQL validation

1. **Per-worker independent snapshot consistency points** mean the framework's
   `RewindCoordinator` must subtract per-`(export_id, partition)` ranges, not one
   global range. The data structure is `BTreeMap<GlobalId, BTreeMap<PartitionId,
   (initial_upper, snapshot_upper)>>` rather than a single `(t_slot, t_snapshot)` pair.
2. **MySQL holds two connections per worker during snapshot**: the lock-holding
   connection and the data-reading connection. The trait's `snapshot_partition` must
   return after the lock is released but the data connection's transaction is open and
   the stream is still being consumed. Today's `mysql/snapshot.rs` does exactly this.
   The trait accommodates it: `MySqlSnapshotStream` owns the data connection.
3. **`initial_gtid_set` in `SourceExportDetails::MySql`** is a `String`; needs decoding
   to `Antichain<GtidPartition>` per-call. Cache or eager-decode.
4. **`validate_mysql_repl_settings`** is per-connection. It runs on the replication
   connection at `stream` startup. Different from PG which has analogous checks at
   purification time. No trait change; just place it in `stream`.

### Verdict

Both connectors validate. The trait, with the three refinements, supports:

- PG (single consistency point, leader-broadcasts plan).
- MySQL (per-worker consistency points, no leader broadcast).

Two more connectors remain for full validation: SQL Server (similar to PG/MySQL CDC
pattern) and the load generator's Simple variant. Expectation: SS will validate without
further refinements; `LoadGenerator/Simple` is small enough that the trait will be
overkill but should fit. Defer those validations to implementation time.

Recommend proceeding to implementation: refactor PG into the trait first, then MySQL,
treating the trait as established by these two validations.
