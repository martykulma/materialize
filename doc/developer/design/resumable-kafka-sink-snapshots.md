# Resumable Kafka Sink Snapshots

## Problem Statement

The Kafka sink performs a snapshot in a single Kafka transaction. Some Kafka
vendors (e.g., Confluent Cloud) impose a maximum transaction duration of 15
minutes. For large snapshots that exceed this limit, the transaction is aborted
and the sink cannot make progress.

We need to split the snapshot into multiple transactions while maintaining
exactly-once delivery semantics without requiring any consumer-side changes.

## Background

### Current Snapshot Transaction Flow

The Kafka sink (`src/storage/src/sink/kafka.rs`) processes snapshot data in a
single transaction:

1. Data arrives from a persist shard via `persist_source`, all at the `as_of`
   timestamp.
2. A transaction is opened when the first message arrives at `time == upper`
   (line 738).
3. Messages are sent to Kafka within this open transaction.
4. The transaction is committed only when a `Progress` event advances the
   frontier past the snapshot timestamp (line 796), writing a `ProgressRecord`
   to the progress topic.

If the snapshot contains millions of messages, this single transaction can
remain open for well over 15 minutes.

### Exactly-Once Guarantees Today

The sink achieves exactly-once delivery through:

- **Transactional producer** with a deterministic `transactional.id` derived
  from the sink ID.
- **`init_transactions()`** on startup, which fences out any previous producer
  instance.
- **Progress records** in a dedicated progress topic, committed atomically
  within each data transaction. The progress record contains the frontier
  (upper) that was committed.
- **Resume on restart**: the sink reads the last committed progress record to
  determine where to resume.

Consumers using `read_committed` isolation see only committed transactions,
so partial writes from aborted transactions are invisible.

### Why Simple Transaction Splitting Breaks Exactly-Once

Naively committing transactions every N messages during the snapshot breaks
exactly-once because on crash recovery we have no way to know which messages
were already committed. Re-reading the snapshot from persist produces messages
in a non-deterministic order (due to random worker assignment in
`shard_source.rs` and parallel fetch scheduling), so we cannot use a simple
row-count offset as a resume cursor.

## Design

### Current Snapshot Data Path

Today, the sink reads from persist via `persist_source` (`shard_source.rs`),
which constructs a `Subscribe` — a snapshot followed by a listen:

1. **`read.snapshot(as_of)`** returns `Vec<LeasedBatchPart<T>>` — a set of
   parts (pointers to S3 blobs) from the shard's trace spine. These parts
   contain all data at timestamps `<= as_of`, with timestamps advanced to
   `as_of`. The snapshot is the full state of the collection: every
   `(key, value, time, diff)` tuple that hasn't been consolidated away.

2. **`read.listen(as_of)`** returns a `Listen` handle for updates after
   `as_of`.

3. **Merging**: The snapshot parts and first listen batch are combined into
   a single stream (`listen_head.chain(listen_tail)`).

4. **Part distribution**: Each part is assigned to a **random worker** for
   fetching (`Instant::now().hashed() % num_workers`). All snapshot parts
   carry the same timestamp (`as_of`).

5. **What the sink sees**: After fetching, decoding, arranging, and
   consolidating (`zip_into_diff_pairs`), the sink operator receives
   `(KafkaMessage, Timestamp, Diff)` tuples. All snapshot data arrives at
   `time == as_of`. The frontier stays at `as_of` until all snapshot parts
   are processed, then a `Progress` event advances past `as_of`.

The snapshot is **not** consolidated or sorted at the persist read level.
It is a bag of parts distributed randomly across workers, fetched in
parallel, and routed to the single sink worker in non-deterministic order.
A given key may appear in multiple parts with different diffs that only
cancel out after the downstream `arrange` + `consolidate` operators.

### Why the Current Path Cannot Support Resumable Snapshots

**Non-deterministic ordering**: The random part distribution and parallel
fetch ordering mean that two reads of the same snapshot at the same
`as_of` can deliver rows to the sink worker in a completely different
order. A position-based cursor (e.g., "resume from row N") is
meaningless when row N is different data on each restart.

**Unbounded memory**: The current path feeds into an arrangement
(`ColValSpine`) via `zip_into_diff_pairs` (`sinks.rs:142`), which
accumulates the **entire snapshot in memory** — every row, fully
decoded, arranged by key. For a large snapshot this can be many GiBs.
The arrangement is then iterated by `combine_at_timestamp` which
processes batches from the spine independently, producing
`DiffPair`-grouped output. While each `OrdValBatch` within the spine is
key-sorted, the spine may contain multiple overlapping batches (due to
lazy merging), and the batch structure depends on data arrival order. So
the arrangement provides neither deterministic output ordering nor
bounded memory.

**`snapshot_cursor()` addresses both problems**: The persist
`Consolidator` streams through the snapshot data with a configurable
memory budget (`COMPACTION_MEMORY_BOUND_BYTES`, default 1 GiB). It
only holds the merge-sort frontier across runs — the head of each run
— fetching subsequent parts on demand as prior parts are consumed. This
is fundamentally more memory-efficient than the arrangement for large
snapshots, and the sorted-merge output is deterministic regardless of
physical batch layout.

### Core Insight: Replace the Snapshot Path with `snapshot_cursor()`

The persist `Consolidator` (used by `snapshot_cursor()`) produces output
that is **deterministic across restarts**:

1. **Sorted merge**: it merge-sorts across all spine runs, producing data
   in `(K, V, T, D)` order — a total order independent of physical batch
   layout.
2. **Consolidated**: duplicate entries are summed, so the output set is
   canonical.
3. **Compaction-resistant**: even if compaction reshapes batches between
   restarts, the sorted-consolidated output is the same logical data in
   the same order.
4. **Has a built-in resume mechanism**: `LowerBound<T>` (`iter.rs:375`)
   supports skipping all KVTs at or below a given bound.

The design replaces the sink's snapshot data path — the
`read.snapshot()` → random worker distribution → parallel fetch pipeline
— with a `snapshot_cursor()` read on the sink worker. The listen path
(`read.listen()`) for post-snapshot updates remains unchanged.

### Architecture

During the snapshot phase, the dataflow is restructured:

```
                    ┌─────────────────────┐
                    │   persist shard      │
                    └──────────┬──────────┘
                               │
                    ┌──────────▼──────────┐
                    │  snapshot_cursor()   │  single worker, deterministic
                    │  with lower_bound   │  sorted + consolidated output
                    │                     │  emits fixed-size chunks
                    └──────────┬──────────┘
                               │ chunk of N rows
          ┌────────────────────▼────────────────────┐
          │         encode (multi-worker)            │
          │  Pipeline exchange, parallel encoding    │
          └────────────────────┬────────────────────┘
                               │
          ┌────────────────────▼────────────────────┐
          │       kafka sink (single worker)         │
          │  commits one chunk per sub-transaction   │
          │  progress record includes lower_bound    │
          └─────────────┬──────────┬────────────────┘
                        │          │
                   data topic   progress topic
```

After the snapshot completes, the dataflow switches to the normal
`persist_source` listen path for ongoing updates:

```
          ┌─────────────────────┐
          │   persist shard     │
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  read.listen()      │  normal persist_source
          │  (via Subscribe)    │  random part distribution
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  encode (multi)     │  existing pipeline
          └──────────┬──────────┘
                     │
          ┌──────────▼──────────┐
          │  kafka sink         │  single transaction per
          │  (single worker)    │  frontier advancement
          └──────────┬──────────┘
```

The determinism requirement applies to **sub-transaction boundaries**, not
to message ordering within a sub-transaction. Each sub-transaction
corresponds to exactly one `Consolidator` chunk:

1. The `Consolidator` on the sink worker produces a fixed-size chunk of
   N rows via `next_chunk()`. The last `(K, V, T)` in the chunk defines
   the `LowerBound` for the progress record.
2. The chunk is distributed to multiple workers for parallel encoding via
   the normal `Pipeline` exchange.
3. Encoded messages arrive at the single sink worker in
   scheduling-dependent order.
4. The sink worker sends all messages within one Kafka transaction and
   commits with the `LowerBound` from step 1.

Since Kafka transactions are atomic (all messages are visible or none
are), the order of messages *within* a sub-transaction is irrelevant to
consumers. What matters is that the boundary between sub-transactions is
deterministic — and that boundary is defined by the `Consolidator`'s
sorted output, not by encoding or scheduling order.

This preserves the existing multi-worker encoding pipeline with no
throughput penalty during snapshot.

### Transaction Protocol

#### During Snapshot

1. Open `snapshot_cursor(as_of)` on the sink worker. If resuming from a
   previous crash, set `lower_bound` to the cursor from the last committed
   progress record; otherwise `None`.

2. Begin a Kafka transaction.

3. Send messages until one of:
   - `N` messages have been sent (configurable batch size), or
   - `T` seconds have elapsed (must be well under the transaction timeout,
     e.g., 5 minutes for a 15-minute limit).

4. Commit the transaction with a **snapshot progress record**:
   ```
   ProgressRecord {
       frontier: <as_of>,              // NOT advanced
       version: <sink_version>,
       snapshot_cursor: <last_kvt>,    // last (K, V, T) committed
       snapshot_complete: false,
   }
   ```

5. Repeat from step 2 until all snapshot data is emitted.

6. Commit a final transaction with:
   ```
   ProgressRecord {
       frontier: <next_frontier>,      // advance past as_of
       version: <sink_version>,
       snapshot_cursor: None,
       snapshot_complete: true,
   }
   ```

#### On Crash Recovery

1. Call `init_transactions()` to fence out the previous producer.

2. Read the last committed `ProgressRecord` from the progress topic.

3. If `snapshot_complete == false` and `snapshot_cursor` is `Some`:
   - Re-open `snapshot_cursor(as_of)` with `lower_bound` set to the
     persisted cursor value.
   - The `Consolidator` skips all KVTs at or below the bound.
   - Resume sending from the first row after the bound.

4. If `snapshot_complete == true` (or no snapshot fields present):
   - Resume from `frontier` as today (no snapshot in progress).

### Progress Record Schema

```rust
pub struct ProgressRecord {
    #[serde(
        deserialize_with = "deserialize_frontier",
        serialize_with = "serialize_frontier"
    )]
    pub frontier: Antichain<Timestamp>,
    #[serde(default)]
    pub version: u64,
    /// When a snapshot is in progress, the last (K, V, T) triple that was
    /// committed. Used to resume the snapshot after a crash by setting
    /// the Consolidator's lower_bound.
    #[serde(default)]
    pub snapshot_cursor: Option<SerializedLowerBound>,
    /// Whether the snapshot has been fully committed. When `false` with a
    /// `Some` snapshot_cursor, the snapshot is in progress. When `true`
    /// or absent, normal frontier-based resume applies.
    #[serde(default)]
    pub snapshot_complete: Option<bool>,
}
```

The `#[serde(default)]` annotations ensure backward compatibility: old
progress records without these fields deserialize with `None`/`None`, which
is interpreted as "no snapshot in progress" (matching current behavior).

Note that the `as_of` is **not** stored in the progress record. It is
owned by the storage controller, persisted in the export state, and
passed to the sink via `RunSinkCommand`. See "as_of Ownership" below.

`SerializedLowerBound` is a serializable representation of the last committed
`(K, V, T)` triple, sufficient to reconstruct a `LowerBound<T>` for the
`Consolidator`. The exact serialization format should use the same columnar
encoding used by persist, or a simpler key/value byte representation, to
ensure stability across restarts.

### Exactly-Once Argument

1. **No duplicates**: each sub-transaction commits a contiguous range of the
   sorted-consolidated output. The `lower_bound` on resume skips all
   previously committed rows. Since the sort order is deterministic and
   `lower_bound` comparison is exclusive, no row appears in two transactions.

2. **No missing data**: the `Consolidator` with a `lower_bound` produces all
   rows strictly greater than the bound. Combined with the final
   `snapshot_complete: true` commit, every row is covered.

3. **Crash safety**: `init_transactions()` aborts any in-flight transaction
   from the crashed instance. The last committed `snapshot_cursor` is the
   exact resume point. The fenced-out producer cannot commit further.

4. **Consumer transparency**: consumers with `read_committed` isolation see
   each committed sub-transaction immediately. All snapshot data is at the
   same logical timestamp, so incremental visibility is semantically
   identical to seeing it all at once. The progress topic frontier does not
   advance until the snapshot completes, so progress-aware consumers
   correctly understand the snapshot is in progress.

### Components to Change

| Component | Change |
|-----------|--------|
| `ProgressRecord` (`kafka.rs:1247`) | Add `snapshot_cursor` and `snapshot_complete` fields |
| `sink_collection()` (`kafka.rs:716`) | Split snapshot into sub-transactions with periodic commits |
| `determine_sink_progress()` (`kafka.rs:900`) | Parse new progress fields, return cursor for resume |
| Sink dataflow (snapshot path) | Use `snapshot_cursor()` with `LowerBound` instead of `persist_source` for snapshot data |
| `LowerBound` (`iter.rs:375`) | Add `Serialize`/`Deserialize` (or create a serializable equivalent) |
| Export state / catalog | Persist the `as_of` once at sink creation; use it immutably on all subsequent restarts |
| `run_export()` (`storage_collections.rs`) | Use persisted `as_of` (not recomputed); register/re-acquire critical since hold at `as_of` |
| `RunSinkCommand` | Pass the persisted `as_of` and `CriticalReaderId` to the sink |
| Sink deletion path | Downgrade critical since hold to empty antichain on sink drop |
| Snapshot completion signal | Sink signals controller to downgrade critical since hold after snapshot completes |

### Configuration

Two new sink configuration parameters:

- **`snapshot_transaction_batch_size`**: maximum number of messages per
  sub-transaction during snapshot (default: e.g., 1,000,000).
- **`snapshot_transaction_timeout`**: maximum wall-clock duration of a
  sub-transaction during snapshot (default: e.g., 5 minutes). Must be less
  than the Kafka broker's `transaction.max.timeout.ms`.

These can be exposed as `WITH` options on `CREATE SINK` or as system
variables.

## Edge Cases

### Compaction and Consolidator Determinism

#### The Consolidator Output Is Deterministic Despite Compaction

The `Consolidator` (`iter.rs`) performs a streaming merge-sort across runs
using a `BinaryHeap<PartRef>`. Each `PartRef` tracks a cursor into a
fetched part. The heap pops the smallest `(KV, T)` tuple across all runs,
and the `consolidate()` method (line 944) sums diffs for equal `(KV, T)`
tuples.

The output order is determined entirely by the `(KV, T)` comparison, not
by the physical run/part structure. The heap guarantees that the globally
smallest unconsumed tuple is always emitted next, regardless of how many
runs exist or how they are organized. This means:

- **Before compaction**: runs `[A, B, C]` feed into the heap. Merge-sort
  produces output in `(KV, T)` order.
- **After compaction** merges `A+B -> D`: runs `[D, C]` feed into the
  heap. `D` contains the same data as `A` union `B` (already consolidated).
  Merge-sort produces the same `(KV, T)` output.

The `unblock_progress()` method (line 559) sorts runs by their
`kvt_lower()` before each chunk, but this only affects fetch priority --
the heap-based merge ensures the same emission order regardless.

#### Chunk Boundaries May Differ

While the output *order* is deterministic, the *chunk boundaries* from
`next_chunk()` may differ after compaction. The `Consolidator` uses an
`upper_bound` (line 543-545) derived from unfetched parts to avoid
emitting data that might need further consolidation. Different run
structures can change where these boundaries fall.

**This is safe for our design** because the `LowerBound` cursor tracks
the last emitted `(KV, T)` tuple, not a chunk index. On resume, the
`Consolidator` skips all tuples `<= lower_bound` (line 984-988) and
emits from the next tuple, regardless of chunk boundaries.

#### Non-One-to-One Encoding

`snapshot_and_fetch()` contains a comment (line 1154): *"We don't
currently guarantee that encoding is one-to-one."* This means the same
logical `(K, V)` could theoretically have multiple encoded
representations.

The `Consolidator` operates on **encoded data** (`ArrayOrd`/`ArrayIdx`),
not decoded Rust types. Its sort order is over encoded bytes. This means:

- Within a single `snapshot_cursor()` call, the encoding is consistent
  (same blob data produces same encoded bytes).
- Compaction re-encodes data using the same codec, so the encoded
  representation of a given logical value is stable across compaction.
- The `LowerBound` is also based on encoded data, so it correctly
  identifies the resume point after compaction.

If encoding were non-deterministic (e.g., HashMap field ordering), two
encodings of the same logical value would be treated as different values
in the sort. This would not break correctness (both would be emitted) but
could cause a `LowerBound` to miss its target on resume. In practice,
persist's columnar encoding is deterministic, so this is not a concern
today. If this assumption were to change, the `LowerBound` would need to
be based on decoded values instead.

### Lease Management

#### Background: Leased Readers vs. Critical Since Handles

Persist provides two mechanisms for holding back compaction:

**Leased readers** (`LeasedReaderId`): Time-limited, with a default TTL
of 15 minutes (`READER_LEASE_DURATION`). The `ReadHandle` heartbeats
every ~3.75 minutes to refresh the lease. If the heartbeat stops (process
crash, async runtime starvation), the lease expires and compaction can
advance `since` past the reader's position. Leases are not durable —
they are lost on process restart.

**Critical since handles** (`CriticalReaderId` / `SinceHandle`): Durable
across process restarts, with no TTL. Must be explicitly downgraded via
`compare_and_downgrade_since()`. If lost without cleanup, the shard's
`since` is stuck forever. In production, critical since handles are only
installed from controllers (`storage_collections.rs`, `catalog`,
`txn-wal`), never from dataflows.

#### Design: Controller-Managed Critical Since Hold

To protect the snapshot's `as_of` from compaction during long-running
resumable snapshots, the **storage controller** manages a critical since
hold on behalf of the sink. This is consistent with the existing pattern
where all critical since handles are controller-managed.

**Lifecycle:**

1. **At sink creation**: The controller computes the `as_of` (from
   `least_valid_read()` as today), persists it in the export state,
   and registers a critical since hold at that `as_of`:

   ```rust
   let critical_reader_id = sink_id.critical_reader_id();
   let since_handle = persist_client
       .open_critical_since(
           source_shard,
           critical_reader_id,
           Opaque::encode(&sink_version),
           diagnostics,
       )
       .await;
   since_handle
       .compare_and_downgrade_since(&opaque, (&opaque, &as_of))
       .await;
   ```

   The `CriticalReaderId` is derived deterministically from the sink ID,
   ensuring the same handle is re-acquired on restart.

2. **On restart**: `run_export()` reads the **persisted `as_of`** from
   export state (not recomputed) and re-acquires the critical since
   hold at the same value. The `RunSinkCommand` carries this persisted
   `as_of`. The sink does not interact with the critical handle
   directly — it only reads from persist via its normal `ReadHandle`.

3. **On snapshot completion**: The sink signals the controller (e.g.,
   via the persist write frontier or a status update) that the snapshot
   is complete. The controller then downgrades the critical since hold
   to the current frontier, allowing compaction to proceed.

4. **On sink deletion**: The controller downgrades the critical since
   hold to the empty antichain, releasing it entirely. Because the
   `CriticalReaderId` is deterministic, the controller can always
   locate and clean up the handle even after crashes.

**Fencing via opaque token**: The `SinceHandle` uses an opaque
compare-and-set token. The controller encodes the sink version (or
persist epoch) into this token, ensuring that a recreated sink (with a
new version) can fence out a stale critical since hold from a previous
incarnation.

#### Edge Case: Lease Expiration During Long Snapshots

**Scenario**: A resumable snapshot takes 2 hours. The sink commits
sub-transactions every 5 minutes. Between sub-transactions, does the
leased reader remain valid?

**Analysis**: The `ReadHandle`'s heartbeat task runs independently of
the `Cursor`. As long as the `ReadHandle` is alive and its async
runtime is functioning, the heartbeat refreshes the lease. The `Cursor`
holds a `Lease` (`Arc<SeqNo>`) that prevents GC of the specific
`SeqNo` it was created at.

**Risk**: If the sink worker's async runtime is starved (e.g., a very
long Kafka commit blocks the executor), the heartbeat may not fire
within the 15-minute window. If the leased reader expires:

1. The `SeqNo` lease is lost — blobs at that `SeqNo` may be GC'd.
2. Subsequent `next_chunk()` calls on the `Cursor` may fail because
   blobs have been deleted.

**However**, the controller's critical since hold guarantees the
shard's `since` does not advance past the `as_of`. This means:

- The **logical data** at `as_of` remains readable even if the `SeqNo`
  lease expires — the data exists in compacted form at a newer `SeqNo`.
- If the current `Cursor` fails due to blob GC, the sink can create a
  new `snapshot_cursor(as_of)` with `lower_bound` set to the last
  committed `(KV, T)` and resume.
- This is the same recovery path as crash recovery, just triggered
  mid-process instead of on restart.

The critical since hold thus provides a safety net: the leased reader
is the fast path (avoids re-reading compacted data), while the critical
since hold is the backstop (guarantees recovery is always possible).

#### Edge Case: Process Down > 15 Minutes

**Scenario**: The sink crashes mid-snapshot. The process stays down for
\>15 minutes. The leased reader expires.

**Without critical since hold**: Compaction advances `since` past
`as_of`. On restart, `snapshot_cursor(as_of)` returns
`Err(Since(...))`. The snapshot cannot be resumed. Committed
sub-transactions are already visible to consumers. Exactly-once is
broken.

**With controller-managed critical since hold**: The `since` cannot
advance past `as_of` regardless of downtime. On restart, the
controller re-acquires the same `CriticalReaderId` (deterministic from
sink ID), reads the persisted `as_of` from export state, confirms the
since hold is still active, and starts the sink with the same `as_of`.
The sink reads the `snapshot_cursor` from the progress topic and
resumes via `lower_bound`.

#### Edge Case: Stuck Critical Since After Sink Deletion

**Scenario**: A sink is deleted while a snapshot is in progress. The
controller must clean up the critical since hold.

**Mitigation**: The controller's sink deletion path must explicitly
downgrade the critical since hold:

```rust
let critical_reader_id = sink_id.critical_reader_id();
let mut since_handle = persist_client
    .open_critical_since(source_shard, critical_reader_id, ...)
    .await;
since_handle
    .compare_and_downgrade_since(&our_opaque, (&our_opaque, &Antichain::new()))
    .await;
```

The deterministic `CriticalReaderId` derivation ensures this cleanup
is always possible. The opaque token fencing ensures that only the
current controller epoch can perform the downgrade.

As an additional safeguard, the shard finalization path
(`storage_collections.rs:3144`) already opens and downgrades critical
since handles during cleanup. The sink's critical reader ID should be
included in this path.

#### Edge Case: Cursor Across Sub-Transactions

**Scenario**: Should the sink drop and recreate the `Cursor` between
sub-transactions, or keep a single cursor open?

**Recommendation**: Keep a single `Cursor` open for the full snapshot
duration. Only use `lower_bound` on crash recovery when creating a
fresh cursor. This avoids:

- The cost of re-doing `snapshot_batches()` between sub-transactions.
- A brief window where no `SeqNo` lease exists (between cursor drop
  and recreation).
- Potential differences in spine structure if compaction runs between
  sub-transactions (irrelevant for correctness, but avoidable work).

### `as_of` Ownership and Immutability

#### Background: Why `as_of` Regression Is Dangerous

Today, the sink's `as_of` is recomputed on each restart by the storage
controller via `run_export()`, which joins the initial `as_of` from the
catalog with the source collection's current `implied_capability`. This
means the `as_of` can legitimately regress (become earlier) on restart.
The current code (lines 752-771 of `kafka.rs`) handles this by refusing
to commit transactions until the frontier advances strictly past the
`as_of`, preventing an empty progress record from causing snapshot data
to be skipped.

With resumable snapshots, `as_of` regression is a **correctness
violation**, not merely an inconvenience. Once the first sub-transaction
commits, consumers with `read_committed` isolation have seen data from
that specific point-in-time snapshot. The committed data cannot be
retracted. If the `as_of` changes on restart:

- The snapshot at a different `as_of` represents a **different logical
  dataset**.
- Completing the snapshot at a different `as_of` would mix rows from
  two different points in time.
- This breaks exactly-once semantics: rows present at one `as_of` but
  not the other will be duplicated or missing.

Discarding partial progress and restarting the snapshot is also not an
option — the already-committed sub-transactions are visible to consumers
and cannot be undone.

#### Solution: Controller Owns the `as_of`

Since the controller already manages the critical since hold for the
snapshot, it is the natural owner of the `as_of` as well. The two are
tightly coupled — the critical since hold protects exactly the `as_of`
timestamp — so having them managed by the same component avoids
coordination issues between the progress topic and controller state.

**Design:**

1. **At sink creation**: The controller computes the `as_of` (from
   `least_valid_read()` as today) and **persists it in the export
   state** (e.g., in the `StorageSinkDesc` stored in the catalog).

2. **On restart**: `run_export()` uses the **persisted `as_of`**
   directly, rather than recomputing it by joining with
   `implied_capability`. The `as_of` is immutable for the lifetime of
   a given sink version.

3. **Passed to the sink**: The `RunSinkCommand` carries the persisted
   `as_of`. The sink uses this value unconditionally for
   `snapshot_cursor()`.

4. **Critical since hold at the same `as_of`**: Before starting the
   sink, the controller registers (or re-acquires) the critical since
   hold at this same persisted `as_of`. Because both the `as_of` and
   the critical since hold are derived from the same persisted value,
   they are guaranteed to be consistent.

5. **After snapshot completion**: The controller downgrades the critical
   since hold to the current frontier. The persisted `as_of` becomes
   irrelevant (the frontier has advanced past it), but it can be
   retained for diagnostics.

**This eliminates `as_of` regression entirely.** The sink always
receives the same `as_of` on every restart, and the critical since
hold guarantees the data at that timestamp remains readable. There is
no need to store `as_of` in the progress topic — it is not the
progress topic's concern.

**Recovery protocol:**

1. Controller reads the persisted `as_of` from export state.
2. Controller re-acquires the critical since hold at the persisted
   `as_of`.
3. Controller sends `RunSinkCommand` with the persisted `as_of`.
4. Sink reads the `ProgressRecord` from the progress topic:
   - If `snapshot_complete == false` and `snapshot_cursor` is `Some`:
     open `snapshot_cursor(as_of)` with `lower_bound` and resume.
   - If `snapshot_complete == true` or no snapshot fields: resume from
     `frontier` as today.

The `as_of` flows from one source (controller export state) and the
cursor flows from one source (progress topic). No cross-referencing
or conflict resolution is needed.

#### Interaction with Existing `as_of` Logic

The existing `as_of` safety logic in `kafka.rs` (lines 752-771) that
suppresses progress commits until the frontier passes `as_of` remains
useful: it prevents the sink from committing empty transactions during
the initial frontier jump to `as_of`. With resumable snapshots, this
logic should be retained — sub-transaction commits only begin once
actual snapshot data is being emitted, which inherently occurs after
the frontier reaches `as_of`.

### Interaction with Sink Versioning

#### Edge Case: Sink Version Bump During Snapshot

The current sink uses `version` in `ProgressRecord` for fencing (line
380-388 of `kafka.rs`). If a sink is dropped and recreated (bumping the
version) while a snapshot is in progress, the new version must not
resume the old version's partial snapshot.

**Already handled**: The `version` field in `ProgressRecord` provides
fencing. The new sink version will see `progress.version < sink_version`,
meaning the old progress record is from a previous incarnation. The new
sink ignores it and starts fresh.

### Summary of Required Additions to Progress Record

```rust
pub struct ProgressRecord {
    pub frontier: Antichain<Timestamp>,
    #[serde(default)]
    pub version: u64,
    /// The last (K, V, T) committed during the snapshot.
    /// Used to set Consolidator lower_bound on resume.
    #[serde(default)]
    pub snapshot_cursor: Option<SerializedLowerBound>,
    /// Whether the snapshot has been fully committed.
    #[serde(default)]
    pub snapshot_complete: Option<bool>,
}
```

Note: The `as_of` is not in the progress record. It is persisted in the
controller's export state and passed to the sink via `RunSinkCommand`.

### Summary of Recommendations

| Edge Case | Recommendation |
|-----------|---------------|
| Compaction changing spine structure | Safe -- `Consolidator` output order is deterministic over encoded `(KV, T)` regardless of run structure |
| Non-one-to-one encoding | Safe today -- persist columnar encoding is deterministic. Monitor if this changes. |
| Lease expiration during long snapshot | Controller-managed `CriticalReaderId` holds `since` at `as_of`; leased reader expiry is recoverable via new `snapshot_cursor` with `lower_bound` |
| Process down >15 min, `since` advances | Controller-managed `CriticalReaderId` prevents `since` from advancing past `as_of` regardless of downtime |
| Stuck critical since after sink deletion | Sink deletion path must downgrade the critical since hold; deterministic `CriticalReaderId` derivation from sink ID ensures cleanup is always possible |
| Cursor recreation between sub-transactions | Keep a single cursor open; only use `lower_bound` on crash recovery |
| `as_of` regression on restart | Eliminated: controller persists `as_of` at creation, uses it immutably on all restarts. Critical since hold protects the same value. |
| Sink version bump during snapshot | Already handled by existing `version` fencing |
