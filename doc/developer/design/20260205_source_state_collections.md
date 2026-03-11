# Source State Collections

- Associated: `maz-state-collection` branch

## The Problem

Materialize sources need a mechanism to durably persist source-specific
operational state that is both:

1. **Read back by the ingestion pipeline on restart** for correctness, and
2. **Queryable by users** for diagnostics.

Today, two concrete needs motivate this for the PostgreSQL source:

- **Persisted definite errors:** When the PostgreSQL source encounters a
  definite (non-retractable) error, it needs to persist that error so it can be
  re-emitted at the same logical timestamp after a restart. Without durable
  state, the source cannot guarantee timestamp-consistent error reporting across
  process boundaries.

- **Timeline history for HA failover:** When a PostgreSQL upstream fails over to
  a standby, the replication timeline changes. The source needs to persist
  timeline history information so that on reconnect it can determine whether it
  is safe to continue replicating or whether the new timeline is incompatible
  with the previously ingested data.

These two concerns are semantically distinct: errors and timeline history have
different schemas, different update frequencies, different compaction needs, and
different consumers within the dataflow. They should be modeled as **separate
state collections**, not columns in a single shared collection.


The remap/progress collection (`remap_collection_id`) is close in spirit. It is a persist-backed collection that the ingestion pipeline reads and writes, with a fixed schema (`timestamp_desc()`) dedicated to reclocking.

## Success Criteria

1. Each source type can declare **zero or more** state collections, each with
   its own schema and semantic purpose.
2. State collections are durable persist-backed shards that survive process
   restarts.
3. The ingestion pipeline can write to and read from each state collection
   independently as part of its normal operation.
4. Users can query each state collection via SQL for diagnostics (e.g.,
   `SELECT * FROM my_pg_source_errors`,
   `SELECT * FROM my_pg_source_timeline_history`).
5. State collections follow the existing subsource lifecycle: they are
   auto-created with the source, participate in `DROP CASCADE`, inherit
   compaction windows, and appear in system catalog tables. (Technically,
   different state collections for the same source could have independent
   compaction policies appropriate to their data characteristics.)
6. Users cannot directly create or modify state collections, they are
   managed internally.

## Out of Scope

- **Defining state collections for every source type.** This design covers the
  infrastructure. Individual source types (MySQL, SQL Server, Kafka, Load
  Generator) will define their state collections in follow-up work as needed.
  The PostgreSQL source is the initial consumer.
- **Writing actual state data in the storage dataflow.** Wiring the PostgreSQL
  source operators to produce definite errors and timeline history into their
  respective state collections is implementation work that builds on this
  infrastructure.
- **Migrating existing state.** Sources created before this feature will not
  retroactively gain state collections. A migration path may be designed
  separately.

## Solution Proposal

### Overview

Introduce state collections as a new kind of auto-generated subsource, following
the same pattern as progress subsources. Each state collection is a
persist-backed collection with its own schema, created automatically during
`CREATE SOURCE` purification, tracked in the catalog as `type = 'state'`, and
queryable by users via SQL.

A single source may have **multiple** state collections. For the PostgreSQL
source, the initial implementation creates two:

1. **Errors collection** - persists definite errors with their timestamps for
   consistent re-emission on restart.
2. **Timeline history collection** - persists PostgreSQL timeline history
   records for HA failover safety checks.

Each state collection is a separate persist shard, a separate catalog entry,
and a separate SQL-queryable relation.

### SQL Syntax

State collections are auto-generated during `CREATE SOURCE`. Users do not
explicitly name them in the `CREATE SOURCE` statement. Naming is
auto-generated based on the source name and the state collection's purpose:

```sql
CREATE SOURCE my_pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'pub');
-- Implicitly creates (for PostgreSQL sources):
--   my_pg_source_errors         (state collection: definite errors)
--   my_pg_source_timeline_history (state collection: timeline history)
```

Users query state via:

```sql
SELECT * FROM my_pg_source_errors;
SELECT * FROM my_pg_source_timeline_history;
```

Direct execution of `CREATE STATE` is blocked, just as `CREATE SUBSOURCE` is
blocked for direct user execution.

### Architecture

The design mirrors the existing progress subsource architecture at every layer,
generalized from a single optional state collection to a list of state
collections:

```
                    Progress Subsource          State Collections
                    ──────────────────          ─────────────────
Purification        create_progress_subsource   create_state_stmts (Vec<CreateStateStatement>)
Plan variant        DataSourceDesc::Progress    DataSourceDesc::State { key }
Catalog variant     DataSourceDesc::Progress    DataSourceDesc::State { key }
Storage variant     DataSource::Progress        DataSource::State
IngestionDesc       remap_collection_id         state_collections (BTreeMap<StateCollectionId, ...>)
Schema provider     timestamp_desc()            StateCollectionKey::desc()
Identity            (single GlobalId)           StateCollectionId (type-erased from StateCollectionKey)
```

### Layer-by-Layer Design

#### 1. Source Type Schema Declaration

Each source type declares its state collections using a **source-specific enum**
that implements a common trait. This avoids positional `Vec` indexing (where
reordering silently breaks the mapping between descriptions and GlobalIds) and
avoids stringly-typed lookups (where a typo in a key like `"errors"` silently
returns `None` at runtime).

Define a `StateCollectionKey` trait in `src/storage-types/src/sources.rs`:

```rust
/// A typed key for source-specific state collections.
///
/// Each source type that needs persistent state collections defines an enum
/// implementing this trait. Generic infrastructure uses the type-erased
/// [`StateCollectionId`] / [`RelationDesc`] pairs returned by
/// [`StateCollectionKey::all_descs`].
pub trait StateCollectionKey: Debug + Clone + Eq + Hash + 'static {
    fn name_suffix(&self) -> &'static str;
    fn desc(&self) -> RelationDesc;
    fn all() -> Vec<Self>;

    fn id(&self) -> StateCollectionId {
        StateCollectionId(self.name_suffix().to_string())
    }

    fn all_descs() -> Vec<(StateCollectionId, RelationDesc)> {
        Self::all()
            .into_iter()
            .map(|k| (k.id(), k.desc()))
            .collect()
    }
}
```

Rather than an associated type on `SourceConnection`, the trait exposes state
collections via a default method that returns `Vec<(StateCollectionId,
RelationDesc)>`. This keeps `SourceConnection` free of a generic parameter and
lets generic infrastructure work with the type-erased `StateCollectionId`:

```rust
pub trait SourceConnection: Debug + Clone + PartialEq + AlterCompatible {
    // ... existing methods ...

    /// The state collections associated with this source type.
    /// Returns an empty vec by default (no state collections).
    fn state_descs(&self) -> Vec<(StateCollectionId, RelationDesc)> {
        vec![]
    }
}
```

Source types that have no state collections use the default (empty vec).
The unit type implements `StateCollectionKey` as the empty set:

```rust
impl StateCollectionKey for () {
    fn name_suffix(&self) -> &'static str { unreachable!("unit has no variants") }
    fn desc(&self) -> RelationDesc { unreachable!("unit has no variants") }
    fn all() -> Vec<Self> { vec![] }
}
```

When a source type needs state collections, it defines a source-specific enum
implementing `StateCollectionKey` and overrides `state_descs()` to return
`Self::StateKey::all_descs()`. For example, the PostgreSQL source will declare:

```rust
// In src/storage-types/src/sources/postgres.rs (future work)

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash,
         Serialize, Deserialize)]
pub enum PgStateCollection {
    Errors,
    TimelineHistory,
}

impl StateCollectionKey for PgStateCollection {
    fn name_suffix(&self) -> &'static str {
        match self {
            PgStateCollection::Errors => "errors",
            PgStateCollection::TimelineHistory => "timeline_history",
        }
    }

    fn desc(&self) -> RelationDesc {
        match self {
            PgStateCollection::Errors => PG_ERRORS_DESC.clone(),
            PgStateCollection::TimelineHistory => PG_TIMELINE_HISTORY_DESC.clone(),
        }
    }

    fn all() -> Vec<Self> {
        vec![PgStateCollection::Errors, PgStateCollection::TimelineHistory]
    }
}

impl SourceConnection for PostgresSourceConnection {
    // ...
    fn state_descs(&self) -> Vec<(StateCollectionId, RelationDesc)> {
        PgStateCollection::all_descs()
    }
}
```

This design provides compile-time safety:

- **Adding a new state collection** means adding an enum variant, which the
  compiler forces you to handle in `name_suffix()`, `desc()`, and `all()`.
- **Removing a state collection** is a compile error everywhere the variant is
  referenced.
- **The dataflow code** looks up collections by enum variant, not by string —
  a typo is a compile error, not a silent runtime `None`.

Other source types keep the default empty `state_descs()` initially, and can
introduce their own enum when they need state collections.

#### 2. Parser and AST

State collections use a dedicated `CREATE STATE` DDL statement (distinct from
`CREATE SUBSOURCE`). This is an internal statement — direct user execution is
blocked in the command handler. The AST node is:

```rust
pub struct CreateStateStatement<T: AstInfo> {
    pub name: UnresolvedItemName,
    pub columns: Vec<ColumnDef<T>>,
    pub constraints: Vec<TableConstraint<T>>,
    pub if_not_exists: bool,
    pub with_options: Vec<CreateStateOption<T>>,
}
```

Options include `Key` (the `StateCollectionId` string) and `RetainHistory`.

During purification, the parent `CREATE SOURCE` statement is updated to include
`EXPOSE STATE <key> AS <name>` clauses that record the association between
state collection keys and their catalog names. This appears in the persisted
`create_sql` but is not user-facing syntax — users do not write it.

#### 3. Purification

During purification in `src/sql/src/pure.rs`, generate a
`CreateStateStatement` for each state collection. The `PurifiedCreateSource`
struct carries them:

```rust
pub enum PurifiedStatement {
    PurifiedCreateSource {
        create_progress_subsource_stmt: Option<CreateSubsourceStatement<Aug>>,
        create_state_stmts: Vec<CreateStateStatement<Aug>>,
        create_source_stmt: CreateSourceStatement<Aug>,
        subsources: BTreeMap<UnresolvedItemName, PurifiedSourceExport>,
        available_source_references: SourceReferences,
    },
    // ...
}
```

Purification calls `source_connection.state_descs()` and iterates over the
returned `Vec<(StateCollectionId, RelationDesc)>`:

1. For each `(state_id, state_desc)`, generate the catalog name:
   `<source_name>_<state_id.0>` (e.g., `my_pg_source_errors`), with
   deconfliction if the name already exists.
2. Convert the `RelationDesc` to column definitions via
   `relation_desc_into_table_defs()`.
3. Create a `CreateStateStatement` with a `Key` option set to the
   `StateCollectionId` string and any inherited `RetainHistory` option.
4. Record `EXPOSE STATE <key> AS <name>` on the parent `CREATE SOURCE`
   statement so the catalog SQL captures the association.

Because state collection identity is derived from `StateCollectionId` strings
(which come from `StateCollectionKey::name_suffix()`), the set of generated
statements is deterministic and does not depend on ordering.

#### 4. Planning

Each `CreateStateStatement` is planned via `plan_create_state()`, which
produces a `CreateSourcePlan` with `data_source: DataSourceDesc::State { key }`.
The `key` field carries the `StateCollectionId` that identifies which state
collection this entry represents. The plan uses
`timeline: Timeline::EpochMilliseconds` (wall-clock time) and `in_cluster: None`
(state collections inherit the parent source's cluster).

Each state collection is independently planned — multiple state collections
produce multiple `CreateSourcePlan` entries.

#### 5. Catalog

The catalog has `DataSourceDesc::State { key: StateCollectionId }` with full
match-arm coverage. Multiple state collections are simply multiple catalog
entries, each with this variant. The `key` field preserves the identity of each
state collection through the catalog layer.

`CatalogItemType::State` is a dedicated in-memory object type, with support in
audit logging and serialization.

Each state collection appears in `mz_sources` with `type = 'state'` and a
distinct name. The parent-child relationship is tracked in
`mz_object_dependencies`.

Existing `is_state_source()` helper works unchanged — it identifies any
catalog entry with `DataSourceDesc::State`.

#### 6. Sequencing

In `src/adapter/src/coord/sequencer/inner.rs`, the `plan_purified_create_source`
method handles progress subsources, data subsources, and state collections. It:

1. Accepts `state_stmts: Vec<CreateStateStatement<Aug>>`.
2. Allocates `CatalogItemId` and `GlobalId` for each state statement.
3. Plans each via `plan_state()`, which extracts the `Key` option and produces
   a `CreateSourcePlan` with `DataSourceDesc::State { key }`.
4. Updates the parent source statement with `EXPOSE STATE <key> AS <name>`.
5. Adds state plans to the overall plan list alongside progress and data
   subsource plans.

#### 7. IngestionDescription

`IngestionDescription` needs to carry the GlobalId for each state collection.
Since `IngestionDescription` is generic over all source types (it uses the
type-erased `SourceDesc`), it cannot use a source-specific enum directly. The
map key is a `String` derived from `StateCollectionKey::name_suffix()`:

```rust
/// Identifies a state collection within an ingestion by its name suffix.
/// Constructed from StateCollectionKey::name_suffix() during setup, then
/// used as a type-erased key in the generic IngestionDescription.
#[derive(Clone, Debug, Eq, PartialEq, Ord, PartialOrd, Hash,
         Serialize, Deserialize)]
pub struct StateCollectionId(pub String);

pub struct IngestionDescription<S> {
    pub desc: SourceDesc,
    pub source_exports: BTreeMap<GlobalId, SourceExport<S>>,
    pub instance_id: StorageInstanceId,
    pub remap_collection_id: GlobalId,
    pub remap_metadata: S,
    /// State collections keyed by their StateCollectionId.
    pub state_collections: BTreeMap<StateCollectionId, (GlobalId, S)>,
}
```

The `collection_ids()` method is updated to include all state collections:

```rust
pub fn collection_ids(&self) -> impl Iterator<Item = GlobalId> + '_ {
    self.source_exports
        .keys()
        .copied()
        .chain(std::iter::once(self.remap_collection_id))
        .chain(self.state_collections.values().map(|(id, _)| *id))
}
```

Source-type-specific code in the storage dataflow converts from the enum to
the `StateCollectionId` for lookups. A helper method on `StateCollectionKey`
makes this safe:

```rust
impl<K: StateCollectionKey> ... {
    /// Look up a state collection by its typed key.
    fn get_state_collection(&self, key: &K) -> Option<&(GlobalId, S)> {
        self.state_collections.get(&StateCollectionId(key.name_suffix().to_string()))
    }
}
```

The dataflow code uses the enum directly — the compiler ensures the variant
exists and the name suffix is consistent:

```rust
// In the PostgreSQL source operator:
let errors_id = ingestion.get_state_collection(&PgStateCollection::Errors);
let timeline_id = ingestion.get_state_collection(&PgStateCollection::TimelineHistory);
```

A typo like `PgStateCollection::Erors` is a compile error, not a silent
runtime `None`.

The `AlterCompatible` implementation validates that the set of
`StateCollectionId` keys and their metadata are compatible between the old
and new ingestion descriptions.

#### 8. Storage Controller

The storage controller in `src/storage-controller/src/lib.rs` and
`src/storage-client/src/storage_collections.rs` already handles
`DataSource::State` in all match arms (from `claude pass1`). The key behavior
is identical for all state collections regardless of count:

- **Not registered with persist table worker** (state is written by the
  dataflow, not the coordinator).
- **Follows wall clock time dependence** (same as progress).
- **Dropped as a simple collection** (same as progress/table/other).

When constructing an `IngestionDescription`, the coordinator populates
`state_collections` by looking up the GlobalIds of all state subsources that
depend on the parent source.

#### 9. Storage Dataflow (Future Work)

The source-specific operators in `src/storage/src/source/` (e.g.,
`postgres.rs`) will be updated to receive handles to each state collection.
Each collection is read and written independently:

**Errors collection:**

1. **On startup:** Read all persisted definite errors.
2. **During operation:** When a definite error is encountered, write it with its
   timestamp into the errors collection.
3. **On restart:** Read persisted errors and re-emit them at their original
   timestamps to maintain consistency.

**Timeline history collection:**

1. **On startup:** Read the full timeline history.
2. **On reconnect after failover:** Compare the upstream's current timeline
   against persisted history to determine if replication can safely continue.
3. **On timeline change:** Write the new timeline entry (timeline ID, LSN,
   parent timeline, switchover LSN) into the collection.

#### Time Domain

State collections use **wall-clock time** (epoch milliseconds) as their persist
shard's time domain, the same as progress subsources. This is distinct from the
source's own timestamp domain used by data subsources and the remap collection.

This works because state collections are **metadata about the ingestion**, not
part of the reclocked data stream. The source operator writes state entries at
the wall-clock time they occur, and reads them back as of the shard's current
frontier on restart. Source-domain timestamps that are semantically meaningful
(e.g., the timestamp at which a definite error should be re-emitted) are stored
as **column values** within the state collection's schema (e.g., the `error_ts`
column in the errors collection), not as the persist shard's timestamp.

This separation is important:

- The **persist shard timestamp** (wall-clock) governs when a row is visible
  and when it can be compacted. It does not need to match the source's logical
  time.
- The **column values** carry whatever source-domain information the operator
  needs to correctly resume. The errors collection stores the source timestamp
  at which the error must be re-emitted; the timeline history collection stores
  LSN positions from the upstream PostgreSQL server.
- Because state collections are not reclocked (they are not part of the data
  path through the remap operator), they have no reason to participate in the
  source's timestamp domain. Wall-clock time is the natural choice, consistent
  with how progress subsources work.

The comparison to the three collection types in an ingestion:

| | Data subsources | Remap/Progress | State collections |
|---|---|---|---|
| **Persist time domain** | Source time | Wall-clock | Wall-clock |
| **Written by** | Ingestion export | Reclocking operator | Source-specific operators |
| **Read by** | Users, downstream | Reclocking operator | Source operators on restart, users |
| **Reclocked** | Yes | N/A (is the reclock mapping) | No |
| **Compaction** | Follows source frontier | Follows source frontier | Independent per collection |

These collections are independent. The errors collection may be written
frequently during error conditions while the timeline history collection is
written only during failover events. Independent persist shards allow each to
have its own compaction policy.

#### 10. Command Handler Guard

Block direct user execution of `CREATE STATE` in
`src/adapter/src/coord/command_handler.rs`, following the pattern used for
`CREATE SUBSOURCE`:

```rust
Statement::CreateState(_) => {
    ctx.retire(Err(AdapterError::Unsupported("CREATE STATE statements")));
    return;
}
```

### Catalog Visibility

State collections appear in existing system tables:

| System Table | Behavior |
|---|---|
| `mz_sources` | Listed with `type = 'state'`, one row per state collection |
| `mz_object_dependencies` | Dependency edge: each state collection depends on parent source |
| `mz_source_statuses` | Excluded (like progress subsources, filtered by `type NOT IN ('progress', 'state', 'log')`) |
| `mz_source_statistics` | Excluded (filtered by type) |

For a PostgreSQL source `my_pg_source`, the user sees:

```sql
SELECT name, type FROM mz_sources WHERE name LIKE 'my_pg_source%';
```

| name | type |
|---|---|
| `my_pg_source` | `postgres` |
| `my_pg_source_errors` | `state` |
| `my_pg_source_timeline_history` | `state` |

### Lifecycle

- **Creation:** Auto-generated during `CREATE SOURCE` purification for each
  `StateCollectionDesc` returned by `state_descs()`.
- **Dependency:** Each state collection depends on its parent source.
- **Drop:** All state collections are dropped via `DROP CASCADE` when the parent
  source is dropped.
- **Compaction:** Each state collection can have its own compaction policy.
  Errors may retain full history for auditability; timeline history may compact
  aggressively since only the latest state matters.
- **Alter:** Validated for compatibility via `AlterCompatible`.

### Example: PostgreSQL Source End-to-End

```sql
-- User creates a PostgreSQL source
CREATE SOURCE my_pg_source
  FROM POSTGRES CONNECTION pg (PUBLICATION 'pub')
  FOR ALL TABLES;

-- System auto-creates:
--   my_pg_source_errors             (state: definite errors)
--   my_pg_source_timeline_history   (state: PG timeline history)

-- User queries errors for diagnostics
SELECT * FROM my_pg_source_errors;
-- Returns:
--   error                          | details
--   "relation pg_catalog.foo ..."  | {"hint": "..."}

-- User queries timeline history after a failover
SELECT * FROM my_pg_source_timeline_history;
-- Returns:
--   timeline_id | switchover_lsn
--   1           | NULL
--   2           | 167843200
```

## Current Implementation Status

The infrastructure is complete across all layers. The 5 commits on the
`maz-state-collection` branch implement:

1. **Foundational layer:** `StateCollectionId`, `StateCollectionKey` trait,
   `IngestionDescription.state_collections` `BTreeMap`, `DataSource::State`
   handled in all storage controller match arms.
2. **Purification and sequencing:** `PurifiedStatement` carries
   `create_state_stmts`, `plan_purified_create_source` allocates IDs and plans
   each state statement, message handler routes state creation.
3. **Named state collections:** State collections keyed by `StateCollectionId`
   (derived from `name_suffix()`) rather than positional indexing.
4. **CREATE STATE statement:** Dedicated `CreateStateStatement` AST node, parser
   support, `plan_create_state()` planner, `DataSourceDesc::State { key }`.
5. **In-memory object type:** `CatalogItemType::State` with audit log and
   serialization support.

**Remaining work:**
- Define `PgStateCollection` enum and override `state_descs()` on
  `PostgresSourceConnection` so that state collections are actually created
  for PostgreSQL sources.
- Wire the storage dataflow operators to read/write state collections.
- Update `mz_sources` type column documentation to list `'state'`.

## Alternatives

### Alternative 1: Single State Collection Per Source

Use one state collection per source with a combined schema containing columns
for all state types (errors, timeline history, etc.).

**Rejected because:**
- Errors and timeline history have fundamentally different schemas. A combined
  schema would require extensive nullable columns and type-tagged rows,
  making queries awkward (`SELECT * FROM state WHERE kind = 'error'`).
- Different compaction needs: timeline history only needs the latest state,
  while errors may need full history for consistent re-emission. A single
  shard forces a single compaction policy.
- Different update frequencies: errors may be written frequently during
  degraded operation; timeline history is written only on failover. Separate
  shards avoid contention.
- Adding new state types in the future requires schema migrations to the
  shared collection, whereas new state collections can be added independently.

### Alternative 2: Vec-Based or Stringly-Typed State Collection Identification

Have `SourceConnection` return a `Vec<StateCollectionDesc>` and key
`IngestionDescription`'s state map with `String` name suffixes.

**Rejected because:**
- Positional `Vec` indexing is fragile: reordering entries silently breaks the
  mapping between descriptors and GlobalIds.
- String-keyed lookups (`state_collections.get("errors")`) fail silently on
  typos at runtime rather than at compile time.
- Adding or removing a state collection does not produce compiler errors at all
  the sites that reference it.
- The enum-based approach (`StateCollectionKey`) provides compile-time safety:
  a typo in a variant name is a compile error, adding a variant forces
  exhaustive handling, and the name suffix is derived from a single canonical
  `match` arm rather than duplicated as a string literal across the codebase.

### Alternative 3: Internal Collections Without Catalog Representation

Create state collections as purely internal persist shards (like remap
collections), without any catalog entry or DDL.

**Rejected because:**
- A primary goal is user-queryable diagnostics. Without a catalog entry, the
  state collection cannot be referenced in SQL.
- Without catalog tracking, lifecycle management (drop, compaction, dependency)
  must be handled ad hoc rather than leveraging existing subsource
  infrastructure.

## Open Questions

1. **Should state collections be created unconditionally or gated by a feature
   flag?** For a phased rollout, state collections could be created only when a
   LaunchDarkly flag is enabled, with the source types falling back to current
   behavior when disabled. Recommended: use a feature flag for initial rollout.

2. **What are the compaction policies for each state collection type?**
   - **Errors:** Retain at least to the source's current frontier, so errors can
     be re-emitted at their original timestamps. May retain longer for audit.
   - **Timeline history:** Can compact aggressively to only the latest value per
     key (timeline_id). Only the current timeline chain is needed for failover
     decisions.
   - The exact policies are implementation details deferred to the storage
     dataflow work.

3. **How should state schema evolution be handled?** When a source type's
   `StateCollectionKey` enum changes across Materialize versions (new variants,
   new columns in `desc()`), existing state collections need migration. This can
   follow the same pattern as catalog migrations, but the details are deferred
   to implementation.

4. **Should users be able to provide custom names for state collections?** The
   current recommendation is auto-generated names only. If users want custom
   names, the syntax could be extended later (e.g.,
   `EXPOSE STATE errors AS my_custom_name`). This is not needed for the initial
   implementation.
