# Phase 3 Design — Memory-first Storage Engine

**Status:** Accepted — implemented as specified here
**Phase:** 3 (storage engine)
**Relates to:** ADR-001, ADR-002, ADR-003

## 1. Purpose

Build a clean, correct, in-memory authoritative storage layer *underneath* the
Phase 2 table API. The storage engine owns the authoritative representation of
every table's rows; the table engine keeps its role as the public
relational-style API and the owner of *derived* indexes.

The diagram from the phase brief:

```text
                 TABLE API
                    │
                    ▼
              STORAGE ENGINE
                    │
          ┌─────────┼─────────┐
          │         │         │
       ROW DATA   VERSIONS  CHANGES
          │         │         │
          └─────────┼─────────┘
                    │
                 INDEXES          ← derived, owned by the table layer
                    │
                    ▼
             TRANSACTION ENGINE   ← Phase 4 consumes this boundary
```

## 2. The one-authoritative-state rule

There is **one** authoritative representation of a table: `StorageTable.rows`
(a `BTreeMap<RowId, StoredRow>`). Everything else is derived:

| Layer | Authoritative or derived |
|---|---|
| row data | **authoritative** |
| row identity (RowId) | **authoritative** |
| row version | **authoritative** |
| indexes (primary key, secondary) | derived |
| change sets (buffer) | derived — eagerly maintained, drainable |
| cached/query structures | derived |

Invariant, enforced by construction: **authoritative row state → index state**.
Index state is only ever produced from authoritative row state by the table
layer's mutation paths; indexes never feed back into authoritative state.

## 3. Answers to the 15 design questions

### Q1. What is the authoritative representation of a table's rows?

`StorageTable { rows: BTreeMap<RowId, StoredRow>, ... }` where

```rust
struct StoredRow { row: Row, version: Version }
```

One `RowId → StoredRow` map is the single source of truth. There is no separate
"row store" and "version store" that could diverge — version lives beside the
row in the same authoritative record.

### Q2. What owns row memory?

`StorageTable` owns every `StoredRow` (and thus every `Row`, and every
`Value`). Rows are moved into the table on insert and moved out on delete.
Readers borrow `&Row`; nothing else holds row memory. `Table` (nexum-table)
does **not** store rows — it owns only indexes and delegates all row storage to
its embedded `StorageTable`.

### Q3. How are RowIds mapped to rows?

`BTreeMap<RowId, StoredRow>` keyed by the engine-assigned monotonic `RowId`.
`RowId`s are allocated per table (`next_row_id` counter), never reused, and
assigned in insertion order. `BTreeMap` gives deterministic ascending scan
order (needed by Phase 9 deterministic simulation).

### Q4. Where are row versions stored?

Inside `StoredRow`, adjacent to the row data — not in a side table. This makes
it impossible for row state and version state to diverge. `version_of(row_id)`
is an O(log n) map lookup.

### Q5. How are inserts, updates, and deletes represented?

As operations on `StorageTable`:

- `insert(row)` → validates, allocates `RowId`, writes `StoredRow { row,
  version: ZERO }`, appends a `Change::Insert`.
- `update(row_id, row)` → validates, replaces the row, bumps version
  (`version = version.next()`), appends a `Change::Update` with old and new
  rows and versions. A no-op update (new row identical to current) bumps
  nothing and emits no change.
- `delete(row_id)` → removes the record, appends a `Change::Delete` with the
  final row and version.

### Q6. How are changes tracked?

A per-table change buffer: `Vec<Change>` appended in commit order. Consumers
**drain** it (`drain_changes() -> Vec<Change>`), which returns the buffer and
clears it. `changes()` gives a read-only peek. The buffer is derived
infrastructure — it is eagerly maintained because every mutation is a
committed fact, but it is never read to reconstruct authoritative state.

**Change representation** (minimum useful form):

```rust
struct Change {
    table_id: TableId,
    kind: ChangeKind,            // Insert | Update | Delete
    row_id: RowId,
    old_row: Option<Row>,        // Some for Update/Delete
    new_row: Option<Row>,        // Some for Insert/Update
    old_version: Option<Version>,
    new_version: Option<Version>,
}
```

Deliberately **not** included: *changed columns*. Rationale: subscriptions can
diff `old_row` vs `new_row` when they need per-column deltas; storing the diff
would duplicate the rows and force every writer to compute it. The `Option`
shape keeps one uniform struct (serializable for WAL later) instead of three
variants.

### Q7. What is the lifetime/ownership model for rows and values?

- `TableStore` owns `Table`s.
- `Table` owns one `StorageTable` plus its derived indexes.
- `StorageTable` owns `StoredRow`s (which own `Row`s, which own `Vec<Value>`).
- Values are `Clone`, so index keys (`Vec<Value>`) are cheap clones for
  derived structures.
- Borrows are short-lived (`&StoredRow`, `&Row`, `&Change`) and never escape
  a single call.

### Q8. How does the storage engine interact with the existing indexes?

It doesn't know they exist. `StorageTable` is index-agnostic. The table layer
(`Table`) orchestrates:

1. validate row against schema
2. check unique constraints in its derived indexes
3. apply the mutation to `StorageTable`
4. commit index entries / moves

Since all validation happens *before* any mutation, a failed op leaves both
storage and indexes untouched. Index consistency is a property of `Table`, and
indexes can be rebuilt from scratch by scanning `StorageTable` — proving they
are derived.

### Q9. How does the storage engine interact with TableStore?

`TableStore` is the registry: it owns `Table`s and assigns `TableId`s. Each
`Table` embeds one `StorageTable`. `create_table` builds a `StorageTable` for
the schema; `drop_table` destroys the table and its storage. `TableStore` also
exposes `drain_changes()` aggregating change buffers across tables in a
deterministic order (by table name), ready for Phase 5 WAL and Phase 8
subscriptions.

### Q10. What API will Phase 4's OCC transaction engine consume?

The storage abstraction (`StorageTable` + `Table`) with:

- `get(row_id) -> Option<&StoredRow>` (row + version atomically)
- `version_of(row_id) -> Option<Version>`
- `contains(row_id) -> bool`
- `insert/update/delete` (validating, version-bumping, change-appending)
- `scan()`, `changes()`, `drain_changes()`

Phase 4 does OCC at the *table* layer: it records read versions, validates
them before commit, and on conflict returns `Error::Conflict` with **no**
storage mutation (mutation only happens after validation). It never needs to
know whether the backing store is a `BTreeMap`, slot map, arena, or pages.

### Q11. How can WAL eventually be attached without redesigning the storage engine?

WAL (Phase 5) subscribes at the change boundary: every committed mutation
already produces a complete `Change` (table id, kind, row id, old/new rows,
old/new versions). WAL appends drained changes as commit records. Recovery
replays them through the same `insert/update/delete` API. No new
authoritative structure is introduced — the storage engine already emits
exactly what a WAL needs.

### Q12. How do snapshots serialize this state?

A snapshot serializes `StorageTable`'s authoritative state: schema, `rows`
(each `StoredRow`: row + version), and `next_row_id`. Indexes are **not**
serialized — they are derived and rebuilt by scanning rows on load. Since
indexes are provably derivable, snapshotting authoritative state alone is
sufficient and cannot produce divergent indexes.

### Q13. What invariants must always hold?

1. `StorageTable.rows` is the single authoritative row representation.
2. Every `StoredRow` has a row and a version; a row exists iff it is in `rows`.
3. Versions are per-table monotonic per row: new row = `Version::ZERO`; each
   update = `version.next()`.
4. `RowId`s are unique, monotonic, never reused.
5. Every committed mutation appends exactly one `Change` to the buffer.
6. A failed mutation leaves authoritative state, indexes, and the change
   buffer untouched (no partial application).
7. Index entries are always derivable from and consistent with `rows`.
8. `scan()` order is deterministic (ascending `RowId`).
9. `drain_changes()` consumes changes exactly once; drained changes are gone
   from the buffer.

### Q14. Expected complexity

| Operation | Complexity |
|---|---|
| `get` / `contains` / `version_of` | O(log n) |
| `insert` / `update` / `delete` (storage) | O(log n) + O(1) change append |
| index maintenance (table layer) | O(k) hash ops, k = number of indexes |
| `scan` | O(n) |
| `drain_changes` | O(number of buffered changes) |
| `validate_row` | O(number of columns) |

### Q15. What concurrency assumptions are made?

**Single-threaded exclusive ownership.** `StorageTable` and `Table` are owned
by `TableStore`; mutations require `&mut`. No locks, no atomics, no interior
mutability. This is the ownership model the Phase 10 partition/worker runtime
will build on: each partition owns its tables exclusively. Distributed
concurrency is explicitly out of scope.

## 4. Crate and module layout

```text
nexum-core/        (dependency-free)
  src/row.rs       Row — moved from nexum-table (see ADR-003 D3)

nexum-storage/     (depends on nexum-core)
  src/lib.rs
  src/change.rs    Change
  src/table.rs     StorageTable, StoredRow

nexum-table/       (depends on nexum-core + nexum-storage)
  src/lib.rs       re-exports Row (from core) + row! macro
  src/index.rs     Index (unchanged)
  src/table.rs     Table = StorageTable + derived indexes
  src/store.rs     TableStore (registry, + drain_changes)
```

Dependency graph: `nexum-core ← nexum-storage ← nexum-table`. No cycles.

## 5. Version semantics (storage level)

```text
new row   →  version = Version::ZERO
update    →  version = version.next()   (1, 2, 3, ...)
no-op     →  no version bump, no change emitted (identical new row)
delete    →  row removed; the Delete change records the final version;
             version_of(row_id) returns None afterwards
```

Deterministic, per-row monotonic, and fully documented. Phase 4 OCC will
compare a transaction's recorded read versions against these live versions at
validation time.

## 6. Change tracking semantics

- Appended on every committed mutation, in commit order.
- `drain_changes()` returns and clears; `changes()` peeks.
- Contains: table id, kind, row id, old/new rows (Optional), old/new versions
  (Optional). Deliberately excludes changed-column lists (derivable by diff).
- Compatible with subscriptions (Phase 8) and WAL (Phase 5) without redesign.

## 7. Atomicity within a single mutation

The storage layer applies each mutation in one step: validate → mutate → append
change. The table layer adds: validate unique constraints → mutate storage →
commit indexes. Because every check precedes every mutation, a failure leaves
no partial authoritative state, no stale index entry, and no orphan change.

## 8. What Table becomes

`Table` no longer stores rows. It becomes:

```rust
struct Table {
    storage: StorageTable,          // authoritative
    primary: Option<Index>,         // derived
    indexes: HashMap<String, Index>,// derived
}
```

Public API preserved (insert/get/update/delete/scan/lookup/get_by_primary_key/
contains/len/id/name/schema/index_names) and extended with `version_of`,
`changes`, `drain_changes` (delegating to storage). All Phase 2 tests keep
passing unchanged.

## 9. Non-goals this phase

OCC transactions, WAL, snapshots, disk persistence, reducers, WASM,
subscriptions, simulation, networking, distributed execution, SQL query
engine, nullability, foreign keys.

## 10. Benchmarks

A dependency-free timing example (`crates/nexum-table/examples/storage_bench.rs`)
measures the hot-path storage operations (insert/get/update/delete/scan/version
lookup/index lookup/change tracking) on both the raw `StorageTable` and the
full `Table` (with indexes). Proper criterion harnesses arrive with Phase 15;
this phase only needs baselines to prove the model is measurable.

## 11. Design checklist

- ✅ One authoritative in-memory state per table (rows + versions together)
- ✅ Indexes provably derived (rebuildable by scan; index-agnostic storage)
- ✅ Change tracking as drained, non-authoritative buffer
- ✅ Version semantics defined and deterministic
- ✅ Table API preserved; no Phase 2 behavior change
- ✅ Single-threaded ownership model documented for the runtime phase
- ✅ WAL/snapshot attach points exist without redesign
- ✅ No unsafe, no new dependencies
