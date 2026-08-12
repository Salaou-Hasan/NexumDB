# ADR-003: Memory-first storage engine

**Status:** Accepted
**Phase:** 3 (storage engine)
**Date:** 2026-08-11

## Context

Phase 2 delivered the typed table engine: `TableStore` (registry), `Table`
(rows + derived indexes), `Row`, `Value`, schema types. Phase 3 must introduce
a clean in-memory authoritative storage layer underneath the table API, with
row versions and change tracking, so that Phase 4 (OCC transactions), Phase 5
(WAL/snapshots), and Phase 8 (subscriptions) can build on it — without
destabilizing Phase 2.

## Decisions

### D1. `nexum-storage` owns the authoritative per-table row state

`StorageTable { rows: BTreeMap<RowId, StoredRow>, next_row_id, changes }` is
the single authoritative representation. `StoredRow { row: Row, version:
Version }` keeps row data and version in one record so they cannot diverge.

### D2. Indexes remain derived, owned by the table layer

`StorageTable` is index-agnostic. `Table` = `StorageTable` + derived indexes
and orchestrates validate → check constraints → mutate storage → commit
indexes. Indexes are rebuildable from a scan of storage, proving they are
derived (invariant: authoritative state → index state, never the reverse).

### D3. `Row` moves from `nexum-table` to `nexum-core`

Justified interface change: `nexum-storage` must store rows, but `nexum-table`
will depend on `nexum-storage` (for `StorageTable`). If `Row` stayed in
`nexum-table`, that would create a circular dependency. `Row` is a fundamental
value type (like `Value`), so it belongs in the dependency-free core.
**Public API preserved**: `nexum_table::Row` and `nexum_table::row!` are
re-exported from core, so existing code and tests compile unchanged.

### D4. Versions live in `StoredRow`, not a side table

`version_of(row_id)` is an O(log n) lookup. New row = `Version::ZERO`; each
update = `version.next()`; delete removes the record (final version captured
in the change). Phase 4 validates recorded read versions against these.

### D5. Change tracking is a drained, non-authoritative buffer

`Vec<Change>` appended in commit order; `drain_changes()` returns and clears.
Minimum useful representation: table id, kind, row id, old/new rows (Option),
old/new versions (Option). **Changed columns excluded** — derivable by diffing
rows; storing them would duplicate data.

### D6. `Table` keeps its public API and delegates row storage

All Phase 2 `Table` methods keep identical signatures/behavior; storage
delegation is internal. Added: `version_of`, `changes`, `drain_changes`.
`TableStore` adds `drain_changes()` aggregating across tables (deterministic
name order).

### D7. Single-threaded exclusive ownership

No locks, no atomics. Mutations require `&mut`. This is the ownership model
the Phase 10 partition/worker runtime builds on.

### D8. WAL and snapshots attach without redesign

WAL consumes drained `Change`s (already complete commit records). Snapshots
serialize `StorageTable` authoritative state (schema, rows, versions,
next_row_id); indexes are rebuilt, not serialized.

### D9. No new dependencies, no unsafe

`nexum-storage` uses only `std` on top of `nexum-core`; `unsafe_code =
"forbid"` remains.

## Consequences

- `nexum-storage` is a complete, testable storage engine independent of
  indexes; `nexum-table` becomes a thin composition layer.
- Phase 4 consumes `StorageTable`/`Table` (row + version reads, validated
  mutations, change drain) without knowing the backing structure.
- Deterministic scan order and versioning are fixed now, de-risking
  simulation and OCC later.
- All Phase 2 tests must pass unchanged after the refactor — a regression
  check that the storage extraction is behavior-preserving.

## Open questions

- Whether `drain_changes` aggregation order should carry a global commit
  sequence — deferred until WAL (Phase 5) needs it.
- Whether `StorageTable` should expose `update` with an expected-version
  guard — deferred to Phase 4 (that's OCC's job, not storage's).
