# ADR-002: Table system design

**Status:** Accepted
**Phase:** 2 (table system)
**Date:** 2026-08-11

## Context

Phase 1 established the foundation: typed ids, `Version`/`Timestamp`, a shared
`Error` model, and the `Id`/`Versioned`/`ChangeKind` interfaces. Phase 2 must
deliver the typed table engine — the authoritative representation of
application state — without SQL, networking, or distributed execution.

## Decisions

### D1. Row identity is the engine-assigned `RowId`; the declared primary key is a unique index

The primary key is a constraint and lookup path, not the row's identity. This
gives transactions, storage, and subscriptions a stable, immutable row handle
across all phases, and lets PK columns be updated freely.

### D2. `ColumnType` and `Value` live in `nexum-core::value`; schema types in `nexum-core::schema`

The tx engine (write sets), storage (row versions), and subscriptions (deltas)
all need these types. Defining them once in the dependency-free core crate
prevents duplicated type definitions and circular dependencies (ADR-001 D7).

### D3. `Value` uses bit-exact float equality and hashing

`Eq + Hash` is required for index keys. Floats compare by `to_bits()`, making
equality lawful and deterministic (`NaN == NaN`, `-0.0 != 0.0`).

### D4. Rows are schema-free ordered value lists

`Row = Vec<Value>` in schema column order. The schema is owned by the `Table`;
rows never duplicate it. `update` is full-row replacement.

### D5. Authoritative storage is a `BTreeMap<RowId, Row>`; indexes are derived

One authoritative row representation, rebuilt/maintained from it. `BTreeMap`
gives deterministic `scan()` order in ascending `RowId` — required later by
deterministic simulation. Indexes are `HashMap<Vec<Value>, RowId>` (unique) or
`HashMap<Vec<Value>, Vec<RowId>>` (non-unique).

### D6. A minimal `TableStore` registry owns named tables

Provides the spec's `create_table()`, enforces unique table names, assigns
`TableId`s, and becomes the transaction engine's target in Phase 4.

### D7. Mutations are all-or-nothing

All constraints (arity, types, PK/unique collisions) are validated before any
state is mutated. A failed operation leaves no partial state.

### D8. No new dependencies, no unsafe code

The table engine uses only `std` (BTreeMap, HashMap) on top of `nexum-core`.

## Consequences

- `nexum-core` gains `value` and a real `schema` module; `nexum-table` gains
  `row`, `index`, `table`, and `store` modules.
- Phase 4 transactions operate on `TableStore` + `RowId`s; rows already carry
  no schema, so tx write sets are plain `(RowId, Row)` pairs.
- Deterministic iteration and float keys are established now, avoiding
  nondeterminism surprises in the simulation phase.

## Open questions

- Global vs. per-table `RowId` spaces (deferred to runtime phase).
- Nullability and richer constraints (deferred).
