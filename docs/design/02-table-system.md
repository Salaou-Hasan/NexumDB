# Phase 2 Design — The Table System

**Status:** Accepted — implemented as specified here
**Phase:** 2 (table system)
**Relates to:** ADR-001 (foundation), ADR-002 (table design decisions)

## 1. Goals

Build the typed table engine — the heart of the "Tables are the world"
philosophy — on top of the `nexum-core` foundation:

- schemas and column definitions
- typed values and rows
- primary keys and secondary indexes
- `insert` / `get` / `update` / `delete` / `scan`
- enough relational feel for `Player { id, zone_id, health, level }`-style
  tables, without implementing SQL

## 2. Non-goals (deferred to later phases)

| Feature | Phase |
|---|---|
| Row versions, change tracking | 3 (storage) |
| Multi-table atomic transactions (OCC) | 4 |
| WAL, snapshots, recovery | 5 |
| SQL / query layer (SELECT, WHERE, ORDER BY, LIMIT) | 13 |
| Nullable columns, foreign keys, arbitrary constraints | later |
| Anything network-visible | 11+ |

## 3. Type model — `nexum-core::value`

A compact, complete set of column types:

- `Bool`, `I8..I64`, `U8..U64`, `F32`, `F64`, `String`, `Bytes`

`Value` is the runtime representation of a single cell. It mirrors
`ColumnType` one-to-one.

**Equality and hashing.** `Value` is used as the key of index maps
(`HashMap<Vec<Value>, RowId>`), so it implements `Eq + Hash`. Floats use
**bit-exact** equality and hashing (`to_bits`): deterministic, `NaN == NaN`,
`-0.0 != 0.0`. This keeps `Eq` lawful and makes float index keys
deterministic across runs — essential for deterministic simulation later.

**Ergonomics.** `From<T>` is implemented for every primitive, `String`,
`&str`, `Vec<u8>`, and generically for any `Id` (`From<T: Id> for Value` →
`Value::U64`), so `row![player_id, 10u64, 100i32]` just works.

## 4. Schema model — `nexum-core::schema`

Shared by `nexum-table`, and later `nexum-tx`/`nexum-storage`, without
circular crate dependencies.

- `ColumnDef { id: ColumnId, name: String, ty: ColumnType }` — `ColumnId`s are
  assigned positionally (0, 1, 2, ...) at build time.
- `IndexDef { name: String, columns: Vec<String>, unique: bool }` — a named
  index over one or more columns.
- `TableSchema { name, columns: Vec<ColumnDef>, primary_key: Option<Vec<String>>,
  indexes: Vec<IndexDef> }` — built only through `TableSchemaBuilder`, which
  enforces: non-empty table/column/index names, unique column names, unique
  index names, at least one column, PK columns exist, index columns exist.

Composite primary keys are supported (the spec example uses a single-column
`id`, but `Vec<Value>` keys make composites free).

## 5. Row identity model — the key decision

**Every row is identified by an engine-assigned `RowId`, not by its primary
key values.**

- `Table` owns a monotonic `next_row_id` counter; `insert` returns the new
  `RowId`.
- The developer-declared *primary key* is enforced as a **unique index** on
  its columns. It is a constraint and a fast lookup path — not the row's
  identity.
- `RowId` never changes, even if PK columns are updated. This is what
  transactions (read/write sets), storage (row versions), and subscriptions
  (row-level change tracking) will key on in later phases.

**Rows.** `Row` is an ordered `Vec<Value>` matching the schema column order.
Rows do **not** carry their schema or column names — the schema is owned by
the `Table` (one authoritative definition; no duplicated state). `Row::get_named`
takes the schema as a parameter.

## 6. Storage representation

```
Table {
    id: TableId,
    schema: TableSchema,
    rows: BTreeMap<RowId, Row>,          // authoritative state
    primary: Option<Index>,              // unique index on PK columns
    indexes: HashMap<String, Index>,     // named secondary indexes
    next_row_id: u64,
}
```

- `rows` is a **`BTreeMap`** so `scan()` iterates in ascending `RowId` order —
  deterministic, which the simulation phase (Phase 9) will require.
- `Index` is an enum: `Unique { entries: HashMap<Vec<Value>, RowId> }` and
  `NonUnique { entries: HashMap<Vec<Value>, Vec<RowId>> }`. The key is the
  tuple of projected column values.
- Indexes are **derived infrastructure**: they are rebuilt from and
  maintained against `rows`. There is exactly one authoritative row
  representation; indexes never hold row data themselves.

## 7. Table API

| Method | Semantics |
|---|---|
| `insert(row) -> Result<RowId>` | validate arity/types → check PK + unique indexes → assign RowId → commit row + all indexes |
| `get(row_id) -> Option<&Row>` | by engine identity |
| `get_by_primary_key(&[Value]) -> Result<Option<&Row>>` | via PK index; error if no PK declared |
| `lookup(index_name, &[Value]) -> Result<Vec<RowId>>` | via secondary index; `NotFound` if index unknown |
| `update(row_id, row) -> Result<()>` | full-row replacement; moves index keys atomically; errors leave no partial state |
| `delete(row_id) -> Result<()>` | removes row + all index entries |
| `scan() -> impl Iterator<Item = (RowId, &Row)>` | deterministic RowId order |

`update` is **full-row replacement** — the primitive the transaction engine
needs; read-modify-write is built on top of it later.

**Error mapping** (shared `nexum-core::Error`):

- arity mismatch / type mismatch → `InvalidArgument`
- PK or unique-index collision → `AlreadyExists`
- `update`/`delete` of a missing row → `NotFound`
- unknown index name → `NotFound`

**Atomicity.** Every mutating operation validates *all* constraints before
mutating *any* state. A failed `update` leaves old index entries and the old
row fully intact.

## 8. TableStore — the registry

A minimal registry owning named tables (the `create_table()` from the spec's
conceptual API). It assigns `TableId`s, enforces unique table names, and will
become the natural target of the Phase 4 transaction engine.

```
TableStore { tables: BTreeMap<String, Table>, next_table_id: u64 }
```

- `create_table(schema) -> Result<TableId>` — `AlreadyExists` on name clash
- `drop_table(name) -> Result<()>`
- `table(name)` / `table_mut(name)` / `table_by_id(id)` / `len` / iteration

## 9. Determinism

`scan()` order, index equality (bit-exact floats), and monotonic `RowId`
assignment are all deterministic. Simulation (Phase 9) and reproducible
benchmarks (Phase 15) depend on this property.

## 10. Open questions

- Nullability: deferred — every column is implicitly NOT NULL in Phase 2.
- Should `lookup` support prefix scans on composite indexes? Deferred to the
  query phase (13); exact-key lookup only for now.
- Id allocation across tables: each table has its own `RowId` space for now;
  a global scheme may be needed for cross-table references later.

## 11. Design checklist vs. spec

- ✅ Tables are the authoritative state (rows map; indexes derived)
- ✅ Relational feel, no SQL parsing
- ✅ Primary keys + secondary indexes, composite-capable
- ✅ Simple data structures (BTreeMap/HashMap) until profiling says otherwise
- ✅ No new dependencies, no unsafe code
- ✅ Deterministic ordering
