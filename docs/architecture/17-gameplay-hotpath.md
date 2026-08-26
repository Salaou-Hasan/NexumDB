# Phase 17 — Gameplay Hot-Path & CCU Scaling: Architecture (ADR-017)

Status: complete.

## ADR-017: Non-unique index lookups through the transaction boundary

### Context

The Phase-16 realistic-gameplay benchmark exposed an application-level O(N)
hot path: every game reducer performed `ctx.scan(TABLE)` + a linear
`find_player`, and the WASM `fire_weapon` scanned twice per call. The
engine itself is not the bottleneck (Phase 15: PK lookup ≈ 45 ns, single-row
update ≈ 1 µs at 10M rows). The `lookup_unique` fast path only covers
**unique** indexes; the gameplay needs a **position query** (cell occupancy,
combat target), which requires a non-unique index.

### Decision

1. **Expose non-unique index lookup through the transaction.** New
   `Transaction::lookup_index(table, index, key)` mirrors
   `Transaction::lookup_unique`: committed owners via `Table::lookup`, the
   pending-write overlay (hide committed owners logically deleted or updated
   away from the key; include pending writes owning the key), deterministic
   sort+dedup, and a table-epoch phantom observation (ADR-004 D12–D13).
   `ReducerContext::lookup_index` and WASM `OP_LOOKUP_INDEX` (op 9) delegate
   to it. No new read/write model — reducers keep the single Phase-4
   transaction semantics.

2. **Indexes stay derived infrastructure.** A composite non-unique index on
   the players `(x, y)` columns is declared in the table schema and
   transactionally maintained by the table (ADR-002 D5). It never holds
   authoritative state; it is rebuildable from a scan (same discipline as
   snapshot restore).

3. **Recovery compatibility via additive `add_index`.** Worlds persisted
   before Phase 17 carry the old `players` schema. `TableStore::add_index`
   / `Table::add_index` build a new derived index over existing rows once at
   factory time; `ensure_schema` calls it when the table exists without the
   `pos` index.

4. **The game reducers use the proven fast paths.** `lookup_unique("primary")`
   + `get` for by-id access; `lookup_index("pos")` for cell queries. The
   client-visible contract is unchanged: caller identity is the gateway
   stamped `__caller`; position/damage/hit decisions stay authoritative.

### Consequences

- Reducer per-command cost: O(N) → O(log N + k).
- Determinism preserved: ascending `RowId` results; occupancy/target checks
  are order-insensitive; the tx overlay sorts and dedups.
- No architecture change: ONE authoritative state, ONE transaction path,
  ONE commit → `Vec<Change>`.
- Expected next bottleneck (measured, not fixed, in Phase 17): subscription
  fanout O(subscriptions x changes) — the interest-management phase (Phase
  20) owns it.

## Interface changes (all additive)

| Crate | API |
|---|---|
| nexum-table | `Table::add_index(&mut self, def: IndexDef) -> Result<()>` |
| nexum-table | `TableStore::add_index(&mut self, table: &str, def: IndexDef) -> Result<()>` |
| nexum-table | `Table::index_keys(&self, row: &Row) -> Result<Vec<(String, Vec<Value>)>>` |
| nexum-tx | `Transaction::lookup_index(&mut self, store, table, index, key) -> Result<Vec<RowId>>` |
| nexum-reducer | `ReducerContext::lookup_index(&mut self, table, index, key) -> Result<Vec<RowId>>` |
| nexum-wasm | `OP_LOOKUP_INDEX = 9` (host op + codec, same envelope as op 4) |

## Preserved invariants

- `unsafe_code = forbid` throughout.
- One authoritative state (TableStore); indexes derived.
- Reducers run inside `World::tick` → Transaction/OCC → commit → `Vec<Change>`.
- Deterministic ordering; serial path remains the correctness oracle.
- No silent command loss; no new queue; no unbounded allocation (index lookup
  results are capacity-bounded like `lookup_unique`).
