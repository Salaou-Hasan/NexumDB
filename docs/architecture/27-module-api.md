# ADR: Nexum Module API — Typed Facade over the Authoritative Engine

## Status

Proposed → Accepted

## Context

Nexum's engine (TableStore, OCC transactions, WASM sandbox, subscriptions)
is production-tested at 20K CCU. But authoring game logic currently
requires understanding internals:

```
ctx.lookup_unique(TABLE, PK, &[Value::U64(id)])?;
let row = ctx.get(TABLE, rid)?;
let x = row.get(COL_X).and_then(Value::as_i64).unwrap_or(0);
row = with(row, COL_X, Value::I64(new_x));
ctx.update(TABLE, rid, row)?;
```

Game developers should write:

```
player.x = new_x;
player.save(ctx)?;
```

## Decision

Build a **typed facade layer** over the existing engine using derive macros.
No second database. No second transaction engine. No duplicated state.

### Architecture

```
┌─────────────────────────────────────────────┐
│  Module API (derive-generated)              │
│  Player::get(ctx, id)                       │
│  player.save(ctx)                           │
│  Player::create(ctx, player)                │
│  Player::delete(ctx, id)                    │
│  Player::all(ctx)                           │
├─────────────────────────────────────────────┤
│  Existing Engine (unchanged)                │
│  ReducerContext / SimulationContext         │
│  Transaction (OCC) → TableStore → Commit    │
│  Change tracking → Subscriptions            │
└─────────────────────────────────────────────┘
```

### How it works

`#[derive(NexumTable)]` on a struct generates:

1. **Schema** — `nexum_table_schema()` builds TableSchema from field types
2. **Serialization** — `nexum_from_row(&Row) -> Self`, `nexum_to_row(&self) -> Row`
3. **CRUD** — typed methods that delegate to existing context methods:
   - `get(ctx, pk)` → lookup_unique + get + from_row
   - `save(ctx)` → to_row + update
   - `create(ctx)` → insert + return self
   - `delete(ctx, pk)` → delete by pk
   - `all(ctx)` → scan + map all rows

4. **Caller helper** — `caller()` extracts gateway-stamped identity

The generated code uses fully qualified paths (`::nexum_core::...`) so
no imports are needed in consumer code.

### What stays internal

Transaction boundaries, OCC validation, WAL appends, snapshot writes,
subscription delta computation, change tracking, serialization buffers,
network queues, partition ownership — all remain inside the engine.

The module author never sees them.

### Performance

Zero overhead: each generated method maps 1:1 to an existing engine call.
`Player::get(ctx, id)` compiles to exactly `ctx.lookup_unique(...) + ctx.get(...)`.
`player.save(ctx)` compiles to exactly `ctx.update(TABLE, rid, row)`.
No extra allocations, no dynamic dispatch, no reflection.

## Consequences

- Game developers focus on game logic, not database plumbing
- Type-safe access prevents column-order mistakes at compile time
- Schema changes propagate automatically via derive re-expansion
- Client SDKs can generate matching types from the same schema definition
