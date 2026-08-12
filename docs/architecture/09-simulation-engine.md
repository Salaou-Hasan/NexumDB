# ADR-009 — Deterministic Single-Partition Simulation

- **Status:** accepted
- **Phase:** 9
- **Date:** 2026-08-12
- **Related:** ADR-004 (transactions/OCC), ADR-006 (reducers),
  ADR-007 (WASM runtime), ADR-008 (subscriptions)

## Context

Nexum must eventually run a deterministic simulation over its authoritative
table state. Phase 9 establishes the single-partition reference
implementation that becomes the correctness oracle for later parallel
execution. The design must (a) reuse the existing transaction/OCC/commit
path without duplicating it, (b) guarantee determinism given identical
inputs, and (c) keep tick atomicity: a failed tick must not leave
half-applied simulation state.

## Decision

### D1 — One World = one authoritative partition

`nexum-simulation::World` owns the `TableStore` (the authoritative state),
the ordered system registry, a native `ReducerRegistry`, an optional
`WasmModuleRegistry`, a deterministic schedule, and the logical tick
counter. There is **no second simulation database**: systems read and write
the same tables reducers and subscriptions use.

### D2 — One tick = one transaction

Each `World::tick` runs scheduled events and systems over a single
`Transaction`, then commits it once (or aborts it once). One commit per
tick → one deterministic `Vec<Change>` per tick; subscriptions and WAL see
the tick as one atomic transition. Option A (per-system transactions) was
rejected: it would allow "system A committed, system B failed" partial
ticks. Option C (deterministic phases as separate transactions) adds
complexity without benefit in a single-threaded world.

### D3 — Reducers inside a tick execute against the tick transaction

Additive APIs `ReducerRegistry::invoke_in_tx` and
`WasmModuleRegistry::invoke_in_tx` run a reducer's logic against an
existing transaction **without** committing (the Phase 6 brief's permitted
"higher-level orchestration layer"). Events are buffered tick-locally and
escape only on tick commit. Standalone `invoke` (one invocation = one
transaction) is unchanged and remains the external/network-facing path.

### D4 — Deterministic ordering

- Systems: ascending `(priority, SystemId)` — registration order is
  irrelevant.
- Scheduled events: ascending `(at_tick, event_id)`.
- Input commands: frame order (the caller must build frames
  deterministically; the same frame reproduces the same simulation).
- Commit/change ordering: the Phase 4 transaction engine's deterministic
  order.
- Event ordering: `emit` order, tick-locally.
- Table iteration: `RowId`-ascending scans.

### D5 — Deterministic RNG

`DeterministicRng` = splitmix64, seeded from `mix(world_seed, tick,
system_id)`, with bias-free `next_below` (Lemire rejection). No OS entropy,
no wall clock, no external rand crate.

### D6 — Tick failure semantics

Any failure (system error, reducer rejection, WASM trap / fuel exhaustion,
panic, OCC conflict) aborts the tick transaction: zero mutation, zero
changes, zero events. The tick counter advances on both success and
failure; failed ticks are deterministic outcomes. Invalid frames (wrong
tick label, over-limit commands) are rejected **before** the tick is
consumed. Scheduled events due during a failed tick are consumed by that
tick (their writes abort with it) and never re-run.

### D7 — Strictly single-threaded

No locks, no atomics, no parallel systems, no speculative execution. The
world is an owned, partition-local object; Phase 10 moves it into a worker.

### D8 — Durability and observation stay caller-owned

`TickResult { tick, tx_id, changes, events }` is returned; the runtime
appends `changes` to the WAL and fans them to the `SubscriptionRegistry`,
in tick order — the same `Vec<Change>` boundary reducers already use.

## Consequences

**Positive.** Simulation inherits the full Phase 4 semantic model
(read-your-writes, version OCC, missing-row and unique-key validation,
table-epoch phantom protection, multi-table atomicity) with zero duplicated
logic. Tick atomicity is structural, not rollback-based. Determinism is
enforced by construction (ordering + RNG + single thread). Reducers —
native and WASM — work unchanged inside ticks. Recovery is ordinary WAL
recovery.

**Negative / accepted.** Per-tick conflict concurrency is zero (intentional
in a single-partition reference model); reducer invocation inside a tick
shares the tick transaction rather than owning one (documented trade-off
for atomicity); frame ordering is the caller's responsibility.

## Alternatives considered

- **Per-system transactions (A):** rejected — partial-tick commits.
- **Deterministic phase transactions (C):** rejected — no benefit
  single-threaded.
- **A separate simulation/ECS state store:** rejected — violates the
  single-authoritative-state principle.
- **External RNG crate:** rejected — splitmix64 is ~40 lines, dependency-free
  and fully deterministic.

## Implementation notes (post-design)

- New core IDs `WorldId`, `SystemId`, `TickId` were added to `nexum-core`
  (the typed-ID philosophy of Phases 1 and 8).
- `ReducerRegistry::invoke_in_tx` and `WasmModuleRegistry::invoke_in_tx`
  are purely additive to Phases 6/7; existing tests are untouched.
- The scaffolded `nexum-simulation` crate (from Phase 0) was implemented as
  the `World` orchestrator; no separate `Simulation` type exists — the
  `World` *is* the simulation in Phase 9.
