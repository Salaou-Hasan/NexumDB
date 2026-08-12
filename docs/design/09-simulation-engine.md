# Phase 9 — Deterministic Single-Partition Simulation Engine

This document is the design for the simulation layer built **on top of** the
Phases 1–8 authoritative state model. It answers the design questions posed
in the Phase 9 brief before implementation, and it is the companion to
`docs/architecture/09-simulation-engine.md` (ADR-009).

## 1. Philosophy

Nexum's core commitment is that there is **one authoritative state**:

```text
TABLES         = authoritative world state
TRANSACTIONS   = atomic state transitions
REDUCERS       = state-transition programs
WASM           = sandboxed reducer execution
CHANGES        = committed state transitions
WAL            = durability
SUBSCRIPTIONS  = observation
SIMULATION     = deterministic execution of the world
```

Simulation therefore adds **no second database, no second transaction
system, and no second OCC implementation**. A simulation tick is just a
deterministic sequence of state transitions executed through the existing
transaction engine, committed through the existing commit path, and exposed
through the existing `Vec<Change>` boundary.

```text
Simulation tick
      │
      ▼
 Transaction (one per tick)
      │
      ▼
   OCC validation
      │
      ▼
   atomic commit
      │
      ▼
  Vec<Change>
      │
      ├──────────► WAL (caller)
      └──────────► SubscriptionRegistry (caller)
```

## 2. What is a World?

A `World` is **one authoritative simulation partition**. It owns:

```text
World
  ├── WorldId
  ├── TableStore          ← the authoritative state (owned, not duplicated)
  ├── SimulationConfig    ← seed + bounds
  ├── tick counter        ← logical time
  ├── SystemRegistry      ← ordered, deterministic systems
  ├── ReducerRegistry     ← native reducers invocable during ticks
  ├── WasmModuleRegistry? ← sandboxed reducers invocable during ticks
  └── Schedule            ← future scheduled events
```

The `TableStore` lives **inside** the world. There is no separate
"simulation database": systems read and write the very same tables that
reducers, transactions, WAL, and subscriptions already use.

## 3. What is a Simulation?

In Phase 9 the concepts "Simulation" and "World" are the same thing: a
`World` *is* a single-partition simulation. Introducing a separate
orchestrator type would be an abstraction with no content yet — the future
runtime (Phase 10) is the natural place for "many worlds / partitions",
session management, and scheduling. Phase 9 therefore exposes exactly one
type, `World`, whose `tick` method advances the simulation.

## 4. What is a Tick?

A tick is **one logical time step**: the smallest unit of simulation
progress, identified by a typed `TickId` (0, 1, 2, … — logical, never
wall-clock). One tick = **one transaction** (see §8).

```text
Tick N
  ├── frame validation          (deterministic input gate)
  ├── scheduled events due at N (reducer invocations, by (tick, id))
  ├── systems in order          (priority asc, SystemId tie-break)
  ├── commit → Vec<Change>
  └── return TickResult { tick, tx_id, changes, events }
```

Wall-clock time is irrelevant to simulation correctness: the outer runtime
may *schedule* ticks using real time, but the simulation itself advances on
logical tick numbers only.

## 5. What is a SimulationSystem?

A system is a **registered, ordered, deterministic state-transition
program**:

```rust
SystemDefinition {
    id: SystemId,        // stable typed identity
    name: String,        // registry-unique, for diagnostics
    priority: u32,       // explicit ordering key
    execute: SystemFn,   // fn(&mut SimulationContext, &InputFrame) -> Result<()>
}
```

Systems are stored sorted by `(priority, SystemId)`; execution order is
**reproducible regardless of registration order** (ADR-009 D4). A system
mutates the world only through the `SimulationContext`, which delegates to
the tick transaction.

## 6. Determinism

This is the central requirement. Given identical **initial state + input
sequence + configuration + reducer code**, a world must produce identical
**committed state, transaction ordering, changes, events, and tick
results**.

Sources of nondeterminism are excluded by construction:

- **No wall-clock time** anywhere in the execution path.
- **No OS randomness / entropy**: the only randomness is
  `DeterministicRng` (splitmix64), seeded from
  `mix(world_seed, tick, system_id)` — a pure function of deterministic
  inputs (ADR-009 D5).
- **No filesystem / network / process state** in system execution.
- **No undefined iteration order**: systems iterate by `(priority, id)`,
  scheduled events by `(at_tick, id)`, commands in frame order, commit
  ordering comes from the Phase 4 transaction engine, event ordering is
  `emit` order, and table scans are `RowId`-ordered.
- **No threads**: Phase 9 is strictly single-threaded (ADR-009 D6) — no
  thread scheduling can influence state order.

The RNG is deliberately per-(world, tick, system): a system's stream is a
pure function of `(seed, tick, system_id)`, so re-running the same tick
reproduces the same stream even if another system's rng usage changes.
(Bias-free `next_below` uses Lemire's rejection method.)

## 7. How systems read and write state

Systems see a controlled `SimulationContext` (mirroring the Phase 6
`ReducerContext` philosophy — never `&mut TableStore`):

```text
SimulationSystem
      │
      ▼
SimulationContext
      │
      ▼
 Transaction (the tick's transaction)
      │
      ▼
     TableStore
```

The context exposes exactly the operations the transaction engine already
provides — `get`, `contains`, `scan`, `lookup_unique`, `insert`, `update`,
`delete`, plus `emit` and reducer invocation — so systems inherit every
Phase 4 semantic unchanged: **read-your-writes, version OCC, missing-row
observations, unique-key validation, table-epoch phantom protection,
deterministic ordering, and multi-table atomicity**. There is no second
read/write model for simulation.

## 8. Transaction model (one tick = one transaction)

The brief's option **B** was chosen (ADR-009 D2): **the entire tick uses one
transaction**.

- **Atomicity**: the whole tick — scheduled events + all systems — commits
  (or aborts) as one unit. A failed tick leaves zero authoritative
  mutation, which is exactly the brief's preferred tick atomicity (§9).
- **Determinism**: one commit per tick gives a single deterministic
  `Vec<Change>` per tick, in Phase 4's deterministic commit order.
- **Observation**: one `apply_changes` fan-out and one WAL frame per tick —
  subscriptions see the tick as one atomic transition.
- **Performance**: one validation pass per tick; transaction memory is
  proportional to the tick's read/write set, never the database size.

Per-system transactions (option A) were rejected because they fragment a
tick into multiple commit units: a mid-tick failure would leave earlier
systems' writes committed — the "System A committed, System B failed"
outcome the brief forbids. Deterministic phase transactions (option C) add
complexity with no benefit for a single-threaded reference implementation.

### 8.1 Reducers inside a tick

Both native and WASM reducers can be invoked from a system (or a scheduled
event) through the context. To preserve tick atomicity, a reducer invoked
**during a tick executes against the tick's transaction** rather than
starting its own (ADR-009 D3). This is the "higher-level orchestration
layer" the Phase 6 brief anticipated:

- `ReducerRegistry::invoke_in_tx(store, &mut tick_tx, name, args)` — runs a
  registered native reducer's `execute` behind the same panic boundary,
  against the tick transaction, returning `(Value, Vec<ReducerEvent>)`
  **without** committing.
- `WasmModuleRegistry::invoke_in_tx(store, &mut tick_tx, name, args)` — the
  same for a sandboxed WASM module (the existing `run_module` host
  machinery, minus the self-owned transaction).

Events emitted during the tick (by systems or reducers) are buffered
**tick-locally** and only escape with a successful tick commit. A reducer
error, WASM trap, or fuel exhaustion fails the whole tick: zero changes,
zero events.

Standalone `ReducerRegistry::invoke` / `WasmModuleRegistry::invoke` (one
invocation = one transaction) remain the API for **external** requests —
the network-facing path the runtime will use. The tick-transaction path is
additive and does not alter it.

## 9. Tick atomicity and failure semantics

```text
Tick
  ↓
frame validation (pre-tick; consumes nothing on error)
  ↓
deterministic execution (scheduled events + systems, one transaction)
  ↓
validation (OCC)
  ↓
atomic commit      ← all-or-nothing
  ↓
complete tick
```

Every failure path — a system error, a reducer rejection, a WASM trap or
fuel exhaustion, an invalid scheduled event, a panic in trusted native
system code, an OCC conflict — aborts the tick's transaction:

- **zero** authoritative mutation (writes were provisional; no rollback
  machinery exists anywhere),
- **zero** committed changes,
- **zero** emitted events,
- the world's tick counter **still advances** (time moves forward; the
  failed tick produced no state change) — ADR-009 D6.

`World::tick` returns `Result<TickResult, TickError>` where `TickError`
carries the failed `TickId` and the underlying `Error`. A failed tick is a
deterministic outcome: the same input sequence fails the same way every run.

A frame that is *invalid* (wrong tick label, over-limit commands) is
rejected **before** the tick is consumed: the counter does not advance and
nothing executes.

## 10. System conflicts

Within a tick, systems run strictly sequentially over one transaction, so
there is **no intra-tick conflict by construction**. The OCC validation at
commit still runs: with exclusive single-partition ownership there is no
concurrent writer to conflict with, but the validation is not skipped —
Phase 4's semantics hold unchanged, and the Phase 10 runtime (which may
execute partitions in parallel) will handle genuine conflicts by
deterministically retrying whole ticks. No speculative parallelism or
rollback machinery is introduced now.

## 11. Input model

```rust
InputFrame { tick: TickId, commands: Vec<InputCommand> }
InputCommand { source: u64, kind: String, payload: Option<Value> }
```

- Inputs are **protocol-independent**: the frame is the same object whether
  the future networking layer builds it from player commands, server
  commands, or synthetic tests.
- **Ordering**: commands are processed in frame order. A frame must be
  constructed deterministically (the caller — ultimately the runtime —
  owns the ordering guarantee; the same frame yields the same simulation).
  Duplicate commands are processed in order, each delivered to every
  system.
- **Validation**: `InputFrame` requires a non-empty command `kind`; the
  world rejects a frame whose `tick` does not match the next tick, or that
  exceeds `max_commands_per_frame`.
- There is no networking, no wall-clock timestamps, and no source
  authentication in the frame — those arrive in later phases.

## 12. Scheduled events

A minimal deterministic scheduler supports future actions:

```rust
ScheduledEvent { id: u64, at_tick: TickId, reducer: String, args: ReducerArgs }
```

- `World::schedule(at_tick, reducer, args)` returns a unique event id.
- At the start of every tick, events with `at_tick <= current tick` are
  executed in `(at_tick, id)` order by invoking their named reducer against
  the tick transaction. Overdue events (scheduled for an earlier tick that
  was skipped) simply run at the next tick — no wall-clock semantics.
- **Failed ticks consume their due events**: due events are taken from the
  schedule before the tick executes, so if the tick fails their writes abort
  with the tick transaction and the events do **not** re-run on a later
  tick. This is deterministic (the same failed tick consumes the same
  events) and matches the design's "logical ticks, never timers" rule —
  time moves forward even when a tick fails.
- `World::cancel_scheduled(id)` removes a pending event.
- The schedule is bounded (`max_scheduled_events`) and iterated
  deterministically. It uses **logical ticks**, never timers.

## 13. Events vs changes

Three distinct concepts, kept separate (as in Phases 6 and 8):

- **Change** — an authoritative state mutation, produced by the tick's
  commit and consumed by WAL / subscriptions.
- **ReducerEvent** — an application-level `(name, payload)` emitted by a
  reducer or system, buffered tick-locally, escaping only on commit.
- **Simulation event** — a logical future action, i.e. a scheduled event.

`TickResult` exposes `changes` (the authoritative transition) and `events`
(application events) separately; they are never merged.

## 14. Entity model

No ECS is built (the brief explicitly warns against a second storage
system). An "entity" is a table row with a stable id; any future ECS-like
API would be a **derived execution abstraction over tables**, never a
parallel authoritative state. Phase 9 systems operate on ordinary tables
directly.

## 15. WAL / subscription integration

`World::tick` returns `TickResult { tick, tx_id, changes, events }`. The
**caller** (the future runtime) is responsible for the established fan-out,
exactly as with reducers:

```rust
let result = world.tick(&frame)?;
wal.append(result.tx_id(), result.changes())?;          // durability
registry.apply_changes(world.store(), result.changes()); // observation
```

- The WAL sees **only committed** tick changes (failed ticks produce none).
- Subscriptions see the tick as **one atomic transition** — never a
  half-applied tick, never provisional state, never failed-tick updates.
- Simulation recovery is WAL recovery: replay reconstructs the tables; the
  world (systems, registries, config, seed) is application code/configuration
  that is re-armed over the recovered store. Tick *numbers* are recovered
  implicitly by replaying the same tick inputs; if the world is rebuilt
  from recovered state, re-running future ticks is deterministic.

## 16. Deterministic RNG

`DeterministicRng` is a splitmix64 generator, seeded per system per tick:

```text
rng_seed = mix(world_seed, tick_id, system_id)
```

- dependency-free (~40 lines, no external crate),
- purely deterministic (no OS entropy),
- unbiased `next_below(bound)` via Lemire's rejection method,
- `ctx.rng()` hands a system a fresh stream; two identical runs produce
  identical streams.

## 17. Panic safety

Native systems are trusted code, but the same disciplined boundary used for
reducers applies: each system's `execute` runs inside `catch_unwind`, and a
panic aborts the tick transaction (zero mutation, zero events) and surfaces
as `TickError { tick, error: Internal("simulation system 'X' panicked") }`.
`catch_unwind` is used **only** at the system boundary — never scattered
through the engine.

## 18. Concurrency model

Phase 9 is strictly **single-threaded, single-partition**. The world owns
its store exclusively; there are no locks, no atomics, no shared state.
The future runtime (Phase 10) will own workers/partitions and route each
partition's ticks to its owning worker — the `World` type is designed to be
moved wholesale into a partition worker, and its tick counter, schedule,
and registries are partition-local by construction.

## 19. Performance

Correctness first. The design nevertheless avoids obvious waste: one
transaction per tick (one validation pass), no copying of tables or
transactions, no allocations per tick beyond the change/event vectors, and
systems compiled to plain fn pointers (no closures capturing nondeterministic
state). Baselines are established by the benchmark example; Phase 15 is the
optimization phase.

## 20. Boundaries

**In scope:** World, one-transaction-per-tick execution, deterministic
systems, deterministic inputs, scheduled events, deterministic RNG, native +
WASM reducer invocation inside ticks, tests, benchmarks, docs.

**Explicitly out of scope (later phases):** networking, sessions, client
SDKs, multi-partition / distributed simulation, sharding, replication,
parallel system execution, speculative execution, final performance
optimization, authentication.

## 21. What Phase 10 should consume

- `World` — an owned, self-contained single-partition simulation
  (`World::new`, `add_system`, `tick`, `store()`, registries).
- `TickResult { tick, tx_id, changes, events }` — the exact commit boundary
  (WAL + subscription fan-out in `tick` order).
- `TickError { tick, error }` — deterministic failure reporting.
- `World` is `Send`-friendly (all owned data; no interior pointers) so a
  runtime can move it into a partition worker.
