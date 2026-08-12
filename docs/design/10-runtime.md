# Phase 10 — Runtime / Worker Architecture

This document is the design for the orchestration layer that owns and
coordinates the Phases 1–9 systems. It answers the Phase 10 brief's design
questions and is the companion to `docs/architecture/10-runtime.md`
(ADR-010).

## 1. Philosophy

The runtime is the **coordinator**, never another state engine. Every
architectural rule from Phases 1–9 stays true: one authoritative state per
world, one transaction model, one commit path, one `Vec<Change>` boundary,
WAL for durability, subscriptions for observation.

```text
NEXUM RUNTIME                (coordinates)
   │
   ├── Worker 0 ─── World A ─── TableStore A ─── simulation
   ├── Worker 1 ─── World B ─── TableStore B ─── simulation
   └── Worker 2 ─── World C ─── TableStore C ─── simulation
                         │
                    Vec<Change>
                    /          \
                   ▼            ▼
                  WAL      Subscriptions
```

The runtime owns **operational metadata** (ownership, lifecycle, queues,
metrics, scheduling). The world owns **authoritative state**. The runtime
never mutates a table directly, never runs OCC, never writes its own WAL,
and never tracks its own subscription changes — it *coordinates* those
systems, which remain exactly the Phases 5/8/9 implementations.

## 2. What is Runtime?

`Runtime` is the single-process orchestrator. It:

- owns a deterministic set of `Worker`s and `World`s,
- owns the `WorldId → WorkerId` ownership mapping (one active owner per
  world),
- routes inputs to worlds through bounded per-world queues,
- drives ticks (deterministically ordered, one transaction per tick per
  world),
- coordinates persistence (append each tick's `TickResult.changes` to the
  world's WAL) and observation (fan the same changes to the world's
  `SubscriptionRegistry`),
- orchestrates world creation and recovery,
- reports runtime events and metrics,
- implements deterministic, safe shutdown.

## 3. What is a Worker?

A worker is the **execution owner** of a deterministic set of worlds:

```text
Worker 0
   ├── World A
   ├── World B
   └── World C
```

Phase 10 is explicitly **single-threaded**: workers are logical ownership
containers, not OS threads. The runtime executes worlds serially in
deterministic `(worker_id, world_id)` order. This keeps simulation semantics
identical regardless of worker count (ADR-010 D3) and gives Phase 11+ a
clean place to add real parallelism without touching world semantics.

A worker has a lifecycle (`Running → Failed | Stopped`) and a bounded set of
owned world ids. A failed worker orphans its worlds: they are marked failed
(and therefore recoverable) — the abstraction supports future migration
without implementing it.

## 4. World ownership

There is **exactly one active owner per world**:

```text
WorldId → WorkerId
```

- `create_world` assigns the world to a worker by deterministic round-robin
  (`assign_counter % worker_count`).
- Duplicate world ids are rejected.
- `reassign_world(world_id, worker_id)` moves a world between two running
  workers — an explicit ownership operation (the seed of future partition
  migration).
- `fail_worker(worker_id)` marks the worker failed and its worlds failed
  (recoverable), isolating the failure.

## 5. World lifecycle

```text
Created → Running → Stopped → Running   (stop / restart)
Running → Failed                        (tick or persistence failure)
```

- **Created** — registered with a worker; not ticking.
- **Running** — scheduled for ticks; accepts inputs.
- **Stopped** — not ticking; inputs rejected; state retained; can restart
  (the logical tick counter continues).
- **Failed** — terminal; the world stopped due to a tick failure or a
  persistence failure. Destroy it and recreate/recover it to continue.

`destroy_world` removes a world from the runtime entirely (explicit API;
committed data remains in the world's WAL file on disk — nothing is
silently erased unless the application deletes the persistence directory).

## 6. Runtime configuration

`RuntimeConfig` is validated at `Runtime::new`:

- `worker_count` (≥ 1)
- `world_factory` — the closure that builds a `World` from
  `(WorldId, TableStore, SimulationConfig)`; it registers the application's
  systems/reducers/WASM modules (the same factory is used by `create_world`
  and `recover_world`, so recovered worlds are identical to fresh ones)
- `persistence` — `None` | `Flush` | `Sync` (maps onto the Phase 5
  `DurabilityPolicy`; `Flush` survives a process crash, `Sync` is the
  durable mode)
- `persistence_dir` — required when persistence is enabled; each world gets
  `persistence_dir/world_<id>/` for its WAL and snapshots
- `max_queued_inputs` — the per-world input queue bound (backpressure)
- `tick_failure_policy` — `FailWorld` (default) | `Continue`
- `snapshot_interval` — optional periodic snapshots (logical ticks)
- `event_log_limit` — the bounded runtime event buffer

Runtime configuration never alters world semantics: a world's simulation
result depends only on seed, inputs, systems, reducer code, and tick — not
on worker count, queue sizes, or scheduling policy.

## 7. Scheduler

The scheduler is the deterministic stepping loop:

```text
step():
  for worker in workers (worker_id asc)
    for world in worker.worlds (world_id asc)
      tick(world)                     // at most one tick per step
```

- **Ordering**: fixed `(worker_id, world_id)` from `BTreeMap`/`BTreeSet` —
  never hash-map iteration.
- **Cadence**: the caller decides *when* to step (wall-clock may drive that
  decision); `step()` advances every running world one tick, `tick_once(id)`
  advances one world. Simulation time is logical — a world that is stepped
  fewer times simply has a lower tick count; there is no "catch-up" and no
  missed-tick semantic to guess.
- **Fairness**: every running world gets exactly one tick per `step()`.
- **Overload**: a world that cannot keep up backs up its **input queue** —
  `submit_input` rejects new frames with a capacity error once the bound is
  reached (explicit backpressure; the runtime never silently drops or
  invents inputs).
- **Shutdown**: `step`/`tick_once` are rejected once shutdown begins.

## 8. Input routing

```text
submit_input(world_id, frame)
      │
      ▼
world's bounded input queue (FIFO)
      │
      ▼
next tick: pop front frame (or an empty frame if the queue is empty)
      │
      ▼
World::tick(frame)
```

- Routing is by `WorldId` to the owning world; the runtime never mutates
  world state directly.
- **Ordering**: FIFO per world; frames must be submitted in tick order (the
  world's own frame gate rejects a mismatched tick — a deterministic error).
- **Late input**: a frame whose tick is already below the world's current
  tick is rejected at submission (`InputRejected`).
- **Unknown world / not running**: rejected (`UnknownWorld` /
  `InvalidWorldState`).
- **Queue limits**: `max_queued_inputs` — full queue ⇒ `InputRejected`
  (capacity). No silent drops.
- Inputs are injected programmatically in Phase 10; networking arrives in
  Phase 11 and will land here.

## 9. Tick pipeline and persistence/observation ordering

```text
Scheduler → select world → gather InputFrame → World::tick()
      │
      ▼
  TickResult { tick, tx_id, changes, events }
      │
      ├─► WAL append (per-world WAL)          ← durability boundary FIRST
      │        │
      │        ▼
      │   durability acknowledged
      │        │
      │        ▼
      └─► SubscriptionRegistry.apply_changes  ← observation AFTER durability
```

The order is **WAL first, subscriptions second** (ADR-010 D4). Rationale:

- Subscriptions observe *durable* state. If we fanned out before the WAL
  append and the append then failed, subscribers would have seen changes
  that crash recovery could erase.
- If the append **fails**, the world's commit exists only in memory (the
  Phase 5 contract — "committed in memory, not durable"). The runtime does
  **not** fan out to subscriptions and applies the configured policy:
  the world enters `Failed` with a `PersistenceFailure` event (default).
  Continuing to tick a world whose memory state may diverge from its
  durable state would break the crash-recovery contract, so the default is
  to stop it. (Retry/degraded modes are future work, explicitly not
  silently assumed.)

There is no second commit path: `World::tick` remains the only thing that
commits; the runtime only consumes `TickResult`.

## 10. WAL coordination

- **One WAL per world** (`persistence_dir/world_<id>/log.wal`). Worlds are
  isolated authoritative partitions whose `TableId`s start at zero and
  collide across worlds, so sharing one log would make per-world recovery
  ambiguous. Per-world logs keep WAL state independent (brief §26).
- Per successful tick: `wal.append(tx_id, changes)` (the Phase 5 contract —
  append returns only after the configured durability policy).
- `snapshot_world(id)` captures a Phase 5 snapshot at the current LSN;
  `snapshot_interval` does this periodically (logical tick counts).
- WAL failure semantics: see §9 — no observation, world failed,
  `PersistenceFailure` event.

## 11. Subscription coordination

- **One `SubscriptionRegistry` per world**, for the same isolation reason as
  the WAL (table ids collide across worlds). Subscriptions are created via
  `runtime.subscribe(world_id, query)` and drained via
  `runtime.drain(world_id, sub_id)`.
- Every successful tick's changes are applied to the world's registry
  **after** durability (Phase 8 semantics preserved: committed-only,
  atomic-per-transaction, deterministic ordering).
- A failed tick produces **zero** subscription updates (its transaction
  aborted — no changes ever reached the boundary).
- Recovery never replays history: `recover_world` reconstructs state and
  returns a fresh world; the application re-subscribes and observes only
  future commits (Phase 8 semantics, brief §17).

## 12. Recovery

`recover_world(world_id, sim_config, resume_tick)`:

```text
fresh TableStore
   ↓
nexum_wal::recover(store, wal, snapshot_dir)     ← the Phase 5 engine
   ↓
factory(world_id, store, sim_config)             ← same factory as create
   ↓
World.resume_tick(resume_tick)                   ← continue logical time
   ↓
assigned to a worker, Created; application starts it
```

The runtime **orchestrates** recovery; storage/WAL remains responsible for
the mechanics. No second recovery engine exists. `resume_tick` is an
additive `World` API (ADR-010 D5): the WAL records changes, not the tick
counter, so the application passes the tick count it had reached and the
world continues from there — inputs submitted after recovery resume
seamlessly.

## 13. Runtime events and metrics

`RuntimeEvent` (operational, distinct from database `Change`,
`ReducerEvent`, and simulation events): `WorldCreated`, `WorldStarted`,
`WorldStopped`, `WorldFailed`, `WorldDestroyed`, `WorldRecovered`,
`WorkerFailed`, `TickCompleted`, `TickFailed`, `PersistenceFailure`,
`InputRejected`, `Shutdown`. The buffer is bounded (`event_log_limit`) and
drained via `drain_events()`.

`RuntimeMetrics` snapshots: worlds/workers counts, ticks total/succeeded/
failed, tick nanoseconds (for averages), inputs accepted/rejected, WAL
appends/failures, snapshots, subscriptions, world failures/creations/
recoveries, uptime. Instrumentation points for Phase 14.

## 14. Error model

`RuntimeError` — a thin taxonomy over the boundary, preserving lower-level
identity (`Error` payloads; `Conflict`, `InvalidArgument`, `Internal`, ...
are never rewrapped):

`InvalidConfig`, `UnknownWorld(WorldId)`, `DuplicateWorld(WorldId)`,
`OwnershipConflict`, `InvalidWorldState`, `InvalidWorkerState`,
`InputRejected { world, reason }`, `Persistence(Error)`,
`Tick { world, error }`, `WorkerFailed(WorkerId)`, `Shutdown`,
`Internal(String)`.

## 15. Failure isolation

- A world failure (tick failure under `FailWorld`, or persistence failure)
  marks only that world failed — other worlds on the same worker keep
  running.
- A worker failure (`fail_worker`) marks the worker and its owned worlds
  failed. Worlds on other workers are unaffected.
- The runtime itself does not have a single point of failure for simulation
  semantics: it reports, isolates, and lets the application recreate or
  recover.

## 16. Shutdown

```text
shutdown():
  1. state → Stopping      (new creates/submits/steps rejected)
  2. stop scheduling
  3. no in-flight ticks (single-threaded)
  4. flush every world's WAL (durability contract)
  5. stop workers          (Running → Stopped)
  6. stop worlds           (Running → Stopped; state retained in memory)
  7. release resources
  8. state → Stopped; emit Shutdown event
```

Committed-but-unflushed state is flushed at shutdown; nothing is silently
discarded. Subsequent operations return `RuntimeError::Shutdown`.

## 17. What belongs to Runtime vs World

| World (Phase 9)                  | Runtime (Phase 10)                    |
|----------------------------------|---------------------------------------|
| TableStore (authoritative)       | ownership map (operational)           |
| systems, reducers, WASM          | lifecycle status                      |
| tick execution (one tx per tick) | scheduling / stepping                  |
| deterministic ordering           | input queues                          |
| scheduled events, RNG            | WAL + subscription coordination       |
| produces TickResult              | events, metrics, recovery, shutdown   |

The runtime never moves simulation logic or authoritative state into
itself, and never exposes `&mut TableStore` (the world abstraction owns the
store; `World` is constructed by the application's factory).

## 18. Future multi-partition compatibility

`World = authoritative partition`, `Worker = execution owner`,
`Runtime = coordinator`. Ownership is an explicit, changeable mapping
(`reassign_world`), recovery is by world, and worlds are fully isolated —
the building blocks for Phase 12+ multi-node operation (nodes owning
workers; worker migration = reassign + recover on the target node). Nothing
in Phase 10 ties a world permanently to one process.

## 19. Boundaries

**In scope:** Runtime, Workers (logical), world lifecycle/ownership, input
routing, deterministic scheduling, WAL + subscription coordination,
recovery orchestration, snapshots, events/metrics, shutdown, tests,
benchmarks, docs.

**Explicitly out of scope (later phases):** networking, client connections,
authentication, matchmaking, distributed clusters, multi-machine workers,
cross-partition transactions, migration, replication, consensus, parallel
execution, final performance optimization.

## 20. What Phase 11 should consume

- `Runtime` — the single-process coordinator: `create_world`,
  `recover_world`, `start/stop/destroy_world`, `submit_input`, `step`,
  `tick_once`, `subscribe`/`drain`, `world_status`, `shutdown`.
- `TickResult`-driven persistence/observation ordering (WAL before
  subscriptions) — networking will feed `InputFrame`s into `submit_input`
  and push `SubscriptionUpdate`s (drained per world) to clients.
- `RuntimeEvent`/`RuntimeMetrics` — operational visibility for the control
  plane.
