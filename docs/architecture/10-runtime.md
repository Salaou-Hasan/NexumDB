# ADR-010 — Runtime / Worker Architecture

- **Status:** accepted
- **Phase:** 10
- **Date:** 2026-08-12
- **Related:** ADR-004 (transactions), ADR-005 (WAL), ADR-008 (subscriptions),
  ADR-009 (simulation)

## Context

Phases 1–9 produced a deterministic single-partition simulation (`World`:
one authoritative `TableStore`, one transaction per tick, one `Vec<Change>`
per commit). Phase 10 must own and orchestrate many worlds inside one
process: lifecycle, worker ownership, input routing, tick scheduling,
persistence and observation coordination, recovery, and shutdown — without
becoming a second state engine.

## Decision

### D1 — Runtime coordinates; World owns state

`Runtime` owns workers, worlds, ownership, queues, lifecycle, metrics, and
events (operational metadata). A `World` owns its `TableStore`, systems,
reducers, tick execution, and `TickResult` production (authoritative). The
runtime never mutates tables directly, never implements OCC, never writes
its own WAL, and never tracks its own subscription changes. `World::tick`
remains the only commit path.

### D2 — Single-threaded; workers are logical ownership units

Phase 10 executes worlds serially in deterministic `(worker_id, world_id)`
order (`BTreeMap`/`BTreeSet`, never hash iteration). Workers are ownership
containers with lifecycle, not OS threads. Per-world simulation results
therefore do not depend on worker count or OS scheduling (brief §23, §33).
Real parallelism is Phase 11+; the worker abstraction is the place it will
attach.

### D3 — One WAL and one SubscriptionRegistry per world

Worlds are isolated partitions whose `TableId`s start at zero and collide
across worlds. A shared WAL would make per-world recovery ambiguous, and a
shared subscription registry would mis-associate table ids. Each world gets
`persistence_dir/world_<id>/` (WAL + snapshots) and its own registry.

### D4 — Durability before observation

Per successful tick: `Wal::append(tx_id, changes)` **first** (Phase 5
contract), then `SubscriptionRegistry.apply_changes`. If the append fails,
the world's commit exists only in memory; the runtime does **not** fan out
to subscriptions and marks the world failed (`PersistenceFailure` event).
Subscriptions observe durable state; failed ticks produce zero updates
(Phase 8 semantics preserved).

### D5 — Recovery orchestration with a resume point

`recover_world` orders the Phase 5 `recover` engine and the **same world
factory** as `create_world` according to what is available on disk (the
engine is never reimplemented):

- **With a snapshot** — `recover` restores the authoritative schema into an
  empty store, then the factory wraps the recovered state.
- **WAL-only (no snapshot)** — the WAL carries changes, not DDL, so the
  factory defines the schema first, then the WAL is replayed into the
  world's store.

Both modes then apply the additive `World::resume_tick(tick)` so logical
time continues from where the application left off (the WAL records
changes, not tick counters). Because a recovered store may already contain
the schema, **factories must create tables only if absent** (the runtime
test factories follow this; `TableStore::table(name)` exposes the check).
Recovered history is never replayed as live subscription updates.

### D6 — Explicit failure isolation and ownership

One world's failure marks only that world failed; `fail_worker` marks a
worker and its worlds failed (recoverable). Ownership is a changeable,
explicit mapping (`reassign_world`) — the seed of future migration — with
duplicate owners rejected. `RuntimeError` is a thin taxonomy over the
boundary that preserves lower-level `Error` identity.

### D7 — Bounded input queues with explicit backpressure

Each world has a FIFO input queue bounded by `max_queued_inputs`; a full
queue rejects new frames (`InputRejected`, capacity) — no silent drops.
Frames must be submitted in tick order; the world's own frame gate rejects
mismatched ticks; frames for already-passed ticks are rejected at
submission (late input).

### D8 — Runtime configuration never alters world semantics

`RuntimeConfig` (worker count, persistence, queue bounds, tick failure
policy, snapshot interval, event log limit) is validated at construction
and affects only operational behavior — never seed, inputs, systems, or
tick logic.

## Consequences

**Positive.** Worlds are fully isolated and deterministic regardless of
runtime arrangement; persistence and observation share one ordering with
an explicit durability boundary; recovery reuses the Phase 5 engine; worker
ownership is a first-class, changeable mapping; failure is isolated per
world/worker; shutdown is deterministic and flush-safe.

**Negative / accepted.** Single-threaded execution caps throughput (Phase 11
will parallelize workers); one WAL/registry per world costs file/registry
overhead per world (correctness over constant-factor savings); a
persistence failure stops the world by default (conservative, documented).

## Alternatives considered

- **Shared WAL / shared registry across worlds**: rejected — table-id
  collisions break per-world recovery and subscription association.
- **OS-thread workers now**: rejected — would let OS scheduling touch
  execution order before the semantics are locked; determinism first.
- **Rollback/retry on WAL failure**: rejected — the Phase 5 contract has no
  rollback; stop-and-report is honest.
- **Runtime-owned authoritative state**: rejected — violates the single
  authoritative state principle.

## Implementation notes (post-design)

- `WorkerId` was added to `nexum-core` (typed-ID philosophy).
- `World::resume_tick` is a purely additive Phase 9 API (one method).
- The scaffolded `nexum-runtime` crate (from Phase 0) was implemented as
  the coordinator; sessions are deferred to the networking phase.
- **Review fix:** `assign_worker` round-robins over **running** workers only
  (a new world is never owned by a failed worker) and returns an explicit
  error when none exist.
- **Review fix:** `create_world` rejects an id whose WAL already exists
  (directing the caller to `recover_world`); `Wal::create` would otherwise
  truncate durable history that recovery could restore.
