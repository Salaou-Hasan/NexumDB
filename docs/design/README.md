# Design Notes

Design notes and worked examples for Nexum subsystems live here as each phase
is implemented.

## Completed

- [03-storage-engine.md](03-storage-engine.md) — Memory-first storage engine
  (Phase 3): one authoritative in-memory state per table, co-located row +
  version, derived indexes, change tracking, and the Phase 4 attach point.
- [04-transaction-engine.md](04-transaction-engine.md) — Transaction engine
  + OCC (Phase 4): read/write sets, pure validation, multi-table atomic
  commit, deterministic ordering — plus the **correction** adding
  read-your-writes, table mutation epochs (phantom protection), and the
  honest conservative-serializability analysis.
- [05-wal-snapshots-recovery.md](05-wal-snapshots-recovery.md) — Durability
  (Phase 5): the WAL record format and commit framing, the durability
  contract, snapshots, and the recovery/replay procedure.
- [06-reducer-api.md](06-reducer-api.md) — Reducer API (Phase 6): one
  reducer = one transaction, the controlled `ReducerContext`, transaction-
  local events, panic behavior, and the WAL attach point.
- [07-wasm-reducer-runtime.md](07-wasm-reducer-runtime.md) — WASM reducer
  runtime (Phase 7): the untrusted-code sandbox, the restricted
  `("nexum","op")` host ABI, deterministic fuel/memory/host-call limits,
  and the shared commit/abort boundary with native reducers.
- [08-subscription-engine.md](08-subscription-engine.md) — Subscription
  engine (Phase 8): committed-change-driven observation, the logical bounded
  query model, atomic establishment, exact top-N windows, the commit-sequence
  cursor, stale-marking backpressure, and resync.
- [09-simulation-engine.md](09-simulation-engine.md) — Simulation engine
  (Phase 9): one World = one authoritative partition, one transaction per
  tick, ordered systems, deterministic inputs, scheduled events, the
  deterministic RNG, and the caller-owned WAL/subscription boundary.
- [10-runtime.md](10-runtime.md) — Runtime (Phase 10): workers own worlds,
  the runtime coordinates lifecycle, input routing, deterministic stepping,
  durability-first/observation-second ordering, per-world WALs and
  subscription registries, snapshot+WAL recovery, and shutdown.
- [11-concurrency.md](11-concurrency.md) — Concurrency & parallel
  execution (canonical Phase 11): the greedy table-disjoint group planner,
  branch-and-absorb exact merge, first-failure-in-system-order
  determinism, the declared-access conflict model, and worker-count
  independence.
- [12-multi-partition.md](12-multi-partition.md) — Multi-partition
  simulation (canonical Phase 12): partition = World, deterministic
  tick-aligned messaging (delivery phase before tick phase, one logical
  tick of latency), handler reducers as the destination interface, bounded
  inbound queues with deterministic drop, and per-partition recovery.
- [13-networking-sdk.md](13-networking-sdk.md) — Networking + client SDKs
  (canonical Phase 13): reducer calls over the protocol (branch/absorb
  execution inside the tick, per-tick budget, FIFO, the correlated
  pending-call lifecycle), server events on `TickUpdate`, request
  correlation ids, the `nexum-sdk` client crate (derived views only), and
  the finalized queue/budget/lifecycle semantics.
- [14-game-server.md](14-game-server.md) — Game server layer (canonical
  Phase 14): the orchestration/product layer that composes Runtime,
  partitions, networking, and the SDK. Player/game-instance/session
  identity (orchestration metadata only — gameplay state stays in the
  simulation), deterministic join/leave/reconnect, deny-by-default reducer
  exposure with server-trusted invocation, per-world command buffering
  (one frame per world per tick), the reserved server request-id
  namespace, failure observation through game events, and the idempotent
  rejoin contract for recovery.

## Frozen early implementation

- [11-networking-control-plane.md](11-networking-control-plane.md) —
  Realtime networking + control plane, implemented ahead of the canonical
  roadmap and preserved intact as the `nexum-network` foundation of
  canonical Phase 13: the versioned binary protocol,
  connection/session/attachment model, the `Authenticator` interface, the
  gateway adapter (routing, subscription attachment, fanout,
  backpressure), the typed operator control plane, and the recovery
  no-replay contract.

## Planned topics

- Performance & benchmarking (Phase 15)
- Production hardening & release (Phase 16)
