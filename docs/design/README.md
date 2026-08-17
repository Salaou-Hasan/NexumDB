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

## Completed (Phase 15)

- [15-performance.md](15-performance.md) — Performance & benchmarking
  (canonical Phase 15): the measure-first methodology, workload/dataset
  definitions (100K → 10M rows), and the bottleneck fixes justified by
  measurement — incremental subscription deltas, O(log n) non-unique index
  removal, and the bench-harness no-op-update correction. Results and
  scaling analysis: [reports/15-performance.md](../reports/15-performance.md).

## Completed (Phase 16)

- [16-production.md](16-production.md) — Production hardening & release
  (canonical Phase 16): validated aggregated `ServerConfig` (config file +
  CLI), gateway rate limiting (per-connection token buckets), graceful
  shutdown (ctrlc/stop-file/`--stop-after`, drain-then-flush WAL), leveled
  logging + aggregate metrics, the release profile (LTO), and the CCU load
  harness with honest PASS/DEGRADED measurements (10K PASS, 15–20K
  DEGRADED, connection-only; ~500 realistic-gameplay on full-scan
  reducers). Findings: the cross-client request-ID collision the harness
  exposed (fixed). Report:
  [reports/16-production.md](../reports/16-production.md).

## Completed (Phase 17)

- [17-gameplay-hotpath.md](17-gameplay-hotpath.md) — Gameplay hot-path &
  CCU scaling (canonical Phase 17): removed O(N) game-reducer scans
  (all 7 native reducers + WASM fire_weapon → direct PK/index lookup),
  fixed TickUpdate re-encode-per-client → encode-once broadcast, added
  `Transaction::lookup_index` / `ReducerContext::lookup_index` /
  `OP_LOOKUP_INDEX` (WASM op 9) / `Table::add_index` for recovery-
  compatible post-creation indexing. Measured honest CCU: 10K PASS
  (connection-only), gameplay profiles SATURATED due to subscription
  all-to-all fan-out O(changes × subs) — explicitly Phase 20 scope.
  Server-side profile D @ 500: 83ms → 2.7ms (30×). Report:
  [reports/17-gameplay-hotpath.md](../reports/17-gameplay-hotpath.md).

## Completed (Phase 19)

- [19-hotpath-profiling.md](19-hotpath-profiling.md) — Execution hot-path
  profiling: instrumented the CCU harness (phase timers) and the runtime
  (per-tick world/WAL/subscription profile), ranked the measured
  bottlenecks (subscription all-to-all fan-out 72% of tick, client
  full-set decode 13%, world tick 28%), and implemented the highest-value
  optimization: `Change` rows are now `Arc<Row>` shared across the WAL
  and every subscription window (ADR-019 D4) — sub_apply 30.5ms →
  11.4ms (2.7×) at 1K profile C. Report:
  [reports/19-hotpath-profiling.md](../reports/19-hotpath-profiling.md).

## Completed (Phase 20)

- [20-interest-management.md](20-interest-management.md) — Interest
  management / AOI (canonical Phase 20): duplicate-subscription grouping
  (one shared derived view per distinct query — evaluations per change
  ~1,000 → 1.00, sub_apply 11.4ms → 0.2ms, 57×) and bounded TickUpdate
  (no full change list in the broadcast — client decode 4.0ms → 1.4ms).
  Preserves per-member buffers, overflow→stale, resync, drop detection;
  adds evaluation/delta counters (`ApplyReport`, `RegistryStats`,
  `RuntimeMetrics`). Remaining measured bottleneck: the WASM fire burst
  (per-call wasmi instantiation) — Phase 22. Report:
  [reports/20-interest-management.md](../reports/20-interest-management.md).

## Planned topics

- **Phase 22 — WASM reducer optimization** — the measured #1 remaining
  cost: the fire burst re-instantiates wasmi per call (~550ms for 1,000
  calls at 1K). Instance/linker reuse, batched host calls.
- **Phase 18 — Multi-core runtime** — parallel execution across
  independent worlds/partitions (linear world-tick path, ~12ms/1K moves).
- **Phase 21 — Memory/alloc** — per-tick allocation reduction.
