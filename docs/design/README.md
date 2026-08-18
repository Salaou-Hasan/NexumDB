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

## Completed (Phase 21)

- [21-networking-hotpath.md](21-networking-hotpath.md) — Networking /
  serialization hot-path (ADR-021): profiled the gateway/SDK delivery
  path with the real CCU harness and ranked the measured costs. Shipped
  D1 — `Arc<[u8]>` transport frames (per-world TickUpdate encoded once,
  delivered to every attached session by refcount bump: zero per-client
  encode/copy; 10K allocs/tick saved at 10K) — and D3 — a per-world
  attached index turning the fan-out pass's O(worlds × CCU) connection
  scans into O(CCU). D2 (per-connection outbound batching) was
  implemented, measured **net-negative** (B@10K p95 44.6 vs 39.5 ms
  baseline), and reverted per the phase rule. Measured: idle fan-out
  5.2 → 4.2 ms (−19%), movement fan-out −23…27%, movement p99 72.9 →
  64.7 ms @ 10K; the movement tick is still bound by the sum of O(CCU)
  per-client work. Report:
  [reports/21-networking-hotpath.md](../reports/21-networking-hotpath.md).

## Completed (Phase 21.5 — investigation, no optimization)

- [21.5-extreme-profiling.md](21.5-extreme-profiling.md) — Extreme execution
  profiling: the full authoritative pipeline instrumented and measured at
  phase, sub-phase, per-reducer, and allocation granularity (no code
  optimized). New instrumentation: per-reducer timing
  (`SimulationConfig::with_reducer_profiling`), a counting global
  allocator (`nexum-alloc-count`, `ccu-alloc` feature), p99.9/max,
  worst-tick spike analysis, and Profile E (extreme gameplay). Ranked cost
  map: WASM fire_weapon 65–69 µs/call ≈ 15× native (the Phase 22 target),
  then gateway fan-out and SDK decode; idle PASS @ 20K; p99.9 ≈ p99 (no
  pathological tail); two measurement bugs fixed (per-world
  `last_tick_profile` under-count, warmup backlog spike). Report:
  [reports/21.5-extreme-profiling.md](../reports/21.5-extreme-profiling.md).

## Completed (Phase 18)

- [18-multi-core.md](18-multi-core.md) — Multi-core runtime (ADR-018):
  the runtime's tick phase executes independent worlds/partitions
  concurrently on scoped threads (one per worker), with per-world
  outcomes collected and merged in the deterministic `(worker_id,
  world_id)` order — observationally identical to serial (proven by
  exact trace-equality tests incl. cross-partition messaging). The Phase
  18 benchmark also uncovered and fixed a gateway inbound O(N²)
  (per-call `pending_calls` scans → per-connection `BTreeSet` index;
  inbound 25.5ms → 2.3ms). Measured at 8K clients × 8 partitions: p95
  movement tick 62.3ms → 31.7ms (8 workers), runtime step 15.3ms →
  7.4ms. Report: [reports/18-multi-core.md](../reports/18-multi-core.md).

## Completed (Phase 22)

- [22-wasm-hotpath.md](22-wasm-hotpath.md) — Transaction overlay optimization
  (canonical Phase 22): COW WriteSet with Arc-based own layer (branch
  O(1) via Arc clone instead of O(N) BTreeMap deep-copy), `has_any_insert()`
  skip (lookup_unique/lookup_index skip the O(N) pending-insert scan when
  no Insert entries exist — the common case for update-heavy workloads),
  absorb fast-path + `Arc::try_unwrap` (skip logical-view check for
  non-Delete workloads, move entries instead of cloning). Measured: harness
  loop 411 µs → 119 µs (3.5×), Profile C @ 1K p99 573 ms → 57.5 ms
  (10×). Report:
  [reports/22-wasm-hotpath.md](../reports/22-wasm-hotpath.md).

## Planned topics

- **Phase 23 — WASM instance reuse** — the isolated WASM cost (13.8 µs/call)
  is dominated by instantiate (47%, 6.5 µs). Caching the compiled Linker
  and using pre-instantiated modules.
- **Phase 24 — networking/transport** — inbound frame batching, real
  (non-in-process) transport benchmarking.
