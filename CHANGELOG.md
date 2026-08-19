# Changelog

All notable changes to Nexum will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.1.0] - 2026-08-19

### Phase 22.5 — Networking Hot-Path Aggressive Optimization

- **Arc-shared SubscriptionUpdate rows** — Insert/Update variants now wrap DeliveredRow in Arc, eliminating N×M deep clones in push_commit. sub_apply: 0.6ms → 0.0-0.1ms.
- **SubscriptionDeltaBatch protocol message** — Multiple deltas per subscription pump are batched into a single encoded frame. Reduces sub_deltas message count by ~28×.
- **Incremental Crc32 hasher** — Eliminates per-message crc_input Vec allocation on both encode and decode paths.
- **Removed redundant pump_subscriptions()** — fan_out_results already drains all subscribers.
- **Results**: Profile C @ 1K p99: 43.7ms → 28.8ms (1.5×). Profile E @ 1K p99: 48.0ms → 38.5ms (1.25×).

### Phase 22 — WASM & Transaction Overlay Optimization

- **COW WriteSet with Arc-based own layer** — branch() is now O(1) via Arc::clone (728× faster: 79µs → 109ns).
- **has_any_insert() skip** — lookup_unique/index skip O(N) pending-insert scan when no Insert entries exist (14× faster: 315µs → 22µs).
- **Absorb fast-path + try_unwrap** — Update-only workloads skip logical-view check and move entries instead of cloning.
- **Results**: Profile C @ 1K p99: ~573ms → 57.5ms (10×). Profile E @ 1K p99: ~1,094ms → 71.9ms (15×).

### Phase 21.5 — Extreme Execution Profiling

- Instrumented the entire authoritative pipeline at phase, sub-phase, per-reducer, and allocation granularity.
- Discovered WASM fire_weapon = 65–69µs/call ≈ 15× native.
- Established complete cost map: subscription fan-out, serialization, WASM, allocations.
- No optimizations applied — investigation phase only.

### Phase 21 — Networking & Serialization Hot-Path

- **Arc<[u8]> frames** — Transport frame type is now Arc. TickUpdate encoded once per world, cloned to every session by refcount bump.
- **Per-world attached index** — Fan-out pass changed from O(worlds × CCU) to O(CCU).
- **Results**: idle fan-out @ 10K: 5.2ms → 4.2ms (-19%). Movement fan-out @ 10K: ~15-20ms → ~12.6ms (-27%).

### Phase 20 — Interest Management / AOI

- **Duplicate-subscription grouping** — One shared derived view per distinct query, evaluated once per commit.
- **Bounded TickUpdate** — Broadcast no longer carries full change list; clients receive windowed subscription deltas.
- **Results**: Subscription evaluations per change: ~1,000 → 1.00. sub_apply: 11.4ms → 0.2ms (57×).

### Phase 19 — Hot-Path Profiling

- Instrumented tick path at phase + sub-phase level.
- Identified subscription all-to-all fan-out as #1 bottleneck (30.5ms/tick, 72% of tick).
- **Arc-shared Change rows** — sub_apply: 30.5ms → 11.4ms (2.7×).

### Phase 18 — Multi-Core Runtime

- Parallel world/partition execution across scoped threads.
- Deterministic merge in (worker_id, world_id) order — proven by exact trace-equality tests.
- **Results**: 8K clients × 8 partitions: workers=1 p99 103.6ms → workers=8 p99 52.4ms.
- Fixed gateway inbound O(N²): pending_calls scans → per-connection BTreeSet index (25.5ms → 2.3ms).

### Phase 17 — Gameplay Hot-Path & CCU Scaling

- Game reducers now use direct PK/index lookups (O(N) scans → O(1)).
- TickUpdate encode-once broadcast (51ms → 3ms at 1K).
- **Results**: Profile D @ 500: 83ms → 2.7ms (30×).

### Phase 16 — Production Hardening & Release

- Production config validation, rate limiting, graceful shutdown, observability.
- Release profile: LTO (fat), single codegen unit, panic=unwind.
- **CCU**: Connection-only 20K PASS (p99 32ms).

### Phase 15 — Performance & Benchmarking

- Micro + scale benchmarks (100K → 10M rows).
- PK lookup: 46ns. Single-row UPDATE: 0.97µs. Tick cost scales with active entity set, not table size.

### Phases 1–14 — Foundation

- Core types, table engine, storage engine, WAL/snapshots, reducer API, WASM sandbox, subscription engine, deterministic simulation, runtime/partition orchestration, networking/SDK, game server layer.
