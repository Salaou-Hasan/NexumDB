# Changelog

All notable changes to Nexum will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.2.0] - 2026-08-26

### Phase 26 — Simulation Battery

- **Battery infrastructure**: six game-genre workloads (SOCIAL, FPS, MMO,
  RTS, SURVIVAL, EXTREME) driven by deterministic seeded player brains.
- **New gameplay reducers**: unit_move (RTS density), gather (RMW economy),
  presence (social RPC), relay_recv (cross-partition handler).
- **Gateway stage profiler**: collect/decode/dispatch/tick_update/deltas/
  results per-step timing.
- **Tick-phase breakdown**: calls/systems/commit wall times per world.
- **Server-only latency series**: excludes in-process client simulation.

### Phase 27 — Performance Campaign

#### Fixed
- WriteSet::absorb released child base Arc after make_mut, causing an
  O(parent-writes) deep clone per call (188x regression from Phase 22).
- Runtime input-frame merge: worlds now consume all queued same-tick
  frames in one tick instead of one frame per tick.
- GameServer::step_authoritative exposed the authoritative half of step()
  so host commands are flushed before the runtime tick.
- Benchmark double-drive bug: stray duplicate drive_profile call measured
  2x load, invalidating earlier 20K results.

#### Changed
- WASM engine fuel/epoch trap classification via structured Trap downcast.
- fire_weapon schedules staggered per client ((tick + i) % k) to eliminate
  manufactured burst bimodality in p99.
- Client pump/drain moved to dedicated pool; drain parallelized.
- Instant::now() hoisted out of per-message rate-limit path (one read per
  process_inbound batch).
- Unresolved-pending sweep skipped when all worlds are Running.
- Composite host ops LOOKUP_GET_UNIQUE and INDEX_SCAN_GET added (fire
  crossings reduced from 6-9 to 4-6).
- Movement stream system processes moves as batched InputFrames with
  O(1) HashSet occupancy checks (was O(log N) BTreeMap + row clones).
- TCP_NODELAY set on connect and accept (Nagle adds ~40ms on real nets).

#### Performance (E workload @ 20K CCU, server-only)
- server p50: ~60ms -> 31.3ms (-48%)
- server p99: ~96ms -> 46.2ms (-52%)
- calls phase: 50.5 -> 10.5-15.9ms (-69%)
- Result frames/tick: 22,800 -> 2,800 (-88%)

#### Battery results (all genres, server p99 ms)

| Workload | 1K | 5K | 10K | 20K |
|---|---|---|---|---|
| SOCIAL | 1.4 | 4.6 | 5.3 | 10.0 |
| FPS | 3.9 | 14.4 | 12.1 | 32.9 |
| MMO | 3.7 | 16.0 | 14.6 | 44.9 |
| SURVIVAL | 3.3 | 14.1 | 12.7 | 31.1 |

All runs: zero failed ticks, zero rejected, zero dropped, deterministic.

## [0.1.0] - 2026-08-19

### Phase 22.5 — Networking Hot-Path Aggressive Optimization

- **Arc-shared SubscriptionUpdate rows** — Insert/Update variants now wrap DeliveredRow in Arc, eliminating NxM deep clones in push_commit. sub_apply: 0.6ms → 0.0-0.1ms.
- **SubscriptionDeltaBatch protocol message** — Multiple deltas per subscription pump are batched into a single encoded frame. Reduces sub_deltas message count by ~28x.
- **Incremental Crc32 hasher** — Eliminates per-message crc_input Vec allocation on both encode and decode paths.
- **Removed redundant pump_subscriptions()** — fan_out_results already drains all subscribers.
- **Results**: Profile C @ 1K p99: 43.7ms → 28.8ms (1.5x). Profile E @ 1K p99: 48.0ms → 38.5ms (1.25x).

### Phase 22 — WASM & Transaction Overlay Optimization

- **COW WriteSet with Arc-based own layer** — branch() is now O(1) via Arc::clone (728x faster: 79µs → 109ns).
- **has_any_insert() skip** — lookup_unique/index skip O(N) pending-insert scan when no Insert entries exist (14x faster: 315µs → 22µs).
- **Absorb fast-path + try_unwrap** — Update-only workloads skip logical-view check and move entries instead of cloning.
- **Results**: Profile C @ 1K p99: ~573ms → 57.5ms (10x). Profile E @ 1K p99: ~1,094ms → 71.9ms (15x).

### Phase 21.5 — Extreme Execution Profiling

- Instrumented the entire authoritative pipeline at phase, sub-phase, per-reducer, and allocation granularity.
- Discovered WASM fire_weapon = 65–69µs/call ≈ 15x native.
- Established complete cost map: subscription fan-out, serialization, WASM, allocations.
- No optimizations applied — investigation phase only.

### Phase 21 — Networking & Serialization Hot-Path

- **Arc<[u8]> frames** — Transport frame type is now Arc. TickUpdate encoded once per world, cloned to every session by refcount bump.
- **Per-world attached index** — Fan-out pass changed from O(worlds x CCU) to O(CCU).
- **Results**: idle fan-out @ 10K: 5.2ms → 4.2ms (-19%). Movement fan-out @ 10K: ~15-20ms → ~12.6ms (-27%).

### Phase 20 — Interest Management / AOI

- **Duplicate-subscription grouping** — One shared derived view per distinct query, evaluated once per commit.
- **Bounded TickUpdate** — Broadcast no longer carries full change list; clients receive windowed subscription deltas.
- **Results**: Subscription evaluations per change: ~1,000 → 1.00. sub_apply: 11.4ms → 0.2ms (57x).

### Phase 19 — Hot-Path Profiling

- Instrumented tick path at phase + sub-phase level.
- Identified subscription all-to-all fan-out as #1 bottleneck (30.5ms/tick, 72% of tick).
- **Arc-shared Change rows** — sub_apply: 30.5ms → 11.4ms (2.7x).

### Phase 18 — Multi-Core Runtime

- Parallel world/partition execution across scoped threads.
- Deterministic merge in (worker_id, world_id) order — proven by exact trace-equality tests.
- **Results**: 8K clients x 8 partitions: workers=1 p99 103.6ms → workers=8 p99 52.4ms.
- Fixed gateway inbound O(N²): pending_calls scans → per-connection BTreeSet index (25.5ms → 2.3ms).

### Phase 17 — Gameplay Hot-Path & CCU Scaling

- Game reducers now use direct PK/index lookups (O(N) scans → O(1)).
- TickUpdate encode-once broadcast (51ms → 3ms at 1K).
- **Results**: Profile D @ 500: 83ms → 2.7ms (30x).

### Phase 16 — Production Hardening & Release

- Production config validation, rate limiting, graceful shutdown, observability.
- Release profile: LTO (fat), single codegen unit, panic=unwind.
- **CCU**: Connection-only 20K PASS (p99 32ms).

### Phase 15 — Performance & Benchmarking

- Micro + scale benchmarks (100K → 10M rows).
- PK lookup: 46ns. Single-row UPDATE: 0.97µs. Tick cost scales with active entity set, not table size.

### Phases 1–14 — Foundation

- Core types, table engine, storage engine, WAL/snapshots, reducer API, WASM sandbox, subscription engine, deterministic simulation, runtime/partition orchestration, networking/SDK, game server layer.
