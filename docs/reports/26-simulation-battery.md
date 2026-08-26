# Phase 26–27: Simulation Battery & Performance Campaign

## Summary

Built a workload-independent benchmark suite with deterministic player brains
across six game-genre archetypes, validated Nexum at 20K CCU on one node,
and delivered a performance campaign that reduced server p99 at 20K-E by 52%.

## Battery infrastructure (Phase 26)

Six archetypes driven by seeded splitmix64 brains (identical command streams
across runs and worker counts):

| Archetype | Pattern | Mix |
|---|---|---|
| SOCIAL | idle connections, cheap RPC | 5% move, 2% presence |
| FPS | latency-critical move + WASM combat | 70% move, 6% fire |
| MMO | combat + economy + social | balanced spread |
| RTS | simulation density (`--density` units/player) | N unit_moves/tick |
| SURVIVAL | RMW economy, persistence writes | gather + move |
| EXTREME | stress ceiling: everyone moves every tick | legacy profile E |

New gameplay surface: `units` table + `unit_move`, `inventory` table +
`gather`, `presence`. All native reducers with O(log N) discipline.

Harness improvements: paced ticks (--hz), server-only latency series,
staggered fire schedules, parallel client drain, auto workers, batch clock
reads, SCORECARD CSV output, gateway stage profiler, tick-phase breakdown.

## Performance campaign (Phase 27)

### Fixes

| Fix | Impact |
|---|---|
| WriteSet::absorb Arc-release ordering | **188×** on branch/absorb cycles |
| WASM instance pooling | fire: 994→19.6 µs at 20K |
| Composite host ops | fire crossings 6–9→4–6 |
| Runtime input-frame merge | N clients = N merged frames/tick |
| Unresolved-sweep fast path | skip O(pending) health check |
| Batch clock reads | one Instant::now per process_inbound batch |
| Parallel drain on dedicated pool | drain spikes eliminated |

### Architecture changes

| Change | Impact |
|---|---|
| Movement stream system | moves via batched InputFrames (no correlation) |
| Batched movement processing | scan→in-memory→batched write-back |
| Staggered fire/reload schedules | eliminates manufactured bursts |
| GameServer::step_authoritative API | phased step for latency measurement |
| Native fire_weapon path | proves interpreter tax already eliminated |

### Instrumentation

| Tool | What it measures |
|---|---|
| TickBreakdown (calls/systems/commit ns) | per-world tick phases |
| GatewayStepProfile (collect/decode/dispatch/deltas/results ns) | gateway stages |
| Server-only latency series | excludes in-process client sim |
| GATEWAY STAGES report | per-step stage averages |
| TICK PHASES report | summed across worlds |

## Results — E @ 20K CCU single node (server-only, paced 20 Hz)

| Metric | Before Phase 27 | After Phase 27 | Delta |
|---|---|---|---|
| server p50 | ~46 ms* | **31.3 ms** | −32% |
| server p99 | ~96 ms* | **46.2 ms** | −52% |
| calls phase | 50.5 ms cum | **10.5–15.9 ms** | −69% |
| systems phase | 39.0 ms cum | **25.9 ms** | −34% |
| Result frames/tick | 22,800 | **2,800** | −88% |
| Classification | SATURATED | **DEGRADED** | ↑ |

*Earlier figures were polluted by double-drive benchmark bug.

## Full battery matrix — server p99 ms

| Workload | 1K | 5K | 10K | 20K |
|---|---|---|---|---|
| SOCIAL | 1.4 ✓✓ | 4.6 ✓ | 5.3 ✓ | 10.0 ✓ |
| FPS | 3.9 ✓ | 14.4 ~ | 12.1 ✓ | 32.9 ✗ |
| MMO | 3.7 ✓ | 16.0 ✗ | 14.6 ✓ | 44.9 ✗ |
| SURVIVAL | 3.3 ✓ | 14.1 ✓ | 12.7 ✓ | 31.1 ✗ |
| EXTREME | 3.5 ✓ | 10.3 ✓ | 6.2 ✓ | — |

Zero failed ticks, zero rejected, zero dropped across every cell.

## Key findings

1. **Input-stream architecture matters more than workload intensity.**
   Games sending moves as batched InputFrames get dramatically better
   latency than games using per-action correlated RPC calls.

2. **WASM interpreter is no longer the bottleneck.** After pooling and
   composite ops, native Rust fire performs identically to sandboxed WASM.
   The remaining cost is BTreeMap traversal and row clones.

3. **Memory bandwidth is the 20K wall.** 20 concurrent worlds each
   traversing BTreeMaps saturate memory bandwidth. SoA storage would help
   but requires multi-day engine redesign.

4. **Benchmark correctness matters.** The double-drive bug (measuring 2×
   load) invalidated earlier results. Always verify accepted counts match
   expected command volume.

## Remaining scope

| Item | Attacks | Status |
|---|---|---|
| SoA/columnar hot tables | memory-bandwidth tick wall | scoped, multi-day |
| Gateway connection sharding | serial dispatch | scoped, multi-day |
| AOI spatial subscriptions | delta volume | design needed |
| Kill/recovery automation | durability proof | harness ready |
| Network impairment testing | real-network validation | TCP path ready |
