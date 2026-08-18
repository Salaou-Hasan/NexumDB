# Implementation Reports

Phase-by-phase implementation reports for Nexum. The companion design notes
and architecture decisions (ADRs) live in
[`docs/design`](../design/README.md) and [`docs/architecture`](../architecture).

## Reports

| Report | Phase | Summary |
|---|---|---|
| [11-12-concurrency-partitions.md](11-12-concurrency-partitions.md) | 11 + 12 | Concurrency & parallel execution + multi-partition simulation: worker-count independence, deterministic parallel ticks, partition ownership, cross-partition messaging. |
| [11-networking-control-plane.md](11-networking-control-plane.md) | 13 (early) | The networking layer implemented ahead of the roadmap — now the canonical Phase 13 foundation. |
| [13-networking-sdk.md](13-networking-sdk.md) | 13 | Networking + client SDKs: sessions, protocol, reducer calls, subscriptions, multi-partition routing, recovery, security review. |
| [14-game-server.md](14-game-server.md) | 14 | Game server layer: game instances, players, lifecycle, reducer exposure, failure semantics. |
| [14b-playable-game.md](14b-playable-game.md) | 14 | The actual playable multiplayer arena game on the Nexum stack. |
| [15-performance.md](15-performance.md) | 15 | Performance & benchmarking: methodology, results, bottlenecks, before/after. |
| [16-production.md](16-production.md) | 16 | Production hardening & release: config, rate limits, shutdown, observability, CCU load measurements, security findings. |
| [17-gameplay-hotpath.md](17-gameplay-hotpath.md) | 17 | Gameplay hot-path & CCU scaling: removed O(N) reducer scans, encode-once broadcast, subscription fan-out ceiling. |
| [19-hotpath-profiling.md](19-hotpath-profiling.md) | 19 | Execution hot-path profiling: measured ranked bottlenecks (subscription fan-out 72% of tick), Arc-shared row payloads — sub_apply 30.5ms → 11.4ms (2.7×). |
| [20-interest-management.md](20-interest-management.md) | 20 | Interest management / AOI: duplicate-subscription grouping (evaluations/change ~1,000 → 1.00, sub_apply 57×) + bounded TickUpdate; measured ladder A@10K & B@1K PASS. |
| [18-multi-core.md](18-multi-core.md) | 18 | Multi-core runtime: parallel world/partition ticks (ADR-018, deterministic — exact trace equality vs serial) + gateway inbound O(N²) fix. 8K×8p movement p95 62.3ms → 31.7ms; inbound 25.5ms → 2.3ms. |
| [21-networking-hotpath.md](21-networking-hotpath.md) | 21 | Networking/serialization hot-path: Arc-shared broadcast frames (zero-copy TU, 10K allocs/tick saved) + per-world attached index (O(worlds×CCU) → O(CCU) scans); D2 batching measured net-negative and reverted. Movement fan-out −23…27%, p99 72.9 → 64.7 ms @ 10K. |

## CCU summary (Phases 16–18)

Post-Phase-18 ladder (8–16 partitions × 8–16 workers, release, 20 Hz):

- **Connection-only: 20K PASS** — p99 32 ms (10K 12 ms, 15K 19 ms, 20K
  32 ms) vs Phase 16's 15K 63.7 ms / 20K 75.5 ms DEGRADED.
- **Movement (profile B): 10K DEGRADED** (p95 40 ms, p99 73 ms), **15K
  SATURATED** (p95 65 ms, p99 98–115 ms) — bounded by the O(clients)
  gateway reducer-result fan-out + SDK decode/drain (Phase 21), not the
  multi-core world tick (~2 ms avg at 15K). Phase 21 (Arc frames + attached
  index) improved the movement fan-out phase −23…27% and movement p99
  72.9 → 64.7 ms @ 10K (B@15K 16×16: p95 64.6 → 59.0, p99 97.6 → 92.4 ms),
  but the movement tick remains bound by the sum of O(CCU) per-client work.
- **Realistic (profile C):** movement plus the simultaneous fire burst
  (wasmi re-instantiation per call): 10K ≈ 0.8 s, 15K ≈ 1.0 s fire tick
  (Phase 22). 15–20K *gameplay* CCU is NOT yet claimed.
- **Memory (measured RSS, profile A):** steady state ≈ 5.7 MB + 24.7 KB
  private per connection (10K 251 MB, 15K 376 MB, 20K 502 MB); a mass
  join storm without client consumption spikes several× (20K peak
  4.1 GB, O(N²) un-drained SDK buffers — settled in ~2 s).

See [18-multi-core.md](18-multi-core.md), [16-production.md](16-production.md),
[17-gameplay-hotpath.md](17-gameplay-hotpath.md), and
[20-interest-management.md](20-interest-management.md).

## Benchmark summary (Phase 15)

Environment: Intel Core i7-14650HX (16 cores / 24 threads), 16 GB RAM,
Windows, rustc 1.97.1, **release** builds. Dataset sizes: 100K / 1M / 5M /
10M rows (largest successfully tested: 10M; 25M not attempted — RAM
headroom). Full numbers, methodology, and scaling analysis in
[15-performance.md](15-performance.md).

| Metric | 100K | 1M | 5M | 10M |
|---|---|---|---|---|
| construct (rows/s) | 1.39M | 1.41M | 1.78M | 1.69M |
| PK lookup | 46 ns | 47 ns | 51 ns | 45 ns |
| random lookup | 105 ns | 313 ns | 374 ns | 520 ns |
| **UPDATE one row (tx + OCC + commit)** | **968 ns** | **984 ns** | **904 ns** | **954 ns** |
| full table scan | 789 µs | 9.7 ms | 50.8 ms | 87.8 ms |
| subscription initial snapshot (10K delivered) | 33 ms | 293 ms | 1.67 s | 3.70 s |
| single-row subscription delta | 1.33 µs | 1.32 µs | 1.35 µs | 1.39 µs |
| snapshot capture + write | 18 ms | 167 ms | 808 ms | 1.59 s |
| snapshot restore | 68 ms | 592 ms | 3.2 s | 7.6 s |
| estimated table memory | 8 MB | 84 MB | ≈420 MB | ≈840 MB |

### Headline findings

1. **A one-row update at 10M rows behaves like one at 100K rows** — the
   UPDATE path (tx + OCC + commit + index maintenance) is flat at ~0.9–1.0 µs
   across the whole range. Cost scales with the changed set, not the table.
2. **Tick cost scales with the active entity set, not total rows**: 214 ns
   per tick touching 100 active entities in a 10M-row store.
3. **Subscription deltas are O(log N) per committed change** (1.4 µs at 10M
   rows), following the Phase 15 fixes. Initial snapshots remain O(N) by
   design (the window holds all matching rows) — 3.7 s at 10M.
4. **Accidental O(N) was found and fixed**: a non-unique-index removal was
   linearly scanning every row sharing a key; a one-row update at 10M went
   from 9.2 µs to 0.95 µs.

### Micro highlights

| Op | ns/op |
|---|---|
| native reducer call (10/tick) | 304 ns |
| WASM reducer call (sandbox) | 14–50 µs |
| game command routing (`submit_command`) | 81 ns |
| empty tick (step) | 347 ns |
| WAL append (flush / sync) | 2.9 µs / 245 µs |
| frame decode / TickUpdate encode | 142 ns / 553 ns |
| 1000 connections per tick | 538 µs |

Correctness is invariant across every optimization: 616 workspace tests, 0
failures; `cargo clippy --workspace --all-targets --all-features -- -D
warnings` clean; `unsafe_code = forbid`; determinism suites (serial ==
parallel == any worker count, partition traces) green.
