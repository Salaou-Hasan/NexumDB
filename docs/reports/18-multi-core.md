# Phase 18 — Multi-Core Runtime: Report

Status: complete. The runtime's tick phase now executes independent
worlds/partitions concurrently (ADR-018), and the benchmark that exposed it
also uncovered and fixed a gateway inbound O(N²). All numbers below are
**MEASURED** (release build, in-process transport, real gateway/runtime/
world/subscription/SDK path) on the reference machine.

## 1. Environment

| | |
|---|---|
| CPU | Intel Core i7-14650HX (24 logical / 16 physical cores) |
| RAM | 16 GB DDR5 |
| OS | Windows 11 (win32, Git Bash) |
| Build | `cargo build --release`, LTO, `unsafe_code = forbid` |
| Transport | in-process (protocol/gateway/runtime/world/SDK are real) |
| Tick rate | 20 Hz (50 ms budget) |
| Workload | CCU harness profile B (movement every 4th tick) and C (realistic + fire) |

## 2. What was implemented

### ADR-018 — parallel world/partition tick execution

- `Runtime::step` / `step_detailed` tick **independent worlds concurrently**
  when `worker_count > 1` and more than one world is running: scoped threads,
  one per worker, over deterministic chunks of the `(worker_id, world_id)`
  ordered world list. `worker_count == 1` keeps the serial path (no spawn —
  the correctness oracle).
- The per-world tick body was refactored into `tick_entry_collected`, which
  **collects** events, metric deltas, and outbound messages into a
  `TickOutcome` instead of mutating shared runtime state. The main thread
  applies outcomes in the exact serial `(worker_id, world_id)` order.
- The delivery phase (ADR-012 D3) stays serial and precedes the tick phase;
  worlds are reinserted even on a thread panic.
- No unsafe: disjoint `&mut WorldEntry` via `split_at_mut`; threads share
  only immutable references.

### Gateway inbound O(N²) fix (discovered by the Phase 18 benchmark)

`NetworkGateway::handle_call_reducer` ran **two full `pending_calls.values()`
scans per incoming reducer call** (pending-count + request-id-reuse checks).
At 8K clients × 1 pending call each, a movement tick performed ~64M predicate
evaluations — ~100 ms/tick of `process_inbound`, independent of partitions.
Replaced with a per-connection `BTreeSet` index (`pending_by_connection`,
kept in lockstep with `pending_calls`): both checks are now O(log n).
Measured: inbound phase **25.5 ms → 2.3 ms avg** (11×).

## 3. Determinism validation

New regression tests (`nexum-runtime`):

- `parallel_step_matches_serial_step_exactly` — 6-ring partition topology
  (multi-sender destinations), 5 steps, worker counts {1, 2, 4, 6}: identical
  per-world `Vec<Change>`, outbound message streams, final tick numbers, and
  metric aggregates.
- `parallel_step_emits_events_in_deterministic_order` — the `TickCompleted`
  stream arrives in the exact `(worker_id, world_id)` order for {2,4,6}
  workers × {6,9} worlds.
- `parallel_step_preserves_failure_isolation` — a failing world fails at the
  same tick at any worker count; reports, event multiset, and tick numbers
  are identical (this test caught a real bug: the first parallel path ticked
  `Failed` worlds; fixed).

Workspace: **649 tests pass, 0 failed** (646 + 3 new).

## 4. Benchmarks (8000 clients, 8 partitions, profile B, 120 ticks)

Round-trip tick (server step + client pumps + drain):

| Workers | p50 | p95 (movement) | p99 | avg | Class |
|--------:|----:|---------------:|----:|----:|-------|
| 1 | 8.1 ms | 62.3 ms | 103.6 ms | 23.5 ms | SATURATED |
| 2 | 7.8 ms | 46.9 ms | 93.2 ms | 18.9 ms | DEGRADED |
| 4 | 7.9 ms | 37.3 ms | 62.5 ms | 16.3 ms | DEGRADED |
| 8 | 7.8 ms | 31.7 ms | 52.4 ms | 15.2 ms | DEGRADED |
| 12 | 7.9 ms | 33.8 ms | 57.2 ms | 15.5 ms | DEGRADED |
| 24 | 8.0 ms | 33.2 ms | 51.7 ms | 15.7 ms | DEGRADED |

Phase breakdown (avg/tick; profile-detail):

| Phase | workers=1 | workers=8 |
|-------|----------:|----------:|
| inbound (gateway decode+dispatch) | 2.3 ms | 2.3 ms |
| **tick (runtime step + gateway fan-out)** | **15.3 ms** | **7.4 ms** |
| clients (SDK pump) | 2.8 ms | 2.8 ms |
| drain (SDK consume) | 1.7 ms | 1.8 ms |

Profile C (1000 clients, 4 partitions, 200 ticks — realistic movement +
occasional fire):

| Workers | p50 | p95 | p99 | Class |
|--------:|----:|----:|----:|-------|
| 1 | 0.7 ms | 45.6 ms | 150.1 ms | SATURATED |
| 4 | 1.0 ms | 37.7 ms | 93.7 ms | DEGRADED |

## 5. Analysis

- **The parallel tick works.** The serial tick phase for 8 worlds × 1000
  moves (≈ 60 ms on movement ticks) drops to ~24 ms with 8 workers; the
  runtime step itself goes 15.3 → 7.4 ms avg. Scaling is clean from 1 → 8
  workers, then plateaus — with only 8 worlds there is nothing left to
  parallelize, and at 12–24 workers the extra threads idle (no regression,
  no oversubscription penalty measured).
- **Parallel efficiency is bounded by the non-parallel remainder.** A
  movement tick round trip is inbound (~9 ms) + world ticks (parallel ~8 ms)
  + gateway result fan-out + client-side SDK decode/drain (~linear in
  clients). Only the world-tick slice is multi-core; the gateway and SDK
  slices are single-threaded and O(clients). Hence p95 improves ~2×, not 8×.
- **The gateway inbound O(N²) was the dominant pre-fix cost** and is now
  fixed; inbound is linear (~1 µs per client frame).
- **Idle ticks are flat at ~8 ms** at every worker count (8K connections +
  client pumps/drain) — the fixed per-tick baseline, not the world tick.

## 6. Bottlenecks (ranked, remaining)

1. **Client-side SDK decode/drain + gateway fan-out** (O(clients) per tick)
   — Phase 21 (networking/serialization) target.
2. **WASM fire bursts** (per-call wasmi re-instantiation; the ~94 ms p99 in
   profile C is the simultaneous 1000-fire tick) — Phase 22 target.
3. **Per-tick thread spawn + merge** — negligible at 20 Hz (~µs), measured,
   not a bottleneck.
4. **Within-world system parallelism** (Phase 11 planner) — unchanged and
   orthogonal; applies when a single world has many parallelizable systems.

## 7. Honest conclusions

- Multi-core world/partition ticking is real, deterministic, and safe: the
  single-threaded path remains the oracle, and 649 tests (including exact
  serial/parallel trace equality with cross-partition messaging) pass.
- 15K–20K gameplay CCU is NOT yet claimed: at 8K clients the movement tick
  is ~50 ms p99 (at the 20 Hz budget) and the remaining ceiling is the
  O(clients) gateway/SDK path plus WASM fire cost, not the world tick.
- Next measured steps: Phase 21 (network/serialization), Phase 22 (WASM
  reuse), then re-running the CCU ladder toward 10K/15K.

## 8. Post-Phase-18 CCU ceiling (10K / 15K / 20K ladder)

Run after the Phase 18 merge on the same reference machine (release, 20 Hz,
in-process transport, real gateway/runtime/world/subscriptions/SDK).
Longer runs (120–150 ticks) are used because the first-run p99 occasionally
contains a one-off warmup/scheduler spike (e.g. A@10K first run 242 ms;
steady state 12 ms). All runs: 0 tick failures, 0 drops, 0 rejections.

| CCU | Profile | P×W | p50 | p95 | p99 | Class |
|----:|---------|-----|----:|----:|----:|-------|
| 10K | A (conn) | 8×8 | 10.0 ms | 11.5 ms | 12.1 ms | **PASS** |
| 15K | A (conn) | 8×8 | 17.0 ms | 18.4 ms | 19.3 ms | **PASS** |
| 20K | A (conn) | 8×8 | 25.4 ms | 27.3 ms | 32.0 ms | **PASS** |
| 10K | B (move) | 8×8 | 10.7 ms | 39.5 ms | 72.9 ms | DEGRADED |
| 15K | B (move) | 8×8 | 17.9 ms | 66.5 ms | 114.5 ms | SATURATED |
| 15K | B (move) | 16×16 | 21.5 ms | 64.6 ms | 97.6 ms | DEGRADED |
| 10K | C (real) | 8×8 | 11.5 ms | 156 ms | 815 ms | SATURATED (fire) |
| 15K | C (real) | 16×16 | 21.5 ms | 341 ms | 1.0 s | SATURATED (fire) |

### Connection-only ceiling: 20K PASS

The steady-state idle per-tick baseline is linear in connections (~1 ms per
1K: 10K ≈ 10 ms, 15K ≈ 17 ms, 20K ≈ 25 ms) and passes the 50 ms budget at
20K. Phase 16 measured A@15K 63.7 ms / A@20K 75.5 ms (both DEGRADED); now
**15K 19.3 ms and 20K 32.0 ms p99 — PASS**. Extrapolating the linear
baseline, connection-only saturates around ~35–40K on this machine, but the
sequential connect path degrades first (15K ≈ 34 s, 20K ≈ 66 s ≈ 300–440
conn/s) — a warmup-path cost, not steady state.

### Movement ceiling: ~10–12K gameplay CCU

Movement ticks (profile B) scale with the O(clients) **gateway reducer-
result fan-out + SDK decode/drain** — now the dominant cost, not the world
tick (parallel world_tick sub-phase is ~2 ms avg at 15K). 10K movement ≈
40 ms p95 (DEGRADED, near budget); 15K movement ≈ 65 ms p95 (over
budget). 16×16 marginally improves p99 (114 → 98 ms) but raises the
per-tick baseline (more worlds). No silent loss at any scale.

### Fire-burst ceiling (WASM, Phase 22)

The simultaneous fire tick (profile C, every 100 ticks) re-instantiates
wasmi per call: 10K fires ≈ 0.8 s, 15K ≈ 1.0 s — parallel worlds cut this
~8× vs the pre-Phase-18 serial ~5.5 s, but it remains the hard p99 spike.
Phase 22 (instance/linker reuse) is the fix.

### Honest statement

- **Connection-only: 20K PASS** (p99 32 ms < 50 ms budget).
- **Gameplay: ~10K movement DEGRADED (p99 73 ms), 15K movement SATURATED
  (p99 98–115 ms)** — bounded by gateway/SDK O(clients) work (Phase 21)
  and the WASM fire burst (Phase 22), not by the multi-core world tick.
- 15–20K *gameplay* CCU is therefore NOT yet claimed.

## 9. Measured process RSS (10K / 15K / 20K)

Measured with a PowerShell sampler polling the harness process's
`WorkingSet64` / `PrivateMemorySize64` every 200 ms through the whole run
(connect + warmup + measured phase). All runs: profile A (connection
only), 8 partitions × 8 workers, 20 Hz. The full stack — server **and**
the in-process SDK clients — lives in one process, so these are
end-to-end numbers (a real deployment's server-only per-connection cost
is a subset, unmeasured separately).

| CCU | steady WS | steady private | connect/join-storm peak WS |
|----:|----------:|---------------:|---------------------------:|
| 5K  | 132 MB    | 131 MB         | 320 MB                    |
| 10K | 245 MB    | 251 MB         | 1,106 MB                  |
| 15K | 362 MB    | 376 MB         | 2,279 MB                  |
| 20K | 481 MB    | 502 MB         | 3,827 MB                  |

(Steady state is the plateau after the join burst settles — reached in the
first warmup ticks; the last sample is excluded because it catches process
teardown.)

**Per-connection cost (linear fit over the four steady-state points):**

```text
private ≈ 5.7 MB + 24.7 KB × CCU     (R² ≈ 1.0: 129/253/377/500 vs 131/251/376/502 MB)
working set ≈ 13 MB + 23.3 KB × CCU
```

So the answer to "bytes per connection": **~24.7 KB private per
connection at steady state, end-to-end** (dominated by the memory-transport
buffers, SDK view/event state, connection/session entries, and subscription
windows). At 20K CCU that is ~500 MB private — comfortably within the
16 GB machine. The harness's earlier `est.~27MB` line (2 KB/conn) was
wrong by ~12× and is now calibrated to the measured fit.

**Join-storm peak (operational caveat):** a mass connect/join without
client consumption spikes memory several× — ~2.4× at 5K, ~4.7× at 10K,
~6.3× at 15K, ~8× at 20K (4.1 GB peak). The superlinear growth is the
un-drained SDK event buffers during connect (each new join delivers a
delta to every already-subscribed client — O(N²) buffered deltas until the
measured phase starts draining); it settles in ~2 s once clients consume.
Real clients that drain every frame avoid this; it is also bounded
server-side by the subscription overflow→stale policy (ADR-008). Worth an
explicit stress test in Phase 26 (reconnect storms) and a bounded SDK event
buffer in Phase 21.
