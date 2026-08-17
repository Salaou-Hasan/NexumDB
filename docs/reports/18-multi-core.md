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
