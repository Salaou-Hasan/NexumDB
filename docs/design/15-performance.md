# Performance & Benchmarking — Design Notes (Phase 15)

This document answers the Phase 15 brief's design questions and defines the
benchmark methodology. The companion
[ADR-015](../architecture/15-performance.md) records the binding decisions;
this note is the worked reasoning.

## 1. Goal

Establish a **reproducible performance/scalability baseline** for Nexum as
authoritative state grows from 100K → 1M → 5M → 10M rows, identify real
bottlenecks from measurements, and fix only those bottlenecks that
measurements justify — without compromising correctness, determinism,
transactional semantics, or the authoritative simulation architecture.

The fundamental invariants are non-negotiable and are validated after every
optimization:

```text
ONE authoritative state
ONE World / partition
ONE tick
ONE logical transaction
ONE atomic commit
ONE Vec<Change>
```

## 2. Methodology

- **Measure first, optimize second.** No code is changed before a baseline
  exists for the operation being considered.
- **Release builds only** for conclusions (`--release`). Debug builds are
  recorded for reference but never used to justify an optimization.
- **Warmup before measurement** (100 warmup iterations in the existing
  example benchmarks; scale benchmarks warm the data set by construction).
- **Measure the full operation**, not a fragment: for a single-row update at
  10M rows, measure lookup + transaction + OCC validation + commit + change
  generation + WAL + subscription propagation, and each stage separately.
- **Report honest numbers.** MEASURED, TARGET, and ESTIMATED are
  distinguished. No extrapolated or faked results. If a dataset cannot run
  on this hardware, that is documented with the hardware limits.

### 2.1 Bench environment (recorded in the report)

CPU model, core/thread count, RAM, OS, Rust version, build profile, commit,
benchmark configuration, worker count, dataset size, reducer configuration.

## 3. Dataset sizes

| Size | Required | Notes |
|------|----------|-------|
| 100K | yes | baseline small-scale |
| 1M   | yes | medium |
| 5M   | yes | **hard requirement** |
| 10M  | yes | **hard requirement** |
| 25M  | if memory permits | documented either way |

If a size cannot be completed safely, the report documents: available RAM,
attempted dataset, failure/resource limit, and the largest successful
dataset. No faked or extrapolated results.

## 4. Critical scale characterization

For every dataset size, the central question is whether small operations
remain **scale-independent** rather than degrading to O(N):

> 10M rows + one-row update ≈ 100K rows + one-row update?

Operations characterized as O(1)-like, O(log N)-like, or O(N)-like from
measurements (never claimed as formal complexity from benchmarks alone):

- primary-key lookup
- single-row update through a full transaction
- single-row subscription delta
- random lookup
- table scan (expected O(N))
- snapshot creation (expected O(N))
- WAL replay (expected O(N))

## 5. Workload definitions

Per subsystem, micro-benchmarks (single operation, tight loop) and system
benchmarks (end-to-end, realistic mixes):

- **Storage/table**: insert, batch insert, PK lookup, random lookup,
  sequential lookup, update, delete, scan, indexed lookup, iteration.
- **Transaction/OCC**: read/write sets at 1/10/100/1K/10K rows; read-only /
  read-write / write-only; conflict rates 0/1/10/50/100%.
- **Reducers**: native vs WASM at 1/10/100/1K calls per tick.
- **Simulation**: 10/100/1K/10K/100K entities per tick; empty, input-heavy,
  transaction-heavy, reducer-heavy, mixed gameplay ticks. **Also** the
  persistent-world scenario: 10M rows with only 100/1K/10K active entities
  per tick, measuring whether huge authoritative data inflates tick cost.
- **Subscriptions**: snapshot generation, delta, resync; 1/10/100/1K
  subscribers; the critical test is a 10M-row table with a 1-row update —
  subscription work must scale with changed rows, not table size.
- **WAL**: append latency/throughput, batched append, durable vs in-memory.
- **Snapshot/recovery**: capture, write, restore, replay, verify
  recovered == original, and that recovery emits no live subscription
  updates.
- **Parallel execution**: workers 1/2/4/8; independent vs conflicting
  systems; speedup = serial/parallel; serial == parallel final state,
  Vec<Change>, events, RNG, outbound messages.
- **Multi-partition**: 1/2/4/8/16 partitions; local vs cross-partition ops;
  identical topology+inputs+seed ⇒ identical traces.
- **Runtime/scheduler**: 1/10/100/1K/10K worlds; fixed per-world overhead.
- **Network/SDK**: encode/decode, session, routing, delta/snapshot
  serialization, view application; 1/10/100/1K clients.
- **Game server**: the Phase 14 playable arena at 2/4/16/32/64/128/256
  players with a mixed movement/combat/respawn/join/leave/subscription
  workload — the real game, not a synthetic approximation.

## 6. Measurement strategy

- **Latency**: mean ns/op (or µs/op) over N iterations after warmup.
- **Throughput**: ops/sec derived from latency; messages/sec where relevant.
- **Memory**: process RSS before/after dataset construction, per-row bytes,
  peak during bulk ops; looked-for artifacts: accidental duplication,
  retained allocations, unbounded queues, leaks, unexpected copies.
- **p50/p95/p99**: recorded where practical; the suite primarily reports
  means with iteration counts for reproducibility.

## 7. Reproducibility & regression thresholds

- A `--seed` is fixed per run; deterministic workloads are used so repeated
  runs agree.
- Numbers are recorded as **data** in the report, not hard-coded test
  assertions. The test suite keeps correctness tests only; gross-regression
  benchmarks (if added) use generous, hardware-independent bounds to avoid
  flakiness (brief §V).

## 8. Optimization rules

Order: **Measure → Profile → Identify bottleneck → Form hypothesis → Smallest
justified change → Run correctness tests → Benchmark again → Compare → Keep
only if real and architecture-correct.**

Do NOT: optimize blindly, rewrite working systems without evidence, remove
safety/validation checks, bypass WAL/subscriptions/`World::tick`/OCC, create
a second storage or transaction engine, sacrifice determinism, or weaken
WASM isolation.

Every optimization is validated by the full workspace test suite plus
`cargo clippy --workspace --all-targets --all-features -- -D warnings` and
`unsafe_code = forbid`.

## 9. Deliverables

- `docs/design/15-performance.md` (this file)
- `docs/architecture/15-performance.md` (ADR-015)
- a benchmark crate (`benchmarks/nexum-bench`) covering micro + scale
  workloads, runnable in release mode with a single command
- `docs/reports/15-performance.md` — results tables, scaling analysis,
  bottlenecks, optimizations with before/after numbers, correctness
  validation, Phase 16 recommendations
- README phase table updated (Phase 15 ✅, Phase 16 still pending)
