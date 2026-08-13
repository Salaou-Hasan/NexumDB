# ADR-015 — Performance & Benchmarking

Status: accepted (Phase 15).

## Context

Nexum has grown through 14 phases into a full authoritative state engine
with deterministic simulation, OCC transactions, WAL, subscriptions,
networking, and a playable game. Before Phase 16 hardening, we need a
serious, reproducible performance baseline and evidence-based
optimizations. The system must be characterized as state grows to 10M rows.

## Decision

### D1 — Measure first, optimize second

No performance-sensitive code changes before a baseline exists for the
operation in question. The benchmark suite is the evidence base; the report
records MEASURED vs TARGET vs ESTIMATED distinctly.

### D2 — Scale is a hard requirement

Benchmarks run at 100K, 1M, 5M, and 10M rows for storage, transaction,
subscription, snapshot, and WAL operations. 25M is attempted if memory
permits. Inability to complete a scale on this hardware is documented
honestly (RAM available, attempted, failure mode, largest success) — never
faked or extrapolated.

### D3 — Correctness is invariant

The full workspace test suite, `cargo clippy --workspace --all-targets
--all-features -- -D warnings`, and `unsafe_code = forbid` gate every
optimization. Determinism, OCC semantics, read-your-writes, phantom
protection, WAL durability, subscription observation, WASM isolation, and
partition isolation are validated after every change.

### D4 — Small operations must scale with the changed set, not the table

The central characterization is that a 10M-row table with a one-row update
behaves like a 100K-row table with a one-row update. Operations that
measure O(N) in table size where O(1)/O(log N) is expected are treated as
bottlenecks to fix; O(N) where O(N) is inherent (scan, snapshot, replay)
is documented as such.

### D5 — Optimizations are the smallest justified change

Per the brief's order: measure, profile, form a hypothesis, make the
smallest change, run correctness tests, benchmark again, keep only if the
improvement is real and the architecture remains correct. No rewrites of
working systems without evidence; no second storage/transaction engine.

### D6 — Benchmark deliverable

A dedicated `benchmarks/nexum-bench` crate (release-mode, reproducible,
fixed seeds) covering the brief's subsystems: storage, transactions/OCC,
WAL/snapshot/recovery, reducers (native + WASM), subscriptions, simulation,
runtime/scheduler, parallel execution, multi-partition, networking/SDK,
and the Phase 14 game-server workload. Existing per-crate example
benchmarks are retained and reported alongside.

### D7 — Subscription per-change sync is incremental

A single changed row's key can cross the window boundary at most once per
commit, so the delivered view (the exact top-`window_cap` rows of the
window) is maintained incrementally: `Subscription` mirrors the window with
`row_keys` (`RowId → Key`) and keeps `visible_keys` (the exact top-cap
keys) alongside `visible_ids`. Every `sync_window` adjusts membership
locally (O(log N) plus the emitted deltas) instead of rebuilding the cap on
every commit. A debug-only invariant check recomputes the exact top-cap
and asserts equality after every sync — the observable contract of the
view is unchanged, only its maintenance cost. Measured: single-row
subscription deltas fell from ~575µs to ~1.4µs at 100K rows, and from
~9.9µs to ~1.5µs at 10M rows.

### D8 — Non-unique index removal is O(log n)

Non-unique index entries are a `BTreeSet<RowId>` instead of a `Vec<RowId>`,
so removing a row from a key's membership is O(log n) rather than a linear
`retain` over every row sharing that key. Consequence: `lookup` on a
non-unique index now returns ids in **ascending RowId order** (deterministic)
instead of insertion order — a documented ordering change that coincides
with insertion order wherever RowIds are allocated in insertion order.
Measured: a one-row update in a 10M-row table with a secondary index fell
from ~9.2µs to ~1.0µs.

### D9 — Repeated-identical-update benchmarks are invalid

`Table::update` with an identical row is a no-op (no epoch advance, no
Change) — a Phase 3 semantic. Any benchmark that updates a row to the same
values every iteration therefore measures only the first iteration and
records a meaningless average. Benchmarks that must measure the update
path alternate between two distinct row values so every iteration commits
a real change. Numbers captured before this fix are marked BOGUS in the
report; only alternating-value measurements are reported as MEASURED.

## Consequences

- The report (`docs/reports/15-performance.md`) records hardware, toolchain,
  methodology, results tables at every dataset size, scaling analysis,
  bottlenecks, optimizations with before/after numbers, correctness
  validation, and Phase 16 recommendations.
- Phase 16 (Production Hardening & Release) is NOT started.
