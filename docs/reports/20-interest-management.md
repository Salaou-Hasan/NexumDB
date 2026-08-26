# Phase 20 — Interest Management / AOI: Report

Status: complete. The subscription fan-out was restructured from
**evaluate-every-change-against-every-subscription** to
**evaluate-once-per-distinct-query**, and the redundant full-change
`TickUpdate` broadcast was made bounded. Both changes are delivery/view
optimizations; authoritative state, transactions, OCC, WAL, and
determinism are untouched.

## 1. The Measured Problem (Phase 19 → 20)

Every client subscribed to the same query (the CCU harness uses
`Query::builder("players").limit(32)`, the arena game client uses
`Query::builder(TABLE)`), so at 1K clients a movement tick performed
1,000 changes x 1,000 subscriptions = **1,000,000 identical
`apply_change` evaluations** — 1,000 identical windows, 1,000x the
maintenance, 1,000x the delta computation. Measured: `sub_apply`
= 11.4 ms/tick (65% of tick) after Phase 19. Separately, every client
decoded the **full change set** broadcast in `TickUpdate` (1,000
changes) though its window is 32 rows — 6.6 ms/tick of redundant client
decode.

## 2. What Was Implemented

### D1 — Duplicate-subscription grouping (the core fix)

`SubscriptionRegistry` now shares **one derived view per distinct
query** (ADR-020 D1). `apply_changes` evaluates each view once per
change into a scratch delta stream, then fans the stream out to every
member's independent buffer:

- Evaluations per change: **N → #distinct_queries** (1 for the harness
  and the arena game).
- The shared view holds `window`/`row_keys`/`visible_keys`/`visible_ids`
  once per distinct query; members keep only id, query, state, cursor,
  and their bounded buffer.
- Per-member semantics preserved exactly: distinct ids, independent
  buffers, overflow → stale, unsubscribe/resync/refresh, atomic
  establishment, drop detection.
- Counters (`ApplyReport::evaluations/deltas`,
  `RegistryStats`, `RuntimeMetrics.subscription_evaluations/
  subscription_deltas/subscription_views/changes_committed`) expose the
  workload metric.

### D2 — Bounded TickUpdate

`NetworkConfig::tick_update_changes` (default **false**): the `TickUpdate`
broadcast carries tick metadata + events but **not** the full change
list — clients receive windowed `SubscriptionDelta` frames as the
delivery path. Removes the O(changes x clients) decode and the redundant
per-tick bandwidth. Opt in for full-change diagnostics.

## 3. Measured Before / After (release, in-process, 20 Hz, 1,000 clients unless noted)

| Metric | Phase 19 | Phase 20 | Δ |
|--------|----------|----------|---|
| **subscription evaluations per change** | ~1,000 (1M/tick) | **1.00** | **1000x less work** |
| sub_apply avg/tick (profile C) | 11.4 ms | **0.2 ms** | **57x** |
| sub_apply % of tick | 65.4% | 3.5% | |
| clients decode avg/tick (profile C) | 4.0 ms | **1.4 ms** | 2.9x |
| p50 tick (profile C) | 0.8 ms | **0.6 ms** | |
| p95 tick (profile C) | 204 ms | **29 ms** | 7x |
| avg tick (profile C) | 31 ms | **9.9 ms** | 3.1x |

### CCU ladder

| Profile | CCU | p50 | p95 | p99 | Classification |
|---------|-----|-----|-----|-----|----------------|
| A (connection only) | 10K | 8.4 ms | 9.8 ms | 10.7 ms | **PASS** (was p99 35 ms) |
| B (light gameplay) | 1K | 0.6 ms | 11.6 ms | 31.4 ms | **PASS** (was SATURATED, p99 153 ms) |
| B | 2.5K | 1.7 ms | 33 ms | 58 ms | DEGRADED (just over budget) |
| C (realistic) | 1K | 0.6 ms | 29 ms | 266 ms | SATURATED (fire ticks) |
| C | 2.5K | 1.9 ms | 40 ms | 1.46 s | SATURATED (fire ticks) |
| D (stress) | 500 | 0.7 ms | 42 ms | 157 ms | SATURATED (fire ticks) |
| D | 1K | 6.2 ms | 85 ms | 300 ms | SATURATED (fire ticks) |

Attribution (profile C @ 1K): grouping alone drops `sub_apply`
11.4 → 0.2 ms; bounded TickUpdate alone drops client decode
4.0 → 1.4 ms.

## 4. The Remaining Measured Bottleneck: the WASM fire burst

Per-tick timing shows the remaining p99/p95 spikes are the **fire
burst** ticks (profile C fires all 1,000 clients simultaneously every
100 ticks):

- Fire tick @ 1K: **~666 ms total — server 550 ms** for 1,000 WASM
  `fire_weapon` calls (~550 µs/call).
- Movement ticks (steady state): ~12 ms server for 1,000 moves.
- Idle ticks: **0.6 ms** — the connection/subscription base is now
  cheap.

Cause: the WASM host (`nexum-wasm`/wasmi) **re-instantiates per
invocation** — a fresh `Store`, `Linker`, and module instantiation per
call (`run_module`). 1,000 calls = 1,000 instantiations. This is the
explicit **Phase 22** target (WASM instance/linker reuse, batched
host calls) — the fire tick cost scales linearly with clients (2.5K
clients → ~1.4 s).

Movement ticks at ~12 ms/1,000 moves are the linear world-tick cost
(Phase 18 multi-core / Phase 21 allocation territory).

## 5. Correctness Validation

- **646 workspace tests pass** (642 + 4 new grouping tests), 0 failures.
- New regression tests: identical queries share one view and evaluate
  once (`evaluations == changes`, identical delta streams); distinct
  queries are NOT grouped; unsubscribe leaves other members intact and
  frees orphan views; a member joining a live group snapshots the
  current view.
- Grouping is value-identical to the per-subscription path: identical
  queries produce identical windows and delta streams (same derivation
  code, same ordering); the pre-existing suite (window-cap, resync,
  backpressure, e2e, WAL/recovery) passes unchanged.
- `unsafe_code = forbid`; determinism and FIFO ordering preserved.
- `cargo clippy --workspace --all-targets --all-features -D warnings`
  clean.

## 6. Files Changed

- `crates/nexum-subscription/src/subscription.rs` — split `SharedView`
  (shared derived view) from `Subscription` (per-member delivery);
  `push_commit` fan-out with per-member overflow/stale.
- `crates/nexum-subscription/src/registry.rs` — grouping, view
  lifecycle (free-on-last-member), `RegistryStats`,
  `ApplyReport{evaluations,deltas}`, `SubscriptionRef`, `view_count`.
- `crates/nexum-subscription/src/tests.rs` — 4 new grouping tests.
- `crates/nexum-network/src/config.rs` + `gateway.rs` — bounded
  `TickUpdate` (`tick_update_changes`).
- `crates/nexum-runtime/src/{metrics,runtime}.rs` — subscription
  evaluation/delta/change counters.
- `crates/game-server/examples/ccu.rs` — bounded tick + `subs:`
  evaluation report line.
- Tests opting into full-change `TickUpdate`: `nexum-network` (unit +
  integration), `nexum-sdk/tests/e2e.rs`, `nexum-game-server/tests/e2e.rs`.
- Docs: `docs/design/20-interest-management.md` (this phase's design).

## 7. Honest Next Steps

1. **Phase 22 — WASM reducer optimization** (the current #1 remaining
   cost): instance/linker reuse, host-call batching — fire ticks at 1K
   are ~550 ms server-side.
2. **Phase 18 — multi-core**: parallelize the linear world-tick path
   (movement ticks ~12 ms/1K moves) across worlds/partitions.
3. **Phase 21 — memory/alloc**: per-tick allocation reduction in the
   fan-out and transaction paths.
