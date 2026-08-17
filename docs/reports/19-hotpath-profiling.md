# Phase 19 — Execution Hot-Path Profiling: Report

Status: complete. The tick path was instrumented at phase and sub-phase
level, the dominant costs were measured (not assumed), and the single
highest-value optimization was implemented, regression-tested, and
re-measured.

## 1. Environment

| | |
|---|---|
| CPU | Intel Core i7-14650HX (16 cores / 24 threads) |
| RAM | 16 GB DDR5 |
| OS | Windows 11 |
| Build | release, LTO enabled |
| Transport | in-process (real gateway / runtime / world / subscriptions / SDK) |
| Tick rate | 20 Hz (budget 50 ms) |
| Harness | `cargo run --release -p game-server --example ccu -- --clients N --profile C --profile-detail` |

## 2. Methodology

- Added `--profile-detail` to the CCU harness: per-tick phase timers
  (inbound / tick / fan-out / pump / flush / clients / drain) plus the
  runtime's per-tick sub-phase profile (`RuntimeMetrics::last_tick_profile`
  → world tick / WAL / subscription apply).
- Workload: profile C (realistic gameplay — movement every 3 ticks, fire
  every 100 ticks) at 500 and 1,000 clients, window = 32.
- Classification: honest (PASS / DEGRADED / SATURATED) per ADR-016 D4.

## 3. Ranked Bottleneck List (measured at 1,000 clients)

### #1 — Subscription all-to-all fan-out — 30.5 ms/tick (72% of tick, 52% of round trip)

`apply_changes` evaluates every committed change against every
subscription on that table: **O(changes × subscriptions)**. At 1K, a
movement tick = 1,000 changes × 1,000 subs = **1,000,000 `apply_change`
calls**, each doing predicate match + window maintenance + a full
`row.clone()` into the window.

### #2 — Client-side TickUpdate decode — 6.6 + 1.0 ms/tick (13% of round trip)

Every attached client decodes the **full** change set (1,000 changes)
even though its subscription window is 32 rows: **O(changes × clients)**.

### #3 — World tick — 11.9 ms/tick (28% of tick, 20% of round trip)

Reducer execution (already PK/index-optimized in Phase 17), transaction
construction, OCC, commit, change generation: O(changes), linear.

## 4. Optimization Implemented (highest value, smallest change)

**Arc-shared row payloads (ADR-019 D4).** `Change` now stores its rows as
`Arc<Row>`. The commit path wraps each committed row once; the WAL and
every subscription window share the same allocation via refcount bumps.
The 1M deep `row.clone()` calls per movement tick become 1M cheap atomic
bumps.

Files changed:

- `crates/nexum-storage/src/change.rs` — `Arc<Row>` payloads; new
  `new_row_shared()` accessor; `old_row()`/`new_row()` unchanged (deref).
- `crates/nexum-subscription/src/subscription.rs` — window is
  `BTreeMap<Key, Arc<Row>>`; `upsert` retains the shared Arc; `rebuild`
  moves scanned rows into `Arc::new`.
- `crates/nexum-subscription/src/tests.rs` — new regression test.
- `crates/nexum-runtime/{runtime,metrics}.rs`, `crates/game-server/
  examples/ccu.rs` — profiling instrumentation (`last_tick_profile`).

## 5. Before / After

| Metric | Before | After | Δ |
|--------|--------|-------|---|
| sub_apply (avg/tick) | 30.5 ms | 11.4 ms | **2.7×** |
| sub_apply (% of tick) | 72.0% | 65.4% | |
| world_tick (avg/tick) | 11.9 ms | 6.0 ms | 2.0× |
| p95 tick (round trip) | ~365 ms | 204 ms | 1.8× |
| p50 tick (idle-dominated) | ~0.9 ms | 0.8 ms | |
| workspace tests | 641 | 642 | +1 regression |

The remaining movement-tick cost is the **evaluation count**
(O(changes × subs) = 1M calls), not per-unit cloning. The next reduction
must reduce the count: **Phase 20 interest management / AOI**
(subscription indexing, spatial relevance, bounded per-subscription
frames) — which also fixes bottleneck #2 (client decode of full change
sets).

## 6. Correctness Validation

- 642 workspace tests pass (0 failed) — the full suite including the
  subscription delta-stream, window-cap, resync, WAL, network, runtime,
  and game-server e2e tests.
- New regression test `shared_row_payloads_across_subscriptions` proves
  two subscriptions over the same query deliver **identical** delta
  streams and retain the **same** `Arc` (`Arc::ptr_eq`) — the window
  holds the change's own payload, not a per-sub clone.
- `unsafe_code = forbid` maintained; determinism and delta ordering
  unchanged (window contents are value-identical).

## 7. Remaining Bottlenecks (next steps)

1. **O(changes × subscriptions) evaluation count** — Phase 20 interest
   management (subscription grouping, per-zone change routing).
2. **O(changes × clients) full-set decode** — Phase 20 bounded
   per-subscription frames (server-side relevance filtering).
3. **World tick / inbound** — linear O(clients) game logic; Phase 18
   (multi-core across worlds/partitions) applies here.
