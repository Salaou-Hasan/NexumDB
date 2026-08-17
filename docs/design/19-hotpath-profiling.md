# Phase 19 — Execution Hot-Path Profiling: Design & Measured Findings

Status: analysis + highest-value optimization complete and measured. The
tick path was instrumented at the phase level (gateway inbound / runtime
tick / gateway fan-out / client pump / client drain) and at the tick
sub-phase level (world tick / WAL / subscription apply). The dominant
cost is measured, not assumed.

---

## 1. Methodology

- Instrumentation: `--profile-detail` flag added to the CCU harness
  (`crates/game-server/examples/ccu.rs`), which times each server phase
  and reads the runtime's per-tick sub-phase profile
  (`RuntimeMetrics::last_tick_profile`).
- Workload: profile C (realistic gameplay — movement every 3 ticks, fire
  every 100 ticks) at 500 and 1,000 clients, 20 Hz, window = 32,
  in-process transport, release build.
- Hardware: Intel i7-14650HX, 16 GB RAM, Windows 11.

## 2. Measured Phase Breakdown

### 500 clients, profile C (avg over 60 ticks)

| Phase | ms/tick | % of round trip |
|-------|---------|-----------------|
| inbound (gateway process) | 1.6 | 8.6% |
| tick (runtime step) | 18.3 | 59.5% |
| fan-out (gateway encode+send) | 1.0 | 3.2% |
| pump (subscription drain) | 0.0 | 0.0% |
| flush (outbound) | 0.0 | 0.0% |
| clients (SDK decode) | 8.6 | 27.8% |
| drain (client consume) | 1.4 | 4.4% |

### 1,000 clients, profile C (avg over 60 ticks)

| Phase | ms/tick | % of round trip |
|-------|---------|-----------------|
| inbound (gateway process) | 6.2 | 14.5% |
| tick (runtime step) | 43.2 | 73.2% |
| fan-out (gateway encode+send) | 1.9 | 3.1% |
| clients (SDK decode) | 6.6 | 11.3% |
| drain (client consume) | 1.0 | 1.8% |

### Inside the tick (1,000 clients, profile C)

| Sub-phase | ms/tick | % of tick |
|-----------|---------|-----------|
| **subscription apply** (`apply_changes`) | **30.5** | **72.0%** |
| world tick (reducers + tx + OCC + commit) | 11.9 | 28.0% |
| WAL | 0.0 | 0.0% |

## 3. Ranked Bottleneck List (from measurements)

### #1 — Subscription all-to-all fan-out (72% of tick, 52% of round trip)

**1. What operation is expensive?**
`SubscriptionRegistry::apply_changes` — every committed change is
evaluated against every subscription whose table it touches. With N
subscribers and M changes on the same table, this is O(N × M)
evaluations, each performing query matching plus window maintenance
(remove + key + insert + sync_window).

**2. How much CPU time?** 30.5 ms/tick at 1,000 clients (measured).
At 500 clients it is ~72% of an 18.3 ms tick.

**3. How many times does it execute?** 1,000 changes × 1,000
subscriptions = 1,000,000 `apply_change` calls per movement tick. Each
call is O(log window) BTreeMap/BTreeSet work plus a full row clone into
the subscription's window.

**4. Scaling complexity?** O(changes × subscriptions) per tick —
quadratic in client count when every client subscribes to the same table
(the arena's all-see-all model).

**5. What data does it touch?** Every subscription's derived window
(BTreeMap<Key, Row>), its `row_keys` mirror, `visible_ids`, and
`visible_keys`. Each update clones the full row into the window.

**6. What allocations/copies?** One `row.clone()` per (change × sub)
pair — 1M clones/tick — plus BTreeMap/BTreeSet node allocations.

**7. Can the amount of work be reduced?** Yes — this is the Phase 20
interest-management target. The immediate measured sub-optimization:
the arena harness subscribes every client to the **same** query (same
table, same limit, no predicate, no order-by). Their windows are
identical; 968/1000 rows per subscription are non-visible yet still get
a full payload clone every tick. Two levers, measured below.

**8. Can each unit of work be made cheaper?** Yes. (a) Avoid the payload
clone for non-visible rows (fetch on promotion instead); (b) share
window evaluation across identical subscriptions; (c) cheap early-exit
for non-visible rows that cannot enter the cap.

**9. Correctness invariants affected?** The delivered delta stream must
remain deterministic and identical per commit; the visible window must
remain the exact top-cap of the window at every committed point; the
initial snapshot and resync must be unaffected. Any change must keep the
ADR-008 D8 atomicity (one apply = one seq) and ADR-015 D5 incremental
membership.

**10. Benchmark that proves it?** CCU profile C at 1K: sub_apply should
drop from 30.5 ms to single-digit ms; p99 tick should move below the
2× budget classification. Regression: identical delta stream + window
contents before/after for the same workload.

### #2 — Client-side TickUpdate decode (11.3% + 1.8% of round trip)

**1. What?** Each attached client decodes the **full** TickUpdate (the
whole committed change set, 1000 changes) even though its subscription
window is only 32 rows.

**2. How much?** 6.6 ms/tick (clients) + 1.0 ms (drain) at 1,000
clients.

**3. How many times?** 1000 changes decoded per client per tick =
1M decodes, O(changes × clients).

**4. Complexity?** O(changes × clients) — quadratic in clients when all
clients receive all changes.

**5. Data touched?** Every change's rows; the SDK view applies each
change then filters to the window.

**6. Allocations?** One decoded change envelope per change per client.

**7. Work reducible?** Yes — server-side relevance filtering (only send
each client its windowed subscription delta, not the full change set).
This is the Phase 20 "bounded result set" target; also
encode-once-per-query-group.

**8. Cheaper per unit?** Yes — skip decoding changes that cannot be in
the client's window; delta-based frames instead of full change sets.

**9. Invariants?** SDK view must remain a correct derived state;
resync/reconnect must reconstruct the same view; no silent drop.

**10. Benchmark?** CCU profile C at 1K — clients phase should drop from
6.6 ms to ~1 ms; p99 tick classification should improve.

### #3 — World tick cost (28% of tick, 20% of round trip)

**1. What?** The world tick: reducer execution (game reducers, already
PK/index-optimized in Phase 17), transaction construction, OCC
validation, commit, change generation.

**2. How much?** 11.9 ms/tick at 1,000 clients (includes idle ticks).

**3. How many times?** 1000 reducer calls + 1 commit per movement tick.

**4. Complexity?** O(changes) — linear in the number of mutations; the
Phase 17 PK/index fix removed the O(N) scan.

**5. Data?** The transaction read/write sets, the table's PK index, the
committed change vec.

**6. Allocations?** Transaction scaffolding per tick; change entries.

**7. Work reducible?** Marginal — this is the actual game logic cost;
Phase 18 (multi-core) can parallelize across worlds/partitions, and
Phase 19 (below) reduces transaction bookkeeping.

**8. Cheaper per unit?** Yes — transaction/OCC bookkeeping and change
construction can be optimized (Phase 19/21); the world tick is already
reducer-cheap.

**9. Invariants?** OCC correctness, read-your-writes, phantom
protection, deterministic commit ordering.

**10. Benchmark?** CCU profile C at 1K — world_tick sub-phase should
decrease as transaction overhead is reduced.

## 4. Decision: Highest-Value Optimization (implemented)

The measured #1 bottleneck is `apply_changes` (72% of tick). Within
Phase 19's mandate (profile, then reduce the dominant cost), the
**smallest justified, correctness-preserving optimization** is:

> **Share row payloads across consumers via `Arc<Row>` (ADR-019 D4).**
> `Change` now holds its rows as `Arc<Row>`; the commit path wraps each
> committed row **once**, and every subscription window retains the same
> `Arc` via a refcount bump instead of a per-(change × sub) deep clone.

This is the cheapest possible reduction of the measured hot path: the
1,000,000 deep `row.clone()` calls per movement tick at 1K clients
become 1,000 `Arc::new` allocations (one per committed row, shared with
the WAL too) plus 1,000,000 atomic refcount bumps. Window semantics are
unchanged — the window still holds every matching row's payload, sorted
identically — so the delta stream and visible-cap invariants are
preserved exactly (proven by the regression suite + a new
`shared_row_payloads_across_subscriptions` test that asserts `Arc::ptr_eq`
across subscriptions).

### Measured result (1,000 clients, profile C, release, in-process)

| Metric | Before | After | Δ |
|--------|--------|-------|---|
| sub_apply (avg) | 30.5 ms/tick | 11.4 ms/tick | **2.7× faster** |
| sub_apply (% of tick) | 72.0% | 65.4% | |
| world_tick (avg) | 11.9 ms/tick | 6.0 ms/tick | 2.0× |
| p95 tick (round trip) | ~365 ms | 204 ms | 1.8× |
| p50 tick (idle-dominated) | ~0.9 ms | 0.8 ms | |
| workspace tests | 641 | 642 | +1 regression test |

Why the remaining cost persists: `apply_changes` is still evaluated
O(changes × subscriptions) — 1M `apply_change` calls per movement tick,
each doing predicate match + BTreeMap window maintenance + sort-value
clone. The count itself, not the per-unit clone, is now the cost; the
next reduction must come from **reducing the number of evaluations**
(Phase 20 interest management), not from making each evaluation
cheaper.

Design constraints honored:
- The authoritative store remains the single source of truth; the
  subscription window remains a derived cache (ADR-008 D5).
- The delta stream ordering per commit is unchanged.
- `unsafe_code = forbid`, determinism, and FIFO preserved.
- Regression tests prove identical deltas before/after.
