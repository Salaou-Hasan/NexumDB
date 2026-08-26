# Phase 20 — Interest Management / AOI: Design

Status: design complete, implementation follows. The subscription
engine's workload was measured in Phase 19: the dominant cost is the
number of evaluations, not the per-evaluation cost.

---

## 1. The Measured Problem (from Phase 19)

At 1,000 clients, profile C, a movement tick commits ~1,000 changes
(player moves). Every client subscribes to the **same** query — the CCU
harness uses `Query::builder("players").limit(32).build()` and the real
game client (`game-server/src/client.rs`) uses
`Query::builder(TABLE).build()` — so:

- `SubscriptionRegistry::apply_changes` evaluates **every change against
  every subscription**: 1,000 x 1,000 = **1,000,000 `apply_change`
  calls per movement tick** — O(changes x subscriptions).
- All 1,000 subscriptions have **identical windows and identical delta
  streams** (same query, same table, same derivation), yet each is
  maintained independently: 1,000 identical BTreeMap windows, 1,000x the
  window maintenance, 1,000x the delta computation.
- Measured: `sub_apply` = 30.5 ms/tick before Phase 19's Arc sharing,
  11.4 ms/tick after (72% → 65% of tick). The count is the problem.

Phase 19 made each evaluation cheaper (shared `Arc<Row>` payloads). The
remaining cost is that there are simply **far too many evaluations** —
for identical work.

## 2. Also Measured: Client-Side Redundant Decode

Every attached client decodes the **full change set** carried by the
`TickUpdate` broadcast (1,000 changes) even though its subscription
window is 32 rows and its SDK view is driven by windowed
`SubscriptionDelta` frames. The `TickUpdate.changes` list feeds only a
diagnostic event (`ServerEvent::Tick`) — the game client reads only the
tick number. Measured: client decode ≈ 6.6 ms/tick at 1K — O(changes x
clients) redundant work plus bandwidth.

## 3. Chosen Architecture (smallest justified by measurement)

Two mechanisms, both strictly delivery/view optimizations — no change to
authoritative state, transactions, OCC, WAL, or simulation semantics.

### D1 — Duplicate-subscription grouping (relevance groups)

The registry detects subscriptions with **identical queries** and shares
one **derived view** per distinct query. Per commit:

```text
apply_changes(changes)
  │
  ├─ per distinct query (view):  evaluate the changes ONCE
  │     → scratch delta stream (deterministic, same code as today)
  │
  └─ per member subscription:    clone the scratch into its buffer
        (per-member overflow → stale, exactly as today)
```

- Evaluations per change: `N` → `#distinct_queries` (1 for the harness
  and the arena game). At 1K: 1,000,000 → ~1,000 view evaluations +
  ~32K small delta clones (window-sized, not changes-sized).
- The shared view keeps: `window`, `row_keys`, `visible_keys`,
  `visible_ids`, `compiled`, `window_cap` — one allocation set per
  distinct query instead of per subscription.
- Members keep: id, query, state, cursor, buffer, max_buffered.
- Correctness: identical queries have identical windows and identical
  delta streams (same derivation function over the same authoritative
  state), so the grouped path is value-identical to the per-subscription
  path — proven by the unchanged existing suite plus new regression
  tests asserting identical streams and `Arc::ptr_eq` shared payloads.

### D2 — Bounded TickUpdate (stop sending irrelevant changes)

Add `GatewayConfig::tick_update_changes: bool` (default **false** —
bounded). When false, the `TickUpdate` carries `(world, tick, tx_id,
change_count, events)` but **not** the full decoded change list; clients
receive windowed `SubscriptionDelta` frames as the delivery path. The
`ServerEvent::Tick` still fires (diagnostics), with an empty change
list. This removes the O(changes x clients) decode and the redundant
per-tick bandwidth. Opt in (`true`) for full per-tick change diagnostics.

### D3 — Counters (the metric this phase is judged by)

`SubscriptionRegistry` exposes cumulative stats:

- `evaluations` — change x distinct-query `apply_change` calls;
- `deltas` — subscription updates produced;
- `fanouts` — member buffer appends.

Surfaced through `RuntimeMetrics` (`subscription_evaluations`,
`subscription_deltas`) so the CCU harness reports **subscription
evaluations per change** before/after.

## 4. Preserved Invariants

- Deterministic simulation and change ordering (view evaluation is the
  same code, same order; per-member streams identical).
- One authoritative state store; the view stays a derived cache
  (ADR-008 D5); no second transaction/OCC/simulation path.
- Per-member lifecycle: distinct ids, independent buffers, independent
  drain, unsubscribe/resync/refresh semantics unchanged.
- Atomic establishment (ADR-008 D4): a member joining a live group
  snapshots the group's current (already-current) view at `next_seq`.
- Drop detection: all members of a dropped-table view go stale.
- Bounded buffers → stale on overflow, exactly as today; no silent loss
  (rejected/stale is explicit).
- `unsafe_code = forbid`.

## 5. Expected Result (to be measured)

| Metric | Phase 19 | Phase 20 target |
|--------|----------|-----------------|
| evaluations per change (1K) | 1,000 (1M/tick) | ~1 (view) |
| sub_apply avg/tick (1K, profile C) | 11.4 ms | ~2–4 ms |
| client decode avg/tick (1K) | 6.6 ms | ~1–2 ms |
| p95 round-trip tick (1K) | 204 ms | well below |

Measurements follow in the report; nothing here is claimed until
measured.
