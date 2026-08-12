# Subscription Engine — Design Notes (Phase 8)

Subscriptions are **reactive views over authoritative table state**, driven
entirely by the existing commit boundary:

```text
Transaction::commit() → Vec<Change> → Subscription Engine → deltas
```

This document answers the Phase 8 brief's design questions precisely. The
companion [ADR-008](../architecture/08-subscription-engine.md) records the
binding decisions; this note is the worked reasoning.

---

## 1. What exactly is a subscription?

A subscription is a **deterministic, bounded observation** of one table's
committed evolution. It owns:

- a logical, serializable [`Query`](../architecture/08-subscription-engine.md) —
  table, predicates, ordering, limit, projection
- a **derived cache** of the rows currently visible to it (its view)
- a **cursor** — the position in the commit sequence up to which it has
  processed
- a **delivery buffer** — a bounded queue of `SubscriptionUpdate`s the
  consumer (future network layer) drains
- a **lifecycle state** — `Active` or `Stale`

It owns **no authoritative state**. The table storage remains the sole truth;
the subscription view is rebuildable from `authoritative state + cursor`
(Section 16 of the brief).

## 2. What state does a subscription own?

| Piece | Role | Authority |
|-------|------|-----------|
| `Query` | logical definition | part of the subscription (serializable) |
| compiled matcher | resolved column positions/type checks | derived from `Query` + schema |
| visible set | RowIds currently matching | **derived cache** |
| cursor | last processed commit sequence | part of the subscription |
| buffer | pending `SubscriptionUpdate`s | transient, bounded |
| state | `Active` / `Stale` | lifecycle |

## 3. How is a query represented?

Logical, protocol-independent, **not Rust closures** (brief §6):

```text
Query {
    table: String,                    // table name — survives snapshot/recreate
    predicates: Vec<Predicate>,       // AND-combined
    order_by: Option<OrderBy>,        // one column + direction
    limit: Option<u32>,               // bounded window
    projection: Option<Vec<String>>,  // delivered column subset
}
Predicate { column: String, op: ComparisonOp, value: Value }
ComparisonOp { Eq, Ne, Lt, Lte, Gt, Gte }
```

Column references are **names** so a query survives schema re-creation and is
transferable to another node with an identical schema. Names resolve to
positions once, at `subscribe` time, into a compiled matcher — predicates and
order columns are type-checked against the schema then (`InvalidArgument` on
unknown column or wrong literal type).

The first implementation is deliberately conservative: only AND-combined
column-vs-literal comparisons, a single sort key, a single projection list.
Everything is bounded by `SubscriptionConfig` (max predicates, max projection
columns, max snapshot rows).

## 4. How are changes matched against subscriptions?

`apply_changes(store, &[Change])` is called **once per committed
transaction**, at the exact boundary where WAL also attaches. For each
subscription whose table appears in the change set:

- **Insert** — evaluate the new row's predicates. Match → row enters the
  window → `Insert` delta. Non-match → nothing.
- **Update** — evaluate **old and new** rows (the `Change` carries both), the
  four-way transition:
  - outside → outside: nothing
  - outside → inside: `Insert` delta
  - inside → inside: `Update` delta (new row state)
  - inside → outside: `Delete` delta
- **Delete** — if the row was visible → `Delete` delta.

The matcher never touches storage: `Change` already carries complete
`old_row`/`new_row` payloads, so matching is pure. Matching happens **after**
commit only — provisional writes are invisible to subscriptions by
construction (they never produce a `Change`).

## 5. Initial snapshot and the establishment race

The brief's critical question: *"Do not implement scan-then-subscribe without
defining the race."*

**There is no race in this architecture.** The registry is owned exclusively
(`&mut`); `subscribe(store, query)`:

1. resolves and compiles the query against the table schema,
2. captures the observation cursor = the registry's `next_seq` **before
   scanning**,
3. scans the authoritative table, filters, orders, limits, projects,
4. records the visible set and pushes `Initial { seq: cursor, rows }`.

Because the caller must obtain `&mut SubscriptionRegistry` and `&TableStore`
(shared) in the same thread, no commit can interleave between the cursor
capture and the scan. Any `apply_changes` call happens strictly after
`subscribe` returns, and therefore strictly after the snapshot — so every
change committed "before" the subscription is in the snapshot, and every
change applied after is delivered live. **No change is missed; none is
duplicated.** This is the atomic observation point; the cursor value makes
the position explicit for the future network layer and for resync.

## 6. Ordering guarantees

- **Initial snapshot / resync rows**: sorted by the query's `order_by`
  (with `RowId` as a deterministic tie-break), or by `RowId` when no
  `order_by` is given. Stable and reproducible.
- **Delta order within one commit**: the order of the input `&[Change]`,
  which is deterministic (Phase 4 commit ordering, ADR-004 D9).
- **Subscription iteration order**: ascending `SubscriptionId`
  (`BTreeMap`), so a commit's fan-out is deterministic.
- **Commit order**: `apply_changes` calls must be made in commit order; the
  registry assigns each a strictly increasing sequence, so a later commit can
  never be observed before an earlier one. This is the ordering contract.

## 7. The cursor / revision model

The engine attaches **before** WAL, so no WAL LSN exists yet at the commit
boundary. The registry therefore maintains its own **monotonic commit
sequence**: every `apply_changes` consumes one sequence number, in commit
order. This is the observation cursor.

- It is *not* a second authoritative ordering: it is a derived counter over
  the single commit order the transaction engine already establishes.
- It is strictly increasing, so it is safe to persist and safe to compare
  across subscriptions and resyncs.
- When the runtime and networking arrive, the runtime can map
  `seq ↔ WAL LSN` (both are monotonic in commit order), and the subscription
  cursor becomes the durable observation position. Nothing in this phase
  changes.
- `RowId` is **never** used as a global cursor (brief §14).

## 8. The limit window (bounded view)

With `limit: N`, the visible set is the **top-N rows by the query ordering**
(ties broken by `RowId`, deterministically). The window is maintained on
every committed change. **Without an explicit `limit`, the view is still
bounded by the engine's `max_snapshot_rows` safety cap (default 10,000)**: a
`subscribe players WHERE zone_id = 42` over a zone with more than 10,000
rows silently delivers the first 10,000 rows (by ordering) — raise
`SubscriptionConfig::max_snapshot_rows` for larger views. This is the
"bounded observation" contract, not an oversight.

- a matching insert that ranks inside the window enters; the row it evicts
  (the new worst-ranked visible row) leaves, with `Delete` emitted;
- a matching update that reorders a visible row re-ranks it; if it falls out
  of the window it is evicted (`Delete`); otherwise `Update` is emitted;
- an update entering/leaving the predicate behaves as Section 4.

A row that enters and is immediately evicted (ranks outside the window) is
never emitted — the net view did not change. This keeps the delivered view
exactly equal to the authoritative top-N at every committed point.

## 9. Backpressure

The commit path never waits for a consumer (brief §12):

- each subscription has a bounded buffer (`config.max_buffered`);
- when a push would overflow, the buffer is **cleared**, a single
  `Stale { seq }` update is pushed, and the subscription enters `Stale`;
- while `Stale`, subsequent commits' deltas are **dropped** (never
  accumulated), so a slow consumer can never grow memory without bound;
- the consumer recovers via `resync` (Section 10).

This is the "mark stale + drop + resync" policy. It never produces an
incorrect *final* state: the stale subscriber's view is explicitly invalid
and a resync regenerates the exact authoritative view.

## 10. Resync

`resync(store, id)`:

1. fails with `NotFound` if the table is gone,
2. rebuilds the visible set from a **fresh full scan** of authoritative
   state (filter → order → limit → project),
3. clears the delivery buffer,
4. pushes `Resync { seq: next_seq, rows }`,
5. sets cursor = `next_seq`, state = `Active`.

A resync is always correct regardless of how far behind the subscriber was,
because the scan reads the authoritative truth and the new cursor covers
everything before it.

## 11. Table / schema changes

- **Table dropped**: at the next `apply_changes`, the registry detects the
  subscription's table no longer exists and marks the subscription `Stale`
  (same overflow path — a `Stale` update, buffer cleared). `resync` then
  fails `NotFound`; the application unsubscribes. Detected by consulting
  `TableStore` at apply time.
- **Schema changes**: no ALTER exists in the current phases, so a compiled
  matcher's positions stay valid for the life of the table. If ALTER arrives
  later, `resync` becomes the recompile point (the logical `Query` is kept
  for exactly this reason).

## 12. Events vs changes

Kept separate (brief §11):

- **Changes** are authoritative state transitions — subscriptions consume
  these and only these.
- **Reducer events** are application-level messages with their own
  transaction-local semantics (Phase 6/7); a future realtime/event channel
  delivers them. The subscription engine never depends on events for
  correctness.

## 13. Reducer / WASM integration

Both native and WASM reducers converge on `Vec<Change>` through the same
commit path (Phases 6–7). The subscription engine is **source-agnostic**: it
consumes `&[Change]` and does not know or care whether the changes came from
a native reducer, a WASM reducer, or a future simulation tick.

## 14. Memory model

Per subscription, the derived cache is:

- `BTreeMap<Key, RowId>` — the ordered window (bounded by `limit`, or the
  matching row count),
- `BTreeSet<RowId>` — O(log n) membership for change processing.

Both are facets of the same derived view (ADR-008 D5); neither feeds back
into authoritative state. The buffer is bounded by config. Nothing scales
with the database size except the matching row count per subscription.

## 15. Determinism guarantees (ADR-008 D6)

Given the same store state, the same queries, and the same commit sequence,
the engine produces byte-identical:

- initial snapshots and resyncs (scan + filter + sort + limit + project all
  deterministic; `RowId` tie-breaks),
- delta sequences (fixed subscription iteration, fixed change order),
- cursor values.

Comparison is a total, deterministic order over `Value` (variants by
declaration order; floats via `total_cmp`, so `NaN` is a comparable value).
Predicates are type-checked at compile time, so cross-type comparisons
cannot occur in practice; the total order exists to keep the sort key's
`Ord` lawful.

## 16. Known limitations (honest)

- One table per subscription; cross-table joins are future work.
- Predicates are AND-combined column-vs-literal comparisons only — no OR, no
  NULL semantics (no NULLs exist in the value model), no string contains.
- One sort key; multi-key ordering is future work.
- No durable subscription state: after WAL recovery the application
  re-subscribes (brief §19 requires that recovered history not replay as new
  live commits — a fresh snapshot over recovered state, then live changes,
  is exactly that).
- The commit sequence is registry-local; alignment with WAL LSN happens in
  the future runtime.
- Schema changes (ALTER) would require recompilation at resync; not needed
  yet because ALTER does not exist.
- Live delta fan-out is O(subscriptions × changes) per commit, and each
  affected change re-synchronizes the window in O(visible rows) (exact
  top-N maintenance) — acceptable and measured (the benchmark shows it
  growing with window size); a differential window and an index-based
  fan-out (brief §7) are future work.
