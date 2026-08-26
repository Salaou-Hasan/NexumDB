# ADR-008 — Subscription Engine

- Status: accepted
- Phase: 8
- Date: 2026-08-11

## Context

NexumDB's authoritative boundary is `Transaction::commit() → Vec<Change>`.
Phases 6–7 already fan that boundary out to reducers (native + WASM) and to
the WAL. Phase 8 adds the third consumer: **subscriptions** — reactive views
over authoritative table state.

Requirements from the brief:

- subscriptions are an *observation system* — never a second storage,
  transaction, OCC, or consistency system;
- one authoritative state, one transaction model, one commit boundary, one
  change stream;
- deterministic, bounded, explicit, serializable query model (no arbitrary
  Rust closures as the permanent representation);
- atomic subscription establishment (no snapshot/live race);
- correct handling of updates entering/leaving a predicate;
- a real cursor/revision model, with resync and bounded backpressure;
- multi-table transactions must not be exposed half-applied;
- native and WASM reducers must produce identical subscription semantics.

## Decisions

### D1 — Subscriptions attach at the `Vec<Change>` boundary

The engine consumes `&[Change]` from the existing commit boundary, exactly
where WAL attaches. It never reads or mutates storage during change
processing (`Change` carries complete old/new rows), and it never participates
in the commit decision. `apply_changes` is a *post-commit observer*.

### D2 — One table per subscription; logical, bounded query model

`Query { table, predicates, order_by, limit, projection }` with AND-combined
`column op literal` predicates. Column references are names, resolved and
type-checked against the schema at `subscribe` time into a compiled matcher.
Bounded by `SubscriptionConfig`. Rust closures are *not* part of the
subscription representation (brief §6) — the logical query is serializable
and transferable.

### D3 — Registry-local monotonic commit sequence as the cursor

WAL LSN does not exist at the commit boundary (the WAL is attached by the
caller after commit). The registry therefore assigns each `apply_changes`
call a strictly increasing sequence number, in commit order, and that
sequence is the observation cursor. It is a *derived counter over the
existing commit order*, not a second ordering system; the future runtime maps
`seq ↔ WAL LSN`. `RowId` is never a global cursor.

### D4 — Atomic establishment (snapshot/live race is impossible)

`subscribe` captures the cursor **before** scanning, inside exclusive
registry ownership. In this single-threaded ownership model no commit can
interleave, so the snapshot and the live stream meet exactly at the captured
cursor: no missed changes, no duplicates. The cursor value makes the
observation point explicit for future networking.

### D5 — The subscription view is a derived cache

The visible set (`BTreeMap<Key, RowId>` ordered window + `BTreeSet<RowId>`
membership) is derived from authoritative state at the observation point and
maintained from committed changes thereafter. It is rebuildable from
`authoritative state + cursor` (`resync`). It never feeds back into
authoritative state, indexes, or the commit decision.

### D6 — Determinism

Fixed iteration order everywhere: subscriptions ascending by id, deltas in
the input change order (itself deterministic per ADR-004), snapshots sorted
by the query ordering with `RowId` tie-breaks. `Value` comparison is a total
deterministic order (floats via `total_cmp`). Identical inputs ⇒ identical
snapshots, deltas, and cursors — required for future simulation, replay, and
replication.

### D7 — Bounded backpressure, never blocking commit

Per-subscription bounded buffer. Overflow ⇒ clear buffer, emit `Stale`,
enter `Stale` state, drop further deltas. `resync` regenerates the exact
view. The commit path never waits on a consumer and never errors because a
consumer is slow.

### D8 — Atomic multi-table observation

One `apply_changes` = one committed transaction. All deltas for that
transaction are pushed to each affected subscription in one synchronous call,
so a consumer draining the buffer always sees the whole transition together —
never a half-applied multi-table commit.

### D9 — Reducer/WASM source-agnosticism

Native and WASM reducers both commit through the shared path and both produce
`Vec<Change>`. The subscription engine consumes changes without knowing their
source, so both reducer types have identical subscription semantics by
construction.

## Consequences

**Positive.** Subscriptions observe the database without becoming it: no
duplicated storage, transactions, or OCC; deterministic, bounded, and
serializable queries; correct entering/leaving semantics; a real cursor with
resync and backpressure; a clean attach point for Phase 11 networking
(drain `SubscriptionUpdate`s per connection). The dependency graph stays
acyclic: `nexum-subscription` depends only on `nexum-core`, `nexum-table`
(`Change`/`TableStore`), and `nexum-storage` (`Change`) — not on `nexum-tx`,
`nexum-reducer`, `nexum-wasm`, or `nexum-wal`.

**Negative / trade-offs.** Conservative, deliberately: one table per
subscription; AND-only predicates; single sort key; registry-local commit
sequence until the runtime aligns it with the WAL LSN; live fan-out is
O(subscriptions x changes) per commit (index-based fan-out is future work);
subscription state is not durable by design (the application re-subscribes
after recovery, which the brief's §19 recovery requirement explicitly wants).

**Risks.** The main risk is treating the derived view as authoritative —
guarded by D5 and by tests that rebuild the view from a scan and compare. A
second risk is ordering drift between the registry sequence and the WAL LSN;
both are monotonic in commit order, so the runtime can map them bijectively.
