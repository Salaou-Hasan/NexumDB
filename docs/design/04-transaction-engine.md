# Phase 4 Design — Transaction Engine + OCC

**Status:** Accepted — to be implemented as specified here
**Phase:** 4 (transaction engine)
**Relates to:** ADR-001, ADR-002, ADR-003, ADR-004

## 1. Purpose

Turn NexumDB from "a database with tables" into an **authoritative transactional
state engine**. One transaction engine serves tables, reducers, and simulation:
every authoritative mutation happens through a transaction that records reads
and buffers writes, validates everything against live state, and only then
mutates storage.

The central invariant (from the phase brief):

> **NO AUTHORITATIVE STATE MAY BE MUTATED UNTIL THE ENTIRE TRANSACTION HAS
> SUCCESSFULLY VALIDATED.**

## 2. Pipeline

```text
BEGIN → READ PHASE → WRITE BUFFER → VALIDATION → COMMIT
                  (reads recorded)  (pure, no mutation)  (apply + Change[])
```

Validation and mutation are strictly separated. There is no apply-then-rollback
path: **validate everything, then apply everything**.

## 3. Answers to the 24 design questions

### Q1. What is a Transaction?

```rust
pub struct Transaction {
    id: TransactionId,          // assigned by TableStore::begin (monotonic per store)
    state: TransactionState,    // Active | Committed | Aborted
    reads: ReadSet,             // BTreeMap<(TableId, RowId), Option<Version>>
    writes: WriteSet,           // BTreeMap<(TableId, RowId), WriteEntry>
    provisional: BTreeMap<TableId, u64>, // per-table counter for provisional ids
}
```

The transaction is a **pure accumulator**: it holds *transaction-local
metadata* proportional to its read/write set, never a copy of the database
(Q16 of the brief).

### Q2. What does a transaction own?

It owns its id, state, read observations, buffered writes, and provisional-id
counter. It does **not** own a reference to the store — the store is passed per
call (`tx.get(&store, ...)`, `tx.commit(&mut store)`). Reasons:

- borrows stay short and per-operation (matches Phase 3's single-threaded
  exclusive-ownership model, no interior mutability)
- direct writes and other transactions can interleave between a tx's calls,
  which is exactly what OCC conflict detection is for (and what tests need)
- the runtime can own transactions without holding the store

### Q3. How are reads recorded?

Reads observe the **transaction's logical view** — committed state overlaid
with the transaction's own buffered writes (read-your-writes, see §17):

- a row with a buffered write in this transaction returns the *pending*
  state (`Insert(row)` / `Update(row)` → the row; `Delete` → absent) and
  records **no** row observation (the write entry governs validation)
- any other row records `(TableId, RowId) → Option<Version>`:
  - existing row → `Some(version)` (from `table.version_of`)
  - missing row → `None` (**absent** is a first-class observation)

Repeated reads of the same row overwrite the observation (last read wins).
`tx.update` / `tx.delete` on a **real** row additionally capture the row's
committed version *at write time* (before buffering) — this is what detects
write/write conflicts without requiring an explicit prior read (see §17).

### Q4. How are writes recorded?

Buffered in `WriteSet`, keyed `(TableId, RowId) → WriteEntry` where
`WriteEntry ∈ { Insert(Row), Update(Row), Delete }`. Nothing touches storage
until commit.

### Q5. What exactly constitutes a conflict?

Two kinds of conflict, both surfaced as `Error::Conflict`:

**Row conflicts** — any row observation differs from the live state at
validation time (a writer intervened between the observation and the commit):

| Observation | Live at validation | Result |
|---|---|---|
| `Some(v)` | `Some(v)` | ok |
| `Some(v)` | `Some(v')`, `v' ≠ v` | **Conflict** (row updated) |
| `Some(v)` | `None` | **Conflict** (row deleted) |
| `None` | `Some(_)` | **Conflict** (row inserted) |
| `None` | `None` | ok |

**Table conflicts (phantom protection, §17)** — a table observed *as a set*
(`tx.scan`, `tx.lookup_unique`) records the table's **mutation epoch**;
any committed row mutation in that table advances the epoch, so at
validation:

| Observation | Live at validation | Result |
|---|---|---|
| epoch `E` | epoch `E` | ok |
| epoch `E` | epoch `E' ≠ E` | **Conflict** (some row changed) |

This is intentionally conservative (false conflicts are possible); it is the
initial serializability mechanism for predicate reads (see §17).

### Q6. What version is captured for every read?

The row's version at read time, atomically with the row (Phase 3 `StoredRow`
stores row and version together, so the observation cannot straddle a
mutation). `Option<Version>` — `None` is the documented "absent" sentinel.

### Q7. How are missing rows represented in the read set?

As `None`. The `None vs Some(_)` mismatch above is what lets T1 (which observed
"absent") detect that T2 inserted the row before T1 committed.

### Q8. How are inserts represented in the write set?

`WriteEntry::Insert(Row)` keyed by a **provisional RowId** (high bit `1<<63`
set, per-table counter). The caller receives the provisional id so it can
reference the row within the transaction; it is only a coalescing handle and
never reaches storage — at commit, storage assigns the real id.

### Q9. How are updates represented?

`WriteEntry::Update(Row)` keyed by the **real** RowId (from a read). Row
identity never changes (Phase 2 decision).

### Q10. How are deletes represented?

`WriteEntry::Delete` keyed by a real RowId.

### Q11. How are multiple writes to the same row coalesced?

One entry per `(TableId, RowId)`; the following deterministic rules apply at
write time:

| Incoming op | Existing entry | Result |
|---|---|---|
| insert (fresh handle) | — | `Insert(row)` |
| insert (existing handle) | any | **InvalidTransaction** (duplicate insert of one handle) |
| update | — (real id) | `Update(row)` |
| update | `Insert(row_old)` | `Insert(row)` (insert→update = final insert) |
| update | `Update(row_old)` | `Update(row)` (latest wins) |
| update | `Delete` | **InvalidTransaction** (delete→update rejected) |
| delete | — (real id) | `Delete` |
| delete | `Insert(_)` | entry **removed** (insert→delete = net no-op) |
| delete | `Update(_)` | `Delete` (update→delete) |
| delete | `Delete` | **InvalidTransaction** (already deleted) |

Operational notes: `update`/`delete` on a **provisional** id that does not
reference a pending insert is **InvalidTransaction** (dangling handle). A
"delete→update" is rejected because it is contradictory and ambiguous; the
caller should use update semantics (or two transactions).

### Q12. How are multiple tables handled?

Reads and writes are keyed by `(TableId, RowId)`; one transaction freely
touches many tables. Validation covers the whole read/write set across all
tables, and commit applies across all tables before any Change is observable.

### Q13. How does validation happen without mutating state?

`validate(store, tx)` is a pure function over `&TableStore` + the tx's sets:

1. **Read set**: for each observation, compare `table.version_of(row_id)`
   against the observed value (Q5 table). Mismatch → `Conflict`.
2. **Deletes**: target must exist live (`contains`) → else `NotFound`. The
   deleted rows' unique keys are collected as **released** keys.
3. **Updates**: target must exist live → else `NotFound`. For each unique
   index key of the new row (via new `Table::unique_keys`): live owners
   (`Table::lookup_unique`) minus released rows must be a subset of `{row_id}`;
   and no other write in this tx may claim the same key (claims map). Else →
   `AlreadyExists`.
4. **Inserts**: for each unique key: live owners minus released must be empty;
   no other tx write may claim it. Else → `AlreadyExists`.

The **claims map** (`HashMap<(TableId, String), HashMap<Vec<Value>, RowId>>`)
tracks keys claimed by this tx's own writes (lookup-only — never iterated, so
behavior stays deterministic). Because `Value` has bit-exact `Eq + Hash`
(Phase 2), unique-key identity is well defined.

Validation performs **zero mutations** — it is a pure read.

### Q14. How does commit happen atomically?

Commit = validate → apply → collect changes (Section 6). Apply runs through the
existing `Table`/`TableStore` mutation API only, in a deterministic order, with
`&mut TableStore` held for the whole apply — no observer can see partial state
(single-threaded exclusive ownership). Because validation mirrors every check
the apply performs, **apply is infallible post-validation**: any residual error
is an internal invariant violation (`expect`, documented).

### Q15. How are indexes kept consistent?

Indexes are derived (Phase 2/3) and maintained by `Table`'s own
insert/update/delete paths. A successful transaction therefore leaves rows,
versions, indexes, and the change buffer consistent by construction. A failed
transaction never reaches apply, so nothing changes at all. Tests verify this
explicitly (index↔storage divergence invariant, multi-table).

### Q16. How are Change records produced?

Each applied `Table` mutation appends exactly one `Change` to its table's
buffer (Phase 3). Before apply, commit records each touched table's current
buffer length; after apply it drains each touched table and keeps only the
delta, concatenated in deterministic order. Failed transactions emit nothing.

### Q17. What happens when validation fails?

- `validate` returns the first error (`Conflict` / `NotFound` / `AlreadyExists`).
- The transaction's state becomes `Aborted`.
- **Zero** mutations, **zero** Change records, zero version bumps.
- The caller may retry by beginning a new transaction (future runtime loop).

### Q18. What isolation guarantees does the OCC model provide?

Updated by the Phase 4 correction (§17):

**Protected (conservative serializable for the supported operation set):**

- read/write conflicts — row versions captured at read *and* write time,
  validated at commit
- write/write conflicts — `update`/`delete` capture the row's version before
  buffering, so a concurrent writer is always detected (no explicit read
  required)
- missing-row reads → inserts — `None` observation vs live
- deletes — `Some(v)` observation vs `None` live (and `delete` itself
  captures the version at write time)
- inserts vs live unique keys — unique constraint checked at commit
- multi-table atomicity — one validation, one apply
- **phantoms** — table-level mutation epochs invalidate any set observation
  (`scan`, `lookup_unique`) when *any* row in the table changes
- **read-your-writes** — the transaction view overlays buffered writes over
  committed state for `get` / `contains` / `scan` / `lookup_unique`

**Deliberately conservative / not claimed:**

- The epoch mechanism over-approximates: a table observed as a set conflicts
  on *any* change to that table, even changes that would not affect the
  predicate result. Correct, never wrong; not minimal. Finer key-range /
  index-range / predicate-dependency observations are future work (they
  replace epochs without changing the transaction model).
- Table drops during an active transaction surface as `NotFound` at commit.

See §17 for the full serializability analysis and the exact dependency
classes that are protected.

### Q19. What concurrency model does the transaction engine assume?

Same as Phase 3: **single-threaded exclusive ownership per store**. The tx
engine adds no locks, no atomics, no interior mutability. Two transactions
against one store execute serially; OCC detects conflicts between a tx's
observation time and its commit time whenever another writer intervened. The
Phase 10 runtime will provide worker/partition ownership → serialized
transaction execution → retry on `Conflict`.

### Q20. What exact API will future reducers use?

`ReducerContext` (Phase 6) will wrap a `Transaction` + `&TableStore`:

```rust
ctx.get("players", row_id)          // records read, returns row
ctx.insert("players", row)          // → provisional RowId
ctx.update("players", row_id, row)
ctx.delete("players", row_id)
ctx.commit()                        // → Vec<Change>, or Conflict to retry
```

No new primitives are needed; the tx API *is* the reducer API.

### Q21. How will future subscriptions observe committed changes?

Subscriptions (Phase 8) consume the change set produced by `commit()` — the
exact `Vec<Change>` returned (and/or the drained per-table buffers). Change
records already carry table id, row id, kind, old/new rows and versions. The
subscription engine stays decoupled from storage internals.

### Q22. How can future WAL consume committed changes?

Phase 5 WAL appends the `Vec<Change>` returned by `commit()` as commit records.
Recovery replays them through the same `insert/update/delete` API. The
transaction engine already produces exactly what a WAL needs; no redesign.

### Q23. What deterministic ordering is used during commit?

Full order is defined (Section 6): tables ascending by `TableId`; within a
table, **all deletes first** (ascending `RowId`), then updates+inserts
(ascending `RowId`; provisional ids sort after real ids, so inserts apply in
submission order). Change records follow the same order. All set structures are
`BTreeMap` (ordered); the claims map is only ever probed by key, never
iterated. No `HashMap` iteration influences observable behavior.

### Q24. What prevents partially committed multi-table transactions?

Validation precedes every mutation; apply is infallible post-validation and
holds exclusive `&mut` store ownership throughout; only after the full apply
are Change records produced. There is no interleaving point where another
observer can see "A committed, B not".

## 4. Transaction state machine

```text
           ┌────────────┐
  begin →  │  Active    │
           └─────┬──────┘
      commit ok  │  │  commit fails / abort
                 ▼  ▼
           ┌────────────┐
           │  Committed │    Aborted ─────────────┐
           └────────────┘    ▲                    │
                             └──── abort (no-op) ─┘
```

Allowed transitions: `Active → Committed`, `Active → Aborted`
(commit failure or abort), `Aborted → Aborted` (abort is idempotent).
Forbidden: `Committed → *` (no reuse), `Aborted → Committed`.

- any operation on a `Committed` tx → `Error::AlreadyCommitted`
- any operation on an `Aborted` tx → `Error::AlreadyAborted`
- `abort()` on `Committed` → `Error::AlreadyCommitted`
- `abort()` on `Aborted` → `Ok` (no-op, already there)
- `commit()` on `Aborted` → `Error::AlreadyAborted`

## 5. OCC algorithm (recap)

```text
begin(id)
  reads = {}; writes = {}; table_reads = {}
get/contains(table, row)                        // overlay: own writes win
  if buffered write exists → return pending state, record nothing
  else reads[(tid,row)] = table.version_of(row) // Some(v) or None
update/delete(table, row)                       // write-time version capture
  if real row and no buffered entry → reads[(tid,row)] = version_of(row)
  buffer the write (coalescing rules)
scan/lookup_unique(table, ...)                  // set observation
  table_reads[tid] = table.epoch()              // phantom protection
  return overlay of committed rows + pending writes
commit(store)
  1. validate(store):                           // pure
       table_reads: epoch == observed, else Conflict
       reads: version_of == observed, else Conflict
       writes: existence, unique keys vs live + released + claims
  2. apply(store):                              // infallible post-validation
       deletes first, then updates/inserts
  3. drain change delta from touched tables
  state = Committed
```

## 6. Commit algorithm (multi-table atomic)

```rust
fn commit(store: &mut TableStore, tx: &mut Transaction) -> Result<Vec<Change>> {
    // 0. touched tables + their change-buffer lengths (pre-apply)
    let bases: BTreeMap<TableId, usize> = touched_tables(tx)
        .map(|t| (t, store.table_by_id(t).unwrap().changes().len()))
        .collect();

    // 1. VALIDATE — pure, no mutation
    validate(&*store, tx)?;                       // on Err: state = Aborted

    // 2. APPLY — deterministic, infallible post-validation
    for ((tid, row), entry) in writes {           // deletes only, ascending
        store.table_mut_by_id(tid).unwrap().delete(row).unwrap(); // invariant
    }
    for ((tid, row), entry) in writes {           // updates + inserts
        match entry {
            Update(r) => store.table_mut_by_id(tid).unwrap().update(row, r).unwrap(),
            Insert(r) => { store.table_mut_by_id(tid).unwrap().insert(r).unwrap(); }
            Delete    => unreachable!(),
        }
    }

    // 3. COLLECT — per touched table, drain delta, in table-id order
    let mut changes = Vec::new();
    for (tid, base) in &bases {
        let table = store.table_mut_by_id(*tid).unwrap();
        changes.extend(table.drain_changes().into_iter().skip(*base));
    }
    Ok(changes)
}
```

Ordering rules (deterministic, documented):

1. tables ascending by `TableId`
2. within a table: all **deletes** first (ascending `RowId`), then
   **updates and inserts** (ascending `RowId`; inserts carry provisional ids
   with the high bit set, so they sort after real ids — i.e. insertion order)
3. Change records follow apply order

Why deletes first: a delete frees unique keys, and key reuse by an
update/insert in the same tx is only valid if the freeing delete applies
first. Deletes-first makes validation (which releases deleted rows' keys)
exactly mirror apply.

## 7. Why apply is infallible (and the swap limitation)

Validation replicates every check `Table` performs at apply time (schema, via
write-time validation; existence, via `contains`; unique constraints, via
`lookup_unique` + released + claims). Under single-threaded ownership nothing
changes between validation and apply, so apply cannot fail; residual errors are
`expect()`ed as invariant violations.

**Known limitation (documented):** unique-key *swaps* between two rows in one
transaction are rejected — e.g. `update X: K1→K2` while `Y` owns `K2` is a
conflict even if `Y` simultaneously moves off `K2`. Only *deleted* rows release
keys; an updated row's old key is not released, because sequential per-op
apply cannot move two keys in one atomic step. Workaround: two transactions.
This is conservative (rejects some valid plans) but never wrong and never
leaves partial state.

## 8. Error model

Extends the shared `nexum-core::Error` (still `#[non_exhaustive]`):

| Error | Meaning |
|---|---|
| `Conflict` | a read observation is stale; retry |
| `NotFound` | update/delete target or table does not exist |
| `AlreadyExists` | unique constraint violated by a write |
| `InvalidArgument` | schema violation (arity/type) at write time |
| `InvalidTransaction` | dangling handle, invalid coalescing (delete→update, double-delete, duplicate insert) |
| `AlreadyCommitted` | operation on a committed transaction |
| `AlreadyAborted` | operation on an aborted transaction |

## 9. Change-buffer ownership invariant

The transaction engine is the **primary write path**. `commit()` drains each
touched table's change buffer and returns the delta (base-length slice). Any
non-transaction changes that were already buffered in a touched table are
drained and discarded by the commit. Direct (non-transactional) writes and
transactional writes must therefore not be mixed against the same table
without draining direct-write buffers first; later consumers (WAL Phase 5,
subscriptions Phase 8) consume the change set returned by `commit()`.

## 10. Storage/table integration

The tx engine uses only the public abstraction:

- reads: `table.version_of`, `table.get`, `table.contains`, `table.epoch`
  (new, correction §17), `table_by_id`
- writes: `table.insert`, `table.update`, `table.delete`
- constraint validation: `table.unique_keys`, `table.lookup_unique`
- set observation: `table.epoch` (mutation epoch for phantom protection)
- change collection: `table.changes`, `table.drain_changes`
- registry: `table_by_id`, `table_mut_by_id`

It never touches `BTreeMap<RowId, StoredRow>` or any storage internals. The
`Table`/`TableStore` methods are additive Phase 4 support (ADR-004 D2);
Phase 2/3 behavior is unchanged.

## 11. Determinism

All observable order is defined by `BTreeMap` ordering (TableId, then RowId)
plus the deletes-first rule. `HashMap`s (claims, provisional counter lookup)
are only probed by key. A replayed commit produces identical Change order and
state.

## 12. Complexity

| Operation | Complexity |
|---|---|
| record read | O(log n) live version lookup |
| buffer write | O(log w) (BTreeMap write set) |
| validate read set | O(r · log n) |
| validate write set | O(w · (log n + k)) k = unique indexes |
| apply | O(w · (log n)) |
| collect changes | O(|changes|) |
| tx memory | O(r + w), never O(database) |

## 13. Testing plan (mapped to the brief)

Basic ops; lifecycle (committed/aborted cannot be reused, abort idempotent);
OCC read conflict; missing-row insert conflict; read-then-delete conflict;
write/write stale-read conflict; **write/write without prior read** (write-time
version capture); multi-table atomic commit; failure atomicity (valid write in
A + conflicting write in B → **A untouched**, zero changes); index consistency
after transactional ops; Change records (ordered, correct kinds/versions, none
on failure); coalescing matrix; provisional handle rules; determinism (change
order independent of submission order across tables); table-not-found; schema
rejection. **Correction (§17):** read-your-writes matrix (insert→get,
insert→update→get, insert→delete→get, update→get, update→update→get,
update→delete→get, delete→get, delete→update→get); phantom conflicts
(scan-then-insert, scan-then-delete, scan-then-update, multi-table scan +
write); transaction overlay vs committed state; a tx that never observes a
table does not conflict because an unrelated table changed. Integration test:
multi-table world (players + matches + economy).

## 14. Benchmarks

`crates/nexum-tx/examples/tx_bench.rs` (dependency-free, like Phase 3):
read-only tx, single-row insert tx, multi-row tx, multi-table tx, successful
validation+commit cost, conflicting commit (validation failure). Baselines
only; criterion in Phase 15.

## 15. Non-goals this phase

WAL/snapshots (Phase 5), reducers (6), WASM (7), subscriptions (8),
simulation (9), runtime (10), networking (11), phantom/range read sets,
predicate isolation, distributed transactions.

## 16. Design checklist

- ✅ No authoritative mutation before full validation
- ✅ ReadSet with absent-as-None semantics
- ✅ WriteSet with documented deterministic coalescing
- ✅ OCC validation: read versions, existence, unique keys, released, claims
- ✅ Multi-table atomic commit with documented deterministic order
- ✅ Failed validation → Aborted, zero mutations, zero changes
- ✅ Indexes consistent by construction; invariant tests
- ✅ Change records from committed txs only; delta-drained deterministically
- ✅ Explicit state machine; forbidden transitions enforced
- ✅ Error model reuses/extended `nexum-core::Error`
- ✅ Honest isolation documentation (phantoms, read-your-writes documented)
- ✅ No locks, no unsafe, tx memory ∝ read/write set
- ✅ WAL (Phase 5) and subscriptions (Phase 8) attach at the Change boundary
- ✅ Correction: read-your-writes overlay; write-time version capture; phantom
  protection via table mutation epochs; conservative serializability claim

## 17. Phase 4 correction — read-your-writes + phantom protection

Applied before Phase 5 per the correction brief. Two semantic upgrades and an
honest serializability analysis. All Phase 1–4 behavior is preserved and the
existing tests remain green.

### 17.1 Read-your-writes

The transaction's logical view is

```text
committed authoritative state  +  the transaction's own buffered writes
```

and its own writes take precedence:

| Operation on a row with a buffered write | Transaction sees |
|---|---|---|
| `get` / `contains` on `Insert(row)` / `Update(row)` | the pending row |
| `get` / `contains` on `Delete` | absent |
| `scan` over a row with `Update(row)` | the pending row |
| `scan` over a row with `Delete` | the row is hidden |
| `scan` | pending inserts appended after committed rows |
| `lookup_unique` on a key | pending owners win; logically-deleted rows are hidden |

Design rules:

- Reads of rows with a buffered write **do not record a row observation** —
  the write entry already governs validation (recording would self-conflict,
  e.g. delete-then-get).
- `update` / `delete` of a **real** row capture the row's committed version
  *at write time* (before buffering). This closes the lost-update window:
  write/write conflicts are now detected even without an explicit prior read.
- A provisional (insert) handle passed to `get` without a buffered insert is
  an `InvalidTransaction` (dangling handle — consistent with `update`/
  `delete`).
- Provisional handles with pending inserts resolve through the write set, so
  `insert → get → update → get → commit` works end to end.
- No authoritative storage is mutated to implement any of this — all
  provisional state stays transaction-local until commit.

Transactional reads return **owned rows** (`Option<Row>`, `Vec<(RowId, Row)>`
for scans): a row may come from the committed store *or* from the write set,
so a single borrowed return type cannot express both. A future zero-copy
`Committed(&'s Row) | Local(Row)` enum is possible if profiling demands it.

### 17.2 Phantom protection — table mutation epoch

Every `StorageTable` carries a **mutation epoch** (a `Version`-typed
counter). **Any committed row mutation advances it**: insert, delete, and
*update that actually changes a row* (a Phase 3 no-op update changes no
predicate result, so it does not advance it).

```text
ANY committed row mutation → table.epoch advances by one
```

Set observations record `(TableId, epoch)` in the read set:

- `tx.scan` records the table's epoch before reading
- `tx.lookup_unique` records the table's epoch (a unique-key lookup observes
  the index as a set)

Validation compares observed vs live epoch; mismatch → `Conflict`. Because
*any* row mutation advances the epoch, this invalidates every set observation
when the table changes in any way — including changes that would not actually
affect the predicate's result. That is the documented, accepted conservatism
of the initial mechanism; key-range / index-range / predicate-dependency
observations can replace it later without changing the transaction model.

Determinism note: the epoch is compared, never iterated, and only grows; its
exact value is reconstructed identically by WAL replay (Phase 5) because every
replayed mutation advances it the same number of times.

### 17.3 What is protected — the honest serializability claim

Dependency classes and their validation:

| Dependency | Mechanism | Protected |
|---|---|---|
| point read of a row | row version observation | ✅ |
| write to a row | write-time version capture (lost-update detection) | ✅ |
| missing-row read → later insert | `None` observation | ✅ |
| delete | delete-time version capture + existence | ✅ |
| unique-key insert/update | live-owner + released + claims validation | ✅ |
| predicate/set read (`scan`, `lookup_unique`) | table mutation epoch | ✅ |
| multi-table atomicity | validate-then-apply with `&mut` store | ✅ |
| arbitrary future predicates | — | ⬜ (epoch today; granular later) |

Claim, stated precisely: **for the supported operation set, the model
provides conservative serializability** — every committed schedule is
serializable (each transaction behaves as if executed atomically at its
commit point), because every dependency a transaction has is validated
against live state at commit, and conflicts abort the transaction. The epoch
over-approximation can produce *false* conflicts but never *missed* ones.
What is **not** claimed: minimal concurrency, optimal throughput, or
protection for predicates outside the current operation set (future query
engine predicates must register their own observations).
