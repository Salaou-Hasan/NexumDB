# ADR-004 — Transaction Engine with Optimistic Concurrency Control

**Status:** Accepted
**Phase:** 4
**Date:** 2026-08-11
**Supersedes:** — (new subsystem)

## Context

NexumDB needs one authoritative mutation path. Tables (Phase 2), storage
(Phase 3), and the future reducer/simulation layers must all change state
through transactions that are atomic across tables, optimistic about
concurrency, and deterministic. The Phase 3 storage model is single-threaded
exclusive ownership; the transaction engine must build on that without
introducing locks or interior mutability.

## Decisions

### D1. Transaction is a pure accumulator; the store is passed per call

`Transaction` owns its id, state, read set, write set, and provisional-id
counter — never a store reference. Every method takes `&TableStore` (reads) or
`&mut TableStore` (commit). This keeps borrows per-operation, matches the Phase 3
ownership model, and lets direct writes / other transactions interleave between
a tx's calls — which is precisely what OCC detects. No interior mutability.

### D2. Three additive `Table`/`TableStore` methods, no interface changes

- `Table::unique_keys(&Row) -> Result<Vec<(String, Vec<Value>)>>` — unique
  index keys of a row ("primary" plus unique secondary indexes), for
  constraint validation without mutation.
- `Table::lookup_unique(&str, &[Value]) -> Result<Vec<RowId>>` — live owners
  of a key in a unique index (primary or secondary), zero-or-one id.
- `TableStore::table_mut_by_id(TableId) -> Option<&mut Table>` — registry
  access by id for commit.

All existing Phase 2/3 APIs and behaviors are unchanged. The tx engine never
touches storage internals.

### D3. OCC validation is pure; apply is infallible post-validation

`validate` compares every read observation against live versions (`None` =
absent, a first-class observation), checks write existence, and checks unique
keys against live owners minus **deleted-row key release** plus a tx-local
**claims** map. Only after full validation does commit apply — deletes first,
then updates/inserts, all in `(TableId, RowId)` order. Apply errors after
validation are invariant violations (`expect`). No apply-then-rollback path.

### D4. Deletes-first commit order; unique-key swaps are rejected

Deletes apply before updates/inserts so freed unique keys can be reused within
the same transaction. An updated row's *old* key is deliberately **not**
released (sequential per-op apply cannot move two keys atomically), so
cross-row unique-key swaps inside one transaction are rejected conservatively
(never wrong, never partial; two transactions is the documented workaround).

### D5. Deterministic coalescing in the write set

One `WriteEntry` per `(TableId, RowId)`. insert→update = final insert;
insert→delete = net no-op; update→update = latest; update→delete = delete;
delete→update and double-delete are errors; a provisional handle must reference
a pending insert. Full matrix in the design doc.

### D6. Provisional RowIds for in-transaction insert handles

Inserts return a RowId with the high bit (`1<<63`) set, allocated per table.
It is a coalescing handle only — storage assigns the real monotonic id at
commit. Real storage ids never set the high bit.

### D7. Transaction state machine is explicit and enforced

`Active → Committed`, `Active → Aborted`, `Aborted → Aborted` (abort is
idempotent). Operations on a committed tx → `AlreadyCommitted`; on an aborted
tx → `AlreadyAborted`. No other transitions exist.

### D8. Error model extends `nexum-core::Error`

New variants `InvalidTransaction`, `AlreadyCommitted`, `AlreadyAborted` added
to the `#[non_exhaustive]` shared enum. Reuses `Conflict`, `NotFound`,
`AlreadyExists`, `InvalidArgument`. No private error system in nexum-tx.

Coalescing misuse (dangling handles, delete→update, double-delete, duplicate
insert) maps to `InvalidTransaction`; schema violations (arity/type) at write
time map to `InvalidArgument`.

### D9. Change records are the future attach point

`commit()` returns the delta-drained `Vec<Change>` (per touched table, in
`TableId` order, base-length-sliced so only this tx's changes are returned).
Phase 5 WAL appends them; Phase 8 subscriptions consume them. Neither is
implemented now.

**Change-buffer ownership invariant:** the transaction engine is the primary
write path. `commit()` drains the full buffer of each touched table and
returns only the delta; any pre-existing non-transaction changes in a touched
table are drained and discarded. Direct writes and transactions must not be
mixed against the same table without draining direct-write buffers first.

### D10. Known forward-compat note: provisional → real RowId mapping

`tx.insert` returns a provisional handle (high bit set) that is only valid
within the transaction. After commit, the caller recovers the real `RowId`
from the returned `Change` records (each insert change carries the real row
id). Phase 6 reducers will likely want a direct mapping (e.g. a map returned
from `commit`, or an insert that reports the real id); this is deferred rather
than invented now. Tests that need real ids use the Change records.

### D11. Isolation is documented, not overstated

Superseded by the Phase 4 correction (D12–D14): read/write, write/write,
missing-row→insert, delete, insert vs live unique keys, multi-table atomicity
were already protected; phantoms and read-your-writes were documented
limitations and are now addressed.

### D12. Read-your-writes: the transaction view overlays buffered writes

Reads observe committed state overlaid with the transaction's own write set
(own writes win). Rows with a buffered write record no row observation (the
write entry governs validation). `update`/`delete` of a real row capture the
row's committed version at write time, closing the lost-update window for
write/write conflicts without an explicit prior read. Transactional reads
return owned rows because a row may come from either the store or the write
set. No storage is mutated for this; all provisional state stays
transaction-local.

### D13. Phantom protection: table mutation epochs

Every `StorageTable` carries a mutation epoch advanced by **any** committed
row mutation (insert / delete / effective update; Phase 3 no-op updates do
not advance it). `tx.scan` and `tx.lookup_unique` record `(TableId, epoch)`
as set observations; validation conflicts if the live epoch differs. This is
deliberately conservative (false conflicts possible, never missed conflicts)
and replaceable by granular key-range / predicate observations later without
changing the transaction model.

### D14. The serializability claim is precise, not casual

For the supported operation set, the model provides **conservative
serializability**: every dependency a transaction has — point reads, writes,
missing rows, deletes, unique keys, set/predicate reads — is validated at
commit, so every committed schedule is serializable. Not claimed: minimal
concurrency, optimal throughput, protection for future arbitrary predicates
(those must register their own observations).

## Consequences

- A conflicting or failed transaction leaves **zero** authoritative mutations,
  zero version bumps, zero Change records.
- Deterministic observable behavior: all ordering comes from `BTreeMap`
  iteration (TableId, RowId); `HashMap`s are probed by key only.
- The Phase 6 reducer API is a thin wrapper over the tx API; Phase 10 runtime
  serializes transactions per partition and retries on `Conflict`.
- Known limitations: unique-key swaps rejected within one transaction;
  conservative (over-approximating) phantom conflicts; table drops during an
  active transaction surface as `NotFound`; single-threaded per store.
