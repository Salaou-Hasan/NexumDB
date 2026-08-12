# ADR-005 — Durability: WAL, Snapshots, Recovery

**Status:** Accepted
**Phase:** 5
**Date:** 2026-08-11
**Supersedes:** — (new subsystem; refines the Phase 0 repo sketch for
`nexum-storage/{wal,snapshots,recovery}`)

## Context

Phase 3/4 established one authoritative in-memory state with a clean change
boundary (`Transaction::commit → Vec<Change>`). Phase 5 must make committed
state durable and recoverable without redesigning storage or transactions.
The original repo layout placed WAL/snapshots under `nexum-storage`, but
recovery must reconstruct a `TableStore`, and `nexum-table → nexum-storage`
already exists — placing recovery in `nexum-storage` would create a cycle.

## Decisions

### D1. Durability lives in a new crate, `nexum-wal`

`nexum-wal` depends on `nexum-core`, `nexum-storage`, and `nexum-table`; it
owns the WAL file format, durability policies, snapshot files, and the
`recover()` orchestration. `nexum-storage` stays a pure in-memory
authoritative store. This is a documented deviation from the Phase 0 repo
sketch (which predates the Phase 3 storage design).

### D2. The WAL consumes committed changes only

WAL records are built from the `Vec<Change>` returned by `commit()`. The WAL
never reads storage internals, write sets, or read sets. Recovery replays
through the public `Table::insert/update/delete` API (the **recovery/replay
boundary**), never through OCC validation.

### D3. Commit framing: BEGIN / CHANGE* / COMMIT

Every transaction is written as `BEGIN_TX`, one `CHANGE` per mutation, then
`COMMIT_TX` with the tx id and change count. Recovery keeps a group only with
a matching, complete commit marker. This is what makes multi-table commits
atomic on disk and what discards crashed transactions whole.

### D4. LSN-based snapshot/WAL split

An LSN is a record's byte offset. A snapshot records the LSN where the next
record would be written; recovery replays only records at or after that LSN.
Snapshots are taken only between appends, so the split never falls inside a
transaction and nothing is double-applied. Snapshots are written via
temp-file + atomic rename and carry header/body CRCs.

### D5. Durability is explicit: committed ≠ durable

`committed` = applied in memory (Phase 4); `durable` = `Wal::append` returned
`Ok` under the chosen policy. `Flush` (write+flush) survives process crash;
`Sync` (write+flush+fsync) survives power loss — the durable mode. Group
commit and batching are deferred; one fsync per transaction for now.
`append` truncates back to the pre-append offset on failure so a torn record
never breaks future appends.

### D6. Recovery reproduces state exactly via replay

Replay through the Table API reproduces rows, RowIds, versions, epochs, and
`next_row_id` exactly (inserts assign monotonic ids; updates bump versions;
every mutation advances the epoch). Replay verifies each insert's assigned id
matches the change's recorded id. Indexes are rebuilt from the restored rows,
not serialized. Replayed history is drained from the change buffers so
consumers see only fresh changes.

### D7. Corrupted tails are dropped, never guessed

A short record or CRC mismatch stops recovery at that point; the tail is
discarded. `Wal::open` truncates the physically invalid tail so subsequent
appends stay recoverable. Replaying the same (snapshot, WAL) twice yields
bit-identical state.

### D8. Shared binary codec in `nexum-core::binary`

`Value`, `Row`, and `TableSchema` need one deterministic encoding shared by
the WAL and snapshots (and later the wire protocol). It lives in the
dependency-free core crate together with a small CRC-32 implementation; no
external serialization dependency is introduced in this phase.

## Consequences

- A crashed, unacknowledged transaction never reappears; every fsynced
  transaction always does; no partial multi-table state is ever visible.
- The change boundary (`commit → Vec<Change>`) remains the single attach
  point for WAL, subscriptions, and later replication.
- `nexum-storage` and `nexum-tx` are unchanged by durability — Phase 1–4
  tests stay green.
- Cost: one fsync per transaction under `Sync` (deferred optimization), and
  replay requires the WAL to retain history since the last snapshot (snapshot
  scheduling/compaction is future work).
