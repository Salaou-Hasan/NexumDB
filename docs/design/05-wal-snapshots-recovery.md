# Phase 5 Design — WAL + Snapshots + Recovery

**Status:** Accepted — to be implemented as specified here
**Phase:** 5 (durability)
**Relates to:** ADR-003 (storage), ADR-004 (transactions), ADR-005 (durability model)

## 1. Purpose

Make committed state **durable** and **recoverable** without redesigning the
Phase 3 storage model or the Phase 4 transaction engine. The durability layer
attaches at the one boundary the transaction engine already produces:

```text
Transaction::commit(&mut store) → Vec<Change>
                                        ↓
                                    WAL append
                                        ↓
                              durability acknowledged
```

The authoritative state stays the in-memory table system; the WAL and
snapshots are **derived, replayable infrastructure** — never competing sources
of truth (ADR-003 D1). Recovery reconstructs the authoritative state and
rebuilds derived indexes from it.

## 2. Where durability lives (architecture)

`nexum-storage` remains the **pure in-memory authoritative store** (Phase 3).
File-backed durability is a new crate, `nexum-wal`, which depends on
`nexum-table` (for reconstruction and replay) — the original repo sketch put
WAL under `nexum-storage`, but recovery needs `TableStore`, and
`nexum-table → nexum-storage` already exists, so WAL-in-storage would be a
cycle. Documented deviation (ADR-005 D1).

```text
nexum-core ← nexum-storage ← nexum-table ← nexum-wal
                  ↑                            (WAL, snapshots, recovery)
                  └── nexum-core::binary (shared codec)
```

- `nexum-core::binary` — deterministic little-endian codec for `Value`, `Row`,
  `TableSchema`, plus a dependency-free CRC-32 implementation.
- `nexum-storage::snapshot` — `TableState` (the serializable authoritative
  per-table state: id, schema, rows+versions, `next_row_id`, `epoch`) and
  capture/restore on `StorageTable`.
- `nexum-table` — `Table::table_state` / `Table::from_state` (index rebuild
  from a full scan), `TableStore::snapshot_tables` / `TableStore::restore`,
  counter accessors.
- `nexum-wal` — WAL file format, durability policies, snapshot files, and the
  `recover()` orchestration.

## 3. Durability contract

Two distinct facts, never conflated:

| Term | Meaning | Who guarantees it |
|---|---|---|
| **committed** | the transaction validated and was applied to in-memory authoritative state; `Change[]` produced | `Transaction::commit` (Phase 4) |
| **durable** | the transaction's `Change[]` are on stable storage such that recovery reproduces them | `Wal::append` returning `Ok` under the `Sync` policy |

Behavior per failure mode:

- **Normal commit**: `tx.commit(&mut store)` (memory) → `wal.append(tx_id, &changes)`
  (durability). A caller must not acknowledge a commit to a client until
  `append` returns `Ok`.
- **WAL write / flush / fsync failure**: `append` returns `Err`. The
  in-memory state already contains the transaction, but it is **not durable**;
  the contract is that it was never acknowledged, so losing it on crash is
  consistent. `append` truncates the file back to the pre-append offset on
  failure so no partial record breaks future appends. A runtime may either
  propagate the error to the caller or reload state from snapshot + WAL.
- **Process crash mid-append**: the WAL tail is a partial record or a group
  without its `COMMIT` marker. Recovery drops exactly that — no partial
  transaction is ever reconstructed (commit framing, §5).
- **Corrupted WAL tail** (bit rot / torn write): CRC mismatch → recovery stops
  at the first bad record and discards the tail (§8).
- **Recovery failure** (e.g. a replayed change cannot be applied): an internal
  invariant violation → `Error::Internal`. Recovery never silently
  approximates.

**Batching/group commit is deferred** — one fsync per committed transaction
under the `Sync` policy. Correctness first (ADR-005 D5).

## 4. WAL file format

Deterministic, dependency-free binary. All integers little-endian.

```text
FILE   := HEADER RECORD*
HEADER := magic "NEXW" (4) | format_version u32 = 1 | created_at u64 (Timestamp millis)
RECORD := payload_len u32 | kind u8 | payload [payload_len] | crc32 u32
CRC    := crc32(kind byte ++ payload)
```

Record kinds (payload layouts):

| kind | name | payload |
|---|---|---|
| 1 | `BEGIN_TX` | tx_id u64 |
| 2 | `CHANGE` | see below |
| 3 | `COMMIT_TX` | tx_id u64 · change_count u32 |

`CHANGE` payload (one per committed row mutation):

```text
table_id u64 | kind u8 (1=insert, 2=update, 3=delete)
row_id u64
old_row    u8 has | (if has) row
new_row    u8 has | (if has) row
old_version u8 has | (if has) u64
new_version u8 has | (if has) u64
row        := value_count u64 | value*           (via nexum-core::binary)
value      := type_tag u8 | payload              (fixed-width LE, string/bytes length-prefixed)
```

**LSN** (log sequence number) is the byte offset of a record's `payload_len`
field within the file. `Wal::lsn()` is the offset where the *next* record will
be written (== file length after the header in a clean log).

Incomplete/corrupt detection: a record needs `4 + 1 + payload_len + 4` bytes;
a short read is an incomplete tail; a CRC mismatch is corruption. Both stop
recovery at that point (§8).

## 5. Commit framing (multi-table atomicity on disk)

A transaction is framed as:

```text
BEGIN_TX (tx_id)
  CHANGE × n          (the exact Vec<Change> from commit(), in commit order)
COMMIT_TX (tx_id, n)
```

Recovery keeps a group only if it sees the matching `COMMIT_TX` with the same
`tx_id` and `change_count`. A group without its commit marker — however many
`CHANGE` records were written — is an **uncommitted/crashed transaction** and
is discarded as a whole. This is what makes multi-table commits atomic on
disk: either the full `Change[]` of tables A+B+C is present with its `COMMIT`,
or none of it is reconstructed.

## 6. WAL durability

`DurabilityPolicy`:

| policy | on `append` | meaning |
|---|---|---|
| `Flush` | write + `flush()` | data handed to the OS; survives process crash, not power loss |
| `Sync` | write + `flush()` + `sync_all()` | fsync; the durable mode — survives power loss |

`append` builds the whole framed group in memory, writes it once, then applies
the policy. It returns the LSN of the `COMMIT_TX` record — the durability
point.

## 7. WAL recovery

`Wal::recover_changes()` reads the file from offset 0:

1. validate the header (magic + version)
2. iterate records; on short read / CRC mismatch → stop (the tail is dropped)
3. `BEGIN_TX` opens a group (an already-open group is a crashed predecessor —
   discard it)
4. `CHANGE` appends to the open group (a `CHANGE` with no open group is
   malformed → `Error::Internal`)
5. `COMMIT_TX` closes the group (tx_id and count must match) and yields a
   `RecoveredTx { tx_id, commit_lsn, changes }`
6. at EOF, an open group is dropped (incomplete transaction)

`Wal::open` performs the same scan and **truncates any physically invalid tail**
to the last valid record boundary, so subsequent appends are always
recoverable (a log with a torn tail must not accumulate unreachable records).

Recovery replays through the **recovery/replay boundary** — the plain
`Table::insert/update/delete` API — *not* through OCC validation (replay is
not a new transaction; it reproduces history). Because insert assigns
monotonic ids, updates bump versions by one, and every row mutation advances
the epoch, replaying the identical change sequence reconstructs rows, row
ids, versions, epochs, `next_row_id`, and derived indexes **exactly**.
Replay verifies each insert assigns the change's recorded `row_id` (else
`Error::Internal` — a bug, not a tolerated deviation).

## 8. Replay idempotency

- Replaying the same WAL (from the same snapshot) twice against fresh stores
  produces bit-identical state — the procedure is a pure function of
  (snapshot, WAL).
- The snapshot LSN splits history: records at offset `< snapshot_lsn` are
  already incorporated into the snapshot and are **never** replayed;
  records at `>= snapshot_lsn` are replayed exactly once. A snapshot is only
  ever taken between appends (the durability layer serializes them), so the
  split never falls inside a transaction.

## 9. Snapshots

A snapshot captures the **authoritative** state — nothing derived:

```text
per table: id · schema · rows (RowId, Row, Version) · next_row_id · epoch
store:     next_table_id · next_transaction_id
```

Indexes are **not** serialized; `Table::from_state` rebuilds them from a full
scan of the restored rows (Phase 3: indexes are provably derived). The
transaction-model metadata (`epoch`) is included so post-restore phantom
validation is correct without any replay.

Snapshot file format:

```text
HEADER := magic "NEXS" (4) | version u32 = 1 | lsn u64 | next_table_id u64
          | next_transaction_id u64 | created_at u64 | header_crc32 u32
BODY   := body_len u64 | table_states (concatenated) | body_crc32 u32
TABLE  := state_len u32 | TableState bytes
```

Two CRCs (header, body) make torn snapshots detectable; a snapshot is written
to a temp file and **atomically renamed** into place, so a crash can leave a
stale-but-valid older snapshot or a valid new one, never a half-written one.
Snapshots are named `snapshot-<lsn>.snap`; `find_latest` picks the highest
valid LSN.

## 10. Snapshot + WAL interaction

```text
recover(store, wal, snapshot_dir):
  1. find the latest valid snapshot → restore tables, counters into store
  2. wal.recover_changes() → committed txs in WAL order
  3. replay txs with commit_lsn >= snapshot.lsn through the Table API
  4. advance next_transaction_id past the last replayed tx
  5. drain every table's change buffer (replayed history is not fresh events)
```

Recovery reproduces the state that existed at the durability boundary: every
transaction whose `COMMIT_TX` was fsynced before the crash is present; every
unacknowledged transaction is absent; nothing is half-applied.

## 11. Change boundary preserved

```text
Transaction → commit() → Vec<Change> → WAL        (Phase 5)
                                     ├→ subscriptions (Phase 8)
                                     └→ replication / event systems (later)
```

WAL never touches `BTreeMap<RowId, StoredRow>`, write sets, or read sets; it
consumes committed changes and replays them through the public Table API.

## 12. Performance

Baseline benchmarks (`nexum-wal/examples/durability_bench.rs`): WAL append
(Flush and Sync), recovery/replay of N transactions, snapshot write and load.
No optimization before correctness (no group commit, no page compression).

## 13. Boundaries (this phase)

**DO:** WAL, durability policies, commit framing, CRCs, recovery, snapshots,
snapshot/WAL integration, crash-consistency tests, benchmarks, documentation.

**DO NOT:** reducers (6), WASM (7), subscriptions (8), simulation (9),
networking (11), replication, multi-file log rotation/segments, async I/O,
group commit, snapshot compaction/scheduling.

## 14. Completion criteria (mapped)

1. ✅ Phase 4 correction landed (read-your-writes + phantom epochs)
2. ✅ Design documented (this file + ADR-005)
3. ✅ WAL format documented and deterministic
4. ✅ Commit framing with BEGIN/CHANGE/COMMIT
5. ✅ Durability contract explicit (`committed` vs `durable`)
6. ✅ CRCs detect corruption; incomplete tails handled safely
7. ✅ Recovery reconstructs committed state; multi-table txs atomic on disk
8. ✅ Snapshots capture authoritative state; indexes rebuilt, not stored
9. ✅ Snapshot LSN/WAL integration works
10. ✅ Crash/recovery tests pass; clippy zero warnings; benches exist
11. ✅ Phase 1–4 tests remain green; no Phase 6+ systems implemented
