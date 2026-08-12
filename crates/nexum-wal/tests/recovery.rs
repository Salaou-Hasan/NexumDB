//! Crash-consistency tests for WAL + snapshots + recovery (Phase 5 brief §9).
//!
//! Covers: append/recover roundtrips · incomplete transaction groups ·
//! truncated record tails · corrupted checksums · snapshot roundtrips ·
//! snapshot + WAL continuation · full recovery without a snapshot ·
//! multi-table atomicity on disk · exact reconstruction of versions, epochs,
//! row ids, and indexes · replay idempotency.
//!
//! Crash scenarios (incomplete groups, torn records, corrupted checksums) are
//! crafted with a small raw-record writer that mirrors the *documented* WAL
//! format — the tests act as an external spec check, not a white-box reuse.

use std::path::PathBuf;

use nexum_core::binary::{crc32, put_row, put_u64};
use nexum_core::{
    ChangeKind, ColumnType, Error, RowId, TableId, TableSchema, Timestamp, TransactionId, Value,
    Version,
};
use nexum_storage::Change;
use nexum_table::{row, TableStore};
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};

// --------------------------------------------------------------- helpers

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nexum-wal-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// A store with `players` (id 0) and `economy` (id 1), both with indexes.
fn world() -> TableStore {
    let mut store = TableStore::new();
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .column("health", ColumnType::I32)
                .column("level", ColumnType::U32)
                .primary_key(&["id"])
                .index("by_zone", &["zone_id"])
                .unique_index("by_level", &["level"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
        .create_table(
            TableSchema::builder("economy")
                .column("owner", ColumnType::U64)
                .column("coins", ColumnType::I64)
                .primary_key(&["owner"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

/// Commits `f` as a transaction and makes it durable, returning the changes.
fn commit_one(
    store: &mut TableStore,
    wal: &mut Wal,
    f: impl FnOnce(&mut Transaction, &TableStore),
) -> Vec<Change> {
    let mut tx = Transaction::begin(store);
    f(&mut tx, store);
    let changes = tx.commit(store).unwrap();
    wal.append(tx.id(), &changes).unwrap();
    changes
}

// ---------------------------------------------- raw WAL crafting (spec check)

fn raw_header(out: &mut Vec<u8>) {
    out.extend_from_slice(b"NEXW");
    out.extend_from_slice(&1u32.to_le_bytes());
    put_u64(out, Timestamp::ZERO.as_millis());
}

fn raw_record(out: &mut Vec<u8>, kind: u8, payload: &[u8]) {
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.push(kind);
    out.extend_from_slice(payload);
    let mut crc_input = Vec::with_capacity(1 + payload.len());
    crc_input.push(kind);
    crc_input.extend_from_slice(payload);
    out.extend_from_slice(&crc32(&crc_input).to_le_bytes());
}

fn raw_begin(out: &mut Vec<u8>, tx_id: u64) {
    let mut payload = Vec::new();
    put_u64(&mut payload, tx_id);
    raw_record(out, 1, &payload);
}

fn raw_change(out: &mut Vec<u8>, change: &Change) {
    let mut payload = Vec::new();
    put_u64(&mut payload, change.table_id().as_u64());
    payload.push(match change.kind() {
        ChangeKind::Insert => 1,
        ChangeKind::Update => 2,
        ChangeKind::Delete => 3,
    });
    put_u64(&mut payload, change.row_id().as_u64());
    for row in [change.old_row(), change.new_row()] {
        match row {
            Some(row) => {
                payload.push(1);
                put_row(&mut payload, row);
            }
            None => payload.push(0),
        }
    }
    for version in [change.old_version(), change.new_version()] {
        match version {
            Some(version) => {
                payload.push(1);
                put_u64(&mut payload, version.as_u64());
            }
            None => payload.push(0),
        }
    }
    raw_record(out, 2, &payload);
}

fn raw_commit(out: &mut Vec<u8>, tx_id: u64, count: u64) {
    let mut payload = Vec::new();
    put_u64(&mut payload, tx_id);
    put_u64(&mut payload, count);
    raw_record(out, 3, &payload);
}

/// Returns the start offsets of every record in a raw WAL byte buffer.
fn record_offsets(bytes: &[u8]) -> Vec<u64> {
    let mut offsets = Vec::new();
    let mut off = 16u64; // HEADER_LEN
    while off < bytes.len() as u64 {
        offsets.push(off);
        let len =
            u32::from_le_bytes(bytes[off as usize..off as usize + 4].try_into().unwrap()) as u64;
        off += 9 + len;
    }
    offsets
}

fn insert_change(table_id: u64, row_id: u64, row: nexum_core::Row) -> Change {
    Change::insert(
        TableId::from_u64(table_id),
        RowId::from_u64(row_id),
        row,
        Version::ZERO,
    )
}

// ------------------------------------------------------------------ tests

#[test]
fn append_and_recover_roundtrips_transactions_in_order() {
    let dir = temp_dir("roundtrip");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();

    let c1 = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    let c2 = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
        tx.insert(store, "economy", row![2u64, 200i64]).unwrap();
    });
    assert_eq!(c1.len(), 1);
    assert_eq!(c2.len(), 2);

    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated, "a clean log end is not a truncated tail");
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[0].tx_id, TransactionId::from_u64(0));
    assert_eq!(txs[0].changes, c1);
    assert_eq!(txs[1].tx_id, TransactionId::from_u64(1));
    assert_eq!(txs[1].changes, c2);
    // Commit LSNs are increasing (the durability points).
    assert!(txs[0].commit_lsn < txs[1].commit_lsn);
}

#[test]
fn incomplete_transaction_group_is_dropped() {
    let dir = temp_dir("incomplete");
    let path = dir.join("log.wal");
    let mut bytes = Vec::new();
    raw_header(&mut bytes);
    // Tx 0: complete.
    raw_begin(&mut bytes, 0);
    raw_change(&mut bytes, &insert_change(0, 1, row![1u64, 10u64, 100i32, 5u32]));
    raw_commit(&mut bytes, 0, 1);
    // Tx 1: BEGIN + one change written, then the process crashed before
    // COMMIT_TX.
    raw_begin(&mut bytes, 1);
    raw_change(&mut bytes, &insert_change(0, 2, row![2u64, 20u64, 80i32, 7u32]));
    std::fs::write(&path, &bytes).unwrap();

    let mut wal = Wal::open(&path, DurabilityPolicy::Flush).unwrap();
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated);
    assert_eq!(txs.len(), 1, "the uncommitted transaction must be dropped whole");
    assert_eq!(txs[0].tx_id, TransactionId::from_u64(0));
}

#[test]
fn truncated_record_tail_is_dropped_and_appends_stay_recoverable() {
    let dir = temp_dir("truncated");
    let path = dir.join("log.wal");
    let mut wal = Wal::create(&path, DurabilityPolicy::Flush).unwrap();
    let mut store = world();
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    let lsn_after_tx0 = wal.lsn().as_u64();
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
    });

    // Simulate a crash mid-record: tear the tail of the second transaction.
    let len = std::fs::metadata(&path).unwrap().len();
    std::fs::File::options()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(len - 6)
        .unwrap();

    // Opening validates records and truncates the physically invalid tail
    // (the last fully valid record ends before the torn COMMIT of tx 1).
    let mut wal = Wal::open(&path, DurabilityPolicy::Flush).unwrap();
    assert!(wal.truncated_on_open(), "the torn tail was dropped");
    assert!(wal.lsn().as_u64() > lsn_after_tx0, "tx 1's valid BEGIN+CHANGE records remain");
    let (txs, _) = wal.recover_changes().unwrap();
    // Tx 1 has no valid COMMIT_TX: the whole group is dropped.
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].tx_id, TransactionId::from_u64(0));

    // Appends after the truncation remain fully recoverable.
    wal.append(
        TransactionId::from_u64(9),
        &[insert_change(0, 9, row![9u64, 10u64, 1i32, 9u32])],
    )
    .unwrap();
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated);
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[1].tx_id, TransactionId::from_u64(9));
}

#[test]
fn corrupted_checksum_stops_recovery_at_that_record() {
    let dir = temp_dir("corrupt");
    let path = dir.join("log.wal");
    let mut bytes = Vec::new();
    raw_header(&mut bytes);
    for tx in 0..3u64 {
        raw_begin(&mut bytes, tx);
        raw_change(&mut bytes, &insert_change(0, tx + 1, row![tx + 1, 10u64, 100i32, 5u32]));
        raw_commit(&mut bytes, tx, 1);
    }

    // Corrupt the payload of the second transaction's CHANGE record (record
    // index 4: BEGIN0, CHANGE0, COMMIT0, BEGIN1, CHANGE1).
    let offsets = record_offsets(&bytes);
    let change1 = offsets[4] as usize + 5; // 4 length bytes + 1 kind byte
    bytes[change1] ^= 0xFF;
    std::fs::write(&path, &bytes).unwrap();

    let mut wal = Wal::open(&path, DurabilityPolicy::Flush).unwrap();
    assert!(wal.truncated_on_open(), "the corrupt tail was dropped");
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated, "open already removed the corrupt tail");
    // Tx 0 is intact; tx 1 is unrecoverable; tx 2 is beyond the corruption.
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].tx_id, TransactionId::from_u64(0));
}

#[test]
fn snapshot_writes_and_reads_back_exactly() {
    let dir = temp_dir("snapshot");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    let _ = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
        tx.insert(store, "economy", row![2u64, 200i64]).unwrap();
    });

    let snapshot = Snapshot::capture(&store, wal.lsn().as_u64());
    let path = snapshot.write(&dir).unwrap();
    let read = Snapshot::read(&path).unwrap();

    assert_eq!(read.lsn, snapshot.lsn);
    assert_eq!(read.next_table_id, store.next_table_id());
    assert_eq!(read.next_transaction_id, store.next_transaction_id());
    assert_eq!(read.tables, snapshot.tables);
    assert_eq!(read.tables.len(), 2);
}

#[test]
fn recovery_restores_snapshot_then_replays_wal_exactly() {
    let dir = temp_dir("snap-wal");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();

    // Phase 1: two transactions.
    let c1 = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    let c2 = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
        tx.insert(store, "economy", row![1u64, 100i64]).unwrap();
    });
    let alice = c1[0].row_id();
    let bob = c2[0].row_id();
    let alice_zone_id = store
        .table("players")
        .unwrap()
        .get(alice)
        .unwrap()
        .get_named(store.table("players").unwrap().schema(), "zone_id")
        .cloned();
    assert_eq!(alice_zone_id, Some(Value::U64(10)));

    // Phase 2: snapshot here.
    Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir).unwrap();

    // Phase 3: two more transactions after the snapshot.
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.update(store, "players", alice, row![1u64, 30u64, 50i32, 5u32]).unwrap();
    });
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.delete(store, "players", bob).unwrap();
        tx.update(store, "economy", RowId::from_u64(0), row![1u64, 250i64]).unwrap();
    });

    // Reference: what the pre-crash store looked like.
    let expected_epoch = store.table("players").unwrap().epoch();
    let expected_next_tx = store.next_transaction_id();
    let expected_next_table = store.next_table_id();
    assert_eq!(
        store.table("players").unwrap().version_of(alice),
        Some(Version::from_u64(1))
    );

    // Phase 4: recover into a fresh store.
    let mut fresh = TableStore::new();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert!(report.snapshot.is_some());
    assert_eq!(report.replayed_txs, 2);
    assert_eq!(report.replayed_changes, 3); // update + (delete + update)
    assert!(!report.truncated_tail);

    // Rows, versions, epochs, and indexes are exact.
    let players = fresh.table("players").unwrap();
    assert_eq!(players.len(), 1);
    assert_eq!(
        players
            .get(alice)
            .unwrap()
            .get_named(players.schema(), "health"),
        Some(&Value::I32(50))
    );
    assert!(players.get(bob).is_none());
    assert_eq!(players.version_of(alice), Some(Version::from_u64(1)));
    assert_eq!(players.epoch(), expected_epoch);
    assert_eq!(players.lookup("by_zone", &[Value::U64(30)]).unwrap(), vec![alice]);
    assert!(players.lookup("by_zone", &[Value::U64(10)]).unwrap().is_empty());

    let economy = fresh.table("economy").unwrap();
    assert_eq!(
        economy.get(RowId::from_u64(0)).unwrap().get_named(economy.schema(), "coins"),
        Some(&Value::I64(250))
    );

    // Counters continue from where the crash happened.
    assert_eq!(fresh.next_table_id(), expected_next_table);
    assert_eq!(fresh.next_transaction_id(), expected_next_tx);

    // Replayed history is not fresh change events.
    assert!(fresh.drain_changes().is_empty());

    // Recovered store is fully functional: new writes commit cleanly and
    // keep unique constraints (by_level) intact.
    let mut tx = Transaction::begin(&mut fresh);
    tx.insert(&fresh, "players", row![9u64, 40u64, 10i32, 9u32]).unwrap();
    let changes = tx.commit(&mut fresh).unwrap();
    assert_eq!(changes[0].row_id().as_u64(), 2); // RowId allocation continued
    assert!(fresh.table("players").unwrap().get_by_primary_key(&[Value::U64(9)]).unwrap().is_some());
}

#[test]
fn recovery_without_snapshot_replays_everything() {
    let dir = temp_dir("no-snapshot");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    for id in 1..=3u64 {
        commit_one(&mut store, &mut wal, |tx, store| {
            tx.insert(store, "players", row![id, 10u64, 100i32, id as u32]).unwrap();
        });
    }
    let expected_next_tx = store.next_transaction_id();

    // Without a snapshot the WAL carries changes only: the store must already
    // define the tables (schemas are a deployment concern).
    let mut fresh = world();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap(); // empty snapshot dir
    assert!(report.snapshot.is_none());
    assert_eq!(report.replayed_txs, 3);
    assert_eq!(fresh.table("players").unwrap().len(), 3);
    assert_eq!(fresh.next_transaction_id(), expected_next_tx);
}

#[test]
fn crash_mid_multi_table_transaction_commits_nothing() {
    let dir = temp_dir("multi-atomic");
    let path = dir.join("log.wal");
    let mut bytes = Vec::new();
    raw_header(&mut bytes);
    // Tx 0: complete — a players insert (the first insert in a fresh store,
    // so its recorded row id is 0).
    raw_begin(&mut bytes, 0);
    raw_change(&mut bytes, &insert_change(0, 0, row![1u64, 10u64, 100i32, 5u32]));
    raw_commit(&mut bytes, 0, 1);
    // Tx 1: BEGIN, players change AND economy change written, then crash
    // before COMMIT — nothing from tx 1 may survive.
    raw_begin(&mut bytes, 1);
    raw_change(&mut bytes, &insert_change(0, 1, row![2u64, 10u64, 90i32, 6u32]));
    raw_change(&mut bytes, &insert_change(1, 0, row![2u64, 200i64]));
    std::fs::write(&path, &bytes).unwrap();

    let mut wal = Wal::open(&path, DurabilityPolicy::Flush).unwrap();
    let mut fresh = world(); // tables deployed; no snapshot needed
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert_eq!(report.replayed_txs, 1);

    // Neither the players row nor the economy row of tx 1 exists.
    assert_eq!(fresh.table("players").unwrap().len(), 1);
    assert!(fresh.table("players").unwrap().get_by_primary_key(&[Value::U64(2)]).unwrap().is_none());
    assert!(fresh.table("economy").unwrap().is_empty());
}

#[test]
fn replay_is_idempotent_across_recoveries() {
    let dir = temp_dir("idempotent");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    let c1 = commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.update(store, "players", c1[0].row_id(), row![1u64, 30u64, 25i32, 5u32]).unwrap();
    });
    Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir).unwrap();
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![2u64, 40u64, 90i32, 6u32]).unwrap();
    });

    let shape = |store: &TableStore| -> Vec<(u64, u64, String)> {
        store
            .table("players")
            .unwrap()
            .scan()
            .map(|(id, row)| {
                (
                    id.as_u64(),
                    row.get_named(store.table("players").unwrap().schema(), "zone_id")
                        .and_then(Value::as_u64)
                        .unwrap_or(0),
                    format!("{:?}", store.table("players").unwrap().version_of(id)),
                )
            })
            .collect()
    };

    let mut first = TableStore::new();
    recover(&mut first, &mut wal, &dir).unwrap();
    let mut second = TableStore::new();
    recover(&mut second, &mut wal, &dir).unwrap();

    assert_eq!(shape(&first), shape(&second));
    assert_eq!(
        first.table("players").unwrap().epoch(),
        second.table("players").unwrap().epoch()
    );
}

#[test]
fn snapshot_restore_rejects_a_non_empty_store() {
    let dir = temp_dir("non-empty");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Flush).unwrap();
    // Write a snapshot into the dir.
    let mut store = world();
    commit_one(&mut store, &mut wal, |tx, store| {
        tx.insert(store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    });
    Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir).unwrap();

    // Restoring the snapshot into a store that already has tables fails.
    let mut fresh = world(); // NOT empty
    let err = recover(&mut fresh, &mut wal, &dir).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn wal_open_of_missing_file_is_not_found() {
    let dir = temp_dir("missing");
    let err = Wal::open(&dir.join("nope.wal"), DurabilityPolicy::Flush).unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[test]
fn corrupt_huge_length_field_is_dropped_without_allocating() {
    // A record whose length field claims ~4 GiB of payload must be treated as
    // an incomplete tail and dropped — never a multi-GiB allocation attempt
    // or a panic (framing robustness against corrupt input, ADR-005 D7).
    let dir = temp_dir("huge-len");
    let path = dir.join("log.wal");
    let mut bytes = Vec::new();
    raw_header(&mut bytes);
    // Tx 0: complete (first insert in a fresh store → row id 0).
    raw_begin(&mut bytes, 0);
    raw_change(&mut bytes, &insert_change(0, 0, row![1u64, 10u64, 100i32, 5u32]));
    raw_commit(&mut bytes, 0, 1);
    // Corrupt record: huge length, a kind byte, and a stub payload.
    bytes.extend_from_slice(&0xFFFF_FFFEu32.to_le_bytes());
    bytes.push(2);
    bytes.extend_from_slice(&[0u8; 16]);
    std::fs::write(&path, &bytes).unwrap();

    let mut wal = Wal::open(&path, DurabilityPolicy::Flush).unwrap();
    assert!(wal.truncated_on_open(), "the bogus record was dropped");
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated);
    assert_eq!(txs.len(), 1);
    assert_eq!(txs[0].tx_id, TransactionId::from_u64(0));

    // Appends after the drop remain fully recoverable.
    wal.append(
        TransactionId::from_u64(1),
        &[insert_change(0, 1, row![2u64, 20u64, 80i32, 7u32])],
    )
    .unwrap();
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated);
    assert_eq!(txs.len(), 2);
    assert_eq!(txs[1].tx_id, TransactionId::from_u64(1));
}
