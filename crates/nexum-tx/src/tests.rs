//! Comprehensive unit tests for the transaction engine (Phase 4 brief §14).
//!
//! Coverage map: basic lifecycle · OCC read conflicts · missing-row
//! conflicts · write/write conflicts · multi-table atomic commit · failure
//! atomicity (valid writes must NOT commit when a sibling write fails) ·
//! index consistency · Change records · coalescing matrix · provisional
//! handles · lifecycle enforcement · determinism.

use nexum_core::{ChangeKind, ColumnType, Error, RowId, TableSchema, Value};
use nexum_table::{row, TableStore};

use crate::{Transaction, TransactionState};

/// A store with a relational player table: primary key on `id`, non-unique
/// `by_zone`, unique `by_level`.
fn player_store() -> TableStore {
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
}

/// A multi-table store: `players` (id 0), `items` (id 1), `matches` (id 2).
fn world_store() -> TableStore {
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
            TableSchema::builder("items")
                .column("owner", ColumnType::U64)
                .column("name", ColumnType::String)
                .build()
                .unwrap(),
        )
        .unwrap();
    store
        .create_table(
            TableSchema::builder("matches")
                .column("id", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

// ---------------------------------------------------------------- basics

#[test]
fn begin_assigns_monotonic_ids_and_active_state() {
    let mut store = player_store();
    let tx = Transaction::begin(&mut store);
    assert_eq!(tx.id().as_u64(), 0);
    assert_eq!(tx.state(), TransactionState::Active);
    let tx = Transaction::begin(&mut store);
    assert_eq!(tx.id().as_u64(), 1);
}

#[test]
fn new_accepts_explicit_ids() {
    let tx = Transaction::new(nexum_core::TransactionId::from_u64(42));
    assert_eq!(tx.id().as_u64(), 42);
    assert_eq!(tx.state(), TransactionState::Active);
}

#[test]
fn get_returns_live_row_and_records_observation() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();

    let mut tx = Transaction::begin(&mut store);
    let fetched = tx.get(&store, "players", p0).unwrap().expect("row exists");
    assert_eq!(fetched.get_named(store.table("players").unwrap().schema(), "health"), Some(&Value::I32(100)));
    assert_eq!(tx.read_count(), 1);
    assert_eq!(
        tx.reads().collect::<Vec<_>>(),
        vec![(nexum_core::TableId::from_u64(0), p0, Some(nexum_core::Version::ZERO))]
    );
}

#[test]
fn get_missing_row_observes_absent() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let absent = tx.get(&store, "players", nexum_core::RowId::from_u64(9)).unwrap();
    assert!(absent.is_none());
    assert_eq!(tx.read_count(), 1);
    let (_, row_id, observed) = tx.reads().next().unwrap();
    assert_eq!(row_id.as_u64(), 9);
    assert_eq!(observed, None);
}

#[test]
fn contains_records_observation_and_returns_existence() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();

    let mut tx = Transaction::begin(&mut store);
    assert!(tx.contains(&store, "players", p0).unwrap());
    assert!(!tx.contains(&store, "players", nexum_core::RowId::from_u64(7)).unwrap());
    assert_eq!(tx.read_count(), 2);
}

#[test]
fn insert_returns_a_provisional_handle() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    assert!(handle.as_u64() & (1 << 63) != 0, "provisional ids set the high bit");
    assert_eq!(tx.write_count(), 1);
}

#[test]
fn full_lifecycle_commit_applies_everything() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.update(&store, "players", handle, row![1u64, 10u64, 80i32, 5u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    // insert→update coalesced: exactly one Insert change with the final row.
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind(), ChangeKind::Insert);
    assert_eq!(changes[0].new_row().unwrap().get_named(store.table("players").unwrap().schema(), "health"), Some(&Value::I32(80)));
    assert_eq!(store.table("players").unwrap().len(), 1);
    assert_eq!(tx.state(), TransactionState::Committed);
}

// ------------------------------------------------------------- lifecycle

#[test]
fn committed_transaction_cannot_be_reused() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.commit(&mut store).unwrap();

    assert!(matches!(
        tx.get(&store, "players", nexum_core::RowId::from_u64(0)).unwrap_err(),
        Error::AlreadyCommitted(_)
    ));
    assert!(matches!(
        tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap_err(),
        Error::AlreadyCommitted(_)
    ));
    assert!(matches!(
        tx.update(&store, "players", nexum_core::RowId::from_u64(0), row![1u64, 10u64, 100i32, 5u32]).unwrap_err(),
        Error::AlreadyCommitted(_)
    ));
    assert!(matches!(
        tx.delete(&store, "players", nexum_core::RowId::from_u64(0)).unwrap_err(),
        Error::AlreadyCommitted(_)
    ));
    assert!(matches!(
        tx.commit(&mut store).unwrap_err(),
        Error::AlreadyCommitted(_)
    ));
}

#[test]
fn aborted_transaction_cannot_commit() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.abort().unwrap();
    assert_eq!(tx.state(), TransactionState::Aborted);

    assert!(matches!(
        tx.commit(&mut store).unwrap_err(),
        Error::AlreadyAborted(_)
    ));
    assert!(matches!(
        tx.get(&store, "players", nexum_core::RowId::from_u64(0)).unwrap_err(),
        Error::AlreadyAborted(_)
    ));
}

#[test]
fn abort_is_idempotent_and_committed_cannot_abort() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.abort().unwrap();
    tx.abort().unwrap(); // no-op, still aborted
    assert_eq!(tx.state(), TransactionState::Aborted);

    let mut tx = Transaction::begin(&mut store);
    tx.commit(&mut store).unwrap();
    assert!(matches!(tx.abort().unwrap_err(), Error::AlreadyCommitted(_)));
}

#[test]
fn failed_commit_marks_transaction_aborted() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", p0).unwrap();
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
    assert_eq!(tx.state(), TransactionState::Aborted);
    assert!(matches!(
        tx.commit(&mut store).unwrap_err(),
        Error::AlreadyAborted(_)
    ));
}

// ------------------------------------------------------------------- OCC

#[test]
fn read_version_conflict_on_concurrent_update() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert!(tx.get(&store, "players", p0).unwrap().is_some());

    // Another writer commits between the read and the commit.
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn missing_row_read_conflicts_when_row_appears() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);

    // Row 0 is absent when read...
    assert!(tx.get(&store, "players", nexum_core::RowId::from_u64(0)).unwrap().is_none());

    // ...and another writer inserts exactly row 0 before the commit.
    store.table_mut("players").unwrap().insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn read_then_delete_conflicts() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert!(tx.get(&store, "players", p0).unwrap().is_some());
    store.table_mut("players").unwrap().delete(p0).unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn fresh_read_after_write_commits_cleanly() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", p0).unwrap();
    tx.update(&store, "players", p0, row![1u64, 10u64, 90i32, 5u32]).unwrap();
    tx.commit(&mut store).unwrap();
    assert_eq!(
        store.table("players").unwrap().version_of(p0),
        Some(nexum_core::Version::from_u64(1))
    );
}

#[test]
fn write_write_conflict_detected() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    // T1 reads v0 and commits an update (v1).
    let mut t1 = Transaction::begin(&mut store);
    t1.get(&store, "players", p0).unwrap();
    t1.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    t1.commit(&mut store).unwrap();

    // T2 starts with a fresh read (v1): commits cleanly.
    let mut t2 = Transaction::begin(&mut store);
    t2.get(&store, "players", p0).unwrap();
    t2.update(&store, "players", p0, row![1u64, 10u64, 25i32, 5u32]).unwrap();
    t2.commit(&mut store).unwrap();

    // T3 reads v1 but another writer bumps to v2 before T3 commits.
    let mut t3 = Transaction::begin(&mut store);
    t3.get(&store, "players", p0).unwrap();
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 10i32, 5u32])
        .unwrap();
    let err = t3.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

// ------------------------------------------------------------ multi-table

#[test]
fn multi_table_atomic_commit() {
    let mut store = world_store();
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.insert(&store, "items", row![1u64, "sword".to_string()]).unwrap();
    tx.insert(&store, "matches", row![5u64, 10u64]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    assert_eq!(store.table("players").unwrap().len(), 1);
    assert_eq!(store.table("items").unwrap().len(), 1);
    assert_eq!(store.table("matches").unwrap().len(), 1);

    // Three inserts across three tables, in TableId order.
    assert_eq!(changes.len(), 3);
    assert!(changes.iter().all(|c| c.kind() == ChangeKind::Insert));
    assert_eq!(changes[0].table_id().as_u64(), 0); // players
    assert_eq!(changes[1].table_id().as_u64(), 1); // items
    assert_eq!(changes[2].table_id().as_u64(), 2); // matches
}

#[test]
fn failure_in_one_table_leaves_other_tables_untouched() {
    let mut store = world_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store
        .table_mut("matches")
        .unwrap()
        .insert(row![7u64, 1u64])
        .unwrap();
    store.drain_changes();

    // Valid write to players + conflicting insert to matches (PK collision).
    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    tx.insert(&store, "matches", row![7u64, 2u64]).unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(tx.state(), TransactionState::Aborted);

    // The valid write to players must NOT have committed.
    assert_eq!(
        store.table("players").unwrap().version_of(p0),
        Some(nexum_core::Version::ZERO),
        "players update must not have been applied"
    );
    assert_eq!(
        store.table("players").unwrap().get(p0).unwrap().get_named(store.table("players").unwrap().schema(), "health"),
        Some(&Value::I32(100))
    );
    assert_eq!(store.table("matches").unwrap().len(), 1);
    // No change records were produced by the failed transaction.
    assert!(store.drain_changes().is_empty());
}

#[test]
fn read_conflict_in_one_table_aborts_whole_transaction() {
    let mut store = world_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    // Valid write to matches + stale read of players.
    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", p0).unwrap();
    tx.insert(&store, "matches", row![9u64, 10u64]).unwrap();
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();
    // Clear the other writer's own change so the buffer reflects only what
    // this transaction may have produced.
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));

    // The matches insert must not have committed, and the failed
    // transaction produced no changes.
    assert!(store.table("matches").unwrap().is_empty());
    assert!(store.drain_changes().is_empty());
}

// --------------------------------------------------------------- indexes

#[test]
fn transactional_ops_keep_indexes_consistent() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let alice_handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    let bob_handle = tx.insert(&store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    // The handles were provisional (high bit set); the real ids come from
    // the commit's Change records (ADR-004 D10).
    assert!(alice_handle.as_u64() & (1 << 63) != 0);
    assert!(bob_handle.as_u64() & (1 << 63) != 0);
    let alice = changes[0].row_id();
    let bob = changes[1].row_id();

    // Indexes were built from the committed rows.
    assert_eq!(
        store.table("players").unwrap().lookup("by_zone", &[Value::U64(10)]).unwrap(),
        vec![alice, bob]
    );

    // Move Alice to zone 30 and level 9 via a transaction.
    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", alice, row![1u64, 30u64, 50i32, 9u32]).unwrap();
    tx.commit(&mut store).unwrap();

    assert_eq!(store.table("players").unwrap().lookup("by_zone", &[Value::U64(10)]).unwrap(), vec![bob]);
    assert_eq!(store.table("players").unwrap().lookup("by_zone", &[Value::U64(30)]).unwrap(), vec![alice]);
    assert_eq!(store.table("players").unwrap().lookup("by_level", &[Value::U32(9)]).unwrap(), vec![alice]);
    assert!(store.table("players").unwrap().lookup("by_level", &[Value::U32(5)]).unwrap().is_empty());

    // Delete Bob via a transaction: indexes must shed him too.
    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", bob).unwrap();
    tx.commit(&mut store).unwrap();

    assert!(store.table("players").unwrap().get(bob).is_none());
    assert!(store.table("players").unwrap().lookup("by_zone", &[Value::U64(10)]).unwrap().is_empty());
    assert!(store.table("players").unwrap().get_by_primary_key(&[Value::U64(2)]).unwrap().is_none());
    assert_eq!(store.table("players").unwrap().len(), 1);
}

#[test]
fn indexes_never_diverge_after_transactional_mutations() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.insert(&store, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();
    store.drain_changes();

    // Real row ids come from the commit's Change records.
    let alice = changes[0].row_id();
    let bob = changes[1].row_id();
    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", bob).unwrap();
    tx.update(&store, "players", alice, row![1u64, 30u64, 40i32, 5u32]).unwrap();
    tx.commit(&mut store).unwrap();

    let table = store.table("players").unwrap();
    let zone30: Vec<nexum_core::RowId> = table
        .scan()
        .filter(|(_, row)| row.get_named(table.schema(), "zone_id") == Some(&Value::U64(30)))
        .map(|(id, _)| id)
        .collect();
    assert_eq!(table.lookup("by_zone", &[Value::U64(30)]).unwrap(), zone30);
    assert_eq!(table.lookup("by_level", &[Value::U32(5)]).unwrap(), vec![alice]);
    assert_eq!(table.len(), table.scan().count());
    assert_eq!(table.len(), 1); // Bob was deleted; only Alice remains.
}

// --------------------------------------------------------------- changes

#[test]
fn successful_commit_produces_ordered_change_records() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let p1 = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 10u64, 90i32, 6u32])
        .unwrap();
    store.drain_changes();

    // Delete p1, update p0, insert a third row — all in one transaction.
    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p1).unwrap();
    tx.update(&store, "players", p0, row![1u64, 20u64, 80i32, 7u32]).unwrap();
    let inserted = tx.insert(&store, "players", row![3u64, 30u64, 70i32, 8u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    // Deterministic order: deletes first, then updates/inserts by RowId.
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].kind(), ChangeKind::Delete);
    assert_eq!(changes[0].row_id(), p1);
    assert_eq!(changes[0].old_version(), Some(nexum_core::Version::ZERO));

    assert_eq!(changes[1].kind(), ChangeKind::Update);
    assert_eq!(changes[1].row_id(), p0);
    assert_eq!(changes[1].old_version(), Some(nexum_core::Version::ZERO));
    assert_eq!(changes[1].new_version(), Some(nexum_core::Version::from_u64(1)));

    assert_eq!(changes[2].kind(), ChangeKind::Insert);
    assert_eq!(changes[2].new_version(), Some(nexum_core::Version::ZERO));
    // Storage assigned a real id (row id 2 — the third insert).
    assert_eq!(changes[2].row_id().as_u64(), 2);
    assert!(inserted.as_u64() & (1 << 63) != 0, "the handle was provisional");
}

#[test]
fn failed_transaction_emits_no_changes() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", p0).unwrap();
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();
    // Clear the other writer's own change so the buffer reflects only what
    // this transaction may have produced.
    store.drain_changes();

    assert!(tx.commit(&mut store).is_err());
    // The failed transaction produced nothing.
    assert!(store.table("players").unwrap().changes().is_empty());
}

#[test]
fn commit_returns_only_this_transactions_changes() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    // Deliberately do NOT drain: the direct insert's change is buffered.

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    // Only the transaction's own update is returned, not the stale insert.
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind(), ChangeKind::Update);
}

// ------------------------------------------------------------ coalescing

#[test]
fn insert_then_update_coalesces_to_one_insert() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.update(&store, "players", handle, row![1u64, 10u64, 25i32, 5u32]).unwrap();

    assert_eq!(tx.write_count(), 1);
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind(), ChangeKind::Insert);
    assert_eq!(
        changes[0].new_row().unwrap().get_named(store.table("players").unwrap().schema(), "health"),
        Some(&Value::I32(25))
    );
}

#[test]
fn insert_then_delete_is_a_net_noop() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.delete(&store, "players", handle).unwrap();

    assert!(tx.write_count() == 0);
    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty());
    assert!(store.table("players").unwrap().is_empty());
}

#[test]
fn update_then_update_keeps_latest() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    tx.update(&store, "players", p0, row![1u64, 10u64, 25i32, 5u32]).unwrap();

    assert_eq!(tx.write_count(), 1);
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind(), ChangeKind::Update);
    assert_eq!(
        changes[0].new_row().unwrap().get_named(store.table("players").unwrap().schema(), "health"),
        Some(&Value::I32(25))
    );
}

#[test]
fn update_then_delete_becomes_delete() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    tx.delete(&store, "players", p0).unwrap();

    assert_eq!(tx.write_count(), 1);
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(changes[0].kind(), ChangeKind::Delete);
    assert!(store.table("players").unwrap().is_empty());
}

#[test]
fn delete_then_update_is_rejected() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    let err = tx
        .update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32])
        .unwrap_err();
    assert!(matches!(err, Error::InvalidTransaction(_)));
    assert_eq!(tx.write_count(), 1);
}

#[test]
fn double_delete_is_rejected() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    let err = tx.delete(&store, "players", p0).unwrap_err();
    assert!(matches!(err, Error::InvalidTransaction(_)));
}

#[test]
fn dangling_provisional_handles_are_rejected() {
    let mut store = player_store();
    let phantom = nexum_core::RowId::from_u64(1 << 63 | 5);
    let mut tx = Transaction::begin(&mut store);
    assert!(matches!(
        tx.update(&store, "players", phantom, row![1u64, 10u64, 100i32, 5u32]).unwrap_err(),
        Error::InvalidTransaction(_)
    ));
    assert!(matches!(
        tx.delete(&store, "players", phantom).unwrap_err(),
        Error::InvalidTransaction(_)
    ));
}

// -------------------------------------------------------------- determinism

#[test]
fn change_order_is_deterministic_across_tables() {
    let shape = |changes: Vec<nexum_storage::Change>| -> Vec<(u64, nexum_core::RowId)> {
        changes.into_iter().map(|c| (c.table_id().as_u64(), c.row_id())).collect()
    };

    // Identical logical content, but submission order differs.
    let mut store_a = world_store();
    let mut tx_a = Transaction::begin(&mut store_a);
    tx_a.insert(&store_a, "matches", row![5u64, 10u64]).unwrap();
    tx_a.insert(&store_a, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx_a.insert(&store_a, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
    let changes_a = tx_a.commit(&mut store_a).unwrap();

    let mut store_b = world_store();
    let mut tx_b = Transaction::begin(&mut store_b);
    tx_b.insert(&store_b, "players", row![2u64, 10u64, 90i32, 6u32]).unwrap();
    tx_b.insert(&store_b, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx_b.insert(&store_b, "matches", row![5u64, 10u64]).unwrap();
    let changes_b = tx_b.commit(&mut store_b).unwrap();

    // Same shape regardless of submission order: grouped by TableId; within
    // a table, sorted by RowId (submission order for inserts).
    assert_eq!(shape(changes_a), shape(changes_b));
}

// ---------------------------------------------------------------- errors

#[test]
fn missing_table_is_not_found() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    assert!(matches!(
        tx.get(&store, "nope", nexum_core::RowId::from_u64(0)).unwrap_err(),
        Error::NotFound(_)
    ));
    assert!(matches!(
        tx.insert(&store, "nope", row![1u64, 10u64, 100i32, 5u32]).unwrap_err(),
        Error::NotFound(_)
    ));
}

#[test]
fn schema_violation_rejected_at_write_time() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let err = tx.insert(&store, "players", row![1u64]).unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(tx.write_count(), 0);
    tx.abort().unwrap();
}

#[test]
fn unique_violation_detected_at_commit() {
    let mut store = player_store();
    store.table_mut("players").unwrap().insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
    store.drain_changes();

    // Same level 5 (unique by_level) as the live row.
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", row![2u64, 20u64, 50i32, 5u32]).unwrap();
    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(store.table("players").unwrap().len(), 1);
    assert!(store.drain_changes().is_empty());
}

#[test]
fn cross_write_unique_collision_detected_at_commit() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.insert(&store, "players", row![2u64, 20u64, 50i32, 5u32]).unwrap(); // same level

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert!(store.table("players").unwrap().is_empty());
}

#[test]
fn delete_of_missing_row_is_not_found() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", nexum_core::RowId::from_u64(9)).unwrap();
    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    assert_eq!(tx.state(), TransactionState::Aborted);
}

#[test]
fn reads_observe_pending_writes_after_correction() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    // The Phase 4 correction made reads observe the transaction's own
    // uncommitted writes (read-your-writes, ADR-004 D12).
    assert!(tx.get(&store, "players", handle).unwrap().is_some());
    assert!(tx.contains(&store, "players", handle).unwrap());
    tx.abort().unwrap();
}

#[test]
fn deleted_row_key_can_be_reused_by_same_transaction() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    // Delete p0 (owns level 5) and insert a fresh row claiming level 5.
    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    let handle = tx.insert(&store, "players", row![2u64, 20u64, 50i32, 5u32]).unwrap();
    let changes = tx.commit(&mut store).unwrap();

    assert_eq!(changes.len(), 2);
    assert_eq!(changes[0].kind(), ChangeKind::Delete);
    assert_eq!(changes[1].kind(), ChangeKind::Insert);
    assert_eq!(
        store.table("players").unwrap().lookup("by_level", &[Value::U32(5)]).unwrap(),
        vec![nexum_core::RowId::from_u64(1)]
    );
    let _ = handle;
}

#[test]
fn empty_transaction_commits_cleanly() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty());
    assert_eq!(tx.state(), TransactionState::Committed);
}

#[test]
fn aborted_transaction_changes_nothing() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes(); // clear the direct insert's change

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 1i32, 5u32]).unwrap();
    tx.delete(&store, "players", p0).unwrap(); // coalesced to delete
    tx.abort().unwrap();

    assert!(store.table("players").unwrap().get(p0).is_some());
    assert_eq!(
        store.table("players").unwrap().version_of(p0),
        Some(nexum_core::Version::ZERO)
    );
    assert!(store.table("players").unwrap().changes().is_empty());
}

// ------------------------------------------- correction: read-your-writes

/// Reads a `Row`'s named value, working on the owned row the tx view returns.
fn health<'s>(row: &'s nexum_core::Row, schema: &nexum_core::TableSchema) -> &'s Value {
    row.get_named(schema, "health").unwrap()
}

#[test]
fn insert_then_get_sees_pending_row() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();

    let fetched = tx.get(&store, "players", handle).unwrap().expect("pending insert visible");
    let schema = store.table("players").unwrap().schema();
    assert_eq!(health(&fetched, schema), &Value::I32(100));
    // The write entry governs validation: no row observation recorded.
    assert_eq!(tx.read_count(), 0);
    assert!(tx.contains(&store, "players", handle).unwrap());
}

#[test]
fn insert_then_update_then_get_sees_final_row() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.update(&store, "players", handle, row![1u64, 10u64, 40i32, 5u32]).unwrap();

    let fetched = tx.get(&store, "players", handle).unwrap().unwrap();
    let schema = store.table("players").unwrap().schema();
    assert_eq!(health(&fetched, schema), &Value::I32(40));
}

#[test]
fn insert_then_delete_then_get_observes_absence() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    let handle = tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    tx.delete(&store, "players", handle).unwrap(); // net no-op

    assert!(tx.get(&store, "players", handle).unwrap().is_none());
    assert!(!tx.contains(&store, "players", handle).unwrap());
    // Commits as the empty transaction it logically is.
    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty());
    assert!(store.table("players").unwrap().is_empty());
}

#[test]
fn update_then_get_sees_pending_row() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 25i32, 5u32]).unwrap();
    let fetched = tx.get(&store, "players", p0).unwrap().expect("pending update visible");
    let schema = store.table("players").unwrap().schema();
    assert_eq!(health(&fetched, schema), &Value::I32(25));
    // The write-time capture is the only observation (no duplicate read).
    assert_eq!(tx.read_count(), 1);
}

#[test]
fn update_then_update_then_get_sees_latest() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    tx.update(&store, "players", p0, row![1u64, 10u64, 5i32, 5u32]).unwrap();
    let fetched = tx.get(&store, "players", p0).unwrap().unwrap();
    let schema = store.table("players").unwrap().schema();
    assert_eq!(health(&fetched, schema), &Value::I32(5));
}

#[test]
fn update_then_delete_then_get_observes_absence() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();
    tx.delete(&store, "players", p0).unwrap();
    assert!(tx.get(&store, "players", p0).unwrap().is_none());
    assert!(!tx.contains(&store, "players", p0).unwrap());
}

#[test]
fn delete_then_get_observes_absence() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    assert!(tx.get(&store, "players", p0).unwrap().is_none());
    assert!(!tx.contains(&store, "players", p0).unwrap());
}

#[test]
fn delete_then_update_respects_coalescing_rules() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    // delete→update is rejected by the documented coalescing matrix.
    assert!(matches!(
        tx.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32])
            .unwrap_err(),
        Error::InvalidTransaction(_)
    ));
}

// ------------------------------------------- correction: phantom protection

#[test]
fn scan_then_external_insert_conflicts() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    assert!(tx.scan(&store, "players").unwrap().is_empty());

    store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    assert!(matches!(tx.commit(&mut store).unwrap_err(), Error::Conflict(_)));
    assert_eq!(tx.state(), TransactionState::Aborted);
}

#[test]
fn scan_then_external_delete_conflicts() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert_eq!(tx.scan(&store, "players").unwrap().len(), 1);
    store.table_mut("players").unwrap().delete(p0).unwrap();
    store.drain_changes();

    assert!(matches!(tx.commit(&mut store).unwrap_err(), Error::Conflict(_)));
}

#[test]
fn scan_then_external_update_conflicts() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert_eq!(tx.scan(&store, "players").unwrap().len(), 1);
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();
    store.drain_changes();

    assert!(matches!(tx.commit(&mut store).unwrap_err(), Error::Conflict(_)));
}

#[test]
fn scan_then_external_noop_update_does_not_conflict() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert_eq!(tx.scan(&store, "players").unwrap().len(), 1);
    // A Phase 3 no-op update (identical row) advances no epoch: no conflict.
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    tx.commit(&mut store).unwrap();
}

#[test]
fn scan_then_unchanged_commits_cleanly() {
    let mut store = player_store();
    store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    assert_eq!(tx.scan(&store, "players").unwrap().len(), 1);
    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty());
    assert_eq!(tx.state(), TransactionState::Committed);
}

#[test]
fn scan_then_own_write_commits_cleanly() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.scan(&store, "players").unwrap();
    tx.insert(&store, "players", row![1u64, 10u64, 100i32, 5u32]).unwrap();
    // The transaction's own write is not a phantom: validation runs before
    // apply, so the observed epoch still matches at commit time.
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 1);
    assert_eq!(store.table("players").unwrap().len(), 1);
}

#[test]
fn scan_of_a_and_write_to_b_conflicts_when_a_changes() {
    let mut store = world_store();
    let mut tx = Transaction::begin(&mut store);
    tx.scan(&store, "players").unwrap();
    tx.insert(&store, "matches", row![9u64, 10u64]).unwrap();

    // Another writer mutates players (the scanned table).
    store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
    // The write to matches must not have committed.
    assert!(store.table("matches").unwrap().is_empty());
    assert!(store.drain_changes().is_empty());
}

#[test]
fn unrelated_table_change_does_not_conflict() {
    let mut store = world_store();
    let mut tx = Transaction::begin(&mut store);
    tx.scan(&store, "players").unwrap();

    // Only *items* changes — an unrelated table. No conflict.
    store
        .table_mut("items")
        .unwrap()
        .insert(row![1u64, "sword".to_string()])
        .unwrap();
    store.drain_changes();

    tx.commit(&mut store).unwrap();
    assert_eq!(tx.state(), TransactionState::Committed);
}

#[test]
fn scan_records_a_table_epoch_observation() {
    let mut store = player_store();
    let mut tx = Transaction::begin(&mut store);
    tx.scan(&store, "players").unwrap();

    assert_eq!(tx.read_count(), 1);
    let observations: Vec<(nexum_core::TableId, nexum_core::Version)> =
        tx.table_reads().collect();
    assert_eq!(observations.len(), 1);
    assert_eq!(observations[0].0, nexum_core::TableId::from_u64(0));
    assert_eq!(observations[0].1, nexum_core::Version::ZERO);
}

// ------------------------------------------- correction: transaction overlay

#[test]
fn scan_overlays_pending_writes_deterministically() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let p1 = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 10u64, 90i32, 6u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 25i32, 5u32]).unwrap();
    tx.delete(&store, "players", p1).unwrap();
    let handle = tx.insert(&store, "players", row![3u64, 20u64, 80i32, 7u32]).unwrap();

    let scanned = tx.scan(&store, "players").unwrap();
    let schema = store.table("players").unwrap().schema();
    // Committed rows in RowId order (p0 overlaid, p1 hidden), then the
    // pending insert (provisional id sorts after every real id).
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].0, p0);
    assert_eq!(health(&scanned[0].1, schema), &Value::I32(25));
    assert_eq!(scanned[1].0, handle);
    assert_eq!(scanned[1].1.get_named(schema, "zone_id"), Some(&Value::U64(20)));
    assert!(!scanned.iter().any(|(id, _)| *id == p1));

    // And it commits cleanly (own writes are not phantom conflicts): the
    // delete of the real row p1, the update of p0, and the insert — three
    // changes, with p1 gone from storage afterwards.
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 3);
    assert_eq!(changes[0].kind(), ChangeKind::Delete);
    assert_eq!(changes[1].kind(), ChangeKind::Update);
    assert_eq!(changes[2].kind(), ChangeKind::Insert);
    assert!(store.table("players").unwrap().get(p1).is_none());
}

#[test]
fn lookup_unique_overlays_pending_writes() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    // Move p0 off level 5, and add a pending insert claiming level 6.
    tx.update(&store, "players", p0, row![1u64, 10u64, 100i32, 9u32]).unwrap();
    let handle = tx.insert(&store, "players", row![2u64, 20u64, 90i32, 6u32]).unwrap();

    // The pending insert is visible under its key...
    assert_eq!(
        tx.lookup_unique(&store, "players", "by_level", &[Value::U32(6)]).unwrap(),
        vec![handle]
    );
    // ...the updated row's old key is released in the tx view...
    assert!(tx.lookup_unique(&store, "players", "by_level", &[Value::U32(5)]).unwrap().is_empty());
    // ...and its new key is visible.
    assert_eq!(
        tx.lookup_unique(&store, "players", "by_level", &[Value::U32(9)]).unwrap(),
        vec![p0]
    );
}

#[test]
fn lookup_unique_hides_logically_deleted_rows() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    // The logically-deleted row no longer owns level 5 in the tx view.
    assert!(tx.lookup_unique(&store, "players", "by_level", &[Value::U32(5)]).unwrap().is_empty());

    // delete X then insert a new X with the same key must be possible:
    // the key is free within this transaction.
    let handle = tx.insert(&store, "players", row![9u64, 30u64, 1i32, 5u32]).unwrap();
    assert_eq!(
        tx.lookup_unique(&store, "players", "by_level", &[Value::U32(5)]).unwrap(),
        vec![handle]
    );
    // And it commits cleanly: the released key is reused by the insert.
    tx.commit(&mut store).unwrap();
    assert_eq!(
        store.table("players").unwrap().lookup_unique("by_level", &[Value::U32(5)]).unwrap().len(),
        1
    );
}

#[test]
fn lookup_index_overlays_pending_writes() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let p1 = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 20u64, 90i32, 6u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    // Move p0 out of zone 10 and add a pending insert claiming zone 20.
    tx.update(&store, "players", p0, row![1u64, 30u64, 100i32, 5u32]).unwrap();
    let handle = tx.insert(&store, "players", row![3u64, 20u64, 80i32, 7u32]).unwrap();

    // The pending insert is visible under its key (ascending ids, deduped)...
    assert_eq!(
        tx.lookup_index(&store, "players", "by_zone", &[Value::U64(20)]).unwrap(),
        vec![p1, handle]
    );
    // ...the updated row's old key is released in the tx view...
    assert!(tx.lookup_index(&store, "players", "by_zone", &[Value::U64(10)]).unwrap().is_empty());
    // ...and its new key is visible.
    assert_eq!(
        tx.lookup_index(&store, "players", "by_zone", &[Value::U64(30)]).unwrap(),
        vec![p0]
    );

    // It commits cleanly: the moved row and the pending insert land. The
    // provisional handle becomes the engine-assigned real id (the next
    // monotonic RowId).
    tx.commit(&mut store).unwrap();
    assert_eq!(
        store.table("players").unwrap().lookup("by_zone", &[Value::U64(20)]).unwrap(),
        vec![p1, RowId::from_u64(2)]
    );
    assert_eq!(
        store.table("players").unwrap().lookup("by_zone", &[Value::U64(30)]).unwrap(),
        vec![p0]
    );
}

#[test]
fn lookup_index_hides_logically_deleted_rows() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.delete(&store, "players", p0).unwrap();
    // The logically-deleted row no longer owns zone 10 in the tx view.
    assert!(tx.lookup_index(&store, "players", "by_zone", &[Value::U64(10)]).unwrap().is_empty());

    // delete X then insert a new X with the same zone must be visible.
    let handle = tx.insert(&store, "players", row![9u64, 10u64, 1i32, 9u32]).unwrap();
    assert_eq!(
        tx.lookup_index(&store, "players", "by_zone", &[Value::U64(10)]).unwrap(),
        vec![handle]
    );
}

#[test]
fn lookup_index_results_are_sorted_ascending() {
    let mut store = player_store();
    // Insert zone-10 rows with non-ascending primary-key *values*: engine
    // RowIds are assigned monotonically, so ascending RowId output proves
    // the index (and the transaction overlay) sorts deterministically.
    let mut ids = Vec::new();
    for id in [3u64, 1u64, 2u64] {
        ids.push(
            store
                .table_mut("players")
                .unwrap()
                .insert(row![id, 10u64, 100i32, id as u32])
                .unwrap(),
        );
    }
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    let owners = tx.lookup_index(&store, "players", "by_zone", &[Value::U64(10)]).unwrap();
    assert_eq!(owners, ids, "ascending RowId order, matching the committed set");
}

// ------------------------------------- correction: write-time version capture

#[test]
fn write_write_conflict_detected_without_prior_read() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    // T1 updates p0 WITHOUT reading it first — the write itself captures the
    // committed version (v0).
    let mut t1 = Transaction::begin(&mut store);
    t1.update(&store, "players", p0, row![1u64, 10u64, 50i32, 5u32]).unwrap();

    // A concurrent writer bumps p0 to v1 before T1 commits.
    store
        .table_mut("players")
        .unwrap()
        .update(p0, row![1u64, 10u64, 90i32, 5u32])
        .unwrap();
    store.drain_changes();

    let err = t1.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
    // The concurrent writer's change survived; T1's did not.
    assert_eq!(
        store.table("players").unwrap().version_of(p0),
        Some(nexum_core::Version::from_u64(1))
    );
}

#[test]
fn write_without_prior_read_commits_cleanly_when_unchanged() {
    let mut store = player_store();
    let p0 = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", p0, row![1u64, 10u64, 25i32, 5u32]).unwrap();
    tx.commit(&mut store).unwrap();
    assert_eq!(
        store.table("players").unwrap().version_of(p0),
        Some(nexum_core::Version::from_u64(1))
    );
}

// ------------------------------------------- phase 11: branch and absorb

/// A three-table store for branch/absorb tests: `a`, `b`, `c` (ids 0,1,2).
fn parallel_store() -> TableStore {
    let mut store = TableStore::new();
    for name in ["a", "b", "c"] {
        store
            .create_table(
                TableSchema::builder(name)
                    .column("id", ColumnType::U64)
                    .primary_key(&["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
    store
}

#[test]
fn branch_inherits_writes_and_provisional_counters() {
    let mut store = parallel_store();
    let mut tx = Transaction::begin(&mut store);
    let a0 = tx.insert(&store, "a", row![1u64]).unwrap();

    let mut child = Transaction::new(nexum_core::TransactionId::from_u64(99));
    child.branch_of(&tx).unwrap();

    // The parent's write is visible through the child (read-your-writes
    // across the branch boundary).
    assert_eq!(child.get(&store, "a", a0).unwrap(), Some(row![1u64]));

    // The child's next insert continues the parent's provisional counter,
    // so merged provisional ids match serial execution exactly.
    let a1 = child.insert(&store, "a", row![2u64]).unwrap();
    assert_eq!(a1.as_u64(), a0.as_u64() + 1);

    // The child's write coalesces against the inherited entry when it
    // touches the same key.
    child.update(&store, "a", a0, row![9u64]).unwrap();
    assert_eq!(child.write_count(), 2);
    assert_eq!(tx.write_count(), 1);

    // Absorb merges: the child's entries overwrite, counters advance.
    tx.absorb(child).unwrap();
    assert_eq!(tx.write_count(), 2);
    let (_, _, entry) = tx.writes().find(|(_, rid, _)| *rid == a0).unwrap();
    assert_eq!(entry.row(), Some(&row![9u64]));
}

#[test]
fn branch_merge_is_equivalent_to_serial_commit() {
    let mut store = parallel_store();
    let mut tx = Transaction::begin(&mut store);

    // Parent (simulating an earlier group) writes table a.
    let a0 = tx.insert(&store, "a", row![1u64]).unwrap();

    // Two branch children simulate independent systems on tables b and c.
    let mut b = Transaction::new(nexum_core::TransactionId::from_u64(1));
    b.branch_of(&tx).unwrap();
    let b0 = b.insert(&store, "b", row![10u64]).unwrap();
    b.get(&store, "a", a0).unwrap(); // read the inherited write

    let mut c = Transaction::new(nexum_core::TransactionId::from_u64(2));
    c.branch_of(&tx).unwrap();
    let c0 = c.insert(&store, "c", row![20u64]).unwrap();

    tx.absorb(b).unwrap();
    tx.absorb(c).unwrap();

    // The merged transaction commits exactly like the serial sequence
    // a-insert, b-insert, c-insert: same keys, same real-id assignment.
    let changes = tx.commit(&mut store).unwrap();
    assert_eq!(changes.len(), 3);
    let ids: Vec<(u64, nexum_core::RowId)> =
        changes.iter().map(|c| (c.table_id().as_u64(), c.row_id())).collect();
    assert_eq!(ids[0], (0, nexum_core::RowId::from_u64(0))); // a's insert
    assert_eq!(ids[1], (1, nexum_core::RowId::from_u64(0))); // b's insert
    assert_eq!(ids[2], (2, nexum_core::RowId::from_u64(0))); // c's insert
    let _ = (b0, c0);
}

#[test]
fn absorb_rejects_committed_or_aborted_children() {
    let mut store = parallel_store();
    let mut tx = Transaction::begin(&mut store);

    let mut child = Transaction::new(nexum_core::TransactionId::from_u64(1));
    child.abort().unwrap();
    assert!(matches!(tx.absorb(child).unwrap_err(), Error::AlreadyAborted(_)));

    let mut child = Transaction::new(nexum_core::TransactionId::from_u64(2));
    child.branch_of(&tx).unwrap();
    child.commit(&mut store).unwrap();
    assert!(matches!(tx.absorb(child).unwrap_err(), Error::AlreadyCommitted(_)));
}

#[test]
fn branch_of_rejects_non_active_parent() {
    let mut store = parallel_store();
    let mut tx = Transaction::begin(&mut store);
    tx.abort().unwrap();
    let mut child = Transaction::new(nexum_core::TransactionId::from_u64(1));
    assert!(matches!(child.branch_of(&tx).unwrap_err(), Error::AlreadyAborted(_)));
}

#[test]
fn absorb_folds_read_observations_and_epochs() {
    let mut store = parallel_store();
    let p0 = store
        .table_mut("a")
        .unwrap()
        .insert(row![1u64])
        .unwrap();
    store.drain_changes();

    let mut tx = Transaction::begin(&mut store);
    let mut child = Transaction::new(nexum_core::TransactionId::from_u64(1));
    child.branch_of(&tx).unwrap();
    child.get(&store, "a", p0).unwrap();
    child.scan(&store, "a").unwrap();

    tx.absorb(child).unwrap();
    // Row observation + table epoch observation both folded in.
    assert_eq!(tx.read_count(), 2);
    assert_eq!(tx.table_reads().count(), 1);
}
