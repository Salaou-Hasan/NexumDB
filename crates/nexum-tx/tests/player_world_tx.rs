//! Multi-table transactional integration test: a small matchmaking + economy
//! world where every mutation crosses several tables atomically.
//!
//! Exercises the Phase 4 completion criteria end to end: multi-table atomic
//! commit, failure atomicity across tables, missing-row conflicts, index
//! consistency, and deterministic change records.

use nexum_core::{ChangeKind, ColumnType, Error, TableSchema, Value};
use nexum_table::{TableStore, row};
use nexum_tx::Transaction;

/// `players` (id 0): id/zone/health/level, PK on id, unique by_level.
/// `matches` (id 1): id/player_a/player_b/zone_id, PK on id.
/// `economy` (id 2): owner/coins, PK on owner.
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
            TableSchema::builder("matches")
                .column("id", ColumnType::U64)
                .column("player_a", ColumnType::U64)
                .column("player_b", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .primary_key(&["id"])
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

/// Creates a player and their economy account in one transaction.
///
/// Returns the real `RowId`s of the player and the economy account, recovered
/// from the commit's Change records (the provisional insert handles are not
/// valid after commit — ADR-004 D10).
fn create_player(
    store: &mut TableStore,
    id: u64,
    zone: u64,
    level: u32,
    coins: i64,
) -> (nexum_core::RowId, nexum_core::RowId) {
    let mut tx = Transaction::begin(store);
    let handle = tx
        .insert(store, "players", row![id, zone, 100i32, level])
        .unwrap();
    // Insert→update coalescing: the update folds into the insert.
    tx.update(store, "players", handle, row![id, zone, 100i32, level])
        .unwrap();
    tx.insert(store, "economy", row![id, coins]).unwrap();
    let changes = tx.commit(store).unwrap();
    // One player insert + one economy insert; players (table id 0) sorts
    // before economy (table id 2) in the change stream.
    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|c| c.kind() == ChangeKind::Insert));
    (changes[0].row_id(), changes[1].row_id())
}

/// Joins two players into a match in one atomic transaction: creates the
/// match and moves both players to the match zone.
fn join_match(
    store: &mut TableStore,
    match_id: u64,
    a: nexum_core::RowId,
    b: nexum_core::RowId,
    a_id: u64,
    b_id: u64,
    zone: u64,
) {
    let mut tx = Transaction::begin(store);
    // Read both players (records versions), then write all three tables.
    let player_a = tx
        .get(store, "players", a)
        .unwrap()
        .expect("player a exists");
    let player_b = tx
        .get(store, "players", b)
        .unwrap()
        .expect("player b exists");
    let (a_zone, b_zone) = (
        player_a.get_named(store.table("players").unwrap().schema(), "zone_id"),
        player_b.get_named(store.table("players").unwrap().schema(), "zone_id"),
    );
    assert_eq!(a_zone, Some(&Value::U64(zone)), "a already where expected");
    assert_eq!(b_zone, Some(&Value::U64(zone)), "b already where expected");

    tx.insert(store, "matches", row![match_id, a_id, b_id, zone])
        .unwrap();
    tx.update(store, "players", a, row![a_id, zone + 1, 100i32, 5u32])
        .unwrap();
    tx.update(store, "players", b, row![b_id, zone + 1, 100i32, 6u32])
        .unwrap(); // distinct level: by_level is unique
    tx.commit(store).unwrap();
}

#[test]
fn matchmaking_world_commits_atomically_across_tables() {
    let mut store = world();

    let (alice, _alice_coins) = create_player(&mut store, 1, 10, 1, 100);
    let (bob, _bob_coins) = create_player(&mut store, 2, 10, 2, 200);

    // Both players start in zone 10.
    assert_eq!(
        store
            .table("players")
            .unwrap()
            .lookup("by_zone", &[Value::U64(10)])
            .unwrap()
            .len(),
        2
    );

    // Atomic join: match + both players move to zone 11 together.
    join_match(&mut store, 100, alice, bob, 1, 2, 10);

    let table = store.table("players").unwrap();
    assert!(
        table
            .lookup("by_zone", &[Value::U64(10)])
            .unwrap()
            .is_empty()
    );
    assert_eq!(table.lookup("by_zone", &[Value::U64(11)]).unwrap().len(), 2);

    let matches = store.table("matches").unwrap();
    assert!(
        matches
            .get_by_primary_key(&[Value::U64(100)])
            .unwrap()
            .is_some()
    );
}

#[test]
fn matchmaking_conflict_aborts_every_table() {
    let mut store = world();
    create_player(&mut store, 1, 10, 1, 100);

    // Player 1 is read at version 0; meanwhile another writer (a direct
    // mutation standing in for a concurrent transaction) bumps player 1.
    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", nexum_core::RowId::from_u64(0))
        .unwrap();
    tx.insert(&store, "matches", row![50u64, 1u64, 2u64, 10u64])
        .unwrap();

    store
        .table_mut("players")
        .unwrap()
        .update(
            nexum_core::RowId::from_u64(0),
            row![1u64, 10u64, 50i32, 1u32],
        )
        .unwrap();
    store.drain_changes(); // the other writer's own change

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));

    // The match insert must not have committed; nothing changed anywhere.
    assert!(store.table("matches").unwrap().is_empty());
    assert!(store.drain_changes().is_empty());
    assert_eq!(
        store
            .table("players")
            .unwrap()
            .version_of(nexum_core::RowId::from_u64(0)),
        Some(nexum_core::Version::from_u64(1))
    );
}

#[test]
fn unique_violation_in_one_table_rolls_back_all_writes() {
    let mut store = world();
    create_player(&mut store, 1, 10, 1, 100);

    // Player 2 claims level 1 — already taken (unique by_level) — while also
    // trying to create a match and an economy account.
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", row![2u64, 20u64, 100i32, 1u32])
        .unwrap();
    tx.insert(&store, "matches", row![1u64, 1u64, 2u64, 20u64])
        .unwrap();
    tx.insert(&store, "economy", row![2u64, 0i64]).unwrap();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));

    // Nothing from the transaction survived: no player 2, no match, no coins.
    assert!(
        store
            .table("players")
            .unwrap()
            .get_by_primary_key(&[Value::U64(2)])
            .unwrap()
            .is_none()
    );
    assert!(store.table("matches").unwrap().is_empty());
    assert!(
        store
            .table("economy")
            .unwrap()
            .get_by_primary_key(&[Value::U64(2)])
            .unwrap()
            .is_none()
    );
    assert_eq!(store.table("players").unwrap().len(), 1);
    assert_eq!(store.table("economy").unwrap().len(), 1);
    assert!(store.drain_changes().is_empty());
}

#[test]
fn economy_transfer_is_atomic() {
    let mut store = world();
    let (_, from) = create_player(&mut store, 1, 10, 1, 100);
    let (_, to) = create_player(&mut store, 2, 10, 2, 200);

    // Transfer 30 coins from player 1 to player 2: two updates, one commit.
    let mut tx = Transaction::begin(&mut store);
    assert!(store.table("economy").unwrap().get(from).is_some());
    assert!(store.table("economy").unwrap().get(to).is_some());
    tx.update(&store, "economy", from, row![1u64, 70i64])
        .unwrap();
    tx.update(&store, "economy", to, row![2u64, 230i64])
        .unwrap();
    let changes = tx.commit(&mut store).unwrap();

    assert_eq!(changes.len(), 2);
    assert!(changes.iter().all(|c| c.kind() == ChangeKind::Update));
    assert_eq!(
        store
            .table("economy")
            .unwrap()
            .get(from)
            .unwrap()
            .get_named(store.table("economy").unwrap().schema(), "coins"),
        Some(&Value::I64(70))
    );
    assert_eq!(
        store
            .table("economy")
            .unwrap()
            .get(to)
            .unwrap()
            .get_named(store.table("economy").unwrap().schema(), "coins"),
        Some(&Value::I64(230))
    );
}
