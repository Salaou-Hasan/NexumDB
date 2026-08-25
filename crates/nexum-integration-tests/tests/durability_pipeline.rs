//! Cross-crate seam: `Transaction::commit` → `Vec<Change>` → WAL append →
//! snapshot + recovery → subscription views.
//!
//! Proves the durability contract across crate boundaries: committed state
//! survives a fresh `TableStore`, row ids and versions are reproduced
//! exactly, WAL replay covers post-snapshot transactions, and a
//! subscription on a recovered store observes exactly what existed before.

use nexum_core::{ColumnType, RowId, TableSchema, Value, Version, row};
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::{Row, TableStore};
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};

fn players_store() -> TableStore {
    let mut store = TableStore::new();
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone", ColumnType::U64)
                .column("health", ColumnType::I32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

/// Dumps the `players` table as (row id, row, version) triples for equality
/// comparisons across recovery.
fn dump(store: &TableStore) -> Vec<(RowId, Row, Version)> {
    let table = store.table("players").unwrap();
    table
        .scan()
        .map(|(id, r)| (id, r.clone(), table.version_of(id).unwrap()))
        .collect()
}

#[test]
fn commit_wal_recovery_and_subscriptions_agree_on_one_state() {
    let dir = std::env::temp_dir().join("nexum-integration-durability");
    let wal_path = dir.join("log.wal");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    // --- Original store: two commits, observed by one subscription. ------
    let mut store = players_store();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry
        .subscribe(
            &store,
            Query::builder("players")
                .predicate_eq("zone", 10u64)
                .build()
                .unwrap(),
        )
        .unwrap();
    let boot = registry.drain(sub).unwrap();
    assert!(
        boot.iter()
            .all(|u| matches!(u, SubscriptionUpdate::Initial { rows, .. } if rows.is_empty())),
        "empty store establishes an empty initial view"
    );

    let mut wal = Wal::create(&wal_path, DurabilityPolicy::Flush).unwrap();

    // Commit 1: two inserts (one out of the subscribed zone). The RowIds
    // returned by `tx.insert` are tx-local handles; resolve the engine
    // identities from the committed store instead.
    let mut tx = Transaction::begin(&mut store);
    let _alice = tx
        .insert(&store, "players", row![1u64, 10u64, 100i32])
        .unwrap();
    let _bob = tx
        .insert(&store, "players", row![2u64, 20u64, 90i32])
        .unwrap();
    let changes = tx.commit(&mut store).unwrap();
    wal.append(tx.id(), &changes).unwrap();
    assert_eq!(registry.apply_changes(&store, &changes).affected(), &[sub]);

    let engine_row_id = |store: &TableStore, id: u64| {
        store
            .table("players")
            .unwrap()
            .scan()
            .find(|(_, r)| r.get(0) == Some(&Value::U64(id)))
            .map(|(rid, _)| rid)
            .expect("committed row exists")
    };
    let alice = engine_row_id(&store, 1);
    let bob = engine_row_id(&store, 2);

    // Snapshot only after commit 1 — recovery must replay commit 2.
    Snapshot::capture(&store, wal.lsn().as_u64())
        .write(&dir)
        .unwrap();
    let after_snapshot = dump(&store);

    // Commit 2: update alice, delete bob (bob leaves by deletion).
    let mut tx = Transaction::begin(&mut store);
    tx.update(&store, "players", alice, row![1u64, 10u64, 80i32])
        .unwrap();
    tx.delete(&store, "players", bob).unwrap();
    let changes = tx.commit(&mut store).unwrap();
    wal.append(tx.id(), &changes).unwrap();
    assert_eq!(registry.apply_changes(&store, &changes).affected(), &[sub]);

    let final_state = dump(&store);
    assert_eq!(final_state.len(), 1);

    let drained = registry.drain(sub).unwrap();
    assert_eq!(
        drained
            .iter()
            .filter(|u| matches!(u, SubscriptionUpdate::Insert { .. }))
            .count(),
        1,
        "only the in-zone insert enters the view"
    );
    assert!(
        drained
            .iter()
            .any(|u| matches!(u, SubscriptionUpdate::Update { row, .. }
                if row.row().values()[2] == nexum_core::Value::I32(80))),
        "the health update is delivered"
    );
    assert!(
        !drained
            .iter()
            .any(|u| matches!(u, SubscriptionUpdate::Delete { .. })),
        "bob was never in the zone-10 view, so his delete is not delivered"
    );

    // --- Recovery into a fresh store from snapshot + WAL replay. ---------
    // `recover` restores the schema from the snapshot: the store starts empty.
    let mut fresh = TableStore::new();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert_eq!(report.replayed_txs, 1, "only the post-snapshot tx replays");
    assert_ne!(after_snapshot, final_state);
    assert_eq!(
        dump(&fresh),
        final_state,
        "recovery reproduces rows AND versions"
    );

    // A subscription on the recovered store sees the same authoritative view.
    let mut recovered_registry = SubscriptionRegistry::new();
    let sub2 = recovered_registry
        .subscribe(
            &fresh,
            Query::builder("players")
                .predicate_eq("zone", 10u64)
                .build()
                .unwrap(),
        )
        .unwrap();
    let boot = recovered_registry.drain(sub2).unwrap();
    assert_eq!(boot.len(), 1);
    assert!(
        matches!(&boot[0], SubscriptionUpdate::Initial { rows, .. } if rows.len() == 1),
        "one in-zone row survives recovery"
    );

    drop(wal);
    let _ = std::fs::remove_dir_all(&dir);
}
