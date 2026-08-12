//! Integration (Phase 8 brief §19 "WAL/recovery"): after a crash, recovery
//! reconstructs the exact authoritative state; the application re-subscribes
//! over the recovered state; and **recovered history never replays as new
//! live commits** — a fresh snapshot covers everything recovered, and only
//! commits after recovery produce deltas.

use std::path::PathBuf;

use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, RowId, Value};
use nexum_core::row;
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nexum-sub-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

fn player(id: u64, zone: u64, health: i32, level: u32) -> nexum_core::Row {
    row![id, zone, health, level]
}

/// Commits one transaction, appends it to the WAL, and fans it out to the
/// subscription registry — the same order the future runtime will use
/// (commit → WAL → subscriptions).
fn commit_and_durably_fan(
    store: &mut TableStore,
    wal: &mut Wal,
    registry: &mut SubscriptionRegistry,
    body: impl FnOnce(&mut Transaction, &TableStore),
) {
    let mut tx = Transaction::begin(store);
    body(&mut tx, store);
    let changes = tx.commit(store).unwrap();
    wal.append(tx.id(), &changes).unwrap();
    registry.apply_changes(store, &changes);
}

#[test]
fn recovered_history_is_not_replayed_as_live_commits() {
    let dir = temp_dir("recovery");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();

    let mut registry = SubscriptionRegistry::new();
    let sub = registry
        .subscribe(&store, Query::builder("players").predicate_eq("zone_id", 10u64).build().unwrap())
        .unwrap();
    registry.drain(sub).unwrap(); // Initial snapshot (empty)

    // Two commits covered by the snapshot.
    commit_and_durably_fan(&mut store, &mut wal, &mut registry, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    commit_and_durably_fan(&mut store, &mut wal, &mut registry, |tx, store| {
        tx.insert(store, "players", player(2, 20, 90, 2)).unwrap();
    });
    assert_eq!(registry.drain(sub).unwrap().len(), 1); // only the zone-10 insert

    // Snapshot the durable state, then one more commit after it.
    Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir).unwrap();
    commit_and_durably_fan(&mut store, &mut wal, &mut registry, |tx, store| {
        tx.insert(store, "players", player(3, 10, 80, 3)).unwrap();
    });
    assert_eq!(registry.drain(sub).unwrap().len(), 1);

    // Pre-crash reference state.
    let expected_epoch = store.table("players").unwrap().epoch();
    let expected_next_tx = store.next_transaction_id();
    let expected_len = store.table("players").unwrap().len();

    // Crash + recover into a fresh store: only the post-snapshot commit is
    // replayed.
    let mut fresh = TableStore::new();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert!(report.snapshot.is_some());
    assert_eq!(report.replayed_txs, 1);
    assert_eq!(report.replayed_changes, 1);
    assert!(!report.truncated_tail);

    // Exact reconstruction.
    assert_eq!(fresh.table("players").unwrap().len(), expected_len);
    let p1 = fresh.table("players").unwrap().get(RowId::from_u64(0)).unwrap();
    assert_eq!(p1.get_named(fresh.table("players").unwrap().schema(), "health"), Some(&Value::I32(100)));
    assert_eq!(fresh.table("players").unwrap().epoch(), expected_epoch);
    assert_eq!(fresh.next_transaction_id(), expected_next_tx);
    assert!(fresh.drain_changes().is_empty());

    // Re-subscribe over the recovered state: the snapshot covers the whole
    // recovered history — players 1 and 3 are in the Initial, and nothing is
    // re-delivered as live.
    let mut registry2 = SubscriptionRegistry::new();
    let sub2 = registry2
        .subscribe(&fresh, Query::builder("players").predicate_eq("zone_id", 10u64).build().unwrap())
        .unwrap();
    let initial = registry2.drain(sub2).unwrap();
    assert_eq!(initial.len(), 1);
    let rows = match &initial[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows.clone(),
        other => panic!("expected Initial, got {other:?}"),
    };
    assert_eq!(rows.len(), 2, "both zone-10 players recovered");
    assert_eq!(rows[0].row_id(), RowId::from_u64(0));
    assert_eq!(rows[1].row_id(), RowId::from_u64(2));

    // Only a commit *after* recovery produces a live delta — the recovered
    // history never replays as new live commits.
    commit_and_durably_fan(&mut fresh, &mut wal, &mut registry2, |tx, store| {
        tx.insert(store, "players", player(4, 10, 70, 4)).unwrap();
    });
    let live = registry2.drain(sub2).unwrap();
    assert_eq!(live.len(), 1, "no duplicate replay of recovered history");
    match &live[0] {
        SubscriptionUpdate::Insert { row, .. } => {
            assert_eq!(row.row_id(), RowId::from_u64(3));
            assert_eq!(
                row.row().get_named(fresh.table("players").unwrap().schema(), "health"),
                Some(&Value::I32(70))
            );
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}
