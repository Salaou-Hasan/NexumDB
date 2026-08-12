//! Tests for the subscription engine: lifecycle, initial state, change
//! processing, transaction semantics, correctness, determinism,
//! backpressure, and resync (Phase 8 brief §19).

use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, Error, RowId, Value};
use nexum_core::row;
use nexum_storage::Change;
use crate::{
    ComparisonOp, OrderDirection, Query, SubscriptionConfig, SubscriptionRegistry,
    SubscriptionState, SubscriptionUpdate,
};
use nexum_table::TableStore;
use nexum_tx::Transaction;

/// A two-table world: `players` (id, zone_id, health, level) with primary
/// key and a unique level index, and `economy` (owner, coins).
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

/// Runs one transaction to completion and returns its committed changes.
fn commit(store: &mut TableStore, body: impl FnOnce(&mut Transaction, &TableStore)) -> Vec<Change> {
    let mut tx = Transaction::begin(store);
    body(&mut tx, store);
    tx.commit(store).unwrap()
}

fn player(id: u64, zone: u64, health: i32, level: u32) -> nexum_core::Row {
    row![id, zone, health, level]
}

fn zone10() -> Query {
    Query::builder("players").predicate_eq("zone_id", 10u64).build().unwrap()
}

// ------------------------------------------------------------- lifecycle

#[test]
fn subscribe_on_missing_table_fails() {
    let store = world();
    let mut registry = SubscriptionRegistry::new();
    let err = registry.subscribe(&store, Query::builder("ghosts").build().unwrap()).unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[test]
fn subscribe_on_empty_table_delivers_empty_initial() {
    let store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Initial { seq, rows } => {
            assert_eq!(*seq, 0);
            assert!(rows.is_empty());
        }
        other => panic!("expected Initial, got {other:?}"),
    }
    assert_eq!(registry.lookup(sub).unwrap().state(), SubscriptionState::Active);
}

#[test]
fn subscribe_on_populated_table_snaps_hot() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 20, 90, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 80, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    let updates = registry.drain(sub).unwrap();
    let rows = match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows,
        other => panic!("expected Initial, got {other:?}"),
    };
    // Two matching rows, ascending RowId order (no order_by).
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row_id(), RowId::from_u64(0));
    assert_eq!(rows[1].row_id(), RowId::from_u64(2));
}

#[test]
fn unsubscribe_and_drain_errors() {
    let store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.unsubscribe(sub).unwrap();
    assert!(registry.lookup(sub).is_none());
    assert!(registry.is_empty());
    assert!(matches!(
        registry.unsubscribe(sub).unwrap_err(),
        Error::NotFound(_)
    ));
    assert!(matches!(registry.drain(sub).unwrap_err(), Error::NotFound(_)));
    assert!(matches!(registry.is_stale(sub).unwrap_err(), Error::NotFound(_)));
    assert!(matches!(registry.resync(&store, sub).unwrap_err(), Error::NotFound(_)));
}

#[test]
fn duplicate_queries_are_distinct_subscriptions() {
    let store = world();
    let mut registry = SubscriptionRegistry::new();
    let a = registry.subscribe(&store, zone10()).unwrap();
    let b = registry.subscribe(&store, zone10()).unwrap();
    assert_ne!(a, b);
    assert_eq!(registry.len(), 2);
    let ids: Vec<u64> = registry.list().map(|s| s.id().as_u64()).collect();
    assert_eq!(ids, vec![a.as_u64(), b.as_u64()]); // ascending id order
}

#[test]
fn with_config_rejects_zero_bounds() {
    let err = SubscriptionRegistry::with_config(SubscriptionConfig {
        max_buffered: 0,
        ..SubscriptionConfig::default()
    })
    .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

// ------------------------------------------------------- initial state

#[test]
fn initial_snapshot_filters_and_orders() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 20, 90, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 40, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .predicate_eq("zone_id", 10u64)
        .order_by("health", OrderDirection::Ascending)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    let rows = match &registry.drain(sub).unwrap()[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows.clone(),
        other => panic!("expected Initial, got {other:?}"),
    };
    // health 40 first, then health 100 — despite row ids 2 and 0.
    assert_eq!(rows[0].row_id(), RowId::from_u64(2));
    assert_eq!(rows[1].row_id(), RowId::from_u64(0));
}

#[test]
fn initial_descending_order_tie_breaks_by_row_id() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 100, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    let rows = match &registry.drain(sub).unwrap()[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows.clone(),
        other => panic!("expected Initial, got {other:?}"),
    };
    // Equal health 100 rows tie-break by ascending RowId.
    assert_eq!(rows[0].row_id(), RowId::from_u64(0));
    assert_eq!(rows[1].row_id(), RowId::from_u64(2));
    assert_eq!(rows[2].row_id(), RowId::from_u64(1));
}

#[test]
fn initial_projection_selects_columns() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .project(&["id", "health"])
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    let rows = match &registry.drain(sub).unwrap()[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows.clone(),
        other => panic!("expected Initial, got {other:?}"),
    };
    assert_eq!(rows[0].row().values(), &[Value::U64(1), Value::I32(100)]);
}

#[test]
fn initial_limit_keeps_top_n() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 10, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 90, 3)).unwrap();
        tx.insert(store, "players", player(4, 10, 30, 4)).unwrap();
        tx.insert(store, "players", player(5, 10, 70, 5)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .limit(2)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    let rows = match &registry.drain(sub).unwrap()[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows.clone(),
        other => panic!("expected Initial, got {other:?}"),
    };
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row_id(), RowId::from_u64(2)); // health 90
    assert_eq!(rows[1].row_id(), RowId::from_u64(4)); // health 70
    assert_eq!(registry.lookup(sub).unwrap().visible_len(), 2);
}

// ------------------------------------------------------ change processing

#[test]
fn insert_matching_and_non_matching() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap(); // consume Initial

    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 20, 90, 2)).unwrap();
    });
    let report = registry.apply_changes(&store, &changes);
    assert_eq!(report.affected(), &[sub]);

    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1, "only the matching insert is delivered");
    match &updates[0] {
        SubscriptionUpdate::Insert { seq, row } => {
            assert_eq!(*seq, report.seq());
            assert_eq!(row.row_id(), RowId::from_u64(0));
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn update_inside_inside_delivers_update() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(0), player(1, 10, 50, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    match &updates[0] {
        SubscriptionUpdate::Update { row, .. } => {
            assert_eq!(row.row().get_named(store.table("players").unwrap().schema(), "health"),
                       Some(&Value::I32(50)));
        }
        other => panic!("expected Update, got {other:?}"),
    }
}

#[test]
fn update_inside_outside_delivers_delete() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(0), player(1, 20, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert!(matches!(&updates[0], SubscriptionUpdate::Delete { row_id, .. } if *row_id == RowId::from_u64(0)));
}

#[test]
fn update_outside_inside_delivers_insert() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 20, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(0), player(1, 10, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert!(matches!(&updates[0], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(0)));
}

#[test]
fn update_outside_outside_delivers_nothing() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 20, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(0), player(1, 30, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.drain(sub).unwrap().is_empty());
}

#[test]
fn delete_visible_and_invisible() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 20, 90, 2)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.delete(store, "players", RowId::from_u64(0)).unwrap();
        tx.delete(store, "players", RowId::from_u64(1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    // Only the visible row's deletion is delivered.
    assert_eq!(updates.len(), 1);
    assert!(matches!(&updates[0], SubscriptionUpdate::Delete { row_id, .. } if *row_id == RowId::from_u64(0)));
}

#[test]
fn limit_window_insert_evicts() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 10, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 90, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .limit(2)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    registry.drain(sub).unwrap(); // Initial: 90, 50

    // Insert health 100: the worst row is evicted (Delete first), then the
    // new top row enters (Insert).
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(4, 10, 100, 4)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2);
    assert!(matches!(&updates[0], SubscriptionUpdate::Delete { row_id, .. } if *row_id == RowId::from_u64(1)));
    assert!(matches!(&updates[1], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(3)));

    // A matching insert that ranks outside the window: no net change.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(5, 10, 5, 5)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.drain(sub).unwrap().is_empty());
}

#[test]
fn limit_window_delete_backfills() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 10, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 90, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .limit(2)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    registry.drain(sub).unwrap(); // Initial: 90, 50; row 0 excluded

    // Deleting the top row must promote the next-best row (health 10) into
    // the window: Delete the removed row, Insert the backfill.
    let changes = commit(&mut store, |tx, store| {
        tx.delete(store, "players", RowId::from_u64(2)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2);
    assert!(matches!(&updates[0], SubscriptionUpdate::Delete { row_id, .. } if *row_id == RowId::from_u64(2)));
    assert!(matches!(&updates[1], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(0)));
    assert_eq!(registry.lookup(sub).unwrap().visible_len(), 2);
}

#[test]
fn resync_clears_pending_deltas() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    // Leave the Initial undrained.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);

    registry.resync(&store, sub).unwrap();
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1, "resync clears the Initial and the delta");
    assert!(matches!(&updates[0], SubscriptionUpdate::Resync { rows, .. } if rows.len() == 1));
}

#[test]
fn limit_window_update_reorders_and_evicts() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 10, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 90, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .limit(2)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    registry.drain(sub).unwrap(); // Initial: 90, 50 (health), row 0 excluded

    // Update the top row to health 5: it drops out of the window and the
    // next-best previously-invisible row (health 10) backfills into it.
    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(2), player(3, 10, 5, 3)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2, "Delete the demoted row, Insert the backfill");
    assert!(matches!(&updates[0], SubscriptionUpdate::Delete { row_id, .. } if *row_id == RowId::from_u64(2)));
    assert!(matches!(&updates[1], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(0)));

    // Update the backfilled row to the top: it stays visible, so only an
    // Update is emitted (membership is unchanged).
    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(0), player(1, 10, 95, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    assert!(matches!(&updates[0], SubscriptionUpdate::Update { row, .. } if row.row_id() == RowId::from_u64(0)));

    // A matching update of an invisible row that still ranks outside the
    // window produces nothing.
    let changes = commit(&mut store, |tx, store| {
        tx.update(store, "players", RowId::from_u64(2), player(3, 10, 7, 3)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.drain(sub).unwrap().is_empty());
}

// ------------------------------------------------------- transaction tests

#[test]
fn aborted_transaction_produces_no_deltas() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", player(1, 10, 100, 1)).unwrap();
    tx.abort().unwrap();

    // An aborted transaction commits nothing: the empty change set still
    // advances the sequence, but no subscription is affected.
    let report = registry.apply_changes(&store, &[]);
    assert!(report.affected().is_empty());
    assert!(registry.drain(sub).unwrap().is_empty());
    assert!(store.table("players").unwrap().is_empty());
}

#[test]
fn multiple_changes_in_one_commit_deliver_in_order() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 90, 2)).unwrap();
        tx.insert(store, "players", player(3, 20, 80, 3)).unwrap();
    });
    let report = registry.apply_changes(&store, &changes);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2);
    assert!(matches!(&updates[0], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(0)));
    assert!(matches!(&updates[1], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(1)));
    assert!(updates.iter().all(|u| update_seq(u) == report.seq()));
}

#[test]
fn multi_table_atomic_delivery() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let players = registry.subscribe(&store, zone10()).unwrap();
    let economy = registry
        .subscribe(
            &store,
            Query::builder("economy").predicate_gt("coins", 0i64).build().unwrap(),
        )
        .unwrap();
    registry.drain(players).unwrap();
    registry.drain(economy).unwrap();

    // One transaction mutating both tables → one apply → both subscriptions
    // see their deltas with the same sequence; neither sees the other's.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
        tx.insert(store, "economy", row![1u64, 100i64]).unwrap();
    });
    let report = registry.apply_changes(&store, &changes);
    assert_eq!(report.affected().len(), 2);

    let player_updates = registry.drain(players).unwrap();
    let economy_updates = registry.drain(economy).unwrap();
    assert_eq!(player_updates.len(), 1);
    assert_eq!(economy_updates.len(), 1);
    assert_eq!(update_seq(&player_updates[0]), report.seq());
    assert_eq!(update_seq(&economy_updates[0]), report.seq());
}

#[test]
fn unrelated_table_changes_do_not_affect() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "economy", row![9u64, 5i64]).unwrap();
    });
    let report = registry.apply_changes(&store, &changes);
    assert!(report.affected().is_empty());
    assert!(registry.drain(sub).unwrap().is_empty());
}

// ---------------------------------------------------------- correctness

#[test]
fn no_missed_no_duplicate_changes() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    for id in 1..=5u64 {
        let changes = commit(&mut store, |tx, store| {
            tx.insert(store, "players", player(id, 10, 100, id as u32)).unwrap();
        });
        registry.apply_changes(&store, &changes);
    }
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 5, "exactly one delta per commit");
    for (index, update) in updates.iter().enumerate() {
        assert!(matches!(&update, SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(index as u64)));
        assert_eq!(update_seq(update), index as u64);
    }
}

#[test]
fn snapshot_live_boundary_is_exact() {
    let mut store = world();
    // Committed before establishment: must appear only in the Initial.
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    let initial = registry.drain(sub).unwrap();
    assert_eq!(initial.len(), 1);
    let rows = match &initial[0] {
        SubscriptionUpdate::Initial { rows, .. } => rows,
        other => panic!("expected Initial, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row_id(), RowId::from_u64(0));

    // Committed after establishment: live delta only.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(2, 10, 90, 2)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    let live = registry.drain(sub).unwrap();
    assert_eq!(live.len(), 1, "no duplicates, no missed change");
    assert!(matches!(&live[0], SubscriptionUpdate::Insert { row, .. } if row.row_id() == RowId::from_u64(1)));
}

#[test]
fn cursor_advances_with_commits_and_resync() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    assert_eq!(registry.next_seq(), 0);
    assert_eq!(registry.lookup(sub).unwrap().cursor(), 0);

    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    let report = registry.apply_changes(&store, &changes);
    assert_eq!(report.seq(), 0);
    assert_eq!(registry.next_seq(), 1);
    // The live cursor only moves on resync.
    assert_eq!(registry.lookup(sub).unwrap().cursor(), 0);

    registry.resync(&store, sub).unwrap();
    assert_eq!(registry.lookup(sub).unwrap().cursor(), 1);
    assert_eq!(registry.next_seq(), 1);
}

#[test]
fn deterministic_across_registries() {
    let run = || -> Vec<SubscriptionUpdate> {
        let mut store = world();
        commit(&mut store, |tx, store| {
            tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
            tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
            tx.insert(store, "players", player(3, 20, 90, 3)).unwrap();
        });
        let mut registry = SubscriptionRegistry::new();
        let query = Query::builder("players")
            .order_by("health", OrderDirection::Descending)
            .limit(2)
            .build()
            .unwrap();
        let sub = registry.subscribe(&store, query).unwrap();
        registry.drain(sub).unwrap();
        let changes = commit(&mut store, |tx, store| {
            tx.update(store, "players", RowId::from_u64(0), player(1, 10, 5, 1)).unwrap();
            tx.insert(store, "players", player(4, 10, 120, 4)).unwrap();
        });
        registry.apply_changes(&store, &changes);
        registry.resync(&store, sub).unwrap();
        registry.drain(sub).unwrap()
    };
    assert_eq!(run(), run(), "identical input sequence ⇒ identical deltas");
}

#[test]
fn resync_rebuilds_the_exact_view() {
    let mut store = world();
    commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 10, 1)).unwrap();
        tx.insert(store, "players", player(2, 10, 50, 2)).unwrap();
        tx.insert(store, "players", player(3, 10, 90, 3)).unwrap();
    });
    let mut registry = SubscriptionRegistry::new();
    let query = Query::builder("players")
        .order_by("health", OrderDirection::Descending)
        .limit(2)
        .build()
        .unwrap();
    let sub = registry.subscribe(&store, query).unwrap();
    registry.drain(sub).unwrap();

    // Two commits that shuffle the window.
    for (id, level) in [(9u64, 9u32), (8u64, 8u32)] {
        let changes = commit(&mut store, |tx, store| {
            tx.insert(store, "players", player(id, 10, 1, level)).unwrap();
        });
        registry.apply_changes(&store, &changes);
    }
    registry.resync(&store, sub).unwrap();

    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1, "resync clears the pending buffer");
    let rows = match &updates[0] {
        SubscriptionUpdate::Resync { seq, rows } => {
            assert_eq!(*seq, registry.next_seq());
            rows
        }
        other => panic!("expected Resync, got {other:?}"),
    };
    // The two highest-health rows remain, in order.
    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].row().get_named(store.table("players").unwrap().schema(), "health"),
               Some(&Value::I32(90)));
    assert_eq!(rows[1].row().get_named(store.table("players").unwrap().schema(), "health"),
               Some(&Value::I32(50)));
    assert_eq!(registry.lookup(sub).unwrap().state(), SubscriptionState::Active);
}

// ---------------------------------------------------------- backpressure

#[test]
fn buffer_overflow_marks_stale_and_drops_deltas() {
    let store = world();
    let mut registry = SubscriptionRegistry::with_config(SubscriptionConfig {
        max_buffered: 1,
        ..SubscriptionConfig::default()
    })
    .unwrap();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    // The Initial snapshot is the first buffered update; it fits.
    assert_eq!(registry.lookup(sub).unwrap().buffer_len(), 1);
    assert!(!registry.is_stale(sub).unwrap());

    let mut store = store;
    // The next delta overflows the buffer of 1: the subscription is marked
    // stale and its buffer is replaced by a single Stale marker.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);

    assert!(registry.is_stale(sub).unwrap());
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    assert!(matches!(&updates[0], SubscriptionUpdate::Stale { .. }));

    // While stale, deltas are dropped entirely.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(2, 10, 90, 2)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.drain(sub).unwrap().is_empty());
}

#[test]
fn resync_recovers_from_stale() {
    let store = world();
    let mut registry = SubscriptionRegistry::with_config(SubscriptionConfig {
        max_buffered: 1,
        ..SubscriptionConfig::default()
    })
    .unwrap();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    // The Initial snapshot fills the buffer of 1; the next delta overflows.
    assert_eq!(registry.lookup(sub).unwrap().buffer_len(), 1);

    let mut store = store;
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(1, 10, 100, 1)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.is_stale(sub).unwrap());
    registry.drain(sub).unwrap(); // consume the Stale marker

    registry.resync(&store, sub).unwrap();
    assert!(!registry.is_stale(sub).unwrap());
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Resync { rows, .. } => assert_eq!(rows.len(), 1),
        other => panic!("expected Resync, got {other:?}"),
    }
}

#[test]
fn dropped_table_marks_stale_and_resync_fails() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    store.drop_table("players").unwrap();

    // The next commit — even one touching only unrelated tables — detects
    // the dropped table and marks the subscription stale.
    let economy_changes = commit(&mut store, |tx, store| {
        tx.insert(store, "economy", row![1u64, 5i64]).unwrap();
    });
    let report = registry.apply_changes(&store, &economy_changes);
    assert!(report.affected().contains(&sub));
    assert!(registry.is_stale(sub).unwrap());
    assert!(matches!(registry.drain(sub).unwrap()[0], SubscriptionUpdate::Stale { .. }));

    // The table is gone: resync cannot rebuild the view.
    assert!(matches!(registry.resync(&store, sub).unwrap_err(), Error::NotFound(_)));

    // On-demand detection is also available without a commit.
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    store.drop_table("players").unwrap();
    registry.refresh(&store);
    assert!(registry.is_stale(sub).unwrap());
}

#[test]
fn dropped_and_recreated_table_resync_reattaches() {
    let mut store = world();
    let mut registry = SubscriptionRegistry::new();
    let sub = registry.subscribe(&store, zone10()).unwrap();
    registry.drain(sub).unwrap();

    store.drop_table("players").unwrap();
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
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(7, 10, 42, 7)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.is_stale(sub).unwrap());

    // Resync recompiles by name and re-attaches to the new table.
    registry.resync(&store, sub).unwrap();
    assert!(!registry.is_stale(sub).unwrap());
    let updates = registry.drain(sub).unwrap();
    let rows = match &updates[0] {
        SubscriptionUpdate::Resync { rows, .. } => rows,
        other => panic!("expected Resync, got {other:?}"),
    };
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].row().get_named(store.table("players").unwrap().schema(), "health"),
               Some(&Value::I32(42)));

    // The re-attached subscription is live again.
    let changes = commit(&mut store, |tx, store| {
        tx.insert(store, "players", player(8, 10, 1, 8)).unwrap();
    });
    registry.apply_changes(&store, &changes);
    assert!(registry.drain(sub).unwrap().len() == 1);
}

#[test]
fn empty_change_set_advances_sequence() {
    let store = world();
    let mut registry = SubscriptionRegistry::new();
    registry.subscribe(&store, zone10()).unwrap();
    let report = registry.apply_changes(&store, &[]);
    assert_eq!(report.seq(), 0);
    assert!(report.affected().is_empty());
    assert_eq!(registry.next_seq(), 1);
}

// ---------------------------------------------------------------- helpers

fn update_seq(update: &SubscriptionUpdate) -> u64 {
    match update {
        SubscriptionUpdate::Initial { seq, .. }
        | SubscriptionUpdate::Insert { seq, .. }
        | SubscriptionUpdate::Update { seq, .. }
        | SubscriptionUpdate::Delete { seq, .. }
        | SubscriptionUpdate::Stale { seq }
        | SubscriptionUpdate::Resync { seq, .. } => *seq,
    }
}

// Exercise every comparison operator through the builder.
#[test]
fn comparison_ops_are_usable_in_queries() {
    let query = Query::builder("players")
        .predicate( "zone_id", ComparisonOp::Eq, 10u64)
        .predicate_ne("zone_id", 0u64)
        .predicate_lt("health", 200i32)
        .predicate_lte("health", 100i32)
        .predicate_gt("health", 0i32)
        .predicate_gte("health", 1i32)
        .build()
        .unwrap();
    assert_eq!(query.predicates().len(), 6);
    assert_eq!(ComparisonOp::Eq.name(), "==");
}
