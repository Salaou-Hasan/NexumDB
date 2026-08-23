//! Integration test for the storage engine lifecycle: authoritative rows,
//! versions, change tracking, and multi-table independence.

use nexum_core::row;
use nexum_core::{ChangeKind, ColumnType, TableId, TableSchema, Version};
use nexum_storage::{Change, StorageTable, StoredRow};

fn player_schema() -> TableSchema {
    TableSchema::builder("players")
        .column("id", ColumnType::U64)
        .column("zone_id", ColumnType::U64)
        .column("health", ColumnType::I32)
        .build()
        .unwrap()
}

#[test]
fn full_lifecycle_with_versions_and_changes() {
    let mut players = StorageTable::new(TableId::from_u64(0), player_schema());

    // Insert.
    let alice = players.insert(row![1u64, 10u64, 100i32]).unwrap();
    let bob = players.insert(row![2u64, 10u64, 90i32]).unwrap();
    assert_eq!(players.version_of(alice), Some(Version::ZERO));
    assert_eq!(players.len(), 2);

    // Update bumps versions.
    players.update(alice, row![1u64, 30u64, 55i32]).unwrap();
    players.update(alice, row![1u64, 30u64, 20i32]).unwrap();
    assert_eq!(players.version_of(alice), Some(Version::from_u64(2)));
    assert_eq!(players.version_of(bob), Some(Version::ZERO)); // untouched

    // Read row + version atomically (the OCC-friendly read).
    let stored: Option<&StoredRow> = players.get(alice);
    assert_eq!(stored.unwrap().version(), Version::from_u64(2));
    assert_eq!(
        stored.unwrap().row().get_named(players.schema(), "health"),
        Some(&nexum_core::Value::I32(20))
    );

    // Delete.
    players.delete(bob).unwrap();
    assert!(players.get(bob).is_none());
    assert_eq!(players.version_of(bob), None);
    assert_eq!(players.len(), 1);

    // Change stream reflects the full history in order.
    let changes = players.drain_changes();
    assert_eq!(changes.len(), 5);
    let kinds: Vec<ChangeKind> = changes.iter().map(Change::kind).collect();
    assert_eq!(
        kinds,
        vec![
            ChangeKind::Insert,
            ChangeKind::Insert,
            ChangeKind::Update,
            ChangeKind::Update,
            ChangeKind::Delete,
        ]
    );
    // Every change carries the table id and a row id.
    for change in &changes {
        assert_eq!(change.table_id(), TableId::from_u64(0));
    }
    // Update changes carry both old and new rows and versions.
    assert_eq!(changes[2].old_version(), Some(Version::ZERO));
    assert_eq!(changes[2].new_version(), Some(Version::from_u64(1)));
    assert!(changes[2].old_row().is_some());
    assert!(changes[2].new_row().is_some());
    // Delete carries the final version.
    assert_eq!(changes[4].old_version(), Some(Version::ZERO));

    assert!(players.changes().is_empty());
}

#[test]
fn multiple_tables_are_independent() {
    let mut players = StorageTable::new(TableId::from_u64(0), player_schema());
    let items_schema = TableSchema::builder("items")
        .column("id", ColumnType::U64)
        .column("qty", ColumnType::I32)
        .build()
        .unwrap();
    let mut items = StorageTable::new(TableId::from_u64(1), items_schema);

    let p = players.insert(row![1u64, 10u64, 100i32]).unwrap();
    let i = items.insert(row![7u64, 3i32]).unwrap();

    assert_eq!(p.as_u64(), 0);
    assert_eq!(i.as_u64(), 0); // per-table id spaces are independent

    players.update(p, row![1u64, 10u64, 1i32]).unwrap();
    assert_eq!(items.version_of(i), Some(Version::ZERO));

    let p_changes = players.drain_changes();
    let i_changes = items.drain_changes();
    assert_eq!(p_changes.len(), 2);
    assert_eq!(i_changes.len(), 1);
    assert_eq!(p_changes[0].table_id(), TableId::from_u64(0));
    assert_eq!(i_changes[0].table_id(), TableId::from_u64(1));
}

#[test]
fn empty_table_and_scan() {
    let mut table = StorageTable::new(TableId::from_u64(2), player_schema());
    assert!(table.is_empty());
    assert_eq!(table.scan().count(), 0);
    assert_eq!(table.drain_changes().len(), 0);

    let a = table.insert(row![1u64, 10u64, 100i32]).unwrap();
    let b = table.insert(row![2u64, 20u64, 90i32]).unwrap();
    let scanned: Vec<_> = table
        .scan()
        .map(|(id, stored)| (id, stored.row().clone()))
        .collect();
    assert_eq!(scanned.len(), 2);
    assert_eq!(scanned[0].0, a);
    assert_eq!(scanned[1].0, b);
    assert!(scanned[0].1.get_named(table.schema(), "zone_id").is_some());
}
