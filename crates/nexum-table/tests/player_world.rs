//! Integration test mirroring the spec's Phase 2 example:
//!
//! ```text
//! Player {
//!     id: PlayerId,      (u64 primary key)
//!     zone_id: ZoneId,   (u64, secondary index)
//!     health: i32,
//!     level: u32
//! }
//! ```

use nexum_core::{ColumnType, Error, TableSchema, Value};
use nexum_table::{TableStore, row};

fn player_schema() -> TableSchema {
    TableSchema::builder("players")
        .column("id", ColumnType::U64)
        .column("zone_id", ColumnType::U64)
        .column("health", ColumnType::I32)
        .column("level", ColumnType::U32)
        .primary_key(&["id"])
        .index("by_zone", &["zone_id"])
        .build()
        .unwrap()
}

#[test]
fn player_world_end_to_end() {
    let mut store = TableStore::new();
    let players_id = store.create_table(player_schema()).unwrap();
    assert_eq!(players_id.as_u64(), 0);

    // Insert a few players across two zones.
    let alice = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let bob = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 10u64, 90i32, 6u32])
        .unwrap();
    let carol = store
        .table_mut("players")
        .unwrap()
        .insert(row![3u64, 20u64, 80i32, 7u32])
        .unwrap();

    let table = store.table("players").unwrap();

    // get() by engine-assigned RowId.
    assert_eq!(
        table
            .get(alice)
            .unwrap()
            .get_named(table.schema(), "health"),
        Some(&Value::I32(100))
    );

    // get() by declared primary key.
    assert_eq!(
        table
            .get_by_primary_key(&[Value::U64(2)])
            .unwrap()
            .unwrap()
            .get_named(table.schema(), "level"),
        Some(&Value::U32(6))
    );

    // Secondary index lookup by zone.
    assert_eq!(
        table.lookup("by_zone", &[Value::U64(10)]).unwrap(),
        vec![alice, bob]
    );
    assert_eq!(
        table.lookup("by_zone", &[Value::U64(20)]).unwrap(),
        vec![carol]
    );

    // Full scan, deterministic ascending RowId order.
    let scanned: Vec<_> = table.scan().map(|(_, row)| row.values().to_vec()).collect();
    assert_eq!(scanned.len(), 3);

    // update() — damage Alice, move Bob to another zone.
    let table = store.table_mut("players").unwrap();
    table.update(alice, row![1u64, 10u64, 55i32, 5u32]).unwrap();
    table.update(bob, row![2u64, 30u64, 90i32, 6u32]).unwrap();

    let table = store.table("players").unwrap();
    assert_eq!(
        table
            .get(alice)
            .unwrap()
            .get_named(table.schema(), "health"),
        Some(&Value::I32(55))
    );
    // Alice is still in zone 10; Bob moved to zone 30.
    assert_eq!(
        table.lookup("by_zone", &[Value::U64(10)]).unwrap(),
        vec![alice]
    );
    assert_eq!(
        table.lookup("by_zone", &[Value::U64(30)]).unwrap(),
        vec![bob]
    );

    // delete() — Carol leaves the world.
    let table = store.table_mut("players").unwrap();
    table.delete(carol).unwrap();
    assert_eq!(table.len(), 2);

    // Constraints hold: duplicate primary key rejected.
    let table = store.table_mut("players").unwrap();
    let err = table.insert(row![1u64, 99u64, 1i32, 1u32]).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
}

#[test]
fn duplicate_table_name_rejected() {
    let mut store = TableStore::new();
    store.create_table(player_schema()).unwrap();
    let err = store.create_table(player_schema()).unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
}
