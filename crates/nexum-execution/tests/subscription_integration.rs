//! Integration (Phase 9 brief §20, §24 "Subscriptions"): a successful
//! simulation tick's `Vec<Change>` fans out to the subscription engine
//! exactly like any other committed transaction — one atomic transition per
//! tick, never partial, never from a failed tick.

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_execution::{InputFrame, Partition, PartitionConfig, SystemDefinition};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::TableStore;

fn fixture() -> Partition {
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
    Partition::new(WorldId::from_u64(0), store, PartitionConfig::new()).unwrap()
}

fn subscribe_zone10(
    store: &TableStore,
) -> (SubscriptionRegistry, nexum_subscription::SubscriptionId) {
    let mut registry = SubscriptionRegistry::new();
    let sub = registry
        .subscribe(
            store,
            Query::builder("players")
                .predicate_eq("zone", 10u64)
                .build()
                .unwrap(),
        )
        .unwrap();
    // Consume the Initial snapshot so live deltas are isolated.
    let initial = registry.drain(sub).unwrap();
    assert_eq!(initial.len(), 1);
    (registry, sub)
}

#[test]
fn tick_commits_flow_to_subscriptions_as_one_atomic_transition() {
    let mut world = fixture();
    world
        .native_mut()
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "spawn", |ctx, args| {
                let id = args.require_u64("id")?;
                ctx.insert("players", row![id, 10u64, 50i32])?;
                Ok(Value::U64(id))
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 10, |ctx, _| {
                ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(1), "invoker", 20, |ctx, _| {
                ctx.invoke_reducer(
                    "spawn",
                    &ReducerArgs::new().insert("id", 200 + ctx.tick().as_u64()),
                )?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (mut registry, sub) = subscribe_zone10(world.store());

    // Tick 0: writer (id 0) + reducer (id 200) both match zone 10 — the
    // subscription must see the whole tick as ONE transition.
    let result = world.tick(&InputFrame::new(TickId::from_u64(0))).unwrap();
    let report = registry.apply_changes(world.store(), result.changes());
    assert_eq!(report.affected(), &[sub]);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2);
    for update in &updates {
        match update {
            SubscriptionUpdate::Insert { seq, row } => {
                assert_eq!(*seq, report.seq());
                assert!(row.row_id().as_u64() < 2); // both rows are fresh
            }
            other => panic!("expected Insert, got {other:?}"),
        }
    }
    assert_eq!(world.store().table("players").unwrap().len(), 2);

    // Tick 1: another atomic transition with two more matching rows.
    let result = world.tick(&InputFrame::new(TickId::from_u64(1))).unwrap();
    let report = registry.apply_changes(world.store(), result.changes());
    assert_eq!(report.seq(), 1);
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 2);
    assert_eq!(world.store().table("players").unwrap().len(), 4);
}

#[test]
fn failed_ticks_produce_no_subscription_updates() {
    let mut world = fixture();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writes_then_fails", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                Err(nexum_core::Error::invalid_argument("nope"))
            })
            .unwrap(),
        )
        .unwrap();

    let (mut registry, sub) = subscribe_zone10(world.store());

    let error = world
        .tick(&InputFrame::new(TickId::from_u64(0)))
        .unwrap_err();
    assert!(matches!(
        error.error(),
        nexum_core::Error::InvalidArgument(_)
    ));
    // Nothing committed, so nothing reached the subscription engine.
    assert!(registry.drain(sub).unwrap().is_empty());
    assert_eq!(registry.next_seq(), 0);
    assert!(world.store().table("players").unwrap().is_empty());
}

#[test]
fn resync_after_ticks_matches_authoritative_state() {
    let mut world = fixture();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                let tick = ctx.tick().as_u64();
                // Mix matching (zone 10) and non-matching (zone 99) rows.
                ctx.insert("players", row![tick, 10u64, 100i32])?;
                ctx.insert("players", row![100 + tick, 99u64, 1i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (mut registry, sub) = subscribe_zone10(world.store());
    for tick in 0..4u64 {
        let result = world
            .tick(&InputFrame::new(TickId::from_u64(tick)))
            .unwrap();
        let _ = registry.apply_changes(world.store(), result.changes());
    }
    registry.drain(sub).unwrap(); // 4 live inserts (the non-matching rows never appeared)

    // Resync rebuilds the view from authoritative state: only the 4 zone-10
    // rows, plus the 100/200/300/400 rows were excluded by the predicate.
    registry.resync(world.store(), sub).unwrap();
    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Resync { rows, .. } => {
            assert_eq!(rows.len(), 4);
        }
        other => panic!("expected Resync, got {other:?}"),
    }
    assert_eq!(world.store().table("players").unwrap().len(), 8);
}

#[test]
fn multi_table_tick_is_observed_atomically() {
    let mut store = TableStore::new();
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone", ColumnType::U64)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
        .create_table(
            TableSchema::builder("items")
                .column("id", ColumnType::U64)
                .column("owner", ColumnType::U64)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    let mut world = Partition::new(WorldId::from_u64(0), store, PartitionConfig::new()).unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "both", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64])?;
                ctx.insert("items", row![1u64, 1u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let mut registry = SubscriptionRegistry::new();
    let players_sub = registry
        .subscribe(world.store(), Query::builder("players").build().unwrap())
        .unwrap();
    let items_sub = registry
        .subscribe(world.store(), Query::builder("items").build().unwrap())
        .unwrap();
    registry.drain(players_sub).unwrap();
    registry.drain(items_sub).unwrap();

    // One tick, one apply_changes, both subscriptions updated in one pass.
    let result = world.tick(&InputFrame::new(TickId::from_u64(0))).unwrap();
    let report = registry.apply_changes(world.store(), result.changes());
    assert_eq!(report.affected(), &[players_sub, items_sub]);
    assert_eq!(registry.drain(players_sub).unwrap().len(), 1);
    assert_eq!(registry.drain(items_sub).unwrap().len(), 1);
}
