//! Integration (Phase 10 brief §32 "Subscriptions"): committed changes reach
//! per-world subscriptions in commit order; recovered history is delivered
//! as a single initial snapshot, never replayed as live updates.

use std::path::PathBuf;

use nexum_core::row;
use nexum_core::{ColumnType, SystemId, TickId, WorldId};
use nexum_execution::{Partition, PartitionConfig, SystemDefinition};
use nexum_runtime::{PartitionFactory, PersistencePolicy, Runtime, RuntimeConfig};
use nexum_subscription::{Query, SubscriptionUpdate};
use nexum_table::TableStore;

fn players_table(store: &mut TableStore) {
    store
        .create_table(
            nexum_core::TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone", ColumnType::U64)
                .column("health", ColumnType::I32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
}

/// A factory whose single system inserts one player per tick.
fn writer_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        if store.table("players").is_none() {
            players_table(&mut store);
        }
        let mut world = Partition::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                    ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

#[test]
fn subscriptions_observe_only_their_own_world_s_commits() {
    let mut runtime =
        Runtime::new(RuntimeConfig::new(writer_factory()).with_worker_count(2)).unwrap();
    for w in 0..2u64 {
        runtime
            .create_partition(WorldId::from_u64(w), PartitionConfig::new())
            .unwrap();
        runtime.start_partition(WorldId::from_u64(w)).unwrap();
    }
    let sub_a = runtime
        .subscribe(
            WorldId::from_u64(0),
            Query::builder("players").build().unwrap(),
        )
        .unwrap();
    let sub_b = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("players").build().unwrap(),
        )
        .unwrap();
    runtime.drain(WorldId::from_u64(0), sub_a).unwrap(); // Initial
    runtime.drain(WorldId::from_u64(1), sub_b).unwrap(); // Initial

    runtime.step().unwrap();

    let a = runtime.drain(WorldId::from_u64(0), sub_a).unwrap();
    let b = runtime.drain(WorldId::from_u64(1), sub_b).unwrap();
    assert_eq!(a.len(), 1);
    assert_eq!(b.len(), 1);
    // Both partitions independently inserted row id 0 — identical ids,
    // fully isolated authoritative state.
    assert!(matches!(&a[0], SubscriptionUpdate::Insert { row, .. } if row.row_id().as_u64() == 0));
    assert!(matches!(&b[0], SubscriptionUpdate::Insert { row, .. } if row.row_id().as_u64() == 0));
}

#[test]
fn recovered_history_is_a_snapshot_not_live_updates() {
    let dir = temp_dir("nexum-runtime-sub-recover");
    let world = WorldId::from_u64(0);
    {
        let mut runtime = Runtime::new(
            RuntimeConfig::new(writer_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        runtime
            .create_partition(world, PartitionConfig::new())
            .unwrap();
        runtime.start_partition(world).unwrap();
        for _ in 0..3 {
            runtime.step().unwrap();
        }
        runtime.shutdown().unwrap();
    }

    let mut runtime = Runtime::new(
        RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    runtime
        .recover_partition(world, PartitionConfig::new(), Some(TickId::from_u64(3)))
        .unwrap();
    runtime.start_partition(world).unwrap();

    // A subscription created after recovery sees exactly one Initial
    // snapshot: the 3 historical rows are not replayed as live inserts.
    let sub = runtime
        .subscribe(world, Query::builder("players").build().unwrap())
        .unwrap();
    let updates = runtime.drain(world, sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => assert_eq!(rows.len(), 3),
        other => panic!("expected Initial, got {other:?}"),
    }

    // Only future commits produce updates.
    let result = runtime.tick_once(world).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(3));
    let updates = runtime.drain(world, sub).unwrap();
    assert_eq!(updates.len(), 1);
    assert!(matches!(&updates[0], SubscriptionUpdate::Insert { .. }));
    let _ = std::fs::remove_dir_all(&dir);
}
