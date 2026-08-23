//! Phase 12 integration test (ADR-012 D6): partition durability and
//! recovery. Verifies that per-partition WAL persistence reconstructs
//! identical authoritative state, that re-registration re-attaches a
//! recovered world to the bus, and that recovered history is **never**
//! replayed as live subscription updates (Phase 8 semantics preserved).

use std::path::PathBuf;

use nexum_core::row;
use nexum_core::{ColumnType, PartitionId, ReducerId, SystemId, TickId, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{SimulationConfig, SystemDefinition, World};
use nexum_subscription::{Query, SubscriptionUpdate};
use nexum_table::TableStore;

fn ensure_ledger(store: &mut TableStore) {
    if store.table("ledger").is_none() {
        store
            .create_table(
                nexum_core::TableSchema::builder("ledger")
                    .column("id", ColumnType::U64)
                    .column("from", ColumnType::U64)
                    .column("to", ColumnType::U64)
                    .column("amount", ColumnType::I64)
                    .primary_key(&["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
}

/// One-way factory: world 0 sends one transfer per tick to partition 1;
/// world 1 registers the handler. Idempotent across recovery (tables created
/// only if absent).
fn one_way_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_ledger(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "transfer", |ctx, args| {
                        let amount = args.require_i64("amount")?;
                        let to = args.require_u64("to")?;
                        let from = args.require_u64("from")?;
                        let seq = args.require_u64("seq")?;
                        ctx.insert("ledger", row![seq, from, to, amount])?;
                        Ok(nexum_core::Value::U64(seq))
                    })
                    .unwrap(),
                )
                .unwrap();
            if id.as_u64() == 0 {
                world.add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                        let tick = ctx.tick().as_u64();
                        ctx.send_to(
                            PartitionId::from_u64(1),
                            "transfer",
                            ReducerArgs::new()
                                .insert("amount", 10i64)
                                .insert("to", 1u64)
                                .insert("from", ctx.partition().as_u64())
                                .insert("seq", tick),
                        )?;
                        Ok(())
                    })
                    .unwrap(),
                )?;
            }
            Ok(world)
        },
    )
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_pair(runtime: &mut Runtime) {
    runtime
        .create_world(WorldId::from_u64(0), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(0), WorldId::from_u64(0))
        .unwrap();
    runtime.start_world(WorldId::from_u64(0)).unwrap();
    runtime
        .create_world(WorldId::from_u64(1), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(1), WorldId::from_u64(1))
        .unwrap();
    runtime.start_world(WorldId::from_u64(1)).unwrap();
}

#[test]
fn recovered_partitions_resume_messaging_without_history_replay() {
    let dir = temp_dir("nexum-runtime-partition-recovery");
    let config = RuntimeConfig::new(one_way_factory())
        .with_persistence(PersistencePolicy::Flush, dir.clone());
    let mut runtime = Runtime::new(config).unwrap();
    start_pair(&mut runtime);

    // Three steps: world 1 commits rows for the tick-0 and tick-1 messages
    // (delivered at its ticks 1 and 2). The tick-2 message is in flight
    // (queued, undelivered) when the process "crashes".
    for _ in 0..3 {
        runtime.step().unwrap();
    }
    assert_eq!(runtime.metrics().messages_delivered, 2);
    runtime.shutdown().unwrap();
    drop(runtime);

    // Recover both worlds into a fresh runtime and re-attach the bus.
    let mut runtime = Runtime::new(
        RuntimeConfig::new(one_way_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    for world in [0u64, 1] {
        runtime
            .recover_world(
                WorldId::from_u64(world),
                SimulationConfig::new(),
                Some(TickId::from_u64(3)),
            )
            .unwrap();
        runtime
            .register_partition(PartitionId::from_u64(world), WorldId::from_u64(world))
            .unwrap();
    }
    runtime.start_world(WorldId::from_u64(0)).unwrap();
    runtime.start_world(WorldId::from_u64(1)).unwrap();

    // The recovered authoritative state is identical: exactly the two
    // pre-crash rows, delivered as the subscription's Initial snapshot —
    // never as replayed live updates.
    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    let updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    assert_eq!(updates.len(), 1, "exactly one Initial snapshot, no replay");
    match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => assert_eq!(rows.len(), 2),
        other => panic!("expected Initial, got {other:?}"),
    }

    // Resume: world 0 ticks from tick 3 and sends; the in-flight tick-2
    // message was runtime-transient and is gone; world 1 receives the new
    // tick-3 message at its tick 4 — one fresh Insert, nothing else.
    runtime.step().unwrap(); // both tick 3
    runtime.step().unwrap(); // world 1 tick 4 delivers the tick-3 message
    let updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    assert_eq!(updates.len(), 1, "one fresh Insert, no replayed history");
    match &updates[0] {
        SubscriptionUpdate::Insert { row, .. } => {
            assert_eq!(row.row().get(0).unwrap().as_u64().unwrap(), 3); // seq = 3
        }
        other => panic!("expected Insert, got {other:?}"),
    }

    // Per-partition durability: each world's WAL was appended independently.
    assert_eq!(runtime.metrics().wal_appends, 4); // 2 post-recovery ticks each
    let _ = std::fs::remove_dir_all(&dir);
}
