//! Cross-crate seam: `World::tick` → Transaction/OCC → `Vec<Change>` →
//! `SubscriptionRegistry` deltas.
//!
//! Proves that simulation ticks produce the exact change stream the
//! subscription engine consumes, that every committed change is delivered
//! to a matching subscription exactly once, and that committed state is
//! deterministic regardless of system registration order.

use nexum_core::{ColumnType, Row, SystemId, TableSchema, TickId, Value, WorldId};
use nexum_simulation::{
    ExecutionMode, InputCommand, InputFrame, SimulationConfig, SystemDefinition, World,
};
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::TableStore;

fn config(execution: ExecutionMode) -> SimulationConfig {
    SimulationConfig::new()
        .with_seed(42)
        .with_execution(execution)
}

/// A world whose spawner system inserts one `players` row per `spawn`
/// command and whose healer system buffs every row by 5 each tick (capped
/// at 150). Systems keep fixed `(priority, id)`; only the *registration*
/// order can be swapped.
fn world(id: u64, execution: ExecutionMode, spawner_first: bool) -> World {
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
    let mut world = World::new(WorldId::from_u64(id), store, config(execution)).unwrap();

    let spawner = || {
        SystemDefinition::new(SystemId::from_u64(0), "spawner", 0, |ctx, frame| {
            for command in frame.commands() {
                if command.kind() == "spawn" {
                    let id = command.payload().and_then(Value::as_u64).unwrap_or(0);
                    ctx.insert("players", nexum_core::row![id, 10u64, 100i32])?;
                }
            }
            Ok(())
        })
        .unwrap()
    };
    let healer = || {
        SystemDefinition::new(SystemId::from_u64(1), "healer", 10, |ctx, _| {
            for (row_id, row) in ctx.scan("players")? {
                let health = row.get(2).and_then(Value::as_i32).unwrap_or(0);
                let mut values = row.into_values();
                values[2] = Value::I32((health + 5).min(150));
                ctx.update("players", row_id, Row::new(values))?;
            }
            Ok(())
        })
        .unwrap()
    };

    if spawner_first {
        world.add_system(spawner()).unwrap();
        world.add_system(healer()).unwrap();
    } else {
        world.add_system(healer()).unwrap();
        world.add_system(spawner()).unwrap();
    }
    world
}

fn frame(tick: u64, ids: &[u64]) -> InputFrame {
    let mut f = InputFrame::new(TickId::from_u64(tick));
    for id in ids {
        f.push(InputCommand::new(*id, "spawn", Some(Value::U64(*id))).unwrap());
    }
    f
}

#[test]
fn ticks_feed_subscriptions_and_the_view_matches_authoritative_state() {
    let mut world = world(7, ExecutionMode::Serial, true);
    let mut registry = SubscriptionRegistry::new();
    let sub = registry
        .subscribe(world.store(), Query::builder("players").build().unwrap())
        .unwrap();
    let boot = registry.drain(sub).unwrap();
    assert!(
        boot.iter()
            .all(|u| matches!(u, SubscriptionUpdate::Initial { rows, .. } if rows.is_empty())),
        "empty store establishes an empty initial view"
    );

    // Every committed change on the subscribed table is delivered exactly
    // once (the spawner's insert coalesces with the same-tick heal into a
    // single Insert carrying the healed value).
    for tick in 0..3u64 {
        let result = world.tick(&frame(tick, &[tick + 1])).unwrap();
        assert!(!result.changes().is_empty(), "tick {tick} commits changes");
        registry.apply_changes(world.store(), result.changes());
        let updates = registry.drain(sub).unwrap();
        assert_eq!(
            updates.len(),
            result.changes().len(),
            "one delta per committed change"
        );
    }
    assert_eq!(world.tick_number(), TickId::from_u64(3));

    // The derived view matches the authoritative scan: one Initial view of
    // three rows, all healed once (spawn at 100 → heal to 105 same tick).
    let fresh = registry
        .subscribe(world.store(), Query::builder("players").build().unwrap())
        .unwrap();
    let snapshot = registry.drain(fresh).unwrap();
    assert_eq!(snapshot.len(), 1, "one Initial view");
    let SubscriptionUpdate::Initial { rows, .. } = &snapshot[0] else {
        panic!("a fresh subscription establishes with an Initial view");
    };
    assert_eq!(rows.len(), 3, "three spawned rows");
    // A row spawned on tick t receives a heal on every tick from t to 2:
    // 100 + 5 * (number of ticks it existed through tick 2).
    let healed_health = |id: u64| Value::I32(100 + 5 * (3 - id) as i32);
    for delivered in rows {
        let id = delivered.row_id();
        assert_eq!(
            delivered.row().values()[2],
            healed_health(id.as_u64()),
            "row {id:?} carries every heal up to tick 2"
        );
    }
}

#[test]
fn identical_inputs_produce_identical_state_across_registration_order() {
    let mut a = world(1, ExecutionMode::Serial, true);
    let mut b = world(2, ExecutionMode::Serial, false);

    for tick in 0..4u64 {
        let inputs = frame(tick, &[100 + tick]);
        let result_a = a.tick(&inputs).unwrap();
        let result_b = b.tick(&inputs).unwrap();
        assert_eq!(
            result_a.changes().len(),
            result_b.changes().len(),
            "identical commit shape"
        );
    }

    let dump = |w: &World| {
        let mut rows: Vec<_> = w
            .store()
            .table("players")
            .unwrap()
            .scan()
            .map(|(id, r)| (id, r.clone()))
            .collect();
        rows.sort_by_key(|(id, _)| *id);
        rows
    };
    assert_eq!(
        dump(&a),
        dump(&b),
        "state independent of registration order"
    );
}
