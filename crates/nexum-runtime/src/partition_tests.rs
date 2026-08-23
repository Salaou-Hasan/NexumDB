//! Phase 12 unit tests (ADR-012): the runtime's partition registry and the
//! deterministic cross-partition message bus — routing, one-logical-tick
//! delivery, worker-count independence, failure isolation, backpressure, and
//! external injection.

use nexum_core::row;
use nexum_core::{ColumnType, Error, PartitionId, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_simulation::{InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::{Query, SubscriptionUpdate};
use nexum_table::TableStore;

use crate::WorldFactory;
use crate::config::RuntimeConfig;
use crate::error::RuntimeError;
use crate::runtime::Runtime;

/// Creates the ledger table only if absent (recovered stores already carry
/// the authoritative schema, ADR-010 D5).
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

/// A factory whose worlds register the `transfer` handler and, per world, a
/// sender system that messages the next partition in the ring
/// (0 → 1 → 2 → 0). All capture-free: everything derives from the context.
fn mesh_factory() -> WorldFactory {
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
                        ctx.emit("transfer", amount)?;
                        Ok(Value::U64(seq))
                    })
                    .unwrap(),
                )
                .unwrap();
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                    let from = ctx.partition().as_u64();
                    let target = match from {
                        0 => 1,
                        1 => 2,
                        _ => 0,
                    };
                    let tick = ctx.tick().as_u64();
                    ctx.send_to(
                        PartitionId::from_u64(target),
                        "transfer",
                        ReducerArgs::new()
                            .insert("amount", 10i64)
                            .insert("to", target)
                            .insert("from", from)
                            .insert("seq", tick),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )?;
            Ok(world)
        },
    )
}

/// A one-way factory: only world 0 has a sender, messaging partition 1.
/// World 1 registers the accepting handler and nothing else — clean
/// count-based assertions for two-partition routing tests.
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
                        ctx.emit("transfer", amount)?;
                        Ok(Value::U64(seq))
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

/// A factory where partition 1's `transfer` handler rejects (all others
/// accept) and every world's sender messages partition 1 — except partition
/// 1 itself, which messages partition 0. For destination-failure tests.
fn rejecting_destination_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_ledger(&mut store);
            let mut world = World::new(id, store, sim)?;
            if id.as_u64() == 1 {
                world
                    .native_mut()
                    .register(
                        ReducerDefinition::new(ReducerId::from_u64(0), "transfer", |_ctx, _| {
                            Err(Error::invalid_argument("handler rejected the message"))
                        })
                        .unwrap(),
                    )
                    .unwrap();
            } else {
                world
                    .native_mut()
                    .register(
                        ReducerDefinition::new(ReducerId::from_u64(0), "transfer", |ctx, args| {
                            let amount = args.require_i64("amount")?;
                            let to = args.require_u64("to")?;
                            let from = args.require_u64("from")?;
                            let seq = args.require_u64("seq")?;
                            ctx.insert("ledger", row![seq, from, to, amount])?;
                            Ok(Value::U64(seq))
                        })
                        .unwrap(),
                    )
                    .unwrap();
            }
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                    let from = ctx.partition().as_u64();
                    let target = if from == 1 { 0 } else { 1 };
                    let tick = ctx.tick().as_u64();
                    ctx.send_to(
                        PartitionId::from_u64(target),
                        "transfer",
                        ReducerArgs::new()
                            .insert("amount", 1i64)
                            .insert("to", target)
                            .insert("from", from)
                            .insert("seq", tick),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )?;
            Ok(world)
        },
    )
}

fn start_partition(runtime: &mut Runtime, world: u64, partition: u64) {
    runtime
        .create_world(WorldId::from_u64(world), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(partition), WorldId::from_u64(world))
        .unwrap();
    runtime.start_world(WorldId::from_u64(world)).unwrap();
}

// -------------------------------------------------------------- lifecycle

#[test]
fn partition_registration_lifecycle() {
    let mut runtime = Runtime::new(RuntimeConfig::new(mesh_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_world(world, SimulationConfig::new())
        .unwrap();

    // Binding an unknown world is rejected; the world's default partition is
    // its raw world id.
    assert!(matches!(
        runtime.register_partition(PartitionId::from_u64(5), WorldId::from_u64(9)),
        Err(RuntimeError::UnknownWorld(_))
    ));
    runtime
        .register_partition(PartitionId::from_u64(7), world)
        .unwrap();
    assert_eq!(
        runtime
            .partition_status(PartitionId::from_u64(7))
            .unwrap()
            .world,
        world
    );
    // Duplicate registration is rejected.
    assert!(matches!(
        runtime.register_partition(PartitionId::from_u64(7), world),
        Err(RuntimeError::DuplicatePartition(_))
    ));
    // The world's partition id was stamped.
    assert!(runtime.world_status(world).unwrap().next_tick == TickId::from_u64(0));

    // The topology propagates to registered worlds.
    let other = WorldId::from_u64(1);
    runtime
        .create_world(other, SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(1), other)
        .unwrap();
    let ids: Vec<u64> = runtime.topology().map(|p| p.as_u64()).collect();
    assert_eq!(ids, vec![1, 7]);

    runtime
        .unregister_partition(PartitionId::from_u64(1))
        .unwrap();
    let ids: Vec<u64> = runtime.topology().map(|p| p.as_u64()).collect();
    assert_eq!(ids, vec![7]);
    assert!(matches!(
        runtime.partition_status(PartitionId::from_u64(1)),
        Err(RuntimeError::UnknownPartition(_))
    ));
    // Idempotent unregister.
    runtime
        .unregister_partition(PartitionId::from_u64(1))
        .unwrap();

    // Destroying the world unregisters its bound partition.
    runtime.destroy_world(world).unwrap();
    assert!(matches!(
        runtime.partition_status(PartitionId::from_u64(7)),
        Err(RuntimeError::UnknownPartition(_))
    ));
}

// ---------------------------------------------------------------- routing

#[test]
fn messages_are_delivered_with_one_logical_tick_of_latency() {
    let mut runtime = Runtime::new(RuntimeConfig::new(one_way_factory())).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);

    // Step 1: world 0's tick 0 sends one message; world 1's tick 0 sees none.
    let report = runtime.step().unwrap();
    assert_eq!(report.succeeded, 2);
    assert_eq!(report.failed, 0);
    assert_eq!(runtime.metrics().messages_sent, 1);
    assert_eq!(runtime.metrics().messages_delivered, 0);

    // Step 2: world 1's tick 1 receives the tick-0 message; the handler
    // commits one ledger row.
    runtime.step().unwrap();
    assert_eq!(runtime.metrics().messages_delivered, 1);
    assert_eq!(runtime.metrics().ticks_succeeded, 4);

    // Verify the handler's commit through the subscription boundary.
    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    let mut updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    match updates.remove(0) {
        SubscriptionUpdate::Initial { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].row().get(1).unwrap().as_u64().unwrap(), 0); // from
        }
        other => panic!("expected Initial, got {other:?}"),
    }
}

#[test]
fn external_messages_share_the_same_delivery_path() {
    let mut runtime = Runtime::new(RuntimeConfig::new(one_way_factory())).unwrap();
    // Register partition 0 but never start its world, so the only message in
    // flight is the externally injected one (world 0's own sender never runs).
    runtime
        .create_world(WorldId::from_u64(0), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(0), WorldId::from_u64(0))
        .unwrap();
    start_partition(&mut runtime, 1, 1);

    // External injection stamped with the sender's current logical tick.
    runtime
        .send_message(
            PartitionId::from_u64(0),
            PartitionId::from_u64(1),
            "transfer",
            ReducerArgs::new()
                .insert("amount", 99i64)
                .insert("to", 1u64)
                .insert("from", 0u64)
                .insert("seq", 777u64),
        )
        .unwrap();
    assert!(matches!(
        runtime.send_message(
            PartitionId::from_u64(0),
            PartitionId::from_u64(9),
            "x",
            ReducerArgs::new()
        ),
        Err(RuntimeError::UnknownPartition(_))
    ));

    runtime.step().unwrap(); // tick 0: queued, not yet delivered
    assert_eq!(runtime.metrics().messages_delivered, 0);
    runtime.step().unwrap(); // tick 1: delivered → handler commits
    assert_eq!(runtime.metrics().messages_delivered, 1);

    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    let updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => {
            assert_eq!(rows.len(), 1);
            assert_eq!(rows[0].row().get(3).unwrap().as_i64().unwrap(), 99);
        }
        other => panic!("expected Initial, got {other:?}"),
    }
}

// ----------------------------------------------------- delivery determinism

#[test]
fn delivery_order_is_deterministic_across_senders() {
    // A factory with three sender systems on world 0, all messaging
    // partition 1 with distinct kinds.
    let factory: WorldFactory = Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_ledger(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "record", |ctx, args| {
                        let from = args.require_u64("from")?;
                        let seq = args.require_u64("seq")?;
                        let kind = args.require_str("kind")?.to_string();
                        ctx.insert("ledger", row![seq, from, 1u64, seq as i64])?;
                        ctx.emit("recorded", kind)?;
                        Ok(Value::U64(seq))
                    })
                    .unwrap(),
                )
                .unwrap();
            if id.as_u64() == 0 {
                for i in 0..3u64 {
                    world
                        .add_system(
                            SystemDefinition::new(
                                SystemId::from_u64(i),
                                format!("sender_{i}"),
                                i as u32,
                                |ctx, _| {
                                    let i = ctx.system().as_u64();
                                    ctx.send_to(
                                        PartitionId::from_u64(1),
                                        "record",
                                        ReducerArgs::new()
                                            .insert("from", ctx.partition().as_u64())
                                            .insert("seq", ctx.tick().as_u64() * 10 + i)
                                            .insert("kind", format!("k{i}")),
                                    )?;
                                    Ok(())
                                },
                            )
                            .unwrap(),
                        )
                        .unwrap();
                }
            }
            Ok(world)
        },
    );
    let mut runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);

    runtime.step().unwrap(); // tick 0 sends 3
    assert_eq!(runtime.metrics().messages_sent, 3);
    runtime.step().unwrap(); // tick 1 delivers 3 in (sent_tick, from, seq) order
    assert_eq!(runtime.metrics().messages_delivered, 3);

    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    let updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => {
            // seqs = 0*10+0, 0*10+1, 0*10+2 → ascending by delivery order.
            let seqs: Vec<i64> = rows
                .iter()
                .map(|r| r.row().get(3).unwrap().as_i64().unwrap())
                .collect();
            assert_eq!(seqs, vec![0, 1, 2]);
        }
        other => panic!("expected Initial, got {other:?}"),
    }
}

// ------------------------------------------- worker-count independence

/// Runs `steps` steps on a 3-partition ring and returns each world's
/// per-tick committed change traces (through `step_detailed`, the committed
/// boundary).
fn ring_traces(workers: usize, steps: u64) -> Vec<Vec<Vec<nexum_storage::Change>>> {
    let config = RuntimeConfig::new(mesh_factory()).with_worker_count(workers);
    let mut runtime = Runtime::new(config).unwrap();
    for p in 0..3u64 {
        start_partition(&mut runtime, p, p);
    }
    let mut traces = vec![Vec::new(); 3];
    for _ in 0..steps {
        for (world, result) in runtime.step_detailed().unwrap() {
            traces[world.as_u64() as usize].push(result.changes().to_vec());
        }
    }
    traces
}

#[test]
fn worker_count_never_changes_partition_traces() {
    // 1 worker: (world 0, 1, 2). 2 workers: worker 0 owns worlds {0, 2},
    // worker 1 owns {1} → tick order (0, 2, 1). The delivery phase makes the
    // per-world traces identical regardless.
    assert_eq!(ring_traces(1, 6), ring_traces(2, 6));
    assert_eq!(ring_traces(2, 6), ring_traces(4, 6));
    // Sanity: after 6 steps each world committed exactly 5 handler rows
    // (its own sends arrive one tick later) — the ring is self-sustaining.
    let traces = ring_traces(1, 6);
    for (world, trace) in traces.iter().enumerate() {
        let rows: usize = trace.iter().map(|c| c.len()).sum();
        assert_eq!(rows, 5, "world {world} handler rows after 6 steps");
    }
}

#[test]
fn repeated_runs_with_identical_setup_are_identical() {
    assert_eq!(ring_traces(2, 5), ring_traces(2, 5));
}

// ------------------------------------------------------- failure semantics

#[test]
fn failed_handler_aborts_the_destination_tick_atomically() {
    let mut runtime = Runtime::new(RuntimeConfig::new(rejecting_destination_factory())).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);
    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    runtime.drain(WorldId::from_u64(1), sub).unwrap(); // Initial (empty)

    // Tick 0: world 0 sends; world 1 has no messages → both succeed.
    let report = runtime.step().unwrap();
    assert_eq!(report.succeeded, 2);
    assert_eq!(report.failed, 0);

    // Tick 1: world 1's handler rejects the delivered message → its tick
    // fails (FailWorld policy); world 0 keeps running.
    let report = runtime.step().unwrap();
    assert_eq!(report.succeeded, 1);
    assert_eq!(report.failed, 1);
    assert_eq!(
        runtime.world_status(WorldId::from_u64(1)).unwrap().state,
        crate::WorldLifecycle::Failed
    );
    // The failed tick produced zero subscription updates.
    assert!(runtime.drain(WorldId::from_u64(1), sub).unwrap().is_empty());
    // The sender's tick committed (its own state is fine).
    assert_eq!(
        runtime
            .world_status(WorldId::from_u64(0))
            .unwrap()
            .next_tick,
        TickId::from_u64(2)
    );
}

#[test]
fn sender_to_unknown_partition_fails_its_tick_atomically() {
    // Register only partition 0: world 0's topology is empty, so its sender
    // system cannot send to partition 1 → its tick fails deterministically.
    let mut runtime = Runtime::new(RuntimeConfig::new(mesh_factory())).unwrap();
    runtime
        .create_world(WorldId::from_u64(0), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(0), WorldId::from_u64(0))
        .unwrap();
    runtime.start_world(WorldId::from_u64(0)).unwrap();

    let report = runtime.step().unwrap();
    assert_eq!(report.failed, 1);
    let failed = runtime
        .drain_events()
        .into_iter()
        .any(|e| matches!(e, crate::RuntimeEvent::TickFailed { .. }));
    assert!(failed);
}

#[test]
fn partition_failure_is_isolated() {
    // World 1 (partition 1) rejects; worlds 0 and 2 feed it and keep
    // ticking regardless.
    let mut runtime = Runtime::new(RuntimeConfig::new(rejecting_destination_factory())).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);
    start_partition(&mut runtime, 2, 2);

    runtime.step().unwrap(); // all tick 0
    runtime.step().unwrap(); // world 1 fails at tick 1

    assert_eq!(
        runtime.world_status(WorldId::from_u64(1)).unwrap().state,
        crate::WorldLifecycle::Failed
    );
    // Worlds 0 and 2 are still running and keep ticking.
    let report = runtime.step().unwrap();
    assert_eq!(report.worlds, 2);
    assert_eq!(report.succeeded, 2);
    assert_eq!(
        runtime
            .world_status(WorldId::from_u64(2))
            .unwrap()
            .next_tick,
        TickId::from_u64(3)
    );
}

#[test]
fn worker_reassignment_preserves_routing() {
    let config = RuntimeConfig::new(one_way_factory()).with_worker_count(2);
    let mut runtime = Runtime::new(config).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);

    // Move world 0 to worker 1; its messages must still reach partition 1.
    runtime
        .reassign_world(WorldId::from_u64(0), nexum_core::WorkerId::from_u64(1))
        .unwrap();
    assert_eq!(
        runtime
            .partition_status(PartitionId::from_u64(0))
            .unwrap()
            .worker
            .as_u64(),
        1
    );

    runtime.step().unwrap();
    runtime.step().unwrap();
    assert_eq!(runtime.metrics().messages_delivered, 1);
}

// ----------------------------------------------------------- backpressure

#[test]
fn inbound_queue_overflow_drops_deterministically() {
    let config = RuntimeConfig::new(mesh_factory()).with_max_queued_partition_messages(2);
    let mut runtime = Runtime::new(config).unwrap();
    start_partition(&mut runtime, 0, 0);
    // Partition 1's world is created but never started: its inbound queue
    // fills and overflows while world 0 keeps sending.
    runtime
        .create_world(WorldId::from_u64(1), SimulationConfig::new())
        .unwrap();
    runtime
        .register_partition(PartitionId::from_u64(1), WorldId::from_u64(1))
        .unwrap();

    runtime.step().unwrap(); // 1 queued
    runtime.step().unwrap(); // 2 queued (cap reached)
    runtime.step().unwrap(); // 3rd dropped
    assert_eq!(runtime.metrics().messages_sent, 2);
    assert_eq!(runtime.metrics().messages_dropped, 1);
    let dropped = runtime
        .drain_events()
        .into_iter()
        .filter(|e| matches!(e, crate::RuntimeEvent::MessageDropped { .. }))
        .count();
    assert_eq!(dropped, 1);
    // The sender's ticks were never blocked.
    assert_eq!(
        runtime
            .world_status(WorldId::from_u64(0))
            .unwrap()
            .next_tick,
        TickId::from_u64(3)
    );
}

#[test]
fn stopping_a_partition_accumulates_messages_for_resume() {
    let mut runtime = Runtime::new(RuntimeConfig::new(one_way_factory())).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);
    runtime.stop_world(WorldId::from_u64(1)).unwrap();

    runtime.step().unwrap(); // world 0 sends at tick 0; world 1 stopped
    runtime.step().unwrap(); // world 0 sends at tick 1; world 1 still stopped
    assert_eq!(runtime.metrics().messages_sent, 2);
    assert_eq!(runtime.metrics().messages_delivered, 0);

    // Resume: world 1 never ticked, so it resumes at tick 0; the messages
    // sent at ticks 0 and 1 are delivered when its ticks reach 1 and 2
    // (sent_tick < current). Each queued message is delivered exactly once.
    runtime.start_world(WorldId::from_u64(1)).unwrap();
    runtime.step().unwrap(); // world 1 tick 0: nothing due
    runtime.step().unwrap(); // world 1 tick 1: delivers the tick-0 message
    runtime.step().unwrap(); // world 1 tick 2: delivers the tick-1 message
    assert_eq!(runtime.metrics().messages_delivered, 2);
    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("ledger").build().unwrap(),
        )
        .unwrap();
    let updates = runtime.drain(WorldId::from_u64(1), sub).unwrap();
    match &updates[0] {
        SubscriptionUpdate::Initial { rows, .. } => assert_eq!(rows.len(), 2),
        other => panic!("expected Initial, got {other:?}"),
    }
}

// ------------------------------------------------------------- input frames

#[test]
fn messages_and_input_frames_coexist_in_one_tick() {
    // World 0's sender is unconditional; add client input on world 1 to prove
    // frames and messages share a tick's transaction.
    let mut runtime = Runtime::new(RuntimeConfig::new(one_way_factory())).unwrap();
    start_partition(&mut runtime, 0, 0);
    start_partition(&mut runtime, 1, 1);

    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(nexum_simulation::InputCommand::simple(7, "hello").unwrap());
    runtime.submit_input(WorldId::from_u64(1), frame).unwrap();
    runtime.step().unwrap(); // world 1 tick 0: frame only
    runtime.step().unwrap(); // world 1 tick 1: frame (empty) + delivered msg
    assert_eq!(runtime.metrics().messages_delivered, 1);
    assert_eq!(runtime.metrics().inputs_accepted, 1);
}
