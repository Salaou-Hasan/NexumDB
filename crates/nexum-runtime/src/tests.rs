//! Phase 10 unit tests (ADR-010).

use std::path::PathBuf;

use nexum_core::row;
use nexum_core::{ColumnType, Error, PartitionId, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_execution::{
    InputCommand, InputFrame, Partition, PartitionConfig, PartitionMessage, SystemDefinition,
};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_subscription::{Query, SubscriptionUpdate};
use nexum_table::TableStore;

use crate::config::{PartitionFactory, PersistencePolicy, RuntimeConfig, TickFailurePolicy};
use crate::error::RuntimeError;
use crate::runtime::{Runtime, RuntimeState};
use crate::worker::WorkerState;
use crate::world::PartitionLifecycle;

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

/// Creates the players table only if absent — recovered stores already
/// contain the authoritative schema (ADR-010 D5).
fn ensure_players(store: &mut TableStore) {
    if store.table("players").is_none() {
        players_table(store);
    }
}

/// A factory whose worlds run a single writer system (one player per tick).
fn writer_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
        world.add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                let tick = ctx.tick().as_u64();
                ctx.insert("players", row![tick, 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )?;
        Ok(world)
    })
}

/// A factory where world 1 additionally runs a failing system (after the
/// writer), for failure-isolation tests.
fn failing_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
        world.add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )?;
        if id.as_u64() == 1 {
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(1), "fails", 10, |_ctx, _| {
                    Err(Error::invalid_argument("boom"))
                })
                .unwrap(),
            )?;
        }
        Ok(world)
    })
}

/// A factory whose system consumes input commands as player rows.
fn input_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
        world.add_system(
            SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                for command in frame.commands() {
                    if command.kind() == "spawn" {
                        let id = command.payload().and_then(Value::as_u64).unwrap();
                        ctx.insert("players", row![id, 10u64, 100i32])?;
                    }
                }
                Ok(())
            })
            .unwrap(),
        )?;
        Ok(world)
    })
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn start_worlds(runtime: &mut Runtime, ids: &[u64]) {
    for id in ids {
        runtime
            .create_partition(WorldId::from_u64(*id), PartitionConfig::new())
            .unwrap();
        runtime.start_partition(WorldId::from_u64(*id)).unwrap();
    }
}

// -------------------------------------------------------------- lifecycle

#[test]
fn world_lifecycle_transitions() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();

    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Created
    );
    runtime.start_partition(world).unwrap();
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Running
    );
    runtime.stop_partition(world).unwrap();
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Stopped
    );
    // Restart continues logical time.
    runtime.start_partition(world).unwrap();
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Running
    );

    // Idempotent start/stop.
    runtime.start_partition(world).unwrap();
    runtime.stop_partition(world).unwrap();
    runtime.stop_partition(world).unwrap();
}

#[test]
fn duplicate_and_unknown_worlds() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    assert!(matches!(
        runtime.create_partition(world, PartitionConfig::new()),
        Err(RuntimeError::DuplicatePartition(_))
    ));
    assert!(matches!(
        runtime.partition_status(WorldId::from_u64(9)),
        Err(RuntimeError::UnknownPartition(_))
    ));
    assert!(matches!(
        runtime.start_partition(WorldId::from_u64(9)),
        Err(RuntimeError::UnknownPartition(_))
    ));

    runtime.destroy_partition(world).unwrap();
    assert!(matches!(
        runtime.partition_status(world),
        Err(RuntimeError::UnknownPartition(_))
    ));
}

#[test]
fn stopping_a_created_world_is_allowed_but_starting_a_failed_one_is_not() {
    let mut runtime = Runtime::new(RuntimeConfig::new(failing_factory())).unwrap();
    let world = WorldId::from_u64(1);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.stop_partition(world).unwrap(); // Created -> Stopped

    runtime.start_partition(world).unwrap();
    runtime.step().unwrap(); // world 1 fails
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Failed
    );
    assert!(matches!(
        runtime.start_partition(world),
        Err(RuntimeError::InvalidPartitionState { .. })
    ));
}

#[test]
fn step_ticks_every_running_world_once_in_deterministic_order() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    // Create in a scrambled order; execution must be by world id.
    start_worlds(&mut runtime, &[2, 0, 1]);

    let report = runtime.step().unwrap();
    assert_eq!(report.worlds, 3);
    assert_eq!(report.ticks, 3);
    assert_eq!(report.succeeded, 3);
    assert_eq!(report.failed, 0);

    let ticked: Vec<u64> = runtime
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            crate::RuntimeEvent::TickCompleted { world, .. } => Some(world.as_u64()),
            _ => None,
        })
        .collect();
    assert_eq!(ticked, vec![0, 1, 2]);

    // Stopped worlds are not ticked.
    runtime.stop_partition(WorldId::from_u64(1)).unwrap();
    let report = runtime.step().unwrap();
    assert_eq!(report.worlds, 2);
    assert_eq!(report.ticks, 2);
}

#[test]
fn step_detailed_returns_every_successful_world_s_result() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    start_worlds(&mut runtime, &[1, 0]);

    let results = runtime.step_detailed().unwrap();
    // Deterministic (world-id) order, one committed result per world.
    let ids: Vec<u64> = results.iter().map(|(world, _)| world.as_u64()).collect();
    assert_eq!(ids, vec![0, 1]);
    for (_, result) in &results {
        assert_eq!(result.tick(), TickId::from_u64(0));
        assert_eq!(result.changes().len(), 1);
    }

    // A failed world is excluded from the results but still recorded.
    let mut runtime = Runtime::new(RuntimeConfig::new(failing_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1]);
    let results = runtime.step_detailed().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].0, WorldId::from_u64(0));
}

#[test]
fn tick_once_ticks_a_single_world() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1]);
    let result = runtime.tick_once(WorldId::from_u64(0)).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(0));
    assert_eq!(result.changes().len(), 1);
    // Partition 1 was not ticked.
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(1))
            .unwrap()
            .next_tick,
        TickId::from_u64(0)
    );
}

// ------------------------------------------------------------- ownership

#[test]
fn worlds_are_assigned_round_robin_and_reassignable() {
    let config = RuntimeConfig::new(writer_factory()).with_worker_count(3);
    let mut runtime = Runtime::new(config).unwrap();
    start_worlds(&mut runtime, &[0, 1, 2, 3, 4]);

    let assigned: Vec<u64> = (0..5)
        .map(|w| {
            runtime
                .assigned_worker(WorldId::from_u64(w))
                .unwrap()
                .as_u64()
        })
        .collect();
    assert_eq!(assigned, vec![0, 1, 2, 0, 1]);

    // Reassign world 0 from worker 0 to worker 2.
    runtime
        .reassign_partition(WorldId::from_u64(0), worker_id2())
        .unwrap();
    assert_eq!(
        runtime
            .assigned_worker(WorldId::from_u64(0))
            .unwrap()
            .as_u64(),
        2
    );
    let worker2 = runtime.worker_status(worker_id2()).unwrap();
    assert_eq!(
        worker2.worlds,
        vec![WorldId::from_u64(0), WorldId::from_u64(2)]
    );
    let worker0 = runtime.worker_status(worker_id0()).unwrap();
    assert_eq!(worker0.worlds, vec![WorldId::from_u64(3)]);

    // Reassign to a failed worker is rejected.
    runtime.fail_worker(worker_id0()).unwrap();
    assert!(matches!(
        runtime.reassign_partition(WorldId::from_u64(3), worker_id0()),
        Err(RuntimeError::InvalidWorkerState { .. })
    ));
}

fn worker_id0() -> nexum_core::WorkerId {
    nexum_core::WorkerId::from_u64(0)
}

fn worker_id2() -> nexum_core::WorkerId {
    nexum_core::WorkerId::from_u64(2)
}

#[test]
fn new_worlds_skip_failed_workers() {
    let config = RuntimeConfig::new(writer_factory()).with_worker_count(2);
    let mut runtime = Runtime::new(config).unwrap();
    start_worlds(&mut runtime, &[0, 1]); // workers: 0, 1
    runtime
        .fail_worker(nexum_core::WorkerId::from_u64(0))
        .unwrap();

    // A new world must not be assigned to the failed worker.
    runtime
        .create_partition(WorldId::from_u64(2), PartitionConfig::new())
        .unwrap();
    assert_eq!(
        runtime
            .assigned_worker(WorldId::from_u64(2))
            .unwrap()
            .as_u64(),
        1
    );

    // With every worker failed, creation is rejected outright.
    runtime
        .fail_worker(nexum_core::WorkerId::from_u64(1))
        .unwrap();
    assert!(matches!(
        runtime.create_partition(WorldId::from_u64(3), PartitionConfig::new()),
        Err(RuntimeError::Internal(_))
    ));
}

#[test]
fn create_world_over_an_existing_wal_is_rejected() {
    let dir = temp_dir("nexum-runtime-wal-reject");
    let world = WorldId::from_u64(0);
    let mut runtime = Runtime::new(
        RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();

    // First world commits one durable transaction, then is destroyed.
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();
    runtime.step().unwrap();
    // destroy_partition leaves the durable WAL intact...
    runtime.destroy_partition(world).unwrap();

    // ...so a fresh create_partition for the same id must not wipe it.
    assert!(matches!(
        runtime.create_partition(world, PartitionConfig::new()),
        Err(RuntimeError::Persistence(Error::AlreadyExists(_)))
    ));
    // recover_partition restores the durable state instead.
    let report = runtime
        .recover_partition(world, PartitionConfig::new(), Some(TickId::from_u64(1)))
        .unwrap();
    assert_eq!(report.replayed_txs, 1);
    runtime.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fail_worker_isolates_its_worlds() {
    let config = RuntimeConfig::new(writer_factory()).with_worker_count(2);
    let mut runtime = Runtime::new(config).unwrap();
    start_worlds(&mut runtime, &[0, 1, 2]); // workers: 0,1,0

    runtime.fail_worker(worker_id0()).unwrap();
    assert_eq!(
        runtime.worker_status(worker_id0()).unwrap().state,
        WorkerState::Failed
    );
    // Worker 0's worlds are failed and recoverable...
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(0))
            .unwrap()
            .state,
        PartitionLifecycle::Failed
    );
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(2))
            .unwrap()
            .state,
        PartitionLifecycle::Failed
    );
    // ...while worker 1's world is untouched.
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(1))
            .unwrap()
            .state,
        PartitionLifecycle::Running
    );

    // A step only ticks worker 1's world.
    let report = runtime.step().unwrap();
    assert_eq!(report.worlds, 1);
    assert_eq!(report.ticks, 1);
}

// ---------------------------------------------------------------- inputs

#[test]
fn inputs_are_routed_and_consumed_in_order() {
    let mut runtime = Runtime::new(RuntimeConfig::new(input_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(7, "spawn", Some(Value::U64(1))).unwrap());
    frame.push(InputCommand::new(7, "spawn", Some(Value::U64(2))).unwrap());
    runtime.submit_input(world, frame).unwrap();

    runtime.step().unwrap();
    assert_eq!(runtime.metrics().inputs_accepted, 1);
    // Two commands -> two rows.
    assert_eq!(runtime.metrics().ticks_succeeded, 1);
    let status = runtime.partition_status(world).unwrap();
    assert_eq!(status.next_tick, TickId::from_u64(1));
}

#[test]
fn late_and_over_limit_inputs_are_rejected() {
    let config = RuntimeConfig::new(input_factory()).with_max_queued_inputs(1);
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    // Queue bound: second frame for the same tick is rejected (capacity).
    runtime
        .submit_input(world, InputFrame::new(TickId::from_u64(0)))
        .unwrap();
    assert!(matches!(
        runtime.submit_input(world, InputFrame::new(TickId::from_u64(0))),
        Err(RuntimeError::InputRejected {
            reason: Error::Capacity(_),
            ..
        })
    ));

    // After the tick, a frame for the already-passed tick is late.
    runtime.step().unwrap();
    assert!(matches!(
        runtime.submit_input(world, InputFrame::new(TickId::from_u64(0))),
        Err(RuntimeError::InputRejected {
            reason: Error::InvalidArgument(_),
            ..
        })
    ));
    assert_eq!(runtime.metrics().inputs_rejected, 2);
}

#[test]
fn inputs_to_unknown_or_stopped_worlds_are_rejected() {
    let mut runtime = Runtime::new(RuntimeConfig::new(input_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.stop_partition(world).unwrap();

    assert!(matches!(
        runtime.submit_input(WorldId::from_u64(9), InputFrame::new(TickId::from_u64(0))),
        Err(RuntimeError::UnknownPartition(_))
    ));
    assert!(matches!(
        runtime.submit_input(world, InputFrame::new(TickId::from_u64(0))),
        Err(RuntimeError::InvalidPartitionState { .. })
    ));
}

#[test]
fn out_of_order_frames_fail_the_tick_deterministically() {
    let config =
        RuntimeConfig::new(writer_factory()).with_tick_failure_policy(TickFailurePolicy::Continue);
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    // A frame for tick 5 is accepted (not yet passed) but the world is at
    // tick 0: the world's own gate rejects it at execution.
    runtime
        .submit_input(world, InputFrame::new(TickId::from_u64(5)))
        .unwrap();
    let report = runtime.step().unwrap();
    assert_eq!(report.failed, 1);
    // Continue policy: the world keeps running. The gate rejects the bad
    // frame before consuming logical time, so the next (empty) frame for
    // tick 0 succeeds.
    let result = runtime.tick_once(world).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(0));
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Running
    );
}

// ------------------------------------------------------------ persistence

#[test]
fn wal_append_and_recovery_restores_world_state() {
    let dir = temp_dir("nexum-runtime-recover");
    let config = RuntimeConfig::new(writer_factory())
        .with_persistence(PersistencePolicy::Flush, dir.clone());
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();
    for _ in 0..3 {
        runtime.step().unwrap();
    }
    assert_eq!(runtime.metrics().wal_appends, 3);
    runtime.shutdown().unwrap();
    drop(runtime);

    // Recover into a fresh runtime and verify the state via a subscription
    // (the runtime never exposes the store directly).
    let mut runtime = Runtime::new(
        RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let report = runtime
        .recover_partition(world, PartitionConfig::new(), Some(TickId::from_u64(3)))
        .unwrap();
    assert_eq!(report.replayed_txs, 3);
    assert_eq!(
        runtime.partition_status(world).unwrap().next_tick,
        TickId::from_u64(3)
    );

    let sub = runtime
        .subscribe(world, Query::builder("players").build().unwrap())
        .unwrap();
    let initial = runtime.drain(world, sub).unwrap();
    assert_eq!(initial.len(), 1); // the Initial snapshot
    match &initial[0] {
        SubscriptionUpdate::Initial { rows, .. } => assert_eq!(rows.len(), 3),
        other => panic!("expected Initial, got {other:?}"),
    }

    // Resume ticking from tick 3.
    runtime.start_partition(world).unwrap();
    let report = runtime.step().unwrap();
    assert_eq!(report.succeeded, 1);
    assert_eq!(
        runtime.partition_status(world).unwrap().next_tick,
        TickId::from_u64(4)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn recovery_requires_persistence_and_an_existing_wal() {
    let dir = temp_dir("nexum-runtime-norecover");
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    assert!(matches!(
        runtime.recover_partition(WorldId::from_u64(0), PartitionConfig::new(), None),
        Err(RuntimeError::Persistence(Error::Unsupported(_)))
    ));
    drop(runtime);

    // Persistence enabled but no WAL exists for the world yet.
    let mut runtime = Runtime::new(
        RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    assert!(matches!(
        runtime.recover_partition(WorldId::from_u64(0), PartitionConfig::new(), None),
        Err(RuntimeError::Persistence(Error::NotFound(_)))
    ));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn periodic_snapshots_are_written_and_used_by_recovery() {
    let dir = temp_dir("nexum-runtime-snapshot");
    let config = RuntimeConfig::new(writer_factory())
        .with_persistence(PersistencePolicy::Flush, dir.clone())
        .with_snapshot_interval(2);
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();
    for _ in 0..4 {
        runtime.step().unwrap();
    }
    assert!(runtime.metrics().snapshots >= 2);
    runtime.shutdown().unwrap();
    drop(runtime);

    let mut runtime = Runtime::new(
        RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let report = runtime
        .recover_partition(world, PartitionConfig::new(), Some(TickId::from_u64(4)))
        .unwrap();
    // The snapshot at the end covers everything; the WAL replay is empty.
    assert!(report.snapshot.is_some());
    assert_eq!(report.replayed_txs, 0);
    let _ = std::fs::remove_dir_all(&dir);
}

// --------------------------------------------------------- subscriptions

#[test]
fn subscriptions_are_isolated_per_world() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1]);

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
    runtime.drain(WorldId::from_u64(1), sub_b).unwrap();

    runtime.step().unwrap();

    let updates_a = runtime.drain(WorldId::from_u64(0), sub_a).unwrap();
    let updates_b = runtime.drain(WorldId::from_u64(1), sub_b).unwrap();
    assert_eq!(updates_a.len(), 1);
    assert_eq!(updates_b.len(), 1);
    // Both worlds' row ids start at zero but are independent partitions.
    assert!(
        matches!(&updates_a[0], SubscriptionUpdate::Insert { row, .. } if row.row_id().as_u64() == 0)
    );
    assert!(
        matches!(&updates_b[0], SubscriptionUpdate::Insert { row, .. } if row.row_id().as_u64() == 0)
    );
}

#[test]
fn failed_ticks_produce_zero_subscription_updates() {
    let mut runtime = Runtime::new(RuntimeConfig::new(failing_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1]);
    // Subscribe to both worlds before the step (the initial snapshot is
    // drained and discarded; only subsequent commits produce updates).
    let sub = runtime
        .subscribe(
            WorldId::from_u64(1),
            Query::builder("players").build().unwrap(),
        )
        .unwrap();
    runtime.drain(WorldId::from_u64(1), sub).unwrap();
    let sub0 = runtime
        .subscribe(
            WorldId::from_u64(0),
            Query::builder("players").build().unwrap(),
        )
        .unwrap();
    runtime.drain(WorldId::from_u64(0), sub0).unwrap();

    let report = runtime.step().unwrap();
    assert_eq!(report.failed, 1);
    // The failed world's tick produced zero committed changes.
    assert!(runtime.drain(WorldId::from_u64(1), sub).unwrap().is_empty());
    // The healthy world's subscription saw its commit.
    let updates = runtime.drain(WorldId::from_u64(0), sub0).unwrap();
    assert_eq!(updates.len(), 1);
}

/// A factory whose writer system commits one row on **every other tick**
/// (the in-between ticks commit zero changes).
fn every_other_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
        world.add_system(
            SystemDefinition::new(SystemId::from_u64(0), "every-other-writer", 0, |ctx, _| {
                if ctx.tick().as_u64() % 2 == 0 {
                    ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
                }
                Ok(())
            })
            .unwrap(),
        )?;
        Ok(world)
    })
}

#[test]
fn empty_change_ticks_do_not_break_subscription_sequences() {
    // Regression (playable-game demo): a tick that commits **zero changes**
    // must not consume a subscription sequence number. Feeding the registry
    // an empty change set assigns a phantom sequence that no subscription can
    // observe; the next real delta then looks like a gap to every client view
    // and is dropped as a `ViewGap` — losing real committed updates.
    let mut runtime = Runtime::new(RuntimeConfig::new(every_other_factory())).unwrap();
    start_worlds(&mut runtime, &[0]);
    let sub = runtime
        .subscribe(
            WorldId::from_u64(0),
            Query::builder("players").build().unwrap(),
        )
        .unwrap();
    runtime.drain(WorldId::from_u64(0), sub).unwrap(); // Initial

    // Ticks 0,1,2,3: rows commit on 0 and 2, nothing on 1 and 3.
    for _ in 0..4 {
        runtime.step().unwrap();
    }
    let updates = runtime.drain(WorldId::from_u64(0), sub).unwrap();
    // Two commits, two deltas, with **contiguous** sequences (0, 1) — the
    // empty ticks produced no phantom sequences.
    assert_eq!(updates.len(), 2, "only the two change commits emit deltas");
    let seqs: Vec<u64> = updates
        .iter()
        .map(|update| match update {
            SubscriptionUpdate::Insert { seq, .. } => *seq,
            other => panic!("expected Insert, got {other:?}"),
        })
        .collect();
    assert_eq!(seqs, vec![0, 1], "contiguous sequences, no phantom gap");
}

// --------------------------------------------------------- reducer calls

/// The `echo` reducer: returns its `v` argument (used to observe per-call
/// execution order through `TickResult.reducer_results`).
fn echo(
    _ctx: &mut nexum_reducer::ReducerContext,
    args: &nexum_reducer::ReducerArgs,
) -> nexum_core::Result<Value> {
    Ok(args.get("v").cloned().unwrap_or(Value::U64(0)))
}

/// A factory whose system fails on tick 0 (then succeeds), registering the
/// `echo` reducer — for failed-tick call-recovery tests.
fn continue_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
        world
            .native_mut()
            .register(
                nexum_reducer::ReducerDefinition::new(
                    nexum_core::ReducerId::from_u64(1),
                    "echo",
                    echo,
                )
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "flaky", 0, |ctx, _| {
                    if ctx.tick().as_u64() == 0 {
                        return Err(Error::invalid_argument("first tick fails"));
                    }
                    ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// A factory whose worlds register the `echo` reducer and nothing else.
fn reducer_factory() -> PartitionFactory {
    Box::new(|id: WorldId, store: TableStore, sim: PartitionConfig| {
        let mut world = Partition::new(id, store, sim)?;
        world
            .native_mut()
            .register(
                nexum_reducer::ReducerDefinition::new(
                    nexum_core::ReducerId::from_u64(1),
                    "echo",
                    echo,
                )
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

#[test]
fn reducer_calls_execute_inside_ticks_respecting_the_per_tick_budget_fifo() {
    // Budget 2 per tick: 5 calls execute 2 + 2 + 1 across three ticks, in
    // FIFO (request-id) order. Overflow stays queued; nothing is dropped.
    let sim = PartitionConfig::new().with_max_reducer_calls_per_tick(2);
    let mut runtime = Runtime::new(RuntimeConfig::new(reducer_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime.create_partition(world, sim).unwrap();
    runtime.start_partition(world).unwrap();

    for request_id in 0..5u64 {
        runtime
            .submit_reducer_call(
                world,
                request_id,
                "echo",
                nexum_reducer::ReducerArgs::new().insert("v", request_id),
            )
            .unwrap();
    }
    assert_eq!(runtime.metrics().reducer_calls_accepted, 5);

    let ids_of = |runtime: &mut Runtime| {
        let results = runtime.step_detailed().unwrap();
        assert_eq!(results.len(), 1);
        results[0]
            .1
            .reducer_results()
            .iter()
            .map(|r| r.request_id())
            .collect::<Vec<_>>()
    };

    // Tick 1: exactly the budget, in submission order.
    assert_eq!(ids_of(&mut runtime), vec![0, 1]);
    // Tick 2: the next two.
    assert_eq!(ids_of(&mut runtime), vec![2, 3]);
    // Tick 3: the remainder.
    assert_eq!(ids_of(&mut runtime), vec![4]);
    // All calls executed; no accepted call was dropped or duplicated.
    assert_eq!(ids_of(&mut runtime), Vec::<u64>::new());
    assert_eq!(runtime.metrics().reducer_calls_rejected, 0);

    // Each result is typed: success with the echoed value.
    runtime
        .submit_reducer_call(
            world,
            99,
            "echo",
            nexum_reducer::ReducerArgs::new().insert("v", 7u64),
        )
        .unwrap();
    let results = runtime.step_detailed().unwrap();
    let result = &results[0].1.reducer_results()[0];
    assert_eq!(result.request_id(), 99);
    assert!(result.is_ok());
    assert_eq!(result.value(), Some(&Value::U64(7)));
}

#[test]
fn reducer_calls_beyond_the_per_tick_budget_survive_until_future_ticks() {
    // A single call sits in the queue until the world ticks; a later call
    // behind it executes on the following tick — the queue is strictly FIFO
    // and never drains a call without executing it.
    let sim = PartitionConfig::new().with_max_reducer_calls_per_tick(1);
    let mut runtime = Runtime::new(RuntimeConfig::new(reducer_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime.create_partition(world, sim).unwrap();
    runtime.start_partition(world).unwrap();

    runtime
        .submit_reducer_call(world, 1, "echo", nexum_reducer::ReducerArgs::new())
        .unwrap();
    // Not yet ticked: the call is still queued, not lost.
    assert_eq!(runtime.metrics().reducer_calls_accepted, 1);
    assert_eq!(runtime.metrics().ticks_succeeded, 0);

    // A second call queues behind the first; budget 1 means one per tick.
    runtime
        .submit_reducer_call(world, 2, "echo", nexum_reducer::ReducerArgs::new())
        .unwrap();
    runtime
        .submit_reducer_call(world, 3, "echo", nexum_reducer::ReducerArgs::new())
        .unwrap();

    let ids: Vec<u64> = runtime.step_detailed().unwrap()[0]
        .1
        .reducer_results()
        .iter()
        .map(|r| r.request_id())
        .collect();
    assert_eq!(ids, vec![1]);
    let ids: Vec<u64> = runtime.step_detailed().unwrap()[0]
        .1
        .reducer_results()
        .iter()
        .map(|r| r.request_id())
        .collect();
    assert_eq!(ids, vec![2]);
    let ids: Vec<u64> = runtime.step_detailed().unwrap()[0]
        .1
        .reducer_results()
        .iter()
        .map(|r| r.request_id())
        .collect();
    assert_eq!(ids, vec![3]);
}

#[test]
fn zero_reducer_call_budget_is_an_invalid_configuration_not_a_hang_or_drop() {
    // `max_reducer_calls_per_tick = 0` is rejected at configuration time:
    // a world cannot be created with it, so calls can never silently
    // disappear into a zero-budget tick.
    let sim = PartitionConfig::new().with_max_reducer_calls_per_tick(0);
    assert!(matches!(sim.validate(), Err(Error::InvalidArgument(_))));

    let mut runtime = Runtime::new(RuntimeConfig::new(reducer_factory())).unwrap();
    assert!(matches!(
        runtime.create_partition(WorldId::from_u64(0), sim),
        Err(RuntimeError::Internal(_))
    ));
}

#[test]
fn reducer_calls_to_unknown_or_stopped_worlds_are_rejected_explicitly() {
    let mut runtime = Runtime::new(RuntimeConfig::new(reducer_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.stop_partition(world).unwrap();

    // Unknown world: explicit error, never queued.
    assert!(matches!(
        runtime.submit_reducer_call(
            WorldId::from_u64(9),
            1,
            "echo",
            nexum_reducer::ReducerArgs::new()
        ),
        Err(RuntimeError::UnknownPartition(_))
    ));
    // Stopped world: explicit error, never queued.
    assert!(matches!(
        runtime.submit_reducer_call(world, 2, "echo", nexum_reducer::ReducerArgs::new()),
        Err(RuntimeError::InvalidPartitionState { .. })
    ));
    assert_eq!(runtime.metrics().reducer_calls_accepted, 0);
}

#[test]
fn reducer_call_queue_overflow_is_rejected_explicitly_never_silently_dropped() {
    let config = RuntimeConfig::new(reducer_factory()).with_max_queued_reducer_calls(2);
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    runtime
        .submit_reducer_call(world, 1, "echo", nexum_reducer::ReducerArgs::new())
        .unwrap();
    runtime
        .submit_reducer_call(world, 2, "echo", nexum_reducer::ReducerArgs::new())
        .unwrap();
    // The third call is rejected with the runtime error (capacity) — the
    // caller receives the error and may retry; nothing is silently lost.
    assert!(matches!(
        runtime.submit_reducer_call(world, 3, "echo", nexum_reducer::ReducerArgs::new()),
        Err(RuntimeError::ReducerCallRejected { .. })
    ));
    assert_eq!(runtime.metrics().reducer_calls_rejected, 1);
    assert_eq!(runtime.metrics().reducer_calls_accepted, 2);

    // Both accepted calls still execute.
    let ids: Vec<u64> = runtime.step_detailed().unwrap()[0]
        .1
        .reducer_results()
        .iter()
        .map(|r| r.request_id())
        .collect();
    assert_eq!(ids, vec![1, 2]);
}

#[test]
fn calls_drained_into_a_failed_tick_are_requeued_not_lost() {
    // Under `TickFailurePolicy::Continue` a failed tick leaves the world
    // running; the calls that were drained into that tick must be requeued
    // (FIFO) and execute on the next eligible tick — never silently lost
    // and never leaving a caller hanging.
    let config = RuntimeConfig::new(continue_factory())
        .with_tick_failure_policy(TickFailurePolicy::Continue);
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    runtime
        .submit_reducer_call(
            world,
            1,
            "echo",
            nexum_reducer::ReducerArgs::new().insert("v", 1u64),
        )
        .unwrap();
    assert_eq!(runtime.metrics().reducer_calls_accepted, 1);

    // Tick 0 fails; under Continue the world keeps running.
    let report = runtime.step().unwrap();
    assert_eq!(report.failed, 1);
    assert_eq!(
        runtime.partition_status(world).unwrap().state,
        PartitionLifecycle::Running
    );

    // Tick 1 executes the requeued call (FIFO) alongside the now-healthy
    // system — exactly one terminal result, not lost.
    let results = runtime.step_detailed().unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].1.tick().as_u64(), 1);
    let ids: Vec<u64> = results[0]
        .1
        .reducer_results()
        .iter()
        .map(|r| r.request_id())
        .collect();
    assert_eq!(ids, vec![1]);
    assert!(results[0].1.reducer_results()[0].is_ok());
    assert_eq!(
        results[0].1.reducer_results()[0].value(),
        Some(&Value::U64(1))
    );
}

// ---------------------------------------------------------- determinism

/// Runs 4 worlds for 5 ticks each (tick_once, world-ascending) and returns
/// the per-world per-tick change traces.
fn run_scenario(workers: usize) -> Vec<Vec<Vec<nexum_storage::Change>>> {
    let config = RuntimeConfig::new(writer_factory()).with_worker_count(workers);
    let mut runtime = Runtime::new(config).unwrap();
    start_worlds(&mut runtime, &[0, 1, 2, 3]);
    let mut traces = vec![Vec::new(); 4];
    for _ in 0..5 {
        for world in 0..4u64 {
            let result = runtime.tick_once(WorldId::from_u64(world)).unwrap();
            traces[world as usize].push(result.changes().to_vec());
        }
    }
    traces
}

#[test]
fn worker_count_never_changes_world_traces() {
    assert_eq!(run_scenario(1), run_scenario(3));
    assert_eq!(run_scenario(2), run_scenario(5));
    assert_eq!(run_scenario(4), run_scenario(4));
}

#[test]
fn failure_isolation_keeps_other_worlds_correct() {
    let mut runtime = Runtime::new(RuntimeConfig::new(failing_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1, 2]);

    // First step: world 1 fails after its writer ran (the writer's insert is
    // rolled back with the tick); worlds 0 and 2 commit.
    let report = runtime.step().unwrap();
    assert_eq!(report.succeeded, 2);
    assert_eq!(report.failed, 1);
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(1))
            .unwrap()
            .state,
        PartitionLifecycle::Failed
    );

    // Subsequent steps tick only the healthy worlds.
    let report = runtime.step().unwrap();
    assert_eq!(report.worlds, 2);
    assert_eq!(report.succeeded, 2);
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(0))
            .unwrap()
            .next_tick,
        TickId::from_u64(2)
    );
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(2))
            .unwrap()
            .next_tick,
        TickId::from_u64(2)
    );

    // The failed world's partial write never committed (0 rows recovered
    // from its store is unobservable directly, but its failed status is).
    let failed_events = runtime
        .drain_events()
        .into_iter()
        .filter(
            |e| matches!(e, crate::RuntimeEvent::PartitionFailed { world, .. } if world.as_u64() == 1),
        )
        .count();
    assert_eq!(failed_events, 1);
}

// --------------------------------------------------------------- shutdown

#[test]
fn shutdown_is_deterministic_and_blocks_new_operations() {
    let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
    start_worlds(&mut runtime, &[0, 1]);
    runtime.step().unwrap();

    runtime.shutdown().unwrap();
    assert_eq!(runtime.state(), RuntimeState::Stopped);
    assert_eq!(
        runtime
            .worker_status(nexum_core::WorkerId::from_u64(0))
            .unwrap()
            .state,
        WorkerState::Stopped
    );
    assert_eq!(
        runtime
            .partition_status(WorldId::from_u64(0))
            .unwrap()
            .state,
        PartitionLifecycle::Stopped
    );

    // Everything is rejected after shutdown.
    assert!(matches!(runtime.step(), Err(RuntimeError::Shutdown)));
    assert!(matches!(
        runtime.tick_once(WorldId::from_u64(0)),
        Err(RuntimeError::Shutdown)
    ));
    assert!(matches!(
        runtime.create_partition(WorldId::from_u64(9), PartitionConfig::new()),
        Err(RuntimeError::Shutdown)
    ));
    assert!(matches!(
        runtime.submit_input(WorldId::from_u64(0), InputFrame::new(TickId::from_u64(0))),
        Err(RuntimeError::Shutdown)
    ));
    // Idempotent.
    runtime.shutdown().unwrap();
}

// ------------------------------------------------------------ config/events

#[test]
fn invalid_configurations_are_rejected() {
    assert!(matches!(
        Runtime::new(RuntimeConfig::new(writer_factory()).with_worker_count(0)),
        Err(RuntimeError::InvalidConfig(_))
    ));
    assert!(matches!(
        Runtime::new(RuntimeConfig::new(writer_factory()).with_max_queued_inputs(0)),
        Err(RuntimeError::InvalidConfig(_))
    ));
    assert!(matches!(
        Runtime::new(RuntimeConfig::new(writer_factory()).with_snapshot_interval(0)),
        Err(RuntimeError::InvalidConfig(_))
    ));
    // Persistence without a directory.
    let no_dir = RuntimeConfig {
        ..RuntimeConfig::new(writer_factory())
            .with_persistence(PersistencePolicy::Flush, PathBuf::new())
    };
    let config = RuntimeConfig {
        persistence_dir: None,
        ..no_dir
    };
    assert!(matches!(
        Runtime::new(config),
        Err(RuntimeError::InvalidConfig(_))
    ));
}

#[test]
fn events_are_bounded_and_metrics_count_work() {
    let mut runtime =
        Runtime::new(RuntimeConfig::new(writer_factory()).with_event_log_limit(4)).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();
    for _ in 0..5 {
        runtime.step().unwrap();
    }

    let metrics = runtime.metrics();
    assert_eq!(metrics.ticks_total, 5);
    assert_eq!(metrics.ticks_succeeded, 5);
    assert!(metrics.avg_tick_ns() > 0);

    // The bounded log keeps only the most recent 4 events.
    assert!(runtime.event_count() <= 4);
    runtime.drain_events();
    assert_eq!(runtime.event_count(), 0);
}

// ------------------------------------------------- Phase 18: parallel tick

/// A factory whose worlds form a message ring (ADR-012): partition `p`
/// sends a `ring` message to partition `(p + 1) % n` every tick; the
/// destination handler appends a ledger row. Every world's sender runs
/// every tick, so outbound collection, cross-partition delivery, and the
/// handler commit path are all exercised through `step`/`step_detailed`.
fn ring_factory() -> PartitionFactory {
    Box::new(
        move |id: WorldId, mut store: TableStore, sim: PartitionConfig| {
            if store.table("ledger").is_none() {
                store
                    .create_table(
                        nexum_core::TableSchema::builder("ledger")
                            .column("seq", ColumnType::U64)
                            .column("from", ColumnType::U64)
                            .column("to", ColumnType::U64)
                            .primary_key(&["seq", "from"])
                            .build()
                            .unwrap(),
                    )
                    .unwrap();
            }
            let mut world = Partition::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "ring", |ctx, args| {
                        let seq = args.require_u64("seq")?;
                        let from = args.require_u64("from")?;
                        let to = args.require_u64("to")?;
                        ctx.insert("ledger", row![seq, from, to])?;
                        Ok(Value::U64(seq))
                    })
                    .unwrap(),
                )
                .unwrap();
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                    // Capture-free (systems are fn pointers): the ring target
                    // derives from the sender partition (0 → 1, 1 → 2, else → 0).
                    // Destinations with several senders still see deterministic
                    // `(sent_tick, from, seq)` delivery order (ADR-012 D5).
                    let from = ctx.partition().as_u64();
                    let to = match from {
                        0 => 1,
                        1 => 2,
                        _ => 0,
                    };
                    let tick = ctx.tick().as_u64();
                    ctx.send_to(
                        PartitionId::from_u64(to),
                        "ring",
                        ReducerArgs::new()
                            .insert("seq", tick)
                            .insert("from", from)
                            .insert("to", to),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )?;
            Ok(world)
        },
    )
}

/// The full observable outcome of a ring scenario — per-world committed
/// change traces, per-world outbound message traces, final world tick
/// numbers, and metric aggregates. All of these are worker-count
/// **independent**: comparing them across worker counts proves parallel
/// execution is observationally identical to serial (ADR-018 D4).
#[derive(Debug, PartialEq, Eq)]
struct RingTrace {
    changes: Vec<Vec<Vec<nexum_storage::Change>>>,
    outbound: Vec<Vec<Vec<PartitionMessage>>>,
    next_ticks: Vec<u64>,
    ticks_total: u64,
    ticks_succeeded: u64,
    changes_committed: u64,
    messages_sent: u64,
    messages_delivered: u64,
}

/// Runs `worlds` ring partitions for `ticks` steps at `workers` workers.
/// Returns the worker-count-independent trace and the drained
/// `TickCompleted` stream (order is `(worker_id, world_id)` by design,
/// ADR-010 D2, so it depends on the worker count).
fn run_ring_scenario(workers: usize, worlds: u64, ticks: u64) -> (RingTrace, Vec<(u64, u64)>) {
    let config = RuntimeConfig::new(ring_factory()).with_worker_count(workers);
    let mut runtime = Runtime::new(config).unwrap();
    for world in 0..worlds {
        runtime
            .create_partition(WorldId::from_u64(world), PartitionConfig::new())
            .unwrap();
        runtime
            .register_partition(PartitionId::from_u64(world), WorldId::from_u64(world))
            .unwrap();
        runtime.start_partition(WorldId::from_u64(world)).unwrap();
    }
    let mut changes = vec![Vec::new(); worlds as usize];
    let mut outbound = vec![Vec::new(); worlds as usize];
    for _ in 0..ticks {
        let results = runtime.step_detailed().unwrap();
        for (world, result) in &results {
            changes[world.as_u64() as usize].push(result.changes().to_vec());
            outbound[world.as_u64() as usize].push(result.outbound().to_vec());
        }
    }
    let completed = runtime
        .drain_events()
        .into_iter()
        .filter_map(|event| match event {
            crate::RuntimeEvent::TickCompleted { world, tick, .. } => {
                Some((world.as_u64(), tick.as_u64()))
            }
            _ => None,
        })
        .collect();
    let next_ticks = (0..worlds)
        .map(|world| {
            runtime
                .partition_status(WorldId::from_u64(world))
                .unwrap()
                .next_tick
                .as_u64()
        })
        .collect();
    let metrics = runtime.metrics();
    let trace = RingTrace {
        changes,
        outbound,
        next_ticks,
        ticks_total: metrics.ticks_total,
        ticks_succeeded: metrics.ticks_succeeded,
        changes_committed: metrics.changes_committed,
        messages_sent: metrics.messages_sent,
        messages_delivered: metrics.messages_delivered,
    };
    (trace, completed)
}

/// The single-threaded path is the correctness oracle: parallel execution at
/// 2, 4, and 6 workers must reproduce the serial run exactly for everything
/// that is worker-count independent — per-world `Vec<Change>`, the outbound
/// message stream, final tick numbers, and metric aggregates (ADR-018 D4).
/// The ring scenario also proves the delivery phase stays order-safe when
/// sender and receiver worlds tick concurrently (worlds 2–5 all message
/// world 0).
#[test]
fn parallel_step_matches_serial_step_exactly() {
    let (serial, _) = run_ring_scenario(1, 6, 5);
    for workers in [2, 4, 6] {
        let (trace, _) = run_ring_scenario(workers, 6, 5);
        assert_eq!(serial, trace, "workers={workers}");
    }
}

/// The `TickCompleted` event stream must arrive in the deterministic
/// `(worker_id, world_id)` order (ADR-010 D2, ADR-018 D2) — worlds assigned
/// round-robin to workers at creation, each worker's worlds ascending.
#[test]
fn parallel_step_emits_events_in_deterministic_order() {
    for (workers, worlds) in [(2u64, 6u64), (4, 6), (4, 9), (6, 6)] {
        let (_, completed) = run_ring_scenario(workers as usize, worlds, 4);
        let expected: Vec<(u64, u64)> = (0..4)
            .flat_map(|tick| {
                (0..workers).flat_map(move |worker| {
                    (0..worlds).filter_map(move |world| {
                        (world % workers == worker).then_some((world, tick))
                    })
                })
            })
            .collect();
        assert_eq!(completed, expected, "workers={workers} worlds={worlds}");
    }
}

/// The worker-count-independent observable of the failing-world scenario:
/// per-step (ticks, succeeded, failed) reports, the sorted event multiset,
/// and final tick numbers.
type FailureTrace = (Vec<(u64, u64, u64)>, Vec<(u8, u64, u64)>, Vec<u64>);

/// Parallel ticking must not change failure semantics: a failing world fails
/// at the same tick, other worlds commit identically, and the step reports
/// (ticks/succeeded/failed per step), the event *multiset*, and final tick
/// numbers are identical at any worker count. (Event *order* is
/// `(worker_id, world_id)` by design, covered by
/// [`parallel_step_emits_events_in_deterministic_order`].)
#[test]
fn parallel_step_preserves_failure_isolation() {
    let run = |workers: usize| -> FailureTrace {
        let config = RuntimeConfig::new(failing_factory()).with_worker_count(workers);
        let mut runtime = Runtime::new(config).unwrap();
        start_worlds(&mut runtime, &[0, 1, 2]);
        let mut reports = Vec::new();
        for _ in 0..3 {
            let report = runtime.step().unwrap();
            reports.push((report.ticks, report.succeeded, report.failed));
        }
        let mut events: Vec<(u8, u64, u64)> = runtime
            .drain_events()
            .into_iter()
            .filter_map(|event| match event {
                crate::RuntimeEvent::TickCompleted { world, tick, .. } => {
                    Some((0u8, world.as_u64(), tick.as_u64()))
                }
                crate::RuntimeEvent::TickFailed { world, tick, .. } => {
                    Some((1u8, world.as_u64(), tick.as_u64()))
                }
                crate::RuntimeEvent::PartitionFailed { world, .. } => {
                    Some((2u8, world.as_u64(), 0))
                }
                _ => None,
            })
            .collect();
        events.sort_unstable();
        let next_ticks = [0u64, 1, 2]
            .iter()
            .map(|world| {
                runtime
                    .partition_status(WorldId::from_u64(*world))
                    .unwrap()
                    .next_tick
                    .as_u64()
            })
            .collect();
        (reports, events, next_ticks)
    };
    let serial = run(1);
    for workers in [2, 4] {
        assert_eq!(serial, run(workers), "workers={workers}");
    }
}
