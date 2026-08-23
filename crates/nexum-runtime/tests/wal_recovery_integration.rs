//! Integration (Phase 10 brief §32 "Persistence", §16 "Recovery"): a
//! crash/recovery round-trip through the runtime — including a WASM reducer
//! invoked from a simulation system — reconstructs identical authoritative
//! state, snapshot-based recovery continues from the snapshot, and a failed
//! worker's world can be destroyed and recovered onto another worker.
//!
//! Recovery goes through the existing Phase 5 engine; the runtime only
//! orders the world factory around it (ADR-010 D5).

use std::path::PathBuf;

use nexum_core::row;
use nexum_core::{ColumnType, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{SimulationConfig, SystemDefinition, World};
use nexum_subscription::{Query, SubscriptionUpdate};
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

/// The WASM ABI helpers (the same known-good module pattern used by the
/// Phase 7/9 integration tests).
const HELPERS: &str = r#"
  (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
    (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
  (func $put_value_u64 (param $p i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $p) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 1)) (local.get $v))
    (i32.add (local.get $p) (i32.const 9)))
  (func $put_row3 (param $p i32) (param $id i64) (param $zone i64) (param $health i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.const 3))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 8)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 9)) (local.get $id))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 17)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 18)) (local.get $zone))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 26)) (i32.const 3))
    (i32.store align=1 (i32.add (local.get $p) (i32.const 27)) (local.get $health))
    (i32.add (local.get $p) (i32.const 31)))
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))
  (func $ret_u64 (param $v i64) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 8))
    (i64.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 9))
"#;

/// A WASM reducer inserting `players [500, 10, 77]` and emitting
/// `"wasm_spawned"` (opcode 5 = INSERT, 8 = EMIT), returning 500.
fn wasm_module() -> Vec<u8> {
    let wat = format!(
        r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
  (data (i32.const 90400) "wasm_spawned")
{helpers}
  (func (export "_nexum_reducer_run") (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row3 (local.get $p) (i64.const 500) (i64.const 10) (i32.const 77)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 12)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 500)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 500))))
"#,
        helpers = HELPERS
    );
    wat::parse_str(&wat).expect("test module is valid WAT")
}

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

/// A factory with a native reducer, a WASM reducer, and three systems
/// (writer, native invoker, WASM invoker at tick 0). Rows per tick: writer
/// (id = tick), native (id = 200 + tick); plus the fixed WASM row at tick 0.
fn full_factory() -> WorldFactory {
    let module = wasm_module();
    Box::new(
        move |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            if store.table("players").is_none() {
                players_table(&mut store);
            }
            let mut world = World::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "spawn", |ctx, args| {
                        let id = args.require_u64("id")?;
                        ctx.insert("players", row![id, 10u64, 50i32])?;
                        ctx.emit("spawned", id)?;
                        Ok(Value::U64(id))
                    })
                    .unwrap(),
                )
                .unwrap();
            let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
            wasm.register("wspawn", 1, module.clone()).unwrap();
            world.set_wasm(wasm);

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
                    SystemDefinition::new(SystemId::from_u64(1), "native_invoker", 20, |ctx, _| {
                        ctx.invoke_reducer(
                            "spawn",
                            &ReducerArgs::new().insert("id", 200 + ctx.tick().as_u64()),
                        )?;
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(2), "wasm_invoker", 30, |ctx, _| {
                        if ctx.tick().as_u64() == 0 {
                            ctx.invoke_wasm("wspawn", &ReducerArgs::new())?;
                        }
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
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

fn snapshot_row_count(runtime: &mut Runtime, world: WorldId, expected: usize) {
    let sub = runtime
        .subscribe(world, Query::builder("players").build().unwrap())
        .unwrap();
    let initial = runtime.drain(world, sub).unwrap();
    assert_eq!(initial.len(), 1, "exactly one Initial snapshot");
    match &initial[0] {
        SubscriptionUpdate::Initial { rows, .. } => assert_eq!(rows.len(), expected),
        other => panic!("expected Initial, got {other:?}"),
    }
}

#[test]
fn crash_then_recover_reconstructs_identical_state_and_continues() {
    let dir = temp_dir("nexum-runtime-crash-recover");
    let world = WorldId::from_u64(0);

    // Phase 1: run 3 ticks, then "crash" — drop without shutdown(). Flush
    // durability already wrote every commit to the OS on append.
    {
        let mut runtime = Runtime::new(
            RuntimeConfig::new(full_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        runtime
            .create_world(world, SimulationConfig::new())
            .unwrap();
        runtime.start_world(world).unwrap();
        for _ in 0..3 {
            runtime.step().unwrap();
        }
    }

    // Phase 2: a fresh runtime recovers the world from its WAL (no
    // snapshots — the WAL-only recovery mode; the factory defines the
    // schema first).
    let mut runtime = Runtime::new(
        RuntimeConfig::new(full_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let report = runtime
        .recover_world(world, SimulationConfig::new(), Some(TickId::from_u64(3)))
        .unwrap();
    assert_eq!(report.replayed_txs, 3);
    assert!(report.snapshot.is_none(), "WAL-only recovery");
    runtime.start_world(world).unwrap();

    // The recovered authoritative state is exactly 3 ticks of rows:
    // 3 writer + 3 native reducer + 1 WASM row.
    snapshot_row_count(&mut runtime, world, 7);

    // Continuation: tick 3 adds writer(3) and native(203). Subscribe before
    // the tick so the committed changes are delivered.
    let sub = runtime
        .subscribe(world, Query::builder("players").build().unwrap())
        .unwrap();
    runtime.drain(world, sub).unwrap(); // Initial (7 rows)
    let result = runtime.tick_once(world).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(3));
    let updates = runtime.drain(world, sub).unwrap();
    assert_eq!(updates.len(), 2);
    runtime.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn snapshot_recovery_uses_the_snapshot_and_resumes() {
    let dir = temp_dir("nexum-runtime-snapshot-recover");
    let world = WorldId::from_u64(0);
    {
        let mut runtime = Runtime::new(
            RuntimeConfig::new(full_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone())
                .with_snapshot_interval(2),
        )
        .unwrap();
        runtime
            .create_world(world, SimulationConfig::new())
            .unwrap();
        runtime.start_world(world).unwrap();
        for _ in 0..4 {
            runtime.step().unwrap();
        }
        runtime.shutdown().unwrap();
    }

    let mut runtime = Runtime::new(
        RuntimeConfig::new(full_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let report = runtime
        .recover_world(world, SimulationConfig::new(), Some(TickId::from_u64(4)))
        .unwrap();
    assert!(report.snapshot.is_some(), "snapshot-based recovery");
    assert_eq!(report.replayed_txs, 0, "the snapshot covers all 4 ticks");
    runtime.start_world(world).unwrap();

    // 4 writer + 4 native + 1 WASM = 9 rows.
    snapshot_row_count(&mut runtime, world, 9);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn a_failed_worker_s_world_can_be_recovered_onto_another_worker() {
    let dir = temp_dir("nexum-runtime-worker-recover");
    let config = RuntimeConfig::new(full_factory())
        .with_worker_count(2)
        .with_persistence(PersistencePolicy::Flush, dir.clone());
    let mut runtime = Runtime::new(config).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_world(world, SimulationConfig::new())
        .unwrap();
    runtime.start_world(world).unwrap();
    runtime.step().unwrap();
    runtime.step().unwrap();
    assert_eq!(runtime.metrics().wal_appends, 2);

    // Worker 0 fails: world 0 becomes Failed and recoverable; worker 1's
    // worlds are untouched (none here).
    runtime
        .fail_worker(nexum_core::WorkerId::from_u64(0))
        .unwrap();
    assert_eq!(
        runtime.world_status(world).unwrap().state,
        nexum_runtime::WorldLifecycle::Failed
    );

    // Destroy the in-memory entry, then recover from the WAL onto the next
    // worker (round-robin counter has advanced past worker 0).
    runtime.destroy_world(world).unwrap();
    let report = runtime
        .recover_world(world, SimulationConfig::new(), Some(TickId::from_u64(2)))
        .unwrap();
    assert_eq!(report.replayed_txs, 2);
    assert_ne!(
        runtime.assigned_worker(world).unwrap(),
        nexum_core::WorkerId::from_u64(0),
        "recovered onto a different worker"
    );

    runtime.start_world(world).unwrap();
    let result = runtime.tick_once(world).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(2));
    runtime.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
