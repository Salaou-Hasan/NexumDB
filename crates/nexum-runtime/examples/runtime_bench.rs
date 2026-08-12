//! Phase 10 runtime benchmarks — honest baselines, not claims (ADR-010 §31).
//!
//! Run with: `cargo run --release -p nexum-runtime --example runtime_bench
//! [iterations]`. The runtime is single-threaded by design (Phase 11 will
//! parallelize); these establish the coordination overhead of worlds,
//! workers, inputs, durability, and observation on top of `World::tick`.

use std::time::Instant;

use nexum_core::row;
use nexum_core::{ColumnType, ReducerId, SystemId, TickId, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..100 {
        f(); // warmup
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    println!("{name:<40} {ns:>12.1} ns/op");
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

fn ensure_players(store: &mut TableStore) {
    if store.table("players").is_none() {
        players_table(store);
    }
}

/// A world with no systems — the empty tick.
fn noop_factory() -> WorldFactory {
    Box::new(|id: WorldId, store: TableStore, sim: SimulationConfig| {
        World::new(id, store, sim)
    })
}

/// A world whose single system inserts one player row per tick.
fn writer_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
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

/// A world whose system inserts ten rows per tick.
fn bulk_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "bulk", 0, |ctx, _| {
                    let base = ctx.tick().as_u64() * 10;
                    for i in 0..10u64 {
                        ctx.insert("players", row![base + i, 10u64, 100i32])?;
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// A world whose system consumes `spawn` input commands as player rows.
fn input_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                    for command in frame.commands() {
                        if command.kind() == "spawn" {
                            let id = command.payload().and_then(nexum_core::Value::as_u64).unwrap();
                            ctx.insert("players", row![id, 10u64, 100i32])?;
                        }
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// A world whose system invokes a native reducer every tick.
fn reducer_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .native_mut()
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "spawn", |ctx, args| {
                    let id = args.require_u64("id")?;
                    ctx.insert("players", row![id, 10u64, 50i32])?;
                    Ok(nexum_core::Value::U64(id))
                })
                .unwrap(),
            )
            .unwrap();
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "invoker", 0, |ctx, _| {
                    ctx.invoke_reducer(
                        "spawn",
                        &ReducerArgs::new().insert("id", 1_000_000 + ctx.tick().as_u64()),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// The WASM module used by the WASM-heavy scenario: reads its single `id`
/// argument (u64 at offset 19 of the in-buffer) and inserts
/// `players [id, 10, 77]` (opcode 5 = INSERT).
fn wasm_module() -> Vec<u8> {
    const HELPERS: &str = r#"
      (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
        (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
        (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
        (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
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
    let wat = format!(
        r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
{helpers}
  (func (export "_nexum_reducer_run") (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row3 (local.get $p) (i64.load align=1 (i32.const 19)) (i64.const 10) (i32.const 77)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 0))))
"#,
        helpers = HELPERS
    );
    wat::parse_str(&wat).expect("bench module is valid WAT")
}

/// A world whose system invokes a WASM reducer every tick.
fn wasm_factory() -> WorldFactory {
    let module = wasm_module();
    Box::new(move |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("wspawn", 1, module.clone()).unwrap();
        world.set_wasm(wasm);
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "wasm_invoker", 0, |ctx, _| {
                    ctx.invoke_wasm(
                        "wspawn",
                        &ReducerArgs::new().insert("id", 2_000_000 + ctx.tick().as_u64()),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);

    // 1. Runtime scheduling overhead — an empty step (no worlds).
    {
        let mut runtime = Runtime::new(
            RuntimeConfig::new(noop_factory()).with_worker_count(8),
        )
        .unwrap();
        bench("scheduler overhead (empty step)", iterations, || {
            runtime.step().unwrap();
        });
    }

    // 2. One world — one empty tick per step.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(noop_factory())).unwrap();
        runtime.create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        runtime.start_world(WorldId::from_u64(0)).unwrap();
        bench("one world (empty tick)", iterations, || {
            runtime.step().unwrap();
        });
    }

    // 3-5. Many worlds — one step ticks every world once.
    for (count, divisor) in [(10usize, 1usize), (100, 5), (1_000, 50)] {
        let mut runtime = Runtime::new(RuntimeConfig::new(noop_factory())).unwrap();
        for w in 0..count as u64 {
            runtime.create_world(WorldId::from_u64(w), SimulationConfig::new()).unwrap();
            runtime.start_world(WorldId::from_u64(w)).unwrap();
        }
        bench(&format!("{count} worlds (empty ticks)"), iterations / divisor, || {
            runtime.step().unwrap();
        });
    }

    // 6. Input-heavy tick — 10 routed commands per frame.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(input_factory())).unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        let mut tick = 0u64;
        bench("input-heavy tick (10 cmds)", iterations / 2, || {
            let mut frame = InputFrame::new(TickId::from_u64(tick));
            for i in 0..10u64 {
                frame.push(
                    InputCommand::new(1, "spawn", Some(nexum_core::Value::U64(tick * 10 + i)))
                        .unwrap(),
                );
            }
            runtime.submit_input(world, frame).unwrap();
            runtime.step().unwrap();
            tick += 1;
        });
    }

    // 7. Transaction-heavy tick — 10-row insert per tick.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(bulk_factory())).unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        bench("transaction-heavy tick (10 rows)", iterations / 2, || {
            runtime.step().unwrap();
        });
    }

    // 8. Reducer-heavy tick — native reducer invoked per tick.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(reducer_factory())).unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        bench("reducer-heavy tick (native)", iterations / 2, || {
            runtime.step().unwrap();
        });
    }

    // 9. WASM-heavy tick — sandboxed reducer invoked per tick.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(wasm_factory())).unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        bench("reducer-heavy tick (wasm)", iterations / 5, || {
            runtime.step().unwrap();
        });
    }

    // 10. Tick + WAL append (Flush policy — no fsync).
    {
        let dir = temp_dir("nexum-runtime-bench-wal");
        let mut runtime = Runtime::new(
            RuntimeConfig::new(writer_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        bench("tick + WAL append", iterations / 2, || {
            runtime.step().unwrap();
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    // 11. Tick + subscription fan-out.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        let sub = runtime
            .subscribe(world, Query::builder("players").build().unwrap())
            .unwrap();
        runtime.drain(world, sub).unwrap(); // Initial
        bench("tick + subscription fan-out", iterations, || {
            runtime.step().unwrap();
            let _ = runtime.drain(world, sub).unwrap();
        });
    }

    // 12. World creation (create + destroy per iteration).
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
        let mut next = 0u64;
        bench("world creation (create+destroy)", iterations, || {
            let id = WorldId::from_u64(next);
            next += 1;
            runtime.create_world(id, SimulationConfig::new()).unwrap();
            runtime.destroy_world(id).unwrap();
        });
    }

    // 13. World recovery from the WAL (destroy + recover per iteration).
    {
        let dir = temp_dir("nexum-runtime-bench-recover");
        let mut runtime = Runtime::new(
            RuntimeConfig::new(writer_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        let world = WorldId::from_u64(0);
        runtime.create_world(world, SimulationConfig::new()).unwrap();
        runtime.start_world(world).unwrap();
        runtime.step().unwrap(); // one durable transaction
        bench("world recovery (WAL replay)", iterations, || {
            runtime.destroy_world(world).unwrap();
            runtime
                .recover_world(world, SimulationConfig::new(), Some(TickId::from_u64(1)))
                .unwrap();
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
