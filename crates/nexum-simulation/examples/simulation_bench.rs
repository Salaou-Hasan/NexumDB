//! Phase 9 simulation benchmarks — honest baselines, not claims.
//!
//! Run with: `cargo run --release -p nexum-simulation --example
//! simulation_bench [iterations]`. Correctness first; these establish the
//! single-threaded reference costs (Phase 15 will optimize).

use std::time::Instant;

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_simulation::{InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::{Query, SubscriptionRegistry};
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Wal};
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

/// Pre-population size for read/update scenarios.
const POOL: usize = 10_000;

fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..100 {
        f(); // warmup
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    println!("{name:<38} {ns:>12.1} ns/op");
}

/// A world with `players`, `logs`, and `items` tables.
fn fresh_world() -> World {
    let mut store = nexum_table::TableStore::new();
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
    store
        .create_table(
            TableSchema::builder("logs")
                .column("mark", ColumnType::U64)
                .primary_key(&["mark"])
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
    World::new(WorldId::from_u64(0), store, SimulationConfig::new()).unwrap()
}

/// Seeds `POOL` player rows directly through a transaction.
fn seed_pool(world: &mut World) {
    let mut tx = Transaction::begin(world.store_mut());
    for i in 0..POOL as u64 {
        tx.insert(world.store(), "players", row![i, 10u64, 100i32])
            .unwrap();
    }
    tx.commit(world.store_mut()).unwrap();
}

/// A non-capturing system that inserts one player row per tick. The player
/// id is derived from the system id, so several such systems can share a
/// tick without colliding on the primary key.
fn insert_system(id: u64, priority: u32) -> SystemDefinition {
    SystemDefinition::new(SystemId::from_u64(id), format!("insert_{id}"), priority, |ctx, _| {
        ctx.insert(
            "players",
            row![ctx.system().as_u64() * 1_000_000 + ctx.tick().as_u64(), 10u64, 100i32],
        )?;
        Ok(())
    })
    .unwrap()
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(10_000);

    // 1. Empty tick — no systems, no writes.
    {
        let mut world = fresh_world();
        let mut tick = 0u64;
        bench("empty tick", iterations, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 2. One system — one insert per tick.
    {
        let mut world = fresh_world();
        world.add_system(insert_system(0, 0)).unwrap();
        let mut tick = 0u64;
        bench("one system (1 write)", iterations, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 3. Ten systems.
    {
        let mut world = fresh_world();
        for i in 0..10 {
            world.add_system(insert_system(i, i as u32)).unwrap();
        }
        let mut tick = 0u64;
        bench("10 systems (10 writes)", iterations / 2, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 4. One hundred systems.
    {
        let mut world = fresh_world();
        for i in 0..100 {
            world.add_system(insert_system(i, i as u32)).unwrap();
        }
        let mut tick = 0u64;
        bench("100 systems (100 writes)", iterations / 10, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 5. Read-only system — scans the pool every tick.
    {
        let mut world = fresh_world();
        seed_pool(&mut world);
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "observer", 0, |ctx, _| {
                    let rows = ctx.scan("players")?;
                    assert_eq!(rows.len(), POOL);
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("read-only system (scan pool)", iterations / 2, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 6. Single-row mutation — one update per tick, rotating victims.
    {
        let mut world = fresh_world();
        seed_pool(&mut world);
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "updater", 0, |ctx, _| {
                    let victim = ctx.tick().as_u64() % POOL as u64;
                    ctx.update(
                        "players",
                        nexum_core::RowId::from_u64(victim),
                        row![victim, 10u64, 50i32],
                    )?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("single-row mutation", iterations, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 7. 100-row mutation — 100 inserts per tick.
    {
        let mut world = fresh_world();
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "bulk", 0, |ctx, _| {
                    let base = ctx.tick().as_u64() * 100;
                    for i in 0..100u64 {
                        ctx.insert("players", row![base + i, 10u64, 100i32])?;
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("100-row mutation", iterations / 20, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 8. Multi-table mutation — one write to each of three tables.
    {
        let mut world = fresh_world();
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "multi", 0, |ctx, _| {
                    let tick = ctx.tick().as_u64();
                    ctx.insert("players", row![tick, 10u64, 100i32])?;
                    ctx.insert("logs", row![tick])?;
                    ctx.insert("items", row![tick, tick])?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("multi-table mutation", iterations / 2, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 9. Native reducer invoked from a system.
    {
        let mut world = fresh_world();
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
                SystemDefinition::new(SystemId::from_u64(0), "invoker", 0, |ctx, _| {
                    ctx.invoke_reducer("spawn", &ReducerArgs::new().insert("id", 1_000_000 + ctx.tick().as_u64()))?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("native reducer invocation", iterations / 2, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 10. WASM reducer invoked from a system (reads its id argument at the
    // documented fixed offset: count u64 + name_len u64 + "id" + tag byte).
    {
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
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("wspawn", 1, wat::parse_str(&wat).unwrap())
            .unwrap();
        let mut world = fresh_world();
        world.set_wasm(wasm);
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "wasm_invoker", 0, |ctx, _| {
                    ctx.invoke_wasm("wspawn", &ReducerArgs::new().insert("id", 2_000_000 + ctx.tick().as_u64()))?;
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        let mut tick = 0u64;
        bench("wasm reducer invocation", iterations / 10, || {
            world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            tick += 1;
        });
    }

    // 11. Subscription fan-out after each tick.
    {
        let mut world = fresh_world();
        world.add_system(insert_system(0, 0)).unwrap();
        let mut registry = SubscriptionRegistry::new();
        let sub = registry
            .subscribe(
                world.store(),
                Query::builder("players")
                    .predicate_eq("zone", 10u64)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        registry.drain(sub).unwrap();
        let mut tick = 0u64;
        bench("tick + subscription fan-out", iterations, || {
            let result = world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            let _ = registry.apply_changes(world.store(), result.changes());
            let _ = registry.drain(sub).unwrap(); // keep the buffer steady
            tick += 1;
        });
    }

    // 12. WAL append after each tick (Flush policy — no fsync).
    {
        let dir = std::env::temp_dir().join("nexum-sim-bench-wal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let wal_path = dir.join("log.wal");
        let mut wal = Wal::create(&wal_path, DurabilityPolicy::Flush).unwrap();

        let mut world = fresh_world();
        world.add_system(insert_system(0, 0)).unwrap();
        let mut tick = 0u64;
        bench("tick + WAL append", iterations / 2, || {
            let result = world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
            wal.append(result.tx_id(), result.changes()).unwrap();
            tick += 1;
        });
        let _ = std::fs::remove_dir_all(&dir);
    }
}
