//! Integration (Phase 9 brief §19, §24 "Persistence"): simulation ticks
//! flow through the commit boundary into the WAL exactly like any other
//! committed transaction, and recovery reconstructs the identical
//! authoritative state — including rows produced by a WASM reducer invoked
//! from a system.

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, ReducerId, Row, RowId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_simulation::{InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_table::TableStore;
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

fn fixture() -> World {
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
    store
        .create_table(
            TableSchema::builder("logs")
                .column("mark", ColumnType::U64)
                .primary_key(&["mark"])
                .build()
                .unwrap(),
        )
        .unwrap();
    World::new(WorldId::from_u64(0), store, SimulationConfig::new()).unwrap()
}

fn dump(store: &TableStore) -> Vec<(String, Vec<(RowId, Row)>)> {
    let mut out = Vec::new();
    for (name, table) in store.tables() {
        let rows: Vec<(RowId, Row)> = table.scan().map(|(id, r)| (id, r.clone())).collect();
        out.push((name.to_string(), rows));
    }
    out
}

// A minimal WASM reducer inserting a row into the 3-column `players` table.
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

fn wasm_module(body: &str) -> Vec<u8> {
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
{body}))
"#,
        helpers = HELPERS,
        body = body
    );
    wat::parse_str(&wat).expect("test module is valid WAT")
}

#[test]
fn ticks_are_durable_and_recovery_reconstructs_identical_state() {
    let dir = std::env::temp_dir().join(format!("nexum-sim-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let wal_path = dir.join("log.wal");

    let mut world = fixture();

    // A native reducer and a WASM reducer, both invocable from systems.
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
    // Insert players [500, 10, 77] + emit "wasm_spawned" + return 500.
    wasm.register(
        "wspawn",
        1,
        wasm_module(
            r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row3 (local.get $p) (i64.const 500) (i64.const 10) (i32.const 77)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 12)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 500)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 500))"#,
        ),
    )
    .unwrap();
    world.set_wasm(wasm);

    // writer inserts one player per tick; the native invoker spawns via the
    // reducer; the wasm invoker runs the sandboxed reducer once (tick 0, to
    // keep the fixed primary key unique).
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
                ctx.invoke_reducer("spawn", &ReducerArgs::new().insert("id", 200 + ctx.tick().as_u64()))?;
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

    // Ticks 0..2 are appended to the WAL; then a snapshot; then ticks 3..4.
    let mut wal = Wal::create(&wal_path, DurabilityPolicy::Flush).unwrap();
    for tick in 0..3u64 {
        let result = world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
        wal.append(result.tx_id(), result.changes()).unwrap();
    }
    Snapshot::capture(world.store(), wal.lsn().as_u64())
        .write(&dir)
        .unwrap();
    for tick in 3..5u64 {
        let result = world.tick(&InputFrame::new(TickId::from_u64(tick))).unwrap();
        wal.append(result.tx_id(), result.changes()).unwrap();
    }

    let expected = dump(world.store());
    drop(world);

    // Crash recovery: snapshot + WAL continuation into a fresh store.
    let mut fresh = TableStore::new();
    let mut wal = Wal::open(&wal_path, DurabilityPolicy::Flush).unwrap();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert_eq!(report.replayed_txs, 2); // only the post-snapshot ticks
    assert_eq!(dump(&fresh), expected);

    let _ = std::fs::remove_dir_all(&dir);
}
