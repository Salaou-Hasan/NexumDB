//! Phase 9 unit tests (ADR-009).

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{
    ColumnType, Error, ReducerId, Row, RowId, SystemId, TickId, TransactionId, Value, WorldId,
};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use crate::input::{InputCommand, InputFrame};
use crate::systems::SystemDefinition;
use crate::{Partition, PartitionConfig};

/// A world with a `players` (id, zone, health) and `logs` (mark) table.
fn fixture(config: PartitionConfig) -> Partition {
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
    Partition::new(WorldId::from_u64(0), store, config).unwrap()
}

fn frame(tick: u64) -> InputFrame {
    InputFrame::new(TickId::from_u64(tick))
}

/// Runs a tick, unwrapping the committed result.
fn run(
    world: &mut Partition,
    inputs: &InputFrame,
) -> (Vec<nexum_storage::Change>, Vec<nexum_reducer::ReducerEvent>) {
    let result = world.tick(inputs).unwrap();
    (result.changes().to_vec(), result.events().to_vec())
}

/// Dumps the authoritative store for equality comparisons.
fn dump(store: &TableStore) -> Vec<(String, Vec<(RowId, Row)>)> {
    let mut out = Vec::new();
    for (name, table) in store.tables() {
        let rows: Vec<(RowId, Row)> = table.scan().map(|(id, r)| (id, r.clone())).collect();
        out.push((name.to_string(), rows));
    }
    out
}

// ------------------------------------------------------------------ ticks

#[test]
fn empty_tick_commits_nothing_and_advances() {
    let mut world = fixture(PartitionConfig::new());
    assert_eq!(world.tick_number(), TickId::from_u64(0));

    let (changes, events) = run(&mut world, &frame(0));
    assert!(changes.is_empty());
    assert!(events.is_empty());
    assert_eq!(world.tick_number(), TickId::from_u64(1));

    // A world with no systems keeps ticking harmlessly.
    for tick in 1..5 {
        let (changes, _) = run(&mut world, &frame(tick));
        assert!(changes.is_empty());
    }
    assert_eq!(world.tick_number(), TickId::from_u64(5));
}

#[test]
fn read_only_system_commits_no_changes() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "observer", 0, |ctx, _| {
                let rows = ctx.scan("players")?;
                assert!(rows.is_empty());
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let (changes, _) = run(&mut world, &frame(0));
    assert!(changes.is_empty());
}

#[test]
fn system_writes_are_committed_every_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "spawner", 0, |ctx, _| {
                let tick = ctx.tick().as_u64();
                ctx.insert("players", row![tick, 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (changes, _) = run(&mut world, &frame(0));
    assert_eq!(changes.len(), 1);
    assert_eq!(world.store().table("players").unwrap().len(), 1);

    let (changes, _) = run(&mut world, &frame(1));
    assert_eq!(changes.len(), 1);
    assert_eq!(world.store().table("players").unwrap().len(), 2);
}

#[test]
fn systems_run_in_priority_then_id_order() {
    let mut world = fixture(PartitionConfig::new());
    // Registration order is scrambled on purpose; execution order must be
    // (priority, id). Each system logs a marker row; the first system to run
    // gets the lowest storage row id.
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(30), "late", 20, |ctx, _| {
                ctx.insert("logs", row![30u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(10), "early", 5, |ctx, _| {
                ctx.insert("logs", row![10u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(20), "mid", 10, |ctx, _| {
                ctx.insert("logs", row![20u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    run(&mut world, &frame(0));

    // Insert order (i.e. storage RowId order) reveals execution order.
    let scan: Vec<u64> = world
        .store()
        .table("logs")
        .unwrap()
        .scan()
        .map(|(_, r)| r.get(0).unwrap().as_u64().unwrap())
        .collect();
    assert_eq!(scan, vec![10, 20, 30]);
}

#[test]
fn multi_table_tick_commits_atomically() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "two_tables", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                ctx.insert("logs", row![1u64])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (changes, _) = run(&mut world, &frame(0));
    assert_eq!(changes.len(), 2);
    assert_eq!(world.store().table("players").unwrap().len(), 1);
    assert_eq!(world.store().table("logs").unwrap().len(), 1);
}

#[test]
fn world_store_mut_seeds_authoritative_state() {
    let mut world = fixture(PartitionConfig::new());
    let mut tx = Transaction::begin(world.store_mut());
    tx.insert(world.store(), "players", row![99u64, 10u64, 1i32])
        .unwrap();
    tx.commit(world.store_mut()).unwrap();

    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "reader", 0, |ctx, _| {
                let owners = ctx.lookup_unique("players", "primary", &[Value::U64(99)])?;
                assert_eq!(owners.len(), 1);
                let got = ctx.get("players", owners[0])?;
                assert_eq!(got.as_ref().unwrap(), &row![99u64, 10u64, 1i32]);
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    run(&mut world, &frame(0));
}

// ----------------------------------------------------------- read-your-writes

#[test]
fn systems_see_their_own_writes() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "ryw", 0, |ctx, _| {
                // insert -> get
                let handle = ctx.insert("players", row![1u64, 10u64, 100i32])?;
                let got = ctx.get("players", handle)?;
                if got.as_ref() != Some(&row![1u64, 10u64, 100i32]) {
                    return Err(Error::internal("insert not visible to get"));
                }
                if !ctx.contains("players", handle)? {
                    return Err(Error::internal("insert not visible to contains"));
                }
                // update -> get
                ctx.update("players", handle, row![1u64, 10u64, 80i32])?;
                let got = ctx.get("players", handle)?;
                if got.as_ref() != Some(&row![1u64, 10u64, 80i32]) {
                    return Err(Error::internal("update not visible to get"));
                }
                // scan overlays the write set
                let scanned = ctx.scan("players")?;
                if scanned.len() != 1 || scanned[0].1 != row![1u64, 10u64, 80i32] {
                    return Err(Error::internal("scan did not overlay writes"));
                }
                // delete -> get
                ctx.delete("players", handle)?;
                if ctx.get("players", handle)?.is_some() {
                    return Err(Error::internal("delete not visible to get"));
                }
                if !ctx.scan("players")?.is_empty() {
                    return Err(Error::internal("delete not visible to scan"));
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    // insert -> delete is a net no-op: the tick commits zero changes.
    let (changes, _) = run(&mut world, &frame(0));
    assert!(changes.is_empty());
    assert!(world.store().table("players").unwrap().is_empty());
}

// --------------------------------------------------------------- failures

#[test]
fn system_error_aborts_tick_with_zero_mutation() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writes_first", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(1), "fails_second", 1, |_ctx, _| {
                Err(Error::invalid_argument("system rejected the tick"))
            })
            .unwrap(),
        )
        .unwrap();

    let error = world.tick(&frame(0)).unwrap_err();
    assert_eq!(error.tick(), TickId::from_u64(0));
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    // The first system's write must NOT have committed.
    assert!(world.store().table("players").unwrap().is_empty());
    // The tick counter still advanced (time moves on; the tick failed).
    assert_eq!(world.tick_number(), TickId::from_u64(1));

    // A subsequent tick works after removing the failing system.
    world.remove_system(SystemId::from_u64(1)).unwrap();
    let (changes, _) = run(&mut world, &frame(1));
    assert_eq!(changes.len(), 1);
}

#[test]
fn panicking_system_aborts_tick_with_zero_mutation() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(1), "panics", 1, |_ctx, _| {
                panic!("boom");
            })
            .unwrap(),
        )
        .unwrap();

    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::Internal(message) if message.contains("panicked")));
    assert!(matches!(error.error(), Error::Internal(message) if message.contains("boom")));
    assert!(world.store().table("players").unwrap().is_empty());
    assert_eq!(world.tick_number(), TickId::from_u64(1));
}

#[test]
fn events_escape_only_on_commit() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "emitter", 0, |ctx, _| {
                ctx.emit("tick_started", ctx.tick().as_u64())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (_, events) = run(&mut world, &frame(0));
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name(), "tick_started");
    assert_eq!(events[0].payload(), &Value::U64(0));
}

#[test]
fn failed_tick_discards_all_events() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "emit_then_fail", 0, |ctx, _| {
                ctx.emit("doomed", 1u64)?;
                Err(Error::invalid_argument("nope"))
            })
            .unwrap(),
        )
        .unwrap();

    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    // No TickResult was produced, so no events escaped.
    assert!(world.store().table("players").unwrap().is_empty());
}

#[test]
fn event_budget_is_enforced() {
    let config = PartitionConfig::new().with_max_events_per_tick(1);
    let mut world = fixture(config);
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "flood", 0, |ctx, _| {
                ctx.emit("a", 1u64)?;
                ctx.emit("b", 2u64)
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::Capacity(_)));
}

// --------------------------------------------------------------- reducers

#[test]
fn native_reducer_invoked_from_system() {
    let mut world = fixture(PartitionConfig::new());
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
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "invoker", 0, |ctx, _| {
                let value = ctx.invoke_reducer("spawn", &ReducerArgs::new().insert("id", 7u64))?;
                if value != Value::U64(7) {
                    return Err(Error::internal("reducer return value mismatch"));
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (changes, events) = run(&mut world, &frame(0));
    assert_eq!(changes.len(), 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name(), "spawned");
    assert_eq!(events[0].payload(), &Value::U64(7));
    assert_eq!(world.store().table("players").unwrap().len(), 1);
}

#[test]
fn reducer_failure_aborts_the_whole_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .native_mut()
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "reject", |_ctx, _| {
                Err(Error::invalid_argument("rejected"))
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writes_then_invokes", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                ctx.invoke_reducer("reject", &ReducerArgs::new())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    // The system's own write before the reducer call must not commit either.
    assert!(world.store().table("players").unwrap().is_empty());
    assert_eq!(world.tick_number(), TickId::from_u64(1));
}

#[test]
fn missing_reducer_fails_the_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "invoker", 0, |ctx, _| {
                ctx.invoke_reducer("ghost", &ReducerArgs::new())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::NotFound(_)));
}

// ------------------------------------------------------------- wasm reducers

/// WAT helpers for the 3-column `players` table (id, zone, health).
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
  (data (i32.const 90400) "spawned")
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
fn wasm_reducer_invoked_from_system() {
    let mut world = fixture(PartitionConfig::new());
    let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    // spawn: insert players [42, 10, 55], emit "spawned", return 42.
    wasm.register(
        "spawn",
        1,
        wasm_module(
            r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row3 (local.get $p) (i64.const 42) (i64.const 10) (i32.const 55)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 7)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 42))"#,
        ),
    )
    .unwrap();
    world.set_wasm(wasm);
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "wasm_invoker", 0, |ctx, _| {
                let value = ctx.invoke_wasm("spawn", &ReducerArgs::new())?;
                if value != Value::U64(42) {
                    return Err(Error::internal("wasm return value mismatch"));
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let (changes, events) = run(&mut world, &frame(0));
    assert_eq!(changes.len(), 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].name(), "spawned");
    let row = world
        .store()
        .table("players")
        .unwrap()
        .get(RowId::from_u64(0))
        .unwrap();
    assert_eq!(row, &row![42u64, 10u64, 55i32]);
}

#[test]
fn wasm_without_registry_fails_the_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "invoker", 0, |ctx, _| {
                ctx.invoke_wasm("spawn", &ReducerArgs::new())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::NotFound(_)));
}

#[test]
fn trapped_wasm_aborts_the_tick() {
    let mut world = fixture(PartitionConfig::new());
    let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    wasm.register("explode", 1, wasm_module("    (unreachable)"))
        .unwrap();
    world.set_wasm(wasm);
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writes_then_traps", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                ctx.invoke_wasm("explode", &ReducerArgs::new())?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    assert!(world.store().table("players").unwrap().is_empty());
}

// ------------------------------------------------------------ schedule

#[test]
fn scheduled_events_run_at_their_target_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .native_mut()
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "log_mark", |ctx, args| {
                let mark = args.require_u64("mark")?;
                ctx.insert("logs", row![mark])?;
                ctx.emit("logged", mark)?;
                Ok(Value::U64(mark))
            })
            .unwrap(),
        )
        .unwrap();
    world
        .schedule(
            TickId::from_u64(3),
            "log_mark",
            ReducerArgs::new().insert("mark", 3u64),
        )
        .unwrap();
    world
        .schedule(
            TickId::from_u64(1),
            "log_mark",
            ReducerArgs::new().insert("mark", 1u64),
        )
        .unwrap();

    // Ticks 0 and 1: only the tick-1 event fires.
    assert!(run(&mut world, &frame(0)).0.is_empty());
    let (changes, events) = run(&mut world, &frame(1));
    assert_eq!(changes.len(), 1);
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].payload(), &Value::U64(1));

    // Tick 2: nothing due.
    assert!(run(&mut world, &frame(2)).0.is_empty());
    // Tick 3: the tick-3 event fires.
    let (changes, events) = run(&mut world, &frame(3));
    assert_eq!(changes.len(), 1);
    assert_eq!(events[0].payload(), &Value::U64(3));

    let marks: Vec<u64> = world
        .store()
        .table("logs")
        .unwrap()
        .scan()
        .map(|(_, r)| r.get(0).unwrap().as_u64().unwrap())
        .collect();
    assert_eq!(marks, vec![1, 3]);
}

#[test]
fn cancelled_scheduled_events_never_run() {
    let mut world = fixture(PartitionConfig::new());
    world
        .native_mut()
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "log_mark", |ctx, args| {
                let mark = args.require_u64("mark")?;
                ctx.insert("logs", row![mark])?;
                Ok(Value::U64(mark))
            })
            .unwrap(),
        )
        .unwrap();
    let id = world
        .schedule(
            TickId::from_u64(1),
            "log_mark",
            ReducerArgs::new().insert("mark", 9u64),
        )
        .unwrap();
    world.cancel_scheduled(id).unwrap();

    run(&mut world, &frame(0));
    assert!(run(&mut world, &frame(1)).0.is_empty());
    assert!(world.store().table("logs").unwrap().is_empty());
}

#[test]
fn missing_scheduled_reducer_fails_the_tick() {
    let mut world = fixture(PartitionConfig::new());
    world
        .schedule(TickId::from_u64(0), "ghost", ReducerArgs::new())
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    assert!(matches!(error.error(), Error::NotFound(_)));
    assert_eq!(world.tick_number(), TickId::from_u64(1));
}

// --------------------------------------------------------------- inputs

#[test]
fn duplicate_input_commands_are_processed_in_order() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "collector", 0, |ctx, frame| {
                for (index, command) in frame.commands().iter().enumerate() {
                    if command.kind() == "collect" {
                        let payload = command.payload().and_then(Value::as_u64).unwrap();
                        // The mark encodes position and payload, proving the
                        // duplicates were processed in frame order.
                        ctx.insert("logs", row![(index as u64) * 1000 + payload])?;
                    }
                }
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();

    let mut frame = InputFrame::new(TickId::from_u64(0));
    for _ in 0..3 {
        frame.push(InputCommand::new(7, "collect", Some(Value::U64(5))).unwrap());
    }
    let (changes, _) = run(&mut world, &frame);
    assert_eq!(changes.len(), 3);
    let marks: Vec<u64> = world
        .store()
        .table("logs")
        .unwrap()
        .scan()
        .map(|(_, r)| r.get(0).unwrap().as_u64().unwrap())
        .collect();
    assert_eq!(marks, vec![5, 1005, 2005]);
}

#[test]
fn systems_receive_input_commands_in_order() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(
                SystemId::from_u64(0),
                "spawn_from_commands",
                0,
                |ctx, frame| {
                    for command in frame.commands() {
                        if command.kind() == "spawn" {
                            let id = command.payload().and_then(Value::as_u64).unwrap();
                            ctx.insert("players", row![id, 10u64, 100i32])?;
                        }
                    }
                    Ok(())
                },
            )
            .unwrap(),
        )
        .unwrap();

    let mut frame = InputFrame::new(TickId::from_u64(0));
    for id in [7u64, 8, 9] {
        frame.push(InputCommand::new(1, "spawn", Some(Value::U64(id))).unwrap());
    }
    let (changes, _) = run(&mut world, &frame);
    assert_eq!(changes.len(), 3);
    assert_eq!(world.store().table("players").unwrap().len(), 3);
}

#[test]
fn wrong_tick_frame_is_rejected_without_consuming() {
    let mut world = fixture(PartitionConfig::new());
    let error = world.tick(&frame(1)).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    // The frame was rejected before the tick was consumed.
    assert_eq!(world.tick_number(), TickId::from_u64(0));
    // The correct frame then works.
    run(&mut world, &frame(0));
    assert_eq!(world.tick_number(), TickId::from_u64(1));
}

#[test]
fn over_limit_frame_is_rejected() {
    let config = PartitionConfig::new().with_max_commands_per_frame(1);
    let mut world = fixture(config);
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::simple(1, "a").unwrap());
    frame.push(InputCommand::simple(2, "b").unwrap());
    let error = world.tick(&frame).unwrap_err();
    assert!(matches!(error.error(), Error::InvalidArgument(_)));
    assert_eq!(world.tick_number(), TickId::from_u64(0));
}

// ------------------------------------------------------------ determinism

/// One tick's committed (changes, events) pair.
type TickOutcome = (Vec<nexum_storage::Change>, Vec<nexum_reducer::ReducerEvent>);
/// A deterministic trace of per-tick outcomes plus the final store dump.
type SimulationTrace = (Vec<TickOutcome>, Vec<(String, Vec<(RowId, Row)>)>);

/// A rich scenario: RNG usage, multiple systems, a native reducer, and a
/// scheduled event. Returns the per-tick (changes, events) trace.
fn determinism_trace(seed: u64) -> SimulationTrace {
    let mut world = fixture(PartitionConfig::new().with_seed(seed));
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "rng_writer", 10, |ctx, _| {
                let health = ctx.rng().next_below(1000) as i32;
                let tick = ctx.tick().as_u64();
                ctx.insert("players", row![tick, 10u64, health])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(1), "reducer_invoker", 20, |ctx, _| {
                let id = 100 + ctx.tick().as_u64();
                ctx.invoke_reducer("spawn", &ReducerArgs::new().insert("id", id))?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
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
    world
        .schedule(
            TickId::from_u64(3),
            "spawn",
            ReducerArgs::new().insert("id", 999u64),
        )
        .unwrap();

    let mut trace = Vec::new();
    for tick in 0..8u64 {
        let frame = InputFrame::new(TickId::from_u64(tick));
        let result = world.tick(&frame).unwrap();
        trace.push((result.changes().to_vec(), result.events().to_vec()));
    }
    let final_state = dump(world.store());
    (trace, final_state)
}

#[test]
fn same_inputs_same_seed_produce_identical_simulation() {
    // Multiple deterministic seeds, each run twice: identical traces and
    // identical final authoritative state (ADR-009 D4/D5).
    for seed in 0..4u64 {
        let (trace_a, state_a) = determinism_trace(seed);
        let (trace_b, state_b) = determinism_trace(seed);
        assert_eq!(trace_a, trace_b, "seed {seed}: trace diverged");
        assert_eq!(state_a, state_b, "seed {seed}: final state diverged");
    }

    // And the trace is not trivially empty: 8 ticks of real work happened.
    let (trace_a, _): SimulationTrace = determinism_trace(1234);
    assert_eq!(trace_a.len(), 8);
    let total_changes: usize = trace_a.iter().map(|(c, _)| c.len()).sum();
    assert!(
        total_changes > 10,
        "expected substantial work, got {total_changes} changes"
    );
}

#[test]
fn different_seeds_produce_different_rng_streams() {
    let (trace_a, _) = determinism_trace(1);
    let (trace_b, _) = determinism_trace(2);
    assert_ne!(trace_a, trace_b);
}

#[test]
fn rng_values_are_deterministic_across_runs() {
    let mut a = fixture(PartitionConfig::new().with_seed(7));
    let mut b = fixture(PartitionConfig::new().with_seed(7));
    let system = SystemDefinition::new(SystemId::from_u64(0), "rng_writer", 0, |ctx, _| {
        let health = ctx.rng().next_below(10_000) as i32;
        ctx.insert("players", row![ctx.tick().as_u64(), 10u64, health])?;
        Ok(())
    })
    .unwrap();
    a.add_system(system.clone()).unwrap();
    b.add_system(system).unwrap();
    for tick in 0..5u64 {
        run(&mut a, &frame(tick));
        run(&mut b, &frame(tick));
    }
    assert_eq!(dump(a.store()), dump(b.store()));
}

// ------------------------------------------------------------ misc

#[test]
fn config_rejects_zero_bounds() {
    assert!(
        PartitionConfig::new()
            .with_max_commands_per_frame(0)
            .validate()
            .is_err()
    );
    assert!(
        PartitionConfig::new()
            .with_max_events_per_tick(0)
            .validate()
            .is_err()
    );
    assert!(
        PartitionConfig::new()
            .with_max_scheduled_events(0)
            .validate()
            .is_err()
    );
    assert!(PartitionConfig::new().validate().is_ok());
}

#[test]
fn tick_result_carries_tx_id() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                ctx.insert("players", row![1u64, 10u64, 100i32])?;
                Ok(())
            })
            .unwrap(),
        )
        .unwrap();
    let result = world.tick(&frame(0)).unwrap();
    assert_eq!(result.tick(), TickId::from_u64(0));
    let tx_id: TransactionId = result.tx_id();
    // The store's transaction allocator gave this tick a real transaction id.
    assert_eq!(tx_id.as_u64(), 0);
}

#[test]
fn tick_error_is_a_std_error() {
    let mut world = fixture(PartitionConfig::new());
    world
        .add_system(
            SystemDefinition::new(SystemId::from_u64(0), "fails", 0, |_ctx, _| {
                Err(Error::invalid_argument("nope"))
            })
            .unwrap(),
        )
        .unwrap();
    let error = world.tick(&frame(0)).unwrap_err();
    let message = error.to_string();
    assert!(message.contains("tick 0 failed"));
    assert!(std::error::Error::source(&error).is_some());
}
