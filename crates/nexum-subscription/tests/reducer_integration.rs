//! Integration (Phase 8 brief §19 "Reducer integration"): native and WASM
//! reducers must produce **identical subscription semantics**. Both converge
//! on the same `Vec<Change>` commit boundary; the subscription engine is
//! source-agnostic. A failed reducer produces no committed changes, so it
//! produces no deltas either.

use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_core::{ColumnType, ReducerId, Value};
use nexum_reducer::{ReducerArgs, ReducerDefinition, ReducerRegistry};
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

fn world() -> TableStore {
    let mut store = TableStore::new();
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .column("health", ColumnType::I32)
                .column("level", ColumnType::U32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

fn zone10_subscription(
    store: &TableStore,
) -> (SubscriptionRegistry, nexum_subscription::SubscriptionId) {
    let mut registry = SubscriptionRegistry::new();
    let sub = registry
        .subscribe(
            store,
            Query::builder("players")
                .predicate_eq("zone_id", 10u64)
                .build()
                .unwrap(),
        )
        .unwrap();
    registry.drain(sub).unwrap(); // consume the Initial snapshot
    (registry, sub)
}

// ------------------------------------------------- native reducer -> sub

#[test]
fn native_reducer_changes_reach_subscriptions() {
    let mut store = world();
    let (mut registry, sub) = zone10_subscription(&store);

    let mut reducers = ReducerRegistry::new();
    reducers
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "spawn_player", |ctx, args| {
                let id = args.require_u64("id")?;
                let zone = args.require_u64("zone")?;
                let _ = ctx.insert("players", row![id, zone, 100i32, 5u32])?;
                ctx.emit("spawned", id)?;
                Ok(Value::U64(id))
            })
            .unwrap(),
        )
        .unwrap();

    let result = reducers
        .invoke(
            &mut store,
            "spawn_player",
            &ReducerArgs::new().insert("id", 7u64).insert("zone", 10u64),
        )
        .unwrap();

    // The committed changes flow into the subscription engine exactly like
    // any other commit.
    let report = registry.apply_changes(&store, result.changes());
    assert_eq!(report.affected(), &[sub]);

    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Insert { seq, row } => {
            assert_eq!(*seq, report.seq());
            assert_eq!(
                row.row()
                    .get_named(store.table("players").unwrap().schema(), "health"),
                Some(&Value::I32(100))
            );
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn rejected_native_reducer_produces_no_deltas() {
    let mut store = world();
    let (mut registry, sub) = zone10_subscription(&store);

    let mut reducers = ReducerRegistry::new();
    reducers
        .register(
            ReducerDefinition::new(ReducerId::from_u64(0), "reject", |_ctx, _args| {
                Err(nexum_core::Error::invalid_argument("nope"))
            })
            .unwrap(),
        )
        .unwrap();

    assert!(
        reducers
            .invoke(&mut store, "reject", &ReducerArgs::new())
            .is_err()
    );
    // The rejection aborted the transaction: no committed changes, so no
    // deltas and no sequence advancement.
    assert!(registry.drain(sub).unwrap().is_empty());
    assert_eq!(registry.next_seq(), 0);
    assert!(store.table("players").unwrap().is_empty());
}

// -------------------------------------------------- wasm reducer -> sub

const HELPERS: &str = r#"
  (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
    (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
  (func $put_value_u64 (param $p i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $p) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 1)) (local.get $v))
    (i32.add (local.get $p) (i32.const 9)))
  (func $put_row4 (param $p i32) (param $id i64) (param $zone i64) (param $health i32) (param $level i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.const 4))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 8)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 9)) (local.get $id))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 17)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 18)) (local.get $zone))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 26)) (i32.const 3))
    (i32.store align=1 (i32.add (local.get $p) (i32.const 27)) (local.get $health))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 31)) (i32.const 7))
    (i32.store align=1 (i32.add (local.get $p) (i32.const 32)) (local.get $level))
    (i32.add (local.get $p) (i32.const 36)))
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
fn wasm_reducer_changes_reach_subscriptions() {
    let mut store = world();
    let (mut registry, sub) = zone10_subscription(&store);

    let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    // spawn: insert players [42, 10, 100, 5].
    wasm.register(
        "spawn",
        1,
        wasm_module(
            r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 42) (i64.const 10) (i32.const 100) (i32.const 5)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 7)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 42))"#,
        ),
    )
    .unwrap();

    let result = wasm
        .invoke(&mut store, "spawn", &ReducerArgs::new())
        .unwrap();
    let report = registry.apply_changes(&store, result.changes());
    assert_eq!(report.affected(), &[sub]);

    let updates = registry.drain(sub).unwrap();
    assert_eq!(updates.len(), 1);
    match &updates[0] {
        SubscriptionUpdate::Insert { seq, row } => {
            assert_eq!(*seq, report.seq());
            assert_eq!(row.row_id().as_u64(), 0);
            assert_eq!(
                row.row()
                    .get_named(store.table("players").unwrap().schema(), "zone_id"),
                Some(&Value::U64(10))
            );
        }
        other => panic!("expected Insert, got {other:?}"),
    }
}

#[test]
fn trapped_wasm_reducer_produces_no_deltas() {
    let mut store = world();
    let (mut registry, sub) = zone10_subscription(&store);

    let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    wasm.register("explode", 1, wasm_module(r#"    (unreachable)"#))
        .unwrap();

    assert!(
        wasm.invoke(&mut store, "explode", &ReducerArgs::new())
            .is_err()
    );
    // The trap aborted the transaction: no changes ever reached the commit
    // boundary, so the subscription saw nothing and no sequence advanced.
    assert!(registry.drain(sub).unwrap().is_empty());
    assert_eq!(registry.next_seq(), 0);
    assert!(store.table("players").unwrap().is_empty());
}
