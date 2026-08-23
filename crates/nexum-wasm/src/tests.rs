//! WASM reducer tests (Phase 7 brief §14): security, resource limits,
//! correctness, transaction semantics, failure paths, determinism.
//!
//! Test modules are written in WAT and embed small guest-side helpers that
//! build ABI op arguments in the module's input buffer and decode the result
//! envelopes in its output buffer — the exact wire format documented in
//! `abi.rs`. Genuine OCC conflicts are tested at the boundary the host uses
//! (`ReducerContext` calls), mirroring the Phase 6 boundary tests: a
//! single-threaded `invoke` cannot race an external transaction.

use nexum_core::{ColumnType, Error, RowId, TableSchema, TransactionId, Value};
use nexum_reducer::{ReducerArgs, ReducerContext};
use nexum_table::{TableStore, row};
use nexum_tx::Transaction;

use crate::{WasmLimits, WasmModuleRegistry};

/// Input-buffer base for the static `players` string in test modules.
const STR_PLAYERS: i32 = 90000;

/// The guest-side helper functions shared by every test module.
const HELPERS: &str = r#"
  (func $put_u64 (param $p i32) (param $v i64) (result i32)
    (i64.store align=1 (local.get $p) (local.get $v))
    (i32.add (local.get $p) (i32.const 8)))
  (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
    (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
  (func $put_value_u64 (param $p i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $p) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 1)) (local.get $v))
    (i32.add (local.get $p) (i32.const 9)))
  (func $put_value_u32 (param $p i32) (param $v i32) (result i32)
    (i32.store8 align=1 (local.get $p) (i32.const 7))
    (i32.store align=1 (i32.add (local.get $p) (i32.const 1)) (local.get $v))
    (i32.add (local.get $p) (i32.const 5)))
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
  (func $put_row2 (param $p i32) (param $owner i64) (param $coins i64) (result i32)
    (i64.store align=1 (local.get $p) (i64.const 2))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 8)) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 9)) (local.get $owner))
    (i32.store8 align=1 (i32.add (local.get $p) (i32.const 17)) (i32.const 4))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 18)) (local.get $coins))
    (i32.add (local.get $p) (i32.const 26)))
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))
  (func $ret_u64 (param $v i64) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 8))
    (i64.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 9))
  (func $ret_i32 (param $v i32) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 3))
    (i32.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 5))
  (func $ret_bool (param $v i32) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 0))
    (i32.store8 align=1 (i32.const 16385) (local.get $v))
    (i32.const 2))
  (func $get_present (result i32)
    (i32.load8_u align=1 (i32.const 16392)))
  (func $get_first_u64 (result i64)
    (if (result i64)
      (i32.load8_u align=1 (i32.const 16392))
      (then (i64.load align=1 (i32.const 16402)))
      (else (i64.const -1))))
  (func $get_health (result i32)
    (if (result i32)
      (i32.load8_u align=1 (i32.const 16392))
      (then (i32.load align=1 (i32.const 16420)))
      (else (i32.const -1))))
  (func $insert_id (result i64)
    (i64.load align=1 (i32.const 16392)))
  (func $scan_count (result i64)
    (i64.load align=1 (i32.const 16392)))
  (func $lookup_count (result i64)
    (i64.load align=1 (i32.const 16392)))
  (func $contains_flag (result i32)
    (i32.load8_u align=1 (i32.const 16392)))
"#;

/// Builds a test module wrapping `body` (the `_nexum_reducer_run` body).
fn module(body: &str) -> Vec<u8> {
    module_with_memory(16, body)
}

/// Builds a test module with `mem_pages` initial memory pages.
fn module_with_memory(mem_pages: u32, body: &str) -> Vec<u8> {
    let wat = format!(
        r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") {mem_pages})
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
  (data (i32.const 90100) "by_level")
  (data (i32.const 90200) "economy")
  (data (i32.const 90300) "by_zone")
  (data (i32.const 90400) "hello")
  (data (i32.const 90500) "nope")
{helpers}
  (func (export "_nexum_reducer_run") (result i32)
{body}))
"#,
        mem_pages = mem_pages,
        helpers = HELPERS,
        body = body
    );
    wat::parse_str(&wat).expect("test module is valid WAT")
}

fn register(registry: &mut WasmModuleRegistry, name: &str, body: &str) {
    registry.register(name, 1, module(body)).unwrap();
}

fn registry() -> WasmModuleRegistry {
    WasmModuleRegistry::new(WasmLimits::default()).unwrap()
}

/// Players `[id, zone_id, health, level]` (unique `by_level`) + economy
/// `[owner, coins]`.
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
                .index("by_zone", &["zone_id"])
                .unique_index("by_level", &["level"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
        .create_table(
            TableSchema::builder("economy")
                .column("owner", ColumnType::U64)
                .column("coins", ColumnType::I64)
                .primary_key(&["owner"])
                .build()
                .unwrap(),
        )
        .unwrap();
    store
}

/// Alice (row id 0, id=1) and Bob (row id 1, id=2); change buffers drained.
fn seeded() -> (TableStore, RowId, RowId) {
    let mut store = world();
    let alice = store
        .table_mut("players")
        .unwrap()
        .insert(row![1u64, 10u64, 100i32, 5u32])
        .unwrap();
    let bob = store
        .table_mut("players")
        .unwrap()
        .insert(row![2u64, 20u64, 90i32, 6u32])
        .unwrap();
    store
        .table_mut("economy")
        .unwrap()
        .insert(row![1u64, 100i64])
        .unwrap();
    store.drain_changes();
    (store, alice, bob)
}

// ------------------------------------------- WAT body builders

/// `GET players row_id` → returns the row's first column (`id`), or -1.
fn get_first_body(row_id: u64) -> String {
    format!(
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const {STR_PLAYERS}) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const {row_id})))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_u64 (call $get_first_u64))"#
    )
}

fn contains_body(row_id: u64) -> String {
    format!(
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const {STR_PLAYERS}) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const {row_id})))
    (drop (call $call_op (i32.const 2) (local.get $p)))
    (call $ret_bool (call $contains_flag))"#
    )
}

/// `INSERT players [id, zone, health, level]` and return `id`.
fn insert_body(id: u64, zone: u64, health: i32, level: u32) -> String {
    format!(
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const {STR_PLAYERS}) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const {id}) (i64.const {zone}) (i32.const {health}) (i32.const {level})))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const {id}))"#
    )
}

// ------------------------------------------------------------- basic

#[test]
fn invoke_runs_a_module_and_returns_its_encoded_value() {
    let mut store = world();
    let mut registry = registry();
    register(&mut registry, "ping", "    (call $ret_u64 (i64.const 42))");

    let result = registry
        .invoke(&mut store, "ping", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::U64(42));
    assert!(result.changes().is_empty());
    assert!(result.events().is_empty());
    assert_eq!(result.tx_id(), TransactionId::from_u64(0));
}

#[test]
fn invoke_unknown_module_is_not_found() {
    let mut store = world();
    let registry = registry();
    let err = registry
        .invoke(&mut store, "nope", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
}

#[test]
fn registry_rejects_duplicate_names() {
    let mut registry = registry();
    register(&mut registry, "a", "    (call $ret_u64 (i64.const 0))");
    let err = registry
        .register("a", 2, module("    (call $ret_u64 (i64.const 1))"))
        .unwrap_err();
    assert!(matches!(err, Error::AlreadyExists(_)));
    assert_eq!(registry.len(), 1);
}

#[test]
fn registry_lists_deterministically_by_name() {
    let mut registry = registry();
    register(&mut registry, "zeta", "    (call $ret_u64 (i64.const 2))");
    register(&mut registry, "alpha", "    (call $ret_u64 (i64.const 0))");
    register(&mut registry, "mid", "    (call $ret_u64 (i64.const 1))");

    let names: Vec<&str> = registry.list().map(|m| m.name()).collect();
    assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    assert_eq!(
        registry.list().map(|m| m.version()).collect::<Vec<_>>(),
        vec![1, 1, 1]
    );
    assert!(registry.contains("mid"));
}

// ------------------------------------------------------------- security

#[test]
fn registration_rejects_wasi_imports() {
    let mut registry = registry();
    let wasi = r#"(module
        (import "wasi_snapshot_preview1" "proc_exit" (func $exit (param i32)))
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("wasi", 1, wat::parse_str(wasi).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(
        err.to_string().contains("wasi_snapshot_preview1"),
        "the WASI module name must surface in the rejection: {err}"
    );
    assert!(registry.is_empty());
}

#[test]
fn registration_rejects_extra_imports() {
    let mut registry = registry();
    let wat = r#"(module
        (import "env" "x" (func $x))
        (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("two", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn registration_rejects_no_imports_at_all() {
    let mut registry = registry();
    let wat = r#"(module
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("none", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn registration_rejects_missing_entry_function() {
    let mut registry = registry();
    let wat = r#"(module
        (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func $helper (result i32) (i32.const 0)))"#;
    let err = registry
        .register("no-entry", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("_nexum_reducer_run"));
}

#[test]
fn registration_rejects_wrong_export_kinds() {
    let mut registry = registry();
    // `_nexum_in_ptr` is a *function*, not an immutable i32 global.
    let wat = r#"(module
        (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 16)
        (func (export "_nexum_in_ptr") (result i32) (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("bad-kind", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("_nexum_in_ptr"));
}

#[test]
fn registration_rejects_buffer_pointers_outside_memory() {
    let mut registry = registry();
    let wat = r#"(module
        (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 100000000))
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("oob", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("buffer pointers"));
}

#[test]
fn registration_rejects_a_start_function_that_touches_state() {
    let mut registry = registry();
    let wat = r#"(module
        (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
        (memory (export "memory") 16)
        (global (export "_nexum_in_ptr") i32 (i32.const 0))
        (global (export "_nexum_out_ptr") i32 (i32.const 16384))
        (func $start_probe
          (drop (call $op (i32.const 1) (i32.const 0) (i32.const 0) (i32.const 16384) (i32.const 65536))))
        (start $start_probe)
        (func (export "_nexum_reducer_run") (result i32) (i32.const 0)))"#;
    let err = registry
        .register("startful", 1, wat::parse_str(wat).unwrap())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("start function"));
}

#[test]
fn registration_rejects_oversized_bytecode() {
    let mut registry = registry();
    // 1 MiB + 1 of zeros is over the cap; the size check fires before parse.
    let err = registry
        .register("huge", 1, vec![0u8; 1024 * 1024 + 1])
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
}

// ------------------------------------------------------------- limits

#[test]
fn fuel_exhaustion_aborts_with_zero_mutation() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    // The `unreachable` makes the function end unreachable so the loop
    // (which never returns) type-checks in a `(result i32)` function.
    register(
        &mut registry,
        "loop",
        "    (loop $l (br $l))\n    (unreachable)",
    );

    let err = registry
        .invoke(&mut store, "loop", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
    assert!(err.to_string().contains("fuel"));
    assert_eq!(store.table("players").unwrap().len(), 2);
    assert!(store.drain_changes().is_empty());
}

#[test]
fn memory_growth_beyond_the_ceiling_is_blocked() {
    let mut store = world();
    let mut registry_with = registry();
    // 64 pages is exactly the 4 MiB ceiling; growing one more page must fail.
    let bytes = module_with_memory(64, "    (call $ret_i32 (memory.grow (i32.const 1)))");
    registry_with.register("grow", 1, bytes).unwrap();
    let result = registry_with
        .invoke(&mut store, "grow", &ReducerArgs::new())
        .unwrap();
    // memory.grow failed → -1 (the limiter blocked it, deterministically).
    assert_eq!(result.return_value(), &Value::I32(-1));
}

#[test]
fn memory_above_the_ceiling_fails_registration() {
    let limits = WasmLimits {
        max_memory_bytes: 128 * 1024, // 2 pages
        ..WasmLimits::default()
    };
    let mut registry = WasmModuleRegistry::new(limits).unwrap();
    let err = registry
        .register("big-mem", 1, module("    (call $ret_u64 (i64.const 0))"))
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
}

#[test]
fn host_call_budget_aborts() {
    let (mut store, _alice, _bob) = seeded();
    let limits = WasmLimits {
        max_host_calls: 2,
        ..WasmLimits::default()
    };
    let mut registry = WasmModuleRegistry::new(limits).unwrap();
    // Three ops in one module: insert, insert, insert.
    let three_ops = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    registry.register("chatty", 1, module(three_ops)).unwrap();

    let err = registry
        .invoke(&mut store, "chatty", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
    assert!(err.to_string().contains("host-call budget"));
    assert_eq!(store.table("players").unwrap().len(), 2);
}

#[test]
fn oversized_event_payload_aborts() {
    let (mut store, _alice, _bob) = seeded();
    let limits = WasmLimits {
        max_event_bytes: 10,
        ..WasmLimits::default()
    };
    let mut registry = WasmModuleRegistry::new(limits).unwrap();
    let emit = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    registry.register("loud", 1, module(emit)).unwrap();

    let err = registry
        .invoke(&mut store, "loud", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
    assert!(err.to_string().contains("event"));
}

#[test]
fn oversized_return_value_aborts() {
    let mut store = world();
    let limits = WasmLimits {
        max_result_bytes: 4,
        ..WasmLimits::default()
    };
    let mut registry = WasmModuleRegistry::new(limits).unwrap();
    registry
        .register("big-ret", 1, module("    (call $ret_u64 (i64.const 42))"))
        .unwrap();

    let err = registry
        .invoke(&mut store, "big-ret", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
    assert!(err.to_string().contains("return value"));
}

#[test]
fn oversized_arguments_abort_before_execution() {
    let mut store = world();
    let limits = WasmLimits {
        max_args_bytes: 8,
        ..WasmLimits::default()
    };
    let mut registry = WasmModuleRegistry::new(limits).unwrap();
    registry
        .register("small", 1, module("    (call $ret_u64 (i64.const 0))"))
        .unwrap();

    let args = ReducerArgs::new().insert("player_id", 7u64);
    let err = registry.invoke(&mut store, "small", &args).unwrap_err();
    assert!(matches!(err, Error::Capacity(_)));
}

// ------------------------------------------------------------- correctness

#[test]
fn wasm_get_reads_a_committed_row() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    register(&mut registry, "get", &get_first_body(0));

    let result = registry
        .invoke(&mut store, "get", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::U64(1), "alice's id column");
}

#[test]
fn wasm_contains_checks_existence() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    register(&mut registry, "has", &contains_body(0));
    register(&mut registry, "lacks", &contains_body(99));

    let result = registry
        .invoke(&mut store, "has", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::Bool(true));
    let result = registry
        .invoke(&mut store, "lacks", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::Bool(false));
}

#[test]
fn wasm_insert_writes_a_row() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    register(&mut registry, "spawn", &insert_body(9, 40, 100, 9));

    let result = registry
        .invoke(&mut store, "spawn", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.changes().len(), 1);
    assert!(matches!(
        result.changes()[0].kind(),
        nexum_core::ChangeKind::Insert
    ));
    let players = store.table("players").unwrap();
    assert_eq!(players.len(), 3);
    assert!(
        players
            .get_by_primary_key(&[Value::U64(9)])
            .unwrap()
            .is_some()
    );
    assert_eq!(
        players.lookup("by_zone", &[Value::U64(40)]).unwrap().len(),
        1
    );
}

#[test]
fn wasm_update_modifies_a_row_and_indexes() {
    let (mut store, alice, _bob) = seeded();
    let mut registry = registry();
    let update = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 1) (i64.const 30) (i32.const 5) (i32.const 5)))
    (drop (call $call_op (i32.const 6) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "move", update);

    registry
        .invoke(&mut store, "move", &ReducerArgs::new())
        .unwrap();
    let players = store.table("players").unwrap();
    let row = players.get(alice).unwrap();
    assert_eq!(row.values()[2], Value::I32(5));
    assert_eq!(row.values()[1], Value::U64(30));
    // The derived index followed the update.
    assert_eq!(
        players.lookup("by_zone", &[Value::U64(30)]).unwrap(),
        vec![alice]
    );
    assert!(
        players
            .lookup("by_zone", &[Value::U64(10)])
            .unwrap()
            .is_empty()
    );
}

#[test]
fn wasm_delete_removes_a_row() {
    let (mut store, alice, bob) = seeded();
    let mut registry = registry();
    let delete = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (drop (call $call_op (i32.const 7) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "remove", delete);

    registry
        .invoke(&mut store, "remove", &ReducerArgs::new())
        .unwrap();
    let players = store.table("players").unwrap();
    assert_eq!(players.len(), 1);
    assert!(players.get(alice).is_none());
    assert!(players.get(bob).is_some());
}

#[test]
fn wasm_scan_counts_rows() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let scan = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (drop (call $call_op (i32.const 3) (local.get $p)))
    (call $ret_u64 (call $scan_count))"#;
    register(&mut registry, "count", scan);

    let result = registry
        .invoke(&mut store, "count", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::U64(2));
}

#[test]
fn wasm_lookup_unique_finds_owners() {
    let (mut store, _alice, bob) = seeded();
    let mut registry = registry();
    let lookup = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_str (local.get $p) (i32.const 90100) (i32.const 8)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 1)))
    (local.set $p (call $put_value_u32 (local.get $p) (i32.const 6)))
    (drop (call $call_op (i32.const 4) (local.get $p)))
    (call $ret_u64 (call $lookup_count))"#;
    register(&mut registry, "levels", lookup);

    let result = registry
        .invoke(&mut store, "levels", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::U64(1), "bob owns level 6");
    assert_eq!(
        store
            .table("players")
            .unwrap()
            .lookup_unique("by_level", &[Value::U32(6)])
            .unwrap(),
        vec![bob]
    );
}

#[test]
fn wasm_emit_buffers_events_transaction_locally() {
    let mut store = world();
    let mut registry = registry();
    let emit = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "cheer", emit);

    let result = registry
        .invoke(&mut store, "cheer", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.events().len(), 1);
    assert_eq!(result.events()[0].name(), "hello");
    assert_eq!(result.events()[0].payload(), &Value::U64(42));
    assert!(result.changes().is_empty());
}

// ------------------------------------------------------------- read-your-writes

#[test]
fn wasm_read_your_writes_insert_then_get() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 7) (i64.const 10) (i32.const 100) (i32.const 7)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (call $insert_id)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_u64 (call $get_first_u64))"#;
    register(&mut registry, "ryw_insert", body);

    let result = registry
        .invoke(&mut store, "ryw_insert", &ReducerArgs::new())
        .unwrap();
    assert_eq!(
        result.return_value(),
        &Value::U64(7),
        "the pending insert is visible"
    );
    assert_eq!(result.changes().len(), 1);
}

#[test]
fn wasm_read_your_writes_insert_update_get() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (call $insert_id)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 5) (i32.const 9)))
    (drop (call $call_op (i32.const 6) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (call $insert_id)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_i32 (call $get_health))"#;
    register(&mut registry, "ryw_iu", body);

    let result = registry
        .invoke(&mut store, "ryw_iu", &ReducerArgs::new())
        .unwrap();
    assert_eq!(
        result.return_value(),
        &Value::I32(5),
        "the updated health is visible"
    );
    assert_eq!(result.changes().len(), 1);
}

#[test]
fn wasm_read_your_writes_insert_delete_get() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (call $insert_id)))
    (drop (call $call_op (i32.const 7) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (call $insert_id)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_bool (call $get_present))"#;
    register(&mut registry, "ryw_id", body);

    let result = registry
        .invoke(&mut store, "ryw_id", &ReducerArgs::new())
        .unwrap();
    assert_eq!(
        result.return_value(),
        &Value::Bool(false),
        "deleted in the transaction view"
    );
    assert!(
        result.changes().is_empty(),
        "insert→delete netted to nothing"
    );
}

#[test]
fn wasm_read_your_writes_update_then_get() {
    let (mut store, alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 1) (i64.const 10) (i32.const 5) (i32.const 5)))
    (drop (call $call_op (i32.const 6) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_i32 (call $get_health))"#;
    register(&mut registry, "ryw_update", body);

    let result = registry
        .invoke(&mut store, "ryw_update", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::I32(5));
    assert_eq!(
        store.table("players").unwrap().get(alice).unwrap().values()[2],
        Value::I32(5)
    );
}

#[test]
fn wasm_read_your_writes_delete_then_get() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (drop (call $call_op (i32.const 7) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    (call $ret_bool (call $get_present))"#;
    register(&mut registry, "ryw_delete", body);

    let result = registry
        .invoke(&mut store, "ryw_delete", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.return_value(), &Value::Bool(false));
    assert_eq!(store.table("players").unwrap().len(), 1);
}

// ------------------------------------------------------------- transaction semantics

#[test]
fn wasm_unique_key_violation_aborts_with_zero_mutation() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 7) (i64.const 10) (i32.const 100) (i32.const 5)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 7) (i64.const 20) (i32.const 80) (i32.const 6)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "cheat", body);

    let err = registry
        .invoke(&mut store, "cheat", &ReducerArgs::new())
        .unwrap_err();
    assert!(
        matches!(err, Error::AlreadyExists(_)),
        "commit validation failed: {err}"
    );
    assert_eq!(
        store.table("players").unwrap().len(),
        2,
        "zero authoritative mutation"
    );
}

#[test]
fn wasm_multi_table_commit_is_atomic() {
    let mut store = world();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90200) (i32.const 7)))
    (local.set $p (call $put_row2 (local.get $p) (i64.const 1) (i64.const 100)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "trade", body);

    let result = registry
        .invoke(&mut store, "trade", &ReducerArgs::new())
        .unwrap();
    assert_eq!(result.changes().len(), 2);
    // Deterministic commit order: table id 0 (players) before table id 1.
    assert_eq!(
        result.changes()[0].table_id(),
        nexum_core::TableId::from_u64(0)
    );
    assert_eq!(
        result.changes()[1].table_id(),
        nexum_core::TableId::from_u64(1)
    );
    assert_eq!(result.events().len(), 1);
    assert_eq!(store.table("players").unwrap().len(), 1);
    assert_eq!(store.table("economy").unwrap().len(), 1);
}

#[test]
fn wasm_multi_table_abort_leaves_nothing() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90500) (i32.const 4)))
    (drop (call $call_op (i32.const 3) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    register(&mut registry, "ghost", body);

    let err = registry
        .invoke(&mut store, "ghost", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::NotFound(_)));
    assert_eq!(
        store.table("players").unwrap().len(),
        2,
        "players untouched"
    );
    assert!(store.drain_changes().is_empty());
}

#[test]
fn wasm_scan_conflicts_with_an_external_mutation() {
    // Boundary test: the host translates `SCAN` into exactly this
    // `ReducerContext::scan` call, which records a table-epoch observation.
    let (mut store, _alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.scan("players").unwrap();
    }
    store
        .table_mut("players")
        .unwrap()
        .insert(row![9u64, 40u64, 10i32, 9u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn wasm_point_read_conflicts_with_an_external_write() {
    let (mut store, alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.get("players", alice).unwrap();
    }
    store
        .table_mut("players")
        .unwrap()
        .update(alice, row![1u64, 10u64, 42i32, 5u32])
        .unwrap();
    store.drain_changes();

    let err = tx.commit(&mut store).unwrap_err();
    assert!(matches!(err, Error::Conflict(_)));
}

#[test]
fn wasm_unrelated_table_change_does_not_conflict() {
    let (mut store, alice, _bob) = seeded();
    let mut tx = Transaction::begin(&mut store);
    {
        let mut ctx = ReducerContext::new(&mut tx, &store);
        ctx.get("players", alice).unwrap();
    }
    store
        .table_mut("economy")
        .unwrap()
        .update(RowId::from_u64(0), row![1u64, 999i64])
        .unwrap();
    store.drain_changes();

    let changes = tx.commit(&mut store).unwrap();
    assert!(changes.is_empty(), "no conflict for an untouched table");
}

// ------------------------------------------------------------- failure paths

#[test]
fn wasm_trap_aborts_with_zero_mutation_and_no_events() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (unreachable)"#;
    register(&mut registry, "explode", body);

    let err = registry
        .invoke(&mut store, "explode", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("trapped"));
    assert_eq!(store.table("players").unwrap().len(), 2, "zero mutation");
    assert!(store.drain_changes().is_empty());
}

#[test]
fn wasm_out_of_bounds_memory_access_aborts() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    register(
        &mut registry,
        "oob-read",
        "    (drop (i32.load align=1 (i32.const 999999999)))\n    (call $ret_u64 (i64.const 0))",
    );

    let err = registry
        .invoke(&mut store, "oob-read", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(store.table("players").unwrap().len(), 2);
}

#[test]
fn wasm_application_rejection_carries_the_guest_message() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    let body = r#"    (i32.store align=1 (i32.const 16384) (i32.const 4))
    (memory.copy (i32.const 16388) (i32.const 90500) (i32.const 4))
    (i32.const -1)"#;
    register(&mut registry, "veto", body);

    let err = registry
        .invoke(&mut store, "veto", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert_eq!(err.to_string(), "invalid argument: nope");
    assert_eq!(store.table("players").unwrap().len(), 2, "rejection aborts");
}

#[test]
fn wasm_malformed_return_value_aborts() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    // Writes an unknown column-type tag (200) as the return value.
    register(
        &mut registry,
        "garbage",
        "    (i32.store8 align=1 (i32.const 16384) (i32.const 200))\n    (i32.const 1)",
    );

    let err = registry
        .invoke(&mut store, "garbage", &ReducerArgs::new())
        .unwrap_err();
    assert!(matches!(err, Error::InvalidArgument(_)));
    assert!(err.to_string().contains("malformed"));
}

// ------------------------------------------------------------- determinism

#[test]
fn same_module_and_state_produce_identical_results() {
    let run = || {
        let (mut store, _alice, _bob) = seeded();
        let mut registry = registry();
        let body = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 42) (i64.const 10) (i32.const 100) (i32.const 7)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 42))"#;
        registry.register("spawn", 1, module(body)).unwrap();
        let args = ReducerArgs::new().insert("x", 1u64);
        registry.invoke(&mut store, "spawn", &args).unwrap()
    };

    let first = run();
    let second = run();
    assert_eq!(first.return_value(), second.return_value());
    assert_eq!(first.changes(), second.changes());
    assert_eq!(first.events(), second.events());
    assert_eq!(first.tx_id(), second.tx_id());
}

#[test]
fn committed_changes_carry_real_row_ids() {
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();
    register(&mut registry, "spawn", &insert_body(9, 40, 100, 9));

    let result = registry
        .invoke(&mut store, "spawn", &ReducerArgs::new())
        .unwrap();
    // The guest saw a provisional handle; the change carries the real id.
    assert_eq!(
        result.changes()[0].row_id(),
        RowId::from_u64(2),
        "next storage id"
    );
}

#[test]
fn malicious_value_count_is_a_clean_error_not_a_panic() {
    // A crafted module claims a row of u64::MAX values (or a lookup key of
    // u64::MAX entries). The decoders use `try_reserve`, so this must be a
    // clean `InvalidArgument` (sticky → abort) — never a capacity-overflow
    // panic or an OOM abort of the host (brief §9: malformed input returns
    // an error, not a panic).
    let (mut store, _alice, _bob) = seeded();
    let mut registry = registry();

    let huge_insert = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (i64.store align=1 (local.get $p) (i64.const -1))
    (local.set $p (i32.add (local.get $p) (i32.const 8)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    registry
        .register("huge-insert", 1, module(huge_insert))
        .unwrap();

    let huge_lookup = r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_str (local.get $p) (i32.const 90100) (i32.const 8)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const -1)))
    (local.set $p (call $put_value_u32 (local.get $p) (i32.const 6)))
    (drop (call $call_op (i32.const 4) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#;
    registry
        .register("huge-lookup", 1, module(huge_lookup))
        .unwrap();

    for name in ["huge-insert", "huge-lookup"] {
        let err = registry
            .invoke(&mut store, name, &ReducerArgs::new())
            .unwrap_err();
        assert!(
            matches!(err, Error::InvalidArgument(_)),
            "{name} must fail cleanly, got: {err}"
        );
    }
    assert_eq!(store.table("players").unwrap().len(), 2, "zero mutation");
}

#[test]
fn arguments_are_written_into_the_input_buffer_unconditionally() {
    // A module that reads nothing still receives its args (encoded) in the
    // input buffer; the write is bounded and precedes the entry call.
    let mut store = world();
    let mut registry = registry();
    register(&mut registry, "ping", "    (call $ret_u64 (i64.const 42))");

    let args = ReducerArgs::new()
        .insert("name", "alice")
        .insert("level", 9u64);
    let result = registry.invoke(&mut store, "ping", &args).unwrap();
    assert_eq!(result.return_value(), &Value::U64(42));
}
