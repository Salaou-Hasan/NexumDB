//! Integration: WASM reducer → commit → `Vec<Change>` → WAL → crash → recover
//! → identical state (Phase 7 brief §14 "Integration").
//!
//! The Phase 5 mechanism is untouched: `invoke` returns a `ReducerResult`
//! whose `changes` the caller appends to the WAL with `tx_id`. Recovery
//! reconstructs the exact rows, versions, epochs, row ids, and indexes.

use std::path::PathBuf;

use nexum_core::{ColumnType, TableSchema, Value};
use nexum_reducer::ReducerArgs;
use nexum_table::{row, TableStore};
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

// --------------------------------------------------------------- helpers

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nexum-wasm-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

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
"#;

fn module(body: &str) -> Vec<u8> {
    let wat = format!(
        r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
  (data (i32.const 90200) "economy")
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

fn register(registry: &mut WasmModuleRegistry, name: &str, body: &str) {
    registry.register(name, 1, module(body)).unwrap();
}

// --------------------------------------------------------------- test

#[test]
fn wasm_reducer_commits_survive_crash_and_recovery() {
    let dir = temp_dir("wal");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();

    // spawn: insert players [42, 10, 100, 5] and emit.
    register(
        &mut registry,
        "spawn",
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 42) (i64.const 10) (i32.const 100) (i32.const 5)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 7)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 42)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 42))"#,
    );

    let r = registry.invoke(&mut store, "spawn", &ReducerArgs::new()).unwrap();
    assert_eq!(r.events().len(), 1);
    wal.append(r.tx_id(), r.changes()).unwrap();

    // Snapshot after the first durable transaction.
    Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir).unwrap();

    // move: update players row 0 (the first storage row) to zone 30, health 50.
    register(
        &mut registry,
        "move",
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 42) (i64.const 30) (i32.const 50) (i32.const 5)))
    (drop (call $call_op (i32.const 6) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#,
    );
    // pay: insert economy [1, 100].
    register(
        &mut registry,
        "pay",
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90200) (i32.const 7)))
    (local.set $p (call $put_row2 (local.get $p) (i64.const 1) (i64.const 100)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (call $ret_u64 (i64.const 0))"#,
    );

    let r = registry.invoke(&mut store, "move", &ReducerArgs::new()).unwrap();
    wal.append(r.tx_id(), r.changes()).unwrap();
    let r = registry.invoke(&mut store, "pay", &ReducerArgs::new()).unwrap();
    wal.append(r.tx_id(), r.changes()).unwrap();

    // Reference state before the crash.
    let expected_epoch_players = store.table("players").unwrap().epoch();
    let expected_epoch_economy = store.table("economy").unwrap().epoch();
    let expected_next_tx = store.next_transaction_id();
    let expected_next_table = store.next_table_id();

    // Crash + recover into a fresh store.
    let mut fresh = TableStore::new();
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert!(report.snapshot.is_some());
    assert_eq!(report.replayed_txs, 2);
    assert_eq!(report.replayed_changes, 2); // update + insert
    assert!(!report.truncated_tail);

    // Exact reconstruction: rows, versions, epochs, indexes, counters.
    let players = fresh.table("players").unwrap();
    assert_eq!(players.len(), 1);
    let alice = players.get(nexum_core::RowId::from_u64(0)).unwrap();
    assert_eq!(alice.get_named(players.schema(), "health"), Some(&Value::I32(50)));
    assert_eq!(alice.get_named(players.schema(), "zone_id"), Some(&Value::U64(30)));
    assert_eq!(players.version_of(nexum_core::RowId::from_u64(0)), Some(nexum_core::Version::from_u64(1)));
    assert_eq!(players.epoch(), expected_epoch_players);
    // The derived index was rebuilt from authoritative rows, not serialized.
    assert_eq!(
        players.lookup("by_zone", &[Value::U64(30)]).unwrap(),
        vec![nexum_core::RowId::from_u64(0)]
    );

    let economy = fresh.table("economy").unwrap();
    assert_eq!(economy.len(), 1);
    assert_eq!(
        economy.get(nexum_core::RowId::from_u64(0)).unwrap().get_named(economy.schema(), "coins"),
        Some(&Value::I64(100))
    );
    assert_eq!(economy.epoch(), expected_epoch_economy);

    assert_eq!(fresh.next_transaction_id(), expected_next_tx);
    assert_eq!(fresh.next_table_id(), expected_next_table);
    // Replayed history is not fresh change events.
    assert!(fresh.drain_changes().is_empty());

    // The recovered store remains fully functional.
    let mut tx = nexum_tx::Transaction::begin(&mut fresh);
    tx.insert(&fresh, "players", row![9u64, 40u64, 10i32, 9u32]).unwrap();
    let changes = tx.commit(&mut fresh).unwrap();
    assert_eq!(changes[0].row_id().as_u64(), 1, "row id allocation continued");
}

#[test]
fn failed_reducer_produces_no_wal_record() {
    // Brief §14 "Failure": a failed reducer must produce no committed
    // writes, no events, and **no WAL record** — recovery matches the
    // pre-failure state.
    let dir = temp_dir("wal-fail");
    let mut wal = Wal::create(&dir.join("log.wal"), DurabilityPolicy::Sync).unwrap();
    let mut store = world();
    let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();

    // The module performs a provisional insert, emits an event, then traps.
    register(
        &mut registry,
        "explode",
        r#"    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_row4 (local.get $p) (i64.const 9) (i64.const 40) (i32.const 100) (i32.const 9)))
    (drop (call $call_op (i32.const 5) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 7)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (unreachable)"#,
    );

    let err = registry
        .invoke(&mut store, "explode", &ReducerArgs::new())
        .unwrap_err();
    assert!(err.to_string().contains("trapped"));

    // Nothing committed, so nothing was appended: the log is empty.
    let (txs, truncated) = wal.recover_changes().unwrap();
    assert!(!truncated);
    assert!(txs.is_empty(), "no WAL record for a failed reducer");
    assert!(store.table("players").unwrap().is_empty());

    // Recovery of the empty log reconstructs the pre-failure state.
    let mut fresh = world(); // no snapshot: the store must define the tables
    let report = recover(&mut fresh, &mut wal, &dir).unwrap();
    assert!(report.snapshot.is_none());
    assert_eq!(report.replayed_txs, 0);
    assert!(fresh.table("players").unwrap().is_empty());
    assert!(fresh.table("economy").unwrap().is_empty());
}
