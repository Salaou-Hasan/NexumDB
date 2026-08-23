//! Baseline benchmarks for WASM reducer execution (Phase 7 brief §13).
//!
//! Scenarios: empty · read-only · single-row read · single-row write ·
//! 10-row write · multi-table transaction · event emission · scan · trap ·
//! fuel exhaustion.
//!
//! Methodology (honest baselines): module *compilation* is cached by the
//! registry; every invocation still pays per-invocation instantiation (fresh
//! `Store` + host state), host↔guest crossings, and — for writing scenarios
//! — OCC validation + commit. Nothing is claimed superior to native without
//! measurement; Criterion harnesses land in Phase 15.
//!
//! Usage: `cargo run --release -p nexum-wasm --example wasm_bench [iters]`

use std::time::Instant;

use nexum_core::{ColumnType, TableSchema};
use nexum_reducer::ReducerArgs;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

fn bench_world() -> TableStore {
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

/// Seeds `count` players rows (row `k` has id `k`), draining change buffers.
fn seed_players(store: &mut TableStore, count: u64) {
    for id in 0..count {
        store
            .table_mut("players")
            .unwrap()
            .insert(nexum_table::row![id, 10u64, 100i32, (id % 1000) as u32])
            .unwrap();
    }
    store.drain_changes();
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
  (func $arg0_u64 (result i64)
    ;; Value payload of the single {"i": u64} argument: count(8) + name
    ;; len(8) + name(1) + tag(1) → payload at offset 18.
    (i64.load align=1 (i32.const 18)))
  (func $get_first_u64 (result i64)
    (if (result i64)
      (i32.load8_u align=1 (i32.const 16392))
      (then (i64.load align=1 (i32.const 16402)))
      (else (i64.const -1))))
  (func $scan_count (result i64)
    (i64.load align=1 (i32.const 16392)))
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
  (data (i32.const 90400) "hello")
{helpers}
  (func (export "_nexum_reducer_run") (result i32)
{body}))
"#,
        helpers = HELPERS,
        body = body
    );
    wat::parse_str(&wat).expect("bench module is valid WAT")
}

/// Builds a 10-row write body: rows `[base+j, 10, 100, j+1]` for j in 0..10,
/// with `base = arg0 * 10` so ids stay unique across invocations.
fn write10_body() -> String {
    let mut out = String::from(
        "    (local $k i64)\n    (local $base i64)\n    (local $p i32)\n\
         (local.set $k (call $arg0_u64))\n\
         (local.set $base (i64.mul (local.get $k) (i64.const 10)))\n",
    );
    for j in 0..10u64 {
        out.push_str(&format!(
            "    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
             (local.set $p (call $put_row4 (local.get $p) (i64.add (local.get $base) (i64.const {j})) (i64.const 10) (i32.const 100) (i32.const {})))\n\
             (drop (call $call_op (i32.const 5) (local.get $p)))\n",
            j + 1
        ));
    }
    out.push_str("    (call $ret_u64 (i64.const 0))");
    out
}

fn time(label: &str, n: usize, mut f: impl FnMut(usize)) {
    let start = Instant::now();
    for i in 0..n {
        f(i);
    }
    let elapsed = start.elapsed();
    let per_us = elapsed.as_secs_f64() * 1e6 / n as f64;
    let ops = n as f64 / elapsed.as_secs_f64();
    println!("{label:<24} {n:>9} iters  {per_us:>10.2} µs/op  {ops:>12.0} ops/s");
}

fn args(i: u64) -> ReducerArgs {
    ReducerArgs::new().insert("i", i)
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(20_000);
    println!("wasm reducer benchmark — {n} invocations per scenario");
    println!("(compilation cached; per-invocation instantiation + host calls + commit included)\n");

    let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    registry
        .register("empty", 1, module("    (call $ret_u64 (i64.const 0))"))
        .unwrap();
    registry
        .register(
            "read0",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
                 (local.set $p (call $put_u64 (local.get $p) (i64.const 0)))\n\
                 (drop (call $call_op (i32.const 1) (local.get $p)))\n\
                 (call $ret_u64 (call $get_first_u64))",
            ),
        )
        .unwrap();
    registry
        .register(
            "read_k",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
                 (local.set $p (call $put_u64 (local.get $p) (call $arg0_u64)))\n\
                 (drop (call $call_op (i32.const 1) (local.get $p)))\n\
                 (call $ret_u64 (call $get_first_u64))",
            ),
        )
        .unwrap();
    registry
        .register(
            "write1",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
                 (local.set $p (call $put_row4 (local.get $p) (call $arg0_u64) (i64.const 10) (i32.const 100) (i32.const 5)))\n\
                 (drop (call $call_op (i32.const 5) (local.get $p)))\n\
                 (call $ret_u64 (i64.const 0))",
            ),
        )
        .unwrap();
    registry
        .register("write10", 1, module(&write10_body()))
        .unwrap();
    registry
        .register(
            "multi",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
                 (local.set $p (call $put_row4 (local.get $p) (call $arg0_u64) (i64.const 10) (i32.const 100) (i32.const 5)))\n\
                 (drop (call $call_op (i32.const 5) (local.get $p)))\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90200) (i32.const 7)))\n\
                 (local.set $p (call $put_row2 (local.get $p) (call $arg0_u64) (i64.const 100)))\n\
                 (drop (call $call_op (i32.const 5) (local.get $p)))\n\
                 (call $ret_u64 (i64.const 0))",
            ),
        )
        .unwrap();
    registry
        .register(
            "emit",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90400) (i32.const 5)))\n\
                 (local.set $p (call $put_value_u64 (local.get $p) (call $arg0_u64)))\n\
                 (drop (call $call_op (i32.const 8) (local.get $p)))\n\
                 (call $ret_u64 (i64.const 0))",
            ),
        )
        .unwrap();
    registry
        .register(
            "scan",
            1,
            module(
                "    (local $p i32)\n\
                 (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))\n\
                 (drop (call $call_op (i32.const 3) (local.get $p)))\n\
                 (call $ret_u64 (call $scan_count))",
            ),
        )
        .unwrap();
    registry
        .register("trap", 1, module("    (unreachable)"))
        .unwrap();

    // Fuel exhaustion uses a dedicated registry with a small budget so each
    // invocation fails fast (deterministically, never wall-clock).
    let fuel_limits = WasmLimits {
        max_fuel: 10_000,
        ..WasmLimits::default()
    };
    let mut fuel_registry = WasmModuleRegistry::new(fuel_limits).unwrap();
    fuel_registry
        .register(
            "burn",
            1,
            module("    (loop $l (br $l))\n    (unreachable)"),
        )
        .unwrap();

    // empty
    let mut store = bench_world();
    time("empty", n, |i| {
        registry
            .invoke(&mut store, "empty", &args(i as u64))
            .unwrap();
    });

    // read-only: fixed row on a one-row store.
    let mut store = bench_world();
    seed_players(&mut store, 1);
    time("read-only (get row 0)", n, |i| {
        registry
            .invoke(&mut store, "read0", &args(i as u64))
            .unwrap();
    });

    // single-row read: row k on an n-row store (seeding excluded from timing).
    let mut store = bench_world();
    seed_players(&mut store, n as u64);
    time("single-row read (get k)", n, |i| {
        registry
            .invoke(&mut store, "read_k", &args(i as u64))
            .unwrap();
    });

    // single-row write: growing store, unique id per invocation.
    let mut store = bench_world();
    time("single-row write", n, |i| {
        registry
            .invoke(&mut store, "write1", &args(i as u64))
            .unwrap();
    });

    // 10-row write.
    let mut store = bench_world();
    time("10-row write", n, |i| {
        registry
            .invoke(&mut store, "write10", &args(i as u64))
            .unwrap();
    });

    // multi-table transaction.
    let mut store = bench_world();
    time("multi-table tx", n, |i| {
        registry
            .invoke(&mut store, "multi", &args(i as u64))
            .unwrap();
    });

    // event emission.
    let mut store = bench_world();
    time("event emission", n, |i| {
        registry
            .invoke(&mut store, "emit", &args(i as u64))
            .unwrap();
    });

    // scan over a 1000-row table.
    let mut store = bench_world();
    seed_players(&mut store, 1_000);
    time("scan (1000 rows)", n, |i| {
        registry
            .invoke(&mut store, "scan", &args(i as u64))
            .unwrap();
    });

    // trap: every invocation fails (abort path cost).
    let mut store = bench_world();
    time("trap (fails)", n, |i| {
        let _ = registry.invoke(&mut store, "trap", &args(i as u64));
    });

    // fuel exhaustion: every invocation fails deterministically.
    let mut store = bench_world();
    time("fuel exhausted (fails)", n, |i| {
        let _ = fuel_registry.invoke(&mut store, "burn", &args(i as u64));
    });
}
