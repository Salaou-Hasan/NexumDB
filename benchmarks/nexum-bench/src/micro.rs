//! Micro benchmarks — tight-loop single operations, ns/op.
//!
//! Subcommands: `storage`, `tx`, `reducer`, `wasm`, `sub`, `sim`, `runtime`,
//! `wal`. Run: `cargo run --release -p nexum-bench -- --micro storage tx ...`

use std::time::Instant;

use nexum_core::{ReducerId, RowId, SystemId, TickId, WorldId};
use nexum_execution::{InputFrame, Partition, PartitionConfig, SystemDefinition};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{PartitionFactory, Runtime, RuntimeConfig};
use nexum_subscription::Query;
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Wal};
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use crate::{bench_ns, ensure_players, player_row};

pub fn run(subcommands: &[String]) {
    let all = subcommands.is_empty() || subcommands.iter().any(|s| s == "all");
    if all || subcommands.iter().any(|s| s == "storage") {
        storage();
    }
    if all || subcommands.iter().any(|s| s == "tx") {
        transactions();
    }
    if all || subcommands.iter().any(|s| s == "reducer") {
        reducers(false);
    }
    if all || subcommands.iter().any(|s| s == "wasm") {
        reducers(true);
        wasm_stages();
    }
    if all || subcommands.iter().any(|s| s == "sub") {
        subscriptions();
    }
    if all || subcommands.iter().any(|s| s == "sim") {
        simulation();
    }
    if all || subcommands.iter().any(|s| s == "runtime") {
        runtime_scheduler();
    }
    if all || subcommands.iter().any(|s| s == "wal") {
        wal_append();
    }
}

// ---------------------------------------------------------------- storage

/// A populated store at `rows` rows with a `by_zone` index.
fn populated(rows: u64) -> nexum_table::TableStore {
    let mut store = nexum_table::TableStore::new();
    ensure_players(&mut store);
    for id in 0..rows {
        let mut tx = Transaction::begin(&mut store);
        tx.insert(&store, "players", player_row(id)).unwrap();
        tx.commit(&mut store).unwrap();
    }
    store
}

fn storage() {
    println!("== storage (micro, 100K rows) ==");
    let store = populated(100_000);
    let table = store.table("players").unwrap();
    let rows: Vec<RowId> = table.scan().map(|(rid, _)| rid).collect();
    let n = rows.len();

    // Insert (fresh store, one row per tx).
    bench_ns("insert 1 row (tx + OCC + commit)", 5_000, 100, || {
        let mut fresh = nexum_table::TableStore::new();
        ensure_players(&mut fresh);
        let mut tx = Transaction::begin(&mut fresh);
        tx.insert(&fresh, "players", player_row(0)).unwrap();
        tx.commit(&mut fresh).unwrap();
    });
    // Batch insert: one tx with N rows.
    for batch in [10usize, 100, 1_000] {
        let iterations = 200;
        bench_ns(
            &format!("batch insert {batch} rows (one tx)"),
            iterations,
            10,
            || {
                let mut fresh = nexum_table::TableStore::new();
                ensure_players(&mut fresh);
                let mut tx = Transaction::begin(&mut fresh);
                for i in 0..batch as u64 {
                    tx.insert(&fresh, "players", player_row(i)).unwrap();
                }
                tx.commit(&mut fresh).unwrap();
            },
        );
    }

    // PK lookup through the table (no tx).
    let _mid = n / 2;
    let get_iterations = 200_000;
    let get_start = Instant::now();
    let mut i = 0usize;
    for _ in 0..get_iterations {
        let _ = table.get(rows[i]);
        i = (i + 1) % rows.len();
    }
    let get_ns = get_start.elapsed().as_secs_f64() * 1e9 / get_iterations as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "table.get (direct, sequential)",
        get_ns,
        1e9 / get_ns
    );
    let get_start = Instant::now();
    for k in 0..get_iterations {
        let _ = table.get(rows[(k * 7919) % rows.len()]);
    }
    let get_ns = get_start.elapsed().as_secs_f64() * 1e9 / get_iterations as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "table.get (direct, random stride)",
        get_ns,
        1e9 / get_ns
    );

    // Random lookup via full tx read path.
    bench_ns("tx.get (random row)", 20_000, 100, || {
        let mut store = nexum_table::TableStore::new();
        ensure_players(&mut store);
        let mut tx = Transaction::begin(&mut store);
        let _ = tx.get(&store, "players", rows[0]).unwrap();
        let _ = tx.commit(&mut store);
    });

    // Update through a full transaction.
    bench_ns("update 1 row (tx + OCC + commit)", 20_000, 100, || {
        let mut fresh = nexum_table::TableStore::new();
        ensure_players(&mut fresh);
        let mut tx = Transaction::begin(&mut fresh);
        tx.insert(&fresh, "players", player_row(0)).unwrap();
        tx.commit(&mut fresh).unwrap();
        let mut tx = Transaction::begin(&mut fresh);
        tx.update(&fresh, "players", RowId::from_u64(0), player_row(1))
            .unwrap();
        tx.commit(&mut fresh).unwrap();
    });

    // Scan (full table).
    let scan_start = Instant::now();
    let mut scanned = 0usize;
    for _ in 0..3 {
        scanned += table.scan().count();
    }
    let scan_ns = scan_start.elapsed().as_secs_f64() * 1e9 / 3.0;
    println!(
        "{:<52} {:>10.1} µs/op  ({} rows, {} rows/µs)",
        "table.scan (full)",
        scan_ns / 1_000.0,
        scanned / 3,
        (scanned / 3) as f64 / (scan_ns / 1_000.0)
    );

    // Indexed lookup.
    bench_ns("index lookup (by_zone, 100K rows)", 20_000, 100, || {
        let _ = table
            .lookup("by_zone", &[nexum_core::Value::U64(42)])
            .unwrap();
    });

    // Delete.
    bench_ns("delete 1 row (tx + OCC + commit)", 10_000, 100, || {
        let mut fresh = nexum_table::TableStore::new();
        ensure_players(&mut fresh);
        let mut tx = Transaction::begin(&mut fresh);
        tx.insert(&fresh, "players", player_row(0)).unwrap();
        tx.commit(&mut fresh).unwrap();
        let mut tx = Transaction::begin(&mut fresh);
        tx.delete(&fresh, "players", RowId::from_u64(0)).unwrap();
        tx.commit(&mut fresh).unwrap();
    });
    drop(store);
    println!();
}

// ------------------------------------------------------------- transactions

fn transactions() {
    println!("== transactions / OCC (micro) ==");
    let mut store = nexum_table::TableStore::new();
    ensure_players(&mut store);
    for id in 0..10_000u64 {
        let mut tx = Transaction::begin(&mut store);
        tx.insert(&store, "players", player_row(id)).unwrap();
        tx.commit(&mut store).unwrap();
    }

    for rows_touched in [1usize, 10, 100, 1_000] {
        let iterations = 2_000;
        // Read-only tx against the shared 10K-row store (stable reads).
        bench_ns(
            &format!("read-only {} rows (tx)", rows_touched),
            iterations,
            20,
            || {
                let mut tx = Transaction::begin(&mut store);
                for i in 0..rows_touched as u64 {
                    let _ = tx.get(&store, "players", RowId::from_u64(i)).unwrap();
                }
                let _ = tx.commit(&mut store).unwrap();
            },
        );
        // Read + write: build the touched rows in a private store first
        // (outside the timed closure), then time the read/write tx against
        // it. Row 0 is restored after each timed commit so the next
        // iteration sees the original value.
        let mut base = nexum_table::TableStore::new();
        ensure_players(&mut base);
        for id in 0..rows_touched as u64 {
            let mut tx = Transaction::begin(&mut base);
            tx.insert(&base, "players", player_row(id)).unwrap();
            tx.commit(&mut base).unwrap();
        }
        bench_ns(
            &format!("read {} + write 1 (tx)", rows_touched),
            iterations,
            20,
            || {
                let mut tx = Transaction::begin(&mut base);
                for i in 0..rows_touched as u64 {
                    let _ = tx.get(&base, "players", RowId::from_u64(i)).unwrap();
                }
                tx.update(&base, "players", RowId::from_u64(0), player_row(1))
                    .unwrap();
                let _ = tx.commit(&mut base).unwrap();
                // Restore row 0 so the next iteration reads the original.
                let mut tx = Transaction::begin(&mut base);
                tx.update(&base, "players", RowId::from_u64(0), player_row(0))
                    .unwrap();
                let _ = tx.commit(&mut base).unwrap();
            },
        );
    }

    // Conflict rates: two tx racing the same row. Each iteration performs a
    // pair of conflicting commits (the second must abort on OCC).
    for conflict_pct in [0u64, 50, 100] {
        let iterations = 1_000;
        bench_ns(
            &format!("conflicting pair ({}% rate)", conflict_pct),
            iterations,
            20,
            || {
                let mut fresh = nexum_table::TableStore::new();
                ensure_players(&mut fresh);
                let mut setup = Transaction::begin(&mut fresh);
                setup.insert(&fresh, "players", player_row(0)).unwrap();
                setup.commit(&mut fresh).unwrap();
                for _ in 0..2 {
                    let mut tx = Transaction::begin(&mut fresh);
                    tx.update(&fresh, "players", RowId::from_u64(0), player_row(1))
                        .unwrap();
                    let _ = tx.commit(&mut fresh);
                }
            },
        );
    }
    println!();
}

// --------------------------------------------------------------- reducers

/// The `bump` reducer: +10 to the `health` column of the named player.
fn bump(ctx: &mut ReducerContext, args: &ReducerArgs) -> nexum_core::Result<nexum_core::Value> {
    let player = args.require_u64("player")?;
    let rows = ctx.scan("players")?;
    let (row_id, row) = rows
        .iter()
        .find(|(_, r)| r.get(0) == Some(&nexum_core::Value::U64(player)))
        .ok_or_else(|| nexum_core::Error::not_found("player"))?;
    let mut values = row.clone().into_values();
    let health = values[2].as_i32().unwrap_or(0);
    values[2] = nexum_core::Value::I32(health + 10);
    ctx.update("players", *row_id, nexum_core::Row::new(values))?;
    ctx.emit("bumped", nexum_core::Value::U64(player))?;
    Ok(nexum_core::Value::I32(health + 10))
}

/// A minimal WASM reducer scanning "players" + emitting "hit", exercising the
/// real sandbox host-call ABI (single `nexum.op` dispatcher): scan + emit +
/// return value through the out envelope.
const WASM_BUMP_WAT: &str = r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 10)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
  (data (i32.const 90100) "hit")
  (func $put_str (param $p i32) (param $src i32) (param $len i32) (result i32)
    (i64.store align=1 (local.get $p) (i64.extend_i32_u (local.get $len)))
    (memory.copy (i32.add (local.get $p) (i32.const 8)) (local.get $src) (local.get $len))
    (i32.add (local.get $p) (i32.add (i32.const 8) (local.get $len))))
  (func $put_value_u64 (param $p i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $p) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $p) (i32.const 1)) (local.get $v))
    (i32.add (local.get $p) (i32.const 9)))
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))
  (func $ret_u64 (param $v i64) (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 8))
    (i64.store align=1 (i32.const 16385) (local.get $v))
    (i32.const 9))
  (func (export "_nexum_reducer_run") (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (drop (call $call_op (i32.const 3) (local.get $p)))
    (local.set $p (call $put_str (i32.const 0) (i32.const 90100) (i32.const 3)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (call $ret_u64 (i64.const 1)))
)"#;

/// A world factory registering the native `bump` reducer.
fn reducer_factory() -> PartitionFactory {
    Box::new(
        |id: WorldId, mut store: nexum_table::TableStore, sim: PartitionConfig| {
            ensure_players(&mut store);
            let mut world = Partition::new(id, store, sim)?;
            world
                .native_mut()
                .register(ReducerDefinition::new(ReducerId::from_u64(1), "bump", bump).unwrap())
                .unwrap();
            Ok(world)
        },
    )
}

/// A world factory registering `bump` as a WASM reducer.
fn wasm_factory() -> PartitionFactory {
    Box::new(
        |id: WorldId, mut store: nexum_table::TableStore, sim: PartitionConfig| {
            ensure_players(&mut store);
            let mut world = Partition::new(id, store, sim)?;
            let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
            wasm.register("bump", 1, wat::parse_str(WASM_BUMP_WAT).unwrap())
                .unwrap();
            world.set_wasm(wasm);
            Ok(world)
        },
    )
}

fn reducers(wasm_mode: bool) {
    let label = if wasm_mode { "WASM" } else { "native" };
    println!("== reducers ({label}) ==");
    let factory: PartitionFactory = if wasm_mode {
        wasm_factory()
    } else {
        reducer_factory()
    };
    let mut runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    // Seed the world with one player via a first tick.
    runtime
        .submit_input(world, InputFrame::new(TickId::from_u64(0)))
        .unwrap();
    runtime.step().unwrap();
    // Insert a player row through the reducer on the next tick.
    runtime
        .submit_reducer_call(world, 1, "bump", ReducerArgs::new().insert("player", 0u64))
        .unwrap();
    runtime.step().unwrap();

    for calls_per_tick in [1usize, 10, 100] {
        let iterations = 200;
        let ns = bench_calls(&mut runtime, world, calls_per_tick, iterations);
        println!(
            "{label} {calls_per_tick:>4} calls/tick   {ns:>10.1} ns/call   {:>10.1} calls/s",
            1e9 / ns
        );
    }
    println!();
}

/// A WASM reducer with the same host-call pattern as the game's
/// `fire_weapon` (lookup_unique → get → update → emit) for the Phase 22
/// per-stage breakdown.
const WASM_FIRELIKE_WAT: &str = r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (data (i32.const 90000) "players")
  (data (i32.const 90100) "primary")
  (func $put_str (param $dst i32) (param $src i32) (param $len i32) (result i32)
    (local $k i32)
    (i64.store align=1 (local.get $dst) (i64.extend_i32_u (local.get $len)))
    (block $done (loop $l
      (br_if $done (i32.ge_u (local.get $k) (local.get $len)))
      (i32.store8 align=1 (i32.add (i32.add (local.get $dst) (i32.const 8)) (local.get $k))
        (i32.load8_u align=1 (i32.add (local.get $src) (local.get $k))))
      (local.set $k (i32.add (local.get $k) (i32.const 1)))
      (br $l)))
    (i32.add (local.get $dst) (i32.add (i32.const 8) (local.get $len))))
  (func $put_u64 (param $dst i32) (param $v i64) (result i32)
    (i64.store align=1 (local.get $dst) (local.get $v))
    (i32.add (local.get $dst) (i32.const 8)))
  (func $put_value_u64 (param $dst i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $dst) (i32.const 8))
    (i64.store align=1 (i32.add (local.get $dst) (i32.const 1)) (local.get $v))
    (i32.add (local.get $dst) (i32.const 9)))
  (func $put_value_i64 (param $dst i32) (param $v i64) (result i32)
    (i32.store8 align=1 (local.get $dst) (i32.const 4))
    (i64.store align=1 (i32.add (local.get $dst) (i32.const 1)) (local.get $v))
    (i32.add (local.get $dst) (i32.const 9)))
  (func $call_op (param $op i32) (param $len i32) (result i32)
    (call $op (local.get $op) (i32.const 0) (local.get $len) (i32.const 16384) (i32.const 65536)))
  (func (export "_nexum_reducer_run") (result i32)
    (local $p i32)
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_str (local.get $p) (i32.const 90100) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 1)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 7)))
    (drop (call $call_op (i32.const 4) (local.get $p)))
    ;; get the row
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 1)))
    (drop (call $call_op (i32.const 1) (local.get $p)))
    ;; update the row
    (local.set $p (call $put_str (i32.const 0) (i32.const 90000) (i32.const 7)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 1)))
    (local.set $p (call $put_u64 (local.get $p) (i64.const 3)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 1)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 2)))
    (local.set $p (call $put_value_i64 (local.get $p) (i64.const 90)))
    (drop (call $call_op (i32.const 6) (local.get $p)))
    ;; emit
    (local.set $p (call $put_str (i32.const 0) (i32.const 90100) (i32.const 7)))
    (local.set $p (call $put_value_u64 (local.get $p) (i64.const 7)))
    (drop (call $call_op (i32.const 8) (local.get $p)))
    (i32.store8 align=1 (i32.const 16384) (i32.const 4))
    (i64.store align=1 (i32.const 16385) (i64.const 25))
    (i32.const 9))
)"#;

/// Phase 22: per-stage WASM invocation cost breakdown (store setup,
/// instantiate, encode, exec incl. host calls, result decode). Uses the
/// `invoke_in_tx_timed` instrumentation.
fn wasm_stages() {
    println!("== wasm stages (per-call breakdown) ==");
    let mut store = nexum_table::TableStore::new();
    store
        .create_table(
            nexum_core::TableSchema::builder("players")
                .column("id", nexum_core::ColumnType::U64)
                .column("zone", nexum_core::ColumnType::U64)
                .column("health", nexum_core::ColumnType::I64)
                .primary_key(&["id"])
                .build()
                .expect("schema builds"),
        )
        .expect("table created");
    let mut tx = Transaction::begin(&mut store);
    tx.insert(&store, "players", nexum_core::row![7u64, 7u64, 100i64])
        .unwrap();
    tx.commit(&mut store).unwrap();

    let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    registry
        .register("firelike", 1, wat::parse_str(WASM_FIRELIKE_WAT).unwrap())
        .unwrap();

    let args = ReducerArgs::new().insert("__caller", 7u64);
    let iterations = 20_000;
    let warmup = 2_000;

    // Warmup + steady-state loop, accumulating stage times.
    let mut acc = nexum_wasm::WasmStageTimes::default();
    let mut n = 0u64;
    for i in 0..(iterations + warmup) {
        let mut tx = Transaction::begin(&mut store);
        let (_, _, times) = registry
            .invoke_in_tx_timed(&store, &mut tx, "firelike", &args)
            .unwrap();
        if i >= warmup {
            acc.store_setup_ns += times.store_setup_ns;
            acc.instantiate_ns += times.instantiate_ns;
            acc.encode_ns += times.encode_ns;
            acc.exec_ns += times.exec_ns;
            acc.result_ns += times.result_ns;
            acc.total_ns += times.total_ns;
            n += 1;
        }
    }
    let ns = |v: u64| v as f64 / n as f64;
    let total = ns(acc.total_ns).max(1.0);
    println!(
        "  store_setup {:>8.1} ns  ({:>5.1}%)",
        ns(acc.store_setup_ns),
        ns(acc.store_setup_ns) / total * 100.0
    );
    println!(
        "  instantiate {:>8.1} ns  ({:>5.1}%)",
        ns(acc.instantiate_ns),
        ns(acc.instantiate_ns) / total * 100.0
    );
    println!(
        "  encode      {:>8.1} ns  ({:>5.1}%)",
        ns(acc.encode_ns),
        ns(acc.encode_ns) / total * 100.0
    );
    println!(
        "  exec        {:>8.1} ns  ({:>5.1}%)",
        ns(acc.exec_ns),
        ns(acc.exec_ns) / total * 100.0
    );
    println!(
        "  result      {:>8.1} ns  ({:>5.1}%)",
        ns(acc.result_ns),
        ns(acc.result_ns) / total * 100.0
    );
    println!("  total       {:>8.1} ns/call", total);
    println!();
}

/// Runs `calls_per_tick` reducer calls on one tick, `iterations` times;
/// returns ns per individual call.
fn bench_calls(
    runtime: &mut Runtime,
    world: WorldId,
    calls_per_tick: usize,
    iterations: usize,
) -> f64 {
    // Warmup.
    for _ in 0..10 {
        for request in 0..calls_per_tick as u64 {
            runtime
                .submit_reducer_call(
                    world,
                    request,
                    "bump",
                    ReducerArgs::new().insert("player", 0u64),
                )
                .unwrap();
        }
        runtime.step().unwrap();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        for request in 0..calls_per_tick as u64 {
            runtime
                .submit_reducer_call(
                    world,
                    request,
                    "bump",
                    ReducerArgs::new().insert("player", 0u64),
                )
                .unwrap();
        }
        runtime.step().unwrap();
    }
    start.elapsed().as_secs_f64() * 1e9 / (iterations * calls_per_tick) as f64
}

// ---------------------------------------------------------- subscriptions

fn subscriptions() {
    println!("== subscriptions (micro, 100K rows) ==");
    let mut store = nexum_table::TableStore::new();
    ensure_players(&mut store);
    for id in 0..100_000u64 {
        let mut tx = Transaction::begin(&mut store);
        tx.insert(&store, "players", player_row(id)).unwrap();
        tx.commit(&mut store).unwrap();
    }

    // Initial snapshot (subscribe = compile + scan + deliver).
    let mut registry = nexum_subscription::SubscriptionRegistry::new();
    let query = Query::builder("players").build().unwrap();
    let sub_start = Instant::now();
    for _ in 0..5 {
        let mut r = nexum_subscription::SubscriptionRegistry::new();
        let _ = r.subscribe(&store, query.clone()).unwrap();
        r.drain(0.into()).unwrap();
    }
    let sub_ns = sub_start.elapsed().as_secs_f64() * 1e9 / 5.0;
    println!(
        "{:<52} {:>10.1} µs  (100K-row snapshot)",
        "subscription initial snapshot",
        sub_ns / 1_000.0
    );

    // One-row delta. The update alternates between two distinct rows so
    // every iteration is a REAL change (identical-value updates are no-ops
    // and would measure nothing — ADR-015 D3).
    let sub = registry.subscribe(&store, query).unwrap();
    registry.drain(sub).unwrap();
    let mut toggle = 0usize;
    bench_ns("single-row delta (1 subscriber)", 10_000, 100, || {
        toggle = (toggle + 1) % 2;
        let changes = {
            let mut tx = Transaction::begin(&mut store);
            tx.update(
                &store,
                "players",
                RowId::from_u64(0),
                player_row(1 + toggle as u64),
            )
            .unwrap();
            tx.commit(&mut store).unwrap()
        };
        registry.apply_changes(&store, &changes);
        registry.drain(sub).unwrap();
    });
    // Deep-row delta: the target sits at the far end of the window's key
    // order, exercising the worst case of the membership lookup.
    let deep = RowId::from_u64(99_999);
    let mut toggle = 0usize;
    bench_ns("single-row delta (deep row)", 1_000, 20, || {
        toggle = (toggle + 1) % 2;
        let changes = {
            let mut tx = Transaction::begin(&mut store);
            tx.update(&store, "players", deep, player_row(1 + toggle as u64))
                .unwrap();
            tx.commit(&mut store).unwrap()
        };
        registry.apply_changes(&store, &changes);
        registry.drain(sub).unwrap();
    });
    println!();
}

// -------------------------------------------------------------- simulation

fn simulation() {
    println!("== simulation ticks ==");
    let mut runtime = Runtime::new(RuntimeConfig::new(sim_factory())).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    runtime.start_partition(world).unwrap();

    for entities in [10u64, 1_000, 10_000] {
        // Seed `entities` rows via a command-bearing frame (the world's
        // `spawn` system reads the frame's commands — one tick per row).
        for id in 0..entities {
            let mut frame = InputFrame::with_capacity(
                TickId::from_u64(runtime.partition_status(world).unwrap().next_tick.as_u64()),
                1,
            );
            frame.push(nexum_execution::InputCommand::simple(id, "spawn").unwrap());
            runtime.submit_input(world, frame).unwrap();
            runtime.step().unwrap();
        }
        let iterations = 50;
        let start = Instant::now();
        for _ in 0..iterations {
            runtime.step().unwrap();
        }
        let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
        println!(
            "{:<52} {:>10.1} µs/tick  ({} rows in store)",
            format!("tick after seeding {entities} rows"),
            ns / 1_000.0,
            entities
        );
    }
    println!();
}

/// A world whose `spawn` system inserts one row per frame command, and whose
/// `scan-all` system reads every row each tick (the "active entities" pass).
fn sim_factory() -> PartitionFactory {
    Box::new(
        |id: WorldId, mut store: nexum_table::TableStore, sim: PartitionConfig| {
            ensure_players(&mut store);
            let mut world = Partition::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "spawn", 0, |ctx, frame| {
                        for command in frame.commands() {
                            ctx.insert("players", player_row(command.source()))?;
                        }
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(1), "scan-all", 1, |ctx, _| {
                        let _ = ctx.scan("players")?;
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
            Ok(world)
        },
    )
}

// ------------------------------------------------------- runtime scheduler

fn runtime_scheduler() {
    println!("== runtime / scheduler ==");
    for worlds in [1usize, 10, 100, 1_000] {
        let mut runtime = Runtime::new(RuntimeConfig::new(noop_factory())).unwrap();
        for id in 0..worlds {
            runtime
                .create_partition(WorldId::from_u64(id as u64), PartitionConfig::new())
                .unwrap();
            runtime
                .start_partition(WorldId::from_u64(id as u64))
                .unwrap();
        }
        let iterations = 100;
        let start = Instant::now();
        for _ in 0..iterations {
            runtime.step().unwrap();
        }
        let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
        println!(
            "{:<52} {:>10.1} µs/step  ({worlds} worlds, {:>8.1} ns/world)",
            format!("step over {worlds} worlds"),
            ns / 1_000.0,
            ns / worlds as f64
        );
    }
    println!();
}

fn noop_factory() -> PartitionFactory {
    Box::new(
        |id: WorldId, store: nexum_table::TableStore, sim: PartitionConfig| {
            Partition::new(id, store, sim)
        },
    )
}

// -------------------------------------------------------------------- wal

fn wal_append() {
    println!("== WAL append ==");
    let dir = std::env::temp_dir().join(format!("nexum-bench-wal-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();

    for policy in [DurabilityPolicy::Flush, DurabilityPolicy::Sync] {
        let mut wal = Wal::create(&dir.join(policy_label(policy)), policy).unwrap();
        let mut store = nexum_table::TableStore::new();
        ensure_players(&mut store);
        let mut tx = Transaction::begin(&mut store);
        tx.insert(&store, "players", player_row(0)).unwrap();
        let changes = tx.commit(&mut store).unwrap();
        let iterations = if policy == DurabilityPolicy::Sync {
            200
        } else {
            5_000
        };
        let start = Instant::now();
        for _ in 0..iterations {
            wal.append(nexum_core::TransactionId::from_u64(0), &changes)
                .unwrap();
        }
        let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
        println!(
            "{:<52} {:>10.1} µs/append  ({:>10.1} appends/s)",
            format!("wal append ({})", policy_label(policy)),
            ns / 1_000.0,
            1e9 / ns
        );
        let _ = wal.flush();
    }
    let _ = std::fs::remove_dir_all(&dir);
    println!();
}

fn policy_label(policy: DurabilityPolicy) -> &'static str {
    match policy {
        DurabilityPolicy::Flush => "flush",
        DurabilityPolicy::Sync => "sync",
    }
}
