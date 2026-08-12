//! Dependency-free timing benchmark for reducer invocation (Phase 6
//! completion criterion).
//!
//! Measures, on a freshly created store per scenario:
//!
//! - empty reducer (no state access)
//! - read-only reducer (get + scan)
//! - single-write reducer (1 insert)
//! - multi-row reducer (10 inserts)
//! - multi-table reducer (2 tables, 2 writes each)
//! - event-emitting reducer (insert + 2 events)
//! - aborting reducer (unique-key violation → validation failure cost)
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nexum-reducer --example reducer_bench [iterations]
//! ```
//!
//! Iterations default to 20_000. Baselines only — criterion harnesses land
//! in Phase 15.

use std::hint::black_box;
use std::time::Instant;

use nexum_core::{ColumnType, ReducerId, TableSchema};
use nexum_reducer::{ReducerArgs, ReducerDefinition, ReducerRegistry};
use nexum_table::{row, TableStore};

fn player_schema(name: &str) -> TableSchema {
    TableSchema::builder(name)
        .column("id", ColumnType::U64)
        .column("zone_id", ColumnType::U64)
        .column("health", ColumnType::I32)
        .column("level", ColumnType::U32)
        .primary_key(&["id"])
        .unique_index("by_level", &["level"])
        .build()
        .unwrap()
}

fn economy_schema() -> TableSchema {
    TableSchema::builder("economy")
        .column("owner", ColumnType::U64)
        .column("coins", ColumnType::I64)
        .primary_key(&["owner"])
        .build()
        .unwrap()
}

fn world() -> TableStore {
    let mut store = TableStore::new();
    store.create_table(player_schema("players")).unwrap();
    store.create_table(economy_schema()).unwrap();
    store
}

fn bench<F: FnMut()>(name: &str, n: usize, mut f: F) {
    for _ in 0..(n / 10).max(1000) {
        f();
    }
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    let ops_per_sec = 1e9 / ns_per_op;
    println!("{name:<42} {ns_per_op:>10.1} ns/op  {ops_per_sec:>12.0} ops/s");
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(20_000);
    println!("Nexum reducer benchmark — {n} iterations\n");

    // Empty reducer: no state access, just a return value.
    {
        let mut store = world();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "empty", |_ctx, _args| {
                    Ok(nexum_core::Value::U64(1))
                })
                .unwrap(),
            )
            .unwrap();
        bench("empty reducer", n, || {
            let result = registry.invoke(&mut store, "empty", &ReducerArgs::new()).unwrap();
            black_box(result);
        });
    }

    // Read-only reducer: get + scan.
    {
        let mut store = world();
        for id in 1..=10u64 {
            store
                .table_mut("players")
                .unwrap()
                .insert(row![id, 10u64, 100i32, id as u32])
                .unwrap();
        }
        store.drain_changes();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "read", |ctx, _args| {
                    black_box(ctx.scan("players")?);
                    Ok(nexum_core::Value::U64(0))
                })
                .unwrap(),
            )
            .unwrap();
        bench("read-only reducer (scan 10 rows)", n, || {
            let result = registry.invoke(&mut store, "read", &ReducerArgs::new()).unwrap();
            black_box(result);
        });
    }

    // Single-write reducer: one insert; the unique level varies per iteration.
    {
        let mut store = world();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "spawn", |ctx, args| {
                    let id = args.require_u64("id")?;
                    let row_id = ctx.insert(
                        "players",
                        row![id, 10u64, 100i32, (id as u32) % 1_000_000],
                    )?;
                    Ok(nexum_core::Value::U64(row_id.as_u64()))
                })
                .unwrap(),
            )
            .unwrap();
        let mut counter = 0u64;
        bench("single-write reducer (1 insert)", n, || {
            counter += 1;
            let args = ReducerArgs::new().insert("id", counter);
            let result = registry.invoke(&mut store, "spawn", &args).unwrap();
            black_box(result);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Multi-row reducer: ten inserts per invocation.
    {
        let mut store = world();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "spawn_many", |ctx, args| {
                    let base = args.require_u64("base")?;
                    for offset in 1..=10u64 {
                        let id = base + offset;
                        ctx.insert(
                            "players",
                            row![id, 10u64, 100i32, (id as u32) % 1_000_000],
                        )?;
                    }
                    Ok(nexum_core::Value::U64(0))
                })
                .unwrap(),
            )
            .unwrap();
        let mut counter = 0u64;
        bench("multi-row reducer (10 inserts)", n, || {
            counter += 10;
            let args = ReducerArgs::new().insert("base", counter);
            let result = registry.invoke(&mut store, "spawn_many", &args).unwrap();
            black_box(result);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Multi-table reducer: two tables, two writes each.
    {
        let mut store = world();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "trade", |ctx, args| {
                    let id = args.require_u64("id")?;
                    let player = ctx.insert(
                        "players",
                        row![id, 10u64, 100i32, (id as u32) % 1_000_000],
                    )?;
                    ctx.update(
                        "players",
                        player,
                        row![id, 10u64, 90i32, (id as u32) % 1_000_000],
                    )?;
                    let coins = ctx.insert("economy", row![id, 100i64])?;
                    ctx.update("economy", coins, row![id, 50i64])?;
                    Ok(nexum_core::Value::U64(0))
                })
                .unwrap(),
            )
            .unwrap();
        let mut counter = 0u64;
        bench("multi-table reducer (2 tables × 2 writes)", n, || {
            counter += 1;
            let args = ReducerArgs::new().insert("id", counter);
            let result = registry.invoke(&mut store, "trade", &args).unwrap();
            black_box(result);
            store.drain_changes();
        });
    }

    // Event-emitting reducer: one insert + two events.
    {
        let mut store = world();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "announce", |ctx, args| {
                    let id = args.require_u64("id")?;
                    let row_id = ctx.insert(
                        "players",
                        row![id, 10u64, 100i32, (id as u32) % 1_000_000],
                    )?;
                    ctx.emit("joined", id)?;
                    ctx.emit("ready", id)?;
                    Ok(nexum_core::Value::U64(row_id.as_u64()))
                })
                .unwrap(),
            )
            .unwrap();
        let mut counter = 0u64;
        bench("event-emitting reducer (insert + 2 events)", n, || {
            counter += 1;
            let args = ReducerArgs::new().insert("id", counter);
            let result = registry.invoke(&mut store, "announce", &args).unwrap();
            black_box(result);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Aborting reducer: a unique-key violation fails validation — the cost of
    // the aborted (conflicting) invocation path.
    {
        let mut store = world();
        store
            .table_mut("players")
            .unwrap()
            .insert(row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        store.drain_changes();
        let mut registry = ReducerRegistry::new();
        registry
            .register(
                ReducerDefinition::new(ReducerId::from_u64(0), "steal", |ctx, _args| {
                    // Level 5 is already owned → validation failure at commit.
                    ctx.insert("players", row![99u64, 10u64, 1i32, 5u32])?;
                    Ok(nexum_core::Value::U64(0))
                })
                .unwrap(),
            )
            .unwrap();
        bench("aborting reducer (unique-key violation)", n, || {
            let err = registry.invoke(&mut store, "steal", &ReducerArgs::new()).unwrap_err();
            black_box(err);
        });
    }

    println!("\ndone. Baseline numbers only — criterion harnesses land in Phase 15.");
}
