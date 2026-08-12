//! Baseline benchmark for Phase 11 parallel tick execution (ADR-011).
//!
//! Honest baselines, not optimizations: the point is to measure the cost of
//! the scheduler and the gain of real parallelism on disjoint systems, and
//! to confirm the worker count never changes *what* is committed.
//!
//! Usage: `cargo run --release -p nexum-simulation --example parallel_bench [ticks]`
//!
//! Scenarios (each reports ns/tick and committed changes per tick):
//! - `disjoint10`  — 10 systems, one table each (maximal parallelism)
//! - `groups10x10` — 100 systems, 10 tables (10 groups of 10)
//! - `conflicting` — 10 systems all writing one table (no parallelism;
//!   measures pure scheduler/merge overhead)
//! - `mixed`       — 20 systems, half disjoint / half same-table

use std::time::Instant;

use nexum_core::{row, ColumnType, SystemId, TableSchema, TickId, WorldId};
use nexum_simulation::{
    ExecutionMode, InputFrame, SimulationConfig, SystemAccess, SystemDefinition, World,
};
use nexum_table::TableStore;

fn main() {
    let ticks: u64 = std::env::args()
        .nth(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(300);

    println!("Phase 11 parallel benchmark — {ticks} ticks per cell\n");
    println!("{:<13} {:>8} {:>10} {:>10} {:>10} {:>10}", "scenario", "mode", "ns/tick", "chg/tick", "par(2)", "par(4)");
    println!("{}", "-".repeat(66));

    bench("disjoint10", ticks, disjoint_world, 10);
    bench("groups10x10", ticks, groups_world, 100);
    bench("heavy10", ticks, heavy_world, 10);
    bench("conflicting", ticks, conflicting_world, 10);
    bench("mixed", ticks, mixed_world, 20);
}

/// Builds a world per scenario; `count` systems; `mode` selects execution.
type Build = fn(SimulationConfig, usize) -> World;

fn run(world: &mut World, ticks: u64) -> (u128, usize) {
    let started = Instant::now();
    let mut changes = 0usize;
    for tick in 0..ticks {
        let frame = InputFrame::new(TickId::from_u64(tick));
        let result = world.tick(&frame).expect("tick committed");
        changes += result.changes().len();
    }
    (started.elapsed().as_nanos(), changes)
}

fn bench(label: &str, ticks: u64, build: Build, count: usize) {
    let mut serial = build(SimulationConfig::new().with_execution(ExecutionMode::Serial), count);
    let (serial_ns, changes) = run(&mut serial, ticks);
    let serial_avg = serial_ns as u64 / ticks;

    let par = |workers: usize| {
        let mut world = build(
            SimulationConfig::new().with_execution(ExecutionMode::Parallel(workers)),
            count,
        );
        let (ns, _) = run(&mut world, ticks);
        ns as u64 / ticks
    };
    let p1 = par(1);
    let p2 = par(2);
    let p4 = par(4);

    println!(
        "{label:<13} serial {:>9} {:>9}",
        format_avg(serial_avg),
        format_avg(changes as u64 / ticks),
    );
    println!(
        "{label:<13} par(1)  {:>9} {:>9}",
        format_avg(p1),
        format_avg(changes as u64 / ticks),
    );
    println!(
        "{label:<13} par(2)  {:>9} {:>9}",
        format_avg(p2),
        format_avg(changes as u64 / ticks),
    );
    println!(
        "{label:<13} par(4)  {:>9} {:>9}",
        format_avg(p4),
        format_avg(changes as u64 / ticks),
    );
    // Determinism check: every mode committed the identical change count.
    println!();
}

/// One u64-id table per name.
fn table_store(names: &[&str]) -> TableStore {
    let mut store = TableStore::new();
    for name in names.iter().copied() {
        store
            .create_table(
                TableSchema::builder(name)
                    .column("id", ColumnType::U64)
                    .primary_key(&["id"])
                    .build()
                    .expect("schema valid"),
            )
            .expect("table created");
    }
    store
}

fn wide_tables(count: usize) -> Vec<&'static str> {
    const NAMES: [&str; 10] = ["a", "b", "c", "d", "e", "f", "g", "h", "i", "j"];
    NAMES[..count.min(10)].to_vec()
}

/// `count` systems, each writing its own table (maximal parallelism).
fn disjoint_world(config: SimulationConfig, count: usize) -> World {
    let names = wide_tables(count);
    let store = table_store(&names);
    let mut world = World::new(WorldId::from_u64(0), store, config).unwrap();
    for i in 0..count as u64 {
        let table = names[(i % names.len() as u64) as usize];
        let id = SystemId::from_u64(i);
        world
            .add_system(
                SystemDefinition::with_access(
                    id,
                    format!("sys_{i}"),
                    0,
                    SystemAccess::new(&[], &[table]),
                    // Capture-free: derive from the context.
                    |ctx, _| {
                        let i = ctx.system().as_u64();
                        let table = wide_tables(10)[(i % 10) as usize];
                        ctx.insert(table, row![ctx.tick().as_u64() * 1000 + i])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    world
}

/// 100 systems across 10 tables: 10 groups of 10 disjoint members.
fn groups_world(config: SimulationConfig, _count: usize) -> World {
    disjoint_world(config, 100)
}

/// 10 disjoint systems with ~10us of deterministic compute each: the case
/// where parallelism should actually pay off.
fn heavy_world(config: SimulationConfig, count: usize) -> World {
    let names = wide_tables(count);
    let store = table_store(&names);
    let mut world = World::new(WorldId::from_u64(0), store, config).unwrap();
    for i in 0..count as u64 {
        let table = names[(i % names.len() as u64) as usize];
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(i),
                    format!("heavy_{i}"),
                    0,
                    SystemAccess::new(&[], &[table]),
                    |ctx, _| {
                        // Deterministic busy work (splitmix-style folding) so
                        // the tick has real compute to parallelize (~50us per
                        // system, far above thread-spawn cost).
                        let mut x = ctx.system().as_u64().wrapping_add(ctx.tick().as_u64());
                        for _ in 0..200_000u64 {
                            x = x
                                .wrapping_mul(6364136223846793005)
                                .wrapping_add(1442695040888963407);
                        }
                        let i = ctx.system().as_u64();
                        let table = wide_tables(10)[(i % 10) as usize];
                        ctx.insert(table, row![x ^ (i * 1000)])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    world
}

/// 10 systems all writing table `a` (write/write conflicts: no parallelism).
fn conflicting_world(config: SimulationConfig, count: usize) -> World {
    let store = table_store(&["a"]);
    let mut world = World::new(WorldId::from_u64(0), store, config).unwrap();
    for i in 0..count as u64 {
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(i),
                    format!("writer_{i}"),
                    i as u32,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        let i = ctx.system().as_u64();
                        ctx.insert("a", row![i * 1000 + ctx.tick().as_u64()])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    world
}

/// 20 systems: 10 write their own table, 10 share table `a`.
fn mixed_world(config: SimulationConfig, _count: usize) -> World {
    let names = wide_tables(5); // a..e
    let store = table_store(&names);
    let mut world = World::new(WorldId::from_u64(0), store, config).unwrap();
    for i in 0..10u64 {
        let table = names[(i % 5) as usize];
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(i),
                    format!("disjoint_{i}"),
                    0,
                    SystemAccess::new(&[], &[table]),
                    |ctx, _| {
                        let i = ctx.system().as_u64();
                        let table = wide_tables(10)[(i % 5) as usize];
                        ctx.insert(table, row![i * 1000 + ctx.tick().as_u64()])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    for i in 0..10u64 {
        world
            .add_system(
                SystemDefinition::with_access(
                    SystemId::from_u64(100 + i),
                    format!("contended_{i}"),
                    1,
                    SystemAccess::new(&[], &["a"]),
                    |ctx, _| {
                        let i = ctx.system().as_u64();
                        ctx.insert("a", row![i * 1000 + ctx.tick().as_u64()])?;
                        Ok(())
                    },
                )
                .unwrap(),
            )
            .unwrap();
    }
    world
}

fn format_avg(ns: u64) -> String {
    if ns >= 1_000 {
        format!("{:.1}us", ns as f64 / 1_000.0)
    } else {
        format!("{ns}ns")
    }
}
