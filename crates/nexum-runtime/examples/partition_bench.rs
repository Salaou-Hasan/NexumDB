//! Phase 12 partition benchmarks — honest baselines, not claims (ADR-012
//! §11). Run with: `cargo run --release -p nexum-runtime --example
//! partition_bench [iterations]`.
//!
//! Measures: partition-scale scheduling (1/2/4/8 partitions), cross-partition
//! message throughput (1 and 10 messages per tick), external injection,
//! delivery latency, and the durability/observation overhead of a
//! partition tick (WAL + subscription).

use std::time::Instant;

use nexum_core::row;
use nexum_core::{ColumnType, PartitionId, ReducerId, SystemId, WorldId};
use nexum_reducer::{ReducerArgs, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;

fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..100 {
        f(); // warmup
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    println!("{name:<44} {ns:>12.1} ns/op");
}

fn ensure_ledger(store: &mut TableStore) {
    if store.table("ledger").is_none() {
        store
            .create_table(
                nexum_core::TableSchema::builder("ledger")
                    .column("id", ColumnType::U64)
                    .column("from", ColumnType::U64)
                    .column("to", ColumnType::U64)
                    .column("amount", ColumnType::I64)
                    .primary_key(&["id"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
    }
}

/// The shared `transfer` handler used by every bench world.
fn transfer_handler(
    ctx: &mut nexum_reducer::ReducerContext<'_>,
    args: &ReducerArgs,
) -> nexum_core::Result<nexum_core::Value> {
    let amount = args.require_i64("amount")?;
    let to = args.require_u64("to")?;
    let from = args.require_u64("from")?;
    let seq = args.require_u64("seq")?;
    ctx.insert("ledger", row![seq, from, to, amount])?;
    Ok(nexum_core::Value::U64(seq))
}

/// A partition world with a `transfer` handler and a ring sender (1 message
/// per tick to the next partition). Capture-free.
fn partition_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_ledger(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "transfer", transfer_handler)
                        .unwrap(),
                )
                .unwrap();
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(0), "sender", 0, |ctx, _| {
                    let from = ctx.partition().as_u64();
                    let target = (from + 1) % 4;
                    let tick = ctx.tick().as_u64();
                    ctx.send_to(
                        PartitionId::from_u64(target),
                        "transfer",
                        ReducerArgs::new()
                            .insert("amount", 10i64)
                            .insert("to", target)
                            .insert("from", from)
                            .insert("seq", tick),
                    )?;
                    Ok(())
                })
                .unwrap(),
            )?;
            Ok(world)
        },
    )
}

/// A message-heavy variant: 10 messages per tick per sender. Capture-free.
fn partition_bulk_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_ledger(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(0), "transfer", transfer_handler)
                        .unwrap(),
                )
                .unwrap();
            world.add_system(
                SystemDefinition::new(SystemId::from_u64(0), "bulk", 0, |ctx, _| {
                    let from = ctx.partition().as_u64();
                    let target = (from + 1) % 4;
                    let tick = ctx.tick().as_u64();
                    for i in 0..10u64 {
                        ctx.send_to(
                            PartitionId::from_u64(target),
                            "transfer",
                            ReducerArgs::new()
                                .insert("amount", 10i64)
                                .insert("to", target)
                                .insert("from", from)
                                .insert("seq", tick * 10 + i),
                        )?;
                    }
                    Ok(())
                })
                .unwrap(),
            )?;
            Ok(world)
        },
    )
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);

    // 1-4. Partition-scale scheduling — N partitions, one step ticks them all.
    for count in [1usize, 2, 4, 8] {
        let mut runtime = Runtime::new(RuntimeConfig::new(partition_factory())).unwrap();
        for p in 0..count as u64 {
            runtime
                .create_world(WorldId::from_u64(p), SimulationConfig::new())
                .unwrap();
            runtime
                .register_partition(PartitionId::from_u64(p), WorldId::from_u64(p))
                .unwrap();
            runtime.start_world(WorldId::from_u64(p)).unwrap();
        }
        bench(
            &format!("{count} partitions, 1 msg/tick (step)"),
            iterations / 2,
            || {
                runtime.step().unwrap();
            },
        );
    }

    // 5. Message-heavy — 10 messages per sender per tick.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(partition_bulk_factory())).unwrap();
        for p in 0..4u64 {
            runtime
                .create_world(WorldId::from_u64(p), SimulationConfig::new())
                .unwrap();
            runtime
                .register_partition(PartitionId::from_u64(p), WorldId::from_u64(p))
                .unwrap();
            runtime.start_world(WorldId::from_u64(p)).unwrap();
        }
        bench("4 partitions, 10 msg/tick (step)", iterations / 2, || {
            runtime.step().unwrap();
        });
    }

    // 6. External injection rate — one send_message per op (delivery follows
    // on the next step).
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(partition_factory())).unwrap();
        for p in 0..4u64 {
            runtime
                .create_world(WorldId::from_u64(p), SimulationConfig::new())
                .unwrap();
            runtime
                .register_partition(PartitionId::from_u64(p), WorldId::from_u64(p))
                .unwrap();
            runtime.start_world(WorldId::from_u64(p)).unwrap();
        }
        let mut seq = 0u64;
        bench("external send_message (routed)", iterations, || {
            runtime
                .send_message(
                    PartitionId::from_u64(0),
                    PartitionId::from_u64(1),
                    "transfer",
                    ReducerArgs::new()
                        .insert("amount", 1i64)
                        .insert("to", 1u64)
                        .insert("from", 0u64)
                        .insert("seq", seq),
                )
                .unwrap();
            seq += 1;
        });
    }

    // 7. Delivery latency — message sent at tick N is delivered at tick N+1.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(partition_factory())).unwrap();
        for p in 0..2u64 {
            runtime
                .create_world(WorldId::from_u64(p), SimulationConfig::new())
                .unwrap();
            runtime
                .register_partition(PartitionId::from_u64(p), WorldId::from_u64(p))
                .unwrap();
            runtime.start_world(WorldId::from_u64(p)).unwrap();
        }
        bench(
            "2 partitions, delivery + commit (2 steps)",
            iterations / 2,
            || {
                runtime.step().unwrap(); // send at tick N
                runtime.step().unwrap(); // deliver at tick N+1
            },
        );
    }

    // 8. Tick + WAL append on a partition.
    {
        let dir = temp_dir("nexum-runtime-partition-bench-wal");
        let mut runtime = Runtime::new(
            RuntimeConfig::new(partition_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        for p in 0..2u64 {
            runtime
                .create_world(WorldId::from_u64(p), SimulationConfig::new())
                .unwrap();
            runtime
                .register_partition(PartitionId::from_u64(p), WorldId::from_u64(p))
                .unwrap();
            runtime.start_world(WorldId::from_u64(p)).unwrap();
        }
        bench("partition tick + WAL append", iterations / 2, || {
            runtime.step().unwrap();
        });
        let _ = std::fs::remove_dir_all(&dir);
    }

    // 9. Tick + subscription fan-out on a partition.
    {
        let mut runtime = Runtime::new(RuntimeConfig::new(partition_factory())).unwrap();
        let world = WorldId::from_u64(1);
        runtime
            .create_world(WorldId::from_u64(0), SimulationConfig::new())
            .unwrap();
        runtime
            .register_partition(PartitionId::from_u64(0), WorldId::from_u64(0))
            .unwrap();
        runtime.start_world(WorldId::from_u64(0)).unwrap();
        runtime
            .create_world(world, SimulationConfig::new())
            .unwrap();
        runtime
            .register_partition(PartitionId::from_u64(1), world)
            .unwrap();
        runtime.start_world(world).unwrap();
        let sub = runtime
            .subscribe(world, Query::builder("ledger").build().unwrap())
            .unwrap();
        runtime.drain(world, sub).unwrap(); // Initial
        bench("partition tick + subscription fan-out", iterations, || {
            runtime.step().unwrap();
            let _ = runtime.drain(world, sub).unwrap();
        });
    }
}
