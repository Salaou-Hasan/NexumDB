//! Baseline benchmarks for the subscription engine (Phase 8 brief §18).
//!
//! Measures the 12 requested scenarios — subscription creation, initial
//! snapshot, single/subscription fan-out, matching and non-matching
//! fan-out, updates entering/leaving a predicate, delete, multi-table
//! transactions, and resync — plus commit overhead. These are *baselines*:
//! correctness first, optimization in a later phase.
//!
//! Run: `cargo run --release -p nexum-subscription --example subscription_bench [players]`

use std::collections::VecDeque;
use std::time::Instant;

use nexum_core::ColumnType;
use nexum_core::RowId;
use nexum_core::row;
use nexum_core::schema::TableSchema;
use nexum_subscription::{Query, SubscriptionRegistry, SubscriptionUpdate};
use nexum_table::TableStore;
use nexum_tx::Transaction;

/// Builds a `players` table pre-populated with `count` rows.
/// Half are in zone 10, half in zone 20; health varies.
fn world(count: usize) -> TableStore {
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
    {
        let table = store.table_mut("players").unwrap();
        for i in 0..count {
            let id = i as u64;
            let zone: u64 = if i % 2 == 0 { 10 } else { 20 };
            table
                .insert(row![id, zone, (i as i32) % 100, (i % 7) as u32])
                .unwrap();
        }
    }
    store
}

fn commit_one(
    store: &mut TableStore,
    body: impl FnOnce(&mut Transaction, &TableStore),
) -> Vec<nexum_storage::Change> {
    let mut tx = Transaction::begin(store);
    body(&mut tx, store);
    tx.commit(store).unwrap()
}

/// Times `iterations` runs of `f` and prints the average in µs.
fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    f(); // warm-up
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let elapsed = start.elapsed().as_nanos() as f64 / iterations as f64 / 1_000.0;
    println!("{name:<44} {elapsed:>10.2} µs/op");
}

fn zone10() -> Query {
    Query::builder("players")
        .predicate_eq("zone_id", 10u64)
        .build()
        .unwrap()
}

fn main() {
    let players: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(5_000);
    println!("players seeded: {players}");

    // 1. Subscription creation on an empty table.
    bench("create subscription (empty table)", 10_000, || {
        let mut store = TableStore::new();
        store
            .create_table(
                TableSchema::builder("players")
                    .column("id", ColumnType::U64)
                    .column("zone_id", ColumnType::U64)
                    .column("health", ColumnType::I32)
                    .column("level", ColumnType::U32)
                    .build()
                    .unwrap(),
            )
            .unwrap();
        let mut registry = SubscriptionRegistry::new();
        let _ = registry.subscribe(&store, zone10()).unwrap();
        let _ = std::hint::black_box(&registry);
    });

    // 2. Initial snapshot over a populated table.
    bench("initial snapshot (populated table)", 200, || {
        let store = world(players);
        let mut registry = SubscriptionRegistry::new();
        let _ = registry.subscribe(&store, zone10()).unwrap();
        let _ = std::hint::black_box(&registry);
    });

    // 3. One subscription + one committed change.
    {
        let mut store = world(100);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        let mut next = 10_000u64;
        bench("one sub + one change (matching insert)", 10_000, || {
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            let _ = registry.drain(sub).unwrap();
        });
    }

    // 4 & 5. Fan-out: one change, many subscriptions.
    for (count, matching) in [(100usize, true), (1_000, true), (1_000, false)] {
        let label = if matching {
            format!("{count} subs + one change (matching)")
        } else {
            format!("{count} subs + one change (not matching)")
        };
        let mut store = world(100);
        let mut registry = SubscriptionRegistry::new();
        let query = if matching {
            zone10()
        } else {
            Query::builder("players")
                .predicate_eq("zone_id", 99u64)
                .build()
                .unwrap()
        };
        let mut subs = Vec::new();
        for _ in 0..count {
            let sub = registry.subscribe(&store, query.clone()).unwrap();
            let _ = registry.drain(sub).unwrap();
            subs.push(sub);
        }
        let mut next = 20_000u64;
        bench(&label, 1_000, || {
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            for sub in &subs {
                let _ = registry.drain(*sub).unwrap();
            }
        });
    }

    // 6 & 7. Many subscriptions watching the same rows vs. unrelated rows.
    {
        let mut store = world(10);
        let mut registry = SubscriptionRegistry::new();
        let mut subs = Vec::new();
        for _ in 0..1_000 {
            let sub = registry.subscribe(&store, zone10()).unwrap();
            let _ = registry.drain(sub).unwrap();
            subs.push(sub);
        }
        let mut next = 30_000u64;
        bench("1,000 subs, change matches every sub", 1_000, || {
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            for sub in &subs {
                let _ = registry.drain(*sub).unwrap();
            }
        });
    }

    // 8. Update entering the predicate. Rows rotate zone 20 → zone 10 so
    // every iteration genuinely re-enters the view.
    {
        let mut store = world(50);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        let mut zone20_rows: VecDeque<RowId> = (1..50).step_by(2).map(RowId::from_u64).collect();
        let mut next = 40_000u64;
        let mut row_count = 50u64;
        bench("update entering predicate", 10_000, || {
            let victim = zone20_rows.pop_front().unwrap();
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 20u64, 50i32, 1u32])
                    .unwrap();
                tx.update(
                    store,
                    "players",
                    victim,
                    row![victim.as_u64(), 10u64, 1i32, 1u32],
                )
                .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            let _ = registry.drain(sub).unwrap();
            zone20_rows.push_back(RowId::from_u64(row_count));
            row_count += 1;
        });
    }

    // 9. Update leaving the predicate. Rows rotate zone 10 → zone 20 so
    // every iteration genuinely leaves the view.
    {
        let mut store = world(50);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        let mut zone10_rows: VecDeque<RowId> = (0..50).step_by(2).map(RowId::from_u64).collect();
        let mut next = 50_000u64;
        let mut row_count = 50u64;
        bench("update leaving predicate", 10_000, || {
            let victim = zone10_rows.pop_front().unwrap();
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                tx.update(
                    store,
                    "players",
                    victim,
                    row![victim.as_u64(), 20u64, 1i32, 1u32],
                )
                .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            let _ = registry.drain(sub).unwrap();
            zone10_rows.push_back(RowId::from_u64(row_count));
            row_count += 1;
        });
    }

    // 10. Delete of a visible row, rotating over a pool of zone-10 victims
    // refilled by each iteration's insert.
    {
        let mut store = world(50);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        let mut victims: VecDeque<RowId> = (0..50).step_by(2).map(RowId::from_u64).collect();
        let mut next = 60_000u64;
        let mut row_count = 50u64;
        bench("delete visible row", 10_000, || {
            let victim = victims.pop_front().unwrap();
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                tx.delete(store, "players", victim).unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            let _ = registry.drain(sub).unwrap();
            victims.push_back(RowId::from_u64(row_count));
            row_count += 1;
        });
    }

    // 11. Multi-table transaction: players + economy in one commit.
    {
        let mut store = world(50);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        let mut next = 70_000u64;
        bench("multi-table transaction", 10_000, || {
            let changes = commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                tx.insert(store, "economy", row![next, next as i64])
                    .unwrap();
                next += 1;
            });
            registry.apply_changes(&store, &changes);
            let _ = registry.drain(sub).unwrap();
        });
    }

    // 12. Resynchronization over a populated table.
    {
        let store = world(players);
        let mut registry = SubscriptionRegistry::new();
        let sub = registry.subscribe(&store, zone10()).unwrap();
        let _ = registry.drain(sub).unwrap();
        bench("resynchronization", 500, || {
            registry.resync(&store, sub).unwrap();
            let updates = registry.drain(sub).unwrap();
            assert!(matches!(&updates[0], SubscriptionUpdate::Resync { .. }));
        });
    }

    // Overhead reference: the raw commit cost the subscription rides on.
    {
        let mut store = world(100);
        let mut next = 80_000u64;
        bench("reference: raw commit (no subscriptions)", 10_000, || {
            commit_one(&mut store, |tx, store| {
                tx.insert(store, "players", row![next, 10u64, 50i32, 1u32])
                    .unwrap();
                next += 1;
            });
        });
    }
}
