//! Dependency-free timing benchmark for the transaction engine (Phase 4
//! completion criterion).
//!
//! Measures, on a freshly created store per scenario:
//!
//! - read-only transaction (1 get + commit)
//! - single-row write transaction (1 insert)
//! - multi-row transaction (10 inserts)
//! - multi-table transaction (2 tables, 2 writes each)
//! - successful validation+commit cost (overhead above raw storage writes)
//! - conflicting commit (stale read → `Error::Conflict`)
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nexum-tx --example tx_bench [iterations]
//! ```
//!
//! Iterations default to 50_000. Baselines only — criterion harnesses land
//! in Phase 15.

use std::hint::black_box;
use std::time::Instant;

use nexum_core::{ColumnType, Error, TableSchema};
use nexum_table::{row, TableStore};
use nexum_tx::Transaction;

fn player_schema(name: &str) -> TableSchema {
    TableSchema::builder(name)
        .column("id", ColumnType::U64)
        .column("zone_id", ColumnType::U64)
        .column("health", ColumnType::I32)
        .column("level", ColumnType::U32)
        .primary_key(&["id"])
        .index("by_zone", &["zone_id"])
        .unique_index("by_level", &["level"])
        .build()
        .unwrap()
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
        .unwrap_or(50_000);
    println!("Nexum transaction benchmark — {n} iterations\n");

    // Read-only transaction: one get + commit.
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        let p0 = store
            .table_mut("players")
            .unwrap()
            .insert(row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        store.drain_changes();
        bench("read-only tx (1 get + commit)", n, || {
            let mut tx = Transaction::begin(&mut store);
            black_box(tx.get(&store, "players", p0).unwrap());
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
        });
    }

    // Single-row write transaction: one insert + commit. The primary key and
    // unique level vary per iteration so each insert is distinct.
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        let mut counter = 0u64;
        bench("single-row write tx (1 insert)", n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            let handle = tx
                .insert(&store, "players", row![counter, 10u64, 100i32, (counter as u32) % 1_000_000])
                .unwrap();
            black_box(handle);
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Multi-row transaction: ten inserts + commit.
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        let mut counter = 0u64;
        bench("multi-row tx (10 inserts)", n, || {
            counter += 10;
            let mut tx = Transaction::begin(&mut store);
            for offset in 1..=10u64 {
                let id = counter + offset;
                let handle = tx
                    .insert(&store, "players", row![id, 10u64, 100i32, (id as u32) % 1_000_000])
                    .unwrap();
                black_box(handle);
            }
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Multi-table transaction: two tables, two writes each.
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
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
        let mut counter = 0u64;
        bench("multi-table tx (2 tables × 2 writes)", n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            let handle = tx
                .insert(&store, "players", row![counter, 10u64, 100i32, (counter as u32) % 1_000_000])
                .unwrap();
            tx.update(&store, "players", handle, row![counter, 10u64, 90i32, (counter as u32) % 1_000_000])
                .unwrap();
            let coins = tx.insert(&store, "economy", row![counter, 100i64]).unwrap();
            tx.update(&store, "economy", coins, row![counter, 50i64]).unwrap();
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
            store.drain_changes();
        });
    }

    // Successful validation + commit cost on an update transaction.
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        let p0 = store
            .table_mut("players")
            .unwrap()
            .insert(row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        store.drain_changes();
        let mut counter = 100i32;
        bench("validated update tx (read + write)", n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            tx.get(&store, "players", p0).unwrap();
            tx.update(&store, "players", p0, row![1u64, 10u64, counter, 5u32])
                .unwrap();
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    // Scan transaction: set observation + overlay (Phase 4 correction).
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        for id in 1..=10u64 {
            store
                .table_mut("players")
                .unwrap()
                .insert(row![id, 10u64, 100i32, id as u32])
                .unwrap();
        }
        store.drain_changes();
        bench("scan tx (10 rows, epoch obs)", n, || {
            let mut tx = Transaction::begin(&mut store);
            black_box(tx.scan(&store, "players").unwrap());
            let changes = tx.commit(&mut store).unwrap();
            black_box(changes);
        });
    }

    // Conflicting commit: stale read → Error::Conflict (validation cost).
    {
        let mut store = TableStore::new();
        store.create_table(player_schema("players")).unwrap();
        let p0 = store
            .table_mut("players")
            .unwrap()
            .insert(row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        store.drain_changes();
        let mut counter = 100i32;
        bench("conflicting commit (stale read)", n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            tx.get(&store, "players", p0).unwrap();
            // Simulate a concurrent writer bumping the version (always a
            // fresh value, so the no-op update guard never fires).
            store
                .table_mut("players")
                .unwrap()
                .update(p0, row![1u64, 10u64, counter, 5u32])
                .unwrap();
            let err = tx.commit(&mut store).unwrap_err();
            debug_assert!(matches!(err, Error::Conflict(_)));
            black_box(err);
            store.table_mut("players").unwrap().drain_changes();
        });
    }

    println!("\ndone. Baseline numbers only — criterion harnesses land in Phase 15.");
}
