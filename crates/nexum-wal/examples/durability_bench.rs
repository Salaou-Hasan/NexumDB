//! Dependency-free timing benchmark for durability (Phase 5 completion
//! criterion). Measures, against files in a temp directory:
//!
//! - WAL append with `Flush` policy (process-crash safe)
//! - WAL append with `Sync` policy (fsync per transaction — the durable mode)
//! - full recovery + replay of N transactions into a fresh store
//! - snapshot write and snapshot load
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nexum-wal --example durability_bench [iterations]
//! ```
//!
//! Baselines only — criterion harnesses land in Phase 15.

use std::hint::black_box;
use std::path::PathBuf;
use std::time::Instant;

use nexum_core::{ColumnType, TableSchema};
use nexum_table::{row, TableStore};
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};

fn bench<F: FnMut()>(name: &str, n: usize, mut f: F) {
    for _ in 0..(n / 10).max(100) {
        f();
    }
    let start = Instant::now();
    for _ in 0..n {
        f();
    }
    let elapsed = start.elapsed();
    let ns_per_op = elapsed.as_nanos() as f64 / n as f64;
    let ops_per_sec = 1e9 / ns_per_op;
    println!("{name:<48} {ns_per_op:>10.1} ns/op  {ops_per_sec:>12.0} ops/s");
}

fn temp_dir(tag: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("nexum-wal-bench-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn player_schema() -> TableSchema {
    TableSchema::builder("players")
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

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(20_000);
    println!("Nexum durability benchmark — {n} iterations\n");

    // WAL append (Flush): one transaction with a single insert per append.
    {
        let dir = temp_dir("append-flush");
        let path = dir.join("log.wal");
        let mut wal = Wal::create(&path, DurabilityPolicy::Flush).unwrap();
        let mut store = TableStore::new();
        store.create_table(player_schema()).unwrap();
        let mut counter = 0u64;
        bench("wal append (Flush), 1 change/tx", n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            tx.insert(
                &store,
                "players",
                row![counter, 10u64, 100i32, (counter as u32) % 1_000_000],
            )
            .unwrap();
            let changes = tx.commit(&mut store).unwrap();
            black_box(wal.append(tx.id(), &changes).unwrap());
            store.drain_changes();
        });
    }

    // WAL append (Sync / fsync): the durable mode is dominated by fsync, so
    // use far fewer iterations.
    {
        let sync_n = 1_000.min(n);
        let dir = temp_dir("append-sync");
        let path = dir.join("log.wal");
        let mut wal = Wal::create(&path, DurabilityPolicy::Sync).unwrap();
        let mut store = TableStore::new();
        store.create_table(player_schema()).unwrap();
        let mut counter = 0u64;
        bench("wal append (Sync/fsync), 1 change/tx", sync_n, || {
            counter += 1;
            let mut tx = Transaction::begin(&mut store);
            tx.insert(
                &store,
                "players",
                row![counter, 10u64, 100i32, (counter as u32) % 1_000_000],
            )
            .unwrap();
            let changes = tx.commit(&mut store).unwrap();
            black_box(wal.append(tx.id(), &changes).unwrap());
            store.drain_changes();
        });
    }

    // Recovery + replay of n transactions (built once, replayed per probe).
    {
        let dir = temp_dir("recover");
        let path = dir.join("log.wal");
        let mut wal = Wal::create(&path, DurabilityPolicy::Flush).unwrap();
        let mut store = TableStore::new();
        store.create_table(player_schema()).unwrap();
        for id in 1..=n as u64 {
            let mut tx = Transaction::begin(&mut store);
            tx.insert(&store, "players", row![id, 10u64, 100i32, id as u32]).unwrap();
            let changes = tx.commit(&mut store).unwrap();
            wal.append(tx.id(), &changes).unwrap();
            store.drain_changes();
        }

        bench("recovery + replay of n txs", 100, || {
            let mut fresh = TableStore::new();
            fresh.create_table(player_schema()).unwrap();
            let report = recover(&mut fresh, &mut wal, &dir).unwrap();
            black_box(report);
        });
    }

    // Snapshot write and load.
    {
        let dir = temp_dir("snapshot");
        let mut store = TableStore::new();
        store.create_table(player_schema()).unwrap();
        for id in 1..=n as u64 {
            store
                .table_mut("players")
                .unwrap()
                .insert(row![id, 10u64, 100i32, id as u32])
                .unwrap();
        }
        store.drain_changes();

        let snapshot = Snapshot::capture(&store, 0);
        let path = snapshot.write(&dir).unwrap();
        bench("snapshot load", 100, || {
            black_box(Snapshot::read(&path).unwrap());
        });
        // Snapshot write is measured separately (fresh file each iteration).
        let _ = std::fs::remove_file(&path);
        bench("snapshot write", 50, || {
            black_box(snapshot.write(&dir).unwrap());
            let _ = std::fs::remove_dir_all(&dir);
            std::fs::create_dir_all(&dir).unwrap();
        });
    }

    println!("\ndone. Baseline numbers only — criterion harnesses land in Phase 15.");
}
