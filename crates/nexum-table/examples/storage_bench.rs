//! Dependency-free timing benchmark for the storage engine hot paths.
//!
//! Measures the important storage operations (Phase 3 completion criterion)
//! on both the raw [`StorageTable`] and the full [`Table`] (with derived
//! indexes), so the cost of index maintenance is visible:
//!
//! - insert, get, update, delete, scan, version lookup, change drain
//!   (storage)
//! - insert, lookup (index), get-by-primary-key (table with indexes)
//!
//! Run with:
//!
//! ```text
//! cargo run --release -p nexum-table --example storage_bench [iterations]
//! ```
//!
//! Iterations default to 100_000. Proper criterion harnesses arrive with
//! Phase 15; this establishes baseline numbers only.

use std::hint::black_box;
use std::time::Instant;

use nexum_core::{ColumnType, TableSchema, TableId, Value};
use nexum_storage::StorageTable;
use nexum_table::{row, TableStore};

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

/// Runs `f` `n` times, returns nanoseconds per iteration.
fn bench<F: FnMut()>(name: &str, n: usize, mut f: F) {
    // Warm up.
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
    println!(
        "{name:<28} {ns_per_op:>10.1} ns/op  {ops_per_sec:>12.0} ops/s"
    );
}

fn storage_table() -> StorageTable {
    StorageTable::new(TableId::from_u64(0), player_schema())
}

fn main() {
    let n: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(100_000);
    println!("Nexum storage benchmark — {n} iterations\n");

    // ---- Raw storage engine ----
    println!("storage (no indexes)");
    let mut t = storage_table();
    bench("insert", n, || {
        let id = t.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        black_box(id);
    });

    let mut t = storage_table();
    let probe = t.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
    bench("get", n, || {
        black_box(t.get(probe));
    });
    bench("version_of", n, || {
        black_box(t.version_of(probe));
    });
    bench("update", n, || {
        t.update(probe, row![1u64, 10u64, 99i32, 5u32]).unwrap();
    });
    let mut cur = probe;
    bench("delete+reinsert", n, || {
        t.delete(cur).unwrap();
        cur = t.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        black_box(cur);
    });
    // Note: storage scan runs after delete+reinsert, so it measures a 1-row
    // map — the table-side scan below measures n+1 rows (see the table
    // section for the O(n) picture).
    bench("scan (full)", n, || {
        black_box(t.scan().count());
    });
    bench("drain_changes (clear)", n, || {
        black_box(t.drain_changes());
    });

    // ---- Table layer (with derived indexes) ----
    println!("\ntable (with derived indexes)");
    let mut store = TableStore::new();
    store.create_table(player_schema()).unwrap();

    // Probe row first: id and level are far outside the insert bench's ranges
    // (insert ids run 1..~1.1n, levels are counter modulo 1_000_000), so
    // nothing collides with the unique `by_level` and `primary` indexes.
    // The borrow is scoped so `store` is free for the closures below.
    let mut probe = {
        let table = store.table_mut("players").unwrap();
        table
            .insert(row![9_000_000u64, 20u64, 90i32, 2_000_000u32])
            .unwrap()
    };

    // Insert with a varying primary key and unique level so each iteration
    // adds a distinct row (the cost of a full insert with index maintenance).
    let mut counter = 0u64;
    bench("insert", n, || {
        counter += 1;
        let table = store.table_mut("players").unwrap();
        let id = table
            .insert(row![counter, 10u64, 100i32, (counter as u32) % 1_000_000])
            .unwrap();
        black_box(id);
    });
    bench("get", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.get(probe));
    });
    bench("version_of", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.version_of(probe));
    });
    // Lookups run before the update bench, which moves the probe's keys.
    bench("lookup by_zone (index)", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.lookup("by_zone", &[Value::U64(20)]).unwrap());
    });
    bench("lookup by_level (unique)", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.lookup("by_level", &[Value::U32(2_000_000)]).unwrap());
    });
    bench("get_by_primary_key", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.get_by_primary_key(&[Value::U64(9_000_000)]).unwrap());
    });
    bench("update (moves index keys)", n, || {
        let table = store.table_mut("players").unwrap();
        table
            .update(probe, row![9_000_001u64, 21u64, 89i32, 2_000_001u32])
            .unwrap();
    });
    bench("delete+reinsert", n, || {
        let table = store.table_mut("players").unwrap();
        table.delete(probe).unwrap();
        probe = table
            .insert(row![9_000_001u64, 21u64, 89i32, 2_000_001u32])
            .unwrap();
    });
    bench("scan (full)", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.scan().count());
    });
    bench("drain_changes", n, || {
        let table = store.table_mut("players").unwrap();
        black_box(table.drain_changes());
    });

    println!("\ndone. Baseline numbers only — criterion harnesses land in Phase 15.");
}
