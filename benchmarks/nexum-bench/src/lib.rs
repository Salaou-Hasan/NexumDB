//! Phase 15 performance benchmarks (ADR-015). Two tiers:
//!
//! - **micro** — tight-loop single operations (ns/op) per subsystem
//! - **scale** — dataset-sized workloads at 100K/1M/5M/10M rows measuring
//!   insert, PK lookup, random lookup, single-row update through a full
//!   transaction, scan, index lookup, subscription snapshot + delta,
//!   snapshot creation, WAL append, and recovery.
//!
//! Run (release only — conclusions require it):
//!
//! ```text
//! cargo run --release -p nexum-bench -- --scale 1_000_000
//! cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal
//! cargo run --release -p nexum-bench -- --list
//! ```
//!
//! No optimization is performed before measurement (ADR-015 D1).

#![forbid(unsafe_code)]

pub mod micro;
pub mod scale;

use std::time::Instant;

use nexum_core::{ColumnType, TableSchema};
use nexum_table::TableStore;

/// The canonical bench table schema (id PK + secondary index + payload).
pub fn players_schema() -> TableSchema {
    TableSchema::builder("players")
        .column("id", ColumnType::U64)
        .column("zone", ColumnType::U64)
        .column("health", ColumnType::I32)
        .index("by_zone", &["zone"])
        .build()
        .expect("schema builds")
}

/// Creates the bench schema in `store`, idempotently.
pub fn ensure_players(store: &mut TableStore) {
    if store.table("players").is_none() {
        store
            .create_table(players_schema())
            .expect("table creation succeeds");
    }
}

/// A row for player `id` in zone `id % 256`.
pub fn player_row(id: u64) -> nexum_core::Row {
    nexum_core::row![id, id % 256, 100i32]
}

/// Mean ns/op for `f` after `warmup` iterations.
pub fn bench_ns(name: &str, iterations: usize, warmup: usize, mut f: impl FnMut()) {
    for _ in 0..warmup {
        f();
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    let unit = if ns >= 1_000.0 { "µs" } else { "ns" };
    let value = if ns >= 1_000.0 { ns / 1_000.0 } else { ns };
    println!("{name:<52} {value:>10.1} {unit}/op");
}

/// Approximate per-row bytes for the bench schema's three columns.
///
/// `Value` is a tagged enum; each stored row owns a `Vec<Value>`. This is a
/// documented ESTIMATE (ADR-015: MEASURED vs ESTIMATED are kept distinct) —
/// exact RSS deltas are reported by the scale benchmarks where the OS makes
/// them available.
pub fn estimated_row_bytes() -> usize {
    // Value enum: tag (1) + 8-byte payload, padded to 16 bytes on 64-bit.
    // 3 values + Vec header (24) + allocation overhead (~16).
    3 * 16 + 24 + 16
}

/// Formats an ops/sec figure from a measured ns/op value.
pub fn ops_per_sec(ns_per_op: f64) -> f64 {
    1e9 / ns_per_op
}
