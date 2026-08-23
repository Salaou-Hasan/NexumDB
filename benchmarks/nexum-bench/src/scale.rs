//! Scale benchmarks — dataset-sized workloads at 100K / 1M / 5M / 10M rows
//! (ADR-015 D2). The central question: does a one-row update at 10M rows
//! behave like a one-row update at 100K rows (scale with the *changed set*,
//! not the table), or does it degrade toward O(N)?
//!
//! Run: `cargo run --release -p nexum-bench -- --scale 1_000_000`
//! (pass `0` to run every configured size; `--rows N` selects one).

use std::path::PathBuf;
use std::time::Instant;

use nexum_core::RowId;
use nexum_subscription::Query;
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wal::{DurabilityPolicy, Wal};

use crate::{ensure_players, player_row};

/// The dataset sizes Phase 15 requires. 25M is attempted only if `--25m`
/// is passed (hardware-dependent).
pub const SIZES: &[u64] = &[100_000, 1_000_000, 5_000_000, 10_000_000];

/// Populates `store` with `rows` players (one transaction per row — the
/// honest per-row commit cost — and returns construction time).
fn populate(store: &mut TableStore, rows: u64) -> f64 {
    ensure_players(store);
    let start = Instant::now();
    for id in 0..rows {
        let mut tx = Transaction::begin(store);
        tx.insert(store, "players", player_row(id)).unwrap();
        tx.commit(store).unwrap();
    }
    start.elapsed().as_secs_f64()
}

/// Bulk populate in one transaction per batch (the fast path; used where
/// construction time would otherwise dominate the measurement).
fn populate_bulk(store: &mut TableStore, rows: u64) -> f64 {
    ensure_players(store);
    let start = Instant::now();
    const BATCH: u64 = 10_000;
    for batch_start in (0..rows).step_by(BATCH as usize) {
        let mut tx = Transaction::begin(store);
        for id in batch_start..(batch_start + BATCH).min(rows) {
            tx.insert(store, "players", player_row(id)).unwrap();
        }
        tx.commit(store).unwrap();
    }
    start.elapsed().as_secs_f64()
}

fn mb(bytes: usize) -> f64 {
    bytes as f64 / (1024.0 * 1024.0)
}

pub fn run(rows: u64) {
    let sizes: Vec<u64> = if rows > 0 { vec![rows] } else { SIZES.to_vec() };
    for &size in &sizes {
        if size < 5_000_000 {
            scale_at(size);
        } else {
            // 5M/10M: per-row-transaction construction is the dominant
            // cost; use bulk populate for the pre-populated measurements,
            // and measure the per-row path separately at a smaller sample.
            scale_at_large(size);
        }
    }
}

/// Full measurement at a size where per-row transaction construction is
/// affordable.
fn scale_at(rows: u64) {
    println!("================ scale: {rows} rows ================");
    let mut store = TableStore::new();
    let construct_s = populate(&mut store, rows);
    println!(
        "{:<52} {:>10.2} s   ({:>10.0} rows/s, per-row tx path)",
        "construct (1 tx per row)",
        construct_s,
        rows as f64 / construct_s
    );

    let row_ids: Vec<RowId> = store
        .table("players")
        .unwrap()
        .scan()
        .map(|(rid, _)| rid)
        .collect();
    assert_eq!(row_ids.len(), rows as usize, "all rows present");

    // --- primary-key lookup (direct table.get, no tx) ---
    let n = row_ids.len();
    let sample = n.min(200_000);
    let mid = row_ids[n / 2];
    let start = Instant::now();
    let mut sum = 0usize;
    for i in 0..sample {
        let row = store.table("players").unwrap().get(row_ids[i % n]).unwrap();
        sum += row.len();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "PK lookup (direct)",
        ns,
        1e9 / ns
    );

    // --- random lookup (uniform stride) ---
    let start = Instant::now();
    for i in 0..sample {
        let idx = (i * 7919) % n; // deterministic pseudo-random stride
        let row = store.table("players").unwrap().get(row_ids[idx]).unwrap();
        sum += row.len();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "random lookup (deterministic stride)",
        ns,
        1e9 / ns
    );

    // --- THE critical test: update exactly one row ---
    // The value alternates every iteration so each commit is a REAL change
    // (identical-value updates are no-ops and would measure nothing —
    // ADR-015 D3).
    let mut tx = Transaction::begin(&mut store);
    tx.get(&store, "players", mid).unwrap();
    tx.update(&store, "players", mid, player_row(1)).unwrap();
    let changes = tx.commit(&mut store).unwrap();
    let change_ns = {
        let mut toggle = 0u64;
        let start = Instant::now();
        for _ in 0..sample.min(50_000) {
            toggle = 1 - toggle;
            let mut t = Transaction::begin(&mut store);
            let _ = t.get(&store, "players", mid).unwrap();
            t.update(&store, "players", mid, player_row(1 + toggle))
                .unwrap();
            let _ = t.commit(&mut store).unwrap();
        }
        start.elapsed().as_secs_f64() * 1e9 / sample.min(50_000) as f64
    };
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s, {} changes)",
        "UPDATE exactly one row (tx + OCC + commit)",
        change_ns,
        1e9 / change_ns,
        changes.len()
    );

    // --- scan (full table, O(N) by design) ---
    let start = Instant::now();
    let mut scanned = 0usize;
    for _ in 0..3 {
        scanned += store.table("players").unwrap().scan().count();
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / 3.0;
    println!(
        "{:<52} {:>10.1} µs  ({:>10.0} rows/µs)",
        "full table scan",
        scan_ns / 1_000.0,
        scanned as f64 / 3.0 / (scan_ns / 1_000.0)
    );

    // --- index lookup ---
    let start = Instant::now();
    let mut total = 0usize;
    for _ in 0..sample.min(50_000) {
        total += store
            .table("players")
            .unwrap()
            .lookup("by_zone", &[nexum_core::Value::U64(42)])
            .unwrap()
            .len();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample.min(50_000) as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s, ~{} hits)",
        "index lookup (by_zone)",
        ns,
        1e9 / ns,
        total / sample.clamp(1, 50_000)
    );

    // --- subscription snapshot at this size ---
    let mut registry = nexum_subscription::SubscriptionRegistry::new();
    let query = Query::builder("players").build().unwrap();
    let start = Instant::now();
    let sub = registry.subscribe(&store, query).unwrap();
    let snapshot_ns = start.elapsed().as_secs_f64() * 1e9;
    let updates = registry.drain(sub).unwrap();
    let rows_in_snapshot = match &updates[0] {
        nexum_subscription::SubscriptionUpdate::Initial { rows, .. } => rows.len(),
        other => panic!("expected Initial, got {other:?}"),
    };
    println!(
        "{:<52} {:>10.1} ms  ({} rows delivered)",
        "subscription initial snapshot",
        snapshot_ns / 1e6,
        rows_in_snapshot
    );

    // --- single-row subscription delta (real change every iteration) ---
    let mut toggle = 0u64;
    let start = Instant::now();
    for _ in 0..sample.min(50_000) {
        toggle = 1 - toggle;
        let mut t = Transaction::begin(&mut store);
        t.update(&store, "players", mid, player_row(1 + toggle))
            .unwrap();
        let changes = t.commit(&mut store).unwrap();
        registry.apply_changes(&store, &changes);
        registry.drain(sub).unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample.min(50_000) as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "single-row subscription delta",
        ns,
        1e9 / ns
    );

    // --- snapshot creation (capture + serialize) ---
    let dir = temp_dir("nexum-bench-snap");
    let snapshot = nexum_wal::Snapshot::capture(&store, 0);
    let start = Instant::now();
    let path = snapshot.write(&dir).unwrap();
    let write_ns = start.elapsed().as_secs_f64() * 1e9;
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    println!(
        "{:<52} {:>10.1} ms  ({:.1} MB on disk)",
        "snapshot capture + write",
        write_ns / 1e6,
        size as f64 / (1024.0 * 1024.0)
    );

    // --- restore ---
    let start = Instant::now();
    let read_back = nexum_wal::Snapshot::read(&path).unwrap();
    let mut restored = TableStore::new();
    restored
        .restore(
            read_back.tables,
            read_back.next_table_id,
            read_back.next_transaction_id,
        )
        .unwrap();
    let restore_ns = start.elapsed().as_secs_f64() * 1e9;
    let restored_count = restored.table("players").unwrap().len();
    println!(
        "{:<52} {:>10.1} ms  ({} rows restored)",
        "snapshot restore",
        restore_ns / 1e6,
        restored_count
    );
    assert_eq!(restored_count, rows as usize, "recovered state == original");
    let _ = std::fs::remove_dir_all(&dir);

    // --- WAL append of the one-row change set ---
    let dir = temp_dir("nexum-bench-wal");
    let mut wal = Wal::create(&dir.join("t.wal"), DurabilityPolicy::Flush).unwrap();
    let start = Instant::now();
    let appends = sample.min(50_000);
    for _ in 0..appends {
        wal.append(nexum_core::TransactionId::from_u64(0), &changes)
            .unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / appends as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} appends/s, flush)",
        "WAL append (1-row change set)",
        ns,
        1e9 / ns
    );
    let wal_size = std::fs::metadata(dir.join("t.wal"))
        .map(|m| m.len())
        .unwrap_or(0);
    println!(
        "{:<52} {:>10.1} MB  ({:>10.0} bytes/append)",
        "WAL size after appends",
        mb(wal_size as usize),
        if appends > 0 {
            wal_size as f64 / appends as f64
        } else {
            0.0
        }
    );

    // --- WAL replay (recovery) ---
    let start = Instant::now();
    let (recovered, _truncated) = wal.recover_changes().unwrap();
    let replay_ns = start.elapsed().as_secs_f64() * 1e9;
    println!(
        "{:<52} {:>10.1} ms  ({} txs replayed)",
        "WAL replay",
        replay_ns / 1e6,
        recovered.len()
    );
    let _ = std::fs::remove_dir_all(&dir);

    println!(
        "{:<52} {:>8.0} MB estimated  ({:.0} bytes/row est.)",
        "estimated table memory",
        mb(rows as usize * crate::estimated_row_bytes()),
        crate::estimated_row_bytes()
    );
    let _ = sum;
    println!();
}

/// 5M/10M: bulk-populate to keep construction tractable, then measure the
/// same critical operations (update, lookup, scan, subscription, snapshot,
/// recovery) at full size.
fn scale_at_large(rows: u64) {
    println!("================ scale: {rows} rows (bulk populate) ================");
    let mut store = TableStore::new();
    let construct_s = populate_bulk(&mut store, rows);
    println!(
        "{:<52} {:>10.2} s   ({:>10.0} rows/s, batched tx)",
        "construct (batched tx)",
        construct_s,
        rows as f64 / construct_s
    );

    let n = store.table("players").unwrap().len();
    let row_ids: Vec<RowId> = store
        .table("players")
        .unwrap()
        .scan()
        .map(|(rid, _)| rid)
        .collect();
    assert_eq!(row_ids.len(), rows as usize, "all rows present");

    // PK + random lookup.
    let sample = n.min(100_000);
    let start = Instant::now();
    for i in 0..sample {
        let _ = store.table("players").unwrap().get(row_ids[i % n]).unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample as f64;
    println!("{:<52} {:>10.1} ns", "PK lookup (direct)", ns);

    let start = Instant::now();
    for i in 0..sample {
        let _ = store
            .table("players")
            .unwrap()
            .get(row_ids[(i * 7919) % n])
            .unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / sample as f64;
    println!(
        "{:<52} {:>10.1} ns",
        "random lookup (deterministic stride)", ns
    );

    // THE critical test at large scale: update exactly one row. The value
    // alternates so every commit is a REAL change (no-op updates would
    // measure nothing — ADR-015 D3).
    let mid = row_ids[n / 2];
    let updates = sample.min(20_000);
    let mut toggle = 0u64;
    let start = Instant::now();
    for _ in 0..updates {
        toggle = 1 - toggle;
        let mut t = Transaction::begin(&mut store);
        let _ = t.get(&store, "players", mid).unwrap();
        t.update(&store, "players", mid, player_row(1 + toggle))
            .unwrap();
        let _ = t.commit(&mut store).unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / updates as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "UPDATE exactly one row (tx + OCC + commit)",
        ns,
        1e9 / ns
    );

    // Scan.
    let start = Instant::now();
    let mut scanned = 0usize;
    for _ in 0..3 {
        scanned += store.table("players").unwrap().scan().count();
    }
    let scan_ns = start.elapsed().as_secs_f64() * 1e9 / 3.0;
    println!(
        "{:<52} {:>10.1} µs  ({:>10.0} rows/µs)",
        "full table scan",
        scan_ns / 1_000.0,
        scanned as f64 / 3.0 / (scan_ns / 1_000.0)
    );

    // Subscription snapshot + single-row delta.
    let mut registry = nexum_subscription::SubscriptionRegistry::new();
    let query = Query::builder("players").build().unwrap();
    let start = Instant::now();
    let sub = registry.subscribe(&store, query).unwrap();
    let snapshot_ns = start.elapsed().as_secs_f64() * 1e9;
    let rows_in_snapshot = match &registry.drain(sub).unwrap()[0] {
        nexum_subscription::SubscriptionUpdate::Initial { rows, .. } => rows.len(),
        other => panic!("expected Initial, got {other:?}"),
    };
    println!(
        "{:<52} {:>10.1} ms  ({} rows delivered)",
        "subscription initial snapshot",
        snapshot_ns / 1e6,
        rows_in_snapshot
    );

    let deltas = sample.min(10_000);
    let mut toggle = 0u64;
    let start = Instant::now();
    for _ in 0..deltas {
        toggle = 1 - toggle;
        let mut t = Transaction::begin(&mut store);
        t.update(&store, "players", mid, player_row(1 + toggle))
            .unwrap();
        let changes = t.commit(&mut store).unwrap();
        registry.apply_changes(&store, &changes);
        registry.drain(sub).unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / deltas as f64;
    println!(
        "{:<52} {:>10.1} ns  ({:>10.0} ops/s)",
        "single-row subscription delta",
        ns,
        1e9 / ns
    );

    // Snapshot + restore.
    let dir = temp_dir("nexum-bench-snap");
    let snapshot = nexum_wal::Snapshot::capture(&store, 0);
    let start = Instant::now();
    let path = snapshot.write(&dir).unwrap();
    let write_ns = start.elapsed().as_secs_f64() * 1e9;
    let size = std::fs::metadata(&path).map(|m| m.len()).unwrap_or(0);
    println!(
        "{:<52} {:>10.1} ms  ({:.1} MB on disk)",
        "snapshot capture + write",
        write_ns / 1e6,
        size as f64 / (1024.0 * 1024.0)
    );
    let start = Instant::now();
    let read_back = nexum_wal::Snapshot::read(&path).unwrap();
    let mut restored = TableStore::new();
    restored
        .restore(
            read_back.tables,
            read_back.next_table_id,
            read_back.next_transaction_id,
        )
        .unwrap();
    let restore_ns = start.elapsed().as_secs_f64() * 1e9;
    let restored_count = restored.table("players").unwrap().len();
    println!(
        "{:<52} {:>10.1} ms  ({} rows restored)",
        "snapshot restore",
        restore_ns / 1e6,
        restored_count
    );
    assert_eq!(restored_count, rows as usize, "recovered state == original");
    let _ = std::fs::remove_dir_all(&dir);

    println!(
        "{:<52} {:>8.0} MB estimated  ({:.0} bytes/row est.)",
        "estimated table memory",
        mb(rows as usize * crate::estimated_row_bytes()),
        crate::estimated_row_bytes()
    );
    println!();
}

/// Part J — the "large authoritative dataset, few active entities" scenario:
/// 10M rows exist, but each tick touches only `active` entities. Measures
/// whether tick cost scales with the *active set* or the *total table size*.
/// The `scan_all` system is the workload reference: a tick that scans every
/// row is O(N) by design; the `touch_active` system (PK lookups only) should
/// stay flat as the table grows.
pub fn large_state_tick(total_rows: u64, active: u64) {
    use nexum_core::{ReducerId, SystemId, WorldId};
    use nexum_runtime::WorldFactory;
    use nexum_simulation::{InputFrame, SimulationConfig, SystemDefinition, World};

    println!(
        "================ large-state tick: {total_rows} rows, {active} active ================"
    );
    let mut store = TableStore::new();
    populate_bulk(&mut store, total_rows);
    let row_ids: Vec<RowId> = store
        .table("players")
        .unwrap()
        .scan()
        .map(|(rid, _)| rid)
        .collect();
    let active_ids: Vec<RowId> = row_ids[..active as usize].to_vec();

    // The system is a plain `fn` (no captures), so the active row ids are
    // carried in a small `active` table the system scans each tick.
    let factory: WorldFactory = Box::new(
        move |id: WorldId, mut s: TableStore, sim: SimulationConfig| {
            s.create_table(
                nexum_core::TableSchema::builder("active")
                    .column("id", nexum_core::ColumnType::U64)
                    .build()
                    .unwrap(),
            )
            .unwrap();
            {
                let mut t = Transaction::begin(&mut s);
                for rid in &active_ids {
                    t.insert(
                        &s,
                        "active",
                        nexum_core::Row::new(vec![nexum_core::Value::U64(rid.as_u64())]),
                    )
                    .unwrap();
                }
                t.commit(&mut s).unwrap();
            }
            let mut world = World::new(id, s, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(1), "touch-active", 1, touch_active)
                        .unwrap(),
                )
                .unwrap();
            Ok(world)
        },
    );
    let mut runtime =
        nexum_runtime::Runtime::new(nexum_runtime::RuntimeConfig::new(factory)).unwrap();
    let world = WorldId::from_u64(0);
    runtime
        .create_world(world, SimulationConfig::new())
        .unwrap();
    runtime.start_world(world).unwrap();
    runtime
        .submit_input(world, InputFrame::new(nexum_core::TickId::from_u64(0)))
        .unwrap();

    let ticks = 2_000u64;
    let start = Instant::now();
    for _ in 0..ticks {
        runtime.step().unwrap();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / ticks as f64;
    println!(
        "{:<52} {:>10.1} ns/tick  ({} rows in store)",
        "tick touching only active rows", ns, total_rows
    );
    let _ = ReducerId::from_u64(0);
    println!();
}

/// Reads the `active` table and PK-looks-up each id in `players`: the
/// "few active entities in a huge authoritative dataset" tick.
fn touch_active(
    ctx: &mut nexum_simulation::SimulationContext,
    _frame: &nexum_simulation::InputFrame,
) -> nexum_core::Result<()> {
    for (_rid, row) in ctx.scan("active")? {
        if let Some(nexum_core::Value::U64(id)) = row.get(0) {
            let _ = ctx.get("players", RowId::from_u64(*id))?;
        }
    }
    Ok(())
}

fn temp_dir(name: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}
