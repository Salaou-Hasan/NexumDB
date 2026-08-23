//! Phase 15 benchmark runner (ADR-015). Release builds only for
//! conclusions: `cargo run --release -p nexum-bench -- ...`.
//!
//! Usage:
//! ```text
//! nexum-bench --micro [storage tx reducer wasm sub sim runtime wal] [--all]
//! nexum-bench --scale N             # one dataset size (e.g. 1_000_000)
//! nexum-bench --scale               # every required size (100K..10M)
//! nexum-bench --list
//! ```

use std::process::ExitCode;

fn usage() -> ! {
    println!(
        "nexum-bench — Phase 15 performance benchmarks\n\
         \n\
         USAGE:\n\
         \x20 nexum-bench --micro [sub...]     micro benchmarks (storage tx reducer wasm sub sim runtime wal; default all)\n\
         \x20 nexum-bench --scale [ROWS]       scale benchmarks at ROWS rows (default: all sizes 100K/1M/5M/10M)\n\
         \x20 nexum-bench --list               list the benchmark groups\n\
         \n\
         Run with --release; conclusions require release builds."
    );
    std::process::exit(2);
}

fn main() -> ExitCode {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() || args.iter().any(|a| a == "--help" || a == "-h") {
        usage();
    }
    if args.iter().any(|a| a == "--list") {
        println!("micro: storage, tx, reducer, wasm, sub, sim, runtime, wal");
        println!(
            "scale: 100K / 1M / 5M / 10M row workloads (insert, lookup, update, scan, index, subscription, snapshot, WAL, recovery)"
        );
        return ExitCode::SUCCESS;
    }
    if let Some(pos) = args.iter().position(|a| a == "--micro") {
        let subcommands: Vec<String> = args[pos + 1..]
            .iter()
            .take_while(|a| !a.starts_with("--"))
            .cloned()
            .collect();
        nexum_bench::micro::run(&subcommands);
        return ExitCode::SUCCESS;
    }
    if let Some(pos) = args.iter().position(|a| a == "--scale") {
        let rows: u64 = args.get(pos + 1).and_then(|v| v.parse().ok()).unwrap_or(0);
        nexum_bench::scale::run(rows);
        return ExitCode::SUCCESS;
    }
    if let Some(pos) = args.iter().position(|a| a == "--large-tick") {
        let total: u64 = args
            .get(pos + 1)
            .and_then(|v| v.parse().ok())
            .unwrap_or(10_000_000);
        let active: u64 = args
            .get(pos + 2)
            .and_then(|v| v.parse().ok())
            .unwrap_or(100);
        nexum_bench::scale::large_state_tick(total, active);
        return ExitCode::SUCCESS;
    }
    usage();
}
