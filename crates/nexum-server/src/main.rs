//! Nexum server binary.
//!
//! The server wires the partition/worker runtime, reducers, subscriptions,
//! simulation, and both network planes together. It is scaffolded in Phase 0
//! and becomes a real server in later phases.

fn main() {
    println!("Nexum server {} — foundation build (Phase 0/1)", env!("CARGO_PKG_VERSION"));
}
