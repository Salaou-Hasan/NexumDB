//! Cross-crate integration tests for Nexum.
//!
//! Cargo does not auto-discover tests at the workspace root (see
//! `tests/integration/README.md`), so tests that span multiple crates live
//! here as a dedicated workspace member and run with
//! `cargo test --workspace`. Tests that exercise a single crate belong in
//! that crate's own `tests/` directory.
//!
//! Each file in `tests/` exercises one real seam of the execution flow:
//!
//! ```text
//! World::tick → Transaction/OCC → Vec<Change> → WAL → SubscriptionRegistry
//! ```
