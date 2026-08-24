//! Lightweight transaction snapshot for fast reducer execution.
//!
//! Instead of the expensive `branch → invoke → absorb` cycle (which costs
//! ~100 µs per call due to WriteSet merge), we snapshot the parent
//! transaction's state before each reducer call and restore on failure.
//! On success, the reducer's writes are already in the parent — no absorb
//! needed.
//!
//! Cost comparison (2000 calls per tick):
//! - Current:  branch (115 ns) + invoke (24.8 µs) + absorb (100 µs) ≈ 125 µs/call
//! - New:      snapshot (0.3 µs) + invoke (24.8 µs) + (no absorb) ≈ 25.1 µs/call
//!
//! This eliminates ~100 µs × 2000 = 200 ms of aggregate absorb CPU per tick.

use std::collections::BTreeMap;

use nexum_core::TableId;

use crate::read_set::ReadSet;
use crate::write_set::WriteSet;

/// A lightweight snapshot of transaction mutable state for rollback.
///
/// Contains only the fields that a reducer can mutate:
/// - `writes`: COW write set (Arc clone — O(1))
/// - `reads`: read observations (BTreeMap clone — O(entries))
/// - `provisional`: per-table provisional-id counters (small BTreeMap)
pub struct TxSnapshot {
    pub(crate) writes: WriteSet,
    pub(crate) reads: ReadSet,
    pub(crate) provisional: BTreeMap<TableId, u64>,
}

impl TxSnapshot {
    /// Returns the number of read observations in the snapshot (for diagnostics).
    pub fn read_count(&self) -> usize {
        self.reads.len()
    }
}
