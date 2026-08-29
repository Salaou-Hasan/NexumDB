//! Lightweight transaction snapshot for fast reducer execution.
//!
//! Instead of the expensive `branch → invoke → absorb` cycle (which costs
//! ~100 µs per call due to WriteSet merge), we snapshot the parent
//! transaction's state before each reducer call and restore on failure.
//! On success, the reducer's writes are already in the parent — no absorb
//! needed.
//!
//! The snapshot records a **read-set watermark** (the entry count at snapshot
//! time) instead of cloning the full read-set BTreeMap. On rollback, entries
//! added after the watermark are discarded in O(delta) time. This eliminates
//! the O(N²) quadratic blowup from cloning a growing read set on every call.

use std::collections::BTreeMap;

use nexum_core::TableId;

use crate::write_set::WriteSet;

/// A lightweight snapshot of transaction mutable state for rollback.
///
/// Contains only the fields that a reducer can mutate:
/// - `writes`: COW write set (Arc clone — O(1))
/// - `read_watermark`: entry count at snapshot time (O(1) to record)
/// - `provisional`: per-table provisional-id counters (small BTreeMap clone)
pub struct TxSnapshot {
    pub(crate) writes: WriteSet,
    pub(crate) read_watermark: usize,
    pub(crate) provisional: BTreeMap<TableId, u64>,
}

impl TxSnapshot {
    /// Returns the read-set watermark (entry count at snapshot time).
    pub fn read_watermark(&self) -> usize {
        self.read_watermark
    }
}
