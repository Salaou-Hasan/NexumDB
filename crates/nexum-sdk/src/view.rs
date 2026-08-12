//! The derived client-side subscription view ([`View`], ADR-013).
//!
//! A [`View`] mirrors one server subscription. It is **derived state**: the
//! server's `SubscriptionRegistry` remains authoritative, and the view is
//! always rebuildable from a snapshot. The client detects silent loss via
//! strict delta-sequence continuity: a snapshot establishes the base
//! sequence, and every following delta must be exactly `base + 1`, `+2`, …
//! A violation surfaces as a [`ViewGap`] and the handle is marked stale
//! (the caller must `resync`).

use std::collections::BTreeMap;

use nexum_core::RowId;
use nexum_subscription::DeliveredRow;

use crate::protocol::DeltaKind;

/// A sequence gap in the delta stream (silent-loss detection).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ViewGap {
    /// The next expected sequence.
    pub expected: u64,
    /// The sequence actually received.
    pub got: u64,
}

/// The derived view of one subscription.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct View {
    rows: BTreeMap<RowId, DeliveredRow>,
    seq: u64,
}

impl View {
    /// Creates an empty view (sequence 0).
    pub fn new() -> Self {
        Self {
            rows: BTreeMap::new(),
            seq: 0,
        }
    }

    /// Returns the last applied sequence (0 before any snapshot).
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the number of visible rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` when no rows are visible.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns `true` when `row_id` is visible.
    pub fn contains(&self, row_id: RowId) -> bool {
        self.rows.contains_key(&row_id)
    }

    /// Returns the delivered row for `row_id`, if visible.
    pub fn get(&self, row_id: RowId) -> Option<&DeliveredRow> {
        self.rows.get(&row_id)
    }

    /// Iterates the visible rows in ascending row-id order (the
    /// deterministic order established by the server's snapshot).
    pub fn rows(&self) -> impl Iterator<Item = &DeliveredRow> {
        self.rows.values()
    }

    /// Replaces the whole view with a snapshot (initial establishment or
    /// resync). Always succeeds.
    pub fn apply_snapshot(&mut self, seq: u64, rows: Vec<DeliveredRow>) {
        self.rows = rows.into_iter().map(|row| (row.row_id(), row)).collect();
        self.seq = seq;
    }

    /// Applies one delta. Returns [`ViewGap`] when the commit sequence is
    /// out of the legal window.
    ///
    /// The server's sequence model (ADR-008 D3): every delta carries the
    /// commit sequence of its transaction, the first commit after
    /// establishment sits **at** the observation point (`Initial.seq`), and
    /// several deltas of one transaction share a sequence. A delta is
    /// therefore legal when `cursor ≤ seq ≤ cursor + 1`; a smaller `seq` is
    /// a duplicate/reorder of an already-applied commit and a larger one
    /// means commits were missed. Either way the view is suspect and the
    /// caller must resync.
    pub fn apply_delta(
        &mut self,
        seq: u64,
        kind: DeltaKind,
        row_id: RowId,
        row: Option<DeliveredRow>,
    ) -> Result<(), ViewGap> {
        if seq < self.seq || seq > self.seq + 1 {
            return Err(ViewGap {
                expected: self.seq + 1,
                got: seq,
            });
        }
        match kind {
            DeltaKind::Insert | DeltaKind::Update => {
                if let Some(row) = row {
                    self.rows.insert(row_id, row);
                }
            }
            DeltaKind::Delete => {
                self.rows.remove(&row_id);
            }
        }
        if seq > self.seq {
            self.seq = seq;
        }
        Ok(())
    }
}
