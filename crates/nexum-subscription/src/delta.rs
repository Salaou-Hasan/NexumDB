//! The delivered output of the subscription engine.
//!
//! A consumer (the future network layer, Phase 11) drains
//! [`SubscriptionUpdate`]s from a subscription's bounded delivery buffer.
//! Every update carries the commit-sequence position of the transition that
//! produced it, so a consumer can order and acknowledge deliveries without
//! inventing its own ordering (ADR-008 D3, D8).

use std::sync::Arc;

use nexum_core::{Row, RowId};

/// A row delivered to a consumer: identity plus the (possibly projected) row.
///
/// Wrapped in `Arc` so that `SubscriptionUpdate` variants sharing the same
/// logical row (e.g. across view-group members in `push_commit`) avoid deep
/// cloning — only the refcount is bumped. The inner `Row` is immutable once
/// constructed, so sharing is safe.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DeliveredRow {
    row_id: RowId,
    row: Row,
}

impl DeliveredRow {
    /// Creates a delivered row.
    pub fn new(row_id: RowId, row: Row) -> Self {
        Self { row_id, row }
    }

    /// Wraps an existing delivered row in `Arc` for cheap sharing.
    pub fn into_shared(self) -> Arc<Self> {
        Arc::new(self)
    }

    /// Returns the row's identity.
    pub fn row_id(&self) -> RowId {
        self.row_id
    }

    /// Returns the delivered (possibly projected) row.
    pub fn row(&self) -> &Row {
        &self.row
    }
}

/// One observable event on a subscription's stream.
///
/// Variants mirror the conceptual subscription result of the Phase 8 brief
/// (§21) and are serializable-friendly: they carry only ids, rows, and the
/// commit sequence — no references into Nexum internals.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SubscriptionUpdate {
    /// The full current view at establishment, in query order. Everything
    /// committed before this `seq` is included; the live stream resumes at
    /// this `seq`.
    Initial {
        /// Observation point in the commit sequence.
        seq: u64,
        /// The rows visible at establishment, in query order.
        rows: Vec<DeliveredRow>,
    },
    /// A row entered the view (insert or update crossing the predicate).
    Insert {
        /// Commit sequence of the transition.
        seq: u64,
        /// The new visible row.
        row: Arc<DeliveredRow>,
    },
    /// A visible row changed but remains visible (new state only).
    Update {
        /// Commit sequence of the transition.
        seq: u64,
        /// The row's new state.
        row: Arc<DeliveredRow>,
    },
    /// A visible row left the view (delete, predicate leave, or window
    /// eviction).
    Delete {
        /// Commit sequence of the transition.
        seq: u64,
        /// The departed row's identity.
        row_id: RowId,
    },
    /// The subscription fell behind; its view is invalid. Drain this and
    /// call `resync` to regenerate the exact authoritative view.
    Stale {
        /// Commit sequence at which the subscription was marked stale.
        seq: u64,
    },
    /// A full replacement view after `resync`, in query order.
    Resync {
        /// Observation point of the new view in the commit sequence.
        seq: u64,
        /// The rows visible at the resync point, in query order.
        rows: Vec<DeliveredRow>,
    },
}
