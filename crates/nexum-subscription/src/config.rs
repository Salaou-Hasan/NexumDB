//! Resource bounds for the subscription engine (ADR-008).
//!
//! Every bound keeps the query model and delivery buffers explicit and
//! bounded — a malicious or mistaken query cannot force unbounded memory
//! allocation or unbounded buffering.

use nexum_core::{Error, Result};

/// Bounds enforced by the subscription engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SubscriptionConfig {
    /// Maximum number of buffered [`SubscriptionUpdate`](crate::SubscriptionUpdate)s
    /// per subscription before it is marked stale (backpressure, ADR-008 D7).
    pub max_buffered: usize,
    /// The safety cap on a subscription's view: the window never grows past
    /// this many rows, and initial snapshots / resyncs deliver at most this
    /// many rows. A query's own `limit` is a smaller cap when set.
    pub max_snapshot_rows: usize,
    /// Maximum number of predicates in one query.
    pub max_predicates: usize,
    /// Maximum number of columns in one projection.
    pub max_projection_columns: usize,
}

impl Default for SubscriptionConfig {
    fn default() -> Self {
        Self {
            max_buffered: 1024,
            max_snapshot_rows: 10_000,
            max_predicates: 16,
            max_projection_columns: 64,
        }
    }
}

impl SubscriptionConfig {
    /// Validates the configuration: every bound must be non-zero.
    pub fn validate(&self) -> Result<()> {
        if self.max_buffered == 0 {
            return Err(Error::invalid_argument(
                "max_buffered must be greater than zero",
            ));
        }
        if self.max_snapshot_rows == 0 {
            return Err(Error::invalid_argument(
                "max_snapshot_rows must be greater than zero",
            ));
        }
        if self.max_predicates == 0 {
            return Err(Error::invalid_argument(
                "max_predicates must be greater than zero",
            ));
        }
        if self.max_projection_columns == 0 {
            return Err(Error::invalid_argument(
                "max_projection_columns must be greater than zero",
            ));
        }
        Ok(())
    }
}
