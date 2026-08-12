//! The runtime's partition registry (ADR-012 D1, D3).
//!
//! A partition is the message-bus address of an existing authoritative
//! [`World`]. The registry owns **only** routing metadata: the world a
//! partition addresses, its owning worker, and its bounded inbound message
//! queue. It owns no state — the world's `TableStore` remains the sole
//! authoritative state, and `World::tick_messages` remains the only commit
//! path.

use std::collections::VecDeque;

use nexum_core::{PartitionId, WorkerId, WorldId};
use nexum_simulation::PartitionMessage;

/// The runtime's internal per-partition entry (operational metadata only).
#[derive(Debug)]
pub(crate) struct PartitionEntry {
    /// The world this partition addresses.
    pub(crate) world: WorldId,
    /// The worker owning the world.
    pub(crate) worker: WorkerId,
    /// The bounded inbound queue of undelivered messages, in arrival order.
    pub(crate) inbound: VecDeque<PartitionMessage>,
}

impl PartitionEntry {
    /// Builds an entry for a partition bound to `world` on `worker`.
    pub(crate) fn new(world: WorldId, worker: WorkerId) -> Self {
        Self {
            world,
            worker,
            inbound: VecDeque::new(),
        }
    }
}

/// A point-in-time view of a partition for introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionStatus {
    /// The partition id.
    pub partition: PartitionId,
    /// The world this partition addresses.
    pub world: WorldId,
    /// The worker owning the world.
    pub worker: WorkerId,
    /// Messages currently queued, undelivered.
    pub queued_messages: usize,
}
