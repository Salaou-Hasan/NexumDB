//! The runtime's message-bus partition registry (ADR-012 D1, D3).
//!
//! A partition (see [`nexum_core::PartitionId`]) is the message-bus address
//! of an existing authoritative [`nexum_execution::Partition`]. The registry
//! owns **only** routing metadata: the partition a world is addressed under,
//! its owning worker, and its bounded inbound message queue. It owns no
//! state — the world's `TableStore` remains the sole authoritative state,
//! and `Partition::tick_messages` remains the only commit path.

use std::collections::VecDeque;

use nexum_core::{PartitionId, WorkerId, WorldId};
use nexum_execution::PartitionMessage;

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

/// A point-in-time view of a message-bus routing entry for introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RoutingStatus {
    /// The partition (message-bus address) id.
    pub partition: PartitionId,
    /// The world this partition addresses.
    pub world: WorldId,
    /// The worker owning the world.
    pub worker: WorkerId,
    /// Messages currently queued, undelivered.
    pub queued_messages: usize,
}
