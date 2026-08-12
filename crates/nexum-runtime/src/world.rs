//! World lifecycle and status ([`WorldLifecycle`], [`WorldStatus`]) plus the
//! runtime's internal per-world entry ([`WorldEntry`], ADR-010 D1, D3).
//!
//! The world itself (authoritative state, tick execution) lives in
//! `nexum-simulation`; the runtime entry adds **operational** metadata:
//! ownership, lifecycle, the input queue, the per-world WAL and snapshot
//! directory, the per-world subscription registry, and counters. One
//! WAL and one registry per world keeps isolated partitions (whose
//! `TableId`s collide) from corrupting each other's persistence and
//! observation.

use std::collections::VecDeque;
use std::fmt;
use std::path::PathBuf;

use nexum_core::{TickId, WorkerId, WorldId};
use nexum_simulation::{InputFrame, ReducerCall, World};
use nexum_subscription::SubscriptionRegistry;
use nexum_wal::Wal;

/// The lifecycle state of a world inside the runtime.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorldLifecycle {
    /// Registered and owned by a worker, not yet ticking.
    Created,
    /// Scheduled for ticks; accepts inputs.
    Running,
    /// Stopped; state retained; can restart.
    Stopped,
    /// Failed (tick or persistence failure); must be recreated or
    /// recovered.
    Failed,
}

impl fmt::Display for WorldLifecycle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Created => f.write_str("created"),
            Self::Running => f.write_str("running"),
            Self::Stopped => f.write_str("stopped"),
            Self::Failed => f.write_str("failed"),
        }
    }
}

impl WorldLifecycle {
    /// Returns `true` for states that may be started (Created/Stopped).
    pub(crate) fn can_start(self) -> bool {
        matches!(self, Self::Created | Self::Stopped)
    }

    /// Returns `true` for states that may be stopped (Created/Running).
    pub(crate) fn can_stop(self) -> bool {
        matches!(self, Self::Created | Self::Running)
    }
}

/// A point-in-time view of a world for introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorldStatus {
    /// The world id.
    pub id: WorldId,
    /// The owning worker.
    pub worker: WorkerId,
    /// The lifecycle state.
    pub state: WorldLifecycle,
    /// The next tick the world will execute (logical time).
    pub next_tick: TickId,
    /// Inputs currently queued.
    pub queued_inputs: usize,
    /// Subscriptions currently registered on this world.
    pub subscriptions: usize,
    /// Successful ticks executed since creation/recovery.
    pub ticks_run: u64,
}

/// The runtime's internal per-world entry (operational metadata only).
#[derive(Debug)]
pub(crate) struct WorldEntry {
    pub(crate) world: World,
    pub(crate) worker: WorkerId,
    pub(crate) state: WorldLifecycle,
    pub(crate) inputs: VecDeque<InputFrame>,
    /// Queued client reducer calls (ADR-013 D3), drained into the world's
    /// next tick in FIFO order.
    pub(crate) calls: VecDeque<ReducerCall>,
    pub(crate) wal: Option<Wal>,
    pub(crate) snapshot_dir: Option<PathBuf>,
    pub(crate) subscriptions: SubscriptionRegistry,
    pub(crate) ticks_run: u64,
    pub(crate) ticks_since_snapshot: u64,
}

impl WorldEntry {
    /// Builds an entry for a freshly created (or recovered) world.
    pub(crate) fn new(
        world: World,
        worker: WorkerId,
        wal: Option<Wal>,
        snapshot_dir: Option<PathBuf>,
    ) -> Self {
        Self {
            world,
            worker,
            state: WorldLifecycle::Created,
            inputs: VecDeque::new(),
            calls: VecDeque::new(),
            wal,
            snapshot_dir,
            subscriptions: SubscriptionRegistry::new(),
            ticks_run: 0,
            ticks_since_snapshot: 0,
        }
    }

    /// Snapshot the world's status.
    pub(crate) fn status(&self) -> WorldStatus {
        WorldStatus {
            id: self.world.id(),
            worker: self.worker,
            state: self.state,
            next_tick: self.world.tick_number(),
            queued_inputs: self.inputs.len(),
            subscriptions: self.subscriptions.len(),
            ticks_run: self.ticks_run,
        }
    }
}
