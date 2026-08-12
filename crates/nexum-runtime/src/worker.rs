//! Workers ([`Worker`], [`WorkerState`], [`WorkerStatus`], ADR-010 D2).
//!
//! A worker is the **execution owner** of a deterministic set of worlds. In
//! Phase 10 workers are logical ownership containers, not OS threads: the
//! runtime executes worlds serially in deterministic `(worker_id,
//! world_id)` order, so worker count never affects a world's results. A
//! failed worker orphans its worlds (they become recoverable), and the
//! ownership mapping is changeable — the seed of future partition
//! migration.

use std::collections::BTreeSet;
use std::fmt;

use nexum_core::WorkerId;
use nexum_core::WorldId;

/// The lifecycle state of a worker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum WorkerState {
    /// Accepting and executing worlds.
    Running,
    /// Failed; its worlds are recoverable (and marked failed).
    Failed,
    /// Stopped by runtime shutdown.
    Stopped,
}

impl fmt::Display for WorkerState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("running"),
            Self::Failed => f.write_str("failed"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

/// A logical execution owner of a set of worlds.
#[derive(Debug)]
pub struct Worker {
    id: WorkerId,
    state: WorkerState,
    worlds: BTreeSet<WorldId>,
}

impl Worker {
    /// Creates a running worker owning no worlds.
    pub(crate) fn new(id: WorkerId) -> Self {
        Self {
            id,
            state: WorkerState::Running,
            worlds: BTreeSet::new(),
        }
    }

    /// Returns the worker id.
    pub fn id(&self) -> WorkerId {
        self.id
    }

    /// Returns the lifecycle state.
    pub fn state(&self) -> WorkerState {
        self.state
    }

    /// Iterates over owned world ids in ascending order (deterministic).
    pub fn worlds(&self) -> impl Iterator<Item = WorldId> + '_ {
        self.worlds.iter().copied()
    }

    /// Adds a world to this worker's ownership set.
    pub(crate) fn add_world(&mut self, world: WorldId) {
        self.worlds.insert(world);
    }

    /// Removes a world from this worker's ownership set.
    pub(crate) fn remove_world(&mut self, world: WorldId) {
        self.worlds.remove(&world);
    }

    /// Sets the lifecycle state.
    pub(crate) fn set_state(&mut self, state: WorkerState) {
        self.state = state;
    }
}

/// A point-in-time view of a worker for introspection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WorkerStatus {
    /// The worker id.
    pub id: WorkerId,
    /// The lifecycle state.
    pub state: WorkerState,
    /// Owned world ids, ascending.
    pub worlds: Vec<WorldId>,
}
