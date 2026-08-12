//! The runtime error taxonomy ([`RuntimeError`], ADR-010 D6).
//!
//! A thin enum over the runtime boundary that categorizes failures without
//! rewrapping lower-level identity: the `Error` payloads (core `Conflict`,
//! `InvalidArgument`, `Internal`, ...) and `TickError` are preserved, never
//! flattened into a generic string.

use std::fmt;

use nexum_core::{Error, PartitionId, WorkerId, WorldId};

use crate::worker::WorkerState;
use crate::world::WorldLifecycle;

/// A failure at the runtime boundary.
//
// Variant payloads are self-documenting (`world`, `worker`, `error`, ...),
// so the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RuntimeError {
    /// The runtime configuration was invalid.
    InvalidConfig(String),
    /// A referenced world does not exist.
    UnknownWorld(WorldId),
    /// A referenced worker does not exist.
    UnknownWorker(WorkerId),
    /// A world with this id already exists.
    DuplicateWorld(WorldId),
    /// A referenced partition does not exist.
    UnknownPartition(PartitionId),
    /// A partition with this id already exists.
    DuplicatePartition(PartitionId),
    /// An ownership operation was invalid (e.g. duplicate owner, unknown
    /// worker).
    OwnershipConflict(WorldId, WorkerId),
    /// An operation required a different world lifecycle state.
    InvalidWorldState {
        world: WorldId,
        operation: &'static str,
        state: WorldLifecycle,
    },
    /// An operation required a different worker state.
    InvalidWorkerState {
        worker: WorkerId,
        operation: &'static str,
        state: WorkerState,
    },
    /// An input was rejected (unknown/late/over-limit). The reason is the
    /// underlying `Error` identity.
    InputRejected { world: WorldId, reason: Error },
    /// A client reducer call was rejected (queue full, wrong state, invalid
    /// name) (ADR-013 D3). The reason is the underlying `Error` identity.
    ReducerCallRejected { world: WorldId, reason: Error },
    /// A persistence (WAL/snapshot) failure. The `Error` identity is
    /// preserved.
    Persistence(Error),
    /// A lower-level core error surfaced through a runtime operation
    /// (subscription subscribe/drain/resync, ...). Identity preserved.
    Core(Error),
    /// A world's tick failed. `error` is the underlying `TickError`'s
    /// `Error`, identity preserved.
    Tick { world: WorldId, error: Error },
    /// A worker failed; its worlds are recoverable.
    WorkerFailed(WorkerId),
    /// The runtime is shutting down (or already stopped).
    Shutdown,
    /// An internal invariant violation (a bug).
    Internal(String),
}

impl RuntimeError {
    /// Builds an invalid-world-state error.
    pub fn world_state(
        world: WorldId,
        operation: &'static str,
        state: WorldLifecycle,
    ) -> Self {
        Self::InvalidWorldState {
            world,
            operation,
            state,
        }
    }

    /// Builds an invalid-worker-state error.
    pub fn worker_state(
        worker: WorkerId,
        operation: &'static str,
        state: WorkerState,
    ) -> Self {
        Self::InvalidWorkerState {
            worker,
            operation,
            state,
        }
    }

    /// Returns the underlying core `Error` if this failure carries one.
    pub fn core_error(&self) -> Option<&Error> {
        match self {
            Self::InputRejected { reason, .. }
            | Self::ReducerCallRejected { reason, .. }
            | Self::Persistence(reason)
            | Self::Core(reason)
            | Self::Tick { error: reason, .. } => Some(reason),
            _ => None,
        }
    }
}

impl fmt::Display for RuntimeError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(message) => write!(f, "invalid runtime configuration: {message}"),
            Self::UnknownWorld(world) => write!(f, "world {world} does not exist"),
            Self::UnknownWorker(worker) => write!(f, "worker {worker} does not exist"),
            Self::DuplicateWorld(world) => write!(f, "world {world} already exists"),
            Self::UnknownPartition(partition) => write!(f, "partition {partition} does not exist"),
            Self::DuplicatePartition(partition) => {
                write!(f, "partition {partition} already exists")
            }
            Self::OwnershipConflict(world, worker) => {
                write!(f, "ownership conflict for world {world} on worker {worker}")
            }
            Self::InvalidWorldState { world, operation, state } => {
                write!(f, "cannot {operation} world {world}: current state is {state}")
            }
            Self::InvalidWorkerState { worker, operation, state } => {
                write!(f, "cannot {operation} worker {worker}: current state is {state}")
            }
            Self::InputRejected { world, reason } => {
                write!(f, "input rejected for world {world}: {reason}")
            }
            Self::ReducerCallRejected { world, reason } => {
                write!(f, "reducer call rejected for world {world}: {reason}")
            }
            Self::Persistence(error) => write!(f, "persistence failure: {error}"),
            Self::Core(error) => write!(f, "runtime core operation failed: {error}"),
            Self::Tick { world, error } => write!(f, "tick of world {world} failed: {error}"),
            Self::WorkerFailed(worker) => write!(f, "worker {worker} has failed"),
            Self::Shutdown => write!(f, "runtime is shutting down or stopped"),
            Self::Internal(message) => write!(f, "internal runtime error: {message}"),
        }
    }
}

impl std::error::Error for RuntimeError {}
