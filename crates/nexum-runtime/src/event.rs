//! Runtime-level operational events ([`RuntimeEvent`]).
//!
//! These are **operational** events about the runtime itself — distinct from
//! database `Change`s (authoritative mutations), `ReducerEvent`s
//! (application events), and simulation scheduled events. They are
//! buffered in a bounded log and drained by the application (or, later, the
//! control plane).

use nexum_core::{Error, PartitionId, TickId, WorkerId, WorldId};

/// One operational runtime event (ADR-010, design §13).
//
// Variant payloads are self-documenting (`world`, `tick`, `error`, ...), so
// the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum RuntimeEvent {
    /// A world was created and assigned to a worker.
    WorldCreated { world: WorldId, worker: WorkerId },
    /// A world entered the running state.
    WorldStarted { world: WorldId },
    /// A world was stopped.
    WorldStopped { world: WorldId },
    /// A world failed (tick or persistence failure) and stopped ticking.
    WorldFailed { world: WorldId, reason: Error },
    /// A world was removed from the runtime.
    WorldDestroyed { world: WorldId },
    /// A world was reconstructed from persisted state.
    WorldRecovered { world: WorldId, replayed_txs: usize },
    /// A worker failed; its worlds are recoverable.
    WorkerFailed { worker: WorkerId },
    /// A successful tick of one world, with its measured duration.
    TickCompleted {
        world: WorldId,
        tick: TickId,
        duration_ns: u64,
    },
    /// A tick failed (the world's fate depends on the tick-failure policy).
    TickFailed {
        world: WorldId,
        tick: TickId,
        error: Error,
    },
    /// A WAL append failed; the world's commit exists only in memory.
    PersistenceFailure {
        world: WorldId,
        tick: TickId,
        error: Error,
    },
    /// An input was rejected (queue full, late, wrong state).
    InputRejected { world: WorldId, reason: Error },
    /// A client reducer call was rejected (queue full, wrong state, invalid
    /// name) (ADR-013 D3).
    ReducerCallRejected { world: WorldId, reason: Error },
    /// A partition was bound to a world (ADR-012 D1).
    PartitionRegistered {
        partition: PartitionId,
        world: WorldId,
    },
    /// A partition was unbound from its world.
    PartitionUnregistered { partition: PartitionId },
    /// A cross-partition message was dropped (queue overflow or unknown
    /// destination) — the deterministic backpressure policy (ADR-012 D7).
    MessageDropped {
        from: PartitionId,
        to: PartitionId,
        reason: Error,
    },
    /// The runtime completed shutdown.
    Shutdown,
}
