//! Nexum runtime — **Phases 10, 12** (ADR-010, ADR-012): the single-process
//! coordinator.
//!
//! The runtime owns and orchestrates [`World`]s (each an authoritative
//! partition from Phase 9) through logical [`Worker`]s — **without becoming
//! another state engine**. Worlds own their `TableStore`; `World::tick`
//! remains the only commit path; the runtime coordinates durability (WAL
//! first) and observation (subscriptions second) per successful tick:
//!
//! ```text
//! Runtime → Worker → World → World::tick() → TickResult
//!                                                │
//!                                    Wal::append (durability first)
//!                                                │
//!                                    SubscriptionRegistry.apply_changes
//! ```
//!
//! Deterministic: worlds execute serially in `(worker_id, world_id)` order
//! (`BTreeMap`/`BTreeSet`, never hash iteration), each world gets one tick
//! per [`Runtime::step`], and per-world results never depend on worker
//! count, queue bounds, or OS scheduling. Failure is isolated per
//! world/worker; one WAL and one subscription registry per world keep
//! isolated partitions from colliding on table ids.
//!
//! Since Phase 12, the runtime also owns the **partition registry**
//! (ADR-012): a partition is the message-bus address of a world; the runtime
//! routes deterministic cross-partition messages with a delivery phase
//! strictly before the tick phase (one logical tick of latency), so
//! per-partition traces are worker-count independent.
//!
//! - [`Runtime`] — coordinator: world lifecycle, ownership, input routing,
//!   stepping, persistence/observation coordination, recovery, shutdown
//! - [`RuntimeConfig`] / [`PersistencePolicy`] / [`TickFailurePolicy`] —
//!   validated operational configuration
//! - [`Worker`] / [`WorkerState`] — logical execution owners
//! - [`WorldStatus`] / [`WorldLifecycle`] — world introspection and states
//! - [`RuntimeEvent`] / [`RuntimeMetrics`] — operational events and counters
//! - [`PartitionStatus`] — partition introspection (ADR-012)
//! - [`RuntimeError`] — the runtime-boundary error taxonomy
//!
//! **Still out of scope:** networking, client connections, authentication,
//! matchmaking, distributed clusters, multi-machine workers, cross-partition
//! *transactions*, migration, replication, and consensus. Parallel tick
//! execution lives in `nexum-simulation` (Phase 11), not the runtime.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod error;
mod event;
mod metrics;
mod partition;
mod runtime;
mod worker;
mod world;

pub use config::{PersistencePolicy, RuntimeConfig, TickFailurePolicy, WorldFactory};
pub use error::RuntimeError;
pub use event::RuntimeEvent;
pub use metrics::RuntimeMetrics;
pub use partition::PartitionStatus;
pub use nexum_wal::RecoveryReport;
pub use runtime::{Runtime, RuntimeState, RuntimeStepReport};
pub use worker::{Worker, WorkerState, WorkerStatus};
pub use world::{WorldLifecycle, WorldStatus};

#[cfg(test)]
mod partition_tests;
#[cfg(test)]
mod tests;
