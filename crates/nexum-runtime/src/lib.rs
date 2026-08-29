//! Nexum runtime — **Phases 10, 12** (ADR-010, ADR-012): the single-process
//! coordinator.
//!
//! The runtime owns and orchestrates
//! [`nexum_execution::Partition`]s (each an authoritative partition from
//! Phase 9) through logical [`Worker`]s — **without becoming another state
//! engine**. Partitions own their `TableStore`; `Partition::tick`
//! remains the only commit path; the runtime coordinates durability (WAL
//! first) and observation (subscriptions second) per successful tick:
//!
//! ```text
//! Runtime → Worker → Partition → Partition::tick() → TickResult
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
//! - [`PartitionStatus`] / [`PartitionLifecycle`] — world introspection and states
//! - [`RuntimeEvent`] / [`RuntimeMetrics`] — operational events and counters
//! - [`PartitionStatus`] — partition introspection (ADR-012)
//! - [`RuntimeError`] — the runtime-boundary error taxonomy
//!
//! **Still out of scope:** networking, client connections, authentication,
//! matchmaking, distributed clusters, multi-machine workers, cross-partition
//! *transactions*, migration, replication, and consensus. Parallel tick
//! execution lives in `nexum-execution` (Phase 11), not the runtime.

#![allow(unsafe_code)]
#![warn(missing_docs)]

mod config;
mod error;
mod event;
mod metrics;
mod partition;
mod runtime;
mod worker;
mod world;

pub use config::{PartitionFactory, PersistencePolicy, RuntimeConfig, TickFailurePolicy};
pub use error::RuntimeError;
pub use event::RuntimeEvent;
pub use metrics::RuntimeMetrics;
pub use nexum_wal::RecoveryReport;
pub use partition::RoutingStatus;
pub use runtime::{Runtime, RuntimeState, RuntimeStepReport};
pub use worker::{Worker, WorkerState, WorkerStatus};
pub use world::{PartitionLifecycle, PartitionStatus};

#[cfg(test)]
mod partition_tests;
#[cfg(test)]
mod tests;
