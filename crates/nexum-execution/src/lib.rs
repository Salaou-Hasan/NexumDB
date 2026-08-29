#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Nexum execution engine — deterministic partition execution over the
//! authoritative state model (ADR-009, ADR-011, ADR-012).
//!
//! The execution engine is a **higher-level execution layer over the
//! authoritative state model** — not a second database. A [`Partition`] owns
//! the authoritative [`TableStore`], and every tick runs as **one
//! transaction** through the existing OCC engine:
//!
//! ```text
//! Partition::tick(&frame)
//!   ├── frame validation            (pre-tick; consumes nothing on error)
//!   ├── scheduled events due now    (reducer invocations, by (tick, id))
//!   ├── systems in order            (priority asc, SystemId tie-break)
//!   ├── one transaction → OCC validation → atomic commit
//!   └── TickResult { tick, tx_id, changes, events }
//! ```
//!
//! `TickResult.changes` is the exact `Vec<Change>` boundary WAL and
//! subscriptions consume — the caller appends it to the WAL and fans it to
//! the `SubscriptionRegistry` in tick order (ADR-009 D8).
//!
//! Determinism is enforced by construction (ADR-009 D4, D5): systems run in
//! explicit `(priority, id)` order regardless of registration order;
//! scheduled events run by `(at_tick, id)`; input commands run in frame
//! order; the only randomness is [`DeterministicRng`] (splitmix64) seeded
//! per `(world_seed, tick, system)`; and execution is strictly
//! single-threaded. Native and WASM reducers invoked during a tick execute
//! against the **tick's transaction** (additive `invoke_in_tx` hooks), so a
//! whole tick — systems, scheduled events, and reducer writes — commits
//! atomically or aborts completely with zero mutation.
//!
//! - [`Partition`] — one authoritative partition; owns the store, systems,
//!   reducer registries, schedule, and tick counter
//! - [`ExecutionContext`] — the only surface a system sees (reads, writes,
//!   events, reducer invocation, deterministic RNG)
//! - [`SystemDefinition`] / [`SystemRegistry`] — ordered deterministic
//!   systems
//! - [`InputFrame`] / [`InputCommand`] — deterministic, protocol-independent
//!   input
//! - [`PartitionConfig`] — seed, execution mode, and bounded-resource limits
//! - [`PartitionMessage`] — the deterministic cross-partition envelope
//! - [`TickResult`] / [`TickError`] — the committed outcome (changes, events,
//!   outbound messages) and the deterministic failure report
//!
//! **In scope since Phase 11/12:** deterministic parallel tick execution
//! (`ExecutionMode::Parallel`, ADR-011) and the multi-partition message bus
//! (`PartitionMessage` / `Partition::tick_messages`, ADR-012). Still out of
//! scope: networking, sessions, client SDKs, distributed execution across
//! machines, replication, and final performance optimization.

#![allow(unsafe_code)]
#![warn(missing_docs)]

mod calls;
mod config;
mod context;
mod input;
mod parallel;
mod partition;
mod rng;
mod schedule;
mod systems;
mod world;

pub use calls::{ReducerCall, ReducerCallResult};
pub use config::{ExecutionMode, PartitionConfig};
pub use context::ExecutionContext;
pub use input::{InputCommand, InputFrame};
pub use partition::PartitionMessage;
pub use rng::DeterministicRng;
pub use schedule::{Schedule, ScheduledEvent};
pub use systems::{SystemAccess, SystemDefinition, SystemFn, SystemRegistry};
pub use world::{Partition, TickBreakdown, TickError, TickResult, World};

#[cfg(test)]
mod parallel_tests;
#[cfg(test)]
mod partition_tests;
#[cfg(test)]
mod tests;
