//! Nexum game server layer (Phase 14, ADR-014).
//!
//! The Game Server is the orchestration / product layer that composes the
//! authoritative Phases 1–13 stack — tables, transactions, WAL, reducers,
//! WASM, subscriptions, simulation, runtime, partitions, networking, and the
//! client SDK — into a multiplayer game server API.
//!
//! It holds **game metadata only**: game instances, players, reducer
//! exposure, and lifecycle. Authoritative gameplay state lives inside
//! `Partition → World → TableStore` and mutates only through `World::tick`
//! via the runtime. The Game Server never becomes another authoritative state
//! system, another transaction engine, another storage engine, or another
//! simulation engine.
//!
//! Architecture:
//!
//! ```text
//! Client → SDK → Gateway (owned by GameServer) → Runtime → World::tick
//!                                    │
//! GameServer: games, players, exposure, policy, lifecycle events
//! ```
//!
//! The `GameServer` owns a `NetworkGateway`, which owns the `Runtime`. The
//! gateway's authorization policy is the server's live [`GamePolicyTable`]
//! (exposure + active-player membership), so client attach / input / reducer
//! operations are denied *before* any authoritative execution.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod config;
pub mod error;
pub mod events;
pub mod lifecycle;
pub mod metrics;
pub mod policy;
pub mod server;

pub use config::{GameInstanceConfig, GameServerConfig};
pub use error::GameServerError;
pub use events::GameServerEvent;
pub use lifecycle::{GameLifecycle, GameStatus, JoinOutcome, PartitionState, PlayerState, PlayerStatus};
pub use metrics::GameServerMetrics;
pub use policy::{GamePolicyTable, PolicyHandle, ReducerExposure, ReducerPolicy, Role};
pub use server::{GameRecoveryReport, GameServer};

#[cfg(test)]
mod tests;
