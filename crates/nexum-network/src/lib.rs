//! Nexum networking — **Phase 11** (ADR-011): the realtime protocol and
//! control-plane **adapter** around the runtime.
//!
//! The network layer is never another state engine: it authenticates
//! connections, establishes sessions and world attachments, routes client
//! commands as `InputFrame`s through `Runtime::submit_input`, attaches
//! clients to the existing per-world `SubscriptionRegistry`, and serializes
//! committed `TickResult`/`SubscriptionUpdate` data to clients. It owns no
//! tables, transactions, OCC, WAL, reducers, simulation systems, or
//! authoritative subscription state.
//!
//! ```text
//! Client ──versioned binary protocol──▶ NetworkGateway ──▶ Runtime
//!                                                           │ World::tick
//!                                                           ▼
//!                                                     Vec<Change>
//!                                                WAL  ◄──┴──► SubscriptionRegistry
//!                                                └──▶ network fanout
//! ```
//!
//! - [`NetworkGateway`] — connections, sessions, routing, fanout,
//!   backpressure
//! - [`NetworkConfig`] / [`OutboundOverflowPolicy`] — every bound
//! - [`protocol`] — versioned, bounded, checksummed frames and messages
//! - [`auth`] — the `Authenticator` interface and `Principal`
//! - [`session`] — connection/session/attachment model
//! - [`transport`] — the `Connection` trait + memory and nonblocking TCP
//!   transports
//! - [`control`] — the typed operator control plane over the runtime
//! - [`NetworkError`] / [`ProtocolError`] / [`AuthError`] / `TransportError`
//! - [`NetworkMetrics`] / [`NetworkEvent`]
//!
//! **Out of scope in this phase:** distributed worlds, clustering, world
//! migration, replication, sharding, matchmaking, presence, consensus,
//! gateway clustering, HTTP/gRPC control binding, and QUIC/custom
//! transports.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod auth;
pub mod config;
pub mod control;
pub mod error;
pub mod gateway;
pub mod metrics;
pub mod policy;
pub mod protocol;
pub mod rate;
pub mod session;
pub mod transport;

pub use auth::{Authenticator, Principal, TokenAuthenticator};
pub use config::{NetworkConfig, NetworkEvent, OutboundOverflowPolicy};
pub use rate::RateLimitConfig;
pub use control::{ControlPlane, HealthReport};
pub use error::{AuthError, NetworkError, ProtocolError};
pub use gateway::{NetworkGateway, ProcessReport, StepReport, CALLER_SOURCE_ARG, SERVER_REQUEST_MSB};
pub use policy::{AllowAllPolicy, GamePolicy};
pub use metrics::NetworkMetrics;
pub use protocol::{DeltaKind, PROTOCOL_VERSION};
pub use session::Session;
pub use transport::{Connection, MemoryConnection, MemoryTransport, TcpConnection, TcpTransport};

#[cfg(test)]
mod tests;
