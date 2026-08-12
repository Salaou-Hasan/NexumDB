//! Nexum client SDK (Phase 13, ADR-013): the poll-driven client adapter
//! around the canonical realtime protocol.
//!
//! The SDK is a **client adapter**, never a second authoritative state
//! engine. It connects to a
//! [`NetworkGateway`](nexum_network::NetworkGateway), speaks the versioned
//! binary protocol (`nexum_network::protocol`), routes inputs and reducer
//! calls into the runtime through the gateway, and maintains only
//! **derived** client-side views of server subscriptions. It never exposes
//! tables, transactions, OCC, the WAL, or worker/partition internals.
//!
//! ```text
//! Client ──▶ ClientTransport ──▶ protocol frames ──▶ NetworkGateway ──▶ Runtime
//!                                                                         │ World::tick
//!                                                                         ▼
//!                                                                   Vec<Change>
//! ```
//!
//! The main entry point is [`Client`]. The driver loop is **poll-based**:
//! the host services the transport (the gateway flushes responses back),
//! then calls [`Client::pump`] to dispatch server messages into typed
//! [`ServerEvent`]s, correlated [`ReducerResult`]s, and derived [`View`]s.
//!
//! - [`Client`] — lifecycle, session, inputs, reducer calls, subscriptions
//! - [`SdkConfig`] — every client-side bound
//! - [`ClientTransport`] — the transport abstraction (memory + TCP)
//! - [`View`] — the derived, per-subscription client state
//! - [`ServerEvent`] — typed server messages surfaced to the application
//! - [`SdkError`] — typed SDK/protocol/transport errors
//!
//! **Out of scope in this phase:** TLS, HTTP/gRPC control bindings, and
//! QUIC/custom transports — the transport abstraction is ready for them.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod connection;
pub mod error;
pub mod event;
pub mod input;
pub mod protocol;
pub mod reconnect;
pub mod reducer;
pub mod request;
pub mod session;
pub mod subscription;
pub mod transport;
pub mod view;

pub use client::{Client, PumpReport};
pub use config::SdkConfig;
pub use connection::{ConnectionState, ConnectionStatus};
pub use error::SdkError;
pub use event::ServerEvent;
pub use protocol::PROTOCOL_VERSION;
pub use reconnect::ReconnectPolicy;
pub use request::{PendingCall, ReducerResult};
pub use session::SessionInfo;
pub use subscription::SubscriptionHandle;
pub use view::{View, ViewGap};

#[cfg(test)]
mod tests;
