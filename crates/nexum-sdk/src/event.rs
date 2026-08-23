//! Typed server events surfaced to the application ([`ServerEvent`],
//! ADR-013).
//!
//! These are **client-facing observations**, not authoritative state: the
//! server owns every transition they describe. The client dispatches
//! inbound messages into events (plus correlated [`ReducerResult`]s and
//! derived [`View`]s) and the application drains them with
//! [`Client::take_events`](crate::Client::take_events).

use nexum_core::{SubscriptionId, TickId, TransactionId, WorldId};
use nexum_network::auth::Principal;
use nexum_reducer::ReducerEvent;
use nexum_storage::Change;

/// One server message, typed for the application.
// Variant payloads are self-documenting (`world`, `tick`, `local`, ...), so
// the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ServerEvent {
    /// The handshake completed; the connection is live.
    Connected { version: u16, server_name: String },
    /// The connection ended (locally, by the server, or by the transport).
    Disconnected { reason: String },
    /// Authentication succeeded.
    Authenticated { principal: Principal },
    /// Authentication was rejected.
    AuthFailed { message: String },
    /// The session attached to a world.
    Attached { world: WorldId },
    /// The world attachment was rejected.
    AttachFailed { message: String },
    /// The session detached from its world.
    Detached,
    /// One committed world tick: its authoritative changes and the events
    /// emitted during it, in `emit` order.
    Tick {
        world: WorldId,
        tick: TickId,
        tx_id: TransactionId,
        changes: Vec<Change>,
        events: Vec<ReducerEvent>,
    },
    /// A generic server error with a stable code.
    Error { code: u16, message: String },
    /// A subscription was established and its initial snapshot applied.
    SubscriptionBound {
        local: u64,
        server: SubscriptionId,
        seq: u64,
    },
    /// A subscription request was rejected by the server.
    SubscriptionRejected { local: u64, message: String },
    /// A resync replaced a subscription's view.
    SubscriptionResynced { local: u64, seq: u64 },
    /// The server marked a subscription stale; its view is invalid until
    /// resync.
    Stale {
        subscription: SubscriptionId,
        seq: u64,
    },
    /// A delta-sequence gap was detected in a subscription's stream; the
    /// handle is stale until resync (silent-loss detection).
    ViewGap { local: u64, expected: u64, got: u64 },
    /// A `Ping` was answered.
    Pong { nonce: u64 },
}

/// The row-level change kind of a subscription delta (re-exported).
pub use crate::protocol::DeltaKind as SubscriptionDeltaKind;
