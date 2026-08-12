//! The connection state machine ([`ConnectionState`], [`ConnectionStatus`],
//! ADR-013).
//!
//! ```text
//! Disconnected → Connecting → Connected ⇄ Reconnecting → Closed
//! ```
//!
//! `Closing` is reserved for a graceful-close handshake in later phases;
//! the current protocol closes by transport teardown.

use std::fmt;

/// The lifecycle state of the client's connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// No transport is attached.
    Disconnected,
    /// A transport is attached and the handshake is in flight.
    Connecting,
    /// The handshake completed; the client may authenticate and operate.
    Connected,
    /// The transport is being replaced (reconnect in flight).
    Reconnecting,
    /// A graceful close was requested.
    Closing,
    /// The connection ended (locally or by the server/transport).
    Closed,
}

impl fmt::Display for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Disconnected => f.write_str("disconnected"),
            Self::Connecting => f.write_str("connecting"),
            Self::Connected => f.write_str("connected"),
            Self::Reconnecting => f.write_str("reconnecting"),
            Self::Closing => f.write_str("closing"),
            Self::Closed => f.write_str("closed"),
        }
    }
}

/// A point-in-time snapshot of the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ConnectionStatus {
    /// The current state.
    pub state: ConnectionState,
    /// The transport's peer label, once attached.
    pub peer: Option<String>,
}
