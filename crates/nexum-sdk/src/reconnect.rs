//! Reconnection (ADR-013): the reconnect policy and the
//! disconnect/reconnect API.
//!
//! The SDK is poll-driven, so reconnection is **caller-driven**: the host
//! calls [`Client::reconnect`] with a fresh transport (a handshake is
//! issued), then — once `Connected` — replays
//! [`authenticate`](Client::authenticate),
//! [`attach`](Client::attach), and [`subscribe`](Client::subscribe). Pending
//! reducer calls fail with a correlated result (never a hang); derived
//! subscription state is discarded and rebuilt from fresh snapshots.
//!
//! Recovered server history is **never** replayed as live updates: after
//! recovery the world resumes from its recovered tick, the client reattaches
//! and resubscribes, and only future commits arrive as deltas.

use nexum_network::transport::Connection;

use crate::client::Client;
use crate::connection::ConnectionState;
use crate::error::SdkError;
use crate::protocol::ClientMessage;

/// The caller's reconnect policy (bounds for the host's retry loop; the SDK
/// itself never blocks or retries).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ReconnectPolicy {
    /// The maximum number of reconnect attempts before giving up.
    max_attempts: usize,
}

impl Default for ReconnectPolicy {
    fn default() -> Self {
        Self { max_attempts: 3 }
    }
}

impl ReconnectPolicy {
    /// Creates a policy with `max_attempts` (≥ 1).
    pub fn new(max_attempts: usize) -> Self {
        Self {
            max_attempts: max_attempts.max(1),
        }
    }

    /// The maximum number of reconnect attempts.
    pub fn max_attempts(&self) -> usize {
        self.max_attempts
    }
}

impl Client {
    /// Closes the connection (transport teardown; the server notices on its
    /// next poll). Pending reducer calls fail with a correlated result.
    pub fn disconnect(&mut self) {
        self.transition_to_closed("client disconnect");
    }

    /// Replaces the transport and reissues the handshake (state →
    /// `Reconnecting`). The session, subscriptions, and derived views are
    /// discarded; after the handshake completes the caller replays
    /// authenticate/attach/subscribe. Returns [`SdkError::AlreadyConnected`]
    /// while a connection is already live.
    pub fn reconnect(&mut self, transport: Box<dyn Connection>) -> Result<(), SdkError> {
        if self.state == ConnectionState::Connected {
            return Err(SdkError::AlreadyConnected);
        }
        self.config.validate()?;
        self.transport = Some(crate::transport::ClientTransport::new(transport));
        self.state = ConnectionState::Reconnecting;
        self.session = None;
        self.fail_pending_calls("connection reconnecting");
        self.pending_subscribes.clear();
        self.subscriptions.clear();
        self.views.clear();
        self.send_message(&ClientMessage::Handshake {
            version: self.config.protocol_version(),
            name: self.config.client_name().to_string(),
        })
    }
}
