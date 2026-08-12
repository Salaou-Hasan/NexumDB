//! Network configuration ([`NetworkConfig`], [`OutboundOverflowPolicy`],
//! [`NetworkEvent`], ADR-011 D3–D5).
//!
//! Every client-controlled size is bounded here — frame payloads, per-frame
//! command counts, per-session subscriptions, connection and queue counts —
//! so hostile input can never grow memory without limit. Bounds are
//! validated at [`NetworkGateway::new`](crate::NetworkGateway::new).

use nexum_core::{ConnectionId, Error, Result, WorldId};

/// What the gateway does when a connection's bounded outbound queue is
/// full (ADR-011 D5). Simulation, WAL, and other clients are never blocked
/// either way.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutboundOverflowPolicy {
    /// Mark the session stale: drop `TickUpdate`/subscription deltas, send
    /// one `StaleNotification`; the client must `Resync` (or reattach).
    Stale,
    /// Close the connection with an explicit reason.
    Disconnect,
}

/// A bounded operational event (the gateway's log, like the runtime's).
// Variant payloads are self-documenting (`connection`, `world`, `reason`,
// ...), so the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum NetworkEvent {
    /// A transport connection was registered.
    ConnectionOpened { connection: ConnectionId },
    /// A connection was closed (locally or by the transport).
    ConnectionClosed { connection: ConnectionId, reason: String },
    /// A session authenticated successfully.
    Authenticated { connection: ConnectionId, principal_id: u64 },
    /// Authentication was rejected.
    AuthFailed { connection: ConnectionId },
    /// A session attached to a world.
    Attached {
        connection: ConnectionId,
        world: WorldId,
    },
    /// A session detached from a world.
    Detached { connection: ConnectionId },
    /// A session fell behind and was marked stale (outbound overflow).
    SessionStale { connection: ConnectionId },
    /// A protocol violation was detected.
    ProtocolError { connection: ConnectionId },
    /// A connection was dropped for exceeding an inbound or policy bound.
    ClientDropped { connection: ConnectionId, reason: String },
}

/// The validated network configuration.
pub struct NetworkConfig {
    pub(crate) max_frame_payload: u32,
    pub(crate) max_queued_inbound_frames: usize,
    pub(crate) max_queued_outbound_frames: usize,
    pub(crate) max_connections: usize,
    pub(crate) max_subscriptions_per_session: usize,
    pub(crate) max_commands_per_frame: usize,
    pub(crate) max_reducer_name_len: usize,
    pub(crate) max_reducer_args: usize,
    pub(crate) max_pending_calls_per_connection: usize,
    pub(crate) overflow_policy: OutboundOverflowPolicy,
    pub(crate) event_log_limit: usize,
}

impl Default for NetworkConfig {
    fn default() -> Self {
        Self {
            max_frame_payload: 64 * 1024,
            max_queued_inbound_frames: 256,
            max_queued_outbound_frames: 1_024,
            max_connections: 10_000,
            max_subscriptions_per_session: 64,
            max_commands_per_frame: 128,
            max_reducer_name_len: 256,
            max_reducer_args: 128,
            max_pending_calls_per_connection: 64,
            overflow_policy: OutboundOverflowPolicy::Stale,
            event_log_limit: 1_024,
        }
    }
}

impl NetworkConfig {
    /// Creates a default configuration (all bounds sensible; overrides via
    /// the `with_*` builders).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the maximum frame payload in bytes (≥ 1). The frame overhead is
    /// fixed, so the total frame size is `payload + 15`.
    pub fn with_max_frame_payload(mut self, bytes: u32) -> Self {
        self.max_frame_payload = bytes;
        self
    }

    /// Sets the per-connection inbound queue bound (≥ 1).
    pub fn with_max_queued_inbound_frames(mut self, n: usize) -> Self {
        self.max_queued_inbound_frames = n;
        self
    }

    /// Sets the per-connection outbound queue bound (≥ 1).
    pub fn with_max_queued_outbound_frames(mut self, n: usize) -> Self {
        self.max_queued_outbound_frames = n;
        self
    }

    /// Sets the maximum simultaneous connections (≥ 1).
    pub fn with_max_connections(mut self, n: usize) -> Self {
        self.max_connections = n;
        self
    }

    /// Sets the maximum subscriptions per session (≥ 1).
    pub fn with_max_subscriptions_per_session(mut self, n: usize) -> Self {
        self.max_subscriptions_per_session = n;
        self
    }

    /// Sets the maximum commands per input frame (≥ 1).
    pub fn with_max_commands_per_frame(mut self, n: usize) -> Self {
        self.max_commands_per_frame = n;
        self
    }

    /// Sets the maximum reducer-call name length in bytes (≥ 1, ADR-013 D3).
    pub fn with_max_reducer_name_len(mut self, n: usize) -> Self {
        self.max_reducer_name_len = n;
        self
    }

    /// Sets the maximum reducer-call argument count (≥ 1, ADR-013 D3).
    pub fn with_max_reducer_args(mut self, n: usize) -> Self {
        self.max_reducer_args = n;
        self
    }

    /// Sets the maximum simultaneously pending reducer calls per connection
    /// (≥ 1, ADR-013 D3).
    pub fn with_max_pending_calls_per_connection(mut self, n: usize) -> Self {
        self.max_pending_calls_per_connection = n;
        self
    }

    /// Sets the outbound overflow policy.
    pub fn with_overflow_policy(mut self, policy: OutboundOverflowPolicy) -> Self {
        self.overflow_policy = policy;
        self
    }

    /// Sets the bounded event log size (≥ 1).
    pub fn with_event_log_limit(mut self, limit: usize) -> Self {
        self.event_log_limit = limit;
        self
    }

    /// Validates every bound. Called by `NetworkGateway::new`.
    pub fn validate(&self) -> Result<()> {
        if self.max_frame_payload == 0 {
            return Err(Error::invalid_argument("max_frame_payload must be at least 1"));
        }
        if self.max_queued_inbound_frames == 0 {
            return Err(Error::invalid_argument(
                "max_queued_inbound_frames must be at least 1",
            ));
        }
        if self.max_queued_outbound_frames == 0 {
            return Err(Error::invalid_argument(
                "max_queued_outbound_frames must be at least 1",
            ));
        }
        if self.max_connections == 0 {
            return Err(Error::invalid_argument("max_connections must be at least 1"));
        }
        if self.max_subscriptions_per_session == 0 {
            return Err(Error::invalid_argument(
                "max_subscriptions_per_session must be at least 1",
            ));
        }
        if self.max_commands_per_frame == 0 {
            return Err(Error::invalid_argument(
                "max_commands_per_frame must be at least 1",
            ));
        }
        if self.max_reducer_name_len == 0 {
            return Err(Error::invalid_argument(
                "max_reducer_name_len must be at least 1",
            ));
        }
        if self.max_reducer_args == 0 {
            return Err(Error::invalid_argument(
                "max_reducer_args must be at least 1",
            ));
        }
        if self.max_pending_calls_per_connection == 0 {
            return Err(Error::invalid_argument(
                "max_pending_calls_per_connection must be at least 1",
            ));
        }
        if self.event_log_limit == 0 {
            return Err(Error::invalid_argument("event_log_limit must be at least 1"));
        }
        Ok(())
    }

    /// The maximum frame payload in bytes.
    pub fn max_frame_payload(&self) -> u32 {
        self.max_frame_payload
    }

    /// The per-connection inbound queue bound.
    pub fn max_queued_inbound_frames(&self) -> usize {
        self.max_queued_inbound_frames
    }

    /// The per-connection outbound queue bound.
    pub fn max_queued_outbound_frames(&self) -> usize {
        self.max_queued_outbound_frames
    }

    /// The maximum simultaneous connections.
    pub fn max_connections(&self) -> usize {
        self.max_connections
    }

    /// The maximum subscriptions per session.
    pub fn max_subscriptions_per_session(&self) -> usize {
        self.max_subscriptions_per_session
    }

    /// The maximum commands per input frame.
    pub fn max_commands_per_frame(&self) -> usize {
        self.max_commands_per_frame
    }

    /// The maximum reducer-call name length in bytes.
    pub fn max_reducer_name_len(&self) -> usize {
        self.max_reducer_name_len
    }

    /// The maximum reducer-call argument count.
    pub fn max_reducer_args(&self) -> usize {
        self.max_reducer_args
    }

    /// The maximum simultaneously pending reducer calls per connection.
    pub fn max_pending_calls_per_connection(&self) -> usize {
        self.max_pending_calls_per_connection
    }

    /// The outbound overflow policy.
    pub fn overflow_policy(&self) -> OutboundOverflowPolicy {
        self.overflow_policy
    }

    /// The bounded event log size.
    pub fn event_log_limit(&self) -> usize {
        self.event_log_limit
    }
}

impl std::fmt::Debug for NetworkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkConfig")
            .field("max_frame_payload", &self.max_frame_payload)
            .field("max_queued_inbound_frames", &self.max_queued_inbound_frames)
            .field("max_queued_outbound_frames", &self.max_queued_outbound_frames)
            .field("max_connections", &self.max_connections)
            .field("max_subscriptions_per_session", &self.max_subscriptions_per_session)
            .field("max_commands_per_frame", &self.max_commands_per_frame)
            .field("max_reducer_name_len", &self.max_reducer_name_len)
            .field("max_reducer_args", &self.max_reducer_args)
            .field(
                "max_pending_calls_per_connection",
                &self.max_pending_calls_per_connection,
            )
            .field("overflow_policy", &self.overflow_policy)
            .field("event_log_limit", &self.event_log_limit)
            .finish()
    }
}
