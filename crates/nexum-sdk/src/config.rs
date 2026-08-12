//! SDK configuration ([`SdkConfig`], ADR-013).
//!
//! Every client-side bound lives here. The protocol version and frame bound
//! must agree with the server's — a mismatch fails the handshake or the
//! encode loudly, never silently.

use crate::error::SdkError;
use crate::protocol::PROTOCOL_VERSION;

/// The validated client-side configuration.
pub struct SdkConfig {
    /// The protocol version the client speaks (must match the server's).
    protocol_version: u16,
    /// The client name announced in the handshake.
    client_name: String,
    /// The maximum frame payload in bytes (encode bound; must not exceed
    /// the server's bound).
    max_frame_payload: u32,
    /// The maximum simultaneously pending reducer calls.
    max_pending_calls: usize,
    /// The maximum commands per input frame.
    max_commands_per_frame: usize,
    /// The bounded server-event queue size (oldest events are dropped).
    max_events: usize,
}

impl Default for SdkConfig {
    fn default() -> Self {
        Self {
            protocol_version: PROTOCOL_VERSION,
            client_name: "nexum-sdk".to_string(),
            max_frame_payload: 64 * 1024,
            max_pending_calls: 64,
            max_commands_per_frame: 128,
            max_events: 1_024,
        }
    }
}

impl SdkConfig {
    /// Creates a default configuration (overrides via the `with_*`
    /// builders).
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the protocol version the client speaks (defaults to the current
    /// `PROTOCOL_VERSION`).
    pub fn with_protocol_version(mut self, version: u16) -> Self {
        self.protocol_version = version;
        self
    }

    /// Sets the client name announced in the handshake.
    pub fn with_client_name(mut self, name: impl Into<String>) -> Self {
        self.client_name = name.into();
        self
    }

    /// Sets the maximum frame payload in bytes (≥ 1).
    pub fn with_max_frame_payload(mut self, bytes: u32) -> Self {
        self.max_frame_payload = bytes;
        self
    }

    /// Sets the maximum simultaneously pending reducer calls (≥ 1).
    pub fn with_max_pending_calls(mut self, n: usize) -> Self {
        self.max_pending_calls = n;
        self
    }

    /// Sets the maximum commands per input frame (≥ 1).
    pub fn with_max_commands_per_frame(mut self, n: usize) -> Self {
        self.max_commands_per_frame = n;
        self
    }

    /// Sets the bounded server-event queue size (≥ 1).
    pub fn with_max_events(mut self, limit: usize) -> Self {
        self.max_events = limit;
        self
    }

    /// Validates every bound. Called by [`Client::new`](crate::Client::new).
    pub fn validate(&self) -> Result<(), SdkError> {
        if self.protocol_version == 0 {
            return Err(SdkError::InvalidArgument(
                "protocol_version must be non-zero".to_string(),
            ));
        }
        if self.client_name.is_empty() {
            return Err(SdkError::InvalidArgument(
                "client_name must not be empty".to_string(),
            ));
        }
        if self.max_frame_payload == 0 {
            return Err(SdkError::InvalidArgument(
                "max_frame_payload must be at least 1".to_string(),
            ));
        }
        if self.max_pending_calls == 0 {
            return Err(SdkError::InvalidArgument(
                "max_pending_calls must be at least 1".to_string(),
            ));
        }
        if self.max_commands_per_frame == 0 {
            return Err(SdkError::InvalidArgument(
                "max_commands_per_frame must be at least 1".to_string(),
            ));
        }
        if self.max_events == 0 {
            return Err(SdkError::InvalidArgument(
                "max_events must be at least 1".to_string(),
            ));
        }
        Ok(())
    }

    /// The protocol version the client speaks.
    pub fn protocol_version(&self) -> u16 {
        self.protocol_version
    }

    /// The client name announced in the handshake.
    pub fn client_name(&self) -> &str {
        &self.client_name
    }

    /// The maximum frame payload in bytes.
    pub fn max_frame_payload(&self) -> u32 {
        self.max_frame_payload
    }

    /// The maximum simultaneously pending reducer calls.
    pub fn max_pending_calls(&self) -> usize {
        self.max_pending_calls
    }

    /// The maximum commands per input frame.
    pub fn max_commands_per_frame(&self) -> usize {
        self.max_commands_per_frame
    }

    /// The bounded server-event queue size.
    pub fn max_events(&self) -> usize {
        self.max_events
    }
}

impl std::fmt::Debug for SdkConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SdkConfig")
            .field("protocol_version", &self.protocol_version)
            .field("client_name", &self.client_name)
            .field("max_frame_payload", &self.max_frame_payload)
            .field("max_pending_calls", &self.max_pending_calls)
            .field("max_commands_per_frame", &self.max_commands_per_frame)
            .field("max_events", &self.max_events)
            .finish()
    }
}
