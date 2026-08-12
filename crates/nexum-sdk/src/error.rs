//! The typed SDK error taxonomy ([`SdkError`], ADR-013).
//!
//! Protocol and transport identity is preserved from the network layer
//! rather than flattened into strings, so callers can distinguish a
//! handshake mismatch from a rejected reducer call from a transport
//! failure.

use std::fmt;

use nexum_network::error::{NetworkError, ProtocolError};
use nexum_network::transport::TransportError;

/// A client-side failure.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum SdkError {
    /// The client is not connected (or not connected yet).
    NotConnected,
    /// A connection is already in progress or established.
    AlreadyConnected,
    /// The operation requires an authenticated session.
    AuthenticationRequired,
    /// The operation requires a world attachment.
    NotAttached,
    /// The server's protocol version differs from the client's.
    HandshakeMismatch {
        /// The version the client speaks.
        expected: u16,
        /// The version the server returned.
        received: u16,
    },
    /// Authentication was rejected by the server.
    AuthFailed(String),
    /// A world attachment was rejected by the server.
    AttachFailed(String),
    /// The transport closed while the SDK expected it open.
    TransportClosed,
    /// The bounded outbound queue is full (apply backpressure locally).
    TransportFull,
    /// A transport-level failure.
    Transport(TransportError),
    /// A server message failed to decode (malformed or corrupted).
    Protocol(ProtocolError),
    /// A generic server error with a stable code.
    Server {
        /// The stable server error code.
        code: u16,
        /// The server-provided message.
        message: String,
    },
    /// The pending reducer-call limit was reached.
    PendingCallLimit,
    /// The subscription's server binding is not known yet.
    InFlightSubscription(u64),
    /// No subscription with this local id.
    UnknownSubscription(u64),
    /// No pending request with this id.
    UnknownRequest(u64),
    /// A bounded resource overflowed.
    Capacity(String),
    /// An argument was invalid.
    InvalidArgument(String),
    /// An internal invariant violation (a bug).
    Internal(String),
}

impl SdkError {
    /// Returns the stable server error code, if this error carries one.
    pub fn server_code(&self) -> Option<u16> {
        match self {
            Self::Server { code, .. } => Some(*code),
            _ => None,
        }
    }
}

impl fmt::Display for SdkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotConnected => f.write_str("client is not connected"),
            Self::AlreadyConnected => f.write_str("client is already connected"),
            Self::AuthenticationRequired => f.write_str("authentication required"),
            Self::NotAttached => f.write_str("session is not attached to a world"),
            Self::HandshakeMismatch { expected, received } => write!(
                f,
                "protocol version mismatch: client speaks {expected}, server returned {received}"
            ),
            Self::AuthFailed(message) => write!(f, "authentication failed: {message}"),
            Self::AttachFailed(message) => write!(f, "attach failed: {message}"),
            Self::TransportClosed => f.write_str("transport closed"),
            Self::TransportFull => f.write_str("outbound queue full"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
            Self::Server { code, message } => write!(f, "server error {code}: {message}"),
            Self::PendingCallLimit => f.write_str("pending reducer-call limit reached"),
            Self::InFlightSubscription(local) => {
                write!(f, "subscription {local} is still binding on the server")
            }
            Self::UnknownSubscription(local) => {
                write!(f, "unknown subscription {local}")
            }
            Self::UnknownRequest(id) => write!(f, "unknown request id {id}"),
            Self::Capacity(message) => write!(f, "capacity exceeded: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Internal(message) => write!(f, "internal SDK error: {message}"),
        }
    }
}

impl std::error::Error for SdkError {}

impl From<ProtocolError> for SdkError {
    fn from(error: ProtocolError) -> Self {
        Self::Protocol(error)
    }
}

impl From<NetworkError> for SdkError {
    /// The SDK only observes the network layer through the protocol codec
    /// and the transport trait; every other `NetworkError` is a server-side
    /// invariant violation from the client's perspective.
    fn from(error: NetworkError) -> Self {
        match error {
            NetworkError::Protocol(error) => Self::Protocol(error),
            NetworkError::Capacity(message) => Self::Capacity(message),
            NetworkError::Transport(error) => Self::Transport(error),
            NetworkError::Shutdown => Self::Internal("server gateway shut down".to_string()),
            other => Self::Internal(other.to_string()),
        }
    }
}
