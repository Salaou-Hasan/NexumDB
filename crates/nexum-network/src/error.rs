//! The network error taxonomy ([`NetworkError`], [`ProtocolError`],
//! [`AuthError`], [`TransportError`], ADR-011 D3).
//!
//! Protocol errors carry a stable numeric code for the wire `Error` message.
//! Lower-level identity is preserved (`RuntimeError`, core `Error`) rather
//! than flattened into strings.

use std::fmt;

use nexum_core::{ConnectionId, Error};
use nexum_runtime::RuntimeError;

use crate::transport::TransportError;

/// A failure while decoding or validating a protocol frame.
// Variant payloads are self-documenting (`len`, `max`, `kind`, ...), so the
// enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ProtocolError {
    /// The frame did not start with the `NEXN` magic.
    BadMagic,
    /// The client negotiated an unsupported protocol version.
    UnsupportedVersion(u16),
    /// The declared payload length exceeds the configured bound.
    Oversized { len: u64, max: u64 },
    /// The frame ended before its declared length (truncated).
    Truncated,
    /// The frame checksum did not match.
    BadChecksum,
    /// The frame carried an unknown message kind.
    UnknownKind(u8),
    /// The payload did not decode (malformed message body).
    Malformed(String),
}

impl ProtocolError {
    /// A stable numeric code for the wire `Error` message.
    pub const fn code(&self) -> u16 {
        match self {
            Self::BadMagic => 1,
            Self::UnsupportedVersion(_) => 2,
            Self::Oversized { .. } => 3,
            Self::Truncated => 4,
            Self::BadChecksum => 5,
            Self::UnknownKind(_) => 6,
            Self::Malformed(_) => 7,
        }
    }
}

impl fmt::Display for ProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::BadMagic => f.write_str("bad frame magic"),
            Self::UnsupportedVersion(version) => {
                write!(f, "unsupported protocol version {version}")
            }
            Self::Oversized { len, max } => {
                write!(f, "frame payload of {len} bytes exceeds the maximum of {max}")
            }
            Self::Truncated => f.write_str("truncated frame"),
            Self::BadChecksum => f.write_str("frame checksum mismatch"),
            Self::UnknownKind(kind) => write!(f, "unknown message kind {kind}"),
            Self::Malformed(message) => write!(f, "malformed message: {message}"),
        }
    }
}

/// A failure while authenticating a connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum AuthError {
    /// The presented credentials did not map to a principal.
    InvalidCredentials,
    /// The authenticator could not complete the request.
    Internal(String),
}

impl fmt::Display for AuthError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidCredentials => f.write_str("invalid credentials"),
            Self::Internal(message) => write!(f, "authenticator failure: {message}"),
        }
    }
}

/// A failure at the network boundary.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NetworkError {
    /// A referenced connection does not exist.
    UnknownConnection(ConnectionId),
    /// The connection limit was reached.
    ConnectionLimit,
    /// A protocol (frame/message) violation.
    Protocol(ProtocolError),
    /// An authentication failure.
    Auth(AuthError),
    /// A session-state violation (e.g. operation requires authentication or
    /// attachment).
    Session(String),
    /// A runtime operation failed (identity preserved).
    Runtime(RuntimeError),
    /// A core operation failed (identity preserved).
    Core(Error),
    /// A bounded resource (queue, subscription count) overflowed.
    Capacity(String),
    /// The transport reported a failure.
    Transport(TransportError),
    /// The gateway is not running (shutdown).
    Shutdown,
    /// An internal invariant violation (a bug).
    Internal(String),
}

impl NetworkError {
    /// Returns the underlying core `Error`, if this failure carries one.
    pub fn core_error(&self) -> Option<&Error> {
        match self {
            Self::Runtime(error) => error.core_error(),
            Self::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownConnection(connection) => {
                write!(f, "connection {connection} does not exist")
            }
            Self::ConnectionLimit => f.write_str("connection limit reached"),
            Self::Protocol(error) => write!(f, "protocol error: {error}"),
            Self::Auth(error) => write!(f, "authentication failed: {error}"),
            Self::Session(message) => write!(f, "session error: {message}"),
            Self::Runtime(error) => write!(f, "runtime error: {error}"),
            Self::Core(error) => write!(f, "core error: {error}"),
            Self::Capacity(message) => write!(f, "capacity exceeded: {message}"),
            Self::Transport(error) => write!(f, "transport error: {error}"),
            Self::Shutdown => f.write_str("gateway is shut down"),
            Self::Internal(message) => write!(f, "internal network error: {message}"),
        }
    }
}

impl std::error::Error for NetworkError {}

impl From<RuntimeError> for NetworkError {
    fn from(error: RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<Error> for NetworkError {
    fn from(error: Error) -> Self {
        Self::Core(error)
    }
}
