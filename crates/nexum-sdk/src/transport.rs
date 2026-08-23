//! The transport abstraction ([`ClientTransport`], ADR-013 D4).
//!
//! The SDK depends only on the bounded, non-blocking
//! [`Connection`](nexum_network::transport::Connection) trait from the
//! network crate, so transports can be swapped without touching SDK
//! semantics. Two constructors ship:
//!
//! - [`ClientTransport::memory_pair`] — an in-process deterministic link
//!   used by tests and benchmarks (the server end is registered with the
//!   gateway).
//! - [`ClientTransport::tcp_connect`] — the dependency-free nonblocking TCP
//!   transport.
//!
//! No QUIC/WebSocket/TLS transports yet (later phases); the abstraction is
//! ready for them.

use std::net::ToSocketAddrs;
use std::sync::Arc;

use nexum_network::protocol::ServerMessage;
use nexum_network::transport::{
    Connection, MemoryConnection, MemoryTransport, TcpConnection, TransportError,
};

use crate::error::SdkError;

/// Result of a combined direct+frame receive.
pub type RecvAnyResult = (Option<ServerMessage>, Option<Arc<[u8]>>);

/// A client-side transport: a bounded, non-blocking frame queue.
///
/// Both directions are bounded by the underlying transport; a full outbound
/// queue returns [`SdkError::TransportFull`] (never blocks, never grows
/// without limit).
pub struct ClientTransport {
    inner: Box<dyn Connection>,
    closed: bool,
}

impl std::fmt::Debug for ClientTransport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClientTransport")
            .field("peer", &self.peer())
            .field("closed", &self.closed)
            .finish()
    }
}

impl ClientTransport {
    /// Wraps any `Connection` as a client transport.
    pub fn new(inner: Box<dyn Connection>) -> Self {
        Self {
            inner,
            closed: false,
        }
    }

    /// Opens an in-process client/server pair. The first value is the
    /// **client-side** transport (drive the [`Client`](crate::Client) with
    /// it); the second is the server-side end to register with the gateway.
    /// `inbound_cap` bounds client→server frames, `outbound_cap` bounds
    /// server→client frames.
    pub fn memory_pair(inbound_cap: usize, outbound_cap: usize) -> (Self, MemoryConnection) {
        let (server, client) = MemoryTransport::connect(inbound_cap, outbound_cap);
        (Self::new(Box::new(client)), server)
    }

    /// Connects to a TCP endpoint (blocking connect, then nonblocking I/O).
    /// `outbound_cap` bounds the client→server queue; `max_payload` bounds
    /// the frame size enforced at this transport.
    pub fn tcp_connect(
        addr: impl ToSocketAddrs,
        outbound_cap: usize,
        max_payload: u32,
    ) -> Result<Self, SdkError> {
        let connection = TcpConnection::connect(addr, outbound_cap, max_payload)
            .map_err(|error| SdkError::Internal(error.to_string()))?;
        Ok(Self::new(Box::new(connection)))
    }

    /// Returns the peer label.
    pub fn peer(&self) -> &str {
        self.inner.peer()
    }

    /// Returns the next buffered inbound frame, or `None` when none is
    /// ready. `Err(SdkError::TransportClosed)` means the link is gone.
    ///
    /// Frames are `Arc<[u8]>` (ADR-021 D1): the shared broadcast frame is
    /// never copied per client; decoding reads `&frame[..]`.
    pub fn recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, SdkError> {
        if self.closed {
            return Ok(None);
        }
        match self.inner.try_recv_frame() {
            Ok(Some(frame)) => Ok(Some(frame)),
            Ok(None) => Ok(None),
            Err(TransportError::Closed) => {
                self.closed = true;
                Err(SdkError::TransportClosed)
            }
            Err(error) => Err(SdkError::Transport(error)),
        }
    }

    /// Tries to receive a [`ServerMessage`] directly, bypassing frame decode.
    /// Returns `Ok(Some(msg))` when a direct message was available; `Ok(None)`
    /// when there are none (caller should fall back to `recv_frame` + decode).
    pub fn recv_direct(&mut self) -> Result<Option<ServerMessage>, SdkError> {
        if self.closed {
            return Ok(None);
        }
        match self.inner.try_recv_direct() {
            Ok(Some(msg)) => Ok(Some(msg)),
            Ok(None) => Ok(None),
            Err(TransportError::Closed) => {
                self.closed = true;
                Err(SdkError::TransportClosed)
            }
            Err(error) => Err(SdkError::Transport(error)),
        }
    }

    /// Buffers one outbound frame. Returns `SdkError::TransportFull` when
    /// the bounded queue is at capacity.
    ///
    /// For stream transports (TCP) the buffered frame is pushed to the
    /// socket immediately, non-blocking — the SDK's poll-driven host never
    /// needs a separate flush step. Queue transports flush trivially.
    pub fn send_frame(&mut self, frame: Vec<u8>) -> Result<(), SdkError> {
        if self.closed {
            return Err(SdkError::TransportClosed);
        }
        match self.inner.try_send_frame(Arc::from(frame)) {
            Ok(()) => {}
            Err(TransportError::Full) => return Err(SdkError::TransportFull),
            Err(TransportError::Closed) => {
                self.closed = true;
                return Err(SdkError::TransportClosed);
            }
            Err(error) => return Err(SdkError::Transport(error)),
        }
        // Write buffered bytes to the transport now (non-blocking); a
        // failed flush means the link broke, not that the frame is queued.
        self.inner.flush_outbound().map_err(|error| match error {
            TransportError::Closed => {
                self.closed = true;
                SdkError::TransportClosed
            }
            other => SdkError::Transport(other),
        })
    }

    /// Closes the transport (idempotent).
    pub fn close(&mut self) {
        self.closed = true;
        self.inner.close();
    }

    /// Returns `true` once the transport has closed.
    pub fn is_closed(&self) -> bool {
        self.closed
    }

    /// Combined receive: tries direct then frame in a single lock.
    pub fn recv_any(&mut self) -> Result<RecvAnyResult, SdkError> {
        if self.closed {
            return Ok((None, None));
        }
        let mut msg = None;
        let mut frame = None;
        match self.inner.try_recv_any_combined(&mut msg, &mut frame) {
            Ok(true) => Ok((msg, frame)),
            Ok(false) => Ok((None, None)),
            Err(TransportError::Closed) => {
                self.closed = true;
                Err(SdkError::TransportClosed)
            }
            Err(error) => Err(SdkError::Transport(error)),
        }
    }

    /// Consumes the transport, returning the underlying `Connection` (e.g.
    /// to hand it to [`Client::connect`](crate::Client::connect)).
    pub fn into_inner(self) -> Box<dyn Connection> {
        self.inner
    }
}
