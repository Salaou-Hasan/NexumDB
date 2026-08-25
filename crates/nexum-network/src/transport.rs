//! Transports (ADR-011 D4).
//!
//! The gateway depends only on the [`Connection`] trait: bounded inbound/
//! outbound frame queues and non-blocking poll/flush.  Two concrete
//! transports ship:
//!
//! - [`MemoryTransport`] — deterministic in-process links used by tests and
//!   benchmarks (one end is registered with the gateway, the other drives
//!   the client).  Uses **lock-free SPSC ring buffers** (zero Mutex
//!   contention on the hot send/receive path).
//! - [`TcpConnection`] / [`TcpTransport`] — a dependency-free **nonblocking**
//!   TCP transport: complete frames are length-delimited and validated with
//!   the protocol's own parser (bounds + checksum) before they enter the
//!   gateway, and outbound writes never block the gateway.
//!
//! No QUIC/UDP/custom transports yet (later phases).

use std::collections::VecDeque;
use std::fmt;
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream, ToSocketAddrs};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use crate::error::{NetworkError, ProtocolError};
use crate::protocol::{ServerMessage, parse_frame};
use crate::spsc::SpscRing;

/// A transport-level failure.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransportError {
    /// The bounded peer queue is at capacity; apply the overflow policy.
    Full,
    /// The connection is closed (or the transport broke).
    Closed,
    /// An I/O failure occurred.
    Io,
}

impl fmt::Display for TransportError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Full => f.write_str("outbound queue full"),
            Self::Closed => f.write_str("transport closed"),
            Self::Io => f.write_str("transport I/O failure"),
        }
    }
}

impl std::error::Error for TransportError {}

impl From<ProtocolError> for TransportError {
    /// A protocol violation detected at the transport boundary (bad magic,
    /// over-limit length, checksum failure) breaks the connection.
    fn from(_: ProtocolError) -> Self {
        Self::Io
    }
}

/// A transport connection: whole-frame queues, non-blocking.
///
/// Implementations bound both directions (`try_send_frame` returns
/// [`TransportError::Full`] at capacity) so a slow peer can never block the
/// gateway.
pub trait Connection: Send + Sync {
    /// A display identity for the peer (for logging and metrics).
    fn peer(&self) -> &str;

    /// Returns the next complete inbound frame, or `None` when none is
    /// buffered, or an error when the transport is broken (the gateway then
    /// closes the connection).
    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError>;

    /// Whether any inbound data is pending on this connection's receiving
    /// side. When `false`, callers can skip `try_recv_frame` entirely —
    /// implementations should make this a single atomic load. Defaults to
    /// `true` so transports without a pending-data flag stay correct.
    fn has_pending_data(&self) -> bool {
        true
    }

    /// Buffers one outbound frame for delivery.  Returns
    /// [`TransportError::Full`] when the peer's queue is at capacity (never
    /// blocks).
    ///
    /// Frames are `Arc<[u8]>` so an immutable, already-encoded payload can
    /// be delivered to many connections by refcount bump instead of a
    /// per-recipient copy (ADR-021 D1).  One-off frames convert with a single
    /// `Arc::from` allocation — no copy.
    fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError>;

    /// Attempts to flush buffered outbound bytes to the transport
    /// (non-blocking).  A no-op for queue-based transports.
    fn flush_outbound(&mut self) -> Result<(), TransportError>;

    /// Closes the connection (idempotent).
    fn close(&mut self);

    /// Send a [`ServerMessage`] directly, bypassing encode → frame.
    /// Takes ownership to avoid a clone inside the queue push.
    /// Returns `Ok(())` when the message was stored; the caller must NOT
    /// fall back to `try_send_frame`.
    ///
    /// Default: unsupported — returns `Err(TransportError::Closed)` so
    /// callers can fall back to encode+`try_send_frame`.
    fn try_send_direct(
        &mut self,
        _message: ServerMessage,
        _max_payload: u32,
    ) -> Result<(), TransportError> {
        Err(TransportError::Closed)
    }

    /// Receive a [`ServerMessage`] directly, bypassing frame → decode.
    /// Returns `Ok(Some(msg))` when a direct message was available; `Ok(None)`
    /// when there are no direct messages (caller should fall back to
    /// `try_recv_frame` + decode).
    fn try_recv_direct(&mut self) -> Result<Option<ServerMessage>, TransportError> {
        Ok(None)
    }

    /// Combined receive: tries direct then frame in one call.
    /// Returns true if a message was received (either direct or frame).
    fn try_recv_any_combined(
        &mut self,
        _msg_out: &mut Option<ServerMessage>,
        _frame_out: &mut Option<Arc<[u8]>>,
    ) -> Result<bool, TransportError> {
        // Default: try direct, then frame separately.
        if let Some(msg) = self.try_recv_direct()? {
            *_msg_out = Some(msg);
            return Ok(true);
        }
        if let Some(frame) = self.try_recv_frame()? {
            *_frame_out = Some(frame);
            return Ok(true);
        }
        Ok(false)
    }
}

// ------------------------------------------------------------- memory (lock-free)

/// An in-process connection backed by **lock-free SPSC ring buffers**.
///
/// `connect` returns a pair; one end is registered with the gateway, the
/// other drives the client.  Deterministic FIFO delivery, zero contention
/// on the hot send/receive path.
///
/// Two separate SPSC rings per direction: one for raw frames
/// (`Arc<[u8]>`) and one for direct messages (`ServerMessage`).
/// This avoids the "skip mismatched item" problem of a single unified ring.
pub struct MemoryConnection {
    peer: String,
    server_side: bool,
    /// Server → Client: frame ring (server pushes, client pops).
    outbound_frames: Arc<SpscRing<Arc<[u8]>>>,
    /// Server → Client: direct-message ring (server pushes, client pops).
    outbound_msgs: Arc<SpscRing<ServerMessage>>,
    /// Client → Server: frame ring (client pushes, server pops).
    inbound: Arc<SpscRing<Arc<[u8]>>>,
    /// Configured cap for server → client (shared across both rings).
    outbound_cap: usize,
    /// Configured cap for client → server.
    inbound_cap: usize,
    /// Shared atomic flag: true when `inbound` has pending frames.
    has_inbound: Arc<AtomicBool>,
    /// Shared atomic flag: true when `outbound_frames` or `outbound_msgs`
    /// has pending data.
    has_outbound: Arc<AtomicBool>,
    /// Closed flag — checked before every push.
    closed: Arc<AtomicBool>,
}

impl fmt::Debug for MemoryConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConnection")
            .field("peer", &self.peer)
            .field("server_side", &self.server_side)
            .finish()
    }
}

/// Creates an in-process connection pair backed by lock-free SPSC rings.
/// The first value is the **server-side** end (register it with the
/// gateway); the second is the client side.
pub struct MemoryTransport;

impl MemoryTransport {
    /// Opens a fresh server/client pair.
    pub fn connect(
        inbound_cap: usize,
        outbound_cap: usize,
    ) -> (MemoryConnection, MemoryConnection) {
        // Ring capacities must be powers of two >= 2.
        let out_ring_cap = outbound_cap.next_power_of_two().max(2);
        let in_ring_cap = inbound_cap.next_power_of_two().max(2);

        let has_inbound = Arc::new(AtomicBool::new(false));
        let has_outbound = Arc::new(AtomicBool::new(false));
        let closed = Arc::new(AtomicBool::new(false));

        let outbound_frames = Arc::new(SpscRing::<Arc<[u8]>>::new(out_ring_cap));
        let outbound_msgs = Arc::new(SpscRing::<ServerMessage>::new(out_ring_cap));
        let inbound = Arc::new(SpscRing::<Arc<[u8]>>::new(in_ring_cap));

        let server = MemoryConnection {
            peer: "memory:server".to_string(),
            server_side: true,
            outbound_frames: Arc::clone(&outbound_frames),
            outbound_msgs: Arc::clone(&outbound_msgs),
            inbound: Arc::clone(&inbound),
            outbound_cap,
            inbound_cap,
            has_inbound: Arc::clone(&has_inbound),
            has_outbound: Arc::clone(&has_outbound),
            closed: Arc::clone(&closed),
        };
        let client = MemoryConnection {
            peer: "memory:client".to_string(),
            server_side: false,
            outbound_frames,
            outbound_msgs,
            inbound,
            outbound_cap,
            inbound_cap,
            has_inbound,
            has_outbound,
            closed,
        };
        (server, client)
    }
}

impl Connection for MemoryConnection {
    fn peer(&self) -> &str {
        &self.peer
    }

    fn has_pending_data(&self) -> bool {
        // Single relaxed atomic load — O(1), no lock, no ring access.
        if self.server_side {
            self.has_inbound.load(Ordering::Relaxed)
        } else {
            self.has_outbound.load(Ordering::Relaxed)
        }
    }

    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
        // Fast path: skip ring access when no data is pending.
        if self.server_side && !self.has_inbound.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if !self.server_side && !self.has_outbound.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if self.server_side {
            // Server reads frames from inbound (client → server).
            match self.inbound.pop() {
                Some(frame) => {
                    if self.inbound.is_empty() {
                        self.has_inbound.store(false, Ordering::Relaxed);
                    }
                    Ok(Some(frame))
                }
                None => {
                    self.has_inbound.store(false, Ordering::Relaxed);
                    Ok(None)
                }
            }
        } else {
            // Client reads frames from outbound_frames (server → client).
            match self.outbound_frames.pop() {
                Some(frame) => {
                    self.maybe_clear_outbound();
                    Ok(Some(frame))
                }
                None => {
                    self.maybe_clear_outbound();
                    Ok(None)
                }
            }
        }
    }

    fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TransportError::Closed);
        }
        if self.server_side {
            // Unified cap across frame + msg rings.
            if self.outbound_frames.len() + self.outbound_msgs.len() >= self.outbound_cap {
                return Err(TransportError::Full);
            }
            self.outbound_frames
                .push(frame)
                .map_err(|_| TransportError::Full)?;
            self.has_outbound.store(true, Ordering::Relaxed);
        } else {
            if self.inbound.len() >= self.inbound_cap {
                return Err(TransportError::Full);
            }
            self.inbound.push(frame).map_err(|_| TransportError::Full)?;
            self.has_inbound.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn try_send_direct(
        &mut self,
        message: ServerMessage,
        _max_payload: u32,
    ) -> Result<(), TransportError> {
        if self.closed.load(Ordering::Relaxed) {
            return Err(TransportError::Closed);
        }
        if self.server_side {
            if self.outbound_frames.len() + self.outbound_msgs.len() >= self.outbound_cap {
                return Err(TransportError::Full);
            }
            self.outbound_msgs
                .push(message)
                .map_err(|_| TransportError::Full)?;
            self.has_outbound.store(true, Ordering::Relaxed);
            Ok(())
        } else {
            // Client → server direct messages not used.
            Err(TransportError::Closed)
        }
    }

    fn try_recv_direct(&mut self) -> Result<Option<ServerMessage>, TransportError> {
        if self.server_side {
            Ok(None)
        } else {
            // Client reads direct messages from outbound_msgs (server → client).
            match self.outbound_msgs.pop() {
                Some(msg) => {
                    self.maybe_clear_outbound();
                    Ok(Some(msg))
                }
                None => {
                    self.maybe_clear_outbound();
                    Ok(None)
                }
            }
        }
    }

    fn try_recv_any_combined(
        &mut self,
        msg_out: &mut Option<ServerMessage>,
        frame_out: &mut Option<Arc<[u8]>>,
    ) -> Result<bool, TransportError> {
        if self.server_side {
            return Ok(false);
        }
        // Fast path: skip ring access when no outbound data is pending.
        if !self.has_outbound.load(Ordering::Relaxed) {
            return Ok(false);
        }
        // Try direct messages first (hot path for subscription deltas).
        if let Some(msg) = self.outbound_msgs.pop() {
            *msg_out = Some(msg);
            self.maybe_clear_outbound();
            return Ok(true);
        }
        // Then try frames.
        if let Some(frame) = self.outbound_frames.pop() {
            *frame_out = Some(frame);
            self.maybe_clear_outbound();
            return Ok(true);
        }
        // Both empty — clear the flag.
        self.has_outbound.store(false, Ordering::Relaxed);
        Ok(false)
    }

    fn flush_outbound(&mut self) -> Result<(), TransportError> {
        Ok(()) // ring-based: nothing to flush
    }

    fn close(&mut self) {
        self.closed.store(true, Ordering::Relaxed);
    }
}

impl MemoryConnection {
    /// Clears the `has_outbound` flag when both outbound rings are empty.
    #[inline]
    fn maybe_clear_outbound(&self) {
        if self.outbound_frames.is_empty() && self.outbound_msgs.is_empty() {
            self.has_outbound.store(false, Ordering::Relaxed);
        }
    }
}

// ----------------------------------------------------------------- TCP

/// A nonblocking TCP connection. Frames are delimited and validated with
/// the protocol parser; outbound bytes are written in `flush_outbound`
/// (called by the gateway), never in the gateway's dispatch path.
///
/// `max_payload` is the configured frame bound enforced at the transport:
/// a header declaring a larger payload is rejected as soon as the header
/// arrives, before the body is buffered (bounded memory per connection).
pub struct TcpConnection {
    stream: TcpStream,
    peer: String,
    read_buf: Vec<u8>,
    outbound: VecDeque<(Arc<[u8]>, usize)>,
    outbound_cap: usize,
    max_payload: u32,
    closed: bool,
}

impl fmt::Debug for TcpConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TcpConnection")
            .field("peer", &self.peer)
            .field("closed", &self.closed)
            .finish()
    }
}

impl TcpConnection {
    /// Connects to `addr` (blocking connect, then nonblocking I/O).
    pub fn connect(
        addr: impl ToSocketAddrs,
        outbound_cap: usize,
        max_payload: u32,
    ) -> Result<Self, NetworkError> {
        let stream = TcpStream::connect(addr)
            .map_err(|error| NetworkError::Internal(format!("tcp connect failed: {error}")))?;
        // Phase 26: disable Nagle. The protocol sends many small frames at
        // tick rate; Nagle would coalesce/delay them for up to ~40 ms on
        // real networks, dominating the latency budget.
        stream
            .set_nodelay(true)
            .map_err(|error| NetworkError::Internal(format!("tcp set_nodelay failed: {error}")))?;
        stream.set_nonblocking(true).map_err(|error| {
            NetworkError::Internal(format!("tcp set_nonblocking failed: {error}"))
        })?;
        let peer = stream
            .peer_addr()
            .map(|addr| addr.to_string())
            .unwrap_or_else(|_| "tcp:unknown".to_string());
        Ok(Self {
            stream,
            peer,
            read_buf: Vec::new(),
            outbound: VecDeque::new(),
            outbound_cap,
            max_payload,
            closed: false,
        })
    }

    /// The local address of the connection.
    pub fn local_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.stream
            .local_addr()
            .map_err(|error| NetworkError::Internal(format!("tcp local_addr failed: {error}")))
    }
}

impl Connection for TcpConnection {
    fn peer(&self) -> &str {
        &self.peer
    }

    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        // Drain whatever the socket has right now.
        let mut scratch = [0u8; 8192];
        loop {
            match self.stream.read(&mut scratch) {
                Ok(0) => {
                    // EOF: only a clean close if no bytes are pending.
                    self.closed = true;
                    return Err(TransportError::Closed);
                }
                Ok(n) => self.read_buf.extend_from_slice(&scratch[..n]),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.closed = true;
                    return Err(TransportError::Io);
                }
            }
        }
        // Try to parse a complete frame from the buffer.
        match parse_frame(&self.read_buf, self.max_payload) {
            Ok(Some((payload, consumed))) => {
                self.read_buf.drain(..consumed);
                Ok(Some(Arc::from(payload)))
            }
            Ok(None) => Ok(None),
            Err(error) => {
                self.closed = true;
                Err(error.into())
            }
        }
    }

    fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        if self.outbound.len() >= self.outbound_cap {
            return Err(TransportError::Full);
        }
        self.outbound.push_back((frame, 0));
        Ok(())
    }

    fn flush_outbound(&mut self) -> Result<(), TransportError> {
        if self.closed {
            return Err(TransportError::Closed);
        }
        while let Some((frame, offset)) = self.outbound.front_mut() {
            let slice = &frame.as_ref()[*offset..];
            match self.stream.write(slice) {
                Ok(n) => {
                    *offset += n;
                    if *offset >= frame.len() {
                        self.outbound.pop_front();
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => {
                    self.closed = true;
                    return Err(TransportError::Io);
                }
            }
        }
        Ok(())
    }

    fn close(&mut self) {
        self.closed = true;
    }
}

// ----------------------------------------------------------------- TCP transport

/// A nonblocking TCP transport: one [`TcpListener`] accepts inbound
/// connections, creating [`TcpConnection`] pairs for the gateway.
pub struct TcpTransport {
    listener: TcpListener,
    #[allow(dead_code)]
    outbound_cap: usize,
    max_payload: u32,
}

impl TcpTransport {
    /// Binds a nonblocking listener on `addr`.  Cap and payload settings
    /// are stored for use when accepting connections.
    pub fn listen(addr: impl ToSocketAddrs) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind(addr)
            .map_err(|error| NetworkError::Internal(format!("tcp bind failed: {error}")))?;
        listener.set_nonblocking(true).map_err(|error| {
            NetworkError::Internal(format!("tcp set_nonblocking failed: {error}"))
        })?;
        Ok(Self {
            listener,
            outbound_cap: 256,
            max_payload: 64 * 1024,
        })
    }

    /// The local address the listener is bound to.
    pub fn local_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.listener
            .local_addr()
            .map_err(|error| NetworkError::Internal(format!("tcp local_addr failed: {error}")))
    }

    /// Accepts a new inbound connection (non-blocking).
    pub fn accept(
        &self,
        _inbound_cap: usize,
        outbound_cap: usize,
    ) -> Result<Option<TcpConnection>, NetworkError> {
        match self.listener.accept() {
            Ok((stream, _addr)) => {
                // Phase 26: NODELAY on server-accepted sockets too — same
                // Nagle rationale as `TcpConnection::connect`.
                stream.set_nodelay(true).map_err(|error| {
                    NetworkError::Internal(format!("tcp set_nodelay failed: {error}"))
                })?;
                stream.set_nonblocking(true).map_err(|error| {
                    NetworkError::Internal(format!("tcp set_nonblocking failed: {error}"))
                })?;
                let peer = stream
                    .peer_addr()
                    .map(|addr| addr.to_string())
                    .unwrap_or_else(|_| "tcp:unknown".to_string());
                Ok(Some(TcpConnection {
                    stream,
                    peer,
                    read_buf: Vec::new(),
                    outbound: VecDeque::new(),
                    outbound_cap,
                    max_payload: self.max_payload,
                    closed: false,
                }))
            }
            Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => Ok(None),
            Err(error) => Err(NetworkError::Internal(format!(
                "tcp accept failed: {error}"
            ))),
        }
    }
}
