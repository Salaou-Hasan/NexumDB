//! Transports (ADR-011 D4).
//!
//! The gateway depends only on the [`Connection`] trait: bounded inbound/
//! outbound frame queues and non-blocking poll/flush. Two concrete
//! transports ship:
//!
//! - [`MemoryTransport`] — deterministic in-process links used by tests and
//!   benchmarks (one end is registered with the gateway, the other drives
//!   the client).
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
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};

use crate::error::{NetworkError, ProtocolError};
use crate::protocol::{ServerMessage, parse_frame};

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
pub trait Connection {
    /// A display identity for the peer (for logging and metrics).
    fn peer(&self) -> &str;

    /// Returns the next complete inbound frame, or `None` when none is
    /// buffered, or an error when the transport is broken (the gateway then
    /// closes the connection).
    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError>;

    /// Buffers one outbound frame for delivery. Returns
    /// [`TransportError::Full`] when the peer's queue is at capacity (never
    /// blocks).
    ///
    /// Frames are `Arc<[u8]>` so an immutable, already-encoded payload can
    /// be delivered to many connections by refcount bump instead of a
    /// per-recipient copy (ADR-021 D1). One-off frames convert with a single
    /// `Arc::from` allocation — no copy.
    fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError>;

    /// Attempts to flush buffered outbound bytes to the transport
    /// (non-blocking). A no-op for queue-based transports.
    fn flush_outbound(&mut self) -> Result<(), TransportError>;

    /// Closes the connection (idempotent).
    fn close(&mut self);

    /// Send a [`ServerMessage`] directly, bypassing encode → frame.
    /// Returns `Ok(())` when the message was stored; the caller must NOT
    /// fall back to `try_send_frame`.
    ///
    /// Default: unsupported — returns `Err(TransportError::Closed)` so
    /// callers can fall back to encode+`try_send_frame`.
    fn try_send_direct(
        &mut self,
        _message: &ServerMessage,
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

// ------------------------------------------------------------- memory

/// The shared state of one memory link (both directions, bounded).
///
/// Outbound capacity is shared across frame and direct-message queues so
/// that overflow/stale policy applies uniformly regardless of which path
/// the gateway uses.
struct MemoryLink {
    // --- inbound (client → server) ---
    to_server: VecDeque<Arc<[u8]>>,
    // --- outbound (server → client): frame queue ---
    to_client: VecDeque<Arc<[u8]>>,
    /// Direct message queue — bypass encode/decode for in-process transport.
    to_client_msg: VecDeque<ServerMessage>,
    /// Cap for client → server (the gateway's inbound bound).
    inbound_cap: usize,
    /// Cap for server → client (shared across frame + direct queues).
    outbound_cap: usize,
    closed: bool,
}

/// An in-process connection: `connect` returns a pair; one end is
/// registered with the gateway, the other drives the client. Deterministic
/// FIFO delivery, fully synchronous.
pub struct MemoryConnection {
    peer: String,
    link: Arc<Mutex<MemoryLink>>,
    server_side: bool,
    /// Shared atomic flag: true when `to_server` has pending frames.
    /// Lives outside the Mutex so the gateway can check without locking.
    has_inbound: Arc<AtomicBool>,
    /// Shared atomic flag: true when `to_client` or `to_client_msg` has
    /// pending data. Allows the client pump to skip the Mutex acquisition
    /// entirely when there is nothing to receive.
    has_outbound: Arc<AtomicBool>,
}

impl fmt::Debug for MemoryConnection {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MemoryConnection")
            .field("peer", &self.peer)
            .field("server_side", &self.server_side)
            .finish()
    }
}

/// Creates an in-process connection pair. The first value is the
/// **server-side** end (register it with the gateway); the second is the
/// client side. `inbound_cap` bounds client→server frames, `outbound_cap`
/// bounds server→client frames.
pub struct MemoryTransport;

impl MemoryTransport {
    /// Opens a fresh server/client pair.
    pub fn connect(
        inbound_cap: usize,
        outbound_cap: usize,
    ) -> (MemoryConnection, MemoryConnection) {
        let has_inbound = Arc::new(AtomicBool::new(false));
        let has_outbound = Arc::new(AtomicBool::new(false));
        let link = Arc::new(Mutex::new(MemoryLink {
            to_server: VecDeque::new(),
            to_client: VecDeque::new(),
            to_client_msg: VecDeque::new(),
            inbound_cap,
            outbound_cap,
            closed: false,
        }));
        let server = MemoryConnection {
            peer: "memory:server".to_string(),
            link: Arc::clone(&link),
            server_side: true,
            has_inbound: Arc::clone(&has_inbound),
            has_outbound: Arc::clone(&has_outbound),
        };
        let client = MemoryConnection {
            peer: "memory:client".to_string(),
            link,
            server_side: false,
            has_inbound,
            has_outbound,
        };
        (server, client)
    }
}

impl Connection for MemoryConnection {
    fn peer(&self) -> &str {
        &self.peer
    }

    fn try_recv_frame(&mut self) -> Result<Option<Arc<[u8]>>, TransportError> {
        // Fast path: skip Mutex acquisition when no data is pending.
        if self.server_side && !self.has_inbound.load(Ordering::Relaxed) {
            return Ok(None);
        }
        if !self.server_side && !self.has_outbound.load(Ordering::Relaxed) {
            return Ok(None);
        }
        let mut link = self.link.lock().expect("memory link lock");
        let queue = if self.server_side {
            &mut link.to_server
        } else {
            &mut link.to_client
        };
        let result = queue.pop_front();
        if self.server_side && result.is_none() {
            self.has_inbound.store(false, Ordering::Relaxed);
        }
        if !self.server_side && result.is_none() {
            self.has_outbound.store(false, Ordering::Relaxed);
        }
        Ok(result)
    }

    fn try_send_frame(&mut self, frame: Arc<[u8]>) -> Result<(), TransportError> {
        let mut link = self.link.lock().expect("memory link lock");
        if link.closed {
            return Err(TransportError::Closed);
        }
        // Unified capacity: frame + direct queues share the same cap.
        let total = if self.server_side {
            link.to_client.len() + link.to_client_msg.len()
        } else {
            link.to_server.len()
        };
        let cap = if self.server_side {
            link.outbound_cap
        } else {
            link.inbound_cap
        };
        if total >= cap {
            return Err(TransportError::Full);
        }
        if self.server_side {
            link.to_client.push_back(frame);
            self.has_outbound.store(true, Ordering::Relaxed);
        } else {
            link.to_server.push_back(frame);
            self.has_inbound.store(true, Ordering::Relaxed);
        }
        Ok(())
    }

    fn try_send_direct(
        &mut self,
        message: &ServerMessage,
        _max_payload: u32,
    ) -> Result<(), TransportError> {
        let mut link = self.link.lock().expect("memory link lock");
        if link.closed {
            return Err(TransportError::Closed);
        }
        // Unified capacity: frame + direct queues share the same cap.
        let total = if self.server_side {
            link.to_client.len() + link.to_client_msg.len()
        } else {
            link.to_server.len()
        };
        let cap = if self.server_side {
            link.outbound_cap
        } else {
            link.inbound_cap
        };
        if total >= cap {
            return Err(TransportError::Full);
        }
        if self.server_side {
            link.to_client_msg.push_back(message.clone());
            self.has_outbound.store(true, Ordering::Relaxed);
        } else {
            // Client → server direct messages not used; fall through to frame.
            return Err(TransportError::Closed);
        }
        Ok(())
    }

    fn try_recv_direct(&mut self) -> Result<Option<ServerMessage>, TransportError> {
        let mut link = self.link.lock().expect("memory link lock");
        if self.server_side {
            Ok(None)
        } else {
            Ok(link.to_client_msg.pop_front())
        }
    }

    fn try_recv_any_combined(
        &mut self,
        msg_out: &mut Option<ServerMessage>,
        frame_out: &mut Option<Arc<[u8]>>,
    ) -> Result<bool, TransportError> {
        // Fast path: skip Mutex acquisition when no outbound data is pending.
        if !self.server_side && !self.has_outbound.load(Ordering::Relaxed) {
            return Ok(false);
        }
        let mut link = self.link.lock().expect("memory link lock");
        if self.server_side {
            return Ok(false);
        }
        if let Some(msg) = link.to_client_msg.pop_front() {
            *msg_out = Some(msg);
            // If both queues are now empty, clear the flag.
            if link.to_client_msg.is_empty() && link.to_client.is_empty() {
                self.has_outbound.store(false, Ordering::Relaxed);
            }
            return Ok(true);
        }
        if let Some(frame) = link.to_client.pop_front() {
            *frame_out = Some(frame);
            if link.to_client_msg.is_empty() && link.to_client.is_empty() {
                self.has_outbound.store(false, Ordering::Relaxed);
            }
            return Ok(true);
        }
        // Queues empty but flag was stale — clear it.
        self.has_outbound.store(false, Ordering::Relaxed);
        Ok(false)
    }

    fn flush_outbound(&mut self) -> Result<(), TransportError> {
        Ok(()) // queue-based: nothing to flush
    }

    fn close(&mut self) {
        // Mark the link closed without discarding already-buffered frames:
        // like a TCP FIN, the peer can still read what was queued before the
        // close (e.g. a final `Disconnect` reason), while new sends fail.
        if let Ok(mut link) = self.link.lock() {
            link.closed = true;
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
    /// `max_payload` bounds the frame size enforced at this transport.
    pub fn connect(
        addr: impl ToSocketAddrs,
        outbound_cap: usize,
        max_payload: u32,
    ) -> Result<Self, NetworkError> {
        let stream = TcpStream::connect(addr)
            .map_err(|error| NetworkError::Internal(format!("tcp connect failed: {error}")))?;
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
        // Extract the first complete, valid frame (bounded by the
        // configured `max_payload`; oversized declarations are rejected as
        // soon as the header arrives).
        match parse_frame(&self.read_buf, self.max_payload)? {
            Some((frame, consumed)) => {
                self.read_buf.drain(..consumed);
                Ok(Some(Arc::from(frame)))
            }
            None => Ok(None),
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
            // A closed connection discards queued bytes silently; the
            // gateway already knows.
            self.outbound.clear();
            return Ok(());
        }
        while let Some((frame, offset)) = self.outbound.front_mut() {
            match self.stream.write(&frame[*offset..]) {
                Ok(0) => break, // should not happen on a live socket
                Ok(n) => {
                    *offset += n;
                    if *offset == frame.len() {
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
        self.outbound.clear();
        let _ = self.stream.shutdown(std::net::Shutdown::Both);
    }
}

/// A nonblocking TCP listener producing [`TcpConnection`]s.
pub struct TcpTransport {
    listener: TcpListener,
}

impl TcpTransport {
    /// Binds and listens on `addr`.
    pub fn listen(addr: impl ToSocketAddrs) -> Result<Self, NetworkError> {
        let listener = TcpListener::bind(addr)
            .map_err(|error| NetworkError::Internal(format!("tcp listen failed: {error}")))?;
        listener.set_nonblocking(true).map_err(|error| {
            NetworkError::Internal(format!("tcp set_nonblocking failed: {error}"))
        })?;
        Ok(Self { listener })
    }

    /// Returns the bound local address.
    pub fn local_addr(&self) -> Result<SocketAddr, NetworkError> {
        self.listener
            .local_addr()
            .map_err(|error| NetworkError::Internal(format!("tcp local_addr failed: {error}")))
    }

    /// Accepts a pending connection, or returns `None` when none is
    /// waiting (nonblocking). Callers poll this; the accepted connection
    /// must be registered with the gateway. `max_payload` bounds the frame
    /// size enforced at the accepted transport.
    pub fn accept(
        &self,
        outbound_cap: usize,
        max_payload: u32,
    ) -> Result<Option<TcpConnection>, NetworkError> {
        match self.listener.accept() {
            Ok((stream, _)) => {
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
                    max_payload,
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
