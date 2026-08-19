//! The versioned binary realtime protocol (ADR-011 D3).
//!
//! Deterministic little-endian encoding over the shared `nexum_core::binary`
//! codec. Every frame is bounded, versioned, and checksummed:
//!
//! ```text
//! magic "NEXN" (4) | version u16 | kind u8 | payload_len u32 | payload | crc32
//! ```
//!
//! `payload_len` is validated against the configured maximum **before** any
//! allocation; checksums and checked decoding reject corruption and
//! malformed input as typed [`ProtocolError`]s — never panics, never OOM.
//!
//! The same frame parser drives the TCP transport's length-delimited reader,
//! so framing bounds are enforced at the transport too.

use nexum_core::binary::{Crc32, get_bool, get_row, get_str, get_u64, get_value, put_bool, put_row, put_str, put_u64, put_value};
use nexum_core::{Error, Row, RowId, SubscriptionId, TickId, TransactionId, Value, Version, WorldId};
use nexum_reducer::{ReducerArgs, ReducerEvent};
use nexum_simulation::{InputCommand, InputFrame};
use nexum_storage::Change;
use nexum_subscription::{ComparisonOp, DeliveredRow, OrderDirection, Query};

use crate::auth::Principal;
use crate::error::NetworkError;
use crate::error::ProtocolError;

/// The four-byte frame magic.
pub const PROTOCOL_MAGIC: &[u8; 4] = b"NEXN";
/// The current protocol version.
///
/// v2 (ADR-013): `Subscribe` carries a client `request_id` echoed in the
/// `SubscriptionSnapshot` and in rejection `Error`s, so subscription
/// correlation is never ambiguous; `Error` carries an optional `request_id`.
pub const PROTOCOL_VERSION: u16 = 2;
/// Header size: magic (4) + version (2) + kind (1) + length (4).
pub const HEADER_LEN: usize = 11;
/// Total fixed frame overhead: header (11) + checksum (4).
pub const FRAME_OVERHEAD: usize = 15;

// Message kinds — client → server (0x01..).
const KIND_HANDSHAKE: u8 = 0x01;
const KIND_AUTHENTICATE: u8 = 0x02;
const KIND_ATTACH: u8 = 0x03;
const KIND_INPUT_FRAME: u8 = 0x04;
const KIND_SUBSCRIBE: u8 = 0x05;
const KIND_UNSUBSCRIBE: u8 = 0x06;
const KIND_RESYNC: u8 = 0x07;
const KIND_PING: u8 = 0x08;
const KIND_DETACH: u8 = 0x09;
const KIND_CALL_REDUCER: u8 = 0x0A;
// Server → client (0x81..).
const KIND_HANDSHAKE_RESPONSE: u8 = 0x81;
const KIND_AUTH_RESULT: u8 = 0x82;
const KIND_ATTACH_RESULT: u8 = 0x83;
const KIND_TICK_UPDATE: u8 = 0x84;
const KIND_SUB_SNAPSHOT: u8 = 0x85;
const KIND_SUB_DELTA: u8 = 0x86;
const KIND_STALE: u8 = 0x87;
const KIND_ERROR: u8 = 0x88;
const KIND_PONG: u8 = 0x89;
const KIND_DISCONNECT: u8 = 0x8A;
const KIND_DETACH_RESULT: u8 = 0x8B;
const KIND_REDUCER_RESULT: u8 = 0x8C;
const KIND_SUB_DELTA_BATCH: u8 = 0x8D;

/// A client → server message.
// Variant payloads are self-documenting (`version`, `world`, `nonce`, ...),
// so the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ClientMessage {
    /// Protocol version negotiation (must match `PROTOCOL_VERSION`).
    Handshake { version: u16, name: String },
    /// Present opaque credentials to the `Authenticator`.
    Authenticate { credentials: String },
    /// Attach the session to a world.
    AttachWorld { world: WorldId },
    /// Submit one tick's input frame (command sources are stamped
    /// server-side with the principal id).
    InputFrame { frame: InputFrame },
    /// Establish a subscription with a logical query. `request_id` is
    /// echoed in the `SubscriptionSnapshot` (and in the rejection `Error`)
    /// so the client can correlate the result (ADR-013).
    Subscribe { request_id: u64, query: Query },
    /// End a subscription.
    Unsubscribe { subscription: SubscriptionId },
    /// Regenerate a subscription's exact view.
    Resync { subscription: SubscriptionId },
    /// Liveness probe; answered with `Pong { nonce }`.
    Ping { nonce: u64 },
    /// End the session's world attachment and its subscriptions.
    DetachWorld,
    /// Invoke a registered reducer on the attached world's next tick
    /// (ADR-013 D3). Correlated by `request_id`.
    CallReducer {
        request_id: u64,
        reducer: String,
        args: ReducerArgs,
    },
}

/// A server → client message.
// Variant payloads are self-documenting (`world`, `tick`, `seq`, ...), so
// the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum ServerMessage {
    /// Server version + name.
    HandshakeResponse { version: u16, server_name: String },
    /// Outcome of `Authenticate`.
    AuthResult {
        ok: bool,
        principal: Option<Principal>,
        error: Option<String>,
    },
    /// Outcome of `AttachWorld`.
    AttachResult {
        ok: bool,
        world: Option<WorldId>,
        error: Option<String>,
    },
    /// Outcome of `DetachWorld`.
    DetachResult {
        ok: bool,
        error: Option<String>,
    },
    /// One committed world tick's authoritative changes and the events
    /// emitted during it (ADR-013 D5), in `emit` order.
    TickUpdate {
        world: WorldId,
        tick: TickId,
        tx_id: TransactionId,
        changes: Vec<Change>,
        events: Vec<ReducerEvent>,
    },
    /// A subscription's full view (initial establishment or resync).
    /// `request_id` echoes the originating `Subscribe` (0 for resyncs).
    SubscriptionSnapshot {
        request_id: u64,
        subscription: SubscriptionId,
        seq: u64,
        rows: Vec<DeliveredRow>,
    },
    /// One subscription delta (row entered / changed / left the view).
    SubscriptionDelta {
        subscription: SubscriptionId,
        seq: u64,
        kind: DeltaKind,
        row_id: RowId,
        row: Option<DeliveredRow>,
    },
    /// A subscription fell behind; its view is invalid until resync.
    StaleNotification { subscription: SubscriptionId, seq: u64 },
    /// A protocol error with a stable code. `request_id` correlates the
    /// error to a request when known (a rejected `Subscribe`; 0 otherwise).
    Error {
        code: u16,
        message: String,
        request_id: u64,
    },
    /// Reply to `Ping`.
    Pong { nonce: u64 },
    /// The server is closing the connection.
    Disconnect { reason: String },
    /// The outcome of a `CallReducer` (ADR-013 D3), correlated by
    /// `request_id`. Carries the reducer's return value on success or a
    /// stable-code error on failure.
    ReducerResult {
        request_id: u64,
        ok: bool,
        value: Option<Value>,
        error: Option<String>,
    },
    /// A batch of subscription deltas for the same subscription in one
    /// frame — reduces per-delta encode/decode/queue overhead from O(N)
    /// to O(1) per pump_subscription call.
    SubscriptionDeltaBatch {
        subscription: SubscriptionId,
        request_id: u64,
        deltas: Vec<SubscriptionDeltaEntry>,
    },
}

/// A single delta entry inside a [`SubscriptionDeltaBatch`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionDeltaEntry {
    /// Commit sequence of the transition.
    pub seq: u64,
    /// Row-level change kind.
    pub kind: DeltaKind,
    /// Identity of the affected row.
    pub row_id: RowId,
    /// The row data (absent for deletes).
    pub row: Option<DeliveredRow>,
}

/// The row-level change kind of a subscription delta.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DeltaKind {
    /// The row entered the view.
    Insert,
    /// The row changed but remains visible.
    Update,
    /// The row left the view.
    Delete,
}

// ---------------------------------------------------------------- encoding

/// Encodes a client message into one bounded frame.
pub fn encode_client(message: &ClientMessage, max_payload: u32) -> Result<Vec<u8>, NetworkError> {
    let (kind, payload) = match message {
        ClientMessage::Handshake { version, name } => {
            let mut payload = Vec::new();
            put_u16(&mut payload, *version);
            put_str(&mut payload, name);
            (KIND_HANDSHAKE, payload)
        }
        ClientMessage::Authenticate { credentials } => {
            let mut payload = Vec::new();
            put_str(&mut payload, credentials);
            (KIND_AUTHENTICATE, payload)
        }
        ClientMessage::AttachWorld { world } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, world.as_u64());
            (KIND_ATTACH, payload)
        }
        ClientMessage::InputFrame { frame } => {
            let mut payload = Vec::new();
            encode_frame_payload(&mut payload, frame);
            (KIND_INPUT_FRAME, payload)
        }
        ClientMessage::Subscribe { request_id, query } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *request_id);
            encode_query(&mut payload, query);
            (KIND_SUBSCRIBE, payload)
        }
        ClientMessage::Unsubscribe { subscription } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, subscription.as_u64());
            (KIND_UNSUBSCRIBE, payload)
        }
        ClientMessage::Resync { subscription } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, subscription.as_u64());
            (KIND_RESYNC, payload)
        }
        ClientMessage::Ping { nonce } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *nonce);
            (KIND_PING, payload)
        }
        ClientMessage::DetachWorld => (KIND_DETACH, Vec::new()),
        ClientMessage::CallReducer {
            request_id,
            reducer,
            args,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *request_id);
            put_str(&mut payload, reducer);
            put_reducer_args(&mut payload, args);
            (KIND_CALL_REDUCER, payload)
        }
    };
    build_frame(kind, &payload, max_payload)
}

/// Encodes a server message into one bounded frame.
pub fn encode_server(message: &ServerMessage, max_payload: u32) -> Result<Vec<u8>, NetworkError> {
    let (kind, payload) = match message {
        ServerMessage::HandshakeResponse { version, server_name } => {
            let mut payload = Vec::new();
            put_u16(&mut payload, *version);
            put_str(&mut payload, server_name);
            (KIND_HANDSHAKE_RESPONSE, payload)
        }
        ServerMessage::AuthResult {
            ok,
            principal,
            error,
        } => {
            let mut payload = Vec::new();
            put_bool(&mut payload, *ok);
            if *ok {
                let principal = principal.as_ref().expect("ok auth carries a principal");
                put_u64(&mut payload, principal.id());
                put_str(&mut payload, principal.name());
            } else {
                put_str(&mut payload, error.as_deref().unwrap_or("authentication failed"));
            }
            (KIND_AUTH_RESULT, payload)
        }
        ServerMessage::AttachResult { ok, world, error } => {
            let mut payload = Vec::new();
            put_bool(&mut payload, *ok);
            if *ok {
                put_u64(&mut payload, world.expect("ok attach carries a world").as_u64());
            } else {
                put_str(&mut payload, error.as_deref().unwrap_or("attach failed"));
            }
            (KIND_ATTACH_RESULT, payload)
        }
        ServerMessage::DetachResult { ok, error } => {
            let mut payload = Vec::new();
            put_bool(&mut payload, *ok);
            if !*ok {
                put_str(&mut payload, error.as_deref().unwrap_or("detach failed"));
            }
            (KIND_DETACH_RESULT, payload)
        }
        ServerMessage::TickUpdate {
            world,
            tick,
            tx_id,
            changes,
            events,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, world.as_u64());
            put_u64(&mut payload, tick.as_u64());
            put_u64(&mut payload, tx_id.as_u64());
            put_u64(&mut payload, changes.len() as u64);
            for change in changes {
                encode_change(&mut payload, change);
            }
            put_u64(&mut payload, events.len() as u64);
            for event in events {
                put_str(&mut payload, event.name());
                put_value(&mut payload, event.payload());
            }
            (KIND_TICK_UPDATE, payload)
        }
        ServerMessage::SubscriptionSnapshot {
            request_id,
            subscription,
            seq,
            rows,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *request_id);
            put_u64(&mut payload, subscription.as_u64());
            put_u64(&mut payload, *seq);
            put_u64(&mut payload, rows.len() as u64);
            for row in rows {
                put_u64(&mut payload, row.row_id().as_u64());
                put_row(&mut payload, row.row());
            }
            (KIND_SUB_SNAPSHOT, payload)
        }
        ServerMessage::SubscriptionDelta {
            subscription,
            seq,
            kind,
            row_id,
            row,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, subscription.as_u64());
            put_u64(&mut payload, *seq);
            payload.push(delta_kind_tag(*kind));
            put_u64(&mut payload, row_id.as_u64());
            match kind {
                DeltaKind::Delete => {}
                _ => {
                    let row = row.as_ref().expect("insert/update deltas carry a row");
                    put_row(&mut payload, row.row());
                }
            }
            (KIND_SUB_DELTA, payload)
        }
        ServerMessage::StaleNotification { subscription, seq } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, subscription.as_u64());
            put_u64(&mut payload, *seq);
            (KIND_STALE, payload)
        }
        ServerMessage::Error {
            code,
            message,
            request_id,
        } => {
            let mut payload = Vec::new();
            put_u16(&mut payload, *code);
            put_str(&mut payload, message);
            put_u64(&mut payload, *request_id);
            (KIND_ERROR, payload)
        }
        ServerMessage::Pong { nonce } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *nonce);
            (KIND_PONG, payload)
        }
        ServerMessage::Disconnect { reason } => {
            let mut payload = Vec::new();
            put_str(&mut payload, reason);
            (KIND_DISCONNECT, payload)
        }
        ServerMessage::ReducerResult {
            request_id,
            ok,
            value,
            error,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, *request_id);
            put_bool(&mut payload, *ok);
            if *ok {
                put_value(
                    &mut payload,
                    value.as_ref().expect("ok reducer result carries a value"),
                );
            } else {
                put_str(
                    &mut payload,
                    error.as_deref().unwrap_or("reducer call failed"),
                );
            }
            (KIND_REDUCER_RESULT, payload)
        }
        ServerMessage::SubscriptionDeltaBatch {
            subscription,
            request_id,
            deltas,
        } => {
            let mut payload = Vec::new();
            put_u64(&mut payload, subscription.as_u64());
            put_u64(&mut payload, *request_id);
            put_u64(&mut payload, deltas.len() as u64);
            for d in deltas {
                put_u64(&mut payload, d.seq);
                payload.push(delta_kind_tag(d.kind));
                put_u64(&mut payload, d.row_id.as_u64());
                match d.kind {
                    DeltaKind::Delete => {}
                    _ => {
                        let row = d.row.as_ref().expect("insert/update deltas carry a row");
                        put_row(&mut payload, row.row());
                    }
                }
            }
            (KIND_SUB_DELTA_BATCH, payload)
        }
    };
    build_frame(kind, &payload, max_payload)
}

/// Builds one checksummed, length-prefixed frame; rejects an over-limit
/// payload before allocating the frame.
fn build_frame(kind: u8, payload: &[u8], max_payload: u32) -> Result<Vec<u8>, NetworkError> {
    if payload.len() as u64 > u64::from(max_payload) {
        return Err(NetworkError::Capacity(format!(
            "message payload of {} bytes exceeds the maximum of {max_payload}",
            payload.len()
        )));
    }
    let mut frame = Vec::with_capacity(FRAME_OVERHEAD + payload.len());
    frame.extend_from_slice(PROTOCOL_MAGIC);
    put_u16(&mut frame, PROTOCOL_VERSION);
    frame.push(kind);
    put_u32(&mut frame, payload.len() as u32);
    frame.extend_from_slice(payload);
    let checksum = Crc32::new()
        .chain(&PROTOCOL_VERSION.to_le_bytes())
        .chain(&[kind])
        .chain(payload)
        .finalize();
    put_u32(&mut frame, checksum);
    Ok(frame)
}

// ---------------------------------------------------------------- decoding

/// Decodes a complete frame into a client message.
pub fn decode_client(frame: &[u8], max_payload: u32) -> Result<ClientMessage, ProtocolError> {
    let (kind, payload) = decode_frame(frame, max_payload)?;
    Ok(match kind {
        KIND_HANDSHAKE => {
            let mut cursor = payload.as_slice();
            let version = get_u16(&mut cursor)?;
            let name = get_str(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::Handshake { version, name }
        }
        KIND_AUTHENTICATE => {
            let mut cursor = payload.as_slice();
            let credentials = get_str(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::Authenticate { credentials }
        }
        KIND_ATTACH => {
            let mut cursor = payload.as_slice();
            let world = WorldId::from_u64(get_u64(&mut cursor)?);
            ensure_consumed(cursor)?;
            ClientMessage::AttachWorld { world }
        }
        KIND_INPUT_FRAME => {
            let mut cursor = payload.as_slice();
            let frame = decode_frame_payload(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::InputFrame { frame }
        }
        KIND_SUBSCRIBE => {
            let mut cursor = payload.as_slice();
            let request_id = get_u64(&mut cursor)?;
            let query = decode_query(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::Subscribe { request_id, query }
        }
        KIND_UNSUBSCRIBE => {
            let mut cursor = payload.as_slice();
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            ensure_consumed(cursor)?;
            ClientMessage::Unsubscribe { subscription }
        }
        KIND_RESYNC => {
            let mut cursor = payload.as_slice();
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            ensure_consumed(cursor)?;
            ClientMessage::Resync { subscription }
        }
        KIND_PING => {
            let mut cursor = payload.as_slice();
            let nonce = get_u64(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::Ping { nonce }
        }
        KIND_DETACH => {
            let cursor = payload.as_slice();
            ensure_consumed(cursor)?;
            ClientMessage::DetachWorld
        }
        KIND_CALL_REDUCER => {
            let mut cursor = payload.as_slice();
            let request_id = get_u64(&mut cursor)?;
            let reducer = get_str(&mut cursor)?;
            if reducer.is_empty() {
                return Err(malformed("reducer call name must not be empty"));
            }
            let args = get_reducer_args(&mut cursor)?;
            ensure_consumed(cursor)?;
            ClientMessage::CallReducer {
                request_id,
                reducer,
                args,
            }
        }
        _ => return Err(ProtocolError::UnknownKind(kind)),
    })
}

/// Decodes a complete frame into a server message.
pub fn decode_server(frame: &[u8], max_payload: u32) -> Result<ServerMessage, ProtocolError> {
    let (kind, payload) = decode_frame(frame, max_payload)?;
    Ok(match kind {
        KIND_HANDSHAKE_RESPONSE => {
            let mut cursor = payload.as_slice();
            let version = get_u16(&mut cursor)?;
            let server_name = get_str(&mut cursor)?;
            ensure_consumed(cursor)?;
            ServerMessage::HandshakeResponse { version, server_name }
        }
        KIND_AUTH_RESULT => {
            let mut cursor = payload.as_slice();
            let ok = get_bool(&mut cursor)?;
            if ok {
                let id = get_u64(&mut cursor)?;
                let name = get_str(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::AuthResult {
                    ok: true,
                    principal: Some(Principal::new(id, name)),
                    error: None,
                }
            } else {
                let error = get_str(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::AuthResult {
                    ok: false,
                    principal: None,
                    error: Some(error),
                }
            }
        }
        KIND_ATTACH_RESULT => {
            let mut cursor = payload.as_slice();
            let ok = get_bool(&mut cursor)?;
            if ok {
                let world = WorldId::from_u64(get_u64(&mut cursor)?);
                ensure_consumed(cursor)?;
                ServerMessage::AttachResult {
                    ok: true,
                    world: Some(world),
                    error: None,
                }
            } else {
                let error = get_str(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::AttachResult {
                    ok: false,
                    world: None,
                    error: Some(error),
                }
            }
        }
        KIND_DETACH_RESULT => {
            let mut cursor = payload.as_slice();
            let ok = get_bool(&mut cursor)?;
            if ok {
                ensure_consumed(cursor)?;
                ServerMessage::DetachResult { ok: true, error: None }
            } else {
                let error = get_str(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::DetachResult { ok: false, error: Some(error) }
            }
        }
        KIND_TICK_UPDATE => {
            let mut cursor = payload.as_slice();
            let world = WorldId::from_u64(get_u64(&mut cursor)?);
            let tick = TickId::from_u64(get_u64(&mut cursor)?);
            let tx_id = TransactionId::from_u64(get_u64(&mut cursor)?);
            let count = get_u64(&mut cursor)?;
            let mut changes = Vec::new();
            changes
                .try_reserve(count as usize)
                .map_err(|_| malformed("change count exceeds memory capacity"))?;
            for _ in 0..count {
                changes.push(decode_change(&mut cursor)?);
            }
            let event_count = get_u64(&mut cursor)?;
            let mut events = Vec::new();
            events
                .try_reserve(event_count as usize)
                .map_err(|_| malformed("event count exceeds memory capacity"))?;
            for _ in 0..event_count {
                let name = get_str(&mut cursor)?;
                let payload = get_value(&mut cursor)?;
                events.push(ReducerEvent::new(name, payload));
            }
            ensure_consumed(cursor)?;
            ServerMessage::TickUpdate {
                world,
                tick,
                tx_id,
                changes,
                events,
            }
        }
        KIND_SUB_SNAPSHOT => {
            let mut cursor = payload.as_slice();
            let request_id = get_u64(&mut cursor)?;
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            let seq = get_u64(&mut cursor)?;
            let count = get_u64(&mut cursor)?;
            let mut rows = Vec::new();
            rows.try_reserve(count as usize)
                .map_err(|_| malformed("row count exceeds memory capacity"))?;
            for _ in 0..count {
                let row_id = RowId::from_u64(get_u64(&mut cursor)?);
                let row = get_row(&mut cursor)?;
                rows.push(DeliveredRow::new(row_id, row));
            }
            ensure_consumed(cursor)?;
            ServerMessage::SubscriptionSnapshot {
                request_id,
                subscription,
                seq,
                rows,
            }
        }
        KIND_SUB_DELTA => {
            let mut cursor = payload.as_slice();
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            let seq = get_u64(&mut cursor)?;
            let kind = delta_kind_from_tag(take_byte(&mut cursor)?)?;
            let row_id = RowId::from_u64(get_u64(&mut cursor)?);
            let row = if kind == DeltaKind::Delete {
                None
            } else {
                Some(DeliveredRow::new(row_id, get_row(&mut cursor)?))
            };
            ensure_consumed(cursor)?;
            ServerMessage::SubscriptionDelta {
                subscription,
                seq,
                kind,
                row_id,
                row,
            }
        }
        KIND_STALE => {
            let mut cursor = payload.as_slice();
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            let seq = get_u64(&mut cursor)?;
            ensure_consumed(cursor)?;
            ServerMessage::StaleNotification { subscription, seq }
        }
        KIND_ERROR => {
            let mut cursor = payload.as_slice();
            let code = get_u16(&mut cursor)?;
            let message = get_str(&mut cursor)?;
            let request_id = get_u64(&mut cursor)?;
            ensure_consumed(cursor)?;
            ServerMessage::Error {
                code,
                message,
                request_id,
            }
        }
        KIND_PONG => {
            let mut cursor = payload.as_slice();
            let nonce = get_u64(&mut cursor)?;
            ensure_consumed(cursor)?;
            ServerMessage::Pong { nonce }
        }
        KIND_DISCONNECT => {
            let mut cursor = payload.as_slice();
            let reason = get_str(&mut cursor)?;
            ensure_consumed(cursor)?;
            ServerMessage::Disconnect { reason }
        }
        KIND_REDUCER_RESULT => {
            let mut cursor = payload.as_slice();
            let request_id = get_u64(&mut cursor)?;
            let ok = get_bool(&mut cursor)?;
            if ok {
                let value = get_value(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::ReducerResult {
                    request_id,
                    ok: true,
                    value: Some(value),
                    error: None,
                }
            } else {
                let error = get_str(&mut cursor)?;
                ensure_consumed(cursor)?;
                ServerMessage::ReducerResult {
                    request_id,
                    ok: false,
                    value: None,
                    error: Some(error),
                }
            }
        }
        KIND_SUB_DELTA_BATCH => {
            let mut cursor = payload.as_slice();
            let subscription = SubscriptionId::from_u64(get_u64(&mut cursor)?);
            let request_id = get_u64(&mut cursor)?;
            let count = get_u64(&mut cursor)? as usize;
            let mut deltas = Vec::with_capacity(count);
            for _ in 0..count {
                let seq = get_u64(&mut cursor)?;
                let kind_tag = cursor.first().copied().ok_or(ProtocolError::Truncated)?;
                cursor = &cursor[1..];
                let kind = delta_kind_from_tag(kind_tag)?;
                let row_id = RowId::from_u64(get_u64(&mut cursor)?);
                let row = match kind {
                    DeltaKind::Delete => None,
                    _ => Some(DeliveredRow::new(row_id, get_row(&mut cursor)?)),
                };
                deltas.push(SubscriptionDeltaEntry { seq, kind, row_id, row });
            }
            ensure_consumed(cursor)?;
            ServerMessage::SubscriptionDeltaBatch {
                subscription,
                request_id,
                deltas,
            }
        }
        _ => return Err(ProtocolError::UnknownKind(kind)),
    })
}

/// Validates one complete frame and returns `(kind, payload)`. The frame's
/// declared length is checked before the payload is sliced (bounded
/// allocations); the checksum is verified.
fn decode_frame(frame: &[u8], max_payload: u32) -> Result<(u8, Vec<u8>), ProtocolError> {
    if frame.len() < HEADER_LEN {
        return Err(ProtocolError::Truncated);
    }
    if &frame[0..4] != PROTOCOL_MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    let version = u16::from_le_bytes([frame[4], frame[5]]);
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = frame[6];
    let payload_len = u32::from_le_bytes([frame[7], frame[8], frame[9], frame[10]]) as u64;
    if payload_len > u64::from(max_payload) {
        return Err(ProtocolError::Oversized {
            len: payload_len,
            max: u64::from(max_payload),
        });
    }
    let total = HEADER_LEN + payload_len as usize + 4;
    if frame.len() < total {
        return Err(ProtocolError::Truncated);
    }
    let payload = &frame[HEADER_LEN..HEADER_LEN + payload_len as usize];
    let stored = u32::from_le_bytes(
        frame[HEADER_LEN + payload_len as usize..total]
            .try_into()
            .expect("4 bytes"),
    );
    let computed = Crc32::new()
        .chain(&version.to_le_bytes())
        .chain(&[kind])
        .chain(payload)
        .finalize();
    if computed != stored {
        return Err(ProtocolError::BadChecksum);
    }
    Ok((kind, payload.to_vec()))
}

/// Incremental frame parsing for streaming transports (TCP): attempts to
/// extract one complete, valid frame from the front of `buf`. Returns
/// `Some((frame, consumed))` on success, `None` when more bytes are needed,
/// or an error when the buffered bytes are provably invalid (bad magic,
/// over-limit length) or fail the checksum.
pub fn parse_frame(
    buf: &[u8],
    max_payload: u32,
) -> Result<Option<(Vec<u8>, usize)>, ProtocolError> {
    if buf.len() < HEADER_LEN {
        return Ok(None);
    }
    if &buf[0..4] != PROTOCOL_MAGIC {
        return Err(ProtocolError::BadMagic);
    }
    let version = u16::from_le_bytes([buf[4], buf[5]]);
    if version != PROTOCOL_VERSION {
        return Err(ProtocolError::UnsupportedVersion(version));
    }
    let kind = buf[6];
    let payload_len = u32::from_le_bytes([buf[7], buf[8], buf[9], buf[10]]) as u64;
    if payload_len > u64::from(max_payload) {
        return Err(ProtocolError::Oversized {
            len: payload_len,
            max: u64::from(max_payload),
        });
    }
    let total = HEADER_LEN + payload_len as usize + 4;
    if buf.len() < total {
        return Ok(None);
    }
    let frame = buf[..total].to_vec();
    let payload = &frame[HEADER_LEN..HEADER_LEN + payload_len as usize];
    let stored = u32::from_le_bytes(
        frame[HEADER_LEN + payload_len as usize..total]
            .try_into()
            .expect("4 bytes"),
    );
    let computed = Crc32::new()
        .chain(&version.to_le_bytes())
        .chain(&[kind])
        .chain(payload)
        .finalize();
    if computed != stored {
        return Err(ProtocolError::BadChecksum);
    }
    Ok(Some((frame, total)))
}

// ------------------------------------------------------ payload codecs

/// Input frame payload: `tick | count | (source | kind | has_payload |
/// value)*`.
fn encode_frame_payload(out: &mut Vec<u8>, frame: &InputFrame) {
    put_u64(out, frame.tick().as_u64());
    put_u64(out, frame.commands().len() as u64);
    for command in frame.commands() {
        put_u64(out, command.source());
        put_str(out, command.kind());
        match command.payload() {
            Some(value) => {
                out.push(1);
                put_value(out, value);
            }
            None => out.push(0),
        }
    }
}

/// Decodes an input frame payload; empty command kinds are rejected as
/// malformed.
fn decode_frame_payload(cursor: &mut &[u8]) -> Result<InputFrame, ProtocolError> {
    let tick = TickId::from_u64(get_u64(cursor)?);
    let count = get_u64(cursor)?;
    // Every command encodes as at least 18 bytes (source u64 + kind-length
    // u64 + one kind byte + payload flag). Reject an impossible count
    // BEFORE allocating so a hostile count can never trigger a capacity
    // overflow (bounded allocations, no panics).
    if count > cursor.len() as u64 / 18 {
        return Err(malformed("command count exceeds the remaining payload"));
    }
    let mut frame = InputFrame::with_capacity(tick, count as usize);
    for _ in 0..count {
        let source = get_u64(cursor)?;
        let kind = get_str(cursor)?;
        if kind.is_empty() {
            return Err(malformed("input command kind must not be empty"));
        }
        let payload = match take_byte(cursor)? {
            0 => None,
            1 => Some(get_value(cursor)?),
            other => return Err(malformed(format!("invalid payload flag {other}"))),
        };
        let command = InputCommand::new(source, kind, payload)
            .map_err(|error| malformed(error.to_string()))?;
        frame.push(command);
    }
    Ok(frame)
}

/// Query payload: `table | predicate_count | (column | op | value)* |
/// order_flag | (column | direction)? | limit_flag | u32? | project_flag |
/// (count | column*)?`.
fn encode_query(out: &mut Vec<u8>, query: &Query) {
    put_str(out, query.table());
    put_u64(out, query.predicates().len() as u64);
    for predicate in query.predicates() {
        put_str(out, predicate.column());
        out.push(comparison_op_tag(predicate.op()));
        put_value(out, predicate.value());
    }
    match query.order_by() {
        Some(order) => {
            out.push(1);
            put_str(out, order.column());
            out.push(order_direction_tag(order.direction()));
        }
        None => out.push(0),
    }
    match query.limit() {
        Some(limit) => {
            out.push(1);
            put_u32(out, limit);
        }
        None => out.push(0),
    }
    match query.projection() {
        Some(columns) => {
            out.push(1);
            put_u64(out, columns.len() as u64);
            for column in columns {
                put_str(out, column);
            }
        }
        None => out.push(0),
    }
}

/// Decodes a query payload, reconstructing it through the validating
/// builder (so an empty table name etc. is a malformed error).
fn decode_query(cursor: &mut &[u8]) -> Result<Query, ProtocolError> {
    let table = get_str(cursor)?;
    let mut builder = Query::builder(table);
    let predicate_count = get_u64(cursor)?;
    for _ in 0..predicate_count {
        let column = get_str(cursor)?;
        let op = comparison_op_from_tag(take_byte(cursor)?)?;
        let value = get_value(cursor)?;
        builder = builder.predicate(column, op, value);
    }
    if take_byte(cursor)? == 1 {
        let column = get_str(cursor)?;
        let direction = order_direction_from_tag(take_byte(cursor)?)?;
        builder = builder.order_by(column, direction);
    }
    if take_byte(cursor)? == 1 {
        builder = builder.limit(get_u32(cursor)?);
    }
    if take_byte(cursor)? == 1 {
        let count = get_u64(cursor)?;
        let mut columns = Vec::new();
        columns
            .try_reserve(count as usize)
            .map_err(|_| malformed("projection count exceeds memory capacity"))?;
        for _ in 0..count {
            columns.push(get_str(cursor)?);
        }
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        builder = builder.project(&names);
    }
    builder.build().map_err(|error| malformed(error.to_string()))
}

/// Reducer args payload: `count | (name | value)*` — the deterministic
/// (key-sorted) iteration order of `ReducerArgs` (ADR-013 D3).
fn put_reducer_args(out: &mut Vec<u8>, args: &ReducerArgs) {
    put_u64(out, args.len() as u64);
    for (name, value) in args.iter() {
        put_str(out, name);
        put_value(out, value);
    }
}

/// Decodes a reducer args payload. The declared count is bounded by the
/// remaining payload (each arg encodes as at least one name-length byte plus
/// a value) before allocation.
fn get_reducer_args(cursor: &mut &[u8]) -> Result<ReducerArgs, ProtocolError> {
    let count = get_u64(cursor)?;
    if count > cursor.len() as u64 {
        return Err(malformed("reducer args count exceeds the remaining payload"));
    }
    let mut args = ReducerArgs::new();
    for _ in 0..count {
        let name = get_str(cursor)?;
        let value = get_value(cursor)?;
        args = args.insert(name, value);
    }
    Ok(args)
}

/// A `Change` payload: `table_id | kind | row_id | old_row? | new_row? |
/// old_version? | new_version?` (the same field order as the WAL).
fn encode_change(out: &mut Vec<u8>, change: &Change) {
    put_u64(out, change.table_id().as_u64());
    out.push(change_kind_tag(change.kind()));
    put_u64(out, change.row_id().as_u64());
    put_opt_row(out, change.old_row());
    put_opt_row(out, change.new_row());
    put_opt_version(out, change.old_version());
    put_opt_version(out, change.new_version());
}

/// Decodes a `Change` payload.
fn decode_change(cursor: &mut &[u8]) -> Result<Change, ProtocolError> {
    let table_id = nexum_core::TableId::from_u64(get_u64(cursor)?);
    let kind = change_kind_from_tag(take_byte(cursor)?)?;
    let row_id = RowId::from_u64(get_u64(cursor)?);
    let old_row = get_opt_row(cursor)?;
    let new_row = get_opt_row(cursor)?;
    let old_version = get_opt_version(cursor)?;
    let new_version = get_opt_version(cursor)?;
    Ok(match kind {
        nexum_core::ChangeKind::Insert => Change::insert(
            table_id,
            row_id,
            new_row.ok_or_else(|| malformed("insert change lacks a new row"))?,
            new_version.ok_or_else(|| malformed("insert change lacks a new version"))?,
        ),
        nexum_core::ChangeKind::Update => Change::update(
            table_id,
            row_id,
            old_row.ok_or_else(|| malformed("update change lacks an old row"))?,
            old_version.ok_or_else(|| malformed("update change lacks an old version"))?,
            new_row.ok_or_else(|| malformed("update change lacks a new row"))?,
            new_version.ok_or_else(|| malformed("update change lacks a new version"))?,
        ),
        nexum_core::ChangeKind::Delete => Change::delete(
            table_id,
            row_id,
            old_row.ok_or_else(|| malformed("delete change lacks an old row"))?,
            old_version.ok_or_else(|| malformed("delete change lacks an old version"))?,
        ),
    })
}

fn put_opt_row(out: &mut Vec<u8>, row: Option<&Row>) {
    match row {
        Some(row) => {
            out.push(1);
            put_row(out, row);
        }
        None => out.push(0),
    }
}

fn get_opt_row(cursor: &mut &[u8]) -> Result<Option<Row>, ProtocolError> {
    match take_byte(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(get_row(cursor)?)),
        other => Err(malformed(format!("invalid optional-row flag {other}"))),
    }
}

fn put_opt_version(out: &mut Vec<u8>, version: Option<Version>) {
    match version {
        Some(version) => {
            out.push(1);
            put_u64(out, version.as_u64());
        }
        None => out.push(0),
    }
}

fn get_opt_version(cursor: &mut &[u8]) -> Result<Option<Version>, ProtocolError> {
    match take_byte(cursor)? {
        0 => Ok(None),
        1 => Ok(Some(Version::from_u64(get_u64(cursor)?))),
        other => Err(malformed(format!("invalid optional-version flag {other}"))),
    }
}

// ------------------------------------------------------------- tag tables

const fn comparison_op_tag(op: ComparisonOp) -> u8 {
    match op {
        ComparisonOp::Eq => 0,
        ComparisonOp::Ne => 1,
        ComparisonOp::Lt => 2,
        ComparisonOp::Lte => 3,
        ComparisonOp::Gt => 4,
        ComparisonOp::Gte => 5,
    }
}

fn comparison_op_from_tag(tag: u8) -> Result<ComparisonOp, ProtocolError> {
    Ok(match tag {
        0 => ComparisonOp::Eq,
        1 => ComparisonOp::Ne,
        2 => ComparisonOp::Lt,
        3 => ComparisonOp::Lte,
        4 => ComparisonOp::Gt,
        5 => ComparisonOp::Gte,
        _ => return Err(malformed(format!("unknown comparison op tag {tag}"))),
    })
}

const fn order_direction_tag(direction: OrderDirection) -> u8 {
    match direction {
        OrderDirection::Ascending => 0,
        OrderDirection::Descending => 1,
    }
}

fn order_direction_from_tag(tag: u8) -> Result<OrderDirection, ProtocolError> {
    Ok(match tag {
        0 => OrderDirection::Ascending,
        1 => OrderDirection::Descending,
        _ => return Err(malformed(format!("unknown order direction tag {tag}"))),
    })
}

const fn delta_kind_tag(kind: DeltaKind) -> u8 {
    match kind {
        DeltaKind::Insert => 1,
        DeltaKind::Update => 2,
        DeltaKind::Delete => 3,
    }
}

fn delta_kind_from_tag(tag: u8) -> Result<DeltaKind, ProtocolError> {
    Ok(match tag {
        1 => DeltaKind::Insert,
        2 => DeltaKind::Update,
        3 => DeltaKind::Delete,
        _ => return Err(malformed(format!("unknown delta kind tag {tag}"))),
    })
}

const fn change_kind_tag(kind: nexum_core::ChangeKind) -> u8 {
    match kind {
        nexum_core::ChangeKind::Insert => 1,
        nexum_core::ChangeKind::Update => 2,
        nexum_core::ChangeKind::Delete => 3,
    }
}

fn change_kind_from_tag(tag: u8) -> Result<nexum_core::ChangeKind, ProtocolError> {
    Ok(match tag {
        1 => nexum_core::ChangeKind::Insert,
        2 => nexum_core::ChangeKind::Update,
        3 => nexum_core::ChangeKind::Delete,
        _ => return Err(malformed(format!("unknown change kind tag {tag}"))),
    })
}

// --------------------------------------------------------------- helpers

fn put_u16(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn get_u16(cursor: &mut &[u8]) -> Result<u16, ProtocolError> {
    let bytes = take_bytes(cursor, 2)?;
    Ok(u16::from_le_bytes(bytes.try_into().expect("2 bytes")))
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn get_u32(cursor: &mut &[u8]) -> Result<u32, ProtocolError> {
    let bytes = take_bytes(cursor, 4)?;
    Ok(u32::from_le_bytes(bytes.try_into().expect("4 bytes")))
}

fn take_bytes<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8], ProtocolError> {
    if cursor.len() < len {
        return Err(ProtocolError::Truncated);
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

fn take_byte(cursor: &mut &[u8]) -> Result<u8, ProtocolError> {
    Ok(take_bytes(cursor, 1)?[0])
}

fn ensure_consumed(cursor: &[u8]) -> Result<(), ProtocolError> {
    if cursor.is_empty() {
        Ok(())
    } else {
        Err(malformed("trailing bytes after message payload"))
    }
}

fn malformed(message: impl Into<String>) -> ProtocolError {
    ProtocolError::Malformed(message.into())
}

impl From<Error> for ProtocolError {
    /// The binary codec reports byte-format violations as core
    /// [`Error::Internal`]; the network boundary classifies them as
    /// malformed messages.
    fn from(error: Error) -> Self {
        match error {
            Error::Internal(message) => ProtocolError::Malformed(message),
            other => ProtocolError::Malformed(other.to_string()),
        }
    }
}
