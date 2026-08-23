//! The [`Client`] orchestrator (ADR-013): lifecycle, the polling pump, and
//! the correlation state.
//!
//! The client is **poll-driven**: the host services the transport (the
//! gateway flushes responses back), then calls [`Client::pump`] to drain
//! inbound frames, decode them, and dispatch into typed
//! [`ServerEvent`](crate::event::ServerEvent)s, correlated
//! [`ReducerResult`](crate::request::ReducerResult)s, and derived
//! [`View`](crate::view::View)s. Nothing here is authoritative — every
//! transition originated at the server.

use std::collections::{BTreeMap, VecDeque};

use nexum_core::{RowId, SubscriptionId};
use nexum_network::transport::Connection;

use crate::config::SdkConfig;
use crate::connection::{ConnectionState, ConnectionStatus};
use crate::error::SdkError;
use crate::event::ServerEvent;
use crate::protocol::{ClientMessage, DeltaKind, ServerMessage};
use crate::request::{PendingCall, ReducerResult};
use crate::session::SessionInfo;
use crate::subscription::SubscriptionHandle;
use crate::transport::ClientTransport;
use crate::view::{View, ViewGap};

/// The report of one [`Client::pump`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct PumpReport {
    /// Frames pulled from the transport.
    pub frames: u64,
    /// Frames decoded and dispatched.
    pub dispatched: u64,
    /// Frames rejected (decode failures) or transport errors.
    pub rejected: u64,
    /// The transport closed during the pass.
    pub closed: bool,
}

/// The poll-driven realtime client (ADR-013).
pub struct Client {
    pub(crate) config: SdkConfig,
    pub(crate) transport: Option<ClientTransport>,
    pub(crate) state: ConnectionState,
    /// Monotonic id space for requests (reducer calls, subscribes, pings).
    pub(crate) next_request_id: u64,
    /// Monotonic local id space for subscription handles.
    pub(crate) next_local_subscription: u64,
    /// Pending reducer calls: `request_id -> call`.
    pub(crate) pending_calls: BTreeMap<u64, PendingCall>,
    /// Correlated reducer results awaiting `take_reducer_results`.
    pub(crate) reducer_results: VecDeque<ReducerResult>,
    /// In-flight subscribes: `request_id -> local` (the server echoes the
    /// request id on the snapshot and on rejections).
    pub(crate) pending_subscribes: BTreeMap<u64, u64>,
    /// Bound and in-flight subscription handles: `local -> handle`.
    pub(crate) subscriptions: BTreeMap<u64, SubscriptionHandle>,
    /// Derived views: `local -> view`.
    pub(crate) views: BTreeMap<u64, View>,
    /// The authenticated session, once established.
    pub(crate) session: Option<SessionInfo>,
    /// The bounded server-event queue (oldest dropped at capacity).
    pub(crate) events: VecDeque<ServerEvent>,
    pub(crate) last_error: Option<SdkError>,
}

impl Client {
    /// Creates a client with `config`. Returns [`SdkError::InvalidArgument`]
    /// for an invalid configuration.
    pub fn new(config: SdkConfig) -> Result<Self, SdkError> {
        config.validate()?;
        Ok(Self {
            config,
            transport: None,
            state: ConnectionState::Disconnected,
            next_request_id: 1,
            next_local_subscription: 0,
            pending_calls: BTreeMap::new(),
            reducer_results: VecDeque::new(),
            pending_subscribes: BTreeMap::new(),
            subscriptions: BTreeMap::new(),
            views: BTreeMap::new(),
            session: None,
            events: VecDeque::new(),
            last_error: None,
        })
    }

    /// Returns the configuration.
    pub fn config(&self) -> &SdkConfig {
        &self.config
    }

    /// Returns the current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// Returns a snapshot of the connection status.
    pub fn status(&self) -> ConnectionStatus {
        ConnectionStatus {
            state: self.state,
            peer: self
                .transport
                .as_ref()
                .map(|transport| transport.peer().to_string()),
        }
    }

    /// Returns `true` once the handshake completed.
    pub fn is_connected(&self) -> bool {
        self.state == ConnectionState::Connected
    }

    /// Returns `true` once the connection ended.
    pub fn is_closed(&self) -> bool {
        matches!(
            self.state,
            ConnectionState::Closed | ConnectionState::Disconnected
        )
    }

    /// Attaches a transport and begins the handshake. The handshake
    /// completes on the next [`Client::pump`] (a `HandshakeResponse`).
    pub fn connect(&mut self, transport: Box<dyn Connection>) -> Result<(), SdkError> {
        if !matches!(
            self.state,
            ConnectionState::Disconnected | ConnectionState::Closed
        ) {
            return Err(SdkError::AlreadyConnected);
        }
        self.config.validate()?;
        self.transport = Some(ClientTransport::new(transport));
        self.state = ConnectionState::Connecting;
        self.send_message(&ClientMessage::Handshake {
            version: self.config.protocol_version(),
            name: self.config.client_name().to_string(),
        })
    }

    /// Drains every buffered inbound frame: decode, dispatch, and apply.
    /// Returns a per-pass report; a benign transport close is reported via
    /// `report.closed` (the client transitions to `Closed`).
    pub fn pump(&mut self) -> Result<PumpReport, SdkError> {
        let mut report = PumpReport::default();
        loop {
            // Combined receive: tries direct then frame in a single lock.
            let (msg_opt, frame_opt) = {
                let Some(transport) = self.transport.as_mut() else {
                    return Err(SdkError::NotConnected);
                };
                match transport.recv_any() {
                    Ok(pair) => pair,
                    Err(SdkError::TransportClosed) | Err(SdkError::TransportFull) => {
                        self.transition_to_closed("transport closed");
                        report.closed = true;
                        break;
                    }
                    Err(error) => {
                        self.last_error = Some(error.clone());
                        report.rejected += 1;
                        break;
                    }
                }
            };
            if let Some(message) = msg_opt {
                report.frames += 1;
                report.dispatched += 1;
                self.dispatch(message);
                continue;
            }
            let Some(frame) = frame_opt else {
                break;
            };
            report.frames += 1;
            match crate::protocol::decode_server(&frame, self.config.max_frame_payload()) {
                Ok(message) => {
                    report.dispatched += 1;
                    self.dispatch(message);
                }
                Err(error) => {
                    report.rejected += 1;
                    let code = match &error {
                        SdkError::Protocol(protocol) => protocol.code(),
                        _ => 30,
                    };
                    self.push_event(ServerEvent::Error {
                        code,
                        message: error.to_string(),
                    });
                }
            }
        }
        Ok(report)
    }

    /// Dispatches one decoded server message into events, results, and
    /// views.
    pub(crate) fn dispatch(&mut self, message: ServerMessage) {
        match message {
            ServerMessage::HandshakeResponse {
                version,
                server_name,
            } => {
                if version != self.config.protocol_version() {
                    self.last_error = Some(SdkError::HandshakeMismatch {
                        expected: self.config.protocol_version(),
                        received: version,
                    });
                    self.state = ConnectionState::Closed;
                    self.close_transport();
                    return;
                }
                self.state = ConnectionState::Connected;
                self.push_event(ServerEvent::Connected {
                    version,
                    server_name,
                });
            }
            ServerMessage::AuthResult {
                ok,
                principal,
                error,
            } => {
                if ok {
                    if let Some(principal) = principal {
                        self.session = Some(SessionInfo::new(principal.clone()));
                        self.push_event(ServerEvent::Authenticated { principal });
                    }
                } else {
                    self.push_event(ServerEvent::AuthFailed {
                        message: error.unwrap_or_else(|| "authentication failed".to_string()),
                    });
                }
            }
            ServerMessage::AttachResult { ok, world, error } => {
                if ok {
                    if let Some(world) = world {
                        if let Some(session) = self.session.as_mut() {
                            session.attach(world);
                        }
                        self.push_event(ServerEvent::Attached { world });
                    }
                } else {
                    self.push_event(ServerEvent::AttachFailed {
                        message: error.unwrap_or_else(|| "attach failed".to_string()),
                    });
                }
            }
            ServerMessage::DetachResult { ok, error } => {
                if let Some(session) = self.session.as_mut() {
                    session.detach();
                }
                self.fail_pending_calls("session detached");
                if ok {
                    self.push_event(ServerEvent::Detached);
                } else {
                    self.push_event(ServerEvent::Error {
                        code: 21,
                        message: error.unwrap_or_else(|| "detach failed".to_string()),
                    });
                }
            }
            ServerMessage::TickUpdate {
                world,
                tick,
                tx_id,
                changes,
                events,
            } => {
                self.push_event(ServerEvent::Tick {
                    world,
                    tick,
                    tx_id,
                    changes,
                    events,
                });
            }
            ServerMessage::SubscriptionSnapshot {
                request_id,
                subscription,
                seq,
                rows,
            } => self.apply_snapshot(request_id, subscription, seq, rows),
            ServerMessage::SubscriptionDelta {
                subscription,
                seq,
                kind,
                row_id,
                row,
            } => self.apply_delta(subscription, seq, kind, row_id, row),
            ServerMessage::StaleNotification { subscription, seq } => {
                if let Some(local) = self.local_of_server(subscription) {
                    if let Some(handle) = self.subscriptions.get_mut(&local) {
                        handle.mark_stale();
                    }
                    self.push_event(ServerEvent::Stale { subscription, seq });
                }
            }
            ServerMessage::Error {
                code,
                message,
                request_id,
            } => {
                if request_id != 0
                    && let Some(local) = self.pending_subscribes.remove(&request_id)
                {
                    self.subscriptions.remove(&local);
                    self.views.remove(&local);
                    self.push_event(ServerEvent::SubscriptionRejected { local, message });
                    return;
                }
                self.push_event(ServerEvent::Error { code, message });
            }
            ServerMessage::Pong { nonce } => {
                self.push_event(ServerEvent::Pong { nonce });
            }
            ServerMessage::Disconnect { reason } => {
                self.transition_to_closed(&reason);
            }
            ServerMessage::ReducerResult {
                request_id,
                ok,
                value,
                error,
            } => {
                if self.pending_calls.remove(&request_id).is_some() {
                    self.reducer_results
                        .push_back(ReducerResult::new(request_id, ok, value, error));
                } else {
                    self.push_event(ServerEvent::Error {
                        code: 23,
                        message: format!("unknown request id {request_id}"),
                    });
                }
            }
            ServerMessage::SubscriptionDeltaBatch {
                subscription,
                request_id: _,
                deltas,
            } => {
                for entry in deltas {
                    self.apply_delta(
                        subscription,
                        entry.seq,
                        entry.kind,
                        entry.row_id,
                        entry.row.map(|arc| (*arc).clone()),
                    );
                }
            }
        }
    }

    /// Binds or refreshes a subscription view from a snapshot. A snapshot
    /// echoing a pending subscribe's `request_id` binds that handle; a
    /// snapshot for an already-bound server subscription refreshes it
    /// (resync); anything else is surfaced as an error.
    fn apply_snapshot(
        &mut self,
        request_id: u64,
        subscription: SubscriptionId,
        seq: u64,
        rows: Vec<nexum_subscription::DeliveredRow>,
    ) {
        if let Some(local) = self.pending_subscribes.remove(&request_id) {
            if let Some(handle) = self.subscriptions.get_mut(&local) {
                handle.bind(subscription);
            }
            if let Some(view) = self.views.get_mut(&local) {
                view.apply_snapshot(seq, rows);
            }
            self.push_event(ServerEvent::SubscriptionBound {
                local,
                server: subscription,
                seq,
            });
            return;
        }
        if let Some(local) = self.local_of_server(subscription) {
            if let Some(handle) = self.subscriptions.get_mut(&local) {
                handle.clear_stale();
            }
            if let Some(view) = self.views.get_mut(&local) {
                view.apply_snapshot(seq, rows);
            }
            self.push_event(ServerEvent::SubscriptionResynced { local, seq });
            return;
        }
        self.push_event(ServerEvent::Error {
            code: 22,
            message: format!("snapshot for unknown subscription {subscription}"),
        });
    }

    /// Applies one subscription delta to the matching view, detecting
    /// sequence gaps (silent loss). Deltas for unknown or stale
    /// subscriptions are ignored (the caller must resync a stale handle).
    fn apply_delta(
        &mut self,
        subscription: SubscriptionId,
        seq: u64,
        kind: DeltaKind,
        row_id: RowId,
        row: Option<nexum_subscription::DeliveredRow>,
    ) {
        let Some(local) = self.local_of_server(subscription) else {
            self.push_event(ServerEvent::Error {
                code: 22,
                message: format!("delta for unknown subscription {subscription}"),
            });
            return;
        };
        if self
            .subscriptions
            .get(&local)
            .is_some_and(|handle| handle.is_stale())
        {
            return;
        }
        let Some(view) = self.views.get_mut(&local) else {
            return;
        };
        if let Err(ViewGap { expected, got }) = view.apply_delta(seq, kind, row_id, row) {
            if let Some(handle) = self.subscriptions.get_mut(&local) {
                handle.mark_stale();
            }
            self.push_event(ServerEvent::ViewGap {
                local,
                expected,
                got,
            });
        }
    }

    /// Finds the local subscription handle for a server subscription id.
    fn local_of_server(&self, subscription: SubscriptionId) -> Option<u64> {
        self.subscriptions
            .iter()
            .find(|(_, handle)| handle.server() == Some(subscription))
            .map(|(local, _)| *local)
    }

    // ---------------------------------------------------------- core plumbing

    /// Encodes and buffers one client message.
    pub(crate) fn send_message(&mut self, message: &ClientMessage) -> Result<(), SdkError> {
        let frame = crate::protocol::encode_client(message, self.config.max_frame_payload())?;
        let Some(transport) = self.transport.as_mut() else {
            return Err(SdkError::NotConnected);
        };
        transport.send_frame(frame)
    }

    /// Enqueues a typed event, dropping the oldest at capacity.
    pub(crate) fn push_event(&mut self, event: ServerEvent) {
        if self.events.len() >= self.config.max_events() {
            self.events.pop_front();
        }
        self.events.push_back(event);
    }

    /// Fails every pending reducer call with `reason` (results remain
    /// observable via `take_reducer_results`, so callers never hang).
    pub(crate) fn fail_pending_calls(&mut self, reason: &str) {
        let ids: Vec<u64> = self.pending_calls.keys().copied().collect();
        for id in ids {
            self.pending_calls.remove(&id);
            self.reducer_results.push_back(ReducerResult::new(
                id,
                false,
                None,
                Some(reason.to_string()),
            ));
        }
    }

    /// Transitions to `Closed`: closes the transport, drops the session and
    /// all subscriptions, fails pending calls, and surfaces a
    /// `Disconnected` event.
    pub(crate) fn transition_to_closed(&mut self, reason: &str) {
        self.close_transport();
        self.state = ConnectionState::Closed;
        self.session = None;
        self.fail_pending_calls(&format!("connection closed: {reason}"));
        self.pending_subscribes.clear();
        self.subscriptions.clear();
        self.views.clear();
        self.push_event(ServerEvent::Disconnected {
            reason: reason.to_string(),
        });
    }

    fn close_transport(&mut self) {
        if let Some(transport) = self.transport.as_mut() {
            transport.close();
        }
    }

    // --------------------------------------------------------------- guards

    /// Guards an operation that requires a completed handshake.
    pub(crate) fn require_connected(&self) -> Result<(), SdkError> {
        if self.state == ConnectionState::Connected {
            Ok(())
        } else {
            Err(SdkError::NotConnected)
        }
    }

    /// Guards an operation that requires an authenticated, attached
    /// session.
    pub(crate) fn require_attached(&self) -> Result<(), SdkError> {
        self.require_connected()?;
        if self.session.is_none() {
            return Err(SdkError::AuthenticationRequired);
        }
        if self
            .session
            .as_ref()
            .is_none_or(|session| !session.is_attached())
        {
            return Err(SdkError::NotAttached);
        }
        Ok(())
    }

    /// Sends a liveness probe; the server answers with a `Pong` event
    /// (surfaced by [`Client::take_events`]).
    pub fn ping(&mut self) -> Result<(), SdkError> {
        self.require_connected()?;
        let nonce = self.next_request_id;
        self.next_request_id += 1;
        self.send_message(&ClientMessage::Ping { nonce })
    }

    // --------------------------------------------------------------- drains

    /// Takes every buffered server event in order, clearing the queue.
    pub fn take_events(&mut self) -> Vec<ServerEvent> {
        self.events.drain(..).collect()
    }

    /// Returns the number of buffered server events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Returns the last non-fatal error recorded by the client, if any.
    pub fn last_error(&self) -> Option<&SdkError> {
        self.last_error.as_ref()
    }

    /// Takes the last recorded error, if any.
    pub fn take_error(&mut self) -> Option<SdkError> {
        self.last_error.take()
    }
}

impl std::fmt::Debug for Client {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Client")
            .field("state", &self.state)
            .field("peer", &self.status().peer)
            .field("pending_calls", &self.pending_calls.len())
            .field("subscriptions", &self.subscriptions.len())
            .finish()
    }
}

impl Default for Client {
    fn default() -> Self {
        Self::new(SdkConfig::new()).expect("default config is valid")
    }
}
