//! The [`NetworkGateway`]: the realtime adapter around the runtime
//! (ADR-011 D1).
//!
//! The gateway owns the runtime and orchestrates it, but every
//! authoritative operation flows through the existing runtime boundary:
//! `submit_input` (inputs), `subscribe`/`drain`/`unsubscribe`/`resync`
//! (observation), `step_detailed` (scheduling), `recover_world`
//! (durability). It never touches tables, transactions, the WAL, or the
//! subscription registries directly.
//!
//! Backpressure (ADR-011 D5): each connection's transport queue is bounded;
//! an outbound overflow applies the configured policy (mark the session
//! stale + deliver `StaleNotification`s when the queue drains, or
//! disconnect). Simulation and other clients are never blocked.

use std::collections::{BTreeMap, VecDeque};
use std::sync::Arc;

use nexum_core::{ConnectionId, Error, SessionId, SubscriptionId, WorldId};
use nexum_reducer::ReducerArgs;
use nexum_runtime::{Runtime, RuntimeError};
use nexum_simulation::{InputCommand, InputFrame};
use nexum_subscription::SubscriptionUpdate;

use crate::auth::Authenticator;
use crate::config::{NetworkConfig, NetworkEvent, OutboundOverflowPolicy};
use crate::control::ControlPlane;
use crate::error::NetworkError;
use crate::metrics::NetworkMetrics;
use crate::protocol::{self, ClientMessage, DeltaKind, PROTOCOL_VERSION, ServerMessage};
use crate::session::Session;
use crate::transport::{Connection, TransportError};

/// One registered connection and its operational state.
pub(crate) struct ConnectionEntry {
    connection: Box<dyn Connection>,
    session: Option<Session>,
    /// The session's network subscriptions (keyed by the registry's
    /// subscription id, unique within the attached world).
    subscriptions: BTreeMap<SubscriptionId, NetworkSubscription>,
    /// The session fell behind: TickUpdates and deltas are dropped until a
    /// `Resync` (or reattach) clears it.
    stale: bool,
    /// Encoded `StaleNotification`s queued while the outbound queue was
    /// full; flushed as soon as the queue has room.
    pending_stale: VecDeque<Vec<u8>>,
}

impl ConnectionEntry {
    fn new(connection: Box<dyn Connection>) -> Self {
        Self {
            connection,
            session: None,
            subscriptions: BTreeMap::new(),
            stale: false,
            pending_stale: VecDeque::new(),
        }
    }
}

/// A session's subscription mapping onto the runtime's registry.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct NetworkSubscription {
    world: WorldId,
    server: SubscriptionId,
}

/// The report of one [`NetworkGateway::process_inbound`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ProcessReport {
    /// Frames pulled from the transports.
    pub frames_received: u64,
    /// Frames decoded and dispatched.
    pub dispatched: u64,
    /// Frames rejected (protocol violations).
    pub rejected: u64,
    /// Connections dropped during the pass.
    pub disconnected: usize,
}

/// The report of one [`NetworkGateway::step_worlds`] pass.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct StepReport {
    /// Worlds that committed a tick.
    pub worlds: u64,
    /// `TickUpdate` broadcasts enqueued to attached sessions.
    pub tick_updates_sent: u64,
    /// Subscription messages serialized (snapshots + deltas).
    pub subscription_messages_sent: u64,
    /// Messages dropped by the stale/overflow policy.
    pub messages_dropped: u64,
}

/// The realtime gateway: owns the runtime and adapts it to clients.
pub struct NetworkGateway {
    runtime: Runtime,
    config: NetworkConfig,
    authenticator: Arc<dyn Authenticator>,
    server_name: String,
    connections: BTreeMap<ConnectionId, ConnectionEntry>,
    next_connection: u64,
    next_session: u64,
    /// Pending reducer calls awaiting their world's next tick (ADR-013 D3):
    /// `(world, request_id) -> connection`. A call is removed when its
    /// `ReducerResult` is routed, or cleared on detach/disconnect/world
    /// failure (the caller then receives an error, never a hang).
    pending_calls: BTreeMap<(WorldId, u64), ConnectionId>,
    /// Pending `Subscribe` request ids awaiting their subscription's
    /// `Initial` snapshot: `(connection, subscription) -> request_id`
    /// (ADR-013). Entries live for one `pump_subscription` call.
    snapshot_requests: BTreeMap<(ConnectionId, SubscriptionId), u64>,
    events: VecDeque<NetworkEvent>,
    metrics: NetworkMetrics,
}

impl NetworkGateway {
    /// Creates a gateway around `runtime` with `config` bounds and the
    /// `authenticator` hook. Returns [`NetworkError::Core`] for an invalid
    /// configuration.
    pub fn new(
        runtime: Runtime,
        config: NetworkConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> Result<Self, NetworkError> {
        config.validate().map_err(NetworkError::Core)?;
        Ok(Self {
            runtime,
            config,
            authenticator,
            server_name: "nexum".to_string(),
            connections: BTreeMap::new(),
            next_connection: 0,
            next_session: 0,
            pending_calls: BTreeMap::new(),
            snapshot_requests: BTreeMap::new(),
            events: VecDeque::new(),
            metrics: NetworkMetrics::empty(),
        })
    }

    /// Returns the configuration.
    pub fn config(&self) -> &NetworkConfig {
        &self.config
    }

    /// Returns the server name announced in the handshake.
    pub fn server_name(&self) -> &str {
        &self.server_name
    }

    /// Sets the server name announced in the handshake.
    pub fn set_server_name(&mut self, name: impl Into<String>) {
        self.server_name = name.into();
    }

    /// Returns the owned runtime (shared access).
    pub fn runtime(&self) -> &Runtime {
        &self.runtime
    }

    /// Returns the owned runtime (mutable access for the control plane).
    pub fn runtime_mut(&mut self) -> &mut Runtime {
        &mut self.runtime
    }

    /// Returns a typed control-plane handle over the runtime (ADR-011 D7).
    pub fn control(&mut self) -> ControlPlane<'_> {
        ControlPlane::new(&mut self.runtime)
    }

    // --------------------------------------------------------- connections

    /// Registers a transport connection (bounded by `max_connections`).
    /// Returns its connection id.
    pub fn register_connection(
        &mut self,
        connection: Box<dyn Connection>,
    ) -> Result<ConnectionId, NetworkError> {
        if self.connections.len() >= self.config.max_connections() {
            return Err(NetworkError::ConnectionLimit);
        }
        let id = ConnectionId::from_u64(self.next_connection);
        self.next_connection += 1;
        self.connections.insert(id, ConnectionEntry::new(connection));
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            NetworkEvent::ConnectionOpened { connection: id },
        );
        Ok(id)
    }

    /// Closes a connection, best-effort-delivering a `Disconnect` reason.
    pub fn disconnect(&mut self, connection: ConnectionId, reason: &str) -> Result<(), NetworkError> {
        if let Some(entry) = self.connections.get_mut(&connection) {
            if let Ok(frame) = protocol::encode_server(
                &ServerMessage::Disconnect {
                    reason: reason.to_string(),
                },
                self.config.max_frame_payload(),
            ) {
                let _ = entry.connection.try_send_frame(frame);
            }
            let _ = entry.connection.flush_outbound();
        }
        self.drop_connection(&connection, reason);
        Ok(())
    }

    /// Returns the number of registered connections.
    pub fn connection_count(&self) -> usize {
        self.connections.len()
    }

    /// Returns a connection's peer label.
    pub fn connection_peer(&self, connection: ConnectionId) -> Result<&str, NetworkError> {
        self.connections
            .get(&connection)
            .map(|entry| entry.connection.peer())
            .ok_or(NetworkError::UnknownConnection(connection))
    }

    /// Returns a connection's session, if authenticated.
    pub fn session_of(&self, connection: ConnectionId) -> Option<&Session> {
        self.connections
            .get(&connection)
            .and_then(|entry| entry.session.as_ref())
    }

    // ------------------------------------------------------------ inbound

    /// Drains every connection's inbound frames, decodes and dispatches
    /// them, then flushes outbound to the transports. Never blocks; a
    /// protocol violation closes the offending connection.
    #[allow(clippy::while_let_loop)] // per-frame `self.dispatch` needs `&mut self` while the pull borrows `self.connections`
    pub fn process_inbound(&mut self) -> ProcessReport {
        let mut report = ProcessReport::default();
        let ids: Vec<ConnectionId> = self.connections.keys().copied().collect();
        for connection in ids {
            loop {
                let frame = match self.connections.get_mut(&connection) {
                    Some(entry) => match entry.connection.try_recv_frame() {
                        Ok(Some(frame)) => frame,
                        Ok(None) => break,
                        Err(_) => {
                            self.drop_connection(&connection, "transport closed");
                            report.disconnected += 1;
                            break;
                        }
                    },
                    None => break,
                };
                report.frames_received += 1;
                self.metrics.frames_received += 1;
                match protocol::decode_client(&frame, self.config.max_frame_payload()) {
                    Ok(message) => {
                        report.dispatched += 1;
                        self.dispatch(connection, message);
                    }
                    Err(protocol_error) => {
                        report.rejected += 1;
                        self.metrics.frames_rejected += 1;
                        self.metrics.protocol_errors += 1;
                        Self::push_event(
                            &mut self.events,
                            self.config.event_log_limit(),
                            NetworkEvent::ProtocolError { connection },
                        );
                        let code = protocol_error.code();
                        let detail = protocol_error.to_string();
                        let _ = self.send_error(connection, code, &detail);
                        self.drop_connection(&connection, &format!("protocol violation: {detail}"));
                        report.disconnected += 1;
                        break;
                    }
                }
            }
        }
        let _ = self.flush_outbound();
        report
    }

    /// Dispatches one decoded client message.
    fn dispatch(&mut self, connection: ConnectionId, message: ClientMessage) {
        match message {
            ClientMessage::Handshake { version, .. } => {
                if version != PROTOCOL_VERSION {
                    let _ = self.send_error(connection, 2, "unsupported protocol version");
                    let _ = self.disconnect(connection, "unsupported protocol version");
                    return;
                }
                let _ = self.send(
                    connection,
                    &ServerMessage::HandshakeResponse {
                        version: PROTOCOL_VERSION,
                        server_name: self.server_name.clone(),
                    },
                );
            }
            ClientMessage::Authenticate { credentials } => {
                if self
                    .connections
                    .get(&connection)
                    .is_some_and(|entry| entry.session.is_some())
                {
                    let _ = self.send_error(connection, 20, "already authenticated");
                    return;
                }
                match self.authenticator.authenticate(&credentials) {
                    Ok(principal) => {
                        let session = Session::new(
                            SessionId::from_u64(self.next_session),
                            connection,
                            principal.clone(),
                        );
                        self.next_session += 1;
                        if let Some(entry) = self.connections.get_mut(&connection) {
                            entry.session = Some(session);
                        }
                        Self::push_event(
                            &mut self.events,
                            self.config.event_log_limit(),
                            NetworkEvent::Authenticated {
                                connection,
                                principal_id: principal.id(),
                            },
                        );
                        let _ = self.send(
                            connection,
                            &ServerMessage::AuthResult {
                                ok: true,
                                principal: Some(principal),
                                error: None,
                            },
                        );
                    }
                    Err(error) => {
                        self.metrics.auth_failures += 1;
                        Self::push_event(
                            &mut self.events,
                            self.config.event_log_limit(),
                            NetworkEvent::AuthFailed { connection },
                        );
                        let _ = self.send(
                            connection,
                            &ServerMessage::AuthResult {
                                ok: false,
                                principal: None,
                                error: Some(error.to_string()),
                            },
                        );
                    }
                }
            }
            ClientMessage::AttachWorld { world } => {
                let Some(session) = self
                    .connections
                    .get_mut(&connection)
                    .and_then(|entry| entry.session.as_mut())
                else {
                    let _ = self.send_error(connection, 20, "authentication required");
                    return;
                };
                if let Some(current) = session.attached_world() {
                    if current == world {
                        let _ = self.send(
                            connection,
                            &ServerMessage::AttachResult {
                                ok: true,
                                world: Some(world),
                                error: None,
                            },
                        );
                    } else {
                        let _ = self.send_error(
                            connection,
                            21,
                            "session is already attached to a different world",
                        );
                    }
                    return;
                }
                let world_exists = self.runtime.world_status(world).is_ok();
                if !world_exists {
                    let _ = self.send(
                        connection,
                        &ServerMessage::AttachResult {
                            ok: false,
                            world: None,
                            error: Some(format!("unknown world {world}")),
                        },
                    );
                    return;
                }
                session.attach(world);
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    NetworkEvent::Attached { connection, world },
                );
                let _ = self.send(
                    connection,
                    &ServerMessage::AttachResult {
                        ok: true,
                        world: Some(world),
                        error: None,
                    },
                );
            }
            ClientMessage::InputFrame { frame } => self.handle_input(connection, frame),
            ClientMessage::Subscribe { request_id, query } => {
                self.handle_subscribe(connection, request_id, query)
            }
            ClientMessage::Unsubscribe { subscription } => {
                let removed = self
                    .connections
                    .get_mut(&connection)
                    .and_then(|entry| entry.subscriptions.remove(&subscription));
                match removed {
                    Some(net_sub) => {
                        let _ = self.runtime.unsubscribe(net_sub.world, net_sub.server);
                    }
                    None => {
                        let _ = self.send_error(connection, 22, "unknown subscription");
                    }
                }
            }
            ClientMessage::Resync { subscription } => {
                let Some(net_sub) = self
                    .connections
                    .get_mut(&connection)
                    .and_then(|entry| entry.subscriptions.get(&subscription).cloned())
                else {
                    let _ = self.send_error(connection, 22, "unknown subscription");
                    return;
                };
                if let Some(entry) = self.connections.get_mut(&connection) {
                    entry.stale = false;
                    entry.pending_stale.clear();
                }
                match self.runtime.resync(net_sub.world, net_sub.server) {
                    Ok(()) => self.pump_subscription(connection, net_sub.world, net_sub.server),
                    Err(error) => {
                        let _ = self.send_error(
                            connection,
                            runtime_error_code(&error),
                            &runtime_error_message(&error),
                        );
                    }
                }
            }
            ClientMessage::DetachWorld => {
                let Some(session) = self
                    .connections
                    .get_mut(&connection)
                    .and_then(|entry| entry.session.as_mut())
                else {
                    let _ = self.send_error(connection, 20, "authentication required");
                    return;
                };
                if !session.is_attached() {
                    let _ = self.send_error(connection, 21, "session is not attached to a world");
                    return;
                }
                session.detach();
                // End every session subscription on the runtime registry.
                let subs: Vec<(WorldId, SubscriptionId)> = self
                    .connections
                    .get_mut(&connection)
                    .expect("session exists")
                    .subscriptions
                    .values()
                    .map(|sub| (sub.world, sub.server))
                    .collect();
                self.connections
                    .get_mut(&connection)
                    .expect("session exists")
                    .subscriptions
                    .clear();
                for (world, subscription) in subs {
                    let _ = self.runtime.unsubscribe(world, subscription);
                }
                // Pending reducer calls die with the attachment.
                self.pending_calls.retain(|_, conn| *conn != connection);
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    NetworkEvent::Detached { connection },
                );
                let _ = self.send(connection, &ServerMessage::DetachResult { ok: true, error: None });
            }
            ClientMessage::CallReducer {
                request_id,
                reducer,
                args,
            } => self.handle_call_reducer(connection, request_id, reducer, args),
            ClientMessage::Ping { nonce } => {
                let _ = self.send(connection, &ServerMessage::Pong { nonce });
            }
        }
    }

    /// Routes a client reducer call to the session's attached world
    /// (ADR-013 D3). The gateway validates the session, the attachment, the
    /// reducer name and argument bounds, and the per-connection pending cap;
    /// a duplicate pending request id is rejected (a client reusing a request
    /// id while an earlier call of the same world is still pending would make
    /// correlation ambiguous). The call then executes inside the world's
    /// next tick; its `ReducerResult` is routed back to this connection.
    fn handle_call_reducer(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        reducer: String,
        args: ReducerArgs,
    ) {
        let Some(session) = self
            .connections
            .get(&connection)
            .and_then(|entry| entry.session.as_ref())
            .cloned()
        else {
            let _ = self.send_reducer_error(connection, request_id, "authentication required");
            self.metrics.reducer_calls_rejected += 1;
            return;
        };
        let Some(world) = session.attached_world() else {
            let _ = self.send_reducer_error(connection, request_id, "attach to a world before calling a reducer");
            self.metrics.reducer_calls_rejected += 1;
            return;
        };
        if reducer.len() > self.config.max_reducer_name_len() {
            let _ = self.send_reducer_error(connection, request_id, "reducer name too long");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        if args.len() > self.config.max_reducer_args() {
            let _ = self.send_reducer_error(connection, request_id, "too many reducer arguments");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        let pending_for_connection = self
            .pending_calls
            .values()
            .filter(|conn| **conn == connection)
            .count();
        if pending_for_connection >= self.config.max_pending_calls_per_connection() {
            let _ = self.send_reducer_error(connection, request_id, "too many pending reducer calls");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        if self.pending_calls.contains_key(&(world, request_id)) {
            let _ = self.send_reducer_error(connection, request_id, "request id already pending");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        match self.runtime.submit_reducer_call(world, request_id, reducer, args) {
            Ok(()) => {
                self.pending_calls.insert((world, request_id), connection);
                self.metrics.reducer_calls_accepted += 1;
            }
            Err(error) => {
                self.metrics.reducer_calls_rejected += 1;
                let _ = self.send_reducer_error(
                    connection,
                    request_id,
                    &runtime_error_message(&error),
                );
            }
        }
    }

    /// Sends a correlated failure for one `CallReducer`: every reducer call
    /// receives exactly one `ReducerResult` (success or failure) so the
    /// client's request correlation never hangs or misattributes a generic
    /// `Error` (ADR-013 D3).
    fn send_reducer_error(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        message: &str,
    ) -> Result<bool, NetworkError> {
        self.send(
            connection,
            &ServerMessage::ReducerResult {
                request_id,
                ok: false,
                value: None,
                error: Some(message.to_string()),
            },
        )
    }

    /// Routes one input frame to the session's attached world. The gateway
    /// stamps every command's source with the authenticated principal id
    /// (anti-spoofing) and hands the frame to the runtime, which owns
    /// late/capacity/world rejection.
    fn handle_input(&mut self, connection: ConnectionId, frame: InputFrame) {
        let Some(session) = self
            .connections
            .get(&connection)
            .and_then(|entry| entry.session.as_ref())
            .cloned()
        else {
            let _ = self.send_error(connection, 20, "authentication required");
            self.metrics.inputs_rejected += 1;
            return;
        };
        let Some(world) = session.attached_world() else {
            let _ = self.send_error(connection, 21, "attach to a world before submitting input");
            self.metrics.inputs_rejected += 1;
            return;
        };
        if frame.commands().len() > self.config.max_commands_per_frame() {
            let _ = self.send_error(connection, 17, "too many commands in frame");
            self.metrics.inputs_rejected += 1;
            return;
        }
        let principal_id = session.principal().id();
        let mut stamped = InputFrame::with_capacity(frame.tick(), frame.commands().len());
        for command in frame.commands() {
            let command =
                match InputCommand::new(principal_id, command.kind(), command.payload().cloned()) {
                    Ok(command) => command,
                    Err(error) => {
                        let _ = self.send_error(connection, error_code(&error), &error.to_string());
                        self.metrics.inputs_rejected += 1;
                        return;
                    }
                };
            stamped.push(command);
        }
    match self.runtime.submit_input(world, stamped) {
        Ok(()) => self.metrics.inputs_accepted += 1,
        Err(error) => {
            self.metrics.inputs_rejected += 1;
            let _ = self.send_error(
                connection,
                runtime_error_code(&error),
                &runtime_error_message(&error),
            );
        }
    }
}

    /// Establishes a session subscription on the attached world and
    /// delivers its Initial snapshot, echoing the client's `request_id` on
    /// both the snapshot and any rejection so the SDK's correlation is
    /// unambiguous (ADR-013).
    fn handle_subscribe(
        &mut self,
        connection: ConnectionId,
        request_id: u64,
        query: nexum_subscription::Query,
    ) {
        let Some(session) = self
            .connections
            .get(&connection)
            .and_then(|entry| entry.session.as_ref())
            .cloned()
        else {
            let _ = self.send_error_for(connection, 20, "authentication required", request_id);
            return;
        };
        let Some(world) = session.attached_world() else {
            let _ = self.send_error_for(connection, 21, "attach to a world before subscribing", request_id);
            return;
        };
        if self.connections.get(&connection).is_some_and(|entry| {
            entry.subscriptions.len() >= self.config.max_subscriptions_per_session()
        }) {
            let _ = self.send_error_for(connection, 17, "subscription limit reached", request_id);
            return;
        }
        match self.runtime.subscribe(world, query) {
            Ok(server_sub) => {
                if let Some(entry) = self.connections.get_mut(&connection) {
                    entry.subscriptions.insert(
                        server_sub,
                        NetworkSubscription { world, server: server_sub },
                    );
                }
                // The next `Initial` snapshot of this subscription carries
                // the client's request id (removed after the pump).
                self.snapshot_requests
                    .insert((connection, server_sub), request_id);
                self.pump_subscription(connection, world, server_sub);
                self.snapshot_requests.remove(&(connection, server_sub));
            }
            Err(error) => {
                let _ = self.send_error_for(
                    connection,
                    runtime_error_code(&error),
                    &runtime_error_message(&error),
                    request_id,
                );
            }
        }
    }

    /// Drains one subscription's pending updates and serializes them to the
    /// session's connection. A pending `Subscribe` request id is attached to
    /// the `Initial` snapshot (0 for resyncs and later deltas).
    fn pump_subscription(
        &mut self,
        connection: ConnectionId,
        world: WorldId,
        subscription: SubscriptionId,
    ) {
        let updates = match self.runtime.drain(world, subscription) {
            Ok(updates) => updates,
            Err(_) => return,
        };
        let request_id = self
            .snapshot_requests
            .get(&(connection, subscription))
            .copied()
            .unwrap_or(0);
        for update in updates {
            if let Some(message) = serialize_update(subscription, request_id, &update)
                && self.send(connection, &message).unwrap_or(false)
            {
                self.metrics.subscription_messages_sent += 1;
            }
        }
    }

    // ------------------------------------------------------------- stepping

    /// Advances every running world by one tick (the runtime schedules and
    /// commits — ADR-011 D1) and fans the results out: per successful
    /// world, a `TickUpdate` (changes + emitted events) to every attached
    /// session, then every network subscription on that world is drained
    /// and serialized, then each committed reducer-call result is routed to
    /// its requesting connection (ADR-013 D3). Pending calls of worlds that
    /// failed during this step are answered with the tick error so callers
    /// never hang.
    pub fn step_worlds(&mut self) -> Result<StepReport, NetworkError> {
        let results = self.runtime.step_detailed()?;
        let mut report = StepReport::default();
        for (world, result) in results {
            report.worlds += 1;
            let message = ServerMessage::TickUpdate {
                world,
                tick: result.tick(),
                tx_id: result.tx_id(),
                changes: result.changes().to_vec(),
                events: result.events().to_vec(),
            };
            let attached: Vec<ConnectionId> = self
                .connections
                .iter()
                .filter(|(_, entry)| {
                    entry
                        .session
                        .as_ref()
                        .is_some_and(|session| session.attached_world() == Some(world))
                })
                .map(|(id, _)| *id)
                .collect();
            for connection in attached {
                if self.send(connection, &message).unwrap_or(false) {
                    report.tick_updates_sent += 1;
                    self.metrics.tick_updates_sent += 1;
                } else {
                    report.messages_dropped += 1;
                }
            }

            // Deliver subscription deltas for this world (deterministic:
            // connections ascending, then subscriptions ascending).
            let subscribers: Vec<(ConnectionId, SubscriptionId)> = self
                .connections
                .iter()
                .flat_map(|(id, entry)| {
                    entry
                        .subscriptions
                        .iter()
                        .filter(|(_, sub)| sub.world == world)
                        .map(move |(sub_id, _)| (*id, *sub_id))
                })
                .collect();
            for (connection, subscription) in subscribers {
                let before = self.metrics.subscription_messages_sent;
                self.pump_subscription(connection, world, subscription);
                report.subscription_messages_sent +=
                    self.metrics.subscription_messages_sent - before;
            }

            // Route committed reducer-call results (ADR-013 D3) to their
            // requesting connections, in call order.
            for call_result in result.reducer_results() {
                if let Some(connection) =
                    self.pending_calls.remove(&(world, call_result.request_id()))
                {
                    let message = if call_result.is_ok() {
                        ServerMessage::ReducerResult {
                            request_id: call_result.request_id(),
                            ok: true,
                            value: call_result.value().cloned(),
                            error: None,
                        }
                    } else {
                        ServerMessage::ReducerResult {
                            request_id: call_result.request_id(),
                            ok: false,
                            value: None,
                            error: Some(
                                call_result
                                    .error()
                                    .map(ToString::to_string)
                                    .unwrap_or_else(|| "reducer call failed".to_string()),
                            ),
                        }
                    };
                    if self.send(connection, &message).unwrap_or(false) {
                        self.metrics.reducer_results_sent += 1;
                    }
                }
            }
        }

        // Answer pending calls whose world can no longer produce a result —
        // it failed, was stopped, or was destroyed during this step — with a
        // correlated failure, so no caller is left hanging (ADR-013 D3).
        // A still-Running world's calls stay pending for its next tick.
        let unresolved: Vec<(WorldId, u64, ConnectionId)> = self
            .pending_calls
            .iter()
            .filter_map(|((world, request_id), connection)| {
                let unresolved = match self.runtime.world_status(*world) {
                    Ok(status) => status.state != nexum_runtime::WorldLifecycle::Running,
                    // Destroyed: the world no longer exists.
                    Err(_) => true,
                };
                unresolved.then_some((*world, *request_id, *connection))
            })
            .collect();
        for (world, request_id, connection) in unresolved {
            self.pending_calls.remove(&(world, request_id));
            let message = ServerMessage::ReducerResult {
                request_id,
                ok: false,
                value: None,
                error: Some(format!(
                    "world {world} is no longer running; the call could not commit"
                )),
            };
            if self.send(connection, &message).unwrap_or(false) {
                self.metrics.reducer_results_sent += 1;
            }
        }
        let _ = self.flush_outbound();
        Ok(report)
    }

    /// Drains every network subscription (used after subscribe/resync when
    /// no tick ran). Returns the number of messages serialized.
    pub fn pump_subscriptions(&mut self) -> u64 {
        let subs: Vec<(ConnectionId, WorldId, SubscriptionId)> = self
            .connections
            .iter()
            .flat_map(|(id, entry)| {
                entry
                    .subscriptions
                    .iter()
                    .map(move |(sub_id, sub)| (*id, sub.world, *sub_id))
            })
            .collect();
        let mut sent = 0;
        for (connection, world, subscription) in subs {
            let before = self.metrics.subscription_messages_sent;
            self.pump_subscription(connection, world, subscription);
            sent += self.metrics.subscription_messages_sent - before;
        }
        let _ = self.flush_outbound();
        sent
    }

    // ------------------------------------------------------------ outbound

    /// Sends a message to a connection. Returns `Ok(true)` when the frame
    /// was enqueued, `Ok(false)` when it was dropped by the stale/overflow
    /// policy, and `Err` for unknown connections or transport failures.
    ///
    /// Pending `StaleNotification`s (queued when the outbound queue was
    /// full) are flushed first whenever the queue has room.
    fn send(
        &mut self,
        connection: ConnectionId,
        message: &ServerMessage,
    ) -> Result<bool, NetworkError> {
        let frame = protocol::encode_server(message, self.config.max_frame_payload())?;
        let entry = self
            .connections
            .get_mut(&connection)
            .ok_or(NetworkError::UnknownConnection(connection))?;

        // Flush any pending stale notifications.
        while let Some(pending) = entry.pending_stale.front().cloned() {
            match entry.connection.try_send_frame(pending) {
                Ok(()) => {
                    entry.pending_stale.pop_front();
                    self.metrics.messages_outbound += 1;
                }
                Err(TransportError::Full) | Err(TransportError::Closed) => break,
                Err(_) => break,
            }
        }

        if entry.stale && !is_stale_signal(message) {
            self.metrics.messages_dropped += 1;
            return Ok(false);
        }

        match entry.connection.try_send_frame(frame) {
            Ok(()) => {
                self.metrics.messages_outbound += 1;
                Ok(true)
            }
            Err(TransportError::Full) => {
                self.metrics.messages_dropped += 1;
                match self.config.overflow_policy() {
                    OutboundOverflowPolicy::Stale => {
                        if !entry.stale {
                            entry.stale = true;
                            for sub in entry.subscriptions.keys() {
                                if let Ok(notify) = protocol::encode_server(
                                    &ServerMessage::StaleNotification {
                                        subscription: *sub,
                                        seq: 0,
                                    },
                                    self.config.max_frame_payload(),
                                ) {
                                    entry.pending_stale.push_back(notify);
                                }
                            }
                            Self::push_event(
                                &mut self.events,
                                self.config.event_log_limit(),
                                NetworkEvent::SessionStale { connection },
                            );
                        }
                        Ok(false)
                    }
                    OutboundOverflowPolicy::Disconnect => {
                        let reason = "slow consumer (outbound queue full)".to_string();
                        let _ = self.disconnect(connection, &reason);
                        Ok(false)
                    }
                }
            }
            Err(TransportError::Closed) | Err(TransportError::Io) => {
                self.drop_connection(&connection, "transport failure");
                Ok(false)
            }
        }
    }

    /// Sends an `Error` message (passes through the stale check — errors
    /// are stale signals) with no request correlation.
    fn send_error(
        &mut self,
        connection: ConnectionId,
        code: u16,
        message: &str,
    ) -> Result<bool, NetworkError> {
        self.send_error_for(connection, code, message, 0)
    }

    /// Sends an `Error` message correlated to a request by `request_id`
    /// (a rejected `Subscribe`; 0 = uncorrelated).
    fn send_error_for(
        &mut self,
        connection: ConnectionId,
        code: u16,
        message: &str,
        request_id: u64,
    ) -> Result<bool, NetworkError> {
        self.send(
            connection,
            &ServerMessage::Error {
                code,
                message: message.to_string(),
                request_id,
            },
        )
    }

    /// Flushes buffered outbound bytes to every transport.
    pub fn flush_outbound(&mut self) -> Result<(), NetworkError> {
        let ids: Vec<ConnectionId> = self.connections.keys().copied().collect();
        for connection in ids {
            let broken = match self.connections.get_mut(&connection) {
                Some(entry) => entry.connection.flush_outbound().is_err(),
                None => false,
            };
            if broken {
                self.drop_connection(&connection, "transport write failure");
            }
        }
        Ok(())
    }

    // ------------------------------------------------------------- helpers

    /// Removes a connection: closes its transport, unsubscribes its
    /// session's subscriptions from the runtime registries, and records the
    /// drop.
    fn drop_connection(&mut self, connection: &ConnectionId, reason: &str) {
        let Some(mut entry) = self.connections.remove(connection) else {
            return;
        };
        // Best-effort cleanup of the session's runtime subscriptions.
        let subs: Vec<(WorldId, SubscriptionId)> = entry
            .subscriptions
            .values()
            .map(|sub| (sub.world, sub.server))
            .collect();
        for (world, sub) in subs {
            let _ = self.runtime.unsubscribe(world, sub);
        }
        // Drop the connection's pending reducer calls (ADR-013 D3): the
        // results can no longer be delivered. The runtime may still execute
        // accepted calls (fire-and-forget); the correlation state is gone.
        self.pending_calls.retain(|_, conn| *conn != *connection);
        entry.connection.close();
        self.metrics.clients_dropped += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            NetworkEvent::ConnectionClosed {
                connection: *connection,
                reason: reason.to_string(),
            },
        );
    }

    fn push_event(events: &mut VecDeque<NetworkEvent>, limit: usize, event: NetworkEvent) {
        if events.len() >= limit {
            events.pop_front();
        }
        events.push_back(event);
    }

    // ---------------------------------------------------- events & metrics

    /// Takes every buffered event in order, clearing the log.
    pub fn drain_events(&mut self) -> Vec<NetworkEvent> {
        self.events.drain(..).collect()
    }

    /// Returns the number of buffered events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Returns a point-in-time metrics snapshot (current-state fields are
    /// derived from live connections; counters are monotonic).
    pub fn metrics(&self) -> NetworkMetrics {
        let mut metrics = self.metrics.clone();
        metrics.connections = self.connections.len();
        metrics.sessions = self
            .connections
            .values()
            .filter(|entry| entry.session.is_some())
            .count();
        metrics.attached = self
            .connections
            .values()
            .filter(|entry| entry.session.as_ref().is_some_and(Session::is_attached))
            .count();
        metrics.connections_per_world.clear();
        for entry in self.connections.values() {
            if let Some(world) = entry.session.as_ref().and_then(Session::attached_world) {
                *metrics.connections_per_world.entry(world).or_insert(0) += 1;
            }
        }
        metrics.subscriptions = self
            .connections
            .values()
            .map(|entry| entry.subscriptions.len())
            .sum();
        metrics.sessions_stale = self
            .connections
            .values()
            .filter(|entry| entry.stale)
            .count();
        metrics
    }
}

impl std::fmt::Debug for NetworkGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkGateway")
            .field("connections", &self.connections.len())
            .field("server_name", &self.server_name)
            .finish()
    }
}

/// Whether a message must pass through even while the session is stale.
fn is_stale_signal(message: &ServerMessage) -> bool {
    matches!(
        message,
        ServerMessage::StaleNotification { .. }
            | ServerMessage::Disconnect { .. }
            | ServerMessage::Error { .. }
    )
}

/// Serializes one `SubscriptionUpdate` into a protocol message. `request_id`
/// echoes the originating `Subscribe` on `Initial` snapshots (0 otherwise).
fn serialize_update(
    subscription: SubscriptionId,
    request_id: u64,
    update: &SubscriptionUpdate,
) -> Option<ServerMessage> {
    match update {
        SubscriptionUpdate::Initial { seq, rows } => Some(ServerMessage::SubscriptionSnapshot {
            request_id,
            subscription,
            seq: *seq,
            rows: rows.clone(),
        }),
        SubscriptionUpdate::Resync { seq, rows } => Some(ServerMessage::SubscriptionSnapshot {
            request_id: 0,
            subscription,
            seq: *seq,
            rows: rows.clone(),
        }),
        SubscriptionUpdate::Insert { seq, row } => Some(ServerMessage::SubscriptionDelta {
            subscription,
            seq: *seq,
            kind: DeltaKind::Insert,
            row_id: row.row_id(),
            row: Some(row.clone()),
        }),
        SubscriptionUpdate::Update { seq, row } => Some(ServerMessage::SubscriptionDelta {
            subscription,
            seq: *seq,
            kind: DeltaKind::Update,
            row_id: row.row_id(),
            row: Some(row.clone()),
        }),
        SubscriptionUpdate::Delete { seq, row_id } => Some(ServerMessage::SubscriptionDelta {
            subscription,
            seq: *seq,
            kind: DeltaKind::Delete,
            row_id: *row_id,
            row: None,
        }),
        SubscriptionUpdate::Stale { seq } => Some(ServerMessage::StaleNotification {
            subscription,
            seq: *seq,
        }),
    }
}

/// Maps a core `Error` to a stable wire error code. (`Error` is
/// `#[non_exhaustive]`, so unknown future variants map to a generic code.)
pub(crate) fn error_code(error: &Error) -> u16 {
    match error {
        Error::NotFound(_) => 10,
        Error::AlreadyExists(_) => 11,
        Error::InvalidArgument(_) => 12,
        Error::Conflict(_) => 13,
        Error::AlreadyCommitted(_) => 14,
        Error::AlreadyAborted(_) => 15,
        Error::InvalidTransaction(_) => 16,
        Error::Capacity(_) => 17,
        Error::Internal(_) => 18,
        Error::Unsupported(_) => 19,
        _ => 30,
    }
}

/// Maps a `RuntimeError` to a stable wire error code (its core error if
/// carried, otherwise a runtime-specific code).
pub(crate) fn runtime_error_code(error: &RuntimeError) -> u16 {
    error.core_error().map(error_code).unwrap_or(30)
}

/// The wire message for a `RuntimeError`.
pub(crate) fn runtime_error_message(error: &RuntimeError) -> String {
    error.to_string()
}
