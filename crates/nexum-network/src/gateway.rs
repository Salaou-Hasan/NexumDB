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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
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
use crate::policy::{AllowAllPolicy, GamePolicy};
use crate::protocol::{self, ClientMessage, DeltaKind, PROTOCOL_VERSION, ServerMessage};
use crate::rate::{RateBucket, RateLimitConfig, RateLimiter};
use crate::session::Session;
use crate::transport::{Connection, TransportError};

/// The top bit of a reducer-call request id is reserved for
/// server-originated calls (ADR-014 D3). Client-supplied request ids with
/// this bit set are rejected, so `GameServer::invoke_reducer` can never
/// collide with a client's pending call on the same world.
pub const SERVER_REQUEST_MSB: u64 = 1 << 63;

/// The reserved reducer-argument key under which the gateway stamps the
/// authenticated caller's principal id on every client reducer call
/// (ADR-013 D3 / ADR-014 D8).
///
/// Reducers that act on behalf of a player must read the caller from this
/// key and must **never** trust a client-supplied identity argument: the
/// gateway overwrites any client value before the call is queued, so
/// identity cannot be forged. Server-originated calls (the game server's
/// own invocations) stamp the same key with the player id they act for.
pub const CALLER_SOURCE_ARG: &str = "__caller";

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
    /// Fast-path flag: true when `pending_stale` is non-empty.
    /// Avoids VecDeque::front() call on every send (Phase 23-25).
    has_pending_stale: bool,
    /// Per-connection operational rate limits (ADR-016 D1).
    rate: RateLimiter,
}

impl ConnectionEntry {
    fn new(connection: Box<dyn Connection>, rate_limits: &RateLimitConfig) -> Self {
        Self {
            connection,
            session: None,
            subscriptions: BTreeMap::new(),
            stale: false,
            pending_stale: VecDeque::new(),
            has_pending_stale: false,
            rate: RateLimiter::new(rate_limits),
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

/// A pending client reducer call, keyed by a gateway-allocated request id
/// (Phase 16: client request ids are only unique per connection, so the
/// gateway must namespace them to keep correlation unambiguous on shared
/// worlds).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PendingCall {
    /// The connection awaiting the result.
    connection: ConnectionId,
    /// The client's original request id (echoed back on the result).
    client_request_id: u64,
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
    /// Authorization hook (ADR-014 D2). Defaults to [`AllowAllPolicy`]
    /// (Phase 13 semantics); a host may install a game-aware policy.
    policy: Box<dyn GamePolicy>,
    server_name: String,
    connections: Vec<Option<ConnectionEntry>>,
    /// Monotonic connection id counter.
    next_connection: u64,
    /// Number of live (non-None) slots in `connections`.
    active_connections: usize,
    next_session: u64,
    /// Pending reducer calls awaiting their world's next tick (ADR-013 D3),
    /// keyed by a **gateway-allocated** request id: `(world, gateway_id) ->
    /// pending`. The gateway never trusts client request ids for correlation
    /// — two clients may (and the SDK always does) start their ids at 1, so
    /// a `(world, client_request_id)` key would collide across connections
    /// (Phase 16 finding). The runtime echoes the gateway id; the gateway
    /// translates it back to the requesting connection and the client's
    /// original id when routing the result. Entries are removed when their
    /// `ReducerResult` is routed, or cleared on detach/disconnect/world
    /// failure (the caller then receives an error, never a hang).
    pending_calls: BTreeMap<(WorldId, u64), PendingCall>,
    /// Per-connection index of pending client request ids (Phase 18 finding:
    /// the per-call `pending_calls` scans were O(pending) per call, making
    /// inbound reducer routing O(clients²) — e.g. ~64M predicate evaluations
    /// per movement tick at 8K clients). Kept in lockstep with
    /// `pending_calls`; enables O(log n) pending-count and reuse checks.
    pending_by_connection: BTreeMap<ConnectionId, BTreeSet<u64>>,
    /// The next gateway-allocated request id (monotonic per gateway, so
    /// unique across every world and connection).
    next_gateway_request: u64,
    /// Pending `Subscribe` request ids awaiting their subscription's
    /// `Initial` snapshot: `(connection, subscription) -> request_id`
    /// (ADR-013). Entries live for one `pump_subscription` call.
    snapshot_requests: BTreeMap<(ConnectionId, SubscriptionId), u64>,
    /// Attached sessions indexed by world (ADR-021 D3): the fan-out path
    /// iterates a world's attached set directly instead of scanning every
    /// connection per world — O(CCU) per pass instead of O(worlds × CCU).
    /// Maintained on attach / detach / disconnect; never authoritative.
    attached_by_world: BTreeMap<WorldId, BTreeSet<ConnectionId>>,
    /// Pre-computed per-world subscriber list: the fan-out path iterates
    /// this instead of scanning every connection's subscriptions per world.
    /// O(subscribers_with_data) per tick instead of O(CCU).
    world_subscribers: BTreeMap<WorldId, Vec<(ConnectionId, SubscriptionId)>>,
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
            policy: Box::new(AllowAllPolicy),
            server_name: "nexum".to_string(),
            connections: Vec::new(),
            next_connection: 0,
            active_connections: 0,
            next_session: 0,
            pending_calls: BTreeMap::new(),
            pending_by_connection: BTreeMap::new(),
            next_gateway_request: 1,
            snapshot_requests: BTreeMap::new(),
            attached_by_world: BTreeMap::new(),
            world_subscribers: BTreeMap::new(),
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

    /// Installs the authorization policy consulted before executing client
    /// attach / input / reducer operations (ADR-014 D2). Replaces the
    /// default [`AllowAllPolicy`].
    pub fn set_policy(&mut self, policy: Box<dyn GamePolicy>) {
        self.policy = policy;
    }

    // --------------------------------------------------------- connections

    /// O(1) slab access — ConnectionId is a u64 used directly as Vec index.
    fn conn_get(&self, id: ConnectionId) -> Option<&ConnectionEntry> {
        self.connections
            .get(id.as_u64() as usize)
            .and_then(|o| o.as_ref())
    }

    fn conn_get_mut(&mut self, id: ConnectionId) -> Option<&mut ConnectionEntry> {
        self.connections
            .get_mut(id.as_u64() as usize)
            .and_then(|o| o.as_mut())
    }

    /// Registers a transport connection (bounded by `max_connections`).
    /// Returns its connection id.
    pub fn register_connection(
        &mut self,
        connection: Box<dyn Connection>,
    ) -> Result<ConnectionId, NetworkError> {
        if self.active_connections >= self.config.max_connections() {
            return Err(NetworkError::ConnectionLimit);
        }
        let idx = self.next_connection as usize;
        let id = ConnectionId::from_u64(self.next_connection);
        self.next_connection += 1;
        if idx < self.connections.len() {
            self.connections[idx] =
                Some(ConnectionEntry::new(connection, &self.config.rate_limits));
        } else {
            self.connections.push(Some(ConnectionEntry::new(
                connection,
                &self.config.rate_limits,
            )));
        }
        self.active_connections += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            NetworkEvent::ConnectionOpened { connection: id },
        );
        Ok(id)
    }

    /// Closes a connection, best-effort-delivering a `Disconnect` reason.
    pub fn disconnect(
        &mut self,
        connection: ConnectionId,
        reason: &str,
    ) -> Result<(), NetworkError> {
        let max_payload = self.config.max_frame_payload();
        if let Some(entry) = self.conn_get_mut(connection) {
            if let Ok(frame) = protocol::encode_server(
                &ServerMessage::Disconnect {
                    reason: reason.to_string(),
                },
                max_payload,
            ) {
                let _ = entry.connection.try_send_frame(Arc::from(frame));
            }
            let _ = entry.connection.flush_outbound();
        }
        self.drop_connection(&connection, reason);
        Ok(())
    }

    /// Returns the number of registered connections.
    pub fn connection_count(&self) -> usize {
        self.active_connections
    }

    /// Returns a connection's peer label.
    pub fn connection_peer(&self, connection: ConnectionId) -> Result<&str, NetworkError> {
        self.conn_get(connection)
            .map(|entry| entry.connection.peer())
            .ok_or(NetworkError::UnknownConnection(connection))
    }

    /// Returns a connection's session, if authenticated.
    pub fn session_of(&self, connection: ConnectionId) -> Option<&Session> {
        self.conn_get(connection)
            .and_then(|entry| entry.session.as_ref())
    }

    // ------------------------------------------------------------ inbound

    /// Drains every connection's inbound frames, decodes and dispatches
    /// them, then flushes outbound to the transports. Never blocks; a
    /// protocol violation closes the offending connection.
    pub fn process_inbound(&mut self) -> ProcessReport {
        let mut report = ProcessReport::default();
        // Iterate the slab directly — O(1) indexed access, zero allocation.
        let len = self.connections.len();
        for idx in 0..len {
            // Skip empty slots.
            if self.connections[idx].is_none() {
                continue;
            }
            let connection = ConnectionId::from_u64(idx as u64);
            loop {
                // Extract frame in a scoped block so conn_get_mut borrow drops
                // before we call dispatch/send/drop_connection.
                let frame_result = {
                    match self.conn_get_mut(connection) {
                        Some(entry) => entry.connection.try_recv_frame(),
                        None => break,
                    }
                };
                let frame = match frame_result {
                    Ok(Some(frame)) => frame,
                    Ok(None) => break,
                    Err(_) => {
                        self.drop_connection(&connection, "transport closed");
                        report.disconnected += 1;
                        break;
                    }
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
                if !self.check_rate(connection, RateBucket::Auth) {
                    return;
                }
                if self
                    .conn_get(connection)
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
                        if let Some(entry) = self.conn_get_mut(connection) {
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
                // Use immutable borrow to check session state without holding &mut self.
                let (already_attached, attached_to, is_authenticated) = {
                    match self.conn_get(connection) {
                        Some(entry) => (
                            entry.session.as_ref().is_some_and(Session::is_attached),
                            entry.session.as_ref().and_then(Session::attached_world),
                            entry.session.is_some(),
                        ),
                        None => (false, None, false),
                    }
                };
                if !is_authenticated {
                    let _ = self.send_error(connection, 20, "authentication required");
                    return;
                }
                if already_attached {
                    if attached_to == Some(world) {
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
                // Get principal from immutable borrow.
                let principal = self
                    .conn_get(connection)
                    .and_then(|entry| entry.session.as_ref())
                    .map(|s| s.principal().clone());
                let Some(principal) = principal else {
                    let _ = self.send_error(connection, 20, "authentication required");
                    return;
                };
                if !self.policy.authorize_attach(&principal, world) {
                    self.metrics.policy_rejections += 1;
                    let _ = self.send(
                        connection,
                        &ServerMessage::AttachResult {
                            ok: false,
                            world: None,
                            error: Some("not authorized to attach to this world".to_string()),
                        },
                    );
                    return;
                }
                if let Some(session) = self
                    .conn_get_mut(connection)
                    .and_then(|e| e.session.as_mut())
                {
                    session.attach(world);
                }
                self.attached_by_world
                    .entry(world)
                    .or_default()
                    .insert(connection);
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
            ClientMessage::InputFrame { frame } => {
                if !self.check_rate(connection, RateBucket::Input) {
                    return;
                }
                self.handle_input(connection, frame);
            }
            ClientMessage::Subscribe { request_id, query } => {
                self.handle_subscribe(connection, request_id, query)
            }
            ClientMessage::Unsubscribe { subscription } => {
                let removed = self
                    .conn_get_mut(connection)
                    .and_then(|entry| entry.subscriptions.remove(&subscription));
                match removed {
                    Some(net_sub) => {
                        let _ = self.runtime.unsubscribe(net_sub.world, net_sub.server);
                        if let Some(subs) = self.world_subscribers.get_mut(&net_sub.world) {
                            subs.retain(|(c, s)| !(*c == connection && *s == subscription));
                        }
                    }
                    None => {
                        let _ = self.send_error(connection, 22, "unknown subscription");
                    }
                }
            }
            ClientMessage::Resync { subscription } => {
                if !self.check_rate(connection, RateBucket::Resync) {
                    return;
                }
                let Some(net_sub) = self
                    .conn_get_mut(connection)
                    .and_then(|entry| entry.subscriptions.get(&subscription).cloned())
                else {
                    let _ = self.send_error(connection, 22, "unknown subscription");
                    return;
                };
                if let Some(entry) = self.conn_get_mut(connection) {
                    entry.stale = false;
                    entry.pending_stale.clear();
                    entry.has_pending_stale = false;
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
                    .conn_get_mut(connection)
                    .and_then(|entry| entry.session.as_mut())
                else {
                    let _ = self.send_error(connection, 20, "authentication required");
                    return;
                };
                if !session.is_attached() {
                    let _ = self.send_error(connection, 21, "session is not attached to a world");
                    return;
                }
                let detached_world = session.attached_world().expect("attached");
                session.detach();
                if let Some(set) = self.attached_by_world.get_mut(&detached_world) {
                    set.remove(&connection);
                    if set.is_empty() {
                        self.attached_by_world.remove(&detached_world);
                    }
                }
                // End every session subscription on the runtime registry.
                let subs: Vec<(WorldId, SubscriptionId)> = self
                    .conn_get_mut(connection)
                    .expect("session exists")
                    .subscriptions
                    .values()
                    .map(|sub| (sub.world, sub.server))
                    .collect();
                self.conn_get_mut(connection)
                    .expect("session exists")
                    .subscriptions
                    .clear();
                for (world, subscription) in &subs {
                    let _ = self.runtime.unsubscribe(*world, *subscription);
                    if let Some(world_subs) = self.world_subscribers.get_mut(world) {
                        world_subs.retain(|(c, s)| !(*c == connection && *s == *subscription));
                    }
                }
                // Pending reducer calls die with the attachment.
                self.pending_calls
                    .retain(|_, pending| pending.connection != connection);
                self.pending_by_connection.remove(&connection);
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    NetworkEvent::Detached { connection },
                );
                let _ = self.send(
                    connection,
                    &ServerMessage::DetachResult {
                        ok: true,
                        error: None,
                    },
                );
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
        if !self.check_rate(connection, RateBucket::Reducer) {
            return;
        }
        // The top bit of a request id is reserved for server-originated
        // calls (ADR-014 D3): a client claiming it would make correlation
        // ambiguous with `GameServer::invoke_reducer`, so it is rejected.
        if request_id & SERVER_REQUEST_MSB != 0 {
            let _ = self.send_reducer_error(
                connection,
                request_id,
                "request id reserved for server use",
            );
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        // Extract principal_id and world WITHOUT cloning Session — avoids
        // per-frame heap allocation on the hot path (Phase 23-25 optimization).
        let (principal_id, world) = {
            let Some(entry) = self.conn_get(connection) else {
                let _ = self.send_reducer_error(connection, request_id, "authentication required");
                self.metrics.reducer_calls_rejected += 1;
                return;
            };
            let Some(session) = entry.session.as_ref() else {
                let _ = self.send_reducer_error(connection, request_id, "authentication required");
                self.metrics.reducer_calls_rejected += 1;
                return;
            };
            (session.principal().id(), session.attached_world())
        };
        let Some(world) = world else {
            let _ = self.send_reducer_error(
                connection,
                request_id,
                "attach to a world before calling a reducer",
            );
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
        // O(log n) per-connection pending-call bookkeeping (Phase 18
        // finding): the previous `pending_calls.values()` scans were O(pending)
        // per call, i.e. O(clients²) on a movement tick.
        let pending_for_connection = self
            .pending_by_connection
            .get(&connection)
            .map_or(0, BTreeSet::len);
        if pending_for_connection >= self.config.max_pending_calls_per_connection() {
            let _ =
                self.send_reducer_error(connection, request_id, "too many pending reducer calls");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        // A client reusing the same request id while one of its own calls is
        // still pending would be ambiguous *to that client* (it correlates
        // by its own id); reject defensively. Cross-client ids never collide
        // here because correlation uses the gateway-allocated id below.
        let client_id_reused = self
            .pending_by_connection
            .get(&connection)
            .is_some_and(|ids| ids.contains(&request_id));
        if client_id_reused {
            let _ = self.send_reducer_error(connection, request_id, "request id already pending");
            self.metrics.reducer_calls_rejected += 1;
            return;
        }
        // Policy check using a borrow — no clone needed.
        {
            let Some(entry) = self.conn_get(connection) else {
                let _ = self.send_reducer_error(connection, request_id, "authentication required");
                self.metrics.reducer_calls_rejected += 1;
                return;
            };
            let Some(session) = entry.session.as_ref() else {
                let _ = self.send_reducer_error(connection, request_id, "authentication required");
                self.metrics.reducer_calls_rejected += 1;
                return;
            };
            if !self
                .policy
                .authorize_reducer(session.principal(), world, &reducer)
            {
                self.metrics.policy_rejections += 1;
                self.metrics.reducer_calls_rejected += 1;
                let _ =
                    self.send_reducer_error(connection, request_id, "not authorized by game policy");
                return;
            }
        }
        // Stamp the caller's authoritative identity into a reserved argument
        // (ADR-013 D3 / ADR-014 D8): a client-supplied value for the key is
        // overwritten, so identity can never be forged through `args`.
        let args = args.insert(
            CALLER_SOURCE_ARG,
            nexum_core::Value::U64(principal_id),
        );
        // Allocate a gateway-unique request id so concurrent calls from
        // different clients on the same world never collide (Phase 16
        // finding: all SDK clients start their ids at 1).
        let gateway_id = self.next_gateway_request;
        self.next_gateway_request += 1;
        match self
            .runtime
            .submit_reducer_call(world, gateway_id, reducer, args)
        {
            Ok(()) => {
                self.pending_calls.insert(
                    (world, gateway_id),
                    PendingCall {
                        connection,
                        client_request_id: request_id,
                    },
                );
                self.pending_by_connection
                    .entry(connection)
                    .or_default()
                    .insert(request_id);
                self.metrics.reducer_calls_accepted += 1;
            }
            Err(error) => {
                self.metrics.reducer_calls_rejected += 1;
                let _ =
                    self.send_reducer_error(connection, request_id, &runtime_error_message(&error));
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
            .conn_get(connection)
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
        if !self
            .policy
            .authorize_input(session.principal(), world, &stamped)
        {
            self.metrics.policy_rejections += 1;
            self.metrics.inputs_rejected += 1;
            let _ = self.send_error(connection, 18, "not authorized by game policy");
            return;
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
        if !self.check_rate_for(connection, RateBucket::Subscribe, request_id) {
            return;
        }
        let Some(session) = self
            .conn_get(connection)
            .and_then(|entry| entry.session.as_ref())
            .cloned()
        else {
            let _ = self.send_error_for(connection, 20, "authentication required", request_id);
            return;
        };
        let Some(world) = session.attached_world() else {
            let _ = self.send_error_for(
                connection,
                21,
                "attach to a world before subscribing",
                request_id,
            );
            return;
        };
        if self.conn_get(connection).is_some_and(|entry| {
            entry.subscriptions.len() >= self.config.max_subscriptions_per_session()
        }) {
            let _ = self.send_error_for(connection, 17, "subscription limit reached", request_id);
            return;
        }
        match self.runtime.subscribe(world, query) {
            Ok(server_sub) => {
                if let Some(entry) = self.conn_get_mut(connection) {
                    entry.subscriptions.insert(
                        server_sub,
                        NetworkSubscription {
                            world,
                            server: server_sub,
                        },
                    );
                }
                self.world_subscribers
                    .entry(world)
                    .or_default()
                    .push((connection, server_sub));
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
        // Fast path: skip drain entirely when no pending updates.
        let has_data = self
            .runtime
            .has_pending(world, subscription)
            .unwrap_or(false);
        if !has_data {
            return;
        }
        let updates = match self.runtime.drain(world, subscription) {
            Ok(updates) => updates,
            Err(_) => return,
        };
        let request_id = self
            .snapshot_requests
            .get(&(connection, subscription))
            .copied()
            .unwrap_or(0);
        if updates.len() <= 1 {
            // Single delta: send directly (no clone — ownership moved).
            for update in &updates {
                if let Some(message) = serialize_update(subscription, request_id, update)
                    && self.send_direct(connection, message).unwrap_or(false)
                {
                    self.metrics.subscription_messages_sent += 1;
                }
            }
        } else {
            // Batch multiple deltas into one frame.
            let mut deltas = Vec::with_capacity(updates.len());
            for update in &updates {
                match update {
                    SubscriptionUpdate::Insert { seq, row } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Insert,
                            row_id: row.row_id(),
                            row: Some(std::sync::Arc::clone(row)),
                        });
                    }
                    SubscriptionUpdate::Update { seq, row } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Update,
                            row_id: row.row_id(),
                            row: Some(std::sync::Arc::clone(row)),
                        });
                    }
                    SubscriptionUpdate::Delete { seq, row_id } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Delete,
                            row_id: *row_id,
                            row: None,
                        });
                    }
                    _ => {
                        // Initial/Resync/Stale: send individually.
                        if let Some(message) = serialize_update(subscription, request_id, update)
                            && self.send_direct(connection, message).unwrap_or(false)
                        {
                            self.metrics.subscription_messages_sent += 1;
                        }
                    }
                }
            }
            if !deltas.is_empty() {
                let message = ServerMessage::SubscriptionDeltaBatch {
                    subscription,
                    request_id,
                    deltas: std::sync::Arc::new(deltas),
                };
                // Use send_direct to avoid clone: message is moved into the
                // connection's direct queue under one Mutex lock.
                if self.send_direct(connection, message).unwrap_or(false) {
                    self.metrics.subscription_messages_sent += 1;
                }
            }
        }
    }

    /// Sends pre-drained subscription updates to a connection.
    /// This is the batched version of pump_subscription — updates are already
    /// drained, so no has_pending/drain overhead per subscriber.
    fn send_subscription_updates(
        &mut self,
        connection: ConnectionId,
        subscription: SubscriptionId,
        updates: &[nexum_subscription::SubscriptionUpdate],
    ) {
        use crate::protocol::DeltaKind;
        let request_id = self
            .snapshot_requests
            .get(&(connection, subscription))
            .copied()
            .unwrap_or(0);
        if updates.len() <= 1 {
            // Single delta: send directly (no clone — ownership moved).
            for update in updates {
                if let Some(message) = serialize_update(subscription, request_id, update)
                    && self.send_direct(connection, message).unwrap_or(false)
                {
                    self.metrics.subscription_messages_sent += 1;
                }
            }
        } else {
            // Batch multiple deltas into one frame.
            let mut deltas = Vec::with_capacity(updates.len());
            for update in updates {
                match update {
                    nexum_subscription::SubscriptionUpdate::Insert { seq, row } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Insert,
                            row_id: row.row_id(),
                            row: Some(std::sync::Arc::clone(row)),
                        });
                    }
                    nexum_subscription::SubscriptionUpdate::Update { seq, row } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Update,
                            row_id: row.row_id(),
                            row: Some(std::sync::Arc::clone(row)),
                        });
                    }
                    nexum_subscription::SubscriptionUpdate::Delete { seq, row_id } => {
                        deltas.push(crate::protocol::SubscriptionDeltaEntry {
                            seq: *seq,
                            kind: DeltaKind::Delete,
                            row_id: *row_id,
                            row: None,
                        });
                    }
                    _ => {
                        // Initial/Resync/Stale: send individually.
                        if let Some(message) = serialize_update(subscription, request_id, update)
                            && self.send(connection, &message).unwrap_or(false)
                        {
                            self.metrics.subscription_messages_sent += 1;
                        }
                    }
                }
            }
            if !deltas.is_empty() {
                let message = ServerMessage::SubscriptionDeltaBatch {
                    subscription,
                    request_id,
                    deltas: std::sync::Arc::new(deltas),
                };
                // Use send_direct to avoid clone: message is moved into the
                // connection's direct queue under one Mutex lock.
                if self.send_direct(connection, message).unwrap_or(false) {
                    self.metrics.subscription_messages_sent += 1;
                }
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
        let report = self.fan_out_results(&results);
        let _ = self.flush_outbound();
        Ok(report)
    }

    /// Fans the results of one `step_detailed` pass out to clients: per
    /// successful world, a `TickUpdate` broadcast to every attached session,
    /// subscription drains, and reducer-call result routing; then answers
    /// pending calls whose world can no longer produce a result. Deterministic
    /// (connections ascending, subscriptions ascending, call order). This is
    /// the fan-out half of [`step_worlds`](Self::step_worlds); composition
    /// layers (like the game server, ADR-014) call it directly after their
    /// own `step_detailed` so they can observe the committed results too.
    pub fn fan_out_results(
        &mut self,
        results: &[(WorldId, nexum_simulation::TickResult)],
    ) -> StepReport {
        let mut report = StepReport::default();
        for (world, result) in results {
            let world = *world;
            report.worlds += 1;
            // One TickUpdate per world, encoded **once** and cloned to every
            // attached session (ADR-017 D4): re-encoding per connection was
            // O(changes × clients) — the dominant fan-out cost at scale.
            //
            // Bounded payload (ADR-020 D2): by default the broadcast carries
            // tick metadata + events only — the full change list is redundant
            // with the windowed `SubscriptionDelta` delivery path and costs
            // O(changes × clients) to decode. Opt in via
            // `NetworkConfig::with_tick_update_changes(true)` for per-tick
            // full-change diagnostics.
            let changes = if self.config.tick_update_changes {
                result.changes().to_vec()
            } else {
                Vec::new()
            };
            let events = result.events().to_vec();
            // When skip_empty_broadcast is enabled and the TickUpdate carries
            // zero useful payload (no changes, no events), skip the O(CCU)
            // broadcast entirely.  Clients learn about state changes via
            // SubscriptionDelta.  This eliminates ~10K message sends per tick
            // at high CCU.
            let has_content = !changes.is_empty() || !events.is_empty();
            if has_content || !self.config.skip_empty_broadcast() {
                let message = ServerMessage::TickUpdate {
                    world,
                    tick: result.tick(),
                    tx_id: result.tx_id(),
                    changes,
                    events,
                };
                let attached: Vec<ConnectionId> = self
                    .attached_by_world
                    .get(&world)
                    .map(|set| set.iter().copied().collect())
                    .unwrap_or_default();
                for connection in attached {
                    if self.send(connection, &message).unwrap_or(false) {
                        report.tick_updates_sent += 1;
                        self.metrics.tick_updates_sent += 1;
                    } else {
                        report.messages_dropped += 1;
                    }
                }
            }

            // Deliver subscription deltas for this world (deterministic:
            // connections ascending, then subscriptions ascending). Use the
            // pre-computed world_subscribers list instead of scanning every
            // connection — O(subscribers) instead of O(CCU).
            //
            // Fast path: skip the O(subscribers) scan entirely when the world
            // produced no changes this tick — no subscription buffer can have
            // new entries. This eliminates ~20K BTreeMap lookups per idle world.
            //
            // Batched drain: drain ALL pending subscriptions in a single O(N)
            // pass, then fan-out from a HashMap — O(1) per subscriber instead
            // of O(log N) BTreeMap lookup per subscriber (Phase 23-25 optimization).
            if !result.changes().is_empty()
                && let Some(subscribers) = self.world_subscribers.get(&world)
            {
                // Single-pass drain: O(N) total instead of N × O(log N).
                let drained = self
                    .runtime
                    .drain_all_pending(world)
                    .unwrap_or_default();
                let mut drained_map: std::collections::HashMap<SubscriptionId, Vec<nexum_subscription::SubscriptionUpdate>> =
                    std::collections::HashMap::with_capacity(drained.len());
                for (sid, updates) in drained {
                    drained_map.insert(sid, updates);
                }
                // Fan-out in deterministic order (connections ascending,
                // subscriptions ascending).
                let subs = subscribers.clone();
                for (connection, subscription) in subs {
                    if let Some(updates) = drained_map.remove(&subscription) {
                        self.send_subscription_updates(
                            connection,
                            subscription,
                            &updates,
                        );
                        report.subscription_messages_sent +=
                            self.metrics.subscription_messages_sent;
                    }
                }
            }

            // Route committed reducer-call results (ADR-013 D3) to their
            // requesting connections, in call order. The runtime echoes the
            // gateway-allocated id; translate it back to the client's own id
            // and the awaiting connection (Phase 16 namespace fix).
            for call_result in result.reducer_results() {
                if let Some(pending) = self
                    .pending_calls
                    .remove(&(world, call_result.request_id()))
                {
                    if let Some(ids) = self.pending_by_connection.get_mut(&pending.connection) {
                        ids.remove(&pending.client_request_id);
                    }
                    let message = if call_result.is_ok() {
                        ServerMessage::ReducerResult {
                            request_id: pending.client_request_id,
                            ok: true,
                            value: call_result.value().cloned(),
                            error: None,
                        }
                    } else {
                        ServerMessage::ReducerResult {
                            request_id: pending.client_request_id,
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
                    if self.send(pending.connection, &message).unwrap_or(false) {
                        self.metrics.reducer_results_sent += 1;
                    }
                }
            }
        }

        // Answer pending calls whose world can no longer produce a result —
        // it failed, was stopped, or was destroyed during this step — with a
        // correlated failure (the client's own request id), so no caller is
        // left hanging (ADR-013 D3). A still-Running world's calls stay
        // pending for its next tick.
        let unresolved: Vec<(WorldId, u64, PendingCall)> = self
            .pending_calls
            .iter()
            .filter_map(|((world, gateway_id), pending)| {
                let unresolved = match self.runtime.world_status(*world) {
                    Ok(status) => status.state != nexum_runtime::WorldLifecycle::Running,
                    // Destroyed: the world no longer exists.
                    Err(_) => true,
                };
                unresolved.then_some((*world, *gateway_id, *pending))
            })
            .collect();
        for (world, gateway_id, pending) in unresolved {
            self.pending_calls.remove(&(world, gateway_id));
            if let Some(ids) = self.pending_by_connection.get_mut(&pending.connection) {
                ids.remove(&pending.client_request_id);
            }
            let message = ServerMessage::ReducerResult {
                request_id: pending.client_request_id,
                ok: false,
                value: None,
                error: Some(format!(
                    "world {world} is no longer running; the call could not commit"
                )),
            };
            if self.send(pending.connection, &message).unwrap_or(false) {
                self.metrics.reducer_results_sent += 1;
            }
        }
        report
    }

    /// Drains every network subscription (used after subscribe/resync when
    /// no tick ran). Returns the number of messages serialized.
    pub fn pump_subscriptions(&mut self) -> u64 {
        let subs: Vec<(ConnectionId, WorldId, SubscriptionId)> = self
            .connections
            .iter()
            .enumerate()
            .filter_map(|(idx, slot)| {
                slot.as_ref()
                    .map(|e| (ConnectionId::from_u64(idx as u64), e))
            })
            .flat_map(|(id, entry)| {
                entry
                    .subscriptions
                    .iter()
                    .map(move |(sub_id, sub)| (id, sub.world, *sub_id))
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

    /// Hot-path direct send: takes ownership of the message and pushes it
    /// to the connection's direct queue under one Mutex lock. Eliminates
    /// the clone that `send()` would perform when converting
    /// &ServerMessage → owned.
    ///
    /// On overflow (Full), marks the connection stale and enqueues
    /// StaleNotifications — same semantics as `send` but faster.
    fn send_direct(
        &mut self,
        connection: ConnectionId,
        message: ServerMessage,
    ) -> Result<bool, NetworkError> {
        let max_payload = self.config.max_frame_payload();
        let event_limit = self.config.event_log_limit();
        let entry = self
            .conn_get_mut(connection)
            .ok_or(NetworkError::UnknownConnection(connection))?;
        if entry.stale {
            return Ok(false);
        }
        match entry.connection.try_send_direct(message, max_payload) {
            Ok(()) => {
                self.metrics.messages_outbound += 1;
                Ok(true)
            }
            Err(TransportError::Full) => {
                if !entry.stale {
                    entry.stale = true;
                    let stale_subs: Vec<SubscriptionId> = entry
                        .subscriptions
                        .keys()
                        .copied()
                        .collect();
                    for sub in &stale_subs {
                        if let Ok(notify) = protocol::encode_server(
                            &ServerMessage::StaleNotification {
                                subscription: *sub,
                                seq: 0,
                            },
                            max_payload,
                        ) {
                            entry.pending_stale.push_back(notify);
                            entry.has_pending_stale = true;
                        }
                    }
                    Self::push_event(
                        &mut self.events,
                        event_limit,
                        NetworkEvent::SessionStale { connection },
                    );
                }
                self.metrics.messages_dropped += 1;
                Ok(false)
            }
            Err(_) => Ok(false),
        }
    }

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
        let stale_signal = is_stale_signal(message);
        let max_payload = self.config.max_frame_payload();
        // Fast path: try direct message passing (bypasses encode/decode).
        // Unified capacity ensures overflow/stale semantics are preserved.
        {
            let entry = self
                .conn_get_mut(connection)
                .ok_or(NetworkError::UnknownConnection(connection))?;

            // Flush pending stale notifications via frame path.
            // Fast-path: skip when no stale notifications pending.
            if entry.has_pending_stale {
                while let Some(pending) = entry.pending_stale.front().cloned() {
                    match entry.connection.try_send_frame(Arc::from(pending)) {
                        Ok(()) => {
                            entry.pending_stale.pop_front();
                        }
                        Err(TransportError::Full) | Err(TransportError::Closed) => break,
                        Err(_) => break,
                    }
                }
                entry.has_pending_stale = !entry.pending_stale.is_empty();
            }

            // Already stale and not a stale signal → drop.
            if entry.stale && !stale_signal {
                self.metrics.messages_dropped += 1;
                return Ok(false);
            }

            // Try direct send.
            if entry
                .connection
                .try_send_direct(message.clone(), max_payload)
                .is_ok()
            {
                self.metrics.messages_outbound += 1;
                return Ok(true);
            }
        }
        // Slow path: encode to frame bytes and send.
        let frame = protocol::encode_server(message, max_payload)?;
        self.send_encoded(connection, Arc::from(frame), stale_signal)
    }

    /// Sends a **pre-encoded** frame to a connection, applying the same
    /// stale/overflow policy as [`send`](Self::send) (`stale_signal` is the
    /// message's classification — see [`is_stale_signal`]). Encoding once
    /// and sharing the immutable bytes (an `Arc<[u8]>`, ADR-021 D1) with
    /// every recipient avoids re-serializing and re-copying a large message
    /// (e.g. a `TickUpdate` broadcast) once per connection.
    fn send_encoded(
        &mut self,
        connection: ConnectionId,
        frame: Arc<[u8]>,
        stale_signal: bool,
    ) -> Result<bool, NetworkError> {
        let overflow_policy = self.config.overflow_policy();
        let max_payload = self.config.max_frame_payload();
        let event_limit = self.config.event_log_limit();

        // Phase 1: flush stale + try send (entry-scoped borrow).
        let send_result: Result<(), TransportError>;
        let is_stale: bool;
        let stale_subs: Vec<SubscriptionId>;
        {
            let entry = self
                .conn_get_mut(connection)
                .ok_or(NetworkError::UnknownConnection(connection))?;

            // Flush any pending stale notifications.
            // Fast-path: skip when no stale notifications pending.
            if entry.has_pending_stale {
                while let Some(pending) = entry.pending_stale.front().cloned() {
                    match entry.connection.try_send_frame(Arc::from(pending)) {
                        Ok(()) => {
                            entry.pending_stale.pop_front();
                        }
                        Err(TransportError::Full) | Err(TransportError::Closed) => break,
                        Err(_) => break,
                    }
                }
                entry.has_pending_stale = !entry.pending_stale.is_empty();
            }

            if entry.stale && !stale_signal {
                return Ok(false);
            }

            send_result = entry.connection.try_send_frame(frame);
            is_stale = entry.stale;
            stale_subs = if send_result.is_err() && !entry.stale {
                entry.subscriptions.keys().copied().collect()
            } else {
                Vec::new()
            };
        }
        // entry borrow dropped — safe to access self.metrics etc.
        match send_result {
            Ok(()) => {
                self.metrics.messages_outbound += 1;
                Ok(true)
            }
            Err(TransportError::Full) => {
                self.metrics.messages_dropped += 1;
                match overflow_policy {
                    OutboundOverflowPolicy::Stale => {
                        if !is_stale {
                            if let Some(entry) = self.conn_get_mut(connection) {
                                entry.stale = true;
                                for sub in &stale_subs {
                                    if let Ok(notify) = protocol::encode_server(
                                        &ServerMessage::StaleNotification {
                                            subscription: *sub,
                                            seq: 0,
                                        },
                                        max_payload,
                                    ) {
                                        entry.pending_stale.push_back(notify);
                                        entry.has_pending_stale = true;
                                    }
                                }
                            }
                            Self::push_event(
                                &mut self.events,
                                event_limit,
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
        let mut to_drop: Vec<ConnectionId> = Vec::new();
        for (idx, slot) in self.connections.iter_mut().enumerate() {
            if let Some(entry) = slot.as_mut()
                && entry.connection.flush_outbound().is_err()
            {
                to_drop.push(ConnectionId::from_u64(idx as u64));
            }
        }
        for id in to_drop {
            self.drop_connection(&id, "transport write failure");
        }
        Ok(())
    }

    /// Rejects an operation when its per-connection rate bucket is exhausted
    /// (ADR-016 D1): sends a `19 rate limit exceeded` error and returns
    /// `false`. Never blocks, never drops accepted work silently — the
    /// rejection is explicit and observable via [`NetworkMetrics::rate_limited`].
    fn check_rate(&mut self, connection: ConnectionId, bucket: RateBucket) -> bool {
        self.check_rate_for(connection, bucket, 0)
    }

    /// [`Self::check_rate`] with a `request_id` so the rejection is
    /// correlated to the request that triggered it (used by `Subscribe`).
    fn check_rate_for(
        &mut self,
        connection: ConnectionId,
        bucket: RateBucket,
        request_id: u64,
    ) -> bool {
        let allowed = self
            .conn_get_mut(connection)
            .is_some_and(|entry| entry.rate.try_take(bucket, std::time::Instant::now()));
        if !allowed {
            self.metrics.rate_limited += 1;
            let _ = self.send_error_for(connection, 19, "rate limit exceeded", request_id);
        }
        allowed
    }

    // ------------------------------------------------------------- helpers

    /// Removes a connection: closes its transport, unsubscribes its
    /// session's subscriptions from the runtime registries, and records the
    /// drop.
    fn drop_connection(&mut self, connection: &ConnectionId, reason: &str) {
        let idx = connection.as_u64() as usize;
        let Some(mut entry) = self.connections.get_mut(idx).and_then(|slot| slot.take()) else {
            return;
        };
        self.active_connections = self.active_connections.saturating_sub(1);
        // Best-effort cleanup of the session's runtime subscriptions.
        let subs: Vec<(WorldId, SubscriptionId)> = entry
            .subscriptions
            .values()
            .map(|sub| (sub.world, sub.server))
            .collect();
        for (world, sub) in &subs {
            let _ = self.runtime.unsubscribe(*world, *sub);
            if let Some(world_subs) = self.world_subscribers.get_mut(world) {
                world_subs.retain(|(c, s)| !(*c == *connection && *s == *sub));
            }
        }
        // Drop the connection's pending reducer calls (ADR-013 D3): the
        // results can no longer be delivered. The runtime may still execute
        // accepted calls (fire-and-forget); the correlation state is gone.
        self.pending_calls
            .retain(|_, pending| pending.connection != *connection);
        self.pending_by_connection.remove(connection);
        // Remove the connection from its world's attached index (ADR-021 D3).
        if let Some(world) = entry.session.as_ref().and_then(Session::attached_world)
            && let Some(set) = self.attached_by_world.get_mut(&world)
        {
            set.remove(connection);
            if set.is_empty() {
                self.attached_by_world.remove(&world);
            }
        }
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
        metrics.connections = self.active_connections;
        metrics.sessions = self
            .connections
            .iter()
            .filter_map(|o| o.as_ref())
            .filter(|entry| entry.session.is_some())
            .count();
        metrics.attached = self
            .connections
            .iter()
            .filter_map(|o| o.as_ref())
            .filter(|entry| entry.session.as_ref().is_some_and(Session::is_attached))
            .count();
        metrics.connections_per_world.clear();
        for entry in self.connections.iter().filter_map(|o| o.as_ref()) {
            if let Some(world) = entry.session.as_ref().and_then(Session::attached_world) {
                *metrics.connections_per_world.entry(world).or_insert(0) += 1;
            }
        }
        metrics.subscriptions = self
            .connections
            .iter()
            .filter_map(|o| o.as_ref())
            .map(|entry| entry.subscriptions.len())
            .sum();
        metrics.sessions_stale = self
            .connections
            .iter()
            .filter_map(|o| o.as_ref())
            .filter(|entry| entry.stale)
            .count();
        metrics
    }
}

impl std::fmt::Debug for NetworkGateway {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NetworkGateway")
            .field("connections", &self.active_connections)
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
            row: Some((**row).clone()),
        }),
        SubscriptionUpdate::Update { seq, row } => Some(ServerMessage::SubscriptionDelta {
            subscription,
            seq: *seq,
            kind: DeltaKind::Update,
            row_id: row.row_id(),
            row: Some((**row).clone()),
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
