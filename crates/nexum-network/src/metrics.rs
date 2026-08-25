//! Network metrics ([`NetworkMetrics`], ADR-011).
//!
//! Simple monotonically-counted operational counters snapshot via
//! [`NetworkGateway::metrics`](crate::NetworkGateway::metrics). They never
//! influence simulation semantics; they are instrumentation points for
//! Phase 14.

use nexum_core::WorldId;
use std::collections::BTreeMap;

/// Cumulative wall-time per gateway stage (Phase 27 instrumentation):
/// inbound (collect/decode/dispatch) and fan-out (TickUpdate/deltas/
/// results), in nanoseconds since gateway start. Diff two snapshots to get
/// per-window averages.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct GatewayStepProfile {
    /// Inbound Phase 1: ring collection across the connection slab.
    pub collect_ns: u64,
    /// Inbound Phase 2: protocol decode of collected frames.
    pub decode_ns: u64,
    /// Inbound Phase 2: dispatch of decoded messages.
    pub dispatch_ns: u64,
    /// Fan-out: per-world TickUpdate encode + attached-connection pushes.
    pub tick_update_ns: u64,
    /// Fan-out: subscription delta drain + per-subscriber delivery.
    pub deltas_ns: u64,
    /// Fan-out: reducer-result correlation + routing.
    pub results_ns: u64,
}

/// A point-in-time snapshot of the gateway's counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct NetworkMetrics {
    /// Connections currently registered.
    pub connections: usize,
    /// Authenticated sessions.
    pub sessions: usize,
    /// Sessions attached to a world.
    pub attached: usize,
    /// Connections per attached world (world id → count).
    pub connections_per_world: BTreeMap<WorldId, u64>,
    /// Frames received from clients.
    pub frames_received: u64,
    /// Frames rejected (protocol violations, unknown connections).
    pub frames_rejected: u64,
    /// Messages queued outbound to clients.
    pub messages_outbound: u64,
    /// Messages dropped for a stale session or policy overflow.
    pub messages_dropped: u64,
    /// Clients dropped (overflow policy, transport failure, flood).
    pub clients_dropped: u64,
    /// Network subscriptions currently active (across sessions).
    pub subscriptions: usize,
    /// Sessions currently marked stale.
    pub sessions_stale: usize,
    /// Protocol violations observed.
    pub protocol_errors: u64,
    /// Authentication failures.
    pub auth_failures: u64,
    /// Input frames accepted into world queues.
    pub inputs_accepted: u64,
    /// Input frames rejected by the runtime.
    pub inputs_rejected: u64,
    /// Reducer calls accepted into world queues (ADR-013 D3).
    pub reducer_calls_accepted: u64,
    /// Reducer calls rejected (session, attachment, bounds, runtime).
    pub reducer_calls_rejected: u64,
    /// Client operations denied by the authorization policy (ADR-014 D2).
    pub policy_rejections: u64,
    /// Client operations rejected by a per-connection rate limit (ADR-016 D1).
    pub rate_limited: u64,
    /// Reducer results routed to clients.
    pub reducer_results_sent: u64,
    /// TickUpdate broadcasts sent.
    pub tick_updates_sent: u64,
    /// Subscription deltas/snapshots serialized.
    pub subscription_messages_sent: u64,
    /// Stage breakdown of the most recent step (see [`GatewayStepProfile`]).
    pub last_step: GatewayStepProfile,
}

impl NetworkMetrics {
    /// A zeroed metrics snapshot (before any operation).
    pub fn empty() -> Self {
        Self {
            connections: 0,
            sessions: 0,
            attached: 0,
            connections_per_world: BTreeMap::new(),
            frames_received: 0,
            frames_rejected: 0,
            messages_outbound: 0,
            messages_dropped: 0,
            clients_dropped: 0,
            subscriptions: 0,
            sessions_stale: 0,
            protocol_errors: 0,
            auth_failures: 0,
            inputs_accepted: 0,
            inputs_rejected: 0,
            reducer_calls_accepted: 0,
            reducer_calls_rejected: 0,
            policy_rejections: 0,
            rate_limited: 0,
            reducer_results_sent: 0,
            tick_updates_sent: 0,
            subscription_messages_sent: 0,
            last_step: GatewayStepProfile::default(),
        }
    }
}
