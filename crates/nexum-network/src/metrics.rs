//! Network metrics ([`NetworkMetrics`], ADR-011).
//!
//! Simple monotonically-counted operational counters snapshot via
//! [`NetworkGateway::metrics`](crate::NetworkGateway::metrics). They never
//! influence simulation semantics; they are instrumentation points for
//! Phase 14.

use nexum_core::WorldId;
use std::collections::BTreeMap;

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
    /// Reducer results routed to clients.
    pub reducer_results_sent: u64,
    /// TickUpdate broadcasts sent.
    pub tick_updates_sent: u64,
    /// Subscription deltas/snapshots serialized.
    pub subscription_messages_sent: u64,
}

impl NetworkMetrics {
    pub(crate) fn empty() -> Self {
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
            reducer_results_sent: 0,
            tick_updates_sent: 0,
            subscription_messages_sent: 0,
        }
    }
}
