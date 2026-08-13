//! Game server metrics (ADR-014 §Observability).
//!
//! Monotonic counters plus live counts computed at snapshot time. Metrics
//! never influence simulation semantics — they are instrumentation points
//! for Phase 15/16.

/// A point-in-time snapshot of the game server's counters.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct GameServerMetrics {
    /// Games created (cumulative).
    pub games_created: u64,
    /// Games recovered (cumulative).
    pub games_recovered: u64,
    /// Games destroyed (cumulative).
    pub games_destroyed: u64,
    /// Games failed (cumulative).
    pub games_failed: u64,
    /// Player joins (cumulative).
    pub players_joined: u64,
    /// Player reconnects (cumulative).
    pub players_reconnected: u64,
    /// Player disconnects (cumulative).
    pub players_disconnected: u64,
    /// Player leaves (cumulative).
    pub players_left: u64,
    /// Server-side commands accepted (cumulative).
    pub commands_received: u64,
    /// Server-side commands rejected (cumulative).
    pub commands_rejected: u64,
    /// Server-side reducer invocations accepted (cumulative).
    pub reducer_calls: u64,
    /// Reducer invocations that failed or were rejected (cumulative).
    pub reducer_failures: u64,
    /// WASM reducer failures observed in world ticks (cumulative).
    pub wasm_failures: u64,
    /// Server-side subscriptions created (cumulative).
    pub subscriptions: u64,
    /// Per-player subscription-limit hits (cumulative).
    pub subscription_limits_hit: u64,
    /// World tick failures observed (cumulative).
    pub tick_failures: u64,
    /// World failures observed (cumulative).
    pub world_failures: u64,
    /// Partition failures observed (cumulative).
    pub partition_failures: u64,
    /// Operations denied by the authorization policy (cumulative).
    pub policy_rejections: u64,
    // ------------------------------------------------------------ live
    /// Games currently running.
    pub games_active: usize,
    /// Players currently present (joining or active).
    pub players_active: usize,
    /// Authoritative partitions across all games.
    pub partitions: usize,
    /// Partitions whose world is failed.
    pub failed_partitions: usize,
}
