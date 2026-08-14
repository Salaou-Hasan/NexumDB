//! Runtime metrics ([`RuntimeMetrics`]).
//!
//! Simple monotonically-counted operational metrics, snapshotted via
//! [`Runtime::metrics`](crate::Runtime::metrics). These are instrumentation
//! points for Phase 14's full observability; they never influence
//! simulation semantics.

/// A point-in-time snapshot of the runtime's counters.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeMetrics {
    /// Configured worker count.
    pub workers: usize,
    /// Worlds currently registered (any lifecycle).
    pub worlds: usize,
    /// Worlds currently running.
    pub running_worlds: usize,
    /// Partitions currently registered.
    pub partitions: usize,
    /// Total tick attempts.
    pub ticks_total: u64,
    /// Successful ticks (committed + persisted).
    pub ticks_succeeded: u64,
    /// Failed ticks.
    pub ticks_failed: u64,
    /// Cumulative tick wall time in nanoseconds (for averages).
    pub tick_ns_total: u64,
    /// Inputs accepted into queues.
    pub inputs_accepted: u64,
    /// Inputs rejected (full queue, late, wrong state).
    pub inputs_rejected: u64,
    /// Client reducer calls accepted into queues (ADR-013 D3).
    pub reducer_calls_accepted: u64,
    /// Client reducer calls rejected (full queue, wrong state, invalid).
    pub reducer_calls_rejected: u64,
    /// Cross-partition messages accepted into inbound queues.
    pub messages_sent: u64,
    /// Cross-partition messages drained into a tick's delivered batch
    /// (counted before the tick runs — includes batches whose tick then
    /// fails; "delivered into a tick", not "processed successfully").
    pub messages_delivered: u64,
    /// Cross-partition messages dropped (overflow, unknown destination).
    pub messages_dropped: u64,
    /// Successful WAL appends.
    pub wal_appends: u64,
    /// WAL append failures.
    pub wal_failures: u64,
    /// Snapshots written.
    pub snapshots: u64,
    /// Subscriptions currently registered across all worlds.
    pub subscriptions: usize,
    /// Worlds created.
    pub world_creations: u64,
    /// Worlds recovered from persisted state.
    pub recoveries: u64,
    /// Worlds that failed.
    pub world_failures: u64,
    /// Runtime uptime in nanoseconds.
    pub uptime_ns: u64,
}

impl RuntimeMetrics {
    /// A zeroed metrics snapshot (before any operation).
    pub fn empty() -> Self {
        Self {
            workers: 0,
            worlds: 0,
            running_worlds: 0,
            partitions: 0,
            ticks_total: 0,
            ticks_succeeded: 0,
            ticks_failed: 0,
            tick_ns_total: 0,
            inputs_accepted: 0,
            inputs_rejected: 0,
            reducer_calls_accepted: 0,
            reducer_calls_rejected: 0,
            messages_sent: 0,
            messages_delivered: 0,
            messages_dropped: 0,
            wal_appends: 0,
            wal_failures: 0,
            snapshots: 0,
            subscriptions: 0,
            world_creations: 0,
            recoveries: 0,
            world_failures: 0,
            uptime_ns: 0,
        }
    }

    /// Average tick duration in nanoseconds, or 0 when no ticks have run.
    pub fn avg_tick_ns(&self) -> u64 {
        self.tick_ns_total.checked_div(self.ticks_total).unwrap_or(0)
    }
}
