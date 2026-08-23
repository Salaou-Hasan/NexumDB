//! Runtime configuration ([`RuntimeConfig`], [`PersistencePolicy`],
//! [`TickFailurePolicy`], ADR-010 D8).
//!
//! Configuration affects only **operational** behavior — worker counts,
//! queue bounds, persistence, scheduling policy — never a world's
//! simulation semantics. A world's result depends only on its seed, inputs,
//! systems, reducer code, and tick.

use std::path::PathBuf;

use nexum_core::{Error, Result, WorkerId, WorldId};
use nexum_simulation::{SimulationConfig, World};
use nexum_table::TableStore;

/// The persistence policy applied to every world's WAL (ADR-010 D4).
///
/// Maps directly onto the Phase 5 [`DurabilityPolicy`]: `Flush` survives a
/// process crash, `Sync` (fsync) survives power loss and is the durable
/// mode.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PersistencePolicy {
    /// No WAL: commits are in-memory only (crash loses all state).
    None,
    /// Write the WAL records to the OS on every commit.
    Flush,
    /// Write and fsync the WAL on every commit — the durable mode.
    Sync,
}

impl PersistencePolicy {
    /// Returns `true` when persistence is enabled.
    pub fn is_enabled(self) -> bool {
        !matches!(self, Self::None)
    }

    /// The matching Phase 5 durability policy, if enabled.
    pub fn durability(self) -> Option<nexum_wal::DurabilityPolicy> {
        match self {
            Self::None => None,
            Self::Flush => Some(nexum_wal::DurabilityPolicy::Flush),
            Self::Sync => Some(nexum_wal::DurabilityPolicy::Sync),
        }
    }
}

/// What the runtime does when a world's tick returns a [`TickError`]
/// (ADR-010 D6).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TickFailurePolicy {
    /// Mark the world failed and stop ticking it (default). A failed world
    /// is isolated; other worlds are unaffected.
    FailWorld,
    /// Record the failure (event + metric) and keep ticking the world.
    Continue,
}

/// Builds a [`World`] from an id, an authoritative store, and a simulation
/// configuration.
///
/// The same factory is used by `create_world` and `recover_world`, so a
/// recovered world is byte-identical in setup to a fresh one (same systems,
/// reducers, WASM modules — determinism across recovery).
///
/// A recovered store may already contain the authoritative schema (restored
/// from a snapshot), so factories must create tables only if absent
/// (ADR-010 D5).
pub type WorldFactory = Box<dyn Fn(WorldId, TableStore, SimulationConfig) -> Result<World>>;

/// The runtime configuration, validated at [`Runtime::new`](crate::Runtime::new).
pub struct RuntimeConfig {
    pub(crate) worker_count: usize,
    pub(crate) factory: WorldFactory,
    pub(crate) persistence: PersistencePolicy,
    pub(crate) persistence_dir: Option<PathBuf>,
    pub(crate) max_queued_inputs: usize,
    pub(crate) max_queued_partition_messages: usize,
    pub(crate) max_queued_reducer_calls: usize,
    pub(crate) tick_failure_policy: TickFailurePolicy,
    pub(crate) snapshot_interval: Option<u64>,
    pub(crate) event_log_limit: usize,
}

impl RuntimeConfig {
    /// Creates a default configuration (1 worker, no persistence, `FailWorld`
    /// tick policy) with `factory` building worlds.
    pub fn new(factory: WorldFactory) -> Self {
        Self {
            worker_count: 1,
            factory,
            persistence: PersistencePolicy::None,
            persistence_dir: None,
            max_queued_inputs: 1_024,
            max_queued_partition_messages: 10_000,
            max_queued_reducer_calls: 1_024,
            tick_failure_policy: TickFailurePolicy::FailWorld,
            snapshot_interval: None,
            event_log_limit: 1_024,
        }
    }

    /// Sets the number of logical workers (≥ 1).
    pub fn with_worker_count(mut self, count: usize) -> Self {
        self.worker_count = count;
        self
    }

    /// Enables per-world persistence. `dir` is required and worlds get
    /// `dir/world_<id>/` for their WAL and snapshots.
    pub fn with_persistence(mut self, policy: PersistencePolicy, dir: PathBuf) -> Self {
        self.persistence = policy;
        self.persistence_dir = Some(dir);
        self
    }

    /// Sets the per-world input queue bound (≥ 1).
    pub fn with_max_queued_inputs(mut self, max: usize) -> Self {
        self.max_queued_inputs = max;
        self
    }

    /// Sets the per-partition inbound message queue bound (≥ 1, ADR-012
    /// D7). Overflow drops the message deterministically (event + metric);
    /// it never blocks the sender's tick.
    pub fn with_max_queued_partition_messages(mut self, max: usize) -> Self {
        self.max_queued_partition_messages = max;
        self
    }

    /// Sets the per-world queued reducer-call bound (≥ 1, ADR-013 D3).
    /// Overflow rejects the call to the caller (never blocks a tick).
    pub fn with_max_queued_reducer_calls(mut self, max: usize) -> Self {
        self.max_queued_reducer_calls = max;
        self
    }

    /// Sets the tick-failure policy.
    pub fn with_tick_failure_policy(mut self, policy: TickFailurePolicy) -> Self {
        self.tick_failure_policy = policy;
        self
    }

    /// Enables periodic snapshots every `interval` successful ticks.
    pub fn with_snapshot_interval(mut self, interval: u64) -> Self {
        self.snapshot_interval = Some(interval);
        self
    }

    /// Sets the bounded runtime event buffer size (≥ 1).
    pub fn with_event_log_limit(mut self, limit: usize) -> Self {
        self.event_log_limit = limit;
        self
    }

    /// Validates the configuration. Called by `Runtime::new`.
    pub fn validate(&self) -> Result<()> {
        if self.worker_count == 0 {
            return Err(Error::invalid_argument("worker_count must be at least 1"));
        }
        if self.max_queued_inputs == 0 {
            return Err(Error::invalid_argument(
                "max_queued_inputs must be at least 1",
            ));
        }
        if self.max_queued_partition_messages == 0 {
            return Err(Error::invalid_argument(
                "max_queued_partition_messages must be at least 1",
            ));
        }
        if self.max_queued_reducer_calls == 0 {
            return Err(Error::invalid_argument(
                "max_queued_reducer_calls must be at least 1",
            ));
        }
        if self.event_log_limit == 0 {
            return Err(Error::invalid_argument(
                "event_log_limit must be at least 1",
            ));
        }
        if let Some(interval) = self.snapshot_interval
            && interval == 0
        {
            return Err(Error::invalid_argument(
                "snapshot_interval must be at least 1",
            ));
        }
        if self.persistence.is_enabled() && self.persistence_dir.is_none() {
            return Err(Error::invalid_argument(
                "a persistence directory is required when persistence is enabled",
            ));
        }
        Ok(())
    }

    /// Returns the configured worker count.
    pub fn worker_count(&self) -> usize {
        self.worker_count
    }

    /// Returns the world factory.
    pub fn factory(&self) -> &WorldFactory {
        &self.factory
    }

    /// Returns the persistence policy.
    pub fn persistence(&self) -> PersistencePolicy {
        self.persistence
    }

    /// Returns the persistence directory, if any.
    pub fn persistence_dir(&self) -> Option<&PathBuf> {
        self.persistence_dir.as_ref()
    }

    /// Returns the per-world input queue bound.
    pub fn max_queued_inputs(&self) -> usize {
        self.max_queued_inputs
    }

    /// Returns the per-partition inbound message queue bound.
    pub fn max_queued_partition_messages(&self) -> usize {
        self.max_queued_partition_messages
    }

    /// Returns the per-world queued reducer-call bound.
    pub fn max_queued_reducer_calls(&self) -> usize {
        self.max_queued_reducer_calls
    }

    /// Returns the tick-failure policy.
    pub fn tick_failure_policy(&self) -> TickFailurePolicy {
        self.tick_failure_policy
    }

    /// Returns the snapshot interval (in ticks), if enabled.
    pub fn snapshot_interval(&self) -> Option<u64> {
        self.snapshot_interval
    }

    /// Returns the event log bound.
    pub fn event_log_limit(&self) -> usize {
        self.event_log_limit
    }

    /// The first worker id.
    pub fn first_worker(&self) -> WorkerId {
        WorkerId::from_u64(0)
    }

    /// The last worker id.
    pub fn last_worker(&self) -> WorkerId {
        WorkerId::from_u64((self.worker_count - 1) as u64)
    }
}

impl std::fmt::Debug for RuntimeConfig {
    /// The world factory is a closure (not `Debug`); the remaining fields are
    /// shown.
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RuntimeConfig")
            .field("worker_count", &self.worker_count)
            .field("factory", &"<world factory>")
            .field("persistence", &self.persistence)
            .field("persistence_dir", &self.persistence_dir)
            .field("max_queued_inputs", &self.max_queued_inputs)
            .field(
                "max_queued_partition_messages",
                &self.max_queued_partition_messages,
            )
            .field("max_queued_reducer_calls", &self.max_queued_reducer_calls)
            .field("tick_failure_policy", &self.tick_failure_policy)
            .field("snapshot_interval", &self.snapshot_interval)
            .field("event_log_limit", &self.event_log_limit)
            .finish()
    }
}
