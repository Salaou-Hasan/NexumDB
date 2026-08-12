//! The [`Runtime`]: the single-process coordinator (ADR-010).
//!
//! The runtime owns **operational** metadata only — workers, worlds,
//! ownership, input queues, lifecycle, metrics, events. Authoritative state
//! stays inside each `World`; `World::tick` remains the only commit path.
//! Per successful tick the runtime coordinates **durability first,
//! observation second** (ADR-010 D4): `Wal::append(tx_id, changes)`, then
//! `SubscriptionRegistry.apply_changes`. One WAL and one registry per world
//! keep isolated partitions from colliding on table ids (ADR-010 D3).
//!
//! ```rust,no_run
//! use nexum_core::{WorldId};
//! use nexum_runtime::{Runtime, RuntimeConfig};
//! use nexum_simulation::{SimulationConfig, World};
//! use nexum_table::TableStore;
//!
//! # fn main() -> Result<(), nexum_runtime::RuntimeError> {
//! let factory = Box::new(|id: WorldId, store: TableStore, sim: SimulationConfig| {
//!     let mut world = World::new(id, store, sim)?;
//!     world.add_system(nexum_simulation::SystemDefinition::new(
//!         nexum_core::SystemId::from_u64(0), "noop", 0, |_ctx, _| Ok(()),
//!     )?)?;
//!     Ok(world)
//! });
//! let mut runtime = Runtime::new(RuntimeConfig::new(factory))?;
//! runtime.create_world(WorldId::from_u64(0), SimulationConfig::new())?;
//! runtime.start_world(WorldId::from_u64(0))?;
//! let _report = runtime.step()?;
//! runtime.shutdown()?;
//! # Ok(())
//! # }
//! ```

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fmt;
use std::path::PathBuf;
use std::time::Instant;

use nexum_core::{Error, PartitionId, WorkerId, WorldId};
use nexum_reducer::ReducerArgs;
use nexum_simulation::{PartitionMessage, ReducerCall};
use nexum_subscription::{Query, SubscriptionId, SubscriptionUpdate};
use nexum_table::TableStore;
use nexum_wal::{RecoveryReport, Snapshot, Wal, recover};

use crate::config::{RuntimeConfig, TickFailurePolicy};
use crate::error::RuntimeError;
use crate::event::RuntimeEvent;
use crate::metrics::RuntimeMetrics;
use crate::partition::{PartitionEntry, PartitionStatus};
use crate::worker::{Worker, WorkerState, WorkerStatus};
use crate::world::{WorldEntry, WorldLifecycle, WorldStatus};

/// The lifecycle state of the runtime itself.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RuntimeState {
    /// Accepting control operations, inputs, and steps.
    Running,
    /// Shutting down: new operations are rejected, in-flight work drains.
    Stopping,
    /// Stopped; only `shutdown` (idempotent) succeeds.
    Stopped,
}

impl fmt::Display for RuntimeState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => f.write_str("running"),
            Self::Stopping => f.write_str("stopping"),
            Self::Stopped => f.write_str("stopped"),
        }
    }
}

/// The report of one [`Runtime::step`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStepReport {
    /// Worlds that were running at the start of the step.
    pub worlds: usize,
    /// Tick attempts.
    pub ticks: u64,
    /// Successful ticks (committed; persisted when enabled).
    pub succeeded: u64,
    /// Failed ticks.
    pub failed: u64,
    /// WAL appends performed.
    pub wal_appends: u64,
    /// Total step wall time in nanoseconds.
    pub duration_ns: u64,
}

/// The single-process runtime coordinator.
#[derive(Debug)]
pub struct Runtime {
    config: RuntimeConfig,
    state: RuntimeState,
    workers: BTreeMap<WorkerId, Worker>,
    worlds: BTreeMap<WorldId, WorldEntry>,
    /// The partition registry: message-bus address -> routing metadata
    /// (ADR-012 D1). Owns no authoritative state.
    partitions: BTreeMap<PartitionId, PartitionEntry>,
    /// The sorted topology of registered partitions, propagated to every
    /// world so `send_to` validation and routing share one source of truth.
    topology: BTreeSet<PartitionId>,
    /// Deterministic outbound sequence counters for externally injected
    /// messages (ADR-012 D5).
    sent_seq: BTreeMap<PartitionId, u64>,
    assign_counter: u64,
    events: VecDeque<RuntimeEvent>,
    metrics: RuntimeMetrics,
    started_at: Instant,
}

impl Runtime {
    /// Creates a running runtime from a validated configuration.
    ///
    /// Creates the persistence directory when enabled. Returns
    /// [`RuntimeError::InvalidConfig`] for an invalid configuration.
    pub fn new(config: RuntimeConfig) -> Result<Self, RuntimeError> {
        config
            .validate()
            .map_err(|error| RuntimeError::InvalidConfig(error.to_string()))?;
        if let Some(dir) = config.persistence_dir() {
            std::fs::create_dir_all(dir).map_err(|error| {
                RuntimeError::Persistence(Error::internal(format!(
                    "cannot create persistence directory '{}': {error}",
                    dir.display()
                )))
            })?;
        }
        let mut workers = BTreeMap::new();
        for index in 0..config.worker_count() as u64 {
            let id = WorkerId::from_u64(index);
            workers.insert(id, Worker::new(id));
        }
        let mut metrics = RuntimeMetrics::empty();
        metrics.workers = config.worker_count();
        Ok(Self {
            config,
            state: RuntimeState::Running,
            workers,
            worlds: BTreeMap::new(),
            partitions: BTreeMap::new(),
            topology: BTreeSet::new(),
            sent_seq: BTreeMap::new(),
            assign_counter: 0,
            events: VecDeque::new(),
            metrics,
            started_at: Instant::now(),
        })
    }

    // ------------------------------------------------------------- runtime

    /// Returns the runtime lifecycle state.
    pub fn state(&self) -> RuntimeState {
        self.state
    }

    /// Returns `true` while the runtime accepts operations.
    pub fn is_running(&self) -> bool {
        self.state == RuntimeState::Running
    }

    /// Returns the configuration.
    pub fn config(&self) -> &RuntimeConfig {
        &self.config
    }

    fn ensure_running(&self) -> Result<(), RuntimeError> {
        if self.is_running() {
            Ok(())
        } else {
            Err(RuntimeError::Shutdown)
        }
    }

    // ------------------------------------------------------------- workers

    /// Returns a worker's status.
    pub fn worker_status(&self, worker: WorkerId) -> Result<WorkerStatus, RuntimeError> {
        let worker = self.workers.get(&worker).ok_or(RuntimeError::UnknownWorker(worker))?;
        Ok(WorkerStatus {
            id: worker.id(),
            state: worker.state(),
            worlds: worker.worlds().collect(),
        })
    }

    /// Returns the configured worker ids in ascending order.
    pub fn workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.workers.keys().copied()
    }

    /// Marks a worker failed (ADR-010 D6): its worlds are marked failed and
    /// become recoverable; worlds on other workers are unaffected.
    /// Idempotent on an already-failed worker.
    pub fn fail_worker(&mut self, worker: WorkerId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let failed = {
            let handle = self.workers.get_mut(&worker).ok_or(RuntimeError::UnknownWorker(worker))?;
            if handle.state() == WorkerState::Failed {
                return Ok(());
            }
            handle.set_state(WorkerState::Failed);
            handle.worlds().collect::<Vec<_>>()
        };
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorkerFailed { worker },
        );
        for world in failed {
            let entry = self.worlds.get_mut(&world).expect("worker owns registered worlds");
            if entry.state != WorldLifecycle::Failed {
                entry.state = WorldLifecycle::Failed;
                self.metrics.world_failures += 1;
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    RuntimeEvent::WorldFailed {
                        world,
                        reason: Error::internal(format!("owner worker {worker} failed")),
                    },
                );
            }
        }
        Ok(())
    }

    /// Reassigns a world to another running worker (ADR-010 D6). The seed
    /// of future partition migration.
    pub fn reassign_world(&mut self, world_id: WorldId, to: WorkerId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        {
            let target = self.workers.get(&to).ok_or(RuntimeError::UnknownWorker(to))?;
            if target.state() != WorkerState::Running {
                return Err(RuntimeError::worker_state(to, "reassign onto", target.state()));
            }
        }
        let from = {
            let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
            entry.worker
        };
        if from == to {
            return Ok(());
        }
        self.workers
            .get_mut(&from)
            .expect("entry worker exists")
            .remove_world(world_id);
        self.workers
            .get_mut(&to)
            .expect("target exists")
            .add_world(world_id);
        self.worlds.get_mut(&world_id).expect("world exists").worker = to;
        // Keep the partition registry's ownership view in sync.
        if let Some(partition) = self.partition_for_world(world_id)
            && let Some(entry) = self.partitions.get_mut(&partition)
        {
            entry.worker = to;
        }
        Ok(())
    }

    /// Returns the worker currently owning a world.
    pub fn assigned_worker(&self, world_id: WorldId) -> Result<WorkerId, RuntimeError> {
        self.worlds
            .get(&world_id)
            .map(|entry| entry.worker)
            .ok_or(RuntimeError::UnknownWorld(world_id))
    }

    // ------------------------------------------------------------- worlds

    /// Creates a world from the configured factory and assigns it to a
    /// worker (deterministic round-robin). The world starts `Created`; call
    /// [`start_world`](Self::start_world) to run it.
    pub fn create_world(
        &mut self,
        world_id: WorldId,
        sim_config: nexum_simulation::SimulationConfig,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if self.worlds.contains_key(&world_id) {
            return Err(RuntimeError::DuplicateWorld(world_id));
        }
        let store = TableStore::new();
        let world = (self.config.factory())(world_id, store, sim_config).map_err(|error| {
            RuntimeError::Internal(format!(
                "world factory failed for world {world_id}: {error}"
            ))
        })?;
        let worker = self.assign_worker()?;
        let (wal, snapshot_dir) = self.open_persistence(world_id)?;
        self.workers
            .get_mut(&worker)
            .expect("assigned worker exists")
            .add_world(world_id);
        self.worlds
            .insert(world_id, WorldEntry::new(world, worker, wal, snapshot_dir));
        self.metrics.world_creations += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorldCreated { world: world_id, worker },
        );
        Ok(())
    }

    /// Reconstructs a world from persisted state (ADR-010 D5): the Phase 5
    /// `recover` engine into a fresh store, then the **same factory** as
    /// [`create_world`](Self::create_world). `resume_tick` continues the
    /// world's logical time where it stopped.
    ///
    /// Requires persistence to be enabled and the world's WAL to exist. The
    /// world starts `Created`; call [`start_world`](Self::start_world) to
    /// resume ticking.
    pub fn recover_world(
        &mut self,
        world_id: WorldId,
        sim_config: nexum_simulation::SimulationConfig,
        resume_tick: Option<nexum_core::TickId>,
    ) -> Result<RecoveryReport, RuntimeError> {
        self.ensure_running()?;
        if self.worlds.contains_key(&world_id) {
            return Err(RuntimeError::DuplicateWorld(world_id));
        }
        let durability = self
            .config
            .persistence()
            .durability()
            .ok_or_else(|| {
                RuntimeError::Persistence(Error::unsupported(
                    "cannot recover a world when persistence is disabled",
                ))
            })?;
        let dir = self.world_dir(world_id);
        let wal_path = dir.join("log.wal");
        if !wal_path.exists() {
            return Err(RuntimeError::Persistence(Error::not_found(format!(
                "no WAL exists for world {world_id} at '{}'",
                wal_path.display()
            ))));
        }
        let snapshot_dir = self.snapshot_dir(world_id);
        let mut wal = Wal::open(&wal_path, durability).map_err(RuntimeError::Persistence)?;
        let has_snapshot =
            Snapshot::find_latest(&snapshot_dir).map_err(RuntimeError::Persistence)?.is_some();
        let mut store = TableStore::new();

        // Two recovery modes (Phase 5 semantics, ADR-010 D5):
        // 1. Snapshot exists — `recover` restores the authoritative schema
        //    into an empty store; the factory then wraps the recovered
        //    state. Factories must create tables only if absent.
        // 2. No snapshot — the WAL carries changes, not DDL, so the factory
        //    defines the schema first and the WAL is replayed into the
        //    world's store (the tables already exist).
        let (mut world, report) = if has_snapshot {
            let report =
                recover(&mut store, &mut wal, &snapshot_dir).map_err(RuntimeError::Persistence)?;
            let world =
                (self.config.factory())(world_id, store, sim_config).map_err(|error| {
                    RuntimeError::Internal(format!(
                        "world factory failed for recovered world {world_id}: {error}"
                    ))
                })?;
            (world, report)
        } else {
            let mut world =
                (self.config.factory())(world_id, store, sim_config).map_err(|error| {
                    RuntimeError::Internal(format!(
                        "world factory failed for recovered world {world_id}: {error}"
                    ))
                })?;
            let report = recover(world.store_mut(), &mut wal, &snapshot_dir)
                .map_err(RuntimeError::Persistence)?;
            (world, report)
        };
        if let Some(tick) = resume_tick {
            world.resume_tick(tick);
        }

        let worker = self.assign_worker()?;
        self.workers
            .get_mut(&worker)
            .expect("assigned worker exists")
            .add_world(world_id);
        self.worlds
            .insert(world_id, WorldEntry::new(world, worker, Some(wal), Some(snapshot_dir)));
        self.metrics.recoveries += 1;
        self.metrics.world_creations += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorldRecovered {
                world: world_id,
                replayed_txs: report.replayed_txs,
            },
        );
        Ok(report)
    }

    /// Starts a `Created` or `Stopped` world (idempotent when already
    /// running). A `Failed` world must be destroyed and recreated/recovered.
    pub fn start_world(&mut self, world_id: WorldId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        if entry.state == WorldLifecycle::Running {
            return Ok(());
        }
        if !entry.state.can_start() {
            return Err(RuntimeError::world_state(world_id, "start", entry.state));
        }
        entry.state = WorldLifecycle::Running;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorldStarted { world: world_id },
        );
        Ok(())
    }

    /// Stops a `Created` or `Running` world, retaining its state
    /// (idempotent when already stopped). Inputs are rejected while stopped;
    /// the logical tick counter continues on restart.
    pub fn stop_world(&mut self, world_id: WorldId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        if entry.state == WorldLifecycle::Stopped {
            return Ok(());
        }
        if !entry.state.can_stop() {
            return Err(RuntimeError::world_state(world_id, "stop", entry.state));
        }
        entry.state = WorldLifecycle::Stopped;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorldStopped { world: world_id },
        );
        Ok(())
    }

    /// Removes a world from the runtime (explicit API). Committed data
    /// remains in the world's WAL on disk — nothing is silently erased
    /// unless the application deletes the persistence directory. Any
    /// partition bound to the world is unregistered first, so messages can
    /// no longer be routed to it.
    pub fn destroy_world(&mut self, world_id: WorldId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if let Some(partition) = self.partition_for_world(world_id) {
            self.unregister_partition(partition)?;
        }
        let entry = self.worlds.remove(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        self.workers
            .get_mut(&entry.worker)
            .expect("entry worker exists")
            .remove_world(world_id);
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::WorldDestroyed { world: world_id },
        );
        Ok(())
    }

    /// Returns a world's status.
    pub fn world_status(&self, world_id: WorldId) -> Result<WorldStatus, RuntimeError> {
        self.worlds
            .get(&world_id)
            .map(WorldEntry::status)
            .ok_or(RuntimeError::UnknownWorld(world_id))
    }

    /// Returns every world's status in deterministic (world id) order.
    pub fn list_worlds(&self) -> Vec<(WorldId, WorldStatus)> {
        self.worlds.iter().map(|(id, entry)| (*id, entry.status())).collect()
    }

    /// Snapshot a world's authoritative state at its current WAL LSN
    /// (Phase 5). Requires persistence.
    pub fn snapshot_world(&mut self, world_id: WorldId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let (snapshot_dir, lsn) = {
            let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
            let snapshot_dir = entry.snapshot_dir.clone().ok_or_else(|| {
                RuntimeError::Persistence(Error::unsupported(
                    "snapshots require persistence to be enabled",
                ))
            })?;
            let wal = entry.wal.as_mut().ok_or_else(|| {
                RuntimeError::Persistence(Error::unsupported(
                    "snapshots require persistence to be enabled",
                ))
            })?;
            (snapshot_dir, wal.lsn().as_u64())
        };
        let entry = self.worlds.get_mut(&world_id).expect("world exists");
        Snapshot::capture(entry.world.store(), lsn).write(&snapshot_dir).map_err(|error| {
            RuntimeError::Persistence(Error::internal(format!(
                "snapshot failed for world {world_id}: {error}"
            )))
        })?;
        self.metrics.snapshots += 1;
        Ok(())
    }

    // ---------------------------------------------------------- partitions

    /// Binds a partition to an existing world (ADR-012 D1).
    ///
    /// Sets the world's partition id, adds `partition` to the topology, and
    /// propagates the updated sorted topology to every registered world so
    /// `send_to` validates against the live partition set. Messages queue
    /// (bounded) and are delivered to the world's **next** tick.
    pub fn register_partition(
        &mut self,
        partition: PartitionId,
        world_id: WorldId,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if self.partitions.contains_key(&partition) {
            return Err(RuntimeError::DuplicatePartition(partition));
        }
        let worker = {
            let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
            entry.world.set_partition(partition);
            entry.worker
        };
        self.topology.insert(partition);
        self.propagate_topology();
        self.partitions.insert(partition, PartitionEntry::new(world_id, worker));
        self.metrics.partitions = self.partitions.len();
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::PartitionRegistered {
                partition,
                world: world_id,
            },
        );
        Ok(())
    }

    /// Unbinds a partition from its world (ADR-012 D1).
    ///
    /// Drops any queued inbound messages (they are runtime-transient) and
    /// removes the partition from the propagated topology. Idempotent for an
    /// unknown partition.
    pub fn unregister_partition(&mut self, partition: PartitionId) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if self.partitions.remove(&partition).is_none() {
            return Ok(());
        }
        self.topology.remove(&partition);
        self.sent_seq.remove(&partition);
        self.propagate_topology();
        self.metrics.partitions = self.partitions.len();
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::PartitionUnregistered { partition },
        );
        Ok(())
    }

    /// Returns a partition's status.
    pub fn partition_status(
        &self,
        partition: PartitionId,
    ) -> Result<PartitionStatus, RuntimeError> {
        let entry = self
            .partitions
            .get(&partition)
            .ok_or(RuntimeError::UnknownPartition(partition))?;
        Ok(PartitionStatus {
            partition,
            world: entry.world,
            worker: entry.worker,
            queued_messages: entry.inbound.len(),
        })
    }

    /// Returns the registered partition ids in ascending order.
    pub fn partitions(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.partitions.keys().copied()
    }

    /// Returns the propagated partition topology (ascending).
    pub fn topology(&self) -> impl Iterator<Item = PartitionId> + '_ {
        self.topology.iter().copied()
    }

    /// Injects an external cross-partition message into the bus (ADR-012
    /// D7) — the control-surface entry for tests and the future Phase 13
    /// gateway. The message is stamped with the sender's current logical tick
    /// and a deterministic sequence number, then delivered to the
    /// destination's next tick like any intra-tick message. Bounded by the
    /// destination's queue; overflow drops with an event + metric.
    pub fn send_message(
        &mut self,
        from: PartitionId,
        to: PartitionId,
        kind: &str,
        payload: nexum_reducer::ReducerArgs,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        if from == to {
            return Err(RuntimeError::InvalidConfig(
                "cannot send a partition message to the sender itself".to_string(),
            ));
        }
        let sent_tick = {
            let entry = self
                .partitions
                .get(&from)
                .ok_or(RuntimeError::UnknownPartition(from))?;
            let world = self.worlds.get(&entry.world).expect("partition world exists");
            world.world.tick_number()
        };
        self.partitions
            .get(&to)
            .ok_or(RuntimeError::UnknownPartition(to))?;
        let seq = self.sent_seq.entry(from).or_insert(0);
        let message = PartitionMessage::new(
            from,
            to,
            sent_tick,
            *seq,
            kind.to_string(),
            payload,
        )
        .map_err(RuntimeError::Core)?;
        *seq += 1;
        Self::enqueue_message(
            &self.config,
            &mut self.metrics,
            &mut self.events,
            &mut self.partitions,
            message,
        );
        Ok(())
    }

    // ------------------------------------------------------------- inputs

    /// Queues a client reducer call for a running world (ADR-013 D3).
    ///
    /// The call executes inside the world's **next tick** (Phase 0c) — the
    /// single commit path is preserved. Rejects: unknown worlds, non-running
    /// worlds, an empty reducer name, and calls beyond the queue bound
    /// (explicit backpressure — the caller receives the error and may retry
    /// with a new invocation; no silent drops). Results flow back through
    /// `TickResult.reducer_results` correlated by request id.
    pub fn submit_reducer_call(
        &mut self,
        world_id: WorldId,
        request_id: u64,
        reducer: impl Into<String>,
        args: ReducerArgs,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        if entry.state != WorldLifecycle::Running {
            return Err(RuntimeError::world_state(world_id, "submit reducer call to", entry.state));
        }
        let call = ReducerCall::new(request_id, reducer, args).map_err(RuntimeError::Core)?;
        if entry.calls.len() >= self.config.max_queued_reducer_calls() {
            self.metrics.reducer_calls_rejected += 1;
            Self::push_event(
                &mut self.events,
                self.config.event_log_limit(),
                RuntimeEvent::ReducerCallRejected {
                    world: world_id,
                    reason: Error::capacity("reducer call queue full"),
                },
            );
            return Err(RuntimeError::ReducerCallRejected {
                world: world_id,
                reason: Error::capacity("reducer call queue full"),
            });
        }
        entry.calls.push_back(call);
        self.metrics.reducer_calls_accepted += 1;
        Ok(())
    }

    /// Queues an input frame for a running world.
    ///
    /// Rejects: unknown worlds, non-running worlds, frames for an
    /// already-passed tick (late input), and frames beyond the queue bound
    /// (explicit backpressure — no silent drops). Frames must be submitted
    /// in tick order; the world's own frame gate rejects a mismatched tick
    /// at execution.
    pub fn submit_input(
        &mut self,
        world_id: WorldId,
        frame: nexum_simulation::InputFrame,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        if entry.state != WorldLifecycle::Running {
            return Err(RuntimeError::world_state(world_id, "submit input to", entry.state));
        }
        if frame.tick() < entry.world.tick_number() {
            self.metrics.inputs_rejected += 1;
            Self::push_event(
                &mut self.events,
                self.config.event_log_limit(),
                RuntimeEvent::InputRejected {
                    world: world_id,
                    reason: Error::invalid_argument("late input: tick already passed"),
                },
            );
            return Err(RuntimeError::InputRejected {
                world: world_id,
                reason: Error::invalid_argument("late input: tick already passed"),
            });
        }
        if entry.inputs.len() >= self.config.max_queued_inputs() {
            self.metrics.inputs_rejected += 1;
            Self::push_event(
                &mut self.events,
                self.config.event_log_limit(),
                RuntimeEvent::InputRejected {
                    world: world_id,
                    reason: Error::capacity("input queue full"),
                },
            );
            return Err(RuntimeError::InputRejected {
                world: world_id,
                reason: Error::capacity("input queue full"),
            });
        }
        entry.inputs.push_back(frame);
        self.metrics.inputs_accepted += 1;
        Ok(())
    }

    /// Advances one running world by one tick, coordinating durability
    /// (WAL first) and observation (subscriptions second) on success
    /// (ADR-010 D4). Returns the world's committed `TickResult` or a runtime
    /// error; a tick failure applies the configured
    /// [`TickFailurePolicy`].
    pub fn tick_once(
        &mut self,
        world_id: WorldId,
    ) -> Result<nexum_simulation::TickResult, RuntimeError> {
        self.ensure_running()?;
        let tick_number = {
            let entry = self.worlds.get(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
            if entry.state != WorldLifecycle::Running {
                return Err(RuntimeError::world_state(world_id, "tick", entry.state));
            }
            entry.world.tick_number()
        };
        // Delivery phase for this single world (ADR-012 D3).
        let delivered = Self::take_deliverable(
            &mut self.partitions,
            &mut self.metrics,
            world_id,
            tick_number,
        );
        let entry = self.worlds.get_mut(&world_id).expect("world exists");
        Self::tick_entry(
            &self.config,
            &mut self.metrics,
            &mut self.events,
            &mut self.partitions,
            entry,
            world_id,
            &delivered,
        )
    }

    /// Advances every running world by exactly one tick, in deterministic
    /// `(worker_id, world_id)` order (ADR-010 D2). Per-world failures are
    /// recorded (events + metrics) and follow the configured tick-failure
    /// policy without affecting other worlds.
    pub fn step(&mut self) -> Result<RuntimeStepReport, RuntimeError> {
        self.ensure_running()?;
        let started = Instant::now();
        // Deterministic execution order: workers ascending, then each
        // worker's worlds ascending (ADR-010 D2).
        let order: Vec<WorldId> = self
            .workers
            .values()
            .flat_map(|worker| worker.worlds().collect::<Vec<_>>())
            .collect();
        let worlds = order
            .iter()
            .filter(|world| {
                self.worlds
                    .get(world)
                    .is_some_and(|entry| entry.state == WorldLifecycle::Running)
            })
            .count();
        let mut report = RuntimeStepReport {
            worlds,
            ticks: 0,
            succeeded: 0,
            failed: 0,
            wal_appends: 0,
            duration_ns: 0,
        };
        // Delivery phase (ADR-012 D3): drain every running world's inbound
        // messages (sent_tick < its next tick) **before any world ticks**, so
        // delivery order never depends on the tick phase's world order.
        let mut delivered: BTreeMap<WorldId, Vec<PartitionMessage>> = BTreeMap::new();
        for world_id in &order {
            let (running, tick_number) = {
                let entry = self.worlds.get(world_id).expect("ordered worlds exist");
                (entry.state == WorldLifecycle::Running, entry.world.tick_number())
            };
            if !running {
                continue;
            }
            let batch = Self::take_deliverable(
                &mut self.partitions,
                &mut self.metrics,
                *world_id,
                tick_number,
            );
            if !batch.is_empty() {
                delivered.insert(*world_id, batch);
            }
        }
        // Tick phase: worlds tick in the same deterministic order; each
        // world's result never depends on its position (partitions do not
        // read each other's state within a step).
        for world_id in order {
            let entry = self.worlds.get_mut(&world_id).expect("ordered worlds exist");
            if entry.state != WorldLifecycle::Running {
                continue;
            }
            report.ticks += 1;
            let wal_before = self.metrics.wal_appends;
            let batch: &[PartitionMessage] = delivered.get(&world_id).map(Vec::as_slice).unwrap_or(&[]);
            match Self::tick_entry(
                &self.config,
                &mut self.metrics,
                &mut self.events,
                &mut self.partitions,
                entry,
                world_id,
                batch,
            ) {
                Ok(_) => {
                    report.succeeded += 1;
                    report.wal_appends += self.metrics.wal_appends - wal_before;
                }
                Err(_) => report.failed += 1,
            }
        }
        report.duration_ns = started.elapsed().as_nanos() as u64;
        Ok(report)
    }

    /// Advances every running world by exactly one tick in the same
    /// deterministic order as [`step`](Self::step) (ADR-010 D2) and returns
    /// each successful world's committed [`TickResult`] alongside its id,
    /// so callers (like the network gateway, ADR-011 D1) can fan results
    /// out per world. Per-world failures are recorded exactly as in `step`
    /// (events, metrics, and the configured [`TickFailurePolicy`]) and are
    /// excluded from the returned results.
    pub fn step_detailed(
        &mut self,
    ) -> Result<Vec<(WorldId, nexum_simulation::TickResult)>, RuntimeError> {
        self.ensure_running()?;
        // Deterministic execution order: workers ascending, then each
        // worker's worlds ascending (ADR-010 D2).
        let order: Vec<WorldId> = self
            .workers
            .values()
            .flat_map(|worker| worker.worlds().collect::<Vec<_>>())
            .collect();
        let mut results = Vec::new();
        // Delivery phase (ADR-012 D3), identical to [`step`](Self::step).
        let mut delivered: BTreeMap<WorldId, Vec<PartitionMessage>> = BTreeMap::new();
        for world_id in &order {
            let (running, tick_number) = {
                let entry = self.worlds.get(world_id).expect("ordered worlds exist");
                (entry.state == WorldLifecycle::Running, entry.world.tick_number())
            };
            if !running {
                continue;
            }
            let batch = Self::take_deliverable(
                &mut self.partitions,
                &mut self.metrics,
                *world_id,
                tick_number,
            );
            if !batch.is_empty() {
                delivered.insert(*world_id, batch);
            }
        }
        for world_id in order {
            let entry = self.worlds.get_mut(&world_id).expect("ordered worlds exist");
            if entry.state != WorldLifecycle::Running {
                continue;
            }
            let batch: &[PartitionMessage] = delivered.get(&world_id).map(Vec::as_slice).unwrap_or(&[]);
            if let Ok(result) = Self::tick_entry(
                &self.config,
                &mut self.metrics,
                &mut self.events,
                &mut self.partitions,
                entry,
                world_id,
                batch,
            ) {
                results.push((world_id, result));
            }
        }
        Ok(results)
    }

    /// Ticks one entry: deliver its inbound batch, gather input, execute,
    /// then coordinate durability, observation, and outbound message
    /// enqueue. The single tick-processing path shared by `step`, `tick_once`
    /// and `step_detailed`. Takes the runtime's mutable fields explicitly so
    /// the caller can keep the `&mut WorldEntry` borrow alive.
    fn tick_entry(
        config: &RuntimeConfig,
        metrics: &mut RuntimeMetrics,
        events: &mut VecDeque<RuntimeEvent>,
        partitions: &mut BTreeMap<PartitionId, PartitionEntry>,
        entry: &mut WorldEntry,
        world_id: WorldId,
        delivered: &[PartitionMessage],
    ) -> Result<nexum_simulation::TickResult, RuntimeError> {
        let frame = entry
            .inputs
            .pop_front()
            .unwrap_or_else(|| nexum_simulation::InputFrame::new(entry.world.tick_number()));
        // Drain the queued client reducer calls (ADR-013 D3) into this tick
        // in FIFO order, bounded by the world's per-tick gate. Overflow
        // stays queued for the next tick — a misconfiguration can never
        // fail a tick or silently drop an accepted call.
        let per_tick = entry.world.config().max_reducer_calls_per_tick();
        let mut calls: Vec<ReducerCall> = Vec::with_capacity(per_tick.min(entry.calls.len()));
        for _ in 0..per_tick {
            match entry.calls.pop_front() {
                Some(call) => calls.push(call),
                None => break,
            }
        }
        let started = Instant::now();

        let result = match entry.world.tick_with_calls(&frame, delivered, &calls) {
            Ok(result) => result,
            Err(tick_error) => {
                metrics.ticks_total += 1;
                metrics.ticks_failed += 1;
                metrics.tick_ns_total += started.elapsed().as_nanos() as u64;
                Self::push_event(
                    events,
                    config.event_log_limit(),
                    RuntimeEvent::TickFailed {
                        world: world_id,
                        tick: tick_error.tick(),
                        error: tick_error.error().clone(),
                    },
                );
                // A failed tick committed nothing (zero authoritative
                // mutation): requeue the drained calls at the front in FIFO
                // order so no accepted call is silently lost (ADR-013 D3).
                // Under `FailWorld` the requeue is moot (the world is dead
                // and the gateway answers the pending calls); under
                // `Continue` the calls execute on the next eligible tick.
                for call in calls.into_iter().rev() {
                    entry.calls.push_front(call);
                }
                match config.tick_failure_policy() {
                    TickFailurePolicy::FailWorld => {
                        entry.state = WorldLifecycle::Failed;
                        metrics.world_failures += 1;
                        Self::push_event(
                            events,
                            config.event_log_limit(),
                            RuntimeEvent::WorldFailed {
                                world: world_id,
                                reason: tick_error.error().clone(),
                            },
                        );
                    }
                    TickFailurePolicy::Continue => {}
                }
                return Err(RuntimeError::Tick {
                    world: world_id,
                    error: tick_error.error().clone(),
                });
            }
        };
        metrics.ticks_total += 1;
        metrics.ticks_succeeded += 1;
        metrics.tick_ns_total += started.elapsed().as_nanos() as u64;
        entry.ticks_run += 1;
        Self::push_event(
            events,
            config.event_log_limit(),
            RuntimeEvent::TickCompleted {
                world: world_id,
                tick: result.tick(),
                duration_ns: started.elapsed().as_nanos() as u64,
            },
        );

        // Durability first (ADR-010 D4).
        let mut persisted = false;
        if let Some(wal) = entry.wal.as_mut() {
            match wal.append(result.tx_id(), result.changes()) {
                Ok(_) => {
                    metrics.wal_appends += 1;
                    persisted = true;
                }
                Err(error) => {
                    metrics.wal_failures += 1;
                    entry.state = WorldLifecycle::Failed;
                    metrics.world_failures += 1;
                    Self::push_event(
                        events,
                        config.event_log_limit(),
                        RuntimeEvent::PersistenceFailure {
                            world: world_id,
                            tick: result.tick(),
                            error: error.clone(),
                        },
                    );
                    return Err(RuntimeError::Persistence(error));
                }
            }
        }
        if persisted {
            // Periodic snapshots (best effort; failures are recorded in
            // metrics, never in world semantics).
            if let Some(interval) = config.snapshot_interval() {
                entry.ticks_since_snapshot += 1;
                if entry.ticks_since_snapshot >= interval
                    && let Some(dir) = entry.snapshot_dir.clone()
                {
                    let lsn = entry.wal.as_ref().expect("persistence enabled").lsn().as_u64();
                    if Snapshot::capture(entry.world.store(), lsn).write(&dir).is_ok() {
                        metrics.snapshots += 1;
                        entry.ticks_since_snapshot = 0;
                    }
                }
            }
        }

        // Observation second (ADR-010 D4): only durable changes.
        let _ = entry.subscriptions.apply_changes(entry.world.store(), result.changes());

        // Outbound message enqueue (ADR-012 D3): committed messages are
        // queued to the destinations for their next tick. Bounded and
        // non-blocking; drops are events + metrics.
        Self::enqueue_outbound(config, metrics, events, partitions, result.outbound());
        Ok(result)
    }

    // --------------------------------------------------------- subscriptions

    /// Subscribes a query against a world's authoritative store. The
    /// subscription observes only future commits (the initial snapshot is
    /// delivered into its buffer).
    pub fn subscribe(
        &mut self,
        world_id: WorldId,
        query: Query,
    ) -> Result<SubscriptionId, RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        entry
            .subscriptions
            .subscribe(entry.world.store(), query)
            .map_err(RuntimeError::Core)
    }

    /// Takes the pending updates of one of a world's subscriptions.
    pub fn drain(
        &mut self,
        world_id: WorldId,
        subscription: SubscriptionId,
    ) -> Result<Vec<SubscriptionUpdate>, RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        entry
            .subscriptions
            .drain(subscription)
            .map_err(RuntimeError::Core)
    }

    /// Ends a subscription.
    pub fn unsubscribe(
        &mut self,
        world_id: WorldId,
        subscription: SubscriptionId,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        entry
            .subscriptions
            .unsubscribe(subscription)
            .map_err(RuntimeError::Core)
    }

    /// Regenerates a subscription's exact view from authoritative state.
    pub fn resync(
        &mut self,
        world_id: WorldId,
        subscription: SubscriptionId,
    ) -> Result<(), RuntimeError> {
        self.ensure_running()?;
        let entry = self.worlds.get_mut(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        entry
            .subscriptions
            .resync(entry.world.store(), subscription)
            .map_err(RuntimeError::Core)
    }

    /// Returns `true` if a subscription is stale.
    pub fn is_stale(
        &self,
        world_id: WorldId,
        subscription: SubscriptionId,
    ) -> Result<bool, RuntimeError> {
        let entry = self.worlds.get(&world_id).ok_or(RuntimeError::UnknownWorld(world_id))?;
        entry
            .subscriptions
            .is_stale(subscription)
            .map_err(RuntimeError::Core)
    }

    // ------------------------------------------------------ events & metrics

    /// Takes every buffered runtime event in order, clearing the log.
    pub fn drain_events(&mut self) -> Vec<RuntimeEvent> {
        self.events.drain(..).collect()
    }

    /// Returns the number of buffered events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Returns a point-in-time metrics snapshot.
    pub fn metrics(&self) -> RuntimeMetrics {
        let mut metrics = self.metrics.clone();
        metrics.workers = self.config.worker_count();
        metrics.worlds = self.worlds.len();
        metrics.running_worlds = self
            .worlds
            .values()
            .filter(|entry| entry.state == WorldLifecycle::Running)
            .count();
        metrics.subscriptions = self
            .worlds
            .values()
            .map(|entry| entry.subscriptions.len())
            .sum();
        metrics.partitions = self.partitions.len();
        metrics.uptime_ns = self.started_at.elapsed().as_nanos() as u64;
        metrics
    }

    // ------------------------------------------------------------ shutdown

    /// Deterministically shuts the runtime down:
    ///
    /// 1. state → `Stopping` (rejects new operations)
    /// 2. stops scheduling (no in-flight ticks — single-threaded)
    /// 3. flushes every world's WAL (the durability contract)
    /// 4. stops workers and worlds (state retained in memory)
    /// 5. releases resources; state → `Stopped`
    ///
    /// Returns `Err(Persistence)` if any flush failed (shutdown still
    /// completes; the failure is reported in events). Idempotent.
    pub fn shutdown(&mut self) -> Result<(), RuntimeError> {
        if self.state == RuntimeState::Stopped {
            return Ok(());
        }
        self.state = RuntimeState::Stopping;

        // Durability contract: flush committed-but-unflushed state.
        let mut flush_error = None;
        for (world, entry) in self.worlds.iter_mut() {
            if let Some(wal) = entry.wal.as_mut()
                && let Err(error) = wal.flush()
            {
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    RuntimeEvent::PersistenceFailure {
                        world: *world,
                        tick: entry.world.tick_number(),
                        error: error.clone(),
                    },
                );
                flush_error = Some(RuntimeError::Persistence(error));
            }
            if entry.state == WorldLifecycle::Running {
                entry.state = WorldLifecycle::Stopped;
            }
        }
        for worker in self.workers.values_mut() {
            worker.set_state(WorkerState::Stopped);
        }
        self.state = RuntimeState::Stopped;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            RuntimeEvent::Shutdown,
        );
        match flush_error {
            Some(error) => Err(error),
            None => Ok(()),
        }
    }

    // ------------------------------------------------------------- helpers

    /// Pushes an event onto the bounded log (drops the oldest when full).
    fn push_event(events: &mut VecDeque<RuntimeEvent>, limit: usize, event: RuntimeEvent) {
        if events.len() >= limit {
            events.pop_front();
        }
        events.push_back(event);
    }

    /// Returns the partition bound to `world_id`, if any.
    fn partition_for_world(&self, world_id: WorldId) -> Option<PartitionId> {
        self.partitions
            .iter()
            .find(|(_, entry)| entry.world == world_id)
            .map(|(id, _)| *id)
    }

    /// Propagates the sorted topology to every registered world (ADR-012
    /// D5). The worlds' own `set_known_partitions` filters self and keeps the
    /// list sorted and deduplicated.
    fn propagate_topology(&mut self) {
        let topology: Vec<PartitionId> = self.topology.iter().copied().collect();
        for entry in self.worlds.values_mut() {
            entry.world.set_known_partitions(topology.clone());
        }
    }

    /// Drains the deliverable inbound messages of the partition bound to
    /// `world_id` — those with `sent_tick < tick_number` (ADR-012 D3). Keeps
    /// the rest queued (e.g. a message that arrives while the destination is
    /// behind; it is delivered on a later tick). Counts delivered messages.
    fn take_deliverable(
        partitions: &mut BTreeMap<PartitionId, PartitionEntry>,
        metrics: &mut RuntimeMetrics,
        world_id: WorldId,
        tick_number: nexum_core::TickId,
    ) -> Vec<PartitionMessage> {
        let Some((_, entry)) = partitions.iter_mut().find(|(_, p)| p.world == world_id) else {
            return Vec::new();
        };
        let mut batch = Vec::new();
        let mut remaining = VecDeque::new();
        while let Some(message) = entry.inbound.pop_front() {
            if message.sent_tick().as_u64() < tick_number.as_u64() {
                batch.push(message);
            } else {
                remaining.push_back(message);
            }
        }
        entry.inbound = remaining;
        metrics.messages_delivered += batch.len() as u64;
        batch
    }

    /// Enqueues the outbound messages of a committed tick to their
    /// destinations (ADR-012 D3). Never blocks; overflow and unknown
    /// destinations drop deterministically with an event + metric.
    fn enqueue_outbound(
        config: &RuntimeConfig,
        metrics: &mut RuntimeMetrics,
        events: &mut VecDeque<RuntimeEvent>,
        partitions: &mut BTreeMap<PartitionId, PartitionEntry>,
        outbound: &[PartitionMessage],
    ) {
        for message in outbound {
            Self::enqueue_message(config, metrics, events, partitions, message.clone());
        }
    }

    /// Enqueues one message into its destination's bounded inbound queue.
    /// The deterministic backpressure policy (ADR-012 D7): a full queue or
    /// an unregistered destination drops the message with an event + metric.
    fn enqueue_message(
        config: &RuntimeConfig,
        metrics: &mut RuntimeMetrics,
        events: &mut VecDeque<RuntimeEvent>,
        partitions: &mut BTreeMap<PartitionId, PartitionEntry>,
        message: PartitionMessage,
    ) {
        let Some(target) = partitions.get_mut(&message.to()) else {
            metrics.messages_dropped += 1;
            Self::push_event(
                events,
                config.event_log_limit(),
                RuntimeEvent::MessageDropped {
                    from: message.from(),
                    to: message.to(),
                    reason: Error::not_found("destination partition is not registered"),
                },
            );
            return;
        };
        if target.inbound.len() >= config.max_queued_partition_messages() {
            metrics.messages_dropped += 1;
            Self::push_event(
                events,
                config.event_log_limit(),
                RuntimeEvent::MessageDropped {
                    from: message.from(),
                    to: message.to(),
                    reason: Error::capacity("destination inbound queue is full"),
                },
            );
            return;
        }
        target.inbound.push_back(message);
        metrics.messages_sent += 1;
    }

    /// Assigns the next **running** worker by deterministic round-robin,
    /// skipping failed workers (ADR-010 D6) so a new world is never owned by
    /// a failed worker. Errors when no running worker exists.
    fn assign_worker(&mut self) -> Result<WorkerId, RuntimeError> {
        let count = self.config.worker_count() as u64;
        let start = self.assign_counter % count;
        for offset in 0..count {
            let index = (start + offset) % count;
            let id = WorkerId::from_u64(index);
            if self
                .workers
                .get(&id)
                .is_some_and(|worker| worker.state() == WorkerState::Running)
            {
                self.assign_counter += 1;
                return Ok(id);
            }
        }
        Err(RuntimeError::Internal(
            "no running worker available to own a world".to_string(),
        ))
    }

    /// Opens (or creates) the per-world WAL and snapshot directory when
    /// persistence is enabled.
    ///
    /// A `create_world` over an id that already has a WAL is rejected:
    /// `Wal::create` would truncate durable history that `recover_world`
    /// could restore, so the caller must recover instead (ADR-010 D5).
    fn open_persistence(
        &mut self,
        world_id: WorldId,
    ) -> Result<(Option<Wal>, Option<PathBuf>), RuntimeError> {
        let Some(dir) = self.config.persistence_dir().cloned() else {
            return Ok((None, None));
        };
        let Some(durability) = self.config.persistence().durability() else {
            return Ok((None, None));
        };
        let world_dir = dir.join(format!("world_{}", world_id.as_u64()));
        let snapshot_dir = world_dir.join("snapshots");
        std::fs::create_dir_all(&snapshot_dir).map_err(|error| {
            RuntimeError::Persistence(Error::internal(format!(
                "cannot create world directory '{}': {error}",
                snapshot_dir.display()
            )))
        })?;
        let wal_path = world_dir.join("log.wal");
        if wal_path.exists() {
            return Err(RuntimeError::Persistence(Error::already_exists(format!(
                "world {world_id} already has a WAL at '{}'; use recover_world instead of create_world",
                wal_path.display()
            ))));
        }
        let wal = Wal::create(&wal_path, durability).map_err(RuntimeError::Persistence)?;
        Ok((Some(wal), Some(snapshot_dir)))
    }

    /// The per-world directory under the persistence root.
    fn world_dir(&self, world_id: WorldId) -> PathBuf {
        self.config
            .persistence_dir()
            .expect("persistence enabled")
            .join(format!("world_{}", world_id.as_u64()))
    }

    /// The per-world snapshot directory.
    fn snapshot_dir(&self, world_id: WorldId) -> PathBuf {
        self.world_dir(world_id).join("snapshots")
    }
}
