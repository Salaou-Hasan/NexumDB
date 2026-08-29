//! The control plane ([`ControlPlane`], [`HealthReport`], ADR-011 D7).
//!
//! A typed, operator-facing API over the runtime: world lifecycle, recovery,
//! status, metrics, health, worker reassignment, and shutdown. It is
//! deliberately **separate** from the realtime client protocol — player
//! messages and operator messages never mix — and never exposes raw storage
//! internals.

use nexum_core::{TickId, WorkerId, WorldId};
use nexum_execution::{InputFrame, PartitionConfig, TickResult};
use nexum_runtime::{
    PartitionStatus, RecoveryReport, Runtime, RuntimeError, RuntimeMetrics, RuntimeState,
    RuntimeStepReport, WorkerStatus,
};

/// A point-in-time health summary for operators.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HealthReport {
    /// The runtime lifecycle state.
    pub state: RuntimeState,
    /// Worlds registered (any lifecycle).
    pub worlds: usize,
    /// Worlds currently running.
    pub running_partitions: usize,
    /// Configured workers.
    pub workers: usize,
    /// Workers currently running.
    pub workers_running: usize,
    /// Active subscriptions across worlds.
    pub subscriptions: usize,
    /// Runtime uptime in nanoseconds.
    pub uptime_ns: u64,
}

/// A typed, operator-facing facade over `&mut Runtime`.
///
/// Obtain one via [`crate::NetworkGateway::control`]. Every method maps 1:1
/// onto the runtime API — the control plane adds no state and no second
/// commit path.
pub struct ControlPlane<'a> {
    runtime: &'a mut Runtime,
}

impl<'a> ControlPlane<'a> {
    pub(crate) fn new(runtime: &'a mut Runtime) -> Self {
        Self { runtime }
    }

    /// Creates a world from the configured factory (deterministic worker
    /// assignment).
    pub fn create_partition(
        &mut self,
        world: WorldId,
        sim: PartitionConfig,
    ) -> Result<(), RuntimeError> {
        self.runtime.create_partition(world, sim)
    }

    /// Reconstructs a world from persisted state (Phase 5 engine).
    pub fn recover_partition(
        &mut self,
        world: WorldId,
        sim: PartitionConfig,
        resume_tick: Option<TickId>,
    ) -> Result<RecoveryReport, RuntimeError> {
        self.runtime.recover_partition(world, sim, resume_tick)
    }

    /// Starts a created/stopped world.
    pub fn start_partition(&mut self, world: WorldId) -> Result<(), RuntimeError> {
        self.runtime.start_partition(world)
    }

    /// Stops a running world (state retained; restarts continue time).
    pub fn stop_partition(&mut self, world: WorldId) -> Result<(), RuntimeError> {
        self.runtime.stop_partition(world)
    }

    /// Removes a world from the runtime (committed data remains on disk).
    pub fn destroy_partition(&mut self, world: WorldId) -> Result<(), RuntimeError> {
        self.runtime.destroy_partition(world)
    }

    /// Returns a world's status.
    pub fn partition_status(&self, world: WorldId) -> Result<PartitionStatus, RuntimeError> {
        self.runtime.partition_status(world)
    }

    /// Returns every world's status in deterministic (world-id) order.
    pub fn list_partitions(&self) -> Vec<(WorldId, PartitionStatus)> {
        self.runtime.list_partitions()
    }

    /// Returns a worker's status.
    pub fn worker_status(&self, worker: WorkerId) -> Result<WorkerStatus, RuntimeError> {
        self.runtime.worker_status(worker)
    }

    /// Returns the configured worker ids.
    pub fn workers(&self) -> impl Iterator<Item = WorkerId> + '_ {
        self.runtime.workers()
    }

    /// Marks a worker failed (its worlds become recoverable).
    pub fn fail_worker(&mut self, worker: WorkerId) -> Result<(), RuntimeError> {
        self.runtime.fail_worker(worker)
    }

    /// Reassigns a world to another running worker.
    pub fn reassign_partition(&mut self, world: WorldId, to: WorkerId) -> Result<(), RuntimeError> {
        self.runtime.reassign_partition(world, to)
    }

    /// Queues an input frame for a running world (bounded, late/capacity
    /// rejection preserved).
    pub fn submit_input(&mut self, world: WorldId, frame: InputFrame) -> Result<(), RuntimeError> {
        self.runtime.submit_input(world, frame)
    }

    /// Advances one running world by one tick.
    pub fn tick_once(&mut self, world: WorldId) -> Result<TickResult, RuntimeError> {
        self.runtime.tick_once(world)
    }

    /// Advances every running world by one tick (deterministic order).
    pub fn step(&mut self) -> Result<RuntimeStepReport, RuntimeError> {
        self.runtime.step()
    }

    /// Returns a metrics snapshot.
    pub fn metrics(&self) -> RuntimeMetrics {
        self.runtime.metrics()
    }

    /// Returns a health summary.
    pub fn health(&self) -> HealthReport {
        let runtime_metrics = self.runtime.metrics();
        HealthReport {
            state: self.runtime.state(),
            worlds: runtime_metrics.worlds,
            running_partitions: runtime_metrics.running_partitions,
            workers: runtime_metrics.workers,
            workers_running: self
                .runtime
                .workers()
                .filter(|worker| {
                    self.runtime
                        .worker_status(*worker)
                        .is_ok_and(|status| status.state == nexum_runtime::WorkerState::Running)
                })
                .count(),
            subscriptions: runtime_metrics.subscriptions,
            uptime_ns: runtime_metrics.uptime_ns,
        }
    }

    /// Deterministcally shuts the runtime down (flush-safe, idempotent).
    pub fn shutdown(&mut self) -> Result<(), RuntimeError> {
        self.runtime.shutdown()
    }
}
