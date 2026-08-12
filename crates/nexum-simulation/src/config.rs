//! Simulation configuration ([`SimulationConfig`], ADR-009).

use nexum_core::{Error, Result};

/// How a world executes the systems of a tick (ADR-011 D1, D7).
///
/// Both modes produce **identical** results for identical worlds, inputs,
/// seeds, and reducer code — the worker count is a pure performance knob.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[allow(clippy::exhaustive_enums)]
pub enum ExecutionMode {
    /// The Phase 9 reference loop: every system runs serially against the
    /// tick's single transaction (the correctness oracle).
    #[default]
    Serial,
    /// The Phase 11 plan-based executor: systems with declared, mutually
    /// disjoint table access run concurrently on `workers` threads inside
    /// the tick's single transaction.
    Parallel(usize),
}

/// The configuration of a simulation world: the deterministic RNG seed, the
/// execution mode, and the bounded-resource limits that keep a world (and
/// its inputs, events, and schedule) from growing without bound (ADR-009
/// D1).
///
/// The seed is part of the determinism contract: the same seed + inputs +
/// systems reproduce the same simulation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct SimulationConfig {
    seed: u64,
    execution: ExecutionMode,
    max_commands_per_frame: usize,
    max_events_per_tick: usize,
    max_scheduled_events: usize,
    max_messages_per_tick: usize,
    max_message_kind_len: usize,
    max_message_args: usize,
    max_reducer_calls_per_tick: usize,
    max_reducer_name_len: usize,
    max_reducer_args: usize,
}

impl Default for SimulationConfig {
    fn default() -> Self {
        Self {
            seed: 0,
            execution: ExecutionMode::default(),
            max_commands_per_frame: 10_000,
            max_events_per_tick: 10_000,
            max_scheduled_events: 10_000,
            max_messages_per_tick: 10_000,
            max_message_kind_len: 256,
            max_message_args: 10_000,
            max_reducer_calls_per_tick: 10_000,
            max_reducer_name_len: 256,
            max_reducer_args: 10_000,
        }
    }
}

impl SimulationConfig {
    /// Creates a configuration with default bounds and seed 0.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the deterministic RNG seed.
    pub fn with_seed(mut self, seed: u64) -> Self {
        self.seed = seed;
        self
    }

    /// Selects how systems execute within a tick: `Serial` (the Phase 9
    /// reference loop, the default) or `Parallel(workers)` (the Phase 11
    /// planner). Both produce identical results (ADR-011 D7).
    pub fn with_execution(mut self, execution: ExecutionMode) -> Self {
        self.execution = execution;
        self
    }

    /// Sets the maximum number of commands accepted in one input frame.
    pub fn with_max_commands_per_frame(mut self, max: usize) -> Self {
        self.max_commands_per_frame = max;
        self
    }

    /// Sets the maximum number of events (emitted by systems or reducers)
    /// a single tick may buffer.
    pub fn with_max_events_per_tick(mut self, max: usize) -> Self {
        self.max_events_per_tick = max;
        self
    }

    /// Sets the maximum number of pending scheduled events.
    pub fn with_max_scheduled_events(mut self, max: usize) -> Self {
        self.max_scheduled_events = max;
        self
    }

    /// Sets the maximum number of outbound partition messages one tick may
    /// produce (ADR-012 D7).
    pub fn with_max_messages_per_tick(mut self, max: usize) -> Self {
        self.max_messages_per_tick = max;
        self
    }

    /// Sets the maximum length of a partition message kind (ADR-012 D7).
    pub fn with_max_message_kind_len(mut self, max: usize) -> Self {
        self.max_message_kind_len = max;
        self
    }

    /// Sets the maximum number of payload arguments in one partition message
    /// (ADR-012 D7).
    pub fn with_max_message_args(mut self, max: usize) -> Self {
        self.max_message_args = max;
        self
    }

    /// Sets the maximum number of client reducer calls one tick may execute
    /// (ADR-013 D3).
    pub fn with_max_reducer_calls_per_tick(mut self, max: usize) -> Self {
        self.max_reducer_calls_per_tick = max;
        self
    }

    /// Sets the maximum length of a reducer call name (ADR-013 D3).
    pub fn with_max_reducer_name_len(mut self, max: usize) -> Self {
        self.max_reducer_name_len = max;
        self
    }

    /// Sets the maximum number of arguments in one reducer call (ADR-013
    /// D3).
    pub fn with_max_reducer_args(mut self, max: usize) -> Self {
        self.max_reducer_args = max;
        self
    }

    /// Validates the bounds (all must be non-zero).
    pub fn validate(&self) -> Result<()> {
        if self.max_commands_per_frame == 0 {
            return Err(Error::invalid_argument(
                "max_commands_per_frame must be greater than zero",
            ));
        }
        if self.max_events_per_tick == 0 {
            return Err(Error::invalid_argument(
                "max_events_per_tick must be greater than zero",
            ));
        }
        if self.max_scheduled_events == 0 {
            return Err(Error::invalid_argument(
                "max_scheduled_events must be greater than zero",
            ));
        }
        if let ExecutionMode::Parallel(0) = self.execution {
            return Err(Error::invalid_argument(
                "parallel execution requires at least one worker",
            ));
        }
        if self.max_messages_per_tick == 0 {
            return Err(Error::invalid_argument(
                "max_messages_per_tick must be greater than zero",
            ));
        }
        if self.max_message_kind_len == 0 {
            return Err(Error::invalid_argument(
                "max_message_kind_len must be greater than zero",
            ));
        }
        if self.max_message_args == 0 {
            return Err(Error::invalid_argument(
                "max_message_args must be greater than zero",
            ));
        }
        if self.max_reducer_calls_per_tick == 0 {
            return Err(Error::invalid_argument(
                "max_reducer_calls_per_tick must be greater than zero",
            ));
        }
        if self.max_reducer_name_len == 0 {
            return Err(Error::invalid_argument(
                "max_reducer_name_len must be greater than zero",
            ));
        }
        if self.max_reducer_args == 0 {
            return Err(Error::invalid_argument(
                "max_reducer_args must be greater than zero",
            ));
        }
        Ok(())
    }

    /// Returns the deterministic RNG seed.
    pub fn seed(&self) -> u64 {
        self.seed
    }

    /// Returns the configured execution mode.
    pub fn execution(&self) -> ExecutionMode {
        self.execution
    }

    /// Returns the maximum commands accepted in one input frame.
    pub fn max_commands_per_frame(&self) -> usize {
        self.max_commands_per_frame
    }

    /// Returns the maximum events a single tick may buffer.
    pub fn max_events_per_tick(&self) -> usize {
        self.max_events_per_tick
    }

    /// Returns the maximum number of pending scheduled events.
    pub fn max_scheduled_events(&self) -> usize {
        self.max_scheduled_events
    }

    /// Returns the maximum number of outbound partition messages one tick
    /// may produce.
    pub fn max_messages_per_tick(&self) -> usize {
        self.max_messages_per_tick
    }

    /// Returns the maximum length of a partition message kind.
    pub fn max_message_kind_len(&self) -> usize {
        self.max_message_kind_len
    }

    /// Returns the maximum number of payload arguments in one partition
    /// message.
    pub fn max_message_args(&self) -> usize {
        self.max_message_args
    }

    /// Returns the maximum number of client reducer calls one tick may
    /// execute.
    pub fn max_reducer_calls_per_tick(&self) -> usize {
        self.max_reducer_calls_per_tick
    }

    /// Returns the maximum length of a reducer call name.
    pub fn max_reducer_name_len(&self) -> usize {
        self.max_reducer_name_len
    }

    /// Returns the maximum number of arguments in one reducer call.
    pub fn max_reducer_args(&self) -> usize {
        self.max_reducer_args
    }
}
