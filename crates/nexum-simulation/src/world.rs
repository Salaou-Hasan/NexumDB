//! The [`World`]: one authoritative simulation partition (ADR-009 D1, D2).
//!
//! A world owns the authoritative [`TableStore`], the ordered system
//! registry, a native [`ReducerRegistry`], an optional
//! [`WasmModuleRegistry`], the deterministic schedule, and the logical tick
//! counter. **One tick = one transaction** (ADR-009 D2):
//!
//! ```text
//! Tick N
//!   â”œâ”€â”€ frame validation           (pre-tick; consumes nothing on error)
//!   â”œâ”€â”€ scheduled events due at N  (reducer invocations, by (tick, id))
//!   â”œâ”€â”€ systems in order           (priority asc, SystemId tie-break)
//!   â”œâ”€â”€ commit â†’ Vec<Change>
//!   â””â”€â”€ return TickResult { tick, tx_id, changes, events }
//! ```
//!
//! Every failure â€” a system error, a reducer rejection, a WASM trap or fuel
//! exhaustion, a panic, an OCC conflict â€” aborts the tick transaction: zero
//! authoritative mutation, zero committed changes, zero events. The tick
//! counter advances on both success and failure; failed ticks are
//! deterministic outcomes (ADR-009 D6).
//!
//! Durability and observation stay caller-owned, at the exact boundary
//! reducers already use (ADR-009 D8):
//!
//! ```rust
//! # use nexum_core::{ColumnType, TableSchema, WorldId};
//! # use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
//! # use nexum_tx::Transaction;
//! # let mut store = nexum_table::TableStore::new();
//! # store.create_table(
//! #     TableSchema::builder("counter")
//! #         .column("tick", ColumnType::U64)
//! #         .primary_key(&["tick"])
//! #         .build().unwrap(),
//! # ).unwrap();
//! # let mut world = World::new(WorldId::from_u64(0), store, SimulationConfig::new()).unwrap();
//! # world.add_system(SystemDefinition::new(
//! #     nexum_core::SystemId::from_u64(0), "count", 0,
//! #     |ctx, _| { ctx.insert("counter", nexum_table::row![ctx.tick().as_u64()])?; Ok(()) },
//! # ).unwrap()).unwrap();
//! # let frame = InputFrame::new(nexum_core::TickId::from_u64(0));
//! let result = world.tick(&frame).expect("tick committed"); // one atomic commit
//! let changes = result.changes();
//! # let _ = Transaction::new; // keep imports meaningful
//! # Ok::<(), nexum_core::Error>(())
//! ```

use std::collections::BTreeMap;
use std::fmt;

use nexum_core::{Error, PartitionId, Result, SystemId, TickId, TransactionId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerEvent, ReducerRegistry};
use nexum_storage::Change;
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wasm::WasmModuleRegistry;

use crate::calls::{ReducerCall, ReducerCallResult};
use crate::config::{ExecutionMode, SimulationConfig};
use crate::context::append_events;
use crate::input::InputFrame;
use crate::parallel;
use crate::partition::PartitionMessage;
use crate::schedule::{Schedule, ScheduledEvent};
use crate::systems::{SystemDefinition, SystemRegistry};

/// The successful outcome of one tick: the committed transaction and its
/// changes and events.
///
/// `changes` is the exact committed `Vec<Change>` â€” the same boundary the
/// WAL and the subscription engine consume. The runtime appends
/// `result.changes` to the WAL with `result.tx_id` and fans them to the
/// `SubscriptionRegistry`, in tick order (ADR-009 D8).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickResult {
    tick: TickId,
    tx_id: TransactionId,
    changes: Vec<Change>,
    events: Vec<ReducerEvent>,
    outbound: Vec<PartitionMessage>,
    reducer_results: Vec<ReducerCallResult>,
}

impl TickResult {
    /// Builds a result from a committed tick's parts.
    pub(crate) fn new(
        tick: TickId,
        tx_id: TransactionId,
        changes: Vec<Change>,
        events: Vec<ReducerEvent>,
        outbound: Vec<PartitionMessage>,
        reducer_results: Vec<ReducerCallResult>,
    ) -> Self {
        Self {
            tick,
            tx_id,
            changes,
            events,
            outbound,
            reducer_results,
        }
    }

    /// Returns the tick that committed.
    pub fn tick(&self) -> TickId {
        self.tick
    }

    /// Returns the id of the transaction that committed this tick.
    pub fn tx_id(&self) -> TransactionId {
        self.tx_id
    }

    /// Returns the committed change records, in commit order.
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Returns the emitted events (by systems and reducers), in `emit`
    /// order.
    pub fn events(&self) -> &[ReducerEvent] {
        &self.events
    }

    /// Returns the outbound cross-partition messages committed with this
    /// tick, in `send_to` order (ADR-012 D2). The runtime delivers them to
    /// the destinations' next ticks.
    pub fn outbound(&self) -> &[PartitionMessage] {
        &self.outbound
    }

    /// Returns the client reducer-call results executed during this tick,
    /// in call order (ADR-013 D3). The runtime exposes them to the network
    /// gateway, which routes each result back to the requesting client by
    /// request id.
    pub fn reducer_results(&self) -> &[ReducerCallResult] {
        &self.reducer_results
    }
}

/// A failed tick: the tick that failed and the underlying error.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TickError {
    tick: TickId,
    error: Error,
}

impl TickError {
    /// Builds an error for `tick`.
    pub(crate) fn new(tick: TickId, error: Error) -> Self {
        Self { tick, error }
    }

    /// Returns the tick that failed.
    pub fn tick(&self) -> TickId {
        self.tick
    }

    /// Returns the underlying error.
    pub fn error(&self) -> &Error {
        &self.error
    }
}

impl fmt::Display for TickError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "tick {} failed: {}", self.tick, self.error)
    }
}

impl std::error::Error for TickError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}

/// Phase-level wall-time breakdown of the most recent tick (Phase 27b
/// instrumentation): reducer calls, systems, and validate+commit. Purely
/// observational â€” never influences simulation semantics.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TickBreakdown {
    /// Phase 0a-0c: message handlers, scheduled events, client calls.
    pub calls_ns: u64,
    /// Phase 1: systems execution.
    pub systems_ns: u64,
    /// OCC validation + atomic apply (commit).
    pub commit_ns: u64,
}

impl TickBreakdown {
    /// Total instrumented nanoseconds.
    pub fn total_ns(&self) -> u64 {
        self.calls_ns + self.systems_ns + self.commit_ns
    }
}

/// One authoritative simulation partition (ADR-009 D1, ADR-012 D1).
#[derive(Debug)]
pub struct World {
    id: WorldId,
    /// The partition this world is registered under (the message-bus
    /// address; defaults to the world id's raw value).
    partition: PartitionId,
    /// The sorted topology of partitions this world may message (ADR-012
    /// D5). Set by the runtime on registration.
    known: Vec<PartitionId>,
    store: TableStore,
    config: SimulationConfig,
    /// The next tick to run (logical time).
    tick: u64,
    systems: SystemRegistry,
    native: ReducerRegistry,
    wasm: Option<WasmModuleRegistry>,
    schedule: Schedule,
    /// Per-reducer execution profile (Phase 21.5 instrumentation): name â†’
    /// (calls, cumulative wall ns). Empty unless profiling is enabled in
    /// the simulation config; never influences semantics.
    reducer_profile: BTreeMap<String, (u64, u64)>,
    /// Phase breakdown of the most recent tick (Phase 27b instrumentation).
    last_breakdown: TickBreakdown,
}

impl World {
    /// Creates a world owning `store`, with `config` bounds validated.
    pub fn new(id: WorldId, store: TableStore, config: SimulationConfig) -> Result<Self> {
        config.validate()?;
        let schedule = Schedule::new(config.max_scheduled_events());
        Ok(Self {
            id,
            partition: PartitionId::from_u64(id.as_u64()),
            known: Vec::new(),
            store,
            config,
            tick: 0,
            systems: SystemRegistry::new(),
            native: ReducerRegistry::new(),
            wasm: None,
            schedule,
            reducer_profile: BTreeMap::new(),
            last_breakdown: TickBreakdown::default(),
        })
    }

    /// Returns the world id.
    pub fn id(&self) -> WorldId {
        self.id
    }

    /// Returns the partition this world is registered under.
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Sets the partition this world is registered under (ADR-012 D1).
    /// Called by the runtime on registration; defaults to the world id's raw
    /// value.
    pub fn set_partition(&mut self, partition: PartitionId) {
        self.partition = partition;
    }

    /// Returns the sorted topology of partitions this world may message.
    pub fn known_partitions(&self) -> &[PartitionId] {
        &self.known
    }

    /// Sets the sorted topology of partitions this world may message
    /// (ADR-012 D5). Duplicates and the world's own partition are removed;
    /// the result is sorted deterministically.
    pub fn set_known_partitions(&mut self, mut known: Vec<PartitionId>) {
        known.sort_unstable();
        known.dedup();
        known.retain(|id| *id != self.partition);
        self.known = known;
    }

    /// Returns the authoritative store.
    pub fn store(&self) -> &TableStore {
        &self.store
    }

    /// Returns the authoritative store mutably (setup before simulation).
    pub fn store_mut(&mut self) -> &mut TableStore {
        &mut self.store
    }

    /// Returns the configuration.
    pub fn config(&self) -> &SimulationConfig {
        &self.config
    }

    /// Returns the next tick that `tick` will execute.
    pub fn tick_number(&self) -> TickId {
        TickId::from_u64(self.tick)
    }

    /// Returns the accumulated per-reducer execution profile (Phase 21.5
    /// instrumentation): reducer name â†’ (calls, cumulative wall ns). Empty
    /// unless profiling is enabled in the simulation config.
    pub fn reducer_profile(&self) -> &BTreeMap<String, (u64, u64)> {
        &self.reducer_profile
    }

    /// Resets the accumulated per-reducer execution profile.
    pub fn clear_reducer_profile(&mut self) {
        self.reducer_profile.clear();
    }

    /// Returns the phase breakdown of the most recent tick (Phase 27b
    /// instrumentation): calls / systems / commit wall times.
    pub fn last_tick_breakdown(&self) -> TickBreakdown {
        self.last_breakdown
    }

    /// Resumes logical time at `tick` (ADR-010 D5).
    ///
    /// Used by the runtime's recovery orchestration: the WAL records
    /// changes, not tick counters, so after recovering a world from a
    /// snapshot + WAL replay the application tells the world where its
    /// logical time was when it stopped. The frame gate then accepts inputs
    /// for `tick` onward. Must be called before ticking; any future tick
    /// number is allowed.
    pub fn resume_tick(&mut self, tick: TickId) {
        self.tick = tick.as_u64();
    }

    // --------------------------------------------------------------- systems

    /// Registers a system in deterministic `(priority, id)` order.
    pub fn add_system(&mut self, definition: SystemDefinition) -> Result<()> {
        self.systems.register(definition)
    }

    /// Removes a system by id.
    pub fn remove_system(&mut self, id: SystemId) -> Result<()> {
        self.systems.remove(id)
    }

    /// Looks up a system by id.
    pub fn system(&self, id: SystemId) -> Option<&SystemDefinition> {
        self.systems.lookup(id)
    }

    /// Returns every system in deterministic execution order.
    pub fn systems(&self) -> &[SystemDefinition] {
        self.systems.ordered()
    }

    // -------------------------------------------------------------- reducers

    /// Returns the native reducer registry (register reducers here before
    /// they can be invoked by systems or scheduled events).
    pub fn native(&self) -> &ReducerRegistry {
        &self.native
    }

    /// Returns the native reducer registry mutably.
    pub fn native_mut(&mut self) -> &mut ReducerRegistry {
        &mut self.native
    }

    /// Returns the WASM module registry, if configured.
    pub fn wasm(&self) -> Option<&WasmModuleRegistry> {
        self.wasm.as_ref()
    }

    /// Returns the WASM module registry mutably, if configured.
    pub fn wasm_mut(&mut self) -> Option<&mut WasmModuleRegistry> {
        self.wasm.as_mut()
    }

    /// Installs a WASM module registry (register modules on it before they
    /// can be invoked by systems or scheduled events).
    pub fn set_wasm(&mut self, registry: WasmModuleRegistry) {
        self.wasm = Some(registry);
    }

    /// Removes the WASM module registry; `invoke_wasm` then fails with
    /// `NotFound`.
    pub fn clear_wasm(&mut self) {
        self.wasm = None;
    }

    // -------------------------------------------------------------- schedule

    /// Schedules `reducer` with `args` to run at the start of `at_tick`.
    ///
    /// Returns the unique event id (for cancellation).
    pub fn schedule(
        &mut self,
        at_tick: TickId,
        reducer: impl Into<String>,
        args: ReducerArgs,
    ) -> Result<u64> {
        self.schedule.schedule(at_tick, reducer, args)
    }

    /// Cancels a pending scheduled event.
    pub fn cancel_scheduled(&mut self, id: u64) -> Result<()> {
        self.schedule.cancel(id)
    }

    /// Returns the pending scheduled events in `(at_tick, id)` order.
    pub fn scheduled_events(&self) -> &[ScheduledEvent] {
        self.schedule.pending()
    }

    // ----------------------------------------------------------------- tick

    /// Advances the simulation by one tick (ADR-009 D2, D6) with no inbound
    /// cross-partition messages and no client reducer calls.
    ///
    /// Delegates to [`tick_messages`](Self::tick_messages) with an empty
    /// delivery batch.
    pub fn tick(&mut self, inputs: &InputFrame) -> std::result::Result<TickResult, TickError> {
        self.tick_messages(inputs, &[])
    }

    /// Advances the simulation by one tick, delivering the inbound
    /// cross-partition messages (ADR-012 D3, D4) with no client reducer
    /// calls.
    ///
    /// Delegates to [`tick_with_calls`](Self::tick_with_calls) with an empty
    /// call batch, so every Phase 9â€“12 call site is unchanged.
    pub fn tick_messages(
        &mut self,
        inputs: &InputFrame,
        delivered: &[PartitionMessage],
    ) -> std::result::Result<TickResult, TickError> {
        self.tick_with_calls(inputs, delivered, &[])
    }

    /// Advances the simulation by one tick, delivering the inbound
    /// cross-partition messages (ADR-012 D3, D4) and executing the queued
    /// client reducer calls (ADR-013 D3).
    ///
    /// Validates `inputs` against the next tick, the delivery batch against
    /// this partition (budget, destination, and a deterministic
    /// `(sent_tick, from, seq)` sort), and the call batch against the
    /// configured bounds. Execution order is fixed and deterministic:
    ///
    /// ```text
    /// Phase 0a â€” delivered messages' handler reducers (batch order)
    /// Phase 0b â€” scheduled events due now ((at_tick, id) order)
    /// Phase 0c â€” client reducer calls (call order): each runs against a
    ///            branch of the tick transaction; success absorbs into the
    ///            tick tx, failure discards the branch and records a typed
    ///            per-call error while the tick continues (ADR-013 D3)
    /// Phase 1  â€” systems, in deterministic (priority, id) order
    /// ```
    ///
    /// All phases run inside **one transaction** and commit atomically. On
    /// success returns the committed changes, events, outbound messages, and
    /// reducer-call results; on any failure returns [`TickError`] with zero
    /// authoritative mutation. The tick counter advances on both outcomes.
    pub fn tick_with_calls(
        &mut self,
        inputs: &InputFrame,
        delivered: &[PartitionMessage],
        calls: &[ReducerCall],
    ) -> std::result::Result<TickResult, TickError> {
        let tick_id = TickId::from_u64(self.tick);

        // Frame gate: an invalid frame consumes nothing (ADR-009 D6).
        if inputs.tick() != tick_id {
            return Err(TickError::new(
                tick_id,
                Error::invalid_argument(format!(
                    "input frame is for tick {} but the world is at tick {tick_id}",
                    inputs.tick()
                )),
            ));
        }
        if inputs.commands().len() > self.config.max_commands_per_frame() {
            return Err(TickError::new(
                tick_id,
                Error::invalid_argument(format!(
                    "input frame has {} commands, exceeding the configured limit of {}",
                    inputs.commands().len(),
                    self.config.max_commands_per_frame()
                )),
            ));
        }
        // Delivery gate: the batch must be bounded and targeted at this
        // partition. A rejected batch is consumed (never requeued) â€” it can
        // only be produced by an internal inconsistency.
        if delivered.len() > self.config.max_messages_per_tick() {
            return Err(TickError::new(
                tick_id,
                Error::capacity(format!(
                    "delivered batch has {} messages, exceeding the configured limit of {}",
                    delivered.len(),
                    self.config.max_messages_per_tick()
                )),
            ));
        }
        for message in delivered {
            if message.to() != self.partition {
                return Err(TickError::new(
                    tick_id,
                    Error::invalid_argument(format!(
                        "delivered message for partition {} reached partition {}",
                        message.to(),
                        self.partition
                    )),
                ));
            }
        }
        // Call gate (ADR-013 D3): the batch is bounded and every call name
        // and argument set is within the configured limits. A rejected batch
        // is consumed (never requeued) â€” it can only be produced by a gateway
        // that failed to enforce its own bounds.
        if calls.len() > self.config.max_reducer_calls_per_tick() {
            return Err(TickError::new(
                tick_id,
                Error::capacity(format!(
                    "reducer call batch has {} calls, exceeding the configured limit of {}",
                    calls.len(),
                    self.config.max_reducer_calls_per_tick()
                )),
            ));
        }
        for call in calls {
            if call.reducer().len() > self.config.max_reducer_name_len() {
                return Err(TickError::new(
                    tick_id,
                    Error::invalid_argument(format!(
                        "reducer call name is {} bytes, exceeding the configured limit of {}",
                        call.reducer().len(),
                        self.config.max_reducer_name_len()
                    )),
                ));
            }
            if call.args().len() > self.config.max_reducer_args() {
                return Err(TickError::new(
                    tick_id,
                    Error::invalid_argument(format!(
                        "reducer call has {} arguments, exceeding the configured limit of {}",
                        call.args().len(),
                        self.config.max_reducer_args()
                    )),
                ));
            }
        }
        self.tick += 1;

        // Deterministic delivery order: (sent_tick, from, seq) (ADR-012 D5).
        let mut batch: Vec<&PartitionMessage> = delivered.iter().collect();
        batch.sort_by_key(|message| {
            (
                message.sent_tick().as_u64(),
                message.from().as_u64(),
                message.seq(),
            )
        });

        let due = self.schedule.take_due(tick_id);
        let world_id = self.id;
        let partition = self.partition;
        let known = self.known.clone();
        let seed = self.config.seed();
        let max_events = self.config.max_events_per_tick();
        let max_messages = self.config.max_messages_per_tick();
        let max_kind_len = self.config.max_message_kind_len();
        let max_args = self.config.max_message_args();
        let execution = self.config.execution();
        // Per-reducer profiling accumulator (Phase 21.5): taken out so the
        // tick body's `&mut self.store` borrow doesn't conflict with the
        // instrumentation writes; restored before returning.
        let profiling_enabled = self.config.reducer_profiling();
        let mut reducer_profile = std::mem::take(&mut self.reducer_profile);

        let mut tx = Transaction::begin(&mut self.store);
        let store = &self.store;
        let systems = &self.systems;
        let native = &self.native;
        let wasm = self.wasm.as_ref();
        let mut tick_events: Vec<ReducerEvent> = Vec::new();
        let mut outbound: Vec<PartitionMessage> = Vec::new();
        let mut reducer_results: Vec<ReducerCallResult> = Vec::new();

        let mut calls_ns = 0u64;
        let mut systems_ns = 0u64;
        let result = (|| -> Result<()> {
            // Phase 0a â€” delivered cross-partition messages, in the
            // deterministic batch order. Each invokes the handler reducer
            // named by its kind against the tick tx (native first, WASM
            // fallback).
            for message in &batch {
                let started = std::time::Instant::now();
                let (_, events) = invoke_handler(store, &mut tx, native, wasm, message)?;
                record_reducer(
                    &mut reducer_profile,
                    profiling_enabled,
                    message.kind(),
                    started.elapsed().as_nanos() as u64,
                );
                append_events(&mut tick_events, events, max_events)?;
            }

            // Phase 0b â€” scheduled events due this tick, in (at_tick, id)
            // order. Each is a reducer invocation against the tick tx.
            for event in &due {
                let started = std::time::Instant::now();
                let (_, events) =
                    native.invoke_in_tx(store, &mut tx, event.reducer(), event.args())?;
                record_reducer(
                    &mut reducer_profile,
                    profiling_enabled,
                    event.reducer(),
                    started.elapsed().as_nanos() as u64,
                );
                append_events(&mut tick_events, events, max_events)?;
            }

            // Phase 0c â€” client reducer calls, in call order (ADR-013 D3).
            // Phase 22.5: execute directly against the tick transaction
            // with snapshot/rollback instead of branch/absorb. This
            // eliminates the ~100 Âµs absorb overhead per call by keeping
            // writes in the parent tx and only rolling back on failure.
            // Resolution is native first, WASM fallback, then `NotFound`.
            let calls_started = std::time::Instant::now();
            for call in calls {
                let snapshot = tx.snapshot();
                let started = std::time::Instant::now();
                let outcome =
                    invoke_reducer(store, &mut tx, native, wasm, call.reducer(), call.args());
                record_reducer(
                    &mut reducer_profile,
                    profiling_enabled,
                    call.reducer(),
                    started.elapsed().as_nanos() as u64,
                );
                match outcome {
                    Ok((value, events)) => {
                        reducer_results.push(ReducerCallResult::ok(call.request_id(), value));
                        append_events(&mut tick_events, events, max_events)?;
                    }
                    Err(error) => {
                        tx.rollback(snapshot);
                        reducer_results.push(ReducerCallResult::err(call.request_id(), error));
                    }
                }
            }

            // Phase 1 â€” systems, in deterministic (priority, id) order.
            // ExecutionMode::Serial is the Phase 9 reference loop; Parallel
            // uses the ADR-011 planner. Both produce identical results.
            calls_ns = calls_started.elapsed().as_nanos() as u64;
            let systems_started = std::time::Instant::now();
            match execution {
                ExecutionMode::Serial => {
                    for definition in systems.ordered() {
                        parallel::run_system(
                            definition,
                            &mut tx,
                            store,
                            native,
                            wasm,
                            world_id,
                            partition,
                            &known,
                            tick_id,
                            seed,
                            max_events,
                            max_messages,
                            max_kind_len,
                            max_args,
                            &mut tick_events,
                            &mut outbound,
                            inputs,
                        )?;
                    }
                }
                ExecutionMode::Parallel(workers) => {
                    let plan = parallel::TickPlan::build(systems.ordered(), store)?;
                    for group in plan.groups() {
                        if group.systems().len() == 1 {
                            // Singleton groups run on the serial path against
                            // the tick transaction â€” the Phase 9 semantics
                            // for opaque or conflicting systems.
                            let definition = &systems.ordered()[group.systems()[0]];
                            parallel::run_system(
                                definition,
                                &mut tx,
                                store,
                                native,
                                wasm,
                                world_id,
                                partition,
                                &known,
                                tick_id,
                                seed,
                                max_events,
                                max_messages,
                                max_kind_len,
                                max_args,
                                &mut tick_events,
                                &mut outbound,
                                inputs,
                            )?;
                        } else {
                            parallel::execute_group(
                                group,
                                systems.ordered(),
                                &mut tx,
                                store,
                                native,
                                wasm,
                                world_id,
                                partition,
                                &known,
                                tick_id,
                                seed,
                                max_events,
                                max_messages,
                                max_kind_len,
                                max_args,
                                inputs,
                                workers,
                                &mut tick_events,
                                &mut outbound,
                            )?;
                        }
                    }
                }
            }
            systems_ns = systems_started.elapsed().as_nanos() as u64;
            Ok(())
        })();

        self.last_breakdown = TickBreakdown {
            calls_ns,
            systems_ns,
            commit_ns: 0,
        };
        self.reducer_profile = reducer_profile;
        match result {
            Ok(()) => {
                let commit_started = std::time::Instant::now();
                let committed = tx.commit(&mut self.store);
                self.last_breakdown.commit_ns = commit_started.elapsed().as_nanos() as u64;
                match committed {
                    Ok(changes) => Ok(TickResult::new(
                        tick_id,
                        tx.id(),
                        changes,
                        tick_events,
                        outbound,
                        reducer_results,
                    )),
                    Err(error) => {
                        let _ = tx.abort();
                        Err(TickError::new(tick_id, error))
                    }
                }
            }
            Err(error) => {
                let _ = tx.abort();
                Err(TickError::new(tick_id, error))
            }
        }
    }
}

/// Records one reducer invocation in the per-reducer profile (Phase 21.5
/// instrumentation). No-op unless profiling is enabled; never influences
/// simulation semantics.
fn record_reducer(profile: &mut BTreeMap<String, (u64, u64)>, enabled: bool, name: &str, ns: u64) {
    if enabled {
        let entry = profile.entry(name.to_string()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += ns;
    }
}

/// Invokes the handler reducer for a delivered cross-partition message
/// against the tick transaction (ADR-012 D4).
///
/// Resolution is native-first, then WASM (when a registry is configured),
/// then `NotFound` â€” an unhandled kind fails the destination tick
/// deterministically with zero mutation.
fn invoke_handler(
    store: &TableStore,
    tx: &mut Transaction,
    native: &ReducerRegistry,
    wasm: Option<&WasmModuleRegistry>,
    message: &PartitionMessage,
) -> Result<(Value, Vec<ReducerEvent>)> {
    invoke_reducer(store, tx, native, wasm, message.kind(), message.payload()).map_err(|error| {
        match error {
            Error::NotFound(_) => Error::not_found(format!(
                "no handler registered for message kind '{}' on partition {}",
                message.kind(),
                message.to()
            )),
            other => other,
        }
    })
}

/// Invokes a named reducer against a transaction (ADR-013 D3).
///
/// Lookup-first resolution: native, then WASM (when a registry is
/// configured), then `NotFound`. The reducer's own errors â€” including a
/// `NotFound` from a missing argument â€” propagate unchanged once the reducer
/// is found.
fn invoke_reducer(
    store: &TableStore,
    tx: &mut Transaction,
    native: &ReducerRegistry,
    wasm: Option<&WasmModuleRegistry>,
    name: &str,
    args: &ReducerArgs,
) -> Result<(Value, Vec<ReducerEvent>)> {
    if native.contains(name) {
        return native.invoke_in_tx(store, tx, name, args);
    }
    if let Some(wasm) = wasm
        && wasm.contains(name)
    {
        return wasm.invoke_in_tx(store, tx, name, args);
    }
    Err(Error::not_found(format!(
        "no reducer registered for '{name}'"
    )))
}
