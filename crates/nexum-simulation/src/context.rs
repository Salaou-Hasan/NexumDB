//! [`SimulationContext`]: the only surface a simulation system sees.
//!
//! The context wraps the tick's single transaction and the store (as a
//! **shared** reference) and exposes exactly the controlled operations a
//! system may perform (ADR-009 D2, D3):
//!
//! - reads: `get`, `contains`, `scan`, `lookup_unique`
//! - writes: `insert`, `update`, `delete`
//! - events: `emit`
//! - reducers: `invoke_reducer` (native) and `invoke_wasm` (sandboxed) —
//!   both execute against the tick's transaction, so a whole tick commits
//!   atomically
//! - determinism: `rng()` (seeded per world/tick/system)
//!
//! Every method delegates to the transaction engine, so systems inherit the
//! full Phase 4 semantics unchanged — read-your-writes, version OCC,
//! missing-row observations, table-epoch phantom protection, unique-key
//! validation, and multi-table atomicity. There is no second read/write
//! model for simulation.
//!
//! The context deliberately does **not** expose `&mut TableStore`: a system
//! cannot mutate authoritative storage outside the tick transaction, cannot
//! commit, and cannot abort. Events are buffered tick-locally and only
//! escape via a successful tick commit.

use nexum_core::{Error, PartitionId, Result, Row, RowId, SystemId, TickId, Value, WorldId};
use nexum_reducer::{ReducerArgs, ReducerEvent, ReducerRegistry};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wasm::WasmModuleRegistry;

use crate::partition::PartitionMessage;
use crate::rng::{DeterministicRng, rng_seed};

/// The controlled execution surface handed to a simulation system.
pub struct SimulationContext<'a> {
    tx: &'a mut Transaction,
    store: &'a TableStore,
    native: &'a ReducerRegistry,
    wasm: Option<&'a WasmModuleRegistry>,
    world_id: WorldId,
    partition: PartitionId,
    known_partitions: &'a [PartitionId],
    tick: TickId,
    system: SystemId,
    seed: u64,
    max_events: usize,
    max_messages: usize,
    max_kind_len: usize,
    max_args: usize,
    events: &'a mut Vec<ReducerEvent>,
    outbound: &'a mut Vec<PartitionMessage>,
}

impl<'a> SimulationContext<'a> {
    /// Wraps the tick's transaction, store, registries, and event buffer.
    ///
    /// Constructed by [`crate::World`] for every system, every tick. A
    /// system never constructs a context itself.
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn new(
        tx: &'a mut Transaction,
        store: &'a TableStore,
        native: &'a ReducerRegistry,
        wasm: Option<&'a WasmModuleRegistry>,
        world_id: WorldId,
        partition: PartitionId,
        known_partitions: &'a [PartitionId],
        tick: TickId,
        system: SystemId,
        seed: u64,
        max_events: usize,
        max_messages: usize,
        max_kind_len: usize,
        max_args: usize,
        events: &'a mut Vec<ReducerEvent>,
        outbound: &'a mut Vec<PartitionMessage>,
    ) -> Self {
        Self {
            tx,
            store,
            native,
            wasm,
            world_id,
            partition,
            known_partitions,
            tick,
            system,
            seed,
            max_events,
            max_messages,
            max_kind_len,
            max_args,
            events,
            outbound,
        }
    }

    // ---------------------------------------------------------------- reads

    /// Reads a row through the tick transaction's logical view
    /// (read-your-writes).
    pub fn get(&mut self, table: &str, row_id: RowId) -> Result<Option<Row>> {
        self.tx.get(self.store, table, row_id)
    }

    /// Checks a row's existence through the tick transaction's logical view.
    pub fn contains(&mut self, table: &str, row_id: RowId) -> Result<bool> {
        self.tx.contains(self.store, table, row_id)
    }

    /// Scans the tick transaction's logical view of a table, recording a
    /// table mutation-epoch observation (conservative phantom protection).
    pub fn scan(&mut self, table: &str) -> Result<Vec<(RowId, Row)>> {
        self.tx.scan(self.store, table)
    }

    /// Looks up the owners of `key` in the named unique index, through the
    /// tick transaction's logical view.
    pub fn lookup_unique(
        &mut self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<RowId>> {
        self.tx.lookup_unique(self.store, table, index_name, key)
    }

    // --------------------------------------------------------------- writes

    /// Buffers an insert; returns a provisional `RowId` handle (storage
    /// assigns the real id at the tick's commit).
    pub fn insert(&mut self, table: &str, row: Row) -> Result<RowId> {
        self.tx.insert(self.store, table, row)
    }

    /// Buffers an update of `row_id` to `row`, coalescing with any earlier
    /// write to the same row.
    pub fn update(&mut self, table: &str, row_id: RowId, row: Row) -> Result<()> {
        self.tx.update(self.store, table, row_id, row)
    }

    /// Buffers a delete of `row_id`, coalescing with any earlier write.
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()> {
        self.tx.delete(self.store, table, row_id)
    }

    // --------------------------------------------------------------- events

    /// Buffers an application event.
    ///
    /// The event is tick-local: it escapes only if the tick commits, in
    /// `emit` order. Returns `InvalidArgument` for an empty name and
    /// `Capacity` when the tick's event budget is exhausted.
    pub fn emit(&mut self, name: &str, payload: impl Into<Value>) -> Result<()> {
        if name.is_empty() {
            return Err(Error::invalid_argument(
                "simulation event name must not be empty",
            ));
        }
        self.append_events(vec![ReducerEvent::new(name, payload)])
    }

    // ----------------------------------------------------------- messaging

    /// Buffers a deterministic cross-partition message to `to` (ADR-012 D2).
    ///
    /// The message is committed with the tick and delivered to the
    /// destination's **next** tick, where it invokes the registered handler
    /// reducer named `kind`. Validates deterministically at send time: the
    /// target must be in the world's known topology and not the world itself,
    /// `kind` must be non-empty and bounded, the payload must be bounded, and
    /// the tick's outbound budget must not be exceeded. Any violation fails
    /// the tick with zero mutation.
    pub fn send_to(&mut self, to: PartitionId, kind: &str, payload: ReducerArgs) -> Result<()> {
        if to == self.partition {
            return Err(Error::invalid_argument(
                "cannot send a partition message to the sending partition itself",
            ));
        }
        if self.known_partitions.binary_search(&to).is_err() {
            return Err(Error::invalid_argument(format!(
                "partition {to} is not in this world's topology"
            )));
        }
        if kind.is_empty() {
            return Err(Error::invalid_argument(
                "partition message kind must not be empty",
            ));
        }
        if kind.len() > self.max_kind_len {
            return Err(Error::invalid_argument(format!(
                "partition message kind exceeds the configured limit of {} bytes",
                self.max_kind_len
            )));
        }
        if payload.len() > self.max_args {
            return Err(Error::invalid_argument(format!(
                "partition message payload exceeds the configured limit of {} arguments",
                self.max_args
            )));
        }
        if self.outbound.len() >= self.max_messages {
            return Err(Error::capacity(format!(
                "tick outbound messages exceed the configured limit of {}",
                self.max_messages
            )));
        }
        let seq = self.outbound.len() as u64;
        let message = PartitionMessage::new(
            self.partition,
            to,
            self.tick,
            seq,
            kind.to_string(),
            payload,
        )?;
        self.outbound.push(message);
        Ok(())
    }

    // -------------------------------------------------------------- reducers

    /// Invokes a registered **native** reducer against the tick's
    /// transaction (ADR-009 D3).
    ///
    /// The reducer runs behind its normal panic boundary but does **not**
    /// commit: its writes are part of the tick transaction, so the whole
    /// tick commits (or aborts) atomically. Emitted events are buffered
    /// tick-locally. Returns the reducer's return value.
    pub fn invoke_reducer(&mut self, name: &str, args: &ReducerArgs) -> Result<Value> {
        let (value, events) = self.native.invoke_in_tx(self.store, self.tx, name, args)?;
        self.append_events(events)?;
        Ok(value)
    }

    /// Invokes a registered **WASM** reducer against the tick's transaction
    /// (ADR-009 D3).
    ///
    /// Same tick-atomic semantics as [`invoke_reducer`](Self::invoke_reducer),
    /// with the full Phase 7 sandbox (fuel, memory, host-call limits, sticky
    /// ABI errors). Returns `NotFound` if no WASM registry is configured on
    /// the world, or the module is unknown.
    pub fn invoke_wasm(&mut self, name: &str, args: &ReducerArgs) -> Result<Value> {
        let wasm = self.wasm.ok_or_else(|| {
            Error::not_found("no wasm module registry is configured on this world")
        })?;
        let (value, events) = wasm.invoke_in_tx(self.store, self.tx, name, args)?;
        self.append_events(events)?;
        Ok(value)
    }

    // ------------------------------------------------------- introspection

    /// Returns the world this tick belongs to.
    pub fn world_id(&self) -> WorldId {
        self.world_id
    }

    /// Returns the partition this tick belongs to (the message-bus address
    /// of this world).
    pub fn partition(&self) -> PartitionId {
        self.partition
    }

    /// Returns the current tick.
    pub fn tick(&self) -> TickId {
        self.tick
    }

    /// Returns the id of the system currently executing.
    pub fn system(&self) -> SystemId {
        self.system
    }

    /// Returns `true` if the named table exists.
    pub fn has_table(&self, table: &str) -> bool {
        self.store.has_table(table)
    }

    /// Returns a deterministic RNG stream for this system and tick.
    ///
    /// Seeded from `mix(world_seed, tick, system)` — a pure function of
    /// deterministic inputs (ADR-009 D5). Two identical runs produce
    /// identical streams.
    pub fn rng(&self) -> DeterministicRng {
        DeterministicRng::new(rng_seed(
            self.seed,
            self.tick.as_u64(),
            self.system.as_u64(),
        ))
    }

    /// Appends reducer/system events to the tick buffer, enforcing the
    /// configured budget.
    fn append_events(&mut self, incoming: Vec<ReducerEvent>) -> Result<()> {
        append_events(self.events, incoming, self.max_events)
    }
}

/// Appends events to a tick's buffer, enforcing the configured budget.
///
/// Shared by the system path (through [`SimulationContext`]) and the
/// scheduled-event path (in [`crate::World`]), so every event that reaches a
/// tick buffer passes the same bound. Returns `Capacity` when the budget is
/// exceeded — the tick then aborts and the buffer is discarded.
pub(crate) fn append_events(
    dst: &mut Vec<ReducerEvent>,
    src: Vec<ReducerEvent>,
    max: usize,
) -> Result<()> {
    if dst.len().saturating_add(src.len()) > max {
        return Err(Error::capacity(format!(
            "tick event buffer exceeds the configured limit of {max} events"
        )));
    }
    dst.extend(src);
    Ok(())
}
