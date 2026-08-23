//! Phase 11 — deterministic parallel tick execution (ADR-011).
//!
//! A tick remains **one logical transaction** (D1): scheduled events run
//! serially, then systems execute in deterministic groups, then the merged
//! transaction commits exactly once. The [`TickPlan`] partitions the ordered
//! system list into groups of pairwise **table-disjoint** systems (D2);
//! multi-member groups execute concurrently — each system on a child
//! transaction branched from the tick transaction (`Transaction::branch_of`)
//! — and merge back in system order (`Transaction::absorb`), which is exact
//! (D3), so the merged transaction is identical to the serial one: same read
//! set, same write set with the same keys, same commit ordering, same
//! `Vec<Change>`, same events, same final state, and the same first-failure
//! error regardless of worker count (D4).
//!
//! The RNG is already per-system (`rng_seed(world_seed, tick, system_id)`),
//! so parallel systems draw identical streams with no shared state (D5).
//! Workers are `std::thread::scope` threads; a group's slots distribute
//! round-robin over the worker budget and results are collected **by slot,
//! never by completion order** (D6). `run_system` is the shared per-system
//! primitive used by the serial reference loop, by singleton groups, and by
//! parallel children — one system, one panic boundary, identical error
//! messages.

use std::collections::BTreeSet;
use std::sync::Mutex;

use nexum_core::{Error, PartitionId, Result, RowId, TableId, TickId, TransactionId, WorldId};
use nexum_reducer::{ReducerEvent, ReducerRegistry};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use nexum_wasm::WasmModuleRegistry;

use crate::context::{SimulationContext, append_events};
use crate::input::InputFrame;
use crate::partition::PartitionMessage;
use crate::systems::{SystemAccess, SystemDefinition};

/// One step of the tick plan: the indices (into the ordered systems slice)
/// of the systems that execute together.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SystemGroup {
    /// Indices into the world's ordered systems slice, in execution order.
    systems: Vec<usize>,
}

impl SystemGroup {
    /// Returns the member system indices in execution order.
    pub(crate) fn systems(&self) -> &[usize] {
        &self.systems
    }
}

/// The deterministic execution plan of one tick (ADR-011 D2).
///
/// A pure function of `(systems, store)` — worker count never enters it.
/// Groups execute in order; within a group, members are pairwise
/// table-disjoint (safe to run concurrently). Singleton groups are either
/// opaque systems or systems that conflicted with the previous group; they
/// execute serially against the tick transaction, exactly like Phase 9.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TickPlan {
    groups: Vec<SystemGroup>,
}

impl TickPlan {
    /// Builds the plan: greedy single pass over the ordered systems.
    ///
    /// A system joins the current group only when it conflicts with none of
    /// the group's members (write/write, write/read, or read/write on a
    /// shared table). Declared table names are resolved against `store`;
    /// an unknown declared table is a deterministic error.
    pub(crate) fn build(systems: &[SystemDefinition], store: &TableStore) -> Result<Self> {
        let mut groups: Vec<SystemGroup> = Vec::new();
        let mut current: Vec<usize> = Vec::new();
        let mut cur_reads: BTreeSet<TableId> = BTreeSet::new();
        let mut cur_writes: BTreeSet<TableId> = BTreeSet::new();

        for (index, system) in systems.iter().enumerate() {
            let access = system.access();
            let footprint = match resolve(store, system, access)? {
                Some(footprint) => footprint,
                None => {
                    // Opaque: always a fresh singleton group (never grouped).
                    flush(&mut groups, &mut current, &mut cur_reads, &mut cur_writes);
                    groups.push(SystemGroup {
                        systems: vec![index],
                    });
                    continue;
                }
            };
            let (reads, writes) = footprint;

            // A conflict means the system cannot join the current group:
            // write/write, write/read, or read/write overlap on a table.
            let conflicts = !writes.is_disjoint(&cur_reads)
                || !writes.is_disjoint(&cur_writes)
                || !reads.is_disjoint(&cur_writes);

            if current.is_empty() || !conflicts {
                current.push(index);
                cur_reads.extend(reads);
                cur_writes.extend(writes);
            } else {
                flush(&mut groups, &mut current, &mut cur_reads, &mut cur_writes);
                current.push(index);
                cur_reads.extend(reads);
                cur_writes.extend(writes);
            }
        }
        flush(&mut groups, &mut current, &mut cur_reads, &mut cur_writes);
        Ok(Self { groups })
    }

    /// Returns the groups in execution order.
    pub(crate) fn groups(&self) -> &[SystemGroup] {
        &self.groups
    }
}

/// Closes the current group and resets the accumulated footprints.
fn flush(
    groups: &mut Vec<SystemGroup>,
    current: &mut Vec<usize>,
    cur_reads: &mut BTreeSet<TableId>,
    cur_writes: &mut BTreeSet<TableId>,
) {
    if !current.is_empty() {
        groups.push(SystemGroup {
            systems: std::mem::take(current),
        });
    }
    cur_reads.clear();
    cur_writes.clear();
}

/// Resolves a declared access footprint against the store.
///
/// Returns `None` for an opaque system; otherwise the resolved
/// `(reads, writes)` `TableId` sets. An unknown declared table is a
/// deterministic `InvalidArgument` error.
fn resolve(
    store: &TableStore,
    system: &SystemDefinition,
    access: &SystemAccess,
) -> Result<Option<(BTreeSet<TableId>, BTreeSet<TableId>)>> {
    if access.is_opaque() {
        return Ok(None);
    }
    let mut reads = BTreeSet::new();
    for name in access.reads() {
        let table = store.table(name).ok_or_else(|| {
            Error::invalid_argument(format!(
                "system '{}' declares reads of unknown table '{name}'",
                system.name()
            ))
        })?;
        reads.insert(table.id());
    }
    let mut writes = BTreeSet::new();
    for name in access.writes() {
        let table = store.table(name).ok_or_else(|| {
            Error::invalid_argument(format!(
                "system '{}' declares writes of unknown table '{name}'",
                system.name()
            ))
        })?;
        writes.insert(table.id());
    }
    Ok(Some((reads, writes)))
}

/// The outcome of one child system's execution inside a parallel group.
enum ChildOutcome {
    /// The system succeeded; its child transaction, events, and outbound
    /// messages merge in slot order.
    Ok(Transaction, Vec<ReducerEvent>, Vec<PartitionMessage>),
    /// The system failed (application error or panic).
    Err(Error),
}

/// Executes one multi-member group of the plan concurrently (ADR-011 D3).
///
/// Every member runs on its own thread (round-robin over `workers` threads),
/// each on a child transaction branched from `parent`. After all threads
/// join, the first failure in **system (slot) order** fails the group with
/// zero merges; otherwise children merge into `parent` in slot order and
/// their events append to `events` (budget enforced). The outcome is a pure
/// function of the plan, the parent, and the inputs — never of thread
/// completion order.
#[allow(clippy::too_many_arguments)]
pub(crate) fn execute_group(
    group: &SystemGroup,
    systems: &[SystemDefinition],
    parent: &mut Transaction,
    store: &TableStore,
    native: &ReducerRegistry,
    wasm: Option<&WasmModuleRegistry>,
    world_id: WorldId,
    partition: PartitionId,
    known: &[PartitionId],
    tick_id: TickId,
    seed: u64,
    max_events: usize,
    max_messages: usize,
    max_kind_len: usize,
    max_args: usize,
    inputs: &InputFrame,
    workers: usize,
    events: &mut Vec<ReducerEvent>,
    outbound: &mut Vec<PartitionMessage>,
) -> Result<()> {
    debug_assert!(
        group.systems.len() >= 2,
        "execute_group is for multi-member groups"
    );
    let members = group.systems();
    let thread_count = workers.max(1).min(members.len());
    let parent_shared: &Transaction = parent;

    // The per-slot execution body, shared by the threaded and the
    // sequential path so both have identical semantics (D6).
    let run_slot = |slot: usize| -> ChildOutcome {
        let index = members[slot];
        let definition = &systems[index];
        // Child ids are ephemeral local counters — never exported, never
        // persisted — so the store's TransactionId allocator matches serial
        // exactly (the tick transaction is allocated first).
        let mut child = Transaction::new(TransactionId::from_u64((index + 1) as u64));
        match child.branch_of(parent_shared) {
            Ok(()) => {
                let mut child_events: Vec<ReducerEvent> = Vec::new();
                let mut child_outbound: Vec<PartitionMessage> = Vec::new();
                match run_system(
                    definition,
                    &mut child,
                    store,
                    native,
                    wasm,
                    world_id,
                    partition,
                    known,
                    tick_id,
                    seed,
                    max_events,
                    max_messages,
                    max_kind_len,
                    max_args,
                    &mut child_events,
                    &mut child_outbound,
                    inputs,
                ) {
                    Ok(()) => ChildOutcome::Ok(child, child_events, child_outbound),
                    Err(error) => ChildOutcome::Err(error),
                }
            }
            Err(error) => ChildOutcome::Err(error),
        }
    };

    let mut results: Vec<Option<ChildOutcome>> = (0..members.len()).map(|_| None).collect();
    if thread_count == 1 {
        // Sequential fast path: one "worker" must not pay thread-spawn
        // costs; the semantics are identical to the threaded path.
        for (slot, _) in members.iter().enumerate() {
            results[slot] = Some(run_slot(slot));
        }
    } else {
        let results_shared = Mutex::new(results);
        std::thread::scope(|scope| {
            for thread in 0..thread_count {
                // Round-robin slot assignment: thread `thread` takes slots
                // `thread, thread + thread_count, …`, ascending. The *slot*
                // (system order) determines the merge order, never
                // completion.
                let slots: Vec<usize> = (thread..members.len()).step_by(thread_count).collect();
                // Scoped threads may borrow: `results`, `parent_shared`, the
                // registries, and the store are all shared immutably across
                // the group's threads and released when the scope joins.
                scope.spawn(|| {
                    for slot in slots {
                        // Compute the outcome *before* locking: the guard
                        // must never be held while a system executes, or
                        // every child in the group would serialize under
                        // the mutex and defeat the parallelism.
                        let outcome = run_slot(slot);
                        results_shared.lock().expect("scope guard")[slot] = Some(outcome);
                    }
                });
            }
        });
        results = results_shared.into_inner().expect("all threads joined");
    }
    // First failure in system order fails the group — the identical error
    // the serial path reports. Nothing merges on failure.
    for outcome in &results {
        if let Some(ChildOutcome::Err(error)) = outcome {
            return Err(error.clone());
        }
    }

    // Undeclared same-group interdependencies are detected deterministically
    // here (in slot order), never silently wrong (ADR-011 D2). A sibling's
    // write key is invisible to another child's branch, so any overlap the
    // declarations failed to express is observable at merge time:
    //
    // - **write/write**: a child writes a row a sibling already wrote
    //   (provisional-handle collisions would otherwise silently overwrite);
    // - **read/write**: a child read a row from the store that a sibling
    //   wrote (in serial it would have seen the sibling's provisional value
    //   — the child could not have seen it through its branch);
    // - **table observation**: a child scanned / looked up a table a sibling
    //   wrote (its scan missed the sibling's rows, diverging from serial).
    // The parent's write keys when the group started. Inherited entries are
    // identical across siblings, so only a child's *fresh* keys (not in the
    // snapshot) can reveal an undeclared same-group overlap.
    let snapshot: BTreeSet<(TableId, RowId)> = parent
        .writes()
        .map(|(table_id, row_id, _)| (table_id, row_id))
        .collect();
    let mut sibling_writes: BTreeSet<(TableId, RowId)> = BTreeSet::new();
    let mut sibling_tables: BTreeSet<TableId> = BTreeSet::new();
    for (slot, outcome) in results.iter_mut().enumerate() {
        let (child, child_events, child_outbound) = match outcome.take() {
            Some(ChildOutcome::Ok(child, child_events, child_outbound)) => {
                (child, child_events, child_outbound)
            }
            Some(ChildOutcome::Err(_)) => unreachable!("checked above"),
            None => {
                return Err(Error::internal(format!(
                    "parallel group lost the result of system slot {slot}"
                )));
            }
        };
        let fresh: Vec<(TableId, RowId)> = child
            .writes()
            .filter(|(table_id, row_id, _)| !snapshot.contains(&(*table_id, *row_id)))
            .map(|(table_id, row_id, _)| (table_id, row_id))
            .collect();
        for &(table_id, row_id) in &fresh {
            if sibling_writes.contains(&(table_id, row_id)) {
                return Err(Error::internal(format!(
                    "parallel group conflict: two systems wrote the same row {row_id} in table {table_id} — their access declarations are not disjoint (undeclared write/write dependency)"
                )));
            }
        }
        for (table_id, row_id, _) in child.reads() {
            if sibling_writes.contains(&(table_id, row_id)) {
                return Err(Error::internal(format!(
                    "parallel group conflict: a system read row {row_id} in table {table_id} that another system in the same group wrote — their access declarations are not disjoint (undeclared read/write dependency)"
                )));
            }
        }
        for (table_id, _) in child.table_reads() {
            if sibling_tables.contains(&table_id) {
                return Err(Error::internal(format!(
                    "parallel group conflict: a system scanned table {table_id} that another system in the same group wrote — their access declarations are not disjoint (undeclared read/write dependency)"
                )));
            }
        }
        for &(table_id, row_id) in &fresh {
            sibling_writes.insert((table_id, row_id));
            sibling_tables.insert(table_id);
        }
        parent.absorb(child)?;
        append_events(events, child_events, max_events)?;
        // Outbound merges in slot (= system) order, exactly like the serial
        // path. Serial seqs are global positions (0..n); children restart
        // from zero, so renumber the merged batch to positions so the
        // committed outbound trace reproduces the serial trace exactly
        // (ADR-011 D7). The group-wide budget check keeps the total
        // deterministic.
        if outbound.len().saturating_add(child_outbound.len()) > max_messages {
            return Err(Error::capacity(format!(
                "tick outbound messages exceed the configured limit of {max_messages}"
            )));
        }
        outbound.extend(child_outbound);
        for (index, message) in outbound.iter_mut().enumerate() {
            message.set_seq(index as u64);
        }
    }
    Ok(())
}

/// Runs one system against `tx` (the tick transaction, a singleton group, or
/// a parallel child) — the shared per-system execution primitive (ADR-011
/// D4).
///
/// One system = one `catch_unwind` boundary. A panic becomes
/// `Error::internal("simulation system '…' panicked during tick …")` —
/// identical to the Phase 9 serial path. System events append to `events`
/// (budget enforced by the context's `emit` path).
#[allow(clippy::too_many_arguments)]
pub(crate) fn run_system(
    definition: &SystemDefinition,
    tx: &mut Transaction,
    store: &TableStore,
    native: &ReducerRegistry,
    wasm: Option<&WasmModuleRegistry>,
    world_id: WorldId,
    partition: PartitionId,
    known: &[PartitionId],
    tick_id: TickId,
    seed: u64,
    max_events: usize,
    max_messages: usize,
    max_kind_len: usize,
    max_args: usize,
    events: &mut Vec<ReducerEvent>,
    outbound: &mut Vec<PartitionMessage>,
    inputs: &InputFrame,
) -> Result<()> {
    let mut ctx = SimulationContext::new(
        tx,
        store,
        native,
        wasm,
        world_id,
        partition,
        known,
        tick_id,
        definition.id(),
        seed,
        max_events,
        max_messages,
        max_kind_len,
        max_args,
        events,
        outbound,
    );
    let execute = definition.execute();
    let outcome =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| execute(&mut ctx, inputs)));
    match outcome {
        Ok(Ok(())) => Ok(()),
        Ok(Err(error)) => Err(error),
        Err(panic) => Err(Error::internal(format!(
            "simulation system '{}' panicked during tick {tick_id}: {}",
            definition.name(),
            panic_detail(&panic),
        ))),
    }
}

/// Extracts a human-readable message from a panic payload.
fn panic_detail(panic: &Box<dyn std::any::Any + Send>) -> String {
    panic
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| panic.downcast_ref::<&str>().copied())
        .unwrap_or("unknown panic payload")
        .to_string()
}
