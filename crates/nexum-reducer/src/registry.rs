//! The reducer registry ([`ReducerRegistry`]) and the invocation entry point.
//!
//! The registry stores definitions by id and by name. Listing is
//! deterministic (ascending [`ReducerId`]). `invoke` is the **only** way to
//! execute a reducer (ADR-006 D1, D6, D7, D8):
//!
//! ```text
//! begin one transaction
//!   → run execute inside catch_unwind with a fresh ReducerContext
//!   → Ok(Ok(value))    → commit; return ReducerResult { tx_id, changes, events, value }
//!   → Ok(Err(error))   → abort; return Err(error)
//!   → Err(panic)       → abort; return Err(Internal "reducer '<name>' panicked")
//! ```
//!
//! Events are retrieved from the context only after execution and are
//! included **only** on success — an aborted invocation discards its buffer.
//! Durability stays outside: the caller appends `result.changes` to the WAL
//! with `result.tx_id` (ADR-006 D8).

use std::collections::BTreeMap;

use nexum_core::{Error, ReducerId, Result, TransactionId, Value};
use nexum_storage::Change;
use nexum_table::TableStore;
use nexum_tx::Transaction;

use crate::args::ReducerArgs;
use crate::context::ReducerContext;
use crate::definition::{ReducerDefinition, ReducerFn};
use crate::event::ReducerEvent;

/// The outcome of a successful reducer invocation.
///
/// `changes` is the exact committed `Vec<Change>` (the same boundary the WAL
/// consumes in Phase 5); `events` are the transaction-local events, in `emit`
/// order; `return_value` is what the reducer's `execute` returned. `tx_id`
/// lets the runtime append the transaction to the WAL.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerResult {
    tx_id: TransactionId,
    changes: Vec<Change>,
    events: Vec<ReducerEvent>,
    return_value: Value,
}

impl ReducerResult {
    /// Builds a result from a committed invocation's parts.
    ///
    /// Used by the shared [`finish_invocation`] path; external code reads the
    /// result through the accessors.
    pub fn new(
        tx_id: TransactionId,
        changes: Vec<Change>,
        events: Vec<ReducerEvent>,
        return_value: Value,
    ) -> Self {
        Self {
            tx_id,
            changes,
            events,
            return_value,
        }
    }

    /// Returns the id of the transaction that committed this result.
    pub fn tx_id(&self) -> TransactionId {
        self.tx_id
    }

    /// Returns the committed change records, in commit order.
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Returns the emitted events, in `emit` order.
    pub fn events(&self) -> &[ReducerEvent] {
        &self.events
    }

    /// Returns the reducer's return value.
    pub fn return_value(&self) -> &Value {
        &self.return_value
    }
}

/// A registry of reducer definitions.
#[derive(Debug, Default)]
pub struct ReducerRegistry {
    by_id: BTreeMap<ReducerId, ReducerDefinition>,
    by_name: BTreeMap<String, ReducerId>,
}

impl ReducerRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a definition, or `AlreadyExists` if its id **or** name is
    /// already taken.
    pub fn register(&mut self, definition: ReducerDefinition) -> Result<()> {
        if self.by_id.contains_key(&definition.id()) {
            return Err(Error::already_exists(format!(
                "reducer id {} is already registered",
                definition.id()
            )));
        }
        if self.by_name.contains_key(definition.name()) {
            return Err(Error::already_exists(format!(
                "reducer '{}' is already registered",
                definition.name()
            )));
        }
        self.by_name
            .insert(definition.name().to_string(), definition.id());
        self.by_id.insert(definition.id(), definition);
        Ok(())
    }

    /// Looks up a definition by id.
    pub fn lookup(&self, id: ReducerId) -> Option<&ReducerDefinition> {
        self.by_id.get(&id)
    }

    /// Looks up a definition by name.
    pub fn lookup_by_name(&self, name: &str) -> Option<&ReducerDefinition> {
        self.by_name.get(name).and_then(|id| self.by_id.get(id))
    }

    /// Returns `true` if a definition with the given name is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.by_name.contains_key(name)
    }

    /// Returns `true` if a definition with the given id is registered.
    pub fn contains_id(&self, id: ReducerId) -> bool {
        self.by_id.contains_key(&id)
    }

    /// Returns the number of registered reducers.
    pub fn len(&self) -> usize {
        self.by_id.len()
    }

    /// Returns `true` if no reducers are registered.
    pub fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    /// Iterates over every definition in deterministic (ascending id) order.
    pub fn list(&self) -> impl Iterator<Item = &ReducerDefinition> {
        self.by_id.values()
    }

    /// Executes the named reducer against `store` in **exactly one
    /// transaction** (ADR-006 D1).
    ///
    /// On success the result carries the committed changes, the emitted
    /// events, and the return value. On a reducer error the transaction is
    /// aborted and the error propagates unchanged (`Error::Conflict` is never
    /// wrapped). On a panic the transaction is aborted and the invocation
    /// fails with `Error::Internal` — zero mutations, zero events, zero
    /// committed changes.
    pub fn invoke(
        &self,
        store: &mut TableStore,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<ReducerResult> {
        let definition = self.lookup_by_name(name).ok_or_else(|| {
            Error::not_found(format!("reducer '{name}' is not registered"))
        })?;
        let execute = definition.execute();

        let mut tx = Transaction::begin(store);

        // Execute behind the panic boundary. Writes are provisional and the
        // event buffer is transaction-local, so a caught panic can only cost
        // the aborted transaction — never authoritative state.
        let outcome = execute_against_tx(execute, name, args, store, &mut tx);

        // The single commit/abort decision point shared with the WASM host
        // (ADR-006 D1, ADR-007 D6).
        match outcome {
            Ok((return_value, events)) => {
                finish_invocation(store, tx, events, Ok(return_value))
            }
            Err(error) => finish_invocation(store, tx, Vec::new(), Err(error)),
        }
    }

    /// Runs a registered reducer's `execute` against **an existing
    /// transaction** without committing (ADR-009 D3).
    ///
    /// This is the simulation tick's orchestration hook: a reducer invoked
    /// during a tick executes against the tick's transaction so the whole
    /// tick commits atomically (or aborts completely). The caller owns the
    /// transaction and the commit/abort decision; on success the reducer's
    /// return value and its emitted events (in `emit` order) are returned,
    /// on any failure the error propagates and the events are dropped.
    ///
    /// Standalone [`invoke`](Self::invoke) — one invocation = one
    /// transaction — is unchanged and remains the external entry point.
    pub fn invoke_in_tx(
        &self,
        store: &TableStore,
        tx: &mut Transaction,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<(Value, Vec<ReducerEvent>)> {
        let definition = self.lookup_by_name(name).ok_or_else(|| {
            Error::not_found(format!("reducer '{name}' is not registered"))
        })?;
        execute_against_tx(definition.execute(), name, args, store, tx)
    }
}

/// Executes one reducer function against an existing transaction behind the
/// panic boundary, returning `(return_value, events)` on success.
///
/// The panic boundary is the same one `invoke` uses: a caught panic becomes
/// an `Error::Internal` naming the reducer; writes were provisional and the
/// event buffer is transaction-local, so a panic can never touch
/// authoritative state. The caller (the native registry or the simulation
/// tick) owns the transaction and the commit/abort decision.
fn execute_against_tx(
    execute: ReducerFn,
    name: &str,
    args: &ReducerArgs,
    store: &TableStore,
    tx: &mut Transaction,
) -> Result<(Value, Vec<ReducerEvent>)> {
    let (events, outcome) = {
        let mut ctx = ReducerContext::new(tx, store);
        let outcome = std::panic::catch_unwind(std::panic::AssertUnwindSafe(
            || execute(&mut ctx, args),
        ));
        let events = ctx.take_events();
        let outcome = match outcome {
            Ok(Ok(return_value)) => Ok(return_value),
            Ok(Err(error)) => Err(error),
            Err(panic) => {
                // Keep the panic payload for debuggability: a message is far
                // more useful than a bare 'panicked' report.
                let detail = panic
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| panic.downcast_ref::<&str>().copied())
                    .unwrap_or("unknown panic payload");
                Err(Error::internal(format!(
                    "reducer '{name}' panicked during execution: {detail}"
                )))
            }
        };
        (events, outcome)
    };
    match outcome {
        Ok(return_value) => Ok((return_value, events)),
        Err(error) => Err(error),
    }
}

/// Completes a reducer invocation: commits on `Ok`, aborts on `Err`.
///
/// The **single** commit/abort decision point for every reducer execution
/// path (native registry and WASM host, ADR-006 D1 / ADR-007 D6), so both
/// paths have identical transaction semantics: on success the committed
/// [`Change`] records and the event buffer are attached to the result; on
/// failure the transaction is aborted, `Error::Conflict` from commit
/// propagates unchanged, and the events are dropped.
///
/// The caller owns the transaction and the context's event buffer; this
/// function consumes the transaction.
pub fn finish_invocation(
    store: &mut TableStore,
    mut tx: Transaction,
    events: Vec<ReducerEvent>,
    outcome: Result<Value>,
) -> Result<ReducerResult> {
    match outcome {
        Ok(return_value) => {
            let changes = tx.commit(store)?;
            Ok(ReducerResult::new(tx.id(), changes, events, return_value))
        }
        Err(error) => {
            let _ = tx.abort();
            Err(error)
        }
    }
}
