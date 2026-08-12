//! [`ReducerContext`]: the only surface a reducer sees.
//!
//! The context wraps the invocation's single transaction and the store (as a
//! **shared** reference) and exposes exactly the controlled operations a
//! reducer may perform (ADR-006 D2):
//!
//! - reads: `get`, `contains`, `scan`, `lookup_unique`
//! - writes: `insert`, `update`, `delete`
//! - events: `emit`
//!
//! Every method delegates to the transaction engine, so reducers inherit the
//! full Phase 4 semantics unchanged — read-your-writes, version OCC,
//! missing-row observations, table-epoch phantom protection, and unique-key
//! validation. There is no second read/write model for reducers.
//!
//! The context deliberately does **not** expose `&mut TableStore`: a reducer
//! cannot mutate authoritative storage outside its transaction, cannot commit,
//! and cannot abort. Native reducers are trusted code (ADR-006 D9); this is an
//! API boundary, not a security sandbox.
//!
//! Events are buffered here, transaction-locally, and only leave via
//! [`take_events`](Self::take_events) after a successful commit.

use nexum_core::{Error, Result, Row, RowId, Value};
use nexum_table::TableStore;
use nexum_tx::Transaction;

use crate::event::ReducerEvent;

/// The controlled execution surface handed to a reducer.
pub struct ReducerContext<'a> {
    tx: &'a mut Transaction,
    store: &'a TableStore,
    events: Vec<ReducerEvent>,
}

impl<'a> ReducerContext<'a> {
    /// Wraps the invocation's transaction and store.
    ///
    /// Used by the native registry and the WASM host (ADR-006 D2, ADR-007
    /// D6) to build the controlled surface for one invocation. A reducer
    /// never constructs a context itself — the invocation does.
    pub fn new(tx: &'a mut Transaction, store: &'a TableStore) -> Self {
        Self {
            tx,
            store,
            events: Vec::new(),
        }
    }

    /// Returns `true` if the named table exists.
    pub fn has_table(&self, table: &str) -> bool {
        self.store.has_table(table)
    }

    /// Reads a row through the transaction's logical view (read-your-writes).
    ///
    /// `None` when the row is absent from the transaction view. Records a
    /// version observation for rows without a pending write, so a concurrent
    /// modification conflicts at commit.
    pub fn get(&mut self, table: &str, row_id: RowId) -> Result<Option<Row>> {
        self.tx.get(self.store, table, row_id)
    }

    /// Checks a row's existence through the transaction's logical view.
    pub fn contains(&mut self, table: &str, row_id: RowId) -> Result<bool> {
        self.tx.contains(self.store, table, row_id)
    }

    /// Scans the transaction's logical view of a table.
    ///
    /// Records a table mutation-epoch observation (conservative phantom
    /// protection, ADR-004 D13): any committed row mutation in the table
    /// before commit becomes a conflict.
    pub fn scan(&mut self, table: &str) -> Result<Vec<(RowId, Row)>> {
        self.tx.scan(self.store, table)
    }

    /// Looks up the owners of `key` in the named unique index, through the
    /// transaction's logical view.
    pub fn lookup_unique(
        &mut self,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<RowId>> {
        self.tx.lookup_unique(self.store, table, index_name, key)
    }

    /// Buffers an insert; returns a provisional `RowId` handle (storage
    /// assigns the real id at commit).
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

    /// Buffers an application event.
    ///
    /// The event is transaction-local: it escapes only if the invocation
    /// commits, in `emit` order. Returns `InvalidArgument` for an empty name.
    pub fn emit(&mut self, name: &str, payload: impl Into<Value>) -> Result<()> {
        if name.is_empty() {
            return Err(Error::invalid_argument(
                "reducer event name must not be empty",
            ));
        }
        self.events.push(ReducerEvent::new(name, payload));
        Ok(())
    }

    /// Returns the buffered events in `emit` order, clearing the buffer.
    ///
    /// Called by the invocation **after** execution completes, so the events
    /// can be attached to the result on success and discarded on failure
    /// (ADR-006 D5).
    pub fn take_events(&mut self) -> Vec<ReducerEvent> {
        std::mem::take(&mut self.events)
    }
}
