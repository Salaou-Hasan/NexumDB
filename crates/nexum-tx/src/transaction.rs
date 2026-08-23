//! The [`Transaction`]: a pure accumulator of reads and writes, with an
//! explicit state machine.
//!
//! A transaction owns its id, state, read set, write set, and per-table
//! provisional-id counter — and **nothing else**. It holds no reference to
//! the store: every method takes the store per call (reads take
//! `&TableStore`, commit takes `&mut TableStore`). This keeps borrows
//! per-operation and matches Phase 3's single-threaded exclusive-ownership
//! model (ADR-004 D1).
//!
//! State machine (design doc §4, ADR-004 D7):
//!
//! ```text
//! Active → Committed      (successful commit)
//! Active → Aborted        (failed commit / explicit abort)
//! Aborted → Aborted       (abort is idempotent)
//! ```
//!
//! Forbidden: any operation on a `Committed` transaction
//! (`AlreadyCommitted`), any operation on an `Aborted` transaction
//! (`AlreadyAborted`), and `abort` of a committed transaction.

use std::collections::BTreeMap;
use std::fmt;

use nexum_core::{Error, Result, Row, RowId, TableId, TransactionId, Value, Version};
use nexum_storage::Change;
use nexum_table::TableStore;

use crate::commit;
use crate::read_set::ReadSet;
use crate::write_set::{WriteEntry, WriteSet};

/// Provisional `RowId`s (insert handles) are real row ids with this bit set.
///
/// Storage assigns real ids from a per-table `u64` counter starting at zero,
/// so no real id ever sets the high bit. A provisional id is a coalescing
/// handle valid only within its transaction; storage assigns the real id at
/// commit (ADR-004 D6).
const PROVISIONAL_FLAG: u64 = 1 << 63;

/// Returns `true` if `row_id` is a provisional in-transaction insert handle.
pub(crate) fn is_provisional(row_id: RowId) -> bool {
    row_id.as_u64() & PROVISIONAL_FLAG != 0
}

/// Builds the `n`-th provisional insert handle for a table.
pub(crate) fn provisional_row_id(n: u64) -> RowId {
    RowId::from_u64(PROVISIONAL_FLAG | n)
}

/// The explicit lifecycle state of a transaction.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TransactionState {
    /// The transaction is accumulating reads and writes; it may commit.
    Active,
    /// The transaction committed; it must not be used again.
    Committed,
    /// The transaction aborted (validation failed or explicit abort); it must
    /// not be committed or reused.
    Aborted,
}

impl fmt::Display for TransactionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Active => f.write_str("active"),
            Self::Committed => f.write_str("committed"),
            Self::Aborted => f.write_str("aborted"),
        }
    }
}

/// A single optimistic transaction: recorded reads + buffered writes.
///
/// Memory is proportional to the read/write set, never the database size.
#[derive(Debug)]
pub struct Transaction {
    id: TransactionId,
    state: TransactionState,
    reads: ReadSet,
    writes: WriteSet,
    provisional: BTreeMap<TableId, u64>,
}

impl Transaction {
    /// Creates a new `Active` transaction with the given id.
    pub fn new(id: TransactionId) -> Self {
        Self {
            id,
            state: TransactionState::Active,
            reads: ReadSet::new(),
            writes: WriteSet::new(),
            provisional: BTreeMap::new(),
        }
    }

    /// Begins a new `Active` transaction against `store`, allocating a fresh
    /// monotonic `TransactionId` from it.
    ///
    /// The transaction does not hold the store — reads and writes take it per
    /// call. This is the convenience entry point; [`Transaction::new`] is the
    /// explicit-id constructor (e.g. for runtime-assigned ids).
    pub fn begin(store: &mut TableStore) -> Self {
        Self::new(store.alloc_transaction_id())
    }

    /// Returns the transaction id.
    pub fn id(&self) -> TransactionId {
        self.id
    }

    /// Branches a child transaction off `parent` (Phase 11 parallel
    /// execution, ADR-011 D3).
    ///
    /// The child starts `Active` with a **copy** of the parent's buffered
    /// writes and per-table provisional-id counters, so its logical view
    /// (read-your-writes, `scan`, `lookup_unique`) matches what the shared
    /// serial transaction would show when the child's system runs. The
    /// child's read set starts empty: inherited entries are *writes* (their
    /// read observations were captured by their original writers in the
    /// parent's read set), and reads of the frozen store are recorded fresh.
    ///
    /// Both transactions must be `Active`. The child is meant to execute one
    /// system concurrently with its siblings and then be merged back via
    /// [`absorb`](Self::absorb).
    ///
    /// Phase 22: the parent's write set is inherited as a **shared, immutable
    /// `Arc`** — branching is O(1) instead of an O(parent-writes) deep copy
    /// that made per-call reducer dispatch quadratic under tick bursts. The
    /// child's own writes accumulate in a private layer over the inherited
    /// view; [`absorb`](Self::absorb) folds only the deltas back.
    pub fn branch_of(&mut self, parent: &Transaction) -> Result<()> {
        self.ensure_active()?;
        parent.ensure_active()?;
        self.writes = parent.writes.branch();
        self.provisional = parent.provisional.clone();
        Ok(())
    }

    /// Merges a completed child transaction into this one (Phase 11
    /// parallel execution, ADR-011 D3).
    ///
    /// Exact, not approximate: the child's read observations union into this
    /// transaction's read set; every key of the child's write set overwrites
    /// this transaction's entry at that key (the child's entry is the final
    /// coalesced state — it started as a copy of this transaction's writes,
    /// and an inherited key can never vanish because provisional handles are
    /// created by, and only ever known to, the transaction that created
    /// them, so `insert → delete` net no-ops only ever remove keys the
    /// parent never held); and each per-table provisional-id counter becomes
    /// the maximum. Both transactions must be `Active`; the child is
    /// consumed.
    pub fn absorb(&mut self, child: Transaction) -> Result<()> {
        self.ensure_active()?;
        match child.state {
            TransactionState::Active => {}
            TransactionState::Committed => {
                return Err(Error::already_committed(format!(
                    "cannot absorb transaction {}: already committed",
                    child.id
                )));
            }
            TransactionState::Aborted => {
                return Err(Error::already_aborted(format!(
                    "cannot absorb transaction {}: already aborted",
                    child.id
                )));
            }
        }
        self.reads.absorb(&child.reads);
        // Phase 22: fold only the child's own writes (its deltas) across the
        // branch boundary, applying the coalescing rules — including the
        // cross-branch `insert → delete` net no-op. This is O(child writes)
        // instead of an O(parent writes) full merge per call.
        self.writes.absorb(child.writes);
        for (table_id, count) in &child.provisional {
            let ours = self.provisional.entry(*table_id).or_insert(0);
            if *count > *ours {
                *ours = *count;
            }
        }
        Ok(())
    }

    /// Returns the transaction's current state.
    pub fn state(&self) -> TransactionState {
        self.state
    }

    /// Returns the number of recorded read observations.
    pub fn read_count(&self) -> usize {
        self.reads.len()
    }

    /// Returns the number of buffered writes.
    pub fn write_count(&self) -> usize {
        self.writes.len()
    }

    /// Iterates over the recorded reads as `(table_id, row_id, observed)`,
    /// where `observed` is `None` when the row was observed absent.
    pub fn reads(&self) -> impl Iterator<Item = (TableId, RowId, Option<Version>)> + '_ {
        self.reads.entries()
    }

    /// Iterates over the recorded **table observations** as
    /// `(table_id, observed_epoch)` — set observations from `scan` /
    /// `lookup_unique`, used by phantom-protection validation.
    pub fn table_reads(&self) -> impl Iterator<Item = (TableId, Version)> + '_ {
        self.reads.table_entries()
    }

    /// Iterates over the buffered writes as `(table_id, row_id, entry)`.
    pub fn writes(&self) -> impl Iterator<Item = (TableId, RowId, &WriteEntry)> + '_ {
        self.writes.entries()
    }

    /// Fails if the transaction is not `Active`.
    fn ensure_active(&self) -> Result<()> {
        match self.state {
            TransactionState::Active => Ok(()),
            TransactionState::Committed => Err(Error::already_committed(format!(
                "transaction {} already committed",
                self.id
            ))),
            TransactionState::Aborted => Err(Error::already_aborted(format!(
                "transaction {} already aborted",
                self.id
            ))),
        }
    }

    /// Reads a row through the **transaction's logical view** (read-your-
    /// writes, ADR-004 D12): the transaction's own buffered writes take
    /// precedence over committed state.
    ///
    /// - a pending `Insert` / `Update` of `row_id` → `Some(pending row)`
    /// - a pending `Delete` of `row_id` → `None` (the row is logically gone)
    /// - otherwise the committed row is returned (or `None` if absent) and
    ///   the observation is recorded, so a later insert of the same row by
    ///   another writer is detected as a conflict at commit
    ///
    /// The returned row is owned because it may come from the write set;
    /// reads of rows with a buffered write record no observation (the write
    /// entry governs validation). A provisional insert handle without a
    /// pending insert is simply absent from the transaction view.
    pub fn get(&mut self, store: &TableStore, table: &str, row_id: RowId) -> Result<Option<Row>> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();

        if let Some(entry) = self.writes.get(table_id, row_id) {
            return Ok(match entry {
                WriteEntry::Insert(row) | WriteEntry::Update(row) => Some(row.clone()),
                WriteEntry::Delete => None,
            });
        }
        // A provisional id with no pending insert is absent from the
        // transaction view — either it was never created, or its insert was
        // net-no-op'd away by an insert→delete. It can never exist in
        // storage, so no observation is recorded and no conflict can arise.
        if is_provisional(row_id) {
            return Ok(None);
        }

        let observed = table.version_of(row_id);
        self.reads.record(table_id, row_id, observed);
        Ok(table.get(row_id).cloned())
    }

    /// Checks a row's existence through the transaction's logical view.
    ///
    /// Pending `Insert`/`Update` count as present; a pending `Delete` counts
    /// as absent. Other rows record their observation in the read set (so a
    /// concurrent insert/delete is detected at commit).
    pub fn contains(&mut self, store: &TableStore, table: &str, row_id: RowId) -> Result<bool> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();

        if let Some(entry) = self.writes.get(table_id, row_id) {
            return Ok(!matches!(entry, WriteEntry::Delete));
        }
        // Same absent-view semantics for unknown provisional ids as `get`.
        if is_provisional(row_id) {
            return Ok(false);
        }

        let observed = table.version_of(row_id);
        self.reads.record(table_id, row_id, observed);
        Ok(observed.is_some())
    }

    /// Scans the **transaction's logical view** of a table, recording a
    /// table-level **mutation epoch observation** (phantom protection,
    /// ADR-004 D13): any committed row mutation in the table before commit
    /// becomes a conflict.
    ///
    /// The result is deterministic — committed rows in ascending `RowId`
    /// order with pending `Update`s overlaid and pending `Delete`s hidden,
    /// followed by this transaction's pending inserts in provisional-id
    /// order (provisional ids sort after every real id, so this is exactly
    /// the merged order).
    pub fn scan(&mut self, store: &TableStore, table: &str) -> Result<Vec<(RowId, Row)>> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();
        self.reads.record_table(table_id, table.epoch());

        let mut rows: Vec<(RowId, Row)> = Vec::new();
        for (row_id, committed) in table.scan() {
            match self.writes.get(table_id, row_id) {
                Some(WriteEntry::Delete) => {}
                Some(WriteEntry::Update(pending)) => rows.push((row_id, pending.clone())),
                // Defensive: provisional insert ids (high bit) never collide
                // with real storage ids, so this arm is unreachable.
                Some(WriteEntry::Insert(_)) => rows.push((row_id, committed.clone())),
                None => rows.push((row_id, committed.clone())),
            }
        }
        for (write_table, row_id, entry) in self.writes.entries() {
            if write_table == table_id
                && let WriteEntry::Insert(row) = entry
            {
                rows.push((row_id, row.clone()));
            }
        }
        Ok(rows)
    }

    /// Looks up the owners of `key` in the named unique index through the
    /// transaction's logical view (ADR-004 D12–D13).
    ///
    /// Committed owners that this transaction logically deleted are hidden;
    /// committed owners updated away from the key are hidden; pending
    /// `Insert`/`Update` writes owning the key are included. Because a
    /// unique-key lookup observes the index *as a set*, it also records a
    /// table mutation-epoch observation (conservative phantom protection).
    ///
    /// Returns `[Error::not_found]` for an unknown index and
    /// `[Error::invalid_argument]` for a non-unique index or malformed key
    /// (delegated to `Table::lookup_unique`).
    pub fn lookup_unique(
        &mut self,
        store: &TableStore,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<RowId>> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();
        self.reads.record_table(table_id, table.epoch());

        let mut owners: Vec<RowId> = Vec::new();
        for owner in table.lookup_unique(index_name, key)? {
            let keep = match self.writes.get(table_id, owner) {
                Some(WriteEntry::Delete) => false,
                Some(WriteEntry::Update(pending)) => row_owns_key(table, index_name, key, pending)?,
                Some(WriteEntry::Insert(_)) => true,
                None => true,
            };
            if keep {
                owners.push(owner);
            }
        }
        // Scan pending writes for newly-inserted rows that own the key.
        // Skip entirely when no Insert entries exist (update-heavy workloads
        // like fire_weapon never insert).
        if self.writes.has_any_insert() {
            for (row_id, entry) in self.writes.entries_for_table(table_id) {
                if let Some(row) = entry.row()
                    && row_owns_key(table, index_name, key, row)?
                {
                    owners.push(row_id);
                }
            }
        }
        owners.sort_unstable();
        owners.dedup();
        Ok(owners)
    }

    /// Looks up the owners of `key` in the named **non-unique** secondary
    /// index through the transaction's logical view.
    ///
    /// The non-unique counterpart of [`lookup_unique`](Self::lookup_unique)
    /// with the identical semantics: committed owners that this transaction
    /// logically deleted or updated away from the key are hidden; pending
    /// `Insert`/`Update` writes owning the key are included; the result is
    /// deterministically sorted and deduplicated; and a table mutation-epoch
    /// observation is recorded (conservative phantom protection, ADR-004
    /// D12–D13).
    ///
    /// Returns `[Error::not_found]` for an unknown index and
    /// `[Error::invalid_argument]` for a malformed key (delegated to
    /// `Table::lookup`).
    pub fn lookup_index(
        &mut self,
        store: &TableStore,
        table: &str,
        index_name: &str,
        key: &[Value],
    ) -> Result<Vec<RowId>> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();
        self.reads.record_table(table_id, table.epoch());

        let mut owners: Vec<RowId> = Vec::new();
        for owner in table.lookup(index_name, key)? {
            let keep = match self.writes.get(table_id, owner) {
                Some(WriteEntry::Delete) => false,
                Some(WriteEntry::Update(pending)) => {
                    row_owns_index_key(table, index_name, key, pending)?
                }
                Some(WriteEntry::Insert(_)) => true,
                None => true,
            };
            if keep {
                owners.push(owner);
            }
        }
        // Same skip optimization as lookup_unique.
        if self.writes.has_any_insert() {
            for (row_id, entry) in self.writes.entries_for_table(table_id) {
                if let Some(row) = entry.row()
                    && row_owns_index_key(table, index_name, key, row)?
                {
                    owners.push(row_id);
                }
            }
        }
        owners.sort_unstable();
        owners.dedup();
        Ok(owners)
    }

    /// Buffers an insert and returns a **provisional** `RowId` handle for it.
    ///
    /// The handle is valid only within this transaction: it can be passed to
    /// [`update`](Self::update) or [`delete`](Self::delete) to coalesce with
    /// the insert (insert→update keeps the final row; insert→delete is a net
    /// no-op). Storage assigns the real row id at commit.
    ///
    /// The row is validated against the table's schema immediately; unique
    /// constraints are validated at commit.
    pub fn insert(&mut self, store: &TableStore, table: &str, row: Row) -> Result<RowId> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        table.schema().validate_row(row.values())?;
        let table_id = table.id();
        let n = self.provisional.entry(table_id).or_insert(0);
        let handle = provisional_row_id(*n);
        *n += 1;
        self.writes.insert(table_id, handle, row)?;
        Ok(handle)
    }

    /// Buffers an update of `row_id` to `row`, coalescing with any earlier
    /// write to the same row (design doc Q11).
    ///
    /// If `row_id` is a provisional insert handle, the entry must exist
    /// (insert→update = final insert). If it is a real id, the row's
    /// existence is validated at commit.
    pub fn update(
        &mut self,
        store: &TableStore,
        table: &str,
        row_id: RowId,
        row: Row,
    ) -> Result<()> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        table.schema().validate_row(row.values())?;
        let table_id = table.id();
        let first_write = !self.writes.contains(table_id, row_id);
        self.writes
            .update(table_id, row_id, is_provisional(row_id), row)?;
        // Write-time version capture (lost-update detection, ADR-004 D12): on
        // the first buffered write to a real row, record its current
        // committed version so a concurrent writer is detected at commit even
        // without an explicit prior read. Recorded only after the write
        // landed, so a rejected coalescing never pollutes the read set.
        if !is_provisional(row_id) && first_write {
            let observed = table.version_of(row_id);
            self.reads.record(table_id, row_id, observed);
        }
        Ok(())
    }

    /// Buffers a delete of `row_id`, coalescing with any earlier write to the
    /// same row (design doc Q11). The row's existence is validated at commit.
    pub fn delete(&mut self, store: &TableStore, table: &str, row_id: RowId) -> Result<()> {
        self.ensure_active()?;
        let table = resolve_table(store, table)?;
        let table_id = table.id();
        let first_write = !self.writes.contains(table_id, row_id);
        self.writes
            .delete(table_id, row_id, is_provisional(row_id))?;
        // Same write-time version capture as `update`.
        if !is_provisional(row_id) && first_write {
            let observed = table.version_of(row_id);
            self.reads.record(table_id, row_id, observed);
        }
        Ok(())
    }

    /// Validates and commits the transaction, returning the committed
    /// [`Change`] records (the future WAL/subscription attach point).
    ///
    /// On success the state becomes `Committed`. On any validation failure
    /// the state becomes `Aborted`, **zero** authoritative state is mutated,
    /// and **zero** Change records are produced — the caller may retry by
    /// beginning a new transaction.
    pub fn commit(&mut self, store: &mut TableStore) -> Result<Vec<Change>> {
        match self.state {
            TransactionState::Active => {}
            TransactionState::Committed => {
                return Err(Error::already_committed(format!(
                    "transaction {} already committed",
                    self.id
                )));
            }
            TransactionState::Aborted => {
                return Err(Error::already_aborted(format!(
                    "transaction {} already aborted",
                    self.id
                )));
            }
        }
        match commit::commit(store, self) {
            Ok(changes) => {
                self.state = TransactionState::Committed;
                Ok(changes)
            }
            Err(error) => {
                self.state = TransactionState::Aborted;
                Err(error)
            }
        }
    }

    /// Aborts the transaction: no mutation, no changes.
    ///
    /// Idempotent on an already-aborted transaction; an error on a committed
    /// one (a committed transaction cannot be "un-committed").
    pub fn abort(&mut self) -> Result<()> {
        match self.state {
            TransactionState::Active => {
                self.state = TransactionState::Aborted;
                Ok(())
            }
            TransactionState::Committed => Err(Error::already_committed(format!(
                "cannot abort transaction {}: already committed",
                self.id
            ))),
            TransactionState::Aborted => Ok(()),
        }
    }
}

/// Resolves a table by name, or fails with `NotFound`.
fn resolve_table<'s>(store: &'s TableStore, table: &str) -> Result<&'s nexum_table::Table> {
    store
        .table(table)
        .ok_or_else(|| Error::not_found(format!("table '{table}' does not exist")))
}

/// Returns `true` if `row` owns `key` in the named **unique** index
/// (`"primary"` or a unique secondary index).
///
/// Used by the read-your-writes overlay of [`Transaction::lookup_unique`].
/// `unique_keys` validates the row against the schema, which is safe here
/// because every write-set row was validated at write time.
fn row_owns_key(
    table: &nexum_table::Table,
    index_name: &str,
    key: &[Value],
    row: &Row,
) -> Result<bool> {
    Ok(table
        .unique_keys(row)?
        .into_iter()
        .any(|(name, k)| name == index_name && k == key))
}

/// Returns `true` if `row` owns `key` in the named **non-unique** secondary
/// index.
///
/// Used by the read-your-writes overlay of [`Transaction::lookup_index`].
/// `index_keys` validates the row against the schema, which is safe here
/// because every write-set row was validated at write time.
fn row_owns_index_key(
    table: &nexum_table::Table,
    index_name: &str,
    key: &[Value],
    row: &Row,
) -> Result<bool> {
    Ok(table
        .index_keys(row)?
        .into_iter()
        .any(|(name, k)| name == index_name && k == key))
}
