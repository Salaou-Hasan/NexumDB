//! The write set: a transaction's buffered writes, coalesced deterministically.
//!
//! A [`WriteSet`] holds at most one [`WriteEntry`] per `(TableId, RowId)`.
//! Writes are **buffered, never applied** until commit. Multiple operations
//! against the same row within one transaction coalesce by the documented
//! rules (design doc Q11, ADR-004 D5):
//!
//! | incoming | existing        | result                                   |
//! |----------|-----------------|------------------------------------------|
//! | insert   | — (fresh)       | `Insert(row)`                            |
//! | insert   | any             | `InvalidArgument` (duplicate handle)     |
//! | update   | — (real id)     | `Update(row)`                            |
//! | update   | `Insert(old)`   | `Insert(row)`  (final insert)            |
//! | update   | `Update(old)`   | `Update(row)`  (latest wins)             |
//! | update   | `Delete`        | `InvalidArgument` (delete→update)        |
//! | delete   | — (real id)     | `Delete`                                 |
//! | delete   | `Insert(_)`     | entry removed (insert→delete = no-op)    |
//! | delete   | `Update(_)`     | `Delete`  (update→delete)                |
//! | delete   | `Delete`        | `InvalidArgument` (already deleted)      |
//!
//! The set is a `BTreeMap` so commit applies and validation iterates in a
//! deterministic `(TableId, RowId)` order.
//!
//! # Copy-on-write branching (Phase 22)
//!
//! [`Transaction::branch_of`] previously **deep-copied** the parent's write
//! set per call — an O(parent-writes) cost that made the per-call reducer
//! path quadratic under tick bursts (Phase 22 measured ~480 µs/call at 2K
//! branch/invoke/absorb calls per tick vs ~12 µs isolated). The write set
//! now keeps two layers:
//!
//! - `base`: the **shared, immutable** write set inherited at branch time
//!   (an `Arc` — branching is O(1));
//! - `own`: this transaction's **private** writes (coalesced against the
//!   inherited view at write time).
//!
//! The *logical* write set is `own` overriding `base`. Branching a
//! top-level transaction (base-less) is an `Arc` clone; branching a
//! branched transaction materializes the folded logical map (the rare
//! nested case, still O(parent-writes) — exactly the old cost). Absorbing
//! folds only the child's own writes into the parent, applying the same
//! coalescing rules across the boundary; an `insert → delete` that spans
//! the branch resolves to a **net no-op** (the row never exists), using a
//! `Delete` tombstone when the inherited insert lives in an ancestor's
//! shared map — resolved to removal when the top-level transaction absorbs.
//!
//! All consumers read the logical view: `get`, `contains`, `scan`,
//! `lookup_unique`, `lookup_index`, `entries`, `len`, and `tables`. The
//! top-level transaction (the only one that commits) has no base, so
//! commit/validation iterate the own map exactly as before.

use std::collections::BTreeMap;
use std::sync::Arc;

use nexum_core::{Error, Result, Row, RowId, TableId};

/// One buffered write operation against one row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum WriteEntry {
    /// Insert a new row (keyed by a provisional `RowId` within the tx).
    Insert(Row),
    /// Replace an existing row's contents (keyed by a real `RowId`).
    Update(Row),
    /// Delete an existing row (keyed by a real `RowId`).
    Delete,
}

impl WriteEntry {
    /// Returns the row carried by insert/update entries, if any.
    pub fn row(&self) -> Option<&Row> {
        match self {
            Self::Insert(row) | Self::Update(row) => Some(row),
            Self::Delete => None,
        }
    }
}

type Key = (TableId, RowId);
type Map = BTreeMap<Key, WriteEntry>;

/// The ordered collection of a transaction's buffered writes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSet {
    /// Inherited writes from the parent at branch time (shared, immutable).
    base: Option<Arc<Map>>,
    /// This transaction's own writes (override `base`). Wrapped in `Arc` so
    /// [`branch`](Self::branch) can share it with children in O(1) — the
    /// child gets `Arc::clone`; the parent mutates via `Arc::make_mut` (cheap
    /// when refcount == 1, i.e. the common case).
    own: Arc<Map>,
}

impl WriteSet {
    /// Creates an empty write set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns a mutable reference to the own layer, cloning via
    /// `Arc::make_mut` only when the Arc is shared (refcount > 1).
    fn own_mut(&mut self) -> &mut Map {
        Arc::make_mut(&mut self.own)
    }

    /// Returns `true` if `row_id` in `table_id` already has a buffered write
    /// in this transaction's own layer.
    fn own_contains(&self, table_id: TableId, row_id: RowId) -> bool {
        self.own.contains_key(&(table_id, row_id))
    }

    /// Returns the buffered entry for `row_id` in `table_id`, if any
    /// (logical view: own layer overrides the inherited layer).
    pub fn get(&self, table_id: TableId, row_id: RowId) -> Option<&WriteEntry> {
        self.own
            .get(&(table_id, row_id))
            .or_else(|| self.base.as_ref().and_then(|b| b.get(&(table_id, row_id))))
    }

    /// Returns `true` if `row_id` in `table_id` has a buffered write in the
    /// **logical** view (own layer or inherited layer). Used by
    /// write-time version capture: a write against a row the parent already
    /// wrote is not the transaction's first write to it.
    pub fn contains(&self, table_id: TableId, row_id: RowId) -> bool {
        self.get(table_id, row_id).is_some()
    }

    /// Buffers an insert of `row`, keyed by a **fresh** (provisional) handle.
    ///
    /// Returns [`Error::invalid_transaction`] if the handle already has a
    /// buffered write (a caller must never re-insert the same handle). A
    /// provisional handle never collides with an inherited key — provisional
    /// ids are allocated by, and only ever known to, the transaction that
    /// created them — so the duplicate check is against the own layer only.
    pub fn insert(&mut self, table_id: TableId, row_id: RowId, row: Row) -> Result<()> {
        let key = (table_id, row_id);
        if self.own.contains_key(&key) {
            return Err(Error::invalid_transaction(format!(
                "duplicate insert for row handle {row_id} in table {table_id}"
            )));
        }
        self.own_mut().insert(key, WriteEntry::Insert(row));
        Ok(())
    }

    /// Buffers an update of `row_id` to `row`, coalescing per the rules
    /// above against the **logical** view (own, then inherited). The
    /// `is_provisional` flag tells the caller's validation whether a missing
    /// entry is a dangling insert handle (error) or a fresh write against a
    /// real row (allowed). The coalesced result always lands in the own
    /// layer.
    pub fn update(
        &mut self,
        table_id: TableId,
        row_id: RowId,
        is_provisional: bool,
        row: Row,
    ) -> Result<()> {
        let key = (table_id, row_id);
        match self.get(table_id, row_id) {
            None => {
                if is_provisional {
                    return Err(Error::invalid_transaction(format!(
                        "cannot update row handle {row_id}: it does not refer to a pending insert in this transaction"
                    )));
                }
                self.own_mut().insert(key, WriteEntry::Update(row));
                Ok(())
            }
            Some(WriteEntry::Insert(_)) => {
                // insert → update: the insert carries the final row values.
                self.own_mut().insert(key, WriteEntry::Insert(row));
                Ok(())
            }
            Some(WriteEntry::Update(_)) => {
                // update → update: the latest row wins.
                self.own_mut().insert(key, WriteEntry::Update(row));
                Ok(())
            }
            Some(WriteEntry::Delete) => Err(Error::invalid_transaction(format!(
                "cannot update row {row_id}: it was already deleted earlier in this transaction"
            ))),
        }
    }

    /// Buffers a delete of `row_id`, coalescing per the rules above against
    /// the **logical** view. The `is_provisional` flag distinguishes a
    /// dangling insert handle (error) from a real row (allowed, existence is
    /// checked at commit).
    ///
    /// An `insert → delete` in the own layer is a net no-op (the key is
    /// removed). An inherited `Insert` deleted here leaves a `Delete`
    /// tombstone in the own layer: the row is logically absent for this
    /// transaction, and the tombstone resolves to removal when an ancestor
    /// (ultimately the top-level transaction) absorbs it.
    pub fn delete(&mut self, table_id: TableId, row_id: RowId, is_provisional: bool) -> Result<()> {
        let key = (table_id, row_id);
        match self.get(table_id, row_id) {
            None => {
                if is_provisional {
                    return Err(Error::invalid_transaction(format!(
                        "cannot delete row handle {row_id}: it does not refer to a pending insert in this transaction"
                    )));
                }
                self.own_mut().insert(key, WriteEntry::Delete);
                Ok(())
            }
            Some(WriteEntry::Insert(_)) => {
                if self.own_contains(table_id, row_id) {
                    // insert → delete: the row is never created (net no-op).
                    self.own_mut().remove(&key);
                } else {
                    // Inherited insert → delete: tombstone the own layer so
                    // the row is logically absent and the net no-op resolves
                    // when the top-level transaction absorbs.
                    self.own_mut().insert(key, WriteEntry::Delete);
                }
                Ok(())
            }
            Some(WriteEntry::Update(_)) => {
                // update → delete: only the delete matters.
                self.own_mut().insert(key, WriteEntry::Delete);
                Ok(())
            }
            Some(WriteEntry::Delete) => Err(Error::invalid_transaction(format!(
                "cannot delete row {row_id}: it was already deleted earlier in this transaction"
            ))),
        }
    }

    /// Overwrites the buffered entry at `(table_id, row_id)` unconditionally
    /// in the own layer.
    ///
    /// Used by [`WriteSet::absorb`] (the Phase 11 parallel merge / Phase 22
    /// fold): the caller guarantees the incoming entry is the *final
    /// coalesced state* of that key — a child transaction's logical write set
    /// starts as the parent's, so any key present in both carries the
    /// child's post-coalesce value, and a key absent from the child was
    /// never touched by it. This method deliberately skips the coalescing
    /// rules of [`update`](Self::update)/[`delete`](Self::delete).
    pub fn set(&mut self, table_id: TableId, row_id: RowId, entry: WriteEntry) {
        self.own_mut().insert((table_id, row_id), entry);
    }

    /// Removes the own-layer entry at `(table_id, row_id)` (used by the
    /// absorb fold for a cross-branch `insert → delete` net no-op when the
    /// inherited insert lives in this transaction's own layer).
    #[expect(dead_code)]
    fn remove(&mut self, table_id: TableId, row_id: RowId) {
        self.own_mut().remove(&(table_id, row_id));
    }

    /// Returns the number of buffered writes (logical view).
    pub fn len(&self) -> usize {
        match &self.base {
            None => self.own.len(),
            Some(base) => {
                self.own.len() + base.len()
                    - base.keys().filter(|k| self.own.contains_key(k)).count()
            }
        }
    }

    /// Returns `true` if the write set is empty (logical view).
    pub fn is_empty(&self) -> bool {
        match &self.base {
            None => self.own.is_empty(),
            Some(base) => self.own.is_empty() && !base.keys().any(|k| !self.own.contains_key(k)),
        }
    }

    /// Iterates over `(table_id, row_id, entry)` in deterministic
    /// `(TableId, RowId)` order over the **logical** view (own layer
    /// overriding the inherited layer).
    pub fn entries(&self) -> impl Iterator<Item = (TableId, RowId, &WriteEntry)> + '_ {
        let mut own = self.own.iter().peekable();
        let mut inherited = self
            .base
            .as_ref()
            .map(|b| b.as_ref())
            .into_iter()
            .flatten()
            .peekable();
        std::iter::from_fn(move || {
            match (own.peek().copied(), inherited.peek().copied()) {
                (Some((o_key, o_entry)), Some((i_key, _))) => {
                    if o_key == i_key {
                        // Own overrides inherited.
                        inherited.next();
                        own.next();
                        Some((o_key.0, o_key.1, o_entry))
                    } else if o_key < i_key {
                        own.next();
                        Some((o_key.0, o_key.1, o_entry))
                    } else {
                        let (i_key, i_entry) = inherited.next().unwrap();
                        Some((i_key.0, i_key.1, i_entry))
                    }
                }
                (Some((o_key, o_entry)), None) => {
                    own.next();
                    Some((o_key.0, o_key.1, o_entry))
                }
                (None, Some((i_key, i_entry))) => {
                    inherited.next();
                    Some((i_key.0, i_key.1, i_entry))
                }
                (None, None) => None,
            }
        })
    }

    /// Iterates over the **own layer only** (used by the absorb fold, which
    /// consults the inherited layer per key for the net-no-op resolution).
    pub fn own_entries(&self) -> impl Iterator<Item = (TableId, RowId, &WriteEntry)> + '_ {
        self.own
            .iter()
            .map(|(&(table_id, row_id), entry)| (table_id, row_id, entry))
    }

    /// Iterates over entries for a specific table only (logical view).
    ///
    /// Filters the logical view to yield only entries matching `table_id`.
    /// Used by `lookup_unique` and `lookup_index` to avoid scanning the
    /// entire write set for entries belonging to other tables.
    pub fn entries_for_table(
        &self,
        table_id: TableId,
    ) -> impl Iterator<Item = (RowId, &WriteEntry)> + '_ {
        self.entries()
            .filter(move |(tid, _, _)| *tid == table_id)
            .map(|(_, row_id, entry)| (row_id, entry))
    }

    /// Returns `true` if any entry in the logical view is an `Insert`.
    ///
    /// `lookup_unique` / `lookup_index` scan all entries to find pending
    /// inserts that newly own an index key. When no inserts exist (common
    /// in update-heavy workloads), the scan can be skipped entirely.
    pub fn has_any_insert(&self) -> bool {
        self.own
            .values()
            .any(|e| matches!(e, WriteEntry::Insert(_)))
            || self
                .base
                .as_ref()
                .is_some_and(|b| b.values().any(|e| matches!(e, WriteEntry::Insert(_))))
    }

    /// Returns the table ids touched by this write set, in ascending order
    /// (logical view).
    pub fn tables(&self) -> impl Iterator<Item = TableId> + '_ {
        let mut tables: Vec<TableId> = self.entries().map(|(table_id, _, _)| table_id).collect();
        tables.sort_unstable();
        tables.dedup();
        tables.into_iter()
    }

    /// Branches this write set: the child inherits this transaction's
    /// logical write set as a shared `Arc` (O(1) for the common top-level
    /// parent) and starts with an empty own layer. The rare nested case —
    /// branching a transaction that already has a base — materializes the
    /// folded logical map, preserving the pre-Phase-22 O(parent-writes)
    /// cost exactly.
    pub fn branch(&self) -> WriteSet {
        let inherited = match &self.base {
            None => Arc::clone(&self.own),
            Some(base) => {
                // Fold: own overrides base, insert→delete nets to removal.
                let mut logical: Map = base.as_ref().clone();
                for (&key, entry) in self.own.iter() {
                    match (logical.get(&key), entry) {
                        (Some(WriteEntry::Insert(_)), WriteEntry::Delete) => {
                            logical.remove(&key);
                        }
                        (_, entry) => {
                            logical.insert(key, entry.clone());
                        }
                    }
                }
                Arc::new(logical)
            }
        };
        WriteSet {
            base: Some(inherited),
            own: Arc::new(Map::new()),
        }
    }

    /// Folds a child's write set into this one (the Phase 11 parallel merge
    /// with Phase 22 cross-branch coalescing).
    ///
    /// Takes the child by value so its `base` Arc is dropped before we
    /// modify our own layer — this keeps `Arc::make_mut` on our `own` at
    /// refcount == 1 (O(1), no clone).
    ///
    /// Only the child's own layer is iterated — the inherited layer is this
    /// transaction's write set at branch time, so the child's own entries
    /// are exactly its deltas. Each delta is folded against this
    /// transaction's **logical** view per the coalescing rules; an
    /// `insert → delete` spanning the branch is a net no-op (the row never
    /// exists): the own-layer case removes the entry, the inherited-layer
    /// case leaves a `Delete` tombstone that the next absorb resolves.
    pub fn absorb(&mut self, child: WriteSet) {
        // Destructure and EXPLICITLY drop the child's inherited layer before
        // touching `self.own`: that layer is an `Arc` handle on our own map,
        // and it must be released so `make_mut` below sees refcount 1.
        // A `_` pattern binding here does NOT release it in time — measured
        // (Phase 26 investigation): with `base: _`, `make_mut` observed
        // refcount 2 and deep-cloned the entire accumulated parent map per
        // call (~50-100 µs at 5K entries, growing O(parent-writes)); with an
        // explicit drop, absorb is ~O(child-writes · log parent).
        let WriteSet {
            base,
            own: child_own,
        } = child;
        drop(base);
        // Fast path: if no Delete entries in child, no coalescing needed.
        let has_delete = child_own.values().any(|e| matches!(e, WriteEntry::Delete));
        if !has_delete {
            let own = Arc::make_mut(&mut self.own);
            match Arc::try_unwrap(child_own) {
                Ok(map) => {
                    for (key, entry) in map {
                        own.insert(key, entry);
                    }
                }
                Err(arc) => {
                    for (&key, entry) in arc.iter() {
                        own.insert(key, entry.clone());
                    }
                }
            }
            return;
        }
        // Slow path: child has deletes that may net-no-op with inherited inserts.
        for (&(table_id, row_id), entry) in child_own.iter() {
            let key = (table_id, row_id);
            // Snapshot the logical view (immutable borrow ends after clone).
            let logical: Option<WriteEntry> = self.get(table_id, row_id).cloned();
            // Refcount is 1 → make_mut is O(1).
            let own = Arc::make_mut(&mut self.own);
            match (logical.as_ref(), entry) {
                (Some(WriteEntry::Insert(_)), WriteEntry::Delete) => {
                    // insert -> delete net no-op: remove from own, or
                    // tombstone if the insert lives in base.
                    if own.remove(&key).is_none() {
                        own.insert(key, WriteEntry::Delete);
                    }
                }
                _ => {
                    own.insert(key, entry.clone());
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;

    const T: TableId = TableId::from_u64(0);

    fn r0() -> RowId {
        RowId::from_u64(0)
    }
    fn r1() -> RowId {
        RowId::from_u64(1)
    }

    #[test]
    fn insert_then_update_coalesces_to_final_insert() {
        let mut ws = WriteSet::new();
        ws.insert(T, r0(), row![1u64, 10u64]).unwrap();
        ws.update(T, r0(), true, row![1u64, 20u64]).unwrap();
        assert_eq!(
            ws.get(T, r0()),
            Some(&WriteEntry::Insert(row![1u64, 20u64]))
        );
        assert_eq!(ws.len(), 1);
    }

    #[test]
    fn insert_then_delete_is_a_net_noop() {
        let mut ws = WriteSet::new();
        ws.insert(T, r0(), row![1u64, 10u64]).unwrap();
        ws.delete(T, r0(), true).unwrap();
        assert!(ws.is_empty());
    }

    #[test]
    fn update_then_update_keeps_latest() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64, 10u64]).unwrap();
        ws.update(T, r0(), false, row![1u64, 30u64]).unwrap();
        assert_eq!(
            ws.get(T, r0()),
            Some(&WriteEntry::Update(row![1u64, 30u64]))
        );
        assert_eq!(ws.len(), 1);
    }

    #[test]
    fn update_then_delete_becomes_delete() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64, 10u64]).unwrap();
        ws.delete(T, r0(), false).unwrap();
        assert_eq!(ws.get(T, r0()), Some(&WriteEntry::Delete));
        assert_eq!(ws.len(), 1);
    }

    #[test]
    fn delete_then_update_is_rejected() {
        let mut ws = WriteSet::new();
        ws.delete(T, r0(), false).unwrap();
        let err = ws.update(T, r0(), false, row![1u64, 10u64]).unwrap_err();
        assert!(matches!(err, Error::InvalidTransaction(_)));
    }

    #[test]
    fn delete_then_delete_is_rejected() {
        let mut ws = WriteSet::new();
        ws.delete(T, r0(), false).unwrap();
        let err = ws.delete(T, r0(), false).unwrap_err();
        assert!(matches!(err, Error::InvalidTransaction(_)));
    }

    #[test]
    fn duplicate_insert_of_one_handle_is_rejected() {
        let mut ws = WriteSet::new();
        ws.insert(T, r0(), row![1u64]).unwrap();
        let err = ws.insert(T, r0(), row![2u64]).unwrap_err();
        assert!(matches!(err, Error::InvalidTransaction(_)));
    }

    #[test]
    fn dangling_provisional_handles_are_rejected() {
        let mut ws = WriteSet::new();
        // A provisional id with no pending insert is a dangling handle.
        let prov = RowId::from_u64(1 << 63);
        assert!(ws.update(T, prov, true, row![1u64]).is_err());
        assert!(ws.delete(T, prov, true).is_err());
    }

    #[test]
    fn fresh_real_row_ops_are_allowed() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64]).unwrap();
        ws.delete(T, r1(), false).unwrap();
        assert_eq!(ws.len(), 2);
    }

    #[test]
    fn iterates_and_lists_tables_in_order() {
        let mut ws = WriteSet::new();
        ws.insert(TableId::from_u64(2), r0(), row![1u64]).unwrap();
        ws.insert(TableId::from_u64(0), r0(), row![2u64]).unwrap();
        ws.insert(TableId::from_u64(1), r1(), row![3u64]).unwrap();

        let ids: Vec<TableId> = ws.tables().collect();
        assert_eq!(
            ids,
            vec![
                TableId::from_u64(0),
                TableId::from_u64(1),
                TableId::from_u64(2)
            ]
        );

        let keys: Vec<(TableId, RowId)> = ws.entries().map(|(t, r, _)| (t, r)).collect();
        assert_eq!(
            keys,
            vec![
                (TableId::from_u64(0), r0()),
                (TableId::from_u64(1), r1()),
                (TableId::from_u64(2), r0()),
            ]
        );
    }

    // -------------------------------------------------- Phase 22 COW

    #[test]
    fn branch_is_arc_share_and_logical_view_matches_parent() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64, 10u64]).unwrap();
        let child = ws.branch();
        assert!(child.base.is_some());
        assert!(child.own.is_empty());
        assert_eq!(child.len(), 1);
        assert_eq!(
            child.get(T, r0()),
            Some(&WriteEntry::Update(row![1u64, 10u64]))
        );
        let keys: Vec<(TableId, RowId)> = child.entries().map(|(t, r, _)| (t, r)).collect();
        assert_eq!(keys, vec![(T, r0())]);
    }

    #[test]
    fn child_writes_do_not_mutate_parent() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64, 10u64]).unwrap();
        let mut child = ws.branch();
        child.update(T, r0(), false, row![1u64, 99u64]).unwrap();
        child.insert(T, r1(), row![2u64]).unwrap();
        // The child sees the logical overlay; the parent is untouched.
        assert_eq!(
            child.get(T, r0()),
            Some(&WriteEntry::Update(row![1u64, 99u64]))
        );
        assert_eq!(child.get(T, r1()), Some(&WriteEntry::Insert(row![2u64])));
        assert_eq!(
            ws.get(T, r0()),
            Some(&WriteEntry::Update(row![1u64, 10u64]))
        );
        assert!(ws.get(T, r1()).is_none());
        assert_eq!(ws.len(), 1);
        assert_eq!(child.len(), 2);
    }

    #[test]
    fn absorb_folds_child_own_writes_over_parent() {
        let mut ws = WriteSet::new();
        ws.update(T, r0(), false, row![1u64, 10u64]).unwrap();
        let mut child = ws.branch();
        child.update(T, r0(), false, row![1u64, 99u64]).unwrap();
        child.insert(T, r1(), row![2u64]).unwrap();
        ws.absorb(child);
        assert_eq!(
            ws.get(T, r0()),
            Some(&WriteEntry::Update(row![1u64, 99u64]))
        );
        assert_eq!(ws.get(T, r1()), Some(&WriteEntry::Insert(row![2u64])));
        assert_eq!(ws.len(), 2);
    }

    #[test]
    fn cross_branch_insert_then_delete_is_net_noop() {
        // Parent inserts a provisional row; the child deletes it. The row
        // never exists: absorb removes it from the parent entirely.
        let mut ws = WriteSet::new();
        let prov = RowId::from_u64(1 << 63);
        ws.insert(T, prov, row![1u64, 10u64]).unwrap();
        let mut child = ws.branch();
        child.delete(T, prov, true).unwrap();
        // The child's own layer carries the Delete tombstone (Transaction::get
        // maps it to `None`); the inherited insert is overridden.
        assert_eq!(child.get(T, prov), Some(&WriteEntry::Delete));
        ws.absorb(child);
        assert!(
            ws.get(T, prov).is_none(),
            "parent insert removed (net no-op)"
        );
        assert!(ws.is_empty());
    }

    #[test]
    fn nested_branch_materializes_and_resolves_tombstone() {
        // Top-level inserts prov; branch B (nested parent); branch C from B;
        // C deletes prov; C absorbs into B; B absorbs into the top level.
        // The row never exists anywhere.
        let mut top = WriteSet::new();
        let prov = RowId::from_u64(1 << 63);
        top.insert(T, prov, row![1u64, 10u64]).unwrap();

        let mut b = top.branch();
        b.update(T, r0(), false, row![5u64]).unwrap();

        let mut c = b.branch();
        assert_eq!(c.get(T, prov), Some(&WriteEntry::Insert(row![1u64, 10u64])));
        assert_eq!(c.get(T, r0()), Some(&WriteEntry::Update(row![5u64])));
        c.delete(T, prov, true).unwrap();
        assert_eq!(c.get(T, prov), Some(&WriteEntry::Delete));
        b.absorb(c);
        // B's logical view: the inherited insert is tombstombed (Transaction::get
        // maps the Delete to `None` — the row is logically absent).
        assert_eq!(b.get(T, prov), Some(&WriteEntry::Delete));
        assert_eq!(b.get(T, r0()), Some(&WriteEntry::Update(row![5u64])));
        top.absorb(b);
        assert!(top.get(T, prov).is_none());
        assert_eq!(top.get(T, r0()), Some(&WriteEntry::Update(row![5u64])));
        assert_eq!(top.len(), 1);
    }

    #[test]
    fn entries_logical_merge_is_deterministic() {
        let mut parent = WriteSet::new();
        parent.update(T, r0(), false, row![1u64]).unwrap();
        parent
            .insert(TableId::from_u64(1), r1(), row![2u64])
            .unwrap();
        let mut child = parent.branch();
        child.update(T, r0(), false, row![9u64]).unwrap();
        // Distinct own key: table 0, row 1 (row![3u64] is the payload).
        child.insert(T, r1(), row![3u64]).unwrap();
        let keys: Vec<(TableId, RowId, &WriteEntry)> = child.entries().collect();
        assert_eq!(keys.len(), 3);
        assert_eq!(keys[0], (T, r0(), &WriteEntry::Update(row![9u64])));
        assert_eq!(keys[1], (T, r1(), &WriteEntry::Insert(row![3u64])));
        assert_eq!(
            keys[2],
            (TableId::from_u64(1), r1(), &WriteEntry::Insert(row![2u64]))
        );
    }
}
