//! The read set: every transactional read observation.
//!
//! A [`ReadSet`] records two kinds of observation:
//!
//! 1. **Row observations** — `(TableId, RowId) → Option<Version>` for every
//!    point read a transaction performs:
//!    - `Some(version)` — the row existed at that version when read
//!    - `None` — the row was **absent** when read (missing, or already
//!      deleted)
//!
//!    `None` is a first-class observation, not a fake version: at validation
//!    time, a live row where the transaction observed absence is a conflict
//!    (another transaction inserted it), and a live absence where the
//!    transaction observed a version is a conflict (another transaction
//!    deleted it). See ADR-004 D3 and the Phase 4 design doc (Q3, Q7).
//!
//! 2. **Table observations** — `TableId → epoch` for every table observed
//!    *as a set* (`tx.scan`, `tx.lookup_unique`). Any committed row mutation
//!    advances the table's mutation epoch, so a mismatch at validation is a
//!    phantom conflict (ADR-004 D13). This is deliberately conservative.
//!
//! Row observations use a `BTreeMap` for O(log N) lookup and deterministic
//! sorted iteration (required by OCC validation). An insertion-order `Vec`
//! tracks the order of first inserts so that `truncate_to` can remove
//! recently-added entries in O(delta) time — the key optimization for the
//! Phase 7 snapshot/rollback pattern (O(1) snapshot, O(delta) rollback).

use std::collections::BTreeMap;

use nexum_core::{RowId, TableId, Version};

/// One read observation: the version the row had when it was read.
///
/// `None` means the row was observed absent.
pub type ReadObservation = Option<Version>;

/// The ordered collection of a transaction's read observations.
///
/// Uses a `BTreeMap` for O(log N) lookup and sorted iteration. An
/// insertion-order `Vec` tracks first-insert order for efficient
/// `truncate_to` (O(delta) rollback without cloning the full map).
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadSet {
    entries: BTreeMap<(TableId, RowId), ReadObservation>,
    /// Insertion order of *first* inserts (not overwrites). Used only by
    /// `truncate_to` to know which keys to remove from `entries` when
    /// rolling back a failed reducer call.
    insert_order: Vec<(TableId, RowId)>,
    tables: BTreeMap<TableId, Version>,
}

impl ReadSet {
    /// Creates an empty read set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Records (or overwrites, for a re-read) the observation of `row_id` in
    /// table `table_id`. The most recent read of a row wins.
    pub fn record(&mut self, table_id: TableId, row_id: RowId, observed: ReadObservation) {
        let key = (table_id, row_id);
        if self.entries.insert(key, observed).is_none() {
            // First observation of this row — track insertion order for
            // efficient rollback.
            self.insert_order.push(key);
        }
    }

    /// Merges every observation of `other` into this set (a union; the most
    /// recent observation of a row wins, so re-reads are idempotent).
    ///
    /// Used by the Phase 11 parallel executor to fold a child transaction's
    /// read set into the tick transaction (`Transaction::absorb`). Because
    /// the store is frozen during a tick, two observations of the same row
    /// always agree, so the union is exact.
    pub fn absorb(&mut self, other: &ReadSet) {
        for (&key, &observed) in &other.entries {
            self.record(key.0, key.1, observed);
        }
        for (&table_id, &epoch) in &other.tables {
            self.record_table(table_id, epoch);
        }
    }

    /// Returns the recorded observation for `row_id` in `table_id`, if the
    /// row was read.
    pub fn get(&self, table_id: TableId, row_id: RowId) -> Option<ReadObservation> {
        self.entries.get(&(table_id, row_id)).copied()
    }

    /// Records (or overwrites) a set observation of `table_id` at the
    /// observed mutation epoch.
    pub fn record_table(&mut self, table_id: TableId, observed_epoch: Version) {
        self.tables.insert(table_id, observed_epoch);
    }

    /// Returns the recorded epoch observation for `table_id`, if the table
    /// was observed as a set.
    pub fn get_table(&self, table_id: TableId) -> Option<Version> {
        self.tables.get(&table_id).copied()
    }

    /// Iterates over `(table_id, observed_epoch)` table observations in
    /// deterministic `TableId` order.
    pub fn table_entries(&self) -> impl Iterator<Item = (TableId, Version)> + '_ {
        self.tables
            .iter()
            .map(|(&table_id, &epoch)| (table_id, epoch))
    }

    /// Returns the number of recorded observations (rows + tables).
    pub fn len(&self) -> usize {
        self.entries.len() + self.tables.len()
    }

    /// Returns `true` if the read set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.tables.is_empty()
    }

    /// Iterates over `(table_id, row_id, observation)` in deterministic
    /// `(TableId, RowId)` order.
    pub fn entries(&self) -> impl Iterator<Item = (TableId, RowId, ReadObservation)> + '_ {
        self.entries
            .iter()
            .map(|(&(table_id, row_id), &observed)| (table_id, row_id, observed))
    }

    /// Returns the number of row observations (excludes table observations).
    pub fn entry_count(&self) -> usize {
        self.entries.len()
    }

    /// Truncates the row observations back to `len` entries, removing all
    /// entries added after that point. Table observations are not affected.
    ///
    /// Used by `Transaction::rollback` to efficiently undo a failed reducer
    /// call's read observations without cloning the entire read set.
    ///
    /// Cost: O(delta) where `delta` = number of entries to remove.
    pub fn truncate_to(&mut self, len: usize) {
        for &key in &self.insert_order[len..] {
            self.entries.remove(&key);
        }
        self.insert_order.truncate(len);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn records_and_overwrites_observations() {
        let mut reads = ReadSet::new();
        let table = TableId::from_u64(0);
        let a = RowId::from_u64(1);
        let b = RowId::from_u64(2);

        reads.record(table, a, Some(Version::from_u64(3)));
        reads.record(table, b, None); // observed absent

        assert_eq!(reads.get(table, a), Some(Some(Version::from_u64(3))));
        assert_eq!(reads.get(table, b), Some(None));
        assert_eq!(reads.get(table, RowId::from_u64(9)), None);
        assert_eq!(reads.len(), 2);

        // Re-read overwrites: latest observation wins.
        reads.record(table, a, Some(Version::from_u64(7)));
        assert_eq!(reads.get(table, a), Some(Some(Version::from_u64(7))));
    }

    #[test]
    fn iterates_in_deterministic_order() {
        let mut reads = ReadSet::new();
        // Insert out of order; iteration must be sorted by (TableId, RowId).
        reads.record(TableId::from_u64(1), RowId::from_u64(5), None);
        reads.record(
            TableId::from_u64(0),
            RowId::from_u64(9),
            Some(Version::ZERO),
        );
        reads.record(TableId::from_u64(0), RowId::from_u64(3), None);

        let order: Vec<(TableId, RowId)> = reads.entries().map(|(t, r, _)| (t, r)).collect();
        assert_eq!(
            order,
            vec![
                (TableId::from_u64(0), RowId::from_u64(3)),
                (TableId::from_u64(0), RowId::from_u64(9)),
                (TableId::from_u64(1), RowId::from_u64(5)),
            ]
        );
    }

    #[test]
    fn records_table_epoch_observations() {
        let mut reads = ReadSet::new();
        reads.record_table(TableId::from_u64(2), Version::from_u64(7));
        reads.record_table(TableId::from_u64(0), Version::from_u64(3));

        assert_eq!(
            reads.get_table(TableId::from_u64(0)),
            Some(Version::from_u64(3))
        );
        assert_eq!(reads.get_table(TableId::from_u64(1)), None);

        // Table observations iterate in TableId order and count toward len.
        let order: Vec<(TableId, Version)> = reads.table_entries().collect();
        assert_eq!(
            order,
            vec![
                (TableId::from_u64(0), Version::from_u64(3)),
                (TableId::from_u64(2), Version::from_u64(7)),
            ]
        );

        // Mixed with row observations.
        reads.record(
            TableId::from_u64(0),
            RowId::from_u64(1),
            Some(Version::ZERO),
        );
        assert_eq!(reads.len(), 3);
        assert!(!reads.is_empty());
    }

    #[test]
    fn empty_read_set() {
        let reads = ReadSet::new();
        assert!(reads.is_empty());
        assert_eq!(reads.entries().count(), 0);
    }

    #[test]
    fn truncate_removes_recent_entries() {
        let mut reads = ReadSet::new();
        let t0 = TableId::from_u64(0);
        let t1 = TableId::from_u64(1);

        reads.record(t0, RowId::from_u64(1), Some(Version::from_u64(1)));
        reads.record(t0, RowId::from_u64(2), Some(Version::from_u64(2)));
        reads.record(t1, RowId::from_u64(3), Some(Version::from_u64(3)));
        reads.record(t0, RowId::from_u64(4), Some(Version::from_u64(4)));
        assert_eq!(reads.entry_count(), 4);

        // Truncate back to 2 entries — removes entries 3 and 4.
        reads.truncate_to(2);
        assert_eq!(reads.entry_count(), 2);
        assert_eq!(reads.len(), 2);
        assert_eq!(
            reads.get(t0, RowId::from_u64(1)),
            Some(Some(Version::from_u64(1)))
        );
        assert_eq!(
            reads.get(t0, RowId::from_u64(2)),
            Some(Some(Version::from_u64(2)))
        );
        assert_eq!(reads.get(t1, RowId::from_u64(3)), None);
        assert_eq!(reads.get(t0, RowId::from_u64(4)), None);

        // Iteration still sorted.
        let order: Vec<(TableId, RowId)> = reads.entries().map(|(t, r, _)| (t, r)).collect();
        assert_eq!(
            order,
            vec![(t0, RowId::from_u64(1)), (t0, RowId::from_u64(2)),]
        );
    }

    #[test]
    fn overwrite_does_not_affect_truncate_order() {
        let mut reads = ReadSet::new();
        let t0 = TableId::from_u64(0);

        reads.record(t0, RowId::from_u64(1), Some(Version::from_u64(1)));
        reads.record(t0, RowId::from_u64(2), Some(Version::from_u64(2)));
        // Overwrite row 1 — should NOT add a new insert_order entry.
        reads.record(t0, RowId::from_u64(1), Some(Version::from_u64(10)));
        assert_eq!(reads.entry_count(), 2);

        reads.truncate_to(1);
        assert_eq!(reads.entry_count(), 1);
        // Row 1 was the first insert; truncating to 1 keeps it.
        assert_eq!(
            reads.get(t0, RowId::from_u64(1)),
            Some(Some(Version::from_u64(10)))
        );
        assert_eq!(reads.get(t0, RowId::from_u64(2)), None);
    }
}
