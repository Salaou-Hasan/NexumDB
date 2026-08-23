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
//! Both sets are `BTreeMap`s so validation iterates in a deterministic order.

use std::collections::BTreeMap;

use nexum_core::{RowId, TableId, Version};

/// One read observation: the version the row had when it was read.
///
/// `None` means the row was observed absent.
pub type ReadObservation = Option<Version>;

/// The ordered collection of a transaction's read observations.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReadSet {
    entries: BTreeMap<(TableId, RowId), ReadObservation>,
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
        self.entries.insert((table_id, row_id), observed);
    }

    /// Merges every observation of `other` into this set (a union; the most
    /// recent observation of a row wins, so re-reads are idempotent).
    ///
    /// Used by the Phase 11 parallel executor to fold a child transaction's
    /// read set into the tick transaction (`Transaction::absorb`). Because
    /// the store is frozen during a tick, two observations of the same row
    /// always agree, so the union is exact.
    pub fn absorb(&mut self, other: &ReadSet) {
        for (table_id, row_id, observed) in other.entries() {
            self.record(table_id, row_id, observed);
        }
        for (table_id, epoch) in other.table_entries() {
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
}
