//! [`StorageTable`]: the authoritative in-memory state of a single table.
//!
//! `StorageTable.rows` — a `BTreeMap<RowId, StoredRow>` — is the **one**
//! authoritative representation of a table's rows. Row data, row identity,
//! and row version live together in each `StoredRow`, so they cannot diverge.
//! Everything else (indexes, change buffers, caches) is derived
//! infrastructure (ADR-003 D1, D4).
//!
//! `StorageTable` is deliberately index-agnostic: it has no knowledge of
//! primary keys or secondary indexes. The table layer (`nexum-table`) owns
//! derived indexes and orchestrates validate → check → mutate → commit. A
//! full scan of `StorageTable` is sufficient to rebuild every index, which is
//! what makes them provably derived.
//!
//! Concurrency: this type assumes **single-threaded exclusive ownership** —
//! every mutation requires `&mut self`. No locks, no atomics (ADR-003 D7).

use std::collections::BTreeMap;

use nexum_core::schema::TableSchema;
use nexum_core::{Error, Result, RowId, TableId, Version};
use nexum_core::Row;

use crate::change::Change;
use crate::snapshot::TableState;

/// A stored row: the authoritative row data together with its version.
///
/// Row and version are one record so that version tracking can never diverge
/// from the data it versions.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StoredRow {
    row: Row,
    version: Version,
}

impl StoredRow {
    /// Returns the row data.
    pub fn row(&self) -> &Row {
        &self.row
    }

    /// Returns the row's current version.
    pub fn version(&self) -> Version {
        self.version
    }
}

/// The authoritative in-memory state of one table.
#[derive(Debug)]
pub struct StorageTable {
    id: TableId,
    schema: TableSchema,
    rows: BTreeMap<RowId, StoredRow>,
    next_row_id: u64,
    /// The table's **mutation epoch**: advanced by every committed row
    /// mutation. Set observations (transactional scans, Phase 4 correction)
    /// record it to detect phantom changes. Authoritative metadata, restored
    /// exactly by snapshots and reconstructed exactly by WAL replay.
    epoch: Version,
    changes: Vec<Change>,
}

impl StorageTable {
    /// Creates an empty storage table for `schema`.
    ///
    /// The schema must be already validated (it can only be constructed via
    /// `TableSchema::builder`, which validates), so this is infallible.
    pub fn new(id: TableId, schema: TableSchema) -> Self {
        Self {
            id,
            schema,
            rows: BTreeMap::new(),
            next_row_id: 0,
            epoch: Version::ZERO,
            changes: Vec::new(),
        }
    }

    /// Returns the table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        self.schema.name()
    }

    /// Returns the table schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.rows.len()
    }

    /// Returns `true` if the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Returns `true` if a row with `row_id` exists.
    pub fn contains(&self, row_id: RowId) -> bool {
        self.rows.contains_key(&row_id)
    }

    /// Returns the table's mutation epoch.
    ///
    /// Advanced by exactly one on every committed row mutation (insert /
    /// delete / effective update; Phase 3 no-op updates do **not** advance
    /// it). Transactional set observations (scans, unique-key lookups)
    /// record it to detect phantom changes (ADR-004 D13).
    pub fn epoch(&self) -> Version {
        self.epoch
    }

    /// Inserts a row, assigning a fresh monotonic `RowId` and an initial
    /// version of [`Version::ZERO`], and appends an insert change.
    ///
    /// The row is validated against the schema (arity and types) before any
    /// state changes; a failed insert leaves the table untouched.
    pub fn insert(&mut self, row: Row) -> Result<RowId> {
        self.schema.validate_row(row.values())?;

        let row_id = RowId::from_u64(self.next_row_id);
        self.next_row_id += 1;

        let version = Version::ZERO;
        self.changes.push(Change::insert(self.id, row_id, row.clone(), version));
        self.rows.insert(
            row_id,
            StoredRow {
                row,
                version,
            },
        );
        self.epoch = self.epoch.next();

        Ok(row_id)
    }

    /// Returns the stored row (data and version) for `row_id`, if any.
    ///
    /// This is the OCC-friendly read: a transaction can obtain the row and
    /// its version atomically from one record.
    pub fn get(&self, row_id: RowId) -> Option<&StoredRow> {
        self.rows.get(&row_id)
    }

    /// Returns the row data for `row_id`, if any.
    pub fn get_row(&self, row_id: RowId) -> Option<&Row> {
        self.rows.get(&row_id).map(StoredRow::row)
    }

    /// Returns the current version of `row_id`, if the row exists.
    ///
    /// Deleted rows return `None` — deletion is observable as the absence of
    /// the row.
    pub fn version_of(&self, row_id: RowId) -> Option<Version> {
        self.rows.get(&row_id).map(StoredRow::version)
    }

    /// Replaces the row with `row_id` with a full new row, advancing its
    /// version by exactly one, and appends an update change with both the old
    /// and new rows and versions.
    ///
    /// Validates the new row against the schema first; on any failure the
    /// table is left untouched.
    ///
    /// A **no-op update** — new row identical to the current row — leaves the
    /// version and change buffer untouched: it emits no change and cannot
    /// cause spurious OCC conflicts or noisy subscription deltas.
    pub fn update(&mut self, row_id: RowId, row: Row) -> Result<()> {
        self.schema.validate_row(row.values())?;

        let stored = self.rows.get_mut(&row_id).ok_or_else(|| {
            Error::not_found(format!(
                "row {row_id} does not exist in table '{}'",
                self.schema.name()
            ))
        })?;

        if stored.row == row {
            return Ok(());
        }

        let old_row = stored.row.clone();
        let old_version = stored.version;
        let new_version = old_version.next();

        stored.row = row.clone();
        stored.version = new_version;
        self.epoch = self.epoch.next();

        self.changes.push(Change::update(
            self.id,
            row_id,
            old_row,
            old_version,
            row,
            new_version,
        ));

        Ok(())
    }

    /// Deletes the row with `row_id`, appending a delete change that records
    /// the final row and version.
    pub fn delete(&mut self, row_id: RowId) -> Result<()> {
        let stored = self.rows.remove(&row_id).ok_or_else(|| {
            Error::not_found(format!(
                "row {row_id} does not exist in table '{}'",
                self.schema.name()
            ))
        })?;

        self.epoch = self.epoch.next();

        self.changes.push(Change::delete(
            self.id,
            row_id,
            stored.row,
            stored.version,
        ));

        Ok(())
    }

    /// Iterates over all rows in ascending `RowId` order — deterministic,
    /// which the simulation phase requires.
    pub fn scan(&self) -> impl Iterator<Item = (RowId, &StoredRow)> {
        self.rows.iter().map(|(&row_id, stored)| (row_id, stored))
    }

    /// Peeks at the buffered changes since the last drain, in commit order.
    pub fn changes(&self) -> &[Change] {
        &self.changes
    }

    /// Returns the buffered changes since the last drain and clears the
    /// buffer. Changes are consumed exactly once.
    pub fn drain_changes(&mut self) -> Vec<Change> {
        std::mem::take(&mut self.changes)
    }

    /// Captures the table's complete authoritative state for snapshotting
    /// (ADR-005 D6): id, schema, rows with exact versions, `next_row_id`,
    /// and the mutation epoch. Indexes and change buffers are not captured.
    pub fn table_state(&self) -> TableState {
        TableState {
            id: self.id,
            schema: self.schema.clone(),
            rows: self
                .rows
                .iter()
                .map(|(&row_id, stored)| (row_id, stored.row().clone(), stored.version()))
                .collect(),
            next_row_id: self.next_row_id,
            epoch: self.epoch,
        }
    }

    /// Reconstructs a storage table from a captured [`TableState`].
    ///
    /// Restores rows with their exact versions, the `next_row_id` counter,
    /// and the mutation epoch; the change buffer starts empty. Every row is
    /// validated against the schema and the row-id/counter invariants are
    /// enforced (defense in depth — [`TableState::decode`] checks them
    /// first).
    pub fn from_state(state: TableState) -> Result<Self> {
        let mut rows = BTreeMap::new();
        for (row_id, row, version) in state.rows {
            state.schema.validate_row(row.values())?;
            if rows.insert(row_id, StoredRow { row, version }).is_some() {
                return Err(Error::internal(format!(
                    "snapshot: duplicate row id {row_id} in table '{}'",
                    state.schema.name()
                )));
            }
        }
        if let Some(&max_id) = rows.keys().next_back()
            && max_id.as_u64() >= state.next_row_id
        {
            return Err(Error::internal(format!(
                "snapshot: next_row_id {} must exceed the highest row id {} in table '{}'",
                state.next_row_id,
                max_id.as_u64(),
                state.schema.name()
            )));
        }
        Ok(Self {
            id: state.id,
            schema: state.schema,
            rows,
            next_row_id: state.next_row_id,
            epoch: state.epoch,
            changes: Vec::new(),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;
    use nexum_core::{ChangeKind, ColumnType};

    fn schema() -> TableSchema {
        TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .build()
            .unwrap()
    }

    fn table() -> StorageTable {
        StorageTable::new(TableId::from_u64(0), schema())
    }

    #[test]
    fn insert_assigns_monotonic_ids_at_version_zero() {
        let mut t = table();
        let a = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        let b = t.insert(row![2u64, 10u64, 90i32]).unwrap();
        assert_eq!(a, RowId::from_u64(0));
        assert_eq!(b, RowId::from_u64(1));
        assert_eq!(t.version_of(a), Some(Version::ZERO));
        assert_eq!(t.len(), 2);
    }

    #[test]
    fn get_returns_row_and_version_together() {
        let mut t = table();
        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        let stored = t.get(id).expect("row exists");
        assert_eq!(stored.row().get_named(t.schema(), "health"), Some(&nexum_core::Value::I32(100)));
        assert_eq!(stored.version(), Version::ZERO);
        assert!(t.get(RowId::from_u64(99)).is_none());
    }

    #[test]
    fn insert_validates_schema() {
        let mut t = table();
        let err = t.insert(row![1u64, 10u64, "oops".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
        assert!(t.is_empty());
        assert!(t.changes().is_empty());
    }

    #[test]
    fn noop_update_bumps_nothing_and_emits_no_change() {
        let mut t = table();
        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        let row = row![1u64, 10u64, 100i32];

        t.update(id, row.clone()).unwrap();

        assert_eq!(t.version_of(id), Some(Version::ZERO));
        assert_eq!(t.changes().len(), 1); // only the insert change
        // A no-op update cannot change any predicate result: the epoch stays.
        assert_eq!(t.epoch(), Version::from_u64(1)); // one for the insert only
    }

    #[test]
    fn mutation_epoch_advances_on_every_real_mutation() {
        let mut t = table();
        assert_eq!(t.epoch(), Version::ZERO);

        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        assert_eq!(t.epoch(), Version::from_u64(1));

        t.update(id, row![1u64, 10u64, 50i32]).unwrap();
        assert_eq!(t.epoch(), Version::from_u64(2));

        t.delete(id).unwrap();
        assert_eq!(t.epoch(), Version::from_u64(3));
    }

    #[test]
    fn update_bumps_version_by_exactly_one() {
        let mut t = table();
        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        t.update(id, row![1u64, 10u64, 50i32]).unwrap();
        assert_eq!(t.version_of(id), Some(Version::from_u64(1)));
        t.update(id, row![1u64, 10u64, 25i32]).unwrap();
        assert_eq!(t.version_of(id), Some(Version::from_u64(2)));
        assert_eq!(t.get_row(id).unwrap().get_named(t.schema(), "health"), Some(&nexum_core::Value::I32(25)));
    }

    #[test]
    fn update_missing_row_errors_and_records_nothing() {
        let mut t = table();
        let err = t.update(RowId::from_u64(9), row![1u64, 10u64, 100i32]).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
        assert!(t.changes().is_empty());
    }

    #[test]
    fn delete_removes_row_and_observability() {
        let mut t = table();
        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        t.update(id, row![1u64, 10u64, 50i32]).unwrap();
        t.delete(id).unwrap();

        assert!(!t.contains(id));
        assert!(t.get(id).is_none());
        assert!(t.version_of(id).is_none());
        assert!(t.is_empty());

        let err = t.delete(id).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn changes_reflect_commits_in_order() {
        let mut t = table();
        let a = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        let b = t.insert(row![2u64, 10u64, 90i32]).unwrap();
        t.update(a, row![1u64, 10u64, 50i32]).unwrap();
        t.delete(b).unwrap();

        let changes = t.drain_changes();
        assert_eq!(changes.len(), 4);

        assert_eq!(changes[0].kind(), ChangeKind::Insert);
        assert_eq!(changes[0].row_id(), a);
        assert_eq!(changes[0].new_version(), Some(Version::ZERO));

        assert_eq!(changes[1].kind(), ChangeKind::Insert);
        assert_eq!(changes[1].row_id(), b);

        assert_eq!(changes[2].kind(), ChangeKind::Update);
        assert_eq!(changes[2].row_id(), a);
        assert_eq!(changes[2].old_version(), Some(Version::ZERO));
        assert_eq!(changes[2].new_version(), Some(Version::from_u64(1)));

        assert_eq!(changes[3].kind(), ChangeKind::Delete);
        assert_eq!(changes[3].row_id(), b);
        assert_eq!(changes[3].old_version(), Some(Version::ZERO));

        assert!(t.changes().is_empty());
    }

    #[test]
    fn drain_consumes_changes_exactly_once() {
        let mut t = table();
        t.insert(row![1u64, 10u64, 100i32]).unwrap();
        t.drain_changes();
        assert!(t.changes().is_empty());
        assert_eq!(t.drain_changes().len(), 0);
    }

    #[test]
    fn scan_is_deterministic_ascending() {
        let mut t = table();
        t.insert(row![3u64, 30u64, 100i32]).unwrap();
        t.insert(row![1u64, 10u64, 100i32]).unwrap();
        t.insert(row![2u64, 20u64, 100i32]).unwrap();
        let ids: Vec<RowId> = t.scan().map(|(id, _)| id).collect();
        assert_eq!(ids, vec![RowId::from_u64(0), RowId::from_u64(1), RowId::from_u64(2)]);
    }

    #[test]
    fn failed_update_leaves_everything_untouched() {
        let mut t = table();
        let id = t.insert(row![1u64, 10u64, 100i32]).unwrap();
        let before_version = t.version_of(id).unwrap();

        let err = t.update(id, row![1u64, 10u64, "oops".to_string()]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));

        assert_eq!(t.version_of(id), Some(before_version));
        assert_eq!(t.len(), 1);
        assert_eq!(t.changes().len(), 1); // only the insert change
    }
}
