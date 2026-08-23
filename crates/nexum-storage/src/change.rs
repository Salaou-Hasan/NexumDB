//! Change records: the observable result of committed storage mutations.
//!
//! A [`Change`] is the minimum useful representation of one committed
//! mutation — enough for future transaction change sets, subscriptions, and
//! WAL records to understand what changed, without making change tracking
//! itself authoritative.
//!
//! Deliberately **not** included: a list of changed columns. Subscriptions can
//! diff `old_row` vs `new_row` when they need per-column deltas; storing the
//! diff would duplicate the row data and force every writer to compute it
//! (ADR-003 D5).

use std::sync::Arc;

use nexum_core::Row;
use nexum_core::{ChangeKind, RowId, TableId, Version};

/// One committed mutation of one row.
///
/// Row payloads are held as `Arc<Row>` **shared across consumers** (ADR-019
/// D4): the commit path wraps each row once, then the WAL and every
/// subscription window share the same allocation via refcount bumps instead
/// of deep-cloning the row per consumer — the measured subscription hot path
/// (O(changes × subscriptions) clones per tick). `old_row`/`new_row` return
/// plain `&Row` (deref), so readers are unchanged.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Change {
    table_id: TableId,
    kind: ChangeKind,
    row_id: RowId,
    old_row: Option<Arc<Row>>,
    new_row: Option<Arc<Row>>,
    old_version: Option<Version>,
    new_version: Option<Version>,
}

impl Change {
    /// Builds an insert change: a new row at its initial version.
    pub fn insert(table_id: TableId, row_id: RowId, row: Row, version: Version) -> Self {
        Self {
            table_id,
            kind: ChangeKind::Insert,
            row_id,
            old_row: None,
            new_row: Some(Arc::new(row)),
            old_version: None,
            new_version: Some(version),
        }
    }

    /// Builds an update change: a row replaced with a new version.
    pub fn update(
        table_id: TableId,
        row_id: RowId,
        old_row: Row,
        old_version: Version,
        new_row: Row,
        new_version: Version,
    ) -> Self {
        Self {
            table_id,
            kind: ChangeKind::Update,
            row_id,
            old_row: Some(Arc::new(old_row)),
            new_row: Some(Arc::new(new_row)),
            old_version: Some(old_version),
            new_version: Some(new_version),
        }
    }

    /// Builds a delete change: a row removed at its final version.
    pub fn delete(table_id: TableId, row_id: RowId, row: Row, version: Version) -> Self {
        Self {
            table_id,
            kind: ChangeKind::Delete,
            row_id,
            old_row: Some(Arc::new(row)),
            new_row: None,
            old_version: Some(version),
            new_version: None,
        }
    }

    /// Returns the table this change belongs to.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns the kind of change.
    pub fn kind(&self) -> ChangeKind {
        self.kind
    }

    /// Returns the row id that changed.
    pub fn row_id(&self) -> RowId {
        self.row_id
    }

    /// Returns the row before the change, if this is an update or delete.
    pub fn old_row(&self) -> Option<&Row> {
        self.old_row.as_deref()
    }

    /// Returns the row after the change, if this is an insert or update.
    pub fn new_row(&self) -> Option<&Row> {
        self.new_row.as_deref()
    }

    /// Returns the shared row payload after the change, if this is an insert
    /// or update. Consumers that retain the row (e.g. subscription windows)
    /// clone this `Arc` instead of deep-cloning the row, so one committed
    /// row is allocated once and shared across every consumer (ADR-019 D4).
    pub fn new_row_shared(&self) -> Option<&Arc<Row>> {
        self.new_row.as_ref()
    }

    /// Returns the version before the change, if this is an update or delete.
    pub fn old_version(&self) -> Option<Version> {
        self.old_version
    }

    /// Returns the version after the change, if this is an insert or update.
    pub fn new_version(&self) -> Option<Version> {
        self.new_version
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;

    #[test]
    fn insert_change_has_only_new_state() {
        let change = Change::insert(
            TableId::from_u64(0),
            RowId::from_u64(1),
            row![1u64, 10u64],
            Version::ZERO,
        );
        assert_eq!(change.table_id(), TableId::from_u64(0));
        assert_eq!(change.kind(), ChangeKind::Insert);
        assert_eq!(change.row_id(), RowId::from_u64(1));
        assert!(change.old_row().is_none());
        assert_eq!(change.new_row(), Some(&row![1u64, 10u64]));
        assert!(change.old_version().is_none());
        assert_eq!(change.new_version(), Some(Version::ZERO));
    }

    #[test]
    fn update_change_has_old_and_new_state() {
        let change = Change::update(
            TableId::from_u64(0),
            RowId::from_u64(1),
            row![1u64, 10u64],
            Version::ZERO,
            row![1u64, 20u64],
            Version::from_u64(1),
        );
        assert_eq!(change.kind(), ChangeKind::Update);
        assert_eq!(change.old_row(), Some(&row![1u64, 10u64]));
        assert_eq!(change.new_row(), Some(&row![1u64, 20u64]));
        assert_eq!(change.old_version(), Some(Version::ZERO));
        assert_eq!(change.new_version(), Some(Version::from_u64(1)));
    }

    #[test]
    fn delete_change_has_only_old_state() {
        let change = Change::delete(
            TableId::from_u64(0),
            RowId::from_u64(1),
            row![1u64, 10u64],
            Version::from_u64(3),
        );
        assert_eq!(change.kind(), ChangeKind::Delete);
        assert_eq!(change.old_row(), Some(&row![1u64, 10u64]));
        assert!(change.new_row().is_none());
        assert_eq!(change.old_version(), Some(Version::from_u64(3)));
        assert!(change.new_version().is_none());
    }
}
