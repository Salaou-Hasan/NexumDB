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

use std::collections::BTreeMap;

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

/// The ordered collection of a transaction's buffered writes.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct WriteSet {
    entries: BTreeMap<(TableId, RowId), WriteEntry>,
}

impl WriteSet {
    /// Creates an empty write set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Returns `true` if `row_id` in `table_id` already has a buffered write.
    pub fn contains(&self, table_id: TableId, row_id: RowId) -> bool {
        self.entries.contains_key(&(table_id, row_id))
    }

    /// Buffers an insert of `row`, keyed by a **fresh** (provisional) handle.
    ///
    /// Returns [`Error::invalid_transaction`] if the handle already has a
    /// buffered write (a caller must never re-insert the same handle).
    pub fn insert(&mut self, table_id: TableId, row_id: RowId, row: Row) -> Result<()> {
        let key = (table_id, row_id);
        if self.entries.contains_key(&key) {
            return Err(Error::invalid_transaction(format!(
                "duplicate insert for row handle {row_id} in table {table_id}"
            )));
        }
        self.entries.insert(key, WriteEntry::Insert(row));
        Ok(())
    }

    /// Buffers an update of `row_id` to `row`, coalescing per the rules
    /// above. The `is_provisional` flag tells the caller's validation whether
    /// a missing entry is a dangling insert handle (error) or a fresh write
    /// against a real row (allowed).
    pub fn update(
        &mut self,
        table_id: TableId,
        row_id: RowId,
        is_provisional: bool,
        row: Row,
    ) -> Result<()> {
        let key = (table_id, row_id);
        match self.entries.get(&key) {
            None => {
                if is_provisional {
                    return Err(Error::invalid_transaction(format!(
                        "cannot update row handle {row_id}: it does not refer to a pending insert in this transaction"
                    )));
                }
                self.entries.insert(key, WriteEntry::Update(row));
                Ok(())
            }
            Some(WriteEntry::Insert(_)) => {
                // insert → update: the insert carries the final row values.
                self.entries.insert(key, WriteEntry::Insert(row));
                Ok(())
            }
            Some(WriteEntry::Update(_)) => {
                // update → update: the latest row wins.
                self.entries.insert(key, WriteEntry::Update(row));
                Ok(())
            }
            Some(WriteEntry::Delete) => Err(Error::invalid_transaction(format!(
                "cannot update row {row_id}: it was already deleted earlier in this transaction"
            ))),
        }
    }

    /// Buffers a delete of `row_id`, coalescing per the rules above. The
    /// `is_provisional` flag distinguishes a dangling insert handle (error)
    /// from a real row (allowed, existence is checked at commit).
    pub fn delete(&mut self, table_id: TableId, row_id: RowId, is_provisional: bool) -> Result<()> {
        let key = (table_id, row_id);
        match self.entries.get(&key) {
            None => {
                if is_provisional {
                    return Err(Error::invalid_transaction(format!(
                        "cannot delete row handle {row_id}: it does not refer to a pending insert in this transaction"
                    )));
                }
                self.entries.insert(key, WriteEntry::Delete);
                Ok(())
            }
            Some(WriteEntry::Insert(_)) => {
                // insert → delete: the row is never created (net no-op).
                self.entries.remove(&key);
                Ok(())
            }
            Some(WriteEntry::Update(_)) => {
                // update → delete: only the delete matters.
                self.entries.insert(key, WriteEntry::Delete);
                Ok(())
            }
            Some(WriteEntry::Delete) => Err(Error::invalid_transaction(format!(
                "cannot delete row {row_id}: it was already deleted earlier in this transaction"
            ))),
        }
    }

    /// Overwrites the buffered entry at `(table_id, row_id)` unconditionally.
    ///
    /// Used only by `Transaction::absorb` (Phase 11 parallel merge): the
    /// caller guarantees the incoming entry is the *final coalesced state*
    /// of that key — a child transaction's write set starts as a copy of the
    /// parent's, so any key present in both carries the child's post-coalesce
    /// value, and a key absent from the child was never touched by it. This
    /// method deliberately skips the coalescing rules of
    /// [`update`](Self::update)/[`delete`](Self::delete).
    pub fn set(&mut self, table_id: TableId, row_id: RowId, entry: WriteEntry) {
        self.entries.insert((table_id, row_id), entry);
    }

    /// Returns the buffered entry for `row_id` in `table_id`, if any.
    pub fn get(&self, table_id: TableId, row_id: RowId) -> Option<&WriteEntry> {
        self.entries.get(&(table_id, row_id))
    }

    /// Returns the number of buffered writes.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if the write set is empty.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over `(table_id, row_id, entry)` in deterministic
    /// `(TableId, RowId)` order.
    pub fn entries(&self) -> impl Iterator<Item = (TableId, RowId, &WriteEntry)> + '_ {
        self.entries
            .iter()
            .map(|(&(table_id, row_id), entry)| (table_id, row_id, entry))
    }

    /// Returns the table ids touched by this write set, in ascending order.
    pub fn tables(&self) -> impl Iterator<Item = TableId> + '_ {
        let mut tables: Vec<TableId> = self
            .entries
            .keys()
            .map(|&(table_id, _)| table_id)
            .collect();
        tables.sort_unstable();
        tables.into_iter()
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
        assert_eq!(ids, vec![TableId::from_u64(0), TableId::from_u64(1), TableId::from_u64(2)]);

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
}
