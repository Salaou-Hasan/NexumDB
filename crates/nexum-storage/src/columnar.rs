//! [`ColumnarStore`]: a column-oriented mirror of [`StorageTable`] for
//! scan-optimized access (Phase 28 — SoA hot-path storage).
//!
//! For hot tables (like "players"), each column is stored as a contiguous
//! `Vec<Value>` instead of the BTreeMap's scattered per-row `Vec<Value>`
//! allocations. This eliminates pointer chasing during full scans.
//!
//! The store is **derived** infrastructure (ADR-003 D4): it mirrors every
//! `StorageTable` mutation and can be rebuilt from the authoritative BTreeMap.
//! It lives alongside `StorageTable` in the `Table` layer, toggled per-table
//! via `Table::enable_columnar()`.

use std::collections::HashMap;

use nexum_core::{Row, RowId, Value, Version};

use crate::table::StoredRow;

/// A borrowed row reference backed by the columnar store.
/// Provides indexed access without allocating a `Vec<Value>`.
pub struct RowRef<'a> {
    columns: &'a [Vec<Value>],
    slot: usize,
    num_columns: usize,
}

impl<'a> RowRef<'a> {
    /// Returns the value at the given column index.
    pub fn get(&self, index: usize) -> Option<&Value> {
        if index < self.num_columns {
            Some(&self.columns[index][self.slot])
        } else {
            None
        }
    }

    /// Returns the number of columns.
    pub fn len(&self) -> usize {
        self.num_columns
    }

    /// Returns true if the row has no columns.
    pub fn is_empty(&self) -> bool {
        self.num_columns == 0
    }

    /// Converts into an owned `Row` (single allocation per column).
    pub fn into_owned(self) -> Row {
        let values = (0..self.num_columns)
            .map(|c| self.columns[c][self.slot].clone())
            .collect();
        Row::new(values)
    }
}

/// An iterator that yields `(RowId, RowRef)` without allocation.
pub struct ColumnarScan<'a> {
    columns: &'a [Vec<Value>],
    slot_to_row_id: &'a [RowId],
    num_columns: usize,
    slot: usize,
    len: usize,
}

impl<'a> Iterator for ColumnarScan<'a> {
    type Item = (RowId, RowRef<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.slot >= self.len {
            return None;
        }
        let slot = self.slot;
        self.slot += 1;
        let row_id = self.slot_to_row_id[slot];
        Some((
            row_id,
            RowRef {
                columns: self.columns,
                slot,
                num_columns: self.num_columns,
            },
        ))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        let remaining = self.len - self.slot;
        (remaining, Some(remaining))
    }
}

impl<'a> ExactSizeIterator for ColumnarScan<'a> {}

/// A column-oriented mirror of `StorageTable` for scan-optimized access.
///
/// Each column is stored as a contiguous `Vec<Value>`. Rows are identified
/// by a dense slot index; a `HashMap` maps `RowId` to slot. Deletions use
/// swap-remove to keep the dense arrays contiguous.
pub struct ColumnarStore {
    /// RowId -> dense slot index.
    row_id_to_slot: HashMap<RowId, u32>,
    /// Slot -> RowId (inverse mapping for scan output).
    slot_to_row_id: Vec<RowId>,
    /// One `Vec<Value>` per column, in schema order. Each Vec has the
    /// same length (= number of live rows).
    columns: Vec<Vec<Value>>,
    /// Parallel to `slot_to_row_id`: one Version per slot.
    versions: Vec<Version>,
    /// Number of columns (cached from schema).
    num_columns: usize,
}

impl std::fmt::Debug for ColumnarStore {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ColumnarStore")
            .field("rows", &self.slot_to_row_id.len())
            .field("columns", &self.num_columns)
            .finish()
    }
}

impl ColumnarStore {
    /// Creates an empty store with the given column count.
    pub fn new(num_columns: usize) -> Self {
        Self {
            row_id_to_slot: HashMap::new(),
            slot_to_row_id: Vec::new(),
            columns: vec![Vec::new(); num_columns],
            versions: Vec::new(),
            num_columns,
        }
    }

    /// Builds from an existing BTreeMap row collection.
    pub fn from_rows(
        num_columns: usize,
        rows: &std::collections::BTreeMap<RowId, StoredRow>,
    ) -> Self {
        let len = rows.len();
        let mut store = Self {
            row_id_to_slot: HashMap::with_capacity(len),
            slot_to_row_id: Vec::with_capacity(len),
            columns: (0..num_columns).map(|_| Vec::with_capacity(len)).collect(),
            versions: Vec::with_capacity(len),
            num_columns,
        };
        for (&row_id, stored) in rows {
            store.insert(row_id, stored.row(), stored.version());
        }
        store
    }

    /// Iterates all live rows in ascending RowId order.
    pub fn scan(&self) -> ColumnarScan<'_> {
        ColumnarScan {
            columns: &self.columns,
            slot_to_row_id: &self.slot_to_row_id,
            num_columns: self.num_columns,
            slot: 0,
            len: self.slot_to_row_id.len(),
        }
    }

    /// Returns a borrowed row reference for the given RowId.
    pub fn get(&self, row_id: RowId) -> Option<RowRef<'_>> {
        let slot = *self.row_id_to_slot.get(&row_id)? as usize;
        Some(RowRef {
            columns: &self.columns,
            slot,
            num_columns: self.num_columns,
        })
    }

    /// Returns the version for the given RowId.
    pub fn version_of(&self, row_id: RowId) -> Option<Version> {
        let slot = *self.row_id_to_slot.get(&row_id)? as usize;
        Some(self.versions[slot])
    }

    /// Inserts a new row at the next dense slot.
    pub fn insert(&mut self, row_id: RowId, row: &Row, version: Version) {
        let slot = self.slot_to_row_id.len() as u32;
        self.row_id_to_slot.insert(row_id, slot);
        self.slot_to_row_id.push(row_id);
        for (c, val) in row.values().iter().enumerate().take(self.num_columns) {
            self.columns[c].push(val.clone());
        }
        // Pad any extra columns with default values.
        for c in row.values().len()..self.num_columns {
            self.columns[c].push(Value::I64(0));
        }
        self.versions.push(version);
    }

    /// Replaces the row at the given RowId's slot in-place.
    pub fn update(&mut self, row_id: RowId, row: &Row, version: Version) {
        let Some(&slot) = self.row_id_to_slot.get(&row_id) else {
            return;
        };
        let slot = slot as usize;
        for (c, val) in row.values().iter().enumerate().take(self.num_columns) {
            self.columns[c][slot] = val.clone();
        }
        self.versions[slot] = version;
    }

    /// Removes the row, swap-filling the gap to keep arrays contiguous.
    pub fn delete(&mut self, row_id: RowId) {
        let Some(slot_u32) = self.row_id_to_slot.remove(&row_id) else {
            return;
        };
        let slot = slot_u32 as usize;
        let last = self.slot_to_row_id.len() - 1;
        if slot != last {
            // Swap the last element into the gap.
            let last_row_id = self.slot_to_row_id[last];
            self.slot_to_row_id.swap(slot, last);
            for c in 0..self.num_columns {
                self.columns[c].swap(slot, last);
            }
            self.versions.swap(slot, last);
            self.row_id_to_slot.insert(last_row_id, slot as u32);
        }
        self.slot_to_row_id.pop();
        for c in 0..self.num_columns {
            self.columns[c].pop();
        }
        self.versions.pop();
    }

    /// Returns the number of live rows.
    pub fn len(&self) -> usize {
        self.slot_to_row_id.len()
    }

    /// Returns true if empty.
    pub fn is_empty(&self) -> bool {
        self.slot_to_row_id.is_empty()
    }

    /// Returns the number of columns.
    pub fn num_columns(&self) -> usize {
        self.num_columns
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;

    #[test]
    fn insert_and_scan() {
        let mut store = ColumnarStore::new(3);
        store.insert(
            RowId::from_u64(0),
            &row![1u64, 10i64, 100i64],
            Version::ZERO,
        );
        store.insert(RowId::from_u64(1), &row![2u64, 20i64, 90i64], Version::ZERO);

        let rows: Vec<_> = store.scan().map(|(id, r)| (id, r.into_owned())).collect();
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].1.get(0), Some(&Value::U64(1)));
        assert_eq!(rows[1].1.get(2), Some(&Value::I64(90)));
    }

    #[test]
    fn update_in_place() {
        let mut store = ColumnarStore::new(3);
        store.insert(
            RowId::from_u64(0),
            &row![1u64, 10i64, 100i64],
            Version::ZERO,
        );
        store.update(
            RowId::from_u64(0),
            &row![1u64, 15i64, 80i64],
            Version::from_u64(1),
        );

        let r = store.get(RowId::from_u64(0)).unwrap();
        assert_eq!(r.get(1), Some(&Value::I64(15)));
        assert_eq!(
            store.version_of(RowId::from_u64(0)),
            Some(Version::from_u64(1))
        );
    }

    #[test]
    fn delete_swap_remove() {
        let mut store = ColumnarStore::new(3);
        store.insert(
            RowId::from_u64(0),
            &row![1u64, 10i64, 100i64],
            Version::ZERO,
        );
        store.insert(RowId::from_u64(1), &row![2u64, 20i64, 90i64], Version::ZERO);
        store.insert(RowId::from_u64(2), &row![3u64, 30i64, 80i64], Version::ZERO);

        store.delete(RowId::from_u64(1)); // middle element

        assert_eq!(store.len(), 2);
        assert!(store.get(RowId::from_u64(1)).is_none());
        // Row 2 should have been swapped into slot 1
        let r = store.get(RowId::from_u64(2)).unwrap();
        assert_eq!(r.get(0), Some(&Value::U64(3)));
    }

    #[test]
    fn from_rows_roundtrip() {
        let mut btree = std::collections::BTreeMap::new();
        btree.insert(
            RowId::from_u64(0),
            StoredRow::new(row![1u64, 10i64, 100i64], Version::ZERO),
        );
        btree.insert(
            RowId::from_u64(1),
            StoredRow::new(row![2u64, 20i64, 90i64], Version::ZERO),
        );

        let store = ColumnarStore::from_rows(3, &btree);
        assert_eq!(store.len(), 2);

        let rows: Vec<_> = store.scan().map(|(id, r)| (id, r.into_owned())).collect();
        assert_eq!(rows[0].0, RowId::from_u64(0));
        assert_eq!(rows[1].0, RowId::from_u64(1));
    }
}
