//! Indexes: derived lookup structures over table rows.
//!
//! An [`Index`] maps a tuple of column values (the key) to the `RowId`s of
//! rows matching that key. Indexes are **derived infrastructure** — they are
//! maintained transactionally by the table on every insert/update/delete and
//! never hold row data themselves. The authoritative state is the table's row
//! map (ADR-002 D5).
//!
//! - unique indexes map a key to at most one `RowId`
//! - non-unique indexes map a key to a set of `RowId`s in ascending `RowId`
//!   order (ADR-015 D5: a `BTreeSet` keeps removal O(log n) — a linear
//!   `retain` over a key's whole row list made zone-moving updates scale
//!   with rows-per-key)

use std::collections::{BTreeSet, HashMap};

use nexum_core::{Error, Result, RowId, Value};

use crate::Row;

/// A derived index over one or more columns of a table.
#[derive(Debug, Clone)]
pub(crate) enum Index {
    /// Enforces uniqueness: a key maps to at most one row.
    Unique {
        name: String,
        columns: Vec<usize>,
        entries: HashMap<Vec<Value>, RowId>,
    },
    /// A key maps to many rows, in ascending `RowId` order.
    NonUnique {
        name: String,
        columns: Vec<usize>,
        entries: HashMap<Vec<Value>, BTreeSet<RowId>>,
    },
}

impl Index {
    /// Creates a unique index over the given column positions.
    pub(crate) fn unique(name: String, columns: Vec<usize>) -> Self {
        Self::Unique {
            name,
            columns,
            entries: HashMap::new(),
        }
    }

    /// Creates a non-unique index over the given column positions.
    pub(crate) fn non_unique(name: String, columns: Vec<usize>) -> Self {
        Self::NonUnique {
            name,
            columns,
            entries: HashMap::new(),
        }
    }

    /// Returns the index name.
    pub(crate) fn name(&self) -> &str {
        match self {
            Self::Unique { name, .. } | Self::NonUnique { name, .. } => name,
        }
    }

    /// Returns the column positions this index covers, in key order.
    pub(crate) fn columns(&self) -> &[usize] {
        match self {
            Self::Unique { columns, .. } | Self::NonUnique { columns, .. } => columns,
        }
    }

    /// Extracts the index key from a row.
    ///
    /// # Panics
    ///
    /// Panics if the row has fewer values than the index's highest column
    /// position. This is an internal invariant: rows only reach index
    /// maintenance after schema validation (arity and types) in
    /// [`Table::insert`](crate::Table::insert) / [`Table::update`](crate::Table::update).
    pub(crate) fn key_of(&self, row: &Row) -> Vec<Value> {
        self.columns()
            .iter()
            .map(|&position| row.get(position).expect("row validated against schema").clone())
            .collect()
    }

    /// Validates that inserting `key` would not violate uniqueness.
    pub(crate) fn check_insert(&self, key: &[Value]) -> Result<()> {
        if let Self::Unique { name, entries, .. } = self
            && entries.contains_key(key)
        {
            return Err(Error::already_exists(format!(
                "unique index '{name}' already contains this key"
            )));
        }
        Ok(())
    }

    /// Validates that updating a row to `new_key` would not violate
    /// uniqueness, allowing the row to keep a key it already owns
    /// (`old_key == new_key`, or `new_key` currently maps to `row_id`).
    pub(crate) fn check_update(&self, old_key: &[Value], new_key: &[Value], row_id: RowId) -> Result<()> {
        if let Self::Unique { name, entries, .. } = self {
            if old_key == new_key {
                return Ok(());
            }
            if let Some(&existing) = entries.get(new_key)
                && existing != row_id
            {
                return Err(Error::already_exists(format!(
                    "unique index '{name}' already contains this key"
                )));
            }
        }
        Ok(())
    }

    /// Commits a key insertion. Callers must have run `check_insert` first.
    pub(crate) fn commit_insert(&mut self, key: Vec<Value>, row_id: RowId) {
        match self {
            Self::Unique { entries, .. } => {
                entries.insert(key, row_id);
            }
            Self::NonUnique { entries, .. } => {
                entries.entry(key).or_default().insert(row_id);
            }
        }
    }

    /// Commits a key removal for a specific row.
    pub(crate) fn commit_remove(&mut self, key: &[Value], row_id: RowId) {
        match self {
            Self::Unique { entries, .. } => {
                entries.remove(key);
            }
            Self::NonUnique { entries, .. } => {
                if let Some(ids) = entries.get_mut(key) {
                    ids.remove(&row_id);
                    if ids.is_empty() {
                        entries.remove(key);
                    }
                }
            }
        }
    }

    /// Looks up the row ids matching `key`. For unique indexes this returns
    /// zero or one id; for non-unique indexes, ids in ascending `RowId`
    /// order (deterministic).
    pub(crate) fn lookup(&self, key: &[Value]) -> Vec<RowId> {
        match self {
            Self::Unique { entries, .. } => entries.get(key).copied().into_iter().collect(),
            Self::NonUnique { entries, .. } => entries
                .get(key)
                .map(|ids| ids.iter().copied().collect())
                .unwrap_or_default(),
        }
    }

}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row;

    fn unique() -> Index {
        Index::unique("by_name".into(), vec![1])
    }

    fn non_unique() -> Index {
        Index::non_unique("by_zone".into(), vec![1])
    }

    #[test]
    fn unique_index_rejects_duplicates_and_removes() {
        let mut index = unique();
        let row_a = row![1u64, "alice".to_string()];
        let row_b = row![2u64, "alice".to_string()];

        let key_a = index.key_of(&row_a);
        let key_b = index.key_of(&row_b);

        index.check_insert(&key_a).unwrap();
        index.commit_insert(key_a.clone(), RowId::from_u64(0));

        // Duplicate key rejected.
        assert!(index.check_insert(&key_b).is_err());

        // Lookup finds the single row.
        assert_eq!(index.lookup(&key_a), vec![RowId::from_u64(0)]);

        // Remove frees the key.
        index.commit_remove(&key_a, RowId::from_u64(0));
        index.check_insert(&key_a).unwrap();
    }

    #[test]
    fn unique_update_allows_keeping_own_key() {
        let mut index = unique();
        let row = row![1u64, "alice".to_string()];
        let key = index.key_of(&row);
        index.commit_insert(key.clone(), RowId::from_u64(0));

        // Same key: fine.
        index.check_update(&key, &key, RowId::from_u64(0)).unwrap();

        // Different key owned by another row: rejected.
        let other = row![2u64, "bob".to_string()];
        let other_key = index.key_of(&other);
        index.commit_insert(other_key.clone(), RowId::from_u64(1));

        let err = index
            .check_update(&key, &other_key, RowId::from_u64(0))
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[test]
    fn non_unique_index_accumulates_and_removes() {
        let mut index = non_unique();
        let row_a = row![1u64, 10u64];
        let key = index.key_of(&row_a);

        index.commit_insert(key.clone(), RowId::from_u64(0));
        index.commit_insert(key.clone(), RowId::from_u64(1));

        assert_eq!(index.lookup(&key), vec![RowId::from_u64(0), RowId::from_u64(1)]);

        index.commit_remove(&key, RowId::from_u64(0));
        assert_eq!(index.lookup(&key), vec![RowId::from_u64(1)]);

        index.commit_remove(&key, RowId::from_u64(1));
        assert!(index.lookup(&key).is_empty());
    }
}
