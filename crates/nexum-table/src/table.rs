//! The [`Table`]: authoritative storage plus derived indexes.
//!
//! `Table` is the public relational-style API. It owns:
//!
//! - one [`StorageTable`] (nexum-storage) — the **authoritative** row data,
//!   row versions, and change buffer
//! - derived indexes — the primary key index and named secondary indexes
//!
//! The table layer orchestrates every mutation: validate schema → check
//! unique constraints in the derived indexes → mutate storage → commit index
//! entries. Because every check precedes every mutation, a failed operation
//! leaves authoritative state, indexes, and the change buffer untouched
//! (ADR-002 D7, ADR-003 D2). Indexes never feed back into authoritative
//! state; they are rebuildable from a full scan of the storage table.

use std::collections::HashMap;

use nexum_core::Row;
use nexum_core::schema::TableSchema;
use nexum_core::{Error, Result, RowId, TableId, Value, Version};
use nexum_storage::{Change, StorageTable, TableState};

use crate::index::Index;

/// A single table: an authoritative storage table plus derived indexes.
#[derive(Debug)]
pub struct Table {
    storage: StorageTable,
    primary: Option<Index>,
    indexes: HashMap<String, Index>,
}

impl Table {
    /// Creates an empty table for `schema` with the given table id.
    ///
    /// Resolves the schema's primary key and index column names to positions.
    /// Returns [`Error::invalid_argument`] if the schema references unknown
    /// columns (the schema builder normally prevents this).
    pub fn new(id: TableId, schema: TableSchema) -> Result<Table> {
        let (primary, indexes) = Self::index_shells(&schema)?;
        Ok(Table {
            storage: StorageTable::new(id, schema),
            primary,
            indexes,
        })
    }

    /// Reconstructs a table from a snapshot [`TableState`]: restores the
    /// authoritative storage exactly (rows, versions, counters, epoch) and
    /// **rebuilds the derived indexes from a full scan** of the restored
    /// rows — indexes are never serialized (ADR-003 D2, ADR-005 D6).
    pub fn from_state(state: TableState) -> Result<Table> {
        let storage = StorageTable::from_state(state)?;
        let (primary, indexes) = Self::index_shells(storage.schema())?;
        let mut table = Table {
            storage,
            primary,
            indexes,
        };
        table.rebuild_indexes();
        Ok(table)
    }

    /// Captures the table's complete authoritative state for snapshotting.
    pub fn table_state(&self) -> TableState {
        self.storage.table_state()
    }

    /// Builds the (empty) derived index shells for a schema: the primary key
    /// index (if declared) and the named secondary indexes, with column
    /// positions resolved.
    fn index_shells(schema: &TableSchema) -> Result<(Option<Index>, HashMap<String, Index>)> {
        let primary = match schema.primary_key() {
            Some(names) => {
                let positions = schema.resolve_columns(names)?;
                Some(Index::unique("primary".into(), positions))
            }
            None => None,
        };

        let mut indexes = HashMap::new();
        for def in schema.indexes() {
            let positions = schema.resolve_columns(def.columns())?;
            let index = if def.is_unique() {
                Index::unique(def.name().to_string(), positions)
            } else {
                Index::non_unique(def.name().to_string(), positions)
            };
            indexes.insert(def.name().to_string(), index);
        }
        Ok((primary, indexes))
    }

    /// Rebuilds every derived index from the authoritative storage rows
    /// (used after a snapshot restore).
    fn rebuild_indexes(&mut self) {
        let rows: Vec<(RowId, Row)> = self
            .storage
            .scan()
            .map(|(row_id, stored)| (row_id, stored.row().clone()))
            .collect();
        for (row_id, row) in rows {
            if let Some(index) = &mut self.primary {
                let key = index.key_of(&row);
                index.commit_insert(key, row_id);
            }
            let secondary: Vec<(String, Vec<Value>)> = self
                .indexes
                .iter()
                .map(|(name, index)| (name.clone(), index.key_of(&row)))
                .collect();
            for (name, key) in secondary {
                let index = self
                    .indexes
                    .get_mut(&name)
                    .expect("key computed from own indexes");
                index.commit_insert(key, row_id);
            }
        }
    }

    /// Returns the table id.
    pub fn id(&self) -> TableId {
        self.storage.id()
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        self.storage.name()
    }

    /// Returns the table schema.
    pub fn schema(&self) -> &TableSchema {
        self.storage.schema()
    }

    /// Returns the number of rows.
    pub fn len(&self) -> usize {
        self.storage.len()
    }

    /// Returns `true` if the table has no rows.
    pub fn is_empty(&self) -> bool {
        self.storage.is_empty()
    }

    /// Returns `true` if a row with `row_id` exists.
    pub fn contains(&self, row_id: RowId) -> bool {
        self.storage.contains(row_id)
    }

    /// Inserts a row, validating it against the schema and all unique
    /// constraints, and returns the newly assigned `RowId`.
    ///
    /// Row ids are assigned monotonically and never reused; new rows start at
    /// [`Version::ZERO`].
    pub fn insert(&mut self, row: Row) -> Result<RowId> {
        self.schema().validate_row(row.values())?;

        // Compute and validate all keys before mutating anything.
        let primary_key = self.primary.as_ref().map(|index| index.key_of(&row));
        if let (Some(index), Some(key)) = (&self.primary, &primary_key) {
            index.check_insert(key)?;
        }
        let secondary_keys: Vec<(String, Vec<Value>)> = self
            .indexes
            .iter()
            .map(|(name, index)| (name.clone(), index.key_of(&row)))
            .collect();
        for (name, key) in &secondary_keys {
            if let Some(index) = self.indexes.get(name) {
                index.check_insert(key)?;
            }
        }

        // All checks passed: commit to authoritative storage, then indexes.
        let row_id = self.storage.insert(row)?;

        if let (Some(index), Some(key)) = (&mut self.primary, primary_key) {
            index.commit_insert(key, row_id);
        }
        for (name, key) in secondary_keys {
            let index = self
                .indexes
                .get_mut(&name)
                .expect("key computed from own indexes");
            index.commit_insert(key, row_id);
        }

        Ok(row_id)
    }

    /// Returns the row with the given engine-assigned id, if any.
    pub fn get(&self, row_id: RowId) -> Option<&Row> {
        self.storage.get_row(row_id)
    }

    /// Returns the current version of the row with `row_id`, if it exists.
    ///
    /// Deleted rows return `None`. This is the hook Phase 4 OCC will use to
    /// record and validate read versions.
    pub fn version_of(&self, row_id: RowId) -> Option<Version> {
        self.storage.version_of(row_id)
    }

    /// Returns the table's **mutation epoch**: advanced by exactly one on
    /// every committed row mutation (insert / delete / effective update).
    ///
    /// Transactional set observations (`tx.scan`, `tx.lookup_unique`) record
    /// it to detect phantom changes — any row mutation invalidates a set
    /// observation of this table (ADR-004 D13).
    pub fn epoch(&self) -> Version {
        self.storage.epoch()
    }

    /// Looks up a row by its declared primary key values.
    ///
    /// Returns [`Error::invalid_argument`] if the table has no primary key, or
    /// if `key` does not match the primary key's arity and column types.
    pub fn get_by_primary_key(&self, key: &[Value]) -> Result<Option<&Row>> {
        let index = self.primary.as_ref().ok_or_else(|| {
            Error::invalid_argument(format!(
                "table '{}' has no primary key",
                self.schema().name()
            ))
        })?;
        self.validate_key(index, key)?;
        let ids = index.lookup(key);
        Ok(ids.first().and_then(|id| self.storage.get_row(*id)))
    }

    /// Adds a derived index to an existing table, populating it from the
    /// current authoritative rows (one-time O(N) at call time).
    ///
    /// Used for recovery compatibility: a table persisted before an index
    /// was declared keeps its old schema, so the index must be built over
    /// existing rows rather than re-creating the table. Mirrors the
    /// schema-construction path ([`Table::new`]): column positions are
    /// resolved, a shell is built, and every row is committed into it.
    /// Indexes stay derived — the authoritative rows are unchanged. A unique
    /// index is validated against existing data and rejected (leaving the
    /// table unchanged) if the data would violate it.
    pub fn add_index(&mut self, def: nexum_core::IndexDef) -> Result<()> {
        if def.name().is_empty() {
            return Err(Error::invalid_argument("index name must not be empty"));
        }
        if def.name() == "primary" {
            return Err(Error::invalid_argument(
                "the primary key index cannot be added to an existing table",
            ));
        }
        if self.indexes.contains_key(def.name()) {
            return Err(Error::already_exists(format!(
                "index '{}' already exists in table '{}'",
                def.name(),
                self.schema().name()
            )));
        }
        let positions = self.schema().resolve_columns(def.columns())?;
        let mut index = if def.is_unique() {
            Index::unique(def.name().to_string(), positions)
        } else {
            Index::non_unique(def.name().to_string(), positions)
        };
        let rows: Vec<(RowId, Row)> = self
            .storage
            .scan()
            .map(|(row_id, stored)| (row_id, stored.row().clone()))
            .collect();
        for (row_id, row) in rows {
            let key = index.key_of(&row);
            index.check_insert(&key)?;
            index.commit_insert(key, row_id);
        }
        self.indexes.insert(def.name().to_string(), index);
        Ok(())
    }

    /// Looks up the ids of rows matching `key` in the named secondary index.
    ///
    /// Returns [`Error::not_found`] if no such index exists, or
    /// [`Error::invalid_argument`] if the key does not match the index's
    /// arity and column types.
    pub fn lookup(&self, index_name: &str, key: &[Value]) -> Result<Vec<RowId>> {
        let index = self.indexes.get(index_name).ok_or_else(|| {
            Error::not_found(format!(
                "index '{index_name}' does not exist in table '{}'",
                self.schema().name()
            ))
        })?;
        self.validate_key(index, key)?;
        Ok(index.lookup(key))
    }

    /// Replaces the row with `row_id` with a new full row.
    ///
    /// The row id never changes — even if primary key values change, identity
    /// stays with the engine-assigned `RowId`. The row's version advances by
    /// exactly one. Index entries are moved atomically; on any constraint
    /// violation the table is left unchanged.
    pub fn update(&mut self, row_id: RowId, row: Row) -> Result<()> {
        self.schema().validate_row(row.values())?;

        // Read the current authoritative row to compute old index keys.
        let old_row = self.storage.get(row_id).ok_or_else(|| {
            Error::not_found(format!(
                "row {row_id} does not exist in table '{}'",
                self.schema().name()
            ))
        })?;
        let old_row: Row = old_row.row().clone();

        // Compute keys against the old and new rows before mutating.
        let old_primary_key = self.primary.as_ref().map(|index| index.key_of(&old_row));
        let new_primary_key = self.primary.as_ref().map(|index| index.key_of(&row));
        if let (Some(index), Some(old), Some(new)) =
            (&self.primary, &old_primary_key, &new_primary_key)
        {
            index.check_update(old, new, row_id)?;
        }

        let secondary_keys: Vec<(String, Vec<Value>, Vec<Value>)> = self
            .indexes
            .iter()
            .map(|(name, index)| (name.clone(), index.key_of(&old_row), index.key_of(&row)))
            .collect();
        for (name, old, new) in &secondary_keys {
            let index = self
                .indexes
                .get(name)
                .expect("key computed from own indexes");
            index.check_update(old, new, row_id)?;
        }

        // All checks passed: commit to storage, then move index entries.
        self.storage.update(row_id, row)?;

        if let (Some(index), Some(old), Some(new)) =
            (&mut self.primary, old_primary_key, new_primary_key)
            && old != new
        {
            index.commit_remove(&old, row_id);
            index.commit_insert(new, row_id);
        }
        for (name, old, new) in secondary_keys {
            let index = self
                .indexes
                .get_mut(&name)
                .expect("key computed from own indexes");
            if old != new {
                index.commit_remove(&old, row_id);
                index.commit_insert(new, row_id);
            }
        }

        Ok(())
    }

    /// Deletes the row with `row_id` from storage and all indexes.
    pub fn delete(&mut self, row_id: RowId) -> Result<()> {
        let stored = self.storage.get(row_id).ok_or_else(|| {
            Error::not_found(format!(
                "row {row_id} does not exist in table '{}'",
                self.schema().name()
            ))
        })?;
        let row: Row = stored.row().clone();

        self.storage.delete(row_id)?;

        if let Some(index) = &mut self.primary {
            let key = index.key_of(&row);
            index.commit_remove(&key, row_id);
        }
        let secondary_keys: Vec<(String, Vec<Value>)> = self
            .indexes
            .iter()
            .map(|(name, index)| (name.clone(), index.key_of(&row)))
            .collect();
        for (name, key) in secondary_keys {
            let index = self
                .indexes
                .get_mut(&name)
                .expect("key computed from own indexes");
            index.commit_remove(&key, row_id);
        }

        Ok(())
    }

    /// Iterates over all rows in ascending `RowId` order — deterministic,
    /// which the simulation phase requires.
    pub fn scan(&self) -> impl Iterator<Item = (RowId, &Row)> {
        self.storage
            .scan()
            .map(|(row_id, stored)| (row_id, stored.row()))
    }

    /// Peeks at the buffered changes since the last drain, in commit order.
    pub fn changes(&self) -> &[Change] {
        self.storage.changes()
    }

    /// Returns the buffered changes since the last drain and clears the
    /// buffer. Changes are consumed exactly once.
    pub fn drain_changes(&mut self) -> Vec<Change> {
        self.storage.drain_changes()
    }

    /// Returns the names of the table's secondary indexes.
    pub fn index_names(&self) -> impl Iterator<Item = &str> {
        self.indexes.keys().map(String::as_str)
    }

    /// Returns the index keys of `row` as `(index_name, key)` pairs: the
    /// primary key under the reserved name `"primary"`, then **every**
    /// secondary index (unique and non-unique).
    ///
    /// Used by the transaction engine's read-your-writes overlays to decide
    /// whether a pending write owns a key in a named index. Secondary
    /// indexes are sorted by name so the order is deterministic even though
    /// the index map is a HashMap. Validates the row against the schema
    /// first, so a malformed row is rejected instead of panicking.
    pub fn index_keys(&self, row: &Row) -> Result<Vec<(String, Vec<Value>)>> {
        self.schema().validate_row(row.values())?;
        let mut keys = Vec::new();
        if let Some(index) = &self.primary {
            keys.push((index.name().to_string(), index.key_of(row)));
        }
        let mut secondary: Vec<(String, Vec<Value>)> = self
            .indexes
            .values()
            .map(|index| (index.name().to_string(), index.key_of(row)))
            .collect();
        secondary.sort_by(|a, b| a.0.cmp(&b.0));
        keys.extend(secondary);
        Ok(keys)
    }

    /// Returns the unique-index keys of `row` as `(index_name, key)` pairs:
    /// the primary key under the reserved name `"primary"`, then every
    /// unique secondary index.
    ///
    /// Used by the transaction engine to validate uniqueness without
    /// mutating state. Validates the row against the schema first (arity and
    /// types), so a malformed row is rejected instead of panicking.
    pub fn unique_keys(&self, row: &Row) -> Result<Vec<(String, Vec<Value>)>> {
        self.schema().validate_row(row.values())?;
        let mut keys = Vec::new();
        if let Some(index) = &self.primary {
            keys.push((index.name().to_string(), index.key_of(row)));
        }
        // Secondary indexes are sorted by name so the key order — and thus
        // the first error reported by validation — is deterministic even
        // though the index map is a HashMap.
        let mut secondary: Vec<(String, Vec<Value>)> = self
            .indexes
            .values()
            .filter(|index| matches!(index, Index::Unique { .. }))
            .map(|index| (index.name().to_string(), index.key_of(row)))
            .collect();
        secondary.sort_by(|a, b| a.0.cmp(&b.0));
        keys.extend(secondary);
        Ok(keys)
    }

    /// Looks up the row ids owning `key` in a **unique** index, named either
    /// `"primary"` (the primary key) or a unique secondary index. Returns
    /// zero or one id.
    ///
    /// Used by the transaction engine for constraint validation without
    /// mutating state. Returns [`Error::not_found`] for an unknown index
    /// name, and [`Error::invalid_argument`] if the named index exists but is
    /// not unique, or if the key does not match the index's arity and column
    /// types.
    pub fn lookup_unique(&self, index_name: &str, key: &[Value]) -> Result<Vec<RowId>> {
        if index_name == "primary" {
            let index = self.primary.as_ref().ok_or_else(|| {
                Error::invalid_argument(format!(
                    "table '{}' has no primary key",
                    self.schema().name()
                ))
            })?;
            self.validate_key(index, key)?;
            return Ok(index.lookup(key));
        }
        let index = self.indexes.get(index_name).ok_or_else(|| {
            Error::not_found(format!(
                "index '{index_name}' does not exist in table '{}'",
                self.schema().name()
            ))
        })?;
        match index {
            Index::Unique { .. } => {
                self.validate_key(index, key)?;
                Ok(index.lookup(key))
            }
            Index::NonUnique { .. } => Err(Error::invalid_argument(format!(
                "index '{index_name}' of table '{}' is not unique",
                self.schema().name()
            ))),
        }
    }

    /// Validates a lookup key against an index: the key must have the same
    /// arity as the index and every value's type must match the type of the
    /// column it targets.
    fn validate_key(&self, index: &Index, key: &[Value]) -> Result<()> {
        if key.len() != index.columns().len() {
            return Err(Error::invalid_argument(format!(
                "index '{}' of table '{}' expects {} key values, got {}",
                index.name(),
                self.schema().name(),
                index.columns().len(),
                key.len()
            )));
        }
        for (&position, value) in index.columns().iter().zip(key) {
            let column = &self.schema().columns()[position];
            if value.type_of() != column.ty() {
                return Err(Error::invalid_argument(format!(
                    "index '{}' of table '{}' expects key value of type {} for column '{}', got {}",
                    index.name(),
                    self.schema().name(),
                    column.ty().name(),
                    column.name(),
                    value.type_of().name()
                )));
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::ColumnType;
    use nexum_core::row;

    fn player_table() -> Table {
        let schema = TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .column("level", ColumnType::U32)
            .primary_key(&["id"])
            .index("by_zone", &["zone_id"])
            .unique_index("by_level", &["level"])
            .build()
            .unwrap();
        Table::new(TableId::from_u64(0), schema).unwrap()
    }

    #[test]
    fn insert_assigns_monotonic_row_ids() {
        let mut table = player_table();
        let a = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let b = table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();
        assert_eq!(a, RowId::from_u64(0));
        assert_eq!(b, RowId::from_u64(1));
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn get_returns_row_and_misses() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        assert_eq!(
            table
                .get(row_id)
                .unwrap()
                .get_named(table.schema(), "health"),
            Some(&Value::I32(100))
        );
        assert!(table.get(RowId::from_u64(99)).is_none());
    }

    #[test]
    fn version_of_tracks_updates() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        assert_eq!(table.version_of(row_id), Some(Version::ZERO));
        table
            .update(row_id, row![1u64, 10u64, 50i32, 5u32])
            .unwrap();
        assert_eq!(table.version_of(row_id), Some(Version::from_u64(1)));
        table.delete(row_id).unwrap();
        assert_eq!(table.version_of(row_id), None);
    }

    #[test]
    fn epoch_advances_on_mutations_only() {
        let mut table = player_table();
        assert_eq!(table.epoch(), Version::ZERO);
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        assert_eq!(table.epoch(), Version::from_u64(1));
        // No-op update (identical row): no epoch advance.
        table
            .update(row_id, row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        assert_eq!(table.epoch(), Version::from_u64(1));
        // Effective update and delete advance it.
        table
            .update(row_id, row![1u64, 10u64, 50i32, 5u32])
            .unwrap();
        assert_eq!(table.epoch(), Version::from_u64(2));
        table.delete(row_id).unwrap();
        assert_eq!(table.epoch(), Version::from_u64(3));
    }

    #[test]
    fn insert_rejects_bad_arity_and_types() {
        let mut table = player_table();
        let err = table.insert(row![1u64, 10u64]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));

        let err = table
            .insert(row![1u64, 10u64, "oops".to_string(), 5u32])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
        assert!(table.is_empty());
    }

    #[test]
    fn insert_rejects_duplicate_primary_key() {
        let mut table = player_table();
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let err = table.insert(row![1u64, 20u64, 50i32, 1u32]).unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn insert_rejects_duplicate_unique_index() {
        let mut table = player_table();
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        // level 5 is taken by the first row.
        let err = table.insert(row![2u64, 10u64, 90i32, 5u32]).unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn get_by_primary_key_finds_rows() {
        let mut table = player_table();
        table.insert(row![7u64, 10u64, 100i32, 5u32]).unwrap();
        let found = table
            .get_by_primary_key(&[Value::U64(7)])
            .unwrap()
            .expect("row exists");
        assert_eq!(
            found.get_named(table.schema(), "zone_id"),
            Some(&Value::U64(10))
        );
        assert!(
            table
                .get_by_primary_key(&[Value::U64(99)])
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn get_by_primary_key_errors_without_pk() {
        let schema = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .build()
            .unwrap();
        let table = Table::new(TableId::from_u64(1), schema).unwrap();
        let err = table.get_by_primary_key(&[Value::U64(1)]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn lookup_via_secondary_index() {
        let mut table = player_table();
        let alice = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let bob = table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();
        let carol = table.insert(row![3u64, 20u64, 80i32, 7u32]).unwrap();

        assert_eq!(
            table.lookup("by_zone", &[Value::U64(10)]).unwrap(),
            vec![alice, bob]
        );
        assert_eq!(
            table.lookup("by_zone", &[Value::U64(20)]).unwrap(),
            vec![carol]
        );
        assert!(
            table
                .lookup("by_zone", &[Value::U64(30)])
                .unwrap()
                .is_empty()
        );

        let err = table.lookup("missing", &[Value::U64(10)]).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));

        let err = table
            .lookup("by_zone", &[Value::U64(10), Value::U64(11)])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn update_replaces_row_and_moves_index_keys() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();

        table
            .update(row_id, row![1u64, 30u64, 50i32, 9u32])
            .unwrap();

        let updated = table.get(row_id).unwrap();
        assert_eq!(
            updated.get_named(table.schema(), "health"),
            Some(&Value::I32(50))
        );

        // Index keys moved: no longer in zone 10, now in zone 30.
        assert!(
            table
                .lookup("by_zone", &[Value::U64(10)])
                .unwrap()
                .is_empty()
        );
        assert_eq!(
            table.lookup("by_zone", &[Value::U64(30)]).unwrap(),
            vec![row_id]
        );
    }

    #[test]
    fn update_rejects_conflicting_unique_key() {
        let mut table = player_table();
        let first = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let second = table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();

        // Move second row's level onto first row's level 5.
        let err = table
            .update(second, row![2u64, 10u64, 90i32, 5u32])
            .unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));

        // Table unchanged.
        assert_eq!(
            table
                .get(second)
                .unwrap()
                .get_named(table.schema(), "level"),
            Some(&Value::U32(6))
        );

        // First row keeps its own key when updating unrelated columns.
        table.update(first, row![1u64, 40u64, 1i32, 5u32]).unwrap();
        assert!(table.get(first).is_some());
    }

    #[test]
    fn update_missing_row_errors() {
        let mut table = player_table();
        let err = table
            .update(RowId::from_u64(9), row![1u64, 10u64, 100i32, 5u32])
            .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn delete_removes_row_and_index_entries() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        table.delete(row_id).unwrap();

        assert!(table.get(row_id).is_none());
        assert!(
            table
                .lookup("by_zone", &[Value::U64(10)])
                .unwrap()
                .is_empty()
        );
        assert!(
            table
                .get_by_primary_key(&[Value::U64(1)])
                .unwrap()
                .is_none()
        );
        assert!(table.is_empty());

        let err = table.delete(row_id).unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn scan_is_deterministic_ascending() {
        let mut table = player_table();
        table.insert(row![3u64, 30u64, 100i32, 5u32]).unwrap();
        table.insert(row![1u64, 10u64, 100i32, 6u32]).unwrap();
        table.insert(row![2u64, 20u64, 100i32, 7u32]).unwrap();

        let ids: Vec<RowId> = table.scan().map(|(id, _)| id).collect();
        assert_eq!(
            ids,
            vec![RowId::from_u64(0), RowId::from_u64(1), RowId::from_u64(2)]
        );
    }

    #[test]
    fn unique_keys_covers_primary_and_unique_secondary() {
        let table = player_table();
        let row = row![1u64, 10u64, 100i32, 5u32];
        let keys = table.unique_keys(&row).unwrap();
        // Primary "primary" plus unique "by_level"; non-unique "by_zone" is
        // excluded.
        assert_eq!(
            keys,
            vec![
                ("primary".to_string(), vec![Value::U64(1)]),
                ("by_level".to_string(), vec![Value::U32(5)]),
            ]
        );

        // Malformed row rejected, not panicked.
        let err = table.unique_keys(&row![1u64]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn lookup_unique_finds_owners() {
        let mut table = player_table();
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();

        // Primary.
        assert_eq!(
            table.lookup_unique("primary", &[Value::U64(1)]).unwrap(),
            vec![RowId::from_u64(0)]
        );
        assert!(
            table
                .lookup_unique("primary", &[Value::U64(99)])
                .unwrap()
                .is_empty()
        );

        // Unique secondary.
        assert_eq!(
            table.lookup_unique("by_level", &[Value::U32(6)]).unwrap(),
            vec![RowId::from_u64(1)]
        );

        // Errors: unknown index, non-unique index, wrong key type.
        assert!(matches!(
            table
                .lookup_unique("missing", &[Value::U64(1)])
                .unwrap_err(),
            Error::NotFound(_)
        ));
        assert!(matches!(
            table
                .lookup_unique("by_zone", &[Value::U64(10)])
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));
        assert!(matches!(
            table
                .lookup_unique("by_level", &[Value::U64(1)])
                .unwrap_err(),
            Error::InvalidArgument(_)
        ));
    }

    #[test]
    fn index_names_lists_secondary_indexes() {
        let table = player_table();
        let names: Vec<&str> = table.index_names().collect();
        assert!(names.contains(&"by_zone"));
        assert!(names.contains(&"by_level"));
    }

    #[test]
    fn changes_flow_through_the_table() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        table
            .update(row_id, row![1u64, 10u64, 50i32, 5u32])
            .unwrap();
        table.delete(row_id).unwrap();

        let changes = table.drain_changes();
        assert_eq!(changes.len(), 3);
        assert!(changes[0].new_row().is_some());
        assert!(changes[1].old_row().is_some() && changes[1].new_row().is_some());
        assert!(changes[2].old_row().is_some());
        assert!(table.changes().is_empty());
    }

    fn composite_table() -> Table {
        let schema = TableSchema::builder("matches")
            .column("match_id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("score", ColumnType::I32)
            .primary_key(&["match_id", "zone_id"])
            .unique_index("by_score", &["score"])
            .build()
            .unwrap();
        Table::new(TableId::from_u64(3), schema).unwrap()
    }

    #[test]
    fn composite_primary_key_lookup_and_collision() {
        let mut table = composite_table();
        table.insert(row![1u64, 10u64, 100i32]).unwrap();
        table.insert(row![1u64, 20u64, 90i32]).unwrap();

        // Same match_id, different zone_id: both rows exist.
        let found = table
            .get_by_primary_key(&[Value::U64(1), Value::U64(20)])
            .unwrap()
            .expect("composite key row exists");
        assert_eq!(
            found.get_named(table.schema(), "score"),
            Some(&Value::I32(90))
        );

        // Duplicate composite key rejected.
        let err = table.insert(row![1u64, 10u64, 5i32]).unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));

        // Wrong arity rejected.
        let err = table.get_by_primary_key(&[Value::U64(1)]).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn composite_unique_index_collision() {
        let mut table = composite_table();
        table.insert(row![1u64, 10u64, 100i32]).unwrap();
        let err = table.insert(row![2u64, 20u64, 100i32]).unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[test]
    fn updating_primary_key_values_moves_pk_index() {
        let mut table = player_table();
        let row_id = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();

        // Change the primary key value: identity (RowId) is unchanged, but the
        // PK index entry must move.
        table
            .update(row_id, row![7u64, 10u64, 100i32, 5u32])
            .unwrap();

        assert!(
            table
                .get_by_primary_key(&[Value::U64(1)])
                .unwrap()
                .is_none()
        );
        let found = table
            .get_by_primary_key(&[Value::U64(7)])
            .unwrap()
            .expect("row findable by new pk");
        assert_eq!(
            found.get_named(table.schema(), "health"),
            Some(&Value::I32(100))
        );
    }

    #[test]
    fn lookup_rejects_wrong_typed_key() {
        let mut table = player_table();
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let err = table
            .lookup("by_zone", &[Value::String("nope".into())])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn table_without_primary_key_still_works() {
        let schema = TableSchema::builder("items")
            .column("name", ColumnType::String)
            .column("qty", ColumnType::I32)
            .build()
            .unwrap();
        let mut table = Table::new(TableId::from_u64(2), schema).unwrap();
        let id = table.insert(row!["sword".to_string(), 3i32]).unwrap();
        assert!(table.get(id).is_some());
        assert_eq!(table.len(), 1);
    }

    /// The invariant test: after any sequence of mutations, the derived
    /// indexes must exactly match the authoritative storage state. We verify
    /// by rebuilding index expectations from a full scan.
    #[test]
    fn indexes_never_diverge_from_storage() {
        let mut table = player_table();
        let alice = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let bob = table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();
        table.insert(row![3u64, 20u64, 80i32, 7u32]).unwrap();
        table.delete(bob).unwrap();
        table.update(alice, row![1u64, 30u64, 40i32, 5u32]).unwrap();

        // Authoritative truth: scan storage.
        let zone10: Vec<RowId> = table
            .scan()
            .filter(|(_, row)| row.get_named(table.schema(), "zone_id") == Some(&Value::U64(10)))
            .map(|(id, _)| id)
            .collect();
        let zone30: Vec<RowId> = table
            .scan()
            .filter(|(_, row)| row.get_named(table.schema(), "zone_id") == Some(&Value::U64(30)))
            .map(|(id, _)| id)
            .collect();

        // Derived indexes must agree.
        assert_eq!(table.lookup("by_zone", &[Value::U64(10)]).unwrap(), zone10);
        assert_eq!(table.lookup("by_zone", &[Value::U64(30)]).unwrap(), zone30);
        assert!(
            table
                .get_by_primary_key(&[Value::U64(2)])
                .unwrap()
                .is_none()
        );
        assert!(
            table
                .get_by_primary_key(&[Value::U64(1)])
                .unwrap()
                .is_some()
        );

        // Row count matches scan count.
        assert_eq!(table.len(), table.scan().count());
    }

    /// A table with only a primary key — no secondary indexes yet.
    fn bare_table() -> Table {
        let schema = TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .column("level", ColumnType::U32)
            .primary_key(&["id"])
            .build()
            .unwrap();
        Table::new(TableId::from_u64(0), schema).unwrap()
    }

    #[test]
    fn add_index_builds_over_existing_rows_and_stays_maintained() {
        let mut table = bare_table();
        let a = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        let b = table.insert(row![2u64, 20u64, 90i32, 6u32]).unwrap();
        let c = table.insert(row![3u64, 10u64, 80i32, 7u32]).unwrap();

        // No index yet: lookup fails with NotFound.
        assert!(table.lookup("by_zone", &[Value::U64(10)]).is_err());

        // Add the derived index over the existing rows.
        let def = nexum_core::IndexDef::new("by_zone", &["zone_id"], false);
        table.add_index(def).unwrap();
        assert_eq!(
            table.lookup("by_zone", &[Value::U64(10)]).unwrap(),
            vec![a, c],
            "populated from existing rows, ascending"
        );
        assert_eq!(table.lookup("by_zone", &[Value::U64(20)]).unwrap(), vec![b]);

        // The index stays transactionally maintained by later writes.
        table.update(c, row![3u64, 30u64, 80i32, 7u32]).unwrap();
        assert_eq!(table.lookup("by_zone", &[Value::U64(10)]).unwrap(), vec![a]);
        assert_eq!(table.lookup("by_zone", &[Value::U64(30)]).unwrap(), vec![c]);
        table.delete(a).unwrap();
        assert!(
            table
                .lookup("by_zone", &[Value::U64(10)])
                .unwrap()
                .is_empty()
        );
        assert_eq!(table.lookup("by_zone", &[Value::U64(20)]).unwrap(), vec![b]);
        assert_eq!(table.len(), 2);
    }

    #[test]
    fn add_index_rejects_primary_duplicate_and_empty_names() {
        let mut table = bare_table();
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();

        assert!(
            table
                .add_index(nexum_core::IndexDef::new("primary", &["id"], true))
                .is_err()
        );
        assert!(
            table
                .add_index(nexum_core::IndexDef::new("", &["zone_id"], false))
                .is_err()
        );
        table
            .add_index(nexum_core::IndexDef::new("by_zone", &["zone_id"], false))
            .unwrap();
        // Duplicate name: rejected, and the existing index is untouched.
        assert!(
            table
                .add_index(nexum_core::IndexDef::new("by_zone", &["level"], false))
                .is_err()
        );
        assert_eq!(
            table.lookup("by_zone", &[Value::U64(10)]).unwrap(),
            vec![RowId::from_u64(0)]
        );
    }

    #[test]
    fn add_index_unique_violation_is_rejected_without_mutation() {
        let mut table = bare_table();
        // Two rows share zone 10 — a *unique* index over zone_id is invalid.
        table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
        table.insert(row![2u64, 10u64, 90i32, 6u32]).unwrap();

        assert!(
            table
                .add_index(nexum_core::IndexDef::new("by_zone", &["zone_id"], true))
                .is_err()
        );
        // The failed add left no index behind.
        assert!(table.lookup("by_zone", &[Value::U64(10)]).is_err());
        assert_eq!(table.len(), 2);

        // The same columns work as a non-unique index.
        table
            .add_index(nexum_core::IndexDef::new("by_zone", &["zone_id"], false))
            .unwrap();
        assert_eq!(table.lookup("by_zone", &[Value::U64(10)]).unwrap().len(), 2);
    }
}
