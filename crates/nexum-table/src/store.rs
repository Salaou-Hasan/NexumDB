//! [`TableStore`]: the registry of named tables.
//!
//! Provides the `create_table` entry point of the conceptual API and enforces
//! unique table names. It assigns `TableId`s and is the natural target of the
//! Phase 4 transaction engine (multi-table atomic operations). Each `Table`
//! embeds its own authoritative [`StorageTable`], so `TableStore` is also the
//! boundary at which committed changes across tables can be drained — the
//! attach point for Phase 5 WAL and Phase 8 subscriptions.

use std::collections::{BTreeMap, HashSet};

use nexum_core::schema::TableSchema;
use nexum_core::{Error, Result, TableId, TransactionId};
use nexum_storage::{Change, TableState};

use crate::Table;

/// A registry of named tables.
#[derive(Debug, Default)]
pub struct TableStore {
    tables: BTreeMap<String, Table>,
    next_table_id: u64,
    next_transaction_id: u64,
}

impl TableStore {
    /// Creates an empty store.
    pub fn new() -> Self {
        Self::default()
    }

    /// Allocates the store's next `TransactionId` (monotonic, never reused).
    ///
    /// The transaction engine (`nexum-tx`) uses this so every transaction
    /// begun against a store has a distinct, ordered id for logging and
    /// replay (ADR-004 D1).
    pub fn alloc_transaction_id(&mut self) -> TransactionId {
        let id = TransactionId::from_u64(self.next_transaction_id);
        self.next_transaction_id += 1;
        id
    }

    /// Creates a table from `schema`, assigning a fresh `TableId`.
    ///
    /// Returns [`Error::already_exists`] if a table with the same name
    /// already exists.
    pub fn create_table(&mut self, schema: TableSchema) -> Result<TableId> {
        let name = schema.name().to_string();
        if self.tables.contains_key(&name) {
            return Err(Error::already_exists(format!(
                "table '{name}' already exists"
            )));
        }
        let id = TableId::from_u64(self.next_table_id);
        self.next_table_id += 1;
        self.tables.insert(name, Table::new(id, schema)?);
        Ok(id)
    }

    /// Drops the named table and all of its rows.
    ///
    /// Returns [`Error::not_found`] if no such table exists.
    pub fn drop_table(&mut self, name: &str) -> Result<()> {
        if self.tables.remove(name).is_none() {
            return Err(Error::not_found(format!("table '{name}' does not exist")));
        }
        Ok(())
    }

    /// Adds a derived index to the named table, populating it from the
    /// table's current rows (see [`Table::add_index`]).
    ///
    /// Returns [`Error::not_found`] if no such table exists.
    pub fn add_index(&mut self, table: &str, def: nexum_core::IndexDef) -> Result<()> {
        let table = self
            .tables
            .get_mut(table)
            .ok_or_else(|| Error::not_found(format!("table '{table}' does not exist")))?;
        table.add_index(def)
    }

    /// Returns the named table, if it exists.
    pub fn table(&self, name: &str) -> Option<&Table> {
        self.tables.get(name)
    }

    /// Returns a mutable handle to the named table, if it exists.
    pub fn table_mut(&mut self, name: &str) -> Option<&mut Table> {
        self.tables.get_mut(name)
    }

    /// Returns the table with the given id, if it exists.
    pub fn table_by_id(&self, id: TableId) -> Option<&Table> {
        self.tables.values().find(|table| table.id() == id)
    }

    /// Returns a mutable handle to the table with the given id, if it exists.
    ///
    /// Used by the transaction engine's commit to apply buffered writes
    /// across tables (ADR-004 D2).
    pub fn table_mut_by_id(&mut self, id: TableId) -> Option<&mut Table> {
        self.tables.values_mut().find(|table| table.id() == id)
    }

    /// Returns `true` if a table with the given name exists.
    pub fn has_table(&self, name: &str) -> bool {
        self.tables.contains_key(name)
    }

    /// Returns the number of tables.
    pub fn len(&self) -> usize {
        self.tables.len()
    }

    /// Returns `true` if the store has no tables.
    pub fn is_empty(&self) -> bool {
        self.tables.is_empty()
    }

    /// Iterates over all tables as `(name, table)` pairs, sorted by name.
    pub fn tables(&self) -> impl Iterator<Item = (&str, &Table)> {
        self.tables
            .iter()
            .map(|(name, table)| (name.as_str(), table))
    }

    /// Drains the committed changes of every table, in deterministic order
    /// (tables sorted by name; within a table, commit order). The buffers are
    /// cleared exactly once.
    pub fn drain_changes(&mut self) -> Vec<Change> {
        let mut all = Vec::new();
        for table in self.tables.values_mut() {
            all.extend(table.drain_changes());
        }
        all
    }

    /// Returns the store's next `TableId` counter (for snapshots).
    pub fn next_table_id(&self) -> u64 {
        self.next_table_id
    }

    /// Returns the store's next `TransactionId` counter (for snapshots).
    pub fn next_transaction_id(&self) -> u64 {
        self.next_transaction_id
    }

    /// Raises the `next_transaction_id` counter to at least `min_next`.
    ///
    /// Used by recovery so replayed history's transaction ids are never
    /// reused (ADR-005 D6). Transaction ids must stay unique; the counter
    /// only moves forward.
    pub fn advance_transaction_id(&mut self, min_next: u64) {
        self.next_transaction_id = self.next_transaction_id.max(min_next);
    }

    /// Captures every table's authoritative state for a snapshot, ordered by
    /// `TableId` (deterministic; id order is creation order).
    pub fn snapshot_tables(&self) -> Vec<TableState> {
        let mut states: Vec<TableState> = self.tables.values().map(Table::table_state).collect();
        states.sort_by_key(TableState::id);
        states
    }

    /// Restores a store from captured [`TableState`]s and the stored
    /// counters — the reconstruction half of recovery (ADR-005 D6).
    ///
    /// Requires an **empty** store (a fresh `TableStore`), validates unique
    /// table ids/names and the `next_table_id` bound, rebuilds every derived
    /// index from the restored rows, and then replays nothing — the caller
    /// replays WAL records after the snapshot LSN.
    pub fn restore(
        &mut self,
        tables: Vec<TableState>,
        next_table_id: u64,
        next_transaction_id: u64,
    ) -> Result<()> {
        if !self.tables.is_empty() {
            return Err(Error::invalid_argument("restore requires an empty store"));
        }
        let mut seen_ids = HashSet::new();
        let mut seen_names = HashSet::new();
        for state in &tables {
            if !seen_ids.insert(state.id()) {
                return Err(Error::internal(format!(
                    "snapshot: duplicate table id {}",
                    state.id()
                )));
            }
            if !seen_names.insert(state.schema().name().to_string()) {
                return Err(Error::internal(format!(
                    "snapshot: duplicate table name '{}'",
                    state.schema().name()
                )));
            }
        }
        if let Some(max_id) = seen_ids.iter().max()
            && max_id.as_u64() >= next_table_id
        {
            return Err(Error::internal(format!(
                "snapshot: next_table_id {next_table_id} must exceed the highest table id {max_id}"
            )));
        }
        for state in tables {
            let table = Table::from_state(state)?;
            self.tables.insert(table.name().to_string(), table);
        }
        self.next_table_id = next_table_id;
        self.next_transaction_id = next_transaction_id;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::ColumnType;
    use nexum_core::Value;
    use nexum_core::row;

    fn schema(name: &str) -> TableSchema {
        TableSchema::builder(name)
            .column("id", ColumnType::U64)
            .primary_key(&["id"])
            .build()
            .unwrap()
    }

    #[test]
    fn allocates_monotonic_transaction_ids() {
        let mut store = TableStore::new();
        assert_eq!(store.alloc_transaction_id(), TransactionId::from_u64(0));
        assert_eq!(store.alloc_transaction_id(), TransactionId::from_u64(1));
        assert_eq!(store.alloc_transaction_id(), TransactionId::from_u64(2));
    }

    #[test]
    fn create_assigns_increasing_ids() {
        let mut store = TableStore::new();
        let a = store.create_table(schema("players")).unwrap();
        let b = store.create_table(schema("items")).unwrap();
        assert_eq!(a, TableId::from_u64(0));
        assert_eq!(b, TableId::from_u64(1));
        assert_eq!(store.len(), 2);
    }

    #[test]
    fn create_rejects_duplicate_names() {
        let mut store = TableStore::new();
        store.create_table(schema("players")).unwrap();
        let err = store.create_table(schema("players")).unwrap_err();
        assert!(matches!(err, Error::AlreadyExists(_)));
    }

    #[test]
    fn drop_removes_and_reports_missing() {
        let mut store = TableStore::new();
        store.create_table(schema("players")).unwrap();
        store.drop_table("players").unwrap();
        assert!(store.is_empty());

        let err = store.drop_table("players").unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn lookups_by_name_and_id() {
        let mut store = TableStore::new();
        let id = store.create_table(schema("players")).unwrap();

        assert!(store.table("players").is_some());
        assert!(store.table("nope").is_none());
        assert!(store.table_by_id(id).is_some());
        assert_eq!(store.table_by_id(id).unwrap().id(), id);
        assert!(store.has_table("players"));
    }

    #[test]
    fn table_mut_by_id_allows_writes() {
        let mut store = TableStore::new();
        let id = store.create_table(schema("players")).unwrap();
        let table = store.table_mut_by_id(id).unwrap();
        table.insert(row![1u64]).unwrap();
        assert_eq!(table.len(), 1);
        assert!(store.table_mut_by_id(TableId::from_u64(99)).is_none());
    }

    #[test]
    fn table_mut_allows_inserts() {
        let mut store = TableStore::new();
        store.create_table(schema("players")).unwrap();
        let table = store.table_mut("players").unwrap();
        table.insert(row![1u64]).unwrap();
        assert_eq!(table.len(), 1);
    }

    #[test]
    fn tables_iterates_sorted_by_name() {
        let mut store = TableStore::new();
        store.create_table(schema("zeta")).unwrap();
        store.create_table(schema("alpha")).unwrap();
        let names: Vec<&str> = store.tables().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["alpha", "zeta"]);
    }

    #[test]
    fn snapshot_and_restore_roundtrip_rebuilds_indexes() {
        let mut store = TableStore::new();
        store
            .create_table(
                TableSchema::builder("players")
                    .column("id", ColumnType::U64)
                    .column("zone_id", ColumnType::U64)
                    .column("health", ColumnType::I32)
                    .column("level", ColumnType::U32)
                    .primary_key(&["id"])
                    .index("by_zone", &["zone_id"])
                    .unique_index("by_level", &["level"])
                    .build()
                    .unwrap(),
            )
            .unwrap();
        store.create_table(schema("items")).unwrap();

        let alice = store
            .table_mut("players")
            .unwrap()
            .insert(row![1u64, 10u64, 100i32, 5u32])
            .unwrap();
        let bob = store
            .table_mut("players")
            .unwrap()
            .insert(row![2u64, 10u64, 90i32, 6u32])
            .unwrap();
        store
            .table_mut("players")
            .unwrap()
            .update(alice, row![1u64, 30u64, 50i32, 5u32])
            .unwrap();
        store
            .table_mut("items")
            .unwrap()
            .insert(row![1u64])
            .unwrap();

        // Capture authoritative state + counters.
        let states = store.snapshot_tables();
        assert_eq!(states.len(), 2);
        let next_table_id = store.next_table_id();
        let next_transaction_id = store.next_transaction_id();

        // Reconstruct into a fresh store.
        let mut restored = TableStore::new();
        restored
            .restore(states, next_table_id, next_transaction_id)
            .unwrap();

        assert_eq!(restored.len(), 2);
        assert_eq!(restored.next_table_id(), next_table_id);
        assert_eq!(restored.next_transaction_id(), next_transaction_id);

        // Rows, versions, and epochs are exact.
        let players = restored.table("players").unwrap();
        assert_eq!(
            players.version_of(alice),
            Some(nexum_core::Version::from_u64(1))
        );
        assert_eq!(players.version_of(bob), Some(nexum_core::Version::ZERO));
        assert_eq!(players.epoch(), nexum_core::Version::from_u64(3));

        // Indexes were rebuilt from the restored rows, not serialized.
        assert_eq!(
            players.lookup("by_zone", &[Value::U64(30)]).unwrap(),
            vec![alice]
        );
        assert_eq!(
            players.lookup("by_zone", &[Value::U64(10)]).unwrap(),
            vec![bob]
        );
        assert_eq!(
            players.lookup("by_level", &[Value::U32(6)]).unwrap(),
            vec![bob]
        );
        assert!(
            restored
                .table("items")
                .unwrap()
                .get(nexum_core::RowId::from_u64(0))
                .is_some()
        );

        // RowId allocation continues from the restored counter.
        let carol = restored
            .table_mut("players")
            .unwrap()
            .insert(row![3u64, 40u64, 1i32, 7u32])
            .unwrap();
        assert_eq!(carol, nexum_core::RowId::from_u64(2));
    }

    #[test]
    fn restore_requires_an_empty_store() {
        let mut store = TableStore::new();
        store.create_table(schema("players")).unwrap();
        let err = store.restore(Vec::new(), 0, 0).unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn drain_changes_aggregates_across_tables() {
        let mut store = TableStore::new();
        store.create_table(schema("alpha")).unwrap();
        store.create_table(schema("beta")).unwrap();

        store
            .table_mut("alpha")
            .unwrap()
            .insert(row![1u64])
            .unwrap();
        store.table_mut("beta").unwrap().insert(row![2u64]).unwrap();
        store
            .table_mut("alpha")
            .unwrap()
            .insert(row![3u64])
            .unwrap();

        let changes = store.drain_changes();
        assert_eq!(changes.len(), 3);
        // Deterministic order: alpha's two changes first (name order), then
        // beta's one. Row ids are per-table spaces (alpha: 0, 1; beta: 0).
        assert_eq!(changes[0].table_id(), TableId::from_u64(0)); // alpha
        assert_eq!(changes[0].row_id().as_u64(), 0);
        assert_eq!(changes[1].table_id(), TableId::from_u64(0)); // alpha
        assert_eq!(changes[1].row_id().as_u64(), 1);
        assert_eq!(changes[2].table_id(), TableId::from_u64(1)); // beta
        assert_eq!(changes[2].row_id().as_u64(), 0);

        // Buffers are consumed.
        let changes = store.drain_changes();
        assert!(changes.is_empty());
    }
}
