//! Snapshots of the authoritative in-memory state: [`TableState`].
//!
//! A [`TableState`] is the complete serializable description of one table's
//! **authoritative** state — id, schema, rows with their exact versions, the
//! `next_row_id` allocation counter, and the table's mutation epoch. Nothing
//! derived (indexes, change buffers) is captured: indexes are rebuilt from a
//! full scan of the restored rows (ADR-003 D2), and change buffers are
//! drained fresh after recovery.
//!
//! [`StorageTable::table_state`] captures a table and
//! [`StorageTable::from_state`] reconstructs it exactly (both defined in the
//! `table` module, which owns `StorageTable`'s fields). The state encodes
//! to/from bytes via `nexum-core::binary` for snapshot files (ADR-005 D4).
//!
//! Capture and restore are exact: rows, row ids, versions, `next_row_id`,
//! and the epoch all round-trip bit-identically, so a snapshot + WAL replay
//! reproduces the authoritative state precisely.

use nexum_core::binary::{get_row, get_u64, get_version, put_row, put_u64, put_version};
use nexum_core::schema::TableSchema;
use nexum_core::{Error, Result, Row, RowId, TableId, Version};

/// The serializable authoritative state of a single table.
///
/// A plain data record — the durability attach point for snapshots. Rows are
/// stored as `(RowId, Row, Version)` in ascending `RowId` order (the
/// deterministic order `StorageTable` iterates).
///
/// Fields are `pub(crate)`: `nexum-table`'s `Table::from_state` path needs
/// them, but external code reads them through the accessors.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableState {
    pub(crate) id: TableId,
    pub(crate) schema: TableSchema,
    pub(crate) rows: Vec<(RowId, Row, Version)>,
    pub(crate) next_row_id: u64,
    pub(crate) epoch: Version,
}

impl TableState {
    /// Returns the table id.
    pub fn id(&self) -> TableId {
        self.id
    }

    /// Returns the table schema.
    pub fn schema(&self) -> &TableSchema {
        &self.schema
    }

    /// Returns the rows as `(row_id, row, version)` in ascending id order.
    pub fn rows(&self) -> &[(RowId, Row, Version)] {
        &self.rows
    }

    /// Returns the `next_row_id` allocation counter.
    pub fn next_row_id(&self) -> u64 {
        self.next_row_id
    }

    /// Returns the table's mutation epoch.
    pub fn epoch(&self) -> Version {
        self.epoch
    }

    /// Encodes this state into `out` (deterministic little-endian).
    pub fn encode(&self, out: &mut Vec<u8>) {
        put_u64(out, self.id.as_u64());
        nexum_core::binary::put_schema(out, &self.schema);
        put_u64(out, self.rows.len() as u64);
        for (row_id, row, version) in &self.rows {
            put_u64(out, row_id.as_u64());
            put_row(out, row);
            put_version(out, *version);
        }
        put_u64(out, self.next_row_id);
        put_version(out, self.epoch);
    }

    /// Decodes a state from `cursor`. Rows are validated to be ascending in
    /// `RowId` and to fall below `next_row_id`.
    pub fn decode(cursor: &mut &[u8]) -> Result<TableState> {
        let id = TableId::from_u64(get_u64(cursor)?);
        let schema = nexum_core::binary::get_schema(cursor)?;
        let row_count = get_u64(cursor)?;
        let mut rows = Vec::with_capacity(row_count as usize);
        let mut last_id: Option<u64> = None;
        for _ in 0..row_count {
            let row_id = RowId::from_u64(get_u64(cursor)?);
            let row = get_row(cursor)?;
            let version = get_version(cursor)?;
            schema.validate_row(row.values())?;
            if let Some(previous) = last_id
                && row_id.as_u64() <= previous
            {
                return Err(Error::internal(format!(
                    "snapshot: rows of table '{}' are not strictly ascending (row {} after {})",
                    schema.name(),
                    row_id.as_u64(),
                    previous
                )));
            }
            last_id = Some(row_id.as_u64());
            rows.push((row_id, row, version));
        }
        let next_row_id = get_u64(cursor)?;
        if let Some((max_id, _, _)) = rows.last()
            && max_id.as_u64() >= next_row_id
        {
            return Err(Error::internal(format!(
                "snapshot: next_row_id {next_row_id} must exceed the highest row id {} in table '{}'",
                max_id.as_u64(),
                schema.name()
            )));
        }
        let epoch = get_version(cursor)?;
        Ok(TableState {
            id,
            schema,
            rows,
            next_row_id,
            epoch,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;
    use nexum_core::ColumnType;
    use nexum_core::Value;

    use crate::StorageTable;

    fn player_state() -> TableState {
        let mut table = StorageTable::new(
            TableId::from_u64(0),
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone_id", ColumnType::U64)
                .column("health", ColumnType::I32)
                .build()
                .unwrap(),
        );
        let alice = table.insert(row![1u64, 10u64, 100i32]).unwrap();
        let bob = table.insert(row![2u64, 10u64, 90i32]).unwrap();
        table.update(alice, row![1u64, 30u64, 50i32]).unwrap();
        table.update(bob, row![2u64, 10u64, 5i32]).unwrap();
        table.delete(bob).unwrap();
        table.table_state()
    }

    #[test]
    fn state_roundtrips_exactly() {
        let state = player_state();
        assert_eq!(state.rows().len(), 1); // alice remains after bob's delete
        assert_eq!(state.rows()[0].0, RowId::from_u64(0));
        assert_eq!(state.rows()[0].2, Version::from_u64(1));
        assert_eq!(state.next_row_id(), 2);
        assert_eq!(state.epoch(), Version::from_u64(5)); // 2 inserts + 2 updates + 1 delete

        let mut bytes = Vec::new();
        state.encode(&mut bytes);
        let mut cursor: &[u8] = &bytes;
        let decoded = TableState::decode(&mut cursor).unwrap();
        assert!(cursor.is_empty());
        assert_eq!(decoded, state);
    }

    #[test]
    fn from_state_reconstructs_authoritative_state() {
        let state = player_state();
        let restored = StorageTable::from_state(state.clone()).unwrap();

        assert_eq!(restored.id(), TableId::from_u64(0));
        assert_eq!(restored.name(), "players");
        assert_eq!(restored.len(), 1);
        let alice = RowId::from_u64(0);
        assert_eq!(restored.version_of(alice), Some(Version::from_u64(1)));
        assert_eq!(
            restored.get_row(alice).unwrap().get_named(restored.schema(), "health"),
            Some(&Value::I32(50))
        );
        assert_eq!(restored.epoch(), state.epoch());
        // Change buffers start empty after a restore.
        assert!(restored.changes().is_empty());

        // Restored state behaves like the original: further mutations bump
        // versions and the epoch from the restored values.
        let mut restored = restored;
        restored.update(alice, row![1u64, 30u64, 25i32]).unwrap();
        assert_eq!(restored.version_of(alice), Some(Version::from_u64(2)));
        assert_eq!(restored.epoch(), state.epoch().next());
        // next_row_id preserved: the next insert gets id 2, exactly as before.
        let carol = restored.insert(row![3u64, 40u64, 1i32]).unwrap();
        assert_eq!(carol, RowId::from_u64(2));
    }

    #[test]
    fn decode_rejects_descending_rows() {
        // Rebuild a state whose rows are descending: decode must reject.
        let mut bad = Vec::new();
        put_u64(&mut bad, 0); // table id
        nexum_core::binary::put_schema(
            &mut bad,
            &TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .build()
                .unwrap(),
        );
        put_u64(&mut bad, 2);
        put_u64(&mut bad, 1);
        put_row(&mut bad, &row![2u64]);
        put_version(&mut bad, Version::ZERO);
        put_u64(&mut bad, 0);
        put_row(&mut bad, &row![1u64]);
        put_version(&mut bad, Version::ZERO);
        put_u64(&mut bad, 2);
        put_version(&mut bad, Version::ZERO);

        let mut cursor: &[u8] = &bad;
        let err = TableState::decode(&mut cursor).unwrap_err();
        assert!(matches!(err, Error::Internal(_)));
    }
}
