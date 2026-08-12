//! Rows: ordered typed value lists matching a table's schema column order.
//!
//! A [`Row`] is deliberately schema-free — it carries only the values. The
//! schema is owned by the table/storage layer; the table validates rows
//! against the schema at the boundary (arity and types) and resolves column
//! names through it. This keeps a single authoritative definition of the
//! schema and makes rows cheap to move through transactions and storage.
//!
//! `Row` lives in the dependency-free core crate (ADR-003 D3): the storage
//! engine stores rows, so storage must be able to name the type without
//! depending on the table crate. `nexum-table` re-exports it, so existing
//! `nexum_table::Row` paths keep working.

use crate::schema::TableSchema;
use crate::Value;

/// An ordered list of typed cell values, matching a schema's column order.
///
/// Construct with [`Row::new`], [`Row::from`]`(Vec<Value>)`, or the
/// [`row!`](crate::row) macro.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Row {
    values: Vec<Value>,
}

impl Row {
    /// Creates a row from ordered values. The values are not validated here;
    /// the owning table/storage validates arity and types on insert/update.
    pub fn new(values: Vec<Value>) -> Self {
        Self { values }
    }

    /// Returns the value at the given position, if any.
    pub fn get(&self, index: usize) -> Option<&Value> {
        self.values.get(index)
    }

    /// Returns the value of the named column by resolving the name through
    /// `schema`, if both the column and the position exist.
    pub fn get_named(&self, schema: &TableSchema, name: &str) -> Option<&Value> {
        schema
            .column_index(name)
            .and_then(|index| self.values.get(index))
    }

    /// Returns all values as a slice.
    pub fn values(&self) -> &[Value] {
        &self.values
    }

    /// Consumes the row, returning the underlying values.
    pub fn into_values(self) -> Vec<Value> {
        self.values
    }

    /// Returns the number of values.
    pub fn len(&self) -> usize {
        self.values.len()
    }

    /// Returns `true` if the row has no values.
    pub fn is_empty(&self) -> bool {
        self.values.is_empty()
    }

    /// Iterates over the values.
    pub fn iter(&self) -> std::slice::Iter<'_, Value> {
        self.values.iter()
    }
}

impl From<Vec<Value>> for Row {
    fn from(values: Vec<Value>) -> Self {
        Self::new(values)
    }
}

impl From<Row> for Vec<Value> {
    fn from(row: Row) -> Self {
        row.values
    }
}

/// Builds a [`Row`] from an ergonomic value list, converting each item with
/// `Into<Value>`:
///
/// ```
/// use nexum_core::{row, Value};
///
/// let r = row![1u64, 10u64, 100i32, 5u32];
/// assert_eq!(r.len(), 4);
/// assert_eq!(r.get(0), Some(&Value::U64(1)));
/// ```
///
/// Re-exported by `nexum-table` as `nexum_table::row!` for existing users.
#[macro_export]
macro_rules! row {
    ($($value:expr),* $(,)?) => {
        $crate::Row::new(::std::vec![$(::std::convert::Into::into($value)),*])
    };
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{ColumnType, TableSchema};

    fn schema() -> TableSchema {
        TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .build()
            .unwrap()
    }

    #[test]
    fn builds_from_values() {
        let row = Row::new(vec![Value::U64(1), Value::U64(10), Value::I32(100)]);
        assert_eq!(row.len(), 3);
        assert!(!row.is_empty());
        assert_eq!(row.get(0), Some(&Value::U64(1)));
        assert_eq!(row.get(9), None);
    }

    #[test]
    fn converts_from_vec_and_back() {
        let values = vec![Value::U64(1)];
        let row: Row = values.clone().into();
        assert_eq!(row.clone().into_values(), values);
        assert_eq!(row.iter().count(), 1);
    }

    #[test]
    fn gets_named_columns_through_schema() {
        let schema = schema();
        let row = row![1u64, 10u64, 100i32];
        assert_eq!(row.get_named(&schema, "health"), Some(&Value::I32(100)));
        assert_eq!(row.get_named(&schema, "missing"), None);
    }

    #[test]
    fn row_macro_converts_types() {
        let row = row![true, 10u64, 3.5f64];
        assert_eq!(row.values(), &[Value::Bool(true), Value::U64(10), Value::F64(3.5)]);
    }
}
