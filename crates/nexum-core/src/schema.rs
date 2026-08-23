//! Table schemas, column definitions, and index definitions.
//!
//! These types are shared by the table engine, transaction engine, storage,
//! and subscriptions, so they live in the dependency-free core crate
//! (ADR-002 D2). A [`TableSchema`] describes:
//!
//! - an ordered list of typed columns ([`ColumnDef`])
//! - an optional primary key (one or more column names, enforced as a unique
//!   index)
//! - named secondary indexes ([`IndexDef`], unique or non-unique)
//!
//! [`TableSchema`] values are only constructible through
//! [`TableSchemaBuilder`], which validates names, column references, and
//! uniqueness before producing a schema.

use crate::ids::ColumnId;
use crate::value::{ColumnType, Value};
use crate::{Error, Result};

/// A single column definition: a stable positional id, a name, and a type.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ColumnDef {
    id: ColumnId,
    name: String,
    ty: ColumnType,
}

impl ColumnDef {
    /// Returns the stable id of this column, assigned positionally (0, 1, 2,
    /// ...) when the schema is built.
    pub fn id(&self) -> ColumnId {
        self.id
    }

    /// Returns the column name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column type.
    pub fn ty(&self) -> ColumnType {
        self.ty
    }
}

/// A named index over one or more columns, unique or non-unique.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IndexDef {
    name: String,
    columns: Vec<String>,
    unique: bool,
}

impl IndexDef {
    /// Builds an index definition directly.
    ///
    /// Used to add a derived index to an **existing** table (recovery
    /// compatibility, ADR-017): the schema builder validates names at build
    /// time, but a persisted table cannot be re-declared, so the definition
    /// is constructed here and validated by `Table::add_index` (name
    /// non-empty, not `"primary"`, not duplicate, columns resolvable).
    pub fn new(name: impl Into<String>, columns: &[&str], unique: bool) -> Self {
        Self {
            name: name.into(),
            columns: columns.iter().map(|name| (*name).to_owned()).collect(),
            unique,
        }
    }

    /// Returns the index name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the column names this index covers, in key order.
    pub fn columns(&self) -> &[String] {
        &self.columns
    }

    /// Returns `true` if this index enforces uniqueness.
    pub fn is_unique(&self) -> bool {
        self.unique
    }
}

/// A validated table schema.
///
/// Only constructible through [`TableSchema::builder`]. Invariants enforced
/// at build time: non-empty table/column/index names, at least one column,
/// unique column names, unique index names, primary key columns exist, index
/// columns exist.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TableSchema {
    name: String,
    columns: Vec<ColumnDef>,
    primary_key: Option<Vec<String>>,
    indexes: Vec<IndexDef>,
}

impl TableSchema {
    /// Creates a builder for a table named `name`.
    pub fn builder(name: impl Into<String>) -> TableSchemaBuilder {
        TableSchemaBuilder::new(name)
    }

    /// Returns the table name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the columns in schema order.
    pub fn columns(&self) -> &[ColumnDef] {
        &self.columns
    }

    /// Returns the column with the given name, if any.
    pub fn column(&self, name: &str) -> Option<&ColumnDef> {
        self.columns.iter().find(|column| column.name == name)
    }

    /// Returns the position (0-based index in schema order) of the named
    /// column, if any.
    pub fn column_index(&self, name: &str) -> Option<usize> {
        self.columns.iter().position(|column| column.name == name)
    }

    /// Returns the number of columns.
    pub fn column_count(&self) -> usize {
        self.columns.len()
    }

    /// Returns the primary key column names, if a primary key is declared.
    pub fn primary_key(&self) -> Option<&[String]> {
        self.primary_key.as_deref()
    }

    /// Returns `true` if this schema declares a primary key.
    pub fn has_primary_key(&self) -> bool {
        self.primary_key.is_some()
    }

    /// Returns the named secondary index definition, if any.
    pub fn index(&self, name: &str) -> Option<&IndexDef> {
        self.indexes.iter().find(|index| index.name == name)
    }

    /// Returns all secondary index definitions.
    pub fn indexes(&self) -> &[IndexDef] {
        &self.indexes
    }

    /// Returns `true` if a secondary index with the given name exists.
    pub fn has_index(&self, name: &str) -> bool {
        self.indexes.iter().any(|index| index.name == name)
    }

    /// Validates a row's values against this schema: arity must match the
    /// column count and every value's type must match its column's type.
    pub fn validate_row(&self, values: &[Value]) -> Result<()> {
        if values.len() != self.columns.len() {
            return Err(Error::invalid_argument(format!(
                "table '{}' expects {} values per row, got {}",
                self.name,
                self.columns.len(),
                values.len()
            )));
        }
        for (column, value) in self.columns.iter().zip(values) {
            if value.type_of() != column.ty {
                return Err(Error::invalid_argument(format!(
                    "column '{}' of table '{}' expects {}, got {}",
                    column.name,
                    self.name,
                    column.ty.name(),
                    value.type_of().name()
                )));
            }
        }
        Ok(())
    }

    /// Resolves column names to their positions in schema order.
    ///
    /// Returns [`Error::invalid_argument`] if any name is unknown. Used by
    /// the table engine to build indexes over resolved positions.
    pub fn resolve_columns(&self, names: &[String]) -> Result<Vec<usize>> {
        names
            .iter()
            .map(|name| {
                self.column_index(name).ok_or_else(|| {
                    Error::invalid_argument(format!(
                        "table '{}' has no column named '{name}'",
                        self.name
                    ))
                })
            })
            .collect()
    }
}

/// A builder for [`TableSchema`] with build-time validation.
#[derive(Debug, Clone)]
pub struct TableSchemaBuilder {
    name: String,
    columns: Vec<ColumnDef>,
    primary_key: Option<Vec<String>>,
    indexes: Vec<IndexDef>,
}

impl TableSchemaBuilder {
    /// Creates a builder for a table named `name`.
    pub fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            columns: Vec::new(),
            primary_key: None,
            indexes: Vec::new(),
        }
    }

    /// Adds a column with the given name and type.
    pub fn column(mut self, name: impl Into<String>, ty: ColumnType) -> Self {
        let id = ColumnId::from_u64(self.columns.len() as u64);
        self.columns.push(ColumnDef {
            id,
            name: name.into(),
            ty,
        });
        self
    }

    /// Declares the primary key as one or more column names.
    ///
    /// The primary key is enforced as a unique index; row identity remains
    /// the engine-assigned `RowId`.
    pub fn primary_key(mut self, columns: &[&str]) -> Self {
        self.primary_key = Some(columns.iter().map(|name| (*name).to_owned()).collect());
        self
    }

    /// Adds a non-unique secondary index over one or more columns.
    pub fn index(mut self, name: impl Into<String>, columns: &[&str]) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            columns: columns.iter().map(|name| (*name).to_owned()).collect(),
            unique: false,
        });
        self
    }

    /// Adds a unique secondary index over one or more columns.
    pub fn unique_index(mut self, name: impl Into<String>, columns: &[&str]) -> Self {
        self.indexes.push(IndexDef {
            name: name.into(),
            columns: columns.iter().map(|name| (*name).to_owned()).collect(),
            unique: true,
        });
        self
    }

    /// Validates and produces the [`TableSchema`].
    pub fn build(self) -> Result<TableSchema> {
        let Self {
            name,
            columns,
            primary_key,
            indexes,
        } = self;

        if name.trim().is_empty() {
            return Err(Error::invalid_argument("table name must not be empty"));
        }
        if columns.is_empty() {
            return Err(Error::invalid_argument(format!(
                "table '{name}' must have at least one column"
            )));
        }
        // Column names: non-empty and unique.
        let mut seen_columns = std::collections::HashSet::new();
        for column in &columns {
            if column.name.trim().is_empty() {
                return Err(Error::invalid_argument(format!(
                    "table '{name}' has a column with an empty name"
                )));
            }
            if !seen_columns.insert(column.name.clone()) {
                return Err(Error::invalid_argument(format!(
                    "table '{name}' declares duplicate column '{}'",
                    column.name
                )));
            }
        }
        // Index names: non-empty and unique.
        let mut seen_indexes = std::collections::HashSet::new();
        for index in &indexes {
            if index.name.trim().is_empty() {
                return Err(Error::invalid_argument(format!(
                    "table '{name}' has an index with an empty name"
                )));
            }
            if !seen_indexes.insert(index.name.clone()) {
                return Err(Error::invalid_argument(format!(
                    "table '{name}' declares duplicate index '{}'",
                    index.name
                )));
            }
            if index.columns.is_empty() {
                return Err(Error::invalid_argument(format!(
                    "index '{}' of table '{name}' must cover at least one column",
                    index.name
                )));
            }
        }
        // Column references must exist.
        if let Some(pk) = &primary_key {
            for column in pk {
                if !columns.iter().any(|c| &c.name == column) {
                    return Err(Error::invalid_argument(format!(
                        "primary key of table '{name}' references unknown column '{column}'"
                    )));
                }
            }
        }
        for index in &indexes {
            for column in &index.columns {
                if !columns.iter().any(|c| &c.name == column) {
                    return Err(Error::invalid_argument(format!(
                        "index '{}' of table '{name}' references unknown column '{column}'",
                        index.name
                    )));
                }
            }
        }

        Ok(TableSchema {
            name,
            columns,
            primary_key,
            indexes,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn player_schema() -> TableSchema {
        TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .column("level", ColumnType::U32)
            .primary_key(&["id"])
            .index("by_zone", &["zone_id"])
            .build()
            .unwrap()
    }

    #[test]
    fn builds_valid_schema() {
        let schema = player_schema();
        assert_eq!(schema.name(), "players");
        assert_eq!(schema.column_count(), 4);
        assert_eq!(schema.column("health").unwrap().ty(), ColumnType::I32);
        assert_eq!(schema.primary_key(), Some(&["id".to_string()][..]));
        assert!(schema.has_primary_key());
        assert!(schema.index("by_zone").is_some());
        assert!(!schema.index("by_zone").unwrap().is_unique());
        assert_eq!(schema.column_index("zone_id"), Some(1));
        assert_eq!(schema.column_index("missing"), None);
    }

    #[test]
    fn column_ids_are_positional() {
        let schema = player_schema();
        assert_eq!(schema.column("id").unwrap().id(), ColumnId::from_u64(0));
        assert_eq!(schema.column("level").unwrap().id(), ColumnId::from_u64(3));
    }

    #[test]
    fn rejects_empty_table_name() {
        let err = TableSchema::builder("  ")
            .column("id", ColumnType::U64)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_no_columns() {
        let err = TableSchema::builder("t").build().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_duplicate_column_names() {
        let err = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .column("a", ColumnType::I32)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_empty_column_name() {
        let err = TableSchema::builder("t")
            .column("", ColumnType::U64)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_duplicate_index_names() {
        let err = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .index("i", &["a"])
            .unique_index("i", &["a"])
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_empty_index_columns() {
        let err = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .index("i", &[])
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_unknown_primary_key_column() {
        let err = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .primary_key(&["nope"])
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_unknown_index_column() {
        let err = TableSchema::builder("t")
            .column("a", ColumnType::U64)
            .index("i", &["nope"])
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn validates_rows() {
        let schema = player_schema();
        schema
            .validate_row(&[
                Value::U64(1),
                Value::U64(10),
                Value::I32(100),
                Value::U32(5),
            ])
            .unwrap();

        // Wrong arity.
        let err = schema
            .validate_row(&[Value::U64(1), Value::U64(10)])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));

        // Wrong type.
        let err = schema
            .validate_row(&[
                Value::U64(1),
                Value::U64(10),
                Value::String("oops".into()),
                Value::U32(5),
            ])
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn resolve_columns_maps_names_to_positions() {
        let schema = player_schema();
        assert_eq!(
            schema
                .resolve_columns(&["zone_id".to_string(), "health".to_string()])
                .unwrap(),
            vec![1, 2]
        );
        assert!(schema.resolve_columns(&["nope".to_string()]).is_err());
    }
}
