//! Query compilation and matching (ADR-008 D2, D6).
//!
//! A logical [`Query`] is compiled once against a table schema into a
//! [`CompiledQuery`]: column names resolve to positions, predicate literals
//! are type-checked, and the ordering/projection columns are pinned. The
//! compiled form evaluates rows purely from `Row` payloads (the `Change`
//! records already carry complete old/new rows), so change matching never
//! touches storage.
//!
//! [`value_cmp`] is the **total deterministic order** over [`Value`] shared
//! by predicates and the sort key (ADR-008 D6): variants compare by
//! declaration order, and floats use `total_cmp`, so `NaN` is a comparable,
//! stable value.

use std::cmp::Ordering;

use nexum_core::Row;
use nexum_core::schema::TableSchema;
use nexum_core::{Error, Result, RowId, TableId, Value};
use nexum_table::{Table, TableStore};

use crate::config::SubscriptionConfig;
use crate::query::{ComparisonOp, OrderDirection, Query};

/// Total, deterministic comparison over [`Value`].
///
/// Variants compare by declaration order (a variant earlier in the enum is
/// "smaller" than a later one); values of the same variant compare by their
/// inner ordering — floats via `total_cmp`, so every pair of values
/// compares consistently across runs and `NaN` is stable. This keeps the
/// sort key's `Ord` lawful while predicate values are type-checked against
/// their column, so cross-type comparisons cannot occur in practice.
pub fn value_cmp(a: &Value, b: &Value) -> Ordering {
    use Value::*;
    fn ordinal(value: &Value) -> u8 {
        match value {
            Bool(_) => 0,
            I8(_) => 1,
            I16(_) => 2,
            I32(_) => 3,
            I64(_) => 4,
            U8(_) => 5,
            U16(_) => 6,
            U32(_) => 7,
            U64(_) => 8,
            F32(_) => 9,
            F64(_) => 10,
            String(_) => 11,
            Bytes(_) => 12,
        }
    }
    ordinal(a).cmp(&ordinal(b)).then_with(|| match (a, b) {
        (Bool(x), Bool(y)) => x.cmp(y),
        (I8(x), I8(y)) => x.cmp(y),
        (I16(x), I16(y)) => x.cmp(y),
        (I32(x), I32(y)) => x.cmp(y),
        (I64(x), I64(y)) => x.cmp(y),
        (U8(x), U8(y)) => x.cmp(y),
        (U16(x), U16(y)) => x.cmp(y),
        (U32(x), U32(y)) => x.cmp(y),
        (U64(x), U64(y)) => x.cmp(y),
        (F32(x), F32(y)) => x.total_cmp(y),
        (F64(x), F64(y)) => x.total_cmp(y),
        (String(x), String(y)) => x.cmp(y),
        (Bytes(x), Bytes(y)) => x.cmp(y),
        // Unreachable for equal ordinals; keep the match total.
        _ => Ordering::Equal,
    })
}

/// One compiled predicate: a resolved column position plus a literal.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledPredicate {
    position: usize,
    op: ComparisonOp,
    value: Value,
}

impl CompiledPredicate {
    /// Evaluates the predicate against a row.
    fn matches(&self, row: &Row) -> bool {
        let Some(value) = row.get(self.position) else {
            return false;
        };
        match self.op {
            ComparisonOp::Eq => value == &self.value,
            ComparisonOp::Ne => value != &self.value,
            ComparisonOp::Lt => value_cmp(value, &self.value) == Ordering::Less,
            ComparisonOp::Lte => value_cmp(value, &self.value) != Ordering::Greater,
            ComparisonOp::Gt => value_cmp(value, &self.value) == Ordering::Greater,
            ComparisonOp::Gte => value_cmp(value, &self.value) != Ordering::Less,
        }
    }
}

/// The compiled form of a [`Query`], resolved against a table schema.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CompiledQuery {
    table_id: TableId,
    predicates: Vec<CompiledPredicate>,
    order_position: Option<usize>,
    descending: bool,
    limit: Option<usize>,
    projection: Option<Vec<usize>>,
}

impl CompiledQuery {
    /// Returns the table this query observes.
    pub fn table_id(&self) -> TableId {
        self.table_id
    }

    /// Returns `true` if the row satisfies every predicate (AND semantics).
    pub fn matches(&self, row: &Row) -> bool {
        self.predicates
            .iter()
            .all(|predicate| predicate.matches(row))
    }

    /// Returns the row's sort value, if the query has an `order_by` clause.
    pub fn sort_value(&self, row: &Row) -> Option<Value> {
        self.order_position.map(|position| {
            row.get(position)
                .cloned()
                .expect("compiled position is in-range for validated rows")
        })
    }

    /// Returns `true` if the query orders descending.
    pub fn descending(&self) -> bool {
        self.descending
    }

    /// Returns the bounded window size, if any.
    pub fn limit(&self) -> Option<usize> {
        self.limit
    }

    /// Projects a row to the query's selected columns. Returns a full row
    /// clone when the query has no projection.
    pub fn project(&self, row: &Row) -> Row {
        match &self.projection {
            Some(positions) => Row::new(
                positions
                    .iter()
                    .map(|position| {
                        row.get(*position)
                            .cloned()
                            .expect("compiled projection position is in-range")
                    })
                    .collect(),
            ),
            None => row.clone(),
        }
    }
}

/// Resolves a table name through the store and compiles `query` against its
/// schema.
///
/// Returns [`Error::not_found`] if the table does not exist.
pub fn compile(
    store: &TableStore,
    query: &Query,
    config: &SubscriptionConfig,
) -> Result<CompiledQuery> {
    let table = store
        .table(query.table())
        .ok_or_else(|| Error::not_found(format!("table '{}' does not exist", query.table())))?;
    compile_for(table.id(), table.schema(), query, config)
}

/// Compiles `query` against `schema`, pinning the observed table id.
///
/// Returns [`Error::invalid_argument`] for an unknown column, a predicate
/// literal whose type does not match its column, or a bound exceeded;
/// [`Error::capacity`] when a query exceeds the configured bounds.
pub fn compile_for(
    table_id: TableId,
    schema: &TableSchema,
    query: &Query,
    config: &SubscriptionConfig,
) -> Result<CompiledQuery> {
    if query.predicates().len() > config.max_predicates {
        return Err(Error::capacity(format!(
            "query on table '{}' exceeds max_predicates {}",
            query.table(),
            config.max_predicates
        )));
    }

    let predicates = query
        .predicates()
        .iter()
        .map(|predicate| {
            let position = schema.column_index(predicate.column()).ok_or_else(|| {
                Error::invalid_argument(format!(
                    "table '{}' has no column named '{}'",
                    query.table(),
                    predicate.column()
                ))
            })?;
            let column = &schema.columns()[position];
            if predicate.value().type_of() != column.ty() {
                return Err(Error::invalid_argument(format!(
                    "predicate on column '{}' of table '{}' expects {}, got {}",
                    predicate.column(),
                    query.table(),
                    column.ty().name(),
                    predicate.value().type_of().name()
                )));
            }
            Ok(CompiledPredicate {
                position,
                op: predicate.op(),
                value: predicate.value().clone(),
            })
        })
        .collect::<Result<Vec<_>>>()?;

    let (order_position, descending) = match query.order_by() {
        Some(order_by) => {
            let position = schema.column_index(order_by.column()).ok_or_else(|| {
                Error::invalid_argument(format!(
                    "table '{}' has no column named '{}'",
                    query.table(),
                    order_by.column()
                ))
            })?;
            (
                Some(position),
                order_by.direction() == OrderDirection::Descending,
            )
        }
        None => (None, false),
    };

    let limit = query.limit().map(|value| value as usize);

    let projection = match query.projection() {
        Some(names) => {
            if names.len() > config.max_projection_columns {
                return Err(Error::capacity(format!(
                    "query on table '{}' exceeds max_projection_columns {}",
                    query.table(),
                    config.max_projection_columns
                )));
            }
            Some(
                names
                    .iter()
                    .map(|name| {
                        schema.column_index(name).ok_or_else(|| {
                            Error::invalid_argument(format!(
                                "table '{}' has no column named '{name}'",
                                query.table()
                            ))
                        })
                    })
                    .collect::<Result<Vec<_>>>()?,
            )
        }
        None => None,
    };

    Ok(CompiledQuery {
        table_id,
        predicates,
        order_position,
        descending,
        limit,
        projection,
    })
}

/// Scans `table` and returns the rows matching `compiled`, in ascending
/// `RowId` order (deterministic — the sort happens at the window layer).
pub fn matching_rows(table: &Table, compiled: &CompiledQuery) -> Vec<(RowId, Row)> {
    table
        .scan()
        .filter(|(_, row)| compiled.matches(row))
        .map(|(row_id, row)| (row_id, row.clone()))
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::{ColumnType, TableSchema};

    fn schema() -> TableSchema {
        TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .column("name", ColumnType::String)
            .build()
            .unwrap()
    }

    fn compiled(store: &TableStore, query: Query) -> CompiledQuery {
        compile(store, &query, &SubscriptionConfig::default()).unwrap()
    }

    fn compile_err(store: &TableStore, query: Query) -> Error {
        compile(store, &query, &SubscriptionConfig::default()).unwrap_err()
    }

    #[test]
    fn value_cmp_is_total_and_deterministic() {
        use nexum_core::row;
        // Same-type comparisons use inner ordering.
        assert_eq!(value_cmp(&Value::U64(1), &Value::U64(2)), Ordering::Less);
        assert_eq!(
            value_cmp(&Value::I32(5), &Value::I32(-5)),
            Ordering::Greater
        );
        assert_eq!(
            value_cmp(&Value::String("a".into()), &Value::String("b".into())),
            Ordering::Less
        );
        // Floats use total_cmp: NaN is stable and comparable.
        let nan = Value::F64(f64::NAN);
        assert_eq!(value_cmp(&nan, &nan), Ordering::Equal);
        assert_eq!(value_cmp(&Value::F64(1.0), &nan), Ordering::Less);
        // Cross-variant comparison is by declaration order, not by value.
        assert_eq!(
            value_cmp(&Value::Bool(true), &Value::I32(0)),
            Ordering::Less
        );
        // Repeatability across "runs".
        for _ in 0..2 {
            assert_eq!(
                value_cmp(&Value::F64(2.5), &Value::F64(2.0)),
                Ordering::Greater
            );
        }
        let _ = row![1u64];
    }

    #[test]
    fn compile_resolves_columns_and_types() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let query = Query::builder("players")
            .predicate_eq("zone_id", 10u64)
            .predicate_gt("health", 50i32)
            .order_by("health", OrderDirection::Descending)
            .project(&["id", "name"])
            .build()
            .unwrap();
        let compiled = compiled(&store, query);
        assert_eq!(compiled.table_id(), store.table("players").unwrap().id());
        assert_eq!(compiled.limit(), None);
        assert!(compiled.descending());
        assert_eq!(compiled.predicates.len(), 2);
        assert_eq!(compiled.order_position, Some(2));
        assert_eq!(compiled.projection, Some(vec![0, 3]));
    }

    #[test]
    fn compile_rejects_unknown_table() {
        let store = TableStore::new();
        let err = compile(
            &store,
            &Query::builder("nope").build().unwrap(),
            &SubscriptionConfig::default(),
        )
        .unwrap_err();
        assert!(matches!(err, Error::NotFound(_)));
    }

    #[test]
    fn compile_rejects_unknown_column() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let err = compile_err(
            &store,
            Query::builder("players")
                .predicate_eq("nope", 1u64)
                .build()
                .unwrap(),
        );
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn compile_rejects_type_mismatched_literal() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        // zone_id is U64; a string literal is rejected.
        let err = compile_err(
            &store,
            Query::builder("players")
                .predicate_eq("zone_id", "ten")
                .build()
                .unwrap(),
        );
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn compile_rejects_excessive_predicates() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let mut query = Query::builder("players");
        for i in 0..2 {
            query = query.predicate_eq("zone_id", i);
        }
        let config = SubscriptionConfig {
            max_predicates: 1,
            ..SubscriptionConfig::default()
        };
        let err = compile(&store, &query.build().unwrap(), &config).unwrap_err();
        assert!(matches!(err, Error::Capacity(_)));
    }

    #[test]
    fn compile_rejects_excessive_projection() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let config = SubscriptionConfig {
            max_projection_columns: 1,
            ..SubscriptionConfig::default()
        };
        let err = compile(
            &store,
            &Query::builder("players")
                .project(&["id", "zone_id"])
                .build()
                .unwrap(),
            &config,
        )
        .unwrap_err();
        assert!(matches!(err, Error::Capacity(_)));
    }

    #[test]
    fn predicates_evaluate_all_operators() {
        let row = nexum_core::row![1u64, 10u64, 50i32];
        let eval = |op: ComparisonOp, value: i32| {
            CompiledPredicate {
                position: 2,
                op,
                value: Value::I32(value),
            }
            .matches(&row)
        };
        assert!(eval(ComparisonOp::Eq, 50));
        assert!(!eval(ComparisonOp::Eq, 51));
        assert!(eval(ComparisonOp::Ne, 51));
        assert!(eval(ComparisonOp::Lt, 51));
        assert!(!eval(ComparisonOp::Lt, 50));
        assert!(eval(ComparisonOp::Lte, 50));
        assert!(eval(ComparisonOp::Gt, 49));
        assert!(!eval(ComparisonOp::Gt, 50));
        assert!(eval(ComparisonOp::Gte, 50));
    }

    #[test]
    fn matching_rows_filters_and_orders_by_row_id() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let table = store.table_mut("players").unwrap();
        table
            .insert(nexum_core::row![1u64, 10u64, 100i32, "a".to_string()])
            .unwrap();
        table
            .insert(nexum_core::row![2u64, 20u64, 90i32, "b".to_string()])
            .unwrap();
        table
            .insert(nexum_core::row![3u64, 10u64, 80i32, "c".to_string()])
            .unwrap();
        let compiled = compiled(
            &store,
            Query::builder("players")
                .predicate_eq("zone_id", 10u64)
                .build()
                .unwrap(),
        );
        let table = store.table("players").unwrap();
        let rows = matching_rows(table, &compiled);
        assert_eq!(rows.len(), 2);
        assert_eq!(rows[0].0, RowId::from_u64(0));
        assert_eq!(rows[1].0, RowId::from_u64(2));
    }

    #[test]
    fn projection_selects_columns() {
        let mut store = TableStore::new();
        store.create_table(schema()).unwrap();
        let projected_query = compiled(
            &store,
            Query::builder("players")
                .project(&["name", "id"])
                .build()
                .unwrap(),
        );
        let row = nexum_core::row![1u64, 10u64, 50i32, "alice".to_string()];
        let projected = projected_query.project(&row);
        assert_eq!(
            projected.values(),
            &[Value::String("alice".into()), Value::U64(1)]
        );
        // No projection: full row clone.
        let full_query = compiled(&store, Query::builder("players").build().unwrap());
        assert_eq!(full_query.project(&row), row);
    }
}
