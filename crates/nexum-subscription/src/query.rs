//! The logical subscription query model (ADR-008 D2).
//!
//! A [`Query`] is a **serializable, protocol-independent** description of a
//! table observation: AND-combined `column op literal` predicates, an
//! optional single sort key, an optional bounded window (`limit`), and an
//! optional column projection. It deliberately does **not** contain Rust
//! closures or implementation-specific references — it is the permanent
//! representation of a subscription (design notes §3).
//!
//! Column references are **names**; they are resolved to positions and
//! type-checked against the schema once, at `subscribe`/`resync` time
//! ([`crate::matcher::compile`]).

use nexum_core::{Error, Result, Value};

/// Comparison operator for one predicate.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ComparisonOp {
    /// `==`
    Eq,
    /// `!=`
    Ne,
    /// `<`
    Lt,
    /// `<=`
    Lte,
    /// `>`
    Gt,
    /// `>=`
    Gte,
}

impl ComparisonOp {
    /// Returns a stable name for error messages.
    pub const fn name(self) -> &'static str {
        match self {
            Self::Eq => "==",
            Self::Ne => "!=",
            Self::Lt => "<",
            Self::Lte => "<=",
            Self::Gt => ">",
            Self::Gte => ">=",
        }
    }
}

/// Sort direction for the optional single sort key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OrderDirection {
    /// Ascending (smallest first).
    Ascending,
    /// Descending (largest first).
    Descending,
}

/// One `column op value` predicate. Predicates within a query are
/// AND-combined: a row matches only when every predicate holds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Predicate {
    column: String,
    op: ComparisonOp,
    value: Value,
}

impl Predicate {
    /// Creates a predicate on the named column.
    pub fn new(column: impl Into<String>, op: ComparisonOp, value: Value) -> Self {
        Self {
            column: column.into(),
            op,
            value,
        }
    }

    /// Returns the predicate's column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the comparison operator.
    pub fn op(&self) -> ComparisonOp {
        self.op
    }

    /// Returns the literal value compared against the column.
    pub fn value(&self) -> &Value {
        &self.value
    }
}

/// The optional single sort key: one column plus a direction.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderBy {
    column: String,
    direction: OrderDirection,
}

impl OrderBy {
    /// Creates a sort key over the named column.
    pub fn new(column: impl Into<String>, direction: OrderDirection) -> Self {
        Self {
            column: column.into(),
            direction,
        }
    }

    /// Returns the sort column name.
    pub fn column(&self) -> &str {
        &self.column
    }

    /// Returns the sort direction.
    pub fn direction(&self) -> OrderDirection {
        self.direction
    }
}

/// The logical, serializable subscription query.
///
/// Construct with [`Query::builder`], which returns a validating builder
/// (the `TableSchema::builder` convention).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Query {
    table: String,
    predicates: Vec<Predicate>,
    order_by: Option<OrderBy>,
    limit: Option<u32>,
    projection: Option<Vec<String>>,
}

impl Query {
    /// Starts a query over the named table.
    pub fn builder(table: impl Into<String>) -> QueryBuilder {
        QueryBuilder::new(table)
    }

    /// Returns the queried table name.
    pub fn table(&self) -> &str {
        &self.table
    }

    /// Returns the AND-combined predicates.
    pub fn predicates(&self) -> &[Predicate] {
        &self.predicates
    }

    /// Returns the optional sort key.
    pub fn order_by(&self) -> Option<&OrderBy> {
        self.order_by.as_ref()
    }

    /// Returns the bounded window size, if any.
    pub fn limit(&self) -> Option<u32> {
        self.limit
    }

    /// Returns the projected column names, if any.
    pub fn projection(&self) -> Option<&[String]> {
        self.projection.as_deref()
    }
}

/// A validating builder for [`Query`].
#[derive(Debug, Clone)]
pub struct QueryBuilder {
    table: String,
    predicates: Vec<Predicate>,
    order_by: Option<OrderBy>,
    limit: Option<u32>,
    projection: Option<Vec<String>>,
}

impl QueryBuilder {
    /// Starts a query over the named table.
    pub fn new(table: impl Into<String>) -> Self {
        Self {
            table: table.into(),
            predicates: Vec::new(),
            order_by: None,
            limit: None,
            projection: None,
        }
    }

    /// Adds an `op` predicate: `column op value`.
    pub fn predicate(
        mut self,
        column: impl Into<String>,
        op: ComparisonOp,
        value: impl Into<Value>,
    ) -> Self {
        self.predicates
            .push(Predicate::new(column, op, value.into()));
        self
    }

    /// Adds a `column == value` predicate.
    pub fn predicate_eq(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Eq, value)
    }

    /// Adds a `column != value` predicate.
    pub fn predicate_ne(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Ne, value)
    }

    /// Adds a `column < value` predicate.
    pub fn predicate_lt(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Lt, value)
    }

    /// Adds a `column <= value` predicate.
    pub fn predicate_lte(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Lte, value)
    }

    /// Adds a `column > value` predicate.
    pub fn predicate_gt(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Gt, value)
    }

    /// Adds a `column >= value` predicate.
    pub fn predicate_gte(self, column: impl Into<String>, value: impl Into<Value>) -> Self {
        self.predicate(column, ComparisonOp::Gte, value)
    }

    /// Sets the single sort key.
    pub fn order_by(mut self, column: impl Into<String>, direction: OrderDirection) -> Self {
        self.order_by = Some(OrderBy::new(column, direction));
        self
    }

    /// Bounds the delivered window to the top-`limit` rows by the ordering
    /// (ties broken by `RowId`). Must be greater than zero.
    pub fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Projects delivered rows to the named columns (in the given order).
    /// When absent, full rows are delivered.
    pub fn project(mut self, columns: &[&str]) -> Self {
        self.projection = Some(columns.iter().map(|name| (*name).to_owned()).collect());
        self
    }

    /// Validates and produces the [`Query`].
    ///
    /// Returns [`Error::invalid_argument`] for an empty table name, an empty
    /// predicate or projection column name, or a zero limit.
    pub fn build(self) -> Result<Query> {
        if self.table.trim().is_empty() {
            return Err(Error::invalid_argument("query table name must not be empty"));
        }
        for predicate in &self.predicates {
            if predicate.column.trim().is_empty() {
                return Err(Error::invalid_argument(
                    "query predicate column must not be empty",
                ));
            }
        }
        if let Some(order_by) = &self.order_by
            && order_by.column.trim().is_empty()
        {
            return Err(Error::invalid_argument(
                "query order_by column must not be empty",
            ));
        }
        if self.limit == Some(0) {
            return Err(Error::invalid_argument(
                "query limit must be greater than zero",
            ));
        }
        if let Some(projection) = &self.projection {
            for column in projection {
                if column.trim().is_empty() {
                    return Err(Error::invalid_argument(
                        "query projection column must not be empty",
                    ));
                }
            }
        }
        Ok(Query {
            table: self.table,
            predicates: self.predicates,
            order_by: self.order_by,
            limit: self.limit,
            projection: self.projection,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;

    #[test]
    fn builder_produces_full_query() {
        let query = Query::builder("players")
            .predicate_eq("zone_id", 10u64)
            .predicate_gt("health", 50i32)
            .order_by("health", OrderDirection::Descending)
            .limit(100)
            .project(&["id", "health"])
            .build()
            .unwrap();
        assert_eq!(query.table(), "players");
        assert_eq!(query.predicates().len(), 2);
        assert_eq!(query.predicates()[0].column(), "zone_id");
        assert_eq!(query.predicates()[0].op(), ComparisonOp::Eq);
        assert_eq!(query.predicates()[0].value(), &Value::U64(10));
        assert_eq!(query.predicates()[1].op(), ComparisonOp::Gt);
        assert_eq!(query.order_by().unwrap().column(), "health");
        assert_eq!(
            query.order_by().unwrap().direction(),
            OrderDirection::Descending
        );
        assert_eq!(query.limit(), Some(100));
        assert_eq!(query.projection().unwrap(), &["id".to_string(), "health".to_string()]);
    }

    #[test]
    fn values_convert_ergonomically() {
        let query = Query::builder("t")
            .predicate_eq("id", 7u64)
            .predicate_ne("name", "x")
            .predicate_lte("score", 3.5f64)
            .build()
            .unwrap();
        assert_eq!(query.predicates()[0].value(), &Value::U64(7));
        assert_eq!(query.predicates()[1].value(), &Value::String("x".into()));
        assert_eq!(query.predicates()[2].value(), &Value::F64(3.5));
    }

    #[test]
    fn defaults_have_no_predicates_order_limit_projection() {
        let query = Query::builder("players").build().unwrap();
        assert!(query.predicates().is_empty());
        assert!(query.order_by().is_none());
        assert!(query.limit().is_none());
        assert!(query.projection().is_none());
    }

    #[test]
    fn rejects_empty_table_name() {
        let err = Query::builder("  ").build().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_empty_predicate_column() {
        let err = Query::builder("t").predicate_eq("", 1u64).build().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_empty_order_column() {
        let err = Query::builder("t")
            .order_by("", OrderDirection::Ascending)
            .build()
            .unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_zero_limit() {
        let err = Query::builder("t").limit(0).build().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn rejects_empty_projection_column() {
        let err = Query::builder("t").project(&["id", ""]).build().unwrap_err();
        assert!(matches!(err, Error::InvalidArgument(_)));
    }

    #[test]
    fn query_is_eq_comparable_and_row_agnostic() {
        let a = Query::builder("players").predicate_eq("zone_id", 10u64).build().unwrap();
        let b = Query::builder("players").predicate_eq("zone_id", 10u64).build().unwrap();
        let c = Query::builder("players").predicate_eq("zone_id", 20u64).build().unwrap();
        assert_eq!(a, b);
        assert_ne!(a, c);
        // Sanity: the model does not reference implementation objects.
        let _ = row![1u64];
    }
}
