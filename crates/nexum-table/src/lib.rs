//! Nexum table engine — **Phases 2 & 3**.
//!
//! Tables are the public relational-style API over the authoritative storage
//! engine:
//!
//! ```text
//! TableStore ── owns ──> Table ── owns ──> StorageTable (authoritative,
//!                                             rows + versions + changes)
//!                                       └── primary key index (derived)
//!                                       └── secondary indexes (derived)
//! ```
//!
//! - schemas and column definitions (shared types live in
//!   [`nexum_core::schema`])
//! - typed values and rows ([`Value`], [`Row`])
//! - primary keys and secondary indexes (unique and non-unique)
//! - insert / get / update / delete / scan / lookup
//! - row versions ([`Table::version_of`]) and change tracking
//!   ([`Table::changes`], [`Table::drain_changes`])
//!
//! Rows are keyed by engine-assigned [`RowId`]s; the declared primary key is
//! enforced as a unique index, not as row identity. The authoritative state
//! lives in the storage engine; indexes here are derived infrastructure
//! maintained transactionally on every mutation and rebuildable from a scan.
//!
//! ## Example
//!
//! ```
//! use nexum_core::{ColumnType, TableSchema, Value};
//! use nexum_table::{row, TableStore};
//!
//! let schema = TableSchema::builder("players")
//!     .column("id", ColumnType::U64)
//!     .column("zone_id", ColumnType::U64)
//!     .column("health", ColumnType::I32)
//!     .column("level", ColumnType::U32)
//!     .primary_key(&["id"])
//!     .index("by_zone", &["zone_id"])
//!     .build()
//!     .unwrap();
//!
//! let mut store = TableStore::new();
//! store.create_table(schema).unwrap();
//!
//! let table = store.table_mut("players").unwrap();
//! let alice = table.insert(row![1u64, 10u64, 100i32, 5u32]).unwrap();
//! assert_eq!(
//!     table.get(alice).unwrap().get_named(table.schema(), "health"),
//!     Some(&Value::I32(100))
//! );
//! assert_eq!(table.lookup("by_zone", &[Value::U64(10)]).unwrap(), vec![alice]);
//! assert_eq!(table.version_of(alice), Some(nexum_core::Version::ZERO));
//! ```

#![forbid(unsafe_code)]
#![warn(missing_docs)]

mod index;
pub mod store;
pub mod table;

pub use nexum_core::row;
pub use nexum_core::row::Row;
pub use store::TableStore;
pub use table::Table;
