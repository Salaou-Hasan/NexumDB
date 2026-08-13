//! Nexum core: the shared foundation of the Nexum authoritative state engine.
//!
//! This crate is deliberately dependency-free. It defines the *vocabulary* of
//! the whole system — the types and interfaces every other crate builds on:
//!
//! - [`ids`] — typed identifiers (`TableId`, `RowId`, `TransactionId`, ...)
//! - [`types`] — [`Version`] and [`Timestamp`], the primitives of concurrency
//!   control and simulation time
//! - [`errors`] — the common [`Error`] model and [`Result`] alias
//! - [`state`] — foundational state interfaces (`Id`, `Versioned`, `ChangeKind`)
//! - [`value`] — typed column types and values (`ColumnType`, `Value`)
//! - [`row`] — the schema-free ordered value list ([`Row`]) and `row!` macro
//! - [`schema`] — shared table-schema primitives (`TableSchema`, `ColumnDef`,
//!   `IndexDef`) defined once and shared by every crate
//! - [`binary`] — deterministic little-endian encoding for values, rows, and
//!   schemas plus CRC-32, shared by WAL records and snapshots (Phase 5)
//!
//! Everything else in Nexum depends on this crate; it depends on nothing.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod binary;
pub mod errors;
pub mod ids;
pub mod row;
pub mod schema;
pub mod state;
pub mod types;
pub mod value;

pub use errors::{Error, Result};
pub use ids::{
    ColumnId, ConnectionId, GameInstanceId, PartitionId, PlayerId, ReducerId, RowId, SessionId,
    SubscriptionId, SystemId, TableId, TickId, TransactionId, WorkerId, WorldId,
};
pub use row::Row;
pub use schema::{ColumnDef, IndexDef, TableSchema};
pub use state::{ChangeKind, Id, Versioned};
pub use types::{Timestamp, Version};
pub use value::{ColumnType, Value};
