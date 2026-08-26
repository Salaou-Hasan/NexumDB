//! Nexum storage engine — **Phase 3** (memory-first authoritative state).
//!
//! This crate owns the **one authoritative in-memory representation** of every
//! table's rows:
//!
//! ```text
//! Table API (nexum-table)
//!     ↓
//! STORAGE ENGINE (this crate)
//!     ├── row data      StorageTable.rows
//!     ├── row versions  StoredRow.version (beside the row)
//!     └── changes       Change buffer (derived, drainable)
//!          ↓
//!      derived indexes (owned by nexum-table)
//! ```
//!
//! - [`StorageTable`] — authoritative per-table row state, version tracking,
//!   and change buffering; index-agnostic by design (ADR-003 D2).
//! - [`StoredRow`] — row data plus its version in one record.
//! - [`Change`] — the minimum useful record of a committed mutation; the
//!   attach point for Phase 5 WAL and Phase 8 subscriptions.
//!
//! Concurrency model: single-threaded exclusive ownership; mutations require
//! `&mut`. No locks, no atomics (ADR-003 D7).
//!
//! **Out of scope in this phase:** OCC transactions (Phase 4), WAL, snapshots,
//! disk persistence (Phase 5).

#![allow(unsafe_code)]
#![warn(missing_docs)]

mod change;
pub mod columnar;
pub mod snapshot;
mod table;

pub use change::Change;
pub use columnar::{ColumnarStore, RowRef};
pub use snapshot::TableState;
pub use table::{StorageTable, StoredRow};
