#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Nexum transaction engine — **Phase 4** (Optimistic Concurrency Control).
//!
//! This crate turns the table/storage layers into an **authoritative
//! transactional state engine**. Every authoritative mutation happens through
//! a transaction:
//!
//! ```text
//! BEGIN → READ PHASE → WRITE BUFFER → VALIDATION → COMMIT
//!    (reads recorded)                 (pure)     (apply + Change[])
//! ```
//!
//! The central invariant: **no authoritative state is mutated until the
//! entire transaction has successfully validated** (ADR-004 D3). Reads record
//! `(TableId, RowId) → Option<Version>` observations; writes are buffered and
//! coalesced in a [`WriteSet`]; validation compares observations and planned
//! writes against live state without mutating; commit then applies everything
//! in a deterministic order and returns the [`Change`] records.
//!
//! ```rust
//! use nexum_core::{ColumnType, TableSchema};
//! use nexum_table::{row, TableStore};
//! use nexum_tx::Transaction;
//!
//! let mut store = TableStore::new();
//! store
//!     .create_table(
//!         TableSchema::builder("players")
//!             .column("id", ColumnType::U64)
//!             .column("health", ColumnType::I32)
//!             .primary_key(&["id"])
//!             .build()
//!             .unwrap(),
//!     )
//!     .unwrap();
//!
//! // A transaction over the store: read, write, commit.
//! let mut tx = Transaction::begin(&mut store);
//! let alice = tx.insert(&store, "players", row![1u64, 100i32]).unwrap();
//! tx.update(&store, "players", alice, row![1u64, 90i32]).unwrap(); // coalesces
//! let changes = tx.commit(&mut store).unwrap();
//! assert_eq!(changes.len(), 1); // one insert (the update coalesced)
//! ```
//!
//! - [`Transaction`] — the transaction with its explicit state machine
//!   (`Active → Committed | Aborted`)
//! - [`TransactionState`] — the lifecycle state
//! - [`ReadSet`] / [`WriteSet`] — recorded reads and buffered writes
//!   (with deterministic coalescing)
//!
//! Concurrency model: single-threaded exclusive ownership per store, no
//! locks (Phase 3 + ADR-004 D10). The Phase 10 runtime will serialize
//! transactions per partition and retry on `Error::Conflict`.
//!
//! **Out of scope in this phase:** WAL/snapshots (Phase 5), reducers (6),
//! WASM (7), subscriptions (8), simulation (9), networking (11).

#![allow(unsafe_code)]
#![warn(missing_docs)]

mod commit;
mod read_set;
mod transaction;
mod write_set;

#[cfg(test)]
mod tests;

pub use read_set::ReadSet;
pub use transaction::{Transaction, TransactionState};
pub use write_set::{WriteEntry, WriteSet};
