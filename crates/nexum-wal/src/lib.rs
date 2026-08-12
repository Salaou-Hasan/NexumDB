//! Nexum durability — **Phase 5**: WAL, snapshots, and recovery.
//!
//! This crate makes committed state durable and recoverable while attaching
//! at the one boundary the transaction engine already produces:
//!
//! ```text
//! Transaction::commit(&mut store) → Vec<Change> → Wal::append(tx_id, &changes)
//!                                                      ↓
//!                                                durability acknowledged
//! ```
//!
//! ```rust,no_run
//! use nexum_core::{ColumnType, TableSchema};
//! use nexum_table::TableStore;
//! use nexum_tx::Transaction;
//! use nexum_wal::{DurabilityPolicy, Snapshot, Wal, recover};
//!
//! # fn main() -> nexum_core::Result<()> {
//! let dir = std::env::temp_dir().join("nexum-wal-doc-example");
//! let wal_path = dir.join("log.wal");
//! # let _ = std::fs::remove_dir_all(&dir);
//! std::fs::create_dir_all(&dir).unwrap();
//!
//! let mut store = TableStore::new();
//! store.create_table(
//!     TableSchema::builder("players")
//!         .column("id", ColumnType::U64)
//!         .column("health", ColumnType::I32)
//!         .primary_key(&["id"])
//!         .build()
//!         .unwrap(),
//! )?;
//!
//! // A transaction: memory-commit, then make it durable.
//! let mut wal = Wal::create(&wal_path, DurabilityPolicy::Sync)?;
//! let mut tx = Transaction::begin(&mut store);
//! tx.insert(&store, "players", nexum_table::row![1u64, 100i32])?;
//! let changes = tx.commit(&mut store)?;
//! wal.append(tx.id(), &changes)?; // now durable
//!
//! // Later: snapshot + recovery into a fresh store.
//! Snapshot::capture(&store, wal.lsn().as_u64()).write(&dir)?;
//! let mut fresh = TableStore::new();
//! let report = recover(&mut fresh, &mut wal, &dir)?;
//! assert_eq!(fresh.table("players").unwrap().len(), 1);
//! assert_eq!(report.replayed_txs, 0); // covered by the snapshot
//! # let _ = std::fs::remove_dir_all(&dir);
//! # Ok(())
//! # }
//! ```
//!
//! - [`Wal`] — the framed, checksummed, append-only log; [`Wal::append`] is
//!   the durability point, [`Wal::recover_changes`] reads committed
//!   transactions back
//! - [`DurabilityPolicy`] — `Flush` (process-crash safe) vs `Sync`/fsync
//!   (power-loss safe, the durable mode)
//! - [`Snapshot`] — authoritative state at a WAL LSN, written atomically
//! - [`recover`] — snapshot restore + WAL replay, reproducing rows, versions,
//!   epochs, row ids, and indexes exactly (ADR-005 D6)
//!
//! **Out of scope in this phase:** reducers (6), WASM (7), subscriptions (8),
//! simulation (9), networking (11), group commit, log rotation.

#![forbid(unsafe_code)]
#![warn(missing_docs)]

pub mod recovery;
pub mod snapshot;
pub mod wal;

pub use recovery::{recover, RecoveryReport, RecoveredSnapshot};
pub use snapshot::{Snapshot, SNAPSHOT_PREFIX, SNAPSHOT_SUFFIX, SNAPSHOT_VERSION};
pub use wal::{DurabilityPolicy, Lsn, RecoveredTx, Wal, FORMAT_VERSION, HEADER_MAGIC};
