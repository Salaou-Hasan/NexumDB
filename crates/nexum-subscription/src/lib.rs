#![cfg_attr(docsrs, feature(doc_auto_cfg))]
//! Nexum subscription engine — **Phase 8** (ADR-008).
//!
//! Subscriptions are **reactive views over authoritative table state**,
//! driven entirely by the existing commit boundary — never by polling:
//!
//! ```text
//! Transaction::commit() → Vec<Change> → apply_changes(store, &changes)
//!                                            │
//!                                            ├─ WAL (the caller)
//!                                            └─ per-subscription deltas
//! ```
//!
//! The engine observes committed state; it never becomes authoritative
//! itself. There is **one** authoritative state (the table storage), **one**
//! transaction model, and **one** change stream — subscriptions are a third
//! consumer of the exact `Vec<Change>` boundary that WAL already attaches
//! to, and they work identically for native and WASM reducers.
//!
//! ```rust
//! use nexum_core::{ColumnType, TableSchema};
//! use nexum_subscription::{OrderDirection, Query, SubscriptionRegistry, SubscriptionUpdate};
//! use nexum_table::{row, TableStore};
//! use nexum_tx::Transaction;
//!
//! let mut store = TableStore::new();
//! store
//!     .create_table(
//!         TableSchema::builder("players")
//!             .column("id", ColumnType::U64)
//!             .column("zone_id", ColumnType::U64)
//!             .column("health", ColumnType::I32)
//!             .primary_key(&["id"])
//!             .build()
//!             .unwrap(),
//!     )
//!     .unwrap();
//!
//! let mut registry = SubscriptionRegistry::new();
//! let sub = registry
//!     .subscribe(
//!         &store,
//!         Query::builder("players")
//!             .predicate_eq("zone_id", 10u64)
//!             .order_by("health", OrderDirection::Ascending)
//!             .build()
//!             .unwrap(),
//!     )
//!     .unwrap();
//! assert_eq!(registry.drain(sub).unwrap().len(), 1); // Initial snapshot
//!
//! // One committed transaction → one apply_changes call → one delta.
//! let mut tx = Transaction::begin(&mut store);
//! tx.insert(&store, "players", row![1u64, 10u64, 100i32]).unwrap();
//! let changes = tx.commit(&mut store).unwrap();
//! let report = registry.apply_changes(&store, &changes);
//! assert_eq!(report.affected(), &[sub]);
//!
//! let updates = registry.drain(sub).unwrap();
//! assert_eq!(updates.len(), 1);
//! assert!(matches!(&updates[0], SubscriptionUpdate::Insert { .. }));
//! ```
//!
//! - [`SubscriptionRegistry`] — subscribe / apply_changes / drain /
//!   unsubscribe / resync; owns the monotonic commit sequence (the cursor)
//! - [`Query`] / [`Predicate`] / [`ComparisonOp`] / [`OrderBy`] /
//!   [`OrderDirection`] — the logical, serializable, bounded query model
//! - [`Subscription`] / [`SubscriptionState`] — one observation's derived
//!   view, cursor, and lifecycle
//! - [`SubscriptionUpdate`] / [`DeliveredRow`] — the delivered stream
//!
//! Semantics: atomic establishment (no snapshot/live race), committed-only
//! observation, correct enter/leave handling for updates, deterministic
//! ordering, bounded per-subscription buffers with stale-marking
//! backpressure, and full resync from authoritative state.
//!
//! **Out of scope in this phase:** networking, client delivery, simulation,
//! and durable subscription state (after WAL recovery the application
//! re-subscribes over the recovered state — recovered history never replays
//! as new live commits).

#![allow(unsafe_code)]

mod config;
mod delta;
mod matcher;
mod query;
mod registry;
mod subscription;

pub use config::SubscriptionConfig;
pub use delta::{DeliveredRow, SubscriptionUpdate};
pub use nexum_core::SubscriptionId;
pub use query::{ComparisonOp, OrderBy, OrderDirection, Predicate, Query};
pub use registry::{ApplyReport, SubscriptionRegistry};
pub use subscription::{Subscription, SubscriptionState};

#[cfg(test)]
mod tests;
