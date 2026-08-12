//! Nexum reducers: authoritative server-side application logic (Phase 6).
//!
//! A reducer receives input, executes against Nexum state through a
//! controlled [`ReducerContext`], and either commits atomically or aborts
//! completely. **One reducer invocation = one transaction** (ADR-006 D1):
//!
//! ```rust
//! use nexum_core::{ColumnType, Error, TableSchema, Value};
//! use nexum_reducer::{ReducerArgs, ReducerDefinition, ReducerRegistry};
//! use nexum_table::{row, TableStore};
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
//! let mut registry = ReducerRegistry::new();
//! let definition = ReducerDefinition::new(
//!     nexum_core::ReducerId::from_u64(0),
//!     "spawn_player",
//!     |ctx, args| {
//!         let id = args.require_u64("player_id")?;
//!         let _row_id = ctx.insert("players", row![id, 100i32])?; // provisional handle
//!         ctx.emit("player_spawned", id)?;
//!         Ok(Value::U64(id)) // reducers address rows by primary key, not
//!                            // storage-assigned row ids
//!     },
//! )
//! .unwrap();
//! registry.register(definition).unwrap();
//!
//! let args = ReducerArgs::new().insert("player_id", 7u64);
//! let result = registry.invoke(&mut store, "spawn_player", &args).unwrap();
//! assert_eq!(result.changes().len(), 1);
//! assert_eq!(result.events().len(), 1);
//! assert_eq!(result.return_value(), &Value::U64(7));
//! assert_eq!(result.changes()[0].row_id().as_u64(), 0); // real row id on the change
//! ```
//!
//! - [`ReducerRegistry`] — register / lookup / list + the `invoke` entry point
//! - [`ReducerContext`] — the only surface a reducer sees (reads, writes,
//!   scans, unique lookups, event emission; never `&mut TableStore`)
//! - [`ReducerDefinition`] / [`ReducerFn`] — the native Rust reducer API
//! - [`ReducerArgs`] — named, deterministic, protocol-independent arguments
//! - [`ReducerEvent`] / [`ReducerResult`] — transaction-local events and the
//!   committed outcome (`changes` + `events` + `return_value`)
//!
//! Semantics inherited from the transaction engine (Phases 4–5):
//! read-your-writes, version OCC, missing-row observations, table-epoch
//! phantom protection, unique-key validation, deterministic commit ordering,
//! and multi-table atomicity. Reducer success = **committed in memory**; the
//! caller appends `result.changes` to the WAL with `result.tx_id` for
//! durability (ADR-006 D8).
//!
//! Panics are caught at the `execute` boundary: the transaction aborts, no
//! authoritative state changes, no events escape, and the invocation fails
//! with `Error::Internal`. Native reducers are **trusted code** — this is an
//! API boundary, not a sandbox; the Phase 7 WASM runtime will provide the
//! untrusted-code boundary.
//!
//! **Out of scope in this phase:** WASM, subscriptions, simulation,
//! networking, automatic retry, hot reload, reducer-to-reducer invocation.

mod args;
mod context;
mod definition;
mod event;
mod registry;

pub use args::ReducerArgs;
pub use context::ReducerContext;
pub use definition::{ReducerDefinition, ReducerFn};
pub use event::ReducerEvent;
pub use registry::{ReducerRegistry, ReducerResult};

pub use registry::finish_invocation;

#[cfg(test)]
mod tests;
