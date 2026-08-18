//! Nexum WASM reducer runtime: sandboxed execution of untrusted reducers
//! (Phase 7, ADR-007).
//!
//! A WASM reducer is a user-provided module that runs inside a WebAssembly
//! sandbox and changes authoritative state **only through a restricted host
//! ABI** that translates into the existing Phase 6
//! [`ReducerContext`](nexum_reducer::ReducerContext) → `Transaction` → OCC →
//! commit path. ONE STATE. ONE TRANSACTION MODEL. ONE OCC IMPLEMENTATION.
//! ONE COMMIT PATH. This crate introduces **no** second storage, transaction,
//! or OCC implementation.
//!
//! ```rust
//! use nexum_core::{ColumnType, TableSchema};
//! use nexum_reducer::ReducerArgs;
//! use nexum_table::TableStore;
//! use nexum_wasm::{WasmLimits, WasmModuleRegistry};
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
//! // A minimal reducer module: imports only ("nexum","op"), exports the
//! // four ABI symbols, and returns the encoded `Value::U64(42)`.
//! let bytes = wat::parse_str(
//!     r#"(module
//!         (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
//!         (memory (export "memory") 16)
//!         (global (export "_nexum_in_ptr") i32 (i32.const 0))
//!         (global (export "_nexum_out_ptr") i32 (i32.const 16384))
//!         (func (export "_nexum_reducer_run") (result i32)
//!           (i32.store8 (i32.const 16384) (i32.const 8))
//!           (i64.store align=1 (i32.const 16385) (i64.const 42))
//!           (i32.const 9)))
//!     "#,
//! )
//! .unwrap();
//!
//! let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
//! registry.register("ping", 1, bytes).unwrap();
//!
//! let result = registry
//!     .invoke(&mut store, "ping", &ReducerArgs::new())
//!     .unwrap();
//! assert_eq!(result.return_value(), &nexum_core::Value::U64(42));
//! ```
//!
//! - [`WasmModuleRegistry`] — validate/register/lookup/list + the `invoke`
//!   entry point (one invocation = one transaction)
//! - [`WasmReducerModule`] — a validated, compiled module (the ABI contract)
//! - [`WasmLimits`] — fuel, memory, host-call, and byte budgets
//!
//! Security model: the guest holds no reference into Nexum (no `&TableStore`,
//! no `Transaction`, no `ReducerContext`); the only import is
//! `("nexum","op")` with a fixed signature; WASI is never linked; input
//! lengths are bounded before allocation; memory growth is arbitrated by a
//! host-side limiter; fuel (not wall-clock time) bounds execution; every ABI
//! error is sticky so it can never be ignored into a commit; and a trap,
//! fuel exhaustion, or limit breach aborts the transaction with zero
//! authoritative mutation (writes were provisional — no rollback machinery
//! exists).
//!
//! **Out of scope in this phase:** subscriptions, simulation, networking,
//! distribution, hot module reload, and WASI.

mod abi;
mod host;
mod limits;
mod module;
mod registry;

pub use limits::{ABI_IN_CAP, ABI_OUT_CAP, WasmLimits};
pub use module::WasmReducerModule;
pub use registry::{WasmModuleRegistry, WasmStageTimes};

#[cfg(test)]
mod tests;
