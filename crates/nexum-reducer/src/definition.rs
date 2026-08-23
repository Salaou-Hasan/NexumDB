//! Reducer definitions ([`ReducerDefinition`]) and the native execute
//! function type ([`ReducerFn`]).
//!
//! Phase 6 defines the **native Rust** reducer API (ADR-006 D9). Future
//! execution backends — most importantly the Phase 7 WASM sandbox — will
//! conform to the same semantic model: one invocation, one transaction, a
//! controlled context, a `Result<Value, Error>` outcome, and a
//! `ReducerResult` carrying changes + events + a return value. This
//! definition type is deliberately small; the WASM ABI is designed later.

use nexum_core::{Error, ReducerId, Result, Value};

use crate::args::ReducerArgs;
use crate::context::ReducerContext;

/// The native execute function of a reducer.
///
/// Higher-ranked so the context borrow can be fresh for every invocation.
pub type ReducerFn = for<'a> fn(&mut ReducerContext<'a>, &ReducerArgs) -> Result<Value>;

/// A registered reducer: identity + the execute function.
#[derive(Debug, Clone)]
pub struct ReducerDefinition {
    id: ReducerId,
    name: String,
    execute: ReducerFn,
}

impl ReducerDefinition {
    /// Creates a definition with a stable numeric id and a registry-unique
    /// name. The name must not be empty.
    pub fn new(id: ReducerId, name: impl Into<String>, execute: ReducerFn) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::invalid_argument("reducer name must not be empty"));
        }
        Ok(Self { id, name, execute })
    }

    /// Returns the stable numeric id.
    pub fn id(&self) -> ReducerId {
        self.id
    }

    /// Returns the registry-unique name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the execute function.
    pub fn execute(&self) -> ReducerFn {
        self.execute
    }
}
