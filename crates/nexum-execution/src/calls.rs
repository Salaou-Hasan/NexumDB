//! Client reducer calls (ADR-013 D3): server API operations executed inside
//! the world's next tick.
//!
//! A [`ReducerCall`] is a request from the network layer (correlated by a
//! client-allocated `request_id`) to invoke a registered reducer against the
//! authoritative state. It is queued by the runtime (bounded) and executed
//! by [`Partition::tick_with_calls`](crate::Partition::tick_with_calls) in Phase 0c —
//! after delivered cross-partition messages and scheduled events, before
//! systems — so reducer calls observe (and are observed by) the same tick
//! transaction as everything else.
//!
//! Each call runs against a **branch** of the tick transaction (Phase 11
//! `branch_of`/`absorb`): a successful call absorbs into the tick
//! transaction — its writes and events commit atomically with the tick — and
//! a failed call discards its branch (zero mutation, zero events) while the
//! tick continues. The per-call outcome is recorded in
//! [`ReducerCallResult`] and delivered to the caller by the network layer;
//! a failed tick answers every still-pending call of its world with the tick
//! error.

use nexum_core::{Error, Result, Value};
use nexum_reducer::ReducerArgs;

/// One client reducer-call request (ADR-013 D3).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerCall {
    request_id: u64,
    reducer: String,
    args: ReducerArgs,
}

impl ReducerCall {
    /// Builds a call. `reducer` must not be empty.
    pub fn new(request_id: u64, reducer: impl Into<String>, args: ReducerArgs) -> Result<Self> {
        let reducer = reducer.into();
        if reducer.is_empty() {
            return Err(Error::invalid_argument(
                "reducer call name must not be empty",
            ));
        }
        Ok(Self {
            request_id,
            reducer,
            args,
        })
    }

    /// Returns the client-allocated correlation id.
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the reducer name to invoke.
    pub fn reducer(&self) -> &str {
        &self.reducer
    }

    /// Returns the reducer arguments.
    pub fn args(&self) -> &ReducerArgs {
        &self.args
    }
}

/// The outcome of one reducer call executed during a tick (ADR-013 D3).
///
/// `ok` is `true` with the reducer's return `value` when the call committed
/// with the tick; `ok` is `false` with the underlying `error` when the call
/// failed (rejected, invalid arguments, not found, or its own conflict).
/// Either way the tick itself continues unless another phase fails it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerCallResult {
    request_id: u64,
    ok: bool,
    value: Option<Value>,
    error: Option<Error>,
}

impl ReducerCallResult {
    /// Builds a successful result carrying `value`.
    pub fn ok(request_id: u64, value: Value) -> Self {
        Self {
            request_id,
            ok: true,
            value: Some(value),
            error: None,
        }
    }

    /// Builds a failed result carrying `error`.
    pub fn err(request_id: u64, error: Error) -> Self {
        Self {
            request_id,
            ok: false,
            value: None,
            error: Some(error),
        }
    }

    /// Returns the correlated request id.
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns `true` when the call succeeded and committed with the tick.
    pub fn is_ok(&self) -> bool {
        self.ok
    }

    /// Returns the reducer's return value on success.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns the underlying error on failure.
    pub fn error(&self) -> Option<&Error> {
        self.error.as_ref()
    }
}
