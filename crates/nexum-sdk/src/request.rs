//! Request correlation (ADR-013): pending requests and correlated results.
//!
//! Reducer calls and subscriptions are correlated by a monotonically
//! allocated client `request_id` echoed by the server. The client never
//! reuses an id while its request is pending, so a result can always be
//! matched exactly.

use nexum_core::Value;

/// A reducer call awaiting its result.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingCall {
    request_id: u64,
    reducer: String,
}

impl PendingCall {
    /// Builds a pending call record.
    pub(crate) fn new(request_id: u64, reducer: &str) -> Self {
        Self {
            request_id,
            reducer: reducer.to_string(),
        }
    }

    /// Returns the correlation id.
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns the reducer name invoked.
    pub fn reducer(&self) -> &str {
        &self.reducer
    }
}

/// The outcome of one reducer call, correlated by `request_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerResult {
    request_id: u64,
    ok: bool,
    value: Option<Value>,
    error: Option<String>,
}

impl ReducerResult {
    /// Builds a result from its parts.
    pub(crate) fn new(
        request_id: u64,
        ok: bool,
        value: Option<Value>,
        error: Option<String>,
    ) -> Self {
        Self {
            request_id,
            ok,
            value,
            error,
        }
    }

    /// Returns the correlation id.
    pub fn request_id(&self) -> u64 {
        self.request_id
    }

    /// Returns `true` when the call committed with the tick.
    pub fn is_ok(&self) -> bool {
        self.ok
    }

    /// Returns the reducer's return value on success.
    pub fn value(&self) -> Option<&Value> {
        self.value.as_ref()
    }

    /// Returns the error message on failure.
    pub fn error(&self) -> Option<&str> {
        self.error.as_deref()
    }
}
