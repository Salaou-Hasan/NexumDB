//! Application events emitted by reducers ([`ReducerEvent`]).
//!
//! An event is a `(name, payload)` pair buffered **transaction-locally** by
//! the [`crate::ReducerContext`] (ADR-006 D5). Events only escape with a
//! successful commit, in `emit` order; a failed, conflicted, or panicking
//! reducer invocation discards its entire buffer. The payload is a single
//! [`Value`]; richer structured payloads can evolve without changing the
//! buffering mechanics.

use nexum_core::Value;

/// One application-level event emitted by a reducer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerEvent {
    name: String,
    payload: Value,
}

impl ReducerEvent {
    /// Creates an event with `name` and `payload`.
    pub fn new(name: impl Into<String>, payload: impl Into<Value>) -> Self {
        Self {
            name: name.into(),
            payload: payload.into(),
        }
    }

    /// Returns the event name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the event payload.
    pub fn payload(&self) -> &Value {
        &self.payload
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn carries_name_and_payload() {
        let event = ReducerEvent::new("player_joined", 42u64);
        assert_eq!(event.name(), "player_joined");
        assert_eq!(event.payload(), &Value::U64(42));
    }

    #[test]
    fn events_compare_by_content() {
        let a = ReducerEvent::new("ping", "pong");
        let b = ReducerEvent::new("ping", "pong");
        assert_eq!(a, b);
        assert_ne!(a, ReducerEvent::new("ping", "other"));
    }
}
