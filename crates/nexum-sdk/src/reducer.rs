//! Server reducer calls (ADR-013): the client API for invoking reducers on
//! the attached world.
//!
//! A reducer invocation is a **server API operation**: the call executes
//! inside the world's next tick against a branch of the tick transaction,
//! and its outcome is correlated back by `request_id`. The client never
//! touches `Transaction`, OCC, or storage — it sends a [`CallReducer`]
//! message and receives exactly one [`ReducerResult`].

use nexum_reducer::ReducerArgs;

use crate::client::Client;
use crate::error::SdkError;
use crate::protocol::ClientMessage;
use crate::request::{PendingCall, ReducerResult};

impl Client {
    /// Invokes `reducer` with `args` on the attached world's next tick.
    ///
    /// Returns the `request_id` used to correlate the eventual
    /// [`ReducerResult`] (collected with [`Client::take_reducer_results`]).
    /// Fails locally when not attached, when the pending-call limit is
    /// reached, or when the frame cannot be encoded. The server rejects
    /// unknown reducers and malformed arguments with a correlated
    /// `ReducerResult` — never a hang.
    pub fn call_reducer(&mut self, reducer: &str, args: ReducerArgs) -> Result<u64, SdkError> {
        self.require_attached()?;
        if reducer.is_empty() {
            return Err(SdkError::InvalidArgument(
                "reducer name must not be empty".to_string(),
            ));
        }
        if self.pending_calls.len() >= self.config.max_pending_calls() {
            return Err(SdkError::PendingCallLimit);
        }
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        self.send_message(&ClientMessage::CallReducer {
            request_id,
            reducer: reducer.to_string(),
            args,
        })?;
        self.pending_calls
            .insert(request_id, PendingCall::new(request_id, reducer));
        Ok(request_id)
    }

    /// Takes every correlated reducer result received so far, in arrival
    /// order.
    pub fn take_reducer_results(&mut self) -> Vec<ReducerResult> {
        self.reducer_results.drain(..).collect()
    }

    /// Returns the number of reducer calls still awaiting a result.
    pub fn pending_call_count(&self) -> usize {
        self.pending_calls.len()
    }

    /// Returns the pending call for `request_id`, if any.
    pub fn pending_call(&self, request_id: u64) -> Option<&PendingCall> {
        self.pending_calls.get(&request_id)
    }
}
