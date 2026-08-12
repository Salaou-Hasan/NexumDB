//! The deterministic cross-partition message envelope (ADR-012 D2, D5).
//!
//! A [`PartitionMessage`] is the **only** cross-partition interaction. It is
//! produced inside a tick by [`SimulationContext::send_to`], committed with
//! the tick in `TickResult.outbound`, and delivered by the runtime to the
//! destination's next tick, where it invokes a registered handler reducer
//! named by `kind`.
//!
//! Every field is deterministic: `payload` is [`ReducerArgs`] (a
//! `BTreeMap`, key-sorted), and `seq` is the sender's outbound index within
//! its tick. Delivery order is a pure function of the batch — the world
//! sorts by `(sent_tick, from, seq)`.

use nexum_core::{Error, PartitionId, Result, TickId};
use nexum_reducer::ReducerArgs;

/// One deterministic cross-partition message (ADR-012 D2).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMessage {
    from: PartitionId,
    to: PartitionId,
    sent_tick: TickId,
    seq: u64,
    kind: String,
    payload: ReducerArgs,
}

impl PartitionMessage {
    /// Builds a message. `kind` must be non-empty. Used by the simulation
    /// engine (`send_to`) and by the runtime (external injection).
    pub fn new(
        from: PartitionId,
        to: PartitionId,
        sent_tick: TickId,
        seq: u64,
        kind: String,
        payload: ReducerArgs,
    ) -> Result<Self> {
        if kind.is_empty() {
            return Err(Error::invalid_argument(
                "partition message kind must not be empty",
            ));
        }
        Ok(Self {
            from,
            to,
            sent_tick,
            seq,
            kind,
            payload,
        })
    }

    /// Returns the sending partition.
    pub fn from(&self) -> PartitionId {
        self.from
    }

    /// Returns the destination partition.
    pub fn to(&self) -> PartitionId {
        self.to
    }

    /// Returns the tick the message was sent from (logical time).
    pub fn sent_tick(&self) -> TickId {
        self.sent_tick
    }

    /// Returns the sender's outbound index within its tick.
    pub fn seq(&self) -> u64 {
        self.seq
    }

    /// Returns the message kind (the destination's handler reducer name).
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the message payload.
    pub fn payload(&self) -> &ReducerArgs {
        &self.payload
    }

    /// Renumbers this message's `seq` (used at the Phase 11 parallel merge
    /// so the committed outbound trace reproduces the serial trace exactly:
    /// serial seqs are global positions, `0..n`).
    pub(crate) fn set_seq(&mut self, seq: u64) {
        self.seq = seq;
    }
}
