//! Deterministic simulation input ([`InputFrame`], [`InputCommand`]).
//!
//! Inputs are protocol-independent: the same frame shape serves future
//! player commands, server commands, simulation events, and tests. A frame
//! is processed in command order, and a frame is only ever applied to the
//! tick it names — the world rejects a mismatched frame before consuming
//! anything (ADR-009 D6).

use nexum_core::{Error, Result, TickId, Value};

/// One command inside an [`InputFrame`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputCommand {
    source: u64,
    kind: String,
    payload: Option<Value>,
}

impl InputCommand {
    /// Creates a command. `kind` must not be empty.
    pub fn new(source: u64, kind: impl Into<String>, payload: Option<Value>) -> Result<Self> {
        let kind = kind.into();
        if kind.is_empty() {
            return Err(Error::invalid_argument(
                "input command kind must not be empty",
            ));
        }
        Ok(Self {
            source,
            kind,
            payload,
        })
    }

    /// Creates a command without a payload. `kind` must not be empty.
    pub fn simple(source: u64, kind: impl Into<String>) -> Result<Self> {
        Self::new(source, kind, None)
    }

    /// Returns the command source (e.g. a player/entity id).
    pub fn source(&self) -> u64 {
        self.source
    }

    /// Returns the command kind (the command's name).
    pub fn kind(&self) -> &str {
        &self.kind
    }

    /// Returns the optional payload.
    pub fn payload(&self) -> Option<&Value> {
        self.payload.as_ref()
    }
}

/// The deterministic input of one tick: commands processed in frame order.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InputFrame {
    tick: TickId,
    commands: Vec<InputCommand>,
}

impl InputFrame {
    /// Creates an empty frame for `tick`.
    pub fn new(tick: TickId) -> Self {
        Self {
            tick,
            commands: Vec::new(),
        }
    }

    /// Creates an empty frame for `tick` with capacity for `cap` commands.
    pub fn with_capacity(tick: TickId, cap: usize) -> Self {
        Self {
            tick,
            commands: Vec::with_capacity(cap),
        }
    }

    /// Appends a command. Commands are processed in the appended order.
    pub fn push(&mut self, command: InputCommand) {
        self.commands.push(command);
    }

    /// Returns the tick this frame belongs to.
    pub fn tick(&self) -> TickId {
        self.tick
    }

    /// Returns the commands in frame order.
    pub fn commands(&self) -> &[InputCommand] {
        &self.commands
    }

    /// Returns the number of commands.
    pub fn len(&self) -> usize {
        self.commands.len()
    }

    /// Returns `true` if the frame carries no commands.
    pub fn is_empty(&self) -> bool {
        self.commands.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commands_roundtrip() {
        let command = InputCommand::new(7, "move", Some(Value::U64(3))).unwrap();
        assert_eq!(command.source(), 7);
        assert_eq!(command.kind(), "move");
        assert_eq!(command.payload(), Some(&Value::U64(3)));
    }

    #[test]
    fn empty_kind_is_rejected() {
        assert!(InputCommand::new(0, "", None).is_err());
    }

    #[test]
    fn frame_preserves_command_order() {
        let mut frame = InputFrame::new(TickId::from_u64(5));
        frame.push(InputCommand::simple(1, "a").unwrap());
        frame.push(InputCommand::simple(2, "b").unwrap());
        frame.push(InputCommand::simple(3, "c").unwrap());
        let kinds: Vec<&str> = frame.commands().iter().map(InputCommand::kind).collect();
        assert_eq!(kinds, vec!["a", "b", "c"]);
        assert_eq!(frame.tick(), TickId::from_u64(5));
        assert_eq!(frame.len(), 3);
        assert!(!frame.is_empty());
    }
}
