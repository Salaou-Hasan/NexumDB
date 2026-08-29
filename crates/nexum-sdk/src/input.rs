//! Client inputs (ADR-013): building input frames and routing them to the
//! attached world.
//!
//! The client never mutates state directly — an input frame is routed
//! through the network gateway into the world's next tick, and the server
//! stamps every command's source with the authenticated principal
//! (client-supplied sources are ignored, so identity can never be forged).

use nexum_core::{Result, TickId};
use nexum_execution::{InputCommand, InputFrame};

use crate::client::Client;
use crate::error::SdkError;
use crate::protocol::ClientMessage;

/// Creates an empty input frame for `tick`.
pub fn input_frame(tick: TickId) -> InputFrame {
    InputFrame::new(tick)
}

/// Builds one input command with a placeholder source (the server stamps
/// the authoritative principal source).
pub fn command(kind: &str, payload: Option<nexum_core::Value>) -> Result<InputCommand> {
    InputCommand::new(0, kind, payload)
}

/// Builds one input command for an explicit source (used by servers/tools;
/// the gateway overwrites it for client traffic).
pub fn command_for(
    source: u64,
    kind: &str,
    payload: Option<nexum_core::Value>,
) -> Result<InputCommand> {
    InputCommand::new(source, kind, payload)
}

/// Builds a frame for `tick` from `(source, kind, payload)` triples,
/// rejecting an empty or otherwise invalid command.
pub fn frame_with(
    tick: TickId,
    commands: &[(u64, &str, Option<nexum_core::Value>)],
) -> Result<InputFrame> {
    let mut frame = InputFrame::with_capacity(tick, commands.len());
    for (source, kind, payload) in commands {
        frame.push(InputCommand::new(*source, *kind, payload.clone())?);
    }
    Ok(frame)
}

impl Client {
    /// Routes one input frame to the session's attached world. The frame's
    /// tick must be the world's next tick (late/duplicate frames are
    /// rejected by the runtime); command sources are overwritten by the
    /// server with the authenticated principal.
    pub fn send_input(&mut self, frame: InputFrame) -> std::result::Result<(), SdkError> {
        self.require_attached()?;
        if frame.commands().len() > self.config.max_commands_per_frame() {
            return Err(SdkError::InvalidArgument(format!(
                "input frame has {} commands, exceeding the configured limit of {}",
                frame.commands().len(),
                self.config.max_commands_per_frame()
            )));
        }
        self.send_message(&ClientMessage::InputFrame { frame })
    }
}
