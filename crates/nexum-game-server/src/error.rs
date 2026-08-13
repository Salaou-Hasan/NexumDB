//! Game server errors (ADR-014).
//!
//! Lower-level errors keep their identity: `Runtime(RuntimeError)` and
//! `Core(Error)` wrap the existing models rather than collapsing them.

use std::fmt;

use nexum_core::{Error, GameInstanceId, PlayerId, WorldId};

/// A game-server operation error.
//
// Variant payloads are self-documenting (`game`, `player`, `world`, ...),
// so the enum carries `allow(missing_docs)`.
#[derive(Debug)]
#[allow(missing_docs)]
pub enum GameServerError {
    /// An invalid configuration value.
    InvalidConfig(String),
    /// The game instance does not exist.
    UnknownGame(GameInstanceId),
    /// The game instance already exists.
    DuplicateGame(GameInstanceId),
    /// The player does not exist.
    UnknownPlayer(PlayerId),
    /// The operation requires the game to be running.
    GameNotRunning(GameInstanceId),
    /// The game is stopped (worlds retained).
    GameStopped(GameInstanceId),
    /// The game failed (authoritative worlds unavailable).
    GameFailed(GameInstanceId),
    /// A lifecycle transition was invalid from the current state.
    InvalidTransition { game: GameInstanceId, detail: String },
    /// The game is at capacity.
    GameFull { game: GameInstanceId, max: usize },
    /// The player is not in an actable state.
    PlayerNotActive(PlayerId),
    /// The player does not belong to the named game.
    PlayerNotInGame { game: GameInstanceId, player: PlayerId },
    /// The player already has a membership in the named game.
    PlayerAlreadyInGame { game: GameInstanceId, player: PlayerId },
    /// The player's world is not running.
    WorldFailed(WorldId),
    /// The world does not exist.
    UnknownWorld(WorldId),
    /// The reducer is not registered.
    UnknownReducer(String),
    /// An authorization denial.
    NotAuthorized(String),
    /// A capacity bound was exceeded.
    Capacity(String),
    /// The per-world pending command buffer is full (ADR-014 D3): the
    /// command was rejected explicitly, never silently dropped.
    CommandBufferFull(WorldId),
    /// The per-player subscription limit was reached.
    SubscriptionLimit { player: PlayerId, limit: usize },
    /// The underlying network layer failed.
    Network(nexum_network::NetworkError),
    /// The underlying runtime failed.
    Runtime(nexum_runtime::RuntimeError),
    /// A core error.
    Core(Error),
    /// An unexpected internal failure.
    Internal(String),
}

impl fmt::Display for GameServerError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidConfig(detail) => write!(f, "invalid configuration: {detail}"),
            Self::UnknownGame(game) => write!(f, "unknown game {game}"),
            Self::DuplicateGame(game) => write!(f, "game {game} already exists"),
            Self::UnknownPlayer(player) => write!(f, "unknown player {player}"),
            Self::GameNotRunning(game) => write!(f, "game {game} is not running"),
            Self::GameStopped(game) => write!(f, "game {game} is stopped"),
            Self::GameFailed(game) => write!(f, "game {game} failed"),
            Self::InvalidTransition { game, detail } => {
                write!(f, "invalid transition for game {game}: {detail}")
            }
            Self::GameFull { game, max } => {
                write!(f, "game {game} is full (capacity {max})")
            }
            Self::PlayerNotActive(player) => write!(f, "player {player} is not active"),
            Self::PlayerNotInGame { game, player } => {
                write!(f, "player {player} is not in game {game}")
            }
            Self::PlayerAlreadyInGame { game, player } => {
                write!(f, "player {player} already has a membership in game {game}")
            }
            Self::WorldFailed(world) => write!(f, "world {world} is not running"),
            Self::UnknownWorld(world) => write!(f, "unknown world {world}"),
            Self::UnknownReducer(reducer) => write!(f, "unknown reducer '{reducer}'"),
            Self::NotAuthorized(detail) => write!(f, "not authorized: {detail}"),
            Self::Capacity(detail) => write!(f, "capacity exceeded: {detail}"),
            Self::CommandBufferFull(world) => write!(f, "pending command buffer full for world {world}"),
            Self::SubscriptionLimit { player, limit } => {
                write!(f, "player {player} reached the subscription limit ({limit})")
            }
            Self::Network(error) => write!(f, "network: {error}"),
            Self::Runtime(error) => write!(f, "runtime: {error}"),
            Self::Core(error) => write!(f, "core: {error}"),
            Self::Internal(detail) => write!(f, "internal: {detail}"),
        }
    }
}

impl std::error::Error for GameServerError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Network(error) => Some(error),
            Self::Runtime(error) => Some(error),
            Self::Core(error) => Some(error),
            _ => None,
        }
    }
}

impl From<nexum_runtime::RuntimeError> for GameServerError {
    fn from(error: nexum_runtime::RuntimeError) -> Self {
        Self::Runtime(error)
    }
}

impl From<Error> for GameServerError {
    fn from(error: Error) -> Self {
        Self::Core(error)
    }
}
