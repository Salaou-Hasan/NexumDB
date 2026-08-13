//! Lifecycle state machines (ADR-014 D6/D8).
//!
//! Game and player lifecycles are orchestration metadata. Their transitions
//! are validated explicitly; invalid transitions are rejected with an error
//! rather than silently accepted.

use nexum_core::{GameInstanceId, PartitionId, PlayerId, WorldId};

/// The lifecycle state of a game instance.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GameLifecycle {
    /// Worlds created but not yet ticking.
    Created,
    /// Transitioning to running.
    Starting,
    /// Worlds ticking.
    Running,
    /// Transitioning to stopped.
    Stopping,
    /// Worlds created but not ticking (state retained).
    Stopped,
    /// One or more authoritative partitions failed; the game cannot run.
    Failed,
    /// Terminal; the game record was removed.
    Destroyed,
}

impl GameLifecycle {
    /// Whether the game may be started from this state.
    pub fn can_start(self) -> bool {
        matches!(self, Self::Created | Self::Stopped)
    }

    /// Whether the game may be stopped from this state.
    pub fn can_stop(self) -> bool {
        matches!(self, Self::Created | Self::Running)
    }

    /// Whether the game is currently running (worlds ticking).
    pub fn is_running(self) -> bool {
        self == Self::Running
    }

    /// A stable human-readable label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Created => "created",
            Self::Starting => "starting",
            Self::Running => "running",
            Self::Stopping => "stopping",
            Self::Stopped => "stopped",
            Self::Failed => "failed",
            Self::Destroyed => "destroyed",
        }
    }
}

/// The lifecycle state of a player membership in a game.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PlayerState {
    /// Joining: membership registered, authoritative init pending.
    Joining,
    /// The player is present and may act in the simulation.
    Active,
    /// The player's connection dropped; membership is retained and a later
    /// join with the same principal restores it.
    Reconnecting,
    /// The player left; the membership is terminal (a later join is fresh).
    Left,
}

impl PlayerState {
    /// A stable human-readable label.
    pub fn label(self) -> &'static str {
        match self {
            Self::Joining => "joining",
            Self::Active => "active",
            Self::Reconnecting => "reconnecting",
            Self::Left => "left",
        }
    }
}

/// The lifecycle state of one partition of a game.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PartitionState {
    /// The partition's world is ticking.
    Running,
    /// The world was stopped.
    Stopped,
    /// The world failed.
    Failed,
    /// The world was recovered after a failure.
    Recovered,
}

/// The outcome of [`GameServer::join_game`](crate::GameServer::join_game).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum JoinOutcome {
    /// A brand-new membership was created.
    Joined,
    /// An existing membership was restored (same principal, same player).
    Reconnected,
}

/// A point-in-time view of a game instance.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GameStatus {
    /// The game instance id.
    pub id: GameInstanceId,
    /// The lifecycle state.
    pub lifecycle: GameLifecycle,
    /// The game type label.
    pub game_type: String,
    /// Present (non-left) players.
    pub players: usize,
    /// The configured player capacity.
    pub max_players: usize,
    /// The number of authoritative partitions.
    pub partitions: usize,
    /// Partitions whose world is failed.
    pub failed_partitions: usize,
}

/// A point-in-time view of a player membership.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlayerStatus {
    /// The player id (equal to the authenticated principal id).
    pub id: PlayerId,
    /// The authenticated principal id.
    pub principal: u64,
    /// The game the player belongs to.
    pub game: GameInstanceId,
    /// The partition the player is routed to.
    pub partition: PartitionId,
    /// The authoritative world of that partition.
    pub world: WorldId,
    /// The membership state.
    pub state: PlayerState,
}
