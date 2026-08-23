//! Game server orchestration events (ADR-014 §Observability).
//!
//! These are **operational** events about game/player/partition lifecycle.
//! They are deliberately distinct from the gameplay domains: `ReducerEvent`
//! and `Vec<Change>` (committed simulation output) and `SubscriptionUpdate`
//! (observation). The Game Server never fabricates gameplay events.

use nexum_core::{GameInstanceId, PartitionId, PlayerId, WorldId};

/// One game-server orchestration event.
//
// Variant payloads are self-documenting (`game`, `player`, `world`, ...),
// so the enum carries `allow(missing_docs)`.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(missing_docs)]
pub enum GameServerEvent {
    /// A game instance was created (worlds allocated).
    GameCreated { game: GameInstanceId },
    /// A game instance started ticking.
    GameStarted { game: GameInstanceId },
    /// A game instance began stopping.
    GameStopping { game: GameInstanceId },
    /// A game instance stopped (worlds retained).
    GameStopped { game: GameInstanceId },
    /// A game instance was destroyed (records and worlds removed).
    GameDestroyed { game: GameInstanceId },
    /// Every partition of a game failed; the game cannot run.
    GameFailed {
        game: GameInstanceId,
        reason: String,
    },
    /// A game instance was reconstructed from persisted state.
    GameRecovered {
        game: GameInstanceId,
        replayed_txs: usize,
    },
    /// A player joined (or reconnected) a game.
    PlayerJoined {
        game: GameInstanceId,
        player: PlayerId,
        world: WorldId,
        reconnected: bool,
    },
    /// A player's connection dropped; membership retained.
    PlayerDisconnected {
        game: GameInstanceId,
        player: PlayerId,
    },
    /// A player left a game.
    PlayerLeft {
        game: GameInstanceId,
        player: PlayerId,
    },
    /// A partition was bound to a game.
    PartitionAssigned {
        game: GameInstanceId,
        partition: PartitionId,
        world: WorldId,
    },
    /// A partition's world failed.
    PartitionFailed {
        game: GameInstanceId,
        partition: PartitionId,
        world: WorldId,
        reason: String,
    },
    /// A partition's world was recovered.
    PartitionRecovered {
        game: GameInstanceId,
        partition: PartitionId,
        world: WorldId,
    },
    /// A server-side command was rejected.
    CommandRejected { player: PlayerId, reason: String },
    /// A reducer call was rejected.
    ReducerRejected {
        player: PlayerId,
        reducer: String,
        reason: String,
    },
    /// A world tick failed (zero authoritative mutation).
    TickFailed { world: WorldId },
    /// A reducer became client-callable.
    ReducerExposed { reducer: String },
    /// A reducer was revoked (no longer client-callable).
    ReducerRevoked { reducer: String },
}
