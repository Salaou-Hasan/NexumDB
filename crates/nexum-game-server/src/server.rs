//! The [`GameServer`]: the orchestration/product layer (ADR-014).
//!
//! The server owns a [`NetworkGateway`], which owns the [`Runtime`]. Every
//! authoritative operation flows through the runtime boundary — inputs and
//! reducer calls terminate in `World::tick`; there is no alternative commit
//! path. The gateway's authorization policy is this server's live
//! [`GamePolicyTable`], installed automatically at construction.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::sync::{Arc, Mutex};

use nexum_core::{GameInstanceId, PartitionId, PlayerId, TickId, Value, WorldId};
use nexum_network::{Authenticator, NetworkConfig, NetworkGateway, Principal, SERVER_REQUEST_MSB};
use nexum_reducer::ReducerArgs;
use nexum_runtime::{Runtime, RuntimeEvent, WorldLifecycle};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, TickResult};
use nexum_subscription::{Query, SubscriptionId};
use nexum_wal::RecoveryReport;

use crate::config::{GameInstanceConfig, GameServerConfig};
use crate::error::GameServerError;
use crate::events::GameServerEvent;
use crate::lifecycle::{
    GameLifecycle, GameStatus, JoinOutcome, PartitionState, PlayerState, PlayerStatus,
};
use crate::metrics::GameServerMetrics;
use crate::policy::{GamePolicyTable, PolicyHandle, ReducerExposure, Role};

/// A second reserved bit for server-originated join calls (ADR-014 D3):
/// `invoke_reducer` ids are `SERVER_REQUEST_MSB | counter` (the counter
/// never sets bit 62), while join ids are `SERVER_REQUEST_MSB |
/// SERVER_JOIN_MSB | player_id`. The two server request-id spaces are
/// therefore disjoint, and both are disjoint from client ids (the gateway
/// rejects any id with `SERVER_REQUEST_MSB` set).
const SERVER_JOIN_MSB: u64 = 1 << 62;

/// One authoritative partition of a game instance.
#[derive(Debug)]
pub(crate) struct GamePartition {
    partition: PartitionId,
    world: WorldId,
    state: PartitionState,
}

/// A game instance: orchestration metadata only (ADR-014 D4).
#[derive(Debug)]
pub(crate) struct GameInstance {
    config: GameInstanceConfig,
    lifecycle: GameLifecycle,
    partitions: Vec<GamePartition>,
}

/// A player membership (orchestration metadata only).
#[derive(Debug)]
pub(crate) struct PlayerRecord {
    id: PlayerId,
    principal: u64,
    game: GameInstanceId,
    partition: PartitionId,
    world: WorldId,
    state: PlayerState,
}

/// The aggregate report of [`GameServer::recover_game`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct GameRecoveryReport {
    /// Transactions replayed across all recovered partitions.
    pub replayed_txs: usize,
    /// Changes replayed across all recovered partitions.
    pub replayed_changes: usize,
    /// Partitions (worlds) recovered.
    pub partitions: usize,
}

/// The game server (ADR-014).
pub struct GameServer {
    gateway: NetworkGateway,
    config: GameServerConfig,
    /// The shared authorization table (writer here, reader in the gateway).
    policy: Arc<Mutex<GamePolicyTable>>,
    games: BTreeMap<GameInstanceId, GameInstance>,
    players: BTreeMap<PlayerId, PlayerRecord>,
    /// `(principal id, game)` → player id for membership lookup.
    player_by_principal: BTreeMap<(u64, GameInstanceId), PlayerId>,
    /// world id → owning game, for failure observation.
    world_to_game: BTreeMap<WorldId, GameInstanceId>,
    /// Per-player server-side subscriptions (bounded).
    player_subs: BTreeMap<PlayerId, BTreeSet<SubscriptionId>>,
    /// Commands accepted between ticks, per world, in submission order
    /// (ADR-014 D3). `step()` merges them into one frame per world so a
    /// burst of commands can never stamp multiple frames with the same tick.
    pending_commands: BTreeMap<WorldId, Vec<InputCommand>>,
    next_game: u64,
    next_world: u64,
    next_partition: u64,
    next_request: u64,
    events: VecDeque<GameServerEvent>,
    metrics: GameServerMetrics,
}

impl GameServer {
    /// Creates a game server owning `runtime` (via an internal gateway) with
    /// `network_config` bounds, the `authenticator` identity hook, and the
    /// validated `config`. The gateway is configured with this server's live
    /// authorization policy immediately.
    pub fn new(
        runtime: Runtime,
        network_config: NetworkConfig,
        authenticator: Arc<dyn Authenticator>,
        config: GameServerConfig,
    ) -> Result<Self, GameServerError> {
        config.validate().map_err(GameServerError::InvalidConfig)?;
        let policy = Arc::new(Mutex::new(GamePolicyTable::new()));
        let mut gateway = NetworkGateway::new(runtime, network_config, authenticator)
            .map_err(GameServerError::Network)?;
        gateway.set_policy(Box::new(PolicyHandle::new(Arc::clone(&policy))));
        Ok(Self {
            gateway,
            config,
            policy,
            games: BTreeMap::new(),
            players: BTreeMap::new(),
            player_by_principal: BTreeMap::new(),
            world_to_game: BTreeMap::new(),
            player_subs: BTreeMap::new(),
            pending_commands: BTreeMap::new(),
            next_game: 0,
            next_world: 0,
            next_partition: 0,
            next_request: 0,
            events: VecDeque::new(),
            metrics: GameServerMetrics::default(),
        })
    }

    /// The underlying network gateway (connection registration, inbound
    /// processing, subscription pumping, network events/metrics).
    pub fn gateway(&self) -> &NetworkGateway {
        &self.gateway
    }

    /// Mutable access to the network gateway.
    pub fn gateway_mut(&mut self) -> &mut NetworkGateway {
        &mut self.gateway
    }

    /// The owned runtime (shared access).
    pub fn runtime(&self) -> &Runtime {
        self.gateway.runtime()
    }

    /// The owned runtime (mutable access).
    pub fn runtime_mut(&mut self) -> &mut Runtime {
        self.gateway.runtime_mut()
    }

    /// The validated server configuration.
    pub fn config(&self) -> &GameServerConfig {
        &self.config
    }

    /// A handle into the live authorization policy (installable on any
    /// gateway, e.g. after cloning a policy table).
    pub fn policy_handle(&self) -> PolicyHandle {
        PolicyHandle::new(Arc::clone(&self.policy))
    }

    // ------------------------------------------------------- game lifecycle

    /// Creates a game instance: allocates `partition_count` worlds in the
    /// runtime and binds one partition to each (Phase 12). The game starts
    /// `Created`; call [`start_game`](Self::start_game) to run it.
    pub fn create_game(
        &mut self,
        config: GameInstanceConfig,
    ) -> Result<GameInstanceId, GameServerError> {
        config.validate(&self.config).map_err(GameServerError::InvalidConfig)?;
        let id = GameInstanceId::from_u64(self.next_game);
        self.next_game += 1;
        if self.games.contains_key(&id) {
            return Err(GameServerError::DuplicateGame(id));
        }
        let mut partitions = Vec::with_capacity(config.partition_count);
        for _ in 0..config.partition_count {
            let world = WorldId::from_u64(self.next_world);
            self.next_world += 1;
            let partition = PartitionId::from_u64(self.next_partition);
            self.next_partition += 1;
            let sim = SimulationConfig::new().with_seed(config.world_seed);
            self.runtime_mut()
                .create_world(world, sim)
                .map_err(GameServerError::Runtime)?;
            self.runtime_mut()
                .register_partition(partition, world)
                .map_err(GameServerError::Runtime)?;
            self.world_to_game.insert(world, id);
            partitions.push(GamePartition {
                partition,
                world,
                state: PartitionState::Running,
            });
        }
        self.games.insert(
            id,
            GameInstance {
                config,
                lifecycle: GameLifecycle::Created,
                partitions,
            },
        );
        self.metrics.games_created += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameCreated { game: id },
        );
        Ok(id)
    }

    /// Starts a `Created` or `Stopped` game: every partition's world starts
    /// ticking (idempotent when already running).
    pub fn start_game(&mut self, game_id: GameInstanceId) -> Result<(), GameServerError> {
        let worlds = {
            let game = self.games.get_mut(&game_id).ok_or(GameServerError::UnknownGame(game_id))?;
            if !game.lifecycle.can_start() {
                return Err(GameServerError::InvalidTransition {
                    game: game_id,
                    detail: format!("cannot start from {:?}", game.lifecycle),
                });
            }
            game.lifecycle = GameLifecycle::Starting;
            game.partitions.iter().map(|partition| partition.world).collect::<Vec<_>>()
        };
        for world in worlds {
            self.runtime_mut().start_world(world).map_err(GameServerError::Runtime)?;
        }
        let game = self.games.get_mut(&game_id).expect("game exists");
        game.lifecycle = GameLifecycle::Running;
        for partition in &mut game.partitions {
            partition.state = PartitionState::Running;
        }
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameStarted { game: game_id },
        );
        Ok(())
    }

    /// Stops a `Created` or `Running` game: worlds stop ticking but retain
    /// their state (idempotent when already stopped).
    pub fn stop_game(&mut self, game_id: GameInstanceId) -> Result<(), GameServerError> {
        let worlds = {
            let game = self.games.get_mut(&game_id).ok_or(GameServerError::UnknownGame(game_id))?;
            if !game.lifecycle.can_stop() {
                return Err(GameServerError::InvalidTransition {
                    game: game_id,
                    detail: format!("cannot stop from {:?}", game.lifecycle),
                });
            }
            game.lifecycle = GameLifecycle::Stopping;
            game.partitions.iter().map(|partition| partition.world).collect::<Vec<_>>()
        };
        for world in &worlds {
            self.runtime_mut().stop_world(*world).map_err(GameServerError::Runtime)?;
        }
        // Buffered commands for a stopped world can never execute: reject
        // them explicitly (ADR-014 D3) rather than silently dropping.
        for world in &worlds {
            if let Some(commands) = self.pending_commands.remove(world) {
                for command in commands {
                    self.metrics.commands_rejected += 1;
                    Self::push_event(
                        &mut self.events,
                        self.config.event_log_limit(),
                        GameServerEvent::CommandRejected {
                            player: PlayerId::from_u64(command.source()),
                            reason: "game stopped".to_string(),
                        },
                    );
                }
            }
        }
        let game = self.games.get_mut(&game_id).expect("game exists");
        game.lifecycle = GameLifecycle::Stopped;
        for partition in &mut game.partitions {
            partition.state = PartitionState::Stopped;
        }
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameStopping { game: game_id },
        );
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameStopped { game: game_id },
        );
        Ok(())
    }

    /// Destroys a game: removes the game record, its player memberships, and
    /// its worlds from the runtime. Committed data remains in each world's
    /// WAL on disk; nothing is silently erased.
    pub fn destroy_game(&mut self, game_id: GameInstanceId) -> Result<(), GameServerError> {
        let game = self.games.remove(&game_id).ok_or(GameServerError::UnknownGame(game_id))?;
        let worlds: Vec<WorldId> = game.partitions.iter().map(|partition| partition.world).collect();
        // Tear down memberships first: revoke active-input grants, drop
        // server-side subscriptions, and remove the records.
        let players: Vec<(PlayerId, u64, WorldId)> = self
            .players
            .values()
            .filter(|player| player.game == game_id)
            .map(|player| (player.id, player.principal, player.world))
            .collect();
        {
            let mut table = self.policy.lock().expect("game policy mutex is not poisoned");
            for (_, principal, world) in &players {
                table.remove_active_player(*principal, *world);
            }
        }
        for (player, _, world) in &players {
            if let Some(subs) = self.player_subs.remove(player) {
                for subscription in subs {
                    let _ = self.runtime_mut().unsubscribe(*world, subscription);
                }
            }
            self.players.remove(player);
            self.player_by_principal.remove(&(player.as_u64(), game_id));
        }
        for world in &worlds {
            self.world_to_game.remove(world);
        }
        for world in worlds {
            // Buffered commands die with the world: reject explicitly.
            if let Some(commands) = self.pending_commands.remove(&world) {
                for command in commands {
                    self.metrics.commands_rejected += 1;
                    Self::push_event(
                        &mut self.events,
                        self.config.event_log_limit(),
                        GameServerEvent::CommandRejected {
                            player: PlayerId::from_u64(command.source()),
                            reason: "game destroyed".to_string(),
                        },
                    );
                }
            }
            let _ = self.runtime_mut().destroy_world(world);
        }
        self.metrics.games_destroyed += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameDestroyed { game: game_id },
        );
        Ok(())
    }

    /// Reconstructs a game from persisted state (ADR-014 D6/D8): each
    /// partition's world is recovered with the Phase 5 engine through the
    /// runtime, partitions are re-registered, and the game is started.
    ///
    /// World/partition ids are a deterministic function of the operation
    /// sequence, so a host that replays the same creation order recovers the
    /// same ids. `resume_tick` continues each world's logical time.
    pub fn recover_game(
        &mut self,
        config: GameInstanceConfig,
        resume_tick: Option<TickId>,
    ) -> Result<(GameInstanceId, GameRecoveryReport), GameServerError> {
        config.validate(&self.config).map_err(GameServerError::InvalidConfig)?;
        let id = GameInstanceId::from_u64(self.next_game);
        self.next_game += 1;
        if self.games.contains_key(&id) {
            return Err(GameServerError::DuplicateGame(id));
        }
        let mut partitions = Vec::with_capacity(config.partition_count);
        let mut report = GameRecoveryReport {
            replayed_txs: 0,
            replayed_changes: 0,
            partitions: 0,
        };
        for _ in 0..config.partition_count {
            let world = WorldId::from_u64(self.next_world);
            self.next_world += 1;
            let partition = PartitionId::from_u64(self.next_partition);
            self.next_partition += 1;
            let sim = SimulationConfig::new().with_seed(config.world_seed);
            let recovery: RecoveryReport = self
                .runtime_mut()
                .recover_world(world, sim, resume_tick)
                .map_err(GameServerError::Runtime)?;
            report.replayed_txs += recovery.replayed_txs;
            report.replayed_changes += recovery.replayed_changes;
            report.partitions += 1;
            self.runtime_mut()
                .register_partition(partition, world)
                .map_err(GameServerError::Runtime)?;
            self.world_to_game.insert(world, id);
            partitions.push(GamePartition {
                partition,
                world,
                state: PartitionState::Recovered,
            });
        }
        self.games.insert(
            id,
            GameInstance {
                config,
                lifecycle: GameLifecycle::Created,
                partitions,
            },
        );
        self.metrics.games_recovered += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::GameRecovered {
                game: id,
                replayed_txs: report.replayed_txs,
            },
        );
        Ok((id, report))
    }

    /// Returns a game's status.
    pub fn game_status(&self, game_id: GameInstanceId) -> Result<GameStatus, GameServerError> {
        let game = self.games.get(&game_id).ok_or(GameServerError::UnknownGame(game_id))?;
        let players = self
            .players
            .values()
            .filter(|player| player.game == game_id && player.state != PlayerState::Left)
            .count();
        let failed_partitions = game
            .partitions
            .iter()
            .filter(|partition| partition.state == PartitionState::Failed)
            .count();
        Ok(GameStatus {
            id: game_id,
            lifecycle: game.lifecycle,
            game_type: game.config.game_type.clone(),
            players,
            max_players: game.config.max_players,
            partitions: game.partitions.len(),
            failed_partitions,
        })
    }

    /// Returns every game's status in deterministic (game id) order.
    pub fn list_games(&self) -> Vec<(GameInstanceId, GameStatus)> {
        self.games
            .keys()
            .copied()
            .filter_map(|id| self.game_status(id).ok().map(|status| (id, status)))
            .collect()
    }

    // ---------------------------------------------------- reducer exposure

    /// Makes a reducer client-callable by any `Player` role.
    pub fn expose_reducer(&mut self, reducer: &str) -> Result<(), GameServerError> {
        self.register_client_reducer(reducer, &[Role::Player])
    }

    /// Makes a reducer client-callable, restricted to the given roles (an
    /// empty role set allows any authenticated principal).
    pub fn register_client_reducer(
        &mut self,
        reducer: &str,
        roles: &[Role],
    ) -> Result<(), GameServerError> {
        if reducer.is_empty() {
            return Err(GameServerError::InvalidConfig(
                "reducer name must not be empty".to_string(),
            ));
        }
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .register_reducer(reducer, ReducerExposure::ClientCallable, roles);
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::ReducerExposed { reducer: reducer.to_string() },
        );
        Ok(())
    }

    /// Revokes a reducer: it is no longer client-callable.
    pub fn revoke_reducer(&mut self, reducer: &str) -> Result<(), GameServerError> {
        {
            let mut table = self.policy.lock().expect("game policy mutex is not poisoned");
            if table.reducer_policy(reducer).is_none() {
                return Err(GameServerError::UnknownReducer(reducer.to_string()));
            }
            table.revoke_reducer(reducer);
        }
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::ReducerRevoked { reducer: reducer.to_string() },
        );
        Ok(())
    }

    /// The exposure of a reducer, if registered.
    pub fn reducer_exposure(&self, reducer: &str) -> Option<ReducerExposure> {
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .exposure(reducer)
    }

    /// Whether clients may currently invoke the named reducer.
    pub fn is_client_callable(&self, reducer: &str) -> bool {
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .is_client_callable(reducer)
    }

    /// Grants a principal a role override (admin/server).
    pub fn set_principal_role(&mut self, principal: u64, role: Role) {
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .set_role(principal, role);
    }

    // ------------------------------------------------------------- players

    /// Joins a player to a game (or reconnects an existing membership).
    ///
    /// - Reconnect: an existing non-`Left` membership for the same principal
    ///   is restored to `Active` (same `PlayerId`, never a duplicate).
    /// - Fresh join: validates the game (exists, running, capacity), routes
    ///   the player deterministically to `partitions[player_id % n]`, and
    ///   optionally invokes the configured `on_player_join` reducer through
    ///   the simulation path (authoritative initialization).
    pub fn join_game(
        &mut self,
        principal: &Principal,
        game_id: GameInstanceId,
    ) -> Result<JoinOutcome, GameServerError> {
        let principal_id = principal.id();
        // Reconnect path: an existing non-Left membership is restored.
        if let Some(&player_id) = self.player_by_principal.get(&(principal_id, game_id)) {
            let record = self.players.get_mut(&player_id).expect("membership exists");
            if record.state != PlayerState::Left {
                let world = record.world;
                record.state = PlayerState::Active;
                self.policy
                    .lock()
                    .expect("game policy mutex is not poisoned")
                    .add_active_player(principal_id, world);
                self.metrics.players_reconnected += 1;
                Self::push_event(
                    &mut self.events,
                    self.config.event_log_limit(),
                    GameServerEvent::PlayerJoined {
                        game: game_id,
                        player: player_id,
                        world,
                        reconnected: true,
                    },
                );
                return Ok(JoinOutcome::Reconnected);
            }
        }
        // Fresh join.
        let (partition, world, on_join) = {
            let game = self.games.get(&game_id).ok_or(GameServerError::UnknownGame(game_id))?;
            if !game.lifecycle.is_running() {
                return Err(game_state_error(game_id, game.lifecycle));
            }
            let active = self
                .players
                .values()
                .filter(|player| player.game == game_id && player.state != PlayerState::Left)
                .count();
            if active >= game.config.max_players {
                return Err(GameServerError::GameFull {
                    game: game_id,
                    max: game.config.max_players,
                });
            }
            let count = game.partitions.len();
            let index = (principal_id as usize) % count;
            let (partition, world) = {
                let slot = &game.partitions[index];
                (slot.partition, slot.world)
            };
            (partition, world, game.config.on_player_join.clone())
        };
        // The partition's world must be running.
        let status = self.runtime().world_status(world).map_err(GameServerError::Runtime)?;
        if status.state != WorldLifecycle::Running {
            return Err(GameServerError::WorldFailed(world));
        }
        let player_id = PlayerId::from_u64(principal_id);
        // A `Left` membership is reused as a fresh join (authoritative
        // initialization runs again through `on_player_join`); any other
        // existing membership would have been handled by the reconnect path
        // above.
        match self.players.get_mut(&player_id) {
            Some(record) => {
                record.state = PlayerState::Active;
                record.game = game_id;
                record.partition = partition;
                record.world = world;
            }
            None => {
                self.players.insert(
                    player_id,
                    PlayerRecord {
                        id: player_id,
                        principal: principal_id,
                        game: game_id,
                        partition,
                        world,
                        state: PlayerState::Active,
                    },
                );
            }
        }
        self.player_by_principal.insert((principal_id, game_id), player_id);
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .add_active_player(principal_id, world);
        if let Some(reducer) = on_join {
            let args = ReducerArgs::new()
                .insert("player_id", principal_id)
                .insert("game_id", game_id.as_u64());
            // Server-originated request ids live in the reserved namespace
            // (ADR-014 D3): they can never collide with a client's pending
            // call on the same world. The join path additionally sets a
            // second reserved bit, placing it in a sub-namespace the
            // `invoke_reducer` counter (which never sets bit 62) can never
            // reach — so a server join call and a server invoke can never
            // share a `(world, request_id)` either.
            let request_id = SERVER_REQUEST_MSB | SERVER_JOIN_MSB | player_id.as_u64();
            match self
                .runtime_mut()
                .submit_reducer_call(world, request_id, reducer.clone(), args)
            {
                Ok(()) => {}
                Err(error) => {
                    self.metrics.reducer_failures += 1;
                    Self::push_event(
                        &mut self.events,
                        self.config.event_log_limit(),
                        GameServerEvent::ReducerRejected {
                            player: player_id,
                            reducer,
                            reason: error.to_string(),
                        },
                    );
                }
            }
        }
        self.metrics.players_joined += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::PlayerJoined {
                game: game_id,
                player: player_id,
                world,
                reconnected: false,
            },
        );
        Ok(JoinOutcome::Joined)
    }

    /// Leaves a game: runs the optional `on_player_leave` reducer
    /// (authoritative cleanup, best effort), revokes active membership,
    /// ends the player's server-side subscriptions, and marks the
    /// membership `Left`. Idempotent.
    pub fn leave_game(&mut self, player_id: PlayerId) -> Result<(), GameServerError> {
        let (game_id, world, principal, on_leave) = {
            let record = self.players.get_mut(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            if record.state == PlayerState::Left {
                return Ok(());
            }
            let game_id = record.game;
            let world = record.world;
            let principal = record.principal;
            let on_leave = self
                .games
                .get(&game_id)
                .and_then(|game| game.config.on_player_leave.clone());
            (game_id, world, principal, on_leave)
        };
        if let Some(reducer) = on_leave {
            let args = ReducerArgs::new()
                .insert("player_id", principal)
                .insert("game_id", game_id.as_u64());
            if self
                .runtime_mut()
                .submit_reducer_call(world, principal, reducer, args)
                .is_err()
            {
                self.metrics.reducer_failures += 1;
            }
        }
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .remove_active_player(principal, world);
        self.players.get_mut(&player_id).expect("player exists").state = PlayerState::Left;
        if let Some(subs) = self.player_subs.remove(&player_id) {
            for subscription in subs {
                let _ = self.runtime_mut().unsubscribe(world, subscription);
            }
        }
        self.metrics.players_left += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::PlayerLeft { game: game_id, player: player_id },
        );
        Ok(())
    }

    /// Marks a player disconnected (host-driven on connection loss): the
    /// membership is retained as `Reconnecting`, active membership is
    /// revoked (no input may flow), and a later join with the same
    /// principal reconnects. Idempotent.
    pub fn disconnect_player(&mut self, player_id: PlayerId) -> Result<(), GameServerError> {
        let (game_id, principal, world) = {
            let record = self.players.get_mut(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            match record.state {
                PlayerState::Left => return Err(GameServerError::PlayerNotActive(player_id)),
                PlayerState::Reconnecting => return Ok(()),
                PlayerState::Active | PlayerState::Joining => {}
            }
            record.state = PlayerState::Reconnecting;
            (record.game, record.principal, record.world)
        };
        self.policy
            .lock()
            .expect("game policy mutex is not poisoned")
            .remove_active_player(principal, world);
        self.metrics.players_disconnected += 1;
        Self::push_event(
            &mut self.events,
            self.config.event_log_limit(),
            GameServerEvent::PlayerDisconnected { game: game_id, player: player_id },
        );
        Ok(())
    }

    /// Returns a player's status.
    pub fn player_status(&self, player_id: PlayerId) -> Result<PlayerStatus, GameServerError> {
        let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
        Ok(PlayerStatus {
            id: record.id,
            principal: record.principal,
            game: record.game,
            partition: record.partition,
            world: record.world,
            state: record.state,
        })
    }

    /// Returns the authoritative world a player is routed to.
    pub fn player_world(&self, player_id: PlayerId) -> Result<WorldId, GameServerError> {
        self.players
            .get(&player_id)
            .map(|record| record.world)
            .ok_or(GameServerError::UnknownPlayer(player_id))
    }

    // ----------------------------------------------- commands & reducers

    /// Submits a server-side intent: an `InputCommand` stamped with the
    /// player id, routed to the player's world for the world's next tick.
    /// The simulation decides the authoritative result.
    pub fn submit_command(
        &mut self,
        player_id: PlayerId,
        kind: impl Into<String>,
        payload: Option<Value>,
    ) -> Result<(), GameServerError> {
        let kind = kind.into();
        let world = {
            let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            if record.state != PlayerState::Active {
                return Err(GameServerError::PlayerNotActive(player_id));
            }
            let game = self.games.get(&record.game).ok_or(GameServerError::UnknownGame(record.game))?;
            if !game.lifecycle.is_running() {
                return Err(game_state_error(record.game, game.lifecycle));
            }
            let status = self.runtime().world_status(record.world).map_err(GameServerError::Runtime)?;
            if status.state != WorldLifecycle::Running {
                return Err(GameServerError::WorldFailed(record.world));
            }
            record.world
        };
        let command = InputCommand::new(player_id.as_u64(), kind, payload).map_err(GameServerError::Core)?;
        // Commands are buffered per world (ADR-014 D3) and merged into ONE
        // frame at `step()`. Submitting one frame per command would stamp
        // every frame with the same tick, and the runtime drains one frame
        // per tick — the surplus frames would fail the deterministic frame
        // gate and kill the world. The buffer is bounded; overflow rejects
        // explicitly and never silently drops an accepted command.
        let buffer = self.pending_commands.entry(world).or_default();
        if buffer.len() >= self.config.max_pending_commands_per_world() {
            self.metrics.commands_rejected += 1;
            Self::push_event(
                &mut self.events,
                self.config.event_log_limit(),
                GameServerEvent::CommandRejected {
                    player: player_id,
                    reason: "pending command buffer full".to_string(),
                },
            );
            return Err(GameServerError::CommandBufferFull(world));
        }
        buffer.push(command);
        self.metrics.commands_received += 1;
        Ok(())
    }

    /// Merges each world's buffered commands into a single `InputFrame`
    /// stamped with the world's current tick and submits it (ADR-014 D3).
    /// Called at the start of `step()`, before the world ticks. A world that
    /// stopped or failed between submit and flush has its buffered commands
    /// rejected explicitly (never silently dropped).
    fn flush_pending_commands(&mut self) {
        let worlds: Vec<WorldId> = self.pending_commands.keys().copied().collect();
        for world in worlds {
            let commands = match self.pending_commands.remove(&world) {
                Some(commands) if !commands.is_empty() => commands,
                _ => continue,
            };
            // A failed lookup (e.g. the world was destroyed) must still
            // reject the buffered commands explicitly — never silently drop
            // accepted commands (ADR-014 D3).
            let status = self.runtime().world_status(world).ok();
            let reject_all = |server: &mut Self, reason: &str, commands: &[InputCommand]| {
                for command in commands {
                    server.metrics.commands_rejected += 1;
                    Self::push_event(
                        &mut server.events,
                        server.config.event_log_limit(),
                        GameServerEvent::CommandRejected {
                            player: PlayerId::from_u64(command.source()),
                            reason: reason.to_string(),
                        },
                    );
                }
            };
            match status {
                None => reject_all(self, "world is gone", &commands),
                Some(status) if status.state != WorldLifecycle::Running => {
                    reject_all(self, "world is not running", &commands);
                }
                Some(status) => {
                    // Capture the sources before the frame is moved into the
                    // runtime, so a rejected frame can report accurate
                    // identity per command (ADR-014 D3).
                    let sources: Vec<u64> =
                        commands.iter().map(InputCommand::source).collect();
                    let mut frame = InputFrame::new(status.next_tick);
                    for command in commands {
                        frame.push(command);
                    }
                    if let Err(error) = self.runtime_mut().submit_input(world, frame) {
                        for source in sources {
                            self.metrics.commands_rejected += 1;
                            Self::push_event(
                                &mut self.events,
                                self.config.event_log_limit(),
                                GameServerEvent::CommandRejected {
                                    player: PlayerId::from_u64(source),
                                    reason: error.to_string(),
                                },
                            );
                        }
                    }
                }
            }
        }
    }

    /// Invokes a reducer server-side (server-trusted) for a player. Returns
    /// the correlated request id; results are delivered through the world's
    /// next `TickResult`.
    pub fn invoke_reducer(
        &mut self,
        player_id: PlayerId,
        reducer: &str,
        args: ReducerArgs,
    ) -> Result<u64, GameServerError> {
        let world = {
            let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            if record.state == PlayerState::Left {
                return Err(GameServerError::PlayerNotActive(player_id));
            }
            record.world
        };
        // Server-originated request ids live in the reserved namespace
        // (ADR-014 D3): the gateway rejects client ids with this bit, so a
        // server result can never be misrouted to a client's pending call.
        let request_id = SERVER_REQUEST_MSB | self.next_request;
        self.next_request += 1;
        match self.runtime_mut().submit_reducer_call(world, request_id, reducer, args) {
            Ok(()) => {
                self.metrics.reducer_calls += 1;
                Ok(request_id)
            }
            Err(error) => {
                self.metrics.reducer_failures += 1;
                Err(GameServerError::Runtime(error))
            }
        }
    }

    // --------------------------------------------------------- subscriptions

    /// Establishes a server-side subscription for a player on their world,
    /// bounded by `GameServerConfig::subscription_limit_per_player`. The
    /// `SubscriptionRegistry` remains the authoritative observation system.
    pub fn subscribe_player(
        &mut self,
        player_id: PlayerId,
        query: Query,
    ) -> Result<SubscriptionId, GameServerError> {
        let world = {
            let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            if record.state == PlayerState::Left {
                return Err(GameServerError::PlayerNotActive(player_id));
            }
            record.world
        };
        let limit = self.config.subscription_limit_per_player;
        if self.player_subs.get(&player_id).is_some_and(|subs| subs.len() >= limit) {
            self.metrics.subscription_limits_hit += 1;
            return Err(GameServerError::SubscriptionLimit { player: player_id, limit });
        }
        let subscription = self.runtime_mut().subscribe(world, query).map_err(GameServerError::Runtime)?;
        self.player_subs.entry(player_id).or_default().insert(subscription);
        self.metrics.subscriptions += 1;
        Ok(subscription)
    }

    /// Ends a server-side subscription of a player.
    pub fn unsubscribe_player(
        &mut self,
        player_id: PlayerId,
        subscription: SubscriptionId,
    ) -> Result<(), GameServerError> {
        let world = {
            let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            record.world
        };
        let tracked = self
            .player_subs
            .get_mut(&player_id)
            .is_some_and(|subs| subs.remove(&subscription));
        if !tracked {
            return Err(GameServerError::Capacity(format!(
                "player {player_id} does not track subscription {subscription}"
            )));
        }
        self.runtime_mut()
            .unsubscribe(world, subscription)
            .map_err(GameServerError::Runtime)?;
        Ok(())
    }

    /// Regenerates a player's server-side subscription from authoritative
    /// state.
    pub fn resync_player(
        &mut self,
        player_id: PlayerId,
        subscription: SubscriptionId,
    ) -> Result<(), GameServerError> {
        let world = {
            let record = self.players.get(&player_id).ok_or(GameServerError::UnknownPlayer(player_id))?;
            record.world
        };
        self.runtime_mut()
            .resync(world, subscription)
            .map_err(GameServerError::Runtime)?;
        Ok(())
    }

    // ------------------------------------------------------------ stepping

    /// Advances every world of every game by one tick in the runtime's
    /// deterministic order, fans the committed results out to network
    /// clients (TickUpdate broadcasts, subscription deltas, reducer-call
    /// results), and returns each successful world's committed `TickResult`.
    /// Runtime events are drained into game state (partition and game
    /// failure observation) and game-server events. This is the canonical
    /// host loop: `process_inbound → step → client pump`.
    /// Advances every running game one tick: buffered commands are merged
    /// into one frame per world (ADR-014 D3), then the runtime ticks, then
    /// committed results fan out to the network and runtime events are
    /// observed. A failed tick is reported through [`GameServerEvent`]s
    /// (`TickFailed` / `PartitionFailed` / `GameFailed`) and the game's
    /// lifecycle state — it is never a silent no-op: the authoritative
    /// world either committed or the game is marked failed.
    pub fn step(&mut self) -> Result<Vec<(WorldId, TickResult)>, GameServerError> {
        self.flush_pending_commands();
        let results = self.runtime_mut().step_detailed().map_err(GameServerError::Runtime)?;
        self.gateway.fan_out_results(&results);
        self.observe_runtime_events();
        Ok(results)
    }

    /// Observes runtime events (ADR-014 D6): a `WorldFailed` marks its
    /// game's partition `Failed` (and the game `Failed` when all partitions
    /// fail); `WorldRecovered` marks the partition `Recovered`; `TickFailed`
    /// increments metrics and surfaces a `TickFailed` event. The Game Server
    /// never reports a dead authoritative world as healthy.
    fn observe_runtime_events(&mut self) {
        for event in self.runtime_mut().drain_events() {
            match event {
                RuntimeEvent::TickFailed { world, .. } => {
                    self.metrics.tick_failures += 1;
                    Self::push_event(
                        &mut self.events,
                        self.config.event_log_limit(),
                        GameServerEvent::TickFailed { world },
                    );
                }
                RuntimeEvent::WorldFailed { world, reason } => {
                    self.metrics.world_failures += 1;
                    let Some(game_id) = self.world_to_game.get(&world).copied() else {
                        continue;
                    };
                    let Some(game) = self.games.get_mut(&game_id) else {
                        continue;
                    };
                    let mut failed_partition = None;
                    for partition in &mut game.partitions {
                        if partition.world == world && partition.state != PartitionState::Failed {
                            partition.state = PartitionState::Failed;
                            failed_partition = Some(partition.partition);
                            break;
                        }
                    }
                    let Some(partition) = failed_partition else {
                        continue;
                    };
                    self.metrics.partition_failures += 1;
                    Self::push_event(
                        &mut self.events,
                        self.config.event_log_limit(),
                        GameServerEvent::PartitionFailed {
                            game: game_id,
                            partition,
                            world,
                            reason: reason.to_string(),
                        },
                    );
                    let all_failed =
                        game.partitions.iter().all(|partition| partition.state == PartitionState::Failed);
                    if all_failed && game.lifecycle.is_running() {
                        game.lifecycle = GameLifecycle::Failed;
                        self.metrics.games_failed += 1;
                        Self::push_event(
                            &mut self.events,
                            self.config.event_log_limit(),
                            GameServerEvent::GameFailed {
                                game: game_id,
                                reason: reason.to_string(),
                            },
                        );
                    }
                }
                RuntimeEvent::WorldRecovered { world, .. } => {
                    let Some(game_id) = self.world_to_game.get(&world).copied() else {
                        continue;
                    };
                    let Some(game) = self.games.get_mut(&game_id) else {
                        continue;
                    };
                    let mut recovered_partition = None;
                    for partition in &mut game.partitions {
                        if partition.world == world {
                            if partition.state == PartitionState::Failed {
                                partition.state = PartitionState::Recovered;
                            }
                            recovered_partition = Some(partition.partition);
                            break;
                        }
                    }
                    if let Some(partition) = recovered_partition {
                        Self::push_event(
                            &mut self.events,
                            self.config.event_log_limit(),
                            GameServerEvent::PartitionRecovered {
                                game: game_id,
                                partition,
                                world,
                            },
                        );
                    }
                }
                _ => {}
            }
        }
    }

    // -------------------------------------------------- events & metrics

    /// Takes every buffered game-server event in order, clearing the log.
    pub fn drain_events(&mut self) -> Vec<GameServerEvent> {
        self.events.drain(..).collect()
    }

    /// Returns the number of buffered events.
    pub fn event_count(&self) -> usize {
        self.events.len()
    }

    /// Returns a point-in-time metrics snapshot (live counts computed from
    /// current state; counters are monotonic).
    pub fn metrics(&self) -> GameServerMetrics {
        let mut metrics = self.metrics.clone();
        metrics.games_active = self.games.values().filter(|game| game.lifecycle.is_running()).count();
        metrics.players_active = self
            .players
            .values()
            .filter(|player| matches!(player.state, PlayerState::Joining | PlayerState::Active))
            .count();
        metrics.partitions = self.games.values().map(|game| game.partitions.len()).sum();
        metrics.failed_partitions = self
            .games
            .values()
            .map(|game| {
                game.partitions
                    .iter()
                    .filter(|partition| partition.state == PartitionState::Failed)
                    .count()
            })
            .sum();
        metrics
    }

    /// Deterministically shuts the server down: the gateway's runtime stops
    /// scheduling, flushes every world's WAL (the durability contract), and
    /// releases resources.
    pub fn shutdown(&mut self) -> Result<(), GameServerError> {
        self.runtime_mut().shutdown().map_err(GameServerError::Runtime)
    }

    // ------------------------------------------------------------- helpers

    fn push_event(events: &mut VecDeque<GameServerEvent>, limit: usize, event: GameServerEvent) {
        if events.len() >= limit {
            events.pop_front();
        }
        events.push_back(event);
    }
}

/// Maps a non-running game lifecycle to the corresponding error, preserving
/// the distinction between a failed game (authoritative worlds dead) and a
/// merely stopped game.
fn game_state_error(game_id: GameInstanceId, lifecycle: GameLifecycle) -> GameServerError {
    match lifecycle {
        GameLifecycle::Failed => GameServerError::GameFailed(game_id),
        GameLifecycle::Stopped | GameLifecycle::Stopping => GameServerError::GameStopped(game_id),
        _ => GameServerError::GameNotRunning(game_id),
    }
}

impl std::fmt::Debug for GameServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("GameServer")
            .field("games", &self.games.len())
            .field("players", &self.players.len())
            .finish()
    }
}
