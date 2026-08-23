//! Phase 14 unit tests: game lifecycle, players, exposure, routing,
//! commands, failure observation, and policy.

use std::sync::Arc;

use nexum_core::{
    ColumnType, Error, GameInstanceId, PlayerId, ReducerId, Result, Row, SystemId, TableSchema,
    Value, WorldId, row,
};
use nexum_network::{GamePolicy, NetworkConfig, Principal, TokenAuthenticator};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{SimulationConfig, SystemDefinition, World};
use nexum_table::TableStore;

use crate::config::{GameInstanceConfig, GameServerConfig};
use crate::error::GameServerError;
use crate::events::GameServerEvent;
use crate::lifecycle::{GameLifecycle, JoinOutcome, PlayerState};
use crate::policy::{GamePolicyTable, PolicyHandle, ReducerExposure, Role};
use crate::server::GameServer;

// ---------------------------------------------------------------- harness

fn players_table(store: &mut TableStore) {
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("health", ColumnType::I32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
}

/// `bump`: +10 health for the named player.
fn bump(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player")?;
    let rows = ctx.scan("players")?;
    let (row_id, row) = rows
        .iter()
        .find(|(_, row)| row.get(0) == Some(&Value::U64(player)))
        .cloned()
        .ok_or_else(|| Error::not_found("player"))?;
    let health = row.get(1).and_then(Value::as_i32).unwrap_or(0);
    let mut values = row.clone().into_values();
    values[1] = Value::I32(health + 10);
    ctx.update("players", row_id, Row::new(values))?;
    Ok(Value::I32(health + 10))
}

/// `player_join`: authoritative initialization — insert the player row.
fn player_join(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player_id")?;
    ctx.insert("players", row![player, 100i32])?;
    Ok(Value::U64(player))
}

/// `player_leave`: authoritative cleanup — delete the player row.
fn player_leave(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player_id")?;
    let rows = ctx.scan("players")?;
    for (row_id, row) in rows {
        if row.get(0) == Some(&Value::U64(player)) {
            ctx.delete("players", row_id)?;
            return Ok(Value::U64(player));
        }
    }
    Ok(Value::U64(0))
}

/// A world with a `spawn` command consumer and native reducers.
fn test_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            players_table(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                        for command in frame.commands() {
                            if command.kind() == "spawn" {
                                let id = command.payload().and_then(Value::as_u64).unwrap();
                                ctx.insert("players", row![id, 100i32])?;
                            }
                        }
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
            world
                .native_mut()
                .register(ReducerDefinition::new(ReducerId::from_u64(1), "bump", bump).unwrap())
                .unwrap();
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(2), "player_join", player_join)
                        .unwrap(),
                )
                .unwrap();
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(ReducerId::from_u64(3), "player_leave", player_leave)
                        .unwrap(),
                )
                .unwrap();
            Ok(world)
        },
    )
}

/// A factory whose world 0 fails on its first tick.
fn failing_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            players_table(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                        ctx.insert("players", row![ctx.tick().as_u64(), 100i32])?;
                        Ok(())
                    })
                    .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(1), "fails", 10, |_ctx, _| {
                        Err(Error::invalid_argument("boom"))
                    })
                    .unwrap(),
                )
                .unwrap();
            Ok(world)
        },
    )
}

fn server_with(factory: WorldFactory) -> GameServer {
    let runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    GameServer::new(
        runtime,
        NetworkConfig::new(),
        Arc::new(TokenAuthenticator::new()),
        GameServerConfig::new(),
    )
    .unwrap()
}

fn alice() -> Principal {
    Principal::new(1, "alice")
}

fn bob() -> Principal {
    Principal::new(2, "bob")
}

fn running_game(server: &mut GameServer, config: GameInstanceConfig) -> GameInstanceId {
    let game = server.create_game(config).unwrap();
    server.start_game(game).unwrap();
    game
}

// ------------------------------------------------------------ game lifecycle

#[test]
fn game_lifecycle_create_start_stop_restart_destroy() {
    let mut server = server_with(test_factory());
    let game = server
        .create_game(GameInstanceConfig::new("arena"))
        .unwrap();
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Created
    );
    assert!(matches!(
        server.drain_events().as_slice(),
        [GameServerEvent::GameCreated { .. }]
    ));

    server.start_game(game).unwrap();
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Running
    );

    server.stop_game(game).unwrap();
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Stopped
    );

    // Restart from Stopped is legal.
    server.start_game(game).unwrap();
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Running
    );

    server.destroy_game(game).unwrap();
    assert!(matches!(
        server.game_status(game),
        Err(GameServerError::UnknownGame(_))
    ));
    assert!(server.list_games().is_empty());
}

#[test]
fn game_lifecycle_invalid_transitions_are_rejected() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    // Starting a running game is invalid.
    assert!(matches!(
        server.start_game(game),
        Err(GameServerError::InvalidTransition { .. })
    ));
    // Stopping a stopped game is invalid.
    server.stop_game(game).unwrap();
    assert!(matches!(
        server.stop_game(game),
        Err(GameServerError::InvalidTransition { .. })
    ));
    // Destroying twice is an unknown-game error.
    server.destroy_game(game).unwrap();
    assert!(matches!(
        server.destroy_game(game),
        Err(GameServerError::UnknownGame(_))
    ));
}

#[test]
fn invalid_game_configs_are_rejected() {
    let mut server = server_with(test_factory());
    assert!(matches!(
        server.create_game(GameInstanceConfig::new("x").with_partition_count(0)),
        Err(GameServerError::InvalidConfig(_))
    ));
    assert!(matches!(
        server.create_game(GameInstanceConfig::new("x").with_max_players(0)),
        Err(GameServerError::InvalidConfig(_))
    ));
    // Server-level config validation.
    let runtime = Runtime::new(RuntimeConfig::new(test_factory())).unwrap();
    assert!(matches!(
        GameServer::new(
            runtime,
            NetworkConfig::new(),
            Arc::new(TokenAuthenticator::new()),
            GameServerConfig::new().with_default_partition_count(0),
        ),
        Err(GameServerError::InvalidConfig(_))
    ));
}

// ------------------------------------------------------------------- players

#[test]
fn join_leave_and_fresh_rejoin() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));

    let outcome = server.join_game(&alice(), game).unwrap();
    assert_eq!(outcome, JoinOutcome::Joined);
    let player = PlayerId::from_u64(alice().id());
    assert_eq!(
        server.player_status(player).unwrap().state,
        PlayerState::Active
    );
    assert_eq!(server.player_status(player).unwrap().game, game);

    server.leave_game(player).unwrap();
    assert_eq!(
        server.player_status(player).unwrap().state,
        PlayerState::Left
    );

    // A join after Left is a fresh join, not a reconnect.
    let outcome = server.join_game(&alice(), game).unwrap();
    assert_eq!(outcome, JoinOutcome::Joined);
}

#[test]
fn duplicate_join_is_a_reconnect_with_the_same_player() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());

    assert_eq!(
        server.join_game(&alice(), game).unwrap(),
        JoinOutcome::Joined
    );
    // Same principal rejoining restores the same membership.
    let outcome = server.join_game(&alice(), game).unwrap();
    assert_eq!(outcome, JoinOutcome::Reconnected);
    assert_eq!(
        server.player_world(player).unwrap(),
        server.player_world(player).unwrap()
    );

    // Disconnect → reconnect restores Active.
    server.disconnect_player(player).unwrap();
    assert_eq!(
        server.player_status(player).unwrap().state,
        PlayerState::Reconnecting
    );
    assert_eq!(
        server.join_game(&alice(), game).unwrap(),
        JoinOutcome::Reconnected
    );
    assert_eq!(
        server.player_status(player).unwrap().state,
        PlayerState::Active
    );
}

#[test]
fn join_rejects_full_games_and_invalid_states() {
    let mut server = server_with(test_factory());
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("arena").with_max_players(1),
    );
    assert_eq!(
        server.join_game(&alice(), game).unwrap(),
        JoinOutcome::Joined
    );
    assert!(matches!(
        server.join_game(&bob(), game),
        Err(GameServerError::GameFull { .. })
    ));

    // Joining a stopped game fails explicitly.
    let stopped = server
        .create_game(GameInstanceConfig::new("lobby"))
        .unwrap();
    assert!(matches!(
        server.join_game(&alice(), stopped),
        Err(GameServerError::GameNotRunning(_))
    ));

    // Unknown game.
    assert!(matches!(
        server.join_game(&alice(), GameInstanceId::from_u64(999)),
        Err(GameServerError::UnknownGame(_))
    ));
}

#[test]
fn disconnect_removes_input_authority_and_reconnect_restores_it() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();
    let world = server.player_world(player).unwrap();

    // Disconnect revokes the active-input grant.
    server.disconnect_player(player).unwrap();
    let handle = server.policy_handle();
    let table = handle.table().lock().unwrap();
    assert!(!table.is_active(alice().id(), world));
    drop(table);

    // Reconnect restores it.
    server.join_game(&alice(), game).unwrap();
    let handle = server.policy_handle();
    let table = handle.table().lock().unwrap();
    assert!(table.is_active(alice().id(), world));
}

// ------------------------------------------------------------ routing

#[test]
fn deterministic_partition_routing() {
    let mut server = server_with(test_factory());
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("two-shard").with_partition_count(2),
    );
    let world_a = server.join_game(&alice(), game).unwrap();
    let world_b = server.join_game(&bob(), game).unwrap();
    let alice_world = server
        .player_world(PlayerId::from_u64(alice().id()))
        .unwrap();
    let bob_world = server.player_world(PlayerId::from_u64(bob().id())).unwrap();
    // 1 % 2 = 1, 2 % 2 = 0 — deterministic pure function of the player id.
    assert_eq!(world_a, JoinOutcome::Joined);
    assert_eq!(world_b, JoinOutcome::Joined);
    assert_ne!(alice_world, bob_world);
    assert_eq!(server.game_status(game).unwrap().partitions, 2);
    assert_eq!(server.game_status(game).unwrap().players, 2);
}

#[test]
fn players_are_isolated_between_games() {
    let mut server = server_with(test_factory());
    let game_a = running_game(&mut server, GameInstanceConfig::new("a"));
    let game_b = running_game(&mut server, GameInstanceConfig::new("b"));
    let player_a = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game_a).unwrap();
    assert_eq!(server.player_status(player_a).unwrap().game, game_a);
    // Bob cannot join game A as Alice, and Alice's membership is game-scoped.
    assert!(matches!(
        server.player_status(PlayerId::from_u64(bob().id())),
        Err(GameServerError::UnknownPlayer(_))
    ));
    server.join_game(&bob(), game_b).unwrap();
    assert_eq!(
        server
            .player_status(PlayerId::from_u64(bob().id()))
            .unwrap()
            .game,
        game_b
    );
}

// ----------------------------------------------------------------- commands

#[test]
fn submit_command_executes_through_the_world_tick() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();

    // Server-side intent: spawn a player row in the authoritative world.
    server
        .submit_command(player, "spawn", Some(Value::U64(alice().id())))
        .unwrap();
    let results = server.step().unwrap();
    assert_eq!(results.len(), 1);
    let (world, result) = &results[0];
    assert_eq!(*world, server.player_world(player).unwrap());
    assert!(
        !result.changes().is_empty(),
        "spawn committed at least one change"
    );

    // A command for an unknown player is rejected.
    assert!(matches!(
        server.submit_command(PlayerId::from_u64(999), "spawn", None),
        Err(GameServerError::UnknownPlayer(_))
    ));
}

#[test]
fn invoke_reducer_is_server_trusted_and_correlated() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();

    let request = server
        .invoke_reducer(
            player,
            "player_join",
            ReducerArgs::new().insert("player_id", alice().id()),
        )
        .unwrap();
    let results = server.step().unwrap();
    let call = results[0]
        .1
        .reducer_results()
        .iter()
        .find(|call| call.request_id() == request)
        .expect("the call commits on the next tick");
    assert!(call.is_ok());
}

// ------------------------------------------------------ reducer exposure

#[test]
fn exposure_is_deny_by_default_and_revocable() {
    let mut server = server_with(test_factory());
    assert!(!server.is_client_callable("bump"));
    assert_eq!(server.reducer_exposure("bump"), None);

    server.expose_reducer("bump").unwrap();
    assert!(server.is_client_callable("bump"));
    assert_eq!(
        server.reducer_exposure("bump"),
        Some(ReducerExposure::ClientCallable)
    );

    server.revoke_reducer("bump").unwrap();
    assert!(!server.is_client_callable("bump"));
    assert!(matches!(
        server.revoke_reducer("bump"),
        Err(GameServerError::UnknownReducer(_))
    ));
}

#[test]
fn policy_handle_enforces_exposure_roles_and_membership() {
    let mut server = server_with(test_factory());
    server.expose_reducer("bump").unwrap();
    server
        .register_client_reducer("admin_only", &[Role::Admin])
        .unwrap();
    server.set_principal_role(1, Role::Player);

    let handle = server.policy_handle();
    let alice = alice();
    let world = WorldId::from_u64(0);

    // Unknown reducers are denied.
    assert!(!handle.authorize_reducer(&alice, world, "nope"));
    // Exposed reducer allowed (player role).
    assert!(handle.authorize_reducer(&alice, world, "bump"));
    // Admin-only reducer denied to a player.
    assert!(!handle.authorize_reducer(&alice, world, "admin_only"));
    // A principal granted Admin can call it.
    server.set_principal_role(9, Role::Admin);
    let admin = Principal::new(9, "admin");
    assert!(handle.authorize_reducer(&admin, world, "admin_only"));

    // Attach/input require active membership in the world.
    assert!(!handle.authorize_attach(&alice, world));
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    server.join_game(&alice, game).unwrap();
    // Re-fetch the policy handle (table is shared, so it reflects updates).
    assert!(server.policy_handle().authorize_attach(&alice, world));
}

#[test]
fn role_overrides_and_active_membership_are_shared_live() {
    let mut table = GamePolicyTable::new();
    table.register_reducer("move", ReducerExposure::ClientCallable, &[Role::Player]);
    let handle = PolicyHandle::new(std::sync::Arc::new(std::sync::Mutex::new(table)));

    // Granting membership through the same table is visible to the handle.
    let alice = alice();
    let world = WorldId::from_u64(0);
    assert!(!handle.authorize_input(&alice, world, &nexum_simulation::InputFrame::new(0.into())));
    handle
        .table()
        .lock()
        .unwrap()
        .add_active_player(alice.id(), world);
    assert!(handle.authorize_input(&alice, world, &nexum_simulation::InputFrame::new(0.into())));
    assert!(handle.authorize_attach(&alice, world));
}

// ------------------------------------------------- failure observation

#[test]
fn world_failure_marks_partition_and_then_game_failed() {
    let mut server = server_with(failing_factory());
    // Both worlds of the game fail; the game must report Failed and events.
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("doomed").with_partition_count(2),
    );
    let _ = server.step().unwrap();
    let events = server.drain_events();
    let partition_failures = events
        .iter()
        .filter(|event| matches!(event, GameServerEvent::PartitionFailed { .. }))
        .count();
    assert_eq!(partition_failures, 2, "both partitions report failure");
    assert!(
        events
            .iter()
            .any(|event| matches!(event, GameServerEvent::GameFailed { .. }))
    );
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Failed
    );
    assert_eq!(server.game_status(game).unwrap().failed_partitions, 2);

    // Joining a failed game fails explicitly.
    assert!(matches!(
        server.join_game(&alice(), game),
        Err(GameServerError::GameFailed(_))
    ));
}

#[test]
fn failing_world_rejects_commands_and_joins() {
    let mut server = server_with(failing_factory());
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("doomed").with_partition_count(1),
    );
    // Join succeeds while the world is up, then the world fails on the first
    // tick and the game itself becomes Failed (its only partition died).
    let outcome = server.join_game(&alice(), game).unwrap();
    assert_eq!(outcome, JoinOutcome::Joined);
    let _ = server.step().unwrap();
    let player = PlayerId::from_u64(alice().id());
    assert!(matches!(
        server.submit_command(player, "spawn", None),
        Err(GameServerError::GameFailed(_))
    ));
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Failed
    );
}

// ------------------------------------------------------------ subscriptions

#[test]
fn server_side_subscriptions_are_bounded_per_player() {
    let runtime = Runtime::new(RuntimeConfig::new(test_factory())).unwrap();
    let mut server = GameServer::new(
        runtime,
        NetworkConfig::new(),
        Arc::new(TokenAuthenticator::new()),
        GameServerConfig::new().with_subscription_limit_per_player(2),
    )
    .unwrap();
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();
    let query = nexum_subscription::Query::builder("players")
        .build()
        .unwrap();
    let sub_a = server.subscribe_player(player, query.clone()).unwrap();
    let sub_b = server.subscribe_player(player, query.clone()).unwrap();
    assert!(matches!(
        server.subscribe_player(player, query),
        Err(GameServerError::SubscriptionLimit { .. })
    ));
    server.unsubscribe_player(player, sub_a).unwrap();
    server.resync_player(player, sub_b).unwrap();
    assert!(matches!(server.unsubscribe_player(player, sub_b), Ok(())));
}

#[test]
fn leave_game_ends_tracked_subscriptions() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();
    let query = nexum_subscription::Query::builder("players")
        .build()
        .unwrap();
    let sub = server.subscribe_player(player, query).unwrap();
    server.leave_game(player).unwrap();
    // The subscription no longer exists on the world's registry.
    let world = server.player_world(player).unwrap();
    assert!(server.runtime().is_stale(world, sub).is_err());
}

// ---------------------------------------------------- regression (review)

/// A burst of commands submitted before a tick must be merged into ONE
/// frame — multiple frames stamped with the same tick would fail the
/// deterministic frame gate and kill the world (ADR-014 D3).
#[test]
fn command_burst_before_a_tick_merges_into_one_frame() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();

    for id in 1..=5u64 {
        server
            .submit_command(player, "spawn", Some(Value::U64(id)))
            .unwrap();
    }
    // One step drains the whole burst in one tick (FIFO), no frame-gate
    // failure, world stays healthy.
    let results = server.step().unwrap();
    assert_eq!(results.len(), 1, "one world ticked");
    let changes = results[0].1.changes().len();
    assert_eq!(changes, 5, "all five commands committed in one tick");
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Running
    );

    // Subsequent ticks continue normally.
    server.step().unwrap();
    assert_eq!(
        server.game_status(game).unwrap().lifecycle,
        GameLifecycle::Running
    );
}

/// Commands submitted between ticks still execute on the next tick, and a
/// stop rejects the buffered remainder explicitly.
#[test]
fn commands_buffered_then_stopped_are_rejected_not_dropped() {
    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();

    server
        .submit_command(player, "spawn", Some(Value::U64(1)))
        .unwrap();
    server
        .submit_command(player, "spawn", Some(Value::U64(2)))
        .unwrap();
    server.stop_game(game).unwrap();

    // Both buffered commands were rejected with events, not silently lost.
    let events = server.drain_events();
    let rejected: Vec<_> = events
        .iter()
        .filter(|event| matches!(event, GameServerEvent::CommandRejected { .. }))
        .collect();
    assert_eq!(
        rejected.len(),
        2,
        "both buffered commands rejected explicitly"
    );
}

/// Server-originated request ids live in the reserved namespace: a server
/// invoke and a client call can never share a `(world, request_id)` key, so
/// results cannot be misrouted.
#[test]
fn server_invoke_request_ids_use_the_reserved_namespace() {
    use nexum_network::SERVER_REQUEST_MSB;

    let mut server = server_with(test_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let player = PlayerId::from_u64(alice().id());
    server.join_game(&alice(), game).unwrap();

    let request = server
        .invoke_reducer(
            player,
            "player_join",
            ReducerArgs::new().insert("player_id", alice().id()),
        )
        .unwrap();
    assert_ne!(request & SERVER_REQUEST_MSB, 0, "server ids are namespaced");
    let results = server.step().unwrap();
    let call = results[0]
        .1
        .reducer_results()
        .iter()
        .find(|call| call.request_id() == request)
        .expect("the server call commits and reports its request id");
    assert!(call.is_ok());
}

// ------------------------------------------------------------ determinism

#[test]
fn repeated_runs_produce_identical_world_ids_and_routing() {
    let mut first = server_with(test_factory());
    let mut second = server_with(test_factory());
    let run = |server: &mut GameServer| -> (u64, u64) {
        let game = running_game(
            server,
            GameInstanceConfig::new("arena").with_partition_count(3),
        );
        server.join_game(&alice(), game).unwrap();
        server.join_game(&bob(), game).unwrap();
        let alice_world = server.player_world(PlayerId::from_u64(1)).unwrap().as_u64();
        let bob_world = server.player_world(PlayerId::from_u64(2)).unwrap().as_u64();
        (alice_world, bob_world)
    };
    assert_eq!(run(&mut first), run(&mut second));
}
