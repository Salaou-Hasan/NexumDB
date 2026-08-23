//! Phase 14 end-to-end tests (ADR-014): the full
//! Client → SDK → Gateway → GameServer → Runtime → World → Transaction →
//! WAL → Subscription → Gateway → SDK path. Covers the authoritative
//! join/command/reducer pipeline, reducer exposure (deny-by-default),
//! server-only reducers, failed ticks (zero authoritative mutation, zero
//! subscription updates), multi-partition isolation, WASM reducers, and
//! recovery without historical replay.

use std::sync::Arc;

use nexum_core::{
    ColumnType, Error, GameInstanceId, PlayerId, ReducerId, Result, Row, RowId, SystemId,
    TableSchema, TickId, Value, WorldId, row,
};
use nexum_game_server::{GameInstanceConfig, GameServer, GameServerConfig, JoinOutcome};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_sdk::{Client, SdkConfig, ServerEvent, transport::ClientTransport};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

// ---------------------------------------------------------------- harness

fn players_table(store: &mut TableStore) {
    store
        .create_table(
            TableSchema::builder("players")
                .column("id", ColumnType::U64)
                .column("zone", ColumnType::U64)
                .column("health", ColumnType::I32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
}

fn ensure_players(store: &mut TableStore) {
    if store.table("players").is_none() {
        players_table(store);
    }
}

/// `bump`: +10 health for the named player; emits a `bumped` event.
fn bump(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args
        .get("player")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::invalid_argument("player id required"))?;
    let rows = ctx.scan("players")?;
    let (row_id, row) = rows
        .iter()
        .find(|(_, row)| row.get(0) == Some(&Value::U64(player)))
        .cloned()
        .ok_or_else(|| Error::not_found("player"))?;
    let health = row.get(2).and_then(Value::as_i32).unwrap_or(0);
    let mut values = row.clone().into_values();
    values[2] = Value::I32(health + 10);
    ctx.update("players", row_id, Row::new(values))?;
    ctx.emit("bumped", player)?;
    Ok(Value::I32(health + 10))
}

/// `player_join`: authoritative join initialization — insert the player
/// row. Idempotent: a rejoin after recovery (the row already exists in the
/// recovered store) must not fail the tick with a duplicate-key error
/// (ADR-014 D10).
fn player_join(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player_id")?;
    let exists = ctx
        .scan("players")?
        .iter()
        .any(|(_, row)| row.get(0) == Some(&Value::U64(player)));
    if !exists {
        ctx.insert("players", row![player, 10u64, 100i32])?;
    }
    Ok(Value::U64(player))
}

/// `server_secret`: a server-only reducer that writes an audit row with a
/// distinct key (the player's own row already exists via `player_join`).
fn server_secret(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player_id")?;
    ctx.insert("players", row![player + 1000, 99u64, 1i32])?;
    Ok(Value::U64(player))
}

/// A world with a `spawn` command consumer and the game reducers.
fn game_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_players(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                        for command in frame.commands() {
                            if command.kind() == "spawn" {
                                let id = command.payload().and_then(Value::as_u64).unwrap();
                                ctx.insert("players", row![id, 10u64, 100i32])?;
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
                    ReducerDefinition::new(ReducerId::from_u64(3), "server_secret", server_secret)
                        .unwrap(),
                )
                .unwrap();
            Ok(world)
        },
    )
}

/// A factory whose worlds fail on their first tick (zero mutation).
fn failing_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_players(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "writer", 0, |ctx, _| {
                        ctx.insert("players", row![ctx.tick().as_u64(), 10u64, 100i32])?;
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

/// A factory with a WASM reducer (`ping_wasm`) returning `U64(42)`.
fn wasm_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_players(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |_ctx, _| Ok(()))
                        .unwrap(),
                )
                .unwrap();
            let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
            wasm.register("ping_wasm", 1, ping_module()).unwrap();
            world.set_wasm(wasm);
            Ok(world)
        },
    )
}

/// A minimal WASM module returning `U64(42)` (no host calls).
fn ping_module() -> Vec<u8> {
    let wat = r#"(module
  (import "nexum" "op" (func $op (param i32 i32 i32 i32 i32) (result i32)))
  (memory (export "memory") 16)
  (global (export "_nexum_in_ptr") i32 (i32.const 0))
  (global (export "_nexum_out_ptr") i32 (i32.const 16384))
  (func (export "_nexum_reducer_run") (result i32)
    (i32.store8 align=1 (i32.const 16384) (i32.const 8))
    (i64.store align=1 (i32.const 16385) (i64.const 42))
    (i32.const 9)))"#;
    wat::parse_str(wat).expect("valid WAT")
}

fn auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    auth.add("alice-token", Principal::new(1, "alice")).unwrap();
    auth.add("bob-token", Principal::new(2, "bob")).unwrap();
    Arc::new(auth)
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn server_with(factory: WorldFactory) -> GameServer {
    let runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    GameServer::new(
        runtime,
        NetworkConfig::new(),
        auth(),
        GameServerConfig::new(),
    )
    .unwrap()
}

fn running_game(server: &mut GameServer, config: GameInstanceConfig) -> GameInstanceId {
    let game = server.create_game(config).unwrap();
    server.start_game(game).unwrap();
    game
}

/// Connects an SDK client, authenticates, joins the game through the host
/// (the server-side authority), and attaches to the player's world.
fn connect_and_join(
    server: &mut GameServer,
    token: &str,
    game: GameInstanceId,
) -> (Client, PlayerId, WorldId, JoinOutcome) {
    let (transport, server_conn) = ClientTransport::memory_pair(256, 512);
    server
        .gateway_mut()
        .register_connection(Box::new(server_conn))
        .unwrap();
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(transport.into_inner()).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert!(client.is_connected(), "handshake completes");

    client.authenticate(token).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    let principal = client.session_principal().expect("authenticated").clone();

    let outcome = server.join_game(&principal, game).unwrap();
    let player = PlayerId::from_u64(principal.id());
    let world = server.player_world(player).unwrap();

    client.attach(world).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert_eq!(
        client.attached_world(),
        Some(world),
        "attached to the player's world"
    );
    client.take_events();
    (client, player, world, outcome)
}

// ------------------------------------------------------------ full pipeline

#[test]
fn full_pipeline_join_reducer_subscription_and_wal() {
    let dir = temp_dir("nexum-game-e2e-pipeline");
    let runtime = Runtime::new(
        RuntimeConfig::new(game_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    // This test asserts the full change list on the Tick event.
    let network = NetworkConfig::new().with_tick_update_changes(true);
    let mut server = GameServer::new(runtime, network, auth(), GameServerConfig::new()).unwrap();
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("arena").with_on_player_join("player_join"),
    );
    server.expose_reducer("bump").unwrap();

    // Join: authoritative initialization runs through the simulation path on
    // the next tick (the player row is committed by the `player_join`
    // reducer, never written by the game server).
    let (mut client, _player, _world, outcome) = connect_and_join(&mut server, "alice-token", game);
    assert_eq!(outcome, JoinOutcome::Joined);
    server.step().unwrap();

    // The subscription observes the authoritative committed row.
    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    let view = client.view(local).unwrap();
    assert_eq!(view.len(), 1, "on_player_join committed the player row");
    assert_eq!(
        view.get(RowId::from_u64(0)).unwrap().row().get(0),
        Some(&Value::U64(1))
    );

    // Client reducer call → policy → runtime → world tick → WAL → subscription
    // → network → SDK view.
    let wal_before = server.runtime().metrics().wal_appends;
    let request = client
        .call_reducer("bump", ReducerArgs::new().insert("player", 1u64))
        .unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();

    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(results[0].is_ok());
    assert_eq!(results[0].value(), Some(&Value::I32(110)));
    assert!(
        server.runtime().metrics().wal_appends > wal_before,
        "the committed tick was persisted"
    );
    let view = client.view(local).unwrap();
    assert_eq!(
        view.get(RowId::from_u64(0)).unwrap().row().get(2),
        Some(&Value::I32(110))
    );
    // The TickUpdate carried the committed changes and the reducer event.
    let events = client.take_events();
    assert!(events.iter().any(|event| matches!(
        event,
        ServerEvent::Tick { changes, events, .. }
            if !changes.is_empty() && events.iter().any(|e| e.name() == "bumped")
    )));

    server.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------------ failed ticks

#[test]
fn failed_tick_produces_zero_mutation_and_zero_subscription_updates() {
    let mut server = server_with(failing_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("doomed"));
    let (mut client, _, _world, _) = connect_and_join(&mut server, "alice-token", game);
    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert!(
        client.view(local).unwrap().is_empty(),
        "initial snapshot is empty"
    );

    // The first tick fails: no authoritative mutation, no TickUpdate, no
    // subscription delta — and the game is reported Failed.
    server.step().unwrap();
    client.pump().unwrap();
    let events = client.take_events();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ServerEvent::Tick { .. })),
        "a failed tick emits no TickUpdate"
    );
    assert!(
        client.view(local).unwrap().is_empty(),
        "the failed tick changed nothing"
    );
    assert!(
        server
            .drain_events()
            .iter()
            .any(|event| matches!(event, nexum_game_server::GameServerEvent::GameFailed { .. }))
    );
}

// --------------------------------------------------- multi-partition isolation

#[test]
fn players_are_isolated_across_partitions_and_games() {
    let mut server = server_with(game_factory());
    let game = running_game(
        &mut server,
        GameInstanceConfig::new("two-shard").with_partition_count(2),
    );
    let (mut alice, _, world_a, _) = connect_and_join(&mut server, "alice-token", game);
    let (mut bob, _, world_b, _) = connect_and_join(&mut server, "bob-token", game);
    assert_ne!(world_a, world_b, "1 % 2 != 2 % 2 — deterministic routing");

    let sub_a = alice
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    let sub_b = bob
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    alice.pump().unwrap();
    bob.pump().unwrap();
    assert!(alice.view(sub_a).unwrap().is_empty());
    assert!(bob.view(sub_b).unwrap().is_empty());

    // Alice spawns a row on her world only.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(0, "spawn", Some(Value::U64(1))).unwrap());
    alice.send_input(frame).unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    alice.pump().unwrap();
    bob.pump().unwrap();

    assert_eq!(alice.view(sub_a).unwrap().len(), 1, "alice sees her row");
    assert!(
        bob.view(sub_b).unwrap().is_empty(),
        "bob's partition is untouched"
    );

    // Bob cannot attach to Alice's world either: he is already bound to his
    // own partition's world, and re-attachment is rejected synchronously
    // (membership is per-world).
    assert!(
        bob.attach(world_a).is_err(),
        "cross-world attach is rejected while already attached"
    );
}

// --------------------------------------------------- exposure & server authority

#[test]
fn unexposed_reducer_is_denied_but_server_can_invoke_it() {
    let mut server = server_with(game_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let (mut client, player, _world, _) = connect_and_join(&mut server, "alice-token", game);

    // `server_secret` is registered on the world but NOT exposed — the client
    // is denied with a correlated error and nothing is submitted.
    let request = client
        .call_reducer("server_secret", ReducerArgs::new())
        .unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(!results[0].is_ok(), "the unexposed reducer is denied");
    assert_eq!(server.gateway().metrics().policy_rejections, 1);

    // The same reducer is server-invocable (server-trusted path). It commits
    // through the normal tick boundary: one atomic tick, one Vec<Change>.
    let req = server
        .invoke_reducer(
            player,
            "server_secret",
            ReducerArgs::new().insert("player_id", 1u64),
        )
        .unwrap();
    let step_results = server.step().unwrap();
    assert_eq!(step_results.len(), 1, "one world ticked");
    let (_, tick) = &step_results[0];
    assert_eq!(tick.tick().as_u64(), 1, "the join tick (0), then this tick");
    assert!(
        tick.changes().iter().any(|change| {
            change
                .new_row()
                .is_some_and(|row| row.get(0) == Some(&Value::U64(1001)))
        }),
        "the audit row was committed by the server-side reducer"
    );
    assert!(
        tick.reducer_results().iter().any(|result| {
            result.request_id() == req && result.is_ok() && result.value() == Some(&Value::U64(1))
        }),
        "the server-side invoke produced a successful correlated result"
    );
}

#[test]
fn unregistered_reducers_are_denied_by_default() {
    let mut server = server_with(game_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("arena"));
    let (mut client, _, _, _) = connect_and_join(&mut server, "alice-token", game);

    let request = client
        .call_reducer("never_registered", ReducerArgs::new())
        .unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(
        !results[0].is_ok(),
        "unknown reducers are denied by the policy"
    );
}

// ---------------------------------------------------------------- WASM

#[test]
fn wasm_reducer_invoked_through_the_game_server() {
    let mut server = server_with(wasm_factory());
    let game = running_game(&mut server, GameInstanceConfig::new("wasm-arena"));
    server.expose_reducer("ping_wasm").unwrap();
    let (mut client, _, _, _) = connect_and_join(&mut server, "alice-token", game);

    let request = client
        .call_reducer("ping_wasm", ReducerArgs::new())
        .unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(results[0].is_ok());
    assert_eq!(results[0].value(), Some(&Value::U64(42)));
}

// ---------------------------------------------------------------- recovery

#[test]
fn recovery_restores_state_and_reconnects_without_history_replay() {
    let dir = temp_dir("nexum-game-e2e-recovery");

    // First server: create, run, and durably commit one join (the
    // `player_join` reducer inserts the player row through the simulation
    // path on the join tick).
    let game_config = GameInstanceConfig::new("arena").with_on_player_join("player_join");
    let runtime = Runtime::new(
        RuntimeConfig::new(game_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let mut server = GameServer::new(
        runtime,
        NetworkConfig::new(),
        auth(),
        GameServerConfig::new(),
    )
    .unwrap();
    let game = running_game(&mut server, game_config.clone());
    let (client, _player, _world, _) = connect_and_join(&mut server, "alice-token", game);
    server.step().unwrap();
    assert!(
        server.runtime().metrics().wal_appends >= 1,
        "the join tick was persisted"
    );
    server.shutdown().unwrap();
    drop(client);
    drop(server);

    // Second server: recover the game from the WAL using the SAME config,
    // so the rejoin re-runs `player_join` against the recovered row — which
    // must be idempotent (no duplicate-key tick failure).
    let runtime = Runtime::new(
        RuntimeConfig::new(game_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let mut server = GameServer::new(
        runtime,
        NetworkConfig::new(),
        auth(),
        GameServerConfig::new(),
    )
    .unwrap();
    server.expose_reducer("bump").unwrap();
    let (game, report) = server.recover_game(game_config, None).unwrap();
    assert!(report.replayed_txs >= 1, "committed history was replayed");
    server.start_game(game).unwrap();

    // The player reconnects: same token → same principal → same PlayerId.
    let (mut client, player, world, _outcome) = connect_and_join(&mut server, "alice-token", game);
    assert_eq!(player.as_u64(), 1);
    assert_eq!(server.player_world(player).unwrap(), world);

    // A fresh subscription sees the current recovered state — and only future
    // commits flow as live updates (no historical replay).
    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    let view = client.view(local).unwrap();
    assert_eq!(view.len(), 1, "recovered row is visible in the snapshot");
    assert_eq!(
        view.get(RowId::from_u64(0)).unwrap().row().get(0),
        Some(&Value::U64(1))
    );

    // An empty tick emits no deltas to the fresh subscription.
    client.take_events();
    server.step().unwrap();
    client.pump().unwrap();
    let events = client.take_events();
    assert!(
        events.iter().all(
            |event| !matches!(event, ServerEvent::Tick { changes, .. } if !changes.is_empty())
        ),
        "no historical changes replayed as live updates"
    );

    // Subsequent simulation works and delivers a live delta.
    let request = client
        .call_reducer("bump", ReducerArgs::new().insert("player", 1u64))
        .unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert_eq!(results[0].value(), Some(&Value::I32(110)));
    assert_eq!(
        client
            .view(local)
            .unwrap()
            .get(RowId::from_u64(0))
            .unwrap()
            .row()
            .get(2),
        Some(&Value::I32(110)),
        "the post-recovery commit updated the view"
    );

    server.shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}
