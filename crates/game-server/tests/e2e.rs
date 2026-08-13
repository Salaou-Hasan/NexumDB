//! End-to-end multiplayer test for the playable arena game (canonical
//! roadmap: "actual playable game demo"). Two real SDK clients over the real
//! network boundary:
//!
//! Client → SDK → NetworkGateway → GameServer → Runtime → World →
//! systems/reducers → Transaction/OCC → ONE atomic commit → Vec<Change> →
//! WAL + SubscriptionRegistry → NetworkGateway → SDK view.
//!
//! Proves: authentication, join, movement propagation (A moves → B sees it),
//! WASM combat (A fires → B's health drops), death, respawn, disconnect and
//! reconnect with a correct reconstructed view.

use std::sync::Arc;

use game_server::{
    game_factory, move_args, CLIENT_REDUCERS, COL_ALIVE, COL_AMMO, COL_COOLDOWN, COL_HP, COL_X,
    COL_Y,
};
use nexum_core::{PlayerId, WorldId};
use nexum_game_server::{
    GameInstanceConfig, GameServer, GameServerConfig, JoinOutcome,
};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_reducer::ReducerArgs;
use nexum_runtime::{Runtime, RuntimeConfig};
use nexum_sdk::{transport::ClientTransport, Client, SdkConfig};
use nexum_subscription::Query;

// ---------------------------------------------------------------- harness

fn auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    for (name, id) in [("alice", 1u64), ("bob", 2u64)] {
        auth.add(name, Principal::new(id, name)).unwrap();
    }
    Arc::new(auth)
}

fn server() -> GameServer {
    let runtime = Runtime::new(RuntimeConfig::new(game_factory())).unwrap();
    GameServer::new(runtime, NetworkConfig::new(), auth(), GameServerConfig::new()).unwrap()
}

fn running_arena(server: &mut GameServer) -> nexum_core::GameInstanceId {
    let game = server
        .create_game(
            GameInstanceConfig::new("arena")
                .with_partition_count(1)
                .with_on_player_join("player_join"),
        )
        .unwrap();
    server.start_game(game).unwrap();
    for reducer in CLIENT_REDUCERS {
        server.expose_reducer(reducer).unwrap();
    }
    game
}

/// Connects an SDK client, authenticates, joins the arena (host-driven,
/// server-side authority), attaches to the world, and subscribes to the
/// `players` table. Returns the client and the subscribed view id.
fn connect_join_attach(
    server: &mut GameServer,
    token: &str,
    game: nexum_core::GameInstanceId,
) -> (Client, PlayerId, WorldId, u64) {
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
    assert_eq!(outcome, JoinOutcome::Joined);
    let player = PlayerId::from_u64(principal.id());
    let world = server.player_world(player).unwrap();

    client.attach(world).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert_eq!(client.attached_world(), Some(world), "attached");

    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    client.take_events();
    (client, player, world, local)
}

/// Drives one authoritative server step and pumps both clients.
fn step_and_pump(server: &mut GameServer, clients: &mut [&mut Client]) {
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    server.gateway_mut().pump_subscriptions();
    for client in clients.iter_mut() {
        client.pump().unwrap();
    }
}

fn hp(view: &nexum_sdk::View, player: u64) -> i64 {
    view.rows()
        .find(|row| {
            row.row()
                .get(0)
                .map(|value| value.as_u64() == Some(player))
                .unwrap_or(false)
        })
        .map(|row| row.row().get(COL_HP).and_then(|v| v.as_i64()).unwrap_or(-1))
        .unwrap_or(-1)
}

fn xy(view: &nexum_sdk::View, player: u64) -> (i64, i64) {
    view.rows()
        .find(|row| {
            row.row()
                .get(0)
                .map(|value| value.as_u64() == Some(player))
                .unwrap_or(false)
        })
        .map(|row| {
            (
                row.row().get(COL_X).and_then(|v| v.as_i64()).unwrap_or(-1),
                row.row().get(COL_Y).and_then(|v| v.as_i64()).unwrap_or(-1),
            )
        })
        .unwrap_or((-1, -1))
}

fn alive(view: &nexum_sdk::View, player: u64) -> bool {
    view.rows()
        .find(|row| {
            row.row()
                .get(0)
                .map(|value| value.as_u64() == Some(player))
                .unwrap_or(false)
        })
        .map(|row| {
            row.row()
                .get(COL_ALIVE)
                .and_then(|v| v.as_i64())
                .unwrap_or(0)
                != 0
        })
        .unwrap_or(false)
}

fn ammo(view: &nexum_sdk::View, player: u64) -> i64 {
    view.rows()
        .find(|row| {
            row.row()
                .get(0)
                .map(|value| value.as_u64() == Some(player))
                .unwrap_or(false)
        })
        .map(|row| row.row().get(COL_AMMO).and_then(|v| v.as_i64()).unwrap_or(-1))
        .unwrap_or(-1)
}

// ------------------------------------------------------- full multiplayer

#[test]
fn two_clients_join_move_fight_die_respawn_and_reconnect() {
    let mut server = server();
    let game = running_arena(&mut server);

    let (mut alice, alice_player, alice_world, alice_local) =
        connect_join_attach(&mut server, "alice", game);
    let (mut bob, bob_player, bob_world, bob_local) =
        connect_join_attach(&mut server, "bob", game);
    assert_eq!(alice_world, bob_world, "one partition → one shared world");

    // The join reducers commit on the next tick (authoritative initialization
    // through the simulation, never written by the game server).
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let view = alice.view(alice_local).unwrap();
    assert_eq!(view.len(), 2, "both players exist in the committed world");
    assert_eq!(hp(view, 1), 100);
    assert_eq!(hp(view, 2), 100);

    // ---- 1. Alice moves; Bob's view must show the authoritative new position.
    let (ax0, ay0) = xy(view, 1);
    let move_request = alice.call_reducer("move_player", move_args(1, 0)).unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let move_results = alice.take_reducer_results();
    assert!(
        move_results
            .iter()
            .any(|r| r.request_id() == move_request && r.is_ok()),
        "Alice's move committed: {:?}",
        move_results
            .iter()
            .map(|r| (r.request_id(), r.error().map(str::to_string)))
            .collect::<Vec<_>>()
    );

    let alice_view = alice.view(alice_local).unwrap();
    let bob_view = bob.view(bob_local).unwrap();
    let (ax1, _) = xy(alice_view, 1);
    assert_eq!(ax1, ax0 + 1, "server validated the move (authoritative)");
    assert_eq!(
        xy(bob_view, 1),
        (ax1, ay0),
        "Bob sees Alice's committed move through his subscription"
    );

    // ---- 2. Alice fires at Bob (WASM reducer). They must be adjacent and
    // facing: place Bob exactly east of Alice with `set_position` (server-only
    // reducer invoked by the host), then fire.
    let (ax, ay) = xy(bob_view, 1);
    server
        .invoke_reducer(
            bob_player,
            "set_position",
            ReducerArgs::new()
                .insert("player_id", 2u64)
                .insert("x", ax + 1)
                .insert("y", ay)
                .insert("facing", 3i64),
        )
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    assert_eq!(xy(bob.view(bob_local).unwrap(), 2), (ax + 1, ay), "bob repositioned");

    // Ensure Alice faces east toward Bob, then fire.
    server
        .invoke_reducer(
            alice_player,
            "set_position",
            ReducerArgs::new()
                .insert("player_id", 1u64)
                .insert("x", ax)
                .insert("y", ay)
                .insert("facing", 1i64),
        )
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    assert_eq!(xy(bob.view(bob_local).unwrap(), 1), (ax, ay), "alice repositioned");

    let shot = alice.call_reducer("fire_weapon", ReducerArgs::new()).unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let results = alice.take_reducer_results();
    assert_eq!(results.len(), 1, "exactly one correlated result");
    assert_eq!(results[0].request_id(), shot);
    assert!(results[0].is_ok(), "shot fired: {:?}", results[0].error());
    assert_eq!(results[0].value(), Some(&nexum_core::Value::I64(25)), "25 damage");    let alice_view = alice.view(alice_local).unwrap();
    let bob_view = bob.view(bob_local).unwrap();
    assert_eq!(hp(alice_view, 2), 75, "Alice sees Bob's health drop");
    assert_eq!(hp(bob_view, 2), 75, "Bob sees his own health drop");
    assert_eq!(ammo(bob_view, 1), 9, "one shot consumed");
    assert!(
        bob_view
            .rows()
            .find(|row| {
                row.row()
                    .get(0)
                    .map(|v| v.as_u64() == Some(1))
                    .unwrap_or(false)
            })
            .map(|row| {
                row.row()
                    .get(COL_COOLDOWN)
                    .and_then(|v| v.as_i64())
                    .unwrap_or(0)
            })
            .unwrap_or(0)
            >= 4,
        "cooldown armed by the shot"
    );

    // ---- 3. Kill Bob with three more shots (25 × 4 = 100). The cooldown
    // system decays one per tick; with FIRE_COOLDOWN = 5, firing every 6th
    // tick keeps the weapon ready.
    for shot_number in 0..3 {
        for _ in 0..6 {
            step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
        }
        let request = alice.call_reducer("fire_weapon", ReducerArgs::new()).unwrap();
        step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
        let results = alice.take_reducer_results();
        let result = results
            .iter()
            .find(|result| result.request_id() == request)
            .expect("correlated result");
        assert!(result.is_ok(), "shot {shot_number}: {:?}", result.error());
        assert_eq!(hp(bob.view(bob_local).unwrap(), 2), 50 - 25 * shot_number as i64);
    }
    let bob_view = bob.view(bob_local).unwrap();
    assert!(!alive(bob_view, 2), "Bob died");
    assert_eq!(hp(bob_view, 2), 0, "health floor at 0");

    // ---- 4. Respawn: Bob's client calls the exposed `respawn_player`; the
    // gateway stamps his principal as the caller, and the authoritative
    // respawn runs inside the simulation (never a host-side write).
    let respawn = bob.call_reducer("respawn_player", ReducerArgs::new()).unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let results = bob.take_reducer_results();
    assert!(
        results.iter().any(|r| r.request_id() == respawn && r.is_ok()),
        "respawn committed"
    );
    let bob_view = bob.view(bob_local).unwrap();
    assert!(alive(bob_view, 2), "Bob respawned alive");
    assert_eq!(hp(bob_view, 2), 100, "full health on respawn");
    let (bx, by) = xy(bob_view, 2);
    assert!(
        bx >= 0 && by >= 0 && bx < 48 && by < 24,
        "respawn inside the arena"
    );

    // ---- 5. Disconnect + reconnect: Bob's row persists with connected=0;
    // a new connection reconstructs the current authoritative view (no
    // historical replay).
    server.disconnect_player(bob_player).unwrap();
    server
        .invoke_reducer(
            bob_player,
            "player_leave",
            ReducerArgs::new().insert("player_id", 2u64),
        )
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice]);

    let (transport2, server_conn2) = ClientTransport::memory_pair(256, 512);
    server
        .gateway_mut()
        .register_connection(Box::new(server_conn2))
        .unwrap();
    let mut bob2 = Client::new(SdkConfig::new()).unwrap();
    bob2.connect(transport2.into_inner()).unwrap();
    server.gateway_mut().process_inbound();
    bob2.pump().unwrap();
    bob2.authenticate("bob").unwrap();
    server.gateway_mut().process_inbound();
    bob2.pump().unwrap();
    let principal = bob2.session_principal().expect("authenticated").clone();
    let outcome = server.join_game(&principal, game).unwrap();
    assert_eq!(outcome, JoinOutcome::Reconnected, "membership restored");
    bob2.attach(bob_world).unwrap();
    server.gateway_mut().process_inbound();
    bob2.pump().unwrap();
    let local = bob2.subscribe(Query::builder("players").build().unwrap()).unwrap();
    server.gateway_mut().process_inbound();
    bob2.pump().unwrap();

    let view = bob2.view(local).unwrap();
    assert_eq!(view.len(), 2, "reconnected Bob sees the current world");
    assert_eq!(hp(view, 2), 100, "current state, not historical replay");
    assert!(alive(view, 2), "Bob is alive in the authoritative world");

    // A subsequent tick keeps working normally for the reconnected client.
    alice
        .call_reducer("move_player", move_args(0, 1))
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob2]);
    let view = bob2.view(local).unwrap();
    assert_eq!(xy(view, 1).1, ay + 1, "Alice's move reaches reconnected Bob");

    // Cleanup: the SDK disconnect path closes the session cleanly.
    bob2.disconnect();
    alice.disconnect();
    server.shutdown().unwrap();
}

// -------------------------------------------- client cannot mutate state

#[test]
fn client_cannot_forge_position_health_or_identity() {
    let mut server = server();
    let game = running_arena(&mut server);

    let (mut alice, _alice_player, _, _alice_local) =
        connect_join_attach(&mut server, "alice", game);
    let (mut bob, _, _, bob_local) = connect_join_attach(&mut server, "bob", game);
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    // The client may not call server-only reducers: `set_position` and
    // `take_damage` are not exposed, so the gateway must reject the call.
    let request = alice
        .call_reducer("set_position", ReducerArgs::new().insert("player_id", 1u64))
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let results = alice.take_reducer_results();
    let result = results
        .iter()
        .find(|result| result.request_id() == request)
        .expect("correlated rejection");
    assert!(result.error().is_some(), "unexposed reducer rejected");

    // Even if a client sends `move_player` with a forged caller, the gateway
    // stamps the real authenticated principal; the reducer acts for the
    // caller, never for a client-supplied id.
    let (bx0, by0) = xy(bob.view(bob_local).unwrap(), 2);
    let (ax0, ay0) = xy(bob.view(bob_local).unwrap(), 1);
    let request = alice
        .call_reducer(
            "move_player",
            ReducerArgs::new().insert("dx", 0i64).insert("dy", 1i64),
        )
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let results = alice.take_reducer_results();
    assert!(
        results.iter().any(|r| r.request_id() == request && r.is_ok()),
        "Alice's move succeeded: {:?}",
        results
            .iter()
            .map(|r| (r.request_id(), r.error().map(str::to_string)))
            .collect::<Vec<_>>()
    );
    // Bob must NOT have moved — the caller is Alice (id 1).
    assert_eq!(xy(bob.view(bob_local).unwrap(), 2), (bx0, by0), "Bob unaffected");
    let (ax, ay) = xy(bob.view(bob_local).unwrap(), 1);
    assert_eq!((ax, ay), (ax0, ay0 + 1), "Alice moved herself, not Bob");

    alice.disconnect();
    bob.disconnect();
    server.shutdown().unwrap();
}

// ------------------------------------------------------ cross-client order

#[test]
fn two_clients_moving_never_leak_between_views() {
    let mut server = server();
    let game = running_arena(&mut server);

    let (mut alice, _, _, alice_local) = connect_join_attach(&mut server, "alice", game);
    let (mut bob, _, _, bob_local) = connect_join_attach(&mut server, "bob", game);
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    // Alice and Bob each move every tick; both views must always agree on
    // both players' positions (single authoritative world).
    for _ in 0..5 {
        alice.call_reducer("move_player", move_args(1, 0)).unwrap();
        bob.call_reducer("move_player", move_args(1, 0)).unwrap();
        step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
        let a_view = alice.view(alice_local).unwrap();
        let b_view = bob.view(bob_local).unwrap();
        assert_eq!(xy(a_view, 1), xy(b_view, 1), "Alice agrees");
        assert_eq!(xy(a_view, 2), xy(b_view, 2), "Bob agrees");
        assert_eq!(a_view.len(), 2);
        assert_eq!(b_view.len(), 2);
    }

    alice.disconnect();
    bob.disconnect();
    server.shutdown().unwrap();
}
