//! End-to-end multiplayer test for the playable arena game.
//!
//! Two real SDK clients over the real network boundary:
//!
//! Client -> SDK -> NetworkGateway -> Runtime -> Partition ->
//! reducers -> Transaction/OCC -> ONE atomic commit -> Vec<Change> ->
//! WAL + SubscriptionRegistry -> NetworkGateway -> SDK view.
//!
//! Proves: authentication, join, movement propagation, combat, death, respawn.

use std::sync::Arc;

use game_server::{COL_ALIVE, COL_AMMO, COL_HP, COL_X, COL_Y, game_factory, move_args};
use nexum_core::{PartitionId, WorldId};
use nexum_network::{NetworkConfig, NexumServer, Principal, TokenAuthenticator};
use nexum_reducer::ReducerArgs;
use nexum_runtime::{Runtime, RuntimeConfig};
use nexum_sdk::{Client, SdkConfig, transport::ClientTransport};
use nexum_subscription::Query;

// ---------------------------------------------------------------- harness

fn auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    for (name, id) in [("alice", 1u64), ("bob", 2u64)] {
        auth.add(name, Principal::new(id, name)).unwrap();
    }
    Arc::new(auth)
}

const TEST_WORLD: u64 = 0;
const TEST_PARTITION: u64 = 0;

fn test_server() -> NexumServer {
    let runtime = Runtime::new(RuntimeConfig::new(game_factory())).unwrap();
    let mut server = NexumServer::new(runtime, NetworkConfig::new(), auth()).unwrap();
    let world_id = WorldId::from_u64(TEST_WORLD);
    let sim = nexum_execution::PartitionConfig::new();
    server
        .runtime_mut()
        .create_partition(world_id, sim)
        .unwrap();
    server.runtime_mut().start_partition(world_id).unwrap();
    server
        .runtime_mut()
        .register_partition(PartitionId::from_u64(TEST_PARTITION), world_id)
        .unwrap();
    server
}

fn test_world() -> WorldId {
    WorldId::from_u64(TEST_WORLD)
}

fn connect_join(server: &mut NexumServer, token: &str, principal_id: u64) -> (Client, u64) {
    let (transport, server_conn) = ClientTransport::memory_pair(256, 512);
    server
        .gateway_mut()
        .register_connection(Box::new(server_conn))
        .unwrap();
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(transport.into_inner()).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert!(client.is_connected());

    client.authenticate(token).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert!(client.session_principal().is_some());

    let wid = test_world();
    let request_id = (1u64 << 63) | principal_id;
    server
        .runtime_mut()
        .submit_reducer_call(
            wid,
            request_id,
            "player_join",
            ReducerArgs::new()
                .insert("player_id", principal_id)
                .insert("game_id", 0u64),
        )
        .unwrap();

    client.attach(wid).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();

    let _sub = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    client.take_events();
    (client, principal_id)
}

fn step_and_pump(server: &mut NexumServer, clients: &mut [&mut Client]) {
    server.gateway_mut().process_inbound();
    server.gateway_mut().step_worlds().unwrap();
    server.gateway_mut().pump_subscriptions();
    for client in clients.iter_mut() {
        client.pump().unwrap();
    }
}

fn player_row(view: &nexum_sdk::View, player: u64) -> Option<(i64, i64, i64, bool, i64)> {
    view.rows()
        .find(|row| {
            row.row()
                .get(0)
                .map(|v| v.as_u64() == Some(player))
                .unwrap_or(false)
        })
        .map(|row| {
            let r = row.row();
            (
                r.get(COL_X).and_then(|v| v.as_i64()).unwrap_or(-1),
                r.get(COL_Y).and_then(|v| v.as_i64()).unwrap_or(-1),
                r.get(COL_HP).and_then(|v| v.as_i64()).unwrap_or(-1),
                r.get(COL_ALIVE).and_then(|v| v.as_i64()).unwrap_or(0) != 0,
                r.get(COL_AMMO).and_then(|v| v.as_i64()).unwrap_or(-1),
            )
        })
}

fn move_one(client: &mut Client, dx: i64, dy: i64) {
    client
        .call_reducer("move_player", move_args(dx, dy))
        .unwrap();
}

/// Move `client` one cell at a time toward target (tx, ty).
fn move_to(
    client: &mut Client,
    server: &mut NexumServer,
    other: &mut Client,
    player: u64,
    tx: i64,
    ty: i64,
) {
    for _ in 0..50 {
        let view = client.view(0).unwrap();
        let (cx, cy, _, _, _) = player_row(view, player).unwrap();
        if cx == tx && cy == ty {
            return;
        }
        let dx = (tx - cx).clamp(-1, 1);
        let dy = (ty - cy).clamp(-1, 1);
        move_one(client, dx, dy);
        step_and_pump(server, &mut [client, other]);
    }
    panic!("move_to: could not reach ({tx}, {ty}) within 50 steps");
}

// ------------------------------------------------------- tests

#[test]
fn two_clients_join_and_see_each_other() {
    let mut server = test_server();

    let (mut alice, _) = connect_join(&mut server, "alice", 1);
    let (mut bob, _) = connect_join(&mut server, "bob", 2);

    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
    let view = alice.view(0).unwrap();
    assert_eq!(view.len(), 2, "both players exist");
    let (_, _, hp1, alive1, _) = player_row(view, 1).unwrap();
    assert_eq!(hp1, 100);
    assert!(alive1);
    let (_, _, hp2, alive2, _) = player_row(view, 2).unwrap();
    assert_eq!(hp2, 100);
    assert!(alive2);
}

#[test]
fn movement_propagates_between_clients() {
    let mut server = test_server();

    let (mut alice, _) = connect_join(&mut server, "alice", 1);
    let (mut bob, _) = connect_join(&mut server, "bob", 2);

    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let view = alice.view(0).unwrap();
    let (spawn_x, _, _, _, _) = player_row(view, 1).unwrap();

    // Alice moves right by 1.
    move_one(&mut alice, 1, 0);
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let aview = alice.view(0).unwrap();
    let (ax, _, _, _, _) = player_row(aview, 1).unwrap();
    assert_eq!(ax, spawn_x + 1, "alice moved right");

    let bview = bob.view(0).unwrap();
    let (bx, _, _, _, _) = player_row(bview, 1).unwrap();
    assert_eq!(bx, spawn_x + 1, "bob sees alice's move");
}

#[test]
fn fire_weapon_deals_damage() {
    let mut server = test_server();

    let (mut alice, _) = connect_join(&mut server, "alice", 1);
    let (mut bob, _) = connect_join(&mut server, "bob", 2);

    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    // Move Alice East once so she faces East (COL_FACING = 1).
    move_one(&mut alice, 1, 0);
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    // Get Alice's new position (after the East step).
    let view = alice.view(0).unwrap();
    let (alice_x, alice_y, _, _, _) = player_row(view, 1).unwrap();

    // Move Bob to be 1 cell east of Alice.
    move_to(&mut bob, &mut server, &mut alice, 2, alice_x + 1, alice_y);

    // Verify Bob is right in front of Alice.
    let view = alice.view(0).unwrap();
    let (bx, by, _, _, _) = player_row(view, 2).unwrap();
    assert_eq!(bx, alice_x + 1, "bob is 1 cell east of alice");
    assert_eq!(by, alice_y, "same row");

    // Alice fires east and hits Bob. The WASM fire_weapon module only reads
    // __caller (stamped by gateway), so pass empty args.
    alice
        .call_reducer("fire_weapon", ReducerArgs::new())
        .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let view = bob.view(0).unwrap();
    let (_, _, hp, _, _) = player_row(view, 2).unwrap();
    assert_eq!(hp, 75, "bob took 25 damage");
    let (_, _, _, _, ammo) = player_row(alice.view(0).unwrap(), 1).unwrap();
    assert_eq!(ammo, 9, "alice used 1 ammo");
}

#[test]
fn death_and_respawn() {
    let mut server = test_server();

    let (mut alice, _) = connect_join(&mut server, "alice", 1);
    let (mut bob, _) = connect_join(&mut server, "bob", 2);

    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    // Move Alice East once so she faces East.
    move_one(&mut alice, 1, 0);
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let view = alice.view(0).unwrap();
    let (alice_x, alice_y, _, _, _) = player_row(view, 1).unwrap();

    // Position Bob 1 cell east of Alice.
    move_to(&mut bob, &mut server, &mut alice, 2, alice_x + 1, alice_y);

    // Fire 5 times to kill Bob (5 * 25 = 125 > 100 HP).
    // FIRE_COOLDOWN is 5 ticks; step 6 times between shots to let it expire.
    for _ in 0..5 {
        alice
            .call_reducer("fire_weapon", ReducerArgs::new())
            .unwrap();
        // Step enough ticks for the cooldown to expire between shots.
        for _ in 0..6 {
            step_and_pump(&mut server, &mut [&mut alice, &mut bob]);
        }
    }

    let view = bob.view(0).unwrap();
    let (_, _, _, alive, _) = player_row(view, 2).unwrap();
    assert!(!alive, "bob is dead");

    // Bob respawns.
    bob.call_reducer(
        "respawn_player",
        ReducerArgs::new().insert("player_id", 2u64),
    )
    .unwrap();
    step_and_pump(&mut server, &mut [&mut alice, &mut bob]);

    let view = bob.view(0).unwrap();
    let (_, _, hp, alive, ammo) = player_row(view, 2).unwrap();
    assert!(alive, "bob respawned");
    assert_eq!(hp, 100);
    assert_eq!(ammo, 10);
}
