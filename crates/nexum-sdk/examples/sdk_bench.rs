//! Phase 13 SDK benchmarks — honest baselines, not claims (ADR-013).
//!
//! Run with: `cargo run --release -p nexum-sdk --example sdk_bench
//! [iterations]`. The client talks to an in-process gateway over the memory
//! transport, so transport I/O is intentionally not in the hot path — the
//! measured costs are protocol encode/decode, session establishment, input
//! submission, reducer correlation, subscription snapshot/delta handling,
//! and the derived client-side View.

use std::sync::Arc;
use std::time::Instant;

use nexum_core::{ColumnType, ReducerId, SystemId, TickId, Value, WorldId};
use nexum_network::{
    protocol::ServerMessage, NetworkConfig, NetworkGateway, Principal, TokenAuthenticator,
};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{Runtime, RuntimeConfig, WorldFactory};
use nexum_sdk::{protocol::ClientMessage, transport::ClientTransport, Client, SdkConfig};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;

fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..50 {
        f(); // warmup
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    println!("{name:<44} {ns:>12.1} ns/op");
}

fn players_table(store: &mut TableStore) {
    store
        .create_table(
            nexum_core::TableSchema::builder("players")
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

/// The `bump` native reducer: `+10` to a player's health, emits an event.
fn bump(ctx: &mut ReducerContext, args: &ReducerArgs) -> nexum_core::Result<Value> {
    let player = args
        .get("player")
        .and_then(Value::as_u64)
        .ok_or_else(|| nexum_core::Error::invalid_argument("player id required"))?;
    let rows = ctx.scan("players")?;
    let found = rows
        .iter()
        .find(|(_, row)| row.get(0) == Some(&Value::U64(player)))
        .cloned()
        .ok_or_else(|| nexum_core::Error::not_found("player"))?;
    let health = found.1.get(2).and_then(Value::as_i32).unwrap_or(0);
    let mut values = found.1.clone().into_values();
    values[2] = Value::I32(health + 10);
    ctx.update("players", found.0, nexum_core::Row::new(values))?;
    ctx.emit("bumped", player)?;
    Ok(Value::I32(health + 10))
}

/// A world whose system consumes `spawn` commands, with `bump` registered.
fn input_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                    for command in frame.commands() {
                        if command.kind() == "spawn" {
                            let id = command.payload().and_then(Value::as_u64).unwrap();
                            ctx.insert("players", nexum_core::row![id, 10u64, 100i32])?;
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
        Ok(world)
    })
}

fn auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    auth.add("token", Principal::new(1, "bench")).unwrap();
    Arc::new(auth)
}

fn gateway_with() -> NetworkGateway {
    let runtime = Runtime::new(RuntimeConfig::new(input_factory())).unwrap();
    NetworkGateway::new(runtime, NetworkConfig::new(), auth()).unwrap()
}

fn open_client(gateway: &mut NetworkGateway) -> Client {
    let (transport, server) = ClientTransport::memory_pair(4096, 100_000);
    gateway.register_connection(Box::new(server)).unwrap();
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(transport.into_inner()).unwrap();
    client
}

/// Connect + authenticate + attach + subscribe once (drained). Returns the
/// client and the local subscription id.
fn joined_client(gateway: &mut NetworkGateway, world: WorldId) -> (Client, u64) {
    let mut client = open_client(gateway);
    gateway.process_inbound();
    client.pump().unwrap();
    client.authenticate("token").unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    client.attach(world).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    client.take_events();
    let local = client.subscribe(Query::builder("players").build().unwrap()).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    client.take_events();
    (client, local)
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);
    let world = WorldId::from_u64(0);
    let max = 64 * 1024;

    // 1. Protocol encode/decode from the SDK's client side.
    {
        let mut input = InputFrame::new(TickId::from_u64(0));
        input.push(InputCommand::new(1, "spawn", Some(Value::U64(1))).unwrap());
        bench("SDK protocol encode (client input)", iterations, || {
            let _ = nexum_sdk::protocol::encode_client(
                &ClientMessage::InputFrame { frame: input.clone() },
                max,
            )
            .unwrap();
        });
        let update = ServerMessage::TickUpdate {
            world,
            tick: TickId::from_u64(1),
            tx_id: nexum_core::TransactionId::from_u64(1),
            changes: vec![nexum_storage::Change::insert(
                nexum_core::TableId::from_u64(0),
                nexum_core::RowId::from_u64(1),
                nexum_core::row![1u64, 10u64, 100i32],
                nexum_core::Version::from_u64(1),
            )],
            events: vec![nexum_reducer::ReducerEvent::new("spawned", 1u64)],
        };
        let frame = nexum_network::protocol::encode_server(&update, max).unwrap();
        bench("SDK protocol decode (server TickUpdate)", iterations, || {
            let _ = nexum_sdk::protocol::decode_server(&frame, max).unwrap();
        });
    }

    // 2. Session establishment (connect + handshake through the gateway).
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        bench("SDK session setup (h+a+a)", iterations, || {
            let mut client = open_client(&mut gateway);
            gateway.process_inbound();
            client.pump().unwrap();
            client.authenticate("token").unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.attach(world).unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.take_events();
        });
    }

    // 3. Input routing through the SDK.
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (mut client, _local) = joined_client(&mut gateway, world);
        let mut tick = 0u64;
        bench("SDK input → tick → events", iterations, || {
            let mut frame = InputFrame::new(TickId::from_u64(tick));
            frame.push(InputCommand::new(0, "spawn", Some(Value::U64(tick))).unwrap());
            client.send_input(frame).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            client.pump().unwrap();
            client.take_events();
            tick += 1;
        });
    }

    // 4. Reducer request round trip (call → tick → correlated result).
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (mut client, _local) = joined_client(&mut gateway, world);
        // Seed one player row so `bump` succeeds.
        let mut frame = InputFrame::new(TickId::from_u64(0));
        frame.push(InputCommand::new(0, "spawn", Some(Value::U64(7))).unwrap());
        client.send_input(frame).unwrap();
        gateway.process_inbound();
        gateway.step_worlds().unwrap();
        client.pump().unwrap();
        client.take_events();
        bench("SDK reducer call roundtrip", iterations / 2, || {
            let request = client
                .call_reducer("bump", ReducerArgs::new().insert("player", 7u64))
                .unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            client.pump().unwrap();
            let results = client.take_reducer_results();
            assert_eq!(results.len(), 1);
            assert_eq!(results[0].request_id(), request);
        });
    }

    // 5. Subscription snapshot (resync) and delta application.
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (mut client, local) = joined_client(&mut gateway, world);
        // Populate 100 rows in 100 ticks.
        for tick in 0..100u64 {
            let mut frame = InputFrame::new(TickId::from_u64(tick));
            frame.push(InputCommand::new(0, "spawn", Some(Value::U64(tick))).unwrap());
            client.send_input(frame).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            client.pump().unwrap();
            client.take_events();
        }
        bench("SDK subscription resync (100 rows)", iterations / 5, || {
            client.resync(local).unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.take_events();
        });
        bench("SDK delta apply (per delta)", iterations, || {
            let mut frame = InputFrame::new(TickId::from_u64(1_000));
            frame.push(InputCommand::new(0, "spawn", Some(Value::U64(9_999))).unwrap());
            client.send_input(frame).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            client.pump().unwrap();
            client.take_events();
        });
    }

    // 6. View read (derived client state; never authoritative).
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (mut client, local) = joined_client(&mut gateway, world);
        for tick in 0..50u64 {
            let mut frame = InputFrame::new(TickId::from_u64(tick));
            frame.push(InputCommand::new(0, "spawn", Some(Value::U64(tick))).unwrap());
            client.send_input(frame).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            client.pump().unwrap();
            client.take_events();
        }
        bench("SDK view lookup (50 rows)", iterations, || {
            for i in 0..50u64 {
                let _ = client.view(local).unwrap().get(nexum_core::RowId::from_u64(i));
            }
        });
    }

    // 7. Subscription correlation bookkeeping (subscribe → snapshot bind →
    //    unsubscribe).
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (mut client, _local) = joined_client(&mut gateway, world);
        bench("SDK subscribe/unsubscribe roundtrip", iterations, || {
            let local = client.subscribe(Query::builder("players").build().unwrap()).unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.take_events();
            client.unsubscribe(local).unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.take_events();
        });
    }

    // 8. Reconnection (fresh session over a fresh connection).
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        bench("SDK reconnect (new session)", iterations / 2, || {
            let mut client = open_client(&mut gateway);
            gateway.process_inbound();
            client.pump().unwrap();
            client.authenticate("token").unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
            client.attach(world).unwrap();
            gateway.process_inbound();
            client.pump().unwrap();
        });
    }

    // 9. Slow-client isolation: a capped client never blocks the world; a
    //    healthy client keeps receiving every tick's events.
    {
        let mut gateway = gateway_with();
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let (slow_transport, slow_server) = ClientTransport::memory_pair(4096, 1);
        gateway.register_connection(Box::new(slow_server)).unwrap();
        let mut slow = Client::new(SdkConfig::new()).unwrap();
        slow.connect(slow_transport.into_inner()).unwrap();
        gateway.process_inbound();
        slow.pump().unwrap();
        slow.authenticate("token").unwrap();
        gateway.process_inbound();
        slow.pump().unwrap();
        slow.attach(world).unwrap();
        gateway.process_inbound();
        slow.pump().unwrap();

        let (mut fast, _local) = joined_client(&mut gateway, world);
        bench("slow-client isolation (per tick)", iterations, || {
            gateway.step_worlds().unwrap();
            fast.pump().unwrap();
            fast.take_events();
            let _ = slow.pump();
            slow.take_events();
        });
    }
}
