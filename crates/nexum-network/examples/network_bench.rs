//! Phase 11 network benchmarks — honest baselines, not claims (ADR-011).
//!
//! Run with: `cargo run --release -p nexum-network --example network_bench
//! [iterations]`. Covers protocol codecs, session creation, input routing,
//! subscription serialization, outbound queue insertion, many connections /
//! subscriptions, and slow-client isolation. The gateway is single-threaded
//! and in-process (memory transport); network I/O is intentionally not in
//! the hot path.

use std::sync::Arc;
use std::time::Instant;

use nexum_core::{row, ColumnType, SystemId, TickId, WorldId};
use nexum_network::{
    protocol::{self, ClientMessage, ServerMessage},
    Connection, MemoryConnection, MemoryTransport, NetworkConfig, NetworkGateway, Principal,
    TokenAuthenticator,
};
use nexum_runtime::{Runtime, RuntimeConfig, WorldFactory};
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
    println!("{name:<40} {ns:>12.1} ns/op");
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

/// A world whose system consumes `spawn` commands as player rows.
fn input_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "consumer", 0, |ctx, frame| {
                    for command in frame.commands() {
                        if command.kind() == "spawn" {
                            let id = command.payload().and_then(nexum_core::Value::as_u64).unwrap();
                            ctx.insert("players", row![id, 10u64, 100i32])?;
                        }
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// A world whose system inserts one player row per tick (for subscription
/// fan-out and slow-client scenarios).
fn writer_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
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
        Ok(world)
    })
}

fn auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    auth.add("token", Principal::new(1, "bench")).unwrap();
    Arc::new(auth)
}

fn gateway_with(factory: WorldFactory) -> NetworkGateway {
    let runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    NetworkGateway::new(runtime, NetworkConfig::new(), auth()).unwrap()
}

fn open_client(gateway: &mut NetworkGateway, outbound_cap: usize) -> MemoryConnection {
    let (server, client) = MemoryTransport::connect(4096, outbound_cap);
    gateway.register_connection(Box::new(server)).unwrap();
    client
}

/// Handshake + authenticate + attach (the connection lifecycle).
fn join_world(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32) {
    let frame = protocol::encode_client(
        &ClientMessage::Handshake {
            version: nexum_network::PROTOCOL_VERSION,
            name: "bench".into(),
        },
        max,
    )
    .unwrap();
    client.try_send_frame(frame).unwrap();
    gateway.process_inbound();
    let _ = client.try_recv_frame().unwrap().unwrap();

    let frame = protocol::encode_client(
        &ClientMessage::Authenticate { credentials: "token".into() },
        max,
    )
    .unwrap();
    client.try_send_frame(frame).unwrap();
    gateway.process_inbound();
    let _ = client.try_recv_frame().unwrap().unwrap();

    let frame = protocol::encode_client(
        &ClientMessage::AttachWorld { world: WorldId::from_u64(0) },
        max,
    )
    .unwrap();
    client.try_send_frame(frame).unwrap();
    gateway.process_inbound();
    let _ = client.try_recv_frame().unwrap().unwrap();
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|value| value.parse().ok())
        .unwrap_or(2_000);
    let max = 64 * 1024;

    // 1-2. Protocol codecs.
    {
        let handshake = ClientMessage::Handshake {
            version: nexum_network::PROTOCOL_VERSION,
            name: "benchmark-client".into(),
        };
        let frame = protocol::encode_client(&handshake, max).unwrap();
        bench("frame encode (client)", iterations, || {
            let _ = protocol::encode_client(&handshake, max).unwrap();
        });
        bench("frame decode (client)", iterations, || {
            let _ = protocol::decode_client(&frame, max).unwrap();
        });
    }
    {
        let update = ServerMessage::TickUpdate {
            world: WorldId::from_u64(0),
            tick: TickId::from_u64(1),
            tx_id: nexum_core::TransactionId::from_u64(1),
            changes: vec![nexum_storage::Change::insert(
                nexum_core::TableId::from_u64(0),
                nexum_core::RowId::from_u64(1),
                row![1u64, 10u64, 100i32],
                nexum_core::Version::from_u64(1),
            )],
            events: vec![nexum_reducer::ReducerEvent::new("spawned", 1u64)],
        };
        let frame = protocol::encode_server(&update, max).unwrap();
        bench("frame encode (server TickUpdate)", iterations, || {
            let _ = protocol::encode_server(&update, max).unwrap();
        });
        bench("frame decode (server TickUpdate)", iterations, || {
            let _ = protocol::decode_server(&frame, max).unwrap();
        });
    }

    // 3. Session creation (handshake + auth + attach through the gateway).
    {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        bench("session creation (h+a+a)", iterations, || {
            let mut client = open_client(&mut gateway, 4096);
            join_world(&mut gateway, &mut client, max);
        });
    }

    // 4. Input routing (one spawn command per frame).
    {
        let mut gateway = gateway_with(input_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut client = open_client(&mut gateway, 4096);
        join_world(&mut gateway, &mut client, max);
        let mut tick = 0u64;
        bench("input routing (1 cmd/frame)", iterations, || {
            let mut frame = InputFrame::new(TickId::from_u64(tick));
            frame.push(InputCommand::new(1, "spawn", Some(nexum_core::Value::U64(tick))).unwrap());
            let encoded = protocol::encode_client(&ClientMessage::InputFrame { frame }, max).unwrap();
            client.try_send_frame(encoded).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            let _ = client.try_recv_frame().unwrap(); // TickUpdate
            tick += 1;
        });
    }

    // 5. Subscription update serialization (drain + serialize a delta).
    {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut client = open_client(&mut gateway, 4096);
        join_world(&mut gateway, &mut client, max);
        // Subscribe once; drain the Initial snapshot so future deltas land.
        let sub_frame = protocol::encode_client(
            &ClientMessage::Subscribe {
                request_id: 0,
                query: Query::builder("players").build().unwrap(),
            },
            max,
        )
        .unwrap();
        client.try_send_frame(sub_frame).unwrap();
        gateway.process_inbound();
        let _ = client.try_recv_frame().unwrap().unwrap(); // Initial snapshot

        bench("subscription serialization (delta)", iterations, || {
            gateway.step_worlds().unwrap();
            let _ = client.try_recv_frame().unwrap(); // TickUpdate
            let _ = client.try_recv_frame().unwrap(); // delta
        });
    }

    // 6. Outbound queue insertion (send-only path, no overflow).
    {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut client = open_client(&mut gateway, 100_000);
        join_world(&mut gateway, &mut client, max);
        let update = ServerMessage::Pong { nonce: 7 };
        bench("outbound queue insertion", iterations, || {
            let frame = protocol::encode_server(&update, max).unwrap();
            client.try_send_frame(frame).unwrap();
        });
    }

    // 7. Many connections stepping one world (fan-out).
    for (count, divisor) in [(100usize, 1usize), (1_000, 5)] {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut clients = Vec::new();
        for _ in 0..count {
            let mut client = open_client(&mut gateway, 4096);
            join_world(&mut gateway, &mut client, max);
            clients.push(client);
        }
        bench(&format!("{count} connections / one tick"), iterations / divisor, || {
            gateway.step_worlds().unwrap();
            for client in &mut clients {
                let _ = client.try_recv_frame().unwrap();
            }
        });
    }

    // 8. Many subscriptions on one client (subscription flood limit raised).
    {
        let config = NetworkConfig::new().with_max_subscriptions_per_session(2_000);
        let runtime = Runtime::new(RuntimeConfig::new(writer_factory())).unwrap();
        let mut gateway = NetworkGateway::new(runtime, config, auth()).unwrap();
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut client = open_client(&mut gateway, 100_000);
        join_world(&mut gateway, &mut client, max);
        for i in 0..500 {
            let frame = protocol::encode_client(
                &ClientMessage::Subscribe {
                    request_id: i as u64,
                    query: Query::builder("players").predicate_eq("id", i as u64).build().unwrap(),
                },
                max,
            )
            .unwrap();
            client.try_send_frame(frame).unwrap();
        }
        gateway.process_inbound();
        for _ in 0..500 {
            let _ = client.try_recv_frame().unwrap().unwrap();
        }
        bench("500 subscriptions / one tick", iterations / 5, || {
            gateway.step_worlds().unwrap();
            for _ in 0..500 {
                let _ = client.try_recv_frame().unwrap();
            }
        });
    }

    // 9. Reducer calls: client CallReducer → gateway → world tick →
    //    correlated ReducerResult.
    {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut client = open_client(&mut gateway, 4096);
        join_world(&mut gateway, &mut client, max);
        let mut request = 0u64;
        let result = ServerMessage::ReducerResult {
            request_id: 1,
            ok: true,
            value: Some(nexum_core::Value::U64(1)),
            error: None,
        };
        let result_frame = protocol::encode_server(&result, max).unwrap();
        bench("frame encode (server ReducerResult)", iterations, || {
            let _ = protocol::encode_server(&result, max).unwrap();
        });
        bench("frame decode (server ReducerResult)", iterations, || {
            let _ = protocol::decode_server(&result_frame, max).unwrap();
        });
        bench("reducer call roundtrip (call → tick → result)", iterations / 2, || {
            let call = ClientMessage::CallReducer {
                request_id: request,
                reducer: "bump".into(),
                args: nexum_reducer::ReducerArgs::new().insert("amount", 1u64),
            };
            let encoded = protocol::encode_client(&call, max).unwrap();
            client.try_send_frame(encoded).unwrap();
            gateway.process_inbound();
            gateway.step_worlds().unwrap();
            let _ = client.try_recv_frame().unwrap(); // TickUpdate
            let _ = client.try_recv_frame().unwrap(); // ReducerResult
            request += 1;
        });
    }

    // 10. Slow-client isolation: one capped client falls stale while a
    //    healthy client keeps receiving every update.
    {
        let mut gateway = gateway_with(writer_factory());
        gateway.control().create_world(WorldId::from_u64(0), SimulationConfig::new()).unwrap();
        gateway.control().start_world(WorldId::from_u64(0)).unwrap();
        let mut slow = open_client(&mut gateway, 1);
        join_world(&mut gateway, &mut slow, max);
        let mut fast = open_client(&mut gateway, 100_000);
        join_world(&mut gateway, &mut fast, max);
        bench("slow-client isolation (per tick)", iterations, || {
            gateway.step_worlds().unwrap();
            let _ = fast.try_recv_frame().unwrap(); // fast always receives
            let _ = slow.try_recv_frame().unwrap_or(None); // slow: stale-dropped
        });
    }
}
