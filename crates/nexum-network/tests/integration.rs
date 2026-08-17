//! Phase 11 end-to-end integration tests (ADR-011): the full
//! client → gateway → runtime → transaction → WAL → subscription → client
//! path, the failed-tick path (zero authoritative mutation, zero WAL, zero
//! subscription delta, zero realtime update), and crash recovery (recovered
//! history is never replayed as live updates).

use std::sync::Arc;

use nexum_core::{
    row, ColumnType, Error, SystemId, TickId, Value, WorldId,
};
use nexum_network::{
    protocol::{self, ClientMessage, DeltaKind, PROTOCOL_VERSION, ServerMessage},
    Connection, MemoryConnection, MemoryTransport, NetworkConfig, NetworkGateway, Principal,
    TokenAuthenticator,
};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory, WorldLifecycle};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;

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
                            let id = command.payload().and_then(Value::as_u64).unwrap();
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

/// A factory where world 1 fails on its first tick.
fn failing_factory() -> WorldFactory {
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
        if id.as_u64() == 1 {
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(1), "fails", 10, |_ctx, _| {
                        Err(Error::invalid_argument("boom"))
                    })
                    .unwrap(),
                )
                .unwrap();
        }
        Ok(world)
    })
}

fn test_auth() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    auth.add("alice-token", Principal::new(1, "alice")).unwrap();
    Arc::new(auth)
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn connect_client(gateway: &mut NetworkGateway) -> MemoryConnection {
    let (server, client) = MemoryTransport::connect(
        gateway.config().max_queued_inbound_frames(),
        gateway.config().max_queued_outbound_frames(),
    );
    gateway.register_connection(Box::new(server)).unwrap();
    client
}

fn send_client(client: &mut MemoryConnection, message: &ClientMessage, max: u32) {
    let frame = protocol::encode_client(message, max).unwrap();
    client.try_send_frame(frame).unwrap();
}

fn recv_server(client: &mut MemoryConnection, max: u32) -> ServerMessage {
    let frame = client
        .try_recv_frame()
        .unwrap()
        .expect("expected a server frame");
    protocol::decode_server(&frame, max).unwrap()
}

fn join_world(
    gateway: &mut NetworkGateway,
    client: &mut MemoryConnection,
    max: u32,
    world: WorldId,
) {
    send_client(
        client,
        &ClientMessage::Handshake {
            version: PROTOCOL_VERSION,
            name: "tester".into(),
        },
        max,
    );
    gateway.process_inbound();
    assert!(matches!(recv_server(client, max), ServerMessage::HandshakeResponse { .. }));

    send_client(client, &ClientMessage::Authenticate { credentials: "alice-token".into() }, max);
    gateway.process_inbound();
    assert!(matches!(recv_server(client, max), ServerMessage::AuthResult { ok: true, .. }));

    send_client(client, &ClientMessage::AttachWorld { world }, max);
    gateway.process_inbound();
    assert!(matches!(
        recv_server(client, max),
        ServerMessage::AttachResult { ok: true, .. }
    ));
}

fn subscribe_players(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32) {
    send_client(
        client,
        &ClientMessage::Subscribe {
            request_id: 0,
            query: Query::builder("players").build().unwrap(),
        },
        max,
    );
    gateway.process_inbound();
    assert!(matches!(recv_server(client, max), ServerMessage::SubscriptionSnapshot { .. }));
}

// ----------------------------------------------------------- full path + WAL

#[test]
fn full_client_to_wal_to_subscription_path() {
    let dir = temp_dir("nexum-network-integration");
    let runtime = Runtime::new(
        RuntimeConfig::new(input_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    // This test asserts the full change list on TickUpdate.
    let network = NetworkConfig::new().with_tick_update_changes(true);
    let mut gateway = NetworkGateway::new(runtime, network, test_auth()).unwrap();
    let world = WorldId::from_u64(0);
    gateway.control().create_world(world, SimulationConfig::new()).unwrap();
    gateway.control().start_world(world).unwrap();
    let max = gateway.config().max_frame_payload();

    let mut client = connect_client(&mut gateway);
    join_world(&mut gateway, &mut client, max, world);
    subscribe_players(&mut gateway, &mut client, max);

    // Client command → runtime → transaction → WAL.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(1, "spawn", Some(Value::U64(42))).unwrap());
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    gateway.step_worlds().unwrap();

    // Exactly one TickUpdate with exactly one committed change.
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::TickUpdate { world: w, tick, changes, .. } => {
            assert_eq!(w, world);
            assert_eq!(tick, TickId::from_u64(0));
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].new_row().unwrap().get(0), Some(&Value::U64(42)));
        }
        other => panic!("expected TickUpdate, got {other:?}"),
    }
    // ...and exactly one subscription delta.
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::SubscriptionDelta { kind: DeltaKind::Insert, row, .. } if row.is_some()
    ));

    // Durability and observation both fired on the single commit.
    let runtime_metrics = gateway.runtime().metrics();
    assert_eq!(runtime_metrics.wal_appends, 1);
    assert_eq!(runtime_metrics.ticks_succeeded, 1);

    gateway.control().shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ----------------------------------------------------------- failed tick path

#[test]
fn failed_tick_produces_no_wal_no_subscription_delta_no_realtime_update() {
    let dir = temp_dir("nexum-network-failed-tick");
    let runtime = Runtime::new(
        RuntimeConfig::new(failing_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    let mut gateway = NetworkGateway::new(runtime, NetworkConfig::new(), test_auth()).unwrap();
    let world = WorldId::from_u64(1); // fails on its first tick
    gateway.control().create_world(world, SimulationConfig::new()).unwrap();
    gateway.control().start_world(world).unwrap();
    let max = gateway.config().max_frame_payload();

    let mut client = connect_client(&mut gateway);
    join_world(&mut gateway, &mut client, max, world);
    subscribe_players(&mut gateway, &mut client, max);

    gateway.step_worlds().unwrap();
    assert_eq!(gateway.runtime().world_status(world).unwrap().state, WorldLifecycle::Failed);

    // No WAL append, no subscription delta, no realtime update.
    assert_eq!(gateway.runtime().metrics().wal_appends, 0);
    assert!(client.try_recv_frame().unwrap().is_none());

    gateway.control().shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------------- recovery, no replay

#[test]
fn recovery_restores_state_without_replaying_history_as_live_updates() {
    let dir = temp_dir("nexum-network-recovery");

    // Phase A: one committed transaction, then "crash" (shutdown + drop).
    {
        let runtime = Runtime::new(
            RuntimeConfig::new(input_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        let mut gateway = NetworkGateway::new(runtime, NetworkConfig::new(), test_auth()).unwrap();
        let world = WorldId::from_u64(0);
        gateway.control().create_world(world, SimulationConfig::new()).unwrap();
        gateway.control().start_world(world).unwrap();
        let max = gateway.config().max_frame_payload();

        let mut client = connect_client(&mut gateway);
        join_world(&mut gateway, &mut client, max, world);
        subscribe_players(&mut gateway, &mut client, max);

        let mut frame = InputFrame::new(TickId::from_u64(0));
        frame.push(InputCommand::new(1, "spawn", Some(Value::U64(1))).unwrap());
        send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
        gateway.process_inbound();
        gateway.step_worlds().unwrap();
        // Drain the two outbound frames.
        let _ = recv_server(&mut client, max); // TickUpdate
        let _ = recv_server(&mut client, max); // delta

        gateway.control().shutdown().unwrap();
    }

    // Phase B: a fresh gateway recovers the world; history is state, not
    // live events.
    {
        let runtime = Runtime::new(
            RuntimeConfig::new(input_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        let mut gateway = NetworkGateway::new(runtime, NetworkConfig::new(), test_auth()).unwrap();
        let world = WorldId::from_u64(0);
        let report = gateway
            .control()
            .recover_world(world, SimulationConfig::new(), Some(TickId::from_u64(1)))
            .unwrap();
        assert_eq!(report.replayed_txs, 1);
        gateway.control().start_world(world).unwrap();
        let max = gateway.config().max_frame_payload();

        // The client reattaches and resubscribes: it sees an Initial
        // snapshot of the recovered state — not a replay of history.
        let mut client = connect_client(&mut gateway);
        join_world(&mut gateway, &mut client, max, world);
        send_client(
            &mut client,
            &ClientMessage::Subscribe {
                request_id: 0,
                query: Query::builder("players").build().unwrap(),
            },
            max,
        );
        gateway.process_inbound();
        let msg = recv_server(&mut client, max);
        match msg {
            ServerMessage::SubscriptionSnapshot { rows, .. } => {
                assert_eq!(rows.len(), 1);
                assert_eq!(rows[0].row().get(0), Some(&Value::U64(1)));
            }
            other => panic!("expected Initial snapshot, got {other:?}"),
        }

        // Subsequent ticks work normally and deliver only new deltas.
        let mut frame = InputFrame::new(TickId::from_u64(1));
        frame.push(InputCommand::new(1, "spawn", Some(Value::U64(2))).unwrap());
        send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
        gateway.process_inbound();
        gateway.step_worlds().unwrap();

        let msg = recv_server(&mut client, max);
        assert!(matches!(&msg, ServerMessage::TickUpdate { tick, .. } if tick.as_u64() == 1));
        let msg = recv_server(&mut client, max);
        match msg {
            ServerMessage::SubscriptionDelta { kind, row, .. } => {
                assert_eq!(kind, DeltaKind::Insert);
                let row = row.expect("insert delta carries a row");
                assert_eq!(row.row().get(0), Some(&Value::U64(2)));
            }
            other => panic!("expected delta, got {other:?}"),
        }

        gateway.control().shutdown().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}
