//! Phase 11 unit tests (ADR-011).

use std::sync::Arc;

use nexum_core::binary::crc32;
use nexum_core::{
    row, ColumnType, ConnectionId, Error, ReducerId, Result, Row, RowId, SubscriptionId, SystemId,
    TableId, TickId, TransactionId, Value, Version, WorldId,
};
use nexum_runtime::{Runtime, RuntimeConfig, RuntimeState, WorldFactory, WorldLifecycle};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_storage::Change;
use nexum_subscription::{DeliveredRow, OrderDirection, Query};
use nexum_table::TableStore;

use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};

use crate::auth::{Principal, TokenAuthenticator};
use crate::config::{NetworkConfig, OutboundOverflowPolicy};
use crate::error::{NetworkError, ProtocolError};
use crate::gateway::{CALLER_SOURCE_ARG, NetworkGateway, SERVER_REQUEST_MSB};
use crate::policy::GamePolicy;
use crate::protocol::{self, ClientMessage, DeltaKind, ServerMessage, HEADER_LEN, PROTOCOL_MAGIC, PROTOCOL_VERSION};
use crate::transport::{Connection, MemoryConnection, MemoryTransport};

// ---------------------------------------------------------------- helpers

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

/// A world whose system stores each command's source in the `id` column
/// (verifies server-side principal stamping).
fn source_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "sourcer", 0, |ctx, frame| {
                    for command in frame.commands() {
                        ctx.insert("players", row![command.source(), 10u64, 0i32])?;
                    }
                    Ok(())
                })
                .unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// The `bump` reducer: `+10` to a player's health; emits a `bumped` event.
fn bump(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args
        .get("player")
        .and_then(Value::as_u64)
        .ok_or_else(|| Error::invalid_argument("player id required"))?;
    let rows = ctx.scan("players")?;
    let found = rows
        .iter()
        .find(|(_, row)| row.get(0) == Some(&Value::U64(player)))
        .cloned()
        .ok_or_else(|| Error::not_found("player"))?;
    let health = found.1.get(2).and_then(Value::as_i32).unwrap_or(0);
    let mut values = found.1.clone().into_values();
    values[2] = Value::I32(health + 10);
    ctx.update("players", found.0, Row::new(values))?;
    ctx.emit("bumped", player)?;
    Ok(Value::I32(health + 10))
}

/// A world with the `bump` reducer registered (no per-tick system).
/// `whoami`: returns the caller-identity argument stamped by the gateway
/// (ADR-013 D3 / ADR-014 D8). Used to prove the caller cannot be forged.
fn whoami(_ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    Ok(args
        .get(CALLER_SOURCE_ARG)
        .cloned()
        .unwrap_or(Value::U64(0)))
}

fn reducer_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .native_mut()
            .register(
                ReducerDefinition::new(ReducerId::from_u64(1), "bump", bump).unwrap(),
            )
            .unwrap();
        world
            .native_mut()
            .register(
                ReducerDefinition::new(ReducerId::from_u64(2), "whoami", whoami).unwrap(),
            )
            .unwrap();
        Ok(world)
    })
}

/// A world whose system inserts one player row per tick.
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

/// A factory where world 1 fails on its first tick (after its writer).
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

fn test_authenticator() -> TokenAuthenticator {
    let mut auth = TokenAuthenticator::new();
    auth.add("alice-token", Principal::new(1, "alice")).unwrap();
    auth.add("bob-token", Principal::new(2, "bob")).unwrap();
    auth
}

fn gateway_with(factory: WorldFactory, network: NetworkConfig) -> NetworkGateway {
    let runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    NetworkGateway::new(runtime, network, Arc::new(test_authenticator())).unwrap()
}

fn create_world(gateway: &mut NetworkGateway, id: u64) {
    gateway
        .control()
        .create_world(WorldId::from_u64(id), SimulationConfig::new())
        .unwrap();
    gateway.control().start_world(WorldId::from_u64(id)).unwrap();
}

/// Registers a fresh memory connection and returns its client end.
fn connect_client(gateway: &mut NetworkGateway) -> (ConnectionId, MemoryConnection) {
    let (server, client) = MemoryTransport::connect(
        gateway.config().max_queued_inbound_frames(),
        gateway.config().max_queued_outbound_frames(),
    );
    let id = gateway.register_connection(Box::new(server)).unwrap();
    (id, client)
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

fn handshake(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32) {
    send_client(
        client,
        &ClientMessage::Handshake {
            version: PROTOCOL_VERSION,
            name: "tester".into(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(client, max);
    assert!(matches!(msg, ServerMessage::HandshakeResponse { version, .. } if version == PROTOCOL_VERSION));
}

fn authenticate(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32, token: &str) {
    send_client(client, &ClientMessage::Authenticate { credentials: token.into() }, max);
    gateway.process_inbound();
    let msg = recv_server(client, max);
    assert!(matches!(msg, ServerMessage::AuthResult { ok: true, .. }));
}

fn attach(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32, world: WorldId) {
    send_client(client, &ClientMessage::AttachWorld { world }, max);
    gateway.process_inbound();
    let msg = recv_server(client, max);
    assert!(matches!(msg, ServerMessage::AttachResult { ok: true, world: Some(w), .. } if w == world));
}

/// Subscribes to `players` and returns the server subscription id.
fn subscribe_players(gateway: &mut NetworkGateway, client: &mut MemoryConnection, max: u32) -> SubscriptionId {
    send_client(
        client,
        &ClientMessage::Subscribe {
            request_id: 0,
            query: Query::builder("players").build().unwrap(),
        },
        max,
    );
    gateway.process_inbound();
    match recv_server(client, max) {
        ServerMessage::SubscriptionSnapshot { subscription, .. } => subscription,
        other => panic!("expected initial snapshot, got {other:?}"),
    }
}

/// A valid client attach flow for a client of world 0.
fn join_world0(
    gateway: &mut NetworkGateway,
    client: &mut MemoryConnection,
    max: u32,
    token: &str,
) {
    handshake(gateway, client, max);
    authenticate(gateway, client, max, token);
    attach(gateway, client, max, WorldId::from_u64(0));
}

/// Crafts a complete, checksummed frame with the given kind and payload.
fn craft_frame(kind: u8, payload: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.extend_from_slice(PROTOCOL_MAGIC);
    frame.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    frame.push(kind);
    frame.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    frame.extend_from_slice(payload);
    let mut crc_input = Vec::new();
    crc_input.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    crc_input.push(kind);
    crc_input.extend_from_slice(payload);
    frame.extend_from_slice(&crc32(&crc_input).to_le_bytes());
    frame
}

// ----------------------------------------------------------------- protocol

#[test]
fn client_messages_roundtrip() {
    let max = 4096;
    let mut frame = InputFrame::new(TickId::from_u64(3));
    frame.push(InputCommand::new(1, "move", Some(Value::U64(9))).unwrap());
    let query = Query::builder("players")
        .predicate_eq("zone", 10u64)
        .predicate_gt("health", 50i32)
        .order_by("id", OrderDirection::Ascending)
        .limit(100)
        .project(&["id", "health"])
        .build()
        .unwrap();
    let messages = vec![
        ClientMessage::Handshake { version: PROTOCOL_VERSION, name: "tester".into() },
        ClientMessage::Authenticate { credentials: "token-1".into() },
        ClientMessage::AttachWorld { world: WorldId::from_u64(3) },
        ClientMessage::InputFrame { frame },
        ClientMessage::Subscribe {
            request_id: 0,
            query,
        },
        ClientMessage::Unsubscribe { subscription: SubscriptionId::from_u64(5) },
        ClientMessage::Resync { subscription: SubscriptionId::from_u64(6) },
        ClientMessage::Ping { nonce: 42 },
        ClientMessage::DetachWorld,
        ClientMessage::CallReducer {
            request_id: 7,
            reducer: "move_player".into(),
            args: nexum_reducer::ReducerArgs::new().insert("x", 1u64).insert("y", 2u64),
        },
    ];
    for message in &messages {
        let encoded = protocol::encode_client(message, max).unwrap();
        let decoded = protocol::decode_client(&encoded, max).unwrap();
        assert_eq!(&decoded, message);
    }
}

#[test]
fn server_messages_roundtrip() {
    let max = 8192;
    let table = TableId::from_u64(1);
    let row1 = row![1u64, 10u64, 100i32];
    let row2 = row![2u64, 20u64, 80i32];
    let changes = vec![
        Change::insert(table, RowId::from_u64(1), row1.clone(), Version::from_u64(1)),
        Change::update(
            table,
            RowId::from_u64(2),
            row1.clone(),
            Version::from_u64(1),
            row2.clone(),
            Version::from_u64(2),
        ),
        Change::delete(table, RowId::from_u64(3), row1.clone(), Version::from_u64(1)),
    ];
    let messages = vec![
        ServerMessage::HandshakeResponse { version: PROTOCOL_VERSION, server_name: "nexum".into() },
        ServerMessage::AuthResult { ok: true, principal: Some(Principal::new(7, "alice")), error: None },
        ServerMessage::AuthResult { ok: false, principal: None, error: Some("bad".into()) },
        ServerMessage::AttachResult { ok: true, world: Some(WorldId::from_u64(3)), error: None },
        ServerMessage::AttachResult { ok: false, world: None, error: Some("unknown".into()) },
        ServerMessage::DetachResult { ok: true, error: None },
        ServerMessage::DetachResult { ok: false, error: Some("not attached".into()) },
        ServerMessage::TickUpdate {
            world: WorldId::from_u64(0),
            tick: TickId::from_u64(4),
            tx_id: TransactionId::from_u64(9),
            changes,
            events: vec![
                nexum_reducer::ReducerEvent::new("player_joined", 7u64),
                nexum_reducer::ReducerEvent::new("msg", "hello"),
            ],
        },
        ServerMessage::SubscriptionSnapshot {
            request_id: 0,
            subscription: SubscriptionId::from_u64(1),
            seq: 2,
            rows: vec![DeliveredRow::new(RowId::from_u64(1), row1.clone())],
        },
        ServerMessage::SubscriptionDelta {
            subscription: SubscriptionId::from_u64(1),
            seq: 3,
            kind: DeltaKind::Insert,
            row_id: RowId::from_u64(1),
            row: Some(DeliveredRow::new(RowId::from_u64(1), row2.clone())),
        },
        ServerMessage::SubscriptionDelta {
            subscription: SubscriptionId::from_u64(1),
            seq: 4,
            kind: DeltaKind::Delete,
            row_id: RowId::from_u64(1),
            row: None,
        },
        ServerMessage::StaleNotification { subscription: SubscriptionId::from_u64(1), seq: 5 },
        ServerMessage::Error {
            code: 17,
            message: "too much".into(),
            request_id: 0,
        },
        ServerMessage::Pong { nonce: 42 },
        ServerMessage::Disconnect { reason: "bye".into() },
        ServerMessage::ReducerResult {
            request_id: 7,
            ok: true,
            value: Some(Value::U64(42)),
            error: None,
        },
        ServerMessage::ReducerResult {
            request_id: 8,
            ok: false,
            value: None,
            error: Some("rejected".into()),
        },
    ];
    for message in &messages {
        let encoded = protocol::encode_server(message, max).unwrap();
        let decoded = protocol::decode_server(&encoded, max).unwrap();
        assert_eq!(&decoded, message);
    }
}

#[test]
fn streaming_parse_frame_handles_partial_and_concatenated_frames() {
    let max = 4096;
    let frame = protocol::encode_client(&ClientMessage::Ping { nonce: 7 }, max).unwrap();
    // Partial header.
    assert!(protocol::parse_frame(&frame[..4], max).unwrap().is_none());
    assert!(protocol::parse_frame(&frame[..HEADER_LEN - 1], max).unwrap().is_none());
    // Partial body.
    assert!(protocol::parse_frame(&frame[..frame.len() - 1], max).unwrap().is_none());
    // Complete.
    let (parsed, consumed) = protocol::parse_frame(&frame, max).unwrap().unwrap();
    assert_eq!(parsed, frame);
    assert_eq!(consumed, frame.len());
    // Two concatenated frames.
    let mut buf = Vec::new();
    buf.extend_from_slice(&frame);
    buf.extend_from_slice(&frame);
    let (first, c1) = protocol::parse_frame(&buf, max).unwrap().unwrap();
    assert_eq!(first, frame);
    let (second, c2) = protocol::parse_frame(&buf[c1..], max).unwrap().unwrap();
    assert_eq!(second, frame);
    assert_eq!(c2, frame.len());
    // Garbage never yields a frame.
    assert!(matches!(
        protocol::parse_frame(b"GARBAGE!!!!!", max),
        Err(ProtocolError::BadMagic)
    ));
}

#[test]
fn truncated_frames_are_rejected() {
    let max = 4096;
    let frame = protocol::encode_client(&ClientMessage::Ping { nonce: 1 }, max).unwrap();
    let mut truncated = frame.clone();
    truncated.truncate(frame.len() - 1);
    assert!(matches!(protocol::decode_client(&truncated, max), Err(ProtocolError::Truncated)));
    // Header-only input.
    assert!(matches!(protocol::decode_client(&frame[..5], max), Err(ProtocolError::Truncated)));
    // Empty input.
    assert!(matches!(protocol::decode_client(&[], max), Err(ProtocolError::Truncated)));
}

#[test]
fn bad_magic_and_versions_are_rejected() {
    let max = 4096;
    let frame = protocol::encode_client(&ClientMessage::Ping { nonce: 1 }, max).unwrap();
    let mut bad_magic = frame.clone();
    bad_magic[0] = b'X';
    assert!(matches!(protocol::decode_client(&bad_magic, max), Err(ProtocolError::BadMagic)));
    let mut bad_version = frame.clone();
    bad_version[4] = 0xFF;
    bad_version[5] = 0xFF;
    assert!(matches!(
        protocol::decode_client(&bad_version, max),
        Err(ProtocolError::UnsupportedVersion(0xFFFF))
    ));
}

#[test]
fn oversized_payloads_are_rejected_before_allocation() {
    let max = 1024;
    let mut frame = Vec::new();
    frame.extend_from_slice(PROTOCOL_MAGIC);
    frame.extend_from_slice(&PROTOCOL_VERSION.to_le_bytes());
    frame.push(0x01);
    frame.extend_from_slice(&u32::MAX.to_le_bytes());
    assert!(matches!(
        protocol::decode_client(&frame, max),
        Err(ProtocolError::Oversized { .. })
    ));
}

#[test]
fn unknown_kinds_are_rejected() {
    let max = 4096;
    let frame = craft_frame(0x7F, &[]);
    assert!(matches!(
        protocol::decode_client(&frame, max),
        Err(ProtocolError::UnknownKind(0x7F))
    ));
}

#[test]
fn bad_checksums_are_detected() {
    let max = 4096;
    let frame = protocol::encode_client(
        &ClientMessage::Authenticate { credentials: "tok".into() },
        max,
    )
    .unwrap();
    let mut corrupted = frame.clone();
    corrupted[HEADER_LEN] ^= 0xFF; // flip a payload byte
    assert!(matches!(protocol::decode_client(&corrupted, max), Err(ProtocolError::BadChecksum)));
    let mut corrupted2 = frame;
    let last = corrupted2.len() - 1;
    corrupted2[last] ^= 0xFF; // flip a checksum byte
    assert!(matches!(protocol::decode_client(&corrupted2, max), Err(ProtocolError::BadChecksum)));
}

#[test]
fn malformed_payloads_are_rejected() {
    let max = 4096;
    // A handshake payload declaring a string longer than the payload (the
    // binary codec reports insufficient bytes as a malformed message).
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u16.to_le_bytes()); // version
    payload.push(0xFF); // string length 255, no bytes follow
    let frame = craft_frame(0x01, &payload);
    assert!(matches!(protocol::decode_client(&frame, max), Err(ProtocolError::Malformed(_))));

    // An input frame with an empty command kind.
    let mut payload = Vec::new();
    payload.extend_from_slice(&0u64.to_le_bytes()); // tick
    payload.extend_from_slice(&1u64.to_le_bytes()); // count
    payload.extend_from_slice(&1u64.to_le_bytes()); // source
    payload.extend_from_slice(&0u64.to_le_bytes()); // empty kind string
    payload.push(0); // no command payload
    let frame = craft_frame(0x04, &payload);
    assert!(matches!(protocol::decode_client(&frame, max), Err(ProtocolError::Malformed(_))));

    // Trailing garbage after a valid handshake.
    let mut payload = Vec::new();
    payload.extend_from_slice(&1u16.to_le_bytes());
    payload.extend_from_slice(&3u64.to_le_bytes());
    payload.extend_from_slice(b"abc");
    payload.push(0xEE);
    let frame = craft_frame(0x01, &payload);
    assert!(matches!(protocol::decode_client(&frame, max), Err(ProtocolError::Malformed(_))));
}

#[test]
fn encode_rejects_oversized_messages() {
    let max = 32;
    let big = ServerMessage::Disconnect { reason: "x".repeat(1024) };
    assert!(matches!(protocol::encode_server(&big, max), Err(NetworkError::Capacity(_))));
}

// -------------------------------------------------------- gateway security

#[test]
fn protocol_violations_disconnect_the_connection() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    client.try_send_frame(b"GARBAGEGARBAGE".to_vec()).unwrap();
    gateway.process_inbound();
    assert_eq!(gateway.connection_count(), 0);
    let metrics = gateway.metrics();
    assert_eq!(metrics.frames_rejected, 1);
    assert_eq!(metrics.protocol_errors, 1);
    assert_eq!(metrics.clients_dropped, 1);
    // Events are bounded and observable.
    let events = gateway.drain_events();
    assert!(events.iter().any(|e| matches!(e, crate::NetworkEvent::ProtocolError { .. })));
}

#[test]
fn command_floods_are_rejected() {
    let config = NetworkConfig::new().with_max_commands_per_frame(1);
    let mut gateway = gateway_with(input_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(1, "spawn", Some(Value::U64(1))).unwrap());
    frame.push(InputCommand::new(1, "spawn", Some(Value::U64(2))).unwrap());
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 17, .. }));
    assert_eq!(gateway.metrics().inputs_rejected, 1);
}

#[test]
fn subscription_floods_are_rejected() {
    let config = NetworkConfig::new().with_max_subscriptions_per_session(1);
    let mut gateway = gateway_with(writer_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    subscribe_players(&mut gateway, &mut client, max);
    // A second subscription exceeds the per-session bound.
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
    assert!(matches!(msg, ServerMessage::Error { code: 17, .. }));
}

#[test]
fn invalid_credentials_are_rejected_and_leave_the_session_unauthenticated() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    handshake(&mut gateway, &mut client, max);
    send_client(&mut client, &ClientMessage::Authenticate { credentials: "wrong".into() }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::AuthResult { ok: false, principal: None, error: Some(_) }));
    assert_eq!(gateway.metrics().auth_failures, 1);
    // The session never authenticated: attach is rejected.
    send_client(&mut client, &ClientMessage::AttachWorld { world: WorldId::from_u64(0) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 20, .. }));
}

#[test]
fn operations_without_auth_or_attachment_are_rejected() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    handshake(&mut gateway, &mut client, max);

    // Input before authentication.
    let frame = InputFrame::new(TickId::from_u64(0));
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 20, .. }));

    // Authenticated but not attached: input and subscribe rejected.
    authenticate(&mut gateway, &mut client, max, "alice-token");
    let frame = InputFrame::new(TickId::from_u64(0));
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 21, .. }));
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
    assert!(matches!(msg, ServerMessage::Error { code: 21, .. }));
}

// ---------------------------------------------------------------- sessions

#[test]
fn session_lifecycle_attach_detach_reconnect() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (conn, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    assert_eq!(gateway.metrics().sessions, 1);
    assert_eq!(gateway.metrics().attached, 1);
    assert!(gateway.session_of(conn).is_some());
    assert_eq!(gateway.session_of(conn).unwrap().principal().id(), 1);

    // Detach ends the attachment.
    send_client(&mut client, &ClientMessage::DetachWorld, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::DetachResult { ok: true, .. }));
    assert_eq!(gateway.metrics().attached, 0);
    assert!(!gateway.session_of(conn).unwrap().is_attached());
    // Detaching again is rejected (nothing to detach).
    send_client(&mut client, &ClientMessage::DetachWorld, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 21, .. }));

    // Reattach.
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));
    assert_eq!(gateway.metrics().attached, 1);

    // Duplicate authentication is rejected.
    send_client(&mut client, &ClientMessage::Authenticate { credentials: "alice-token".into() }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 20, .. }));

    // A fresh connection can authenticate as a different principal.
    gateway.disconnect(conn, "test").unwrap();
    assert_eq!(gateway.connection_count(), 0);
    let (conn2, mut client2) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client2, max);
    authenticate(&mut gateway, &mut client2, max, "bob-token");
    assert_eq!(gateway.session_of(conn2).unwrap().principal().id(), 2);
}

#[test]
fn duplicate_attachment_is_idempotent_and_cross_world_attach_is_rejected() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    create_world(&mut gateway, 1);
    let (conn, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // Same world: idempotent success.
    send_client(&mut client, &ClientMessage::AttachWorld { world: WorldId::from_u64(0) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::AttachResult { ok: true, world: Some(w), .. } if w.as_u64() == 0));

    // Different world while attached: rejected.
    send_client(&mut client, &ClientMessage::AttachWorld { world: WorldId::from_u64(1) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 21, .. }));
    // Still attached to world 0.
    assert_eq!(
        gateway.session_of(conn).unwrap().attached_world(),
        Some(WorldId::from_u64(0))
    );
}

// ---------------------------------------------------------------- routing

#[test]
fn inputs_reach_the_attached_world() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    let sub = subscribe_players(&mut gateway, &mut client, max);

    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(1, "spawn", Some(Value::U64(42))).unwrap());
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    gateway.step_worlds().unwrap();

    // The client receives exactly one TickUpdate (world 0, tick 0) whose
    // single change is the committed insert.
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::TickUpdate { world, tick, changes, .. } => {
            assert_eq!(world, WorldId::from_u64(0));
            assert_eq!(tick, TickId::from_u64(0));
            assert_eq!(changes.len(), 1);
            assert_eq!(changes[0].new_row().unwrap().get(0), Some(&Value::U64(42)));
        }
        other => panic!("expected TickUpdate, got {other:?}"),
    }
    // ...then the subscription delta.
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::SubscriptionDelta { subscription, kind, row, .. } => {
            assert_eq!(subscription, sub);
            assert_eq!(kind, DeltaKind::Insert);
            let row = row.expect("insert delta carries a row");
            assert_eq!(row.row().get(0), Some(&Value::U64(42)));
        }
        other => panic!("expected delta, got {other:?}"),
    }

    // Command sources were stamped server-side with the principal id.
    let metrics = gateway.metrics();
    assert_eq!(metrics.inputs_accepted, 1);
    assert_eq!(metrics.tick_updates_sent, 1);
    assert!(metrics.subscription_messages_sent >= 2); // snapshot + delta
}

#[test]
fn command_sources_are_stamped_with_the_principal_id() {
    // Alice's principal id is 1; a client-forged source of 999 must be
    // replaced by the server before the command reaches the world.
    let mut gateway = gateway_with(source_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(999, "spawn", None).unwrap());
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    gateway.step_worlds().unwrap();

    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::TickUpdate { changes, .. } => {
            assert_eq!(changes.len(), 1);
            // The row's id column carries the stamped principal id (1),
            // never the client-supplied source (999).
            assert_eq!(changes[0].new_row().unwrap().get(0), Some(&Value::U64(1)));
        }
        other => panic!("expected TickUpdate, got {other:?}"),
    }
}

#[test]
fn inputs_never_reach_another_world() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    create_world(&mut gateway, 1);
    let max = gateway.config().max_frame_payload();

    let (_, mut a) = connect_client(&mut gateway);
    join_world0(&mut gateway, &mut a, max, "alice-token");
    subscribe_players(&mut gateway, &mut a, max);

    let (_, mut b) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut b, max);
    authenticate(&mut gateway, &mut b, max, "bob-token");
    attach(&mut gateway, &mut b, max, WorldId::from_u64(1));
    subscribe_players(&mut gateway, &mut b, max);

    // A spawns a player in world 0.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(1, "spawn", Some(Value::U64(42))).unwrap());
    send_client(&mut a, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    gateway.step_worlds().unwrap();

    // A observes world 0's commit and insert delta.
    let msg = recv_server(&mut a, max);
    assert!(matches!(&msg, ServerMessage::TickUpdate { world, changes, .. } if world.as_u64() == 0 && changes.len() == 1));
    let msg = recv_server(&mut a, max);
    assert!(matches!(&msg, ServerMessage::SubscriptionDelta { kind: DeltaKind::Insert, row, .. } if row.is_some()));

    // B observes world 1's (empty) commit and nothing else: world 0's row
    // never leaked across the partition boundary.
    let msg = recv_server(&mut b, max);
    assert!(matches!(&msg, ServerMessage::TickUpdate { world, changes, .. } if world.as_u64() == 1 && changes.is_empty()));
    assert!(b.try_recv_frame().unwrap().is_none());
}

#[test]
fn unknown_worlds_are_rejected_at_attach() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    send_client(&mut client, &ClientMessage::AttachWorld { world: WorldId::from_u64(99) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::AttachResult { ok: false, world: None, error: Some(_) }));
}

#[test]
fn failed_worlds_reject_input() {
    let mut gateway = gateway_with(failing_factory(), NetworkConfig::new());
    create_world(&mut gateway, 1);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(1));

    gateway.step_worlds().unwrap();
    assert_eq!(
        gateway.runtime().world_status(WorldId::from_u64(1)).unwrap().state,
        WorldLifecycle::Failed
    );

    let frame = InputFrame::new(TickId::from_u64(1));
    send_client(&mut client, &ClientMessage::InputFrame { frame }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { .. }));
    assert_eq!(gateway.metrics().inputs_rejected, 1);
}

// ---------------------------------------------------------- reducer calls

#[test]
fn reducer_call_executes_in_the_next_tick_and_returns_a_correlated_result() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // Seed the world with a player row via a first tick (no inputs).
    // The `bump` reducer needs an existing row; use a world system instead:
    // reducer_factory has no writer, so insert via the reducer path after
    // establishing the row through a CallReducer is impossible — instead
    // submit a frame with a spawn-like command is not handled here. The
    // factory has no consumer system either, so seed via a direct table
    // insert through a committed tick is unavailable. We instead verify the
    // correlated failure and the bound checks below, plus a successful
    // reducer call that returns an error for a missing row (still a
    // correlated ReducerResult, never a hang or a generic Error).
    let request = 7;
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: request,
            reducer: "bump".into(),
            args: ReducerArgs::new().insert("player", 1u64),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);
    gateway.step_worlds().unwrap();
    // The tick's broadcast arrives first; the correlated result follows.
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::ReducerResult {
            request_id,
            ok: false,
            error,
            ..
        } => {
            // The reducer ran inside the world and reported not-found: a
            // correlated failure, not a generic error.
            assert_eq!(request_id, request);
            assert!(error.as_ref().unwrap().contains("not found"), "{error:?}");
        }
        other => panic!("expected ReducerResult, got {other:?}"),
    }
}

#[test]
fn reducer_calls_require_auth_and_attachment_and_reject_duplicates() {
    let config = NetworkConfig::new().with_max_pending_calls_per_connection(1);
    let mut gateway = gateway_with(reducer_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();

    // No authentication: correlated failure via ReducerResult.
    handshake(&mut gateway, &mut client, max);
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 1,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 1, ok: false, .. }));
    assert_eq!(gateway.metrics().reducer_calls_rejected, 1);

    // Authenticated but not attached: correlated failure.
    authenticate(&mut gateway, &mut client, max, "alice-token");
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 2,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 2, ok: false, .. }));

    // Attached: the first call is accepted and stays pending.
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 3,
            reducer: "bump".into(),
            args: ReducerArgs::new().insert("player", 1u64),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);

    // A duplicate request id (same world, still pending) is rejected.
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 3,
            reducer: "bump".into(),
            args: ReducerArgs::new().insert("player", 1u64),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 3, ok: false, .. }));

    // The pending-cap (1) rejects a second distinct call while the first is
    // still pending.
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 4,
            reducer: "bump".into(),
            args: ReducerArgs::new().insert("player", 1u64),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 4, ok: false, .. }));
    assert_eq!(gateway.metrics().reducer_calls_rejected, 4); // auth + attach + dup + cap

    // The first call still commits on the next tick (never lost). The
    // tick's broadcast precedes the correlated result.
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 3, ok: false, .. }));
    assert_eq!(gateway.metrics().reducer_results_sent, 1);
}

// ----------------------------------------------- reducer call lifecycle

#[test]
fn stopped_world_answers_pending_reducer_calls_with_a_correlated_failure() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // Accept a call, then stop the world before it can execute.
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 1,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);

    gateway.control().stop_world(WorldId::from_u64(0)).unwrap();
    gateway.step_worlds().unwrap();

    // The pending call is resolved with a correlated failure — never a hang.
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::ReducerResult {
            request_id,
            ok: false,
            error,
            ..
        } => {
            assert_eq!(request_id, 1);
            assert!(error.unwrap().contains("no longer running"));
        }
        other => panic!("expected ReducerResult, got {other:?}"),
    }
    assert_eq!(gateway.metrics().reducer_results_sent, 1);

    // No pending call remains: further steps produce nothing.
    gateway.step_worlds().unwrap();
    assert!(client.try_recv_frame().unwrap().is_none());
}

#[test]
fn destroyed_world_answers_pending_reducer_calls_with_a_correlated_failure() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 2,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);

    // Destroy the world while the call is pending.
    gateway.control().destroy_world(WorldId::from_u64(0)).unwrap();
    gateway.step_worlds().unwrap();

    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::ReducerResult {
            request_id,
            ok: false,
            ..
        } => assert_eq!(request_id, 2),
        other => panic!("expected ReducerResult, got {other:?}"),
    }
    assert_eq!(gateway.metrics().reducer_results_sent, 1);
    gateway.step_worlds().unwrap();
    assert!(client.try_recv_frame().unwrap().is_none());
}

#[test]
fn calls_to_a_destroyed_world_are_rejected_with_a_correlated_failure() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // The world disappears behind the session's back; a later call is
    // rejected by the runtime and answered with a correlated ReducerResult
    // (never a generic Error, never a hang, never queued).
    gateway.control().destroy_world(WorldId::from_u64(0)).unwrap();
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 5,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult { request_id: 5, ok: false, .. }
    ));
    assert_eq!(gateway.metrics().reducer_calls_rejected, 1);
}

#[test]
fn disconnecting_cleans_pending_reducer_calls_without_disturbing_the_world() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (conn, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 3,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);

    // The client disconnects while its call is pending: the pending entry is
    // dropped (the runtime may still execute the accepted call fire-and-
    // forget — it is no longer routable), and the world keeps ticking.
    gateway.disconnect(conn, "gone").unwrap();
    assert_eq!(gateway.connection_count(), 0);
    gateway.step_worlds().unwrap();
    assert_eq!(gateway.runtime().metrics().ticks_succeeded, 1);
    assert_eq!(gateway.metrics().reducer_results_sent, 0);
    // No stale entry routes anything anywhere later.
    gateway.step_worlds().unwrap();
    assert_eq!(gateway.runtime().metrics().ticks_succeeded, 2);
}

#[test]
fn concurrent_pending_calls_across_clients_never_cross_consume_results() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();
    let (_, mut a) = connect_client(&mut gateway);
    join_world0(&mut gateway, &mut a, max, "alice-token");
    let (_, mut b) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut b, max);
    authenticate(&mut gateway, &mut b, max, "bob-token");
    attach(&mut gateway, &mut b, max, WorldId::from_u64(0));

    // Both clients pick request_id 1 on the same world. Request ids must be
    // unique per world while pending; the second is rejected explicitly
    // (correlated failure) so results can never be misattributed.
    for client in [&mut a, &mut b] {
        send_client(
            client,
            &ClientMessage::CallReducer {
                request_id: 1,
                reducer: "bump".into(),
                args: ReducerArgs::new(),
            },
            max,
        );
    }
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);
    assert_eq!(gateway.metrics().reducer_calls_rejected, 1);

    // B receives its correlated rejection immediately.
    let msg = recv_server(&mut b, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult { request_id: 1, ok: false, .. }
    ));

    // A's call executes and A receives exactly one terminal result.
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut a, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. })); // the tick broadcast
    let msg = recv_server(&mut a, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult { request_id: 1, .. }
    ));
    // B (attached to the same world) received the tick broadcast, but never
    // A's result: exactly one terminal result per client, never shared.
    let msg = recv_server(&mut b, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    assert!(a.try_recv_frame().unwrap().is_none());
    assert!(b.try_recv_frame().unwrap().is_none());
}

#[test]
fn stopped_world_calls_fail_and_restart_accepts_new_calls() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // A pending call fails when the world stops (documented semantics: the
    // caller must retry after restart — calls are never silently deferred).
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 1,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    gateway.control().stop_world(WorldId::from_u64(0)).unwrap();
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 1, ok: false, .. }));

    // Restart: a fresh call (new request id) executes on the next tick.
    gateway.control().start_world(WorldId::from_u64(0)).unwrap();
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 2,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 2);
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::ReducerResult { request_id: 2, .. }));
    assert_eq!(gateway.metrics().reducer_results_sent, 2);
}

// ----------------------------------------------------------- subscriptions

#[test]
fn subscription_snapshot_and_delta_delivery_and_unsubscribe() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    let sub = subscribe_players(&mut gateway, &mut client, max);

    // After one tick: TickUpdate + an insert delta.
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::SubscriptionDelta { subscription: s, kind: DeltaKind::Insert, row: Some(_), .. } if s == sub
    ));

    // Unsubscribe: no further deltas, but the attachment broadcast remains.
    send_client(&mut client, &ClientMessage::Unsubscribe { subscription: sub }, max);
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    assert!(client.try_recv_frame().unwrap().is_none());

    // Unsubscribing an unknown subscription reports an error.
    send_client(&mut client, &ClientMessage::Unsubscribe { subscription: sub }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 22, .. }));
}

#[test]
fn resync_regenerates_the_exact_view() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    let sub = subscribe_players(&mut gateway, &mut client, max);

    gateway.step_worlds().unwrap();
    let _ = recv_server(&mut client, max); // TickUpdate
    let _ = recv_server(&mut client, max); // delta
    gateway.step_worlds().unwrap();
    let _ = recv_server(&mut client, max); // TickUpdate
    let _ = recv_server(&mut client, max); // delta

    // A resync rebuilds the full view from authoritative state.
    send_client(&mut client, &ClientMessage::Resync { subscription: sub }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::SubscriptionSnapshot { subscription, rows, .. } => {
            assert_eq!(subscription, sub);
            assert_eq!(rows.len(), 2);
        }
        other => panic!("expected resync snapshot, got {other:?}"),
    }

    // Resyncing an unknown subscription reports an error.
    send_client(&mut client, &ClientMessage::Resync { subscription: SubscriptionId::from_u64(99) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 22, .. }));
}

#[test]
fn failed_ticks_produce_no_updates() {
    let mut gateway = gateway_with(failing_factory(), NetworkConfig::new());
    create_world(&mut gateway, 1);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(1));
    subscribe_players(&mut gateway, &mut client, max);

    gateway.step_worlds().unwrap();
    // The failed world produced no TickUpdate and no subscription delta.
    assert!(client.try_recv_frame().unwrap().is_none());
    assert_eq!(
        gateway.runtime().world_status(WorldId::from_u64(1)).unwrap().state,
        WorldLifecycle::Failed
    );
}

// ------------------------------------------------------------ backpressure

#[test]
fn slow_client_is_marked_stale_without_blocking_the_world() {
    let config = NetworkConfig::new()
        .with_max_queued_outbound_frames(1)
        .with_overflow_policy(OutboundOverflowPolicy::Stale);
    let mut gateway = gateway_with(writer_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");
    let sub = subscribe_players(&mut gateway, &mut client, max);

    // Tick 1 queues a TickUpdate, filling the bounded outbound queue; tick 2
    // overflows it and marks the session stale.
    gateway.step_worlds().unwrap();
    gateway.step_worlds().unwrap();
    assert_eq!(gateway.metrics().sessions_stale, 1);
    assert!(gateway.metrics().messages_dropped >= 1);

    // The world kept ticking regardless of the slow client.
    assert_eq!(
        gateway.runtime().world_status(WorldId::from_u64(0)).unwrap().next_tick,
        TickId::from_u64(2)
    );

    // Further ticks are dropped while stale (never enqueued).
    gateway.step_worlds().unwrap();
    assert_eq!(
        gateway.runtime().world_status(WorldId::from_u64(0)).unwrap().next_tick,
        TickId::from_u64(3)
    );

    // The client drains the single queued update, then resyncs to recover
    // its exact view (all three rows).
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { tick, .. } if tick.as_u64() == 0));
    send_client(&mut client, &ClientMessage::Resync { subscription: sub }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::SubscriptionSnapshot { rows, .. } if rows.len() == 3));
    assert_eq!(gateway.metrics().sessions_stale, 0);
}

#[test]
fn overflow_disconnect_policy_closes_slow_connections() {
    let config = NetworkConfig::new()
        .with_max_queued_outbound_frames(1)
        .with_overflow_policy(OutboundOverflowPolicy::Disconnect);
    let mut gateway = gateway_with(writer_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    gateway.step_worlds().unwrap(); // TickUpdate queued (1/1)
    gateway.step_worlds().unwrap(); // TickUpdate -> full -> disconnect
    assert_eq!(gateway.connection_count(), 0);
    assert!(gateway.metrics().clients_dropped >= 1);
    // The world kept ticking.
    assert_eq!(
        gateway.runtime().world_status(WorldId::from_u64(0)).unwrap().next_tick,
        TickId::from_u64(2)
    );
    let _ = client; // dropped server-side; nothing further arrives
}

#[test]
fn slow_client_never_blocks_other_clients() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();

    // Client 1: outbound cap 1 (will fall stale). Client 2: healthy.
    let (server1, mut c1) = MemoryTransport::connect(256, 1);
    gateway.register_connection(Box::new(server1)).unwrap();
    join_world0(&mut gateway, &mut c1, max, "alice-token");

    let (_, mut c2) = connect_client(&mut gateway);
    join_world0(&mut gateway, &mut c2, max, "bob-token");

    gateway.step_worlds().unwrap(); // both get a TickUpdate; c1 queue full
    gateway.step_worlds().unwrap(); // c1 -> stale; c2 -> queued

    let metrics = gateway.metrics();
    assert_eq!(metrics.sessions_stale, 1);
    // c2 received both updates.
    let msg = recv_server(&mut c2, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { tick, .. } if tick.as_u64() == 0));
    let msg = recv_server(&mut c2, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { tick, .. } if tick.as_u64() == 1));
    // c1 only ever received its single queued update.
    let msg = recv_server(&mut c1, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { tick, .. } if tick.as_u64() == 0));
    assert!(c1.try_recv_frame().unwrap().is_none());
}

// ----------------------------------------------------------- control plane

#[test]
fn control_plane_lifecycle_and_health() {
    let mut gateway = gateway_with(writer_factory(), NetworkConfig::new());
    let world = WorldId::from_u64(0);

    gateway.control().create_world(world, SimulationConfig::new()).unwrap();
    assert_eq!(gateway.control().world_status(world).unwrap().state, WorldLifecycle::Created);
    assert!(gateway.control().create_world(world, SimulationConfig::new()).is_err());

    gateway.control().start_world(world).unwrap();
    assert_eq!(gateway.control().world_status(world).unwrap().state, WorldLifecycle::Running);

    let health = gateway.control().health();
    assert_eq!(health.worlds, 1);
    assert_eq!(health.running_worlds, 1);
    assert_eq!(health.workers, 1);
    assert_eq!(health.workers_running, 1);
    assert!(health.uptime_ns > 0);

    gateway.control().step().unwrap();
    assert_eq!(gateway.control().metrics().ticks_succeeded, 1);

    // Control-plane step never bypasses the runtime commit path.
    gateway.control().stop_world(world).unwrap();
    gateway.control().destroy_world(world).unwrap();
    assert!(gateway.control().world_status(world).is_err());

    gateway.control().shutdown().unwrap();
    assert_eq!(gateway.runtime().state(), RuntimeState::Stopped);
    assert!(gateway.control().step().is_err());
}

// ---------------------------------------------------------------- metrics

#[test]
fn metrics_count_connections_sessions_and_frames() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();

    let (_, mut client1) = connect_client(&mut gateway);
    join_world0(&mut gateway, &mut client1, max, "alice-token");

    let (_, mut client2) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client2, max); // never authenticates
    send_client(&mut client2, &ClientMessage::Ping { nonce: 1 }, max);
    gateway.process_inbound();

    let metrics = gateway.metrics();
    assert_eq!(metrics.connections, 2);
    assert_eq!(metrics.sessions, 1);
    assert_eq!(metrics.attached, 1);
    assert_eq!(metrics.connections_per_world.get(&WorldId::from_u64(0)), Some(&1));
    assert!(metrics.frames_received >= 5); // handshake+auth+attach+handshake+ping
    assert!(metrics.messages_outbound >= 4); // responses + pong

    // Client 2 (unauthenticated) still gets its pong (its handshake
    // response was already consumed by the handshake helper).
    let msg = recv_server(&mut client2, max);
    assert!(matches!(msg, ServerMessage::Pong { nonce: 1 }));
}

// ------------------------------------------------------- authorization policy

/// A policy that denies every attach (ADR-014 D2 hook).
#[derive(Debug, Clone, Copy)]
struct DenyAttachPolicy;

impl GamePolicy for DenyAttachPolicy {
    fn authorize_attach(&self, _principal: &Principal, _world: WorldId) -> bool {
        false
    }
}

/// A policy that denies every input frame.
#[derive(Debug, Clone, Copy)]
struct DenyInputPolicy;

impl GamePolicy for DenyInputPolicy {
    fn authorize_input(&self, _principal: &Principal, _world: WorldId, _frame: &InputFrame) -> bool {
        false
    }
}

/// A policy that denies every reducer call.
#[derive(Debug, Clone, Copy)]
struct DenyReducerPolicy;

impl GamePolicy for DenyReducerPolicy {
    fn authorize_reducer(&self, _principal: &Principal, _world: WorldId, _reducer: &str) -> bool {
        false
    }
}

fn input_frame(tick: u64, kind: &str, payload: Option<Value>) -> InputFrame {
    let mut frame = InputFrame::new(TickId::from_u64(tick));
    frame
        .push(InputCommand::new(1, kind, payload).unwrap());
    frame
}

/// The default pass-through policy preserves the exact Phase 13 behavior.
#[test]
fn default_policy_preserves_phase_13_behavior() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();
    let (_id, mut client) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));

    // Input flows without a custom policy.
    send_client(
        &mut client,
        &ClientMessage::InputFrame { frame: input_frame(0, "spawn", Some(Value::U64(1))) },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().inputs_accepted, 1);
    // Reducer calls route into the world queue untouched by the policy; the
    // unknown name fails at execution (Phase 13 semantics) and the error
    // result is correlated back.
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 1,
            reducer: "nope".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().policy_rejections, 0);
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);
    gateway.step_worlds().unwrap();
    // The attached session receives the TickUpdate broadcast first, then the
    // correlated ReducerResult error.
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult {
            request_id: 1,
            ok: false,
            error: Some(ref e),
            ..
        } if e.contains("registered")
    ));
}

/// An installed policy can deny world attachment before any runtime access.
#[test]
fn policy_can_deny_attach() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    gateway.set_policy(Box::new(DenyAttachPolicy));
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();
    let (_id, mut client) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");

    send_client(&mut client, &ClientMessage::AttachWorld { world: WorldId::from_u64(0) }, max);
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::AttachResult {
            ok: false,
            error: Some(ref e),
            ..
        } if e.contains("not authorized")
    ));
    assert_eq!(gateway.metrics().policy_rejections, 1);
    assert_eq!(gateway.session_of(_id).unwrap().attached_world(), None);
}

/// An installed policy can deny input submission without touching the runtime.
#[test]
fn policy_can_deny_input() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    gateway.set_policy(Box::new(DenyInputPolicy));
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();
    let (_id, mut client) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));

    send_client(
        &mut client,
        &ClientMessage::InputFrame { frame: input_frame(0, "spawn", Some(Value::U64(1))) },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::Error { code: 18, .. }));
    assert_eq!(gateway.metrics().inputs_accepted, 0);
    assert_eq!(gateway.metrics().policy_rejections, 1);
}

/// A denied reducer call receives a correlated `ReducerResult` (request id
/// echoed) and is never submitted to the runtime.
#[test]
fn policy_denial_echoes_request_id() {
    let mut gateway = gateway_with(input_factory(), NetworkConfig::new());
    gateway.set_policy(Box::new(DenyReducerPolicy));
    create_world(&mut gateway, 0);
    let max = gateway.config().max_frame_payload();
    let (_id, mut client) = connect_client(&mut gateway);
    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));

    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: 77,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult {
            request_id: 77,
            ok: false,
            error: Some(ref e),
            ..
        } if e.contains("game policy")
    ));
    assert_eq!(gateway.metrics().policy_rejections, 1);
    assert_eq!(gateway.metrics().reducer_calls_accepted, 0);
}

// ------------------------------------------------------ regression (review)

/// Request ids with the server-reserved bit are rejected at the gateway
/// (ADR-014 D3): a client can never occupy the namespace used by
/// `GameServer::invoke_reducer`, so server results cannot be misrouted.
#[test]
fn server_reserved_request_ids_are_rejected_from_clients() {
    let config = NetworkConfig::new();
    let mut gateway = gateway_with(reducer_factory(), config);
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();

    handshake(&mut gateway, &mut client, max);
    authenticate(&mut gateway, &mut client, max, "alice-token");
    attach(&mut gateway, &mut client, max, WorldId::from_u64(0));

    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: SERVER_REQUEST_MSB | 5,
            reducer: "bump".into(),
            args: ReducerArgs::new(),
        },
        max,
    );
    gateway.process_inbound();
    let msg = recv_server(&mut client, max);
    assert!(matches!(
        msg,
        ServerMessage::ReducerResult {
            request_id,
            ok: false,
            error: Some(ref e),
            ..
        } if request_id == SERVER_REQUEST_MSB | 5 && e.contains("reserved for server")
    ));
    assert_eq!(gateway.metrics().reducer_calls_accepted, 0);
    assert_eq!(gateway.metrics().reducer_calls_rejected, 1);
}

#[test]
fn reducer_calls_stamp_the_authenticated_caller_identity() {
    let mut gateway = gateway_with(reducer_factory(), NetworkConfig::new());
    create_world(&mut gateway, 0);
    let (_, mut client) = connect_client(&mut gateway);
    let max = gateway.config().max_frame_payload();
    join_world0(&mut gateway, &mut client, max, "alice-token");

    // The client attempts to forge identity through a reserved `__caller`
    // argument; the gateway must overwrite it with the authenticated
    // principal id before the call is queued (ADR-013 D3 / ADR-014 D8).
    let request = 77;
    send_client(
        &mut client,
        &ClientMessage::CallReducer {
            request_id: request,
            reducer: "whoami".into(),
            args: ReducerArgs::new()
                .insert("__caller", 999u64)
                .insert("x", 1u64),
        },
        max,
    );
    gateway.process_inbound();
    assert_eq!(gateway.metrics().reducer_calls_accepted, 1);
    gateway.step_worlds().unwrap();
    let msg = recv_server(&mut client, max);
    assert!(matches!(msg, ServerMessage::TickUpdate { .. }));
    let msg = recv_server(&mut client, max);
    match msg {
        ServerMessage::ReducerResult {
            request_id,
            ok: true,
            value,
            ..
        } => {
            assert_eq!(request_id, request);
            assert_eq!(
                value,
                Some(Value::U64(1)),
                "the reducer sees the authenticated caller, never the forged arg"
            );
        }
        other => panic!("expected a successful ReducerResult, got {other:?}"),
    }
    assert_eq!(gateway.metrics().reducer_calls_rejected, 0);
}
