//! Phase 13 end-to-end integration tests (ADR-013): the full
//! SDK → network → runtime → partition → world → transaction → WAL →
//! subscription → network → SDK path, failed ticks (zero updates), multi
//! -partition isolation, recovery without history replay, native and WASM
//! reducer calls, and protocol-version rejection.

use std::sync::Arc;

use nexum_core::{
    ColumnType, Error, ReducerId, Result, Row, RowId, SystemId, TickId, Value, WorldId, row,
};
use nexum_execution::{InputCommand, InputFrame, Partition, PartitionConfig, SystemDefinition};
use nexum_network::{NetworkConfig, NetworkGateway, Principal, TokenAuthenticator};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{
    PartitionFactory, PartitionLifecycle, PersistencePolicy, Runtime, RuntimeConfig,
};
use nexum_sdk::{
    Client, SdkConfig, ServerEvent, protocol::PROTOCOL_VERSION, transport::ClientTransport,
};
use nexum_subscription::Query;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

// ---------------------------------------------------------------- harness

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

/// The `bump` native reducer: `+10` to the named player's health, returns
/// the new health, and emits a `bumped` event.
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

/// A world whose system consumes `spawn` commands as player rows, with the
/// native `bump` reducer registered.
fn input_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
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
        Ok(world)
    })
}

/// A world that additionally registers a WASM reducer returning 42.
fn wasm_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
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
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("ping_wasm", 1, ping_module()).unwrap();
        world.set_wasm(wasm);
        Ok(world)
    })
}

/// A factory where world 1 fails on its first tick.
fn failing_factory() -> PartitionFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: PartitionConfig| {
        ensure_players(&mut store);
        let mut world = Partition::new(id, store, sim)?;
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
    auth.add("bob-token", Principal::new(2, "bob")).unwrap();
    Arc::new(auth)
}

fn temp_dir(name: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("{name}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
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

/// Registers a memory connection with the gateway and drives the SDK client
/// through handshake + authenticate + attach.
fn connect(gateway: &mut NetworkGateway, token: &str, world: WorldId) -> Client {
    let (transport, server) = ClientTransport::memory_pair(256, 512);
    gateway.register_connection(Box::new(server)).unwrap();
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(transport.into_inner()).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(client.is_connected(), "handshake completes");

    client.authenticate(token).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(client.session_principal().is_some(), "authenticated");

    client.attach(world).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert_eq!(client.attached_world(), Some(world), "attached");
    // The test starts with a clean event slate (handshake/auth/attach were
    // consumed here).
    client.take_events();
    client
}

fn gateway_with(factory: PartitionFactory) -> NetworkGateway {
    let runtime = Runtime::new(RuntimeConfig::new(factory)).unwrap();
    // These tests assert the full change list on the Tick event.
    NetworkGateway::new(
        runtime,
        NetworkConfig::new().with_tick_update_changes(true),
        test_auth(),
    )
    .unwrap()
}

// ------------------------------------------------------------ full pipeline

#[test]
fn full_sdk_pipeline_input_tick_subscription_and_reducer_call() {
    let dir = temp_dir("nexum-sdk-e2e");
    let runtime = Runtime::new(
        RuntimeConfig::new(input_factory()).with_persistence(PersistencePolicy::Flush, dir.clone()),
    )
    .unwrap();
    // These tests assert the full change list on the Tick event.
    let mut gateway = NetworkGateway::new(
        runtime,
        NetworkConfig::new().with_tick_update_changes(true),
        test_auth(),
    )
    .unwrap();
    let world = WorldId::from_u64(0);
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();
    let mut client = connect(&mut gateway, "alice-token", world);

    // Subscribe: the initial snapshot of the empty table.
    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(client.view(local).unwrap().is_empty());
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::SubscriptionBound { .. }]
    ));

    // Client input → runtime → world tick → WAL → subscription delta.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(0, "spawn", Some(Value::U64(42))).unwrap());
    client.send_input(frame).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();

    let events = client.take_events();
    let tick = events
        .iter()
        .find_map(|event| match event {
            ServerEvent::Tick {
                tick,
                changes,
                events,
                ..
            } => {
                assert_eq!(changes.len(), 1);
                assert_eq!(changes[0].new_row().unwrap().get(0), Some(&Value::U64(42)));
                assert!(events.is_empty(), "the system emitted no events");
                Some(*tick)
            }
            _ => None,
        })
        .expect("exactly one TickUpdate");
    assert_eq!(tick, TickId::from_u64(0));
    // The subscription view absorbed the insert.
    let view = client.view(local).unwrap();
    assert_eq!(view.len(), 1);
    assert_eq!(
        view.get(RowId::from_u64(0)).unwrap().row().get(0),
        Some(&Value::U64(42))
    );
    assert_eq!(gateway.runtime().metrics().wal_appends, 1);

    // Reducer call: bump(42) executes inside the next tick and returns 110.
    let request = client
        .call_reducer("bump", ReducerArgs::new().insert("player", 42u64))
        .unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();

    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(results[0].is_ok());
    assert_eq!(results[0].value(), Some(&Value::I32(110)));

    // The update also flowed to the subscription (health 100 → 110) and the
    // reducer's event arrived in the TickUpdate of that tick.
    let view = client.view(local).unwrap();
    assert_eq!(
        view.get(RowId::from_u64(0)).unwrap().row().get(2),
        Some(&Value::I32(110))
    );
    let events = client.take_events();
    assert!(events.iter().any(|event| matches!(
        event,
        ServerEvent::Tick { events, .. } if events.iter().any(|e| e.name() == "bumped")
    )));
    assert_eq!(gateway.runtime().metrics().wal_appends, 2);

    gateway.control().shutdown().unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------- multi-change commits

#[test]
fn one_commit_with_many_rows_applies_all_deltas_without_a_false_gap() {
    // A single transaction inserting several rows produces several deltas
    // that share the commit sequence (ADR-008 D3). The derived View must
    // absorb them all without flagging a ViewGap (same-commit deltas are
    // legal) — silent-loss detection must not false-positive.
    let mut gateway = gateway_with(input_factory());
    let world = WorldId::from_u64(0);
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();
    let mut client = connect(&mut gateway, "alice-token", world);

    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(client.view(local).unwrap().is_empty());
    client.take_events();

    // One frame, three commands → one atomic commit → three same-seq deltas.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    for i in 0..3u64 {
        frame.push(InputCommand::new(0, "spawn", Some(Value::U64(i))).unwrap());
    }
    client.send_input(frame).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();

    let events = client.take_events();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ServerEvent::ViewGap { .. })),
        "same-commit deltas must never false-flag a gap: {events:?}"
    );
    let view = client.view(local).unwrap();
    assert_eq!(view.len(), 3);
    for i in 0..3u64 {
        assert_eq!(
            view.get(RowId::from_u64(i)).unwrap().row().get(0),
            Some(&Value::U64(i))
        );
    }
    assert!(!client.subscription(local).unwrap().is_stale());
}

// ------------------------------------------------------------- failed ticks

#[test]
fn failed_tick_produces_no_updates_and_correlated_call_failure() {
    let mut gateway = gateway_with(failing_factory());
    let world = WorldId::from_u64(1); // fails on its first tick
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();
    let mut client = connect(&mut gateway, "alice-token", world);

    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    gateway.process_inbound();
    client.pump().unwrap();

    // A reducer call is queued, then the tick fails.
    let request = client
        .call_reducer("bump", ReducerArgs::new().insert("player", 1u64))
        .unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();

    assert_eq!(
        gateway.runtime().partition_status(world).unwrap().state,
        PartitionLifecycle::Failed
    );
    assert_eq!(gateway.runtime().metrics().wal_appends, 0);
    assert_eq!(gateway.runtime().metrics().ticks_succeeded, 0);

    // Zero realtime updates (no TickUpdate, no subscription delta) and the
    // pending call answered with a correlated failure — never a hang.
    let events = client.take_events();
    assert!(
        !events
            .iter()
            .any(|event| matches!(event, ServerEvent::Tick { .. })),
        "no TickUpdate for a failed tick"
    );
    assert_eq!(client.view(local).unwrap().len(), 0, "no delta applied");
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(!results[0].is_ok());
    // The correlated failure reports the world could not produce a result
    // (ADR-013 D3) — a terminal error, never a hang.
    assert!(results[0].error().unwrap().contains("could not commit"));
}

// -------------------------------------------------- multi-partition isolation

#[test]
fn worlds_are_isolated_across_sessions() {
    let mut gateway = gateway_with(input_factory());
    let world_a = WorldId::from_u64(0);
    let world_b = WorldId::from_u64(1);
    gateway
        .control()
        .create_partition(world_a, PartitionConfig::new())
        .unwrap();
    gateway
        .control()
        .create_partition(world_b, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world_a).unwrap();
    gateway.control().start_partition(world_b).unwrap();

    let mut client_a = connect(&mut gateway, "alice-token", world_a);
    let mut client_b = connect(&mut gateway, "bob-token", world_b);

    let sub_a = client_a
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    let sub_b = client_b
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    gateway.process_inbound();
    client_a.pump().unwrap();
    client_b.pump().unwrap();

    // Alice spawns in world A.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    frame.push(InputCommand::new(0, "spawn", Some(Value::U64(1))).unwrap());
    client_a.send_input(frame).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client_a.pump().unwrap();
    client_b.pump().unwrap();

    // Alice sees her world's tick and the row; Bob never observes world A
    // (his own world may tick empty — but never with world A's changes).
    let a_events = client_a.take_events();
    assert!(a_events.iter().any(|event| matches!(
        event,
        ServerEvent::Tick { world: w, changes, .. } if *w == world_a && !changes.is_empty()
    )));
    assert_eq!(client_a.view(sub_a).unwrap().len(), 1);
    let b_events = client_b.take_events();
    assert!(
        !b_events
            .iter()
            .any(|event| matches!(event, ServerEvent::Tick { world: w, .. } if *w == world_a)),
        "world B must not observe world A's tick"
    );
    assert_eq!(client_b.view(sub_b).unwrap().len(), 0);

    // Bob spawns in world B at the next tick number; Alice sees nothing.
    let mut frame = InputFrame::new(TickId::from_u64(1));
    frame.push(InputCommand::new(0, "spawn", Some(Value::U64(2))).unwrap());
    client_b.send_input(frame).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client_a.pump().unwrap();
    client_b.pump().unwrap();

    assert_eq!(
        client_a.view(sub_a).unwrap().len(),
        1,
        "still only world A's row"
    );
    assert_eq!(client_b.view(sub_b).unwrap().len(), 1);
    assert_eq!(
        client_b
            .view(sub_b)
            .unwrap()
            .get(RowId::from_u64(0))
            .unwrap()
            .row()
            .get(0),
        Some(&Value::U64(2))
    );
}

// ------------------------------------------------------------ WASM reducers

#[test]
fn wasm_reducer_call_round_trips_through_the_network() {
    let mut gateway = gateway_with(wasm_factory());
    let world = WorldId::from_u64(0);
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();
    let mut client = connect(&mut gateway, "alice-token", world);

    let request = client
        .call_reducer("ping_wasm", ReducerArgs::new())
        .unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();

    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(results[0].is_ok());
    assert_eq!(results[0].value(), Some(&Value::U64(42)));

    // An unknown reducer is a correlated failure, not a hang.
    let request = client.call_reducer("nope", ReducerArgs::new()).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    assert_eq!(results.len(), 1);
    assert_eq!(results[0].request_id(), request);
    assert!(!results[0].is_ok());
    assert!(results[0].error().unwrap().contains("no reducer"));
}

// ------------------------------------------------------------- recovery

#[test]
fn recovery_restores_state_without_replaying_history_to_the_sdk() {
    let dir = temp_dir("nexum-sdk-recovery");

    // Phase A: one committed tick, then "crash".
    {
        let runtime = Runtime::new(
            RuntimeConfig::new(input_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        // These tests assert the full change list on the Tick event.
        let mut gateway = NetworkGateway::new(
            runtime,
            NetworkConfig::new().with_tick_update_changes(true),
            test_auth(),
        )
        .unwrap();
        let world = WorldId::from_u64(0);
        gateway
            .control()
            .create_partition(world, PartitionConfig::new())
            .unwrap();
        gateway.control().start_partition(world).unwrap();
        let mut client = connect(&mut gateway, "alice-token", world);
        let mut frame = InputFrame::new(TickId::from_u64(0));
        frame.push(InputCommand::new(0, "spawn", Some(Value::U64(1))).unwrap());
        client.send_input(frame).unwrap();
        gateway.process_inbound();
        gateway.step_worlds().unwrap();
        client.pump().unwrap();
        let events = client.take_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ServerEvent::Tick { .. }))
        );
        gateway.control().shutdown().unwrap();
    }

    // Phase B: a fresh gateway recovers the world; the SDK reattaches and
    // resubscribes — the recovered row is an Initial snapshot, not a replay.
    {
        let runtime = Runtime::new(
            RuntimeConfig::new(input_factory())
                .with_persistence(PersistencePolicy::Flush, dir.clone()),
        )
        .unwrap();
        // These tests assert the full change list on the Tick event.
        let mut gateway = NetworkGateway::new(
            runtime,
            NetworkConfig::new().with_tick_update_changes(true),
            test_auth(),
        )
        .unwrap();
        let world = WorldId::from_u64(0);
        let report = gateway
            .control()
            .recover_partition(world, PartitionConfig::new(), Some(TickId::from_u64(1)))
            .unwrap();
        assert_eq!(report.replayed_txs, 1);
        gateway.control().start_partition(world).unwrap();

        let mut client = connect(&mut gateway, "alice-token", world);
        let local = client
            .subscribe(Query::builder("players").build().unwrap())
            .unwrap();
        gateway.process_inbound();
        client.pump().unwrap();
        // The recovered row is the initial view — not a replayed live delta.
        assert_eq!(client.view(local).unwrap().len(), 1);
        assert_eq!(
            client
                .view(local)
                .unwrap()
                .get(RowId::from_u64(0))
                .unwrap()
                .row()
                .get(0),
            Some(&Value::U64(1))
        );
        assert!(matches!(
            client.take_events().as_slice(),
            [ServerEvent::SubscriptionBound { .. }]
        ));

        // Subsequent ticks deliver only new deltas.
        let mut frame = InputFrame::new(TickId::from_u64(1));
        frame.push(InputCommand::new(0, "spawn", Some(Value::U64(2))).unwrap());
        client.send_input(frame).unwrap();
        gateway.process_inbound();
        gateway.step_worlds().unwrap();
        client.pump().unwrap();
        let events = client.take_events();
        assert!(
            events
                .iter()
                .any(|event| matches!(event, ServerEvent::Tick { tick, .. } if tick.as_u64() == 1))
        );
        assert_eq!(client.view(local).unwrap().len(), 2);

        gateway.control().shutdown().unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

// ------------------------------------------------- version negotiation

#[test]
fn mismatched_protocol_version_is_rejected_and_closes_cleanly() {
    let mut gateway = gateway_with(input_factory());
    let world = WorldId::from_u64(0);
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();

    let (transport, server) = ClientTransport::memory_pair(16, 16);
    gateway.register_connection(Box::new(server)).unwrap();
    let config = SdkConfig::new().with_protocol_version(PROTOCOL_VERSION + 50);
    let mut client = Client::new(config).unwrap();
    client.connect(transport.into_inner()).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();

    // The gateway rejects the version, delivers a correlated error and a
    // Disconnect reason, and drops the connection. The client observes both
    // and transitions to Closed.
    assert!(
        client.is_closed(),
        "the client closed after the server drop"
    );
    let events = client.take_events();
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ServerEvent::Error { code: 2, .. }))
    );
    assert!(
        events
            .iter()
            .any(|event| matches!(event, ServerEvent::Disconnected { .. }))
    );
    assert_eq!(
        gateway.connection_count(),
        0,
        "the server dropped the connection"
    );
}

// ------------------------------------------------------ backpressure basics

#[test]
fn slow_client_falls_stale_and_resync_restores_the_exact_view() {
    // A client whose outbound queue cannot hold a tick's messages falls
    // stale; dropped deltas are never silently lost — the session is marked
    // stale, a StaleNotification arrives, and a resync regenerates the exact
    // view. Simulation and other clients are never blocked.
    let mut gateway = gateway_with(input_factory());
    let world = WorldId::from_u64(0);
    gateway
        .control()
        .create_partition(world, PartitionConfig::new())
        .unwrap();
    gateway.control().start_partition(world).unwrap();

    let (transport, server) = ClientTransport::memory_pair(256, 1);
    gateway.register_connection(Box::new(server)).unwrap();
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(transport.into_inner()).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    client.authenticate("alice-token").unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    client.attach(world).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();

    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(client.view(local).unwrap().is_empty());

    // One tick spawning 4 rows overflows the single-slot outbound queue.
    let mut frame = InputFrame::new(TickId::from_u64(0));
    for i in 0..4u64 {
        frame.push(InputCommand::new(0, "spawn", Some(Value::U64(i))).unwrap());
    }
    client.send_input(frame).unwrap();
    gateway.process_inbound();
    gateway.step_worlds().unwrap();
    client.pump().unwrap();
    assert_eq!(gateway.metrics().sessions_stale, 1);
    assert!(
        client.view(local).unwrap().is_empty(),
        "the overflowed deltas were dropped, not queued"
    );
    client.take_events(); // drain the delivered TickUpdate

    // The queued StaleNotification is flushed on the next outbound send
    // (the client's ping makes the gateway write again).
    client.ping().unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::Stale { .. }]
    ));

    // Resync regenerates the exact view — no silently lost rows.
    client.resync(local).unwrap();
    gateway.process_inbound();
    client.pump().unwrap();
    assert_eq!(client.view(local).unwrap().len(), 4);
    assert!(matches!(
        client.take_events().as_slice(),
        [ServerEvent::SubscriptionResynced { .. }]
    ));
}
