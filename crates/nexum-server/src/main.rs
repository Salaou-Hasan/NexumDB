//! Nexum server binary — a **runnable demo** of the full authoritative stack
//! (ADR-014).
//!
//! The demo wires the game server layer over the runtime over the
//! simulation, then drives both authoritative paths end to end:
//!
//! 1. **Host-driven path** — the server creates a game, joins players,
//!    submits server-side commands and reducer calls, and steps the world.
//!    Every mutation goes through `World::tick` → Transaction/OCC →
//!    `Vec<Change>` → WAL → SubscriptionRegistry.
//! 2. **Client path** — a real `nexum-sdk` client connects over an in-process
//!    transport, authenticates, joins, attaches to a world, subscribes to a
//!    table, and observes committed changes as a derived view.
//!
//! Run with:
//!
//! ```text
//! cargo run -p nexum-server [--ticks N] [--persist DIR]
//! ```
//!
//! - `--ticks N` — how many simulation ticks to run (default 8).
//! - `--persist DIR` — enable WAL durability + recovery-capable persistence
//!   into `DIR` (default: in-memory only).
//!
//! The simulation remains authoritative throughout: the demo never touches
//! tables, transactions, or the WAL directly.

use std::sync::Arc;

use nexum_core::{
    row, ColumnType, Error, PlayerId, ReducerId, Result, Row, RowId, SystemId, TableSchema,
    Value, WorldId,
};
use nexum_game_server::{GameInstanceConfig, GameServer, GameServerConfig};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig, WorldFactory};
use nexum_sdk::{transport::ClientTransport, Client, SdkConfig};
use nexum_simulation::{InputCommand, InputFrame, SimulationConfig, SystemDefinition, World};
use nexum_subscription::Query;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

/// The demo `players` table: `id` (pk), `zone`, `health`.
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

/// `player_join`: authoritative join initialization. Idempotent so a rejoin
/// after recovery never fails the tick with a duplicate-key error
/// (ADR-014 D3).
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

/// `server_secret`: a server-only reducer (writes an audit row). Not exposed
/// to clients — the demo proves deny-by-default exposure.
fn server_secret(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player = args.require_u64("player_id")?;
    ctx.insert("players", row![player + 1000, 99u64, 1i32])?;
    Ok(Value::U64(player))
}

/// A minimal WASM reducer returning `U64(42)` (no host calls).
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

/// The world factory: `players` table, a `spawn` command consumer system,
/// native reducers (`bump`, `player_join`, `server_secret`), and a WASM
/// reducer (`ping_wasm`).
fn demo_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_players(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "spawner", 0, |ctx, frame| {
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
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("ping_wasm", 1, ping_module()).unwrap();
        world.set_wasm(wasm);
        Ok(world)
    })
}

/// The demo identity table: `alice-token` → principal 1, `bob-token` →
/// principal 2, `carol-token` → principal 3.
fn authenticator() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    auth.add("alice-token", Principal::new(1, "alice")).unwrap();
    auth.add("bob-token", Principal::new(2, "bob")).unwrap();
    auth.add("carol-token", Principal::new(3, "carol")).unwrap();
    Arc::new(auth)
}

/// The help text shown for `--help` / `-h`.
const HELP: &str = "usage: nexum-server [--ticks N] [--persist DIR]\n\
\n\
options:\n\
\x20 --ticks N      simulation ticks to run (default 8)\n\
\x20 --persist DIR  enable WAL durability into DIR (default: in-memory)";

/// Prints the help text to stdout and exits successfully.
fn print_help() -> ! {
    println!("{HELP}");
    std::process::exit(0);
}

/// Prints the usage to stderr and exits with a failure code. Returns `!` so
/// it can stand in for any value in `unwrap_or_else`.
fn usage<T>() -> T {
    eprintln!("{HELP}");
    std::process::exit(2);
}

fn main() {
    let mut ticks = 8usize;
    let mut persist: Option<std::path::PathBuf> = None;
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--ticks" => {
                let raw = args.next().unwrap_or_else(usage::<String>);
                ticks = raw.parse().unwrap_or_else(|_| usage::<usize>());
            }
            "--persist" => {
                let value = args.next().unwrap_or_else(usage::<String>);
                persist = Some(std::path::PathBuf::from(value));
            }
            "--help" | "-h" => print_help(),
            other => {
                eprintln!("unknown argument: {other}");
                usage::<()>();
            }
        }
    }

    println!("══════════════════════════════════════════════════════════════");
    println!("  NEXUM v{} — authoritative state engine demo (Phase 14)", env!("CARGO_PKG_VERSION"));
    println!("══════════════════════════════════════════════════════════════");
    println!(
        "  stack:  GameServer → Runtime → World → Transaction/OCC → Vec<Change>\n\
         \x20                          → WAL → SubscriptionRegistry → network → SDK\n\
         \x20 mode:   {}\n\
         \x20 ticks:  {ticks}",
        match &persist {
            Some(dir) => format!("durable (WAL into {})", dir.display()),
            None => "in-memory".to_string(),
        }
    );
    println!();

    // ------------------------------------------------------------------
    // Wire the authoritative stack: Runtime (world factory) → GameServer.
    // ------------------------------------------------------------------
    let mut runtime_config = RuntimeConfig::new(demo_factory());
    if let Some(dir) = &persist {
        let _ = std::fs::create_dir_all(dir);
        runtime_config = runtime_config.with_persistence(PersistencePolicy::Flush, dir.clone());
    }
    let runtime = Runtime::new(runtime_config).expect("runtime starts");
    let mut server = GameServer::new(
        runtime,
        NetworkConfig::new(),
        authenticator(),
        GameServerConfig::new(),
    )
    .expect("game server starts");

    // Expose two client-callable reducers; `server_secret` stays server-only
    // (the demo proves a client call to it is denied).
    server.expose_reducer("bump").unwrap();
    server.expose_reducer("ping_wasm").unwrap();

    // ------------------------------------------------------------------
    // 1. Host-driven path: create + start a game, join players.
    // ------------------------------------------------------------------
    let game = server
        .create_game(
            GameInstanceConfig::new("arena")
                .with_partition_count(2)
                .with_on_player_join("player_join"),
        )
        .unwrap();
    server.start_game(game).unwrap();

    let alice = Principal::new(1, "alice");
    let bob = Principal::new(2, "bob");
    let carol = Principal::new(3, "carol");
    for (name, principal) in [("alice", &alice), ("bob", &bob), ("carol", &carol)] {
        let outcome = server.join_game(principal, game).unwrap();
        println!("  [host]  {name:>5} joined  ({outcome:?})");
    }

    // ------------------------------------------------------------------
    // Run the tick loop. Each tick demonstrates a different authoritative
    // operation through the single commit path.
    // ------------------------------------------------------------------
    println!();
    println!("  ── simulation ────────────────────────────────────────────────");
    let alice_player = PlayerId::from_u64(1);
    let bob_player = PlayerId::from_u64(2);

    for tick in 0..ticks {
        // Tick 0 already committed the joins (player_join reducer) — the
        // first `step` below consumes the buffered commands.
        // Spawn distinct ids — the join reducers already inserted rows 1–3,
        // and a duplicate primary key would fail the tick.
        if tick == 0 {
            server.submit_command(alice_player, "spawn", Some(Value::U64(4))).unwrap();
            println!("  [tick {tick:>2}]  alice spawns player #4");
        }
        if tick == 2 {
            server.submit_command(bob_player, "spawn", Some(Value::U64(5))).unwrap();
            println!("  [tick {tick:>2}]  bob spawns player #5");
        }
        if tick == 4 {
            let result = server.invoke_reducer(alice_player, "bump", ReducerArgs::new().insert("player", 1u64)).unwrap();
            // Server request ids live in the reserved `1 << 63` namespace
            // (ADR-014 D3), disjoint from client ids.
            println!("  [tick {tick:>2}]  server invokes `bump` (server request #{result:#x})");
        }
        if tick == 5 {
            let result = server.invoke_reducer(bob_player, "server_secret", ReducerArgs::new().insert("player_id", 2u64)).unwrap();
            println!("  [tick {tick:>2}]  server invokes `server_secret` (audit, server request #{result:#x})");
        }
        if tick == 6 {
            let result = server.invoke_reducer(alice_player, "ping_wasm", ReducerArgs::new()).unwrap();
            println!("  [tick {tick:>2}]  server invokes WASM `ping_wasm` (server request #{result:#x})");
        }

        let results = server.step().expect("tick commits");
        for (world, tick_result) in &results {
            let changes = tick_result.changes().len();
            let reducer_results = tick_result.reducer_results();
            let wal = server.runtime().metrics().wal_appends;
            if changes > 0 || !reducer_results.is_empty() {
                println!(
                    "  [tick {tick:>2}]  world {world:?} committed {changes} change(s), \
                     {} reducer result(s), WAL appends = {wal}",
                    reducer_results.len()
                );
            }
        }
    }

    // ------------------------------------------------------------------
    // 2. Client path: a real SDK client connects, authenticates, joins,
    //    attaches, subscribes, and observes committed changes as a derived
    //    view — over the same gateway/runtime/world.
    // ------------------------------------------------------------------
    println!();
    println!("  ── client (SDK) ──────────────────────────────────────────────");
    let (client_transport, server_conn) = ClientTransport::memory_pair(256, 512);
    server
        .gateway_mut()
        .register_connection(Box::new(server_conn))
        .expect("connection registers");
    let mut client = Client::new(SdkConfig::new()).unwrap();
    client.connect(client_transport.into_inner()).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    assert!(client.is_connected(), "handshake completes");
    println!("  [client] connected");

    client.authenticate("alice-token").unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    println!("  [client] authenticated as {:?}", client.session_principal().unwrap());

    // The client authenticated as alice (principal 1 → world 1). Attaching
    // to that same world succeeds because alice is an active member of it
    // (1 % 2 == 1); carol (3 % 2 == 1) happens to share it, which also
    // demonstrates deterministic routing.
    let carol_player = PlayerId::from_u64(3);
    let world = server.player_world(carol_player).unwrap();
    println!("  [host]  carol's world (routed deterministically) = {world:?}");
    client.attach(world).unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    println!("  [client] attached to {world:?}");

    let local = client
        .subscribe(Query::builder("players").build().unwrap())
        .unwrap();
    server.gateway_mut().process_inbound();
    client.pump().unwrap();
    let snapshot = client.view(local).unwrap().len();
    println!("  [client] subscribed: initial snapshot has {snapshot} row(s)");

    // A client command: carol spawns player #3. It routes through the
    // gateway policy → runtime → World::tick → WAL → subscription → view.
    let mut frame = InputFrame::new(server.runtime().world_status(world).unwrap().next_tick);
    frame.push(InputCommand::new(3, "spawn", Some(Value::U64(6))).unwrap());
    client.send_input(frame).unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();

    let view = client.view(local).unwrap();
    let carol_row = view.get(RowId::from_u64(0)).map(|r| r.row().values().to_vec());
    println!("  [client] after spawn command, view rows = {}", view.len());
    println!("  [client] row 0 = {carol_row:?}");

    // A client reducer call to the WASM reducer, correlated by request id.
    let request = client.call_reducer("ping_wasm", ReducerArgs::new()).unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    println!(
        "  [client] WASM `ping_wasm` → {:?} (request {request})",
        results.first().map(|r| (r.is_ok(), r.value().cloned()))
    );

    // A client reducer call to a *server-only* reducer is denied by the
    // exposure policy with a correlated error.
    let denied = client.call_reducer("server_secret", ReducerArgs::new()).unwrap();
    server.gateway_mut().process_inbound();
    server.step().unwrap();
    client.pump().unwrap();
    let results = client.take_reducer_results();
    println!(
        "  [client] `server_secret` (unexposed) → denied={} (request {denied})",
        results.first().map(|r| !r.is_ok()).unwrap_or(false)
    );

    // ------------------------------------------------------------------
    // Summary: metrics, events, and shutdown.
    // ------------------------------------------------------------------
    println!();
    println!("  ── summary ───────────────────────────────────────────────────");
    let metrics = server.metrics();
    let runtime_metrics = server.runtime().metrics();
    println!(
        "  games created:        {}\n\
         \x20 players joined:       {}\n\
         \x20 commands received:    {}\n\
         \x20 commands rejected:    {}\n\
         \x20 ticks succeeded:      {}\n\
         \x20 WAL appends:          {}\n\
         \x20 subscription messages:{}\n\
         \x20 reducer calls (server): {}",
        metrics.games_created,
        metrics.players_joined,
        metrics.commands_received,
        metrics.commands_rejected,
        runtime_metrics.ticks_succeeded,
        runtime_metrics.wal_appends,
        server.gateway().metrics().subscription_messages_sent,
        metrics.reducer_calls,
    );
    let events = server.drain_events();
    println!(
        "  events observed:      {} (last few: {:?})",
        server.event_count() + events.len(),
        events.iter().rev().take(4).collect::<Vec<_>>()
    );

    println!();
    println!("  [host]  shutting down…");
    server.shutdown().unwrap();
    println!("  [host]  done. The simulation was authoritative throughout.");
}
