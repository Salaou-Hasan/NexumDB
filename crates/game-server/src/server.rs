//! The authoritative arena game server (ADR-010/014): wires the stack
//! `GameServer → Runtime → Partition → World → Simulation → reducers → OCC →
//! one atomic commit → Vec<Change> → WAL → SubscriptionRegistry → network`,
//! accepts real TCP clients, auto-joins authenticated principals, handles
//! disconnect/reconnect, and ticks every world at a fixed logical rate.
//!
//! The server itself is orchestration: it never touches tables,
//! transactions, or the WAL directly, and it never decides gameplay results.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nexum_core::PlayerId;
use nexum_game_server::{GameServer, GameServerConfig, JoinOutcome};
use nexum_network::{
    NetworkConfig, NetworkEvent, Principal, TokenAuthenticator, TcpTransport,
};
use nexum_reducer::ReducerArgs;
use nexum_runtime::{PersistencePolicy, Runtime, RuntimeConfig};

use crate::game::{game_factory, CLIENT_REDUCERS};

/// Configuration for the game server.
#[derive(Debug, Clone)]
pub struct ServerArgs {
    /// The TCP listen port.
    pub port: u16,
    /// Arena partition count (1 = one shared world; players route to
    /// `principal % partitions`).
    pub partitions: usize,
    /// Logical ticks per second.
    pub hz: u32,
    /// Deterministic world seed.
    pub seed: u64,
    /// Maximum players per game.
    pub max_players: usize,
    /// Optional WAL durability directory (enables crash recovery).
    pub persist: Option<PathBuf>,
    /// Quiet mode (no per-tick log chatter).
    pub quiet: bool,
}

impl Default for ServerArgs {
    fn default() -> Self {
        Self {
            port: 9337,
            partitions: 1,
            hz: 20,
            seed: 42,
            max_players: 64,
            persist: None,
            quiet: false,
        }
    }
}

/// The demo roster: token → principal.
pub fn authenticator() -> Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    for (name, id) in [
        ("alice", 1u64),
        ("bob", 2u64),
        ("carol", 3u64),
        ("dave", 4u64),
    ] {
        auth.add(name, Principal::new(id, name)).unwrap();
    }
    Arc::new(auth)
}

/// Whether the WAL directory already contains a previous run (recoverable).
fn has_wal(dir: &std::path::Path) -> bool {
    std::fs::read_dir(dir)
        .map(|entries| entries.flatten().count() > 0)
        .unwrap_or(false)
}

/// Runs the game server until interrupted.
pub fn run_server(args: ServerArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let mut runtime_config = RuntimeConfig::new(game_factory());
    if let Some(dir) = &args.persist {
        std::fs::create_dir_all(dir)?;
        runtime_config = runtime_config.with_persistence(PersistencePolicy::Flush, dir.clone());
    }
    let runtime = Runtime::new(runtime_config)?;
    let server_config = GameServerConfig::new().with_tick_rate_hz(args.hz);
    let mut server = GameServer::new(
        runtime,
        NetworkConfig::new(),
        authenticator(),
        server_config,
    )?;

    let game_config = nexum_game_server::GameInstanceConfig::new("arena")
        .with_partition_count(args.partitions)
        .with_world_seed(args.seed)
        .with_max_players(args.max_players)
        .with_on_player_join("player_join");

    let (game, recovered) = match &args.persist {
        Some(dir) if has_wal(dir) => {
            let (game, report) = server.recover_game(game_config, None)?;
            if !args.quiet {
                println!(
                    "[server] recovered arena from {} ({} tx replayed)",
                    dir.display(),
                    report.replayed_txs
                );
            }
            (game, true)
        }
        _ => {
            let game = server.create_game(game_config)?;
            (game, false)
        }
    };
    server.start_game(game)?;

    for reducer in CLIENT_REDUCERS {
        server.expose_reducer(reducer)?;
    }

    let listen_addr: SocketAddr = (std::net::Ipv4Addr::LOCALHOST, args.port).into();
    let transport = TcpTransport::listen(listen_addr)?;
    println!("══════════════════════════════════════════════════════════");
    println!("  NEXUM ARENA — the playable multiplayer demo");
    println!(
        "  listening on {listen_addr}  ·  {} partition(s)  ·  {} ticks/s  ·  seed {}",
        args.partitions, args.hz, args.seed
    );
    println!(
        "  clients:  cargo run -p game-server -- client --name alice  (alice/bob/carol/dave)"
    );
    println!("══════════════════════════════════════════════════════════");

    // connection id → principal id (for disconnect handling).
    let mut connection_players: HashMap<u64, u64> = HashMap::new();
    let tick_duration = Duration::from_millis(1000 / args.hz as u64);
    let mut loop_count: u64 = 0;

    loop {
        // 1. Accept new TCP connections.
        while let Some(connection) = transport.accept(1024, 64 * 1024)? {
            server.gateway_mut().register_connection(Box::new(connection))?;
            if !args.quiet {
                println!("[net] connection opened");
            }
        }

        // 2. Drain inbound client messages into the runtime.
        server.gateway_mut().process_inbound();

        // 3. Handle network events: auto-join authenticated principals,
        //    mark disconnected players on close.
        for event in server.gateway_mut().drain_events() {
            match event {
                NetworkEvent::Authenticated {
                    connection,
                    principal_id,
                } => {
                    connection_players.insert(connection.as_u64(), principal_id);
                    let principal = Principal::new(principal_id, format!("player-{principal_id}"));
                    match server.join_game(&principal, game) {
                        Ok(JoinOutcome::Reconnected) => {
                            // Restore the authoritative connected flag (the
                            // row persisted with connected = 0).
                            let _ = server.invoke_reducer(
                                PlayerId::from_u64(principal_id),
                                "player_join",
                                ReducerArgs::new()
                                    .insert("player_id", principal_id)
                                    .insert("game_id", game.as_u64()),
                            );
                            println!("[game] player {principal_id} reconnected");
                        }
                        Ok(JoinOutcome::Joined) => {
                            let world = server
                                .player_world(PlayerId::from_u64(principal_id))
                                .map(|world| world.to_string())
                                .unwrap_or_else(|_| "?".into());
                            println!("[game] player {principal_id} joined arena (world {world})");
                        }
                        Err(error) => {
                            println!("[game] join rejected for {principal_id}: {error}");
                        }
                    }
                }
                NetworkEvent::ConnectionClosed { connection, .. } => {
                    if let Some(principal_id) = connection_players.remove(&connection.as_u64()) {
                        let player = PlayerId::from_u64(principal_id);
                        // Membership → Reconnecting (no input may flow)…
                        let _ = server.disconnect_player(player);
                        // …and the authoritative connected flag → 0.
                        let _ = server.invoke_reducer(
                            player,
                            "player_leave",
                            ReducerArgs::new()
                                .insert("player_id", principal_id)
                                .insert("game_id", game.as_u64()),
                        );
                        println!("[game] player {principal_id} disconnected");
                    }
                }
                _ => {}
            }
        }

        // 4. One authoritative tick per world (flush server commands, tick,
        //    fan out TickUpdates + subscription deltas + reducer results).
        let results = server.step()?;
        for (_world, tick_result) in &results {
            for event in tick_result.events() {
                if event.name() == "kill" {
                    println!("[game] KILL: {}", event.payload());
                }
            }
        }

        // 5. Deliver subscription snapshots and write TCP bytes.
        server.gateway_mut().pump_subscriptions();
        server.gateway_mut().flush_outbound()?;

        loop_count += 1;
        if !args.quiet && loop_count.is_multiple_of(args.hz as u64 * 10) {
            let metrics = server.metrics();
            let runtime_metrics = server.runtime().metrics();
            println!(
                "[tick {}] worlds {} · WAL appends {} · players joined {} · connections {}",
                runtime_metrics.ticks_succeeded,
                server.list_games().len(),
                runtime_metrics.wal_appends,
                metrics.players_joined,
                server.gateway().connection_count()
            );
        }
        if !recovered {
            // Keep the simulation honest: no wall-clock deadline, the loop
            // simply paces the logical ticks.
        }
        std::thread::sleep(tick_duration);
    }
}
