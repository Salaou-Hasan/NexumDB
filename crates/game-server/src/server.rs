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
use nexum_network::{NetworkEvent, Principal, TcpTransport, TokenAuthenticator};
use nexum_reducer::ReducerArgs;
use nexum_runtime::Runtime;

use crate::config::ServerConfig;
use crate::game::{CLIENT_REDUCERS, game_factory};

/// Configuration for the game server (CLI flags override the config file
/// and defaults; see [`ServerConfig`] for the full knob list).
#[derive(Debug, Clone, Default)]
pub struct ServerArgs {
    /// Optional production config file (`key = value` lines, `#` comments).
    pub config: Option<PathBuf>,
    /// The TCP listen port (overrides config).
    pub port: Option<u16>,
    /// Arena partition count (overrides config).
    pub partitions: Option<usize>,
    /// Logical ticks per second (overrides config).
    pub hz: Option<u32>,
    /// Deterministic world seed (overrides config).
    pub seed: Option<u64>,
    /// Maximum players per game (overrides config).
    pub max_players: Option<usize>,
    /// Optional WAL durability directory (overrides config).
    pub persist: Option<PathBuf>,
    /// Logical worker count (overrides config).
    pub workers: Option<usize>,
    /// Shut down cleanly after this many server-loop iterations (scripted
    /// shutdown; 0 = run until signaled).
    pub stop_after: Option<u64>,
    /// A stop-file: when it appears, the server shuts down cleanly.
    pub stop_file: Option<PathBuf>,
    /// Quiet mode (log level → error; no per-tick chatter).
    pub quiet: bool,
}

/// Builds the effective [`ServerConfig`]: defaults → config file → CLI.
/// Fails fast on an invalid configuration (ADR-016).
pub fn effective_config(args: &ServerArgs) -> Result<ServerConfig, String> {
    let mut config = match &args.config {
        Some(path) => ServerConfig::from_file(path)?,
        None => ServerConfig::default(),
    };
    if let Some(port) = args.port {
        config.port = port;
    }
    if let Some(partitions) = args.partitions {
        config.default_partitions = partitions;
    }
    if let Some(hz) = args.hz {
        config.tick_hz = hz;
    }
    if let Some(seed) = args.seed {
        config.seed = seed;
    }
    if let Some(max_players) = args.max_players {
        config.max_players = max_players;
    }
    if let Some(persist) = &args.persist {
        config.persistence = nexum_runtime::PersistencePolicy::Flush;
        config.persistence_dir = Some(persist.clone());
    }
    if let Some(workers) = args.workers {
        config.workers = workers;
    }
    if args.quiet {
        config.log_level = crate::LogLevel::Error;
    }
    if config.tokens.is_empty() {
        // The demo roster: token → principal.
        for (name, id) in [
            ("alice", 1u64),
            ("bob", 2u64),
            ("carol", 3u64),
            ("dave", 4u64),
        ] {
            config.tokens.insert(name.to_string(), id);
        }
    }
    config.validate()?;
    Ok(config)
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
/// Runs the game server until interrupted, then shuts down cleanly:
/// stops accepting connections, drains inbound, and flushes every world's
/// WAL (ADR-016 D3).
pub fn run_server(args: ServerArgs) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let config =
        effective_config(&args).map_err(Box::<dyn std::error::Error + Send + Sync>::from)?;
    if !args.quiet {
        println!(
            "[config] {} ticks/s · {} workers · {} partitions · seed {}",
            config.tick_hz, config.workers, config.default_partitions, config.seed
        );
        println!(
            "[config] persistence: {:?}{}",
            config.persistence,
            config
                .persistence_dir
                .as_ref()
                .map(|d| format!(" -> {}", d.display()))
                .unwrap_or_default()
        );
    }

    let mut runtime_config = config.runtime_config(game_factory());
    if let Some(dir) = config.persistence_dir.clone()
        && config.persistence.is_enabled()
    {
        std::fs::create_dir_all(&dir)?;
        runtime_config = runtime_config.with_persistence(config.persistence, dir.clone());
    }
    let runtime = Runtime::new(runtime_config)?;
    let server_config: GameServerConfig = config.game_server_config();
    let mut server = GameServer::new(
        runtime,
        config.network_config(),
        authenticator(),
        server_config,
    )?;

    let game_config = nexum_game_server::GameInstanceConfig::new("arena")
        .with_partition_count(config.default_partitions)
        .with_world_seed(config.seed)
        .with_max_players(config.max_players)
        .with_on_player_join("player_join");

    let game = match config.persistence_dir.clone() {
        Some(dir) if config.persistence.is_enabled() && has_wal(&dir) => {
            let (game, report) = server.recover_game(game_config, None)?;
            if !args.quiet {
                println!(
                    "[server] recovered arena from {} ({} tx replayed)",
                    dir.display(),
                    report.replayed_txs
                );
            }
            game
        }
        _ => server.create_game(game_config)?,
    };
    server.start_game(game)?;

    for reducer in CLIENT_REDUCERS {
        server.expose_reducer(reducer)?;
    }

    let listen_addr: SocketAddr = (
        config.bind.parse::<std::net::Ipv4Addr>().map_err(|_| {
            Box::<dyn std::error::Error + Send + Sync>::from(format!(
                "invalid bind address: {}",
                config.bind
            ))
        })?,
        config.port,
    )
        .into();
    let transport = TcpTransport::listen(listen_addr)?;
    println!("══════════════════════════════════════════════════════════");
    println!("  NEXUM ARENA — the playable multiplayer demo");
    println!(
        "  listening on {listen_addr}  ·  {} partition(s)  ·  {} ticks/s  ·  seed {}",
        config.default_partitions, config.tick_hz, config.seed
    );
    println!("  clients:  cargo run -p game-server -- client --name alice  (alice/bob/carol/dave)");
    println!("══════════════════════════════════════════════════════════");

    // connection id → principal id (for disconnect handling).
    let mut connection_players: HashMap<u64, u64> = HashMap::new();
    let tick_duration = Duration::from_millis(1000 / config.tick_hz as u64);
    let mut loop_count: u64 = 0;

    // Structured logging (ADR-016 §Observability): `timestamp level module
    // message` lines on stderr, filtered by the configured level.
    let logger = crate::Logger::new(config.log_level, "server");

    // Graceful shutdown (ADR-016 D2/D3): a signal, stop-file, or tick
    // budget triggers the drain-then-flush path below.
    let shutdown = crate::ShutdownHandle::new(args.stop_file.clone());
    shutdown.install_signal_handler();

    loop {
        if shutdown.is_requested() || args.stop_after.is_some_and(|n| loop_count >= n) {
            if !args.quiet {
                println!("[server] shutdown requested — draining and flushing WAL…");
            }
            break;
        }
        shutdown.poll();
        // 1. Accept new TCP connections.
        while let Some(connection) = transport.accept(1024, 64 * 1024)? {
            server
                .gateway_mut()
                .register_connection(Box::new(connection))?;
            if !args.quiet {
                println!("[net] connection opened");
            }
            logger.info("connection opened", &[]);
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
                    logger.info(
                        "authenticated",
                        &[
                            ("connection", connection.as_u64().to_string()),
                            ("principal", principal_id.to_string()),
                        ],
                    );
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
                        logger.warn(
                            "player disconnected",
                            &[("player", principal_id.to_string())],
                        );
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
        if !args.quiet
            && config.metrics_interval_ticks > 0
            && loop_count.is_multiple_of(config.metrics_interval_ticks)
        {
            let snapshot = crate::ServerMetricsSnapshot::capture(
                server.runtime().metrics(),
                server.gateway().metrics(),
                server.metrics(),
                0, // row count is not exposed by the game server; keep 0
                server.gateway().connection_count(),
            );
            println!("[metrics] {}", snapshot.summary_line());
        }
        // Pace the logical ticks (a scheduling hint only — correctness is
        // logical-time based and never depends on wall-clock pacing).
        std::thread::sleep(tick_duration);
    }

    // Drain-then-flush: one final inbound drain, then the idempotent
    // `GameServer::shutdown()` (runtime stops scheduling, every world's WAL
    // is flushed — the durability contract — and resources are released).
    server.gateway_mut().process_inbound();
    server.gateway_mut().pump_subscriptions();
    server.gateway_mut().flush_outbound()?;
    if !args.quiet {
        let metrics = server.metrics();
        let runtime_metrics = server.runtime().metrics();
        println!(
            "[server] shut down after {} ticks · WAL appends {} · players joined {} · connections {}",
            runtime_metrics.ticks_succeeded,
            runtime_metrics.wal_appends,
            metrics.players_joined,
            server.gateway().connection_count()
        );
    }
    server.shutdown()?;
    Ok(())
}
