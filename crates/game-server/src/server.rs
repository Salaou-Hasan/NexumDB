//! The Nexum Database Server for the arena game.
//!
//! Wires the stack: `NexumServer -> Runtime -> Partition -> reducers -> OCC ->
//! one atomic commit -> Vec<Change> -> WAL -> SubscriptionRegistry -> network`.
//!
//! Accepts real TCP clients, auto-joins authenticated principals, handles
//! disconnect/reconnect, and ticks every world at a fixed logical rate.
//!
//! The server itself is orchestration: it never touches tables,
//! transactions, or the WAL directly, and it never decides gameplay results.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use nexum_core::{PartitionId, WorldId};
use nexum_network::{NetworkEvent, NexumServer, Principal, TcpTransport, TokenAuthenticator};
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
    /// Quiet mode (log level -> error; no per-tick chatter).
    pub quiet: bool,
}

/// Builds the effective [`ServerConfig`]: defaults -> config file -> CLI.
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

/// The demo roster: token -> principal.
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

/// Runs the game server until interrupted, then shuts down cleanly.
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

    // Build the runtime with the game's world factory.
    let mut runtime_config = config.runtime_config(game_factory());
    if let Some(dir) = config.persistence_dir.clone()
        && config.persistence.is_enabled()
    {
        std::fs::create_dir_all(&dir)?;
        runtime_config = runtime_config.with_persistence(config.persistence, dir.clone());
    }
    let runtime = Runtime::new(runtime_config)?;

    // Create the Nexum Database Server (database = server).
    let mut server = NexumServer::new(runtime, config.network_config(), authenticator())?;

    // Create worlds (one per partition) for the arena game.
    let mut world_ids = Vec::new();
    for partition in 0..config.default_partitions {
        let world_id = WorldId::from_u64(partition as u64);
        let sim = nexum_execution::PartitionConfig::new().with_seed(config.seed);
        server.runtime_mut().create_partition(world_id, sim)?;
        server.runtime_mut().start_partition(world_id)?;
        let partition_id = PartitionId::from_u64(partition as u64);
        server
            .runtime_mut()
            .register_partition(partition_id, world_id)?;
        world_ids.push(world_id);
        if !args.quiet {
            println!(
                "[server] partition {} -> world {}",
                partition,
                world_id.as_u64()
            );
        }
    }

    // Expose client-callable reducers (AllowAllPolicy on the gateway
    // means all authenticated clients may invoke any reducer).
    // The game logic in reducers handles authorization internally.
    if !args.quiet {
        for reducer in CLIENT_REDUCERS {
            println!("[server] reducer exposed: {reducer}");
        }
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

    // Track connection -> principal mapping for disconnect handling.
    let mut connection_players: HashMap<u64, u64> = HashMap::new();
    // Track principal -> world mapping for player routing.
    let mut player_worlds: HashMap<u64, WorldId> = HashMap::new();
    let tick_duration = Duration::from_millis(1000 / config.tick_hz as u64);
    let mut loop_count: u64 = 0;

    let logger = crate::Logger::new(config.log_level, "server");
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

                    // Route player to a world via deterministic partition.
                    let partition = (principal_id as usize) % config.default_partitions;
                    let world_id = world_ids[partition];
                    player_worlds.insert(principal_id, world_id);

                    // Invoke the player_join reducer (idempotent insert or
                    // reconnect-mark) via the runtime's reducer call queue.
                    let request_id = (1u64 << 63) | principal_id; // SERVER_REQUEST_MSB
                    let _ = server.runtime_mut().submit_reducer_call(
                        world_id,
                        request_id,
                        "player_join",
                        ReducerArgs::new()
                            .insert("player_id", principal_id)
                            .insert("game_id", 0u64),
                    );
                    println!("[game] player {principal_id} joined (world {world_id})");
                }
                NetworkEvent::ConnectionClosed { connection, .. } => {
                    if let Some(principal_id) = connection_players.remove(&connection.as_u64()) {
                        // Invoke the player_leave reducer.
                        if let Some(&world_id) = player_worlds.get(&principal_id) {
                            let request_id = (1u64 << 63) | principal_id | (1u64 << 62);
                            let _ = server.runtime_mut().submit_reducer_call(
                                world_id,
                                request_id,
                                "player_leave",
                                ReducerArgs::new()
                                    .insert("player_id", principal_id)
                                    .insert("game_id", 0u64),
                            );
                        }
                        player_worlds.remove(&principal_id);
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

        // 4. One authoritative tick per world: tick -> fan-out ->
        //    TickUpdates + subscription deltas + reducer results.
        //    NexumServer::step() = process_inbound + step_worlds + flush.
        //    We already did process_inbound above, so call step_worlds
        //    directly through the gateway for the tick + fan-out.
        let _step_report = server.gateway_mut().step_worlds()?;
        let _ = server.gateway_mut().flush_outbound();

        // 5. Deliver subscription snapshots and write TCP bytes.
        server.gateway_mut().pump_subscriptions();
        server.gateway_mut().flush_outbound()?;

        loop_count += 1;
        if !args.quiet
            && config.metrics_interval_ticks > 0
            && loop_count.is_multiple_of(config.metrics_interval_ticks)
        {
            let runtime_metrics = server.runtime().metrics();
            let network_metrics = server.gateway().metrics();
            let snapshot = crate::ServerMetricsSnapshot::capture(
                runtime_metrics,
                network_metrics,
                0,
                server.gateway().connection_count(),
            );
            println!("[metrics] {}", snapshot.summary_line());
        }
        std::thread::sleep(tick_duration);
    }

    // Drain-then-flush: one final inbound drain, then shutdown.
    server.gateway_mut().process_inbound();
    server.gateway_mut().pump_subscriptions();
    server.gateway_mut().flush_outbound()?;
    if !args.quiet {
        let runtime_metrics = server.runtime().metrics();
        println!(
            "[server] shut down after {} ticks · WAL appends {} · connections {}",
            runtime_metrics.ticks_succeeded,
            runtime_metrics.wal_appends,
            server.gateway().connection_count()
        );
    }
    server.shutdown();
    Ok(())
}
