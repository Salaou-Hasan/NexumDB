//! Phase 16 CCU (concurrent users) load harness (ADR-016 D4).
//!
//! Boots the **real stack** — GameServer → Runtime → World with the arena
//! game — and drives N simulated clients through the **real gateway + real
//! protocol codec + real SDK client objects** over in-process transport
//! (honest scope: the socket layer is in-process; the protocol, gateway,
//! runtime, world, subscriptions, and SDK views are all real).
//!
//! Profiles:
//!
//! - `A` — connection only: connect + authenticate + attach + subscribe, then
//!   stay connected.
//! - `B` — light input: each client sends movement at a realistic rate.
//! - `C` — realistic game: movement + received subscription updates +
//!   occasional `fire_weapon` reducer calls.
//! - `D` — stress: high input + reducer pressure until saturation.
//!
//! Every client is a real [`nexum_sdk::Client`] over a memory transport pair,
//! so the measured cost includes the real SDK encode/decode and view
//! application path.
//!
//! Run (release, from the repo root):
//!
//! ```text
//! cargo run --release -p game-server --example ccu -- --clients 1000 --profile C --ticks 200
//! cargo run --release -p game-server --example ccu -- --clients 10000 --profile B --ticks 300
//! ```
//!
//! Results are classified PASS / DEGRADED / SATURATED / FAILED against the
//! tick budget (p99 tick ≤ budget) and the no-silent-loss rule.

use std::time::{Duration, Instant};

use game_server::{
    game_factory, move_args, CLIENT_REDUCERS, ARENA_HEIGHT, ARENA_WIDTH,
};
use nexum_game_server::{
    GameInstanceConfig, GameServer, GameServerConfig, JoinOutcome,
};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_runtime::{Runtime, RuntimeConfig};
use nexum_sdk::{transport::ClientTransport, Client, SdkConfig};
use nexum_subscription::Query;

// ---------------------------------------------------------------- harness

struct Args {
    clients: usize,
    profile: char,
    ticks: u64,
    hz: u64,
    partitions: usize,
    workers: usize,
    /// Subscription window per client (realistic interest management —
    /// clients never hold the whole table; Phase 15 measured full-table
    /// snapshots as O(N) per subscriber).
    window: u32,
    /// Runtime per-world queued reducer-call cap (sized to the workload;
    /// overflow is explicit backpressure, never silent loss).
    queue: usize,
}

fn parse_args() -> Args {
    let mut args = Args {
        clients: 1_000,
        profile: 'B',
        ticks: 200,
        hz: 20,
        partitions: 1,
        workers: 1,
        window: 32,
        queue: 1 << 20,
    };
    let mut it = std::env::args().skip(1);
    while let Some(arg) = it.next() {
        let mut value = || it.next().unwrap_or_else(|| panic!("{arg} needs a value"));
        match arg.as_str() {
            "--clients" => args.clients = value().parse().unwrap(),
            "--profile" => args.profile = value().parse().unwrap(),
            "--ticks" => args.ticks = value().parse().unwrap(),
            "--hz" => args.hz = value().parse().unwrap(),
            "--partitions" => args.partitions = value().parse().unwrap(),
            "--workers" => args.workers = value().parse().unwrap(),
            "--window" => args.window = value().parse().unwrap(),
            "--queue" => args.queue = value().parse().unwrap(),
            "--help" | "-h" => {
                println!(
                    "usage: ccu [--clients N] [--profile A|B|C|D] [--ticks N] \
                     [--hz N] [--partitions N] [--workers N] [--window N] \
                     [--queue N]"
                );
                std::process::exit(0);
            }
            other => panic!("unknown option: {other}"),
        }
    }
    args
}

fn auth_for(n: usize) -> std::sync::Arc<TokenAuthenticator> {
    let mut auth = TokenAuthenticator::new();
    for i in 0..n {
        let name = format!("p{i}");
        auth.add(&name, Principal::new(i as u64 + 1, &name)).unwrap();
    }
    std::sync::Arc::new(auth)
}

/// A simulated client: a real SDK client (its player id and subscription
/// handle are implicit in the server-side join and the SDK's own state).
struct SimClient {
    client: Client,
}

/// Boots the real stack with the arena game running and reducers exposed.
fn boot(args: &Args) -> (GameServer, nexum_core::GameInstanceId) {
    let runtime_config = RuntimeConfig::new(game_factory())
        .with_worker_count(args.workers)
        .with_max_queued_reducer_calls(args.queue);
    let runtime = Runtime::new(runtime_config).expect("runtime");
    let network = NetworkConfig::new()
        .with_max_connections(args.clients.saturating_add(16))
        .with_max_queued_outbound_frames(args.clients.saturating_add(16));
    let server_config = GameServerConfig::new().with_tick_rate_hz(args.hz as u32);
    let mut server = GameServer::new(
        runtime,
        network,
        auth_for(args.clients),
        server_config,
    )
    .expect("game server");
    let game = server
        .create_game(
            GameInstanceConfig::new("arena")
                .with_partition_count(args.partitions)
                .with_max_players(args.clients)
                .with_on_player_join("player_join"),
        )
        .expect("create game");
    server.start_game(game).expect("start game");
    for reducer in CLIENT_REDUCERS {
        server.expose_reducer(reducer).expect("expose reducer");
    }
    (server, game)
}

/// Connects one SDK client end-to-end: handshake, authenticate, join (the
/// server routes to a partition), attach, subscribe with a bounded window.
fn connect_one(
    server: &mut GameServer,
    game: nexum_core::GameInstanceId,
    token: &str,
    window: u32,
) -> SimClient {
    let (transport, server_conn) = ClientTransport::memory_pair(4096, 4096);
    server
        .gateway_mut()
        .register_connection(Box::new(server_conn))
        .expect("register connection");
    let mut client = Client::new(SdkConfig::new()).expect("sdk client");
    client.connect(transport.into_inner()).expect("connect");
    server.gateway_mut().process_inbound();
    client.pump().expect("pump handshake");
    assert!(client.is_connected(), "handshake completes");

    client.authenticate(token).expect("authenticate");
    server.gateway_mut().process_inbound();
    client.pump().expect("pump auth");
    let principal = client.session_principal().expect("authenticated").clone();

    let outcome = server.join_game(&principal, game).expect("join game");
    assert_eq!(outcome, JoinOutcome::Joined, "fresh join");
    let world = server
        .player_world(nexum_core::PlayerId::from_u64(principal.id()))
        .expect("player world");

    client.attach(world).expect("attach");
    server.gateway_mut().process_inbound();
    client.pump().expect("pump attach");
    assert_eq!(client.attached_world(), Some(world), "attached");

    let query = Query::builder("players")
        .limit(window)
        .build()
        .expect("query");
    client.subscribe(query).expect("subscribe");
    server.gateway_mut().process_inbound();
    client.pump().expect("pump subscribe");
    client.take_events();
    SimClient { client }
}

/// One server-side step + client pumps (mirrors the game server loop).
fn step(server: &mut GameServer, clients: &mut [SimClient]) {
    server.gateway_mut().process_inbound();
    let _ = server.step();
    server.gateway_mut().pump_subscriptions();
    server.gateway_mut().flush_outbound().expect("flush outbound");
    for sim in clients.iter_mut() {
        sim.client.pump().expect("client pump");
    }
}

/// Server-side half of one step (inbound + tick + fan-out), timed separately
/// from the client pumps for profiling.
fn step_server(server: &mut GameServer) {
    server.gateway_mut().process_inbound();
    let _ = server.step();
    server.gateway_mut().pump_subscriptions();
    server.gateway_mut().flush_outbound().expect("flush outbound");
}

/// Client-side half: drain every client's inbound frames.
fn step_clients(clients: &mut [SimClient]) {
    for sim in clients.iter_mut() {
        sim.client.pump().expect("client pump");
    }
}

/// A realistic client consumes its event stream every tick (like a render
/// loop); the harness must drain too, or queues grow over the measured run.
fn drain_clients(clients: &mut [SimClient]) {
    for sim in clients.iter_mut() {
        sim.client.take_events();
        sim.client.take_reducer_results();
    }
}

/// Drives the profile workload for one tick across all clients.
fn drive_profile(profile: char, tick: u64, clients: &mut [SimClient], hz: u64) {
    match profile {
        'A' => {
            // Connection only: nothing to send; subscription deltas are the
            // only traffic (join commits on the first tick, then quiet).
        }
        'B' => {
            // Movement at a realistic ~5 Hz (every hz/5 ticks), alternating
            // directions.
            if tick.is_multiple_of((hz / 5).max(1)) {
                for (i, sim) in clients.iter_mut().enumerate() {
                    let dx = if i % 2 == 0 { 1 } else { -1 };
                    let _ = sim.client.call_reducer("move_player", move_args(dx, 0));
                }
            }
        }
        'C' => {
            // Movement every 3 ticks + an occasional fire.
            if tick.is_multiple_of(3) {
                for (i, sim) in clients.iter_mut().enumerate() {
                    let dx = if i % 2 == 0 { 1 } else { -1 };
                    let dy = if i % 3 == 0 { 1 } else { 0 };
                    let _ = sim.client.call_reducer("move_player", move_args(dx, dy));
                }
            }
            if tick.is_multiple_of(hz * 5) {
                for sim in clients.iter_mut() {
                    let _ = sim.client.call_reducer(
                        "fire_weapon",
                        nexum_reducer::ReducerArgs::new(),
                    );
                }
            }
        }
        'D' => {
            // Stress: every client moves every tick; every 4th tick fire.
            for (i, sim) in clients.iter_mut().enumerate() {
                let dx = if i % 2 == 0 { 1 } else { -1 };
                let _ = sim.client.call_reducer("move_player", move_args(dx, 0));
            }
            if tick.is_multiple_of(4) {
                for sim in clients.iter_mut() {
                    let _ = sim.client.call_reducer(
                        "fire_weapon",
                        nexum_reducer::ReducerArgs::new(),
                    );
                }
            }
        }
        _ => panic!("unknown profile"),
    }
}

/// Runs the harness and classifies the result honestly.
fn main() {
    let args = parse_args();
    println!(
        "CCU harness: clients={} profile={} ticks={} hz={} partitions={} workers={} window={}",
        args.clients, args.profile, args.ticks, args.hz, args.partitions, args.workers, args.window
    );

    let started = Instant::now();
    let (mut server, game) = boot(&args);

    // Connect every client (one at a time through the real gateway).
    let mut clients = Vec::with_capacity(args.clients);
    for i in 0..args.clients {
        let token = format!("p{i}");
        let sim = connect_one(&mut server, game, &token, args.window);
        clients.push(sim);
    }
    let connect_elapsed = started.elapsed();
    println!(
        "connected {} clients in {:.1}s ({:.0} conn/s)",
        args.clients,
        connect_elapsed.as_secs_f64(),
        args.clients as f64 / connect_elapsed.as_secs_f64()
    );

    // Let the first join commits land.
    step(&mut server, &mut clients);

    // Warmup: a few ticks to populate caches before measuring.
    for tick in 0..10 {
        drive_profile(args.profile, tick, &mut clients, args.hz);
        step(&mut server, &mut clients);
    }

    // Measured phase.
    let tick_budget = Duration::from_millis(1000 / args.hz);
    let mut tick_samples: Vec<Duration> = Vec::with_capacity(args.ticks as usize);
    let measured_started = Instant::now();
    let accepted_before = server.gateway().metrics().inputs_accepted
        + server.gateway().metrics().reducer_calls_accepted;
    let dropped_before = server.gateway().metrics().messages_dropped;
    let rejected_before = server.gateway().metrics().inputs_rejected
        + server.gateway().metrics().reducer_calls_rejected;

    for tick in 0..args.ticks {
        drive_profile(args.profile, tick, &mut clients, args.hz);
        let tick_started = Instant::now();
        step_server(&mut server);
        step_clients(&mut clients);
        drain_clients(&mut clients);
        tick_samples.push(tick_started.elapsed());
    }
    let measured_elapsed = measured_started.elapsed();

    // ---- analysis -------------------------------------------------------
    tick_samples.sort_unstable();
    let p50 = tick_samples[tick_samples.len() / 2];
    let p95 = tick_samples[(tick_samples.len() as f64 * 0.95) as usize];
    let p99 = tick_samples[(tick_samples.len() as f64 * 0.99) as usize];
    let avg = tick_samples.iter().sum::<Duration>() / tick_samples.len() as u32;

    let metrics = server.gateway().metrics();
    let runtime_metrics = server.runtime().metrics();
    let game_metrics = server.metrics();
    let accepted_delta = (metrics.inputs_accepted + metrics.reducer_calls_accepted)
        .saturating_sub(accepted_before);
    let dropped_delta = metrics.messages_dropped.saturating_sub(dropped_before);
    let rejected_delta = (metrics.inputs_rejected + metrics.reducer_calls_rejected)
        .saturating_sub(rejected_before);

    println!("\n=== RESULTS (measured phase: {:.1}s) ===", measured_elapsed.as_secs_f64());
    println!("tick:  p50={:.1}us  p95={:.1}us  p99={:.1}us  avg={:.1}us  budget={:.1}ms",
        p50.as_micros(), p95.as_micros(), p99.as_micros(), avg.as_micros(),
        tick_budget.as_millis() as f64);
    println!("work:  accepted={accepted_delta}  rejected={rejected_delta}  dropped={dropped_delta}");
    println!("state: ticks_ok={} ticks_failed={} worlds={} partitions={}",
        runtime_metrics.ticks_succeeded, runtime_metrics.ticks_failed,
        runtime_metrics.running_worlds, runtime_metrics.partitions);
    println!("net:   conns={} sessions={} subs={} frames={} rate_limited={}",
        metrics.connections, metrics.sessions, metrics.subscriptions,
        metrics.frames_received, metrics.rate_limited);
    println!("game:  players_active={} games={} reducer_calls={}",
        game_metrics.players_active, game_metrics.games_active, game_metrics.reducer_calls);
    println!("mem:   est.~{}MB (rows × 88B + conns × 2KB + 8MB base)",
        (args.clients as u64).saturating_mul(2 * 1024) / (1024 * 1024) + 8);

    // ---- classification (honest, ADR-016 D4) ---------------------------
    let p99_over_budget = p99 > tick_budget;
    let silently_lost = dropped_delta > 0 && accepted_delta > 0;
    let any_failure = runtime_metrics.ticks_failed > 0;
    let rejected_heavily = rejected_delta as f64 / (accepted_delta.max(1)) as f64 > 0.5;

    let class = if silently_lost {
        "FAILED — accepted work was dropped"
    } else if any_failure {
        "FAILED — tick failures observed"
    } else if p99_over_budget {
        if p99 > tick_budget.saturating_mul(2) {
            "SATURATED — p99 exceeds 2× tick budget (the ceiling)"
        } else {
            "DEGRADED — p99 approaches/ exceeds the tick budget"
        }
    } else if rejected_heavily {
        "DEGRADED — heavy explicit rejections (rate limits / queues)"
    } else {
        "PASS — p99 within budget, no silent loss, no failures"
    };
    println!("\nCLASSIFICATION: {class}");
    println!("note: in-process transport; protocol/gateway/runtime/world/subscriptions/SDK are real.");
    println!("arena size: {ARENA_WIDTH}x{ARENA_HEIGHT}; movement validation is authoritative.");
}
