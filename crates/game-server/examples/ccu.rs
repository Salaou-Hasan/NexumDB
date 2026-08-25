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
    ARENA_HEIGHT, ARENA_WIDTH, CLIENT_REDUCERS, POS_INDEX, TABLE, fire_weapon_module, game_factory,
    move_args,
};
use nexum_game_server::{GameInstanceConfig, GameServer, GameServerConfig, JoinOutcome};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_runtime::{Runtime, RuntimeConfig};
use nexum_sdk::{Client, SdkConfig, transport::ClientTransport};
use nexum_subscription::Query;

// ---------------------------------------------------------------- alloc

/// Phase 21.5 allocation profiling: installs the workspace counting global
/// allocator (`nexum-alloc-count`) only when the `ccu-alloc` feature is
/// enabled. The timing ladder runs without this feature so measurements are
/// unperturbed; dedicated `--count-alloc` runs report allocs/tick and
/// bytes/tick.
#[cfg(feature = "ccu-alloc")]
#[global_allocator]
static ALLOCATOR: nexum_alloc_count::CountingAlloc = nexum_alloc_count::CountingAlloc::new();

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
    /// Number of lobbies (separate games/worlds). When > 1, clients are
    /// distributed evenly across lobbies, each lobby having its own world.
    /// This tests the lobby-based architecture where 1K players per lobby
    /// replaces 20K in a single world.
    lobbies: usize,
    /// Print per-phase timing breakdown for every tick.
    profile_detail: bool,
    /// Enable per-reducer execution profiling (Phase 21.5) and print the
    /// measured per-reducer call counts and average execution time.
    reducer_profile: bool,
    /// Enable the counting allocator (requires the `ccu-alloc` feature) and
    /// report allocs/tick and bytes/tick over the measured phase.
    count_alloc: bool,
    /// Phase 22: print a per-stage breakdown of one real `fire_weapon` WASM
    /// invocation (store setup / instantiate / encode / exec / result) and
    /// exit. Requires no clients; ignores the rest of the harness.
    wasm_stages: bool,
    /// Phase 26 OS tuning (Windows): 1 ms timer resolution,
    /// HIGH_PRIORITY_CLASS, rayon-pool worker affinity. On by default;
    /// `--no-os-tune` restores raw scheduler behavior for A/B comparison.
    os_tune: bool,
}

fn parse_args() -> Args {
    // workers = 0 means "auto": resolved in boot() to
    // min(available_parallelism, lobbies × partitions) — a production
    // deployment ticks independent lobbies in parallel (Phase 18), so the
    // harness must too, or it measures a serial strawman. More workers than
    // worlds is pure idle overhead.
    let mut args = Args {
        clients: 1_000,
        profile: 'B',
        ticks: 200,
        hz: 20,
        partitions: 1,
        workers: 0,
        window: 32,
        queue: 1 << 20,
        profile_detail: false,
        reducer_profile: false,
        count_alloc: false,
        lobbies: 1,
        wasm_stages: false,
        os_tune: true,
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
            "--lobbies" => args.lobbies = value().parse().unwrap(),
            "--profile-detail" => args.profile_detail = true,
            "--reducer-profile" => args.reducer_profile = true,
            "--count-alloc" => args.count_alloc = true,
            "--wasm-stages" => args.wasm_stages = true,
            "--os-tune" => args.os_tune = true,
            "--no-os-tune" => args.os_tune = false,
            "--help" | "-h" => {
                println!(
                    "usage: ccu [--clients N] [--profile A|B|C|D|E] [--ticks N] \
                     [--hz N] [--partitions N] [--workers N] [--window N] \
                     [--queue N] [--lobbies N] [--profile-detail] \
                     [--reducer-profile] [--count-alloc] [--wasm-stages]"
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
        auth.add(&name, Principal::new(i as u64 + 1, &name))
            .unwrap();
    }
    std::sync::Arc::new(auth)
}

/// A simulated client: a real SDK client (its player id and subscription
/// handle are implicit in the server-side join and the SDK's own state).
struct SimClient {
    client: Client,
}

/// Boots the real stack with arena games (lobbies) running and reducers exposed.
/// Returns the server and a list of game ids (one per lobby).
fn boot(args: &Args) -> (GameServer, Vec<nexum_core::GameInstanceId>) {
    let runtime_config = RuntimeConfig::new(game_factory())
        .with_worker_count(args.workers)
        .with_max_queued_reducer_calls(args.queue);
    let runtime = Runtime::new(runtime_config).expect("runtime");
    let network = NetworkConfig::new()
        .with_max_connections(args.clients.saturating_add(16))
        .with_max_queued_outbound_frames(args.clients.saturating_add(16))
        .with_tick_update_changes(false)
        .with_skip_empty_broadcast(true);
    let server_config = GameServerConfig::new()
        .with_tick_rate_hz(args.hz as u32)
        .with_reducer_profiling(args.reducer_profile);
    let mut server = GameServer::new(runtime, network, auth_for(args.clients), server_config)
        .expect("game server");
    // Create lobbies: each lobby is a separate game with its own world.
    let players_per_lobby = args.clients.div_ceil(args.lobbies);
    let mut games = Vec::with_capacity(args.lobbies);
    for lobby in 0..args.lobbies {
        let game = server
            .create_game(
                GameInstanceConfig::new(format!("arena_{lobby}"))
                    .with_partition_count(args.partitions)
                    .with_max_players(players_per_lobby)
                    .with_on_player_join("player_join"),
            )
            .expect("create game");
        server.start_game(game).expect("start game");
        games.push(game);
    }
    for reducer in CLIENT_REDUCERS {
        server.expose_reducer(reducer).expect("expose reducer");
    }
    (server, games)
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
    // pump_subscriptions removed: fan_out_results inside step() already drains.
    server
        .gateway_mut()
        .flush_outbound()
        .expect("flush outbound");
    for sim in clients.iter_mut() {
        sim.client.pump().expect("client pump");
    }
}

/// Server-side half of one step (inbound + tick + fan-out), timed separately
/// from the client pumps for profiling.
/// Per-phase timing accumulator for profiling.
#[derive(Default)]
struct PhaseTimers {
    inbound_ns: u64,
    tick_ns: u64,
    fanout_ns: u64,
    pump_ns: u64,
    flush_ns: u64,
    client_ns: u64,
    drain_ns: u64,
    count: u64,
    /// Sub-phases of the runtime tick: world tick / WAL / subscription apply.
    world_tick_ns: u64,
    wal_ns: u64,
    sub_apply_ns: u64,
    /// Per-tick phase samples (inbound, tick, fanout, pump, flush, clients,
    /// drain) for latency-spike investigation.
    tick_samples: Vec<(u64, u64, u64, u64, u64, u64, u64)>,
}

fn step_server_timed(server: &mut GameServer, t: &mut PhaseTimers) {
    let t0 = Instant::now();
    server.gateway_mut().process_inbound();
    let t1 = Instant::now();
    // Split the game-server step into its public halves: the authoritative
    // runtime tick (parallel worlds) and the gateway fan-out (TickUpdate
    // broadcast + subscription pumps + reducer-result routing).
    let results = server.runtime_mut().step_detailed().expect("runtime tick");
    let t2 = Instant::now();
    let _ = server.gateway_mut().fan_out_results(&results);
    // pump_subscriptions is NOT called here: fan_out_results already drains
    // every subscriber's buffer during the per-world pump pass. A separate
    // pump_subscriptions call would re-iterate all connections finding empty
    // buffers — pure overhead.
    let t3 = Instant::now();
    server
        .gateway_mut()
        .flush_outbound()
        .expect("flush outbound");
    let t4 = Instant::now();
    let inbound = t1.duration_since(t0).as_nanos() as u64;
    let tick = t2.duration_since(t1).as_nanos() as u64;
    let fanout = t3.duration_since(t2).as_nanos() as u64;
    let pump = 0u64;
    let flush = t4.duration_since(t3).as_nanos() as u64;
    t.inbound_ns += inbound;
    t.tick_ns += tick;
    t.fanout_ns += fanout;
    t.pump_ns += pump;
    t.flush_ns += flush;
    t.count += 1;
    // Read the runtime's per-tick sub-phase profile (world tick / WAL /
    // subscription apply) from the last committed tick.
    let profile = server.runtime_mut().metrics().last_tick_profile;
    t.world_tick_ns += profile.0;
    t.wal_ns += profile.1;
    t.sub_apply_ns += profile.2;
    // Keep a per-tick phase sample; the caller fills in clients/drain after
    // the pumps.
    t.tick_samples
        .push((inbound, tick, fanout, pump, flush, 0, 0));
}

/// Client-side half: drain every client's inbound frames.
fn step_clients(clients: &mut [SimClient]) {
    use rayon::prelude::*;
    // Parallel pump: each chunk is processed sequentially, chunks in
    // parallel. At 20K clients this eliminates the O(CCU) serial pump
    // bottleneck (was 72ms at 20K). Chunk size 256 balances work per
    // thread with scheduling overhead.
    clients.par_chunks_mut(256).for_each(|chunk| {
        for sim in chunk.iter_mut() {
            sim.client.pump().expect("client pump");
        }
    });
}

/// A realistic client consumes its event stream every tick (like a render
/// loop); the harness must drain too, or queues grow over the measured run.
fn drain_clients(clients: &mut [SimClient]) {
    // Drain is intentionally sequential: take_events/take_reducer_results
    // are trivial O(1) swaps. Parallel rayon overhead at 20K clients
    // exceeds the work, making it 3× slower.
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
                    let _ = sim
                        .client
                        .call_reducer("fire_weapon", nexum_reducer::ReducerArgs::new());
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
                    let _ = sim
                        .client
                        .call_reducer("fire_weapon", nexum_reducer::ReducerArgs::new());
                }
            }
        }
        'E' => {
            // Extreme real gameplay (Phase 21.5): every client moves every
            // tick, fires every 10 ticks (≈2/s at 20 Hz), and reloads every
            // 25 ticks — the full hot path (native move + WASM fire +
            // native reload) with subscription deltas, TickUpdate decode,
            // and drain active every tick.
            for (i, sim) in clients.iter_mut().enumerate() {
                let dx = if i % 2 == 0 { 1 } else { -1 };
                let dy = if i % 3 == 0 { 1 } else { 0 };
                let _ = sim.client.call_reducer("move_player", move_args(dx, dy));
            }
            if tick.is_multiple_of(10) {
                for sim in clients.iter_mut() {
                    let _ = sim
                        .client
                        .call_reducer("fire_weapon", nexum_reducer::ReducerArgs::new());
                }
            }
            if tick.is_multiple_of(25) {
                for sim in clients.iter_mut() {
                    let _ = sim
                        .client
                        .call_reducer("reload_weapon", nexum_reducer::ReducerArgs::new());
                }
            }
        }
        _ => panic!("unknown profile"),
    }
}

/// Phase 22: per-stage cost of one real `fire_weapon` WASM invocation.
///
/// Uses the production module bytes and the exact game schema, populated
/// with a dense arena so the combat lookup path is exercised (not the
/// early-reject path). Each invocation runs against a fresh transaction via
/// the timed entry point; the stage totals are aggregated over `n` calls.
fn wasm_stage_breakdown() {
    use nexum_core::{ColumnType, TableSchema, row};
    use nexum_reducer::ReducerArgs;
    use nexum_table::TableStore;
    use nexum_tx::Transaction;
    use nexum_wasm::{WasmLimits, WasmModuleRegistry};

    let mut store = TableStore::new();
    let schema = TableSchema::builder(TABLE)
        .column("id", ColumnType::U64)
        .column("x", ColumnType::I64)
        .column("y", ColumnType::I64)
        .column("hp", ColumnType::I64)
        .column("max_hp", ColumnType::I64)
        .column("alive", ColumnType::I64)
        .column("score", ColumnType::I64)
        .column("cooldown", ColumnType::I64)
        .column("facing", ColumnType::I64)
        .column("ammo", ColumnType::I64)
        .column("connected", ColumnType::I64)
        .primary_key(&["id"])
        .index(POS_INDEX, &["x", "y"])
        .build()
        .expect("valid players schema");
    store.create_table(schema).expect("players table created");

    // Dense arena: every cell occupied, alive, ready to fire, so the combat
    // target lookup always finds a candidate.
    const PLAYERS: u64 = 4_000;
    for id in 1..=PLAYERS {
        // 1-based ids (the game's player ids start at 1); dense placement.
        let n = id - 1;
        let (x, y) = (
            n % ARENA_WIDTH as u64,
            (n / ARENA_WIDTH as u64) % ARENA_HEIGHT as u64,
        );
        let mut tx = Transaction::begin(&mut store);
        tx.insert(
            &store,
            TABLE,
            row![
                id, x as i64, y as i64, 100i64, 100i64, 1i64, 0i64, 0i64, 1i64, 10i64, 1i64
            ],
        )
        .unwrap();
        tx.commit(&mut store).unwrap();
    }

    let mut registry = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
    registry
        .register("fire_weapon", 1, fire_weapon_module())
        .unwrap();
    // Match the production game configuration (Phase 26 instance pooling).
    registry.set_poolable("fire_weapon", true).unwrap();

    let iterations = 20_000usize;
    let warmup = 2_000usize;
    let mut acc = nexum_wasm::WasmStageTimes::default();
    let mut n = 0u64;
    for i in 0..(iterations + warmup) {
        let caller = (i as u64 % PLAYERS) + 1; // ids are 1-based in the game
        let args = ReducerArgs::new().insert("target", caller);
        let mut tx = Transaction::begin(&mut store);
        let outcome = registry.invoke_in_tx_timed(&store, &mut tx, "fire_weapon", &args);
        let _ = tx.abort();
        let (_, _, times) = outcome.expect("real fire_weapon invocation succeeds");
        if i >= warmup {
            acc.store_setup_ns += times.store_setup_ns;
            acc.instantiate_ns += times.instantiate_ns;
            acc.encode_ns += times.encode_ns;
            acc.exec_ns += times.exec_ns;
            acc.result_ns += times.result_ns;
            acc.total_ns += times.total_ns;
            n += 1;
        }
    }
    let ns = |v: u64| v as f64 / n as f64;
    let total = ns(acc.total_ns).max(1.0);
    println!(
        "\n=== WASM STAGE BREAKDOWN (real fire_weapon, {} calls) ===",
        n
    );
    println!(
        "  store_setup {:>9.1} ns  ({:>5.1}%)",
        ns(acc.store_setup_ns),
        ns(acc.store_setup_ns) / total * 100.0
    );
    println!(
        "  instantiate {:>9.1} ns  ({:>5.1}%)",
        ns(acc.instantiate_ns),
        ns(acc.instantiate_ns) / total * 100.0
    );
    println!(
        "  encode      {:>9.1} ns  ({:>5.1}%)",
        ns(acc.encode_ns),
        ns(acc.encode_ns) / total * 100.0
    );
    println!(
        "  exec        {:>9.1} ns  ({:>5.1}%)",
        ns(acc.exec_ns),
        ns(acc.exec_ns) / total * 100.0
    );
    println!(
        "  result      {:>9.1} ns  ({:>5.1}%)",
        ns(acc.result_ns),
        ns(acc.result_ns) / total * 100.0
    );
    println!("  total       {:>9.1} ns/call", total);

    // Harness-style loop: one tick transaction; each call branches off it,
    // invokes, and absorbs back (exactly the World Phase 0c pattern). This
    // isolates the transaction branch/absorb cost from the rest of the
    // pipeline: if it is far above the isolated per-call cost above, the
    // O(parent-writes) copy per call is the measured harness bottleneck.
    // One tick tx with CALLS_PER_TICK branch/invoke/absorb calls (the World
    // Phase 0c pattern at harness burst scale). With the write set growing
    // every call, an O(parent-writes) branch copy shows up as a per-call
    // cost that grows with burst position; the average over the burst is
    // reported.
    const CALLS_PER_TICK: usize = 2_000;
    let mut total_ns = 0u64;
    let mut branch_ns = 0u64;
    let mut invoke_ns = 0u64;
    let mut absorb_ns = 0u64;
    let mut total_n = 0u64;
    for _ in 0..200 {
        let mut tick_tx = Transaction::begin(&mut store);
        for i in 0..CALLS_PER_TICK {
            let caller = (i as u64 % PLAYERS) + 1;
            let args = ReducerArgs::new().insert("target", caller);
            let t0 = std::time::Instant::now();
            let mut child = Transaction::new(tick_tx.id());
            child.branch_of(&tick_tx).expect("branch");
            let t1 = std::time::Instant::now();
            let outcome = registry.invoke_in_tx(&store, &mut child, "fire_weapon", &args);
            let t2 = std::time::Instant::now();
            match outcome {
                Ok((_, _events)) => {
                    tick_tx.absorb(child).expect("absorb");
                }
                Err(_) => {
                    let _ = child.abort();
                }
            }
            let t3 = std::time::Instant::now();
            branch_ns += t1.duration_since(t0).as_nanos() as u64;
            invoke_ns += t2.duration_since(t1).as_nanos() as u64;
            absorb_ns += t3.duration_since(t2).as_nanos() as u64;
            total_ns += t3.duration_since(t0).as_nanos() as u64;
            total_n += 1;
        }
        let _ = tick_tx.abort();
    }
    println!(
        "\n=== HARNESS-STYLE LOOP ({CALLS_PER_TICK} branch/invoke/absorb per tick tx, {total_n} calls) ==="
    );
    println!(
        "  branch    {:>9.1} ns/call",
        branch_ns as f64 / total_n as f64
    );
    println!(
        "  invoke    {:>9.1} ns/call",
        invoke_ns as f64 / total_n as f64
    );
    println!(
        "  absorb    {:>9.1} ns/call",
        absorb_ns as f64 / total_n as f64
    );
    println!(
        "  total     {:>9.1} ns/call",
        total_ns as f64 / total_n as f64
    );

    // -------------------------------------------------------------- absorb scaling
    // Isolates the branch/write/absorb cycle from WASM entirely: does the
    // per-call cost grow with the parent transaction's accumulated write
    // set (the O(parent) signature), or stay flat?
    use nexum_core::{RowId as WRowId, TableSchema as WSchema};
    use nexum_tx::Transaction as WTx;
    println!("\n=== BRANCH/WRITE/ABSORB SCALING (pure tx cycle, no WASM) ===");
    // Variant A: raw WriteSet only (no Transaction, no ReadSet, no store).
    {
        use nexum_tx::WriteSet;
        let mkrow = || nexum_core::row![9_999u64, 7i32];
        for w in [0usize, 1000] {
            let mut pws = WriteSet::new();
            for i in 0..w {
                pws.update(
                    nexum_core::TableId::from_u64(0),
                    WRowId::from_u64(i as u64),
                    false,
                    mkrow(),
                )
                .unwrap();
            }
            const CALLS: usize = 20_000;
            let mut t = 0u128;
            for i in 0..CALLS {
                let mut cws = pws.branch();
                cws.update(
                    nexum_core::TableId::from_u64(0),
                    WRowId::from_u64((w + 50_000 + i) as u64),
                    false,
                    mkrow(),
                )
                .unwrap();
                let t0 = Instant::now();
                pws.absorb(cws);
                t += (Instant::now() - t0).as_nanos();
            }
            println!(
                "  [raw WriteSet]   parent_writes={w:>5}:   absorb {:>8.1} ns/call",
                t as f64 / CALLS as f64
            );
        }
    }
    let mut scale_store = nexum_table::TableStore::new();
    scale_store
        .create_table(
            WSchema::builder("players")
                .column("id", nexum_core::ColumnType::U64)
                .column("x", nexum_core::ColumnType::I32)
                .primary_key(&["id"])
                .build()
                .unwrap(),
        )
        .unwrap();
    {
        fn seed(store: &nexum_table::TableStore, parent: &mut WTx, n: usize) {
            for i in 0..n {
                parent
                    .update(
                        store,
                        "players",
                        WRowId::from_u64(i as u64),
                        nexum_core::row![i as u64, 5i32],
                    )
                    .unwrap();
            }
        }
        for w in [0usize, 250, 500, 1000, 2000] {
            let mut parent = WTx::begin(&mut scale_store);
            seed(&scale_store, &mut parent, w);
            const CALLS: usize = 3_000;
            let mut t_absorb = 0u128;
            let mut t_cycle = 0u128;
            for i in 0..CALLS {
                let rid = WRowId::from_u64((w + 10_000 + i) as u64);
                let t0 = Instant::now();
                let mut child = WTx::new(parent.id());
                child.branch_of(&parent).unwrap();
                child
                    .update(
                        &scale_store,
                        "players",
                        rid,
                        nexum_core::row![9_999u64, 7i32],
                    )
                    .unwrap();
                let t1 = Instant::now();
                parent.absorb(child).unwrap();
                let t2 = Instant::now();
                t_absorb += (t2 - t1).as_nanos();
                t_cycle += (t2 - t0).as_nanos();
            }
            println!(
                "  parent_writes={w:>5}:   absorb {:>8.1} ns/call   full cycle {:>8.1} ns/call",
                t_absorb as f64 / CALLS as f64,
                t_cycle as f64 / CALLS as f64
            );
            let _ = parent.abort();
        }
        println!();
    }
}

/// Phase 26 OS tuning: reduce kernel-side latency noise so the measured
/// percentiles reflect the engine, not the scheduler.
///
/// - **Timer resolution** (`timeBeginPeriod(1)`): Windows' default 15.6 ms
///   timer granularity makes `thread::sleep` overshoot and lengthens
///   scheduler quanta; both inflate tail latencies.
/// - **Process priority** (`HIGH_PRIORITY_CLASS`): keeps worker threads from
///   being preempted by background work under load.
/// - **Worker affinity**: the runtime's parallel tick path runs on the
///   global rayon pool; pre-building that pool with a custom spawner pins
///   each pool thread to one logical processor (round-robin), cutting
///   cross-core migration and cache bouncing. On Intel hybrid CPUs Windows
///   enumerates P-cores at lower processor numbers, so round-robin packs
///   the first workers onto P-cores.
#[cfg(windows)]
fn os_tune() {
    use windows_sys::Win32::Media::timeBeginPeriod;
    use windows_sys::Win32::System::Threading::{
        GetCurrentProcess, GetCurrentThread, HIGH_PRIORITY_CLASS, SetPriorityClass,
        SetThreadAffinityMask,
    };
    // SAFETY: these are plain Win32 configuration calls with no memory
    // ownership implications.
    unsafe {
        timeBeginPeriod(1);
        SetPriorityClass(GetCurrentProcess(), HIGH_PRIORITY_CLASS);
    }
    let procs = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    let _ = rayon::ThreadPoolBuilder::new()
        .num_threads(procs)
        .spawn_handler(move |thread| {
            let index = thread.index();
            std::thread::Builder::new()
                .name(format!("nexum-worker-{index}"))
                .spawn(move || {
                    // Round-robin over logical processors (see doc comment).
                    #[allow(clippy::useless_conversion)]
                    let mask: usize = 1usize << (index % procs);
                    unsafe {
                        SetThreadAffinityMask(GetCurrentThread(), mask);
                    }
                    thread.run();
                })
                .expect("spawn rayon pool thread");
            Ok(())
        })
        .build_global();
}

/// Non-Windows fallback: nothing to do yet (Linux equivalent would be
/// SCHED_FIFO / pthread_setaffinity_np).
#[cfg(not(windows))]
fn os_tune() {}

/// Runs the harness and classifies the result honestly.
fn main() {
    let mut args = parse_args();
    if args.os_tune {
        os_tune();
    }
    // Resolve the auto worker default (workers == 0) up front so the config
    // echo reports what actually runs: min(available_parallelism,
    // lobbies × partitions). More workers than worlds is idle overhead.
    if args.workers == 0 {
        let cores = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
        args.workers = cores.min(args.lobbies * args.partitions).max(1);
    }
    if args.wasm_stages {
        wasm_stage_breakdown();
        return;
    }
    println!(
        "CCU harness: clients={} profile={} ticks={} hz={} partitions={} workers={} window={} lobbies={}",
        args.clients,
        args.profile,
        args.ticks,
        args.hz,
        args.partitions,
        args.workers,
        args.window,
        args.lobbies
    );

    let started = Instant::now();
    let (mut server, games) = boot(&args);

    // Connect every client, distributing across lobbies.
    let mut clients = Vec::with_capacity(args.clients);
    for i in 0..args.clients {
        let token = format!("p{i}");
        let lobby = i % games.len();
        let sim = connect_one(&mut server, games[lobby], &token, args.window);
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

    // Warmup: a few ticks to populate caches before measuring. Clients are
    // drained too — otherwise the first measured tick pays the accumulated
    // warmup backlog as an artificial p99.9 spike (Phase 21.5 spike
    // investigation).
    for tick in 0..100 {
        drive_profile(args.profile, tick, &mut clients, args.hz);
        step(&mut server, &mut clients);
        drain_clients(&mut clients);
    }

    // Measured phase.
    // Let the per-connection fixed-window rate buckets (1 s) reset first:
    // the unpaced warmup burst would otherwise consume the entire first
    // measured second and report artificial rejections.
    std::thread::sleep(Duration::from_secs(1));
    let tick_budget = Duration::from_millis(1000 / args.hz);
    let mut tick_samples: Vec<Duration> = Vec::with_capacity(args.ticks as usize);
    let measured_started = Instant::now();
    let accepted_before = server.gateway().metrics().inputs_accepted
        + server.gateway().metrics().reducer_calls_accepted;
    let dropped_before = server.gateway().metrics().messages_dropped;
    let rejected_before = server.gateway().metrics().inputs_rejected
        + server.gateway().metrics().reducer_calls_rejected;
    let tick_before = server.gateway().metrics().tick_updates_sent;
    let sub_msg_before = server.gateway().metrics().subscription_messages_sent;
    let result_before = server.gateway().metrics().reducer_results_sent;
    // Phase 21.5: snapshot the per-reducer execution profile and (when the
    // `ccu-alloc` feature is built) the allocation counters so the measured
    // phase can be diffed.
    let reducer_before = if args.reducer_profile {
        server.runtime().reducer_profile()
    } else {
        std::collections::BTreeMap::new()
    };
    #[cfg(feature = "ccu-alloc")]
    let alloc_before = if args.count_alloc {
        nexum_alloc_count::enable();
        nexum_alloc_count::reset();
        nexum_alloc_count::snapshot()
    } else {
        (0, 0, 0)
    };

    let mut phase_timers = PhaseTimers::default();
    // Server-only per-tick samples: inbound+tick+fanout+flush wall time.
    // On a dedicated deployment the client pump/drain below runs on client
    // machines, so this is the honest server latency series.
    let mut server_samples: Vec<Duration> = Vec::with_capacity(args.ticks as usize);
    for tick in 0..args.ticks {
        drive_profile(args.profile, tick, &mut clients, args.hz);
        let tick_started = Instant::now();
        step_server_timed(&mut server, &mut phase_timers);
        let t_server_end = Instant::now();
        let t_mid1 = t_server_end;
        server_samples.push(t_server_end.duration_since(tick_started));
        step_clients(&mut clients);
        let t_mid2 = Instant::now();
        drain_clients(&mut clients);
        let t_end = Instant::now();
        tick_samples.push(t_end.duration_since(tick_started));
        // Pace the loop to the configured tick rate. A production server
        // ticks at --hz wall-clock; without this the harness blasts every
        // tick's work into a single wall-clock second, exhausting the
        // per-connection fixed-window rate limits and measuring rejection,
        // not gameplay. Percentiles above are unaffected: the sample is
        // pushed before the sleep, so it measures work time only.
        let elapsed = t_end.duration_since(tick_started);
        if elapsed < tick_budget {
            std::thread::sleep(tick_budget - elapsed);
        }
        if args.profile_detail {
            let client_ns = t_mid2.duration_since(t_mid1).as_nanos() as u64;
            let drain_ns = t_end.duration_since(t_mid2).as_nanos() as u64;
            phase_timers.client_ns += client_ns;
            phase_timers.drain_ns += drain_ns;
            // Complete this tick's phase sample for spike investigation.
            if let Some(sample) = phase_timers.tick_samples.last_mut() {
                sample.5 = client_ns;
                sample.6 = drain_ns;
            }
        }
    }
    let measured_elapsed = measured_started.elapsed();
    // Diff the per-reducer profile across the measured phase.
    let reducer_profile = if args.reducer_profile {
        let after = server.runtime().reducer_profile();
        let mut diff: std::collections::BTreeMap<String, (u64, u64)> =
            std::collections::BTreeMap::new();
        for (name, (calls, ns)) in after {
            let before = reducer_before.get(&name).copied().unwrap_or((0, 0));
            diff.insert(name, (calls - before.0, ns - before.1));
        }
        diff
    } else {
        std::collections::BTreeMap::new()
    };
    #[cfg(feature = "ccu-alloc")]
    let alloc_after = if args.count_alloc {
        nexum_alloc_count::snapshot()
    } else {
        (0, 0, 0)
    };

    // ---- analysis -------------------------------------------------------
    tick_samples.sort_unstable();
    let p50 = tick_samples[tick_samples.len() / 2];
    let p95 = tick_samples[(tick_samples.len() as f64 * 0.95) as usize];
    let p99 = tick_samples[(tick_samples.len() as f64 * 0.99) as usize];
    let p999 = tick_samples[(tick_samples.len() as f64 * 0.999) as usize];
    let max = *tick_samples.last().unwrap();
    let avg = tick_samples.iter().sum::<Duration>() / tick_samples.len() as u32;

    // Server-only latency series (inbound+tick+fanout+flush): what a
    // dedicated deployment would pay; the client pump/drain above runs on
    // client machines in production.
    server_samples.sort_unstable();
    let sp50 = server_samples[server_samples.len() / 2];
    let sp95 = server_samples[(server_samples.len() as f64 * 0.95) as usize];
    let sp99 = server_samples[(server_samples.len() as f64 * 0.99) as usize];
    let sp999 = server_samples[(server_samples.len() as f64 * 0.999) as usize];
    let savg = server_samples.iter().sum::<Duration>() / server_samples.len() as u32;

    let metrics = server.gateway().metrics();
    let runtime_metrics = server.runtime().metrics();
    let game_metrics = server.metrics();
    let accepted_delta =
        (metrics.inputs_accepted + metrics.reducer_calls_accepted).saturating_sub(accepted_before);
    let dropped_delta = metrics.messages_dropped.saturating_sub(dropped_before);
    let rejected_delta =
        (metrics.inputs_rejected + metrics.reducer_calls_rejected).saturating_sub(rejected_before);

    println!(
        "\n=== RESULTS (measured phase: {:.1}s) ===",
        measured_elapsed.as_secs_f64()
    );
    println!(
        "tick:  p50={:.1}us  p95={:.1}us  p99={:.1}us  p99.9={:.1}us  max={:.1}us  avg={:.1}us  budget={:.1}ms",
        p50.as_micros(),
        p95.as_micros(),
        p99.as_micros(),
        p999.as_micros(),
        max.as_micros(),
        avg.as_micros(),
        tick_budget.as_millis() as f64
    );
    println!(
        "work:  accepted={accepted_delta}  rejected={rejected_delta}  dropped={dropped_delta}"
    );
    println!(
        "server: p50={:.1}us  p95={:.1}us  p99={:.1}us  p99.9={:.1}us  avg={:.1}us",
        sp50.as_micros(),
        sp95.as_micros(),
        sp99.as_micros(),
        sp999.as_micros(),
        savg.as_micros()
    );
    println!(
        "state: ticks_ok={} ticks_failed={} worlds={} partitions={}",
        runtime_metrics.ticks_succeeded,
        runtime_metrics.ticks_failed,
        runtime_metrics.running_worlds,
        runtime_metrics.partitions
    );
    println!(
        "xpart: messages_sent={} delivered={} dropped={}",
        runtime_metrics.messages_sent,
        runtime_metrics.messages_delivered,
        runtime_metrics.messages_dropped
    );
    let subs_eval = runtime_metrics.subscription_evaluations;
    let subs_deltas = runtime_metrics.subscription_deltas;
    let per_change = subs_eval as f64 / (runtime_metrics.changes_committed.max(1)) as f64;
    println!(
        "subs:  evaluations={subs_eval} deltas={subs_deltas} per_change={per_change:.2} views={}",
        runtime_metrics.subscription_views
    );
    let tick_delta = metrics.tick_updates_sent.saturating_sub(tick_before);
    let sub_msg_delta = metrics
        .subscription_messages_sent
        .saturating_sub(sub_msg_before);
    let result_delta = metrics.reducer_results_sent.saturating_sub(result_before);
    println!(
        "net:   conns={} sessions={} subs={} frames={} rate_limited={}",
        metrics.connections,
        metrics.sessions,
        metrics.subscriptions,
        metrics.frames_received,
        metrics.rate_limited
    );
    println!(
        "out:   tick_updates={tick_delta} sub_deltas={sub_msg_delta} reducer_results={result_delta} total={}",
        tick_delta + sub_msg_delta + result_delta
    );
    println!(
        "game:  players_active={} games={} reducer_calls={}",
        game_metrics.players_active, game_metrics.games_active, game_metrics.reducer_calls
    );
    // Calibrated to measured steady-state RSS (Phase 18 follow-up): the
    // full stack (server + in-process SDK clients) needs ≈ 24.7 KB private
    // per connection with a ~6 MB base — linear fit over 5K/10K/15K/20K
    // measured samples. A mass-join storm without client consumption spikes
    // several× higher (un-drained SDK event buffers).
    println!(
        "mem:   est.~{}MB (steady-state fit: ~24.7KB/conn + 6MB base, incl. in-process SDK clients; join storm spikes several×)",
        (args.clients as u64).saturating_mul(25 * 1024) / (1024 * 1024) + 6
    );

    // ---- Phase 21.5: per-reducer execution profile ---------------------
    if args.reducer_profile {
        println!("\n=== REDUCER PROFILE (measured phase) ===");
        println!(
            "  {:<16} {:>10} {:>12} {:>14}",
            "reducer", "calls", "total ms", "avg \u{00b5}s"
        );
        for (name, (calls, ns)) in &reducer_profile {
            let avg_us = if *calls > 0 {
                *ns as f64 / *calls as f64 / 1e3
            } else {
                0.0
            };
            println!(
                "  {:<16} {:>10} {:>12.2} {:>14.2}",
                name,
                calls,
                *ns as f64 / 1e6,
                avg_us
            );
        }
    }

    // ---- Phase 21.5: allocation profile (requires --count-alloc) -------
    #[cfg(feature = "ccu-alloc")]
    if args.count_alloc {
        let ticks = args.ticks.max(1) as f64;
        let (allocs, bytes, frees) = (
            alloc_after.0.saturating_sub(alloc_before.0),
            alloc_after.1.saturating_sub(alloc_before.1),
            alloc_after.2.saturating_sub(alloc_before.2),
        );
        println!("\n=== ALLOCATION PROFILE (measured phase, counting allocator) ===");
        println!("  allocs/tick: {:>10.0}", allocs as f64 / ticks);
        println!("  bytes/tick:  {:>10.0}", bytes as f64 / ticks);
        println!("  frees/tick:  {:>10.0}", frees as f64 / ticks);
        println!(
            "  allocs/client/tick: {:>8.2}",
            allocs as f64 / args.clients.max(1) as f64 / ticks
        );
        println!(
            "  bytes/client/tick:  {:>8.2}",
            bytes as f64 / args.clients.max(1) as f64 / ticks
        );
        println!("  total allocs: {allocs}  bytes: {bytes}  frees: {frees}");
    }
    #[cfg(not(feature = "ccu-alloc"))]
    if args.count_alloc {
        println!(
            "\nnote: --count-alloc requires building with `--features ccu-alloc`; skipping allocation profile."
        );
    }

    // ---- Phase 21.5: latency spike investigation ------------------------
    if args.profile_detail && phase_timers.tick_samples.len() >= 3 {
        // Rank ticks by total server-side time (inbound+tick+fanout+pump+
        // flush); identify the worst few and print their phase composition.
        let mut ranked: Vec<(u64, usize)> = phase_timers
            .tick_samples
            .iter()
            .enumerate()
            .map(|(i, s)| (s.0 + s.1 + s.2 + s.3 + s.4, i))
            .collect();
        ranked.sort_unstable_by_key(|(total, _)| std::cmp::Reverse(*total));
        println!("\n=== WORST TICKS (server-side phase composition) ===");
        for (rank, (total, i)) in ranked.iter().take(5).enumerate() {
            let s = &phase_timers.tick_samples[*i];
            println!(
                "  #{rank} tick {i}: total={:.2}ms  inbound={:.2}  tick={:.2}  fanout={:.2}  pump={:.2}  flush={:.2}  clients={:.2}  drain={:.2}",
                *total as f64 / 1e6,
                s.0 as f64 / 1e6,
                s.1 as f64 / 1e6,
                s.2 as f64 / 1e6,
                s.3 as f64 / 1e6,
                s.4 as f64 / 1e6,
                s.5 as f64 / 1e6,
                s.6 as f64 / 1e6,
            );
        }
    }

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
    println!(
        "note: in-process transport; protocol/gateway/runtime/world/subscriptions/SDK are real."
    );
    println!("arena size: {ARENA_WIDTH}x{ARENA_HEIGHT}; movement validation is authoritative.");

    if phase_timers.count > 0 {
        let n = phase_timers.count as f64;
        println!(
            "\n=== PHASE BREAKDOWN (avg over {count} ticks) ===",
            count = phase_timers.count
        );
        println!(
            "  inbound:  {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.inbound_ns as f64 / n / 1e6,
            phase_timers.inbound_ns as f64 / phase_timers.tick_ns.max(1) as f64 * 100.0
        );
        println!(
            "  tick:     {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.tick_ns as f64 / n / 1e6,
            phase_timers.tick_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!(
            "  fanout:   {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.fanout_ns as f64 / n / 1e6,
            phase_timers.fanout_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!(
            "  pump:     {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.pump_ns as f64 / n / 1e6,
            phase_timers.pump_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!(
            "  flush:    {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.flush_ns as f64 / n / 1e6,
            phase_timers.flush_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!(
            "  clients:  {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.client_ns as f64 / n / 1e6,
            phase_timers.client_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!(
            "  drain:    {:>7.1} ms/tick ({:>4.1}%)",
            phase_timers.drain_ns as f64 / n / 1e6,
            phase_timers.drain_ns as f64
                / (phase_timers.inbound_ns
                    + phase_timers.tick_ns
                    + phase_timers.fanout_ns
                    + phase_timers.pump_ns
                    + phase_timers.flush_ns
                    + phase_timers.client_ns
                    + phase_timers.drain_ns)
                    .max(1) as f64
                * 100.0
        );
        println!("  -- inside tick --");
        let tick_total =
            (phase_timers.world_tick_ns + phase_timers.wal_ns + phase_timers.sub_apply_ns).max(1);
        println!(
            "  world_tick: {:>7.1} ms/tick ({:>4.1}% of tick)",
            phase_timers.world_tick_ns as f64 / n / 1e6,
            phase_timers.world_tick_ns as f64 / tick_total as f64 * 100.0
        );
        println!(
            "  wal:        {:>7.1} ms/tick ({:>4.1}% of tick)",
            phase_timers.wal_ns as f64 / n / 1e6,
            phase_timers.wal_ns as f64 / tick_total as f64 * 100.0
        );
        println!(
            "  sub_apply:  {:>7.1} ms/tick ({:>4.1}% of tick)",
            phase_timers.sub_apply_ns as f64 / n / 1e6,
            phase_timers.sub_apply_ns as f64 / tick_total as f64 * 100.0
        );
    }
}
