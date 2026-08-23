//! Phase 14 game-server benchmarks — honest baselines, not claims (ADR-014).
//!
//! Run with: `cargo run --release -p nexum-game-server --example game_bench
//! [iterations]`. The game server is an orchestration layer around the
//! authoritative simulation; these measure its orchestration overhead
//! (lifecycle, player routing, command routing, exposure checks, reducer
//! dispatch) on top of the Phase 9–13 costs.

use std::sync::Arc;
use std::time::Instant;

use nexum_core::row;
use nexum_core::{ColumnType, Error, PlayerId, ReducerId, Result, Row, SystemId, Value, WorldId};
use nexum_game_server::{GameInstanceConfig, GameServer, GameServerConfig};
use nexum_network::{NetworkConfig, Principal, TokenAuthenticator};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::{Runtime, RuntimeConfig, WorldFactory};
use nexum_simulation::{SimulationConfig, SystemDefinition, World};
use nexum_table::TableStore;

fn bench(name: &str, iterations: usize, mut f: impl FnMut()) {
    for _ in 0..100 {
        f(); // warmup
    }
    let start = Instant::now();
    for _ in 0..iterations {
        f();
    }
    let ns = start.elapsed().as_secs_f64() * 1e9 / iterations as f64;
    println!("{name:<44} {ns:>12.1} ns/op");
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

/// `bump`: +10 health for the named player.
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
    Ok(Value::I32(health + 10))
}

fn game_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
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
            Ok(world)
        },
    )
}

fn auth() -> Arc<TokenAuthenticator> {
    Arc::new(TokenAuthenticator::new())
}

fn server() -> GameServer {
    // Large queues so routing benchmarks measure routing, not backpressure.
    let runtime = Runtime::new(
        RuntimeConfig::new(game_factory())
            .with_max_queued_inputs(1 << 20)
            .with_max_queued_reducer_calls(1 << 20),
    )
    .unwrap();
    GameServer::new(
        runtime,
        NetworkConfig::new(),
        auth(),
        GameServerConfig::new(),
    )
    .unwrap()
}

fn main() {
    let iterations: usize = std::env::args()
        .nth(1)
        .and_then(|arg| arg.parse().ok())
        .unwrap_or(1_000);

    // ------------------------------------------------------- game lifecycle

    bench("game create", iterations, || {
        let mut srv = server();
        let _ = srv.create_game(GameInstanceConfig::new("arena")).unwrap();
    });
    bench("game create + destroy", iterations, || {
        let mut srv = server();
        let game = srv.create_game(GameInstanceConfig::new("arena")).unwrap();
        srv.destroy_game(game).unwrap();
    });

    // ------------------------------------------- join/leave (player routing)

    // Routing benchmarks run against a server that is never ticked (each
    // `submit_command` stamps the world's current `next_tick`, so all routed
    // frames share tick 0 — valid to measure routing, but the world must
    // never tick afterwards). Tick benchmarks use a fresh, seeded server.
    let mut route = server();
    let game = route.create_game(GameInstanceConfig::new("arena")).unwrap();
    route.start_game(game).unwrap();
    route.expose_reducer("bump").unwrap();
    let alice = Principal::new(1, "alice");
    let _ = route.join_game(&alice, game).unwrap();
    let player = PlayerId::from_u64(1);
    let args = ReducerArgs::new().insert("player", 1u64);

    bench("player join", iterations, || {
        let mut srv = server();
        let game = srv.create_game(GameInstanceConfig::new("arena")).unwrap();
        srv.start_game(game).unwrap();
        let principal = Principal::new(1, "alice");
        let _ = srv.join_game(&principal, game).unwrap();
    });
    bench("player join + leave", iterations, || {
        let mut srv = server();
        let game = srv.create_game(GameInstanceConfig::new("arena")).unwrap();
        srv.start_game(game).unwrap();
        let principal = Principal::new(1, "alice");
        let _ = srv.join_game(&principal, game).unwrap();
        srv.leave_game(PlayerId::from_u64(1)).unwrap();
    });

    // ------------------------------------------- command / reducer routing

    bench("exposure check (is_client_callable)", iterations, || {
        let _ = route.is_client_callable("bump");
    });
    let mut next_id = 0u64;
    bench("command routing (submit_command)", iterations, || {
        next_id += 1;
        route
            .submit_command(player, "spawn", Some(Value::U64(next_id)))
            .unwrap();
    });
    bench("reducer routing (invoke_reducer)", iterations, || {
        route.invoke_reducer(player, "bump", args.clone()).unwrap();
    });

    // ----------------------------------------------- full-tick dispatch

    // A fresh server whose world is seeded with player 1's row (one spawn
    // consumed by tick 0) so every subsequent tick is a clean, successful
    // tick.
    let mut srv = server();
    let game = srv.create_game(GameInstanceConfig::new("arena")).unwrap();
    srv.start_game(game).unwrap();
    srv.expose_reducer("bump").unwrap();
    let alice = Principal::new(1, "alice");
    let _ = srv.join_game(&alice, game).unwrap();
    srv.submit_command(player, "spawn", Some(Value::U64(1)))
        .unwrap();
    srv.step().unwrap();

    bench("empty tick (step)", iterations, || {
        srv.step().unwrap();
    });
    bench("tick with one reducer call", iterations, || {
        srv.invoke_reducer(player, "bump", args.clone()).unwrap();
        srv.step().unwrap();
    });

    println!(
        "\nnote: game create + join include world creation; per-tick costs include the\n\
         Phase 9 tick and Phase 10 runtime coordination. Phase 15 optimizes."
    );
}
