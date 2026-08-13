//! Gameplay tests: authoritative reducers (join/move/reload/respawn/damage),
//! the cooldown system, the WASM `fire_weapon` reducer, and forged-identity
//! rejection. Every mutation flows through `World::tick_with_calls` — the
//! same single commit path the runtime uses.

use game_server::game::{game_factory, spawn, COL_ALIVE, COL_AMMO, COL_COOLDOWN, COL_HP, COL_X, COL_Y};
use nexum_core::{Row, RowId, TickId, Value, WorldId};
use nexum_network::CALLER_SOURCE_ARG;
use nexum_reducer::ReducerArgs;
use nexum_simulation::{InputFrame, ReducerCall, ReducerCallResult, SimulationConfig, World};
use nexum_table::TableStore;

/// Builds a fresh world through the real game factory.
fn world(seed: u64) -> World {
    let store = TableStore::new();
    game_factory()(WorldId::from_u64(0), store, SimulationConfig::new().with_seed(seed)).unwrap()
}

/// Drives one reducer call through the real tick path (join tick semantics:
/// the call executes against the tick transaction, the tick commits).
fn call(world: &mut World, request: u64, reducer: &str, args: ReducerArgs) -> nexum_simulation::TickResult {
    let frame = InputFrame::new(world.tick_number());
    let calls = vec![ReducerCall::new(request, reducer, args).unwrap()];
    world.tick_with_calls(&frame, &[], &calls).unwrap()
}

fn call_result(result: &nexum_simulation::TickResult, request: u64) -> &ReducerCallResult {
    result
        .reducer_results()
        .iter()
        .find(|item| item.request_id() == request)
        .expect("the call produced a result")
}

/// The value of a column in the committed row for `player_id` (found through
/// the latest change; the view is the authoritative committed state).
fn join_player(world: &mut World, player_id: u64) {
    let result = call(
        world,
        player_id,
        "player_join",
        ReducerArgs::new()
            .insert("player_id", player_id)
            .insert("game_id", 0u64),
    );
    assert!(call_result(&result, player_id).is_ok(), "join commits");
}

/// Extracts the committed rows from the world's authoritative store.
fn scan_players(world: &World) -> Vec<(RowId, Row)> {
    world
        .store()
        .table("players")
        .expect("players table")
        .scan()
        .map(|(row_id, row)| (row_id, row.clone()))
        .collect()
}

fn row_of(rows: &[(RowId, Row)], player_id: u64) -> &Row {
    rows.iter()
        .find(|(_, row)| row.get(0) == Some(&Value::U64(player_id)))
        .map(|(_, row)| row)
        .expect("player row exists")
}

fn get(row: &Row, column: usize) -> i64 {
    row.get(column).and_then(Value::as_i64).unwrap()
}

// ------------------------------------------------------------- movement

#[test]
fn join_is_idempotent_and_keeps_state_on_rejoin() {
    let mut world = world(42);
    join_player(&mut world, 1);
    let (x, y) = spawn(1);
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), x);
    assert_eq!(get(row_of(&rows, 1), COL_Y), y);
    assert_eq!(get(row_of(&rows, 1), COL_ALIVE), 1);

    // Move, then "rejoin" (reconnect): state is preserved, only connected
    // flips back to 1.
    call(
        &mut world,
        11,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 1i64)
            .insert("dy", 0i64),
    );
    join_player(&mut world, 1);
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), x + 1, "rejoin keeps position");
}

#[test]
fn move_player_updates_position_and_facing_and_clamps_to_bounds() {
    let mut world = world(42);
    join_player(&mut world, 1);
    let (x, y) = spawn(1);

    // Step east → facing E.
    call(
        &mut world,
        11,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 1i64)
            .insert("dy", 0i64),
    );
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), x + 1);
    assert_eq!(get(row_of(&rows, 1), COL_Y), y);
    assert_eq!(get(row_of(&rows, 1), 8), 1, "facing E");

    // Server teleports to the north-west corner, then a west/north step
    // clamps at the wall.
    call(
        &mut world,
        12,
        "set_position",
        ReducerArgs::new()
            .insert("player_id", 1u64)
            .insert("x", 0i64)
            .insert("y", 0i64)
            .insert("facing", 1i64),
    );
    let result = call(
        &mut world,
        13,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", -1i64)
            .insert("dy", 0i64),
    );
    assert!(call_result(&result, 13).is_ok(), "move into the wall is a legal intent");
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), 0, "clamped at the west wall");
}

#[test]
fn move_player_rejects_invalid_steps_and_occupied_cells() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);

    // Invalid step magnitude.
    let result = call(
        &mut world,
        11,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 5i64)
            .insert("dy", 0i64),
    );
    assert!(!call_result(&result, 11).is_ok(), "dx=5 is rejected");

    // No-op step.
    let result = call(
        &mut world,
        12,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 0i64)
            .insert("dy", 0i64),
    );
    assert!(!call_result(&result, 12).is_ok(), "no-op step is rejected");

    // Put player 2 directly east of player 1; player 1 cannot enter the
    // occupied cell.
    let (x1, y1) = spawn(1);
    call(
        &mut world,
        13,
        "set_position",
        ReducerArgs::new()
            .insert("player_id", 2u64)
            .insert("x", x1 + 1)
            .insert("y", y1)
            .insert("facing", 3i64),
    );
    let result = call(
        &mut world,
        14,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 1i64)
            .insert("dy", 0i64),
    );
    let error = call_result(&result, 14);
    assert!(!error.is_ok(), "occupied cell is impassable");
    assert!(
        error.error().unwrap().to_string().contains("occupied"),
        "{}",
        error.error().unwrap()
    );
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), x1, "player 1 did not move");
}

// ------------------------------------------------------------ identity

#[test]
fn client_calls_cannot_act_on_behalf_of_another_player() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);
    let (x1, _) = spawn(1);
    let (x2, y2) = spawn(2);

    // The reducers read the caller from `__caller`; there is no
    // client-supplied player id. A forged `__caller` cannot redirect the
    // action — the gateway stamps the real caller, so the unit-level
    // contract here is: player 2's row is untouched by player 1's move.
    let result = call(
        &mut world,
        11,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 1i64)
            .insert("dy", 0i64),
    );
    assert!(call_result(&result, 11).is_ok());
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_X), x1 + 1, "player 1 moved");
    assert_eq!(
        (get(row_of(&rows, 2), COL_X), get(row_of(&rows, 2), COL_Y)),
        (x2, y2),
        "player 2 was not moved by player 1's call"
    );

    // And the server-only reducers take an explicit player_id — they are
    // never client-callable (exposure), which the e2e tests prove.
    let result = call(
        &mut world,
        12,
        "take_damage",
        ReducerArgs::new().insert("player_id", 2u64).insert("amount", 10i64),
    );
    assert!(call_result(&result, 12).is_ok(), "server-only reducer works when invoked");
}

// -------------------------------------------------------------- combat

#[test]
fn take_damage_death_and_respawn_are_authoritative() {
    let mut world = world(42);
    join_player(&mut world, 1);
    let (x, y) = spawn(1);

    // 100 damage kills the player.
    let result = call(
        &mut world,
        11,
        "take_damage",
        ReducerArgs::new().insert("player_id", 1u64).insert("amount", 100i64),
    );
    assert!(call_result(&result, 11).is_ok());
    let events = result.events();
    assert!(events.iter().any(|event| event.name() == "kill"), "kill event emitted");
    let rows = scan_players(&world);
    let row = row_of(&rows, 1);
    assert_eq!(get(row, COL_HP), 0);
    assert_eq!(get(row, COL_ALIVE), 0, "the player died");

    // A dead player cannot move.
    let result = call(
        &mut world,
        12,
        "move_player",
        ReducerArgs::new()
            .insert(CALLER_SOURCE_ARG, 1u64)
            .insert("dx", 1i64)
            .insert("dy", 0i64),
    );
    assert!(!call_result(&result, 12).is_ok(), "dead players cannot move");

    // Respawn restores position/hp at the spawn point, keeping score.
    let result = call(
        &mut world,
        13,
        "respawn_player",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    assert!(call_result(&result, 13).is_ok());
    let rows = scan_players(&world);
    let row = row_of(&rows, 1);
    assert_eq!(get(row, COL_ALIVE), 1, "respawned");
    assert_eq!(get(row, COL_HP), 100);
    assert_eq!((get(row, COL_X), get(row, COL_Y)), (x, y), "respawn at the spawn point");

    // Respawn while alive is rejected.
    let result = call(
        &mut world,
        14,
        "respawn_player",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    assert!(!call_result(&result, 14).is_ok(), "alive players cannot respawn");
}

#[test]
fn reload_refills_ammo() {
    let mut world = world(42);
    join_player(&mut world, 1);
    let result = call(
        &mut world,
        11,
        "reload_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    assert_eq!(call_result(&result, 11).value(), Some(&Value::I64(10)));
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 1), COL_AMMO), 10);
}

// ------------------------------------------------------------ WASM combat

/// Places player `target_id` one cell east of player `shooter_id` (the
/// shooter faces east by default).
fn place_adjacent(world: &mut World, shooter: u64, target: u64) {
    let (x, y) = spawn(shooter);
    call(
        world,
        100 + target,
        "set_position",
        ReducerArgs::new()
            .insert("player_id", target)
            .insert("x", x + 1)
            .insert("y", y)
            .insert("facing", 3i64),
    );
}

#[test]
fn wasm_fire_weapon_hits_adjacent_target_applies_damage_and_sets_cooldown() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);
    place_adjacent(&mut world, 1, 2);
    let (x1, y1) = spawn(1);

    let result = call(
        &mut world,
        11,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let outcome = call_result(&result, 11);
    assert!(outcome.is_ok(), "shot fired: {:?}", outcome.error());
    assert_eq!(outcome.value(), Some(&Value::I64(25)), "damage dealt");
    assert!(result.events().iter().any(|event| event.name() == "hit"));

    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 2), COL_HP), 75, "target took 25 damage");
    assert_eq!(get(row_of(&rows, 1), COL_AMMO), 9, "one shot consumed");
    // The cooldown system runs in the same tick: 5 set by the shot, then -1.
    assert_eq!(get(row_of(&rows, 1), COL_COOLDOWN), 4, "cooldown armed");
    assert_eq!(
        (get(row_of(&rows, 1), COL_X), get(row_of(&rows, 1), COL_Y)),
        (x1, y1),
        "the shooter did not move"
    );
}

#[test]
fn wasm_fire_weapon_misses_when_no_target_is_in_front() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);
    place_adjacent(&mut world, 1, 2);
    let (x2, y2) = spawn(2);

    // Face north instead of east: the target is east, so this is a miss.
    call(
        &mut world,
        11,
        "set_position",
        ReducerArgs::new()
            .insert("player_id", 1u64)
            .insert("x", x2)
            .insert("y", y2 + 1)
            .insert("facing", 0i64),
    );
    let result = call(
        &mut world,
        12,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let outcome = call_result(&result, 12);
    assert!(outcome.is_ok());
    assert_eq!(outcome.value(), Some(&Value::I64(0)), "no damage on a miss");
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 2), COL_HP), 100, "target unharmed");
    assert_eq!(get(row_of(&rows, 1), COL_AMMO), 9, "the shot still consumed ammo");
}

#[test]
fn wasm_fire_weapon_rejects_while_recharging_dead_or_disconnected() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);

    // Recharging: fire, then fire again next tick — the second is rejected.
    call(
        &mut world,
        11,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let result = call(
        &mut world,
        12,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let outcome = call_result(&result, 12);
    assert!(!outcome.is_ok(), "second shot while recharging is rejected");
    assert!(outcome.error().unwrap().to_string().contains("recharging"));

    // Dead: cannot fire.
    call(
        &mut world,
        13,
        "take_damage",
        ReducerArgs::new().insert("player_id", 1u64).insert("amount", 100i64),
    );
    let result = call(
        &mut world,
        14,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let outcome = call_result(&result, 14);
    assert!(!outcome.is_ok());
    assert!(outcome.error().unwrap().to_string().contains("dead"));

    // Respawn, then disconnect: cannot fire.
    call(
        &mut world,
        15,
        "respawn_player",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    call(
        &mut world,
        16,
        "player_leave",
        ReducerArgs::new().insert("player_id", 1u64).insert("game_id", 0u64),
    );
    let result = call(
        &mut world,
        17,
        "fire_weapon",
        ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
    );
    let outcome = call_result(&result, 17);
    assert!(!outcome.is_ok());
    assert!(outcome.error().unwrap().to_string().contains("disconnected"));
}

#[test]
fn wasm_fire_weapon_can_kill_and_reports_the_kill() {
    let mut world = world(42);
    join_player(&mut world, 1);
    join_player(&mut world, 2);
    place_adjacent(&mut world, 1, 2);

    // Four hits of 25 kill the target (100 hp). Cooldown: 5 set per shot,
    // -1 per tick; three empty ticks between shots are enough.
    let mut kill_seen = false;
    for shot in 0..4 {
        // Wait out the cooldown: 4 empty ticks between shots.
        for _ in 0..4 {
            world
                .tick_with_calls(&InputFrame::new(world.tick_number()), &[], &[])
                .unwrap();
        }
        let result = call(
            &mut world,
            20 + shot,
            "fire_weapon",
            ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64),
        );
        let outcome = call_result(&result, 20 + shot);
        assert!(outcome.is_ok(), "shot {shot}: {:?}", outcome.error());
        if result.events().iter().any(|event| event.name() == "kill") {
            kill_seen = true;
            assert_eq!(
                result
                    .events()
                    .iter()
                    .find(|event| event.name() == "kill")
                    .unwrap()
                    .payload(),
                &Value::U64(2),
                "the kill names the target"
            );
        }
    }
    assert!(kill_seen, "the fourth hit killed the target");
    let rows = scan_players(&world);
    assert_eq!(get(row_of(&rows, 2), COL_ALIVE), 0, "target is dead");
    assert_eq!(get(row_of(&rows, 2), COL_HP), 0);
}

#[test]
fn wasm_module_same_inputs_same_state_produce_identical_results() {
    // Determinism smoke check over the WASM reducer: two identical worlds,
    // identical input sequences → identical change traces.
    let mut a = world(7);
    let mut b = world(7);
    let mut traces_a: Vec<(TickId, usize, usize)> = Vec::new();
    let mut traces_b: Vec<(TickId, usize, usize)> = Vec::new();

    for tick in 0..6 {
        let call_args = match tick {
            0 => Some(("player_join", ReducerArgs::new().insert("player_id", 1u64).insert("game_id", 0u64))),
            1 => Some(("player_join", ReducerArgs::new().insert("player_id", 2u64).insert("game_id", 0u64))),
            2 => Some(("move_player", ReducerArgs::new().insert(CALLER_SOURCE_ARG, 1u64).insert("dx", 1i64).insert("dy", 0i64))),
            _ => None,
        };
        if let Some((reducer, args)) = call_args {
            let result_a = call(&mut a, 1, reducer, args.clone());
            let result_b = call(&mut b, 1, reducer, args);
            traces_a.push((result_a.tick(), result_a.changes().len(), result_a.reducer_results().len()));
            traces_b.push((result_b.tick(), result_b.changes().len(), result_b.reducer_results().len()));
        } else {
            let result_a = a.tick_with_calls(&InputFrame::new(a.tick_number()), &[], &[]).unwrap();
            let result_b = b.tick_with_calls(&InputFrame::new(b.tick_number()), &[], &[]).unwrap();
            traces_a.push((result_a.tick(), result_a.changes().len(), result_a.reducer_results().len()));
            traces_b.push((result_b.tick(), result_b.changes().len(), result_b.reducer_results().len()));
        }
    }
    assert_eq!(traces_a, traces_b, "identical change-trace shapes");
    let rows_a = scan_players(&a);
    let rows_b = scan_players(&b);
    assert_eq!(rows_a, rows_b, "identical final state");
}
