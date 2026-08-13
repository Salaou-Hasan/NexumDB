//! The arena game: authoritative gameplay reducers, the cooldown system,
//! and the world factory (ADR-009/014 discipline).
//!
//! Everything here is **game code** built on the Nexum stack: mutations
//! happen only through reducers/systems that run inside `World::tick` →
//! Transaction/OCC → one atomic commit → `Vec<Change>` → WAL +
//! SubscriptionRegistry. There is no second state store and no second
//! transaction engine. The client never supplies an identity: client-callable
//! reducers read the caller from the gateway-stamped `__caller` argument
//! (ADR-014 D8), so identity cannot be forged.

use nexum_core::{row, Error, ReducerId, Result, Row, RowId, SystemId, TableSchema, Value, WorldId};
use nexum_network::CALLER_SOURCE_ARG;
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::WorldFactory;
use nexum_simulation::{
    InputFrame, SimulationConfig, SimulationContext, SystemDefinition, World,
};
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use crate::wasm::fire_weapon_module;

// ------------------------------------------------------------ constants

/// The arena dimensions (authoritative world bounds).
pub const ARENA_WIDTH: i64 = 48;
/// The arena height (authoritative world bounds).
pub const ARENA_HEIGHT: i64 = 24;

/// Starting / maximum health.
pub const START_HP: i64 = 100;
/// Starting ammunition per clip.
pub const START_AMMO: i64 = 10;
/// The cooldown (in ticks) a shot sets before the next shot is allowed.
pub const FIRE_COOLDOWN: i64 = 5;
/// Damage dealt by one hit.
pub const FIRE_DAMAGE: i64 = 25;

/// The `players` table (authoritative gameplay state).
pub const TABLE: &str = "players";

/// Column indices of the `players` table (all numeric; every value is a
/// 1-byte tag + fixed payload, which the WASM reducer relies on).
/// The `id` column (U64, primary key).
pub const COL_ID: usize = 0;
/// The `x` column (I64).
pub const COL_X: usize = 1;
/// The `y` column (I64).
pub const COL_Y: usize = 2;
/// The `hp` column (I64).
pub const COL_HP: usize = 3;
/// The `max_hp` column (I64).
pub const COL_MAX_HP: usize = 4;
/// The `alive` column (I64, 1 or 0).
pub const COL_ALIVE: usize = 5;
/// The `score` column (I64).
pub const COL_SCORE: usize = 6;
/// The `cooldown` column (I64, ticks until the next shot).
pub const COL_COOLDOWN: usize = 7;
/// The `facing` column (I64, 0=N, 1=E, 2=S, 3=W).
pub const COL_FACING: usize = 8;
/// The `ammo` column (I64).
pub const COL_AMMO: usize = 9;
/// The `connected` column (I64, 1 or 0).
pub const COL_CONNECTED: usize = 10;

/// Facing directions: 0=N, 1=E, 2=S, 3=W (the direction of the last move,
/// used as the aim direction when firing).
/// Facing north.
pub const FACING_N: i64 = 0;
/// Facing east.
pub const FACING_E: i64 = 1;
/// Facing south.
pub const FACING_S: i64 = 2;
/// Facing west.
pub const FACING_W: i64 = 3;

/// The client-callable reducers (exposed by the demo server).
pub const CLIENT_REDUCERS: &[&str] = &[
    "move_player",
    "fire_weapon",
    "reload_weapon",
    "respawn_player",
];

// ------------------------------------------------------------- helpers

fn as_i64(value: Option<&Value>) -> i64 {
    value.and_then(Value::as_i64).unwrap_or(0)
}

fn get(row: &Row, column: usize) -> i64 {
    as_i64(row.get(column))
}

/// Finds the row whose id column equals `player_id`.
fn find_player(rows: &[(RowId, Row)], player_id: u64) -> Option<&(RowId, Row)> {
    rows.iter()
        .find(|(_, row)| row.get(COL_ID) == Some(&Value::U64(player_id)))
}

/// Returns `row` with `column` replaced (consumes the row for chaining).
fn with(row: Row, column: usize, value: Value) -> Row {
    let mut values = row.into_values();
    values[column] = value;
    Row::new(values)
}

/// The deterministic spawn point for `player_id` (inside the arena).
pub fn spawn(player_id: u64) -> (i64, i64) {
    let x = 4 + ((player_id as i64 * 7) % (ARENA_WIDTH - 10));
    let y = 3 + ((player_id as i64 * 5) % (ARENA_HEIGHT - 6));
    (x, y)
}

/// The facing index for a movement delta (0=N, 1=E, 2=S, 3=W).
fn facing_of(dx: i64, dy: i64) -> i64 {
    if dx > 0 {
        FACING_E
    } else if dx < 0 {
        FACING_W
    } else if dy > 0 {
        FACING_S
    } else {
        FACING_N
    }
}

/// Builds a `move_player` argument set for a one-cell step.
pub fn move_args(dx: i64, dy: i64) -> ReducerArgs {
    ReducerArgs::new().insert("dx", dx).insert("dy", dy)
}

fn alive(player: &Row) -> bool {
    get(player, COL_ALIVE) != 0
}

// ------------------------------------------------------------- reducers

/// `player_join` — authoritative join/reconnect initialization (server-only;
/// invoked by the game server's `on_player_join` hook and by the demo server
/// on reconnect). Idempotent: a reconnect keeps position/hp/score and just
/// marks the player connected; a first join inserts a fresh row at the
/// deterministic spawn point.
pub fn player_join(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    let rows = ctx.scan(TABLE)?;
    if let Some((row_id, row)) = find_player(&rows, player_id) {
        let row = with(row.clone(), COL_CONNECTED, Value::I64(1));
        ctx.update(TABLE, *row_id, row)?;
        ctx.emit("rejoin", Value::U64(player_id))?;
        return Ok(Value::U64(player_id));
    }
    let (x, y) = spawn(player_id);
    ctx.insert(
        TABLE,
        row![
            player_id,
            x,
            y,
            START_HP,
            START_HP,
            1i64,
            0i64,
            0i64,
            FACING_E,
            START_AMMO,
            1i64
        ],
    )?;
    ctx.emit("join", Value::U64(player_id))?;
    Ok(Value::U64(player_id))
}

/// `player_leave` — authoritative disconnect marking (server-only). The row
/// persists so a reconnect reconstructs the current state.
pub fn player_leave(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    let rows = ctx.scan(TABLE)?;
    if let Some((row_id, row)) = find_player(&rows, player_id) {
        let row = with(row.clone(), COL_CONNECTED, Value::I64(0));
        ctx.update(TABLE, *row_id, row)?;
        ctx.emit("leave", Value::U64(player_id))?;
    }
    Ok(Value::U64(player_id))
}

/// `move_player` — client-callable. The caller is the gateway-stamped
/// `__caller` (never a client-supplied id). The server validates the step,
/// enforces arena bounds and cell occupancy, and derives the authoritative
/// new position. `dx`/`dy` must each be -1, 0, or 1 (a one-cell step).
pub fn move_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let dx = args
        .get("dx")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::invalid_argument("dx required"))?;
    let dy = args
        .get("dy")
        .and_then(Value::as_i64)
        .ok_or_else(|| Error::invalid_argument("dy required"))?;
    if !(-1..=1).contains(&dx) || !(-1..=1).contains(&dy) || (dx == 0 && dy == 0) {
        return Err(Error::invalid_argument(
            "dx and dy must each be -1, 0, or 1, and not both 0",
        ));
    }
    let rows = ctx.scan(TABLE)?;
    let (row_id, player) = find_player(&rows, caller)
        .ok_or_else(|| Error::not_found("player"))?;
    if !alive(player) {
        return Err(Error::invalid_argument("player is dead — respawn first"));
    }
    if get(player, COL_CONNECTED) != 1 {
        return Err(Error::invalid_argument("player is disconnected"));
    }
    let x = get(player, COL_X);
    let y = get(player, COL_Y);
    let nx = (x + dx).clamp(0, ARENA_WIDTH - 1);
    let ny = (y + dy).clamp(0, ARENA_HEIGHT - 1);
    // No stacking: a cell occupied by another alive player is impassable
    // (deterministic scan order).
    let occupied = rows.iter().any(|(other_id, other)| {
        *other_id != *row_id
            && alive(other)
            && get(other, COL_X) == nx
            && get(other, COL_Y) == ny
    });
    if occupied {
        return Err(Error::invalid_argument("cell is occupied"));
    }
    let facing = facing_of(dx, dy);
    let row = with(
        with(with(player.clone(), COL_X, Value::I64(nx)), COL_Y, Value::I64(ny)),
        COL_FACING,
        Value::I64(facing),
    );
    ctx.update(TABLE, *row_id, row)?;
    Ok(Value::U64(1))
}

/// `reload_weapon` — client-callable. Refills the caller's ammunition.
pub fn reload_weapon(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let rows = ctx.scan(TABLE)?;
    let (row_id, player) = find_player(&rows, caller)
        .ok_or_else(|| Error::not_found("player"))?;
    if !alive(player) {
        return Err(Error::invalid_argument("player is dead"));
    }
    let row = with(player.clone(), COL_AMMO, Value::I64(START_AMMO));
    ctx.update(TABLE, *row_id, row)?;
    ctx.emit("reload", Value::U64(caller))?;
    Ok(Value::I64(START_AMMO))
}

/// `respawn_player` — client-callable. A dead player may request a respawn;
/// an alive player's request is rejected. Position resets to the spawn
/// point, hp/cooldown/ammo reset, score is kept.
pub fn respawn_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let rows = ctx.scan(TABLE)?;
    let (row_id, player) = find_player(&rows, caller)
        .ok_or_else(|| Error::not_found("player"))?;
    if alive(player) {
        return Err(Error::invalid_argument("player is already alive"));
    }
    let (x, y) = spawn(caller);
    let row = with(
        with(
            with(
                with(player.clone(), COL_X, Value::I64(x)),
                COL_Y,
                Value::I64(y),
            ),
            COL_HP,
            Value::I64(START_HP),
        ),
        COL_ALIVE,
        Value::I64(1),
    );
    let row = with(
        with(row, COL_COOLDOWN, Value::I64(0)),
        COL_AMMO,
        Value::I64(START_AMMO),
    );
    ctx.update(TABLE, *row_id, row)?;
    ctx.emit("respawn", Value::U64(caller))?;
    Ok(Value::U64(1))
}

/// `take_damage` — server-only (never exposed). Applies `amount` damage to
/// `player_id`; a player reaching zero health dies (and emits `kill`).
pub fn take_damage(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    let amount = args
        .get("amount")
        .and_then(Value::as_i64)
        .unwrap_or(0);
    let rows = ctx.scan(TABLE)?;
    let (row_id, player) = find_player(&rows, player_id)
        .ok_or_else(|| Error::not_found("player"))?;
    let hp = (get(player, COL_HP) - amount).max(0);
    let row = with(player.clone(), COL_HP, Value::I64(hp));
    let row = if hp == 0 {
        with(row, COL_ALIVE, Value::I64(0))
    } else {
        row
    };
    ctx.update(TABLE, *row_id, row)?;
    ctx.emit("damage", Value::U64(player_id))?;
    if hp == 0 {
        ctx.emit("kill", Value::U64(player_id))?;
    }
    Ok(Value::I64(hp))
}

/// `set_position` — server-only (never exposed). Arranges a player for
/// tests/demo scenarios: clamps into the arena and sets facing.
pub fn set_position(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    let x = args
        .get("x")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .clamp(0, ARENA_WIDTH - 1);
    let y = args
        .get("y")
        .and_then(Value::as_i64)
        .unwrap_or(0)
        .clamp(0, ARENA_HEIGHT - 1);
    let facing = args
        .get("facing")
        .and_then(Value::as_i64)
        .unwrap_or(FACING_E);
    let rows = ctx.scan(TABLE)?;
    let (row_id, player) = find_player(&rows, player_id)
        .ok_or_else(|| Error::not_found("player"))?;
    let row = with(
        with(
            with(player.clone(), COL_X, Value::I64(x)),
            COL_Y,
            Value::I64(y),
        ),
        COL_FACING,
        Value::I64(facing),
    );
    ctx.update(TABLE, *row_id, row)?;
    ctx.emit("warp", Value::U64(player_id))?;
    Ok(Value::U64(1))
}

// -------------------------------------------------------------- systems

/// The per-tick cooldown system: every alive player's weapon cooldown ticks
/// down by one per tick. Runs on every world, every tick, through the tick
/// transaction (a cooldown change is a committed `Vec<Change>` the clients
/// observe).
fn cooldown_tick(ctx: &mut SimulationContext, _frame: &InputFrame) -> Result<()> {
    let rows = ctx.scan(TABLE)?;
    for (row_id, row) in rows {
        let cooldown = get(&row, COL_COOLDOWN);
        if cooldown > 0 {
            ctx.update(TABLE, row_id, with(row.clone(), COL_COOLDOWN, Value::I64(cooldown - 1)))?;
        }
    }
    Ok(())
}

// -------------------------------------------------------------- factory

/// Builds the `players` table if missing (idempotent across recovery).
fn ensure_schema(store: &mut TableStore) {
    if store.table(TABLE).is_none() {
        let schema = TableSchema::builder(TABLE)
            .column("id", nexum_core::ColumnType::U64)
            .column("x", nexum_core::ColumnType::I64)
            .column("y", nexum_core::ColumnType::I64)
            .column("hp", nexum_core::ColumnType::I64)
            .column("max_hp", nexum_core::ColumnType::I64)
            .column("alive", nexum_core::ColumnType::I64)
            .column("score", nexum_core::ColumnType::I64)
            .column("cooldown", nexum_core::ColumnType::I64)
            .column("facing", nexum_core::ColumnType::I64)
            .column("ammo", nexum_core::ColumnType::I64)
            .column("connected", nexum_core::ColumnType::I64)
            .primary_key(&["id"])
            .build()
            .expect("valid players schema");
        store.create_table(schema).expect("players table created");
    }
}

/// The game world factory: registers the native reducers, the cooldown
/// system, and the WASM `fire_weapon` module on every world.
pub fn game_factory() -> WorldFactory {
    Box::new(|id: WorldId, mut store: TableStore, sim: SimulationConfig| {
        ensure_schema(&mut store);
        let mut world = World::new(id, store, sim)?;
        world
            .add_system(
                SystemDefinition::new(SystemId::from_u64(0), "cooldown_tick", 0, cooldown_tick)
                    .unwrap(),
            )
            .unwrap();
        type NativeReducer = fn(&mut ReducerContext, &ReducerArgs) -> Result<Value>;
        let reducers: &[(&str, NativeReducer)] = &[
            ("player_join", player_join),
            ("player_leave", player_leave),
            ("move_player", move_player),
            ("reload_weapon", reload_weapon),
            ("respawn_player", respawn_player),
            ("take_damage", take_damage),
            ("set_position", set_position),
        ];
        for (index, (name, function)) in reducers.iter().enumerate() {
            world
                .native_mut()
                .register(
                    ReducerDefinition::new(
                        ReducerId::from_u64((index + 1) as u64),
                        *name,
                        *function,
                    )
                    .unwrap(),
                )
                .unwrap();
        }
        let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
        wasm.register("fire_weapon", 1, fire_weapon_module()).unwrap();
        world.set_wasm(wasm);
        Ok(world)
    })
}
