//! Arena game module: declarative tables, reducers, and module registration.

use nexum_core::{IndexDef, ReducerId, Result, Row, RowId, Value, row};
use nexum_execution::{ExecutionContext, InputFrame, Partition, PartitionConfig, SystemDefinition};
use nexum_macros::NexumTable;
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::PartitionFactory;
use nexum_table::TableStore;
use nexum_wasm::{WasmLimits, WasmModuleRegistry};

use nexum_network::CALLER_SOURCE_ARG;

// ═══════════════════════════════════════════════════════════════════════
//  Tables
// ═══════════════════════════════════════════════════════════════════════

/// The `players` table (authoritative gameplay state).
#[derive(Debug, Clone, NexumTable)]
#[table_name = "players"]
pub struct Player {
    /// Unique player id.
    #[primary_key]
    pub id: u64,
    /// X position.
    pub x: i64,
    /// Y position.
    pub y: i64,
    /// Current hit points.
    pub hp: i64,
    /// Maximum hit points.
    pub max_hp: i64,
    /// Alive flag (0 = dead, 1 = alive).
    pub alive: i64,
    /// Score.
    pub score: i64,
    /// Weapon cooldown ticks remaining.
    pub cooldown: i64,
    /// Facing direction (0=N, 1=E, 2=S, 3=W).
    pub facing: i64,
    /// Ammunition.
    pub ammo: i64,
    /// Connected flag (0 = disconnected, 1 = connected).
    pub connected: i64,
}

/// The `units` table: RTS-density entities owned by players.
#[derive(Debug, Clone, NexumTable)]
#[table_name = "units"]
pub struct Unit {
    /// Unique unit id.
    #[primary_key]
    pub id: u64,
    /// Owning player id.
    pub owner: u64,
    /// X position.
    pub x: i64,
    /// Y position.
    pub y: i64,
}

/// The `inventory` table: gathered resources.
#[derive(Debug, Clone, NexumTable)]
#[table_name = "inventory"]
pub struct InventoryItem {
    /// Unique item id.
    #[primary_key]
    pub id: u64,
    /// Owning player id.
    pub owner: u64,
    /// Item kind.
    pub kind: i64,
}

// ═══════════════════════════════════════════════════════════════════════
//  Arena constants
// ═══════════════════════════════════════════════════════════════════════

/// Arena width in cells.
pub const ARENA_WIDTH: i64 = 48;
/// Arena height in cells.
pub const ARENA_HEIGHT: i64 = 24;
/// Starting hit points.
pub const START_HP: i64 = 100;
/// Starting ammo.
pub const START_AMMO: i64 = 10;
/// Fire cooldown in ticks.
pub const FIRE_COOLDOWN: i64 = 5;
/// Damage per shot.
pub const FIRE_DAMAGE: i64 = 25;
/// Area-of-interest radius for batched movement.
pub const AOI_RADIUS: i64 = 12;

/// Table name for the players table.
pub const TABLE: &str = "players";
#[allow(dead_code)]
const UNITS_TABLE: &str = "units";
#[allow(dead_code)]
const INVENTORY_TABLE: &str = "inventory";
#[allow(dead_code)]
const PK: &str = "primary";
/// Position index name (x, y).
pub const POS_INDEX: &str = "pos";

// Column indices (players)
/// Column index for player id.
pub const COL_ID: usize = 0;
/// Column index for x position.
pub const COL_X: usize = 1;
/// Column index for y position.
pub const COL_Y: usize = 2;
/// Column index for hit points.
pub const COL_HP: usize = 3;
/// Column index for max hit points.
pub const COL_MAX_HP: usize = 4;
/// Column index for alive flag.
pub const COL_ALIVE: usize = 5;
/// Column index for score.
pub const COL_SCORE: usize = 6;
/// Column index for cooldown.
pub const COL_COOLDOWN: usize = 7;
/// Column index for facing direction.
pub const COL_FACING: usize = 8;
/// Column index for ammo.
pub const COL_AMMO: usize = 9;
/// Column index for connected flag.
pub const COL_CONNECTED: usize = 10;

// Facing constants
const FACING_N: i64 = 0;
const FACING_E: i64 = 1;
const FACING_S: i64 = 2;
const FACING_W: i64 = 3;

// ═══════════════════════════════════════════════════════════════════════
//  Helpers
// ═══════════════════════════════════════════════════════════════════════

/// Reads an `I64` column from a row.
pub fn get(row: &Row, column: usize) -> i64 {
    row.values()
        .get(column)
        .and_then(Value::as_i64)
        .unwrap_or(0)
}

/// Returns a new row with `column` replaced by `value`.
pub fn with(row: &Row, column: usize, value: Value) -> Row {
    let mut values: Vec<Value> = row.values().to_vec();
    if column < values.len() {
        values[column] = value;
    }
    Row::new(values)
}

/// Interprets a `Value` as `i64`.
pub fn as_i64(v: &Value) -> i64 {
    v.as_i64().unwrap_or(0)
}

/// Maps `(dx, dy)` to a facing constant (0=N, 1=E, 2=S, 3=W).
pub fn facing_of(dx: i64, dy: i64) -> i64 {
    if dy < 0 {
        FACING_N
    } else if dx > 0 {
        FACING_E
    } else if dy > 0 {
        FACING_S
    } else {
        FACING_W
    }
}

/// Returns the alive column value (0 or 1).
pub fn alive(row: &Row) -> i64 {
    get(row, COL_ALIVE)
}

/// Looks up a player by id using the primary index.
pub fn player_by_id(ctx: &mut ReducerContext, id: u64) -> Option<(RowId, Row)> {
    let rids = ctx.lookup_unique(TABLE, PK, &[Value::U64(id)]).ok()?;
    let rid = rids.first()?;
    let row = ctx.get(TABLE, *rid).ok()??;
    Some((*rid, row))
}

/// Builds reducer args for `move_player(dx, dy)`.
pub fn move_args(dx: i64, dy: i64) -> ReducerArgs {
    ReducerArgs::new().insert("dx", dx).insert("dy", dy)
}

/// Builds reducer args for `fire_weapon(__caller)`.
pub fn fire_args(caller: u64) -> ReducerArgs {
    ReducerArgs::new().insert(CALLER_SOURCE_ARG, caller)
}
/// Computes the spawn position for a player (pure, deterministic).
pub fn spawn(player_id: u64) -> (i64, i64) {
    let x = (player_id as i64 * 7 + 3) % ARENA_WIDTH;
    let y = (player_id as i64 * 13 + 5) % ARENA_HEIGHT;
    (x, y)
}

/// Inserts a player row at the spawn position with full health.
fn spawn_row(ctx: &mut ReducerContext, player_id: u64) -> Result<()> {
    let (x, y) = spawn(player_id);
    let row = row![
        player_id, x, y, START_HP, START_HP, 1i64, 0i64, 0i64, FACING_E, START_AMMO, 1i64
    ];
    ctx.insert(TABLE, row)?;
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
//  Reducers
// ═══════════════════════════════════════════════════════════════════════

/// Joins a player (idempotent: reconnects if already present).
fn player_join(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.get("player_id").and_then(Value::as_u64).unwrap_or(0);
    if player_id == 0 {
        return Ok(Value::I64(0));
    }
    match player_by_id(ctx, player_id) {
        Some((rid, row)) => {
            let updated = with(&row, COL_CONNECTED, Value::I64(1));
            ctx.update(TABLE, rid, updated)?;
        }
        None => {
            spawn_row(ctx, player_id)?;
        }
    }
    Ok(Value::I64(0))
}

/// Marks a player as disconnected.
fn player_leave(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.get("player_id").and_then(Value::as_u64).unwrap_or(0);
    if let Some((rid, row)) = player_by_id(ctx, player_id) {
        ctx.update(TABLE, rid, with(&row, COL_CONNECTED, Value::I64(0)))?;
    }
    Ok(Value::I64(0))
}

/// Moves a player by relative delta.
fn move_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args
        .get(CALLER_SOURCE_ARG)
        .and_then(Value::as_u64)
        .or_else(|| args.get("player_id").and_then(Value::as_u64))
        .unwrap_or(0);
    let dx = args.get("dx").and_then(Value::as_i64).unwrap_or(0);
    let dy = args.get("dy").and_then(Value::as_i64).unwrap_or(0);
    if !(-1..=1).contains(&dx) || !(-1..=1).contains(&dy) || (dx == 0 && dy == 0) {
        return Err(nexum_core::Error::invalid_argument("invalid step"));
    }
    let Some((rid, row)) = player_by_id(ctx, player_id) else {
        return Ok(Value::I64(0));
    };
    if alive(&row) == 0 || get(&row, COL_CONNECTED) == 0 {
        return Err(nexum_core::Error::invalid_argument("dead or disconnected"));
    }
    let nx = (get(&row, COL_X) + dx).clamp(0, ARENA_WIDTH - 1);
    let ny = (get(&row, COL_Y) + dy).clamp(0, ARENA_HEIGHT - 1);
    // Reject if the target cell is occupied by another player.
    // Use the position index for O(log N) lookup instead of a full table scan.
    let occupants = ctx.lookup_index(TABLE, POS_INDEX, &[Value::I64(nx), Value::I64(ny)])?;
    for other_rid in occupants {
        if other_rid == rid {
            continue;
        }
        if let Some(other) = ctx.get(TABLE, other_rid)?
            && get(&other, COL_ALIVE) != 0
        {
            return Err(nexum_core::Error::invalid_argument("occupied"));
        }
    }
    let row = with(&row, COL_X, Value::I64(nx));
    let row = with(&row, COL_Y, Value::I64(ny));
    let row = with(&row, COL_FACING, Value::I64(facing_of(dx, dy)));
    ctx.update(TABLE, rid, row)?;
    Ok(Value::I64(0))
}

/// Fires a weapon (WASM version is the authoritative one; native is a fallback).
fn fire_weapon_native(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args
        .get(CALLER_SOURCE_ARG)
        .and_then(Value::as_u64)
        .unwrap_or(0);
    let Some((rid, row)) = player_by_id(ctx, player_id) else {
        return Ok(Value::I64(0));
    };
    if alive(&row) == 0 || get(&row, COL_CONNECTED) == 0 {
        return Ok(Value::I64(0));
    }
    if get(&row, COL_COOLDOWN) > 0 {
        return Ok(Value::I64(0));
    }
    if get(&row, COL_AMMO) <= 0 {
        return Ok(Value::I64(0));
    }
    let row = with(&row, COL_COOLDOWN, Value::I64(FIRE_COOLDOWN));
    let row = with(&row, COL_AMMO, Value::I64(get(&row, COL_AMMO) - 1));
    ctx.update(TABLE, rid, row)?;
    Ok(Value::I64(FIRE_DAMAGE))
}

/// Reloads ammo to full.
fn reload_weapon(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args
        .get(CALLER_SOURCE_ARG)
        .and_then(Value::as_u64)
        .or_else(|| args.get("player_id").and_then(Value::as_u64))
        .unwrap_or(0);
    if let Some((rid, row)) = player_by_id(ctx, player_id).filter(|(_, row)| alive(row) != 0) {
        ctx.update(TABLE, rid, with(&row, COL_AMMO, Value::I64(START_AMMO)))?;
    }
    Ok(Value::I64(START_AMMO))
}

/// Respawns a dead player at a random position.
fn respawn_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args
        .get(CALLER_SOURCE_ARG)
        .and_then(Value::as_u64)
        .or_else(|| args.get("player_id").and_then(Value::as_u64))
        .unwrap_or(0);
    if player_id == 0 {
        return Ok(Value::I64(0));
    }
    if let Some((rid, row)) = player_by_id(ctx, player_id) {
        // Alive players cannot respawn.
        if alive(&row) != 0 {
            return Err(nexum_core::Error::invalid_argument("alive"));
        }
        let x = (player_id as i64 * 7 + 3) % ARENA_WIDTH;
        let y = (player_id as i64 * 13 + 5) % ARENA_HEIGHT;
        let row = row![
            player_id, x, y, START_HP, START_HP, 1i64, 0i64, 0i64, FACING_E, START_AMMO, 1i64
        ];
        ctx.update(TABLE, rid, row)?;
    } else {
        spawn_row(ctx, player_id)?;
    }
    Ok(Value::I64(0))
}

/// Placeholder for unit movement (no-op).
fn unit_move(_ctx: &mut ReducerContext, _args: &ReducerArgs) -> Result<Value> {
    Ok(Value::I64(0))
}

/// Placeholder for gathering (no-op).
fn gather(_ctx: &mut ReducerContext, _args: &ReducerArgs) -> Result<Value> {
    Ok(Value::I64(0))
}

/// Presence heartbeat (no-op; connection state managed by join/leave).
fn presence(_ctx: &mut ReducerContext, _args: &ReducerArgs) -> Result<Value> {
    Ok(Value::I64(0))
}

/// Server-only: teleports a player to an exact position with a facing.
fn set_position(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.get("player_id").and_then(Value::as_u64).unwrap_or(0);
    let x = args.get("x").and_then(Value::as_i64).unwrap_or(0);
    let y = args.get("y").and_then(Value::as_i64).unwrap_or(0);
    let facing = args
        .get("facing")
        .and_then(Value::as_i64)
        .unwrap_or(FACING_S);
    if let Some((rid, row)) = player_by_id(ctx, player_id) {
        let row = with(&row, COL_X, Value::I64(x));
        let row = with(&row, COL_Y, Value::I64(y));
        let row = with(&row, COL_FACING, Value::I64(facing));
        ctx.update(TABLE, rid, row)?;
    }
    Ok(Value::I64(0))
}

/// Server-only: applies damage to a player and emits kill/death events.
fn take_damage(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.get("player_id").and_then(Value::as_u64).unwrap_or(0);
    let amount = args.get("amount").and_then(Value::as_i64).unwrap_or(0);
    if amount <= 0 {
        return Ok(Value::I64(0));
    }
    let Some((rid, row)) = player_by_id(ctx, player_id) else {
        return Ok(Value::I64(0));
    };
    if alive(&row) == 0 {
        return Ok(Value::I64(0));
    }
    let new_hp = (get(&row, COL_HP) - amount).max(0);
    let row = with(&row, COL_HP, Value::I64(new_hp));
    let row = if new_hp == 0 {
        with(&row, COL_ALIVE, Value::I64(0))
    } else {
        row
    };
    ctx.update(TABLE, rid, row)?;
    if new_hp == 0 {
        ctx.emit("kill", Value::U64(player_id))?;
    } else {
        ctx.emit("hit", Value::U64(player_id))?;
    }
    Ok(Value::I64(amount))
}

// ═══════════════════════════════════════════════════════════════════════
//  Systems (run every tick via ExecutionContext)
// ═══════════════════════════════════════════════════════════════════════

/// Weapon cooldown decrement per tick.
pub fn cooldown_tick(ctx: &mut ExecutionContext<'_>, _frame: &InputFrame) -> Result<()> {
    for (rid, row) in ctx.scan(TABLE)? {
        let cd = get(&row, COL_COOLDOWN);
        if cd > 0 {
            ctx.update(TABLE, rid, with(&row, COL_COOLDOWN, Value::I64(cd - 1)))?;
        }
    }
    Ok(())
}

/// Relay cross-partition messages.
pub fn relay_station(ctx: &mut ExecutionContext<'_>, frame: &InputFrame) -> Result<()> {
    let known = ctx.known_partitions();
    if known.len() < 2 {
        return Ok(());
    }
    let from = ctx.partition();
    let idx = known.iter().position(|p| *p == from).unwrap_or(0);
    let to = known[(idx + 1) % known.len()];
    for cmd in frame.commands() {
        if cmd.kind() == "relay" {
            ctx.send_to(to, "relay_recv", ReducerArgs::new())?;
        }
    }
    Ok(())
}

/// Batched movement processing with AOI visibility.
pub fn movement_stream(ctx: &mut ExecutionContext<'_>, frame: &InputFrame) -> Result<()> {
    #[derive(Clone)]
    struct P {
        rid: RowId,
        id: u64,
        x: i64,
        y: i64,
        hp: i64,
        max_hp: i64,
        alive: i64,
        score: i64,
        cd: i64,
        facing: i64,
        ammo: i64,
        conn: i64,
    }

    let mut players: Vec<P> = Vec::new();
    let mut id_to_idx = std::collections::HashMap::new();
    let mut occupied = std::collections::HashSet::new();

    for (rid, row) in ctx.scan(TABLE)? {
        if get(&row, COL_ALIVE) == 0 || get(&row, COL_CONNECTED) == 0 {
            continue;
        }
        let pid = get(&row, COL_ID) as u64;
        let x = get(&row, COL_X);
        let y = get(&row, COL_Y);
        occupied.insert((x, y));
        id_to_idx.insert(pid, players.len());
        players.push(P {
            rid,
            id: pid,
            x,
            y,
            hp: get(&row, COL_HP),
            max_hp: get(&row, COL_MAX_HP),
            alive: get(&row, COL_ALIVE),
            score: get(&row, COL_SCORE),
            cd: get(&row, COL_COOLDOWN),
            facing: get(&row, COL_FACING),
            ammo: get(&row, COL_AMMO),
            conn: get(&row, COL_CONNECTED),
        });
    }
    if players.is_empty() {
        return Ok(());
    }

    let mut pending = Vec::new();
    for cmd in frame.commands() {
        if cmd.kind() != "mv" {
            continue;
        }
        let pid = cmd.source();
        let code = cmd.payload().map(as_i64).unwrap_or(0);
        let dy = code % 3 - 1;
        let dx = code / 3 - 1;
        if !(-1..=1).contains(&dx) || !(-1..=1).contains(&dy) || (dx == 0 && dy == 0) {
            continue;
        }
        let Some(&idx) = id_to_idx.get(&pid) else {
            continue;
        };
        let p = &mut players[idx];
        let nx = (p.x + dx).clamp(0, ARENA_WIDTH - 1);
        let ny = (p.y + dy).clamp(0, ARENA_HEIGHT - 1);
        if nx == p.x && ny == p.y {
            continue;
        }
        if occupied.contains(&(nx, ny)) {
            continue;
        }
        occupied.remove(&(p.x, p.y));
        occupied.insert((nx, ny));
        p.x = nx;
        p.y = ny;
        p.facing = facing_of(dx, dy);
        pending.push(idx);
    }

    let r2 = AOI_RADIUS * AOI_RADIUS;
    let mut active = vec![false; players.len()];
    for &idx in &pending {
        active[idx] = true;
    }
    for i in 0..players.len() {
        if active[i] {
            continue;
        }
        for &m in &pending {
            let dx = players[i].x - players[m].x;
            let dy = players[i].y - players[m].y;
            if dx * dx + dy * dy <= r2 {
                active[i] = true;
                break;
            }
        }
    }

    for &idx in &pending {
        if !active[idx] {
            continue;
        }
        let p = &players[idx];
        ctx.update(
            TABLE,
            p.rid,
            row![
                p.id, p.x, p.y, p.hp, p.max_hp, p.alive, p.score, p.cd, p.facing, p.ammo, p.conn
            ],
        )?;
    }
    Ok(())
}

// ═══════════════════════════════════════════════════════════════════════
//  Client-callable reducers list
// ═══════════════════════════════════════════════════════════════════════

/// Reducers callable by connected clients.
pub const CLIENT_REDUCERS: &[&str] = &[
    "move_player",
    "fire_weapon",
    "fire_weapon_native",
    "reload_weapon",
    "respawn_player",
    "unit_move",
    "gather",
    "presence",
];

// ═══════════════════════════════════════════════════════════════════════
//  Module factory
// ═══════════════════════════════════════════════════════════════════════

/// Builds the module: creates tables, registers reducers, installs WASM, registers systems.
pub fn game_factory() -> PartitionFactory {
    Box::new(
        move |world_id: nexum_core::WorldId, mut store: TableStore, config: PartitionConfig| {
            // Ensure tables
            for schema in [Player::schema(), Unit::schema(), InventoryItem::schema()] {
                if !store.has_table(schema.name()) {
                    store.create_table(schema).unwrap();
                }
            }
            // Ensure position index
            if !store.has_table(TABLE)
                || store
                    .table(TABLE)
                    .unwrap()
                    .index_names()
                    .find(|&n| n == POS_INDEX)
                    .is_none()
            {
                store
                    .add_index(TABLE, IndexDef::new(POS_INDEX, &["x", "y"], false))
                    .unwrap();
            }

            let mut world = Partition::new(world_id, store, config).unwrap();

            // Register native reducers
            let reducers = [
                ReducerDefinition::new(ReducerId::from_u64(1), "player_join", player_join as _)
                    .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(2), "player_leave", player_leave as _)
                    .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(3), "move_player", move_player as _)
                    .unwrap(),
                ReducerDefinition::new(
                    ReducerId::from_u64(4),
                    "fire_weapon_native",
                    fire_weapon_native as _,
                )
                .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(5), "reload_weapon", reload_weapon as _)
                    .unwrap(),
                ReducerDefinition::new(
                    ReducerId::from_u64(6),
                    "respawn_player",
                    respawn_player as _,
                )
                .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(7), "unit_move", unit_move as _)
                    .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(8), "gather", gather as _).unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(9), "presence", presence as _).unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(10), "set_position", set_position as _)
                    .unwrap(),
                ReducerDefinition::new(ReducerId::from_u64(11), "take_damage", take_damage as _)
                    .unwrap(),
            ];
            for r in reducers {
                world.native_mut().register(r).unwrap();
            }

            // Register WASM module
            let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
            wasm.register("fire_weapon", 1, crate::wasm::fire_weapon_module())
                .unwrap();
            wasm.set_poolable("fire_weapon", true).unwrap();
            world.set_wasm(wasm);

            // Register systems (run every tick)
            world
                .add_system(
                    SystemDefinition::new(
                        nexum_core::SystemId::from_u64(10),
                        "cooldown_tick",
                        0,
                        cooldown_tick as _,
                    )
                    .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(
                        nexum_core::SystemId::from_u64(11),
                        "relay_station",
                        10,
                        relay_station as _,
                    )
                    .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(
                        nexum_core::SystemId::from_u64(12),
                        "movement_stream",
                        5,
                        movement_stream as _,
                    )
                    .unwrap(),
                )
                .unwrap();

            Ok(world)
        },
    )
}
