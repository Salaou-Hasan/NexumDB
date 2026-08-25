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

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use nexum_core::{
    Error, ReducerId, Result, Row, RowId, SystemId, TableSchema, Value, WorldId, row,
};
use nexum_network::CALLER_SOURCE_ARG;
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerDefinition};
use nexum_runtime::WorldFactory;
use nexum_simulation::{InputFrame, SimulationConfig, SimulationContext, SystemDefinition, World};
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

/// The primary-key index of the `players` table (on `id`).
pub const PK: &str = "primary";
/// The non-unique secondary index on `(x, y)`: the derived position index
/// used by the cell-occupancy check and the combat target lookup. Indexes
/// are derived infrastructure (ADR-002 D5) — authoritative position stays in
/// the row columns.
pub const POS_INDEX: &str = "pos";

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
    // Phase 26 simulation battery: density (RTS-style entity ownership),
    // resource gathering (read-modify-write economy), and presence (cheap
    // social RPC).
    "unit_move",
    "gather",
    "presence",
];

/// The `units` table: RTS-density entities owned by players
/// `[id, owner, x, y]`. One player commands many units — the battery's
/// simulation-density axis (entities ≠ connections).
pub const UNITS_TABLE: &str = "units";
/// The `inventory` table: gathered resources `[id, owner, kind]`.
pub const INVENTORY_TABLE: &str = "inventory";

// ------------------------------------------------------------- helpers

fn as_i64(value: Option<&Value>) -> i64 {
    value.and_then(Value::as_i64).unwrap_or(0)
}

fn get(row: &Row, column: usize) -> i64 {
    as_i64(row.get(column))
}

/// Fetches the row whose primary key equals `player_id` through the
/// transaction's logical view, returning `(row_id, row)`.
///
/// O(log N): the primary-key index is the proven Phase-15 fast path — never
/// a table scan (Phase 17 hot-path discipline).
fn player_by_id(ctx: &mut ReducerContext, player_id: u64) -> Result<Option<(RowId, Row)>> {
    let owners = ctx.lookup_unique(TABLE, PK, &[Value::U64(player_id)])?;
    let Some(&row_id) = owners.first() else {
        return Ok(None);
    };
    match ctx.get(TABLE, row_id)? {
        Some(row) => Ok(Some((row_id, row))),
        None => Ok(None),
    }
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
    if let Some((row_id, row)) = player_by_id(ctx, player_id)? {
        let row = with(row, COL_CONNECTED, Value::I64(1));
        ctx.update(TABLE, row_id, row)?;
        ctx.emit("rejoin", Value::U64(player_id))?;
        return Ok(Value::U64(player_id));
    }
    let (x, y) = spawn(player_id);
    ctx.insert(
        TABLE,
        row![
            player_id, x, y, START_HP, START_HP, 1i64, 0i64, 0i64, FACING_E, START_AMMO, 1i64
        ],
    )?;
    ctx.emit("join", Value::U64(player_id))?;
    Ok(Value::U64(player_id))
}

/// `player_leave` — authoritative disconnect marking (server-only). The row
/// persists so a reconnect reconstructs the current state.
pub fn player_leave(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    if let Some((row_id, row)) = player_by_id(ctx, player_id)? {
        let row = with(row, COL_CONNECTED, Value::I64(0));
        ctx.update(TABLE, row_id, row)?;
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
    let (row_id, player) = player_by_id(ctx, caller)?.ok_or_else(|| Error::not_found("player"))?;
    if !alive(&player) {
        return Err(Error::invalid_argument("player is dead — respawn first"));
    }
    if get(&player, COL_CONNECTED) != 1 {
        return Err(Error::invalid_argument("player is disconnected"));
    }
    let x = get(&player, COL_X);
    let y = get(&player, COL_Y);
    let nx = (x + dx).clamp(0, ARENA_WIDTH - 1);
    let ny = (y + dy).clamp(0, ARENA_HEIGHT - 1);
    // No stacking: a cell occupied by another alive player is impassable.
    // The derived `(x, y)` index answers the cell query in O(log N + k)
    // instead of a full-table scan (Phase 17 hot-path discipline). The index
    // lookup records a table-epoch observation, so concurrent row mutations
    // still conflict at commit (conservative phantom protection).
    let occupants = ctx.lookup_index(TABLE, POS_INDEX, &[Value::I64(nx), Value::I64(ny)])?;
    let occupied = occupants.iter().any(|&other_id| {
        if other_id == row_id {
            return false;
        }
        ctx.get(TABLE, other_id)
            .ok()
            .flatten()
            .is_some_and(|other| alive(&other))
    });
    if occupied {
        return Err(Error::invalid_argument("cell is occupied"));
    }
    let facing = facing_of(dx, dy);
    let row = with(
        with(
            with(player.clone(), COL_X, Value::I64(nx)),
            COL_Y,
            Value::I64(ny),
        ),
        COL_FACING,
        Value::I64(facing),
    );
    ctx.update(TABLE, row_id, row)?;
    Ok(Value::U64(1))
}

/// `reload_weapon` — client-callable. Refills the caller's ammunition.
pub fn reload_weapon(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let (row_id, player) = player_by_id(ctx, caller)?.ok_or_else(|| Error::not_found("player"))?;
    if !alive(&player) {
        return Err(Error::invalid_argument("player is dead"));
    }
    let row = with(player, COL_AMMO, Value::I64(START_AMMO));
    ctx.update(TABLE, row_id, row)?;
    ctx.emit("reload", Value::U64(caller))?;
    Ok(Value::I64(START_AMMO))
}

/// `respawn_player` — client-callable. A dead player may request a respawn;
/// an alive player's request is rejected. Position resets to the spawn
/// point, hp/cooldown/ammo reset, score is kept.
pub fn respawn_player(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let (row_id, player) = player_by_id(ctx, caller)?.ok_or_else(|| Error::not_found("player"))?;
    if alive(&player) {
        return Err(Error::invalid_argument("player is already alive"));
    }
    let (x, y) = spawn(caller);
    let row = with(
        with(
            with(with(player, COL_X, Value::I64(x)), COL_Y, Value::I64(y)),
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
    ctx.update(TABLE, row_id, row)?;
    ctx.emit("respawn", Value::U64(caller))?;
    Ok(Value::U64(1))
}

/// `take_damage` — server-only (never exposed). Applies `amount` damage to
/// `player_id`; a player reaching zero health dies (and emits `kill`).
pub fn take_damage(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let player_id = args.require_u64("player_id")?;
    let amount = args.get("amount").and_then(Value::as_i64).unwrap_or(0);
    let (row_id, player) =
        player_by_id(ctx, player_id)?.ok_or_else(|| Error::not_found("player"))?;
    let hp = (get(&player, COL_HP) - amount).max(0);
    let row = with(player, COL_HP, Value::I64(hp));
    let row = if hp == 0 {
        with(row, COL_ALIVE, Value::I64(0))
    } else {
        row
    };
    ctx.update(TABLE, row_id, row)?;
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
    let (row_id, player) =
        player_by_id(ctx, player_id)?.ok_or_else(|| Error::not_found("player"))?;
    let row = with(
        with(with(player, COL_X, Value::I64(x)), COL_Y, Value::I64(y)),
        COL_FACING,
        Value::I64(facing),
    );
    ctx.update(TABLE, row_id, row)?;
    ctx.emit("warp", Value::U64(player_id))?;
    Ok(Value::U64(1))
}

// ------------------------------------------------- battery reducers (P26)

/// Fetches a battery unit row by its primary key (`units.id`).
fn unit_by_id(ctx: &mut ReducerContext, unit_id: u64) -> Result<Option<(RowId, Row)>> {
    let owners = ctx.lookup_unique(UNITS_TABLE, PK, &[Value::U64(unit_id)])?;
    let Some(&row_id) = owners.first() else {
        return Ok(None);
    };
    match ctx.get(UNITS_TABLE, row_id)? {
        Some(row) => Ok(Some((row_id, row))),
        None => Ok(None),
    }
}

/// `unit_move` — RTS-density entity movement. The caller owns units; an
/// unknown id is lazily claimed by its first mover at a deterministic spot
/// so the density workload needs no separate spawn phase. O(log N):
/// primary-key lookup plus a bounded position update.
pub fn unit_move(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let unit_id = args.require_u64("unit_id")?;
    let dx = args.get("dx").and_then(Value::as_i64).unwrap_or(0);
    let dy = args.get("dy").and_then(Value::as_i64).unwrap_or(0);
    match unit_by_id(ctx, unit_id)? {
        Some((row_id, row)) => {
            if get(&row, 1) != caller as i64 {
                return Err(Error::invalid_argument("unit belongs to another player"));
            }
            let x = (get(&row, 2) + dx).clamp(0, ARENA_WIDTH - 1);
            let y = (get(&row, 3) + dy).clamp(0, ARENA_HEIGHT - 1);
            ctx.update(UNITS_TABLE, row_id, row![unit_id, caller, x, y])?;
        }
        None => {
            let (x, y) = spawn(unit_id);
            ctx.insert(UNITS_TABLE, row![unit_id, caller, x, y])?;
        }
    }
    Ok(Value::U64(unit_id))
}

/// `gather` — survival/crafting economy: a read-modify-write on the player's
/// score plus one inventory insert. Exercises OCC read sets and multi-row
/// transactions per call.
pub fn gather(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    let kind = args.get("kind").and_then(Value::as_u64).unwrap_or(1);
    let (row_id, player) = player_by_id(ctx, caller)?.ok_or_else(|| Error::not_found("player"))?;
    let score = get(&player, COL_SCORE);
    ctx.update(
        TABLE,
        row_id,
        with(player, COL_SCORE, Value::I64(score + 1)),
    )?;
    // Unique per owner while a player gathers fewer than ~1M resources.
    let inv_id = caller
        .wrapping_mul(1_048_576)
        .wrapping_add(score as u64 % 1_048_576);
    ctx.insert(INVENTORY_TABLE, row![inv_id, caller, kind])?;
    ctx.emit("gathered", Value::U64(kind))?;
    Ok(Value::I64(score + 1))
}

/// `presence` — social/idle RPC: a cheap call that still crosses the full
/// gateway → runtime → transaction path. Anchors the light workload.
pub fn presence(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let caller = args.require_u64(CALLER_SOURCE_ARG)?;
    ctx.emit("present", Value::U64(caller))?;
    Ok(Value::U64(caller))
}

/// `relay_recv` — cross-partition message handler (Phase 26 battery). The
/// Phase 12 bus executes the handler reducer named by the message `kind` on
/// the destination partition; this one just emits, so the delivered counter
/// is observable without extra state.
pub fn relay_recv(ctx: &mut ReducerContext, args: &ReducerArgs) -> Result<Value> {
    let n = args.get("n").and_then(Value::as_u64).unwrap_or(0);
    ctx.emit("relayed", Value::U64(n))?;
    Ok(Value::U64(n))
}

// -------------------------------------------------------------- systems

/// `relay_station` — Phase 26 battery: forwards every host-submitted
/// `relay` command as a cross-partition message to the next partition in
/// the topology, exercising the deterministic Phase 12 message bus at a
/// controlled rate. A no-op in single-partition worlds.
fn relay_station(ctx: &mut SimulationContext, frame: &InputFrame) -> Result<()> {
    let known = ctx.known_partitions();
    if std::env::var_os("NEXUM_RELAY_DEBUG").is_some() {
        eprintln!(
            "[relay] p={:?} known={:?} cmds={}",
            ctx.partition(),
            known,
            frame.commands().len()
        );
    }
    if known.len() < 2 {
        return Ok(());
    }
    let from = ctx.partition();
    let idx = known.iter().position(|p| *p == from).unwrap_or(0);
    let to = known[(idx + 1) % known.len()];
    for command in frame.commands() {
        if command.kind() == "relay" {
            ctx.send_to(to, "relay_recv", ReducerArgs::new())?;
        }
    }
    Ok(())
}

/// `movement_stream` — Phase 27a battery system: applies client movement
/// commands (`mv`, payload `(dx+1)*3+(dy+1)`, source = gateway-stamped
/// principal) submitted as input frames instead of correlated reducer
/// calls. Identical authority and semantics to `move_player` (alive check,
/// arena clamp, occupancy via the position index, facing derivation);
/// commands arrive merged per world-tick in FIFO order.
fn movement_stream(ctx: &mut SimulationContext, frame: &InputFrame) -> Result<()> {
    for command in frame.commands() {
        if command.kind() != "mv" {
            continue;
        }
        let player_id = command.source();
        let code = as_i64(command.payload());
        let dy = code % 3 - 1;
        let dx = code / 3 - 1;
        if !(-1..=1).contains(&dx) || !(-1..=1).contains(&dy) || (dx == 0 && dy == 0) {
            continue;
        }
        let Some((row_id, player)) = unit_or_player(ctx, player_id)? else {
            continue;
        };
        if get(&player, COL_ALIVE) == 0 {
            continue;
        }
        let x = get(&player, COL_X);
        let y = get(&player, COL_Y);
        let nx = (x + dx).clamp(0, ARENA_WIDTH - 1);
        let ny = (y + dy).clamp(0, ARENA_HEIGHT - 1);
        if nx == x && ny == y {
            continue;
        }
        let occupants = ctx.lookup_index(TABLE, POS_INDEX, &[Value::I64(nx), Value::I64(ny)])?;
        let occupied = occupants.iter().any(|&other_id| {
            other_id != row_id
                && ctx
                    .get(TABLE, other_id)
                    .ok()
                    .flatten()
                    .is_some_and(|other| get(&other, COL_ALIVE) != 0)
        });
        if occupied {
            continue;
        }
        let row = with(
            with(with(player, COL_X, Value::I64(nx)), COL_Y, Value::I64(ny)),
            COL_FACING,
            Value::I64(facing_of(dx, dy)),
        );
        ctx.update(TABLE, row_id, row)?;
    }
    Ok(())
}

/// Resolves a battery actor by id: a player row if present, else nothing.
/// (Kept separate from `player_by_id` so the stream system can skip absent
/// actors instead of erroring a whole tick.)
fn unit_or_player(ctx: &mut SimulationContext, player_id: u64) -> Result<Option<(RowId, Row)>> {
    let owners = ctx.lookup_unique(TABLE, PK, &[Value::U64(player_id)])?;
    let Some(&row_id) = owners.first() else {
        return Ok(None);
    };
    match ctx.get(TABLE, row_id)? {
        Some(row) => Ok(Some((row_id, row))),
        None => Ok(None),
    }
}

// Per-thread cooldown tracking map. Each world runs on its own thread
// (parallel via `std::thread::scope`, serial by definition), so a
// thread-local provides natural isolation without cross-world or
// cross-test contamination.
//
// Using a tracked set avoids the O(N) full-table scan that was the
// dominant cost at high CCU. With 10K players, only the handful who
// recently fired are tracked — reducing tick cost from O(total_players)
// to O(active_cooldowns).
thread_local! {
    static COOLDOWN_MAP: RefCell<BTreeMap<WorldId, BTreeSet<RowId>>> =
        const { RefCell::new(BTreeMap::new()) };
}

/// The per-tick cooldown system: weapon cooldowns tick down by one per tick.
/// Runs on every world, every tick, through the tick transaction.
///
/// **Optimization (tick-23):** Instead of scanning every player row (O(N)),
/// we maintain a per-world set of players with active cooldowns. The set is
/// populated from the transaction's pending writes (fire_weapon sets
/// cooldown > 0 via WASM) and pruned when cooldown reaches zero.
/// Complexity: O(active_cooldowns) instead of O(total_players).
fn cooldown_tick(ctx: &mut SimulationContext, _frame: &InputFrame) -> Result<()> {
    let world = ctx.world_id();

    // 1. Discover newly-fired players from this tick's pending writes.
    //    fire_weapon (WASM) calls ctx.update which buffers the write;
    //    we inspect the buffer to find cooldown > 0 without a full scan.
    {
        let pending = ctx.pending_writes_for_table(TABLE);
        COOLDOWN_MAP.with(|cell| {
            let mut map = cell.borrow_mut();
            let set = map.entry(world).or_default();
            for (row_id, entry) in &pending {
                if let nexum_tx::WriteEntry::Update(row) = entry {
                    let cd = get(row, COL_COOLDOWN);
                    if cd > 0 {
                        set.insert(*row_id);
                    }
                }
            }
        });
    }

    // 2. Iterate only tracked players — O(active_cooldowns).
    let to_process: Vec<RowId> = COOLDOWN_MAP.with(|cell| {
        let map = cell.borrow();
        match map.get(&world) {
            Some(set) => set.iter().copied().collect(),
            None => Vec::new(),
        }
    });
    if to_process.is_empty() {
        return Ok(());
    }
    let mut to_remove = Vec::new();
    for row_id in to_process {
        // Read through the transaction's logical view (read-your-writes)
        // to see the latest cooldown value including any in-tick mutation.
        match ctx.get(TABLE, row_id)? {
            Some(row) => {
                let cd = get(&row, COL_COOLDOWN);
                if cd > 0 {
                    ctx.update(TABLE, row_id, with(row, COL_COOLDOWN, Value::I64(cd - 1)))?;
                }
                if cd <= 1 {
                    // Will reach zero after this decrement (or was already zero).
                    to_remove.push(row_id);
                }
            }
            None => {
                // Player no longer exists (killed/dropped).
                to_remove.push(row_id);
            }
        }
    }
    if !to_remove.is_empty() {
        COOLDOWN_MAP.with(|cell| {
            let mut map = cell.borrow_mut();
            if let Some(set) = map.get_mut(&world) {
                for id in &to_remove {
                    set.remove(id);
                }
            }
        });
    }
    Ok(())
}

// -------------------------------------------------------------- factory

/// Builds the `players` table if missing, and ensures the derived position
/// index exists (idempotent across recovery — a table persisted before the
/// index was declared gets it added over existing rows).
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
            .index(POS_INDEX, &["x", "y"])
            .build()
            .expect("valid players schema");
        store.create_table(schema).expect("players table created");
    } else if store
        .table(TABLE)
        .is_some_and(|table| !table.index_names().any(|name| name == POS_INDEX))
    {
        let def = nexum_core::IndexDef::new(POS_INDEX, &["x", "y"], false);
        store
            .add_index(TABLE, def)
            .expect("pos index added over existing rows");
    }
    if store.table(UNITS_TABLE).is_none() {
        let schema = TableSchema::builder(UNITS_TABLE)
            .column("id", nexum_core::ColumnType::U64)
            .column("owner", nexum_core::ColumnType::U64)
            .column("x", nexum_core::ColumnType::I64)
            .column("y", nexum_core::ColumnType::I64)
            .primary_key(&["id"])
            .build()
            .expect("valid units schema");
        store.create_table(schema).expect("units table created");
    }
    if store.table(INVENTORY_TABLE).is_none() {
        let schema = TableSchema::builder(INVENTORY_TABLE)
            .column("id", nexum_core::ColumnType::U64)
            .column("owner", nexum_core::ColumnType::U64)
            .column("kind", nexum_core::ColumnType::U64)
            .primary_key(&["id"])
            .build()
            .expect("valid inventory schema");
        store.create_table(schema).expect("inventory table created");
    }
}

/// The game world factory: registers the native reducers, the cooldown
/// system, and the WASM `fire_weapon` module on every world.
pub fn game_factory() -> WorldFactory {
    Box::new(
        |id: WorldId, mut store: TableStore, sim: SimulationConfig| {
            ensure_schema(&mut store);
            let mut world = World::new(id, store, sim)?;
            world
                .add_system(
                    SystemDefinition::new(SystemId::from_u64(0), "cooldown_tick", 0, cooldown_tick)
                        .unwrap(),
                )
                .unwrap();
            world
                .add_system(
                    SystemDefinition::new(
                        SystemId::from_u64(1),
                        "relay_station",
                        10,
                        relay_station,
                    )
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
                ("unit_move", unit_move),
                ("gather", gather),
                ("presence", presence),
                ("relay_recv", relay_recv),
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
            world
                .add_system(
                    SystemDefinition::new(
                        SystemId::from_u64(2),
                        "movement_stream",
                        5,
                        movement_stream,
                    )
                    .unwrap(),
                )
                .unwrap();
            let mut wasm = WasmModuleRegistry::new(WasmLimits::default()).unwrap();
            wasm.register("fire_weapon", 1, fire_weapon_module())
                .unwrap();
            // Stateless scratch-memory module (ADR-007): immutable globals,
            // output envelope fully rewritten per call → instance pooling
            // is safe and saves ~3.3 µs of instantiate per invocation.
            wasm.set_poolable("fire_weapon", true).unwrap();
            world.set_wasm(wasm);
            Ok(world)
        },
    )
}
