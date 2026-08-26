//! Dead-simple game authoring example — shows what a Nexum game looks
//! like with the derive macros. Compare with the ~200 lines of manual
//! setup in `game.rs`: this achieves the same result in ~40 lines.

use nexum_core::{Error, Row, Value};
use nexum_reducer::ReducerContext;

#[allow(dead_code)]
fn col_i64(row: &Row, col: usize) -> i64 {
    row.get(col).and_then(Value::as_i64).unwrap_or(0)
}

/// A player in the arena.
#[allow(dead_code)]
#[allow(dead_code, clippy::all)]
#[derive(nexum_macros::NexumTable)]
#[nexum(table = "players")]
struct Player {
    #[nexum(primary_key)]
    id: u64,
    x: i64,
    y: i64,
    hp: i64,
    alive: i64,
    score: i64,
}

#[allow(dead_code)]
fn move_player(
    ctx: &mut ReducerContext,
    args: &nexum_reducer::ReducerArgs,
) -> Result<Value, Error> {
    let caller = args.require_u64("__caller")?;
    let dx = args.get("dx").and_then(Value::as_i64).unwrap_or(0);
    let dy = args.get("dy").and_then(Value::as_i64).unwrap_or(0);

    let owners = ctx.lookup_unique("players", "primary", &[Value::U64(caller)])?;
    if let Some(&rid) = owners.first() {
        if let Some(row) = ctx.get("players", rid)? {
            let x = col_i64(&row, 1);
            let y = col_i64(&row, 2);
            let nx = (x + dx).clamp(0, 47);
            let ny = (y + dy).clamp(0, 23);
            let hp = col_i64(&row, 3);
            let alive = col_i64(&row, 4);
            let score = col_i64(&row, 5);
            ctx.update(
                "players",
                rid,
                nexum_core::row![caller, nx, ny, hp, 100, alive, score, 0, 0, 10, 1,],
            )?;
        }
    }
    Ok(Value::U64(1))
}

fn main() {
    println!("=== Simple game example ===");
    println!("Player struct auto-generates TableSchema via #[derive(NexumTable)].");
    println!("No manual TableSchema::builder() needed.");
}
