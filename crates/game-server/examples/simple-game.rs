//! Dead-simple game authoring example.
//!
//! `#[derive(NexumTable)]` generates schema + full CRUD from struct fields.
//! First field is the primary key.

use nexum_macros::NexumTable;

#[allow(dead_code)]
#[derive(NexumTable)]
struct Player {
    id: u64,
    x: i64,
    y: i64,
    hp: i64,
    alive: bool,
    score: i64,
}

fn main() {
    println!("=== Simple game example ===");
    println!();
    println!("#[derive(NexumTable)] auto-generates:");
    println!("  Player::schema()          -> TableSchema");
    println!("  Player::get(ctx, id)      -> Option<Player>");
    println!("  player.save(ctx)          -> Result<()>");
    println!("  player.create(ctx)        -> Result<()>");
    println!("  Player::delete(ctx, id)   -> Result<()>");
    println!("  Player::all(ctx)          -> Vec<Player>");
}
