//! The **actual playable multiplayer arena game** built on the Nexum stack.
//!
//! Three distinct layers (per the Nexum roadmap):
//!
//! - [`nexum-game-server`] — the reusable game-server framework (game
//!   instances, players, exposure, routing). No game mechanics.
//! - [`nexum-server`] — the reference Nexum stack demo (no gameplay).
//! - **this crate** — the playable game: authoritative gameplay reducers
//!   (native + WASM), simulation systems, the realtime server, and the
//!   terminal client, all over the real SDK/TCP network boundary.
//!
//! The simulation remains authoritative. Every mutation flows
//! `World::tick → Transaction/OCC → one atomic commit → Vec<Change> → WAL +
//! SubscriptionRegistry → network → SDK view`. The client only sends intents
//! (reducer calls); the server validates and decides.
//!
//! Run:
//!
//! ```text
//! cargo run -p game-server -- server          # the authoritative game server
//! cargo run -p game-server -- client --name alice
//! ```
//!
//! See `README.md` for the full controls and multi-client procedure.

#![allow(unsafe_code)]
#![warn(missing_docs)]

pub mod client;
pub mod config;
pub mod game;
pub mod observability;
pub mod server;
mod shutdown;
mod wasm;

pub use client::{ClientArgs, ClientOutcome, run_client};
pub use config::{LogLevel, ServerConfig};
pub use game::{
    ARENA_HEIGHT, ARENA_WIDTH, CLIENT_REDUCERS, COL_ALIVE, COL_AMMO, COL_CONNECTED, COL_COOLDOWN,
    COL_FACING, COL_HP, COL_ID, COL_MAX_HP, COL_SCORE, COL_X, COL_Y, FIRE_COOLDOWN, FIRE_DAMAGE,
    POS_INDEX, START_AMMO, START_HP, TABLE, game_factory, move_args, spawn,
};
pub use observability::{Logger, ServerMetricsSnapshot};
pub use server::{ServerArgs, run_server};
pub use shutdown::ShutdownHandle;
pub use wasm::fire_weapon_module;
