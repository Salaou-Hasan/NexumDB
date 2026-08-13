//! The playable terminal client (ADR-013): a real `nexum-sdk` client over
//! TCP that authenticates, joins the arena, subscribes to the authoritative
//! `players` table, renders the derived `View` as an ASCII arena, and sends
//! **intents** (reducer calls) — never state.
//!
//! Two modes:
//!
//! - **interactive** (default): type `w/a/s/d` to move, `f` to fire, `r` to
//!   reload, `x` to respawn, `q` to quit.
//! - **auto** (`--auto SECONDS`): a deterministic scripted player that
//!   chases other players, fires when aligned, reloads, and respawns — used
//!   to demonstrate genuine multiplayer and for the manual playability test.
//!
//! Everything rendered comes from the SDK's derived `View` (the server's
//! `SubscriptionRegistry` is authoritative). The client owns no gameplay
//! state.

use std::net::ToSocketAddrs;
use std::time::{Duration, Instant};

use nexum_core::{Value, WorldId};
use nexum_sdk::transport::ClientTransport;
use nexum_sdk::{Client, ServerEvent, SdkConfig};
use nexum_subscription::Query;

use crate::game::{
    COL_ALIVE, COL_AMMO, COL_CONNECTED, COL_COOLDOWN, COL_FACING, COL_HP, COL_ID, COL_SCORE,
    COL_X, COL_Y, TABLE, ARENA_HEIGHT, ARENA_WIDTH, CLIENT_REDUCERS, START_HP,
};

/// A client-side error (any error surfaced by the SDK or setup).
pub type ClientError = Box<dyn std::error::Error + Send + Sync>;

/// The outcome of a client run (used by the auto mode to prove playability).
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub struct ClientOutcome {
    /// Reducer calls sent (moves, shots, reloads, respawns).
    pub calls_sent: u64,
    /// Times the client fired the weapon.
    pub shots_fired: u64,
    /// Other players whose health dropped (hits observed in the view).
    pub hits_observed: u64,
    /// Other players who died (alive 1 → 0 observed).
    pub kills_observed: u64,
    /// Times this player died.
    pub deaths: u64,
    /// Respawns requested.
    pub respawns: u64,
    /// The last committed tick observed.
    pub last_tick: u64,
}

/// Configuration for one client run.
#[derive(Debug, Clone)]
pub struct ClientArgs {
    /// The authentication token / player name (`alice`, `bob`, `carol`, `dave`).
    pub name: String,
    /// The server host.
    pub addr: String,
    /// The server port.
    pub port: u16,
    /// Partition count of the server's arena game (the client routes to
    /// `principal_id % partitions`; default 1 = one shared world).
    pub partitions: u64,
    /// Seconds to run in auto mode (`None` = interactive).
    pub auto_seconds: Option<u64>,
    /// Automatically try to respawn when dead (auto mode always does).
    pub quiet: bool,
}

impl Default for ClientArgs {
    fn default() -> Self {
        Self {
            name: "alice".into(),
            addr: "127.0.0.1".into(),
            port: 9337,
            partitions: 1,
            auto_seconds: None,
            quiet: false,
        }
    }
}

/// A per-player snapshot of the derived view.
#[derive(Debug, Clone)]
struct PlayerView {
    id: u64,
    x: i64,
    y: i64,
    hp: i64,
    alive: bool,
    connected: bool,
    cooldown: i64,
    ammo: i64,
    score: i64,
    facing: i64,
}

/// Renders one row into a `PlayerView`.
fn player_of(row: &nexum_subscription::DeliveredRow) -> PlayerView {
    let values = row.row().values();
    // The id column is stored as `U64`; the remaining gameplay columns are
    // `I64`. Read each with the matching accessor.
    let get_id = || values.get(COL_ID).and_then(Value::as_u64).unwrap_or(0);
    let get = |column: usize, default: i64| -> i64 {
        values
            .get(column)
            .and_then(Value::as_i64)
            .unwrap_or(default)
    };
    PlayerView {
        id: get_id(),
        x: get(COL_X, 0),
        y: get(COL_Y, 0),
        hp: get(COL_HP, 0),
        alive: get(COL_ALIVE, 0) != 0,
        connected: get(COL_CONNECTED, 0) != 0,
        cooldown: get(COL_COOLDOWN, 0),
        ammo: get(COL_AMMO, 0),
        score: get(COL_SCORE, 0),
        facing: get(COL_FACING, 0),
    }
}

/// Pumps the client once and returns the events seen this pass.
fn pump(client: &mut Client) -> Vec<ServerEvent> {
    let _ = client.pump();
    client.take_events()
}

/// Pumps until `ready` is true or the deadline passes.
fn pump_until(
    client: &mut Client,
    ready: impl Fn(&Client) -> bool,
    timeout: Duration,
) -> bool {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        let _ = client.pump();
        if ready(client) {
            return true;
        }
        std::thread::sleep(Duration::from_millis(40));
    }
    client.pump().is_ok() && ready(client)
}

/// The aim cell in front of a player given its facing (0=N, 1=E, 2=S, 3=W).
fn aim_cell(player: &PlayerView) -> (i64, i64) {
    match player.facing {
        0 => (player.x, player.y - 1),
        1 => (player.x + 1, player.y),
        2 => (player.x, player.y + 1),
        _ => (player.x - 1, player.y),
    }
}

/// Renders the arena + HUD from the derived view.
fn render(client: &Client, local: u64, self_id: u64, tick: u64, clear: bool) {
    if clear {
        print!("\x1b[2J\x1b[H");
    }
    let players: Vec<PlayerView> = client
        .view(local)
        .map(|view| view.rows().map(player_of).collect())
        .unwrap_or_default();
    let mut grid = vec![vec![' '; ARENA_WIDTH as usize]; ARENA_HEIGHT as usize];
    for player in &players {
        let x = player.x.clamp(0, ARENA_WIDTH - 1) as usize;
        let y = player.y.clamp(0, ARENA_HEIGHT - 1) as usize;
        let marker = if player.id == self_id {
            '*'
        } else if !player.alive {
            'x'
        } else if !player.connected {
            '.'
        } else {
            (b'0' + (player.id % 9) as u8) as char
        };
        grid[y][x] = marker;
    }
    println!("+{}+", "-".repeat(ARENA_WIDTH as usize));
    for row in &grid {
        let line: String = row.iter().collect();
        println!("|{line}|");
    }
    println!("+{}+", "-".repeat(ARENA_WIDTH as usize));
    for player in &players {
        let status = if !player.alive {
            "DEAD".to_string()
        } else if !player.connected {
            "offline".to_string()
        } else {
            "alive".to_string()
        };
        if player.id == self_id {
            println!(
                "  *YOU   hp {:>3}/{} ammo {:>2} cd {:>2} score {} @({},{}) {}",
                player.hp,
                START_HP,
                player.ammo,
                player.cooldown,
                player.score,
                player.x,
                player.y,
                status
            );
        } else {
            println!(
                "   P{}   hp {:>3}/{} ammo {:>2} cd {:>2} @({},{}) {}",
                player.id,
                player.hp,
                START_HP,
                player.ammo,
                player.cooldown,
                player.x,
                player.y,
                status
            );
        }
    }
    println!("  tick {tick}  |  {self_id}@world |  keys: w/a/s/d move · f fire · r reload · x respawn · q quit");
}

/// Extracts the last committed tick from the pending events.
fn tick_of(events: &[ServerEvent], fallback: u64) -> u64 {
    events
        .iter()
        .rev()
        .find_map(|event| match event {
            ServerEvent::Tick { tick, .. } => Some(tick.as_u64()),
            _ => None,
        })
        .unwrap_or(fallback)
}

/// The main client entry point: connects over TCP, authenticates, joins the
/// arena, subscribes, and runs (interactive or auto).
pub fn run_client(args: ClientArgs) -> Result<ClientOutcome, ClientError> {
    let mut client = Client::new(SdkConfig::new())?;
    let addr = (args.addr.as_str(), args.port)
        .to_socket_addrs()?
        .next()
        .ok_or("could not resolve server address")?;
    client.connect(ClientTransport::tcp_connect(addr, 256, 64 * 1024)?.into_inner())?;
    if !pump_until(&mut client, |client| client.is_connected(), Duration::from_secs(5)) {
        return Err(format!(
            "handshake with {}:{} did not complete: {}",
            args.addr,
            args.port,
            client
                .take_error()
                .map(|error| error.to_string())
                .unwrap_or_else(|| "no error recorded".into())
        )
        .into());
    }
    if !args.quiet {
        println!("[client] connected to {}:{}", args.addr, args.port);
    }

    client.authenticate(&args.name)?;
    if !pump_until(
        &mut client,
        |client| client.session_principal().is_some(),
        Duration::from_secs(5),
    ) {
        return Err("authentication failed (is the token registered on the server?)".into());
    }
    let principal = client.session_principal().expect("authenticated").clone();
    if !args.quiet {
        println!("[client] authenticated as {:?}", principal.name());
    }

    // Deterministic routing: the demo arena assigns world `principal % N`.
    let world = WorldId::from_u64(principal.id() % args.partitions);
    client.attach(world)?;
    if !pump_until(
        &mut client,
        |client| client.attached_world() == Some(world),
        Duration::from_secs(5),
    ) {
        return Err(format!(
            "attach to {world} failed (the server must have joined this principal into the arena)"
        )
        .into());
    }
    if !args.quiet {
        println!("[client] attached to world {world}");
    }

    let local = client.subscribe(Query::builder(TABLE).build()?)?;
    // Wait for the authoritative join tick to land and this player's row to
    // appear in the derived view.
    let self_id = principal.id();
    if !pump_until(
        &mut client,
        |client| {
            client
                .view(local)
                .map(|view| {
                    view.rows()
                        .any(|row| player_of(row).id == self_id)
                })
                .unwrap_or(false)
        },
        Duration::from_secs(5),
    ) {
        return Err("never saw this player's row in the arena (join failed?)".into());
    }
    if !args.quiet {
        println!("[client] subscribed — in the arena");
    }

    match args.auto_seconds {
        Some(seconds) => run_auto(&mut client, &args, local, self_id, seconds),
        None => run_interactive(&mut client, &args, local, self_id),
    }
}

/// Sends a reducer call, coalescing so the bounded per-tick queue is never
/// flooded (each call executes on the next eligible tick in FIFO order).
fn send_call(
    client: &mut Client,
    outcome: &mut ClientOutcome,
    reducer: &str,
    args: nexum_reducer::ReducerArgs,
) -> bool {
    if client.pending_call_count() >= 4 {
        return false;
    }
    if client.call_reducer(reducer, args).is_ok() {
        outcome.calls_sent += 1;
        if reducer == "fire_weapon" {
            outcome.shots_fired += 1;
        }
        true
    } else {
        false
    }
}

/// The interactive loop: pump, render, read a command line, act.
fn run_interactive(
    client: &mut Client,
    _args: &ClientArgs,
    local: u64,
    self_id: u64,
) -> Result<ClientOutcome, ClientError> {
    let mut outcome = ClientOutcome::default();
    let mut tick = 0u64;
    let stdin = std::io::stdin();
    let mut line = String::new();
    loop {
        let events = pump(client);
        tick = tick_of(&events, tick);
        render(client, local, self_id, tick, true);
        println!("  > ",);
        line.clear();
        if stdin.read_line(&mut line).is_err() || line.trim() == "q" {
            break;
        }
        let key = line.trim().to_ascii_lowercase();
        let sent = match key.as_str() {
            "w" => send_call(client, &mut outcome, "move_player", crate::game::move_args(0, -1)),
            "a" => send_call(client, &mut outcome, "move_player", crate::game::move_args(-1, 0)),
            "s" => send_call(client, &mut outcome, "move_player", crate::game::move_args(0, 1)),
            "d" => send_call(client, &mut outcome, "move_player", crate::game::move_args(1, 0)),
            "f" => send_call(client, &mut outcome, "fire_weapon", nexum_reducer::ReducerArgs::new()),
            "r" => send_call(client, &mut outcome, "reload_weapon", nexum_reducer::ReducerArgs::new()),
            "x" => send_call(client, &mut outcome, "respawn_player", nexum_reducer::ReducerArgs::new()),
            _ => {
                println!("  unknown key '{key}'");
                false
            }
        };
        if !sent && !matches!(key.as_str(), "" | "q") {
            println!("  (input queue full — wait for the server)");
        }
        if client.is_closed() {
            println!("  connection closed");
            break;
        }
    }
    Ok(outcome)
}

/// The auto loop: a deterministic scripted player. Every ~100 ms it pumps,
/// renders a one-line observation, and acts: respawn when dead, reload when
/// low, chase the nearest other player, fire when the target sits at the aim
/// cell. It logs every observation so a human can verify multiplayer.
fn run_auto(
    client: &mut Client,
    _args: &ClientArgs,
    local: u64,
    self_id: u64,
    seconds: u64,
) -> Result<ClientOutcome, ClientError> {
    let mut outcome = ClientOutcome::default();
    let mut tick = 0u64;
    let deadline = Instant::now() + Duration::from_secs(seconds);
    // Remember other players' health to observe hits/kills.
    let mut last_hp: std::collections::BTreeMap<u64, i64> = std::collections::BTreeMap::new();
    let mut last_dead = false;

    while Instant::now() < deadline {
        let events = pump(client);
        tick = tick_of(&events, tick);
        outcome.last_tick = tick;
        if client.is_closed() {
            println!("[client] connection closed");
            break;
        }

        let players: Vec<PlayerView> = client
            .view(local)
            .map(|view| view.rows().map(player_of).collect())
            .unwrap_or_default();
        let me = players.iter().find(|player| player.id == self_id).cloned();

        // Observe hits/kills on OTHER players (derived from the view).
        for other in players.iter().filter(|player| player.id != self_id) {
            if let Some(previous) = last_hp.get(&other.id) {
                if other.hp < *previous {
                    outcome.hits_observed += 1;
                }
                if !other.alive && *previous > 0 {
                    outcome.kills_observed += 1;
                }
            }
            last_hp.insert(other.id, other.hp);
        }
        if let Some(me) = &me {
            if !me.alive && !last_dead {
                outcome.deaths += 1;
            }
            last_dead = !me.alive;
        }

        // Act.
        if let Some(me) = &me {
            if !me.alive {
                if send_call(client, &mut outcome, "respawn_player", nexum_reducer::ReducerArgs::new()) {
                    outcome.respawns += 1;
                }
            } else if me.ammo <= 2 {
                send_call(client, &mut outcome, "reload_weapon", nexum_reducer::ReducerArgs::new());
            } else if let Some(target) = nearest_other(&players, self_id) {
                let aim = aim_cell(me);
                let aligned = target.x == aim.0 && target.y == aim.1;
                if aligned && me.cooldown == 0 {
                    send_call(client, &mut outcome, "fire_weapon", nexum_reducer::ReducerArgs::new());
                } else if me.cooldown > 0 && aligned {
                    // Wait for the cooldown; do nothing this pass.
                } else {
                    let (dx, dy) = step_toward(me, &target);
                    send_call(client, &mut outcome, "move_player", crate::game::move_args(dx, dy));
                }
            } else {
                // No other player: drift toward the center.
                let (dx, dy) = step_toward(me, &PlayerView {
                    id: 0,
                    x: ARENA_WIDTH / 2,
                    y: ARENA_HEIGHT / 2,
                    hp: 0,
                    alive: true,
                    connected: true,
                    cooldown: 0,
                    ammo: 0,
                    score: 0,
                    facing: 1,
                });
                send_call(client, &mut outcome, "move_player", crate::game::move_args(dx, dy));
            }
        }

        // Observation line (the multiplayer proof).
        let seen: Vec<String> = players
            .iter()
            .filter(|player| player.id != self_id)
            .map(|player| {
                format!(
                    "P{}@({},{})hp{}{}",
                    player.id,
                    player.x,
                    player.y,
                    player.hp,
                    if player.alive { "" } else { " DEAD" }
                )
            })
            .collect();
        let me_desc = players
            .iter()
            .find(|player| player.id == self_id)
            .map(|player| {
                format!(
                    "me@({},{})hp{} ammo{} cd{}",
                    player.x, player.y, player.hp, player.ammo, player.cooldown
                )
            })
            .unwrap_or_else(|| "me? (not joined yet)".into());
        println!(
            "[tick {tick:>3}] {me_desc} | {} | shots {} hits {} kills {} deaths {}",
            if seen.is_empty() { "no other players".into() } else { seen.join(" ") },
            outcome.shots_fired,
            outcome.hits_observed,
            outcome.kills_observed,
            outcome.deaths
        );

        std::thread::sleep(Duration::from_millis(100));
    }
    println!(
        "[client] auto-run finished: {outcome:?} (reducers: {})",
        CLIENT_REDUCERS.join(", ")
    );
    Ok(outcome)
}

/// The nearest other alive player (deterministic: squared distance, lowest
/// id breaks ties).
fn nearest_other(players: &[PlayerView], self_id: u64) -> Option<PlayerView> {
    let me = players.iter().find(|player| player.id == self_id)?;
    players
        .iter()
        .filter(|player| player.id != self_id && player.alive)
        .min_by(|a, b| {
            let da = (a.x - me.x).pow(2) + (a.y - me.y).pow(2);
            let db = (b.x - me.x).pow(2) + (b.y - me.y).pow(2);
            da.cmp(&db).then(a.id.cmp(&b.id))
        })
        .cloned()
}

/// One deterministic step toward `target` (axis with the larger delta).
fn step_toward(me: &PlayerView, target: &PlayerView) -> (i64, i64) {
    let dx = target.x - me.x;
    let dy = target.y - me.y;
    if dx.abs() >= dy.abs() && dx != 0 {
        (dx.signum(), 0)
    } else if dy != 0 {
        (0, dy.signum())
    } else {
        (0, 0)
    }
}
