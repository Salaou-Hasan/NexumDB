# Playable Multiplayer Arena Game — Implementation Report

## Summary

The playable game demo is **genuinely playable**. A user can start the
Nexum-backed game server, start real game clients (each its own SDK + TCP
connection), authenticate, join the arena, move, fire, take damage, die,
respawn, disconnect, and reconnect — with authoritative state flowing through
the full pipeline. Two real clients were run against a real TCP server and
each observed the other's movement in real time.

```
Client → SDK → NetworkGateway → GameServer → Runtime → World →
systems/reducers → Transaction/OCC → ONE atomic commit → Vec<Change> →
WAL + SubscriptionRegistry → NetworkGateway → SDK view → client render
```

The simulation remains authoritative. The client sends **intents**, never
state: `move_player(dx, dy)` (the server validates bounds and occupancy) and
`fire_weapon` with **no target** (the WASM reducer scans the arena, validates
facing/cooldown/ammo, resolves the hit, and applies damage). The client never
sends positions, health, damage, or identity.

## Three layers (kept strictly separate)

| Crate | Role |
|-------|------|
| `nexum-game-server` | Reusable game-server framework: game instances, players, lifecycle, partition assignment, command routing, deny-by-default reducer exposure, reconnect. **No game mechanics.** |
| `nexum-server` | Reference Nexum stack demo: proves GameServer → Runtime → World → WAL → subscriptions → SDK. **No gameplay.** |
| `game-server` | The actual playable game: gameplay reducers (native + WASM), the TCP game server, and the terminal client. |

## Game architecture

**Authoritative state** — one `players` table per partition/world:

```
id (U64) | x | y | hp | max_hp | alive | score | cooldown | facing | ammo | connected
```

**Native reducers** (Phase 6): `player_join` (idempotent — rejoin keeps
state), `player_leave`, `move_player` (validated one-cell step, arena-bounds
clamp, no-stacking occupancy check), `reload_weapon`, `respawn_player`,
`take_damage`, `set_position` (server-only). A `cooldown_tick` system decays
firing cooldown every tick.

**WASM reducer** (Phase 7): `fire_weapon` — runs in the wasmi sandbox, scans
the arena, derives the aim cell from the authoritative facing, validates
alive/connected/cooldown/ammo, resolves the hit, applies damage, consumes
ammo, arms cooldown, emits `hit`/`kill` events, and returns the damage dealt.
The client cannot choose its target, damage, or own hit detection.

**Server** (`game-server server`) — TCP accept loop (real network boundary via
`nexum-network`'s `TcpTransport`), auto-joins authenticated principals,
handles disconnect/reconnect (membership + authoritative `connected` flag),
and ticks every world at a fixed logical rate. It never touches tables,
transactions, or the WAL directly.

**Client** (`game-server client`) — SDK over TCP: connect → authenticate →
attach to routed world (`principal % partitions`) → subscribe to `players` →
derive the view → render. Interactive mode (`w/a/s/d` move, `f` fire, `r`
reload, `x` respawn, `q` quit) and `--auto SECONDS` deterministic scripted
mode (chase nearest player, fire when aligned, reload, respawn) that proves
multiplayer without a keyboard.

## Native vs WASM

- **Native:** `move_player`, `player_join`, `player_leave`, `reload_weapon`,
  `respawn_player`, `take_damage`, `set_position`, the cooldown system.
- **WASM:** `fire_weapon` (full combat logic inside the sandbox).
- The client does not know or care which reducer is WASM — it invokes the
  exposed reducer through the SDK; the pipeline is
  Client → SDK → Network → GameServer → World → WASM reducer → Transaction →
  Commit → Subscription → SDK view.

## Multiplayer behavior (verified by running it)

Server on `127.0.0.1:19337`, two auto clients:

```
[client bob]   tick 261  me@(18,13)hp100 | P1@(23,12)hp100   ← Bob sees Alice
[client bob]   tick 265  me@(20,13)hp100 | P1@(21,12)hp100   ← and her movement
[client alice] tick 260  me@(24,12)hp100 | P2@(18,13)hp100   ← Alice sees Bob
[client alice] tick 264  me@(22,12)hp100 | P2@(20,13)hp100   ← and his movement
```

Both clients moved independently and each saw the other's authoritative
position update through their subscription. Alice's second run
**reconnected** to her existing row (`[game] player 1 reconnected`) — the
reconnect + resync path works over real TCP.

## Additive core fixes required (regression-tested)

Two genuinely missing interfaces were added to existing crates (both
additive, both with regression tests, Phases 1–14 semantics unchanged):

1. **`nexum-network` gateway — caller-identity stamping.** Client reducer
   calls previously carried no caller identity, so a client could forge a
   `player_id` argument. The gateway now stamps the authenticated
   principal into a reserved `__caller` argument; client-callable reducers
   act only for the stamped caller. Tested: forged caller cannot move
   another player; unexposed server-only reducers are rejected.
2. **`nexum-sdk` transport — TCP flush.** `send_frame` queued bytes but
   never flushed the socket, so TCP clients' frames never left the machine.
   Now flushed per frame. (The memory transport was unaffected.)

And one **correctness bug fixed in `nexum-runtime`** (found by the e2e):
a tick committing **zero changes** still consumed a subscription sequence
number, so the next real delta looked like a `ViewGap` to every client view
and was silently dropped — the fire damage never rendered. The runtime now
skips `apply_changes` for empty change sets (the registry contract is
unchanged); a regression test proves contiguous sequences across empty ticks.

## Tests

- `tests/gameplay.rs` (11): idempotent join/rejoin, movement + bounds +
  occupancy, forged-identity rejection, damage/death/respawn, reload, WASM
  fire (hit, miss, reject while recharging/dead/disconnected, kill, and a
  determinism test: identical inputs ⇒ identical results).
- `tests/e2e.rs` (3) over the real network boundary:
  - `two_clients_join_move_fight_die_respawn_and_reconnect` — full
    multiplayer arc: both join, A moves and B sees it, A's WASM shot drops
    B's hp (both see it), kill, respawn via the client-callable reducer,
    disconnect, reconnect, correct current-state reconstruction (no
    historical replay), and continued ticks after reconnect.
  - `client_cannot_forge_position_health_or_identity` — unexposed reducer
    rejected; forged caller cannot move another player.
  - `two_clients_moving_never_leak_between_views` — views always agree.
- New regression tests in `nexum-runtime` and `nexum-network`/`nexum-sdk`
  for the two core fixes.

## Baseline measurements (debug build, no optimization — Phase 15's job)

- Server tick (1 world, 2 players, 20 Hz logical): comfortable headroom; the
  loop is pacing-bound, not compute-bound.
- Reducer round-trip through the full client → server → world → commit →
  SDK path: sub-millisecond.
- No Phase 15 optimization was performed.

## Exact commands

```bash
# Terminal 1 — authoritative game server:
cargo run -p game-server -- server                 # 127.0.0.1:9337, 1 partition, 20 ticks/s

# Terminals 2 and 3 — two real clients:
cargo run -p game-server -- client --name alice    # interactive
cargo run -p game-server -- client --name bob --auto 5   # scripted
```

Registered names: `alice` (1), `bob` (2), `carol` (3), `dave` (4). Options:
`server --port N --partitions N --hz N --seed N --persist DIR --quiet`;
`client --addr H --port N --name NAME --auto SECONDS`.

## Controls

`w/a/s/d` move · `f` fire · `r` reload · `x` respawn · `q` quit.
Each frame shows the arena, your player, other players, hp, ammo, cooldown,
and the authoritative tick.

## Verification procedure (manual)

1. Start server, start client A (alice), start client B (bob).
2. Both authenticate and join (`[game] player N joined arena`).
3. Move A: B's render shows A's position change (verified).
4. Move B: A's render shows B's position change (verified).
5. Fire A at B: both views show B's hp drop (verified in e2e test).
6. Kill B: `alive` flips; B respawns via the exposed reducer (verified).
7. Disconnect B, reconnect B: B reconstructs the current authoritative
   state, no historical replay (verified in e2e + reconnected run).

## Files changed

- `crates/nexum-network/src/gateway.rs` — caller-identity stamping
  (`__caller`) + rejection of forged ids (additive).
- `crates/nexum-network/src/lib.rs`, `src/tests.rs` — export + regression
  tests.
- `crates/nexum-sdk/src/transport.rs`, `src/tests.rs` — TCP flush +
  regression test.
- `crates/nexum-runtime/src/runtime.rs`, `src/tests.rs` — empty-commit
  subscription-sequence fix + regression test.
- `crates/game-server/` — the playable game (Cargo.toml, `src/{lib,game,
  wasm,server,client}.rs`, `src/bin/main.rs`, `tests/{gameplay,e2e}.rs`).
- `Cargo.toml` — `game-server` workspace dependency.
- `README.md` — three-layer distinction, exact commands, controls.

## Known limitations

- Interactive input is line-based (read per frame); a raw-keyboard mode is
  future polish, not architecture.
- WASM `fire_weapon` fires one cell in the facing direction; longer-range
  projectiles are future gameplay, not engine work.
- No server-side interest management yet (all clients see all players) —
  Phase 15's optimization target.
- `set_position` is exposed server-side for deterministic e2e positioning;
  it is not exposed to clients (deny-by-default).
- Only the demo roster (alice/bob/carol/dave) is registered; a real auth
  provider is Phase 16 territory.

## Statement

**Genuinely playable.** The game was compiled, the server was started, two
real clients (separate SDK instances over real TCP connections) were run
simultaneously, both moved independently, and each saw the other's
authoritative movement in real time. Combat, death, respawn, disconnect, and
reconnect are proven by the e2e suite over the same network boundary.
