# Phase 14 — Game Server Layer (Design)

Status: **Design** · Canonical roadmap: Phase 14 of 16

---

## 1. Position in the architecture

Nexum is **simulation-centered**. The Game Server Layer is the orchestration /
product layer that composes the existing authoritative stack — TableStore,
Transaction/OCC, WAL, reducers, WASM, subscriptions, simulation, runtime,
concurrency, multi-partition, networking, and the client SDK — into a
multiplayer game server API.

```
Client
  ↓
nexum-sdk            (client-side derived state / API)
  ↓
nexum-network        (connections, sessions, protocol, routing)
  ↓
nexum-game-server    ← THIS PHASE: game instances, players, exposure, policy
  ↓
nexum-runtime        (workers, worlds, scheduling, lifecycle)
  ↓
Partition → World    (one authoritative partition per world)
  ↓
Simulation systems / reducers
  ↓
Transaction/OCC
  ↓
ONE atomic commit
  ↓
Vec<Change> ──► WAL ──► SubscriptionRegistry ──► network fanout ──► SDK view
```

The Game Server **never** becomes another authoritative state system, another
transaction engine, another storage engine, or another simulation engine. It
holds **game metadata only** (Part D of ADR-014); authoritative gameplay state
stays inside `Partition → World → TableStore`.

---

## 2. What is a Game Server?

A `nexum-game-server` crate exposing a high-level, authoritative multiplayer
API. It owns:

- **Game instances** — named, configured authoritative running games, each
  backed by one or more partitions/worlds.
- **Players** — stable identities (derived from the authenticated principal),
  membership, lifecycle, and per-game routing.
- **Reducer exposure & permissions** — which reducers clients may invoke, and
  by which roles.
- **Server-authoritative commands** — intents routed into the simulation; the
  simulation decides the authoritative result.
- **Server-side subscription orchestration** — per-player subscription setup
  and limits, delegating truth to the `SubscriptionRegistry`.
- **Game lifecycle events & metrics** — orchestration observability.

It delegates everything that is authoritative: state (World), mutation
(Transaction/OCC), durability (WAL), observation (SubscriptionRegistry),
identity (network `Principal`), transport (network gateway), and execution
scheduling (Runtime).

---

## 3. Responsibility boundaries

| Concern | Owner |
|---|---|
| Authoritative gameplay state | World → TableStore |
| Atomic mutation | Transaction/OCC (`World::tick`) |
| Durability | WAL (`Runtime::step` persists committed `TickResult`) |
| Observation | SubscriptionRegistry (committed changes only) |
| Deterministic execution | Simulation (Phase 9) + concurrency (Phase 11) |
| Partition topology & cross-partition messages | Runtime (Phase 12) |
| Connections, sessions, protocol, transport | nexum-network (Phase 13) |
| Client-side derived state | nexum-sdk (Phase 13) |
| **Game instances, players, exposure, policy, lifecycle events** | **nexum-game-server (this phase)** |

The Game Server owns **no** TableStore, no Transaction, no WAL, no
SubscriptionRegistry instance, no World. It calls into the Runtime, which calls
into the World. There is exactly one commit path: `World::tick`.

---

## 4. Core model

### 4.1 Identity chain

```
Principal (authenticated, protocol-independent)
  ↓  player_id := principal.id          (server-stamped, never client-provided)
PlayerId
  ↓
GameInstanceId + PlayerRecord (per-game membership)
  ↓
PartitionId (deterministic routing)
  ↓
WorldId (the partition's authoritative world)
```

- `PlayerId` and `GameInstanceId` are new typed ids in `nexum-core`, following
  the existing `define_id!` newtype convention.
- A **player is a principal**; the same `PlayerId` never refers to two
  different people. Connection/session/socket identity is transport state and
  is never used as player identity.

### 4.2 GameInstance (metadata only)

```
GameInstance
  ├── GameInstanceId
  ├── GameInstanceConfig   (game_type, max_players, partition_count, reducers…)
  ├── GameLifecycle        (Created → Starting → Running → Stopping → Stopped → Destroyed, Failed)
  ├── partitions: Vec<GamePartition>   (PartitionId + WorldId + PartitionState)
  └── players: BTreeMap<PlayerId, PlayerRecord>
```

Authoritative gameplay state of a game lives in its partitions' worlds. The
GameInstance record is orchestration metadata and is not authoritative
gameplay state.

### 4.3 PlayerRecord

```
PlayerRecord
  ├── PlayerId  (== principal id)
  ├── GameInstanceId
  ├── partition + world     (assigned at join; stable while active)
  ├── state: PlayerState    (Joining → Active ⇄ Reconnecting → Left)
  └── session/connection    (optional association, cleared on disconnect)
```

### 4.4 Lifecycles

- `GameLifecycle`: `Created → Starting → Running → Stopping → Stopped` and
  `Running → Failed`. `Destroyed` is terminal (removes the record).
- `PlayerState`: `Joining → Active ⇄ Reconnecting → Left`. `Left` is terminal
  for a membership; a subsequent join is a fresh join.

---

## 5. Game Server API (conceptual)

```
create_game / start_game / stop_game / destroy_game / game_status / list_games
register_client_reducer / expose_reducer / revoke_reducer / reducer_exposure
join_game / leave_game / disconnect_player / player_status / player_world
submit_command (server-side intent) / invoke_reducer (server-trusted)
subscribe_player / unsubscribe_player / resync_player
step() → per-world TickResults   (delegates to runtime, updates game state)
drain_events() / metrics()
recover_game(...)  (orchestrates runtime recovery, then resubscribes clients)
```

Details and exact signatures are decided in ADR-014 and implemented in
`crates/nexum-game-server`.

---

## 6. Server authority (Part I)

Clients never assert authoritative outcomes. Clients submit **intents**
(commands, reducer calls); the simulation decides the **authoritative result**.

| Client sends | Simulation decides |
|---|---|
| `move_player(direction)` | new transform (validated, clamped) |
| `attack(target)` | damage, kill, loot |
| `craft(item)` | costs, success, inventory deltas |

The Game Server's exposure model is the gate: **only explicitly registered
client-callable reducers** are invokable by clients; everything else is
server-only or internal. Unknown reducer names are denied by default.

---

## 7. Reducer exposure & permissions

Classification of every reducer:

1. **Client-callable** — explicitly registered via `expose_reducer` /
   `register_client_reducer`; invoked by clients through
   `Client → gateway → policy → runtime → World::tick`.
2. **Server-only** — invocable by the Game Server (`invoke_reducer`,
   `on_player_join` / `on_player_leave`) but denied to clients.
3. **Internal** — invoked only by simulation systems.
4. **System / WASM** — the existing Phase 6/7 machinery; classification does
   not change their execution path.

**Enforcement point (network path):** the gateway consults a `GamePolicy`
hook before executing client reducer calls, input frames, and world
attachment. The default policy allows everything (Phase 13 behavior is
unchanged); the Game Server installs a live `PolicyHandle` that shares the
server's exposure table and active-player membership.

**Enforcement point (server path):** `GameServer::invoke_reducer` and the
join/leave hooks are server-trusted; they check game/player validity but do
not need client permissions (the server is authorized by construction).

Roles are minimal (`Player`, `Server`, `Admin`) with optional per-principal
role overrides — deliberately **not** a full RBAC.

---

## 8. Join / leave / reconnect (Part E–G)

### Join (`join_game(principal, game)`)

1. Validate the principal and the game (exists, running, capacity).
2. If a membership already exists (any state except `Left`) → **reconnect**:
   restore `Active`, update session, return `Reconnected`.
3. Otherwise create the player, assign `partition = partitions[player_id %
   partitions.len()]` (deterministic), register membership.
4. Optionally invoke the configured `on_player_join` reducer through the
   runtime (authoritative initialization via the simulation path — the game
   server never writes tables directly).
5. Return `Joined { player, world }`.

**Idempotent rejoin contract (D3):** game-server membership is ephemeral
(orchestration metadata, not persisted), so after a crash + recovery a
rejoining player is a fresh join and `on_player_join` runs again — against a
store that may already contain the player's row. **`on_player_join` reducers
must therefore be idempotent** (e.g. check for the row before inserting); the
recovery e2e proves the full rejoin-after-recovery path with matching game
configs. This is a contract on game reducers, documented here and in ADR-014,
not a second write path.

### Leave (`leave_game(player)`)

1. Validate the player.
2. Optionally invoke the configured `on_player_leave` reducer (authoritative
   cleanup through the simulation).
3. Remove the player from the active-input set, clear session state, state →
   `Left`.
4. Player *records* are retained (metadata) so a future join is detected as a
   fresh join, never a silent second authoritative player.

**Authoritative player state is deleted/persisted by the game's own reducers,
not by the Game Server.** The Game Server never deletes rows out-of-band.

### Disconnect / reconnect

- On connection loss the host calls `disconnect_player`: state → `Reconnecting`
  (session cleared, removed from active-input set).
- Reconnect = `join_game` with the same principal: same `PlayerId`, no
  duplicate player, state → `Active`, membership restored. If the game or its
  partition is unavailable, the join fails with an explicit lifecycle error
  (never a silent half-join).

---

## 9. Commands & inputs

- **Network path:** `Client → SDK → gateway (decode, session, policy, source
  stamping) → Runtime::submit_input → World::tick`. The gateway already stamps
  the authoritative principal source (Phase 13 anti-spoofing).
- **Server path:** `GameServer::submit_command(player, kind, payload)` builds
  an `InputCommand` whose source is the **player id** (server-side).
- **Per-world command buffering (D3):** commands accepted between ticks are
  buffered per world and merged into **one `InputFrame` per world per
  `step()`**, stamped with the world's current tick. This is required: the
  runtime drains one frame per tick, so submitting one frame per command would
  stamp every frame with the same tick and the surplus frames would fail the
  deterministic frame gate and kill the world. The buffer is bounded
  (`max_pending_commands_per_world`, default 10 000); overflow rejects
  explicitly with a `CommandBufferFull` error + `CommandRejected` event —
  never a silent drop. On `stop_game`/`destroy_game`, buffered commands are
  rejected with events (never silently discarded).
- Both paths terminate in `World::tick`; there is no alternative mutation
  path. Duplicate/stale commands are handled by the existing Phase 13
  request/tick gates (request-id correlation, late-frame rejection).
- **Server request-id namespace (D3):** request ids with the top bit
  (`SERVER_REQUEST_MSB = 1 << 63`) are reserved for server-originated calls.
  The gateway rejects any client reducer call carrying that bit, so a server
  invoke (`MSB | counter`) or a join/leave hook call (`MSB | 1 << 62 |
  player_id`, a disjoint sub-namespace) can never collide with a client's
  pending `(world, request_id)` — server results can never be misrouted to a
  client.

---

## 10. Partitions & routing (Part M)

- `create_game(config)` with `partition_count = n` creates `n` worlds in the
  runtime and binds `n` partitions to them (Phase 12 `register_partition`).
- Player → partition routing is **deterministic**: `partitions[player_id %
  n]`. No RNG, no hash-map order, no OS scheduling influence.
- Cross-partition gameplay uses the existing Phase 12 message bus
  (`Runtime::send_message`). The Game Server does not introduce a second
  message or transaction mechanism.
- `reassign_world` (Phase 10) is exposed via the control plane; a reassigned
  world keeps its partition binding and its players' routing metadata points
  at the same world id — routing does not break.

---

## 11. Failure semantics (Part N, U)

- **World/partition failure:** `GameServer::step()` drains `RuntimeEvent`s. On
  `WorldFailed`, the owning partition is marked `Failed` and a
  `PartitionFailed` event is emitted; if every partition of a game fails, the
  game enters `Failed` and a `GameFailed` event is emitted. The Game Server
  never pretends a game is healthy when its authoritative world is dead.
  A failed tick is **never a silent no-op**: `step()` reports it through
  `TickFailed` / `PartitionFailed` / `GameFailed` events and the game's
  lifecycle state — the authoritative world either committed or the game is
  marked failed.
- **Worker failure:** surfaces as `WorldFailed` via the runtime's existing
  failure propagation; handled identically.
- **Recovery:** `recover_game` orchestrates `Runtime::recover_world` (Phase 5
  engine, Phase 10 orchestration) for each partition, then re-registers
  partitions and starts. Recovered history is **never replayed as live
  subscription updates** (Phase 8/13 semantics preserved).
- **Command/reducer failures:** correlated results/errors via Phase 13
  request ids; failed ticks commit nothing and produce zero subscription
  updates (Phase 9/10 semantics preserved). Accepted calls are never silently
  dropped (ADR-013 D3 per-tick budget, FIFO, requeue-on-failure).

---

## 12. Determinism (Part V)

The Game Server adds no nondeterminism:

- All collections are `BTreeMap`/`Vec` — iteration order is value order.
- ID allocation is a monotonic counter — the same operation sequence produces
  the same ids.
- Partition routing is `% n`.
- World ids/partition ids for a game are allocated in a fixed order.
- The shared `PolicyHandle` uses `Arc<Mutex<…>>` for interior mutability only;
  it is uncontended in the single-threaded model and never participates in
  execution ordering.

Networking timing does not enter simulation semantics: identical seed, game
configuration, partition topology, inputs, reducer code, and system
definitions produce identical authoritative state, `Vec<Change>`, and events
regardless of worker count, connection identity, or network batching
(Phases 9/11/12 remain the correctness authority).

---

## 13. Security (Part X)

The Game Server adds these boundaries on top of the Phase 13 protections:

- **No client-provided identity:** player ids derive from the server-side
  `Principal`; forged `PlayerId`/`SessionId`/`ConnectionId` fields in frames
  are ignored (Phase 13) and never consulted by the Game Server.
- **Deny-by-default reducer exposure:** unregistered reducers are not
  client-callable; server-only reducers are denied with correlated errors.
- **Membership-gated inputs:** the live policy rejects input frames and world
  attachment from principals that are not active players of that world.
- **Bounded everything:** per-player subscription limits, existing gateway
  frame/queue/request caps, bounded event logs and metrics.
- **Cross-game / cross-player isolation:** routing is per-world; a principal
  attached to game A's world cannot submit to game B's world (policy +
  attachment gates).
- `unsafe_code = forbid` remains enforced workspace-wide.

---

## 14. Observability (Part T)

`GameServerMetrics` (bounded counters): games active, players active,
connections active (delegated to gateway), players per game, partitions per
game, commands received/rejected/executed, reducer failures, WASM failures,
joins, leaves, reconnects, partition/world/tick failures.

`GameServerEvent` (bounded log, drained by the host): `GameCreated/Started/
Stopping/Stopped/Destroyed/Failed`, `PlayerJoined/Reconnected/Disconnected/
Left/Removed`, `PartitionAssigned/Failed/Recovered`, `CommandRejected`,
`ReducerRejected`, `TickFailed` passthrough. These are **orchestration
events** — distinct from `ReducerEvent`/`Vec<Change>`/`SubscriptionUpdate`
(the gameplay domains).

---

## 15. Matchmaking integration point (Part Q)

Not implemented. The integration point is explicit: a future matchmaker
produces a `GameInstanceId`; the Game Server's `join_game(principal, game)`
performs validation, capacity checks, deterministic partition assignment,
player initialization, and subscription setup. The Game Server does not own
matchmaking policy.

---

## 16. Recovery (Part W)

```
normal game ── crash ── recovery ── game instance restored ── players reconnect ── simulation continues
```

- `GameServer::recover_game` uses `Runtime::recover_world` (Phase 5 engine).
- Reconnected clients **reattach** (Phase 13 semantics): authenticate →
  `join_game` (returns `Reconnected`) → attach → (re)subscribe → **fresh
  snapshot**, then only future commits flow as updates. Historical changes are
  never re-emitted as live updates.

---

## 17. Interface Phase 15 consumes

Phase 15 (performance) consumes the Game Server through its public API only:
`GameServer::step()` (tick batching), `join_game`/`leave_game`, `submit_command`,
`invoke_reducer`, `subscribe_player`, `drain_events`, `metrics()`. Identified
baseline bottlenecks to measure (not optimize now): game/world creation cost,
command routing overhead, exposure lookups on the hot input path, per-player
subscription setup, and partition routing math.
