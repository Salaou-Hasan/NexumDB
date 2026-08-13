# Phase 14 — Game Server Layer (ADR-014)

Status: **Accepted** · Canonical roadmap: Phase 14 of 16

---

## Context

Phases 1–13 established one authoritative state (TableStore), one transaction
model (OCC via `World::tick`), one commit boundary (`Vec<Change>`), WAL
durability, subscription observation, deterministic simulation, runtime
orchestration, multi-partition execution, networking, and the client SDK.
Phase 14 adds the product layer that composes these into an authoritative
multiplayer game server.

**Invariants that must survive Phase 14:** the simulation is authoritative;
`World::tick` is the only commit path; networking is an adapter; the SDK holds
derived state only; determinism is worker-count and partition-count
independent.

---

## Decision D1 — The Game Server owns the gateway (which owns the Runtime)

`GameServer` owns the network `Gateway`, which in turn owns the `Runtime`
(built from the host's `RuntimeConfig` — the host supplies the world factory):

```
host app ──► GameServer ──► NetworkGateway ──► Runtime ──► World::tick
```

Composition: `GameServer::new(runtime, network_config, authenticator, config)`
consumes a ready `Runtime`, constructs the gateway over it, and installs the
server's live authorization policy into the gateway immediately. `step()` is
the single composition root: it flushes buffered commands, delegates to
`Runtime::step_detailed()`, fans committed results out to the gateway, and
observes runtime events — one deterministic pass, one commit boundary.

Because the gateway owns the runtime, the Game Server reaches the runtime
through `server.runtime()` / `server.runtime_mut()` (and `server.gateway()` /
`gateway_mut()` for connection registration and network metrics).

Rationale: the gateway needs the runtime anyway; owning it makes the Game
Server the natural single owner and composition root while preserving the
strict control-flow direction — the Game Server and gateway both terminate
every authoritative operation in `Runtime::submit_*` → `World::tick`.

## Decision D2 — `GamePolicy` hook on the gateway; default allow

`nexum-network` gains an additive, opt-in `GamePolicy` trait:

```rust
pub trait GamePolicy: Send + Sync {
    fn authorize_attach(&self, principal: &Principal, world: WorldId) -> bool { true }
    fn authorize_input(&self, principal: &Principal, world: WorldId, _frame: &InputFrame) -> bool { true }
    fn authorize_reducer(&self, principal: &Principal, world: WorldId, reducer: &str) -> bool { true }
}
```

- The gateway holds `Box<dyn GamePolicy>` defaulting to a pass-through policy;
  **Phase 13 semantics are unchanged** unless a host installs a policy via
  `Gateway::set_policy`.
- The gateway consults the policy at the three client-driven mutation
  boundaries: attach, input submission, and reducer calls. A denial produces a
  correlated protocol error (reducer denial echoes the `request_id`) and a
  rejection metric — never a panic, never a partial mutation.
- Rejection happens **before** `Runtime::submit_*` — the authoritative path is
  untouched by denials.

## Decision D3 — Player identity is the principal identity

`PlayerId::from_principal(&Principal)` == `principal.id`. The Game Server
never accepts a client-supplied player id; identity is stamped server-side by
authentication (Phase 13) and the same `PlayerId` never refers to two
principals. Membership is per-game: `(principal, game) → PlayerRecord`. The
same principal reconnecting to a game yields the **same** `PlayerId` — no
duplicate authoritative player is ever created.

## Decision D4 — Game metadata vs gameplay state

`GameInstance` and `PlayerRecord` are orchestration metadata (id, lifecycle,
membership, routing, exposure). They are **not** authoritative gameplay state
and are never written through a storage engine or transaction. Authoritative
gameplay state lives only in `Partition → World → TableStore` and mutates only
through `World::tick`.

Join/leave initialization and cleanup are therefore **delegated to game
reducers**: `GameInstanceConfig.on_player_join` / `on_player_leave` name
reducers invoked through the runtime (server-trusted, `request_id =
SERVER_REQUEST_MSB | SERVER_JOIN_MSB | player_id`, in the reserved server
namespace — see D11). The default (`None`) performs metadata-only joins — a
valid, documented mode for games that initialize players inside tick systems
instead.

## Decision D5 — Deterministic partition routing

`create_game(config)` with `partition_count = n` allocates worlds and
partitions in fixed ascending order. A joining player is routed to
`partitions[player_id.as_u64() % n]` — a pure function of the player id and
the topology. No RNG, no hash iteration, no scheduling influence. Cross-
partition gameplay uses the Phase 12 message bus only; the Game Server
introduces no second messaging or transaction mechanism.

## Decision D6 — Failure observation via runtime events

`GameServer::step()` delegates to `Runtime::step_detailed()` (one deterministic
pass, per-world `TickResult`s) and then drains `RuntimeEvent`s to update game
state: `WorldFailed` marks the owning partition `Failed` (and the game
`Failed` when all partitions fail); `WorldRecovered` marks the partition
`Recovered`. The Game Server never invents health: a game whose authoritative
world failed is reported `Failed`, and `join_game`/`submit_command` reject it
explicitly. A failed tick commits nothing and produces zero subscription
updates (Phases 9/10). Accepted reducer calls are never silently dropped
(ADR-013 D3: per-tick budget, FIFO, requeue-on-failure).

## Decision D7 — Shared live policy via `Arc<Mutex<GamePolicyTable>>`

The gateway's policy must reflect **live** membership (which principals are
active players of which worlds) as well as static exposure. The Game Server
publishes a `PolicyHandle` (`Arc<Mutex<GamePolicyTable>>`) that implements
`GamePolicy` by locking the shared table. The Game Server updates the same
table on join/leave/disconnect/expose/revoke. The mutex is uncontended in the
single-threaded model (it provides interior mutability, never ordering), so it
does not affect determinism.

`GamePolicyTable`:
- `reducers: BTreeMap<String, ReducerPolicy>` — `{ exposure, roles }`;
  unregistered names are denied.
- `active_players: BTreeSet<(u64, WorldId)>` — currently-active membership.
- `role_overrides: BTreeMap<u64, Role>` — optional per-principal roles.

## Decision D8 — Player lifecycle semantics

- `Joining → Active` (join/reconnect), `Active ⇄ Reconnecting` (disconnect/
  reconnect), `Left` (terminal; a later join is fresh).
- `join_game` returns `JoinOutcome::{Joined, Reconnected}` — reconnect restores
  the existing membership instead of creating a new player.
- `disconnect_player` is host-driven (the host observes connection loss) and
  removes the principal from the active-input set so a disconnected client
  cannot submit through the gateway policy.
- `leave_game` runs the optional `on_player_leave` reducer, clears active
  membership and session state, and marks `Left`. Records are retained as
  metadata; authoritative deletion is the game's reducer's responsibility.

## Decision D9 — Bounded server-side subscriptions

`subscribe_player` delegates to `Runtime::subscribe` and enforces
`GameServerConfig.subscription_limit_per_player` (default 16). The
SubscriptionRegistry remains the authoritative observation system; per-player
limits are the Game Server's guard against subscription flooding on top of the
gateway's per-connection caps.

## Decision D10 — No second anything

The Game Server introduces no storage engine, no transaction engine, no OCC,
no WAL, no simulation engine, no subscription engine, no matchmaking, no
interest-management. All of those remain the existing Phases 1–13 systems.
Phase 15 optimization and Phase 16 hardening consume the Game Server purely
through the public API defined in §Interfaces.

## Decision D11 — Command buffering, request-id namespace, idempotent rejoin

**Command buffering (D11a):** commands accepted between ticks are buffered per
world and merged into one `InputFrame` per world per `step()`, stamped with the
world's current tick. One-frame-per-command would stamp every frame with the
same tick, and the runtime drains one frame per tick — the surplus frames
would fail the deterministic frame gate and kill the world. The buffer is
bounded (`GameServerConfig.max_pending_commands_per_world`, default 10 000);
overflow returns `CommandBufferFull` + a `CommandRejected` event. Buffered
commands are rejected with events (never silently dropped) when a game is
stopped or destroyed, when a world is gone or not running at flush time, or
when the merged frame is rejected by the runtime.

**Server request-id namespace (D11b):** `SERVER_REQUEST_MSB = 1 << 63` is
reserved for server-originated reducer calls. The gateway rejects any client
call carrying that bit. `invoke_reducer` uses `MSB | counter`; the join/leave
hooks use `MSB | 1 << 62 | player_id` (a disjoint sub-namespace — the counter
never sets bit 62). Server and client `(world, request_id)` keys are therefore
always disjoint, so a server result can never be misrouted to a client's
pending call.

**Idempotent rejoin contract (D11c):** game-server membership is ephemeral
(orchestration metadata, not persisted). After crash + recovery a rejoining
player is a fresh join and `on_player_join` runs again against a store that
may already contain the player's row. `on_player_join` reducers must therefore
be idempotent (check-then-insert); the recovery e2e proves the full
rejoin-after-recovery path with matching game configs. This is a contract on
game reducers, documented here — not a second write path.

---

## Interfaces

### nexum-core (additive)

```rust
GameInstanceId   // define_id! newtype
PlayerId         // define_id! newtype
```

### nexum-network (additive, opt-in)

```rust
pub trait GamePolicy: Send + Sync { /* D2 */ }
pub struct AllowAllPolicy;                        // the default
impl NetworkGateway {
    pub fn set_policy(&mut self, policy: Box<dyn GamePolicy>);
}
```

### nexum-game-server (new crate)

```
lib.rs       — re-exports
config.rs    — GameServerConfig, GameInstanceConfig
lifecycle.rs — GameLifecycle, PlayerState, GameStatus, PlayerStatus,
               PartitionState, JoinOutcome
policy.rs    — Role, ReducerExposure, ReducerPolicy, GamePolicyTable,
               PolicyHandle (impl nexum_network::GamePolicy)
events.rs    — GameServerEvent
metrics.rs   — GameServerMetrics
server.rs    — GameServer
```

Key `GameServer` methods:

```rust
GameServer::new(runtime_config: RuntimeConfig, config: GameServerConfig)
    -> Result<Self, GameServerError>
server.runtime() / runtime_mut()
server.policy_handle() -> PolicyHandle
server.shutdown() -> Result<(), GameServerError>

// Game lifecycle
create_game(GameInstanceConfig) -> Result<GameInstanceId, GameServerError>
start_game(GameInstanceId) / stop_game / destroy_game
game_status(GameInstanceId) / list_games()
recover_game(GameInstanceId, GameInstanceConfig, Option<TickId>)
    -> Result<RecoveryReport, GameServerError>

// Reducer exposure
expose_reducer(&str) / register_client_reducer(&str, &[Role]) / revoke_reducer(&str)
reducer_exposure(&str) -> Option<ReducerExposure> / is_client_callable(&str) -> bool
set_principal_role(u64, Role)

// Players
join_game(&Principal, GameInstanceId) -> Result<JoinOutcome, GameServerError>
leave_game(PlayerId) / disconnect_player(PlayerId) / reconnect handled by join_game
player_status(PlayerId) / player_world(PlayerId)

// Commands & reducers (server path)
submit_command(PlayerId, kind, Option<Value>) -> Result<(), GameServerError>  // buffered (D11a)
invoke_reducer(PlayerId, &str, ReducerArgs) -> Result<u64 /* request id */, GameServerError>  // MSB namespace (D11b)

// Subscriptions (server path, bounded)
subscribe_player(PlayerId, Query) -> Result<SubscriptionId, GameServerError>
unsubscribe_player(PlayerId, SubscriptionId) / resync_player(PlayerId, SubscriptionId)

// Orchestration
step() -> Result<Vec<(WorldId, TickResult)>, GameServerError>   // D6
drain_events() -> Vec<GameServerEvent>
metrics() -> GameServerMetrics
```

`GameServerError` preserves lower-level identity (`Runtime(RuntimeError)`,
`Core(Error)`) plus game-layer variants: `UnknownGame`, `UnknownPlayer`,
`DuplicateGame`, `GameNotRunning`, `GameFull`, `PlayerAlreadyInGame`,
`PlayerNotActive`, `PlayerNotInGame`, `WorldFailed`, `NotAuthorized`,
`Capacity`, `SubscriptionLimit`, `InvalidConfig`.

---

## Invariants (tested)

1. Game Server never owns or mutates authoritative state.
2. `World::tick` remains the only commit path (server paths call
   `Runtime::submit_input` / `submit_reducer_call` only).
3. One game instance = one or more partitions, each with exactly one world.
4. Player routing is a deterministic function of `(player_id, topology)`.
5. Unregistered / server-only reducers are denied to clients with a
   correlated error; no partial mutation on denial.
6. A principal that is not an active player of a world cannot attach to it or
   submit input to it (policy).
7. Reconnect yields the same `PlayerId`, never a duplicate authoritative
   player.
8. A failed partition/world marks the game failed — the Game Server never
   reports a dead game as healthy.
9. Recovered history is never replayed as live subscription updates.
10. `unsafe_code = forbid` remains enforced.
11. A burst of commands before a tick merges into one frame — no frame-gate
    failure, FIFO preserved, world stays healthy.
12. Buffered commands are never silently dropped: overflow, stop, destroy,
    and flush-time world failure all produce explicit `CommandRejected`
    events.
13. Server-originated request ids are disjoint from client ids (MSB
    namespace); a client cannot claim the reserved range.
14. `on_player_join` reducers are idempotent: rejoin after recovery cannot
    fail the tick with a duplicate-key error.
