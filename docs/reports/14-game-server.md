# Phase 14 — Game Server Layer: Implementation Report

## Summary

Canonical Phase 14 adds the **`nexum-game-server`** crate — the
orchestration/product layer that composes the existing authoritative stack
(Runtime, partitions, worlds, networking, SDK) into a multiplayer game server
API. The phase was designed first (ADR-014), implemented incrementally,
tested, benchmarked, and architecture/security-reviewed. The review found
four real issues (command-burst frame-gate kills, silent step() failures,
server/client request-id collision, non-idempotent rejoin after recovery) —
all fixed with regression tests proving the semantics. **Stopped here** — no
Phase 15, no matchmaking service, no interest management, no clustering.

## Validation

- **599 tests passing** (was 595 at phase start), **0 failures**, **clippy
  zero warnings** (`--all-targets --all-features -- -D warnings`),
  `unsafe_code = forbid` maintained
- Phases 1–13 untouched and green; all WAL/recovery, subscription, WASM/
  native reducer, partition, determinism, networking, and SDK tests pass
- New Phase 14 tests: 22 game-server unit (3 review regressions), 7 e2e
  (full client→SDK→gateway→game server→world→WAL→subscription→SDK path,
  failed ticks, multi-partition isolation, exposure enforcement, WASM,
  recovery-without-replay), 1 network (server-reserved request-id rejection)
- Benchmarks recorded (release): game create ~2.3 µs, player join ~2.4 µs,
  exposure check ~11 ns, command routing ~97 ns, reducer routing ~303 ns,
  empty tick ~555 ns, tick with one reducer call ~3.1 µs

## Architecture

```
Client ── SDK ── NetworkGateway ── GameServer ── Runtime ── Partition ── World
                                                              │ systems/reducers
                                                              │ Transaction/OCC
                                                              │ ONE atomic commit
                                                              ▼
                                                          Vec<Change>
                                                      WAL ◄─┴─► SubscriptionRegistry
                                                                └─► gateway fanout
```

- `GameServer` **owns the gateway** (which owns the Runtime) — the natural
  composition root; `step()` flushes buffered commands, ticks, fans out, and
  observes events in one deterministic pass.
- The server holds **orchestration metadata only** (games, players,
  memberships, exposure, routing, events). Authoritative gameplay state lives
  exclusively in `Partition → World → TableStore`; every mutation path ends
  in `World::tick` through `Runtime::submit_input` / `submit_reducer_call`.

## Game server responsibilities (per ADR-014)

- **Game instances** — `GameInstanceId`, lifecycle
  (Created→Starting→Running→Stopping→Stopped/Destroyed, Failed), one or more
  partitions bound to runtime worlds, capacity, deterministic routing
  `partitions[player_id % n]`.
- **Players** — `PlayerId` == authenticated `Principal` id (server-stamped,
  never client-provided), per-game membership, deterministic
  join/leave/disconnect/reconnect lifecycle, no duplicate players.
- **Reducer exposure** — deny-by-default `GamePolicyTable` shared live with
  the gateway (`PolicyHandle`): client-callable vs server-only vs internal;
  roles (Player/Server/Admin); membership-gated inputs and attachment.
- **Server commands** — `submit_command` (intents, server-side source) and
  `invoke_reducer` (server-trusted), both through the simulation.
- **Server-side subscriptions** — bounded per-player, delegating truth to the
  SubscriptionRegistry.
- **Failure observation** — runtime events → `TickFailed` / `PartitionFailed`
  / `GameFailed` + lifecycle state; never a silent no-op, never a healthy
  claim over a dead world.
- **Recovery orchestration** — `recover_game` via `Runtime::recover_world`;
  recovered history is never replayed as live updates.

## Review findings & fixes (all proven by regression tests)

1. **Command-burst frame-gate kill (critical).** `submit_command` previously
   stamped every frame with the world's current tick; a burst before a tick
   produced many same-tick frames, the runtime drains one frame per tick, and
   the surplus frames failed the deterministic frame gate → world `Failed`.
   **Fix:** per-world command buffering, merged into ONE frame per world at
   `step()` (FIFO, bounded, overflow rejects explicitly with `CommandBufferFull`
   + `CommandRejected`). Test: 5-command burst commits in one tick, world
   stays Running.
2. **Silent step() failures.** A failed tick could become an invisible no-op.
   **Fix:** documented — `step()` reports failures through
   `TickFailed`/`PartitionFailed`/`GameFailed` events and lifecycle state;
   e2e asserts a failed tick yields zero mutation, zero subscription updates,
   and a `GameFailed` event.
3. **Server/client request-id collision.** Server invokes used a counter
   starting at 0 while the SDK starts at 1; a server result could be
   misrouted to a client's pending call on the same world. **Fix:**
   `SERVER_REQUEST_MSB = 1 << 63` reserved namespace; the gateway rejects
   client ids with the bit; `invoke_reducer` uses `MSB | counter` and the
   join/leave hooks use `MSB | 1 << 62 | player_id` (disjoint sub-namespace).
   Tests: client MSB id rejected with correlated error; server ids asserted
   namespaced and correlated.
4. **Non-idempotent rejoin after recovery.** After crash + recovery, a
   rejoining player re-runs `on_player_join` against a store that already
   holds the row → duplicate-key tick failure. **Fix:** documented contract —
   join reducers must be idempotent; the recovery e2e now uses matching game
   configs with `on_player_join` and proves rejoin-after-recovery commits
   cleanly, no historical replay, subsequent ticks work.

Additional review polish: buffered-command rejection now reports accurate
per-command source identity and never silently discards on a failed world
lookup; join and invoke request ids are provably disjoint.

## Files changed

- `crates/nexum-core/src/ids.rs`, `lib.rs` — `GameInstanceId`, `PlayerId`
- `crates/nexum-network/src/policy.rs` (new) — `GamePolicy` trait, default
  allow; `gateway.rs` — policy hook at attach/input/reducer boundaries,
  `fan_out_results` split, `SERVER_REQUEST_MSB`; `lib.rs` — exports
- `crates/nexum-game-server/` (new crate) — `config.rs` (incl.
  `max_pending_commands_per_world`), `lifecycle.rs`, `policy.rs`,
  `events.rs`, `metrics.rs`, `error.rs` (incl. `CommandBufferFull`),
  `server.rs` (composition root, buffering, namespacing), `lib.rs`,
  `tests.rs` (22 tests), `tests/e2e.rs` (7 tests),
  `examples/game_bench.rs`
- `Cargo.toml` — workspace member
- `docs/design/14-game-server.md`, `docs/architecture/14-game-server.md`
  (ADR-014), `docs/reports/14-game-server.md` (this report), `README.md`,
  `docs/design/README.md`

## Invariants (tested)

1. Game Server never owns or mutates authoritative state.
2. `World::tick` remains the only commit path.
3. One game = one or more partitions, each with exactly one world.
4. Player routing is deterministic; partition state is isolated (no leakage
   across worlds/games; cross-world attach rejected).
5. Unexposed/unknown reducers are denied to clients with correlated errors.
6. Failed ticks: zero authoritative mutation, zero WAL, zero subscription
   deltas, zero realtime updates, game reported Failed.
7. Reconnect yields the same PlayerId — never a duplicate player.
8. Server request ids are disjoint from client ids (both directions
   enforced).
9. A command burst before a tick merges into one frame — world stays healthy.
10. Buffered commands are never silently dropped (overflow/stop/destroy/
    flush-failure all reject with events).
11. Rejoin after recovery is idempotent; recovered history is never replayed
    as live updates.
12. `unsafe_code = forbid` remains enforced.

## Known limitations

- Game-server membership (metadata) is not persisted; after recovery, all
  players must rejoin (idempotent join reducers required).
- Join/leave hooks and matchmaking integration are explicit extension points,
  not built out.
- Single-threaded by design; no parallel game-server execution (that is
  Phase 15 territory, and Phases 9/11 already make world execution
  worker-count independent).
- Interest management per client view is out of scope (subscription layer is
  the extension point).

## Phase 15 optimization targets (baseline bottlenecks)

- Game/world creation (~2.3 µs — mostly world + store construction)
- Per-player join (~2.4 µs — membership bookkeeping + routing)
- Tick with a reducer call (~3.1 µs — dominated by the Phase 9 tick +
  Phase 10 coordination)
- Exposure lookups on the hot input path are already cheap (~11 ns)
- Command routing after buffering is ~97 ns/op

## Interface Phase 15 consumes

`GameServer::step()` (tick batching), `join_game`/`leave_game`,
`submit_command`, `invoke_reducer`, `subscribe_player`, `drain_events`,
`metrics()` — the Game Server remains a pure orchestration layer over the
authoritative stack; Phase 15 optimizes inside Phases 9–13 without touching
game-server semantics.
