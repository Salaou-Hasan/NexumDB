# Nexum — Authoritative State Engine

> **State is authoritative. Transactions change state. Simulation produces state transitions. Subscriptions expose state.**

Nexum is a high-performance multiplayer backend and authoritative state engine written in Rust.
It is inspired by the ideas behind SpacetimeDB, Nakama, relational databases, realtime multiplayer
backends, and simulation engines — but it is **not** a clone of any of them. It is a new architecture.

## The three primitives

1. **Tables** — Tables are the authoritative state of the application/world. They carry schemas,
   typed columns, rows, primary keys, indexes, constraints, and version metadata.
2. **Reducers** — Reducers are authoritative state transitions. Each reducer executes inside a
   transaction (read → execute → write → validate → commit).
3. **Subscriptions** — Subscriptions are reactive views over authoritative table state, driven by
   committed change sets — never by polling.

Everything else — transactions (OCC), the memory-first storage engine, WAL + snapshots, the WASM
reducer sandbox, the simulation scheduler, the partition/worker runtime, and the QUIC/HTTP planes —
exists to make those three primitives reliable, deterministic, transactional, durable, scalable, and
extremely fast.

## Implementation status

The build follows a strict order: correctness first, distribution last.

| Phase | Area | Status |
|-------|------|--------|
| 0 | Repository / workspace foundation | ✅ Done |
| 1 | Core types and state model | ✅ Done |
| 2 | Table system | ✅ Done |
| 3 | Memory-first storage engine | ✅ Done |
| 4 | Transaction engine (OCC) | ✅ Done |
| 4c | Read-your-writes + phantom protection | ✅ Done |
| 5 | WAL, snapshots, recovery | ✅ Done |
| 6 | Reducer API | ✅ Done |
| 7 | WASM reducer runtime | ✅ Done |
| 8 | Subscription engine | ✅ Done |
| 9 | Deterministic simulation core | ✅ Done |
| 10 | Nexum runtime (partition/worker orchestration) | ✅ Done |
| 11 | Concurrency & parallel tick execution | ✅ Done |
| 12 | Multi-partition simulation (deterministic message bus) | ✅ Done |
| 13 | Networking + client SDKs (realtime gateway, reducer calls, `nexum-sdk`) | ✅ Done |
| 14 | Game server layer (games, players, exposure, command routing) | ✅ Done |
| 15 | Performance & benchmarking (100K→10M rows, bottleneck fixes, report) | ✅ Done |
| 16 | Production hardening & release | ⬜ |

## Repository layout

```
crates/
├── nexum-core/         # Types & state model: ids, versions, timestamps, errors, interfaces
├── nexum-table/        # Typed table engine (Phase 2)
├── nexum-tx/           # OCC transaction engine (Phase 4 + read-your-writes/phantom fix)
├── nexum-storage/      # Memory-first authoritative storage (Phase 3)
├── nexum-wal/          # WAL, snapshots, recovery (Phase 5)
├── nexum-reducer/      # Reducer execution model (Phase 6)
├── nexum-wasm/         # WASM reducer sandbox (Phase 7): wasmi host, fuel/memory limits, ABI
├── nexum-subscription/ # Subscription engine (Phase 8): committed-change observation, bounded queries, resync
├── nexum-simulation/   # Deterministic simulation (Phases 9/11/12): one World = one partition, one tx per tick, systems/schedule/RNG, deterministic parallel ticks (Phase 11), cross-partition messaging (Phase 12)
├── nexum-runtime/      # Runtime + partitions (Phases 10/12): world lifecycle, ownership, input routing, WAL+subscription coordination, recovery, the partition registry and deterministic message bus
├── nexum-network/      # Realtime networking + control plane (Phase 13): versioned binary protocol, sessions/auth, gateway, transports, reducer-call routing, typed operator API (originally implemented ahead of the roadmap; now the canonical Phase 13 foundation)
├── nexum-sdk/          # Client SDK (Phase 13): poll-driven `Client`, canonical protocol codec, sessions, correlated reducer calls, derived subscription views, reconnect/resync
├── nexum-game-server/  # Game server layer (Phase 14): game instances, players, join/leave/reconnect, deny-by-default reducer exposure, per-world command buffering, failure observation — orchestration metadata only; gameplay state stays in the simulation
```

```
benchmarks/
└── nexum-bench/        # Phase 15 benchmark crate (release-mode): micro + scale (100K/1M/5M/10M rows) + large-state tick; run `cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal` or `--scale 1_000_000`
├── nexum-server/       # Server binary — reference demo of the full stack (no gameplay)
└── game-server/        # The actual playable multiplayer arena game — real gameplay reducers (native + WASM), a TCP game server, and a terminal client over the real SDK (see below)
tests/                  # Workspace-level test harnesses (organized by area)
benchmarks/             # Benchmark harnesses
docs/
├── architecture/       # Architectural decision records
├── protocols/          # Wire protocol design
└── design/             # Design notes
```

All crates depend on `nexum-core`; `nexum-core` depends on nothing. The dependency graph is the
architecture map.

## Building and testing

```bash
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets
```

## Running the demo server

The `nexum-server` binary is a runnable demo of the full authoritative stack
(GameServer → Runtime → World → Transaction/OCC → `Vec<Change>` → WAL →
SubscriptionRegistry → network → SDK). It creates a two-partition game, joins
players, drives server-side commands and reducers (native **and** WASM), and
connects a real SDK client over an in-process transport that authenticates,
joins, attaches, subscribes, and observes committed changes as a derived view
— including a denied call to a server-only reducer (deny-by-default exposure).

```bash
cargo run -p nexum-server                # in-memory, 8 ticks
cargo run -p nexum-server -- --ticks 20  # more simulation
cargo run -p nexum-server -- --persist data  # WAL-durable run into ./data
```

## Playing the actual game

Three distinct layers (per the roadmap):

- `nexum-game-server` — the **reusable game-server framework** (game
  instances, players, exposure, routing). Contains no game mechanics.
- `nexum-server` — the **reference Nexum stack demo** (no gameplay).
- `game-server` — the **actual playable multiplayer arena game** built on
  Nexum: authoritative gameplay reducers (native `move_player`/`player_join`/
  `respawn_player`/… and a **WASM `fire_weapon`** reducer), a TCP game
  server, and a terminal client over the real SDK/network boundary.

The simulation is authoritative. The client sends **intents** (`move_player`
with a direction, `fire_weapon` with no target — the WASM reducer scans the
arena, validates facing/cooldown/ammo, resolves the hit and damage) and the
server decides the result. The client never sends positions, health, or
identity.

```bash
# Terminal 1 — the authoritative game server (1 partition, 20 ticks/s):
cargo run -p game-server -- server

# Terminals 2 and 3 — two real clients (each its own SDK + TCP connection):
cargo run -p game-server -- client --name alice
cargo run -p game-server -- client --name bob
```

Registered names: `alice`, `bob`, `carol`, `dave`. Use `--port` to change the
default `9337` and `--auto SECONDS` for a deterministic scripted player
(proves multiplayer without a keyboard).

**Controls (interactive client):** `w/a/s/d` move, `f` fire, `r` reload,
`x` respawn, `q` quit. Each render frame shows the arena, your player, other
players, health, ammo, cooldown, and the authoritative tick.

```bash
cargo run -p game-server -- server --help        # server options
cargo run -p game-server -- client --help        # client options
```

## Benchmarks

Phase 15 (see the [full report](docs/reports/15-performance.md) and the
[benchmark crate](benchmarks/nexum-bench)): Nexum was measured as
authoritative state grows from 100K → 1M → 5M → 10M rows, in release mode,
on an Intel i7-14650HX / 16 GB. Headline numbers (MEASURED):

| Metric | 100K rows | 10M rows |
|---|---|---|
| PK lookup | 46 ns | 45 ns |
| UPDATE exactly one row (tx + OCC + commit) | 0.97 µs | 0.95 µs |
| single-row subscription delta | 1.3 µs | 1.4 µs |
| subscription initial snapshot (10K delivered) | 33 ms | 3.7 s |
| tick with 100 active entities in a 10M-row store | — | 214 ns |

**The critical scale result:** a one-row update behaves the same at 10M
rows as at 100K rows — cost scales with the *changed set*, not the table.
Tick cost scales with the *active entity set*, not total rows.

```bash
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal
cargo run --release -p nexum-bench -- --scale 1_000_000
cargo run --release -p nexum-bench -- --large-tick 10_000_000 100
```

## Production (Phase 16)

Phase 16 (see the [full report](docs/reports/16-production.md)) hardened
Nexum for production and measured its real concurrent-user ceiling:

- **Production config**: one validated `ServerConfig` (`key = value` file +
  CLI overrides) covering network bounds, queues, persistence, rate limits,
  tick rate, seed, logging, and a static token→principal auth table.
  Invalid configs fail at startup.
  `cargo run -p game-server -- server --config server.conf`
- **Rate limiting**: per-connection token buckets (auth, input/s,
  reducer/s, subscribe, resync) with explicit `19` errors — never silent
  drops, never inside the simulation.
- **Graceful shutdown**: SIGINT/SIGTERM (via `ctrlc`), a stop-file, or
  `--stop-after N` → drain inbound → flush every world's WAL → exit 0.
- **Observability**: leveled structured logging + aggregate metrics
  snapshot (runtime + network + game + memory estimate).
- **Release profile**: LTO (fat), single codegen unit, panic=unwind.

**CCU (honest, measured on this laptop, in-process transport, real
protocol/gateway/runtime/world/SDK):**

| CCU (connection-only) | tick p99 | 50 ms budget | class |
|---|---|---|---|
| 1K | 2.8 ms | ✓ | PASS |
| 5K | 15.5 ms | ✓ | PASS |
| 10K | 35.5 ms | ✓ | **PASS** |
| 15K | 63.7 ms | ✗ | DEGRADED |
| 20K | 75.5 ms | ✗ | DEGRADED |

10,000 concurrent connections pass; 15–20K connect without loss but exceed
the tick budget. Realistic gameplay now uses direct PK/index lookups (Phase 17 fixed the
full-scan bottleneck — 30× server-side improvement) but remains bounded
by the subscription engine's all-to-all fan-out O(changes × subscriptions)
per tick, which is the Phase 20 interest-management target. The harness also exposed and we fixed a real bug:
cross-client request-ID collision in the gateway (all SDK clients start
request ids at 1). Reproduce:

```bash
cargo run --release -p game-server --example ccu -- --clients 10000 --profile A --ticks 100
cargo run --release -p game-server --example ccu -- --clients 500 --profile C --ticks 100
```

## Gameplay Hot-Path (Phase 17)

Phase 17 (see the [full report](docs/reports/17-gameplay-hotpath.md))
removed accidental O(N) game-reducer scans and measured the honest CCU
ceiling after those fixes:

- **Game reducers**: all 7 native reducers + WASM `fire_weapon` now use
  direct PK/index lookups — server-side profile D @ 500: **83ms → 2.7ms
  (30×)**.
- **TickUpdate encode-once**: gateway encodes the full change set once
  per world and clones bytes to each connection — 51ms → 3ms at 1K.
- **New APIs**: `Transaction::lookup_index`, `ReducerContext::lookup_index`,
  `OP_LOOKUP_INDEX` (WASM op 9), `Table::add_index`.
- **CCU**: connection-only 10K PASS; gameplay profiles saturate at ~1K due
  to the subscription engine's all-to-all fan-out O(changes × subs),
  which is explicitly Phase 20 scope.

## Hot-Path Profiling (Phase 19)

Phase 19 (see the [full report](docs/reports/19-hotpath-profiling.md))
instrumented the tick path at phase + sub-phase level and ranked the
measured bottlenecks (not assumptions):

- **#1 — Subscription all-to-all fan-out** — 30.5 ms/tick (72% of tick) at
  1K: O(changes × subs) = 1M `apply_change` calls per movement tick, each
  deep-cloning the row into the window.
- **#2 — Client-side full-set decode** — 6.6 ms/tick: O(changes × clients).
- **#3 — World tick** — 11.9 ms/tick: linear O(changes) game logic.

**Optimization (measured 2.7× on the #1 bottleneck):** `Change` now holds
its rows as `Arc<Row>` (ADR-019 D4) — the commit path wraps each
committed row once, and every subscription window shares the payload via
a refcount bump instead of a per-(change × sub) deep clone. sub_apply:
**30.5ms → 11.4ms** (avg/tick at 1K, profile C); p95 round-trip tick
**~365ms → 204ms**. The remaining cost is the O(changes × subs)
evaluation count itself — the Phase 20 interest-management target.

## License

MIT — see [LICENSE](LICENSE).
