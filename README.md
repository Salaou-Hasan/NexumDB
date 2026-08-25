# Nexum — Authoritative State Engine for Multiplayer Games & Simulation

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

**Nexum** is an authoritative state engine for realtime multiplayer games and simulation backends, written in Rust. It provides OCC transactions, WASM-sandboxed reducers, deterministic simulation, reactive subscriptions, and a versioned binary protocol — all in a single-node, memory-first architecture.

> **State is authoritative. Transactions change state. Simulation produces state transitions. Subscriptions expose state.**

Nexum is inspired by SpacetimeDB, Nakama, relational databases, and simulation engines — but it is **not** a clone of any of them. It is a new architecture designed for the specific demands of authoritative multiplayer state synchronization.

### Key features

- **Authoritative state** — one source of truth; clients send intents, never positions
- **OCC transactions** — optimistic concurrency control with read/write set validation
- **WASM reducers** — sandboxed, deterministic gameplay logic with host-call ABI
- **Deterministic simulation** — reproducible ticks, parallel world execution, partition sharding
- **Reactive subscriptions** — committed-change observation with bounded queries and resync
- **Realtime protocol** — versioned binary frames, sessions, auth, gateway, client SDK
- **20K+ connection-only CCU** — measured on a single laptop, in-process transport
- **Minimal, contained unsafe** — the workspace lint allows `unsafe_code`, but unsafe exists only in three audited modules (the lock-free SPSC transport ring, the WASM linker cache, and the `nexum-alloc-count` counting allocator); avoid adding new unsafe without strong justification

## Why Nexum?

Most multiplayer backends separate the database from the simulation. Nexum **unifies** them: the
authoritative state store **is** the simulation. There is no data drift, no reconciliation, no
catch-up sync. Every tick produces deterministic state transitions that are immediately visible to
subscribers.

**Compared to SpacetimeDB:** Nexum is Rust-native with a custom WASM reducer sandbox, OCC
transactions, and a simulation-first architecture.

**Compared to Nakama:** Nexum is not a general-purpose game server. It is a state engine where
the simulation IS the database. No Lua/JS runtime — just authoritative state at extreme scale.

**Compared to writing your own:** Nexum provides the hard parts out of the box: OCC, WAL,
determinism, WASM sandboxing, subscriptions, and a realtime protocol. You write reducers; Nexum
makes them fast, durable, and concurrent.

## The three primitives

1. **Tables** — The authoritative state of the application/world. Schemas, typed columns, rows,
   primary keys, indexes, constraints, and version metadata.
2. **Reducers** — Authoritative state transitions. Each reducer executes inside a transaction
   (read → execute → write → validate → commit).
3. **Subscriptions** — Reactive views over authoritative table state, driven by committed change
   sets — never by polling.

Everything else — OCC transactions, the memory-first storage engine, WAL + snapshots, the WASM
reducer sandbox, the simulation scheduler, the partition/worker runtime, and the realtime protocol —
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
| 16 | Production hardening & release | ✅ Done |
| 17 | Gameplay hot-path & CCU scaling (O(N) reducer scans → direct lookup) | ✅ Done |
| 18 | Multi-core runtime (parallel world/partition ticks) | ✅ Done |
| 19 | Execution hot-path profiling (ranked bottlenecks, Arc-shared rows) | ✅ Done |
| 20 | Interest management / AOI (duplicate-subscription grouping) | ✅ Done |
| 21 | Networking & serialization hot-path (Arc frames, attached index) | ✅ Done |
| 21.5 | Extreme execution profiling (per-reducer/allocation cost map, spike analysis) | ✅ Done |
| 22 | WASM reducer optimization (COW WriteSet, has_any_insert skip, absorb fast-path) | ✅ Done |
| 22.5 | Networking hot-path (Arc rows, delta batching, incremental CRC, pump removal) | ✅ Done |
| 23–25 | Performance campaign (rayon pool, zero-copy deltas, atomic fast-paths) | ✅ Done |

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

## Performance & benchmarks

Nexum is built measure-first: every optimization ships with before/after
numbers from the same harness, and a change with no measured improvement is
reverted. The full campaign history — Phases 15–25, each with methodology
and honest PASS / DEGRADED / SATURATED verdicts — lives in the numbered
reports under [`docs/reports/`](docs/reports/README.md) (ADRs in
[`docs/architecture/`](docs/architecture), design notes in
[`docs/design/`](docs/design)).

Measured headlines (release mode, in-process transport, one laptop):

- **Scale-invariant costs** — a one-row update (tx + OCC + commit) costs ~1 µs
  at both 100K and 10M rows; tick cost scales with active entities, not table
  size.
- **20K connection-only CCU PASS** — p99 well inside the 50 ms/tick budget.
- **~10K gameplay CCU** — realistic profiles hold p50 < 3 ms up to 10K clients;
  15–20K *gameplay* CCU is not yet claimed.
- **Memory** — ≈5.7 MB base + ≈24.7 KB private per connection (measured RSS).
- **WASM reducers** cost ~15× native per call; Phase 22's COW WriteSet still
  cut the fire-burst path 3.5×.

Reproduce:

```bash
# micro + scale benchmarks
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal
cargo run --release -p nexum-bench -- --scale 1_000_000

# CCU harness (profiles A=idle B=movement C=realistic E=extreme)
cargo run --release -p game-server --example ccu -- --clients 10000 --profile A --ticks 100
cargo run --release -p game-server --example ccu -- --clients 500 --profile C --ticks 100
```

## License

MIT — see [LICENSE](LICENSE).
