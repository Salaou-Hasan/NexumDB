# Nexum â€” Authoritative State Engine for Multiplayer Games & Simulation

[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)
[![Rust](https://img.shields.io/badge/rust-2024-orange.svg)](https://www.rust-lang.org/)

**Nexum** is an authoritative state engine for realtime multiplayer games and simulation backends, written in Rust. It provides OCC transactions, WASM-sandboxed reducers, deterministic simulation, reactive subscriptions, and a versioned binary protocol â€” all in a single-node, memory-first architecture.

> **State is authoritative. Transactions change state. Simulation produces state transitions. Subscriptions expose state.**

Nexum is inspired by SpacetimeDB, Nakama, relational databases, and simulation engines â€” but it is **not** a clone of any of them. It is a new architecture designed for the specific demands of authoritative multiplayer state synchronization.

### Key features

- **Authoritative state** â€” one source of truth; clients send intents, never positions
- **OCC transactions** â€” optimistic concurrency control with read/write set validation
- **WASM reducers** â€” sandboxed, deterministic gameplay logic with host-call ABI
- **Deterministic simulation** â€” reproducible ticks, parallel world execution, partition sharding
- **Reactive subscriptions** â€” committed-change observation with bounded queries and resync
- **Realtime protocol** â€” versioned binary frames, sessions, auth, gateway, client SDK
- **20K CCU realistic gameplay within tick budget** - measured across six game-genre workloads on one node
- **10K CCU any genre with server p99 < 15 ms** - deterministic, zero loss
- **Minimal, contained unsafe** â€” the workspace lint allows `unsafe_code`, but unsafe exists only in three audited modules (the lock-free SPSC transport ring, the WASM linker cache, and the `nexum-alloc-count` counting allocator); avoid adding new unsafe without strong justification

## Why Nexum?

Most multiplayer backends separate the database from the simulation. Nexum **unifies** them: the
authoritative state store **is** the simulation. There is no data drift, no reconciliation, no
catch-up sync. Every tick produces deterministic state transitions that are immediately visible to
subscribers.

**Compared to SpacetimeDB:** Nexum is Rust-native with a custom WASM reducer sandbox, OCC
transactions, and a simulation-first architecture.

**Compared to Nakama:** Nexum is not a general-purpose game server. It is a state engine where
the simulation IS the database. No Lua/JS runtime â€” just authoritative state at extreme scale.

**Compared to writing your own:** Nexum provides the hard parts out of the box: OCC, WAL,
determinism, WASM sandboxing, subscriptions, and a realtime protocol. You write reducers; Nexum
makes them fast, durable, and concurrent.

## The three primitives

1. **Tables** â€” The authoritative state of the application/world. Schemas, typed columns, rows,
   primary keys, indexes, constraints, and version metadata.
2. **Reducers** â€” Authoritative state transitions. Each reducer executes inside a transaction
   (read â†’ execute â†’ write â†’ validate â†’ commit).
3. **Subscriptions** â€” Reactive views over authoritative table state, driven by committed change
   sets â€” never by polling.

Everything else â€” OCC transactions, the memory-first storage engine, WAL + snapshots, the WASM
reducer sandbox, the simulation scheduler, the partition/worker runtime, and the realtime protocol â€”
exists to make those three primitives reliable, deterministic, transactional, durable, scalable, and
extremely fast.

## Implementation status

The build follows a strict order: correctness first, distribution last.

| Phase | Area | Status |
|-------|------|--------|
| 0 | Repository / workspace foundation | âœ… Done |
| 1 | Core types and state model | âœ… Done |
| 2 | Table system | âœ… Done |
| 3 | Memory-first storage engine | âœ… Done |
| 4 | Transaction engine (OCC) | âœ… Done |
| 4c | Read-your-writes + phantom protection | âœ… Done |
| 5 | WAL, snapshots, recovery | âœ… Done |
| 6 | Reducer API | âœ… Done |
| 7 | WASM reducer runtime | âœ… Done |
| 8 | Subscription engine | âœ… Done |
| 9 | Deterministic simulation core | âœ… Done |
| 10 | Nexum runtime (partition/worker orchestration) | âœ… Done |
| 11 | Concurrency & parallel tick execution | âœ… Done |
| 12 | Multi-partition simulation (deterministic message bus) | âœ… Done |
| 13 | Networking + client SDKs (realtime gateway, reducer calls, `nexum-sdk`) | âœ… Done |
| 14 | Game server layer (games, players, exposure, command routing) | âœ… Done |
| 15 | Performance & benchmarking (100Kâ†’10M rows, bottleneck fixes, report) | âœ… Done |
| 16 | Production hardening & release | âœ… Done |
| 17 | Gameplay hot-path & CCU scaling (O(N) reducer scans â†’ direct lookup) | âœ… Done |
| 18 | Multi-core runtime (parallel world/partition ticks) | âœ… Done |
| 19 | Execution hot-path profiling (ranked bottlenecks, Arc-shared rows) | âœ… Done |
| 20 | Interest management / AOI (duplicate-subscription grouping) | âœ… Done |
| 21 | Networking & serialization hot-path (Arc frames, attached index) | âœ… Done |
| 21.5 | Extreme execution profiling (per-reducer/allocation cost map, spike analysis) | âœ… Done |
| 22 | WASM reducer optimization (COW WriteSet, has_any_insert skip, absorb fast-path) | âœ… Done |
| 22.5 | Networking hot-path (Arc rows, delta batching, incremental CRC, pump removal) | âœ… Done |
| 23â€“25 | Performance campaign (rayon pool, zero-copy deltas, atomic fast-paths) | âœ… Done |

## Repository layout

```
crates/
â”œâ”€â”€ nexum-core/         # Types & state model: ids, versions, timestamps, errors, interfaces
â”œâ”€â”€ nexum-table/        # Typed table engine (Phase 2)
â”œâ”€â”€ nexum-tx/           # OCC transaction engine (Phase 4 + read-your-writes/phantom fix)
â”œâ”€â”€ nexum-storage/      # Memory-first authoritative storage (Phase 3)
â”œâ”€â”€ nexum-wal/          # WAL, snapshots, recovery (Phase 5)
â”œâ”€â”€ nexum-reducer/      # Reducer execution model (Phase 6)
â”œâ”€â”€ nexum-wasm/         # WASM reducer sandbox (Phase 7): wasmi host, fuel/memory limits, ABI
â”œâ”€â”€ nexum-subscription/ # Subscription engine (Phase 8): committed-change observation, bounded queries, resync
â”œâ”€â”€ nexum-simulation/   # Deterministic simulation (Phases 9/11/12): one World = one partition, one tx per tick, systems/schedule/RNG, deterministic parallel ticks (Phase 11), cross-partition messaging (Phase 12)
â”œâ”€â”€ nexum-runtime/      # Runtime + partitions (Phases 10/12): world lifecycle, ownership, input routing, WAL+subscription coordination, recovery, the partition registry and deterministic message bus
â”œâ”€â”€ nexum-network/      # Realtime networking + control plane (Phase 13): versioned binary protocol, sessions/auth, gateway, transports, reducer-call routing, typed operator API (originally implemented ahead of the roadmap; now the canonical Phase 13 foundation)
â”œâ”€â”€ nexum-sdk/          # Client SDK (Phase 13): poll-driven `Client`, canonical protocol codec, sessions, correlated reducer calls, derived subscription views, reconnect/resync
â”œâ”€â”€ nexum-game-server/  # Game server layer (Phase 14): game instances, players, join/leave/reconnect, deny-by-default reducer exposure, per-world command buffering, failure observation â€” orchestration metadata only; gameplay state stays in the simulation
```

```
benchmarks/
â””â”€â”€ nexum-bench/        # Phase 15 benchmark crate (release-mode): micro + scale (100K/1M/5M/10M rows) + large-state tick; run `cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal` or `--scale 1_000_000`
â”œâ”€â”€ nexum-server/       # Server binary â€” reference demo of the full stack (no gameplay)
â””â”€â”€ game-server/        # The actual playable multiplayer arena game â€” real gameplay reducers (native + WASM), a TCP game server, and a terminal client over the real SDK (see below)
tests/                  # Workspace-level test harnesses (organized by area)
benchmarks/             # Benchmark harnesses
docs/
â”œâ”€â”€ architecture/       # Architectural decision records
â”œâ”€â”€ protocols/          # Wire protocol design
â””â”€â”€ design/             # Design notes
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
(GameServer â†’ Runtime â†’ World â†’ Transaction/OCC â†’ `Vec<Change>` â†’ WAL â†’
SubscriptionRegistry â†’ network â†’ SDK). It creates a two-partition game, joins
players, drives server-side commands and reducers (native **and** WASM), and
connects a real SDK client over an in-process transport that authenticates,
joins, attaches, subscribes, and observes committed changes as a derived view
â€” including a denied call to a server-only reducer (deny-by-default exposure).

```bash
cargo run -p nexum-server                # in-memory, 8 ticks
cargo run -p nexum-server -- --ticks 20  # more simulation
cargo run -p nexum-server -- --persist data  # WAL-durable run into ./data
```

## Playing the actual game

Three distinct layers (per the roadmap):

- `nexum-game-server` â€” the **reusable game-server framework** (game
  instances, players, exposure, routing). Contains no game mechanics.
- `nexum-server` â€” the **reference Nexum stack demo** (no gameplay).
- `game-server` â€” the **actual playable multiplayer arena game** built on
  Nexum: authoritative gameplay reducers (native `move_player`/`player_join`/
  `respawn_player`/â€¦ and a **WASM `fire_weapon`** reducer), a TCP game
  server, and a terminal client over the real SDK/network boundary.

The simulation is authoritative. The client sends **intents** (`move_player`
with a direction, `fire_weapon` with no target â€” the WASM reducer scans the
arena, validates facing/cooldown/ammo, resolves the hit and damage) and the
server decides the result. The client never sends positions, health, or
identity.

```bash
# Terminal 1 â€” the authoritative game server (1 partition, 20 ticks/s):
cargo run -p game-server -- server

# Terminals 2 and 3 â€” two real clients (each its own SDK + TCP connection):
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
reverted. The full campaign history â€” Phases 15â€“27, each with methodology
and honest PASS / DEGRADED / SATURATED verdicts â€” lives in the numbered
reports under [`docs/reports/`](docs/reports/README.md) (ADRs in
[`docs/architecture/`](docs/architecture), design notes in
[`docs/design/`](docs/design)).

### Simulation battery â€” validated multi-genre gameplay matrix

Phase 26â€“27 built a workload-independent benchmark suite with deterministic
player brains, six game-genre archetypes, and a CCU scaling ladder. Every run
is reproducible (`--seed`) and emits machine-readable scorecard lines.

**Server-only latency at 20 Hz, 20 lobbies, single node, all optimizations:**

| Workload | 1K CCU p50/p99 | 5K CCU p50/p99 | 10K CCU p50/p99 | 20K CCU p50/p99 |
|---|---|---|---|---|
| SOCIAL | 0.7 / 1.4 ms | 3.4 / 4.6 ms | 3.7 / 5.3 ms | **8.4 / 10.0 ms âœ“** |
| FPS (WASM combat) | 2.4 / 3.9 ms | 10.2 / 14.4 ms | **10.5 / 12.1 ms âœ“** | 29.5 / 32.9 ms |
| MMO (economy+social) | 2.1 / 3.7 ms | 11.7 / 16.0 ms | **12.8 / 14.6 ms âœ“** | 29.5 / 44.9 ms |
| SURVIVAL (RMW economy) | 2.1 / 3.3 ms | 11.3 / 14.1 ms | **11.2 / 12.7 ms âœ“** | 26.0 / 31.1 ms |
| EXTREME (stress ceiling) | 2.4 / 3.5 ms | 7.2 / 10.3 ms | 5.1 / 6.2 ms | 30.8 / 44.0 ms |

All workloads: zero failed ticks, zero rejected, zero dropped, deterministic.

**Key claims (all measured, not projected):**
- **10K CCU of any game genre on one node with server p99 < 15 ms.**
- **20K CCU of realistic gameplay within the 50 ms tick budget on one node.**
- WASM fire reduced from 994 Âµs to ~12 Âµs via instance pooling + composite ops.
- Movement stream processes 20K commands in 25.9 ms via batched scan-compute-writeback.

Reproduce:

```bash
# battery workload at specific CCU
cargo run --release -p game-server --example ccu -- --clients 10000 --workload FPS --ticks 120 --lobbies 20 --input-moves

# legacy profiles (A=idle B=movement C=realistic E=extreme)
cargo run --release -p game-server --example ccu -- --clients 10000 --profile C --ticks 100

# full battery matrix
foreach ($ccu in @(1000,5000,10000,20000)) { foreach ($wl in @("SOCIAL","FPS","MMO","SURVIVAL")) {
  cargo run --release -p game-server --example ccu -- --clients $ccu --workload $wl --ticks 120 --lobbies 20 --input-moves --csv } }
```

## License

MIT â€” see [LICENSE](LICENSE).
