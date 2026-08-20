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
- **unsafe_code = forbid** — no unsafe anywhere in the codebase

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
| 23 | Networking/transport & inbound batching | ⬜ Planned |

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
protocol/gateway/runtime/world/SDK, post-Phase-18: 8–16 partitions × 8–16
workers):**

| CCU (connection-only) | tick p99 | 50 ms budget | class |
|---|---|---|---|
| 1K | 2.8 ms | ✓ | PASS |
| 5K | 15.5 ms | ✓ | PASS |
| 10K | 12.1 ms | ✓ | **PASS** |
| 15K | 19.3 ms | ✓ | **PASS** |
| 20K | 32.0 ms | ✓ | **PASS** |

**Connection-only: 20K PASS** (Phase 16: 15K 63.7 ms and 20K 75.5 ms were
DEGRADED). **Gameplay: 10K movement DEGRADED** (p95 39 ms, p99 65 ms),
**15K movement SATURATED** (p95 59 ms, p99 92 ms) — bounded by the sum of
O(CCU) per-client work (inbound decode, world tick, gateway fan-out, SDK
decode/drain) plus the WASM fire burst (Phase 22), not by the multi-core
world tick (Phase 18). Phase 21 (Arc-shared frames + per-world attached
index) cut the fan-out phase 23–27% and movement p99 72.9 → 64.7 ms @ 10K.
15–20K *gameplay* CCU is NOT yet claimed. The harness also exposed and we
fixed real bugs: cross-client request-ID collision in the gateway (all SDK
clients start request ids at 1) and a gateway inbound O(N²) (per-call
`pending_calls` scans → per-connection index).

**Memory (measured RSS, profile A steady state):** ≈ 5.7 MB + **24.7 KB
private per connection** (10K 251 MB, 15K 376 MB, 20K 502 MB) — end-to-end
including the in-process SDK clients; a mass join storm without client
consumption spikes several× (20K peak 4.1 GB, O(N²) un-drained SDK
buffers, settles in ~2 s). Reproduce:

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

## Interest Management (Phase 20)

Phase 20 (see the [full report](docs/reports/20-interest-management.md))
replaced the all-to-all subscription fan-out with **duplicate-subscription
grouping**: one shared derived view per distinct query, evaluated once per
commit, fanned out to each member's buffer. Plus a **bounded TickUpdate**
(the broadcast no longer carries the full change list; clients receive
windowed subscription deltas). Measured at 1K (profile C, release):

- **Subscription evaluations per change: ~1,000 → 1.00** (the workload
  metric; 1M evaluations/tick → ~1K).
- **sub_apply: 11.4ms → 0.2ms/tick (57×)**, now 3.5% of tick.
- **Client decode: 4.0ms → 1.4ms**; p95 round-trip tick **204ms → 29ms**.
- CCU: **A@10K PASS** (p99 10.7ms), **B@1K PASS** (p99 31ms).

The remaining spikes are the WASM **fire burst** (1,000 simultaneous
`fire_weapon` calls re-instantiate wasmi per invocation — ~550ms
server-side at 1K) — the explicit Phase 22 target.

## Multi-Core Runtime (Phase 18)

Phase 18 (see the [full report](docs/reports/18-multi-core.md)) made the
runtime's tick phase **parallel across independent worlds/partitions**
(ADR-018): `worker_count` workers now tick worlds concurrently on scoped
threads, with outcomes merged in the deterministic `(worker_id, world_id)`
order — identical results to the serial path (the correctness oracle),
proven by exact trace-equality tests including cross-partition messaging.

Measured at 8K clients × 8 partitions (profile B, release):

| Workers | p95 (movement) | p99 | avg |
|--------:|---------------:|----:|----:|
| 1 | 62.3 ms | 103.6 ms | 23.5 ms |
| 4 | 37.3 ms | 62.5 ms | 16.3 ms |
| 8 | 31.7 ms | 52.4 ms | 15.2 ms |

World ticks drop from ~60 ms (serial) to ~24 ms (8 workers) on movement
ticks; scaling plateaus at 8 workers (only 8 worlds). The benchmark also
found and fixed a gateway inbound **O(N²)**: per-call `pending_calls`
scans → per-connection `BTreeSet` index — inbound 25.5ms → 2.3ms. The
remaining movement-tick cost is the O(clients) gateway fan-out / SDK
decode path (Phase 21) and the WASM fire burst (Phase 22).

## Networking Hot-Path (Phase 21)

Phase 21 (see the [full report](docs/reports/21-networking-hotpath.md),
[design](docs/design/21-networking-hotpath.md), [ADR-021](docs/architecture/21-networking-hotpath.md))
profiled the gateway/SDK delivery path with the real CCU harness and
shipped two measured optimizations (ADR-021):

- **D1 — `Arc<[u8]>` frames**: the transport's frame type is now an Arc.
  The per-world TickUpdate is encoded **once** per tick and delivered to
every attached session by refcount bump — zero per-client encode, zero
per-client copy (10K allocs/tick saved at 10K). One-off frames convert
via a single `Arc::from` (no copy).
- **D3 — per-world attached index**: the fan-out pass previously scanned
  all connections for each world twice (O(worlds × CCU) per tick); a
  per-world `BTreeSet` of attached connections makes the pass O(CCU).
  Maintained on attach/detach/disconnect; never authoritative.
- **D2 — per-connection batching: measured net-negative and reverted**
  (B@10K p95 44.6 vs 39.5 ms baseline) — per the phase rule, an
  optimization with no measured improvement is not kept.

Measured (release, 8–16 partitions × 8–16 workers, 20 Hz):

| Metric | before | after |
|---|---|---|
| idle fan-out @ 10K | 5.2 ms/tick | 4.2 ms/tick (−19%) |
| movement fan-out @ 10K (movement ticks) | ≈15–20 ms/tick | ≈12.6 ms/tick (−27%) |
| movement p99 @ 10K | 72.9 ms | 64.7 ms |
| movement p95/p99 @ 15K (16×16) | 64.6 / 97.6 ms | 59.0 / 92.4 ms |

654 workspace tests pass; `unsafe_code = forbid` maintained. The movement
tick remains bound by the **sum** of O(CCU) per-client work — the next
lever is reducing per-client work items (Phase 20 interest management
already cut subscription evaluations; Phase 22 WASM, Phase 23 inbound
batching), not making individual sends cheaper.

## Extreme Execution Profiling (Phase 21.5)

Phase 21.5 (see the [full report](docs/reports/21.5-extreme-profiling.md),
[design](docs/design/21.5-extreme-profiling.md)) is an **investigation
phase**: the entire authoritative pipeline was instrumented and measured
at phase, sub-phase, per-reducer, and allocation granularity — **no code
was optimized**. New instrumentation: per-reducer timing
(`--reducer-profile`, native vs WASM), a counting global allocator
(`nexum-alloc-count`, `--features ccu-alloc --count-alloc`), p99.9/max
reporting, worst-tick spike analysis, and Profile E (extreme gameplay:
move every tick + WASM fire 2/s + reload). Two measurement bugs were
found and fixed: `RuntimeMetrics.last_tick_profile` kept only the last
world's sub-phase times (under-reported ~N× at N partitions; now
aggregated across worlds), and the harness warmup never drained client
queues (an artificial tick-0 p99.9 spike; now drained).

Measured cost map (release, in-process transport, 20 Hz):

- **WASM `fire_weapon` = 65–69 µs/call ≈ 15× native** (`move_player`
  3.9–4.8 µs, `reload_weapon` 6.6 µs) — the measured Phase 22 target.
- **Connection-only: PASS at 20K** (p99 25 ms); no O(CCU²) remains;
  **p99.9 ≈ p99 in every profile** (no pathological tail — the only
  reproducible spike was the fixed warmup artifact).
- **Idle 20K = 21.6 ms avg tick**; **movement 10K DEGRADED / 15K
  SATURATED**; **extreme (E) 5K SATURATED at p99 1.09 s** — the fire
  burst (5K × ~68 µs ≈ 340 ms aggregate CPU every 10th tick) dominates.
- **Parallelism confirmed**: at E@5K the aggregate world-tick CPU is
  166.8 ms/tick vs 25.6 ms wall (8 worlds) — the Phase 18 runtime is
  doing real parallel work, the cost is per-world work, not serialization.
- **Allocation**: 4 allocs/client/tick idle → 43 movement → 397 extreme
  (WASM path dominates); churn scales with work, not table size.
- **Cross-partition traffic = 0** in every profile: the arena workload is
  partition-local — true horizontal sharding with independent worlds.

Reproduce:

```bash
# per-reducer + per-phase cost map
cargo run --release -p game-server --example ccu -- --clients 1000 --profile E --ticks 100 --profile-detail --reducer-profile
# allocation profile
cargo run --release -p game-server --example ccu --features ccu-alloc -- --clients 5000 --profile B --ticks 100 --count-alloc
```

## WASM & Transaction Overlay Optimization (Phase 22)

Phase 22 (see the [full report](docs/reports/22-wasm-hotpath.md),
[design](docs/design/22-wasm-hotpath.md)) discovered that the dominant
gameplay bottleneck was NOT WASM execution itself, but the **transaction
overlay path** that WASM host calls traverse. The isolated WASM cost
(~13 µs/call) was dwarfed by the per-call branch/absorb overhead under
burst load (~411 µs/call → 119 µs/call, **3.5× faster**).

Three structural changes to the transaction engine:

1. **COW WriteSet with Arc-based own layer** — `branch()` is now O(1)
   via `Arc::clone` instead of O(parent-writes) BTreeMap deep-copy
   (728× faster: 79 µs → 109 ns).
2. **`has_any_insert()` skip** — `lookup_unique`/`lookup_index` skip the
   O(N) pending-insert scan when no Insert entries exist (14× faster:
   315 µs → 22 µs per invoke).
3. **Absorb fast-path + `try_unwrap`** — for update-only workloads (no
   Deletes), skip the logical-view check and move entries instead of
   cloning.

Measured results (release, 8 workers × 8 partitions):

- **fire_weapon**: 65–69 µs → 47–56 µs/call (1.2–1.5×)
- **Profile C @ 1K p99**: ~573 ms → 57.5 ms (**10× faster**, now DEGRADED
  instead of SATURATED)
- **Profile E @ 1K p99**: ~1,094 ms → 71.9 ms (**15× faster**)
- **Harness loop total**: 411 µs → 119 µs (**3.5× faster**)

Current honest ceiling: realistic gameplay ~1K CCU (Profile C DEGRADED at
p99 = 57.5 ms, just above 50 ms budget). The remaining bottleneck is
subscription fan-out at higher CCU levels.

## License

MIT — see [LICENSE](LICENSE).
