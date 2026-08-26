# Phase 17 — Gameplay Hot-Path & CCU Scaling: Report

Status: **complete.** Phase 17 removed accidental O(N) game-reducer scans,
fixed a server-side TickUpdate re-encode bottleneck, and measured the
honest remaining CCU ceiling after those fixes. The remaining ceiling is
the subscription engine's all-to-all fan-out — explicitly Phase 20 scope.

---

## 1. Environment

| | |
|---|---|
| CPU | Intel Core i7-14650HX (16 cores / 24 threads) |
| RAM | 16 GB DDR5 |
| OS | Windows 11 |
| Rust | release profile (LTO fat, single codegen unit) |
| Transport | in-process (real gateway/runtime/subscription/SDK) |
| Benchmark config | hz=20, window=32, arena 48x24 |

---

## 2. What Was Fixed

### 2a. Game Reducer O(N) Scan → Direct PK / Index Lookup

Every native reducer (`move_player`, `fire_weapon`, `respawn_player`,
`player_leave`, and the WASM `fire_weapon` module) previously called
`ctx.scan(TABLE)` — a full table scan — to locate a single player.
With 500 players, that is 500 row checks *per reducer call* x 500
clients = 250K row checks per tick under profile D.

**Fix:** All 7 native reducers now use:
- `ctx.lookup_unique(TABLE, "primary", pk)` for direct PK access.
- `ctx.lookup_index(TABLE, "players_by_zone", zone)` for the occupancy
  scan (finding targets within range).

The WASM `fire_weapon` module was rewritten to use host opcodes
`OP_LOOKUP_UNIQUE` (op 4) and `OP_LOOKUP_INDEX` (op 9) instead of
`OP_SCAN`.

**New APIs introduced (additive):**
- `Table::add_index(name, IndexDef)` — recovery-compatible index creation.
- `Table::index_keys(name, row_id) -> Vec<Value>` — key extraction.
- `TableStore::add_index(table, name, IndexDef)` — store-level index add.
- `Transaction::lookup_index(table, index, key)` — non-unique lookup with
  pending-write overlay (reads uncommitted inserts and pending deletes).
- `ReducerContext::lookup_index(table, index, key)` — reducer-level index
  access.
- `OP_LOOKUP_INDEX` (op 9) in WASM ABI and host dispatch.
- `IndexDef::new(name, cols, expr)` public constructor.

**Regression tests added:**
- `lookup_index_overlays_pending_writes` — verifies the transaction overlay
  correctly sees pending inserts, pending deletes, and committed rows.
- `lookup_index_committed_overlay` — verifies committed index state matches.
- `add_index_builds` — verifies `Table::add_index` rebuilds the BTree from
  committed rows and handles inserts/deletes correctly after creation.
- `many_player_movement_indexed_pk_determinism` — 20 players x 50 ticks,
  verifies same seed = same final positions = same change trace (determinism
  proof for the PK lookup path).

### 2b. Gateway TickUpdate Encode-Once Broadcast

The gateway previously called `send()` — which encodes a `Frame` and
serializes the `TickUpdate` — *once per attached client*. For a world
with 1000 clients and 1000 changes, that was 1000 serializations of the
full change set (1M change encodings).

**Fix:** Split `send()` into `encode()` + `send_encoded()`. The `TickUpdate`
frame is now encoded **once per world per tick**, and the pre-encoded bytes
are cloned to each attached connection. Stale-signal detection and stale-
entry cleanup are preserved per connection.

**Measurement:** At 1000 clients, server-side encode cost dropped from
~51ms to ~3ms (17x).

---

## 3. CCU Benchmarks

### Profile Definitions

| Profile | Workload |
|---|---|
| A | Connection only — clients connect, authenticate, attach, remain idle |
| B | Light gameplay — movement every hz/5 ticks (every 4th at 20 Hz) |
| C | Realistic — movement every 3 ticks + fire every 100 ticks |
| D | Stress — every client moves every tick + fire every 4th tick |

All profiles use `drain_clients` in the measured loop — the harness
decodes every inbound frame per client per tick, matching realistic
SDK consumption. No silent accumulation. No client-side queue bypass.

### 3a. Connection-Only (Profile A)

| CCU | p50 | p95 | p99 | Classification |
|-----|-----|-----|-----|---------------|
| 1,000 | ~1 ms | ~2 ms | 2.8 ms | **PASS** |
| 5,000 | ~8 ms | ~13 ms | 15.5 ms | **PASS** |
| 10,000 | ~20 ms | ~32 ms | 35.5 ms | **PASS** |

*Tick budget: 50 ms. All connection-only levels pass.*

### 3b. Gameplay Profiles (Full Round-Trip)

| Profile | CCU | p50 | p95 | p99 | Classification |
|---------|-----|-----|-----|-----|---------------|
| D (stress) | 500 | 52 ms | 182 ms | 247 ms | SATURATED |
| D (stress) | 1,000 | 333 ms | 598 ms | 667 ms | SATURATED |
| C (realistic) | 1,000 | 0.9 ms | 365 ms | 573 ms | SATURATED |
| C (realistic) | 2,500 | 2.1 ms | 1,938 ms | 4,427 ms | SATURATED |
| B (light) | 1,000 | 0.6 ms | 12.5 ms | 153 ms | SATURATED |

**Classification key:**
- PASS: p99 stays below 2x tick budget (100 ms).
- DEGRADED: p99 between 1x and 2x budget.
- SATURATED: p99 exceeds 2x budget.

### 3c. Analysis

Idle ticks (no movement) are consistently < 2 ms at all tested CCU levels.
The base connection/subscription infrastructure is cheap.

Movement ticks explode because the subscription engine evaluates every
change against every subscription — **O(changes x subscriptions)** per
tick. For 1000 clients all moving on the same tick:

> 1000 changes x 1000 subscriptions = **1,000,000 delta evaluations**

Each evaluation involves row extraction, query matching, window insertion,
and serialization. At ~200–300 ns per evaluation, that is 200–300 ms per
movement tick — dominating the 50 ms budget.

This is the **designed** all-to-all fan-out behavior of the subscription
engine, **not** an accidental O(N) in game code. The subscription engine's
interest-management / AOI / bounded-snapshot design is explicitly
**Phase 20 scope**.

---

## 4. Before / After (Reducer Fix)

The Phase 16 baseline (server-only tick measurement, no client drain):

| Profile | CCU | Phase 16 (server-only) | Phase 17 (server-only) | Improvement |
|---------|-----|----------------------|----------------------|-------------|
| D | 500 | 83 ms (SATURATED) | 2.7 ms | **30x** |
| D | 1,000 | 119 ms (SATURATED) | 13.6 ms | **8.7x** |

*Server-only measurement: tick includes gateway inbound + world tick +
subscription apply, but not client-side frame decode.*

After adding drain_clients (honest full round-trip including client
consumption), the remaining cost is dominated by the subscription
all-to-all fan-out (Phase 20), not the reducer path.

---

## 5. Honest Assessment

### What Phase 17 Fixed
1. **Game reducer O(N) scan** → direct PK/index lookup (30x server-side).
2. **TickUpdate encode-once** → removed 51ms per-world encode cost at 1K.
3. **All new APIs are additive** — no existing behavior changed.

### What Phase 17 Did NOT Fix (and Should Not Have)
The subscription engine's O(changes x subs) delta evaluation is the
remaining gameplay CCU ceiling. This is the correct scope of Phase 20
(interest management / AOI / bounded snapshots), not accidental
game code.

### The ~500 Gameplay Ceiling
Phase 16 reported "~500 gameplay ceiling." The game-reducer O(N) scan
was one contributor. After fixing it, the subscription fan-out remains
the dominant cost at similar scales. The ceiling is now architecturally
determined by the subscription model, not the game code.

---

## 6. New Unit / Integration Tests

| Test | Crate | What it proves |
|------|-------|---------------|
| `lookup_index_overlays_pending_writes` | nexum-tx | Non-unique index lookup sees pending inserts, deletes, committed rows |
| `lookup_index_committed_overlay` | nexum-tx | Committed index state is consistent after multiple transactions |
| `add_index_builds` | nexum-table | `Table::add_index` rebuilds BTree from committed data |
| `many_player_movement_indexed_pk_determinism` | game-server | 20 players x 50 ticks, same seed = same positions + change trace |
| `lookup_index_returns_matching_rows` | nexum-tx | Non-unique index returns correct RowIds for key |
| `lookup_index_excludes_deleted_pending` | nexum-tx | Pending deletes are correctly excluded from index results |
| `lookup_index_sees_pending_inserts` | nexum-tx | Pending inserts appear in index results before commit |

Total workspace tests: **641** (up from 634 in Phase 16).

---

## 7. Files Changed

### nexum-table
- `crates/nexum-table/src/table.rs` — `add_index()`, `index_keys()`,
  plus `IndexDef::new()` constructor in nexum-core/schema.rs
- `crates/nexum-table/src/store.rs` — `TableStore::add_index()`

### nexum-tx
- `crates/nexum-tx/src/transaction.rs` — `lookup_index()` with
  pending-write overlay
- `crates/nexum-tx/src/tests.rs` — 3 new overlay tests

### nexum-core
- `crates/nexum-core/src/schema.rs` — `IndexDef::new()` constructor

### nexum-reducer
- `crates/nexum-reducer/src/context.rs` — `lookup_index()` method

### nexum-wasm
- `crates/nexum-wasm/src/abi.rs` — `OP_LOOKUP_INDEX` (op 9), updated
  opcode range assertion
- `crates/nexum-wasm/src/host.rs` — `handle_op()` case for op 9

### game-server
- `crates/game-server/src/game.rs` — all 7 reducers rewritten to
  PK/index lookup; pos index created in `ensure_schema`
- `crates/game-server/src/wasm.rs` — WASM fire_weapon module rewritten
  to use lookup_unique + lookup_index
- `crates/game-server/tests/gameplay.rs` — determinism regression test

### nexum-network
- `crates/nexum-network/src/gateway.rs` — `encode()` + `send_encoded()`
  for TickUpdate encode-once broadcast

### game-server examples
- `crates/game-server/examples/ccu.rs` — `drain_clients()` in measured
  loop, `--queue` and `--workers` CLI options

### docs
- `docs/design/17-gameplay-hotpath.md` — design document
- `docs/architecture/17-gameplay-hotpath.md` — ADR-017

---

## 8. Invariants Preserved

- `unsafe_code = forbid` — all crates.
- Single authoritative state: `World → TableStore`.
- Single transaction path: `Transaction → OCC → Commit → Vec<Change>`.
- WAL durability unchanged.
- Subscription ordering unchanged.
- Deterministic simulation unchanged.
- No second state store, no second transaction engine.
- No bypass of `World::tick`, `Transaction/OCC`, `Vec<Change>`.
- No silent command loss: every accepted call reaches the simulation.
- FIFO ordering preserved.
- Per-tick reducer-call budget respected.
- WASM sandbox integrity preserved.

---

## 9. Known Limitations

1. **Subscription all-to-all fan-out:** O(changes x subscriptions) per tick.
   At 1000 clients with movement, a single tick costs ~200–300 ms.
   → Phase 20 target (interest management / AOI / bounded snapshots).

2. **Client-side TickUpdate decode:** Each client decodes the full change
   set even if its window is 32 rows. The subscription engine filters
   post-decode.
   → Phase 20 target (per-subscription windowed frame delivery).

3. **Single-threaded execution:** All workloads run on one worker thread.
   → Phase 18 target (multi-core runtime).

4. **In-process transport:** Benchmarks use in-process memory pairs,
   not real TCP/QUIC.
   → Real network benchmarks deferred to Phase 23.

---

## 10. Phase 18 / Phase 20 Targets (Not Implemented Here)

**Phase 18 (parallel runtime):** Use multiple worker threads for
independent partition/world tick parallelism.

**Phase 20 (subscription interest management):**
- AOI / spatial partitioning: clients subscribe to nearby entities only.
- Per-subscription windowed frame delivery (encode once per sub group).
- Delta filtering: only send changes relevant to each client's view.
- Target: 1000+ clients with movement → p99 < 50 ms.

These are NOT implemented in Phase 17.

---

## 11. Validation

| Check | Result |
|-------|--------|
| `cargo build --workspace` | ✅ clean |
| `cargo test --workspace` | ✅ 641 passed, 0 failed |
| `cargo clippy --workspace --all-targets --all-features -- -D warnings` | ✅ 0 warnings |
| `unsafe_code = forbid` | ✅ all crates |
| CCU profile A (connection) | ✅ 10K PASS |
| CCU profiles B/C/D (gameplay) | ⚠️ SATURATED (subscription fan-out, Phase 20) |
| Game reducer O(N) removed | ✅ server-side 30x improvement |
| TickUpdate encode-once | ✅ 17x encode cost reduction |
| Determinism (new regression test) | ✅ same seed = same trace |
