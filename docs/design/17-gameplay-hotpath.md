# Phase 17 — Gameplay Hot-Path & CCU Scaling: Design

Status: complete. Phase 17 removes the accidental O(N) work in the realistic
gameplay path identified at the end of Phase 16, re-benchmarks the CCU curve,
and documents the next bottleneck honestly.

## 1. Problem

Phase 16's realistic-gameplay benchmark saturated at **~500 clients**. The
root cause is **not** the storage engine (PK lookup ≈ 45 ns, single-row update
≈ 1 µs at 10M rows — Phase 15) but the **game workload itself**:

- every `move_player` call did `ctx.scan(TABLE)` (materialize **all** rows),
  a linear `find_player`, and a linear occupancy scan — O(N) per command;
- every other reducer (`player_join`, `player_leave`, `reload_weapon`,
  `respawn_player`, `take_damage`, `set_position`) did the same scan+find;
- the WASM `fire_weapon` module scanned the whole table twice per call.

Phase 16 baseline (this machine, release build, in-process transport):

| clients | profile | tick p50 | tick p95 | tick p99 | class |
|---|---|---|---|---|---|
| 500 | D (move every tick) | 83 ms | 266 ms | 324 ms | SATURATED |
| 1000 | D | 119 ms | 174 ms | 183 ms | SATURATED |

Tick budget: 50 ms @ 20 Hz.

## 2. Goal

Answer the Phase-17 question with measurements:

> How far can the EXISTING Nexum architecture go once accidental O(N)
> gameplay work is removed?

The ideal gameplay operation is:

```
client command
  → direct player lookup (primary key, O(log N))
  → transaction
  → OCC
  → atomic commit
  → minimal Change set
  → localized subscription/AOI fanout
```

## 3. Approach — smallest justified changes

### 3.1 Direct primary-key lookup (all native reducers)

`ctx.scan(TABLE)` + `find_player` → `ctx.lookup_unique(TABLE, "primary", …)`
+ `ctx.get`. The primary key already exists and is the proven O(log N) fast
path (Phase 15). This removes the full-table materialization and linear search
from every reducer.

### 3.2 Position index for cell queries (occupancy + combat target)

`move_player`'s "cell occupied?" check and `fire_weapon`'s "alive target at
aim cell" are **position queries**, not key queries. A full scan per command
is the same O(N) class the roadmap calls out. The simplest correct structure
is a **composite non-unique secondary index on `(x, y)`**:

- `TableSchema::builder().index("pos", &["x", "y"])` — already supported and
  transactionally maintained by the table (ADR-002 D5: indexes are derived,
  never authoritative).
- a new `Transaction::lookup_index(table, index, key)` — the non-unique
  counterpart of `lookup_unique`, with the identical pending-write overlay
  (hide deleted/updated-away committed owners, include pending writes) and
  table-epoch phantom observation (ADR-004 D12–D13);
- `ReducerContext::lookup_index` delegating to it;
- a new WASM ABI op `OP_LOOKUP_INDEX` (op 9) mirroring `OP_LOOKUP_UNIQUE`
  (same result envelope: `[count u64][row ids…]`).

Cost per cell query: O(log N + k) instead of O(N).

### 3.3 Recovery compatibility

Persisted worlds created before Phase 17 have the `players` schema **without**
the `pos` index. `ensure_schema` is idempotent and only creates the table when
missing, so a recovered table would lack the index and `lookup_index("pos")`
would fail. Fix: a small additive `TableStore::add_index(table, def)` /
`Table::add_index(def)` that builds the shell and populates it from a full
scan (one-time O(N) at factory time — indexes are derived and rebuildable,
same discipline as snapshot restore). `ensure_schema` adds the index when the
table exists without it.

### 3.4 WASM fire_weapon

Rewrite the module to the same discipline as the native reducers:

- shooter row: `OP_LOOKUP_UNIQUE` ("primary", caller) → row id → `OP_GET`;
- aim cell from the authoritative facing;
- target: `OP_LOOKUP_INDEX` ("pos", aim cell) → at most a handful of ids →
  `OP_GET` each, pick the alive non-self one (ids are ascending, deterministic);
- consume the shot, apply damage, emit `hit`/`kill` as before.

The client-visible contract (authoritative shooter identity, server-computed
aim/hit/damage, sandboxed decision) is unchanged.

### 3.5 What is NOT changed in Phase 17

- **The per-tick cooldown system** keeps its O(N)/tick scan: decrementing
  every alive player's cooldown is an inherent per-tick simulation cost (one
  scan per tick, not per command; ~100–200 µs at 20K rows). Documented.
- **Subscription fanout** (O(subscriptions x changes) per commit) is the
  **expected next bottleneck** and belongs to the interest-management phase
  (Phase 20). Phase 17 measures and documents it; it does not build AOI.
- The storage engine, transaction/OCC path, WAL, and subscription registry
  are untouched.

## 4. Determinism

- Index lookups return ascending `RowId`s; the occupancy check is a boolean
  and the target pick is "any alive non-self", so ordering never affects the
  outcome.
- The transaction overlay sorts and dedups owners exactly like `lookup_unique`.
- Same seed + inputs + reducer code ⇒ same authoritative state and
  `Vec<Change>`; the single-threaded path remains the correctness oracle.

## 5. Benchmark plan

Profiles (from the Phase 16 harness): A = connection-only, B = light input
(5 Hz moves), C = realistic (move every 3 ticks + occasional fire), D = stress
(move every tick + fire every 4th).

1. Before (Phase 16 baseline): D @ 500/1000 — **recorded above**.
2. After the fix: D @ 500/1000/2500/5000, C @ 1000/2500, B @ 5000.
3. Then 10K/15K/20K (profile A connection-only curve re-check + gameplay at
   the achievable ceiling).
4. Honest classification: PASS / DEGRADED / SATURATED / FAILED (ADR-016 D4).

## 6. Success criterion

The ~500-client gameplay ceiling must be removed and the remaining bottleneck
must be **identified by measurement** (expected: subscription fanout), with
the storage engine demonstrated not to be the limiter.
