# Phase 7 — Post-Migration Performance: Report

Status: complete. Two high-value optimizations were identified, implemented,
measured, and regression-tested after the Phase 1–5 architecture migration.

## 1. Environment

| | |
|---|---|
| CPU | Intel Core i7-14650HX (16 cores / 24 threads) |
| RAM | 16 GB DDR5 |
| OS | Linux |
| Build | release (fat LTO, codegen-units=1) |
| Transport | in-process (real gateway / runtime / world / subscriptions / SDK) |
| Tick rate | 20 Hz (budget 50 ms) |
| Harness | `cargo run --release -p game-server --example ccu` |

## 2. Methodology

- Baseline measured after Phase 5 migration (game-server consolidated into
  single `game.rs` module).
- Bottleneck identified via CCU harness `--profile-detail` phase breakdown
  and targeted instrumentation of `Partition::tick_with_calls`.
- Two optimizations implemented and measured independently.
- Full verification: `cargo fmt`, `cargo clippy -D warnings`, all tests pass.

## 3. Optimization 1: Position Index for Occupied-Cell Check

**Problem:** `move_player` used `ctx.scan(TABLE)?` to check if the target
cell was occupied — a **full table scan** of all players for every single
move call. With N players and ~N/3 moves per tick, the cost was O(N²).

**Fix:** Replace the full scan with `ctx.lookup_index(TABLE, POS_INDEX, ...)`
to find players at the target position in O(log N) time. Only the matching
rows are fetched via `ctx.get()`.

**Files changed:**
- `crates/game-server/src/game.rs` — `move_player` reducer: replaced
  `ctx.scan(TABLE)?` loop with `ctx.lookup_index` + per-row `ctx.get`.

## 4. Optimization 2: ReadSet Snapshot Watermark

**Problem:** Each reducer call in Phase 0c does `tx.snapshot()` which
clones the `ReadSet` BTreeMap. As the read set grows (more rows observed
per tick), each snapshot becomes increasingly expensive — O(N) per call,
O(N²) total for N calls per tick.

**Fix:** Changed `TxSnapshot` to store a **read-set watermark** (entry count
at snapshot time) instead of cloning the full BTreeMap. On rollback,
`truncate_to(watermark)` removes only the entries added after the snapshot
in O(delta) time. Insertion order is tracked via a separate `Vec`.

**Files changed:**
- `crates/nexum-tx/src/read_set.rs` — Added `insert_order: Vec` for
  truncation tracking, `entry_count()`, `truncate_to(len)`.
- `crates/nexum-tx/src/snapshot.rs` — Changed `reads: ReadSet` to
  `read_watermark: usize`.
- `crates/nexum-tx/src/transaction.rs` — `snapshot()` records length,
  `rollback()` truncates instead of replacing.

## 5. Before / After (1K clients, Profile C, 200 ticks)

| Metric | Before | After | Δ |
|--------|--------|-------|---|
| world_tick avg | 28.1 ms | 1.7 ms | **16.5x** |
| tick avg | 29.3 ms | 2.9 ms | **10x** |
| tick p50 | 1.1 ms | 1.1 ms | — |
| tick p95 | 90.7 ms | 5.2 ms | **17.5x** |
| tick p99 | 93.2 ms | 83.2 ms | 1.1x |
| Classification | DEGRADED | DEGRADED | ↑ improved |

The p99 spike is from the join storm (1000 clients connecting simultaneously)
which is inherent to the benchmark design, not to game logic.

## 6. Scalability (after optimization)

| CCU | world_tick avg | tick p50 | tick p99 | Classification |
|-----|---------------|----------|----------|----------------|
| 1K | 1.7 ms | 1.1 ms | 83 ms | DEGRADED |
| 5K | 27.9 ms | 3.1 ms | 1632 ms | SATURATED |

At 5K, the position index returns ~4 rows per cell on average (5000 players
in 1152 arena cells), so the per-call cost approaches the full-scan cost.
The remaining bottleneck is the O(changes × subscribers) subscription
evaluation and the O(CCU) gateway fan-out.

## 7. Correctness Validation

- `cargo fmt --all -- --check` — clean
- `cargo clippy --workspace --all-targets --all-features -- -D warnings` — clean
- `cargo test -p game-server` — 12/12 passed
- `cargo test -p nexum-tx` — 97/97 passed
- `cargo test -p nexum-integration-tests` — 3/3 passed
- No new warnings, no unsafe introduced

## 8. Remaining Bottlenecks

1. **O(changes × subscribers) subscription evaluation** — at 5K, each
   committed change is evaluated against all subscriber views.
2. **O(CCU) gateway fan-out** — pushing results to all subscribers is
   serial and dominates at high CCU.
3. **Dense arena saturation** — the 48×24 arena fits only 1152 cells;
   with 5K+ players, every cell is occupied and the index provides no
   benefit over a scan.
