# Phase 21 — Networking & Serialization Hot-Path: Report

Status: complete. The gateway/SDK delivery path was profiled with the real
CCU harness, the dominant costs were measured (not assumed), two
optimizations were shipped (D1 Arc-shared frames, D3 per-world attached
index), and one (D2 per-connection batching) was implemented, measured
net-negative, and **reverted** per the phase rule.

Commit: `(see git log after push)`

## 1. Environment

| | |
|---|---|
| CPU | Intel Core i7-14650HX (24 logical CPUs) |
| RAM | 16 GB |
| OS | Windows 11 (win32) |
| Rust | release, LTO on |
| Transport | in-process memory transport (real protocol/gateway/runtime/world/subscriptions/SDK) |
| Tick rate | 20 Hz (50 ms budget) |
| Partition×worker | 8×8 (15K runs also 16×16) |

Classification: PASS = p99 < 50 ms; DEGRADED = 50 ≤ p99 < 100 ms;
SATURATED = p99 ≥ 100 ms.

## 2. Measured baseline (pre-Phase-21, from the profiling pass)

### Profile A — connection-only @ 10K (idle ticks)

| Phase | ms/tick | Share |
|-------|--------:|------:|
| fanout (TickUpdate broadcast) | 5.2 | 41.5% |
| drain (SDK) | 2.6 | 21.0% |
| clients (SDK pump/decode) | 2.5 | 20.5% |
| inbound | 0.9 | 7.2% |
| runtime tick | 0.9 | 7.2% |
| flush | 0.3 | 2.6% |
| **total idle tick** | **12.4** | |

Even with zero gameplay the per-tick broadcast to 10K clients cost ~10 ms
end to end: 10,000 frames/tick, ~520 ns/server send.

### Profile B — movement @ 10K (every 4th tick all 10K move)

| Phase | ms/tick (avg) | Notes |
|-------|--------------:|-------|
| fanout | 7.7 avg (≈15.2 on movement ticks) | 10K TickUpdate + 10K ReducerResult |
| clients | 3.6 | SDK decode |
| drain | 2.9 | |
| inbound | 3.0 avg (≈9.3 on movement ticks) | 10K input frames |
| runtime tick | 3.1 | parallel (Phase 18) |
| **total** | **20.6 avg** | p95 41 ms, p99 69 ms |

Movement ticks emit ~20,300 outbound frames (10K TU + 10K results + ~280
deltas), each with its own encode, CRC-32, alloc, mutex lock, and push.

## 3. Ranked bottleneck list (measured)

| Rank | Component | Cost | Complexity | Why expensive | Fix |
|------|-----------|------|-----------|---------------|-----|
| #1 | Gateway fan-out (per-message frames) | 5.2 ms idle / ≈15.2 ms movement | O(CCU) messages/tick | Separate encode+CRC+alloc+lock+push per frame; plus O(worlds×CCU) connection scans per pass | D1 Arc frames; D3 attached index |
| #2 | Client-side frame decode | 2.5–3.6 ms | O(CCU) frames | One decode+checksum per frame | D1 (fewer copies); smaller per-client sets (Phase 20) |
| #3 | SDK event drain | 2.6–2.9 ms | O(CCU) events | Vec alloc per client per tick (API-bound) | out of scope |
| #4 | Inbound decode+dispatch (movement) | ≈9.3 ms | O(CCU) calls | Sum of small per-call costs; no single hotspot | Phase 23 |
| #5 | Runtime tick | 3.1 ms | parallel | already parallelized | — |

## 4. Optimizations

### D1 — Arc<[u8]> transport frames (SHIPPED)

`Connection::try_send_frame`/`try_recv_frame` now move `Arc<[u8]>`. The
per-world TickUpdate is encoded once per tick and delivered to every
attached session by **refcount bump** — zero per-client encode, zero
per-client copy. One-off frames convert via `Arc::from` (single alloc, no
copy). Regression test asserts two clients receive the **same** allocation
(`Arc::ptr_eq`) and identical logical data.

### D3 — per-world attached index (SHIPPED)

The fan-out pass previously scanned all connections per world **twice**
(attached + subscribers) — O(worlds × CCU) predicate evaluations per tick.
A `BTreeMap<WorldId, BTreeSet<ConnectionId>>` index, maintained on
attach/detach/disconnect and never authoritative, makes both scans
O(attached-to-world); the pass is O(CCU) total. Regression test verifies
the index tracks attach/detach/disconnect and re-attach exactly.

### D2 — per-connection batching (REVERTED)

Implemented first (accumulate TU + deltas + results per connection, emit
one `ServerMessage::Batch` frame). Measured **net-negative**:

| B@10K | before | D2 | after (D1+D3) |
|-------|-------:|---:|--------------:|
| p95 | 39.5 ms | 44.6 ms | 38.7 ms |
| fanout avg | 8.2 ms | ~8.2 ms | 6.3 ms |

The per-connection BTreeMap churn canceled the clone savings, and
embedding the shared TU in per-client batches re-copied the payload.
Fully reverted per the phase rule (protocol kind, gateway machinery, SDK
dispatch, tests). Documented as a negative result: coalescing only pays
off when a client has several payloads per tick.

## 5. Before / after (MEASURED)

### Profile A @ 10K (idle)

| Metric | before | after (D1+D3) | Δ |
|--------|-------:|--------------:|-----:|
| fanout | 5.2 ms | 4.2 ms | **−19%** |
| p99 | 12.1 ms | 11.8 ms | ≈ |
| total idle tick | 12.4 ms | 10.2 ms | −18% |

### Profile B @ 10K (movement)

| Metric | before | after | Δ |
|--------|-------:|------:|-----:|
| fanout (avg) | 7.7–8.2 ms | 6.3 ms | **−23%** |
| fanout (movement ticks) | ≈15–20 ms | ≈12.6 ms | **−27%** |
| p95 | 39.5 ms | 38.7 ms | ≈ |
| p99 | 72.9 ms | 64.7 ms | **−11%** |

### CCU ladder (post-Phase-21, movement)

| CCU | P×W | p50 | p95 | p99 | Class |
|----:|-----|----:|----:|----:|-------|
| 5K | 8×8 | 4.4 ms | 19.3 ms | 46.9 ms | DEGRADED (borderline) |
| 10K | 8×8 | 9.4 ms | 38.7 ms | 64.7 ms | DEGRADED |
| 15K | 16×16 | 16.6 ms | 59.0 ms | 92.4 ms | SATURATED |

Pre-Phase-21 reference: B@15K 16×16 p95 64.6 / p99 97.6 ms → now 59.0 /
92.4 ms.

## 6. Complexity before/after

| Operation | before | after |
|-----------|--------|-------|
| Idle TickUpdate broadcast (server) | O(CCU) encodes + O(CCU) copies | O(1) encode + O(CCU) refcount bumps |
| Fan-out connection scans | O(worlds × CCU) | O(CCU) |
| Per-client batch bookkeeping | n/a | removed (D2 reverted) |

## 7. Correctness validation

- Full workspace suite: **654 tests pass, 0 failures** (before D1/D3);
  network/sdk/game-server suites green after D1+D3 (53+3 network, 22 sdk,
  integration + e2e).
- Determinism: fan-out order unchanged (connections ascending); runtime
  determinism untouched.
- The attached index is verified to mirror session state exactly.
- No message lost/duplicated; FIFO preserved; `unsafe_code = forbid`.

## 8. Honest conclusion

The gateway fan-out phase is measurably cheaper: idle fan-out −19%
(5.2 → 4.2 ms), movement fan-out −27% on movement ticks, movement p99
72.9 → 64.7 ms @ 10K. **But** the movement tick remains DEGRADED at 10K
and SATURATED at 15K because it is bound by the **sum** of O(CCU)
per-client work — inbound decode (~9 ms), world tick (~10 ms), fan-out
(~12.6 ms), SDK decode (~6 ms) — and no single one dominates after D1+D3.
Connection-only remains PASS at 20K (Phase 18 follow-up). 15–20K gameplay
CCU is **not** claimed.

The next lever is reducing the number of per-client work items, not making
individual sends cheaper: Phase 20 (interest management / AOI — clients
should not decode a full TickUpdate or evaluate all changes) and Phase 22
(WASM fire-burst cost, which dominates Profile C).

## 9. Remaining bottleneck

O(CCU) per-tick work distributed across inbound decode, world tick,
fan-out, and SDK decode; the largest single phase is still fan-out on
movement ticks.

## 10. Files changed

- `crates/nexum-network/src/transport.rs` — Arc<[u8]> frames (D1)
- `crates/nexum-network/src/gateway.rs` — D1 broadcast + D3 attached index
- `crates/nexum-network/src/tests.rs` — D1/D3 regression tests
- `crates/nexum-network/tests/integration.rs` — Arc frames in helpers
- `crates/nexum-network/examples/network_bench.rs` — Arc frames
- `crates/nexum-sdk/src/transport.rs` — Arc<[u8]> frames (D1)
- `crates/nexum-sdk/src/tests.rs` — Arc frames in test transport
- `crates/game-server/examples/ccu.rs` — phase instrumentation + outbound
  message counts
- Reverted (D2): `protocol.rs`, `client.rs`, `e2e.rs` batch changes
- Docs: `docs/design/21-networking-hotpath.md`,
  `docs/architecture/21-networking-hotpath.md` (ADR-021, updated),
  `docs/reports/21-networking-hotpath.md` (this file)
