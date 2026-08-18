# Phase 21 — Networking & Serialization Hot-Path

Status: complete. The network/SDK delivery path was profiled with the real
CCU harness (release, 20 Hz, 8 partitions × 8 workers, in-process
transport), a ranked bottleneck list was produced, and the two measured
dominant costs were reduced without changing authoritative semantics.

## 1. Methodology

The existing CCU harness exercises the **real** production path end to end:
Gateway (encode/decode, dispatch, policy) → Runtime → World →
Transaction/OCC → `Vec<Change>` → Subscription registry → Gateway fan-out →
transport → SDK pump/decode → SDK view/drain. The harness times each phase
separately (`inbound`, `tick`, `fanout`, `flush`, `clients`, `drain`) plus
the runtime's `world_tick`/`wal`/`sub_apply` sub-phases.

All measurements: release build, `Intel Core i7-14650HX`, Windows 11,
20 Hz tick budget (50 ms).

## 2. Measured baseline (before Phase 21)

### Profile A — connection-only @ 10K clients (idle ticks)

| Phase | ms/tick | Share |
|-------|--------:|------:|
| fanout (TickUpdate broadcast) | 5.2 | 41.5% |
| drain (SDK take_events) | 2.6 | 21.0% |
| clients (SDK pump/decode) | 2.5 | 20.5% |
| inbound | 0.9 | 7.2% |
| runtime tick | 0.9 | 7.2% |
| flush | 0.3 | 2.6% |
| **total idle tick** | **12.4** | |

Even with **zero gameplay**, the per-tick TickUpdate broadcast to 10K
clients costs ~10 ms end to end (5.2 server send + 2.5 client decode +
2.6 drain): 10,000 frames/tick, ~520 ns per server send, ~250 ns per
client decode, ~260 ns per drain.

### Profile B — movement @ 10K clients (every 4th tick all 10K move)

| Phase | ms/tick (avg) | Notes |
|-------|--------------:|-------|
| fanout | 7.7 avg (≈15.2 on movement ticks) | 10K TickUpdate + 10K ReducerResult |
| clients | 3.6 | |
| drain | 2.9 | |
| inbound | 3.0 avg (≈9.3 on movement ticks) | 10K input frames decoded |
| runtime tick | 3.1 | parallel world ticks (Phase 18) |
| **total** | **20.6 avg** | p95 41 ms, p99 69 ms |

Message counts per movement tick (measured via gateway metrics):
**10,000 TickUpdates + 10,000 ReducerResults + ~280 subscription deltas ≈
20,300 individual outbound frames/tick**, each with its own encode, frame
alloc, CRC-32, mutex lock, and queue push.

## 3. Ranked bottleneck list

| Rank | Component | Cost | Complexity | Why expensive | Fix |
|------|-----------|------|-----------|---------------|-----|
| #1 | Gateway fan-out (per-message frames) | 5.2 ms idle / ≈15.2 ms movement | O(CCU) messages/tick | Every TickUpdate, delta, and reducer result is a separate frame: separate encode + CRC-32 + alloc + mutex lock + queue push | D2 batch per connection; D1 shared Arc frames |
| #2 | Client-side frame decode | 2.5–3.6 ms | O(CCU) frames | One `decode_server` + checksum per frame | Fewer frames via D2 |
| #3 | SDK event drain | 2.6–2.9 ms | O(CCU) events | Vec alloc per client per tick (API-bound) | out of scope (consumption cost) |
| #4 | Inbound decode+dispatch (movement) | ≈9.3 ms movement | O(CCU) calls | Sum of small per-call costs; no single hotspot | Phase 23 (frame batching) |
| #5 | Runtime tick | 3.1 ms | parallel | Already parallelized (Phase 18) | — |

## 4. Selected optimizations

### D1 — Arc-shared broadcast frames (ADR-021 D1) — SHIPPED

The transport's frame type becomes `Arc<[u8]>`. The per-world TickUpdate
is encoded **once** per tick and every attached client receives a refcount
bump instead of a fresh `Vec<u8>` clone + memcpy (10,000 allocs/tick saved
at 10K). One-off frames (results, deltas, handshake…) convert with a
single `Arc::from` allocation — no copy, identical cost to today. The SDK
decodes from `&frame[..]` with zero extra copies. `unsafe_code = forbid` is
preserved (Arc is safe).

### D3 — per-world attached index (ADR-021 D3) — SHIPPED

The fan-out pass previously scanned **all** connections for **each** world
twice (once for the TickUpdate broadcast, once for subscribers):
O(worlds × CCU) predicate evaluations per tick. A `BTreeMap<WorldId,
BTreeSet<ConnectionId>>` index, maintained on attach / detach / disconnect
and never authoritative, makes both scans O(attached to that world) — the
pass is O(CCU) total. The index is a delivery optimization only; the
sessions' own `attached_world` remains the source of truth (regression
test verifies they never diverge).

### D2 — per-connection outbound batching (ADR-021 D2) — REVERTED

D2 was implemented first (accumulate a connection's TickUpdate + deltas +
results during the pass, emit as one `ServerMessage::Batch` frame), but the
before/after measurement was **net-negative**: the per-connection
`BTreeMap` bookkeeping (a node alloc + BTreeMap insert/remove per client
per tick) canceled the clone savings, and merging the TickUpdate into a
per-client batch frame re-copied the shared TU payload once per client,
losing D1's zero-copy broadcast. Profile B @ 10K measured **p95 44.6 ms vs
39.5 ms baseline** (worse), idle flat. Per the phase rule — *revert
optimizations with no measured improvement* — D2 was fully reverted
(protocol `KIND_BATCH`, gateway batch machinery, SDK dispatch arm, and
their tests). It remains a documented negative result: frame coalescing
only pays off when a client has several payloads per tick (multiple
subscriptions/results), which the current workload does not produce.

## 5. Correctness invariants

- One authoritative state / transaction / `Vec<Change>` path — untouched.
- Subscription ordering, delta order, and window semantics unchanged.
- No frame lost, duplicated, or reordered; no command lost; FIFO preserved.
- Determinism: fan-out order unchanged (connections ascending);
  worker-count independence unchanged.
- The attached index mirrors session state exactly (attach adds, detach
  and disconnect remove); it never becomes authoritative.
- `unsafe_code = forbid` maintained.

## 6. Regression tests

1. Shared Arc TickUpdate frames: two clients receiving the same world's
   TickUpdate get identical logical data; the frames share one allocation
   (`Arc::ptr_eq`) and neither can mutate the other's bytes.
2. Attached-index consistency: attach adds a session to the broadcast,
   detach and disconnect remove it, re-attach works after removal — the
   index never diverges from the sessions' own attachment state.
3. Multi-world/multi-client fan-out still delivers per-client data
   (subscription deltas only to subscribers; results only to callers).
4. Existing protocol tests (frame format, checksum, rejection) unchanged.

## 7. Benchmark plan

- Before/after: Profile A @ 5K/10K, Profile B @ 5K/8K/10K/15K, same
  methodology, same binary flags. Report p50/p95/p99, phase breakdown,
  outbound message counts.
- Success: measured reduction in the fan-out phase; p99 on movement
  profiles improve; all correctness tests pass.
