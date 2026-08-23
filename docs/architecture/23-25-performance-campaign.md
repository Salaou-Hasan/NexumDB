# ADR-23/24/25 — Performance Campaign Architecture Decisions

Status: accepted (Phases 23–25).

## Context

After Phases 17–22, the authoritative execution pipeline had been optimized at
the reducer, transaction, and WASM levels. The remaining execution cost was
distributed across the networking/transport, runtime scheduling, subscription
fan-out, and SDK decode paths. No single subsystem dominated — the cost was
O(CCU) in every phase.

## Decisions

### D1 — Use rayon for parallel world ticking

**Decision**: Replace `std::thread::scope` with `rayon::in_place_scope` in
`Runtime::tick_worlds`.

**Rationale**: `std::thread::scope` creates N OS threads per tick (one per
worker). On Windows, thread creation costs ~50–100 µs. At 16 workers × 20 Hz
this adds 1.6–3.2 ms per tick — a constant overhead independent of workload.

Rayon maintains a persistent thread pool. `in_place_scope` reuses the existing
threads without creating/destroying OS threads. The work distribution is
identical (deterministic contiguous chunks), preserving determinism.

**Trade-off**: Adds a workspace dependency (rayon 1.10). The binary size
increases by ~200 KB. The thread pool is created once at process start.

### D2 — Atomic fast-paths for in-process transport

**Decision**: Add `has_outbound: Arc<AtomicBool>` to `MemoryConnection` alongside
the existing `has_inbound` flag. Both server-side and client-side connections
skip Mutex acquisition when the relevant queue is empty.

**Rationale**: At 20K idle connections, the Mutex lock/unlock cycle dominates
per-connection cost (~50 ns each). The atomic flag provides a ~5 ns fast-path
that avoids the Mutex entirely when there is nothing to receive.

**Trade-off**: Relaxed ordering (`Ordering::Relaxed`) is sufficient because
the flag is a hint — a stale `false` causes a harmless extra Mutex acquisition,
and a stale `true` causes a harmless empty Mutex acquisition. No correctness
invariant depends on the flag.

### D3 — Skip subscription pump when no changes exist

**Decision**: In `fan_out_results`, skip the `world_subscribers` iteration
entirely when `result.changes().is_empty()`.

**Rationale**: If a world produced zero changes in a tick, no subscription
buffer can have new entries (the subscription `apply_changes` only runs when
there are changes). The only exception is Initial/Resync snapshots, which are
handled during the subscribe/resync process, not during the tick pump.

This eliminates ~20K BTreeMap lookups per idle world per tick.

**Trade-off**: If a subscription somehow accumulates stale data from a
previous tick (e.g., due to backpressure), it would not be drained until the
next tick with changes. In practice, the pump runs every tick and the buffer
is always drained.

### D4 — Thread Arc<DeliveredRow> through the subscription pipeline

**Decision**: Change `View` from `BTreeMap<RowId, DeliveredRow>` to
`BTreeMap<RowId, Arc<DeliveredRow>>`. Thread `Arc<DeliveredRow>` through
`apply_delta` so the shared payload flows from server subscription registry →
gateway `SubscriptionDeltaEntry` → client `View` without any clone.

**Rationale**: The previous path cloned every `Row` (allocating a new
`Vec<Value>`) per delta in the SDK dispatch. At 20K clients with ~1 delta/tick,
this was ~200K allocations/tick. The `Row` contains `Vec<Value>` — cloning it
copies all values and heap-allocates a new Vec.

With `Arc<DeliveredRow>`, the clone is a single atomic increment (~1 ns). The
`Row` data is shared in memory and only freed when the last `Arc` drops.

**Trade-off**: The `View` now holds `Arc` references that outlive individual
deltas. This is correct because `DeliveredRow` is immutable once created. The
`View` is a derived/client-side structure — it does not participate in the
authoritative state path.

### D5 — Extended warmup for high-CCU benchmarks

**Decision**: Increase warmup from 10 to 50 ticks in the CCU harness.

**Rationale**: At 20K clients, the initial subscription snapshot delivery takes
~20 ticks to fully drain. With only 10 warmup ticks, the first ~10 measured
ticks still pay for the join-storm backlog, artificially inflating p95/p99.

50 warmup ticks provide sufficient headroom for the snapshot backlog to drain
before measurement begins.

## Correctness Invariants Preserved

- Single authoritative state: World → TableStore
- Single transaction path: Transaction → OCC → Commit → Vec<Change>
- Deterministic simulation across worker counts
- FIFO ordering preserved
- No silent command loss
- WASM sandbox integrity
- `unsafe_code = forbid`
- Subscription observation semantics unchanged
- Per-tick reducer-call budget preserved
