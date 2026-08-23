# Phase 23–25 — Full-System Performance Campaign Design

## Objective

Make the entire authoritative execution pipeline — from inbound command
processing through world simulation, subscription evaluation, fan-out
serialization, and client-side decode — as computationally cheap and
latency-efficient as practically possible.

Target: 20,000 real gameplay clients with p50/p95/p99/p99.9 < 3 ms.

## Architecture Overview

```
CLIENT
  ↓ input frame
NETWORK GATEWAY (process_inbound)
  ↓ decode + dispatch
RUNTIME (step_detailed)
  ↓ tick_worlds (rayon parallel)
  ↓ per-world: reducer → transaction → OCC → commit → changes → subscription eval
  ↓ apply_outcome (WAL, metrics, outbound)
GATEWAY (fan_out_results)
  ↓ TickUpdate broadcast
  ↓ pump_subscription per subscriber
  ↓ reducer result routing
TRANSPORT (flush_outbound)
  ↓
CLIENT SDK (pump)
  ↓ recv → decode → dispatch → View::apply_delta
DRAIN (take_events, take_reducer_results)
```

## Key Design Decisions

### 1. Thread Pool vs OS Threads

The runtime's `tick_worlds` runs independent worlds concurrently. The original
implementation used `std::thread::scope` which creates fresh OS threads per
call. This was replaced with `rayon::in_place_scope` which reuses a persistent
thread pool.

The work assignment is identical — deterministic contiguous chunks of the world
list — so parallel execution produces bit-identical results regardless of thread
pool implementation.

### 2. Atomic Hints vs Mutex-for-Every-Connection

The in-process transport previously locked a Mutex on every `try_recv_frame`
and `try_recv_any_combined` call, even when no data was pending. At 20K idle
connections this added ~20K Mutex acquisitions × 50 ns = 1 ms per phase.

The `has_inbound` and `has_outbound` atomic flags provide a ~5 ns fast-path.
The flags use `Ordering::Relaxed` because they are purely advisory — a stale
`true` causes a harmless empty lock, and a stale `false` causes a harmless
extra lock. No correctness depends on the flag value.

### 3. Subscription Pump Skip Logic

The subscription pump runs inside `fan_out_results` for every subscriber of
every world. When a world produces zero changes, no subscription buffer can
contain new entries (the subscription `apply_changes` only runs when there are
changes). The pump is skipped entirely for such worlds.

This eliminates ~20K BTreeMap lookups per idle world per tick, reducing the
fan-out phase from 1.1 ms to 0.0 ms at idle.

### 4. Zero-Copy Subscription Deltas

The subscription pipeline originally cloned every `Row` (a `Vec<Value>`) when
delivering deltas to the client SDK. With `Arc<DeliveredRow>` threaded through
the entire path, the shared payload is reference-counted rather than deep-cloned.

The `View` now stores `BTreeMap<RowId, Arc<DeliveredRow>>`. The `apply_delta`
method accepts `Option<Arc<DeliveredRow>>` and inserts the Arc directly. No
allocation occurs per delta in the steady-state path.

### 5. Benchmark Methodology

The CCU harness exercises the complete authoritative pipeline:
- Real `NetworkGateway` with real protocol codec
- Real `Runtime` with parallel world execution
- Real `World` with game reducers and subscription evaluation
- Real `SubscriptionRegistry` with AOI-filtered views
- Real `Client` SDK with frame decode and View application
- Real `MemoryTransport` with bounded queues

No subsystem is bypassed, mocked, or measured in isolation.

## Scaling Characteristics

| Component | Complexity | Bottleneck |
|-----------|-----------|------------|
| inbound | O(CCU × frames/tick) | Per-connection Mutex lock |
| tick_worlds | O(partitions) parallel | Subscription evaluation |
| subscription eval | O(changes × views) | CPU-bound per change |
| fan-out | O(subscribers_with_data) | Per-subscription send |
| client pump | O(CCU × frames/tick) | Per-client Mutex lock |
| drain | O(CCU) | Negligible |

## Future Optimization Targets

1. **Subscription evaluation batching** — evaluate all changes against views in
   a single pass, reducing the O(changes × views) to O(changes + views).

2. **Lock-free per-connection queues** — SPSC rings for in-process transport
   eliminate all Mutex overhead.

3. **Pre-computed initial snapshots** — deliver initial subscription data during
   the subscribe handshake, eliminating the join-storm backlog entirely.

4. **Per-view encoding** — encode subscription deltas once per view rather than
   per subscriber, sharing the encoded bytes via Arc.
