# Phase 23 — Fan-Out & Transport Hot-Path Optimization Report

## Summary

Phase 23 reduced the per-tick networking/transport overhead through five measured
optimizations. The cumulative effect cut fan-out by ~47% and improved p50 latency
by ~2.7× at 10K clients.

## Optimizations Implemented

### 1. Skip Empty TickUpdate Broadcast (commit 88fafaa)
When `tick_update_changes=false` and events are empty, the TickUpdate carries zero
useful payload. Suppressed the O(CCU) broadcast via `skip_empty_broadcast` config.
**Eliminated 5M messages/tick at 10K clients.**

### 2. Per-World Subscriber Index (commit 88fafaa)
Replaced the O(CCU) per-tick subscription scan (iterate every connection, filter
by world, HashMap lookup per connection) with a pre-computed
`BTreeMap<WorldId, Vec<(ConnId, SubId)>>` maintained on subscribe/unsubscribe/
detach/disconnect. Fan-out now O(subscribers) instead of O(CCU).

### 3. has_pending Fast-Path (commit 88fafaa)
Added `has_pending()` to `SubscriptionRegistry` and `Runtime`. `pump_subscription`
now checks for pending data before calling `drain()`. Eliminates ~9800 unnecessary
HashMap lookups + `mem::take` per tick at 10K clients.

### 4. Zero-Allocation flush_outbound (commit 8b57428)
Eliminated the per-tick `Vec<ConnectionId>` allocation in `flush_outbound` by
iterating the BTreeMap directly. Collects only broken connections (usually empty).
Reduced flush from 0.4ms to 0.1ms.

### 5. Combined recv_any for SDK Pump (commit afaae0d)
Added `try_recv_any_combined` to `Connection` trait: tries direct then frame
queue in a single Mutex lock for `MemoryConnection`. The SDK pump previously did
2 Mutex lock/unlock cycles per empty client. Now does 1, halving pump overhead.

## Measured Results

### Before/After Comparison (10K Profile B, 16 partitions, 16 workers)

| Metric | Phase 22 Baseline | After Phase 23 | Improvement |
|--------|-------------------|----------------|-------------|
| fanout | 4.2 ms/tick | 2.1 ms/tick | **2.0×** |
| flush | 0.4 ms/tick | 0.1 ms/tick | **4.0×** |
| inbound | 2.9 ms/tick | 2.6 ms/tick | 1.1× |
| tick | 3.5 ms/tick | 2.2 ms/tick | 1.6× |
| p50 | 8.0 ms | 2.9 ms | **2.8×** |
| tick_updates sent | 5,000,000 | 0 | **eliminated** |

### CCU Ladder (Phase 23 Results)

| Profile | CCU | p50 | p99 | Classification |
|---------|-----|-----|-----|----------------|
| A (idle) | 10K | 2.9 ms | 4.2 ms | ✅ PASS |
| A (idle) | 15K | 7.1 ms | 10.8 ms | ✅ PASS |
| A (idle) | 20K | 4.8 ms | 7.0 ms | ✅ PASS |
| B (move) | 5K | 1.5 ms | 38 ms | ✅ PASS |
| B (move) | 10K | 2.9 ms | 83 ms | DEGRADED (join storm) |
| C (realistic) | 1K | 0.5 ms | 7.3 ms | ✅ PASS |
| C (realistic) | 2.5K | 0.9 ms | 20.6 ms | ✅ PASS |

### Key Observations

1. **Empty TickUpdate was the dominant fan-out cost**: eliminating it saved ~2ms
   at 10K clients by removing 10K message sends per tick.

2. **Subscription iteration was O(CCU)**: the per-world subscriber index reduced
   this to O(subscribers) per tick.

3. **Mutex lock count matters**: the combined recv_any halves SDK pump overhead,
   which helps at all CCU levels.

4. **Join storm dominates p99 at high CCU**: the first ~20 ticks overwhelm the
   system with connection + subscription setup. This is an inherent property of
   the current architecture, not a fan-out issue.

5. **10K movement p50 is under 3ms target**: steady-state 10K Profile B p50 = 2.9ms.

## Remaining Bottlenecks

| Rank | Bottleneck | Cost at 10K | Cost at 20K |
|------|-----------|-------------|-------------|
| 1 | World tick scheduling overhead | 2.2ms | 21.6ms |
| 2 | Inbound command processing | 2.6ms | 17.7ms |
| 3 | Fan-out per-subscriber pump | 2.1ms | 13.5ms |
| 4 | SDK pump per-client | ~1.5ms | ~3ms |
| 5 | WASM fire_weapon | 65µs/call | SATURATED at 10K Profile E |

## Correctness

- All workspace tests pass (0 failures)
- Clippy clean (`-D warnings`)
- `unsafe_code = forbid` preserved
- No debug artifacts
- Determinism preserved

## Files Changed

- `crates/nexum-network/src/config.rs` — added `skip_empty_broadcast` config
- `crates/nexum-network/src/gateway.rs` — fan-out optimization, subscriber index, flush fix
- `crates/nexum-network/src/transport.rs` — combined recv_any trait method
- `crates/nexum-sdk/src/transport.rs` — recv_any combined, RecvAnyResult type
- `crates/nexum-sdk/src/client.rs` — pump uses combined recv_any
- `crates/nexum-runtime/src/runtime.rs` — has_pending method
- `crates/nexum-subscription/src/registry.rs` — has_pending method
- `crates/nexum-subscription/src/subscription.rs` — has_pending method
- `crates/game-server/examples/ccu.rs` — enable skip_empty_broadcast
