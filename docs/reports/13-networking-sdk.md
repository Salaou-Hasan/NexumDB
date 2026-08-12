# Phase 13 — Networking + Client SDKs: Implementation Report

## Summary

Canonical Phase 13 makes Nexum usable by an external game client. The early
`nexum-network` implementation (old roadmap numbering) was audited and
completed as the canonical foundation, three missing surfaces were added —
**client reducer calls**, **server event delivery**, and **request
correlation ids** — and a new **`nexum-sdk`** client crate was built. The
phase is complete: designed first, implemented incrementally, tested,
benchmarked, and security/architecture-reviewed, with the pending-call
lifecycle and per-tick budget semantics finalized and proven by regression
tests. **Stopped here** — no QUIC/UDP/WebSocket/TLS/HTTP/gRPC, no
authentication provider, no matchmaking, no distribution, no Phase 14.

## Validation

- **565 tests passing** (was 552), **0 failures**, **clippy zero warnings**
  (`--all-targets --all-features`), `unsafe_code = forbid` maintained
- Phases 1–12 untouched and green; WAL/recovery, subscription, WASM/native
  reducer, partition, and determinism tests all pass
- New reducer-call tests: runtime budget/FIFO/zero-budget/requeue +
  gateway lifecycle (stopped/destroyed/unknown/disconnect/restart/
  cross-client correlation) + SDK multi-change View
- No new external dependencies (only workspace crates + existing `wat`
  dev-dependency for the WASM e2e)

## Architecture

```
Client ─▶ nexum-sdk ─▶ versioned binary protocol ─▶ NetworkGateway ─▶ Runtime
                                                                    │ World::tick
                                                                    ▼
                                                              Vec<Change>
                                                          WAL ◄──┴──► SubscriptionRegistry
                                                              └──▶ network fanout
```

**`nexum-network`** (audited + completed): `NetworkGateway` (the adapter),
`NetworkConfig` / `OutboundOverflowPolicy`, the versioned binary `protocol`,
`Authenticator`/`Principal`, `Session` (connection → session → world
attachment), `Connection`/memory/TCP transports, `ControlPlane`, metrics,
bounded events. **Additive changes:** `CallReducer` (0x0A) / `ReducerResult`
(0x8C) messages, `ReducerArgs` + `ReducerEvent` deterministic codecs, the
`events` field on `TickUpdate`, `request_id` on `Subscribe`/`Error` (protocol
version → 2), the gateway `pending_calls` map with the finalized lifecycle,
and new bounds (`max_reducer_name_len`, `max_reducer_args`,
`max_pending_calls_per_connection`).

**`nexum-simulation` / `nexum-runtime` (additive):**
- `ReducerCall { request_id, reducer, args }` + `ReducerCallResult` in
  `nexum-simulation`; `World::tick_with_calls` runs calls in Phase 0c via
  branch/absorb (Phase 11 machinery) inside the tick transaction;
  `TickResult.reducer_results` carries results.
- `Runtime::submit_reducer_call` (bounded, world-state-checked);
  `tick_entry` drains at most `max_reducer_calls_per_tick` calls per tick
  (FIFO) and **requeues drained calls when a tick fails** (no silent loss
  under `TickFailurePolicy::Continue`); new config + metrics + events.

**`nexum-sdk`** (new): one crate, poll-driven `Client`, reusing the
canonical protocol and transport exactly. Modules: `client`, `connection`,
`transport`, `protocol`, `session`, `request`, `input`, `reducer`,
`subscription`, `view`, `event`, `error`, `reconnect`, `config`.

## Finalized reducer-call model

```
CallReducer { request_id, reducer, args }
  → gateway: session + attachment + bounds + pending-cap + per-world
             duplicate-request-id checks
  → Runtime::submit_reducer_call(world, ...)        (bounded queue)
  → tick_entry: drain ≤ max_reducer_calls_per_tick (FIFO); overflow stays queued
  → World::tick_with_calls Phase 0c: per call, branch_of(tick tx) →
       native.invoke_in_tx / WASM fallback →
       Ok: tick_tx.absorb; Err: branch.abort (zero mutation)
  → TickResult.reducer_results
  → gateway: ReducerResult { request_id, ok, value, error } → requesting client
```

- **One tick = one transaction; one commit path; one `Vec<Change>`.** Calls
  use the existing Phase 11 branch/absorb merge — reads union, writes
  overwrite, provisional counters take the max — so determinism is
  preserved and a failed call can never leave partial state.
- **Queueing:** unknown/stopped worlds and a full queue reject at
  submission with a correlated failure (explicit backpressure, never a
  silent drop). Bound: `RuntimeConfig::max_queued_reducer_calls`.
- **Per-tick budget:** the simulation owns the budget
  (`SimulationConfig::max_reducer_calls_per_tick`). The runtime drains at
  most that many per tick, FIFO; overflow stays queued. `0` is an
  **invalid configuration**, rejected by `SimulationConfig::validate()`
  (never a hang or silent loss).
- **Failed tick:** the drained calls are **requeued** (FIFO at the front)
  and execute on the next eligible tick; under `FailWorld` the gateway
  answers them with a correlated failure. Tested under `Continue`.
- **Pending-call lifecycle:** `Running` keeps calls pending; `Stopped`,
  `Failed`, destroyed/unknown worlds resolve every pending call with a
  correlated failure on the next gateway step; disconnect drops the pending
  entry (the world may still execute an accepted call fire-and-forget).
  Every accepted call receives **exactly one terminal `ReducerResult`**.
- **Restart:** stopping a world fails its pending calls (callers retry);
  restarting accepts new calls normally — old calls are never silently
  deferred or replayed.
- **Request-id uniqueness:** unique per world while pending; a duplicate
  (even across connections) is rejected explicitly and results are never
  misattributed (tested). SDK allocates monotonic ids.

## Protocol, sessions, subscriptions, inputs

- **Protocol:** versioned (2), bounded, checksummed binary frames; kinds
  0x01–0x0A client→server, 0x81–0x8C server→client; stable error codes;
  `request_id` correlation on async responses; version mismatch closes the
  connection cleanly (tested).
- **Sessions:** one session per connection, created at authentication via
  the `Authenticator` hook; `Principal` is protocol-independent; a session
  attaches to exactly one world (duplicate attach idempotent, cross-world
  attach rejected). Input command sources are server-stamped with the
  principal id (forgery tested).
- **Subscriptions:** the Phase 8 `SubscriptionRegistry` remains the
  authoritative observation system; the gateway serializes snapshots/deltas
  (`request_id` echoed on the initial snapshot); stale/resync flow
  preserved; the SDK keeps **derived** views only.
- **Multi-partition routing:** sessions attach to a world; the runtime maps
  worlds to partitions; cross-world leakage impossible by construction and
  tested (network + SDK).
- **Recovery:** recovered history is state, never replayed as live updates;
  clients reattach/resubscribe and receive fresh snapshots (tested at both
  the network and SDK level).

## Files changed (this phase)

- `docs/design/13-networking-sdk.md`, `docs/architecture/13-networking-sdk.md`
  (ADR-013, updated with finalized semantics), `docs/design/README.md`,
  `README.md`
- `crates/nexum-simulation/src/{calls.rs (new), world.rs, config.rs, lib.rs}`
- `crates/nexum-runtime/src/{runtime.rs, world.rs, config.rs, error.rs,
  metrics.rs, event.rs, tests.rs}`
- `crates/nexum-network/src/{protocol.rs, gateway.rs, config.rs, metrics.rs,
  transport.rs, tests.rs}`, `crates/nexum-network/tests/integration.rs`,
  `crates/nexum-network/examples/network_bench.rs`
- `crates/nexum-sdk/` (new crate: 15 modules + `tests.rs` +
  `tests/e2e.rs` + `examples/sdk_bench.rs`), `Cargo.toml`

## Tests

- Runtime: per-tick budget (1 and 2) FIFO across ticks, overflow stays
  queued and all execute, zero-budget rejected, unknown/stopped worlds
  rejected, queue overflow rejected explicitly, **failed-tick requeue under
  `Continue`**.
- Network: reducer call success + correlated failure, auth/attach/pending-cap/
  duplicate-id rejections, **stopped/destroyed/unknown world resolution**,
  **disconnect cleanup**, **cross-client correlation isolation**, **restart
  semantics**; existing protocol/session/routing/backpressure/control-plane
  suites unchanged and green.
- SDK: unit (View gap logic incl. same-commit deltas, connection state
  machine, request correlation, malformed-frame handling, API guards) and
  e2e (full pipeline with WAL + subscription + native/WASM reducer calls,
  failed tick with correlated call failure, multi-partition isolation,
  recovery no-replay, version negotiation, slow-client stale→resync,
  multi-change commit without a false ViewGap).

## Benchmarks (release, ns/op, honest baselines)

`network_bench`: frame encode 128–618, frame decode 86–672, session creation
~14.4 µs, input routing ~3.7 µs, subscription serialization ~9.4 µs, 100
connections/tick ~51 µs, 1000 connections/tick ~509 µs, 500 subscriptions/
tick ~24 µs, reducer-call roundtrip ~4.1 µs, slow-client isolation ~2.7 µs.

`sdk_bench`: protocol encode 342 / decode 744, session setup ~15 µs, input→
tick→events ~10.8 µs, reducer-call roundtrip ~8.3 µs, resync (100 rows)
~57 µs, delta apply ~1.9 µs, view lookup ~327 ns, subscribe/unsubscribe ~1.8
µs, reconnect ~11.3 µs, slow-client isolation ~2.6 µs.

## Security review findings and fixes

- **Fixed (review):** pending calls could hang forever when a world was
  stopped or destroyed — the unresolved-pass now answers calls for any
  non-`Running` world (and destroyed worlds) with a correlated failure.
- **Fixed (review):** the runtime drained the entire call queue without
  honoring the world's per-tick budget — it now drains at most
  `max_reducer_calls_per_tick` (FIFO), overflow stays queued.
- **Fixed (review):** calls drained into a failed tick were silently lost
  under `TickFailurePolicy::Continue` (client hang) — they are requeued.
- **Fixed (review):** `reducer_results_sent` was not incremented on
  unresolved-path results — now consistent.
- **Verified:** no unbounded allocations; client-controlled sizes bounded at
  every layer; forged principal/source rejected; WASM/native reducer parity
  (both routed through the identical call path); failed reducer semantics
  (per-call error, tick continues); failed tick → zero mutation, zero WAL,
  zero subscription updates; no `unsafe`.

## Invariants

1. Networking never owns authoritative state; never commits; never bypasses
   the runtime.
2. One world = one `TableStore`; one tick = one transaction = one commit =
   one `Vec<Change>`.
3. WAL is the durability authority; `SubscriptionRegistry` is the
   observation authority; the SDK holds derived view state only.
4. Every accepted reducer call receives exactly one terminal result.
5. No accepted call is silently dropped; no caller is left hanging.
6. The per-tick call budget is owned by the simulation, not by networking.
7. Slow clients never block simulation; one client's failure never corrupts
   another's session; one world's failure never corrupts another.
8. Network timing cannot alter deterministic simulation semantics.
9. Recovered history is never emitted as live realtime history.
10. `unsafe_code = forbid`.

## Known limitations

- Reducer calls are asynchronous with one-tick latency (no synchronous RPC).
- `request_id`s must be unique per world while pending; a second client
  submitting the same id concurrently is rejected explicitly (retryable).
- Events ride `TickUpdate` and are droppable by the stale policy; they are
  not a durable event log.
- Only memory and TCP transports; no TLS/WebSocket/QUIC/HTTP.
- The `Authenticator` trait is an interface, not a real provider.

## Public SDK API (Phase 14 input)

```text
Client::new(SdkConfig) → connect(Box<dyn Connection>) → pump() → take_events()
  authenticate(credentials) / attach(world) / detach()
  send_input(InputFrame) / call_reducer(name, ReducerArgs) -> u64 (request id)
  subscribe(Query) -> u64 (local handle) / unsubscribe(local) / resync(local)
  view(local) -> &View  (derived)   / subscription(local) / subscriptions()
  ping() / status() / state() / is_connected() / take_reducer_results() / take_error()
```

Phase 14 (game-server layer) consumes: the gateway + SDK surfaces above, the
`ServerEvent` taxonomy, `ReducerResult`, the derived `View`, `SdkConfig`
bounds, `ReconnectPolicy`, and the stable wire protocol (v2) with its typed
error codes — without touching any storage/transaction/WAL internals.
