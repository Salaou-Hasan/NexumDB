# Phase 11 — Realtime Networking + Control Plane: Implementation Report

## Summary

Phase 11 builds the realtime networking and control-plane **adapter** around the Phase 10
runtime. It is complete: designed first, implemented incrementally, tested, benchmarked, and
security/architecture-reviewed. **Stopped here** — no distributed worlds, clustering, world
migration, replication, sharding, matchmaking, presence, consensus, or gateway clustering.

## Validation

- **474 tests passing** (was 440), **0 failures**, **clippy zero warnings**,
  `unsafe_code = forbid` maintained
- Phases 1–10 untouched and green; WAL/recovery, subscription, reducer, and WASM
  integration all pass
- No new external dependencies (only workspace crates)

## Architecture

```
Client ──versioned binary protocol──▶ NetworkGateway ──▶ Runtime ──▶ World::tick
                                                              │    (only commit path)
                                                              ▼
                                                        Vec<Change>
                                                   WAL ◄──┴──► SubscriptionRegistry
                                                   └──▶ network fanout
```

**`nexum-network`** (new): `NetworkGateway` (the adapter), `NetworkConfig` /
`OutboundOverflowPolicy`, the versioned binary `protocol` (bounded, checksummed frames),
`Authenticator`/`Principal`, `Session` (connection → session → world attachment),
`Connection`/`MemoryTransport`/nonblocking `TcpConnection`/`TcpTransport`, and
`ControlPlane` (typed operator API). Additive core/runtime changes: `ConnectionId` +
`SessionId` in `nexum-core`; `Runtime::step_detailed` in `nexum-runtime`.

## The invariants, and where they live

1. **Network never owns authoritative state** — the gateway imports no storage/transaction/WAL
   types; it only routes and serializes.
2. **Network never commits** — `World::tick` (through `Runtime`) is the only commit path;
   `step_worlds` fans out `TickResult`s after the fact.
3. **Network never bypasses Runtime** — inputs become `InputFrame`s via
   `Runtime::submit_input`; observation via `subscribe/drain/unsubscribe/resync`; durability
   via the runtime's own WAL coordination.
4. **One world = one TableStore; one tick = one transaction** — unchanged (Phases 9–10).
5. **Durability before observation** — a failed tick produces zero WAL, zero subscription
   delta, zero realtime update (integration-tested).
6. **Backpressure never blocks simulation** — bounded per-connection queues; `Stale`
   (mark + resync) or `Disconnect` overflow policies; slow-client isolation tested.
7. **Client-controlled sizes are bounded** — frame payload, per-frame command count,
   subscriptions per session, connection count, queue caps; wire counts that feed
   allocations are validated before allocating (no capacity-overflow panics).
8. **Malformed input never panics** — checked decoding; hostile counts → `Malformed`;
   oversized declarations rejected at the transport as soon as the header arrives.
9. **Recovered history is never replayed as live history** — recovery + reattach + resync
   integration-tested (the subscriber sees an Initial snapshot of recovered state, then
   only new deltas).
10. **Cross-world isolation** — inputs routed only to the attached world; subscriptions
    carry `(world, server)`; detach unsubscribes; leakage tested end-to-end.
11. **Determinism** — no wall clock in results; `BTreeMap` iteration; network timing can
    only affect the explicitly defined input/tick acceptance rules.

## Where the answers live (brief §"IMPORTANT ARCHITECTURAL CHECK")

- **Player identity** — `Principal` in `nexum-network::auth` (protocol-independent id+name).
- **Session state** — `Session` (connection-scoped, operational, dies with the process).
- **Authentication** — the `Authenticator` trait; `TokenAuthenticator` for tests/dev.
- **World attachment** — `Session::attach`/`detach`, routed by `WorldId`.
- **Authoritative player/game state** — `TableStore` inside `World` (unchanged).
- **Subscriptions** — per-world `SubscriptionRegistry` (Phase 8), never duplicated.
- **WAL state** — the runtime's per-world WAL (Phase 10), never touched by the network.
- **Input acceptance/validity** — `NetworkGateway::handle_input` (session/attachment/
  command-count gates, principal stamping) then `Runtime::submit_input` (late/capacity/
  world-state gates).
- **Execution/commit/persistence** — `World::tick`, OCC, and the runtime's WAL append.
- **Realtime changes** — `TickResult.changes` serialized as `TickUpdate`; subscription
  updates as snapshots/deltas.
- **Serialization** — `nexum-network::protocol` codecs.
- **Who blocks whom** — a slow client may be dropped/staled but can never block a tick,
  a commit, another client, or another world.

## Files changed

- `docs/design/11-networking-control-plane.md` (new)
- `docs/architecture/11-networking-control-plane.md` (new, ADR-011)
- `crates/nexum-core/src/ids.rs` (+ `lib.rs`) — `ConnectionId`, `SessionId`
- `crates/nexum-runtime/src/runtime.rs` (+ tests) — `step_detailed`
- `crates/nexum-network/Cargo.toml`, `src/{lib,config,error,metrics,auth,session,transport,
  protocol,gateway,control}.rs`, `src/tests.rs`, `tests/integration.rs`,
  `examples/network_bench.rs` (all new)
- `README.md`, `docs/design/README.md`

## New public interfaces

- `NetworkGateway` — `register_connection`, `process_inbound`, `step_worlds`, `send`,
  `disconnect`, `session_of`, `subscribe`-flow plumbing, `metrics`, `drain_events`,
  `control()`, `runtime()/runtime_mut()`
- `NetworkConfig` builders + `OutboundOverflowPolicy` (`Stale` / `Disconnect`)
- `protocol` — `encode_client/decode_client/encode_server/decode_server/parse_frame`,
  `ClientMessage` (Handshake, Authenticate, AttachWorld, InputFrame, Subscribe,
  Unsubscribe, Resync, Ping, DetachWorld), `ServerMessage` (HandshakeResponse, AuthResult,
  AttachResult, DetachResult, TickUpdate, SubscriptionSnapshot, SubscriptionDelta,
  StaleNotification, Error, Pong, Disconnect)
- `Authenticator`/`Principal`/`TokenAuthenticator`
- `Connection`/`MemoryTransport`/`TcpConnection`/`TcpTransport`
- `ControlPlane` — world lifecycle, recovery, status, metrics, health, worker
  reassignment, shutdown (all delegating to `Runtime`)

## Security findings and fixes

Reviewed against the brief's checklist (duplicate state, duplicate commit paths, runtime
bypass, unbounded queues/allocations, subscription duplication, WAL bypass, determinism,
cross-world leakage, session/identity confusion):

- **HIGH (fixed)** — the input-frame decoder allocated `with_capacity(count)` from an
  unbounded wire count (capacity-overflow panic / allocator abort). Now validated against
  the remaining payload before allocating.
- **MEDIUM (fixed)** — the TCP transport parsed frames with an effectively unbounded max,
  buffering an oversized declared frame before the gateway could reject it. The transport
  now carries the configured `max_payload` and rejects oversized declarations on the header.
- **LOW (fixed)** — principal stamping was documented but untested; added a test proving a
  client-forged command source is replaced with the authenticated principal id.
- No duplicate state systems, no second commit path, no unbounded client-controlled
  resources, no remaining panic-from-input vectors, no WAL/subscription bypass.

## Benchmarks (honest baselines; `cargo run --release -p nexum-network --example network_bench`)

Frame encode/decode ≈ 0.2 µs; server TickUpdate encode/decode ≈ 0.5–0.6 µs; session
creation (handshake+auth+attach) ≈ 90–200 µs (dominated by runtime world setup);
input routing ≈ 3.3–3.6 µs/tick; subscription delta serialization ≈ 28–59 µs;
outbound insertion ≈ 0.14 µs; 100 connections/tick ≈ 52 µs; 1,000 connections/tick
≈ 0.5 ms; 500 subscriptions/tick ≈ 22–26 µs; slow-client isolation ≈ 2.6 µs/tick.

## Known limitations

- Single-threaded in-process gateway; no TLS/HTTP/gRPC control binding or server binary
  yet (the control plane is a typed in-process API).
- Interest management is exactly the Phase 8 subscription system (per-connection
  subscriptions), not a dedicated spatial/interest layer.
- The `Stale` policy drops world-level `TickUpdate`s until the client resyncs/reattaches.
- TCP transport is a correct nonblocking baseline, not a tuned server.

## Interface Phase 12 (client SDK) should consume

- The versioned binary protocol (`nexum-network::protocol`) — a full client-side codec;
  `parse_frame` supports streaming transports directly.
- `ClientMessage`/`ServerMessage` — the wire contract; protocol version negotiation via
  `Handshake`/`HandshakeResponse`.
- Session lifecycle: `Authenticate` → `AuthResult`, `AttachWorld` → `AttachResult`,
  `DetachWorld` → `DetachResult`, `Ping`/`Pong`.
- Game input: `InputFrame`/`InputCommand` (server-stamped sources).
- Observation: `Subscribe` → `SubscriptionSnapshot`, then `SubscriptionDelta` (Insert /
  Update / Delete), `Resync` → fresh snapshot, `StaleNotification` → must resync.
- The runtime continues to own durability and observation; the SDK is a pure protocol
  consumer.
