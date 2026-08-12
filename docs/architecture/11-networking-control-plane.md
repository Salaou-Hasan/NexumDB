# ADR-011 — Networking + Control Plane

- **Status:** accepted
- **Phase:** 11
- **Date:** 2026-08-12
- **Related:** ADR-008 (subscriptions), ADR-009 (simulation), ADR-010
  (runtime)

## Context

Phases 1–10 produced a single-process authoritative state engine: worlds
own `TableStore`, one tick = one transaction = one `Vec<Change>`, the
runtime coordinates durability-first/observation-second, and subscriptions
observe committed state. Phase 11 must let clients (and operators) interact
with that engine without becoming a second state machine: no second storage,
transaction, OCC, WAL, subscription, or commit path.

## Decision

### D1 — The gateway is an adapter; the runtime is the coordinator

`NetworkGateway` **owns** the `Runtime` and orchestrates it, but all
authoritative operations flow through the existing runtime boundary:
`submit_input` (inputs), `subscribe`/`drain`/`unsubscribe`/`resync`
(observation), `step_detailed` (scheduling — the runtime decides world
order and executes every tick), `recover_world` (durability). The gateway
never touches tables, transactions, the WAL, or the registry directly.

### D2 — Identity lives in the gateway, stamped server-side

`Authenticator::authenticate(credentials) -> Result<Principal, AuthError>`
is the only identity hook (protocol-independent `Principal { id, name }`).
The gateway overwrites every routed command's `source` with the
principal id — client-supplied sources are ignored (anti-spoofing). A
connection is a transport handle; a session is the authenticated identity;
attachment binds a session to exactly one world.

### D3 — Versioned binary protocol with bounds before allocation

Frames are `magic | version | kind | len | payload | crc32`. `len` is
validated against a configured maximum **before** allocation; a checksum
guards integrity; decoding is checked end-to-end (malformed input →
`ProtocolError`, never a panic). Version negotiation is explicit: a
mismatched client version is rejected with an error and disconnect.

### D4 — Transport-independence with two concrete transports

The gateway depends only on a `Connection` trait (bounded inbound/outbound
frame queues, non-blocking poll/flush). Phase 11 ships a deterministic
`MemoryTransport` (tests/benches) and a dependency-free **nonblocking TCP**
transport (a practical first realtime transport). No QUIC/UDP/custom
reliable transports yet. The protocol and session layers are identical
across transports.

### D5 — Backpressure never touches simulation

Inbound queues are bounded per connection (overflow closes the
connection — flooding is hostile). Outbound queues are bounded per
connection; on overflow the session policy applies: `Stale` (drop deltas,
send `StaleNotification`, client must `Resync`) or `Disconnect`. The
gateway is single-threaded and never blocks; worlds tick regardless.

### D6 — Subscriptions stay authoritative; the network serializes

`SubscriptionRegistry` (per world, owned by the runtime) remains the
observation authority. The network holds only session → subscription id
mappings and converts `SubscriptionUpdate` values into protocol messages.
`TickResult.changes` remains the authoritative per-tick change boundary and
is broadcast to attached sessions as `TickUpdate`.

### D7 — Control plane is a separate typed surface

`ControlPlane` is a typed API over `&mut Runtime` (world lifecycle,
recovery, status, metrics, health, worker reassignment, shutdown) — never
the realtime protocol. Player messages and operator messages do not mix.

### D8 — Session state is operational, never authoritative

Connections, sessions, and attachments die with the process. After
recovery, clients reattach and resubscribe; recovered WAL history is never
replayed as live updates (the runtime boundary already guarantees this —
Phase 8 semantics).

## Consequences

**Positive.** Networking adds no state, no second commit path, and no
determinism hazards; security is enforced at the boundary (bounded
allocations, checked decoding, source stamping, caps); backpressure is
explicit and local to each connection; transports are swappable.

**Negative / accepted.** The gateway is single-threaded (throughput
bounded by frame processing; parallelism is a later phase); the `Stale`
policy drops world-level TickUpdates between staleness and resync (the
authoritative re-sync path is reattach + resubscribe, documented); the
TCP transport is a correct baseline, not a tuned server (no TLS, no HTTP,
no connection pooling).

## Alternatives considered

- **Gateway owns a second queue/registry**: rejected — duplicates
  observation state and risks divergence.
- **Clients call reducer/transaction APIs directly**: rejected — bypasses
  the runtime boundary and breaks the one-commit-path invariant.
- **Async/tokio stack**: rejected — the engine is synchronous and
  deterministic; threads/async add scheduling hazards and dependencies
  without a Phase 11 requirement.
- **Blocking transports**: rejected — a slow client must never block the
  gateway.
- **Network-driven ticking**: rejected — `step_detailed` keeps the runtime
  as the tick scheduler (D1).

## Implementation notes (post-design)

- `ConnectionId`/`SessionId` added to `nexum-core` (typed-ID philosophy).
- `Runtime::step_detailed` added (additive): one deterministic step pass
  returning each successful world's `TickResult`; `step()` unchanged.
- The scaffolded `nexum-network` crate (Phase 0) was implemented as the
  gateway; sessions, auth hooks, and transports are in-crate; HTTP/gRPC
  control binding and the server binary are deferred.
- Detach is a real protocol message: `ClientMessage::DetachWorld` ends the
  session's attachment and its subscriptions, answered by a dedicated
  `ServerMessage::DetachResult` (an `AttachResult` with `ok: true` and
  `world: None` would be an encoding contradiction).
- Every client-controlled count that feeds an allocation is bounded before
  the allocation: the input-frame command count is validated against the
  remaining payload (`count > remaining / 18` → malformed) so a hostile
  count can never trigger a capacity overflow; `TickUpdate`/snapshot/query
  counts use `try_reserve` + `map_err` (malformed, not panic).
- The TCP transport now carries the configured `max_payload` and enforces
  it at the transport boundary: an oversized frame declaration is rejected
  as soon as its header arrives, before the body is buffered (bounded
  memory per connection).
- `NetworkMetrics::tick_updates_sent` is incremented in `step_worlds`
  alongside the `StepReport` counter.
- **Security review (per §validation):** no second state system or commit
  path (the gateway imports no storage/transaction/WAL types); no
  unbounded client-controlled resources; checked decoding never panics on
  hostile input; principal stamping replaces client-supplied command
  sources (covered by a dedicated test); cross-world isolation and the
  recovery no-replay contract are integration-tested. Findings fixed:
  the input-frame capacity panic (HIGH), TCP oversized-frame buffering
  (MEDIUM), and untested principal stamping (LOW).
