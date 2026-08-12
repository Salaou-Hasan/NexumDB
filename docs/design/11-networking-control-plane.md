# Phase 11 — Realtime Networking + Control Plane (Design)

> The network layer is an **adapter around NexumDB**, never another runtime.
> One authoritative state machine; the network observes and routes it.

## 0. Scope

Build the client-facing realtime protocol and the operator-facing control
plane as a new `nexum-network` crate layered on `nexum-runtime`. The network
layer:

- authenticates connections (interface, not a provider),
- establishes sessions and world attachments,
- routes client commands as `InputFrame`/`InputCommand` through
  `Runtime::submit_input`,
- attaches clients to the existing `SubscriptionRegistry` (Phase 8) per
  world,
- receives committed `TickResult`/`SubscriptionUpdate` at the runtime
  boundary and serializes them to clients,
- enforces bounded queues and explicit backpressure so a slow client can
  never block simulation.

It **never** owns `TableStore`, world state, transactions, OCC, WAL,
reducers, simulation systems, or authoritative subscription state.

## 1. Answers to the boundary questions

| Question | Answer |
|---|---|
| Where does player identity live? | `nexum-network::Principal`, produced by the `Authenticator` hook. Protocol-independent (id + name). The gateway stamps it onto every routed command (`InputCommand::source = principal.id`), so a client cannot forge another player's source. |
| Where does session state live? | `nexum-network` (per-connection `ConnectionEntry`). Session state is **operational**, never authoritative; it dies with the process and is rebuilt by reattach/resync after recovery. |
| Where does authentication live? | `nexum-network::Authenticator` trait — an interface, not a provider. Phase 11 ships a token-table implementation for tests/dev; real providers plug in later. |
| Where does world attachment live? | `nexum-network` (session → one `WorldId`). Attaching requires an authenticated session and an existing world. Duplicate/different-world attach is rejected. |
| Where does authoritative player/game state live? | The world's `TableStore` — unchanged (Phase 3–10). |
| Where do subscriptions live? | The per-world `SubscriptionRegistry` (Phase 8), owned by the runtime entry. The network holds only a thin id mapping and converts `SubscriptionUpdate` into protocol messages. |
| Where does WAL state live? | `nexum-wal`, attached by the runtime at the commit boundary — unchanged. |
| Who accepts player input? | The runtime (`submit_input`, bounded queues, late/capacity rejection). The gateway routes and relays rejections as protocol errors. |
| Who decides whether input is valid? | Runtime + World (tick gate, per-command bounds) and the network config (frame/command bounds enforced **before** routing). |
| Who executes simulation? | `World::tick` (via `Runtime::step_detailed` — the runtime schedules; the gateway only fans results). |
| Who commits state? | The transaction engine inside `World::tick` — the **only** commit path. |
| Who persists state? | The runtime's WAL coordination (durability first, observation second). |
| Who produces realtime changes? | The runtime's per-world `SubscriptionRegistry` + `TickResult.changes`. The network **serializes**, never invents. |
| Who serializes changes? | `nexum-network::protocol` (deterministic binary codecs over `nexum_core::binary`). |
| Who is allowed to block whom? | **No one blocks simulation.** Outbound queues are bounded; a full queue triggers a per-session policy (mark stale + resync, or disconnect). The gateway is single-threaded and non-blocking. |

## 2. Architecture

```
                       Clients
                          │  versioned binary protocol (bounded frames)
                          ▼
                   Network Gateway          ── nexum-network ──
        ┌──────────────┬──┴──┬──────────────────┐
        │              │     │                  │
   connections     sessions  auth hooks    subscription mapping
        │              │     │                  │
        └──────────────┼─────┼──────────────────┘
                       ▼     ▼
                    Runtime (owns worlds, workers, WAL+registry per world)
                       │     World::tick → Transaction/OCC → commit
                       ▼          Vec<Change>
                  WAL  ◄─────────┴─────────►  SubscriptionRegistry
                       \                    /
                        └────► network fanout (serialized TickResult/SubscriptionUpdate)
```

- **Transport-independence:** the gateway talks to a `Connection` trait
  (bounded inbound/outbound frame queues). Two concrete transports ship:
  `MemoryTransport` (deterministic, used by tests/benches) and a
  dependency-free nonblocking TCP transport. The protocol/session layer is
  identical for both.
- **Control plane** is a separate typed surface (`ControlPlane` over
  `&mut Runtime`): world lifecycle, recovery, status, metrics, health,
  worker reassignment, shutdown. It is **not** part of the realtime
  protocol — player messages and operator messages never mix.

## 3. Protocol

Versioned binary, deterministic, bounded:

```
magic "NEXN" (4) | version u16 | kind u8 | payload_len u32 | payload | crc32(version‖kind‖payload)
```

- `PROTOCOL_VERSION = 1`. A mismatched client version is rejected with an
  explicit error and disconnect.
- `payload_len` is validated **before** any allocation against
  `max_frame_payload` (default 64 KiB); the transport also caps per-frame
  reads. Malicious lengths yield `ProtocolError::Oversized`, never OOM.
- Every frame is CRC-32 checked; checksum failure is a protocol violation →
  error + disconnect.
- Decoding is checked (`nexum_core::binary` codecs): truncated input,
  unknown kinds, and invalid payloads produce typed `ProtocolError`s, never
  panics.

**Client → Server:** Handshake, Authenticate, AttachWorld, InputFrame,
Subscribe, Unsubscribe, Resync, Ping.

**Server → Client:** HandshakeResponse, AuthResult, AttachResult,
TickUpdate, SubscriptionSnapshot, SubscriptionDelta, StaleNotification,
Error, Pong, Disconnect.

Correlation: ping ↔ pong by nonce; subscribe ↔ snapshot by subscription id;
attach ↔ attach result by world id. Simulation ticks are addressed by world
+ tick.

## 4. Sessions and identity

```
Connection ──authenticate──▶ Session (Principal) ──attach──▶ World
```

- A connection is a transport handle (`ConnectionId`). A session is the
  authenticated identity on that connection (`SessionId`). A session may
  attach to at most one world; all of its subscriptions live on that world.
- `Authenticator::authenticate(credentials) -> Result<Principal, AuthError>`.
  The gateway stamps `InputCommand.source` with the principal id — the
  client-supplied source field is ignored (anti-spoofing).
- Connection/session ids are typed (`nexum-core::ConnectionId`,
  `SessionId`) and strictly increasing.

## 5. Routing and input

Client `InputFrame` payload → decode (bounded) → session must be
authenticated + attached → gateway stamps command sources → bounds check
(commands per frame, payloads already frame-bounded) → `Runtime::submit_input`.
The runtime owns late-frame, capacity, and world-state rejection; the
gateway relays the `RuntimeError` as a protocol `Error` message. Input
never reaches a world the session is not attached to.

## 6. Subscriptions and fanout

- `Subscribe` → `Runtime::subscribe(world, query)` → the gateway maps the
  returned `SubscriptionId` to the session. The Initial snapshot is drained
  and delivered as `SubscriptionSnapshot`.
- After each `step_detailed`, per successful world the gateway:
  1. broadcasts `TickUpdate { world, tick, tx_id, changes }` to attached
     sessions (authoritative per-tick changes), then
  2. drains every network subscription on that world (`Runtime::drain`) and
     serializes `SubscriptionUpdate` → `SubscriptionSnapshot` /
     `SubscriptionDelta` / `StaleNotification`.
- Delivery order is deterministic: sessions ascending, subscriptions
  ascending, updates in registry order (the registry's own commit-sequence
  ordering).
- `SubscriptionRegistry` remains the **observation authority**; the network
  is a transport/buffer layer and holds only id mappings.

## 7. Backpressure

- **Inbound:** each connection's transport queue is bounded
  (`max_queued_inbound_frames`). A full inbound queue causes the transport
  to close the connection (a client flooding faster than the gateway drains
  is malicious or broken). The gateway never blocks.
- **Outbound:** each connection's outbound queue is bounded
  (`max_queued_outbound_frames`). When a send reports full, the session
  policy applies:
  - `Stale` — mark the session stale; drop TickUpdate/subscription deltas;
    send one `StaleNotification`; the client must `Resync` (restores
    subscription views; the authoritative re-sync path is documented).
  - `Disconnect` — close the connection with a reason.
- Simulation, WAL, and other clients are never affected. Tests prove a
  slow/full client cannot block a tick.

## 8. Security model

All network input is untrusted. Defenses: bounded frame size and payload
lengths (checked before allocation), CRC validation, checked decoding
(never panics), command-per-frame and subscription-per-session caps,
authenticated-only operations, attachment-gated routing, source
stamping, connection/session caps, and bounded queues everywhere.
`unsafe_code = forbid` stays enforced.

## 9. Determinism

Networking never changes simulation results. A world's result remains a
function of `(seed, tick, inputs, systems, reducer code)`. Network timing
matters only through the runtime's explicit input/tick acceptance rules
(frame tick matching, queue bounds, late rejection). The gateway serializes
frames in the order received per connection and routes them in that order.

## 10. Recovery

Connections/sessions are operational state: a crash loses them (correctly —
they are not authoritative). After runtime recovery:
- historical WAL changes are **never** replayed as live updates (Phase 8
  semantics preserved at the runtime boundary),
- clients reattach and resubscribe, receiving fresh Initial snapshots,
- subsequent ticks deliver normally.

## 11. Out of scope (later phases)

Multi-node clusters, distributed worlds, migration, replication, sharding,
matchmaking, presence, consensus, gateway clustering, QUIC/custom
transports, HTTP/gRPC control binding, and production hardening.

## 12. Interfaces consumed (additive changes only)

- `nexum-core`: add `ConnectionId`, `SessionId` (typed ids).
- `nexum-runtime`: add `Runtime::step_detailed() -> Vec<(WorldId,
  TickResult)>` — one deterministic step pass that returns each successful
  world's committed result so the gateway can fan out per world. `step()`
  is unchanged.
- Everything else is consumed as-is: `submit_input`, `subscribe`, `drain`,
  `unsubscribe`, `resync`, `world_status`, `list_worlds`, `worker_status`,
  `metrics`, `recover_world`, `shutdown`, `RuntimeError`, `TickResult`,
  `SubscriptionUpdate`, `Query`, `InputFrame`, `InputCommand`.
