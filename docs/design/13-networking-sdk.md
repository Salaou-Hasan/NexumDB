# Phase 13 — Networking + Client SDKs (Design)

## 1. Purpose and boundary

Phases 9–12 established the authoritative core: one World = one partition,
one tick = one transaction = one atomic commit = one `Vec<Change>` boundary,
with the runtime coordinating durability (WAL) and observation
(subscriptions). Phase 13 makes Nexum usable by an **external game client**:

- **Server side:** audit and complete `nexum-network` (an early implementation
  of the old roadmap numbering, now the canonical foundation). Add the two
  missing protocol surfaces — **reducer calls** and **server events** — plus
  **request correlation IDs**, all through the existing runtime boundary.
- **Client side:** a new `nexum-sdk` crate that is a **protocol + transport
  adapter** around the stable wire protocol. The SDK owns derived client
  state only; it never becomes authoritative.

The fundamental pipeline is unchanged:

```text
Client → SDK → Network Protocol → Gateway → Runtime → Partition → World
  → Systems/Reducers → Transaction → OCC → ONE commit → Vec<Change>
        ├── WAL
        └── SubscriptionRegistry → Gateway → SDK → Client
```

Networking is an **adapter**. It must never become another state engine,
transaction engine, OCC implementation, commit path, or subscription truth.

## 2. Authoritative boundary (what lives where)

| Concern | Owner |
|---|---|
| Authoritative game state | `TableStore` inside `World` (Phases 1–3) |
| Atomicity / OCC | `Transaction` / `nexum-tx` (Phase 4) |
| Simulation semantics | `World::tick` (Phase 9) — the **only** commit path |
| Durability | per-world `Wal` (Phase 5), coordinated by the runtime (Phase 10) |
| Observation | per-world `SubscriptionRegistry` (Phase 8) |
| Multi-partition routing | runtime partition registry + message bus (Phase 12) |
| Connections, sessions, auth hooks, protocol, fanout, backpressure | `NetworkGateway` (`nexum-network`) |
| Client-side protocol, session mirror, requests, subscriptions, views, reconnect | `nexum-sdk` |

**Server-internal, never crossing the wire:** `TableStore` internals,
`StorageTable`, `Transaction`, `ReadSet`, `WriteSet`, OCC validation, WAL
files, snapshots, worker ownership, `RowId` allocation, storage versions
(except where the protocol contract explicitly carries them, e.g. `Change`
records in `TickUpdate`).

**Allowed across the boundary (the protocol contract):** typed `Value`s,
`Row`s, `RowId`/`TableId`/`WorldId`/`SubscriptionId`/`TickId`/`TransactionId`
ids, committed `Change` records, subscription snapshots/deltas, `ReducerEvent`s,
`ReducerArgs`, and typed errors with stable codes.

## 3. The 40 design questions

1. **Authoritative server/client boundary** — the wire protocol. The server is
   the only writer of authoritative state; the client only sends requests
   (handshake, auth, attach, input, reducer calls, subscriptions) and receives
   observations (tick updates, subscription data, events, errors).
2. **What `nexum-network` owns** — connections, sessions, principals, auth
   hooks, protocol encoding/decoding, request correlation, input routing,
   reducer-call routing, subscription attachment, snapshot/delta delivery,
   resync, backpressure, connection lifecycle, versioning, bounded buffers,
   malformed-frame rejection, metrics, events, control plane.
3. **What the Runtime owns** — world/worker/partition lifecycle, input queues,
   tick scheduling, WAL/subscription coordination, recovery orchestration,
   ownership. The gateway calls the runtime; it never bypasses it.
4. **What a Partition/World owns** — authoritative state, systems, reducers,
   WASM, the tick transaction, the change boundary.
5. **What the SDK owns** — a `Client` with a connection state machine, a
   protocol codec (the canonical one), a session mirror, request correlation,
   subscription handles with **derived** local views, reconnect/resubscribe/
   resync, bounded client-side queues, typed errors.
6. **Information crossing the boundary** — the protocol contract (§2).
7. **Information remaining server-internal** — §2.
8. **Sessions** — `Session { id, connection, principal, attached_world }`,
   created by the gateway on successful authentication; operational state that
   dies with the process (rebuilt by reattach after recovery).
9. **Authenticated principals** — `Principal { id, name }`, protocol-
   independent, produced by the `Authenticator` trait hook. Never supplied by
   the client; the gateway stamps `principal.id` as the authoritative command
   source (anti-spoofing, existing and tested).
10. **Connection ↔ session** — one session per connection, created once at
    authentication, immutable identity for the connection's life.
11. **Session ↔ partition/world** — a session attaches to exactly one world
    (`AttachWorld`). The world is the partition's authoritative owner; routing
    follows the session's attachment, never a client-supplied id.
12. **Client input routing** — `InputFrame` → gateway → stamp source →
    `Runtime::submit_input(world, frame)` → world's tick. Wrong-world input is
    impossible by construction (the session is attached to one world).
13. **Reducer calls over the protocol** — new `CallReducer { request_id,
    reducer, args }` client message. The gateway validates the session and
    bounds, then submits the call to the runtime; the world executes it inside
    its **next tick transaction** (Phase 0c, after delivered messages and
    scheduled events, before systems) using a **branch transaction**
    (Phase 11 `branch_of`/`absorb`): a successful call absorbs into the tick
    transaction and its value + events join the tick result; a failed call
    discards its branch — zero mutation, a typed per-call error, the tick
    continues. The result flows back as `ReducerResult { request_id, ok,
    value, error }`, correlated by request id.
14. **Subscriptions over the protocol** — `Subscribe { query }` attaches a
    session to an existing per-world `SubscriptionRegistry` subscription; the
    server sends the initial snapshot, then deltas. The registry stays the
    authoritative observation system; the network layer only serializes.
15. **Snapshots** — `SubscriptionSnapshot { subscription, seq, rows }`
    (initial establishment or resync), regenerated from authoritative state.
16. **Incremental deltas** — `SubscriptionDelta { subscription, seq, kind,
    row_id, row }` where `kind` ∈ {Insert, Update, Delete}, produced by the
    Phase 8 `apply_changes` boundary, serialized in commit order.
17. **Resynchronization** — `Resync { subscription }` → server rebuilds the
    exact view and sends a fresh snapshot; the stale flag is cleared. Resync
    is the recovery from any missed data; the SDK triggers it on `Stale`.
18. **Disconnects** — `Disconnect { reason }` (server-initiated) or transport
    close; the gateway drops the connection, unsubscribes its runtime
    subscriptions, and clears pending reducer calls.
19. **Reconnects** — the SDK re-runs handshake → authenticate → attach →
    resubscribe → resync. The server never replays history as live updates;
    reattachment always observes the current authoritative state.
20. **Stale subscriptions** — server: outbound overflow marks the session
    stale and queues `StaleNotification`s (delivered when the queue drains);
    live data is dropped while stale, and the client must resync. SDK: a
    `Stale` state on the subscription handle invalidates the derived view
    until a fresh snapshot.
21. **Backpressure** — bounded per-connection inbound/outbound queues; the
    gateway's overflow policy is `Stale` (mark + notify) or `Disconnect`.
    Simulation, WAL, and other clients are never blocked (tested). The SDK
    also bounds its event queue and pending requests.
22. **Malformed/malicious frames** — bounded, versioned, checksummed frames;
    length checks **before** allocation; typed `ProtocolError`s; a protocol
    violation closes the offending connection. Never panics, never OOM
    (existing + extended tests).
23. **Version negotiation** — `PROTOCOL_VERSION` (u16) in every frame; the
    handshake carries the client version and the server rejects mismatches.
24. **Protocol compatibility** — the SDK and gateway share the same codec
    (`nexum-network::protocol`); new message kinds are additive and the
    handshake gates version drift. Documented message kinds in `protocol.rs`.
25. **Request/correlation IDs** — `CallReducer`/`ReducerResult` carry an
    explicit `request_id` (u64) because the response is asynchronous (arrives
    after a tick). All other request/response pairs (handshake, auth, attach,
    subscribe, resync) correlate by strict FIFO ordering on the connection.
26. **Errors** — typed with stable numeric wire codes: protocol errors (1–7),
    core errors (10–19), session errors (20–22), capacity (17), reducer-call
    errors (reuse core codes; `Conflict` is never masked).
27. **Server-generated events** — `TickUpdate` now carries
    `events: Vec<ReducerEvent>` (name + payload) emitted by systems/reducers
    during the tick, in `emit` order. Events are authoritative only in the
    sense that they describe committed ticks; they are **not** a separate
    state system and are discarded with aborted ticks (existing semantics).
28. **Client inputs vs server events** — inputs are client→server commands;
    events are server→client observations attached to committed ticks. The
    protocol kinds are disjoint (0x01–0x09 vs 0x81–0x8C).
29. **Server-authoritative command sources** — the gateway overwrites any
    client-supplied command source with the authenticated principal id
    before the frame reaches the runtime (tested; extend to reducer calls in
    a later phase if caller identity is needed inside reducers).
30. **Forge prevention** — the client can never choose its principal id,
    its world attachment (validated against the runtime), its command source,
    or its subscription id (assigned by the registry). Tested.
31. **Determinism** — the gateway is a stateless adapter between the wire and
    the runtime; it adds no simulation state. Reducer calls execute in
    deterministic queue order within the tick; results and events are
    delivered per world in deterministic order. Network timing cannot alter
    world semantics (the world owns the frame gate and tick counter).
32. **Multi-partition routing** — sessions attach to a world; the runtime
    maps worlds to partitions. Input/subscription routing is world-scoped;
    cross-world leakage is impossible by construction and tested.
33. **Recovery** — the gateway owns the runtime; after a crash the operator
    `recover_world`s partitions (Phase 5/10 machinery) and clients reattach.
    Recovered history is state, never replayed as live updates (tested).
34. **Subscriptions after recovery** — the registry is in-memory; after
    recovery the client resubscribes over the recovered state and receives a
    fresh snapshot — never a replay of historical deltas.
35. **Sessions after recovery** — sessions are operational and die with the
    process; clients reauthenticate and reattach.
36. **Partition failure** — the runtime marks the world failed; the gateway
    rejects new input/reducer calls to it and reports an error to attached
    sessions; other worlds are unaffected (isolation tested).
37. **Worker failure** — worlds become recoverable; the control plane can
    reassign ownership; attachment is by world id so routing is preserved.
38. **Input for a failed partition** — rejected by the runtime; the gateway
    sends a typed error to the caller (tested).
39. **Ordering guarantees** — per-connection FIFO; per-world tick updates in
    commit order; per-subscription deltas in commit sequence; multi-table
    commits delivered atomically in one `TickUpdate`. A subscription never
    observes `T2` before `T1` when commits establish `T1` before `T2`.
40. **Non-guarantees** — no cross-partition transactional semantics over the
    network, no global ordering across worlds, no durability/consensus beyond
    the WAL contract, no exactly-once redelivery (clients resync), no
    server-push without the client having attached/subscribed.

## 4. Reducer calls — semantics

A client reducer call is a **server API operation**:

```text
CallReducer { request_id, reducer, args }
  → gateway: session + attachment + bounds + pending-cap checks
  → Runtime::submit_reducer_call(world, request_id, reducer, args)  (bounded queue)
  → World::tick_with_calls(...) Phase 0c:
      for each call (FIFO):
        child.branch_of(tick_tx)
        native.invoke_in_tx(store, &mut child, name, args)   [WASM fallback]
        Ok  → tick_tx.absorb(child); result = Ok(value); events join tick
        Err → child.abort();           result = Err(error); events discarded
  → TickResult.reducer_results: Vec<ReducerCallResult>
  → gateway: ReducerResult { request_id, ok, value, error } → requesting client
```

- One tick remains one transaction; calls use the exact Phase 11 branch/absorb
  merge (reads union, writes overwrite, provisional counters take the max), so
  determinism is preserved and a failed call cannot leave partial state.
- Per-call errors (rejection, invalid args, `Conflict` from the call's own
  work, not-found) are delivered to the caller without aborting the tick.
- A failed **tick** (system error, OCC conflict, WASM trap) answers every
  still-pending call of that world with the tick error, so callers never hang.
- No automatic retry; a caller may retry with a new invocation.
- The gateway bounds: reducer-name length, args count, pending calls per
  connection, and queued calls per world (runtime config).

### 4.1 Finalized queue, budget, and lifecycle semantics (post-review)

**Queueing.** `Runtime::submit_reducer_call` queues a call for a **running**
world only; unknown/stopped worlds and a full queue are rejected with a
correlated `ReducerResult` failure at submission time (explicit backpressure,
never a silent drop). The per-world queue bound is
`RuntimeConfig::max_queued_reducer_calls`.

**Per-tick budget.** The simulation owns the execution budget:
`SimulationConfig::max_reducer_calls_per_tick`. The runtime drains at most
that many queued calls per tick in **FIFO** order; overflow stays queued for
future ticks. A misconfiguration can never fail a tick or drop an accepted
call — the network/runtime layer cannot bypass the simulation's budget.
`max_reducer_calls_per_tick = 0` is **an invalid configuration**, rejected by
`SimulationConfig::validate()` (world creation fails deterministically); it
is never a hang or a silent loss.

**Failed tick.** A failed tick committed nothing, so the calls drained into
it are **requeued** (FIFO at the front) and execute on the next eligible
tick. Under `TickFailurePolicy::FailWorld` the requeue is moot — the world is
dead and the gateway answers the pending calls with a correlated failure.
Under `Continue` the requeue is what prevents silent loss (tested).

**Pending-call lifecycle (world states).**

| World state | Pending calls |
|---|---|
| `Running` | kept pending; they execute on the next eligible tick |
| `Stopped` | resolved with a correlated `ReducerResult` failure on the next gateway step |
| `Failed` | resolved with a correlated `ReducerResult` failure |
| Destroyed / unknown | resolved with a correlated `ReducerResult` failure |
| Disconnected caller | the pending entry is dropped; the world may still execute the accepted call fire-and-forget |

Every accepted call receives **exactly one terminal `ReducerResult`** (success
or a correlated failure) — never zero, never two. A caller is never left
hanging.

**Restart semantics.** Stopping a world fails its pending calls (documented;
callers retry after restart). Restarting the world accepts new calls normally;
pending calls from before the stop are **not** silently deferred or replayed.

**Request-id uniqueness.** `request_id`s must be unique **per world while
pending** (the wire result carries only the request id, so ambiguous routing
is impossible by construction). A duplicate pending id — even from a different
connection — is rejected with a correlated failure; results are never
misattributed across clients (tested). The SDK allocates monotonic ids and
surfaces the rejection as a retryable error.

## 5. Events

`TickUpdate { world, tick, tx_id, changes, events }` now includes the tick's
`ReducerEvent`s in `emit` order. This keeps one committed tick = one message
(atomic observation of changes + events) and preserves event atomicity:
aborted ticks produce no events (nothing to deliver). Subscriptions remain
change-driven; events are a realtime side-channel that can be dropped by the
stale/overflow policy without corrupting any derived view.

## 6. SDK architecture (`nexum-sdk`)

One crate (no artificial fragmentation), modules:

| Module | Responsibility |
|---|---|
| `client` | `Client` facade: lifecycle, session, subscriptions, requests |
| `connection` | connection state machine + `Connection` wrapper |
| `transport` | reuse `nexum-network::transport` (`Connection` trait, memory/TCP) |
| `protocol` | reuse the canonical `nexum-network::protocol` codec |
| `session` | client-side session mirror (principal, attachment) |
| `request` | request-id allocation + pending reducer-call correlation |
| `input` | input-frame builders |
| `reducer` | `call_reducer` API |
| `subscription` | subscription handles + **derived** local views |
| `view` | per-subscription `BTreeMap<RowId, DeliveredRow>` cache + accessors |
| `event` | `ClientEvent` (what the game code consumes) |
| `error` | `SdkError` taxonomy |
| `reconnect` | reconnect / resubscribe / resync orchestration |
| `config` | bounded client-side configuration |

Public surface (game-developer oriented):

```text
Client::new(Box<dyn Connection>, SdkConfig)
  handshake / authenticate / attach / detach
  send_input / call_reducer / subscribe / unsubscribe / resync / ping
  poll() -> Vec<ClientEvent>        (drives the transport, dispatches)
  take_events() / subscription(id) / state() / disconnect / reconnect
```

The SDK reuses `nexum-network::protocol` and `nexum-network::transport`
**exactly** — one serialization, one framing, one version constant. It
depends on no storage/tx/wal internals.

Client-side views are **derived caches**: `view: BTreeMap<RowId,
DeliveredRow>` rebuilt from snapshots + deltas; a `Stale` handle is invalid
until resync replaces it. The SDK never writes to the server except through
the documented request messages; it can never mutate authoritative state.

## 7. Backpressure and bounds (server + client)

Server (existing, extended): `max_frame_payload`, inbound/outbound queue
caps, `max_connections`, `max_subscriptions_per_session`,
`max_commands_per_frame`, overflow policy (`Stale`/`Disconnect`), and now
`max_reducer_name_len`, `max_reducer_args`, `max_pending_calls_per_connection`.
Client: bounded event queue, bounded pending requests, bounded subscription
count; overflow drops oldest events and marks affected handles stale.

## 8. Security

All network input is untrusted: bounded decoding, checksummed frames, no
allocations from client-controlled lengths, no panics on malformed input,
no identity spoofing, no cross-world routing, no unbounded queues, no
`unsafe` (`unsafe_code = forbid` at the workspace level). A dedicated
security test suite covers oversized/malformed/truncated frames, floods,
forged sources, unauthorized calls, and replay of request ids.

## 9. Out of scope (later phases)

Phase 14 game-server layer; Phase 15 performance; Phase 16 hardening. No
QUIC/UDP/WebSocket/TLS/HTTP/gRPC transports, no authentication provider, no
matchmaking, no presence, no distribution, no client SDKs beyond this one.

## 10. Validation

Build + full workspace tests + clippy `-D warnings` + `unsafe_code = forbid`
+ network and SDK benchmarks + end-to-end integration (SDK → gateway →
runtime → world → WAL → subscription → gateway → SDK) + recovery no-replay +
multi-partition isolation + a security-oriented review.
