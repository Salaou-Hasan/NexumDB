# ADR-013 — Networking + Client SDKs

- **Status:** accepted
- **Phase:** 13
- **Supersedes:** nothing (canonical replacement of the early `nexum-network`
  roadmap numbering; extends ADR-009, ADR-010, ADR-012)

## Context

Phases 9–12 established the authoritative core and the runtime. An early
`nexum-network` implementation (old roadmap Phase 11) exists and is mature,
but it lacks three surfaces Phase 13 requires: **client reducer calls**,
**server event delivery**, and **request correlation IDs** — and there is no
**client SDK** at all. Phase 13 must make Nexum usable by an external game
client without creating a second state/transaction/commit system.

## Decision

1. **The wire protocol is the authoritative server/client boundary.** It
   carries only the protocol contract: typed values, rows, ids, committed
   `Change`s, subscription data, `ReducerEvent`s, `ReducerArgs`, and typed
   errors. Storage, transaction, OCC, WAL, worker, and partition internals
   never cross it.
2. **`nexum-network` is audited and completed, not rewritten.** The gateway
   remains a stateless adapter over the runtime: connections, sessions, auth
   hooks, protocol codec, input routing, subscription attachment, fanout,
   backpressure, metrics, control plane. Nothing it owns is authoritative.
3. **Reducer calls are server API operations executed inside the world's
   next tick.** `CallReducer { request_id, reducer, args }` → the runtime
   queues it (bounded) → `World::tick_with_calls` runs each call in Phase 0c
   (after delivered messages and scheduled events, before systems) against a
   **branch transaction** (`branch_of`/`absorb`, Phase 11 machinery): success
   absorbs into the tick transaction and its value + events join the tick
   result; failure discards the branch (zero mutation) and yields a typed
   per-call error while the tick continues. Results return as
   `ReducerResult { request_id, ok, value, error }`.
   - One tick remains one transaction; one commit path (`World::tick`).
   - A failed **tick** answers every still-pending call of that world with
     the tick error, so callers never hang.
   - No automatic retry.
4. **Server events ride `TickUpdate`.** `TickUpdate { world, tick, tx_id,
   changes, events }` now includes the tick's `ReducerEvent`s in `emit`
   order — one committed tick = one message, preserving atomicity and
   deterministic ordering. Events are a realtime side-channel, droppable by
   the stale/overflow policy without corrupting derived views.
5. **Explicit correlation only where responses are asynchronous.** Reducer
   calls carry a client-allocated `request_id` (u64); every other
   request/response pair correlates by strict FIFO on the connection.
   Duplicate pending request ids are rejected.
6. **The SDK is one crate (`nexum-sdk`) that reuses the canonical protocol
   and transport exactly** — one serialization, one framing, one version
   constant, no second protocol. It is a client adapter: connection state
   machine, session mirror, request correlation, subscription handles with
   **derived** local views, reconnect/resubscribe/resync, bounded queues,
   typed errors. Client state is cache/view state, never authoritative.
7. **Backpressure is bounded on both sides.** Server: per-connection
   inbound/outbound caps + overflow policy (`Stale`/`Disconnect`) + new
   bounds (`max_reducer_name_len`, `max_reducer_args`,
   `max_pending_calls_per_connection`, runtime `max_queued_reducer_calls`).
   Client: bounded event queue and pending-request map. Slow clients never
   block simulation (tested).
8. **Recovery never replays history as live updates.** After crash/recovery,
   clients reattach, resubscribe over recovered state, and receive fresh
   snapshots — exactly the Phase 8/10 semantics.
9. **Security posture unchanged:** bounded decoding, checksums, no panics on
   malformed input, no identity spoofing (server stamps sources), no
   cross-world routing, `unsafe_code = forbid`.

## Consequences

**Positive.** External clients can drive reducers, observe committed state,
and receive events through one stable, versioned, bounded protocol; the SDK
hides every server internal; determinism, atomicity, and the single commit
path are preserved; the early networking work is repurposed rather than
discarded.

**Negative.** Reducer calls are asynchronous with one-tick latency (no
synchronous RPC); events are not durable (aborted/dropped events are lost by
design); client views can diverge and must resync; no transport beyond
memory/TCP yet.

## Implementation notes (post-design)

- `ReducerCall`/`ReducerCallResult` live in `nexum-simulation`; `World::
  tick_with_calls` is additive — `tick`/`tick_messages` delegate with an
  empty call batch, so all Phase 9–12 call sites are unchanged.
- The runtime gains `submit_reducer_call` (bounded, world-state-checked) and
  passes the drained batch into `tick_with_calls` in `tick_entry`.
- The protocol gains `CallReducer` (0x0A) / `ReducerResult` (0x8C) and the
  `events` field on `TickUpdate`; `ReducerArgs` and `ReducerEvent` get
  deterministic codecs.
- The gateway tracks `pending_calls: BTreeMap<(WorldId, u64), ConnectionId>`
  and routes `ReducerResult`s after each successful tick; the pending-call
  lifecycle is finalized: `Running` worlds keep calls pending, while
  `Stopped`, `Failed`, destroyed/unknown worlds resolve every pending call
  with a correlated failure on the next gateway step; detach/disconnect
  clear pending state; `request_id`s must be unique per world while pending
  (a duplicate is rejected explicitly, never misattributed across clients).
- The runtime's `tick_entry` drains at most
  `SimulationConfig::max_reducer_calls_per_tick` calls per tick (FIFO),
  leaving overflow queued; a **failed tick requeues** the drained calls so no
  accepted call is silently lost under `TickFailurePolicy::Continue`
  (tested), and `max_reducer_calls_per_tick = 0` is rejected as an invalid
  configuration. Restart semantics: stopping a world fails its pending calls;
  new calls work normally after restart.
- `nexum-sdk` is a new workspace member depending on `nexum-network`
  (protocol + transport), `nexum-core`, `nexum-simulation` (input frame
  types), `nexum-subscription` (query/row types), and `nexum-reducer`
  (args/events). It depends on no storage/tx/wal internals.
