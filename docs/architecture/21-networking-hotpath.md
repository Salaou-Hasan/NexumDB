# ADR-021 — Networking & Serialization Hot-Path

Status: accepted (Phase 21).

## Context

The Phase 21 profile (docs/design/21-networking-hotpath.md) measured the
gateway fan-out as the dominant networking cost: every TickUpdate,
reducer result, and subscription delta is delivered as an individual
checksummed frame, so a 10K movement tick emits ~20,300 frames/tick, each
with its own encode, CRC-32, allocation, mutex lock, and queue push
(~15.2 ms/tick fan-out; ~5.2 ms even on fully idle ticks). The client pays
the same per-frame cost to decode. A second, structural cost: the fan-out
pass scanned **all** connections per world (O(worlds × CCU)) twice per
tick.

## Decision

Two delivery-layer optimizations shipped; one was tried and reverted on
measurement. All are view/framing-only — authoritative simulation,
transaction/OCC, `Vec<Change>`, subscription registry, and WAL untouched.

### D1 — Arc<[u8]> frames (transport) — SHIPPED

The `Connection` trait's frame type becomes `Arc<[u8]>` (recv and send).
The per-world TickUpdate is encoded once per tick and delivered to every
attached session by refcount bump (no per-client clone/memcpy). One-off
frames convert via `Arc::from` (one allocation, no copy). The SDK decodes
`&frame[..]`.

### D3 — per-world attached index (gateway) — SHIPPED

The fan-out pass previously scanned all connections for each world twice
(TickUpdate broadcast + subscribers): O(worlds × CCU) predicate
evaluations per tick. A `BTreeMap<WorldId, BTreeSet<ConnectionId>>`
index, maintained on attach/detach/disconnect and never authoritative,
makes both scans O(attached-to-world) — the pass is O(CCU) total. The
sessions' `attached_world` remains the source of truth.

### D2 — per-connection outbound batching (protocol) — REVERTED

D2 (new `KIND_BATCH` 0x8D, one frame per connection per pass) was
implemented but measured **net-negative**: per-connection BTreeMap
bookkeeping (node alloc + insert/remove per client per tick) canceled the
clone savings, and embedding the shared TickUpdate in a per-client batch
frame re-copied its payload once per client, losing D1's zero-copy
broadcast. Profile B @ 10K: **p95 44.6 ms vs 39.5 ms baseline** (worse);
idle flat. Per the phase rule — revert optimizations with no measured
improvement — D2 was fully reverted (protocol kind, gateway machinery, SDK
dispatch, tests). The protocol gains no new wire kind this phase.

## Consequences

- The idle TickUpdate broadcast is zero-copy end to end (one encode, one
  allocation, shared `Arc`), and fan-out scans scale with CCU rather than
  worlds × CCU.
- No wire-protocol change; server and SDK stay on the same `PROTOCOL_VERSION`.
- Fan-out and client-decode cost remain O(CCU) per tick — the movement
  p99 improved (72.9 → 64.7 ms @ 10K) but the movement tick is still bound
  by the sum of per-connection inbound decode + world tick + fan-out +
  SDK decode. Further reduction needs fewer/cheaper per-client work items
  (Phase 20 interest management, Phase 22 WASM).
- Determinism preserved: fan-out order unchanged; `unsafe_code = forbid`
  maintained.
