# ADR-012 — Multi-Partition Simulation

- **Status:** accepted
- **Phase:** 12
- **Supersedes:** nothing (extends ADR-009, ADR-010, ADR-011)

## Context

Phases 9–11 established one `World` = one authoritative partition with
one-tick-one-transaction determinism. Phase 12 must introduce multiple
authoritative partitions and define cross-partition semantics without a
second state, transaction, OCC, commit, WAL, or subscription system, and
without sacrificing determinism under worker scheduling.

## Decision

1. **A partition is a `World`.** `PartitionId` is the message-bus address of
   an existing authoritative `World`. The runtime maps partitions to worlds;
   no new state container exists.
2. **Cross-partition operations are deterministic tick-aligned messaging** —
   never distributed transactions. No 2PC, no cross-partition locking, no
   second commit path.
3. **One logical tick of latency.** A message sent at tick `N` is delivered
   before the destination's tick `N+1`. The runtime's `step` runs a delivery
   phase (drain every inbound queue) strictly before its tick phase (tick all
   worlds), so delivery order never depends on intra-step scheduling and
   worker-count independence is preserved by construction.
4. **The destination handles messages by invoking a registered reducer named
   by the message kind** (native first, WASM fallback) against its tick
   transaction — the same machinery as scheduled events. Unhandled kinds are
   deterministic `NotFound` tick failures.
5. **Messages are deterministic values** `(from, to, sent_tick, seq, kind,
   payload: ReducerArgs)`; the world sorts delivered batches by
   `(sent_tick, from, seq)`; `send_to` validates against the world's known
   topology at send time.
6. **Bounded everything.** Outbound budget per tick, kind/payload bounds, and
   a bounded per-destination inbound queue whose overflow policy is
   deterministic drop + event + metric — never blocking the sender.
7. **Recovery is per-world Phase 5 recovery**; re-registration re-attaches a
   recovered world to the bus. Queued in-flight messages are runtime-transient
   (lost on crash) by design.

## Consequences

**Positive.** Multiple authoritative partitions with an explicit, testable
determinism contract; cross-partition behavior is just input exchange; native
and WASM handlers reuse the reducer machinery; Phase 11 parallel ticks compose
unchanged; the future partition-migration interface needs only ownership
mapping plus snapshot/WAL transfer.

**Negative.** Cross-partition operations are asynchronous with a fixed one-tick
latency — no synchronous cross-partition transactions (a deliberate
non-goal); in-flight messages are not durable.

## Implementation notes (post-design)

- `PartitionMessage` lives in `nexum-simulation` (produced by ticks, consumed
  by the runtime).
- `World::tick_messages` is the additive message-aware tick path; `World::tick`
  delegates with an empty batch, so all Phase 9–11 call sites are unchanged.
- `SimulationContext::send_to` is the only emission surface; outbound is
  collected per system and merged in system order (Phase 11 slot order for
  parallel groups).
- The runtime keeps `partitions: BTreeMap<PartitionId, PartitionEntry>` plus a
  sorted `topology` set propagated to every registered world, so `send_to`
  validation and routing share one source of truth.
- `Runtime::step`/`step_detailed`/`tick_once` gained the delivery phase;
  outbound enqueue happens after each successful tick; `destroy_world` also
  unregisters the bound partition.
- External injection (`Runtime::send_message`) is the control-surface entry
  for tests and the future Phase 13 gateway; it shares the same queue bounds
  and delivery path as intra-tick messages.
- Message-handler invocation resolves native first, then WASM — documented in
  the design; unhandled kinds abort the destination tick deterministically.
