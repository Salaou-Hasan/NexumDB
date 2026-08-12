# Phase 12 — Multi-Partition Simulation (Design)

## 1. Purpose

Phase 12 formalizes the partition as a first-class concept and defines
**cross-partition execution** on top of the existing single-partition model
(Phases 9–11). It does **not** create a second storage engine, transaction
system, OCC implementation, commit path, WAL, or subscription system. One
partition = one `World` = one authoritative `TableStore` = one deterministic
tick stream.

```text
Runtime
  +-- Partition A -> World A -> TableStore A
  +-- Partition B -> World B -> TableStore B
  +-- Partition C -> World C -> TableStore C
```

## 2. What is a partition?

A **partition** is an authoritative execution boundary (ADR-012 D1):

- exactly one authoritative `TableStore` (the owning `World`'s store),
- exactly one deterministic tick stream (the `World`'s tick counter),
- isolated transaction/state ownership (Phase 4 OCC, per-world WAL, per-world
  `SubscriptionRegistry`),
- a stable identity: `PartitionId`.

Partitions are **never merged** into one giant global `TableStore`. World A
can neither read nor mutate World B's store — isolation is by construction
(each `World` owns its own `TableStore`).

## 3. What is a cross-partition operation?

A cross-partition operation is **deterministic messaging**, not a distributed
transaction (ADR-012 D2).

Rationale:

- A coordinated multi-partition transaction (2PC/staged prepare-commit) would
  break the one-tick-one-transaction invariant, introduce a second commit
  path, and make commit ordering depend on scheduling — exactly what the
  architecture forbids.
- Deterministic messaging keeps every partition on its own one-tick-one-
  transaction model. A partition never holds another partition's state; it
  exchanges *inputs*.

A system sends a message through the controlled context:

```text
System in partition A (tick N)
        │
        ▼
ctx.send_to(partition B, kind, payload)     // validated against the
        │                                     // world's known topology
        ▼
PartitionMessage { from: A, to: B, sent_tick: N, seq, kind, payload }
        │
        ▼
TickResult.outbound                          // committed with the tick
        │
        ▼
Runtime enqueues to partition B's inbound queue
        │
        ▼
B's tick N+1 (delivery phase)  →  handler reducer "kind" invoked
                                   against B's tick transaction
```

## 4. Delivery semantics — tick-aligned, one logical tick of latency

The runtime steps partitions in two strict phases per step (ADR-012 D3):

```text
step:
  1. DELIVERY phase   — for every world, drain its inbound queue of messages
                        with sent_tick < world.tick_number() into a
                        per-world delivered batch
  2. TICK phase       — for every world (deterministic (worker, world)
                        order), tick_messages(frame, delivered_batch);
                        on success, enqueue the tick's outbound to the
                        destinations' queues
```

Consequences:

- A message sent during tick `N` is delivered before the destination's tick
  `N+1`. **Exactly one logical tick of latency.**
- Delivery is **independent of worker scheduling**: the delivery phase
  completes before any world ticks, and worlds do not read each other's
  state, so the order in which worlds run within the tick phase cannot change
  any partition's trace.
- Worker-count independence holds: `(worker, world)` iteration order changes
  with worker count, but per-world traces never depend on position within the
  tick phase.
- On a stopped/restarted partition, all accumulated messages with
  `sent_tick < current tick` are delivered on resume (bounded by the queue
  limit).

## 5. How the destination consumes a message

A delivered message invokes the destination's registered reducer **named by
the message kind** against the destination's tick transaction (ADR-012 D4) —
the same machinery scheduled events already use. Handler resolution is:

1. native `ReducerRegistry` (registered by name), then
2. WASM `WasmModuleRegistry` (if configured), then
3. `NotFound` — the destination tick aborts with zero mutation.

Consequences:

- Native and WASM reducers work as handlers with no new execution model.
- Handler writes are transactional: a whole delivery batch + scheduled events
  + systems commit atomically or abort completely.
- Handler failures (rejection, panic boundary, WASM trap/fuel) abort the
  destination's tick — existing Phase 9/11 failure semantics.

## 6. Determinism contract

Identical `(world seeds, partition topology, inputs, system definitions,
reducer code)` must produce identical partition traces and final state
regardless of worker scheduling (ADR-012 D5):

- `PartitionMessage` is a fully deterministic value: `(from, to, sent_tick,
  seq, kind, payload)`. `payload` is `ReducerArgs` (a `BTreeMap`, key-sorted).
- `seq` is the sender's outbound index within its tick (system order).
- The world sorts a delivered batch by `(sent_tick, from, seq)` before
  handling, making delivery order a pure function of the messages themselves.
- The topology is a sorted, deduplicated set; a world validates `send_to`
  against it at send time (unknown target or self → deterministic tick error,
  zero mutation).
- No wall clock, no randomness, no OS scheduling enters any partition's
  semantics.

## 7. Bounded resources

- `max_messages_per_tick` — outbound budget per tick (Capacity error aborts
  the tick).
- `max_message_kind_len`, `max_message_args` — kind and payload bounds.
- `max_queued_partition_messages` (runtime config) — per-destination inbound
  queue bound. Overflow policy is **drop + event + metric** (deterministic;
  never blocks the sender's tick).
- Message kind must be non-empty.

## 8. Failure semantics

| Failure | Behavior |
| --- | --- |
| `send_to` unknown/self target | deterministic tick error → tick aborts, zero mutation, no outbound |
| outbound budget exceeded | Capacity error → tick aborts |
| unhandled message kind | destination tick aborts (NotFound), zero mutation |
| handler rejects / panics / WASM trap / fuel | destination tick aborts, zero mutation |
| inbound queue full | message dropped, `MessageDropped` event + metric |
| destination unregistered at enqueue | message dropped, `MessageDropped` event + metric |
| partition (world) fails | other partitions unaffected; messages queued to it accumulate until destroyed/recovered; messages *from* it stop |
| runtime crash | in-flight (queued, undelivered) messages are runtime-transient and lost; authoritative state is durable per world via WAL |

## 9. Recovery

Recovery reuses the Phase 5 engine per world (ADR-012 D6): `recover_world`
reconstructs a world's store from its snapshot + WAL; the application then
re-registers the partition (`register_partition`) and resumes. Recovered
history is **not** replayed as new messages or live subscription updates
(Phase 8 semantics preserved). Queued runtime messages do not survive a crash
by design — they are coordination state, not authoritative state.

## 10. Relation to Phase 11 concurrency

Parallel tick execution (Phase 11) is orthogonal and composes: a partition
may run its tick in `Parallel` mode while exchanging messages with other
partitions. `outbound` collection is threaded through the same slot-ordered
merge that guarantees `Vec<Change>` and events are worker-count independent.

## 11. What is deliberately NOT built

Networking, session/gateway layers (frozen Phase 13), partition migration,
replication, clustering, distributed consensus, cross-partition *transactions*
(2PC), sharding across machines, and any second state/transaction/commit
system. A future partition-migration interface needs only: the partition→world
ownership mapping (already explicit) and a state transfer (snapshot + WAL),
both of which Phase 5 already provides.

## 12. Interface summary (what Phase 13 consumes)

- `nexum_simulation::PartitionMessage` — the deterministic message envelope.
- `World::tick_messages(&frame, &delivered)` — message-aware tick; `tick`
  delegates with an empty batch.
- `SimulationContext::send_to(partition, kind, args)` — the only way a system
  emits a message.
- `TickResult::outbound()` — the committed outbound boundary.
- `Runtime::register_partition` / `unregister_partition` / `send_message`
  (external injection) / `partition_status` — the control surface.
- Per-partition WAL and `SubscriptionRegistry` remain the durability and
  observation boundaries.
