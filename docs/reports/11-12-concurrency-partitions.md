# Phases 11 & 12 — Concurrency & Multi-Partition Simulation: Implementation Report

Canonical roadmap: **9 (simulation core) ✅ · 10 (runtime) ✅ · 11 (concurrency)
✅ · 12 (multi-partition) ✅ · 13 (networking) 🔒 frozen · 14–16 later.**

The existing `nexum-network` crate is an early implementation of the future
Phase 13 networking layer. It was **not** modified in this work and remains
intact and documented as such.

## Validation

- **522 tests passing** (was 497), **0 failures**, **clippy zero warnings**
  (`cargo clippy --workspace --all-targets`), `unsafe_code = forbid`
  maintained.
- All Phase 1–10 tests remain green; both benchmarks run cleanly.
- No new external dependencies.

---

## Part A — Phase 11: Concurrency & Parallel Execution

### Architecture (ADR-011)

One tick stays **one logical transaction** (one commit, one `Vec<Change>`,
one WAL append). The [`TickPlan`] is a pure function of `(systems, store)`:
a greedy single pass groups the ordered systems into **pairwise
table-disjoint** groups using declared `SystemAccess` (write/write,
write/read, read/write overlap = conflict). Multi-member groups execute
concurrently — each system on a child transaction branched from the tick
transaction (`Transaction::branch_of`) — and merge back in system order
(`Transaction::absorb`), which is exact (D3): same read set, same write set
and keys, same commit ordering, same `Vec<Change>`, same events, same final
state. Singleton groups (opaque or conflicting systems) run on the serial
path against the tick transaction — the Phase 9 reference semantics.

- **Worker count is a pure performance knob** (`ExecutionMode::Parallel(N)`,
  `N == 1` takes a sequential fast path with identical semantics).
- **First failure in system order** fails the group identically to serial —
  zero merges, zero mutation.
- **RNG** is already per-system (`rng_seed(world_seed, tick, system_id)`), so
  parallel systems draw identical streams with no shared state.
- **Declared-access enforcement**: undeclared same-group write/write,
  read/write, and table-observation (scan) overlaps are detected
  deterministically at merge time (in slot order) and fail the tick with an
  `Internal` error instead of silently diverging from serial.
- Panics are caught per system with the identical Phase 9 boundary.

### Files changed

- `crates/nexum-tx/src/read_set.rs` — `absorb` (merge a child's reads into a
  parent without duplicating shared rows).
- `crates/nexum-tx/src/write_set.rs` — `set` (provisional-entry coalescing
  needed by `absorb`).
- `crates/nexum-tx/src/transaction.rs` — `branch_of` / `absorb` (exact
  merge primitives; inherited-key disappearance claim holds: child ops never
  remove inherited rows except insert→delete net no-ops).
- `crates/nexum-simulation/src/systems.rs` — `SystemAccess` (declared
  reads/writes, opaque marker).
- `crates/nexum-simulation/src/config.rs` — `ExecutionMode`.
- `crates/nexum-simulation/src/parallel.rs` — `TickPlan`, `run_system`,
  `execute_group` (scoped threads, slot-ordered collection, conflict
  detection, outbound merge).
- `crates/nexum-simulation/src/world.rs` — tick dispatch (serial reference
  loop / singleton / grouped paths).
- `crates/nexum-simulation/src/parallel_tests.rs` — 17 tests.
- `crates/nexum-simulation/examples/parallel_bench.rs`.
- `docs/design/11-concurrency.md`, `docs/architecture/11-concurrency.md`.

### Determinism evidence

`Serial == Parallel(1/2/4/8)` verified for per-tick `(changes, events)`
traces and final state dumps across: a rich mixed tick (disjoint systems,
same-table write chains, RNG, native reducer, scheduled event), 100 systems
in 10 groups, cross-group provisional visibility, identical real-id
assignment, read-your-writes inside children, first-failure and panic
parity, native/WASM reducer invocation and failure parity, and the outbound
message trace (seq renumbering makes it byte-identical to serial). Repeated
runs with the same seed are identical; different seeds diverge.

### Benchmarks (honest baselines)

Per-tick thread spawn (~80 µs) dominates trivial ticks in this reference
implementation; the compute-heavy scenario shows a real parallel gain
(serial 210 µs → Parallel(4) 168 µs). Thread pooling is a documented Phase 15
item. `chg/tick` is identical across all modes in every scenario.

---

## Part B — Phase 12: Multi-Partition Simulation

### Architecture (ADR-012)

A **partition is a `World`** — one authoritative `TableStore`, one
deterministic tick stream. The runtime's partition registry (`PartitionId →
{ world, worker, bounded inbound queue }`) owns routing metadata only; it
owns no state. Cross-partition operations are **deterministic tick-aligned
messaging**, never distributed transactions:

- A system sends via `SimulationContext::send_to(to, kind, args)`; the
  message is validated against the world's known topology at send time
  (unknown target or self → tick error, zero mutation) and committed with
  the tick in `TickResult.outbound`.
- The runtime's `step`/`step_detailed`/`tick_once` run a **delivery phase
  strictly before the tick phase**: every world's inbound messages with
  `sent_tick < tick_number` are drained before any world ticks. A message
  sent at tick N is delivered before the destination's tick N+1 — **one
  logical tick of latency**, independent of intra-step world order.
- The destination invokes the registered handler reducer **named by the
  message kind** (native first, WASM fallback) against its tick transaction
  — the same machinery as scheduled events. Unhandled kinds are
  deterministic `NotFound` tick failures.
- Delivery order is a pure function of the batch: the world sorts by
  `(sent_tick, from, seq)`; `seq` is the sender's outbound position.
- Bounded everything: outbound budget, kind/payload bounds, and a
  per-destination inbound queue whose overflow policy is deterministic drop
  + `MessageDropped` event + metric — never blocking the sender.
- External injection (`Runtime::send_message`) shares the same queue bounds
  and delivery path; it is stamped with the sender's logical tick and a
  deterministic per-sender sequence.

### Files changed

- `crates/nexum-simulation/src/partition.rs` — `PartitionMessage`.
- `crates/nexum-simulation/src/config.rs` — message bounds.
- `crates/nexum-simulation/src/context.rs` — `send_to`, partition identity.
- `crates/nexum-simulation/src/world.rs` — `tick_messages` (delivery gate,
  deterministic batch sort, handler invocation phase before scheduled
  events), `TickResult.outbound`, partition/topology fields.
- `crates/nexum-simulation/src/parallel.rs` — outbound merge in slot order
  with serial-exact seq renumbering.
- `crates/nexum-simulation/src/partition_tests.rs` — 11 tests.
- `crates/nexum-runtime/src/partition.rs` — `PartitionEntry`,
  `PartitionStatus`.
- `crates/nexum-runtime/src/runtime.rs` — registry, topology propagation,
  delivery phase, outbound enqueue, `register_partition`,
  `unregister_partition`, `send_message`, `partition_status`; `destroy_world`
  unregisters its partition; `reassign_world` keeps the registry in sync.
- `crates/nexum-runtime/src/{config,error,event,metrics}.rs` — queue bound,
  `UnknownPartition`/`DuplicatePartition`, `PartitionRegistered`/
  `PartitionUnregistered`/`MessageDropped`, partition/message counters.
- `crates/nexum-runtime/src/partition_tests.rs` — 13 tests;
  `crates/nexum-runtime/tests/partition_recovery.rs` — 1 integration test.
- `crates/nexum-runtime/examples/partition_bench.rs`.
- `docs/design/12-multi-partition.md`, `docs/architecture/12-multi-partition.md`.

### Partition guarantees

- One partition = one World = one authoritative TableStore; partitions are
  never merged and never read each other's state.
- Worker-count independence proven: a 3-partition ring run with 1 worker
  (order 0,1,2) and 2 workers (order 0,2,1) produces byte-identical
  per-world committed change traces (`ring_traces` test), because the
  delivery phase precedes the tick phase.
- Delivery is exactly-once per logical tick gate; stopped partitions
  accumulate (bounded) and receive everything with `sent_tick < current` on
  resume.
- Failure isolation: a failing destination (handler rejection) fails only
  that partition's tick; senders and other partitions keep ticking. A
  sender targeting an unknown partition fails its own tick with zero
  mutation (validated against the propagated topology).
- Per-partition WAL and `SubscriptionRegistry` unchanged; recovery
  reconstructs identical state and **never replays history as live
  updates**.

### Benchmarks (honest baselines)

1 partition/step ≈ 0.1 µs; 2 ≈ 1.3 µs; 8 ≈ 4 µs; 10 msg/tick ≈ 58 µs;
external `send_message` ≈ 0.4 µs; delivery+commit ≈ 2.2 µs per 2-step pair;
tick+WAL ≈ 3.3 µs; tick+subscription fan-out ≈ 1.1 µs. These are baselines;
Phase 15 is the optimization phase.

---

## Concurrency & partition invariants (documented + tested)

1. One World owns exactly one authoritative TableStore.
2. One tick = one logical transaction = one atomic commit = one `Vec<Change>`.
3. Runtime never mutates authoritative state; `World::tick_messages` is the
   only commit path; no second OCC, WAL, or subscription system.
4. Worker count never changes a world's or partition's trace.
5. Failed ticks produce zero authoritative mutation, zero events, zero
   outbound messages, zero WAL append, zero subscription update.
6. Declared-access violations are detected, never silently wrong.
7. Delivery phase strictly precedes tick phase; one logical tick of latency.
8. Partition A's failure never corrupts partition B's state or session.
9. Slow destinations never block the sender (bounded queues, drop policy).
10. Malformed/misdirected delivery fails deterministically.

## Known limitations (deliberate)

- **At-most-once delivery**: in-flight (committed-but-undelivered) messages
  are runtime-transient and lost on crash — recovery test asserts exactly
  this. Exactly-once delivery is future networking/distribution work.
- Per-tick thread spawn cost in the Phase 11 executor (pooling deferred to
  Phase 15); `take_deliverable` is a linear scan of the partition registry
  (fine at Phase 12 scale).
- Parallelism requires declared `SystemAccess`; undeclared overlap is
  detected and fails the tick rather than silently diverging.
- Cross-partition operations are asynchronous with a fixed one-tick latency;
  synchronous cross-partition transactions are explicitly a non-goal.

## Exact interface Phase 13 Networking should consume

- `nexum_simulation::PartitionMessage` — the wire-shaped deterministic
  envelope (`from, to, sent_tick, seq, kind, payload: ReducerArgs`).
- `Runtime::send_message(from, to, kind, args)` — the external injection
  entry the gateway will call for authenticated client messages.
- `Runtime::step` / `step_detailed` / `tick_once` — return committed
  `TickResult`s (with `outbound()`) for fan-out; the delivery phase is
  internal to the runtime.
- `Runtime::register_partition` / `unregister_partition` /
  `partition_status` / `topology()` — the control surface for world/partition
  binding.
- `TickResult::{changes, events, outbound}` — the authoritative
  post-commit boundary; WAL and `SubscriptionRegistry` remain
  runtime-internal.
- The frozen `nexum-network` gateway already consumes
  `Runtime::step_detailed` + `subscribe`/`drain`; adapting it to partitions
  means routing client inputs through `send_message`/`submit_input` by
  partition id.
