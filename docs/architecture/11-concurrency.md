# ADR-011 — Deterministic Parallel Tick Execution

- **Status:** Accepted
- **Phase:** 11 (canonical — concurrency & parallel execution; the old
  networking "Phase 11" is frozen as the future Phase 13)
- **Design doc:** `docs/design/11-concurrency.md`

## Context

Phase 9 established a single-threaded deterministic tick as the correctness
oracle. Phase 11 must parallelize a tick's systems while preserving the
Phase 9 contract byte-for-byte: one logical transaction, one OCC validation,
one atomic commit, one `Vec<Change>`, identical results for identical
inputs regardless of worker count.

## Decision

### D1 — One logical transaction per tick; parallelism is inside the tick.

A tick begins with exactly one `Transaction::begin(&mut store)`; scheduled
events run serially; systems execute in deterministic groups; the merged
transaction commits exactly once. No second commit path exists.

### D2 — A table-granularity declared-access conflict model.

Each `SystemDefinition` may declare `SystemAccess { reads, writes }` (table
names). Undeclared access is `opaque` (conflicts with everything — the safe
default). Two systems conflict on write/write, write/read, or read/write
overlap of a table. The `TickPlan` groups systems greedily over `(priority,
id)` order; every multi-member group is pairwise conflict-free; singletons
(and everything opaque) run on the serial Phase 9 path.

Declarations are the **trusted-code correctness contract** for
parallelization (native systems are trusted, like Phase 6 native reducers).
An *honest* declaration guarantees correctness. A *lying* declaration is
caught at group merge, never silently wrong: a child whose write key a
sibling already wrote (write/write), a child that read a row a sibling wrote
(read/write), or a child that scanned a table a sibling wrote — each fails
the tick deterministically in system order with zero mutation. (A
misconfigured world — a system declaring a nonexistent table — fails its
Parallel tick at plan build; Serial cannot validate declarations, so this is
the one documented mode divergence, and only for invalid configurations.)

### D3 — Children are branch transactions; merging is exact.

A system in a multi-member group executes on `Transaction::branch_of(parent,
id)` — a copy of the parent's write set and per-table provisional counters.
After the group, children merge into the parent in system order via
`Transaction::absorb`: reads union, writes overwrite (a child's write set is
the coalesced final state of every key it touches; inherited keys never
disappear because provisional handles never cross the context boundary), and
provisional counters advance by `max`. The merged transaction equals the
serial transaction key-for-key, so commit ordering, real-row-id assignment,
and the `Vec<Change>` are identical.

### D4 — One system = one error/panic boundary.

Every system runs inside `catch_unwind`. Outcomes are collected by slot; the
first failure in system order fails the tick with the identical error the
serial path reports. Failed ticks abort with zero mutation, zero changes,
zero events.

### D5 — The RNG is already per-system.

`rng_seed(world_seed, tick, system_id)` is a pure function — parallel
systems draw identical streams with no shared state. No change needed.

### D6 — Workers via `std::thread::scope`; results are slot-ordered.

A group's slots distribute round-robin over `min(workers, members)` scoped
threads; results are collected by slot index, never by completion order, so
the outcome is a pure function of the plan. No external dependencies.

### D7 — The serial path stays the oracle.

`SimulationConfig::execution` defaults to `ExecutionMode::Serial` — the
unchanged Phase 9 loop. `Parallel(N)` selects the planner. Tests prove
`Serial == Parallel(1..8)` on identical worlds (per-tick change/event traces
and final store dumps).

## Consequences

**Positive.** Real, provably deterministic parallelism; worker count is a
pure performance knob; the Phase 9 contract is preserved exactly; native and
WASM reducers work unchanged inside children (`invoke_in_tx` is
`Sync`-safe); no new dependencies; `unsafe_code = forbid` maintained.

**Negative.** Table-granularity declaration is coarse; `branch_of` copies the
parent write set per child (O(members x writes)); opaque systems never
parallelize; greedy grouping is not optimal; per-tick scoped-thread spawn
cost dominates trivial ticks (a persistent worker pool is the Phase 15
optimization); declarations are trusted-code contracts — lying declarations
are detected at merge (write/write, read/write, scan-level) but the checks
are per-tick, not free.

## Alternatives considered

- **Shared-transaction thread-safe writes** — would rewrite Phase 4
  internals and compromise the oracle. Rejected.
- **Independent "mini-transactions" with revalidation only** — loses
  cross-group provisional visibility and changes real-id assignment.
  Rejected.
- **Per-group full transactions** — would create multiple commit points.
  Rejected.

## Implementation notes (post-design)

- Additive tx API: `ReadSet::absorb`, `WriteSet::set` (overwrite, absorb
  only), `Transaction::branch_of` / `Transaction::absorb`.
- Additive simulation API: `SystemAccess`, `SystemDefinition::with_access`,
  `ExecutionMode`, `SimulationConfig::with_execution`, `parallel::{TickPlan,
  Group, run_system, execute_group}`; `World::tick` dispatches on
  `config.execution()`.
- Child transaction ids are ephemeral local counters, never exported or
  persisted, so the store's `TransactionId` allocator matches serial exactly
  (the parent is allocated first via `Transaction::begin`).
