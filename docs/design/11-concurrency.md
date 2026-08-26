# Phase 11 — Concurrency & Parallel Execution (design)

Status: **DESIGN** (canonical Phase 11; replaces the old networking number).

Supersedes: nothing. Builds on Phase 9 (World/tick), Phase 10 (Runtime/Worker),
Phase 4 (Transaction/OCC), Phase 7 (WASM), Phase 6 (reducers).

Related ADR: ADR-011 (`docs/architecture/11-concurrency.md`).

---

## 1. Goal

Parallelize the execution of a single world's tick **without weakening any
Phase 9 guarantee**:

- one World → one authoritative TableStore → one tick → one logical
  transaction → one OCC validation → one atomic commit → one `Vec<Change>`
- the same world, inputs, seed, systems, and reducer code produce the same
  final state, change trace, event sequence, and error outcome **regardless
  of the configured worker count** (1, 2, 4, 8, …)
- the Phase 9 serial path remains the correctness oracle and is always
  available as a reference mode

The Phase 10 runtime already serializes *worlds* (each world ticks alone,
worlds never share state). Phase 11 parallelizes *inside* one world's tick:
the independent systems of a tick execute concurrently while the tick stays
one logical transaction.

## 2. What may run concurrently, what must stay serial

**Concurrent:** simulation systems whose declared access is pairwise
disjoint at table granularity — they neither write the same table nor read a
table another writes. They are *provably* order-independent, so running them
on different threads cannot change the result.

**Serial (Phase 9 path, unchanged):**
- the scheduled-event phase (reducer invocations) at the start of a tick
- systems that do **not** declare access (opaque — the default), or that
  conflict with the current group (write/write, write/read, read/write on a
  shared table)

**Never concurrent:** the transaction commit, OCC validation, and the
`Vec<Change>` production. They run exactly once, on the merged tick
transaction, exactly as Phase 9.

## 3. The dependency/conflict model

Systems are the only units of parallelism. A system declares which tables it
touches and how:

```rust
SystemAccess { reads: BTreeSet<String>, writes: BTreeSet<String> }
SystemAccess::opaque()   // default: no declaration — conflicts with everything
SystemAccess::new(&["players"], &["inventory"])   // declared access
```

Two systems **conflict** when, after resolving table names to `TableId`s:

```
a.writes ∩ b.writes ≠ ∅   (write/write)
a.writes ∩ b.reads  ≠ ∅   (write/read)
a.reads  ∩ b.writes ≠ ∅   (read/write)
```

Two systems that only *read* the same table do not conflict (both read-only
observations against a frozen store agree).

A system that invokes reducers (native or WASM) must declare the full
footprint of what those reducers touch, or stay `opaque`. The merge phase
below detects any *actual same-group overlap* that a declaration missed as
a deterministic tick error — a declaration violation is never undefined
behavior and never silently wrong (see §5, merge-time detection).

## 4. The TickPlan (deterministic grouping)

`TickPlan::build(systems, store)` is a pure function of the ordered system
list and the store. Greedy single pass over systems in `(priority, id)`
order:

```
groups = []
current = []            # indices into the ordered systems slice
footprints = (reads: set, writes: set)

for (i, system) in systems:
    access = system.access()
    if access is opaque:
        flush current; current = [i]; flush current      # singleton, serial
        continue
    resolve reads/writes to TableIds (unknown name → deterministic error)
    if current not empty AND system conflicts with the group footprint:
        flush current; current = [i]
        footprints = (system.reads, system.writes)
    else:
        current.push(i); footprints ∪= system access

flush current
```

Properties (all provable by construction):

- the plan is deterministic: a pure function of `(systems, store)` — worker
  count never enters it
- every multi-member group is pairwise conflict-free
- group order == system order (a system in group G runs after every system
  in groups < G)
- a singleton group is either an opaque system or a system that conflicted
  with the previous group — in both cases it runs **serially against the tick
  transaction**, exactly like Phase 9

## 5. Execution: branch transactions, exact merge

A tick still begins with one `Transaction::begin(&mut store)`. Scheduled
events run against it serially. Then groups execute in plan order:

- **Singleton group** → `run_system` against the tick transaction directly
  (the exact Phase 9 code path: `SimulationContext` → transaction → store).
- **Multi-member group** → each member executes on its own **child
  transaction**, concurrently:

```rust
// per system, on its worker thread:
let mut child = Transaction::new(ephemeral_id);
child.branch_of(&parent);            // copies parent writes + provisional counters
let outcome = run_system(definition, &mut child, store, ..., &mut child_events);
```

`branch_of` snapshots the parent's current write set and per-table
provisional-id counters into a fresh `Active` transaction. The child's read
set starts empty (inherited writes are *writes*; their read observations were
captured by their original writers in the parent's read set).

After all threads of the group join, children merge into the parent in
**system (slot) order**:

```rust
parent.absorb(child);                       // reads ∪, writes overwrite, counters max
append_events(tick_events, child_events, max_events)?;
```

`absorb` is exact, not approximate. **Undeclared same-group overlaps are
rejected at merge, never silently wrong** (the trust anchor: a sibling's
write key is invisible to another child's branch, so any overlap the
declarations failed to express is observable at merge time):

- a child writes a row a sibling already wrote → write/write violation;
- a child read a row from the store that a sibling wrote (in serial it would
  have seen the sibling's provisional value) → read/write violation;
- a child scanned / unique-looked-up a table a sibling wrote → scan-level
  read/write violation.

Each is reported in system (slot) order as `Error::internal(... undeclared
… dependency)` — deterministic, with zero authoritative mutation.

- **Reads**: union via `ReadSet::absorb` (BTreeMap overwrite — the store is
  frozen during a tick, so re-reads of the same row always agree).
- **Writes**: the child's write set is the *coalesced final state of every
  key it touches* (it started as a copy of the parent's writes, and every
  child op coalesced through `WriteSet`'s rules). Overwriting the parent's
  entry at the same key is therefore the correct final value.
- **A key can never disappear from a child's set if the parent held it**:
  `WriteSet::delete` removes a key only for `insert → delete` of the same
  handle, and provisional handles are created by — and only ever known to —
  the transaction that created them. A system in another child cannot
  reference another system's provisional handle (handles never cross the
  context boundary), so an inherited key always survives a child's lifetime.
  (Asserted in a test; the invariant is documented on `absorb`.)
- **Provisional ids match serial exactly**: systems run once per tick, in
  order, so in serial each system's inserts occupy a contiguous block of the
  per-table provisional counter. A child starts from the parent's counters;
  group members are table-disjoint so sibling children never advance the same
  counter; the parent takes `max` at merge. The final write set therefore has
  the *same keys* serial would produce, and commit assigns real ids and
  `Change`s in the same `(TableId, RowId)` order — identical `Vec<Change>`.

## 6. Why the result is byte-identical to serial

For any system `S` in group `G`:

1. Everything `S` sees comes from (a) the frozen store and (b) writes
   committed to the parent *before G started* — exactly the state the serial
   shared transaction would present when `S` runs, because groups execute in
   system order and each group's writes are merged before the next group.
2. Group-mate writes are invisible to `S`, but group-mates are pairwise
   conflict-free — they never touch a table `S` reads or writes, so the
   invisible writes could not change `S`'s behavior (nor its read set, since
   `S` never reads those tables).
3. `S`'s own reads/writes/events/RNG are recorded in its own structures and
   merged in system order — the same records serial would produce.

Therefore the merged transaction (read set + write set + events + provisional
counters) is equal to the serial transaction, and the commit — validation,
apply order, `Vec<Change>`, real-id assignment — is identical.

## 7. Determinism of errors, panics, and events

- Every system runs inside `catch_unwind` (the Phase 9 boundary). A panic
  becomes `Error::internal("simulation system '…' panicked during tick …")`.
- Group outcomes are collected in slot order; the **first failure in system
  order fails the tick** — the identical error the serial path reports.
  (Later systems may have wasted work; nothing is merged on failure.)
- Failed ticks abort the tick transaction: zero authoritative mutation, zero
  changes, zero events, no WAL — Phase 9 semantics unchanged.
- Events are buffered per child and merged in system order with the same
  budget check (`append_events`); exceeding the budget fails the tick
  deterministically, exactly as serial does (the failure is raised at merge
  rather than mid-emit; the tick outcome is identical because the event
  buffer never escapes a failed tick).
- The RNG was already per-system (`rng_seed(world_seed, tick, system_id)`) —
  a pure function — so parallel and serial systems draw identical streams
  with no shared state.

## 8. Workers and the reference mode

- Parallel execution uses `std::thread::scope` with a worker budget `N`
  (from `ExecutionMode::Parallel(N)`): a group's slots are distributed
  round-robin over `min(N, members)` threads; results are collected by slot,
  never by completion order. No external crates, no shared mutable state
  beyond the slot-indexed result array.
- `ExecutionMode::Serial` (the default) runs the exact Phase 9 loop — the
  oracle, always available.
- `ExecutionMode::Parallel(N)` runs the plan-based executor. Tests prove
  `Serial == Parallel(1) == Parallel(2) == Parallel(4) == Parallel(8)` on
  identical worlds (see §10).

## 9. WASM and native reducers

- Native reducers: invoked through `SimulationContext` against the child
  transaction; their reads/writes land in the child and merge with the tick —
  identical to serial (a whole tick still commits atomically).
- WASM reducers: `WasmModuleRegistry::invoke_in_tx(&self, …)` is
  `Sync`-safe — the registry holds only compiled `wasmi::Module`s and the
  `Engine`; every invocation builds a fresh `Store` with fresh host state on
  the calling thread. A WASM reducer therefore runs inside a child's sandbox
  exactly as it runs in serial, with the same fuel/memory/host-call budgets.

## 10. Testing strategy

- **Worker-count independence**: identical worlds (same seed, systems,
  inputs) run with `Parallel(1)`, `Parallel(2)`, `Parallel(4)`,
  `Parallel(8)` over many ticks; per-tick (changes, events) traces and the
  final store dump must be equal.
- **Oracle equality**: `Serial` vs `Parallel(4)` on the same world must be
  byte-identical (traces + final state), including with interleaved
  same-table writes across groups (the provisional-id and coalescing proof
  cases).
- **Conflict model**: write/write, write/read, read/write systems remain
  correct; an undeclared overlap produces a deterministic tick error, never
  a wrong state.
- **Failure semantics**: system error, system panic, native reducer failure,
  WASM trap/fuel exhaustion — zero authoritative mutation, identical error,
  in both modes.
- **Plan determinism**: same systems → same groups; unknown declared table →
  deterministic error.
- **Scaling**: 100 independent systems across 10 tables.

## 11. Benchmarks

`examples/parallel_bench.rs` (baseline, no tuning):

- serial vs parallel(1/2/4/8) tick latency
- 10 independent systems (10 tables), 100 systems, conflicting systems,
  mixed workload
- scheduler/dependency-analysis overhead (plan build time) and
  synchronization overhead (group join + merge)

Correctness first: these are honest baselines for Phase 15.

## 12. Known limitations (Phase 11)

- **Declaration validation is a Parallel-mode check.** `TickPlan::build`
  rejects an unknown declared table, so a *misconfigured* world (a system
  declaring a table that does not exist) fails its tick in Parallel mode but
  not in Serial mode (Serial cannot validate — it has no declarations). This
  is the one place the two modes differ, and only for invalid
  configurations; it is deterministic and documented.
- Table-granularity conflict model — row-level declaration is future work.
- `branch_of` copies the parent's write set per child: O(members x writes)
  per group. Fine for typical ticks; a borrowed overlay is the optimization
  path later.
- Opaque (undeclared) systems never run in parallel — the safe default.
- The scheduler is a greedy single-pass planner; optimal grouping is future
  work.
- No automatic retry on `Error::Conflict` — identical to Phase 9 (the
  tick fails; the runtime's tick-failure policy applies).

## 13. Interface Phase 12 consumes

Phase 12 (multi-partition) consumes the **same** `World::tick`/`TickResult`
boundary — a partition is a World whose `SimulationConfig` may select either
execution mode. Cross-partition messaging is added on top (Phase 12 design)
without changing the concurrency model.
