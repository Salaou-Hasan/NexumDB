# Phase 18 — Multi-Core Runtime: Design (ADR-018)

Status: complete. The runtime's tick phase now executes **independent
worlds/partitions concurrently** on one thread per configured worker, while
the single-threaded path remains the correctness oracle. Per-world results,
events, metrics, and outbound messages merge in the same deterministic
`(worker_id, world_id)` order the serial path used, so parallel execution is
**observationally identical** to serial execution.

## 1. Problem (measured, from Phase 19/20)

The tick phase of `Runtime::step` / `step_detailed` ticked every running
world **serially** in `(worker_id, world_id)` order. The Phase 19–20
measurements showed the remaining linear cost is the world tick itself
(reducers + transaction + OCC + commit + subscription apply): ~12 ms per
1K movement calls on one world. With P partitions of N clients each, serial
execution is `P × world_tick(N/P)` — the per-world cost scales linearly
with active partitions even though no world reads another world's state
within a step.

The world model makes this embarrassingly parallel **by construction**:

- Each `World` owns its authoritative `TableStore`, RNG seed, systems,
  reducers, schedule, WAL, and subscription registry. Worlds never share
  mutable state during a tick.
- The delivery phase (ADR-012 D3) drains every world's inbound messages
  **before** any world ticks, so a world's delivered batch never depends on
  tick-phase ordering.
- Within a step, worlds do not read each other's state; cross-partition
  messages produced by a tick are queued to the destinations' **next** tick.

## 2. Design (ADR-018 D1–D4)

### D1 — Workers become execution threads (for the tick phase)

`RuntimeConfig.worker_count` was already the logical ownership model
(ADR-010 D2). Phase 18 makes `worker_count` also the **parallelism level**:

- `worker_count == 1` → the Phase 10 serial path (unchanged semantics, no
  thread spawn — the correctness oracle).
- `worker_count == W > 1` and more than one world is running → the tick
  phase spawns `min(W, running_worlds)` scoped threads; each thread
  processes a deterministic contiguous chunk of the ordered world list.

Thread spawn cost (~10–20 µs per thread) is amortized per 50 ms tick
budget at 20 Hz and is negligible against world ticks of hundreds of µs–ms.

### D2 — Collected outcomes, deterministic merge

The tick-entry body (frame pop, call drain, `World::tick_with_calls`, WAL
append, snapshot, subscription apply, outbound collection) is refactored
into a pure per-world function that **returns a `TickOutcome`** instead of
mutating the runtime's shared `metrics`, `events`, or `partitions`:

- per-world events are collected in emission order into a `Vec<RuntimeEvent>`
- metrics deltas are collected into a per-world delta
- outbound `PartitionMessage`s are collected in `send_to` order

After all threads join, the main thread **applies** the outcomes in the
exact `(worker_id, world_id)` order the serial path used: events pushed in
world order (bounded log truncation reproduces serial exactly), metric
deltas summed, outbound enqueued in world order. The merged state is
identical to serial execution.

### D3 — Delivery phase stays serial and precedes the tick phase

Unchanged (ADR-012 D3): every running world's deliverable inbound messages
are drained before any world ticks, so delivery order never depends on
tick-phase world order or thread scheduling.

### D4 — Determinism contract

Same seed + same initial state + same inputs + same reducer code + same
partition topology ⇒ same:

- final authoritative state
- `Vec<Change>` per world
- reducer-call results per world
- runtime events (order and content)
- outbound cross-partition messages (order and content)
- metrics

for **any** worker count. The serial path remains the oracle; the
determinism regression test runs an identical scenario at
`worker_count ∈ {1, 2, 4, 6}` through `step`/`step_detailed` and asserts
identical traces, events, and outbound messages.

## 3. Safety

- `unsafe_code = forbid` maintained: parallelism uses only
  `std::thread::scope` with disjoint `&mut` slices (`split_at_mut`) over the
  per-world slots — no unsafe, no interior mutability in the tick path.
- A panicking world tick is caught per-system (`run_system`, ADR-011 D4)
  exactly as in serial; `Wal`/`Snapshot`/`apply_changes` failures are typed
  errors, never panics. If a thread panics anyway, the worlds are
  reinserted before the panic propagates (the runtime is never left without
  its worlds).

## 4. What is NOT parallelized

- `World::tick_with_calls` internals: within-world parallelism remains the
  Phase 11 planner (`ExecutionMode::Parallel(workers)`), orthogonal to this
  phase. Cross-world parallelism does not change within-world semantics.
- The delivery phase, the merge phase, WAL files (per-world), and the
  subscription registry (per-world) — all per-world and already isolated.
- The gateway fan-out (`GameServer::step` → `gateway.fan_out_results`) —
  unchanged.

## 5. Benchmark plan

CCU harness (`crates/game-server/examples/ccu.rs`, in-process transport,
real gateway/runtime/world/SDK):

- workload: profile B/C across `--partitions P` worlds, `--clients C`
  distributed `principal % P`
- workers: 1 (serial baseline) → 2 → 4 → 8 → 12 → 24
- metrics: round-trip tick p50/p95/p99, avg; world-tick sub-phase;
  accepted/dropped/rejected; speedup vs the 1-worker baseline; parallel
  efficiency = speedup / workers

Reported honestly: if `P` worlds are independent, expect near-linear
speedup until CPU saturation; scheduler overhead is measured, not assumed.
