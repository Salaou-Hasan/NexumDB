# Phase 18 — Multi-Core Runtime: Architecture (ADR-018)

## ADR-018: Parallel world/partition tick execution

- **Status**: accepted (Phase 18).
- **Scope**: the runtime's tick phase (`Runtime::step`, `step_detailed`).
  Does not change `World::tick` internals, transactions, OCC, the commit
  path, WAL, subscriptions, or the network gateway.

### Context

Serial world ticking makes the tick phase cost `Σ world_tick(w)` even
though worlds are fully independent within a step (per-world stores, seeds,
WALs, subscription registries; the delivery phase precedes the tick phase
— ADR-012 D3). Phase 19–20 profiling showed the remaining linear cost is
the per-world tick, so the multi-core runtime is the next measured lever.

### Decision

1. **Workers are execution threads during the tick phase.** With
   `worker_count == W > 1`, each step spawns `min(W, running_worlds)`
   scoped threads; each thread processes a deterministic chunk of the
   ordered world list. `worker_count == 1` keeps the serial path (no
   spawn, the correctness oracle).
2. **The tick body collects, the main thread merges.** The per-world tick
   (frame pop, call drain, `tick_with_calls`, WAL, snapshot, subscription
   apply) returns a `TickOutcome` — events in emission order, metric
   deltas, outbound messages — instead of mutating shared runtime state.
   The main thread applies outcomes in `(worker_id, world_id)` order:
   events pushed in world order (bounded-log truncation matches serial),
   metrics summed, outbound enqueued in world order.
3. **Delivery phase stays serial and precedes the tick phase** (unchanged).
4. **Threads share no mutable state**: disjoint `&mut` slices via
   `split_at_mut` over per-world slots; only immutable references
   (`&RuntimeConfig` scalars, `&delivered`) cross threads. `unsafe_code =
   forbid` maintained.
5. **Worlds are reinserted even on a thread panic**: the take-out /
   parallel / put-back sequence is panic-guarded so a panic can never
   leave the runtime without its worlds.

### Consequences

- Determinism is preserved by construction: per-world outcomes are
  independent, and the merge is in the exact serial order. Same seed +
  inputs + reducer code + topology ⇒ same state, `Vec<Change>`, events,
  outbound messages, and metrics at any worker count.
- `worker_count` remains a pure performance knob (ADR-010 D2): results
  never depend on it.
- The game-server host loop is unchanged: `GameServer::step` still calls
  `runtime.step_detailed()` and fans results out per world in the same
  order.

### Alternatives considered

- **Rayon `par_iter_mut`**: adds a dependency; `std::thread::scope` +
  `split_at_mut` gives identical semantics with zero new deps.
- **Persistent worker threads + channels**: more moving parts, ordering
  risk; scoped threads amortize to ~µs per step and keep the runtime
  synchronous `&mut self` model intact.
- **Parallelizing inside `World::tick`**: already Phase 11
  (`ExecutionMode::Parallel`), orthogonal and unchanged.
