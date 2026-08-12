# Phase 6 Design — Reducer API

Status: **design** (implementation follows in this phase)
Dependencies: Phases 1–5 (tables, storage, transactions + OCC, WAL/snapshots).
Out of scope: WASM, subscriptions, simulation, networking, distribution.

## 1. Purpose

A reducer is **authoritative server-side application logic**. It receives input,
executes against Nexum state through a controlled context, and either commits
atomically or aborts completely — never partially.

```
                    Reducer
                       │
                       ▼
                ReducerContext
                       │
                       ▼
                  Transaction
                       │
              ┌────────┴────────┐
              │                 │
           ReadSet           WriteSet
              │                 │
              └────────┬────────┘
                       ▼
                    OCC
                       │
                 ┌─────┴─────┐
                 │           │
               abort       commit
                             │
                             ▼
                       Vec<Change>
```

A reducer must **not** directly mutate `TableStore` or `StorageTable`. It
operates through `ReducerContext`, which delegates to the Phase 4 transaction
engine. There is exactly **one transaction per reducer invocation**.

## 2. Answers to the design questions

1. **What is a Reducer?** A named, registered unit of application logic
   (`ReducerDefinition`) that runs inside one transaction with a controlled
   context. It reads/writes state and optionally emits events.
2. **What is a ReducerId?** A strongly typed numeric id (`nexum-core`
   `ReducerId`), part of the existing typed-id family (`TableId`, `RowId`, ...).
   Identity is **numeric + symbolic**: the id is the stable handle; the name is
   a registry-unique human-readable symbol. Versioning is deferred (future
   deployments may version by name).
3. **What identifies a reducer invocation?** A `TransactionId`. Every
   invocation begins a fresh transaction whose id is returned in the result —
   the runtime's hook for WAL append and later metrics.
4. **What is ReducerContext?** The only surface a reducer sees. It wraps
   `&mut Transaction` + `&TableStore` (shared) + a transaction-local event
   buffer. It exposes exactly: `get`, `contains`, `scan`, `lookup_unique`,
   `insert`, `update`, `delete`, `emit`. It never exposes `&mut TableStore`.
5. **How does a reducer access tables?** By **name**, exactly like the
   transaction API (`ctx.get("players", id)`). Table names are resolved by the
   store per call; unknown names → `NotFound`.
6. **How does a reducer read rows?** `get(table, row_id) -> Option<Row>` through
   the transaction's logical view (read-your-writes, ADR-004 D12).
7. **How does a reducer insert rows?** `insert(table, row) -> RowId`. The id is
   **provisional** until commit (Phase 4 semantics: storage assigns the real id
   at commit; provisional ids are in-transaction handles).
8. **How does a reducer update rows?** `update(table, row_id, row)`. Coalescing
   rules and write-time version capture are inherited unchanged.
9. **How does a reducer delete rows?** `delete(table, row_id)`.
10. **How does a reducer perform scans?** `scan(table) -> Vec<(RowId, Row)>` —
    the transactional scan with table-epoch phantom observation (ADR-004 D13).
11. **How does a reducer emit events?** `emit(name, payload)`. Events are
    buffered transaction-locally and only escape with a successful commit.
12. **How are reducer arguments represented?** `ReducerArgs`, a `BTreeMap<name,
    Value>` — named, deterministic (sorted keys), protocol-independent,
    versionable (new optional keys don't break callers), and serializable via
    the existing value codec. Typed accessors (`require_u64`, `get_str`, ...)
    turn lookup errors into `InvalidArgument`/`NotFound` cleanly.
13. **How are reducer errors represented?** The shared `nexum_core::Error`.
    Application rejections are ordinary `Error` values returned by the
    reducer's `execute`. `Error::Conflict` is **never** wrapped — a caller can
    always distinguish "application logic rejected it" from "concurrent state
    changed".
14. **How is reducer execution isolated?** Execution is function-local: the
    reducer sees only its context, which delegates to a transaction it cannot
    commit or abort itself. Isolation = transaction isolation (OCC + epochs).
15. **What happens if a reducer panics?** `invoke` wraps execution in
    `catch_unwind`. A panic aborts the transaction, discards the write set and
    the event buffer, and surfaces as `Error::Internal("reducer 'x' panicked")`.
    Because writes are provisional, **no authoritative state is ever touched** —
    no rollback mechanism exists or is needed. Native reducers are **trusted
    code**; this is not a sandbox (that is WASM, Phase 7). Panic safety
    requires the default `panic = unwind` profile.
16. **What happens if a reducer returns an error?** The transaction is aborted
    explicitly; zero authoritative mutations, zero committed changes, zero
    events. The error propagates unchanged to the caller.
17. **What happens if OCC validation fails?** `commit` returns
    `Error::Conflict`; the transaction enters `Aborted`; the invocation fails
    with `Error::Conflict`; the caller may retry with a new invocation.
    **No automatic retry** — that is a future runtime policy.
18. **Can reducers call other reducers?** Not in Phase 6. Composition is
    ordinary Rust function calls within one reducer; a nested *invocation*
    (with its own transaction) is deliberately not exposed through the context.
    Documented as future work (reducer composition on top of the same
    transaction).
19. **Can reducers recursively invoke themselves?** No — there is no invocation
    surface on the context. (Also prevents unbounded recursion via the
    registry.)
20. **What is the transaction lifetime?** `begin` → `execute` → `commit` or
    `abort`. The reducer cannot extend or shorten it; the invocation owns it.
21. **What is the deterministic execution model?** All state APIs delegate to
    the deterministic transaction/table layers (BTreeMap ordering everywhere).
    Events are appended in `emit` order. The registry lists definitions sorted
    by id. The context exposes no wall-clock time and no randomness. Same
    state + same args → same logical result.
22. **What does a successful reducer return?** `ReducerResult { tx_id, changes,
    events, return_value }`. `changes` is the exact `Vec<Change>` from
    `Transaction::commit` (storage internals never leak into it). Note the
    **provisional-id footgun**: a reducer that returns its insert handle
    returns a provisional id (high bit set) — reducers should address rows by
    primary key; the committed `Change` records carry the real
    storage-assigned row ids (the lib doc example models this).
23. **What happens to emitted events if the transaction aborts?** Discarded.
    The event buffer dies with the invocation; nothing escapes. Event atomicity
    mirrors write atomicity (§5).
24. **Where does WAL attach?** Outside the reducer. The runtime performs
    `invoke → ReducerResult.changes → wal.append(tx_id, changes)`. The reducer
    never writes to WAL. **Committed-in-memory ≠ durable**: the configured WAL
    policy decides durability; reducer success alone does not imply it.
25. **How will subscriptions later observe reducer changes?** The same
    `Vec<Change>` boundary (ADR-005's change boundary) fans out to
    subscriptions in Phase 8.
26. **How will simulation later invoke reducers?** Simulation ticks will call
    the same `invoke` surface (Phase 9) — reducers are the "reducer invocation"
    step of the simulation pipeline.
27. **How will WASM eventually host reducers?** WASM reducers will conform to
    the same semantic model: a WASM module exports an `execute` equivalent
    behind a restricted host interface; the registry/result/error/event shapes
    carry over unchanged. Phase 6 deliberately does not design the WASM ABI.

## 3. Reducer identity

`ReducerId` already exists in `nexum-core` (`define_id!` newtype — numeric,
`Copy`, `Ord`, `Hash`, `Display`, converts to/from `u64`). A definition pairs it
with a registry-unique name:

```rust
pub struct ReducerDefinition {
    pub id: ReducerId,
    pub name: String,        // registry-unique symbol
    pub execute: ReducerFn,  // fn(&mut ReducerContext, &ReducerArgs) -> Result<Value, Error>
}
```

No version component yet: name is the stable symbol for callers; the numeric id
is the stable handle for the runtime. (Versioned deployments are future work.)

## 4. ReducerContext API

```rust
pub struct ReducerContext<'a> { /* tx: &'a mut Transaction, store: &'a TableStore, events: Vec<ReducerEvent> */ }

impl<'a> ReducerContext<'a> {
    pub fn get(&mut self, table: &str, row_id: RowId) -> Result<Option<Row>>;
    pub fn contains(&mut self, table: &str, row_id: RowId) -> Result<bool>;
    pub fn scan(&mut self, table: &str) -> Result<Vec<(RowId, Row)>>;
    pub fn lookup_unique(&mut self, table: &str, index_name: &str, key: &[Value]) -> Result<Vec<RowId>>;
    pub fn insert(&mut self, table: &str, row: Row) -> Result<RowId>;
    pub fn update(&mut self, table: &str, row_id: RowId, row: Row) -> Result<()>;
    pub fn delete(&mut self, table: &str, row_id: RowId) -> Result<()>;
    pub fn emit(&mut self, name: &str, payload: Value) -> Result<()>;
}
```

Every method delegates to the transaction with the store handle — the reducer
inherits read-your-writes, version OCC, missing-row observations, epoch scan
observations, and unique-key validation with **zero duplicated semantics**. The
context adds only the event buffer.

## 5. Event semantics (atomicity)

```
Reducer
   ├── writes          (provisional)
   └── emitted events  (transaction-local buffer)
            │
            ▼
      transaction result
            │
      ┌─────┴─────┐
      │           │
    abort       commit
      │           │
  discard       publish
```

If a reducer writes A, emits X, writes B, then fails: A and B are unchanged and
**X is discarded**. Events escape only with a successful commit, in `emit`
order. `ReducerEvent { name: String, payload: Value }` is deliberately small;
structured payloads can evolve without changing the buffer mechanics. No global
event bus in this phase.

## 6. Error model

| Situation | Error |
|---|---|
| Reducer application rejection | any `Error` returned by `execute` |
| Unknown reducer / table / row / index | `NotFound` |
| Duplicate registry entry / duplicate insert | `AlreadyExists` |
| Bad argument type, malformed key, empty event name | `InvalidArgument` |
| OCC validation failure | `Conflict` (never wrapped) |
| Reducer panic | `Internal("reducer 'x' panicked")` |
| Invariant violation | `Internal` |

The invocation returns `Result<ReducerResult, Error>`; the caller distinguishes
"application rejected it" (`Err` from execute) from "state changed under me"
(`Error::Conflict`) by matching the variant.

## 7. Panic behavior

`invoke` runs `execute` inside `std::panic::catch_unwind` (wrapped in
`AssertUnwindSafe` — the only state the closure captures is the context, which
is either dropped or committed by the caller afterwards). A caught panic:

1. aborts the transaction (no mutation — writes were provisional),
2. discards the event buffer,
3. returns `Error::Internal("reducer 'x' panicked")`.

`panic = unwind` (the workspace default) is required; under `panic = abort` a
panic cannot be caught and would kill the process — documented, not supported
in this phase. No unsafe, no rollback machinery.

## 8. Registry

```rust
pub struct ReducerRegistry {
    by_id: BTreeMap<ReducerId, ReducerDefinition>,   // deterministic listing
    by_name: BTreeMap<String, ReducerId>,            // deterministic name index
}
```

- `register(def) -> Result<()>` — duplicate id or name → `AlreadyExists`.
- `lookup(id)`, `lookup_by_name(name)` — `Option<&ReducerDefinition>`.
- `list()` — sorted by id (deterministic).
- `invoke(&self, store, name, args) -> Result<ReducerResult>` — the single
  execution entry point (§9).

No hot reload, no deployment, no WASM loading, no RPC — later phases.

## 9. Invocation algorithm

```
invoke(name, args)
  ├─ lookup definition (NotFound if absent)
  ├─ tx = Transaction::begin(store)          # one reducer = one transaction
  ├─ ctx = ReducerContext::new(&mut tx, store)
  ├─ outcome = catch_unwind(execute(ctx, args))
  ├─ events = ctx.take_events()
  ├─ Ok(Ok(value))      → changes = tx.commit(store)?
  │                       return ReducerResult { tx_id, changes, events, value }
  ├─ Ok(Err(error))     → tx.abort(); return Err(error)
  └─ Err(panic)         → tx.abort(); return Err(Internal "reducer 'x' panicked")
```

The tx id is recorded before execution and returned in the result so the
runtime can `wal.append(tx_id, changes)` — durability stays outside the reducer
(§2 Q24).

## 10. Determinism rules

- State reads/writes: delegated to the deterministic transaction engine.
- Scans and unique lookups: ascending `RowId` / sorted-key order (Phase 4
  correction semantics).
- Events: `emit` order within the transaction.
- Registry listing: ascending `ReducerId`.
- `ReducerArgs`: `BTreeMap` — iteration is key-sorted, not insertion-sorted.
- No wall-clock, no randomness, no environment access via the context.

## 11. Security boundary

**Native Rust reducers are trusted server code in Phase 6.** `ReducerContext`
is an API boundary, not a security boundary: a native reducer could construct
its own `TableStore` or `Transaction` if it had one — it simply isn't handed
one. The WASM runtime (Phase 7) will provide the real untrusted-code boundary
(memory/instruction limits, restricted host interface). Do not confuse the two.

## 12. WAL boundary

```
reducer invoke
   ↓
tx.commit → Vec<Change>     (committed in memory)
   ↓
wal.append(tx_id, changes)  (runtime; policy decides durability)
```

Reducer success = in-memory commit only. "Durable" requires the configured WAL
policy to complete. The reducer crate has no WAL dependency.

## 13. Testing plan (mapped to the brief)

- Basic: register → invoke → return value; duplicate id/name; deterministic
  listing.
- Reads: get / scan / lookup_unique through the context.
- Writes: insert / update / delete commit.
- Read-your-writes: insert→get, update→get, delete→get via context.
- Atomicity: multi-write reducer that fails later → zero mutations.
- Events: emit + commit → preserved, in order; emit + error → discarded;
  emit + conflict → discarded.
- Conflicts: read A, external tx modifies A, invoke → `Error::Conflict`.
- Multi-table: reducer writes A + B → atomic commit, changes span both.
- Panic: panicking reducer → `Error::Internal`, zero mutations, zero events.
- Registry: register/lookup/duplicates/listing order.
- Determinism: same state + args → identical result shape.

## 14. Benchmarks

Baseline scenarios (`examples/reducer_bench.rs`): empty reducer, read-only
reducer, single-write, multi-row (10), multi-table, event-emitting, conflicting.
Same harness as `tx_bench` (dependency-free `Instant`); criterion in Phase 15.

## 15. Non-goals this phase

WASM, subscriptions, simulation, networking, matchmaking, replication,
distributed execution, SQL parser/planner, authentication, client SDK,
automatic retry, hot reload, reducer-to-reducer invocation, global event bus,
versioned reducer deployment.
