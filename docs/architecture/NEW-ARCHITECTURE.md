# NEW-ARCHITECTURE.md — Nexum: Database as Server

> This document defines the target architecture for Nexum after the complete
> rework from "simulation engine with a database" to "database that IS the server."

---

## 1. Design Principles

### P1: The Database Is the Server

Nexum is a realtime, transactional, authoritative database server. Application
logic lives inside database modules. The database itself is the authoritative
backend. There is no separate game server, simulation server, or
synchronization layer.

### P2: One Authoritative State

There is exactly one `TableStore` — the authoritative representation of all
application state. No simulation state, no database state, no cache state.
One truth. Reducers operate on this state through transactions.

### P3: Reducers Are Server Functions

A reducer receives input, reads database state, performs logic, writes database
state, and commits atomically. Reducers are the primary server-side execution
primitive. There are no separate "systems" or "simulation loops" — reducers
drive all state transitions.

### P4: The Module Is the Application

A Nexum module defines the complete authoritative application behavior: tables,
indexes, reducers, and subscriptions. A game developer defines their backend as
a module. They do not build a traditional backend server.

### P5: Transactions Guarantee Correctness

Every state transition is a transaction. Failed transactions produce zero
authoritative mutation. OCC correctness, deterministic ordering, and atomic
commits are non-negotiable.

### P6: Change Stream Is the Propagation Mechanism

`Vec<Change>` from `Transaction::commit()` is the single canonical output of
every state transition. WAL, subscriptions, replication, and client updates all
observe this same stream. No separate authoritative pipelines.

### P7: Mechanisms Are Implementation Details

Deterministic execution, OCC, Vec<Change>, WAL, subscriptions, partitioning,
parallel execution, Wasmtime sandboxing, profiling — these are mechanisms that
serve the database-as-server architecture. They are not the organizing principle.

### P8: Measure, Don't Claim

Performance is measured, not claimed. Every optimization must include before/after
measurements under defined workloads. The aspirational target is <3ms p99
authoritative execution latency at 20K real gameplay clients on a single machine.

### P9: Correctness > Performance

Priority order: correctness, determinism, data integrity, tail latency, median
latency, throughput, memory efficiency. Never sacrifice correctness for benchmarks.

### P10: Extreme Simplicity

The core conceptual model must remain close to: TABLE, INDEX, REDUCER,
TRANSACTION, SUBSCRIPTION, MODULE. Everything else supports these primitives.
The developer should not need to understand OCC internals, WAL, network packets,
or partition routing unless they explicitly need advanced control.

---

## 2. Target Architecture

```
                        NEXUM DATABASE SERVER
                                 │
                    ┌────────────┼────────────┐
                    │                          │
                  STATE                       LOGIC
                    │                          │
                 Tables                     Reducers
                 Indexes                    Modules
                    │                          │
                    └────────────┬────────────┘
                                 │
                           Transactions
                                 │
                               OCC
                                 │
                             COMMIT
                                 │
                          Vec<Change>
                    ┌────────────┼────────────┐
                    │            │            │
                   WAL     Subscriptions   Clients
                                 │
                              delivered
```

### What the Developer Sees

```
NEXUM DATABASE SERVER
  │
  ├── Module: my-game
  │     ├── Table: Player { id, position, health, ammo }
  │     ├── Table: Projectile { id, owner, position, velocity }
  │     ├── Index: player_by_match(match_id)
  │     ├── Reducer: move_player(player_id, position)
  │     ├── Reducer: fire_weapon(player_id)
  │     └── Subscription: players_in_match(match_id) → { id, position, health }
  │
  ├── Module: my-chat
  │     ├── Table: Message { id, channel, author, body, timestamp }
  │     ├── Reducer: send_message(channel, body)
  │     └── Subscription: channel_messages(channel)
  │
  └── Clients
        ├── Unity (connected via Nexum SDK)
        ├── Godot (connected via Nexum SDK)
        └── Custom (connected via Nexum SDK)
```

### What the Developer Does NOT See

- Transaction objects (unless explicitly needed)
- OCC validation logic
- WAL or persistence mechanics
- Network packet formats
- Replication protocols
- Worker scheduling
- Partition routing
- Simulation ticks or systems
- State broadcasting internals
- The word "simulation" in any API

---

## 3. Current → Target Mapping

### CRATE-LEVEL MAPPING

| Current Crate | Verdict | Target | Rationale |
|---|---|---|---|
| `nexum-core` | **KEEP** | `nexum-core` | Foundation: IDs, versions, timestamps, errors, Value, Row, Schema. No simulation-specific concepts. The database backbone. |
| `nexum-macros` | **KEEP** | `nexum-macros` | Proc macros for `#[table]`, `#[reducer]`. Essential for developer ergonomics. |
| `nexum-storage` | **KEEP** | `nexum-storage` | Storage engine: `TableState`, `StorageTable`, `ColumnarStore`, `Change`, `StoredRow`. Pure database machinery. No simulation concepts. |
| `nexum-table` | **KEEP** | `nexum-table` | Typed table layer over storage. `TableStore`, `Table`, indexes. Pure database. |
| `nexum-tx` | **KEEP** | `nexum-tx` | Transaction engine: `Transaction`, `ReadSet`, `WriteSet`, OCC commit. Pure database. The core correctness mechanism. |
| `nexum-wal` | **KEEP** | `nexum-wal` | Write-ahead log: `Wal`, `Snapshot`, `RecoveryReport`, `recover`. Pure persistence. No simulation concepts. |
| `nexum-reducer` | **KEEP** | `nexum-reducer` | Reducer API: `ReducerContext`, `ReducerArgs`, `ReducerRegistry`, `ReducerResult`. This IS the server function primitive. |
| `nexum-wasm` | **KEEP** | `nexum-wasm` | WASM sandbox: `WasmModuleRegistry`, Wasmtime execution, fuel/memory limits. Module runtime implementation detail. |
| `nexum-subscription` | **KEEP** | `nexum-subscription` | Subscription engine: `SubscriptionRegistry`, `Query`, `SubscriptionUpdate`, deltas. Core database primitive for realtime propagation. |
| `nexum-simulation` | **REWORK** | `nexum-execution` | Contains `World` (the tick loop), `SimulationContext`, `SystemRegistry`, `Schedule`, `DeterministicRng`, `PartitionMessage`. The `World` concept becomes the **partition executor** — the database's internal tick loop that processes queued reducer calls. "Simulation" terminology is removed from the public API. `SimulationContext` becomes a thinner `ExecutionContext`. `SystemRegistry` is **removed** — all logic runs through reducers, not separate "systems". `Schedule` stays as the event scheduler. `DeterministicRng` stays as the internal RNG. |
| `nexum-runtime` | **KEEP + SIMPLIFY** | `nexum-runtime` | Runtime coordinator: `Runtime`, `RuntimeConfig`, workers, worlds/partitions, WAL integration, subscription fanout. Keep. Remove "world" terminology in favor of "partition" or "database instance". The runtime IS the database server's internal scheduler. |
| `nexum-network` | **KEEP** | `nexum-network` | Network gateway: `NetworkGateway`, protocol, transport, auth, sessions. Keep. This is the database server's client connection layer. |
| `nexum-sdk` | **KEEP** | `nexum-sdk` | Client SDK: `Client`, `View`, `SubscriptionHandle`, `ReducerResult`. Keep. This is the database client library. |
| `nexum-game-server` | **DELETE** | — | Orchestration facade that wraps Runtime + Gateway + Simulation. In the new architecture, the Runtime IS the database server. GameServer adds no state systems (ADR-014 D10). Its lifecycle management, reducer exposure, and player tracking either move into the Runtime or become a thin application-level wrapper that does not warrant its own crate. |
| `nexum-server` | **DELETE** | — | Demo binary that wires the full stack. Replace with a `nexum-server` binary that starts the Nexum Database Server directly from the Runtime. |
| `game-server` | **REWORK** | `game-server` (example) | The playable arena game. Becomes an **example** that defines a Nexum module (tables, reducers, indexes, subscriptions) and runs it on the Nexum Database Server. The game logic lives inside the module, not in a separate server crate. |
| `nexum-integration-tests` | **KEEP** | `nexum-integration-tests` | Cross-crate tests. Keep. |
| `nexum-alloc-count` | **KEEP** | `nexum-alloc-count` | Allocation profiling. Keep. |
| `nexum-bench` | **REWORK** | `nexum-bench` | Benchmark suite. Rework to measure the new architecture: module execution latency, transaction throughput, subscription cost, end-to-end client latency. |

### CONCEPT-LEVEL MAPPING

| Current Concept | Target Concept | Action |
|---|---|---|
| "Simulation engine" | (removed from public API) | Delete from developer model. The tick loop is an internal mechanism of the database server. |
| "World" | "Partition" or "Database Instance" | Rename internally. One partition = one `TableStore` + one tick loop + one `SubscriptionRegistry`. |
| "System" | (removed) | All logic runs through reducers. "Systems" are an unnecessary abstraction between the developer and the transaction. |
| "SystemRegistry" | (removed) | Replaced by `ReducerRegistry`. Reducers ARE the server functions. |
| "SimulationContext" | `ExecutionContext` | Thin wrapper over `&mut Transaction` + `&TableStore`. No system-specific fields. |
| "SimulationConfig" | `PartitionConfig` | Configures tick rate, RNG seed, execution mode. No "simulation" naming. |
| "InputFrame" | `InputFrame` | Keep. A batch of reducer calls + input commands for one tick. |
| "InputCommand" | `ReducerCall` | Simplify. All inputs are reducer calls. No separate "command" concept. |
| "TickResult" | `TickResult` | Keep internally. The result of one tick: `Vec<Change>`, `Vec<ReducerEvent>`, success/failure. |
| "GameServer" | (deleted) | Replaced by the Runtime itself. The database IS the server. |
| "GameInstance" | (moved to module) | Game instances are rows in a `Game` table, managed by reducers. Not a framework concept. |
| "PlayerRecord" | (moved to module) | Player identity is a row in a `Player` table, managed by reducers. Not a framework concept. |
| "GamePolicyTable" | `ReducerPolicy` (in Runtime) | Authorization stays: which reducers are exposed to which clients. Moves into the Runtime's configuration. |
| "Simulation-driven" | "Reducer-driven" | State transitions happen through reducer calls, not a simulation loop. The Runtime processes queued reducer calls per partition per tick. |

### ARCHITECTURE-LEVEL CHANGES

| Current Flow | Target Flow | Change |
|---|---|---|
| Client → SDK → Gateway → Runtime → World::tick → Transaction → Commit → Vec<Change> → WAL + Subscriptions → Clients | Client → SDK → Gateway → Runtime → Partition.tick → Transaction → Commit → Vec<Change> → WAL + Subscriptions → Clients | Rename "World" to "Partition". Remove "simulation" from the path. |
| World owns TableStore, runs systems + reducers in one tick | Partition owns TableStore, runs only reducers in one tick | Remove the "systems" concept. All logic is reducer logic. |
| GameServer → Gateway → Runtime → World | Gateway → Runtime → Partition | Delete GameServer layer. Gateway sits directly on Runtime. |
| Developer defines: systems, reducers, WASM modules, game_server | Developer defines: tables, indexes, reducers, subscriptions, modules | Simplify the developer model. |

---

## 4. Module Model

A Nexum module defines the complete server-side behavior of an application.

### Module Structure

```
my-game/
  src/
    tables.rs      -- Table definitions (#[table] structs)
    reducers.rs    -- Reducer functions (#[reducer])
    indexes.rs     -- Index definitions
    subscriptions.rs -- Subscription queries
  nexum.toml       -- Module manifest
```

### Module Manifest (nexum.toml)

```toml
[module]
name = "my-game"
version = "0.1.0"

[module.runtime]
max_fuel = 100_000_000
max_memory_bytes = 268_435_456    # 256 MB
max_instance_bytes = 67_108_864   # 64 MB
```

### Module Registration

Modules are registered with the Nexum Database Server at startup:

```rust
let mut server = NexumServer::new(config);
server.register_module("my-game", module_bytes)?;
server.start()?;
```

### Module Isolation

Each module executes in its own WASM sandbox (Wasmtime). Modules cannot access
each other's state. Cross-module communication happens through reducer calls
with explicit arguments — never through shared memory.

### Module Lifecycle

1. **Register**: Module WASM bytes are compiled and validated at startup.
2. **Instantiate**: Each reducer call creates a fresh or pooled execution context.
3. **Execute**: The reducer reads/writes tables within a transaction.
4. **Commit**: The transaction commits atomically, producing `Vec<Change>`.
5. **Propagate**: Changes flow to WAL, subscriptions, and clients.

---

## 5. Table Model

Tables are the authoritative state container. Every piece of application state
lives in a table.

### Table Definition

```rust
#[table]
struct Player {
    #[primary_key]
    id: PlayerId,

    position_x: f32,
    position_y: f32,
    health: i32,
    ammo: u32,
    match_id: MatchId,
}
```

### Generated Code

The `#[table]` macro generates:
- Schema definition (column types, primary key)
- Serialization/deserialization
- CRUD methods: `Player::get(ctx, pk)`, `player.save(ctx)`, `Player::create(ctx)`, `Player::delete(ctx, pk)`
- Query methods: `Player::all(ctx)`, `Player::filter(ctx, predicate)`

### Table Properties

| Property | Description |
|---|---|
| Schema | Fixed columns with typed `Value` variants |
| Primary Key | One column, typed, unique |
| Row Versioning | Every row has a `Version` for OCC |
| Change Tracking | Every mutation produces a `Change` record |
| Epoch Counter | Incremented on every commit touching the table |
| Indexes | Secondary indexes for efficient lookups |

### Table Internals (unchanged)

- `StorageTable` owns `TableState` with `BTreeMap<RowId, StoredRow>`
- `ColumnarStore` provides column-oriented reads for subscription scans
- `RowRef` provides zero-copy column access
- `Change` records are `Arc<Row>` shared across consumers

---

## 6. Reducer Model

Reducers are the primary server-side execution primitive. A reducer is a
function that receives input, reads database state, performs logic, writes
database state, and commits atomically.

### Reducer Definition

```rust
#[reducer]
fn move_player(
    ctx: &mut Context,
    player_id: PlayerId,
    position_x: f32,
    position_y: f32,
) -> Result<()> {
    let mut player = Player::get(ctx, player_id)?;

    player.position_x = position_x;
    player.position_y = position_y;

    player.save(ctx)?;
    Ok(())
}
```

### Reducer Execution

1. The Runtime receives a `ReducerCall` from a client or from the internal scheduler.
2. The Runtime creates a `Transaction` for the tick (one transaction per tick per partition).
3. The reducer is invoked with a `ReducerContext` wrapping `&mut Transaction` + `&TableStore`.
4. The reducer reads/writes tables through the context.
5. On success: writes are in the tick's transaction.
6. On failure: the transaction is rolled back to the snapshot. Zero mutation.

### Reducer Types

| Type | Trigger | Use Case |
|---|---|---|
| Client Reducer | Client SDK call | Player actions, queries |
| Scheduled Reducer | Timer/schedule | Periodic game logic |
| System Reducer | Internal | Automated state transitions |

### Reducer Isolation

- Native reducers: `catch_unwind` wraps execution. Panics abort the transaction, not the process.
- WASM reducers: Wasmtime sandbox with fuel limits, memory limits, and host ABI restrictions.

### Reducer Registry

The `ReducerRegistry` maps reducer names to their implementations. At startup,
all reducers (native and WASM) are registered. The registry is immutable after
initialization.

---

## 7. Transaction Model

The transaction model is unchanged from the current architecture. OCC provides
serializable isolation without locking.

### Transaction Lifecycle

```
BEGIN
  │
  ├── READ PHASE (during execution)
  │     ├── Row reads → ReadSet: (TableId, RowId) → Option<Version>
  │     └── Table scans → ReadSet: TableId → epoch
  │
  ├── WRITE BUFFER (during execution)
  │     └── WriteSet: (TableId, RowId) → WriteEntry (Insert/Update/Delete)
  │
  ├── VALIDATION (at commit)
  │     ├── Table epoch check (phantom protection)
  │     ├── Row version check (lost update detection)
  │     └── Uniqueness check (unique index integrity)
  │
  ├── APPLY (if validation passes)
  │     ├── Deletes first (ascending TableId, RowId)
  │     └── Upserts second (ascending TableId, RowId)
  │
  └── COLLECT
        └── Vec<Change> (per-table delta in TableId order)
```

### Invariants

- **Failed transaction = zero authoritative mutation.** This is non-negotiable.
- **Deterministic ordering.** Same inputs always produce the same commit order and same `Vec<Change>`.
- **Read-your-writes.** Reducers always see the effects of their own earlier writes within the same transaction.
- **Atomic commits.** The entire tick's changes commit or abort together.

### OCC Correctness

- Version-based detection of lost updates and phantoms.
- Uniqueness enforcement on unique indexes.
- Conflict → transaction abort → zero mutation → caller may retry.

---

## 8. Subscription Model

Subscriptions are a fundamental database primitive for realtime state propagation.

### Subscription Definition

```rust
#[subscription]
fn players_in_match(match_id: MatchId) -> Query {
    Query::builder("players")
        .predicate_eq("match_id", match_id)
        .project(["id", "position_x", "position_y", "health"])
        .build()
}
```

### Subscription Flow

```
Client subscribes → SubscriptionRegistry
                          │
                     Query compiled
                          │
                     Initial snapshot delivered
                          │
              ┌───────────┴───────────┐
              │                       │
         On each commit:        Client receives:
     Vec<Change> arrives    SubscriptionUpdate {
     apply_changes()        inserts, updates, deletes
     interest filter        (only rows the client cares about)
     drain_updates()        }
```

### Interest Filtering

The subscription engine determines which rows changed and which clients care.
Efficient filtering avoids O(changes × all_clients).

Current implementation:
- Per-subscription query with predicates
- Columnar scan for efficient row-level filtering
- Delta computation against committed changes

### Subscription Guarantees

- Exactly-once delivery per commit
- Ordered delivery (per subscription)
- No silent drops (overflow → `Stale` status)
- Heartbeat mechanism for connection health

---

## 9. Persistence Model

Persistence is a database primitive, not a simulation concern.

### WAL

The write-ahead log is the durable record of all committed transactions.

```
Transaction committed → Vec<Change>
                          │
                    Wal::append(tx_id, changes)
                          │
                    Flush / Sync (per DurabilityPolicy)
```

### Snapshots

Periodic snapshots of the `TableStore` for fast recovery.

```
Snapshot::capture(store, lsn, path)
  → Full TableStore state
  → Metadata: next_table_id, next_transaction_id
```

### Recovery

```
recover(dir)
  → Load latest snapshot
  → Replay WAL from snapshot LSN
  → Rebuild TableStore
  → Return RecoveryReport
```

### Persistence Policies

| Policy | Behavior |
|---|---|
| `None` | No persistence (testing, development) |
| `Flush` | WAL flushed to OS page cache |
| `Sync` | WAL synced to disk (full durability) |

---

## 10. WASM / Module Runtime Model

The WASM runtime is an implementation detail of module execution.

### Runtime Configuration

```rust
WasmLimits {
    max_fuel: 100_000_000,        // instruction budget
    max_memory_bytes: 256 * 1024 * 1024,  // 256 MB
    max_instance_bytes: 64 * 1024 * 1024,  // 64 MB
}
```

### Execution Path

```
Reducer call → WasmModuleRegistry::invoke_in_tx()
                  │
              Fresh or Pooled path
                  │
              Store::new(engine, HostState)
                  │
              Fuel armed (set_fuel)
                  │
              Linker::instantiate()
                  │
              Guest calls host ABI ("nexum", "op")
                  │
              ReducerContext methods dispatch
                  │
              Return value / error / trap
```

### Host ABI

Single host function: `("nexum", "op")` with opcodes:
- `OP_GET`, `OP_CONTAINS`, `OP_SCAN`, `OP_LOOKUP_*` — reads
- `OP_INSERT`, `OP_UPDATE`, `OP_DELETE` — writes
- `OP_EMIT` — events

### Safety

- No WASI
- No unrestricted system calls
- Fuel-based instruction limiting
- Memory-based allocation limiting
- Host call budget limiting
- Sticky ABI errors (any failure aborts the invocation)

### Optimization Opportunities

- Module serialization (avoid re-validation)
- Pooled Store/Instance reuse
- Linker caching (~200ns vs ~2ms fresh)
- Cranelift speed optimization (already enabled)
- Future: compiled module caching, zero-copy data paths

---

## 11. Networking Model

The networking layer connects clients to the Nexum Database Server.

### Architecture

```
Client ←→ Nexum SDK ←→ TCP/WebSocket ←→ NetworkGateway ←→ Runtime
```

### NetworkGateway

The gateway handles:
- Connection management (TCP, WebSocket, in-memory for tests)
- Authentication (token-based, pluggable)
- Session lifecycle (attach, detach, reconnect)
- Protocol serialization (versioned, bounded, checksummed frames)
- Authorization (ReducerPolicy: which reducers are exposed)
- Rate limiting
- Backpressure

### Protocol

- Versioned binary frames
- Request/response correlation via request IDs
- Server request namespace (`SERVER_REQUEST_MSB`) for server-initiated calls
- Delta-based subscription updates (inserts, updates, deletes)

### Client Connection Flow

1. Client connects via TCP/WebSocket
2. Gateway authenticates (token or pluggable)
3. Client attaches to a partition
4. Client subscribes to queries
5. Client sends reducer calls
6. Gateway routes calls into the Runtime
7. Runtime processes calls in the tick
8. Subscription updates are fanned out to clients

---

## 12. Client Model

Clients are game engines, custom applications, or test harnesses that connect
to the Nexum Database Server.

### Client SDK

```rust
let mut client = Client::connect("ws://localhost:9337", token)?;

// Subscribe to player data
let sub = client.subscribe(
    Query::builder("players")
        .predicate_eq("match_id", my_match)
        .build()
)?;

// Call a reducer
let result = client.call_reducer("move_player", args! {
    "player_id" => player_id,
    "x" => 10.0,
    "y" => 20.0,
})?;

// Pump events (poll-based)
let events = client.pump()?;
for event in events {
    match event {
        ServerEvent::SubscriptionUpdate { id, update } => { /* apply to view */ }
        ServerEvent::ReducerResult { result } => { /* handle result */ }
        _ => {}
    }
}
```

### Client View

The `View` struct holds derived per-subscription client state. It reflects
server-side committed changes. This is the client's window into the authoritative
database state.

### Client Lifecycle

1. **Connect**: Establish transport (TCP/WebSocket)
2. **Authenticate**: Provide token or credentials
3. **Attach**: Join a partition/database instance
4. **Subscribe**: Register interest queries
5. **Interact**: Send reducer calls, receive updates
6. **Reconnect**: Resume session after disconnection

---

## 13. Partitioning Model

Partitioning is a scalability mechanism. It divides the database into
independent partitions, each with its own `TableStore`, tick loop, and
`SubscriptionRegistry`.

### Partition Architecture

```
Nexum Database Server
  │
  ├── Partition 0: { TableStore, tick loop, SubscriptionRegistry }
  ├── Partition 1: { TableStore, tick loop, SubscriptionRegistry }
  ├── Partition 2: { TableStore, tick loop, SubscriptionRegistry }
  └── Partition 3: { TableStore, tick loop, SubscriptionRegistry }
```

### Partition Routing

Clients are deterministically assigned to partitions:
```
partition = client_id % partition_count
```

### Partition Independence

Each partition processes reducer calls independently. There is no cross-partition
state sharing within a single tick. Cross-partition communication happens through
`PartitionMessage` — queued messages delivered in the next tick.

### Partition Configuration

```rust
RuntimeConfig::new()
    .with_partitions(4)
    .with_workers(4)
```

### Transparency

The developer should ideally not think about partitions. Nexum handles routing.
Partitions are an implementation scalability mechanism that does not leak into
the programming model.

---

## 14. Failure Model

### Core Invariant

**Failed transaction = zero authoritative mutation.**

### Failure Types

| Failure | Response | State Impact |
|---|---|---|
| Reducer rejection | Transaction rollback | Zero mutation |
| WASM trap | Transaction rollback | Zero mutation |
| WASM fuel exhaustion | Transaction rollback | Zero mutation |
| WASM memory exhaustion | Transaction rollback | Zero mutation |
| OCC conflict | Transaction rollback | Zero mutation |
| Native reducer panic | Transaction rollback (catch_unwind) | Zero mutation |
| Invalid arguments | Transaction rollback | Zero mutation |
| Not found | Transaction rollback | Zero mutation |
| Capacity exceeded | Transaction rollback | Zero mutation |

### Tick Failure

If any reducer call within a tick fails:
1. The failed call's writes are rolled back (snapshot/rollback).
2. The tick continues processing remaining calls.
3. The tick's final commit includes only successful calls.
4. OR: The entire tick is aborted (configurable via `TickFailurePolicy`).

### Persistence Failure

- WAL append failure: transaction is NOT committed. The caller receives an error.
- Snapshot failure: logged, does not block transactions.
- Recovery failure: server reports error, does not start with corrupted state.

### Network Failure

- Client disconnection: session is preserved for reconnection.
- Gateway failure: clients reconnect to the same or new gateway.
- Transport failure: protocol detects and reports errors.

---

## 15. Determinism Guarantees

Determinism is required for:
- OCC correctness (same inputs → same validation outcome)
- Replay safety (WAL replay produces identical state)
- Cross-partition consistency (same routing → same result)

### Deterministic Elements

| Element | Mechanism |
|---|---|
| Execution order | `(priority, SystemId)` ascending → `(reducer_name, call_order)` ascending |
| RNG | `DeterministicRng` seeded per (tick, partition). splitmix64. No OS entropy. |
| Table iteration | `RowId` ascending |
| Commit ordering | `(TableId, RowId)` ascending |
| Change collection | Per-table delta in `TableId` order |
| Subscription deltas | Deterministic row-level interest filtering |
| Partition routing | `client_id % partition_count` (pure function) |

### Non-Deterministic Elements (Prohibited)

- Wall clock timestamps (use `TickId` or logical timestamps)
- OS random number generators
- Thread scheduling order
- HashMap iteration order (use BTreeMap)
- Allocation addresses
- Pointer values for ordering

---

## 16. Performance Architecture

### Target

- 20,000 real gameplay clients
- <3 ms p99 authoritative execution latency
- Single machine, sufficiently powerful hardware

### Critical Path

```
Client call → Gateway → Runtime → Partition.tick
  → Transaction::begin()
  → Reducer execution (native or WASM)
  → Transaction::commit()
    → OCC validation
    → Apply writes
    → Collect Vec<Change>
  → WAL append
  → Subscription delta computation
  → Subscription fanout to clients
```

### Optimization Levers

| Lever | Current State | Potential |
|---|---|---|
| WASM execution | Wasmtime 48, pooled path ~16µs/call | Module caching, zero-copy |
| Transaction commit | ~100-300ns per transaction | Batch optimization |
| OCC validation | O(read_set + write_set) | Incremental validation |
| WAL append | ~1-5µs per append | Batched writes, io_uring |
| Subscription deltas | O(changes × subscriptions) | Interest tree, bloom filters |
| Serialization | ~100-500ns per message | Zero-copy, flatbuffers |
| Memory allocation | ~50-200ns per allocation | Arena allocation, object pooling |
| Index lookups | O(log n) BTreeMap | Hash indexes for O(1) |

### Allocation Philosophy

- Minimize per-tick allocations
- Use arena allocation for transaction-local data
- `Arc<Row>` for cross-consumer sharing (WAL + subscriptions)
- Object pooling for frequently created/destroyed types

---

## 17. Benchmark Methodology

### Workload Profiles

| Profile | Description |
|---|---|
| A — Idle | Connections only, no state changes |
| B — Movement | Position updates, low combat |
| C — Realistic | Movement + combat + inventory + chat |
| D — Combat-heavy | Frequent firing, damage, respawns |
| E — Extreme | All of the above at maximum rate |

### Scale Points

- 1,000 clients
- 2,500 clients
- 5,000 clients
- 10,000 clients
- 15,000 clients
- 20,000 clients

### Metrics

| Category | Metrics |
|---|---|
| Latency | p50, p95, p99, p99.9, max (server execution, end-to-end) |
| Throughput | Reducer calls/sec, transactions/sec, changes/sec |
| Resources | CPU utilization, memory usage, allocations/sec, cache miss rate |
| WASM | Store creation ns, instantiate ns, exec ns, total ns per call |
| Subscriptions | Delta computation ns, fanout ns, delivered updates/sec |
| Persistence | WAL append ns, snapshot capture ms, recovery time |

### Benchmark Commands

```bash
# Micro benchmarks
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal

# Scale benchmarks
cargo run --release -p nexum-bench -- --scale 1_000_000

# CCU benchmarks (profiles A-E)
cargo run --release -p game-server --example ccu -- --clients 20000 --profile C --ticks 1000
```

### Rules

1. Every optimization must include before/after numbers.
2. Same hardware, same workload, same measurement methodology.
3. Report p50, p95, p99, p99.9, max — not just averages.
4. Do not claim 20K until the actual workload succeeds at 20K.
5. Optimizations without measured improvement are reverted.

---

## 18. Migration Plan

### Phase 1: Documentation (NOW)
- [x] Map entire repository
- [x] Produce NEW-ARCHITECTURE.md
- [x] Produce MIGRATION-PLAN.md

### Phase 2: Rename and Restructure
- [x] Rename `nexum-simulation` → `nexum-execution`
- [x] Rename `World` → `Partition` (internal, `World` kept as a deprecated alias)
- [x] Rename `SimulationContext` → `ExecutionContext`
- [ ] Remove `SystemRegistry` and `SystemDefinition` — **DEFERRED** (would delete the tested Phase 11 parallel executor, which this doc (Section 6) says should stay in `nexum-execution`; also conflicts with MIGRATION-PLAN Step 2.2's pure-reducer model)
- [x] Remove `SimulationConfig` naming (→ `PartitionConfig`)
- [~] Remove "simulation" from all public documentation (crate docs updated; runtime error strings intentionally untouched)

### Phase 3: Delete GameServer Layer
- [x] Delete `nexum-game-server` crate (removed from workspace)
- [x] Delete `nexum-server` demo binary (removed from workspace)
- [~] Move `ReducerPolicy` into `nexum-runtime` (superseded: `NexumServer` defaults `AllowAllPolicy`)
- [ ] Move player lifecycle management into module-level reducers
- [ ] Move game instance management into module-level reducers
- [x] Move reducer exposure into Runtime configuration (via `NexumServer` + `AllowAllPolicy`)

### Phase 4: Simplify Runtime
- [x] Make Runtime the database server entry point
- [x] Add `NexumServer` as the public-facing server type
- [ ] Remove "world" from Runtime public API
- [ ] Clean up partition management

### Phase 5: Rework game-server Example
- [ ] Convert `game-server` crate to a module-based example
- [ ] Define tables, reducers, indexes, subscriptions as a module
- [ ] Run the module on `NexumServer`
- [ ] Verify gameplay correctness

### Phase 6: Rework Module API
- [ ] Ensure `#[table]` generates clean CRUD
- [ ] Ensure `#[reducer]` generates clean dispatch
- [ ] Add `#[subscription]` macro if beneficial
- [ ] Add `nexum.toml` manifest parsing

### Phase 7: Rework Benchmarks
- [ ] New benchmark methodology for module-based architecture
- [ ] Measure module execution latency
- [ ] Measure transaction throughput
- [ ] Measure subscription cost
- [ ] Measure end-to-end client latency at 20K scale

### Phase 8: Profile and Optimize
- [ ] Profile the new architecture
- [ ] Identify top bottleneck
- [ ] Optimize
- [ ] Benchmark
- [ ] Repeat

---

## 19. Removed Concepts

| Concept | Reason for Removal |
|---|---|
| "Simulation engine" | The database IS the server. There is no separate simulation. |
| "World" (public API) | Replaced by "Partition" — an internal scalability mechanism. |
| "System" / "SystemRegistry" | All logic runs through reducers. Systems are an unnecessary abstraction. |
| "GameServer" layer | The Runtime IS the database server. No separate game server wrapper needed. |
| "GameInstance" (framework) | Game instances are rows in a table, managed by reducers. Not a framework concept. |
| "PlayerRecord" (framework) | Player identity is a row in a table, managed by reducers. Not a framework concept. |
| "SimulationConfig" (public) | Renamed to `PartitionConfig`. No "simulation" naming. |
| "InputCommand" (separate type) | All inputs are reducer calls. No separate command concept. |
| "TickResult" (public API) | Internal implementation detail. The developer sees `Vec<Change>` from transactions, not tick results. |
| "WorldFactory" | The Runtime creates partitions internally. No external factory needed. |
| Separate "game server" binaries | The Nexum Database Server IS the server binary. Examples run ON the server. |

---

## 20. Remaining Risks

### R1: Tick-Based Execution Model

The current architecture uses a tick-based execution model where all reducer calls
within one tick share a single transaction. This is a simulation-engine concept
that may need rethinking.

**Risk**: If we keep the tick model, it is hard to explain as "database as server"
because traditional databases do not have ticks.

**Mitigation**: The tick is an internal batching mechanism. From the developer's
perspective, each reducer call is an independent transaction. The tick batches
multiple calls for efficiency. This is similar to how a database batches writes
in a commit group.

**Decision needed**: Should each reducer call be its own transaction (simpler
developer model, potentially lower throughput), or should calls be batched per
tick (higher throughput, more complex mental model)?

### R2: Removing GameServer May Lose Functionality

The GameServer layer provides: game instance lifecycle, player lifecycle,
reducer exposure policy, partition routing, and event forwarding.

**Risk**: If these are not properly preserved in the new architecture, game
developers lose important features.

**Mitigation**: Each feature is either:
- Moved into the Runtime (reducer exposure, partition routing)
- Moved into module-level reducers (game instances, player lifecycle)
- Preserved in the SDK (event forwarding)

### R3: Module API Maturity

The `#[table]` and `#[reducer]` macros (ADR-027) are currently a thin facade.
The new architecture requires them to be the primary developer interface.

**Risk**: The macros may need significant enhancement to provide a clean
developer experience.

**Mitigation**: Incremental enhancement. Start with the existing macros and
add features as needed based on developer feedback.

### R4: Performance Regression Risk

Restructuring the architecture may introduce performance regressions (extra
indirection, renamed paths, changed ownership patterns).

**Risk**: The migration may temporarily slow the system down.

**Mitigation**: Benchmark before and after every major change. Revert if
regression is detected without corresponding improvement elsewhere.

### R5: Single-Machine Constraint

The architecture is designed for single-machine operation. Multi-node
partitioning is a future concern.

**Risk**: The single-machine architecture may not cleanly extend to multi-node.

**Mitigation**: Design clean partition boundaries now. Validate single-node
ceiling first. Multi-node is a later phase.

### R6: Developer Model Transition

Existing users think in terms of "simulation engine." The new model requires
thinking in terms of "database as server."

**Risk**: Existing documentation, examples, and mental models are wrong.

**Mitigation**: Complete documentation rewrite. New examples. Clear migration
guide for existing users.

---

## Appendix: Mechanism Preservation Checklist

The following mechanisms are preserved from the old architecture. They serve
the new database-as-server architecture, not a simulation engine.

- [ ] Deterministic execution (deterministic RNG, deterministic ordering)
- [ ] OCC (read set, write set, validation, commit)
- [ ] `Vec<Change>` commit stream
- [ ] WAL (durable log, snapshots, recovery)
- [ ] Subscription engine (query compilation, interest filtering, delta delivery)
- [ ] Partitioning (independent partitions, deterministic routing)
- [ ] Parallel execution (rayon-based worker pool)
- [ ] Wasmtime sandboxing (fuel, memory, host ABI limits)
- [ ] Profiling infrastructure (micro benchmarks, CCU benchmarks)
- [ ] Allocation profiling (nexum-alloc-count)
- [ ] Zero-copy / Arc optimizations (shared Row across consumers)
- [ ] Authoritative state (one TableStore, one truth)
