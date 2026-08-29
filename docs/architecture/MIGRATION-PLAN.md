# MIGRATION-PLAN.md — Execution Sequence

> This document defines the exact sequence of implementation steps for the
> Nexum architecture rework from "simulation engine" to "database as server."

---

## Guiding Rules

1. **cargo test --workspace** and **cargo clippy** must pass after every step.
2. No destructive changes until the replacement is verified.
3. Benchmark before and after every performance-sensitive change.
4. Each step is independently committable.
5. Steps may be reordered if dependencies allow, but the listed order is safe.

---

## Phase 1: Rename Core Concepts (Low Risk)

These are naming changes that do not alter behavior. They establish the new
vocabulary.

### Step 1.1: Rename `nexum-simulation` → `nexum-execution`

**Files affected:**
- `crates/nexum-simulation/Cargo.toml` → rename to `crates/nexum-execution/`
- `crates/nexum-execution/src/lib.rs` → update module docs
- All crates that depend on `nexum-simulation` → update to `nexum-execution`
- Root `Cargo.toml` → update workspace member path
- `docs/architecture/*.md` → update references

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
grep -r "nexum-simulation" --include="*.toml" --include="*.rs" | grep -v target/
```

### Step 1.2: Rename `World` → `Partition` (internal)

**Files affected:**
- `crates/nexum-execution/src/world.rs` → rename to `partition.rs`
- `crates/nexum-execution/src/lib.rs` → update re-exports
- `crates/nexum-runtime/src/world.rs` → rename to `partition.rs` (internal)
- `crates/nexum-runtime/src/runtime.rs` → update internal naming
- All references to `World` → `Partition` within execution/runtime crates

**Note:** Keep `World` as a type alias temporarily for backward compatibility:
```rust
pub type World = Partition;  // deprecated, will be removed
```

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 1.3: Rename `SimulationContext` → `ExecutionContext`

**Files affected:**
- `crates/nexum-execution/src/context.rs` → rename struct
- `crates/nexum-execution/src/lib.rs` → update re-exports
- `crates/nexum-execution/src/partition.rs` → update usage
- `crates/nexum-runtime/src/runtime.rs` → update usage

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 1.4: Rename `SimulationConfig` → `PartitionConfig`

**Files affected:**
- `crates/nexum-execution/src/config.rs` → rename struct
- `crates/nexum-execution/src/lib.rs` → update re-exports
- `crates/nexum-runtime/src/runtime.rs` → update usage

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 1.5: Remove "simulation" from documentation

**Files affected:**
- All `docs/architecture/*.md` files
- All crate-level doc comments
- README.md
- AGENTS.md

**Verification:**
```bash
grep -ri "simulation" docs/ --include="*.md" | grep -v "deterministic" | grep -v "Migration"
```

---

## Phase 2: Remove System Abstraction (Medium Risk)

The "system" concept is an unnecessary layer between the developer and the
transaction. All logic runs through reducers.

### Step 2.1: Remove `SystemRegistry` and `SystemDefinition`

**Files affected:**
- `crates/nexum-execution/src/systems.rs` → delete or gut the file
- `crates/nexum-execution/src/lib.rs` → remove re-exports
- `crates/nexum-execution/src/partition.rs` → remove system execution from tick
- `crates/nexum-execution/src/schedule.rs` → keep (event scheduling stays)

**What stays:**
- `Schedule` / `ScheduledEvent` — the event scheduler is useful
- `DeterministicRng` — deterministic RNG is essential

**What goes:**
- `SystemDefinition` — reducers replace systems
- `SystemRegistry` — reducer registry replaces system registry
- `SystemAccess` — not needed without systems

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 2.2: Update `Partition::tick` to run only reducers

**Files affected:**
- `crates/nexum-execution/src/partition.rs` → simplify tick to:
  1. Process scheduled events (as reducer calls)
  2. Process input commands (as reducer calls)
  3. Process reducer calls from queue
  4. Commit

**Verification:**
```bash
cargo test --workspace
cargo test -p nexum-execution
cargo test -p nexum-runtime
```

### Step 2.3: Remove `SystemAccess` enum

**Files affected:**
- `crates/nexum-execution/src/systems.rs` → remove
- All references → remove

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

---

## Phase 3: Delete GameServer Layer (High Risk)

The GameServer layer adds no state systems (ADR-014 D10). Its functionality
moves into the Runtime and module-level reducers.

### Step 3.1: Move ReducerPolicy into Runtime

**Files affected:**
- `crates/nexum-game-server/src/policy.rs` → move types to `nexum-runtime`
- `crates/nexum-runtime/src/lib.rs` → add policy module
- `crates/nexum-network/src/policy.rs` → update references

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 3.2: Move player/game lifecycle into Runtime or module

**Analysis needed:** Determine where `GameLifecycle`, `PlayerState`, and
`PartitionState` move. Options:
- Into `nexum-runtime` (server-level metadata)
- Into module-level tables (game state is rows in tables)
- Into the SDK (client-side tracking)

**Files affected:**
- `crates/nexum-game-server/src/lifecycle.rs` → decompose
- `crates/nexum-runtime/src/` → add lifecycle types if appropriate

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 3.3: Redirect Gateway composition to Runtime directly

**Files affected:**
- `crates/nexum-network/src/gateway.rs` → accept Runtime directly (not via GameServer)
- `crates/nexum-runtime/src/runtime.rs` → expose Gateway-compatible interface

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 3.4: Delete `nexum-game-server` crate

**Files affected:**
- `crates/nexum-game-server/` → delete entire directory
- Root `Cargo.toml` → remove from workspace members
- All crates that depend on `nexum-game-server` → remove dependency
- `game-server` crate → update imports
- `nexum-server` crate → update imports

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
grep -r "nexum-game-server" --include="*.toml" --include="*.rs" | grep -v target/
```

### Step 3.5: Delete `nexum-server` demo binary

**Files affected:**
- `crates/nexum-server/` → delete entire directory
- Root `Cargo.toml` → remove from workspace members

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

---

## Phase 4: Simplify Runtime as Database Server (Medium Risk)

The Runtime becomes the Nexum Database Server entry point.

### Step 4.1: Add `NexumServer` public type

**Files affected:**
- `crates/nexum-runtime/src/server.rs` → new file, public `NexumServer` type

```rust
pub struct NexumServer {
    runtime: Runtime,
    gateway: NetworkGateway,
}

impl NexumServer {
    pub fn new(config: ServerConfig) -> Result<Self> { ... }
    pub fn register_module(&mut self, name: &str, bytes: &[u8]) -> Result<()> { ... }
    pub fn start(&mut self) -> Result<()> { ... }
    pub fn step(&mut self) -> Result<()> { ... }
    pub fn shutdown(&mut self) { ... }
}
```

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 4.2: Clean up Runtime public API

**Files affected:**
- `crates/nexum-runtime/src/runtime.rs` → rename `create_world` → `create_partition`, etc.
- `crates/nexum-runtime/src/lib.rs` → update re-exports

**Remove from public API:**
- `WorldFactory` type alias (internal detail)
- `WorldEntry` (internal)
- Direct worker manipulation (internal)

**Verification:**
```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Step 4.3: Update `game-server` to use NexumServer

**Files affected:**
- `crates/game-server/src/server.rs` → use `NexumServer` instead of `GameServer`

**Verification:**
```bash
cargo test -p game-server
cargo run --release -p game-server -- server --help
```

---

## Phase 5: Rework game-server as Module Example (Medium Risk)

The game-server crate becomes a module-based example of the Nexum Database
Server in action.

### Step 5.1: Convert game tables to module-style definitions

**Files affected:**
- `crates/game-server/src/game.rs` → define tables as `#[table]` structs
- Ensure all gameplay state is in tables, not in framework types

### Step 5.2: Convert game reducers to module-style definitions

**Files affected:**
- `crates/game-server/src/game.rs` → define reducers as `#[reducer]` functions
- `crates/game-server/src/wasm.rs` → keep WASM reducers, they are already module-style

### Step 5.3: Define subscriptions for client state

**Files affected:**
- `crates/game-server/src/game.rs` → add subscription definitions

### Step 5.4: Verify gameplay correctness

**Verification:**
```bash
cargo test -p game-server
cargo run --release -p game-server -- server --port 9337 --partitions 1 --hz 20
cargo run --release -p game-server -- client --name alice --auto 10
```

---

## Phase 6: Documentation and Benchmarks (Low Risk)

### Step 6.1: Update all architecture docs

**Files affected:**
- `docs/architecture/NEW-ARCHITECTURE.md` → update with final decisions
- `docs/architecture/*.md` → remove simulation terminology
- `AGENTS.md` → update with new architecture description

### Step 6.2: Rework benchmark suite

**Files affected:**
- `benchmarks/nexum-bench/src/main.rs` → update for new architecture
- Add module execution benchmarks
- Add subscription cost benchmarks
- Add end-to-end client latency benchmarks

### Step 6.3: Run full benchmark suite

**Verification:**
```bash
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub runtime wal
cargo run --release -p nexum-bench -- --scale 1_000_000
cargo run --release -p game-server --example ccu -- --clients 20000 --profile C --ticks 1000
```

---

## Phase 7: Profile and Optimize (Ongoing)

### Step 7.1: Profile the new architecture

```bash
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub runtime wal
```

### Step 7.2: Identify top bottleneck

Profile each stage of the critical path:
1. Client call → Gateway (network overhead)
2. Gateway → Runtime (routing overhead)
3. Runtime → Partition.tick (scheduling overhead)
4. Transaction::begin (setup overhead)
5. Reducer execution (native or WASM)
6. Transaction::commit (OCC + apply + collect)
7. WAL append (persistence overhead)
8. Subscription delta computation (filtering overhead)
9. Subscription fanout (delivery overhead)

### Step 7.3: Optimize the most expensive operation

For each bottleneck, determine:
- CPU cost
- Memory cost
- Allocation cost
- Whether it can be eliminated, batched, or moved off the critical path

### Step 7.4: Benchmark and verify correctness

```bash
cargo test --workspace
cargo clippy --workspace --all-targets --all-features -- -D warnings
# Run benchmark suite
# Compare before/after
```

### Step 7.5: Repeat

Continue until no major bottleneck remains or further optimization requires
disproportionate complexity.

---

## Dependency Graph After Migration

```
                        nexum-core  (leaf)
                       / |  |  \     \
                      /  |  |   \     \
              nexum-storage |  nexum-macros  nexum-alloc-count (leaf)
              /  |   |  |  |
             /   |   |  |  |
     nexum-table |  nexum-wal
         \   |   |  /
          \  |   | /
       nexum-tx  |
            \    |
         nexum-reducer
            /    \
           /      \
    nexum-wasm     \
   (wasmtime)       \
        \         nexum-subscription
         \        /         \
          \      /           \
     nexum-execution          \
     (renamed)                 \
          |                nexum-sdk
          |               /
     nexum-runtime -----/
     (rayon)            \
          \        nexum-network
           \       (tungstenite)
            \      /
        NexumServer (new)
              |
       game-server (example)
```

**Deleted crates:**
- `nexum-game-server` (merged into Runtime + module reducers)
- `nexum-server` (replaced by NexumServer)

**Renamed crates:**
- `nexum-simulation` → `nexum-execution`

---

## Risk Mitigation

### Before Each Phase

1. Run full test suite: `cargo test --workspace`
2. Run clippy: `cargo clippy --workspace --all-targets --all-features -- -D warnings`
3. Run benchmarks (if performance-sensitive phase)
4. Create a git branch for the phase

### After Each Phase

1. Run full test suite again
2. Run clippy again
3. Verify no regressions in benchmark numbers
4. Merge branch if all checks pass

### Rollback Plan

Each phase is independently committable. If a phase introduces unacceptable
regressions, revert the phase's commits and reassess.

---

## Estimated Effort

| Phase | Risk | Estimated Effort |
|---|---|---|
| Phase 1: Rename Core Concepts | Low | 1-2 hours |
| Phase 2: Remove System Abstraction | Medium | 2-4 hours |
| Phase 3: Delete GameServer Layer | High | 4-8 hours |
| Phase 4: Simplify Runtime | Medium | 2-4 hours |
| Phase 5: Rework game-server | Medium | 4-8 hours |
| Phase 6: Documentation + Benchmarks | Low | 2-4 hours |
| Phase 7: Profile and Optimize | Ongoing | Continuous |
| **Total** | | **15-30 hours** |

---

## Success Criteria

After all phases are complete:

1. `cargo test --workspace` passes
2. `cargo clippy --workspace --all-targets --all-features -- -D warnings` passes
3. No references to "simulation" in public API or documentation
4. No references to "GameServer" in public API (except example code)
5. No references to "World" in public API (only "Partition" internally)
6. No references to "System" or "SystemRegistry" in any code
7. The game-server example runs correctly with module-based architecture
8. Benchmark numbers are equal or better than pre-migration
9. The developer model is clearly: tables + reducers + indexes + subscriptions = module
10. The product identity is: Nexum = Database as Server
