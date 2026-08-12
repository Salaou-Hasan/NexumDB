# Nexum — Authoritative State Engine

> **State is authoritative. Transactions change state. Simulation produces state transitions. Subscriptions expose state.**

Nexum is a high-performance multiplayer backend and authoritative state engine written in Rust.
It is inspired by the ideas behind SpacetimeDB, Nakama, relational databases, realtime multiplayer
backends, and simulation engines — but it is **not** a clone of any of them. It is a new architecture.

## The three primitives

1. **Tables** — Tables are the authoritative state of the application/world. They carry schemas,
   typed columns, rows, primary keys, indexes, constraints, and version metadata.
2. **Reducers** — Reducers are authoritative state transitions. Each reducer executes inside a
   transaction (read → execute → write → validate → commit).
3. **Subscriptions** — Subscriptions are reactive views over authoritative table state, driven by
   committed change sets — never by polling.

Everything else — transactions (OCC), the memory-first storage engine, WAL + snapshots, the WASM
reducer sandbox, the simulation scheduler, the partition/worker runtime, and the QUIC/HTTP planes —
exists to make those three primitives reliable, deterministic, transactional, durable, scalable, and
extremely fast.

## Implementation status

The build follows a strict order: correctness first, distribution last.

| Phase | Area | Status |
|-------|------|--------|
| 0 | Repository / workspace foundation | ✅ Done |
| 1 | Core types and state model | ✅ Done |
| 2 | Table system | ✅ Done |
| 3 | Memory-first storage engine | ✅ Done |
| 4 | Transaction engine (OCC) | ✅ Done |
| 4c | Read-your-writes + phantom protection | ✅ Done |
| 5 | WAL, snapshots, recovery | ✅ Done |
| 6 | Reducer API | ✅ Done |
| 7 | WASM reducer runtime | ✅ Done |
| 8 | Subscription engine | ✅ Done |
| 9 | Deterministic simulation core | ✅ Done |
| 10 | Nexum runtime (partition/worker orchestration) | ✅ Done |
| 11 | Concurrency & parallel tick execution | ✅ Done |
| 12 | Multi-partition simulation (deterministic message bus) | ✅ Done |
| 13 | Networking + client SDKs (realtime gateway, reducer calls, `nexum-sdk`) | ✅ Done |
| 14 | Game server layer | ⬜ |
| 15 | Performance & benchmarking | ⬜ |
| 16 | Production hardening & release | ⬜ |

## Repository layout

```
crates/
├── nexum-core/         # Types & state model: ids, versions, timestamps, errors, interfaces
├── nexum-table/        # Typed table engine (Phase 2)
├── nexum-tx/           # OCC transaction engine (Phase 4 + read-your-writes/phantom fix)
├── nexum-storage/      # Memory-first authoritative storage (Phase 3)
├── nexum-wal/          # WAL, snapshots, recovery (Phase 5)
├── nexum-reducer/      # Reducer execution model (Phase 6)
├── nexum-wasm/         # WASM reducer sandbox (Phase 7): wasmi host, fuel/memory limits, ABI
├── nexum-subscription/ # Subscription engine (Phase 8): committed-change observation, bounded queries, resync
├── nexum-simulation/   # Deterministic simulation (Phases 9/11/12): one World = one partition, one tx per tick, systems/schedule/RNG, deterministic parallel ticks (Phase 11), cross-partition messaging (Phase 12)
├── nexum-runtime/      # Runtime + partitions (Phases 10/12): world lifecycle, ownership, input routing, WAL+subscription coordination, recovery, the partition registry and deterministic message bus
├── nexum-network/      # Realtime networking + control plane (Phase 13): versioned binary protocol, sessions/auth, gateway, transports, reducer-call routing, typed operator API (originally implemented ahead of the roadmap; now the canonical Phase 13 foundation)
├── nexum-sdk/          # Client SDK (Phase 13): poll-driven `Client`, canonical protocol codec, sessions, correlated reducer calls, derived subscription views, reconnect/resync
└── nexum-server/       # Server binary
tests/                  # Workspace-level test harnesses (organized by area)
benchmarks/             # Benchmark harnesses
docs/
├── architecture/       # Architectural decision records
├── protocols/          # Wire protocol design
└── design/             # Design notes
```

All crates depend on `nexum-core`; `nexum-core` depends on nothing. The dependency graph is the
architecture map.

## Building and testing

```bash
cargo build --workspace
cargo test  --workspace
cargo clippy --workspace --all-targets
```

## License

MIT — see [LICENSE](LICENSE).
