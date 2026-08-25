# AGENTS.md

Rust 2024 workspace (needs a recent stable toolchain). No existing AGENTS/CLAUDE instructions were present; this file was derived from CI config, CONTRIBUTING.md, and the code.

## Verification (mirrors .github/workflows/ci.yml)

Run in this order; all must pass:

```bash
cargo fmt --all -- --check
cargo clippy --workspace --all-targets --all-features -- -D warnings
cargo test --workspace
cargo doc --workspace --no-deps   # CI step — don't add new broken intra-doc links (existing rustdoc warnings are tolerated)
```

CI additionally runs tests on Linux/macOS/Windows and a release build.

- Single crate/test: `cargo test -p nexum-tx` or `cargo test -p game-server --test gameplay wasm_fire_weapon`
- Release builds are slow (fat LTO + `codegen-units = 1`) but required for any benchmark or CCU measurement.

## Architecture

Cargo workspace; dependency graph = architecture map. Every crate depends on `nexum-core`, which depends on nothing.

Execution flow: Input → Network/Gateway → World → Reducer → Transaction (OCC) → Commit → `Vec<Change>` → Subscriptions → Gateway → Clients.

Three similarly named things, easy to confuse:

- `crates/nexum-game-server/` — reusable game-server *framework* (games, players, deny-by-default reducer exposure). Contains no game mechanics; gameplay state lives only in the simulation.
- `crates/nexum-server/` — runnable demo of the full stack, no gameplay.
- `crates/game-server/` — the actual playable arena game: `cargo run -p game-server -- server|client`.

## Gotchas

- **Root `tests/` directories are not real tests** — they hold only READMEs; cargo does not auto-discover tests there. Per-crate tests go in that crate's `tests/`; cross-crate tests go in `crates/nexum-integration-tests/`.
- **WASM reducers need no wasm32 toolchain**: modules are inline WAT strings parsed at runtime via the `wat` crate (see `crates/game-server/src/wasm.rs`). Host ABI is a single `("nexum","op")` host function; wire formats documented in that file.
- **Unsafe is contained by convention**: the workspace lint is `unsafe_code = "allow"` (root Cargo.toml), but unsafe exists only in `nexum-wasm/src/linker_cache.rs`, `nexum-network/src/spsc.rs`, and `nexum-alloc-count`. Don't add new unsafe without strong justification.
- **`panic = unwind` in release is deliberate** — reducer/WASM isolation relies on `catch_unwind`. Do not change to `panic = abort`.
- **Allocation profiling is opt-in**: `--features ccu-alloc` on `game-server` swaps in the counting allocator from `nexum-alloc-count` (the one crate allowed to be unsafe by design). Timing runs must not use it.

## Performance-change workflow (repo convention)

Measure first, then change, then re-measure with the same harness; include before/after numbers. Optimizations without measured improvement are reverted (precedent: Phase 21 D2).

```bash
# micro/scale benchmarks
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal
cargo run --release -p nexum-bench -- --scale 1_000_000
# CCU harness (profiles A=idle B=movement C=realistic E=extreme)
cargo run --release -p game-server --example ccu -- --clients 1000 --profile C --ticks 100
```

Each roadmap phase gets an ADR in `docs/architecture/`, design notes in `docs/design/`, and a report in `docs/reports/` (numbered by phase). Commit messages follow `Phase NN: <change>`.
