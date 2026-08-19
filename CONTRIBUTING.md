# Contributing to Nexum

Thank you for your interest in contributing to Nexum! This document provides guidelines and instructions for contributing.

## Development Setup

### Prerequisites

- Rust 2024 edition (stable)
- `cargo clippy` and `cargo fmt`
- Git

### Getting Started

```bash
# Clone the repository
git clone https://github.com/Salaou-Hasan/NexumDB.git
cd NexumDB

# Build the workspace
cargo build --workspace

# Run all tests
cargo test --workspace

# Run clippy
cargo clippy --workspace --all-targets --all-features -- -D warnings
```

### Running the Demo

```bash
# Terminal 1 — the authoritative game server
cargo run -p game-server -- server

# Terminal 2 — a client
cargo run -p game-server -- client --name alice
```

## Code Style

- **No unsafe code** — `unsafe_code = forbid` is enforced across all crates (except `nexum-alloc-count` for profiling).
- **Clippy clean** — All code must pass `cargo clippy --workspace --all-targets --all-features -- -D warnings`.
- **Rustfmt** — Run `cargo fmt` before committing.
- **Tests** — Every major feature must include unit tests. Run `cargo test --workspace` before pushing.

## Architecture

Nexum follows a strict layered architecture:

```
Input → Network/Gateway → World → Reducer → Transaction → OCC → Commit → Vec<Change> → Subscription → Gateway → Clients
```

Key invariants:
- ONE authoritative state store
- ONE transaction system
- ONE simulation path
- ONE commit path
- Deterministic simulation
- WAL durability
- OCC correctness
- WASM sandbox integrity

See `docs/architecture/` for architectural decision records (ADRs).

## Development Phases

Nexum follows a phased development approach. Each phase:

1. **Measures** the current state
2. **Identifies** the bottleneck
3. **Implements** the smallest justified change
4. **Verifies** correctness (tests + clippy)
5. **Benchmarks** the improvement
6. **Documents** the result

See `docs/reports/` for phase reports and `docs/design/` for design documents.

## Pull Requests

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/my-feature`)
3. Make your changes
4. Run `cargo test --workspace` and `cargo clippy --workspace --all-targets --all-features -- -D warnings`
5. Commit your changes with a clear message
6. Push to your fork and submit a pull request

### PR Guidelines

- Keep PRs focused on a single change
- Include before/after measurements for performance changes
- Update documentation if your change affects the public API
- Ensure all CI checks pass

## Benchmarking

When making performance changes:

1. **Measure first** — Run the benchmark before your change
2. **Make the change** — Implement the optimization
3. **Measure again** — Run the same benchmark after
4. **Report** — Include before/after numbers in your PR

```bash
# Run the CCU benchmark
cargo run --release -p game-server --example ccu -- --clients 1000 --profile C --ticks 600

# Run micro-benchmarks
cargo run --release -p nexum-bench -- --micro storage tx reducer wasm sub sim runtime wal
```

## Reporting Issues

Please use the GitHub issue tracker to report bugs or request features. When reporting bugs:

1. Include steps to reproduce
2. Include your Rust version (`rustc --version`)
3. Include the expected vs actual behavior
4. Include any relevant error messages

## License

By contributing to Nexum, you agree that your contributions will be licensed under the MIT License.
