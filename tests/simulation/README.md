# Simulation Tests

Cross-crate tests for the simulation engine (tick scheduling, deterministic
ordering, system execution through transactions).

Crate-local simulation tests belong in `crates/nexum-simulation/tests/` and run
automatically. Tests in this workspace-root directory are NOT auto-discovered
by Cargo — wire them up as a workspace member test crate before adding files
here (see `tests/integration/README.md`).
