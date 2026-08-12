# Integration Tests

Workspace-level integration tests spanning multiple crates. Cargo does not
auto-discover test files at the workspace root, so each directory here is an
explicit harness area:

- Tests that exercise a single crate belong in that crate's `tests/` directory
  (e.g. `crates/nexum-tx/tests/`) and run automatically with
  `cargo test --workspace`.
- Tests that cross crate boundaries must be wired up explicitly — either as a
  dedicated workspace member crate (e.g. `crates/nexum-integration-tests/`)
  added to `workspace.members`, or driven by a script — before real test files
  are added here.
