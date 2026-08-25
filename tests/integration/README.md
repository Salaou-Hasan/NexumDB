# Integration Tests

Workspace-level integration tests spanning multiple crates. Cargo does not
auto-discover test files at the workspace root, so nothing here is runnable —
each directory is documentation only.

- Tests that exercise a single crate belong in that crate's `tests/` directory
  (e.g. `crates/nexum-tx/tests/`) and run automatically with
  `cargo test --workspace`.
- Tests that cross crate boundaries live in the dedicated workspace member
  crate [`crates/nexum-integration-tests/`](../../crates/nexum-integration-tests)
  and also run with `cargo test --workspace`. Add new cross-crate tests there.
