# Storage Tests

Cross-crate tests for the storage engine (WAL append/replay, snapshots,
recovery, index rebuild).

Crate-local storage tests belong in `crates/nexum-storage/tests/` and run
automatically. Tests in this workspace-root directory are NOT auto-discovered
by Cargo — wire them up as a workspace member test crate before adding files
here (see `tests/integration/README.md`).
