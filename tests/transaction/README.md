# Transaction Tests

Cross-crate tests for the transaction engine (OCC validation, read/write sets,
commit/abort behavior, multi-table atomicity).

Crate-local transaction tests belong in `crates/nexum-tx/tests/` and run
automatically. Tests in this workspace-root directory are NOT auto-discovered
by Cargo — wire them up as a workspace member test crate before adding files
here (see `tests/integration/README.md`).
