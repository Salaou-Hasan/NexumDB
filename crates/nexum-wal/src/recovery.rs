//! Recovery: snapshot restore + WAL replay.
//!
//! [`recover`] reconstructs the authoritative state that existed at the
//! durability boundary (ADR-005 D4, D6):
//!
//! ```text
//! load latest valid snapshot → restore tables/counters into a fresh store
//! → replay committed WAL transactions at/after the snapshot LSN
//! → advance next_transaction_id past replayed history
//! → drain change buffers (replayed history is not fresh change events)
//! ```
//!
//! Replay goes through the plain `Table::insert/update/delete` API — the
//! **recovery/replay boundary** — never through OCC validation: replay is not
//! a new transaction, it reproduces history. Because inserts assign
//! monotonic row ids, updates bump versions by one, and every row mutation
//! advances the table epoch, replaying the identical change sequence
//! reconstructs rows, row ids, versions, epochs, `next_row_id`, and the
//! derived indexes exactly. Each replayed insert verifies that storage
//! assigned the change's recorded row id — a mismatch is an internal bug,
//! never a tolerated deviation.

use std::path::{Path, PathBuf};

use nexum_core::{ChangeKind, Error, Result};
use nexum_storage::Change;
use nexum_table::TableStore;

use crate::snapshot::Snapshot;
use crate::wal::Wal;

/// A summary of the snapshot used by a recovery, if any.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveredSnapshot {
    /// The snapshot file that was loaded.
    pub path: PathBuf,
    /// Its WAL LSN; records before this were already incorporated.
    pub lsn: u64,
    /// How many tables were restored from it.
    pub tables: usize,
}

/// What a recovery did.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RecoveryReport {
    /// The snapshot used, if any.
    pub snapshot: Option<RecoveredSnapshot>,
    /// Number of transactions replayed from the WAL.
    pub replayed_txs: usize,
    /// Number of changes replayed.
    pub replayed_changes: usize,
    /// Whether the WAL had a physically invalid tail that was dropped.
    pub truncated_tail: bool,
}

/// Reconstructs authoritative state from the newest valid snapshot in
/// `snapshot_dir` plus the committed WAL records after the snapshot LSN.
///
/// - **With a snapshot**: the store must be empty (a fresh `TableStore`);
///   `restore` builds every table from the snapshot (returning
///   [`Error::invalid_argument`] otherwise).
/// - **Without a snapshot**: the WAL carries *changes, not DDL* — the store
///   must already define the tables (schemas are a deployment concern, as in
///   the future `define tables` step). Replay validates each change's table
///   exists.
///
/// Returns [`Error::internal`] if replay cannot reproduce the recorded
/// history (a bug in the writer or a corrupt-but-checksummed record).
pub fn recover(
    store: &mut TableStore,
    wal: &mut Wal,
    snapshot_dir: &Path,
) -> Result<RecoveryReport> {
    let mut report = RecoveryReport {
        snapshot: None,
        replayed_txs: 0,
        replayed_changes: 0,
        truncated_tail: false,
    };

    // 1. Snapshot.
    let snapshot_lsn = match Snapshot::find_latest(snapshot_dir)? {
        Some((path, snapshot)) => {
            report.snapshot = Some(RecoveredSnapshot {
                path,
                lsn: snapshot.lsn,
                tables: snapshot.tables.len(),
            });
            store.restore(
                snapshot.tables,
                snapshot.next_table_id,
                snapshot.next_transaction_id,
            )?;
            snapshot.lsn
        }
        None => 0,
    };

    // 2. WAL.
    let (txs, truncated) = wal.recover_changes()?;
    // A tail dropped at open time is also a truncated tail.
    report.truncated_tail = wal.truncated_on_open() || truncated;

    // 3. Replay only the transactions that the snapshot does not cover.
    let mut max_tx_id: Option<u64> = None;
    for tx in txs {
        if tx.commit_lsn.as_u64() < snapshot_lsn {
            continue;
        }
        for change in &tx.changes {
            replay_change(store, change)?;
            report.replayed_changes += 1;
        }
        report.replayed_txs += 1;
        let raw = tx.tx_id.as_u64();
        max_tx_id = Some(max_tx_id.map_or(raw, |current| current.max(raw)));
    }

    // 4. Never reuse transaction ids.
    if let Some(last) = max_tx_id {
        store.advance_transaction_id(last + 1);
    }

    // 5. Replayed history is not fresh change events.
    store.drain_changes();

    Ok(report)
}

/// Applies one committed change through the Table API (the recovery/replay
/// boundary). Errors are mapped to [`Error::internal`]: replay reproduces
/// state that was once committed, so any failure is an invariant violation.
fn replay_change(store: &mut TableStore, change: &Change) -> Result<()> {
    let table = store.table_mut_by_id(change.table_id()).ok_or_else(|| {
        Error::internal(format!(
            "recovery: table {} does not exist",
            change.table_id()
        ))
    })?;
    match change.kind() {
        ChangeKind::Insert => {
            let row = change
                .new_row()
                .cloned()
                .ok_or_else(|| Error::internal("recovery: insert change lacks a new row"))?;
            let assigned = table
                .insert(row)
                .map_err(|e| Error::internal(format!("recovery: replay insert failed: {e}")))?;
            if assigned != change.row_id() {
                return Err(Error::internal(format!(
                    "recovery: replay assigned row id {assigned}, expected {}",
                    change.row_id()
                )));
            }
        }
        ChangeKind::Update => {
            let row = change
                .new_row()
                .cloned()
                .ok_or_else(|| Error::internal("recovery: update change lacks a new row"))?;
            table
                .update(change.row_id(), row)
                .map_err(|e| Error::internal(format!("recovery: replay update failed: {e}")))?;
        }
        ChangeKind::Delete => {
            table
                .delete(change.row_id())
                .map_err(|e| Error::internal(format!("recovery: replay delete failed: {e}")))?;
        }
    }
    Ok(())
}
