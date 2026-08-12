//! The OCC commit algorithm: **validate everything, then apply everything**.
//!
//! [`commit`] runs three strictly separated phases (design doc §6,
//! ADR-004 D3):
//!
//! 1. **Validate** — a pure read of the store against the transaction's read
//!    set and write set. On any failure, zero authoritative state is mutated,
//!    zero Change records are produced, and the caller marks the transaction
//!    aborted.
//! 2. **Apply** — deterministic, infallible post-validation: deletes first
//!    (ascending `RowId`), then updates and inserts, all ordered by
//!    `(TableId, RowId)`.
//! 3. **Collect** — drain each touched table's change buffer and keep only
//!    the delta (pre-apply base lengths are recorded before apply), in
//!    `TableId` order.
//!
//! Validation checks, in order:
//!
//! - every **table observation** (from `scan` / `lookup_unique`) against the
//!   live mutation epoch; any difference is a phantom [`Error::Conflict`]
//!   (ADR-004 D13)
//! - every read observation against the live version (`None` = absent); any
//!   difference is a [`Error::Conflict`]
//! - every delete target exists (else `NotFound`); the deleted rows'
//!   unique-index keys are collected as **released**
//! - every update target exists (else `NotFound`); for each unique key of the
//!   new row, live owners (minus released rows) must be a subset of the
//!   target itself, and no other write in this transaction may claim the key
//! - every insert's unique keys must be free of live owners (minus released
//!   rows) and of claims by other writes in this transaction

use std::collections::{BTreeMap, HashMap};

use nexum_core::{Error, Result, RowId, TableId, Value};
use nexum_storage::Change;
use nexum_table::TableStore;

use crate::transaction::Transaction;
use crate::write_set::WriteEntry;

/// Unique-index keys released by rows this transaction deletes, keyed by
/// `(table_id, row_id)` — each value is `(index_name, key)`.
type ReleasedKeys = HashMap<(TableId, RowId), Vec<(String, Vec<Value>)>>;

/// Unique-index keys claimed by this transaction's own writes, keyed by
/// `(table_id, index_name)`; the inner map is `key → row_id`.
type ClaimedKeys = HashMap<(TableId, String), HashMap<Vec<Value>, RowId>>;

/// The full commit algorithm. `tx` must be `Active`.
pub(crate) fn commit(store: &mut TableStore, tx: &Transaction) -> Result<Vec<Change>> {
    // 1. VALIDATE — pure, no mutation.
    validate(&*store, tx)?;

    // Record pre-apply change-buffer lengths per touched table so the delta
    // below contains exactly this transaction's changes.
    let mut tables: Vec<TableId> = tx.writes().map(|(table_id, _, _)| table_id).collect();
    tables.sort_unstable();
    tables.dedup();
    let bases: BTreeMap<TableId, usize> = tables
        .into_iter()
        .map(|table_id| {
            let table = store.table_by_id(table_id).expect("validated table exists");
            (table_id, table.changes().len())
        })
        .collect();

    // 2. APPLY — infallible post-validation, deterministic order.
    apply(store, tx);

    // 3. COLLECT — per touched table, in TableId order, the delta only.
    let mut changes = Vec::new();
    for (table_id, base) in &bases {
        let table = store.table_mut_by_id(*table_id).expect("validated table exists");
        let drained = table.drain_changes();
        changes.extend(drained.into_iter().skip(*base));
    }

    Ok(changes)
}

/// Pure validation: compares the transaction's observations and planned
/// writes against live state without mutating anything.
fn validate(store: &TableStore, tx: &Transaction) -> Result<()> {
    // 0. Table observations (phantom protection): the observed mutation
    //    epoch must still be the live one. Coarsest checks first.
    for (table_id, observed_epoch) in tx.table_reads() {
        let table = store.table_by_id(table_id).ok_or_else(|| {
            Error::not_found(format!("table {table_id} does not exist"))
        })?;
        let live = table.epoch();
        if live != observed_epoch {
            return Err(Error::conflict(format!(
                "table '{}' changed since it was observed as a set (observed epoch {observed_epoch}, now {live})",
                table.name()
            )));
        }
    }

    // 1. Read set: every observation must still hold.
    for (table_id, row_id, observed) in tx.reads() {
        let table = store.table_by_id(table_id).ok_or_else(|| {
            Error::not_found(format!("table {table_id} does not exist"))
        })?;
        let live = table.version_of(row_id);
        if live != observed {
            return Err(Error::conflict(format!(
                "row {row_id} in table '{}' changed since it was read (observed {observed:?}, now {live:?})",
                table.name()
            )));
        }
    }

    // 2. Deletes: existence, and collect the unique keys they release.
    let mut released: ReleasedKeys = HashMap::new();
    for (table_id, row_id, entry) in tx.writes() {
        if let WriteEntry::Delete = entry {
            let table = store.table_by_id(table_id).ok_or_else(|| {
                Error::not_found(format!("table {table_id} does not exist"))
            })?;
            if !table.contains(row_id) {
                return Err(Error::not_found(format!(
                    "cannot delete row {row_id} in table '{}': it does not exist",
                    table.name()
                )));
            }
            let old_row = table
                .get(row_id)
                .expect("contains() implies the row is readable");
            released.insert((table_id, row_id), table.unique_keys(old_row)?);
        }
    }

    // 3. Updates and inserts: existence (updates) + uniqueness (both),
    //    against live owners minus released rows, plus a claims map for
    //    cross-write collisions within this transaction.
    let mut claims: ClaimedKeys = HashMap::new();
    for (table_id, row_id, entry) in tx.writes() {
        let row = match entry {
            WriteEntry::Insert(row) | WriteEntry::Update(row) => row,
            WriteEntry::Delete => continue,
        };
        let table = store.table_by_id(table_id).ok_or_else(|| {
            Error::not_found(format!("table {table_id} does not exist"))
        })?;

        if matches!(entry, WriteEntry::Update(_)) && !table.contains(row_id) {
            return Err(Error::not_found(format!(
                "cannot update row {row_id} in table '{}': it does not exist",
                table.name()
            )));
        }

        let allowed: &[RowId] = if matches!(entry, WriteEntry::Update(_)) {
            std::slice::from_ref(&row_id)
        } else {
            &[]
        };

        for (index_name, key) in table.unique_keys(row)? {
            // Live owners, minus rows this transaction deletes (their keys
            // are freed — deletes apply first, ADR-004 D4).
            let owners: Vec<RowId> = table
                .lookup_unique(&index_name, &key)?
                .into_iter()
                .filter(|id| !released.contains_key(&(table_id, *id)))
                .collect();
            if owners.iter().any(|owner| !allowed.contains(owner)) {
                return Err(Error::already_exists(format!(
                    "unique index '{index_name}' of table '{}' already contains this key",
                    table.name()
                )));
            }

            // Cross-write claims within this transaction.
            let claim_map = claims.entry((table_id, index_name.clone())).or_default();
            if let Some(other) = claim_map.get(&key) {
                if other != &row_id {
                    return Err(Error::already_exists(format!(
                        "unique index '{index_name}' of table '{}' is already claimed by another write in this transaction",
                        table.name()
                    )));
                }
            } else {
                claim_map.insert(key, row_id);
            }
        }
    }

    Ok(())
}

/// Applies the write set through the table API in deterministic order:
/// deletes first (they free unique keys), then updates and inserts. All
/// ordered by `(TableId, RowId)`; provisional (insert) ids sort after real
/// ids, so inserts apply in submission order.
///
/// # Panics
///
/// Infallible post-validation by construction (ADR-004 D3): every existence
/// and uniqueness check `Table` performs at apply time was already verified
/// against live state under single-threaded ownership. A panic here is an
/// internal invariant violation (a bug), not a user error.
fn apply(store: &mut TableStore, tx: &Transaction) {
    for (table_id, row_id, entry) in tx.writes() {
        if let WriteEntry::Delete = entry {
            let table = store
                .table_mut_by_id(table_id)
                .expect("validated table exists");
            table
                .delete(row_id)
                .expect("validation guarantees delete succeeds");
        }
    }
    for (table_id, row_id, entry) in tx.writes() {
        match entry {
            WriteEntry::Insert(row) => {
                let table = store
                    .table_mut_by_id(table_id)
                    .expect("validated table exists");
                table
                    .insert(row.clone())
                    .expect("validation guarantees insert succeeds");
            }
            WriteEntry::Update(row) => {
                let table = store
                    .table_mut_by_id(table_id)
                    .expect("validated table exists");
                table
                    .update(row_id, row.clone())
                    .expect("validation guarantees update succeeds");
            }
            WriteEntry::Delete => {}
        }
    }
}
