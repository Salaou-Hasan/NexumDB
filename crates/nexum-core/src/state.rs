//! Foundational state interfaces.
//!
//! The central philosophy of Nexum is that there is **one authoritative
//! state**, represented by the table system. Everything else — caches,
//! indexes, subscription views, snapshots, the WAL — is derived
//! infrastructure. These traits are the minimal shared vocabulary that
//! expresses that model across crates.

use std::fmt;
use std::hash::Hash;

use crate::types::Version;

/// A typed identifier.
///
/// Implemented by every id in [`crate::ids`]. Providing the raw `u64` value
/// lets generic infrastructure (partition hashing, wire formats, storage
/// keys) handle any id uniformly while callers keep full type safety.
pub trait Id: Copy + Eq + Ord + Hash + fmt::Display + fmt::Debug + Send + Sync + 'static {
    /// Returns the raw `u64` value of this id.
    fn as_u64(self) -> u64;

    /// Creates an id from a raw `u64` value.
    fn from_u64(value: u64) -> Self;
}

/// Anything that carries a version for optimistic concurrency control.
///
/// Rows, transactions, and any other object that participates in conflict
/// detection exposes its current [`Version`] through this trait.
pub trait Versioned {
    /// The current version of this object.
    fn version(&self) -> Version;
}

/// The kind of change applied to a row, used for change tracking.
///
/// Committed transactions produce change sets made of these; subscriptions
/// observe them to compute deltas.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ChangeKind {
    /// A row was inserted.
    Insert,
    /// An existing row was updated.
    Update,
    /// A row was deleted.
    Delete,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::*;

    /// All id types implement `Id` and can be used through it generically.
    #[test]
    fn all_ids_implement_id_trait() {
        fn assert_id<I: Id>(id: I, raw: u64) {
            assert_eq!(id.as_u64(), raw);
            assert_eq!(I::from_u64(raw), id);
        }
        assert_id(TableId::from_u64(1), 1);
        assert_id(RowId::from_u64(2), 2);
        assert_id(ColumnId::from_u64(3), 3);
        assert_id(PartitionId::from_u64(4), 4);
        assert_id(TransactionId::from_u64(5), 5);
        assert_id(ReducerId::from_u64(6), 6);
        assert_id(SubscriptionId::from_u64(7), 7);
    }

    #[test]
    fn change_kind_is_comparable() {
        assert_eq!(ChangeKind::Insert, ChangeKind::Insert);
        assert_ne!(ChangeKind::Insert, ChangeKind::Update);
        assert_ne!(ChangeKind::Update, ChangeKind::Delete);
    }
}
