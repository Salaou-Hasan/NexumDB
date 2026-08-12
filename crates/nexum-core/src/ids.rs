//! Typed identifiers.
//!
//! Nexum addresses every entity through a dedicated newtype id rather than a
//! bare `u64`. The Rust type system then prevents mixing up a table id with a
//! row id at compile time, while every id still converts cleanly to/from its
//! raw `u64` representation for wire formats, storage keys, and partition
//! hashing.
//!
//! All ids are `Copy`, `Eq`, `Ord`, and `Hash`, so they can be used directly
//! as map keys and set members.

use std::fmt;

use crate::state::Id;

macro_rules! define_id {
    (
        $(#[$outer:meta])*
        $name:ident
    ) => {
        $(#[$outer])*
        #[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
        #[repr(transparent)]
        pub struct $name(u64);

        impl $name {
            /// Creates a new id from a raw `u64` value.
            pub const fn from_u64(value: u64) -> Self {
                Self(value)
            }

            /// Returns the raw `u64` value of this id.
            pub const fn as_u64(self) -> u64 {
                self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl From<u64> for $name {
            fn from(value: u64) -> Self {
                Self::from_u64(value)
            }
        }

        impl From<$name> for u64 {
            fn from(id: $name) -> u64 {
                id.as_u64()
            }
        }

        impl Id for $name {
            fn as_u64(self) -> u64 {
                self.as_u64()
            }

            fn from_u64(value: u64) -> Self {
                Self::from_u64(value)
            }
        }
    };
}

define_id! {
    /// Identifies a table in the authoritative state.
    TableId
}

define_id! {
    /// Identifies a single row within a table.
    RowId
}

define_id! {
    /// Identifies a column within a table schema.
    ColumnId
}

define_id! {
    /// Identifies a partition of the authoritative state.
    PartitionId
}

define_id! {
    /// Identifies a transaction.
    TransactionId
}

define_id! {
    /// Identifies a reducer.
    ReducerId
}

define_id! {
    /// Identifies a subscription.
    SubscriptionId
}

define_id! {
    /// Identifies a simulation world (one authoritative partition).
    WorldId
}

define_id! {
    /// Identifies a runtime worker (the execution owner of a set of worlds).
    WorkerId
}

define_id! {
    /// Identifies a simulation system.
    SystemId
}

define_id! {
    /// Identifies a simulation tick (logical time step).
    TickId
}

define_id! {
    /// Identifies a network connection (a transport handle).
    ConnectionId
}

define_id! {
    /// Identifies an authenticated network session on a connection.
    SessionId
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;

    fn assert_id<I: Id>(id: I, raw: u64) {
        assert_eq!(id.as_u64(), raw);
        assert_eq!(I::from_u64(raw), id);
    }

    #[test]
    fn roundtrip_raw_value() {
        assert_id(TableId::from_u64(7), 7);
        assert_id(RowId::from_u64(42), 42);
        assert_id(ColumnId::from_u64(1), 1);
        assert_id(PartitionId::from_u64(0), 0);
        assert_id(TransactionId::from_u64(99), 99);
        assert_id(ReducerId::from_u64(3), 3);
        assert_id(SubscriptionId::from_u64(12), 12);
        assert_id(WorldId::from_u64(1), 1);
        assert_id(WorkerId::from_u64(2), 2);
        assert_id(SystemId::from_u64(4), 4);
        assert_id(TickId::from_u64(17), 17);
        assert_id(ConnectionId::from_u64(9), 9);
        assert_id(SessionId::from_u64(11), 11);
    }

    #[test]
    fn converts_via_from() {
        let id: TableId = 7u64.into();
        let raw: u64 = id.into();
        assert_eq!(raw, 7);
        assert_eq!(id, TableId::from_u64(7));
    }

    #[test]
    fn displays_as_raw_number() {
        assert_eq!(TableId::from_u64(7).to_string(), "7");
        assert_eq!(RowId::from_u64(42).to_string(), "42");
    }

    #[test]
    fn ordering_and_hash_usable_in_maps() {
        let mut map = HashMap::new();
        map.insert(RowId::from_u64(1), "first");
        map.insert(RowId::from_u64(2), "second");
        assert_eq!(map[&RowId::from_u64(2)], "second");
        assert!(RowId::from_u64(1) < RowId::from_u64(2));
    }

    #[test]
    fn ids_are_distinct_types_with_matching_raw_values() {
        // Different id types are not comparable with each other (a compile-time
        // guarantee). They only agree on their raw `u64` representation.
        assert_eq!(TableId::from_u64(1).as_u64(), RowId::from_u64(1).as_u64());
    }
}
