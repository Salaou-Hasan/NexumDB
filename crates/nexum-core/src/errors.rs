//! The common error model shared by every Nexum subsystem.
//!
//! One shared [`Error`] type keeps the crate boundaries clean: the table
//! engine, transaction engine, storage layer, and reducers all return the
//! same errors instead of translating between private error types.

use std::fmt;

/// The common error type for all Nexum subsystems.
///
/// The enum is `#[non_exhaustive]`: new variants may be added in later phases
/// without breaking downstream match statements that provide a wildcard arm.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Error {
    /// A referenced entity (table, row, column, id, ...) does not exist.
    NotFound(String),
    /// An entity with the same identity already exists.
    AlreadyExists(String),
    /// An argument to an operation was invalid.
    InvalidArgument(String),
    /// An optimistic concurrency conflict occurred; the transaction must
    /// abort and may be retried.
    Conflict(String),
    /// An operation was attempted on a transaction that already committed.
    AlreadyCommitted(String),
    /// An operation was attempted on a transaction that already aborted.
    AlreadyAborted(String),
    /// The transaction was used in an invalid way (e.g. a dangling write
    /// handle, an invalid state transition, a duplicate insert).
    InvalidTransaction(String),
    /// A capacity or resource limit was exceeded.
    Capacity(String),
    /// An internal invariant was violated; this indicates a bug.
    Internal(String),
    /// The requested operation is not supported.
    Unsupported(String),
}

impl Error {
    /// Builds a [`Error::NotFound`].
    pub fn not_found(message: impl Into<String>) -> Self {
        Self::NotFound(message.into())
    }

    /// Builds a [`Error::AlreadyExists`].
    pub fn already_exists(message: impl Into<String>) -> Self {
        Self::AlreadyExists(message.into())
    }

    /// Builds a [`Error::InvalidArgument`].
    pub fn invalid_argument(message: impl Into<String>) -> Self {
        Self::InvalidArgument(message.into())
    }

    /// Builds a [`Error::Conflict`].
    pub fn conflict(message: impl Into<String>) -> Self {
        Self::Conflict(message.into())
    }

    /// Builds a [`Error::AlreadyCommitted`].
    pub fn already_committed(message: impl Into<String>) -> Self {
        Self::AlreadyCommitted(message.into())
    }

    /// Builds a [`Error::AlreadyAborted`].
    pub fn already_aborted(message: impl Into<String>) -> Self {
        Self::AlreadyAborted(message.into())
    }

    /// Builds a [`Error::InvalidTransaction`].
    pub fn invalid_transaction(message: impl Into<String>) -> Self {
        Self::InvalidTransaction(message.into())
    }

    /// Builds a [`Error::Capacity`].
    pub fn capacity(message: impl Into<String>) -> Self {
        Self::Capacity(message.into())
    }

    /// Builds a [`Error::Internal`].
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Builds a [`Error::Unsupported`].
    pub fn unsupported(message: impl Into<String>) -> Self {
        Self::Unsupported(message.into())
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::NotFound(message) => write!(f, "not found: {message}"),
            Self::AlreadyExists(message) => write!(f, "already exists: {message}"),
            Self::InvalidArgument(message) => write!(f, "invalid argument: {message}"),
            Self::Conflict(message) => write!(f, "conflict: {message}"),
            Self::AlreadyCommitted(message) => write!(f, "already committed: {message}"),
            Self::AlreadyAborted(message) => write!(f, "already aborted: {message}"),
            Self::InvalidTransaction(message) => write!(f, "invalid transaction: {message}"),
            Self::Capacity(message) => write!(f, "capacity exceeded: {message}"),
            Self::Internal(message) => write!(f, "internal error: {message}"),
            Self::Unsupported(message) => write!(f, "unsupported: {message}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(error: std::io::Error) -> Self {
        Self::Internal(format!("io error: {error}"))
    }
}

/// Convenience alias for results produced by Nexum subsystems.
pub type Result<T> = std::result::Result<T, Error>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn display_includes_variant_context() {
        assert_eq!(
            Error::not_found("player row").to_string(),
            "not found: player row"
        );
        assert_eq!(
            Error::conflict("version mismatch on row 7").to_string(),
            "conflict: version mismatch on row 7"
        );
        assert_eq!(
            Error::already_exists("table players").to_string(),
            "already exists: table players"
        );
    }

    #[test]
    fn helper_constructors_build_correct_variants() {
        assert!(matches!(Error::not_found("x"), Error::NotFound(_)));
        assert!(matches!(
            Error::already_exists("x"),
            Error::AlreadyExists(_)
        ));
        assert!(matches!(
            Error::invalid_argument("x"),
            Error::InvalidArgument(_)
        ));
        assert!(matches!(Error::conflict("x"), Error::Conflict(_)));
        assert!(matches!(
            Error::already_committed("x"),
            Error::AlreadyCommitted(_)
        ));
        assert!(matches!(
            Error::already_aborted("x"),
            Error::AlreadyAborted(_)
        ));
        assert!(matches!(
            Error::invalid_transaction("x"),
            Error::InvalidTransaction(_)
        ));
        assert!(matches!(Error::capacity("x"), Error::Capacity(_)));
        assert!(matches!(Error::internal("x"), Error::Internal(_)));
        assert!(matches!(Error::unsupported("x"), Error::Unsupported(_)));
    }

    #[test]
    fn result_alias_carries_error() {
        fn fail() -> Result<u64> {
            Err(Error::conflict("retry"))
        }
        let outcome: Result<u64> = fail();
        assert_eq!(outcome, Err(Error::Conflict("retry".to_string())));
        assert!(outcome.is_err());
    }
}
