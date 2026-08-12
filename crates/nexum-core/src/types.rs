//! Time and version primitives.
//!
//! [`Version`] is the backbone of optimistic concurrency control: every state
//! object that participates in conflict detection carries one, and writes
//! advance it. [`Timestamp`] provides a common clock representation for
//! simulation time, WAL records, and metrics.

use std::fmt;
use std::time::{SystemTime, UNIX_EPOCH};

/// A monotonically increasing version number.
///
/// A transaction records the versions of the objects it reads and re-checks
/// them during validation; a write advances the version of the objects it
/// touches. If two transactions modify the same object, the second one to
/// validate sees a mismatched version and aborts cleanly.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Version(u64);

impl Version {
    /// The initial version of any freshly created object.
    pub const ZERO: Version = Version(0);

    /// Creates a version from a raw `u64` value.
    pub const fn from_u64(value: u64) -> Self {
        Self(value)
    }

    /// Returns the raw `u64` value of this version.
    pub const fn as_u64(self) -> u64 {
        self.0
    }

    /// Returns the next version in the sequence.
    ///
    /// Writes call this to produce the successor version of the object they
    /// modify, so every committed change is observable as a version bump.
    ///
    /// # Panics
    ///
    /// Panics in debug builds if `self` is `u64::MAX` (wraps in release); a
    /// per-object version counter should never reach this in practice.
    pub const fn next(self) -> Self {
        Self(self.0 + 1)
    }

    /// Returns the next version, or `None` at `u64::MAX`.
    pub const fn checked_next(self) -> Option<Self> {
        match self.0.checked_add(1) {
            Some(value) => Some(Self(value)),
            None => None,
        }
    }
}

impl fmt::Display for Version {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Version {
    fn from(value: u64) -> Self {
        Self::from_u64(value)
    }
}

impl From<Version> for u64 {
    fn from(version: Version) -> u64 {
        version.as_u64()
    }
}

/// A timestamp measured in milliseconds since the Unix epoch.
///
/// Wall-clock time is suitable for WAL records, metrics, and client-facing
/// metadata. Simulation phases may additionally introduce a logical tick
/// counter; `Timestamp` remains the common representation for both.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Default)]
#[repr(transparent)]
pub struct Timestamp(u64);

impl Timestamp {
    /// The zero timestamp — the Unix epoch itself.
    pub const ZERO: Timestamp = Timestamp(0);

    /// Creates a timestamp from milliseconds since the Unix epoch.
    pub const fn from_millis(millis: u64) -> Self {
        Self(millis)
    }

    /// Returns the raw milliseconds-since-epoch value.
    pub const fn as_millis(self) -> u64 {
        self.0
    }

    /// Returns the current wall-clock time as a [`Timestamp`].
    pub fn now() -> Self {
        let millis = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_millis() as u64)
            .unwrap_or(0);
        Self(millis)
    }
}

impl fmt::Display for Timestamp {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u64> for Timestamp {
    fn from(value: u64) -> Self {
        Self::from_millis(value)
    }
}

impl From<Timestamp> for u64 {
    fn from(timestamp: Timestamp) -> u64 {
        timestamp.as_millis()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn version_advances_monotonically() {
        let v = Version::ZERO;
        assert_eq!(v.next().as_u64(), 1);
        assert_eq!(v.next().next().as_u64(), 2);
        assert!(v < v.next());
    }

    #[test]
    fn version_roundtrips_raw_value() {
        let v = Version::from_u64(100);
        assert_eq!(v.as_u64(), 100);
        assert_eq!(Version::from(100u64), v);
        assert_eq!(u64::from(v), 100);
    }

    #[test]
    fn version_checked_next_saturates_at_max() {
        let max = Version::from_u64(u64::MAX);
        assert_eq!(max.checked_next(), None);
        assert_eq!(Version::from_u64(5).checked_next(), Some(Version::from_u64(6)));
    }

    #[test]
    fn timestamp_roundtrips_millis() {
        let ts = Timestamp::from_millis(1_700_000_000_000);
        assert_eq!(ts.as_millis(), 1_700_000_000_000);
        assert!(ts > Timestamp::ZERO);
    }

    #[test]
    fn now_is_nonzero_wall_clock() {
        // The Unix epoch is long past; a live clock always reports far above 0.
        assert!(Timestamp::now() > Timestamp::ZERO);
    }
}
