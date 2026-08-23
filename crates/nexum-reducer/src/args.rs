//! Reducer arguments: a named, deterministic, protocol-independent map.
//!
//! [`ReducerArgs`] wraps a `BTreeMap<String, Value>` (ADR-006 D4): names are
//! self-documenting, iteration is key-sorted (deterministic), and the
//! representation has no coupling to HTTP, JSON, WebSockets, or any network
//! protocol — a future network layer deserializes into this same shape.
//! Adding new optional keys never breaks existing callers, so the shape is
//! versionable.
//!
//! Typed accessors map a missing key to [`Error::NotFound`] and a present key
//! of the wrong type to [`Error::InvalidArgument`].

use std::collections::BTreeMap;

use nexum_core::{Error, Result, Value};

/// The named argument set of a reducer invocation.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ReducerArgs {
    entries: BTreeMap<String, Value>,
}

impl ReducerArgs {
    /// Creates an empty argument set.
    pub fn new() -> Self {
        Self::default()
    }

    /// Inserts a named argument, replacing any earlier value for the name.
    ///
    /// Consumes and returns `self`, so arguments build with a single
    /// chained expression: `ReducerArgs::new().insert("a", 1).insert("b", 2)`.
    pub fn insert(mut self, name: impl Into<String>, value: impl Into<Value>) -> Self {
        self.entries.insert(name.into(), value.into());
        self
    }

    /// Returns the argument with the given name, if present.
    pub fn get(&self, name: &str) -> Option<&Value> {
        self.entries.get(name)
    }

    /// Returns `true` if `name` is present.
    pub fn contains(&self, name: &str) -> bool {
        self.entries.contains_key(name)
    }

    /// Returns the number of arguments.
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    /// Returns `true` if there are no arguments.
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    /// Iterates over `(name, value)` pairs in deterministic (key) order.
    pub fn iter(&self) -> impl Iterator<Item = (&str, &Value)> {
        self.entries
            .iter()
            .map(|(name, value)| (name.as_str(), value))
    }

    /// Returns the argument with the given name, or `NotFound` if absent.
    pub fn require(&self, name: &str) -> Result<&Value> {
        self.get(name)
            .ok_or_else(|| Error::not_found(format!("reducer argument '{name}' is missing")))
    }

    /// Returns the argument as a `u64`, or `NotFound`/`InvalidArgument`.
    pub fn require_u64(&self, name: &str) -> Result<u64> {
        self.require(name)?
            .as_u64()
            .ok_or_else(|| wrong_type(name, "u64"))
    }

    /// Returns the argument as an `i32`, or `NotFound`/`InvalidArgument`.
    pub fn require_i32(&self, name: &str) -> Result<i32> {
        self.require(name)?
            .as_i32()
            .ok_or_else(|| wrong_type(name, "i32"))
    }

    /// Returns the argument as an `i64`, or `NotFound`/`InvalidArgument`.
    pub fn require_i64(&self, name: &str) -> Result<i64> {
        self.require(name)?
            .as_i64()
            .ok_or_else(|| wrong_type(name, "i64"))
    }

    /// Returns the argument as a `u32`, or `NotFound`/`InvalidArgument`.
    pub fn require_u32(&self, name: &str) -> Result<u32> {
        self.require(name)?
            .as_u32()
            .ok_or_else(|| wrong_type(name, "u32"))
    }

    /// Returns the argument as a `bool`, or `NotFound`/`InvalidArgument`.
    pub fn require_bool(&self, name: &str) -> Result<bool> {
        self.require(name)?
            .as_bool()
            .ok_or_else(|| wrong_type(name, "bool"))
    }

    /// Returns the argument as a `&str`, or `NotFound`/`InvalidArgument`.
    pub fn require_str(&self, name: &str) -> Result<&str> {
        self.require(name)?
            .as_str()
            .ok_or_else(|| wrong_type(name, "string"))
    }

    /// Returns the argument as a `f64`, or `NotFound`/`InvalidArgument`.
    pub fn require_f64(&self, name: &str) -> Result<f64> {
        self.require(name)?
            .as_f64()
            .ok_or_else(|| wrong_type(name, "f64"))
    }
}

fn wrong_type(name: &str, expected: &str) -> Error {
    Error::invalid_argument(format!("reducer argument '{name}' is not a {expected}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn inserts_and_reads_named_values() {
        let args = ReducerArgs::new()
            .insert("player_id", 42u64)
            .insert("name", "alice");
        assert_eq!(args.require_u64("player_id").unwrap(), 42);
        assert_eq!(args.require_str("name").unwrap(), "alice");
        assert_eq!(args.len(), 2);
    }

    #[test]
    fn iteration_is_key_sorted_and_deterministic() {
        let args = ReducerArgs::new()
            .insert("zeta", 1u64)
            .insert("alpha", 2u64)
            .insert("mid", 3u64);
        let names: Vec<&str> = args.iter().map(|(name, _)| name).collect();
        assert_eq!(names, vec!["alpha", "mid", "zeta"]);
    }

    #[test]
    fn missing_key_is_not_found() {
        let args = ReducerArgs::new();
        assert!(args.get("nope").is_none());
        assert!(matches!(args.require_u64("nope"), Err(Error::NotFound(_))));
    }

    #[test]
    fn wrong_type_is_invalid_argument() {
        let args = ReducerArgs::new().insert("key", "text");
        assert!(matches!(
            args.require_u64("key"),
            Err(Error::InvalidArgument(_))
        ));
        assert!(matches!(
            args.require_bool("key"),
            Err(Error::InvalidArgument(_))
        ));
    }

    #[test]
    fn insert_replaces_and_is_chainable() {
        let args = ReducerArgs::new().insert("x", 1u64).insert("y", 2u64);
        assert_eq!(args.len(), 2);
        let args = args.insert("x", 9u64);
        assert_eq!(args.require_u64("x").unwrap(), 9);
        assert_eq!(args.len(), 2);
    }
}
