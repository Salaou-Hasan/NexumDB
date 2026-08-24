//! Typed column types and values.
//!
//! [`ColumnType`] is the schema-level description of a column. [`Value`] is
//! the runtime representation of a single cell. Both live in the
//! dependency-free core crate because the table engine, transaction engine,
//! storage layer, and subscriptions all share them (ADR-002 D2).
//!
//! [`Value`] implements `Eq + Hash` so it can be used as an index key. Floats
//! use **bit-exact** equality and hashing (`f32::to_bits`/`f64::to_bits`):
//! `NaN == NaN` and `-0.0 != 0.0`. This keeps the `Eq` contract lawful and
//! makes float keys deterministic across runs.

use std::fmt;
use std::hash::{Hash, Hasher};

use crate::state::Id;

/// The set of column types supported by the table engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ColumnType {
    /// Boolean.
    Bool,
    /// 8-bit signed integer.
    I8,
    /// 16-bit signed integer.
    I16,
    /// 32-bit signed integer.
    I32,
    /// 64-bit signed integer.
    I64,
    /// 8-bit unsigned integer.
    U8,
    /// 16-bit unsigned integer.
    U16,
    /// 32-bit unsigned integer.
    U32,
    /// 64-bit unsigned integer.
    U64,
    /// 32-bit IEEE-754 float.
    F32,
    /// 64-bit IEEE-754 float.
    F64,
    /// UTF-8 string.
    String,
    /// Opaque byte blob.
    Bytes,
}

impl ColumnType {
    /// Returns a stable human-readable name for this type, used in error
    /// messages ("column `health` expects `i32`, got `string`").
    pub const fn name(self) -> &'static str {
        match self {
            Self::Bool => "bool",
            Self::I8 => "i8",
            Self::I16 => "i16",
            Self::I32 => "i32",
            Self::I64 => "i64",
            Self::U8 => "u8",
            Self::U16 => "u16",
            Self::U32 => "u32",
            Self::U64 => "u64",
            Self::F32 => "f32",
            Self::F64 => "f64",
            Self::String => "string",
            Self::Bytes => "bytes",
        }
    }
}

impl fmt::Display for ColumnType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.name())
    }
}

/// A single typed cell value.
///
/// Variants mirror [`ColumnType`] one-to-one. Equality and hashing are
/// **bit-exact for floats** — see the module documentation.
#[derive(Debug, Clone)]
pub enum Value {
    /// Boolean.
    Bool(bool),
    /// 8-bit signed integer.
    I8(i8),
    /// 16-bit signed integer.
    I16(i16),
    /// 32-bit signed integer.
    I32(i32),
    /// 64-bit signed integer.
    I64(i64),
    /// 8-bit unsigned integer.
    U8(u8),
    /// 16-bit unsigned integer.
    U16(u16),
    /// 32-bit unsigned integer.
    U32(u32),
    /// 64-bit unsigned integer.
    U64(u64),
    /// 32-bit IEEE-754 float.
    F32(f32),
    /// 64-bit IEEE-754 float.
    F64(f64),
    /// UTF-8 string (Box<str> = 16B vs String = 24B, saving 8 bytes per value).
    String(Box<str>),
    /// Opaque byte blob (Box<[u8]> = 16B vs Vec<u8> = 24B, saving 8 bytes per value).
    Bytes(Box<[u8]>),
}

impl Value {
    /// Returns the [`ColumnType`] of this value.
    pub const fn type_of(&self) -> ColumnType {
        match self {
            Self::Bool(_) => ColumnType::Bool,
            Self::I8(_) => ColumnType::I8,
            Self::I16(_) => ColumnType::I16,
            Self::I32(_) => ColumnType::I32,
            Self::I64(_) => ColumnType::I64,
            Self::U8(_) => ColumnType::U8,
            Self::U16(_) => ColumnType::U16,
            Self::U32(_) => ColumnType::U32,
            Self::U64(_) => ColumnType::U64,
            Self::F32(_) => ColumnType::F32,
            Self::F64(_) => ColumnType::F64,
            Self::String(_) => ColumnType::String,
            Self::Bytes(_) => ColumnType::Bytes,
        }
    }

    /// Returns `true` if this value's type is `ty`.
    pub fn is(&self, ty: ColumnType) -> bool {
        self.type_of() == ty
    }

    /// Returns the boolean if this is a `Bool`.
    pub fn as_bool(&self) -> Option<bool> {
        match self {
            Self::Bool(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `i8` if this is an `I8`.
    pub fn as_i8(&self) -> Option<i8> {
        match self {
            Self::I8(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `i16` if this is an `I16`.
    pub fn as_i16(&self) -> Option<i16> {
        match self {
            Self::I16(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `i32` if this is an `I32`.
    pub fn as_i32(&self) -> Option<i32> {
        match self {
            Self::I32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `i64` if this is an `I64`.
    pub fn as_i64(&self) -> Option<i64> {
        match self {
            Self::I64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `u8` if this is a `U8`.
    pub fn as_u8(&self) -> Option<u8> {
        match self {
            Self::U8(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `u16` if this is a `U16`.
    pub fn as_u16(&self) -> Option<u16> {
        match self {
            Self::U16(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `u32` if this is a `U32`.
    pub fn as_u32(&self) -> Option<u32> {
        match self {
            Self::U32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `u64` if this is a `U64`.
    pub fn as_u64(&self) -> Option<u64> {
        match self {
            Self::U64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `f32` if this is an `F32`.
    pub fn as_f32(&self) -> Option<f32> {
        match self {
            Self::F32(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the `f64` if this is an `F64`.
    pub fn as_f64(&self) -> Option<f64> {
        match self {
            Self::F64(v) => Some(*v),
            _ => None,
        }
    }

    /// Returns the string contents if this is a `String`.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::String(v) => Some(v),
            _ => None,
        }
    }

    /// Returns the byte slice if this is a `Bytes`.
    pub fn as_bytes(&self) -> Option<&[u8]> {
        match self {
            Self::Bytes(v) => Some(v),
            _ => None,
        }
    }
}

/// Bit-exact equality: floats compare by raw bits (`NaN == NaN`,
/// `-0.0 != 0.0`).
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        use Value::*;
        match (self, other) {
            (Bool(a), Bool(b)) => a == b,
            (I8(a), I8(b)) => a == b,
            (I16(a), I16(b)) => a == b,
            (I32(a), I32(b)) => a == b,
            (I64(a), I64(b)) => a == b,
            (U8(a), U8(b)) => a == b,
            (U16(a), U16(b)) => a == b,
            (U32(a), U32(b)) => a == b,
            (U64(a), U64(b)) => a == b,
            (F32(a), F32(b)) => a.to_bits() == b.to_bits(),
            (F64(a), F64(b)) => a.to_bits() == b.to_bits(),
            (String(a), String(b)) => a == b,
            (Bytes(a), Bytes(b)) => a == b,
            _ => false,
        }
    }
}

impl Eq for Value {}

/// Hashes floats by raw bits, consistent with [`PartialEq`].
impl Hash for Value {
    fn hash<H: Hasher>(&self, state: &mut H) {
        use Value::*;
        std::mem::discriminant(self).hash(state);
        match self {
            Bool(v) => v.hash(state),
            I8(v) => v.hash(state),
            I16(v) => v.hash(state),
            I32(v) => v.hash(state),
            I64(v) => v.hash(state),
            U8(v) => v.hash(state),
            U16(v) => v.hash(state),
            U32(v) => v.hash(state),
            U64(v) => v.hash(state),
            F32(v) => v.to_bits().hash(state),
            F64(v) => v.to_bits().hash(state),
            String(v) => v.hash(state),
            Bytes(v) => v.hash(state),
        }
    }
}

impl fmt::Display for Value {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        use Value::*;
        match self {
            Bool(v) => write!(f, "{v}"),
            I8(v) => write!(f, "{v}"),
            I16(v) => write!(f, "{v}"),
            I32(v) => write!(f, "{v}"),
            I64(v) => write!(f, "{v}"),
            U8(v) => write!(f, "{v}"),
            U16(v) => write!(f, "{v}"),
            U32(v) => write!(f, "{v}"),
            U64(v) => write!(f, "{v}"),
            F32(v) => write!(f, "{v}"),
            F64(v) => write!(f, "{v}"),
            String(v) => write!(f, "{v}"),
            Bytes(v) => {
                write!(f, "0x")?;
                for byte in v {
                    write!(f, "{byte:02x}")?;
                }
                Ok(())
            }
        }
    }
}

impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Self::Bool(value)
    }
}

impl From<i8> for Value {
    fn from(value: i8) -> Self {
        Self::I8(value)
    }
}

impl From<i16> for Value {
    fn from(value: i16) -> Self {
        Self::I16(value)
    }
}

impl From<i32> for Value {
    fn from(value: i32) -> Self {
        Self::I32(value)
    }
}

impl From<i64> for Value {
    fn from(value: i64) -> Self {
        Self::I64(value)
    }
}

impl From<u8> for Value {
    fn from(value: u8) -> Self {
        Self::U8(value)
    }
}

impl From<u16> for Value {
    fn from(value: u16) -> Self {
        Self::U16(value)
    }
}

impl From<u32> for Value {
    fn from(value: u32) -> Self {
        Self::U32(value)
    }
}

impl From<u64> for Value {
    fn from(value: u64) -> Self {
        Self::U64(value)
    }
}

impl From<f32> for Value {
    fn from(value: f32) -> Self {
        Self::F32(value)
    }
}

impl From<f64> for Value {
    fn from(value: f64) -> Self {
        Self::F64(value)
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Self::String(value.into_boxed_str())
    }
}

impl From<&str> for Value {
    fn from(value: &str) -> Self {
        Self::String(value.into())
    }
}

impl From<Vec<u8>> for Value {
    fn from(value: Vec<u8>) -> Self {
        Self::Bytes(value.into_boxed_slice())
    }
}

impl From<&[u8]> for Value {
    fn from(value: &[u8]) -> Self {
        Self::Bytes(value.into())
    }
}

/// Any id converts to a `U64` value — `row![player_id, ...]` just works.
impl<T: Id> From<T> for Value {
    fn from(id: T) -> Self {
        Self::U64(id.as_u64())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ids::RowId;

    #[test]
    fn type_of_matches_variant() {
        assert_eq!(Value::Bool(true).type_of(), ColumnType::Bool);
        assert_eq!(Value::I32(1).type_of(), ColumnType::I32);
        assert_eq!(Value::U64(1).type_of(), ColumnType::U64);
        assert_eq!(Value::F64(1.0).type_of(), ColumnType::F64);
        assert_eq!(Value::String("x".into()).type_of(), ColumnType::String);
        assert_eq!(Value::Bytes(vec![1].into_boxed_slice()).type_of(), ColumnType::Bytes);
    }

    #[test]
    fn typed_accessors() {
        assert_eq!(Value::I32(7).as_i32(), Some(7));
        assert_eq!(Value::I32(7).as_u64(), None);
        assert_eq!(Value::U64(9).as_u64(), Some(9));
        assert_eq!(Value::String("hi".into()).as_str(), Some("hi"));
        assert_eq!(Value::Bytes(vec![1, 2].into_boxed_slice()).as_bytes(), Some(&[1, 2][..]));
        assert_eq!(Value::F64(2.5).as_f64(), Some(2.5));
        assert!(Value::Bool(true).is(ColumnType::Bool));
        assert!(!Value::Bool(true).is(ColumnType::I32));
    }

    #[test]
    fn from_conversions() {
        assert_eq!(Value::from(7i32), Value::I32(7));
        assert_eq!(Value::from(7u64), Value::U64(7));
        assert_eq!(Value::from("hi"), Value::String("hi".into()));
        assert_eq!(Value::from(vec![1u8]), Value::Bytes(vec![1].into_boxed_slice()));
        assert_eq!(Value::from(1.5f64), Value::F64(1.5));
    }

    #[test]
    fn ids_convert_to_u64_values() {
        assert_eq!(Value::from(RowId::from_u64(42)), Value::U64(42));
    }

    #[test]
    fn float_equality_is_bit_exact() {
        // NaN equals NaN (same bits), -0.0 differs from 0.0.
        let nan = f64::NAN;
        assert_eq!(Value::F64(nan), Value::F64(nan));
        assert_ne!(Value::F64(-0.0), Value::F64(0.0));
        assert_eq!(Value::F32(1.0), Value::F32(1.0));
        assert_ne!(Value::F32(1.0), Value::F32(2.0));
    }

    #[test]
    fn hash_is_consistent_with_equality() {
        use std::collections::hash_map::DefaultHasher;
        let mut hasher = DefaultHasher::new();
        Value::F64(f64::NAN).hash(&mut hasher);
        let first = hasher.finish();
        let mut hasher = DefaultHasher::new();
        Value::F64(f64::NAN).hash(&mut hasher);
        assert_eq!(first, hasher.finish());
    }

    #[test]
    fn values_are_index_key_usable() {
        let mut map = std::collections::HashMap::new();
        map.insert(vec![Value::U64(1), Value::I32(10)], "first");
        map.insert(vec![Value::U64(2), Value::I32(20)], "second");
        assert_eq!(
            map.get(&vec![Value::U64(2), Value::I32(20)]),
            Some(&"second")
        );
    }

    #[test]
    fn column_type_names_are_stable() {
        assert_eq!(ColumnType::I32.name(), "i32");
        assert_eq!(ColumnType::String.name(), "string");
        assert_eq!(ColumnType::Bytes.name(), "bytes");
        assert_eq!(format!("{}", ColumnType::Bytes), "bytes");
    }
}
