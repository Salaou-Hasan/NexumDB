//! Deterministic little-endian binary encoding for the shared value/row/schema
//! types, plus a dependency-free CRC-32 implementation.
//!
//! Phase 5 durability (WAL records, snapshots) needs a stable byte encoding
//! for [`Value`], [`Row`], and [`TableSchema`] that both `nexum-storage`
//! (snapshots) and `nexum-wal` (WAL records) can share. It lives in the
//! dependency-free core crate (ADR-005 D8) and is the seed of the compact
//! binary protocol planned for the wire planes.
//!
//! The encoding is fully deterministic: all integers are fixed-width
//! little-endian, strings/bytes are `u64`-length-prefixed, and float bits are
//! written raw (`f32::to_bits` / `f64::to_bits`), so the same logical state
//! always produces the same bytes on every platform and run.
//!
//! All decoding functions take a cursor (`&mut &[u8]`) and return
//! [`Error::internal`] on truncated or malformed input — a byte-format
//! violation is a durability/integrity problem, not a user error.

use std::sync::OnceLock;

use crate::schema::{TableSchema, TableSchemaBuilder};
use crate::value::{ColumnType, Value};
use crate::{Error, Result};

/// Appends `v` as eight little-endian bytes.
pub fn put_u64(out: &mut Vec<u8>, v: u64) {
    out.extend_from_slice(&v.to_le_bytes());
}

/// Reads eight little-endian bytes from `cursor`.
pub fn get_u64(cursor: &mut &[u8]) -> Result<u64> {
    let bytes = take(cursor, 8)?;
    Ok(u64::from_le_bytes(bytes.try_into().expect("8 bytes")))
}

/// Appends a length-prefixed byte slice.
pub fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    put_u64(out, bytes.len() as u64);
    out.extend_from_slice(bytes);
}

/// Reads a length-prefixed byte slice.
pub fn get_bytes(cursor: &mut &[u8]) -> Result<Vec<u8>> {
    let len = get_u64(cursor)?;
    let bytes = take(cursor, len as usize)?;
    Ok(bytes.to_vec())
}

/// Appends a length-prefixed UTF-8 string.
pub fn put_str(out: &mut Vec<u8>, s: &str) {
    put_bytes(out, s.as_bytes());
}

/// Reads a length-prefixed UTF-8 string.
pub fn get_str(cursor: &mut &[u8]) -> Result<String> {
    let bytes = get_bytes(cursor)?;
    String::from_utf8(bytes).map_err(|_| Error::internal("binary: invalid UTF-8 string"))
}

/// Appends a boolean as a single byte.
pub fn put_bool(out: &mut Vec<u8>, value: bool) {
    out.push(u8::from(value));
}

/// Reads a boolean from a single byte.
pub fn get_bool(cursor: &mut &[u8]) -> Result<bool> {
    let byte = take(cursor, 1)?[0];
    match byte {
        0 => Ok(false),
        1 => Ok(true),
        _ => Err(Error::internal("binary: invalid boolean byte")),
    }
}

/// Appends a [`ColumnType`] as a single tag byte.
pub fn put_column_type(out: &mut Vec<u8>, ty: ColumnType) {
    out.push(column_type_tag(ty));
}

/// Reads a [`ColumnType`] from a tag byte.
pub fn get_column_type(cursor: &mut &[u8]) -> Result<ColumnType> {
    let tag = take(cursor, 1)?[0];
    column_type_from_tag(tag)
}

/// Appends a [`Value`]: a tag byte plus its fixed-width payload.
pub fn put_value(out: &mut Vec<u8>, value: &Value) {
    put_column_type(out, value.type_of());
    match value {
        Value::Bool(v) => put_bool(out, *v),
        Value::I8(v) => out.push(*v as u8),
        Value::I16(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::I32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::I64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::U8(v) => out.push(*v),
        Value::U16(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::U32(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::U64(v) => out.extend_from_slice(&v.to_le_bytes()),
        Value::F32(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        Value::F64(v) => out.extend_from_slice(&v.to_bits().to_le_bytes()),
        Value::String(v) => put_str(out, v),
        Value::Bytes(v) => put_bytes(out, v),
    }
}

/// Reads a [`Value`] from its tag + payload.
pub fn get_value(cursor: &mut &[u8]) -> Result<Value> {
    let ty = get_column_type(cursor)?;
    Ok(match ty {
        ColumnType::Bool => Value::Bool(get_bool(cursor)?),
        ColumnType::I8 => Value::I8(take(cursor, 1)?[0] as i8),
        ColumnType::I16 => Value::I16(i16::from_le_bytes(short(cursor)?)),
        ColumnType::I32 => Value::I32(i32::from_le_bytes(quad(cursor)?)),
        ColumnType::I64 => Value::I64(i64::from_le_bytes(long(cursor)?)),
        ColumnType::U8 => Value::U8(take(cursor, 1)?[0]),
        ColumnType::U16 => Value::U16(u16::from_le_bytes(short(cursor)?)),
        ColumnType::U32 => Value::U32(u32::from_le_bytes(quad(cursor)?)),
        ColumnType::U64 => Value::U64(u64::from_le_bytes(long(cursor)?)),
        ColumnType::F32 => Value::F32(f32::from_bits(u32::from_le_bytes(quad(cursor)?))),
        ColumnType::F64 => Value::F64(f64::from_bits(u64::from_le_bytes(long(cursor)?))),
        ColumnType::String => Value::String(get_str(cursor)?),
        ColumnType::Bytes => Value::Bytes(get_bytes(cursor)?),
    })
}

/// Appends a [`Version`] as eight little-endian bytes.
pub fn put_version(out: &mut Vec<u8>, version: crate::types::Version) {
    put_u64(out, version.as_u64());
}

/// Reads a [`Version`] from eight little-endian bytes.
pub fn get_version(cursor: &mut &[u8]) -> Result<crate::types::Version> {
    Ok(crate::types::Version::from_u64(get_u64(cursor)?))
}

/// Appends a [`Row`]: value count + values.
pub fn put_row(out: &mut Vec<u8>, row: &crate::Row) {
    put_u64(out, row.len() as u64);
    for value in row.iter() {
        put_value(out, value);
    }
}

/// Reads a [`Row`] from its encoding.
///
/// The value count is decoded from the input, so the reserve uses
/// [`Vec::try_reserve`] — a hostile count (e.g. `u64::MAX`) yields a clean
/// error instead of a capacity-overflow panic or OOM abort, keeping the
/// codec safe against untrusted input (the WASM ABI feeds guest bytes here).
pub fn get_row(cursor: &mut &[u8]) -> Result<crate::Row> {
    let count = get_u64(cursor)?;
    let mut values = Vec::new();
    values
        .try_reserve(count as usize)
        .map_err(|_| Error::internal("binary: row value count exceeds memory capacity"))?;
    for _ in 0..count {
        values.push(get_value(cursor)?);
    }
    Ok(crate::Row::new(values))
}

/// Appends a [`TableSchema`]: name, columns, primary key, indexes.
pub fn put_schema(out: &mut Vec<u8>, schema: &TableSchema) {
    put_str(out, schema.name());
    put_u64(out, schema.columns().len() as u64);
    for column in schema.columns() {
        put_str(out, column.name());
        put_column_type(out, column.ty());
    }
    put_bool(out, schema.primary_key().is_some());
    if let Some(primary) = schema.primary_key() {
        put_u64(out, primary.len() as u64);
        for name in primary {
            put_str(out, name);
        }
    }
    put_u64(out, schema.indexes().len() as u64);
    for index in schema.indexes() {
        put_str(out, index.name());
        put_bool(out, index.is_unique());
        put_u64(out, index.columns().len() as u64);
        for column in index.columns() {
            put_str(out, column);
        }
    }
}

/// Reads a [`TableSchema`] from its encoding, reconstructing it through the
/// validating builder (unknown column references are rejected there).
pub fn get_schema(cursor: &mut &[u8]) -> Result<TableSchema> {
    let name = get_str(cursor)?;
    let mut builder = TableSchemaBuilder::new(name);
    let column_count = get_u64(cursor)?;
    for _ in 0..column_count {
        let column_name = get_str(cursor)?;
        let ty = get_column_type(cursor)?;
        builder = builder.column(column_name, ty);
    }
    let has_primary = get_bool(cursor)?;
    if has_primary {
        let primary_count = get_u64(cursor)?;
        let mut primary = Vec::with_capacity(primary_count as usize);
        for _ in 0..primary_count {
            primary.push(get_str(cursor)?);
        }
        let names: Vec<&str> = primary.iter().map(String::as_str).collect();
        builder = builder.primary_key(&names);
    }
    let index_count = get_u64(cursor)?;
    for _ in 0..index_count {
        let index_name = get_str(cursor)?;
        let unique = get_bool(cursor)?;
        let column_count = get_u64(cursor)?;
        let mut columns = Vec::with_capacity(column_count as usize);
        for _ in 0..column_count {
            columns.push(get_str(cursor)?);
        }
        let names: Vec<&str> = columns.iter().map(String::as_str).collect();
        builder = if unique {
            builder.unique_index(index_name, &names)
        } else {
            builder.index(index_name, &names)
        };
    }
    builder.build()
}

/// CRC-32 (IEEE 802.3, reflected polynomial `0xEDB88320`), table-driven.
///
/// Used for WAL record and snapshot integrity checks. Dependency-free and
/// deterministic. The standard polynomial catches burst errors typical of
/// torn/corrupted tails.
pub fn crc32(data: &[u8]) -> u32 {
    static TABLE: OnceLock<[u32; 256]> = OnceLock::new();
    let table = TABLE.get_or_init(build_crc_table);
    let mut crc = 0xFFFF_FFFFu32;
    for &byte in data {
        let index = ((crc ^ u32::from(byte)) & 0xFF) as usize;
        crc = table[index] ^ (crc >> 8);
    }
    crc ^ 0xFFFF_FFFF
}

/// Builds the CRC-32 lookup table once.
fn build_crc_table() -> [u32; 256] {
    let mut table = [0u32; 256];
    for (i, entry) in table.iter_mut().enumerate() {
        let mut crc = i as u32;
        for _ in 0..8 {
            crc = if crc & 1 != 0 {
                (crc >> 1) ^ 0xEDB8_8320
            } else {
                crc >> 1
            };
        }
        *entry = crc;
    }
    table
}

/// Takes exactly `len` bytes from the cursor, or fails as truncated input.
fn take<'a>(cursor: &mut &'a [u8], len: usize) -> Result<&'a [u8]> {
    if cursor.len() < len {
        return Err(Error::internal(format!(
            "binary: unexpected end of input (needed {len} bytes, have {})",
            cursor.len()
        )));
    }
    let (head, tail) = cursor.split_at(len);
    *cursor = tail;
    Ok(head)
}

fn short(cursor: &mut &[u8]) -> Result<[u8; 2]> {
    take(cursor, 2)?.try_into().map_err(|_| Error::internal("binary: 2 bytes"))
}

fn quad(cursor: &mut &[u8]) -> Result<[u8; 4]> {
    take(cursor, 4)?.try_into().map_err(|_| Error::internal("binary: 4 bytes"))
}

fn long(cursor: &mut &[u8]) -> Result<[u8; 8]> {
    take(cursor, 8)?.try_into().map_err(|_| Error::internal("binary: 8 bytes"))
}

/// Maps a [`ColumnType`] to its stable one-byte tag.
const fn column_type_tag(ty: ColumnType) -> u8 {
    match ty {
        ColumnType::Bool => 0,
        ColumnType::I8 => 1,
        ColumnType::I16 => 2,
        ColumnType::I32 => 3,
        ColumnType::I64 => 4,
        ColumnType::U8 => 5,
        ColumnType::U16 => 6,
        ColumnType::U32 => 7,
        ColumnType::U64 => 8,
        ColumnType::F32 => 9,
        ColumnType::F64 => 10,
        ColumnType::String => 11,
        ColumnType::Bytes => 12,
    }
}

/// Maps a one-byte tag back to a [`ColumnType`].
fn column_type_from_tag(tag: u8) -> Result<ColumnType> {
    Ok(match tag {
        0 => ColumnType::Bool,
        1 => ColumnType::I8,
        2 => ColumnType::I16,
        3 => ColumnType::I32,
        4 => ColumnType::I64,
        5 => ColumnType::U8,
        6 => ColumnType::U16,
        7 => ColumnType::U32,
        8 => ColumnType::U64,
        9 => ColumnType::F32,
        10 => ColumnType::F64,
        11 => ColumnType::String,
        12 => ColumnType::Bytes,
        _ => return Err(Error::internal(format!("binary: unknown column type tag {tag}"))),
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::row;

    fn roundtrip<T>(encode: impl Fn(&mut Vec<u8>, &T), decode: impl Fn(&mut &[u8]) -> Result<T>, value: T)
    where
        T: std::fmt::Debug + PartialEq,
    {
        let mut bytes = Vec::new();
        encode(&mut bytes, &value);
        let mut cursor: &[u8] = &bytes;
        let decoded = decode(&mut cursor).unwrap();
        assert_eq!(decoded, value);
        assert!(cursor.is_empty(), "whole input consumed");
    }

    #[test]
    fn crc32_is_stable_and_detects_flips() {
        let a = crc32(b"hello");
        let b = crc32(b"hello");
        assert_eq!(a, b);
        // A single flipped byte must change the checksum.
        let mut corrupted = b"hello".to_vec();
        corrupted[0] ^= 0xFF;
        assert_ne!(a, crc32(&corrupted));
        // Known vector: crc32("123456789") == 0xCBF43926.
        assert_eq!(crc32(b"123456789"), 0xCBF4_3926);
    }

    #[test]
    fn values_roundtrip_all_types() {
        let values = vec![
            Value::Bool(true),
            Value::I8(-8),
            Value::I16(-16),
            Value::I32(-32),
            Value::I64(-64),
            Value::U8(8),
            Value::U16(16),
            Value::U32(32),
            Value::U64(64),
            Value::F32(-1.5),
            Value::F64(2.5),
            Value::String("héllo wörld".into()),
            Value::Bytes(vec![0, 1, 2, 255]),
        ];
        for value in values {
            roundtrip(put_value, get_value, value);
        }
    }

    #[test]
    fn rows_roundtrip() {
        roundtrip(put_row, get_row, row![1u64, 10u64, 100i32, 5u32]);
        roundtrip(put_row, get_row, row![]);
        roundtrip(put_row, get_row, row![3.5f64, "x".to_string(), vec![9u8]]);
    }

    #[test]
    fn schemas_roundtrip() {
        let schema = TableSchema::builder("players")
            .column("id", ColumnType::U64)
            .column("zone_id", ColumnType::U64)
            .column("health", ColumnType::I32)
            .column("level", ColumnType::U32)
            .primary_key(&["id"])
            .index("by_zone", &["zone_id"])
            .unique_index("by_level", &["level"])
            .build()
            .unwrap();
        roundtrip(put_schema, get_schema, schema);
    }

    #[test]
    fn schema_without_primary_key_roundtrips() {
        let schema = TableSchema::builder("items")
            .column("name", ColumnType::String)
            .column("qty", ColumnType::I32)
            .build()
            .unwrap();
        roundtrip(put_schema, get_schema, schema);
    }

    #[test]
    fn truncation_is_an_internal_error() {
        let mut bytes = Vec::new();
        put_u64(&mut bytes, 1);
        let mut cursor: &[u8] = &bytes[..4];
        assert!(matches!(get_u64(&mut cursor), Err(Error::Internal(_))));
    }

    #[test]
    fn unknown_type_tag_is_an_internal_error() {
        let bytes = vec![200u8];
        let mut cursor: &[u8] = &bytes;
        assert!(matches!(get_value(&mut cursor), Err(Error::Internal(_))));
    }
}
