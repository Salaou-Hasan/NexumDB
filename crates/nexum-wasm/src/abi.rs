//! The restricted host ABI (design doc §4, ADR-007 D2–D4).
//!
//! A WASM reducer communicates with Nexum through a single imported function,
//! `("nexum","op")`, with signature `(i32, i32, i32, i32, i32) -> i32`:
//!
//! ```text
//! op(opcode, in_ptr, in_len, out_ptr, out_cap) -> u32
//! ```
//!
//! Inputs are encoded operation arguments in the guest's input buffer;
//! outputs are an **envelope** written to the guest's output buffer:
//!
//! ```text
//! [status: u32][payload_len: u32][payload…]
//! ```
//!
//! The host returns `0` when the envelope fit, or the required capacity when
//! it did not (the guest may retry with a larger buffer). `status == 0` means
//! success; nonzero means an ABI error whose payload is the error message.
//!
//! All encodings reuse `nexum-core::binary` (little-endian, length-prefixed,
//! deterministic, bounds-checked). Malformed input is an ABI error, never a
//! panic.

use nexum_core::binary::{
    get_bool, get_row, get_str, get_u64, put_bool, put_row, put_str, put_u64, put_value,
};
use nexum_core::{Error, Result, Row, RowId, Value};
use nexum_reducer::ReducerArgs;

/// `GET` a row through the transaction view.
pub const OP_GET: u32 = 1;
/// `CONTAINS` a row through the transaction view.
pub const OP_CONTAINS: u32 = 2;
/// `SCAN` the transaction view of a table (epoch observation).
pub const OP_SCAN: u32 = 3;
/// `LOOKUP_UNIQUE` owners of a key in a unique index (epoch observation).
pub const OP_LOOKUP_UNIQUE: u32 = 4;
/// `LOOKUP_INDEX` owners of a key in a non-unique secondary index (epoch
/// observation). Same wire format as `OP_LOOKUP_UNIQUE`.
pub const OP_LOOKUP_INDEX: u32 = 9;
/// `INSERT` a row; returns the provisional row id.
pub const OP_INSERT: u32 = 5;
/// `UPDATE` a row.
pub const OP_UPDATE: u32 = 6;
/// `DELETE` a row.
pub const OP_DELETE: u32 = 7;
/// `EMIT` a transaction-local event.
pub const OP_EMIT: u32 = 8;

/// The guest entry point returns this to signal application-level rejection,
/// with `[msg_len: u32][utf8 message]` written at the output buffer.
pub const RET_REJECT: u32 = u32::MAX;

/// Returns the opcode for a numeric code, or `InvalidArgument`.
pub fn opcode(code: u32) -> Result<u32> {
    if (OP_GET..=OP_LOOKUP_INDEX).contains(&code) {
        Ok(code)
    } else {
        Err(Error::invalid_argument(format!(
            "unknown ABI opcode {code}"
        )))
    }
}

// --------------------------------------------------------------- envelope

/// Builds a success envelope with `payload`.
pub fn envelope_ok(payload: &[u8]) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + payload.len());
    out.extend_from_slice(&0u32.to_le_bytes()); // status = ok
    out.extend_from_slice(&(payload.len() as u32).to_le_bytes());
    out.extend_from_slice(payload);
    out
}

/// Builds an error envelope with `message` as the payload.
pub fn envelope_err(message: &str) -> Vec<u8> {
    let mut out = Vec::with_capacity(8 + message.len());
    out.extend_from_slice(&1u32.to_le_bytes()); // status = error
    out.extend_from_slice(&(message.len() as u32).to_le_bytes());
    out.extend_from_slice(message.as_bytes());
    out
}

// ------------------------------------------------------- operation inputs

// The guest-side *encoders* and result *decoders* below are the documented
// reference of the ABI wire format (ADR-007 D4): the host only decodes op
// inputs and encodes op results, while the round-trip tests and a future
// guest toolchain build the same bytes from the other direction. They are
// kept next to the host code they mirror, and are exercised by the tests.
#[cfg_attr(not(test), allow(dead_code))]
/// Encodes `GET`/`CONTAINS`/`DELETE` args: table name + row id.
pub fn encode_table_row(table: &str, row_id: u64) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, table);
    put_u64(&mut out, row_id);
    out
}

/// Decodes `GET`/`CONTAINS`/`DELETE` args.
pub fn decode_table_row(cursor: &mut &[u8]) -> Result<(String, u64)> {
    let table = get_str(cursor)?;
    let row_id = get_u64(cursor)?;
    Ok((table, row_id))
}

/// Encodes `SCAN` args: table name.
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_table(table: &str) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, table);
    out
}

/// Decodes `SCAN` args.
pub fn decode_table(cursor: &mut &[u8]) -> Result<String> {
    get_str(cursor)
}

/// Encodes `LOOKUP_UNIQUE` args: table name, index name, key values.
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_lookup(table: &str, index: &str, key: &[Value]) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, table);
    put_str(&mut out, index);
    put_u64(&mut out, key.len() as u64);
    for value in key {
        put_value(&mut out, value);
    }
    out
}

/// Decodes `LOOKUP_UNIQUE` args.
///
/// The key count is guest-controlled, so the reserve uses [`Vec::try_reserve`]
/// — a hostile count yields a clean error, never a capacity-overflow panic.
pub fn decode_lookup(cursor: &mut &[u8]) -> Result<(String, String, Vec<Value>)> {
    let table = get_str(cursor)?;
    let index = get_str(cursor)?;
    let count = get_u64(cursor)?;
    let mut key = Vec::new();
    key.try_reserve(count as usize)
        .map_err(|_| Error::invalid_argument("lookup key count exceeds memory capacity"))?;
    for _ in 0..count {
        key.push(nexum_core::binary::get_value(cursor)?);
    }
    Ok((table, index, key))
}

/// Encodes `INSERT`/`UPDATE` args: table name (+ row id) + row.
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_write(table: &str, row: &Row) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, table);
    put_row(&mut out, row);
    out
}

/// Encodes `UPDATE` args: table name + row id + row.
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_update(table: &str, row_id: u64, row: &Row) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, table);
    put_u64(&mut out, row_id);
    put_row(&mut out, row);
    out
}

/// Decodes `INSERT` args (table + row).
pub fn decode_insert(cursor: &mut &[u8]) -> Result<(String, Row)> {
    let table = get_str(cursor)?;
    let row = get_row(cursor)?;
    Ok((table, row))
}

/// Decodes `UPDATE` args (table + row id + row).
pub fn decode_update(cursor: &mut &[u8]) -> Result<(String, u64, Row)> {
    let table = get_str(cursor)?;
    let row_id = get_u64(cursor)?;
    let row = get_row(cursor)?;
    Ok((table, row_id, row))
}

/// Encodes `EMIT` args: event name + payload value.
#[cfg_attr(not(test), allow(dead_code))]
pub fn encode_emit(name: &str, payload: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    put_str(&mut out, name);
    put_value(&mut out, payload);
    out
}

/// Decodes `EMIT` args.
pub fn decode_emit(cursor: &mut &[u8]) -> Result<(String, Value)> {
    let name = get_str(cursor)?;
    let payload = nexum_core::binary::get_value(cursor)?;
    Ok((name, payload))
}

// ------------------------------------------------------------- results

/// Encodes a `GET` result: presence byte + row when present.
pub fn encode_get_result(row: Option<&Row>) -> Vec<u8> {
    let mut out = Vec::new();
    match row {
        Some(row) => {
            put_bool(&mut out, true);
            put_row(&mut out, row);
        }
        None => put_bool(&mut out, false),
    }
    out
}

/// Decodes a `GET` result (used by tests that re-encode guest output).
#[cfg_attr(not(test), allow(dead_code))]
pub fn decode_get_result(cursor: &mut &[u8]) -> Result<Option<Row>> {
    if get_bool(cursor)? {
        Ok(Some(get_row(cursor)?))
    } else {
        Ok(None)
    }
}

/// Encodes an `INSERT` result: the provisional row id.
pub fn encode_insert_result(row_id: RowId) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, row_id.as_u64());
    out
}

/// Encodes a scan result: count + (row id, row) pairs.
pub fn encode_scan_result(rows: &[(RowId, Row)]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, rows.len() as u64);
    for (row_id, row) in rows {
        put_u64(&mut out, row_id.as_u64());
        put_row(&mut out, row);
    }
    out
}

/// Decodes a scan result's row count (used by tests to observe guest reads).
#[cfg_attr(not(test), allow(dead_code))]
pub fn decode_scan_count(cursor: &mut &[u8]) -> Result<u64> {
    get_u64(cursor)
}

/// Encodes a `LOOKUP_UNIQUE` result: count + row ids.
pub fn encode_lookup_result(owners: &[RowId]) -> Vec<u8> {
    let mut out = Vec::new();
    put_u64(&mut out, owners.len() as u64);
    for owner in owners {
        put_u64(&mut out, owner.as_u64());
    }
    out
}

// --------------------------------------------------------------- arguments

/// Encodes reducer arguments deterministically: `u64 count`, then each entry
/// as `(name, value)` in key-sorted order (BTreeMap iteration).
pub fn encode_args(out: &mut Vec<u8>, args: &ReducerArgs) {
    put_u64(out, args.len() as u64);
    for (name, value) in args.iter() {
        put_str(out, name);
        put_value(out, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use nexum_core::row;

    fn roundtrip<F>(encode: F, bytes: Vec<u8>)
    where
        F: Fn(&mut &[u8]) -> Result<()>,
    {
        let mut cursor: &[u8] = &bytes;
        encode(&mut cursor).unwrap();
        assert!(cursor.is_empty(), "whole input consumed");
    }

    #[test]
    fn envelopes_carry_status_and_payload() {
        let ok = envelope_ok(&[1, 2, 3]);
        assert_eq!(&ok[0..4], &0u32.to_le_bytes());
        assert_eq!(&ok[4..8], &3u32.to_le_bytes());
        assert_eq!(&ok[8..], &[1, 2, 3]);

        let err = envelope_err("boom");
        assert_eq!(&err[0..4], &1u32.to_le_bytes());
        assert_eq!(&err[4..8], &4u32.to_le_bytes());
        assert_eq!(&err[8..], b"boom");
    }

    #[test]
    fn operation_inputs_roundtrip() {
        let mut bytes = encode_table_row("players", 7);
        roundtrip(
            |c| {
                let (table, row_id) = decode_table_row(c)?;
                assert_eq!(table, "players");
                assert_eq!(row_id, 7);
                Ok(())
            },
            bytes.clone(),
        );

        bytes = encode_table("players");
        roundtrip(
            |c| {
                assert_eq!(decode_table(c).unwrap(), "players");
                Ok(())
            },
            bytes,
        );

        bytes = encode_lookup("players", "by_level", &[Value::U32(6)]);
        roundtrip(
            |c| {
                let (table, index, key) = decode_lookup(c)?;
                assert_eq!(table, "players");
                assert_eq!(index, "by_level");
                assert_eq!(key, vec![Value::U32(6)]);
                Ok(())
            },
            bytes,
        );

        let player = row![1u64, 10u64, 100i32, 5u32];
        bytes = encode_write("players", &player);
        roundtrip(
            |c| {
                let (table, row) = decode_insert(c)?;
                assert_eq!(table, "players");
                assert_eq!(row, player);
                Ok(())
            },
            bytes,
        );

        bytes = encode_update("players", 3, &player);
        roundtrip(
            |c| {
                let (table, row_id, row) = decode_update(c)?;
                assert_eq!(table, "players");
                assert_eq!(row_id, 3);
                assert_eq!(row, player);
                Ok(())
            },
            bytes,
        );

        bytes = encode_emit("joined", &Value::U64(42));
        roundtrip(
            |c| {
                let (name, payload) = decode_emit(c)?;
                assert_eq!(name, "joined");
                assert_eq!(payload, Value::U64(42));
                Ok(())
            },
            bytes,
        );
    }

    #[test]
    fn get_and_scan_results_roundtrip() {
        let player = row![1u64, 10u64, 100i32, 5u32];
        let bytes = encode_get_result(Some(&player));
        let mut cursor: &[u8] = &bytes;
        assert_eq!(
            decode_get_result(&mut cursor).unwrap(),
            Some(player.clone())
        );

        let bytes = encode_get_result(None);
        let mut cursor: &[u8] = &bytes;
        assert!(decode_get_result(&mut cursor).unwrap().is_none());

        let bytes = encode_scan_result(&[(RowId::from_u64(0), player)]);
        let mut cursor: &[u8] = &bytes;
        assert_eq!(decode_scan_count(&mut cursor).unwrap(), 1);
    }

    #[test]
    fn args_encode_key_sorted() {
        let args = ReducerArgs::new()
            .insert("zeta", 1u64)
            .insert("alpha", "x")
            .insert("mid", true);
        let mut bytes = Vec::new();
        encode_args(&mut bytes, &args);
        // Deterministic key-sorted encoding: alpha, mid, zeta (each name is a
        // u64-length-prefixed string).
        assert_eq!(bytes[0..8], 3u64.to_le_bytes());
        assert_eq!(bytes[8..16], 5u64.to_le_bytes());
        assert_eq!(&bytes[16..21], b"alpha");
    }

    #[test]
    fn unknown_opcode_is_rejected() {
        assert!(opcode(0).is_err());
        assert!(opcode(10).is_err());
        assert_eq!(opcode(OP_GET).unwrap(), OP_GET);
        assert_eq!(opcode(OP_EMIT).unwrap(), OP_EMIT);
        assert_eq!(opcode(OP_LOOKUP_INDEX).unwrap(), OP_LOOKUP_INDEX);
    }
}
