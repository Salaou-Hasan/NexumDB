//! The WASM host: per-invocation state, memory limiting, and the single
//! `("nexum","op")` host function (design doc §3–§5, ADR-007 D2–D5).
//!
//! Security model:
//!
//! - The guest holds **no reference** into Nexum: every op translates into a
//!   [`ReducerContext`] call that the host owns.
//! - Inputs are copied out of guest memory **first**, bounded by the
//!   configured argument budget, so a malicious guest-provided length can
//!   never drive a large allocation or an out-of-bounds read.
//! - The transaction context is never touched while guest memory is
//!   borrowed, so no borrow crosses the host-call boundary.
//! - Every ABI error is recorded **sticky** in the host state, so an ignored
//!   status can never let a failed op slip through to commit.
//! - Memory growth is arbitrated by [`MemoryLimiter`] against the configured
//!   ceiling; fuel and the host-call budget bound execution.

use nexum_core::{binary::put_value, Error, RowId};
use nexum_reducer::ReducerContext;
use wasmi::errors::{MemoryError, TableError};
use wasmi::{Caller, Error as WasmError, Linker, Memory, ResourceLimiter};

use crate::abi::{
    decode_emit, decode_insert, decode_lookup, decode_table, decode_table_row, decode_update,
    encode_get_result, encode_insert_result, encode_lookup_result, encode_scan_result,
    envelope_err, envelope_ok, opcode, OP_CONTAINS, OP_DELETE, OP_EMIT, OP_GET, OP_INSERT,
    OP_LOOKUP_UNIQUE, OP_SCAN, OP_UPDATE,
};
use crate::limits::WasmLimits;

/// The per-invocation host state, borrowed from the invocation's transaction
/// context. Lives inside the wasmi `Store` for the duration of one run.
///
/// `ctx` is `None` only during registration-time module validation, when no
/// transaction exists and no op can execute (modules with start functions are
/// rejected, so instantiation never runs guest code).
pub struct HostState<'a, 'b> {
    ctx: Option<&'a mut ReducerContext<'b>>,
    limits: &'a WasmLimits,
    /// The store-level resource limiter, provided to the `Store` through the
    /// closure form of `Store::limiter`.
    pub(crate) memory_limiter: MemoryLimiter,
    host_calls_remaining: u32,
    /// The first ABI error, sticky: if set, the invocation can never commit.
    abi_error: Option<Error>,
}

impl<'a, 'b> HostState<'a, 'b> {
    /// Creates host state for an invocation (`ctx` is `Some`) or for
    /// registration-time validation (`ctx` is `None`).
    pub(crate) fn new(ctx: Option<&'a mut ReducerContext<'b>>, limits: &'a WasmLimits) -> Self {
        Self {
            ctx,
            limits,
            memory_limiter: MemoryLimiter {
                max_memory_bytes: limits.max_memory_bytes,
            },
            host_calls_remaining: limits.max_host_calls,
            abi_error: None,
        }
    }

    /// Returns the sticky ABI error, if any.
    pub(crate) fn abi_error(&self) -> Option<&Error> {
        self.abi_error.as_ref()
    }
}

/// The memory ceiling applied to every `memory.grow`.
///
/// A separate type from [`HostState`] so registration-time validation (which
/// has no transaction context) and invocation-time execution share the same
/// limiter without double-borrowing the store's host data.
pub struct MemoryLimiter {
    /// Maximum linear memory in bytes.
    pub max_memory_bytes: usize,
}

impl ResourceLimiter for MemoryLimiter {
    fn memory_growing(
        &mut self,
        _current: usize,
        desired: usize,
        _maximum: Option<usize>,
    ) -> std::result::Result<bool, MemoryError> {
        Ok(desired <= self.max_memory_bytes)
    }

    fn table_growing(
        &mut self,
        _current: u32,
        desired: u32,
        _maximum: Option<u32>,
    ) -> std::result::Result<bool, TableError> {
        Ok(desired <= 1_000_000)
    }
}

/// Registers the single allowed host import: `("nexum","op")`.
///
/// The closure captures nothing — every op reads its state from the
/// `Caller` — so it is `Send + Sync + 'static` regardless of the host data
/// lifetimes, and the same definition works for registration-time validation
/// and invocation-time execution.
pub(crate) fn define_host<'a, 'b>(linker: &mut Linker<HostState<'a, 'b>>) -> Result<(), WasmError> {
    linker.func_wrap(
        "nexum",
        "op",
        |mut caller: Caller<'_, HostState<'a, 'b>>,
         op: i32,
         in_ptr: i32,
         in_len: i32,
         out_ptr: i32,
         out_cap: i32|
         -> Result<i32, WasmError> {
            // The memory handle is owned (an `Extern`), so no borrow of
            // `caller` is held across the `data_mut` calls below.
            let memory = caller
                .get_export("memory")
                .and_then(|export| export.into_memory())
                .ok_or_else(|| WasmError::new("nexum: module does not export memory"))?;

            let (op, in_len, out_ptr, out_cap) =
                (op as u32, in_len as u32, out_ptr as u32, out_cap as u32);

            // Bound the guest-provided input length before any allocation: a
            // negative or huge `in_len` is rejected here, never reaching
            // `vec![0u8; n]`. The host-call budget is decremented on this path
            // too, so every host-function crossing counts against it.
            let max_args_bytes = caller.data().limits.max_args_bytes;
            if in_len as usize > max_args_bytes {
                let envelope = {
                    let state = caller.data_mut();
                    state.host_calls_remaining = state.host_calls_remaining.saturating_sub(1);
                    sticky(
                        state,
                        Error::invalid_argument(
                            "wasm reducer operation arguments exceed the configured limit",
                        ),
                    )
                };
                return write_envelope(&mut caller, &memory, &envelope, out_ptr, out_cap);
            }

            // Copy the args out of guest memory first; the transaction is
            // then driven with no further guest-memory access.
            let mut args = vec![0u8; in_len as usize];
            if !args.is_empty() {
                memory
                    .read(&caller, in_ptr as usize, &mut args)
                    .map_err(|e| WasmError::new(format!("nexum: cannot read args: {e}")))?;
            }

            let envelope = {
                let state = caller.data_mut();
                handle_op(state, op, &args)
            };

            write_envelope(&mut caller, &memory, &envelope, out_ptr, out_cap)
        },
    )?;
    Ok(())
}

/// Writes `envelope` to the guest output buffer: returns `0` when it fit, or
/// the required capacity when it did not (the guest may retry with a larger
/// buffer).
fn write_envelope(
    caller: &mut Caller<'_, HostState<'_, '_>>,
    memory: &Memory,
    envelope: &[u8],
    out_ptr: u32,
    out_cap: u32,
) -> Result<i32, WasmError> {
    if envelope.len() <= out_cap as usize {
        if !envelope.is_empty() {
            memory
                .write(caller, out_ptr as usize, envelope)
                .map_err(|e| WasmError::new(format!("nexum: cannot write result: {e}")))?;
        }
        Ok(0)
    } else {
        Ok(envelope.len() as i32)
    }
}

/// Dispatches one ABI op: decodes the (already copied) args, drives the
/// transaction context, and encodes the result envelope. Any failure is
/// recorded sticky and returned as an error envelope.
fn handle_op(state: &mut HostState<'_, '_>, op: u32, args: &[u8]) -> Vec<u8> {
    // Host-call budget first: bounded, deterministic work per invocation.
    if state.host_calls_remaining == 0 {
        return sticky(
            state,
            Error::capacity("wasm reducer exceeded the host-call budget"),
        );
    }
    state.host_calls_remaining -= 1;

    // Copy the byte budgets before borrowing the context mutably.
    let max_scan_bytes = state.limits.max_scan_bytes;
    let max_event_bytes = state.limits.max_event_bytes;

    let ctx = match state.ctx.as_deref_mut() {
        Some(ctx) => ctx,
        // Unreachable: registration-time validation never executes ops.
        None => {
            return sticky(
                state,
                Error::internal("host state has no transaction context"),
            );
        }
    };

    let result = match opcode(op) {
        Ok(OP_GET) => op_get(ctx, args),
        Ok(OP_CONTAINS) => op_contains(ctx, args),
        Ok(OP_SCAN) => op_scan(ctx, args, max_scan_bytes),
        Ok(OP_LOOKUP_UNIQUE) => op_lookup_unique(ctx, args, max_scan_bytes),
        Ok(OP_INSERT) => op_insert(ctx, args),
        Ok(OP_UPDATE) => op_update(ctx, args),
        Ok(OP_DELETE) => op_delete(ctx, args),
        Ok(OP_EMIT) => op_emit(ctx, args, max_event_bytes),
        Ok(_) => Err(Error::internal("unreachable opcode")),
        Err(error) => Err(error),
    };

    match result {
        Ok(payload) => envelope_ok(&payload),
        Err(error) => sticky(state, error),
    }
}

/// Records `error` as the sticky ABI error and returns an error envelope.
fn sticky(state: &mut HostState<'_, '_>, error: Error) -> Vec<u8> {
    if state.abi_error.is_none() {
        state.abi_error = Some(error.clone());
    }
    envelope_err(&error.to_string())
}

/// Reclassifies a decode failure at the ABI boundary.
///
/// The shared binary codec reports malformed/truncated input as
/// `Error::internal`, but when that input came from an untrusted guest it is
/// a guest-caused failure — surfacing it as `InvalidArgument` (never as an
/// internal host bug) keeps the error taxonomy honest.
fn decode<T>(result: nexum_core::Result<T>) -> nexum_core::Result<T> {
    result.map_err(|error| match error {
        Error::Internal(message) => {
            Error::invalid_argument(format!("malformed ABI input: {message}"))
        }
        other => other,
    })
}

fn op_get(ctx: &mut ReducerContext<'_>, args: &[u8]) -> Result<Vec<u8>, Error> {
    let (table, row_id) = decode(decode_table_row(&mut &*args))?;
    let row = ctx.get(&table, RowId::from_u64(row_id))?;
    Ok(encode_get_result(row.as_ref()))
}

fn op_contains(ctx: &mut ReducerContext<'_>, args: &[u8]) -> Result<Vec<u8>, Error> {
    let (table, row_id) = decode(decode_table_row(&mut &*args))?;
    let present = ctx.contains(&table, RowId::from_u64(row_id))?;
    Ok(vec![u8::from(present)])
}

fn op_scan(
    ctx: &mut ReducerContext<'_>,
    args: &[u8],
    max_scan_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let table = decode(decode_table(&mut &*args))?;
    let rows = ctx.scan(&table)?;
    // Fast reject before encoding: every row encodes to at least 16 bytes
    // (8 row id + 8 value count), so the byte budget implies a row bound.
    if rows.len() > max_scan_bytes / 16 {
        return Err(Error::capacity(format!(
            "scan of table '{table}' exceeds the configured scan-result limit"
        )));
    }
    let bytes = encode_scan_result(&rows);
    if bytes.len() > max_scan_bytes {
        return Err(Error::capacity(format!(
            "scan of table '{table}' exceeds the configured scan-result limit"
        )));
    }
    Ok(bytes)
}

fn op_lookup_unique(
    ctx: &mut ReducerContext<'_>,
    args: &[u8],
    max_scan_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let (table, index, key) = decode(decode_lookup(&mut &*args))?;
    let owners = ctx.lookup_unique(&table, &index, &key)?;
    // Fast reject before encoding: each owner encodes to 8 bytes.
    if owners.len() > max_scan_bytes / 8 {
        return Err(Error::capacity(format!(
            "unique lookup on table '{table}' exceeds the configured result limit"
        )));
    }
    let bytes = encode_lookup_result(&owners);
    if bytes.len() > max_scan_bytes {
        return Err(Error::capacity(format!(
            "unique lookup on table '{table}' exceeds the configured result limit"
        )));
    }
    Ok(bytes)
}

fn op_insert(ctx: &mut ReducerContext<'_>, args: &[u8]) -> Result<Vec<u8>, Error> {
    let (table, row) = decode(decode_insert(&mut &*args))?;
    let row_id = ctx.insert(&table, row)?;
    Ok(encode_insert_result(row_id))
}

fn op_update(ctx: &mut ReducerContext<'_>, args: &[u8]) -> Result<Vec<u8>, Error> {
    let (table, row_id, row) = decode(decode_update(&mut &*args))?;
    ctx.update(&table, RowId::from_u64(row_id), row)?;
    Ok(Vec::new())
}

fn op_delete(ctx: &mut ReducerContext<'_>, args: &[u8]) -> Result<Vec<u8>, Error> {
    let (table, row_id) = decode(decode_table_row(&mut &*args))?;
    ctx.delete(&table, RowId::from_u64(row_id))?;
    Ok(Vec::new())
}

fn op_emit(
    ctx: &mut ReducerContext<'_>,
    args: &[u8],
    max_event_bytes: usize,
) -> Result<Vec<u8>, Error> {
    let (name, payload) = decode(decode_emit(&mut &*args))?;
    // Encode once and measure against the event budget before buffering.
    let mut payload_buf = Vec::new();
    put_value(&mut payload_buf, &payload);
    if 8 + name.len() + payload_buf.len() > max_event_bytes {
        return Err(Error::capacity(format!(
            "event '{name}' payload exceeds the configured event-size limit"
        )));
    }
    ctx.emit(&name, payload)?;
    Ok(Vec::new())
}
