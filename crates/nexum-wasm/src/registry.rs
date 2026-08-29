//! The WASM module registry ([`WasmModuleRegistry`]) and the `invoke` entry
//! point (design doc 9, ADR-007 D6).
//!
//! The registry owns the fuel-enabled engine, the resource limits, and a
//! deterministic map of validated, compiled modules. `invoke` is the **only**
//! way to execute a WASM reducer and follows the Phase 6 contract exactly:
//!
//! ```text
//! begin one transaction
//!    build a ReducerContext (the host owns it; the guest never sees it)
//!    instantiate the module with fresh host state and run the entry point
//!    sticky ABI errors / traps / fuel exhaustion / malformed returns fail
//!    finish_invocation: commit on success, abort otherwise
//! ```
//!
//! `finish_invocation` is the same shared decision point the native registry
//! uses (ADR-006 D1 / ADR-007 D6), so both paths have identical transaction
//! semantics. The caller appends `result.changes` to the WAL with
//! `result.tx_id`  the Phase 5 boundary is untouched.

use std::collections::BTreeMap;

use nexum_core::{Error, Result, Value, binary::get_value};
use nexum_reducer::{ReducerArgs, ReducerContext, ReducerEvent, ReducerResult, finish_invocation};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use wasmtime::{Config, Engine, Store};

use crate::abi::{RET_REJECT, encode_args};
use crate::host::HostState;
use crate::limits::WasmLimits;
use crate::module::{ENTRY_NAME, WasmReducerModule};

/// A registry of validated WASM reducer modules.
#[derive(Debug)]
pub struct WasmModuleRegistry {
    modules: BTreeMap<String, WasmReducerModule>,
    engine: Engine,
    limits: WasmLimits,
}

impl WasmModuleRegistry {
    /// Creates an empty registry enforcing `limits`.
    ///
    /// The engine is configured with deterministic fuel metering, a bounded
    /// interpreter stack, and strict compile limits for hostile modules.
    pub fn new(limits: WasmLimits) -> Result<Self> {
        limits.validate()?;
        let mut config = Config::new();
        // Enable fuel metering for deterministic execution budgeting
        config.consume_fuel(true);
        // Configure memory limits
        config.memory_init_cow(true);
        config.memory_guaranteed_dense_image_size(0);
        // Configure compilation settings
        config.cranelift_opt_level(wasmtime::OptLevel::Speed);
        // Create the engine
        let engine = Engine::new(&config)
            .map_err(|e| Error::invalid_argument(format!("invalid wasm config: {e}")))?;
        Ok(Self {
            modules: BTreeMap::new(),
            engine,
            limits,
        })
    }

    /// Validates and registers a module, or `AlreadyExists` if the name is
    /// taken. The compiled module is cached for reuse.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        version: u64,
        bytecode: Vec<u8>,
    ) -> Result<()> {
        let name = name.into();
        if self.modules.contains_key(&name) {
            return Err(Error::already_exists(format!(
                "wasm module '{name}' is already registered"
            )));
        }
        let module =
            WasmReducerModule::new(&self.engine, name.clone(), version, bytecode, &self.limits)?;
        self.modules.insert(name, module);
        Ok(())
    }

    /// Looks up a registered module by name.
    pub fn lookup(&self, name: &str) -> Option<&WasmReducerModule> {
        self.modules.get(name)
    }

    /// Opts a registered module into per-thread `Store`/`Instance` pooling
    /// (Phase 26). Only valid for stateless scratch-memory modules:
    /// immutable globals, no start-function side effects, every ABI output
    /// region rewritten before the host reads it.
    pub fn set_poolable(&mut self, name: &str, yes: bool) -> Result<()> {
        self.modules
            .get_mut(name)
            .ok_or_else(|| Error::not_found(format!("wasm module '{name}' is not registered")))?
            .set_poolable(yes);
        Ok(())
    }

    /// Returns `true` if a module with the given name is registered.
    pub fn contains(&self, name: &str) -> bool {
        self.modules.contains_key(name)
    }

    /// Returns the number of registered modules.
    pub fn len(&self) -> usize {
        self.modules.len()
    }

    /// Returns `true` if no modules are registered.
    pub fn is_empty(&self) -> bool {
        self.modules.is_empty()
    }

    /// Iterates over every module in deterministic (ascending name) order.
    pub fn list(&self) -> impl Iterator<Item = &WasmReducerModule> {
        self.modules.values()
    }

    /// Returns the enforced resource limits.
    pub fn limits(&self) -> &WasmLimits {
        &self.limits
    }

    /// Executes the named WASM reducer against `store` in **exactly one
    /// transaction** (ADR-007 D6).
    ///
    /// On success the result carries the committed changes, the emitted
    /// events, and the encoded return value. On any failure  reducer
    /// rejection, sticky ABI error, trap, fuel exhaustion, malformed return,
    /// OCC conflict  the transaction is aborted through the shared
    /// `finish_invocation` path: zero authoritative mutations, zero events,
    /// zero committed changes.
    pub fn invoke(
        &self,
        store: &mut TableStore,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<ReducerResult> {
        let module = self
            .lookup(name)
            .ok_or_else(|| Error::not_found(format!("wasm module '{name}' is not registered")))?;

        let mut tx = Transaction::begin(store);
        let (events, outcome) = {
            let mut ctx = ReducerContext::new(&mut tx, store);
            let outcome = run_module(&self.engine, module, &mut ctx, &self.limits, args, None);
            let events = ctx.take_events();
            (events, outcome)
        };

        // The single commit/abort decision point shared with the native
        // registry: `Error::Conflict` from commit propagates unchanged.
        finish_invocation(store, tx, events, outcome)
    }

    /// Runs the named WASM reducer against **an existing transaction**
    /// without committing, collecting a per-stage time breakdown (Phase 22
    /// instrumentation; the hot-path entry points pass `None` and pay zero
    /// cost). Returns the outcome plus the stage times.
    pub fn invoke_in_tx_timed(
        &self,
        store: &TableStore,
        tx: &mut Transaction,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<(Value, Vec<ReducerEvent>, WasmStageTimes)> {
        let module = self
            .lookup(name)
            .ok_or_else(|| Error::not_found(format!("wasm module '{name}' is not registered")))?;
        let mut ctx = ReducerContext::new(tx, store);
        let mut times = WasmStageTimes::default();
        let outcome = run_module(
            &self.engine,
            module,
            &mut ctx,
            &self.limits,
            args,
            Some(&mut times),
        );
        let events = ctx.take_events();
        match outcome {
            Ok(value) => Ok((value, events, times)),
            Err(error) => Err(error),
        }
    }

    /// Runs the named WASM reducer against **an existing transaction**
    /// without committing (ADR-009 D3).
    ///
    /// This is the simulation tick's orchestration hook: a WASM reducer
    /// invoked during a tick runs inside the tick's transaction so the whole
    /// tick commits atomically (or aborts completely). The sandbox is the
    /// same  the same host ABI, fuel/memory/host-call budgets, sticky
    /// error, and trap handling  only the transaction ownership differs:
    /// the caller owns the transaction and the commit/abort decision. On
    /// success the encoded return value and the emitted events (in `emit`
    /// order) are returned; on any failure the error propagates and the
    /// events are dropped.
    ///
    /// Standalone [`invoke`](Self::invoke)  one invocation = one
    /// transaction  is unchanged and remains the external entry point.
    pub fn invoke_in_tx(
        &self,
        store: &TableStore,
        tx: &mut Transaction,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<(Value, Vec<ReducerEvent>)> {
        let module = self
            .lookup(name)
            .ok_or_else(|| Error::not_found(format!("wasm module '{name}' is not registered")))?;
        let mut ctx = ReducerContext::new(tx, store);
        let outcome = run_module(&self.engine, module, &mut ctx, &self.limits, args, None);
        let events = ctx.take_events();
        match outcome {
            Ok(value) => Ok((value, events)),
            Err(error) => Err(error),
        }
    }
}

/// A per-invocation stage-time breakdown (Phase 22 instrumentation).
///
/// `store_setup` covers `Store` + host-state construction; `instantiate`
/// covers `Linker` + `define_host` + module instantiation + start; `encode`
/// covers argument encoding and the guest-memory copy; `exec` covers the
/// guest entry call (including every host ABI call); `result` covers the
/// return-value read and decode. Filled only by `invoke_in_tx_timed`;
/// hot-path entries pass `None` and the stages are skipped.
#[derive(Debug, Default, Clone, Copy)]
pub struct WasmStageTimes {
    /// Store + host state construction.
    pub store_setup_ns: u64,
    /// Linker + host-function definition + instantiate + start.
    pub instantiate_ns: u64,
    /// Argument encoding + guest-memory write.
    pub encode_ns: u64,
    /// The guest entry call (guest instructions + all host ABI calls).
    pub exec_ns: u64,
    /// Return-value read + decode.
    pub result_ns: u64,
    /// Total wall time of the invocation.
    pub total_ns: u64,
}

/// Runs one module invocation against `ctx`'s transaction.
///
/// Every instantiation is fresh: a new `Store` holds the new host state
/// (which borrows the invocation's context), the fuel budget is armed before
/// any guest code runs, and the entry point is the only guest function ever
/// called. `args` are encoded deterministically into the guest input buffer.
/// When `times` is `Some`, each stage is timed (Phase 22 instrumentation).
///
/// Phase 26: modules flagged **poolable** reuse a per-thread
/// `Store`/`Instance` across invocations instead of rebuilding them per call
/// (~3.3 s saved each). Pooling is gated behind an explicit opt-in because
/// it requires a stateless scratch-memory module (immutable globals, every
/// ABI output region rewritten before the host reads it); between calls the
/// pooled state always holds `ctx: None`, so no reference outlives its
/// referent.
fn run_module(
    engine: &Engine,
    module: &WasmReducerModule,
    ctx: &mut ReducerContext<'_>,
    limits: &WasmLimits,
    args: &ReducerArgs,
    mut times: Option<&mut WasmStageTimes>,
) -> Result<Value> {
    if module.is_poolable() {
        run_module_pooled(engine, module, ctx, limits, args, times.as_deref_mut())
    } else {
        run_module_fresh(engine, module, ctx, limits, args, times)
    }
}

/// A per-thread pooled WASM invocation: everything expensive about an
/// invocation (`Store` + host state, instantiation, export lookup) built
/// once and reused while the engine and module stay the same.
struct PooledInvocation {
    engine: Engine,
    /// Identity of the originating module (its registry address).
    module_key: usize,
    /// Stashed with `ctx: None`; see `HostState::reset_for_call`.
    store: Store<HostState<'static, 'static>>,
    /// Kept for the lifetime of the pooled unit; exports resolve through it.
    #[allow(dead_code)]
    instance: wasmtime::Instance,
    memory: wasmtime::Memory,
    run: wasmtime::TypedFunc<(), i32>,
}

impl PooledInvocation {
    fn matches(&self, engine: &Engine, module_key: usize) -> bool {
        // Compare engines by pointer identity (same Arc-backed Engine)
        std::ptr::eq(&self.engine as *const Engine, engine as *const Engine)
            && self.module_key == module_key
    }
}

thread_local! {
    static POOL: std::cell::RefCell<Option<PooledInvocation>> = const { std::cell::RefCell::new(None) };
}

/// The pooled invocation path (Phase 26).
fn run_module_pooled(
    engine: &Engine,
    module: &WasmReducerModule,
    ctx: &mut ReducerContext<'_>,
    limits: &WasmLimits,
    args: &ReducerArgs,
    mut times: Option<&mut WasmStageTimes>,
) -> Result<Value> {
    let module_key = module as *const WasmReducerModule as usize;
    let total_start = std::time::Instant::now();

    // Take the per-thread slot out for the duration of the call (avoids
    // holding the RefCell borrow across guest execution).
    let mut pooled = match POOL.with(|cell| cell.borrow_mut().take()) {
        Some(p) if p.matches(engine, module_key) => p,
        stale_or_none => {
            let _ = stale_or_none; // mismatched engine/module: dropped here
            let stage_start = std::time::Instant::now();
            // SAFETY: Store::new requires T: 'static, but HostState<'a, 'b>
            // is only used within the function scope and the store is dropped
            // at the end of the function (or returned to the pool with ctx: None).
            let host_state = HostState::new(None, limits);
            let mut store: Store<HostState<'static, 'static>> = unsafe {
                Store::new(
                    engine,
                    std::mem::transmute::<HostState<'_, '_>, HostState<'static, 'static>>(
                        host_state,
                    ),
                )
            };
            store.limiter(|state: &mut HostState<'_, '_>| &mut state.memory_limiter);
            store
                .set_fuel(limits.max_fuel)
                .map_err(|e| Error::internal(format!("cannot arm fuel: {e}")))?;
            let instantiate_start = std::time::Instant::now();
            let linker = crate::linker_cache::clone_cached_linker(engine);
            let instance = linker
                .instantiate(&mut store, module.compiled())
                .map_err(|e| Error::internal(format!("cannot instantiate module: {e}")))?;
            let memory = instance
                .get_export(&mut store, "memory")
                .and_then(|export| export.into_memory())
                .ok_or_else(|| Error::internal("validated module has no memory export"))?;
            let run = instance
                .get_typed_func::<(), i32>(&mut store, ENTRY_NAME)
                .map_err(|e| {
                    Error::internal(format!("cannot access the reducer entry point: {e}"))
                })?;
            if let Some(ref mut t) = times {
                t.store_setup_ns = (instantiate_start - stage_start).as_nanos() as u64;
                t.instantiate_ns = instantiate_start.elapsed().as_nanos() as u64;
            }
            PooledInvocation {
                engine: engine.clone(),
                module_key,
                store,
                instance,
                memory,
                run,
            }
        }
    };

    let outcome = (|| -> Result<Value> {
        // SAFETY: reinstate this invocation's real borrows through
        // `reset_for_call`; released unconditionally below.
        let store: &mut Store<HostState<'_, '_>> = unsafe {
            &mut *(&mut pooled.store as *mut Store<HostState<'static, 'static>> as *mut _)
        };
        // SAFETY: the ctx and limits references are used only within this
        // closure and cleared by release_ctx at the end. The store is
        // transmuted back to 'static at the end of the closure.
        unsafe {
            let ctx_static: &mut ReducerContext<'static> = std::mem::transmute(ctx);
            let limits_static: &WasmLimits = std::mem::transmute(limits);
            store
                .data_mut()
                .reset_for_call(Some(ctx_static), limits_static);
        }
        store
            .set_fuel(limits.max_fuel)
            .map_err(|e| Error::internal(format!("cannot arm fuel: {e}")))?;

        let encode_start = std::time::Instant::now();
        let mut args_bytes = Vec::new();
        encode_args(&mut args_bytes, args);
        if args_bytes.len() > limits.max_args_bytes {
            return Err(Error::capacity(format!(
                "reducer arguments exceed the configured limit of {} bytes",
                limits.max_args_bytes
            )));
        }
        pooled
            .memory
            .write(&mut *store, module.in_ptr() as usize, &args_bytes)
            .map_err(|e| {
                Error::internal(format!(
                    "cannot write reducer arguments into guest memory: {e}"
                ))
            })?;
        if let Some(ref mut t) = times {
            t.encode_ns = encode_start.elapsed().as_nanos() as u64;
        }

        let exec_start = std::time::Instant::now();
        let returned = match pooled.run.call(&mut *store, ()) {
            Ok(returned) => returned,
            Err(error) => {
                // Check if this is a fuel exhaustion trap
                if error.is::<wasmtime::Trap>() {
                    let trap = error.downcast_ref::<wasmtime::Trap>().unwrap();
                    if matches!(trap, wasmtime::Trap::OutOfFuel) {
                        return Err(Error::capacity(
                            "wasm reducer exhausted its fuel budget (max_fuel exceeded)",
                        ));
                    }
                }
                return Err(Error::invalid_argument(format!(
                    "wasm reducer trapped during execution: {error}"
                )));
            }
        };
        if let Some(ref mut t) = times {
            t.exec_ns = exec_start.elapsed().as_nanos() as u64;
        }

        if let Some(error) = store.data().abi_error() {
            return Err(error.clone());
        }

        if returned == RET_REJECT as i32 {
            let mut len_bytes = [0u8; 4];
            pooled
                .memory
                .read(&mut *store, module.out_ptr() as usize, &mut len_bytes)
                .map_err(|e| {
                    Error::internal(format!("cannot read rejection message length: {e}"))
                })?;
            let msg_len = u32::from_le_bytes(len_bytes) as usize;
            if msg_len > limits.max_result_bytes.saturating_sub(4) {
                return Err(Error::capacity(
                    "rejection message exceeds the configured result limit",
                ));
            }
            let mut msg = vec![0u8; msg_len];
            pooled
                .memory
                .read(&mut *store, module.out_ptr() as usize + 4, &mut msg)
                .map_err(|e| Error::internal(format!("cannot read rejection message: {e}")))?;
            let msg = String::from_utf8(msg)
                .map_err(|_| Error::invalid_argument("rejection message is not valid UTF-8"))?;
            return Err(Error::invalid_argument(msg));
        }

        let result_start = std::time::Instant::now();
        let n = returned as usize;
        if n > limits.max_result_bytes {
            return Err(Error::capacity(format!(
                "reducer return value exceeds the configured limit of {} bytes",
                limits.max_result_bytes
            )));
        }
        let mut buf = vec![0u8; n];
        pooled
            .memory
            .read(&mut *store, module.out_ptr() as usize, &mut buf)
            .map_err(|e| Error::internal(format!("cannot read reducer return value: {e}")))?;
        let mut cursor: &[u8] = &buf;
        let value = get_value(&mut cursor)
            .map_err(|_| Error::invalid_argument("reducer return value is malformed"))?;
        if let Some(ref mut t) = times {
            t.result_ns = result_start.elapsed().as_nanos() as u64;
        }
        Ok(value)
    })();

    // Unconditional cleanup on every exit path: drop the invocation's
    // borrows, then return the pooled state to the thread slot.
    {
        let store_ptr: *mut Store<HostState<'static, 'static>> = &mut pooled.store;
        // SAFETY: pointer derived from the same value; no other reference
        // is live here (the closure above has ended).
        unsafe { (*store_ptr).data_mut().release_ctx() };
    }
    POOL.with(|cell| *cell.borrow_mut() = Some(pooled));
    if let Some(ref mut t) = times {
        t.total_ns = total_start.elapsed().as_nanos() as u64;
    }
    outcome
}

/// The legacy fresh-instantiation path (unchanged semantics): every call
/// builds a new `Store`/`Instance` and drops them afterwards.
fn run_module_fresh(
    engine: &Engine,
    module: &WasmReducerModule,
    ctx: &mut ReducerContext<'_>,
    limits: &WasmLimits,
    args: &ReducerArgs,
    mut times: Option<&mut WasmStageTimes>,
) -> Result<Value> {
    let total_start = std::time::Instant::now();
    let stage_start = total_start;
    // SAFETY: Store::new requires T: 'static, but HostState<'a, 'b>
    // is only used within the function scope and the store is dropped
    // at the end of the function.
    let host_state = HostState::new(Some(ctx), limits);
    let mut store: Store<HostState<'static, 'static>> = unsafe {
        Store::new(
            engine,
            std::mem::transmute::<HostState<'_, '_>, HostState<'static, 'static>>(host_state),
        )
    };
    store.limiter(|state: &mut HostState<'_, '_>| &mut state.memory_limiter);
    // Arm the fuel budget before instantiation: a module start function (if
    // any) runs at `InstancePre::start` under the same deterministic budget.
    store
        .set_fuel(limits.max_fuel)
        .map_err(|e| Error::internal(format!("cannot arm fuel: {e}")))?;
    if let Some(ref mut t) = times {
        t.store_setup_ns = stage_start.elapsed().as_nanos() as u64;
    }
    let instantiate_start = std::time::Instant::now();
    // Phase 22.5: reuse a pre-configured Linker per thread. The host ABI
    // definition ("nexum","op") is identical for every invocation and the
    // closure captures nothing  it is Send + Sync + 'static regardless of
    // HostState lifetimes (see host.rs docs). Caching eliminates Linker::new
    // + define_host (~2-3s) per WASM call; the clone is ~200ns.
    let linker = crate::linker_cache::clone_cached_linker(engine);
    let instance = linker
        .instantiate(&mut store, module.compiled())
        .map_err(|e| Error::internal(format!("cannot instantiate module: {e}")))?;

    let memory = instance
        .get_export(&mut store, "memory")
        .and_then(|export| export.into_memory())
        .ok_or_else(|| Error::internal("validated module has no memory export"))?;
    if let Some(ref mut t) = times {
        t.instantiate_ns = instantiate_start.elapsed().as_nanos() as u64;
    }

    // Write the encoded arguments into the guest input buffer, bounded by
    // the configured budget before any guest code runs.
    let encode_start = std::time::Instant::now();
    let mut args_bytes = Vec::new();
    encode_args(&mut args_bytes, args);
    if args_bytes.len() > limits.max_args_bytes {
        return Err(Error::capacity(format!(
            "reducer arguments exceed the configured limit of {} bytes",
            limits.max_args_bytes
        )));
    }
    memory
        .write(&mut store, module.in_ptr() as usize, &args_bytes)
        .map_err(|e| {
            Error::internal(format!(
                "cannot write reducer arguments into guest memory: {e}"
            ))
        })?;

    let run = instance
        .get_typed_func::<(), i32>(&mut store, ENTRY_NAME)
        .map_err(|e| Error::internal(format!("cannot access the reducer entry point: {e}")))?;
    if let Some(ref mut t) = times {
        t.encode_ns = encode_start.elapsed().as_nanos() as u64;
    }

    let exec_start = std::time::Instant::now();
    let returned = match run.call(&mut store, ()) {
        Ok(returned) => returned,
        Err(error) => {
            // Check if this is a fuel exhaustion trap
            if error.is::<wasmtime::Trap>() {
                let trap = error.downcast_ref::<wasmtime::Trap>().unwrap();
                if matches!(trap, wasmtime::Trap::OutOfFuel) {
                    return Err(Error::capacity(
                        "wasm reducer exhausted its fuel budget (max_fuel exceeded)",
                    ));
                }
            }
            return Err(Error::invalid_argument(format!(
                "wasm reducer trapped during execution: {error}"
            )));
        }
    };
    if let Some(ref mut t) = times {
        t.exec_ns = exec_start.elapsed().as_nanos() as u64;
    }

    // A sticky ABI error can never be ignored: even if the guest returned
    // normally, a failed op means the invocation aborts.
    if let Some(error) = store.data().abi_error() {
        return Err(error.clone());
    }

    // Application rejection: the guest wrote `[msg_len: u32][utf8 message]`.
    if returned == RET_REJECT as i32 {
        let mut len_bytes = [0u8; 4];
        memory
            .read(&mut store, module.out_ptr() as usize, &mut len_bytes)
            .map_err(|e| Error::internal(format!("cannot read rejection message length: {e}")))?;
        let msg_len = u32::from_le_bytes(len_bytes) as usize;
        if msg_len > limits.max_result_bytes.saturating_sub(4) {
            return Err(Error::capacity(
                "rejection message exceeds the configured result limit",
            ));
        }
        let mut msg = vec![0u8; msg_len];
        memory
            .read(&mut store, module.out_ptr() as usize + 4, &mut msg)
            .map_err(|e| Error::internal(format!("cannot read rejection message: {e}")))?;
        let msg = String::from_utf8(msg)
            .map_err(|_| Error::invalid_argument("rejection message is not valid UTF-8"))?;
        return Err(Error::invalid_argument(msg));
    }

    // Success: `returned` bytes of a single encoded `Value` at out_ptr.
    let result_start = std::time::Instant::now();
    let n = returned as usize;
    if n > limits.max_result_bytes {
        return Err(Error::capacity(format!(
            "reducer return value exceeds the configured limit of {} bytes",
            limits.max_result_bytes
        )));
    }
    let mut buf = vec![0u8; n];
    memory
        .read(&mut store, module.out_ptr() as usize, &mut buf)
        .map_err(|e| Error::internal(format!("cannot read reducer return value: {e}")))?;
    let mut cursor: &[u8] = &buf;
    let value = get_value(&mut cursor)
        .map_err(|_| Error::invalid_argument("reducer return value is malformed"))?;
    if let Some(ref mut t) = times {
        t.result_ns = result_start.elapsed().as_nanos() as u64;
        t.total_ns = total_start.elapsed().as_nanos() as u64;
    }
    Ok(value)
}
