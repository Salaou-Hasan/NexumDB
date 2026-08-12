//! The WASM module registry ([`WasmModuleRegistry`]) and the `invoke` entry
//! point (design doc §9, ADR-007 D6).
//!
//! The registry owns the fuel-enabled engine, the resource limits, and a
//! deterministic map of validated, compiled modules. `invoke` is the **only**
//! way to execute a WASM reducer and follows the Phase 6 contract exactly:
//!
//! ```text
//! begin one transaction
//!   → build a ReducerContext (the host owns it; the guest never sees it)
//!   → instantiate the module with fresh host state and run the entry point
//!   → sticky ABI errors / traps / fuel exhaustion / malformed returns fail
//!   → finish_invocation: commit on success, abort otherwise
//! ```
//!
//! `finish_invocation` is the same shared decision point the native registry
//! uses (ADR-006 D1 / ADR-007 D6), so both paths have identical transaction
//! semantics. The caller appends `result.changes` to the WAL with
//! `result.tx_id` — the Phase 5 boundary is untouched.

use std::collections::BTreeMap;

use nexum_core::{binary::get_value, Error, Result, Value};
use nexum_reducer::{
    finish_invocation, ReducerArgs, ReducerContext, ReducerEvent, ReducerResult,
};
use nexum_table::TableStore;
use nexum_tx::Transaction;
use wasmi::core::TrapCode;
use wasmi::{Config, Engine, EnforcedLimits, Linker, StackLimits, Store};

use crate::abi::{encode_args, RET_REJECT};
use crate::host::{define_host, HostState};
use crate::limits::WasmLimits;
use crate::module::{WasmReducerModule, ENTRY_NAME};

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
        let mut config = Config::default();
        config.consume_fuel(true);
        config.set_stack_limits(
            StackLimits::new(32, 65_536, 1_024)
                .map_err(|e| Error::invalid_argument(format!("invalid stack limits: {e}")))?,
        );
        config.enforced_limits(EnforcedLimits::strict());
        let engine = Engine::new(&config);
        Ok(Self {
            modules: BTreeMap::new(),
            engine,
            limits,
        })
    }

    /// Validates and registers a module, or `AlreadyExists` if the name is
    /// taken. The compiled module is cached for reuse.
    pub fn register(&mut self, name: impl Into<String>, version: u64, bytecode: Vec<u8>) -> Result<()> {
        let name = name.into();
        if self.modules.contains_key(&name) {
            return Err(Error::already_exists(format!(
                "wasm module '{name}' is already registered"
            )));
        }
        let module = WasmReducerModule::new(&self.engine, name.clone(), version, bytecode, &self.limits)?;
        self.modules.insert(name, module);
        Ok(())
    }

    /// Looks up a registered module by name.
    pub fn lookup(&self, name: &str) -> Option<&WasmReducerModule> {
        self.modules.get(name)
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
    /// events, and the encoded return value. On any failure — reducer
    /// rejection, sticky ABI error, trap, fuel exhaustion, malformed return,
    /// OCC conflict — the transaction is aborted through the shared
    /// `finish_invocation` path: zero authoritative mutations, zero events,
    /// zero committed changes.
    pub fn invoke(
        &self,
        store: &mut TableStore,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<ReducerResult> {
        let module = self.lookup(name).ok_or_else(|| {
            Error::not_found(format!("wasm module '{name}' is not registered"))
        })?;

        let mut tx = Transaction::begin(store);
        let (events, outcome) = {
            let mut ctx = ReducerContext::new(&mut tx, store);
            let outcome = run_module(&self.engine, module, &mut ctx, &self.limits, args);
            let events = ctx.take_events();
            (events, outcome)
        };

        // The single commit/abort decision point shared with the native
        // registry: `Error::Conflict` from commit propagates unchanged.
        finish_invocation(store, tx, events, outcome)
    }

    /// Runs the named WASM reducer against **an existing transaction**
    /// without committing (ADR-009 D3).
    ///
    /// This is the simulation tick's orchestration hook: a WASM reducer
    /// invoked during a tick runs inside the tick's transaction so the whole
    /// tick commits atomically (or aborts completely). The sandbox is the
    /// same — the same host ABI, fuel/memory/host-call budgets, sticky
    /// error, and trap handling — only the transaction ownership differs:
    /// the caller owns the transaction and the commit/abort decision. On
    /// success the encoded return value and the emitted events (in `emit`
    /// order) are returned; on any failure the error propagates and the
    /// events are dropped.
    ///
    /// Standalone [`invoke`](Self::invoke) — one invocation = one
    /// transaction — is unchanged and remains the external entry point.
    pub fn invoke_in_tx(
        &self,
        store: &TableStore,
        tx: &mut Transaction,
        name: &str,
        args: &ReducerArgs,
    ) -> Result<(Value, Vec<ReducerEvent>)> {
        let module = self.lookup(name).ok_or_else(|| {
            Error::not_found(format!("wasm module '{name}' is not registered"))
        })?;
        let mut ctx = ReducerContext::new(tx, store);
        let outcome = run_module(&self.engine, module, &mut ctx, &self.limits, args);
        let events = ctx.take_events();
        match outcome {
            Ok(value) => Ok((value, events)),
            Err(error) => Err(error),
        }
    }
}

/// Runs one module invocation against `ctx`'s transaction.
///
/// Every instantiation is fresh: a new `Store` holds the new host state
/// (which borrows the invocation's context), the fuel budget is armed before
/// any guest code runs, and the entry point is the only guest function ever
/// called. `args` are encoded deterministically into the guest input buffer.
fn run_module(
    engine: &Engine,
    module: &WasmReducerModule,
    ctx: &mut ReducerContext<'_>,
    limits: &WasmLimits,
    args: &ReducerArgs,
) -> Result<Value> {
    let mut store = Store::new(engine, HostState::new(Some(ctx), limits));
    store.limiter(|state: &mut HostState<'_, '_>| &mut state.memory_limiter);
    // Arm the fuel budget before instantiation: a module start function (if
    // any) runs at `InstancePre::start` under the same deterministic budget.
    store
        .set_fuel(limits.max_fuel)
        .map_err(|e| Error::internal(format!("cannot arm fuel: {e}")))?;
    let mut linker = Linker::new(engine);
    define_host(&mut linker)
        .map_err(|e| Error::internal(format!("cannot define host functions: {e}")))?;
    let instance = linker
        .instantiate(&mut store, module.compiled())
        .and_then(|instance| instance.start(&mut store))
        .map_err(|e| Error::internal(format!("cannot instantiate module: {e}")))?;

    let memory = instance
        .get_export(&store, "memory")
        .and_then(|export| export.into_memory())
        .ok_or_else(|| Error::internal("validated module has no memory export"))?;

    // Write the encoded arguments into the guest input buffer, bounded by
    // the configured budget before any guest code runs.
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
        .map_err(|e| Error::internal(format!("cannot write reducer arguments into guest memory: {e}")))?;

    let run = instance
        .get_typed_func::<(), i32>(&store, ENTRY_NAME)
        .map_err(|e| Error::internal(format!("cannot access the reducer entry point: {e}")))?;

    let returned = match run.call(&mut store, ()) {
        Ok(returned) => returned,
        Err(error) if error.as_trap_code() == Some(TrapCode::OutOfFuel) => {
            return Err(Error::capacity(
                "wasm reducer exhausted its fuel budget (max_fuel exceeded)",
            ));
        }
        Err(error) => {
            return Err(Error::invalid_argument(format!(
                "wasm reducer trapped during execution: {error}"
            )));
        }
    };

    // A sticky ABI error can never be ignored: even if the guest returned
    // normally, a failed op means the invocation aborts.
    if let Some(error) = store.data().abi_error() {
        return Err(error.clone());
    }

    // Application rejection: the guest wrote `[msg_len: u32][utf8 message]`.
    if returned == RET_REJECT as i32 {
        let mut len_bytes = [0u8; 4];
        memory
            .read(&store, module.out_ptr() as usize, &mut len_bytes)
            .map_err(|e| Error::internal(format!("cannot read rejection message length: {e}")))?;
        let msg_len = u32::from_le_bytes(len_bytes) as usize;
        if msg_len > limits.max_result_bytes.saturating_sub(4) {
            return Err(Error::capacity(
                "rejection message exceeds the configured result limit",
            ));
        }
        let mut msg = vec![0u8; msg_len];
        memory
            .read(&store, module.out_ptr() as usize + 4, &mut msg)
            .map_err(|e| Error::internal(format!("cannot read rejection message: {e}")))?;
        let msg = String::from_utf8(msg)
            .map_err(|_| Error::invalid_argument("rejection message is not valid UTF-8"))?;
        return Err(Error::invalid_argument(msg));
    }

    // Success: `returned` bytes of a single encoded `Value` at out_ptr.
    let n = returned as usize;
    if n > limits.max_result_bytes {
        return Err(Error::capacity(format!(
            "reducer return value exceeds the configured limit of {} bytes",
            limits.max_result_bytes
        )));
    }
    let mut buf = vec![0u8; n];
    memory
        .read(&store, module.out_ptr() as usize, &mut buf)
        .map_err(|e| Error::internal(format!("cannot read reducer return value: {e}")))?;
    let mut cursor: &[u8] = &buf;
    let value = get_value(&mut cursor)
        .map_err(|_| Error::invalid_argument("reducer return value is malformed"))?;
    Ok(value)
}
