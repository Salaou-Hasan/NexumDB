//! Module validation and the compiled-module cache ([`WasmReducerModule`]).
//!
//! Registration (design doc §4, ADR-007 D4) validates:
//!
//! 1. bytecode size is bounded,
//! 2. the module parses and validates (engine `EnforcedLimits` bound the
//!    compiler),
//! 3. the **only** import is `("nexum","op")` with signature
//!    `(i32,i32,i32,i32,i32) -> i32` — anything else (WASI, `env`, ...) is
//!    rejected by construction,
//! 4. the four required exports exist with the right kinds (`memory`,
//!    `_nexum_in_ptr` / `_nexum_out_ptr` as immutable `i32` globals,
//!    `_nexum_reducer_run` as `() -> i32`),
//! 5. the buffer regions `[in_ptr, in_ptr + ABI_IN_CAP)` and
//!    `[out_ptr, out_ptr + ABI_OUT_CAP)` lie inside the declared memory — by
//!    instantiating the module in a throwaway store (no transaction context).
//!
//! A declared start function is never a state channel: it runs at
//! instantiation under the same fuel, host-call, and memory budgets, and
//! because the validation store has **no** transaction context, a start
//! function that attempts any ABI state operation is rejected at
//! registration (its op fails and the sticky error is observed). The host
//! itself calls exactly one exported function: `_nexum_reducer_run`.
//!
//! The compiled module is cached; per-invocation instantiation uses a fresh
//! store with fresh host data.

use nexum_core::{Error, Result};
use wasmi::core::ValType;
use wasmi::{Engine, ExternType, Linker, Module as WasmModule, Mutability, Store, Val};

use crate::host::{define_host, HostState};
use crate::limits::{ABI_IN_CAP, ABI_OUT_CAP, WasmLimits};

/// The exported reducer entry point.
pub(crate) const ENTRY_NAME: &str = "_nexum_reducer_run";
/// The exported input-buffer base global.
pub(crate) const IN_PTR_NAME: &str = "_nexum_in_ptr";
/// The exported output-buffer base global.
pub(crate) const OUT_PTR_NAME: &str = "_nexum_out_ptr";
/// Hard cap on registered bytecode: bounds parse/compile work in addition to
/// the engine's own `EnforcedLimits`.
const MAX_MODULE_BYTES: usize = 1024 * 1024;

/// A validated, compiled reducer module cached by the registry.
#[derive(Debug)]
pub struct WasmReducerModule {
    name: String,
    version: u64,
    compiled: WasmModule,
    in_ptr: u32,
    out_ptr: u32,
}

impl WasmReducerModule {
    /// Parses, validates, and compiles `bytecode`, enforcing the ABI contract.
    pub fn new(
        engine: &Engine,
        name: String,
        version: u64,
        bytecode: Vec<u8>,
        limits: &WasmLimits,
    ) -> Result<Self> {
        if bytecode.len() > MAX_MODULE_BYTES {
            return Err(Error::capacity(format!(
                "wasm module bytecode exceeds the {MAX_MODULE_BYTES} byte limit"
            )));
        }
        let compiled = WasmModule::new(engine, &bytecode)
            .map_err(|e| Error::invalid_argument(format!("invalid wasm module: {e}")))?;
        validate_imports(&compiled)?;
        validate_exports(&compiled)?;
        let (in_ptr, out_ptr) = validate_buffers(engine, &compiled, limits)?;
        Ok(Self {
            name,
            version,
            compiled,
            in_ptr,
            out_ptr,
        })
    }

    /// Returns the module's registry name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the module's version tag.
    pub fn version(&self) -> u64 {
        self.version
    }

    /// The compiled module, cached for instantiation.
    pub(crate) fn compiled(&self) -> &WasmModule {
        &self.compiled
    }

    /// The validated input-buffer base address.
    pub(crate) fn in_ptr(&self) -> u32 {
        self.in_ptr
    }

    /// The validated output-buffer base address.
    pub(crate) fn out_ptr(&self) -> u32 {
        self.out_ptr
    }
}

/// The module may declare **exactly one** import: `("nexum","op")` with the
/// exact ABI signature. Anything else — WASI, `env`, `spectest`, an extra
/// import — is rejected.
fn validate_imports(module: &WasmModule) -> Result<()> {
    let mut imports = module.imports();
    let first = imports.next().ok_or_else(|| {
        Error::invalid_argument(
            "wasm module imports nothing; expected the ('nexum','op') host function",
        )
    })?;
    if imports.next().is_some() {
        return Err(Error::invalid_argument(
            "wasm module declares more than one import; only ('nexum','op') is allowed",
        ));
    }
    if first.module() != "nexum" || first.name() != "op" {
        return Err(Error::invalid_argument(format!(
            "wasm module imports '{}'.'{}'; only the nexum host function is allowed",
            first.module(),
            first.name()
        )));
    }
    match first.ty() {
        ExternType::Func(ty)
            if ty.params() == [ValType::I32; 5] && ty.results() == [ValType::I32] =>
        {
            Ok(())
        }
        ExternType::Func(_) => Err(Error::invalid_argument(
            "the ('nexum','op') import must have signature (i32, i32, i32, i32, i32) -> i32",
        )),
        _ => Err(Error::invalid_argument("the nexum import must be a function")),
    }
}

/// The module must export `memory` plus the three ABI globals/functions.
fn validate_exports(module: &WasmModule) -> Result<()> {
    let memory = module
        .get_export("memory")
        .ok_or_else(|| Error::invalid_argument("wasm module does not export 'memory'"))?;
    if !matches!(memory, ExternType::Memory(_)) {
        return Err(Error::invalid_argument("'memory' must be a memory export"));
    }
    check_buffer_global(module, IN_PTR_NAME)?;
    check_buffer_global(module, OUT_PTR_NAME)?;
    match module.get_export(ENTRY_NAME) {
        Some(ExternType::Func(ty)) if ty.params().is_empty() && ty.results() == [ValType::I32] => {
            Ok(())
        }
        Some(ExternType::Func(_)) => Err(Error::invalid_argument(format!(
            "'{ENTRY_NAME}' must have signature () -> i32"
        ))),
        _ => Err(Error::invalid_argument(format!(
            "wasm module does not export the '{ENTRY_NAME}' reducer entry function"
        ))),
    }
}

fn check_buffer_global(module: &WasmModule, name: &str) -> Result<()> {
    match module.get_export(name) {
        Some(ExternType::Global(ty))
            if ty.content() == ValType::I32 && ty.mutability() == Mutability::Const =>
        {
            Ok(())
        }
        _ => Err(Error::invalid_argument(format!(
            "'{name}' must be an immutable i32 global export"
        ))),
    }
}

/// Instantiates the module in a throwaway store to read the immutable buffer
/// globals and verify the ABI buffer regions fit the declared memory.
///
/// The store has **no** transaction context (`ctx = None`): a declared start
/// function runs at `InstancePre::start` under the armed fuel and memory
/// budgets, and any attempt to perform a state op fails with a sticky ABI
/// error that is observed here and rejects the module.
fn validate_buffers(engine: &Engine, module: &WasmModule, limits: &WasmLimits) -> Result<(u32, u32)> {
    let mut store = Store::new(engine, HostState::new(None, limits));
    store.limiter(|state: &mut HostState<'_, '_>| &mut state.memory_limiter);
    store
        .set_fuel(limits.max_fuel)
        .map_err(|e| Error::internal(format!("cannot arm fuel: {e}")))?;
    let mut linker = Linker::new(engine);
    define_host(&mut linker)
        .map_err(|e| Error::internal(format!("cannot define host functions: {e}")))?;
    let instance = linker
        .instantiate(&mut store, module)
        .and_then(|instance| instance.start(&mut store))
        .map_err(|e| Error::invalid_argument(format!("wasm module failed to instantiate: {e}")))?;
    if let Some(error) = store.data().abi_error() {
        return Err(Error::invalid_argument(format!(
            "wasm module start function attempted a state operation: {error}"
        )));
    }

    let in_ptr = read_buffer_global(&instance, &store, IN_PTR_NAME)?;
    let out_ptr = read_buffer_global(&instance, &store, OUT_PTR_NAME)?;

    let memory = instance
        .get_export(&store, "memory")
        .and_then(|export| export.into_memory())
        .ok_or_else(|| Error::internal("validated memory export vanished"))?;
    let mem_bytes = memory.data_size(&store);

    let in_end = (in_ptr as usize).checked_add(ABI_IN_CAP);
    let out_end = (out_ptr as usize).checked_add(ABI_OUT_CAP);
    match (in_end, out_end) {
        (Some(in_end), Some(out_end)) if in_end <= mem_bytes && out_end <= mem_bytes => {
            Ok((in_ptr, out_ptr))
        }
        _ => Err(Error::invalid_argument(
            "wasm module buffer pointers exceed its declared memory",
        )),
    }
}

fn read_buffer_global(
    instance: &wasmi::Instance,
    store: &Store<HostState<'_, '_>>,
    name: &str,
) -> Result<u32> {
    let global = instance
        .get_global(store, name)
        .ok_or_else(|| Error::internal(format!("validated global '{name}' vanished")))?;
    match global.get(store) {
        Val::I32(value) => Ok(value as u32),
        _ => Err(Error::internal(format!("global '{name}' is not an i32"))),
    }
}
