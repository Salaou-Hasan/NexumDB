//! Per-thread cached WASM Linker with host ABI pre-configured.
//!
//! Creating a new `Linker` + `define_host` costs ~2-3µs per WASM invocation.
//! Since the host ABI definition is identical for every call (same engine,
//! same function signatures, same closure), we cache a pre-configured
//! `Linker` per thread and clone it for each invocation.
//!
//! # Safety
//!
//! The closure in `define_host` captures nothing — it reads all state from
//! the wasmi `Caller` parameter at call time. It is `Send + Sync + 'static`
//! regardless of `HostState` lifetimes (documented in host.rs). Therefore
//! transmuting the cloned Linker's phantom `T` parameter from
//! `'static` to the caller's actual lifetimes is sound.

use std::cell::RefCell;

use wasmi::{Engine, Linker};

use crate::host::{HostState, define_host};

/// Returns a clone of the per-thread cached `Linker`, transmuted to the
/// caller's `HostState` lifetimes. First call builds + caches the Linker;
/// subsequent calls just clone (~200ns vs ~2-3µs).
///
/// If the engine changes (e.g., different test), the cache is rebuilt.
pub(crate) fn clone_cached_linker<'a, 'b>(engine: &Engine) -> Linker<HostState<'a, 'b>> {
    thread_local! {
        static CACHE: RefCell<Option<(Engine, Linker<HostState<'static, 'static>>)>> =
            const { RefCell::new(None) };
    }
    let cached: Linker<HostState<'static, 'static>> = CACHE.with(|cell| {
        let mut borrow = cell.borrow_mut();
        let needs_rebuild = match borrow.as_ref() {
            Some((cached_engine, _)) => !Engine::same(cached_engine, engine),
            None => true,
        };
        if needs_rebuild {
            let mut linker = Linker::new(engine);
            // SAFETY: define_host's closure is Send + Sync + 'static — it
            // captures nothing and reads all state from Caller.
            unsafe {
                let static_ref: &mut Linker<HostState<'static, 'static>> =
                    std::mem::transmute(&mut linker);
                define_host(static_ref).expect("host ABI definition must succeed on first call");
            }
            *borrow = Some((engine.clone(), linker));
        }
        borrow.as_ref().unwrap().1.clone()
    });
    // SAFETY: see module-level doc comment — the Linker's function
    // wrappers are `'static` closures that don't use the T parameter.
    unsafe { std::mem::transmute(cached) }
}
