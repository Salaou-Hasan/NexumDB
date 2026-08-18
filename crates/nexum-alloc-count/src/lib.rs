//! A counting global allocator for allocation profiling (Phase 21.5).
//!
//! This crate exists so the CCU harness can report **measured** allocation
//! counts (allocs/tick, bytes/tick) without perturbing the timing ladder:
//! the counting allocator is only installed in the harness binary via the
//! `ccu-alloc` feature, and counting can be disabled at runtime so timing
//! runs see only a single relaxed atomic load per allocation.
//!
//! It is deliberately the one crate in the workspace that implements
//! `unsafe impl GlobalAlloc` — every other crate keeps
//! `unsafe_code = forbid`. The `unsafe` here is the unavoidable contract of
//! [`GlobalAlloc`]; the implementation merely forwards to [`System`] and
//! bumps atomics.
//!
//! This is instrumentation only. It never influences application semantics.

use std::alloc::{GlobalAlloc, Layout, System};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

static ENABLED: AtomicBool = AtomicBool::new(false);
static ALLOCS: AtomicU64 = AtomicU64::new(0);
static BYTES: AtomicU64 = AtomicU64::new(0);
static FREES: AtomicU64 = AtomicU64::new(0);

/// A [`GlobalAlloc`] that forwards to [`System`] and, while enabled, counts
/// allocations, allocated bytes, and frees.
///
/// Install it in a **binary** with `#[global_allocator]` (see the `ccu`
/// harness). The library deliberately does not install itself: a library
/// may not know which binary it ends up in, and a second
/// `#[global_allocator]` in the same binary is a compile error.
pub struct CountingAlloc;

// SAFETY: `CountingAlloc` is a stateless unit struct; every method forwards
// to `System` (which is a correct, thread-safe global allocator) and only
// additionally performs relaxed atomic counter updates that cannot affect
// the returned pointer or the allocator's invariants. The `unsafe`
// signatures mirror `GlobalAlloc`'s contract; the implementation upholds it
// by delegating to `System`.
#[allow(unsafe_code)]
unsafe impl GlobalAlloc for CountingAlloc {
    unsafe fn alloc(&self, layout: Layout) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(layout.size() as u64, Ordering::Relaxed);
        }
        // SAFETY: delegated to `System` with the caller-supplied layout;
        // `System` is a valid `GlobalAlloc` and the trait contract is
        // upheld by it.
        unsafe { System.alloc(layout) }
    }

    unsafe fn dealloc(&self, ptr: *mut u8, layout: Layout) {
        if ENABLED.load(Ordering::Relaxed) {
            FREES.fetch_add(1, Ordering::Relaxed);
        }
        // SAFETY: delegated to `System`; same contract as `alloc`.
        unsafe { System.dealloc(ptr, layout) }
    }

    unsafe fn realloc(&self, ptr: *mut u8, layout: Layout, new_size: usize) -> *mut u8 {
        if ENABLED.load(Ordering::Relaxed) {
            ALLOCS.fetch_add(1, Ordering::Relaxed);
            BYTES.fetch_add(new_size as u64, Ordering::Relaxed);
        }
        // SAFETY: delegated to `System`; same contract as `alloc`.
        unsafe { System.realloc(ptr, layout, new_size) }
    }
}

impl Default for CountingAlloc {
    fn default() -> Self {
        Self
    }
}

impl CountingAlloc {
    /// Creates the global allocator instance.
    pub const fn new() -> Self {
        Self
    }
}

/// Enables counting. Safe to call at any time; counters accumulate from the
/// first enabled allocation onward.
pub fn enable() {
    ENABLED.store(true, Ordering::Relaxed);
}

/// Disables counting (allocation still forwards to `System`).
pub fn disable() {
    ENABLED.store(false, Ordering::Relaxed);
}

/// Snapshot of (allocations, allocated bytes, frees) since the allocator
/// was enabled.
pub fn snapshot() -> (u64, u64, u64) {
    (
        ALLOCS.load(Ordering::Relaxed),
        BYTES.load(Ordering::Relaxed),
        FREES.load(Ordering::Relaxed),
    )
}

/// Resets the counters to zero.
pub fn reset() {
    ALLOCS.store(0, Ordering::Relaxed);
    BYTES.store(0, Ordering::Relaxed);
    FREES.store(0, Ordering::Relaxed);
}

#[cfg(test)]
mod tests {
    use super::*;

    // Install the counting allocator for the test binary (a binary may
    // install its own `#[global_allocator]`; the library itself does not).
    #[global_allocator]
    static TEST_ALLOC: CountingAlloc = CountingAlloc::new();

    #[test]
    fn counting_is_opt_in_and_resettable() {
        disable();
        reset();
        let before = snapshot();
        std::hint::black_box(Vec::<u8>::with_capacity(64));
        assert_eq!(snapshot(), before, "disabled counting must not count");
        enable();
        reset();
        std::hint::black_box(Vec::<u8>::with_capacity(64));
        let (allocs, bytes, _frees) = snapshot();
        assert!(allocs >= 1, "enabled counting must observe allocations");
        assert!(bytes >= 64, "allocated bytes must be at least the capacity");
        disable();
    }
}
