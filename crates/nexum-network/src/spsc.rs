//! Lock-free Single-Producer Single-Consumer (SPSC) ring buffer.
//!
//! Used by [`MemoryConnection`](crate::transport::MemoryConnection) to
//! replace the `Mutex<MemoryLink>` on the hot send/receive path.  The
//! producer calls [`push`](SpscRing::push) and the consumer calls
//! [`pop`](SpscRing::pop) — no locking required.
//!
//! # Safety
//!
//! The caller must uphold the SPSC invariant: exactly one thread pushes,
//! exactly one (different) thread pops.  Violating this is UB.

use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use std::sync::atomic::{AtomicUsize, Ordering};

/// A fixed-capacity, lock-free SPSC ring buffer.
///
/// `cap` must be a power of two.
pub struct SpscRing<T> {
    buf: Vec<UnsafeCell<MaybeUninit<T>>>,
    cap: usize,
    mask: usize,
    /// Next slot the producer will write to.
    write_idx: AtomicUsize,
    /// Next slot the consumer will read from.
    read_idx: AtomicUsize,
}

// SAFETY: The ring is SPSC — exactly one producer thread and exactly one
// consumer thread.  The producer only writes `buf[write & mask]` and only
// reads `read_idx`.  The consumer only reads `buf[read & mask]` and only
// writes `read_idx`.  The two indices are Acquire/Release-paired so
// visibility is guaranteed.
unsafe impl<T: Send> Sync for SpscRing<T> {}

impl<T> SpscRing<T> {
    /// Creates a ring with the given capacity (must be a power of two).
    pub fn new(capacity: usize) -> Self {
        assert!(
            capacity.is_power_of_two() && capacity >= 2,
            "SpscRing capacity must be a power of two >= 2, got {capacity}"
        );
        let mut buf = Vec::with_capacity(capacity);
        for _ in 0..capacity {
            buf.push(UnsafeCell::new(MaybeUninit::uninit()));
        }
        Self {
            buf,
            cap: capacity,
            mask: capacity - 1,
            write_idx: AtomicUsize::new(0),
            read_idx: AtomicUsize::new(0),
        }
    }

    /// Returns the usable (non-power-of-two padding) capacity.
    pub fn capacity(&self) -> usize {
        self.cap
    }

    /// Pushes an item.  Returns `Err(item)` when the ring is full.
    ///
    /// The producer **must** be the only caller.
    #[inline]
    pub fn push(&self, item: T) -> Result<(), T> {
        let write = self.write_idx.load(Ordering::Relaxed);
        let read = self.read_idx.load(Ordering::Acquire);
        if write.wrapping_sub(read) >= self.cap {
            return Err(item);
        }
        // SAFETY: `write & mask` is in bounds and only the producer accesses
        // this slot while the ring is non-full (consumer hasn't read it yet).
        #[allow(unused_unsafe)]
        unsafe {
            (*self.buf[write & self.mask].get()).write(item);
        }
        self.write_idx
            .store(write.wrapping_add(1), Ordering::Release);
        Ok(())
    }

    /// Pops an item.  Returns `None` when the ring is empty.
    ///
    /// The consumer **must** be the only caller.
    #[inline]
    pub fn pop(&self) -> Option<T> {
        let read = self.read_idx.load(Ordering::Relaxed);
        let write = self.write_idx.load(Ordering::Acquire);
        if read == write {
            return None;
        }
        // SAFETY: `read & mask` is in bounds and only the consumer accesses
        // this slot while the ring is non-empty (producer hasn't reused it).
        let item = unsafe { (*self.buf[read & self.mask].get()).assume_init_read() };
        self.read_idx.store(read.wrapping_add(1), Ordering::Release);
        Some(item)
    }

    /// Number of items currently in the ring.
    #[inline]
    pub fn len(&self) -> usize {
        let write = self.write_idx.load(Ordering::Acquire);
        let read = self.read_idx.load(Ordering::Acquire);
        write.wrapping_sub(read)
    }

    /// `true` when the ring is empty.
    #[inline]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn basic_push_pop() {
        let ring = SpscRing::<u64>::new(8);
        assert!(ring.is_empty());
        ring.push(1).unwrap();
        ring.push(2).unwrap();
        assert_eq!(ring.len(), 2);
        assert_eq!(ring.pop(), Some(1));
        assert_eq!(ring.pop(), Some(2));
        assert!(ring.is_empty());
    }

    #[test]
    fn full_returns_err() {
        let ring = SpscRing::<u32>::new(4);
        ring.push(10).unwrap();
        ring.push(20).unwrap();
        ring.push(30).unwrap();
        ring.push(40).unwrap();
        assert!(ring.push(50).is_err());
        assert_eq!(ring.pop(), Some(10));
        ring.push(50).unwrap();
    }

    #[test]
    fn spsc_threads() {
        let ring = Arc::new(SpscRing::<u64>::new(1024));
        let ring2 = Arc::clone(&ring);

        let producer = thread::spawn(move || {
            for i in 0..2048u64 {
                loop {
                    if ring2.push(i).is_ok() {
                        break;
                    }
                }
            }
        });

        let mut received = Vec::new();
        for _ in 0..2048 {
            loop {
                if let Some(v) = ring.pop() {
                    received.push(v);
                    break;
                }
            }
        }

        producer.join().unwrap();
        assert_eq!(received.len(), 2048);
        for (i, &v) in received.iter().enumerate() {
            assert_eq!(v, i as u64);
        }
    }

    #[test]
    fn wrap_around() {
        let ring = SpscRing::<u32>::new(4);
        // Fill and drain multiple times to exercise wrapping.
        for round in 0..100u32 {
            ring.push(round * 10).unwrap();
            ring.push(round * 10 + 1).unwrap();
            assert_eq!(ring.pop(), Some(round * 10));
            assert_eq!(ring.pop(), Some(round * 10 + 1));
            assert!(ring.is_empty());
        }
    }
}
