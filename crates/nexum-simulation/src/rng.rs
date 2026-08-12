//! The deterministic RNG ([`DeterministicRng`], ADR-009 D5).
//!
//! Simulation correctness requires that the same inputs reproduce the same
//! simulation, so randomness must never come from OS entropy or wall-clock
//! time. `DeterministicRng` is a splitmix64 generator — ~40 lines,
//! dependency-free, and fully deterministic — seeded from a pure function of
//! `(world_seed, tick, system_id)`.

/// A deterministic pseudo-random number generator (splitmix64).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DeterministicRng {
    state: u64,
}

impl DeterministicRng {
    /// Creates a generator from a seed. The same seed yields the same stream.
    pub fn new(seed: u64) -> Self {
        Self { state: seed }
    }

    /// Returns the next `u64` in the stream.
    pub fn next_u64(&mut self) -> u64 {
        // splitmix64: a 64-bit state, three mixes per output, no external
        // tables or platform dependencies.
        self.state = self.state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = self.state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }

    /// Returns the next `u32` (the high half of the next `u64`).
    pub fn next_u32(&mut self) -> u32 {
        (self.next_u64() >> 32) as u32
    }

    /// Returns a value uniformly in `0..bound`.
    ///
    /// Uses Lemire's rejection method, so the result is unbiased (unlike a
    /// plain modulo). Panics if `bound == 0`.
    pub fn next_below(&mut self, bound: u64) -> u64 {
        assert!(bound > 0, "DeterministicRng::next_below requires bound > 0");
        let mut x = self.next_u64();
        let mut wide = (x as u128) * (bound as u128);
        let mut low = wide as u64;
        if low < bound {
            // Reject the small biased top region.
            let threshold = bound.wrapping_neg() % bound;
            while low < threshold {
                x = self.next_u64();
                wide = (x as u128) * (bound as u128);
                low = wide as u64;
            }
        }
        (wide >> 64) as u64
    }
}

/// Mixes `(world_seed, tick, system)` into a per-system RNG seed.
///
/// A pure, deterministic function of its three inputs (ADR-009 D5): a system
/// in tick N always draws from the same stream, independent of how many
/// other systems used the RNG before it.
pub(crate) fn rng_seed(world_seed: u64, tick: u64, system: u64) -> u64 {
    avalanche(
        world_seed
            .wrapping_mul(0x9E37_79B9_7F4A_7C15)
            .wrapping_add(avalanche(tick.wrapping_mul(0xBF58_476D_1CE4_E5B9) ^ system)),
    )
}

/// A MurmurHash3-style finalizer used for deterministic mixing.
fn avalanche(mut x: u64) -> u64 {
    x ^= x >> 33;
    x = x.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    x ^= x >> 33;
    x = x.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    x ^= x >> 33;
    x
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn same_seed_same_stream() {
        let mut a = DeterministicRng::new(1234);
        let mut b = DeterministicRng::new(1234);
        for _ in 0..100 {
            assert_eq!(a.next_u64(), b.next_u64());
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let mut a = DeterministicRng::new(1);
        let mut b = DeterministicRng::new(2);
        assert_ne!(a.next_u64(), b.next_u64());
    }

    #[test]
    fn next_below_stays_in_bounds() {
        let mut rng = DeterministicRng::new(7);
        for _ in 0..10_000 {
            let value = rng.next_below(10);
            assert!(value < 10);
        }
    }

    #[test]
    fn next_below_is_deterministic() {
        let mut a = DeterministicRng::new(42);
        let mut b = DeterministicRng::new(42);
        for _ in 0..1000 {
            assert_eq!(a.next_below(6), b.next_below(6));
        }
    }

    #[test]
    fn seeds_are_deterministic_and_distinct() {
        assert_eq!(rng_seed(1, 2, 3), rng_seed(1, 2, 3));
        assert_ne!(rng_seed(1, 2, 3), rng_seed(1, 2, 4));
        assert_ne!(rng_seed(1, 2, 3), rng_seed(2, 2, 3));
    }
}
