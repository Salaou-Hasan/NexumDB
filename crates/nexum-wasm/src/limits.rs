//! Resource limits for WASM reducer execution ([`WasmLimits`]).
//!
//! Every untrusted module executes within these budgets (ADR-007 D5). Fuel is
//! the primary, deterministic execution budget (never wall-clock); the memory
//! ceiling is enforced by the host's `ResourceLimiter` on every
//! `memory.grow`; the host-call budget bounds host-function crossings; and the
//! byte budgets bound the size of everything that crosses the ABI.
//!
//! The ABI buffer constants are the guest-side contract: a module must
//! provide at least `ABI_IN_CAP` input and `ABI_OUT_CAP` output bytes at its
//! exported buffer addresses (design doc §4.1). Limits that would overflow
//! these buffers are rejected at registry construction.

use nexum_core::{Error, Result};

/// The minimum size, in bytes, of the module's exported **input** buffer.
pub const ABI_IN_CAP: usize = 16 * 1024;
/// The minimum size, in bytes, of the module's exported **output** buffer.
pub const ABI_OUT_CAP: usize = 64 * 1024;

/// Resource budgets applied to every WASM reducer invocation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WasmLimits {
    /// Maximum linear memory in bytes (covers initial pages and any growth).
    pub max_memory_bytes: usize,
    /// Deterministic instruction budget per invocation.
    pub max_fuel: u64,
    /// Maximum number of `("nexum","op")` calls per invocation.
    pub max_host_calls: u32,
    /// Maximum encoded reducer-arguments size in bytes.
    pub max_args_bytes: usize,
    /// Maximum encoded return-value size in bytes.
    pub max_result_bytes: usize,
    /// Maximum encoded event payload size in bytes.
    pub max_event_bytes: usize,
    /// Maximum encoded scan-result size in bytes.
    pub max_scan_bytes: usize,
}

impl Default for WasmLimits {
    fn default() -> Self {
        Self {
            max_memory_bytes: 4 * 1024 * 1024, // 64 pages
            max_fuel: 1_000_000,
            max_host_calls: 10_000,
            max_args_bytes: 8 * 1024,
            max_result_bytes: 8 * 1024,
            max_event_bytes: 1024,
            max_scan_bytes: 64 * 1024,
        }
    }
}

impl WasmLimits {
    /// Validates the limits against the ABI buffer contract.
    ///
    /// Returns `InvalidArgument` when a byte budget cannot fit the guest
    /// buffers the ABI guarantees, or when a budget is zero.
    pub fn validate(&self) -> Result<()> {
        let checks = [
            ("max_args_bytes", self.max_args_bytes, ABI_IN_CAP),
            ("max_result_bytes", self.max_result_bytes, ABI_OUT_CAP),
            ("max_scan_bytes", self.max_scan_bytes, ABI_OUT_CAP),
            ("max_event_bytes", self.max_event_bytes, ABI_OUT_CAP),
        ];
        for (name, budget, cap) in checks {
            if budget == 0 {
                return Err(Error::invalid_argument(format!(
                    "wasm limit {name} must be nonzero"
                )));
            }
            if budget > cap {
                return Err(Error::invalid_argument(format!(
                    "wasm limit {name} ({budget}) exceeds the ABI buffer capacity {cap}"
                )));
            }
        }
        if self.max_memory_bytes == 0 || self.max_fuel == 0 || self.max_host_calls == 0 {
            return Err(Error::invalid_argument(
                "wasm memory/fuel/host-call limits must be nonzero",
            ));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        WasmLimits::default().validate().unwrap();
    }

    #[test]
    fn oversized_budgets_are_rejected() {
        let limits = WasmLimits {
            max_result_bytes: ABI_OUT_CAP + 1,
            ..WasmLimits::default()
        };
        assert!(matches!(limits.validate(), Err(Error::InvalidArgument(_))));
    }

    #[test]
    fn zero_budgets_are_rejected() {
        let limits = WasmLimits {
            max_fuel: 0,
            ..WasmLimits::default()
        };
        assert!(matches!(limits.validate(), Err(Error::InvalidArgument(_))));
    }
}
