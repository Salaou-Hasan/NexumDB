//! Gateway authorization policy (ADR-014 D2).
//!
//! The gateway consults an opt-in [`GamePolicy`] before executing the three
//! client-driven operations that can mutate authoritative state — attaching
//! to a world, submitting an input frame, and calling a reducer. The default
//! [`AllowAllPolicy`] preserves the exact Phase 13 semantics; a host that
//! wants game-level authorization installs its own policy with
//! [`NetworkGateway::set_policy`]. Denials happen **before** any `Runtime`
//! call, produce a correlated protocol error, and never mutate state.

use nexum_core::WorldId;
use nexum_simulation::InputFrame;

use crate::auth::Principal;

/// Decides whether a client operation is authorized.
///
/// Every method defaults to `true` so implementors only override what they
/// care about, and so the pass-through policy is a no-op.
pub trait GamePolicy: Send + Sync {
    /// Whether `principal` may attach to `world`.
    fn authorize_attach(&self, _principal: &Principal, _world: WorldId) -> bool {
        true
    }

    /// Whether `principal` may submit `frame` (whose commands are already
    /// source-stamped with the principal id) to `world`.
    fn authorize_input(&self, _principal: &Principal, _world: WorldId, _frame: &InputFrame) -> bool {
        true
    }

    /// Whether `principal` may invoke the reducer named `reducer` on `world`.
    fn authorize_reducer(&self, _principal: &Principal, _world: WorldId, _reducer: &str) -> bool {
        true
    }
}

/// The default policy: every authenticated principal may attach, submit
/// input, and call reducers — exactly the Phase 13 behavior.
#[derive(Debug, Default, Clone, Copy)]
pub struct AllowAllPolicy;

impl GamePolicy for AllowAllPolicy {}
