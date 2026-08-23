//! Reducer exposure and permissions (ADR-014 D2/D3/D7).
//!
//! [`GamePolicyTable`] is the server's authorization table: static reducer
//! exposure plus live active-player membership. The gateway consults it
//! through [`PolicyHandle`] (which implements `nexum_network::GamePolicy`)
//! **before** executing client attach / input / reducer operations, so a
//! denial never reaches the authoritative path.
//!
//! The table is shared between the game server (writer) and the gateway
//! (reader) through `Arc<Mutex<…>>`. The mutex provides interior mutability
//! only — it is uncontended in the single-threaded model and never
//! participates in execution ordering, so determinism is unaffected.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Mutex};

use nexum_core::WorldId;
use nexum_network::{GamePolicy, Principal};
use nexum_simulation::InputFrame;

/// The permission role of a principal or of a reducer's permitted callers.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum Role {
    /// An ordinary player.
    Player,
    /// Server-side code (never a client role by default).
    Server,
    /// An administrator/operator.
    Admin,
}

/// Whether a reducer may be invoked by clients.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReducerExposure {
    /// Clients may invoke it (subject to the reducer's permitted roles).
    ClientCallable,
    /// Only the server may invoke it; clients are denied.
    ServerOnly,
}

/// The authorization policy of one reducer name.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReducerPolicy {
    /// Whether clients may invoke it.
    pub exposure: ReducerExposure,
    /// Roles permitted to invoke a client-callable reducer. Empty means any
    /// authenticated principal.
    pub roles: BTreeSet<Role>,
}

/// The shared authorization table (ADR-014 D7).
///
/// Reducers not present in [`GamePolicyTable::reducers`] are denied to
/// clients — exposure is deny-by-default.
#[derive(Debug, Clone, Default)]
pub struct GamePolicyTable {
    reducers: BTreeMap<String, ReducerPolicy>,
    /// Principals currently active in each world: `(principal_id, world)`.
    active_players: BTreeSet<(u64, WorldId)>,
    /// Per-principal role overrides (admin/server); absent = `Player`.
    role_overrides: BTreeMap<u64, Role>,
}

impl GamePolicyTable {
    /// An empty table (everything denied to clients).
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers (or replaces) the policy of one reducer.
    pub fn register_reducer(&mut self, name: &str, exposure: ReducerExposure, roles: &[Role]) {
        self.reducers.insert(
            name.to_string(),
            ReducerPolicy {
                exposure,
                roles: roles.iter().copied().collect(),
            },
        );
    }

    /// Removes a reducer entirely (clients lose access; unknown names are
    /// denied).
    pub fn revoke_reducer(&mut self, name: &str) {
        self.reducers.remove(name);
    }

    /// The policy of a reducer, if registered.
    pub fn reducer_policy(&self, name: &str) -> Option<&ReducerPolicy> {
        self.reducers.get(name)
    }

    /// The exposure of a reducer, if registered.
    pub fn exposure(&self, name: &str) -> Option<ReducerExposure> {
        self.reducers.get(name).map(|policy| policy.exposure)
    }

    /// Whether clients may currently invoke the named reducer.
    pub fn is_client_callable(&self, name: &str) -> bool {
        matches!(
            self.reducers.get(name),
            Some(policy) if policy.exposure == ReducerExposure::ClientCallable
        )
    }

    /// Grants `principal` the given role (overriding the default `Player`).
    pub fn set_role(&mut self, principal: u64, role: Role) {
        self.role_overrides.insert(principal, role);
    }

    /// The effective role of a principal.
    pub fn role_of(&self, principal: u64) -> Role {
        self.role_overrides
            .get(&principal)
            .copied()
            .unwrap_or(Role::Player)
    }

    /// Grants a principal active membership in a world (join/reconnect).
    pub fn add_active_player(&mut self, principal: u64, world: WorldId) {
        self.active_players.insert((principal, world));
    }

    /// Revokes a principal's active membership in a world (leave/disconnect).
    pub fn remove_active_player(&mut self, principal: u64, world: WorldId) {
        self.active_players.remove(&(principal, world));
    }

    /// Whether a principal is an active player of a world.
    pub fn is_active(&self, principal: u64, world: WorldId) -> bool {
        self.active_players.contains(&(principal, world))
    }

    /// The live active-membership set (deterministic order).
    pub fn active_players(&self) -> &BTreeSet<(u64, WorldId)> {
        &self.active_players
    }
}

/// A handle into the shared table implementing the network [`GamePolicy`]
/// (ADR-014 D2). The gateway holds one; the game server updates the same
/// table on join/leave/disconnect/expose/revoke.
#[derive(Debug, Clone)]
pub struct PolicyHandle {
    table: Arc<Mutex<GamePolicyTable>>,
}

impl PolicyHandle {
    /// Wraps a shared table.
    pub fn new(table: Arc<Mutex<GamePolicyTable>>) -> Self {
        Self { table }
    }

    /// The shared table.
    pub fn table(&self) -> &Arc<Mutex<GamePolicyTable>> {
        &self.table
    }
}

impl GamePolicy for PolicyHandle {
    fn authorize_attach(&self, principal: &Principal, world: WorldId) -> bool {
        self.table
            .lock()
            .map(|table| table.is_active(principal.id(), world))
            .unwrap_or(false)
    }

    fn authorize_input(&self, principal: &Principal, world: WorldId, _frame: &InputFrame) -> bool {
        self.table
            .lock()
            .map(|table| table.is_active(principal.id(), world))
            .unwrap_or(false)
    }

    fn authorize_reducer(&self, principal: &Principal, _world: WorldId, reducer: &str) -> bool {
        match self.table.lock() {
            Ok(table) => match table.reducers.get(reducer) {
                None => false,
                Some(policy) => {
                    policy.exposure == ReducerExposure::ClientCallable
                        && (policy.roles.is_empty()
                            || policy.roles.contains(&table.role_of(principal.id())))
                }
            },
            Err(_) => false,
        }
    }
}
