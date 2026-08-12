//! Simulation systems ([`SystemDefinition`], [`SystemRegistry`]).
//!
//! A system is a registered, ordered, deterministic state-transition
//! program. Systems never touch the store directly: each one receives a
//! [`SimulationContext`] that delegates to the tick's transaction.
//!
//! Execution order is **explicit and reproducible** — ascending
//! `(priority, SystemId)` — so registration order never affects behavior
//! (ADR-009 D4).
//!
//! Phase 11 adds the parallel-execution access declaration
//! ([`SystemAccess`]): a system that declares which tables it reads and
//! writes can be grouped with other conflict-free systems and executed
//! concurrently inside the tick's single transaction (ADR-011 D2). Systems
//! that do not declare access are `opaque` — they always execute on the
//! serial Phase 9 path.

use std::collections::BTreeSet;

use nexum_core::{Error, Result, SystemId};

use crate::context::SimulationContext;
use crate::input::InputFrame;

/// The native execute function of a simulation system.
///
/// Higher-ranked so the context borrow can be fresh for every tick.
pub type SystemFn = for<'a> fn(&mut SimulationContext<'a>, &InputFrame) -> Result<()>;

/// A system's declared table-access footprint (ADR-011 D2).
///
/// Two systems **conflict** when one writes a table the other reads or
/// writes. `opaque` (the default) declares nothing and therefore conflicts
/// with everything — an opaque system always runs on the serial Phase 9
/// path, which is always correct. Declared access must cover everything the
/// system touches, including anything its reducers (native or WASM) touch;
/// an actual overlap that a declaration missed is detected by the parallel
/// executor's merge as a deterministic tick error, never undefined
/// behavior.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SystemAccess {
    reads: BTreeSet<String>,
    writes: BTreeSet<String>,
    opaque: bool,
}

impl SystemAccess {
    /// No access declaration: conflicts with every other system; always
    /// executes serially.
    pub fn opaque() -> Self {
        Self {
            reads: BTreeSet::new(),
            writes: BTreeSet::new(),
            opaque: true,
        }
    }

    /// A declared footprint: `reads` and `writes` are table **names**
    /// (resolved to ids against the store when the tick plan is built).
    pub fn new(reads: &[&str], writes: &[&str]) -> Self {
        Self {
            reads: reads.iter().map(|name| (*name).to_string()).collect(),
            writes: writes.iter().map(|name| (*name).to_string()).collect(),
            opaque: false,
        }
    }

    /// Returns the declared read table names.
    pub fn reads(&self) -> &BTreeSet<String> {
        &self.reads
    }

    /// Returns the declared write table names.
    pub fn writes(&self) -> &BTreeSet<String> {
        &self.writes
    }

    /// Returns `true` for an undeclared (opaque) footprint.
    pub fn is_opaque(&self) -> bool {
        self.opaque
    }
}

impl Default for SystemAccess {
    /// The default is [`SystemAccess::opaque`] — no declaration.
    fn default() -> Self {
        Self::opaque()
    }
}

/// A registered simulation system: identity + ordering + execute function.
#[derive(Debug, Clone)]
pub struct SystemDefinition {
    id: SystemId,
    name: String,
    priority: u32,
    access: SystemAccess,
    execute: SystemFn,
}

impl SystemDefinition {
    /// Creates a system with a stable id, a registry-unique name, an
    /// explicit ordering priority (lower runs first), and an execute
    /// function. The name must not be empty.
    ///
    /// The access footprint defaults to [`SystemAccess::opaque`], which
    /// always executes serially (Phase 9 behavior preserved for every
    /// existing caller). Use [`with_access`](Self::with_access) to declare a
    /// footprint and enable parallel grouping.
    pub fn new(
        id: SystemId,
        name: impl Into<String>,
        priority: u32,
        execute: SystemFn,
    ) -> Result<Self> {
        Self::with_access(id, name, priority, SystemAccess::opaque(), execute)
    }

    /// Creates a system exactly like [`new`](Self::new) but with an explicit
    /// access declaration, enabling the Phase 11 parallel planner (ADR-011
    /// D2).
    pub fn with_access(
        id: SystemId,
        name: impl Into<String>,
        priority: u32,
        access: SystemAccess,
        execute: SystemFn,
    ) -> Result<Self> {
        let name = name.into();
        if name.is_empty() {
            return Err(Error::invalid_argument(
                "simulation system name must not be empty",
            ));
        }
        Ok(Self {
            id,
            name,
            priority,
            access,
            execute,
        })
    }

    /// Returns the stable system id.
    pub fn id(&self) -> SystemId {
        self.id
    }

    /// Returns the registry-unique name.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Returns the execution priority (lower runs first).
    pub fn priority(&self) -> u32 {
        self.priority
    }

    /// Returns the declared table-access footprint.
    pub fn access(&self) -> &SystemAccess {
        &self.access
    }

    /// Returns the execute function.
    pub fn execute(&self) -> SystemFn {
        self.execute
    }
}

/// A registry of simulation systems, always sorted by `(priority, id)`.
#[derive(Debug, Default)]
pub struct SystemRegistry {
    systems: Vec<SystemDefinition>,
}

impl SystemRegistry {
    /// Creates an empty registry.
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a system, inserting it at its deterministic position.
    ///
    /// Returns `AlreadyExists` if the system's id **or** name is already
    /// taken.
    pub fn register(&mut self, definition: SystemDefinition) -> Result<()> {
        if self.systems.iter().any(|system| system.id == definition.id) {
            return Err(Error::already_exists(format!(
                "simulation system id {} is already registered",
                definition.id
            )));
        }
        if self.systems.iter().any(|system| system.name == definition.name) {
            return Err(Error::already_exists(format!(
                "simulation system '{}' is already registered",
                definition.name
            )));
        }
        let position = self
            .systems
            .binary_search_by(|system| {
                (system.priority, system.id).cmp(&(definition.priority, definition.id))
            })
            .unwrap_or_else(|position| position);
        self.systems.insert(position, definition);
        Ok(())
    }

    /// Removes a system by id. Returns `NotFound` if it is not registered.
    pub fn remove(&mut self, id: SystemId) -> Result<()> {
        match self.systems.iter().position(|system| system.id == id) {
            Some(position) => {
                self.systems.remove(position);
                Ok(())
            }
            None => Err(Error::not_found(format!(
                "simulation system {id} is not registered"
            ))),
        }
    }

    /// Looks up a system by id.
    pub fn lookup(&self, id: SystemId) -> Option<&SystemDefinition> {
        self.systems.iter().find(|system| system.id == id)
    }

    /// Looks up a system by name.
    pub fn lookup_by_name(&self, name: &str) -> Option<&SystemDefinition> {
        self.systems.iter().find(|system| system.name == name)
    }

    /// Returns `true` if a system with the given id is registered.
    pub fn contains(&self, id: SystemId) -> bool {
        self.systems.iter().any(|system| system.id == id)
    }

    /// Returns `true` if a system with the given name is registered.
    pub fn contains_name(&self, name: &str) -> bool {
        self.systems.iter().any(|system| system.name == name)
    }

    /// Returns every system in deterministic `(priority, id)` execution
    /// order.
    pub fn ordered(&self) -> &[SystemDefinition] {
        &self.systems
    }

    /// Returns the number of registered systems.
    pub fn len(&self) -> usize {
        self.systems.len()
    }

    /// Returns `true` if no systems are registered.
    pub fn is_empty(&self) -> bool {
        self.systems.is_empty()
    }
}

#[cfg(test)]
mod tests {
use super::*;

fn system(id: u64, name: &str, priority: u32) -> SystemDefinition {
        SystemDefinition::new(SystemId::from_u64(id), name, priority, |_, _| Ok(())).unwrap()
    }

    #[test]
    fn registration_is_ordered_by_priority_then_id() {
        let mut registry = SystemRegistry::new();
        // Register in a deliberately scrambled order.
        registry.register(system(30, "late", 20)).unwrap();
        registry.register(system(10, "first", 5)).unwrap();
        registry.register(system(20, "mid", 10)).unwrap();
        registry.register(system(40, "tie-low", 10)).unwrap();
        let names: Vec<&str> = registry.ordered().iter().map(SystemDefinition::name).collect();
        assert_eq!(names, vec!["first", "mid", "tie-low", "late"]);
    }

    #[test]
    fn duplicate_id_and_name_are_rejected() {
        let mut registry = SystemRegistry::new();
        registry.register(system(1, "a", 0)).unwrap();
        assert!(registry.register(system(1, "b", 0)).is_err());
        assert!(registry.register(system(2, "a", 0)).is_err());
        assert_eq!(registry.len(), 1);
    }

    #[test]
    fn remove_and_lookup() {
        let mut registry = SystemRegistry::new();
        registry.register(system(1, "a", 0)).unwrap();
        assert!(registry.contains(SystemId::from_u64(1)));
        assert!(registry.contains_name("a"));
        assert!(registry.lookup_by_name("a").is_some());
        registry.remove(SystemId::from_u64(1)).unwrap();
        assert!(!registry.contains(SystemId::from_u64(1)));
        assert!(registry.remove(SystemId::from_u64(1)).is_err());
        assert!(registry.is_empty());
    }

    #[test]
    fn empty_name_is_rejected() {
        assert!(SystemDefinition::new(SystemId::from_u64(1), "", 0, |_, _| Ok(())).is_err());
    }

    #[test]
    fn execute_is_a_plain_fn_pointer() {
        let definition =
            SystemDefinition::new(SystemId::from_u64(9), "const", 0, |_, _| Ok(()))
                .unwrap();
        // SystemFn must be Copy and usable without closure captures.
        let _second: SystemFn = definition.execute();
    }
}
