//! Connections, sessions, and attachments (ADR-011 D2).
//!
//! A [`Connection`](crate::transport::Connection) is a transport handle; a
//! [`Session`] is the authenticated identity on that connection; an
//! attachment binds a session to exactly one world. All of this state is
//! **operational** — it dies with the process and is rebuilt by reattach
//! after recovery; it never joins authoritative state.

use nexum_core::{ConnectionId, SessionId, WorldId};

use crate::auth::Principal;

/// An authenticated session on a connection.
///
/// Sessions are created by the gateway on successful authentication and are
/// immutable once created (identity never changes mid-connection).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Session {
    id: SessionId,
    connection: ConnectionId,
    principal: Principal,
    attached_world: Option<WorldId>,
}

impl Session {
    /// Creates a session for `connection` authenticated as `principal`.
    pub(crate) fn new(id: SessionId, connection: ConnectionId, principal: Principal) -> Self {
        Self {
            id,
            connection,
            principal,
            attached_world: None,
        }
    }

    /// Returns the session id.
    pub fn id(&self) -> SessionId {
        self.id
    }

    /// Returns the owning connection.
    pub fn connection(&self) -> ConnectionId {
        self.connection
    }

    /// Returns the authenticated principal.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the world this session is attached to, if any.
    pub fn attached_world(&self) -> Option<WorldId> {
        self.attached_world
    }

    /// Returns `true` when the session is attached to a world.
    pub fn is_attached(&self) -> bool {
        self.attached_world.is_some()
    }

    pub(crate) fn attach(&mut self, world: WorldId) {
        self.attached_world = Some(world);
    }

    pub(crate) fn detach(&mut self) {
        self.attached_world = None;
    }
}
