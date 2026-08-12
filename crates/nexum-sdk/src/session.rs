//! The client-side session ([`SessionInfo`], ADR-013).
//!
//! A session is the authenticated identity on the connection plus its
//! world attachment. It is operational client state — the server owns the
//! authoritative session; this is the client's mirror of it.

use nexum_core::WorldId;
use nexum_network::auth::Principal;

use crate::client::Client;
use crate::error::SdkError;
use crate::protocol::ClientMessage;

/// The client's mirror of its authenticated server session.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionInfo {
    principal: Principal,
    attached_world: Option<WorldId>,
}

impl SessionInfo {
    /// Builds a session for `principal` with no attachment.
    pub(crate) fn new(principal: Principal) -> Self {
        Self {
            principal,
            attached_world: None,
        }
    }

    /// Returns the authenticated principal.
    pub fn principal(&self) -> &Principal {
        &self.principal
    }

    /// Returns the attached world, if any.
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

impl Client {
    /// Presents `credentials` to the server's `Authenticator`. The result
    /// arrives on the next [`pump`](Client::pump) as `Authenticated` or
    /// `AuthFailed`. Fails locally when already authenticated.
    pub fn authenticate(&mut self, credentials: &str) -> Result<(), SdkError> {
        self.require_connected()?;
        if self.session.is_some() {
            return Err(SdkError::InvalidArgument(
                "already authenticated".to_string(),
            ));
        }
        self.send_message(&ClientMessage::Authenticate {
            credentials: credentials.to_string(),
        })
    }

    /// Attaches the authenticated session to `world`. The result arrives on
    /// the next [`pump`](Client::pump) as `Attached` or `AttachFailed`.
    pub fn attach(&mut self, world: WorldId) -> Result<(), SdkError> {
        self.require_connected()?;
        if self.session.is_none() {
            return Err(SdkError::AuthenticationRequired);
        }
        if self
            .session
            .as_ref()
            .is_some_and(|session| session.is_attached())
        {
            return Err(SdkError::InvalidArgument(
                "already attached to a world".to_string(),
            ));
        }
        self.send_message(&ClientMessage::AttachWorld { world })
    }

    /// Detaches from the attached world, ending its subscriptions.
    pub fn detach(&mut self) -> Result<(), SdkError> {
        self.require_connected()?;
        if self
            .session
            .as_ref()
            .is_none_or(|session| !session.is_attached())
        {
            return Err(SdkError::NotAttached);
        }
        self.send_message(&ClientMessage::DetachWorld)
    }

    /// Returns the authenticated session, if established.
    pub fn session(&self) -> Option<&SessionInfo> {
        self.session.as_ref()
    }

    /// Returns the authenticated principal, if any.
    pub fn session_principal(&self) -> Option<&Principal> {
        self.session.as_ref().map(|session| session.principal())
    }

    /// Returns the attached world, if any.
    pub fn attached_world(&self) -> Option<WorldId> {
        self.session.as_ref().and_then(SessionInfo::attached_world)
    }
}
