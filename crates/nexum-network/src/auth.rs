//! Authentication ([`Authenticator`], [`Principal`], ADR-011 D2).
//!
//! The network layer defines the identity **interface**, never a concrete
//! provider. `Principal` is protocol-independent (id + name); the gateway
//! stamps it onto every routed command so client-supplied command sources
//! are ignored (anti-spoofing).
//!
//! [`TokenAuthenticator`] is a deterministic token→principal table for
//! tests and development; real providers (accounts, OAuth, ...) implement
//! the trait later without touching the gateway.

use std::collections::BTreeMap;

use crate::error::AuthError;

/// A protocol-independent player/operator identity.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Principal {
    id: u64,
    name: String,
}

impl Principal {
    /// Creates a principal. The name must not be empty.
    pub fn new(id: u64, name: impl Into<String>) -> Self {
        Self {
            id,
            name: name.into(),
        }
    }

    /// Returns the principal's stable numeric id (used as the authoritative
    /// `InputCommand::source`).
    pub fn id(&self) -> u64 {
        self.id
    }

    /// Returns the principal's display name.
    pub fn name(&self) -> &str {
        &self.name
    }
}

impl std::fmt::Display for Principal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{} ({})", self.name, self.id)
    }
}

/// The authentication interface. Implementations are stateless hooks; the
/// gateway owns the session lifecycle.
pub trait Authenticator: Send + Sync {
    /// Maps opaque credentials to a [`Principal`], or fails with
    /// [`AuthError::InvalidCredentials`].
    fn authenticate(&self, credentials: &str) -> Result<Principal, AuthError>;
}

/// A deterministic token→principal table. Tokens are treated as opaque
/// strings (in production these would be issued/signed by a real provider).
pub struct TokenAuthenticator {
    tokens: BTreeMap<String, Principal>,
}

impl TokenAuthenticator {
    /// Creates an empty authenticator.
    pub fn new() -> Self {
        Self {
            tokens: BTreeMap::new(),
        }
    }

    /// Maps `token` to `principal`. Rejects a duplicate token with
    /// [`AuthError::Internal`].
    pub fn add(
        &mut self,
        token: impl Into<String>,
        principal: Principal,
    ) -> Result<(), AuthError> {
        let token = token.into();
        if token.is_empty() {
            return Err(AuthError::Internal(
                "token must not be empty".to_string(),
            ));
        }
        if self.tokens.contains_key(&token) {
            return Err(AuthError::Internal(
                "token already maps to a principal".to_string(),
            ));
        }
        self.tokens.insert(token, principal);
        Ok(())
    }
}

impl Default for TokenAuthenticator {
    fn default() -> Self {
        Self::new()
    }
}

impl Authenticator for TokenAuthenticator {
    fn authenticate(&self, credentials: &str) -> Result<Principal, AuthError> {
        self.tokens
            .get(credentials)
            .cloned()
            .ok_or(AuthError::InvalidCredentials)
    }
}
