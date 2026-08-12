//! Client-side subscriptions (ADR-013): lifecycle and derived views.
//!
//! [`Client::subscribe`] sends a logical [`Query`] and returns a local
//! handle id; the server binds a real subscription and delivers an initial
//! snapshot, which the SDK applies to the handle's derived [`View`]. Deltas
//! keep the view current; a `StaleNotification` or a detected
//! [`ViewGap`](crate::view::ViewGap) marks the handle stale until
//! [`Client::resync`].
//!
//! The server's `SubscriptionRegistry` remains the authoritative
//! observation system. The SDK's views are derived caches, always
//! rebuildable from a snapshot.

use nexum_core::SubscriptionId;
use nexum_subscription::Query;

use crate::client::Client;
use crate::error::SdkError;
use crate::protocol::ClientMessage;
use crate::view::View;

/// One subscription handle, keyed by the SDK's local id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SubscriptionHandle {
    local: u64,
    server: Option<SubscriptionId>,
    stale: bool,
}

impl SubscriptionHandle {
    /// Builds an unbound handle.
    pub(crate) fn new(local: u64) -> Self {
        Self {
            local,
            server: None,
            stale: false,
        }
    }

    /// The SDK's local handle id (used with `view`, `unsubscribe`, and
    /// `resync`).
    pub fn local(&self) -> u64 {
        self.local
    }

    /// The server subscription id, once the initial snapshot bound it.
    pub fn server(&self) -> Option<SubscriptionId> {
        self.server
    }

    /// Returns `true` while the handle is stale (its view is invalid until
    /// resync).
    pub fn is_stale(&self) -> bool {
        self.stale
    }

    pub(crate) fn bind(&mut self, server: SubscriptionId) {
        self.server = Some(server);
        self.stale = false;
    }

    pub(crate) fn mark_stale(&mut self) {
        self.stale = true;
    }

    pub(crate) fn clear_stale(&mut self) {
        self.stale = false;
    }
}

impl Client {
    /// Subscribes to `query` on the attached world.
    ///
    /// Returns the local handle id immediately; the initial snapshot is
    /// applied on the next [`pump`](Client::pump) (`SubscriptionBound`
    /// event). Rejections surface as `SubscriptionRejected` (or
    /// [`SdkError::Server`] with the request's code). Fails locally when
    /// not attached.
    pub fn subscribe(&mut self, query: Query) -> Result<u64, SdkError> {
        self.require_attached()?;
        let request_id = self.next_request_id;
        self.next_request_id += 1;
        let local = self.next_local_subscription;
        self.next_local_subscription += 1;
        self.send_message(&ClientMessage::Subscribe { request_id, query })?;
        self.pending_subscribes.insert(request_id, local);
        self.subscriptions
            .insert(local, SubscriptionHandle::new(local));
        self.views.insert(local, View::new());
        Ok(local)
    }

    /// Ends a subscription. Fails if `local` is unknown or still binding.
    pub fn unsubscribe(&mut self, local: u64) -> Result<(), SdkError> {
        self.require_connected()?;
        let handle = self
            .subscriptions
            .get(&local)
            .ok_or(SdkError::UnknownSubscription(local))?;
        let Some(server) = handle.server() else {
            return Err(SdkError::InFlightSubscription(local));
        };
        self.send_message(&ClientMessage::Unsubscribe {
            subscription: server,
        })?;
        self.subscriptions.remove(&local);
        self.views.remove(&local);
        self.pending_subscribes.retain(|_, pending| *pending != local);
        Ok(())
    }

    /// Regenerates a subscription's exact view. The fresh snapshot replaces
    /// the derived view on the next [`pump`](Client::pump)
    /// (`SubscriptionResynced`), clearing the stale mark.
    pub fn resync(&mut self, local: u64) -> Result<(), SdkError> {
        self.require_connected()?;
        let handle = self
            .subscriptions
            .get(&local)
            .ok_or(SdkError::UnknownSubscription(local))?;
        let Some(server) = handle.server() else {
            return Err(SdkError::InFlightSubscription(local));
        };
        self.send_message(&ClientMessage::Resync { subscription: server })
    }

    /// Returns the derived view of a subscription, if it exists.
    pub fn view(&self, local: u64) -> Option<&View> {
        self.views.get(&local)
    }

    /// Returns a subscription handle.
    pub fn subscription(&self, local: u64) -> Option<&SubscriptionHandle> {
        self.subscriptions.get(&local)
    }

    /// Iterates the subscription handles in local-id order.
    pub fn subscriptions(&self) -> impl Iterator<Item = (&u64, &SubscriptionHandle)> {
        self.subscriptions.iter()
    }

    /// Returns the number of active subscription handles.
    pub fn subscription_count(&self) -> usize {
        self.subscriptions.len()
    }
}
