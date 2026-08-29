//! Operational rate limiting ([`RateLimitConfig`], [`RateLimiter`],
//! ADR-016 D1).
//!
//! The gateway applies bounded, per-connection (and per-session) rate
//! limits to the client operations that could be used to flood the server:
//! authentication attempts, input frames, reducer calls, subscription
//! creation, and resyncs.
//!
//! Guarantees:
//!
//! - **Operational only** — limits live entirely outside `Partition::tick`; they
//!   never alter simulation semantics or determinism.
//! - **Explicit rejection** — an operation that exceeds its bucket is
//!   answered with a correlated protocol error; it is never silently
//!   dropped and never accepted-then-abandoned.
//! - **Bounded memory** — a fixed set of fixed-window counters per
//!   connection; no client-controlled growth.
//! - **No panics** — an empty bucket is just a rejection.

use std::time::{Duration, Instant};

/// One fixed-window counter. The window starts at the first use; when the
/// window elapses the counter resets. `limit` bounds the number of
/// operations allowed per window.
#[derive(Debug, Clone)]
pub(crate) struct RateWindow {
    start: Option<Instant>,
    count: u32,
    limit: u32,
    window: Duration,
}

impl RateWindow {
    fn new(limit: u32, window: Duration) -> Self {
        Self {
            start: None,
            count: 0,
            limit,
            window,
        }
    }

    /// Tries to take one slot. Returns `true` when the operation is allowed
    /// (and the counter advances), `false` when the window is exhausted.
    pub(crate) fn try_take(&mut self, now: Instant) -> bool {
        match self.start {
            Some(start) if now.duration_since(start) >= self.window => {
                // A fresh window.
                self.start = Some(now);
                self.count = 1;
                true
            }
            Some(_) => {
                if self.count >= self.limit {
                    false
                } else {
                    self.count += 1;
                    true
                }
            }
            None => {
                self.start = Some(now);
                self.count = 1;
                true
            }
        }
    }
}

/// The per-connection rate-limit state: one fixed window per operation
/// class. A session's `subscribe`/`resync` windows live here too (a
/// connection owns at most one session).
#[derive(Debug, Clone)]
pub(crate) struct RateLimiter {
    pub(crate) auth: RateWindow,
    pub(crate) input: RateWindow,
    pub(crate) reducer: RateWindow,
    pub(crate) subscribe: RateWindow,
    pub(crate) resync: RateWindow,
}

/// Which operation class a rate-limit rejection applies to (the gateway
/// sends a `19 rate limit exceeded` error with this reason).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RateBucket {
    Auth,
    Input,
    Reducer,
    Subscribe,
    Resync,
}

impl RateLimiter {
    pub(crate) fn new(config: &RateLimitConfig) -> Self {
        Self {
            auth: RateWindow::new(
                config.auth_per_window,
                Duration::from_secs(config.auth_window_secs),
            ),
            input: RateWindow::new(config.input_per_sec, Duration::from_secs(1)),
            reducer: RateWindow::new(config.reducer_per_sec, Duration::from_secs(1)),
            subscribe: RateWindow::new(
                config.subscribe_per_window,
                Duration::from_secs(config.subscribe_window_secs),
            ),
            resync: RateWindow::new(
                config.resync_per_window,
                Duration::from_secs(config.resync_window_secs),
            ),
        }
    }

    /// Tries to take one slot from the bucket for `bucket`. Returns `true`
    /// when allowed (counter advanced), `false` when exhausted.
    pub(crate) fn try_take(&mut self, bucket: RateBucket, now: Instant) -> bool {
        match bucket {
            RateBucket::Auth => self.auth.try_take(now),
            RateBucket::Input => self.input.try_take(now),
            RateBucket::Reducer => self.reducer.try_take(now),
            RateBucket::Subscribe => self.subscribe.try_take(now),
            RateBucket::Resync => self.resync.try_take(now),
        }
    }
}

/// The validated rate-limit configuration ([`NetworkConfig::rate_limits`]).
///
/// All defaults are deliberately generous for development; production
/// configurations tighten them through the config file.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RateLimitConfig {
    /// Maximum authentication attempts per connection per `auth_window_secs`.
    pub auth_per_window: u32,
    /// The authentication window length in seconds.
    pub auth_window_secs: u64,
    /// Maximum input frames per connection per second.
    pub input_per_sec: u32,
    /// Maximum reducer calls per connection per second.
    pub reducer_per_sec: u32,
    /// Maximum subscriptions created per session per `subscribe_window_secs`.
    pub subscribe_per_window: u32,
    /// The subscription window length in seconds.
    pub subscribe_window_secs: u64,
    /// Maximum resyncs per connection per `resync_window_secs`.
    pub resync_per_window: u32,
    /// The resync window length in seconds.
    pub resync_window_secs: u64,
}

impl Default for RateLimitConfig {
    fn default() -> Self {
        Self {
            auth_per_window: 10,
            auth_window_secs: 60,
            input_per_sec: 120,
            reducer_per_sec: 60,
            subscribe_per_window: 16,
            subscribe_window_secs: 60,
            resync_per_window: 8,
            resync_window_secs: 60,
        }
    }
}

impl RateLimitConfig {
    /// Creates a default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the per-connection authentication attempt limit per window.
    pub fn with_auth_per_window(mut self, limit: u32, window_secs: u64) -> Self {
        self.auth_per_window = limit;
        self.auth_window_secs = window_secs;
        self
    }

    /// Sets the per-connection input frame limit per second.
    pub fn with_input_per_sec(mut self, limit: u32) -> Self {
        self.input_per_sec = limit;
        self
    }

    /// Sets the per-connection reducer call limit per second.
    pub fn with_reducer_per_sec(mut self, limit: u32) -> Self {
        self.reducer_per_sec = limit;
        self
    }

    /// Sets the per-session subscription creation limit per window.
    pub fn with_subscribe_per_window(mut self, limit: u32, window_secs: u64) -> Self {
        self.subscribe_per_window = limit;
        self.subscribe_window_secs = window_secs;
        self
    }

    /// Sets the per-connection resync limit per window.
    pub fn with_resync_per_window(mut self, limit: u32, window_secs: u64) -> Self {
        self.resync_per_window = limit;
        self.resync_window_secs = window_secs;
        self
    }

    /// Validates the configuration (every bound ≥ 1, windows ≥ 1 second).
    pub fn validate(&self) -> Result<(), String> {
        if self.auth_per_window == 0 {
            return Err("auth_per_window must be at least 1".to_string());
        }
        if self.auth_window_secs == 0 {
            return Err("auth_window_secs must be at least 1".to_string());
        }
        if self.input_per_sec == 0 {
            return Err("input_per_sec must be at least 1".to_string());
        }
        if self.reducer_per_sec == 0 {
            return Err("reducer_per_sec must be at least 1".to_string());
        }
        if self.subscribe_per_window == 0 {
            return Err("subscribe_per_window must be at least 1".to_string());
        }
        if self.subscribe_window_secs == 0 {
            return Err("subscribe_window_secs must be at least 1".to_string());
        }
        if self.resync_per_window == 0 {
            return Err("resync_per_window must be at least 1".to_string());
        }
        if self.resync_window_secs == 0 {
            return Err("resync_window_secs must be at least 1".to_string());
        }
        Ok(())
    }
}
