//! Game server configuration (ADR-014).
//!
//! Configuration is **metadata**, never authoritative gameplay state. Values
//! that would alter simulation semantics (world seed, reducer names) are
//! fixed per game instance at creation; nothing here can silently change a
//! deterministic simulation while it runs.

/// Server-wide game server configuration.
#[derive(Debug, Clone)]
pub struct GameServerConfig {
    pub(crate) default_partition_count: usize,
    pub(crate) default_max_players: usize,
    pub(crate) subscription_limit_per_player: usize,
    pub(crate) event_log_limit: usize,
    pub(crate) tick_rate_hz: u32,
    /// Bounded per-world command buffer (ADR-014 D3): commands submitted
    /// between steps are merged into one frame per world per tick, and this
    /// is the maximum number of buffered commands per world. Should not
    /// exceed the world's `max_commands_per_frame`.
    pub(crate) max_pending_commands_per_world: usize,
}

impl GameServerConfig {
    /// A configuration with default limits: 1 partition per game, 64 players
    /// per game, 16 server-side subscriptions per player, a 256-event log,
    /// and a 20 Hz scheduling hint.
    pub fn new() -> Self {
        Self::default()
    }

    /// Sets the default partition count applied to game instances that do
    /// not override it.
    pub fn with_default_partition_count(mut self, count: usize) -> Self {
        self.default_partition_count = count;
        self
    }

    /// Sets the default per-game player capacity.
    pub fn with_default_max_players(mut self, max: usize) -> Self {
        self.default_max_players = max;
        self
    }

    /// Sets the per-player server-side subscription limit.
    pub fn with_subscription_limit_per_player(mut self, limit: usize) -> Self {
        self.subscription_limit_per_player = limit;
        self
    }

    /// Sets the bounded game-server event log size (oldest entries are
    /// dropped first).
    pub fn with_event_log_limit(mut self, limit: usize) -> Self {
        self.event_log_limit = limit;
        self
    }

    /// Sets the scheduling hint (ticks per second). This is an outer-runtime
    /// hint only — simulation correctness is logical-time based and never
    /// depends on it.
    pub fn with_tick_rate_hz(mut self, hz: u32) -> Self {
        self.tick_rate_hz = hz;
        self
    }

    /// Sets the bounded per-world command buffer (ADR-014 D3). Commands
    /// submitted between steps are merged into one frame per world per tick.
    /// Should not exceed the world's `max_commands_per_frame`; exceeding the
    /// buffer rejects further commands explicitly (never silently drops).
    pub fn with_max_pending_commands_per_world(mut self, max: usize) -> Self {
        self.max_pending_commands_per_world = max;
        self
    }

    /// The default partition count for new game instances.
    pub fn default_partition_count(&self) -> usize {
        self.default_partition_count
    }

    /// The default per-game player capacity.
    pub fn default_max_players(&self) -> usize {
        self.default_max_players
    }

    /// The per-player server-side subscription limit.
    pub fn subscription_limit_per_player(&self) -> usize {
        self.subscription_limit_per_player
    }

    /// The bounded game-server event log size.
    pub fn event_log_limit(&self) -> usize {
        self.event_log_limit
    }

    /// The scheduling hint in ticks per second.
    pub fn tick_rate_hz(&self) -> u32 {
        self.tick_rate_hz
    }

    /// The bounded per-world command buffer.
    pub fn max_pending_commands_per_world(&self) -> usize {
        self.max_pending_commands_per_world
    }

    /// Validates the configuration. Returns a human-readable error string on
    /// failure.
    pub fn validate(&self) -> Result<(), String> {
        if self.default_partition_count == 0 {
            return Err("default_partition_count must be at least 1".to_string());
        }
        if self.default_max_players == 0 {
            return Err("default_max_players must be at least 1".to_string());
        }
        if self.subscription_limit_per_player == 0 {
            return Err("subscription_limit_per_player must be at least 1".to_string());
        }
        if self.event_log_limit == 0 {
            return Err("event_log_limit must be at least 1".to_string());
        }
        if self.tick_rate_hz == 0 {
            return Err("tick_rate_hz must be at least 1".to_string());
        }
        if self.max_pending_commands_per_world == 0 {
            return Err("max_pending_commands_per_world must be at least 1".to_string());
        }
        Ok(())
    }
}

impl Default for GameServerConfig {
    fn default() -> Self {
        Self {
            default_partition_count: 1,
            default_max_players: 64,
            subscription_limit_per_player: 16,
            event_log_limit: 256,
            tick_rate_hz: 20,
            // Matches the simulation default `max_commands_per_frame`.
            max_pending_commands_per_world: 10_000,
        }
    }
}

/// Per-game configuration (orchestration metadata only).
#[derive(Debug, Clone)]
pub struct GameInstanceConfig {
    pub(crate) game_type: String,
    pub(crate) max_players: usize,
    pub(crate) partition_count: usize,
    pub(crate) world_seed: u64,
    pub(crate) on_player_join: Option<String>,
    pub(crate) on_player_leave: Option<String>,
}

impl GameInstanceConfig {
    /// A configuration for a game of the given type: 64 players, 1
    /// partition, seed 0, and no join/leave reducers.
    pub fn new(game_type: impl Into<String>) -> Self {
        Self {
            game_type: game_type.into(),
            max_players: 64,
            partition_count: 1,
            world_seed: 0,
            on_player_join: None,
            on_player_leave: None,
        }
    }

    /// Sets the per-game player capacity.
    pub fn with_max_players(mut self, max: usize) -> Self {
        self.max_players = max;
        self
    }

    /// Sets the number of authoritative partitions (worlds) backing the
    /// game. Must be at least 1.
    pub fn with_partition_count(mut self, count: usize) -> Self {
        self.partition_count = count;
        self
    }

    /// Sets the deterministic world seed applied to every partition's
    /// simulation configuration.
    pub fn with_world_seed(mut self, seed: u64) -> Self {
        self.world_seed = seed;
        self
    }

    /// Names the reducer invoked (server-trusted) when a player joins, for
    /// authoritative initialization through the simulation path.
    pub fn with_on_player_join(mut self, reducer: impl Into<String>) -> Self {
        self.on_player_join = Some(reducer.into());
        self
    }

    /// Names the reducer invoked (server-trusted) when a player leaves, for
    /// authoritative cleanup through the simulation path.
    pub fn with_on_player_leave(mut self, reducer: impl Into<String>) -> Self {
        self.on_player_leave = Some(reducer.into());
        self
    }

    /// The game type label.
    pub fn game_type(&self) -> &str {
        &self.game_type
    }

    /// The per-game player capacity.
    pub fn max_players(&self) -> usize {
        self.max_players
    }

    /// The number of authoritative partitions backing the game.
    pub fn partition_count(&self) -> usize {
        self.partition_count
    }

    /// The deterministic world seed.
    pub fn world_seed(&self) -> u64 {
        self.world_seed
    }

    /// The join reducer name, if configured.
    pub fn on_player_join(&self) -> Option<&str> {
        self.on_player_join.as_deref()
    }

    /// The leave reducer name, if configured.
    pub fn on_player_leave(&self) -> Option<&str> {
        self.on_player_leave.as_deref()
    }

    /// Validates the configuration against the server defaults. Returns a
    /// human-readable error string on failure.
    pub fn validate(&self, _server: &GameServerConfig) -> Result<(), String> {
        if self.max_players == 0 {
            return Err("max_players must be at least 1".to_string());
        }
        if self.partition_count == 0 {
            return Err("partition_count must be at least 1".to_string());
        }
        if self.game_type.is_empty() {
            return Err("game_type must not be empty".to_string());
        }
        Ok(())
    }
}
