//! Production configuration for the game server (Phase 16, ADR-016).
//!
//! [`ServerConfig`] aggregates every knob the server exposes — network
//! bounds, game-server limits, runtime/persistence, rate limits, and
//! operational settings — into one validated object that can be loaded from
//! defaults, a config file, and explicit CLI flags (in that precedence
//! order, higher wins).
//!
//! The file format is deliberately dependency-free: `key = value` lines
//! with `#` comments and blank lines. Unknown keys are rejected (fail-fast,
//! so a typo cannot silently change behavior).
//!
//! Configuration is **metadata only** — it never becomes authoritative
//! gameplay state, and it never alters a running deterministic simulation.
//! `validate()` fails startup before any world, connection, or WAL exists.

use std::collections::BTreeMap;
use std::path::PathBuf;

use nexum_network::{NetworkConfig, RateLimitConfig};
use nexum_runtime::{PersistencePolicy, TickFailurePolicy};

/// The logging level (a tiny leveled logger, ADR-016).
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum LogLevel {
    /// Fatal and unrecoverable conditions.
    Error,
    /// Recoverable anomalies worth attention.
    Warn,
    /// Normal operational events (the default).
    Info,
    /// Detailed per-operation diagnostics.
    Debug,
}

impl LogLevel {
    /// Parses a level name (`error|warn|info|debug`).
    pub fn parse(value: &str) -> Result<Self, String> {
        match value.trim().to_ascii_lowercase().as_str() {
            "error" => Ok(Self::Error),
            "warn" | "warning" => Ok(Self::Warn),
            "info" => Ok(Self::Info),
            "debug" => Ok(Self::Debug),
            other => Err(format!(
                "invalid log level '{other}' (expected error|warn|info|debug)"
            )),
        }
    }

    /// Whether `self` admits messages at `level`.
    pub fn admits(self, level: Self) -> bool {
        self >= level
    }

    /// The level's display name.
    pub fn name(self) -> &'static str {
        match self {
            Self::Error => "error",
            Self::Warn => "warn",
            Self::Info => "info",
            Self::Debug => "debug",
        }
    }
}

/// The complete, validated production configuration.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    // ---- network ----
    /// TCP bind host.
    pub bind: String,
    /// TCP listen port.
    pub port: u16,
    /// Maximum concurrent connections.
    pub max_connections: usize,
    /// Maximum frame payload in bytes.
    pub max_frame_payload: u32,
    /// Maximum queued inbound frames per connection.
    pub max_queued_inbound_frames: usize,
    /// Maximum queued outbound frames per connection.
    pub max_queued_outbound_frames: usize,
    /// Maximum subscriptions per session.
    pub max_subscriptions_per_session: usize,
    /// Maximum commands per input frame.
    pub max_commands_per_frame: usize,
    /// Maximum reducer name length in bytes.
    pub max_reducer_name_len: usize,
    /// Maximum reducer argument count.
    pub max_reducer_args: usize,
    /// Maximum pending reducer calls per connection.
    pub max_pending_calls_per_connection: usize,

    // ---- game server ----
    /// Default partition count for new game instances.
    pub default_partitions: usize,
    /// Default maximum players per game.
    pub max_players: usize,
    /// Per-player server-side subscription limit.
    pub subscription_limit_per_player: usize,
    /// Bounded per-world command buffer.
    pub max_pending_commands_per_world: usize,
    /// Bounded game-server event log size.
    pub game_event_log_limit: usize,

    // ---- runtime ----
    /// Logical worker count.
    pub workers: usize,
    /// Per-world input queue bound.
    pub max_queued_inputs: usize,
    /// Per-partition inbound message queue bound.
    pub max_queued_partition_messages: usize,
    /// Per-world queued reducer-call bound.
    pub max_queued_reducer_calls: usize,
    /// WAL persistence policy.
    pub persistence: PersistencePolicy,
    /// WAL/snapshot directory (required when persistence is enabled).
    pub persistence_dir: Option<PathBuf>,
    /// Snapshot every N successful ticks (None = disabled).
    pub snapshot_interval: Option<u64>,
    /// Tick-failure policy.
    pub tick_failure_policy: TickFailurePolicy,

    // ---- rate limits ----
    /// Per-connection/session operation rate limits (auth, input/s,
    /// reducer/s, subscribe, resync).
    pub rate_limits: RateLimitConfig,

    // ---- operational ----
    /// Logical ticks per second (scheduling hint).
    pub tick_hz: u32,
    /// Deterministic world seed.
    pub seed: u64,
    /// Logging level.
    pub log_level: LogLevel,
    /// Log a metrics summary every N ticks (0 = never).
    pub metrics_interval_ticks: u64,

    // ---- auth ----
    /// Static token → principal table (`name` → id). A real provider can
    /// plug into the existing `Authenticator` trait later; this keeps the
    /// demo deterministic and dependency-free.
    pub tokens: BTreeMap<String, u64>,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            bind: "127.0.0.1".to_string(),
            port: 9337,
            max_connections: 10_000,
            max_frame_payload: 64 * 1024,
            max_queued_inbound_frames: 256,
            max_queued_outbound_frames: 1_024,
            max_subscriptions_per_session: 64,
            max_commands_per_frame: 128,
            max_reducer_name_len: 256,
            max_reducer_args: 128,
            max_pending_calls_per_connection: 64,
            default_partitions: 1,
            max_players: 64,
            subscription_limit_per_player: 16,
            max_pending_commands_per_world: 10_000,
            game_event_log_limit: 256,
            workers: 1,
            max_queued_inputs: 1_024,
            max_queued_partition_messages: 10_000,
            max_queued_reducer_calls: 1_024,
            persistence: PersistencePolicy::None,
            persistence_dir: None,
            snapshot_interval: None,
            tick_failure_policy: TickFailurePolicy::FailWorld,
            rate_limits: RateLimitConfig::default(),
            tick_hz: 20,
            seed: 42,
            log_level: LogLevel::Info,
            metrics_interval_ticks: 10 * 20, // every 10s at 20 Hz
            tokens: BTreeMap::new(),
        }
    }
}

impl ServerConfig {
    /// A default configuration.
    pub fn new() -> Self {
        Self::default()
    }

    /// Loads a configuration from a `key = value` file on top of the
    /// defaults. Unknown keys or unparsable values fail startup.
    pub fn from_file(path: &std::path::Path) -> Result<Self, String> {
        let mut config = Self::default();
        config.apply_file(path)?;
        Ok(config)
    }

    /// Applies `key = value` lines from `path` on top of the current values.
    pub fn apply_file(&mut self, path: &std::path::Path) -> Result<(), String> {
        let text = std::fs::read_to_string(path)
            .map_err(|error| format!("cannot read config file {}: {error}", path.display()))?;
        for (line_no, line) in text.lines().enumerate() {
            let line = line.trim();
            if line.is_empty() || line.starts_with('#') {
                continue;
            }
            let Some((key, value)) = line.split_once('=') else {
                return Err(format!(
                    "{}:{}: expected 'key = value', got: {line}",
                    path.display(),
                    line_no + 1
                ));
            };
            self.set(key.trim(), value.trim(), &format!("{}:{}", path.display(), line_no + 1))?;
        }
        Ok(())
    }

    /// Applies one `key = value` pair (used by the file loader and tests).
    /// `where_from` is used in error messages.
    pub fn set(&mut self, key: &str, value: &str, where_from: &str) -> Result<(), String> {
        let parse_usize = |value: &str| -> Result<usize, String> {
            value
                .parse::<usize>()
                .map_err(|_| format!("{where_from}: {key} expects a non-negative integer, got '{value}'"))
        };
        let parse_u64 = |value: &str| -> Result<u64, String> {
            value
                .parse::<u64>()
                .map_err(|_| format!("{where_from}: {key} expects an integer, got '{value}'"))
        };
        let parse_u32 = |value: &str| -> Result<u32, String> {
            value
                .parse::<u32>()
                .map_err(|_| format!("{where_from}: {key} expects an integer, got '{value}'"))
        };
        match key {
            "bind" => self.bind = value.to_string(),
            "port" => self.port = parse_u32(value)? as u16,
            "max_connections" => self.max_connections = parse_usize(value)?,
            "max_frame_payload" => self.max_frame_payload = parse_u32(value)?,
            "max_queued_inbound_frames" => self.max_queued_inbound_frames = parse_usize(value)?,
            "max_queued_outbound_frames" => self.max_queued_outbound_frames = parse_usize(value)?,
            "max_subscriptions_per_session" => {
                self.max_subscriptions_per_session = parse_usize(value)?
            }
            "max_commands_per_frame" => self.max_commands_per_frame = parse_usize(value)?,
            "max_reducer_name_len" => self.max_reducer_name_len = parse_usize(value)?,
            "max_reducer_args" => self.max_reducer_args = parse_usize(value)?,
            "max_pending_calls_per_connection" => {
                self.max_pending_calls_per_connection = parse_usize(value)?
            }
            "default_partitions" => self.default_partitions = parse_usize(value)?,
            "max_players" => self.max_players = parse_usize(value)?,
            "subscription_limit_per_player" => {
                self.subscription_limit_per_player = parse_usize(value)?
            }
            "max_pending_commands_per_world" => {
                self.max_pending_commands_per_world = parse_usize(value)?
            }
            "game_event_log_limit" => self.game_event_log_limit = parse_usize(value)?,
            "workers" => self.workers = parse_usize(value)?,
            "max_queued_inputs" => self.max_queued_inputs = parse_usize(value)?,
            "max_queued_partition_messages" => {
                self.max_queued_partition_messages = parse_usize(value)?
            }
            "max_queued_reducer_calls" => self.max_queued_reducer_calls = parse_usize(value)?,
            "persistence" => {
                self.persistence = match value.trim().to_ascii_lowercase().as_str() {
                    "none" => PersistencePolicy::None,
                    "flush" => PersistencePolicy::Flush,
                    "sync" => PersistencePolicy::Sync,
                    other => {
                        return Err(format!(
                            "{where_from}: invalid persistence '{other}' (none|flush|sync)"
                        ));
                    }
                }
            }
            "persistence_dir" => self.persistence_dir = Some(PathBuf::from(value)),
            "snapshot_interval" => self.snapshot_interval = Some(parse_u64(value)?),
            "tick_failure_policy" => {
                self.tick_failure_policy = match value.trim().to_ascii_lowercase().as_str() {
                    "fail_world" => TickFailurePolicy::FailWorld,
                    "continue" => TickFailurePolicy::Continue,
                    other => {
                        return Err(format!(
                            "{where_from}: invalid tick_failure_policy '{other}' (fail_world|continue)"
                        ));
                    }
                }
            }
            "auth_per_window" => {
                self.rate_limits =
                    self.rate_limits.with_auth_per_window(parse_u32(value)?, self.rate_limits.auth_window_secs);
            }
            "auth_window_secs" => {
                self.rate_limits =
                    self.rate_limits.with_auth_per_window(self.rate_limits.auth_per_window, parse_u64(value)?);
            }
            "input_per_sec" => {
                self.rate_limits = self.rate_limits.with_input_per_sec(parse_u32(value)?);
            }
            "reducer_per_sec" => {
                self.rate_limits = self.rate_limits.with_reducer_per_sec(parse_u32(value)?);
            }
            "subscribe_per_window" => {
                self.rate_limits = self.rate_limits.with_subscribe_per_window(
                    parse_u32(value)?,
                    self.rate_limits.subscribe_window_secs,
                );
            }
            "subscribe_window_secs" => {
                self.rate_limits = self.rate_limits.with_subscribe_per_window(
                    self.rate_limits.subscribe_per_window,
                    parse_u64(value)?,
                );
            }
            "resync_per_window" => {
                self.rate_limits = self.rate_limits.with_resync_per_window(
                    parse_u32(value)?,
                    self.rate_limits.resync_window_secs,
                );
            }
            "resync_window_secs" => {
                self.rate_limits = self.rate_limits.with_resync_per_window(
                    self.rate_limits.resync_per_window,
                    parse_u64(value)?,
                );
            }
            "tick_hz" => self.tick_hz = parse_u32(value)?,
            "seed" => self.seed = parse_u64(value)?,
            "log_level" => self.log_level = LogLevel::parse(value)?,
            "metrics_interval_ticks" => self.metrics_interval_ticks = parse_u64(value)?,
            "token" => {
                // token = name:id  (repeated lines build the table)
                let (name, id) = value
                    .split_once(':')
                    .ok_or_else(|| format!("{where_from}: token expects 'name:id', got '{value}'"))?;
                let name = name.trim();
                if name.is_empty() {
                    return Err(format!("{where_from}: empty token name"));
                }
                let id = parse_u64(id.trim())?;
                if id == 0 {
                    return Err(format!("{where_from}: token id must be at least 1"));
                }
                self.tokens.insert(name.to_string(), id);
            }
            other => {
                return Err(format!(
                    "{where_from}: unknown configuration key '{other}'"
                ));
            }
        }
        Ok(())
    }

    /// Validates every bound. Called at startup before anything is created;
    /// an invalid configuration fails fast with a human-readable error.
    pub fn validate(&self) -> Result<(), String> {
        if self.port == 0 {
            return Err("port must be at least 1".to_string());
        }
        if self.bind.is_empty() {
            return Err("bind must not be empty".to_string());
        }
        for (name, value) in [
            ("max_connections", self.max_connections),
            ("max_queued_inbound_frames", self.max_queued_inbound_frames),
            ("max_queued_outbound_frames", self.max_queued_outbound_frames),
            ("max_subscriptions_per_session", self.max_subscriptions_per_session),
            ("max_commands_per_frame", self.max_commands_per_frame),
            ("max_reducer_name_len", self.max_reducer_name_len),
            ("max_reducer_args", self.max_reducer_args),
            ("max_pending_calls_per_connection", self.max_pending_calls_per_connection),
            ("default_partitions", self.default_partitions),
            ("max_players", self.max_players),
            ("subscription_limit_per_player", self.subscription_limit_per_player),
            ("max_pending_commands_per_world", self.max_pending_commands_per_world),
            ("game_event_log_limit", self.game_event_log_limit),
            ("workers", self.workers),
            ("max_queued_inputs", self.max_queued_inputs),
            ("max_queued_partition_messages", self.max_queued_partition_messages),
            ("max_queued_reducer_calls", self.max_queued_reducer_calls),
        ] {
            if value == 0 {
                return Err(format!("{name} must be at least 1"));
            }
        }
        if self.max_frame_payload == 0 {
            return Err("max_frame_payload must be at least 1".to_string());
        }
        if self.tick_hz == 0 {
            return Err("tick_hz must be at least 1".to_string());
        }
        if let Some(interval) = self.snapshot_interval
            && interval == 0
        {
            return Err("snapshot_interval must be at least 1".to_string());
        }
        if self.persistence.is_enabled() && self.persistence_dir.is_none() {
            return Err(
                "persistence_dir is required when persistence is enabled (flush|sync)".to_string(),
            );
        }
        if self.tokens.is_empty() {
            return Err("at least one token is required (e.g. token = alice:1)".to_string());
        }
        self.rate_limits
            .validate()
            .map_err(|message| format!("rate limit configuration invalid: {message}"))
    }

    /// Builds the [`NetworkConfig`] for this configuration.
    pub fn network_config(&self) -> NetworkConfig {
        NetworkConfig::new()
            .with_max_frame_payload(self.max_frame_payload)
            .with_max_queued_inbound_frames(self.max_queued_inbound_frames)
            .with_max_queued_outbound_frames(self.max_queued_outbound_frames)
            .with_max_connections(self.max_connections)
            .with_max_subscriptions_per_session(self.max_subscriptions_per_session)
            .with_max_commands_per_frame(self.max_commands_per_frame)
            .with_max_reducer_name_len(self.max_reducer_name_len)
            .with_max_reducer_args(self.max_reducer_args)
            .with_max_pending_calls_per_connection(self.max_pending_calls_per_connection)
            .with_rate_limits(self.rate_limits)
    }

    /// Builds the [`nexum_game_server::GameServerConfig`] for this
    /// configuration.
    pub fn game_server_config(
        &self,
    ) -> nexum_game_server::GameServerConfig {
        nexum_game_server::GameServerConfig::new()
            .with_default_partition_count(self.default_partitions)
            .with_default_max_players(self.max_players)
            .with_subscription_limit_per_player(self.subscription_limit_per_player)
            .with_max_pending_commands_per_world(self.max_pending_commands_per_world)
            .with_event_log_limit(self.game_event_log_limit)
            .with_tick_rate_hz(self.tick_hz)
    }

    /// Builds the [`RuntimeConfig`] for this configuration. The world
    /// factory is passed through unchanged.
    pub fn runtime_config(
        &self,
        factory: nexum_runtime::WorldFactory,
    ) -> nexum_runtime::RuntimeConfig {
        let mut config = nexum_runtime::RuntimeConfig::new(factory)
            .with_worker_count(self.workers)
            .with_max_queued_inputs(self.max_queued_inputs)
            .with_max_queued_partition_messages(self.max_queued_partition_messages)
            .with_max_queued_reducer_calls(self.max_queued_reducer_calls)
            .with_tick_failure_policy(self.tick_failure_policy)
            .with_event_log_limit(self.game_event_log_limit);
        if self.persistence.is_enabled()
            && let Some(dir) = &self.persistence_dir
        {
            config = config.with_persistence(self.persistence, dir.clone());
        }
        if let Some(interval) = self.snapshot_interval {
            config = config.with_snapshot_interval(interval);
        }
        config
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_validate() {
        let mut config = ServerConfig::default();
        config.tokens.insert("alice".to_string(), 1);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn unknown_key_fails() {
        let mut config = ServerConfig::default();
        assert!(config.set("nonsense", "1", "test").is_err());
    }

    #[test]
    fn file_roundtrip_applies_and_unknown_keys_fail() {
        let dir = std::env::temp_dir().join(format!("nexum-cfg-{}", std::process::id()));
        let path = dir.join("server.conf");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(
            &path,
            "# production arena\n\
             port = 9001\n\
             workers = 4\n\
             persistence = flush\n\
             persistence_dir = data\n\
             token = alice:1\n\
             token = bob:2\n\
             input_per_sec = 60\n",
        )
        .unwrap();
        let config = ServerConfig::from_file(&path).unwrap();
        assert_eq!(config.port, 9001);
        assert_eq!(config.workers, 4);
        assert_eq!(config.persistence, PersistencePolicy::Flush);
        assert_eq!(config.persistence_dir, Some(PathBuf::from("data")));
        assert_eq!(config.tokens.get("alice"), Some(&1));
        assert_eq!(config.tokens.get("bob"), Some(&2));
        assert_eq!(config.rate_limits.input_per_sec, 60);
        assert!(config.validate().is_ok());

        // A bad file fails fast.
        std::fs::write(&path, "bogus_key = 1\n").unwrap();
        assert!(ServerConfig::from_file(&path).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn invalid_values_fail_validation() {
        let mut config = ServerConfig::default();
        config.tokens.insert("alice".to_string(), 1);
        config.workers = 0;
        assert!(config.validate().is_err());
        config.workers = 2;
        config.persistence = PersistencePolicy::Sync;
        config.persistence_dir = None;
        assert!(config.validate().is_err());
        config.persistence_dir = Some(PathBuf::from("data"));
        assert!(config.validate().is_ok());
    }

    #[test]
    fn derived_configs_respect_bounds() {
        let mut config = ServerConfig::default();
        config.tokens.insert("alice".to_string(), 1);
        config.validate().unwrap();
        let network = config.network_config();
        assert_eq!(network.max_connections(), config.max_connections);
        assert_eq!(network.max_commands_per_frame(), config.max_commands_per_frame);
        let game = config.game_server_config();
        assert_eq!(game.default_partition_count(), config.default_partitions);
        assert_eq!(game.tick_rate_hz(), config.tick_hz);
    }
}
