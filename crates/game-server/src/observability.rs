//! Observability (Phase 16, ADR-016): a tiny leveled logger and an
//! aggregate metrics snapshot.
//!
//! The logger is dependency-free: `timestamp level module message` lines on
//! stderr, filtered by the configured [`LogLevel`]. Structured `key=value`
//! fields are emitted for hot operational events. No heavyweight framework
//! — consistent with the project's zero-dependency style.
//!
//! [`ServerMetricsSnapshot`] merges the runtime, network, and game-server
//! metric structs plus a coarse memory estimate into one point-in-time
//! picture. Snapshots never influence simulation semantics.

use std::time::{SystemTime, UNIX_EPOCH};

use nexum_game_server::GameServerMetrics;
use nexum_network::NetworkMetrics;
use nexum_runtime::RuntimeMetrics;

use crate::LogLevel;

/// A tiny leveled logger.
#[derive(Debug, Clone)]
pub struct Logger {
    level: LogLevel,
    /// The module tag shown on every line.
    module: String,
}

impl Logger {
    /// Creates a logger at `level` with the given module tag.
    pub fn new(level: LogLevel, module: impl Into<String>) -> Self {
        Self {
            level,
            module: module.into(),
        }
    }

    /// The configured level.
    pub fn level(&self) -> LogLevel {
        self.level
    }

    fn emit(&self, level: LogLevel, message: &str, fields: &[(&str, String)]) {
        if !self.level.admits(level) {
            return;
        }
        let secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs())
            .unwrap_or(0);
        let mut line = format!("{} {} {} {}", secs, level.name(), self.module, message);
        for (key, value) in fields {
            line.push_str(&format!(" {key}={value}"));
        }
        eprintln!("{line}");
    }

    /// Logs at `error`.
    pub fn error(&self, message: &str, fields: &[(&str, String)]) {
        self.emit(LogLevel::Error, message, fields);
    }

    /// Logs at `warn`.
    pub fn warn(&self, message: &str, fields: &[(&str, String)]) {
        self.emit(LogLevel::Warn, message, fields);
    }

    /// Logs at `info`.
    pub fn info(&self, message: &str, fields: &[(&str, String)]) {
        self.emit(LogLevel::Info, message, fields);
    }

    /// Logs at `debug`.
    pub fn debug(&self, message: &str, fields: &[(&str, String)]) {
        self.emit(LogLevel::Debug, message, fields);
    }
}

/// A point-in-time aggregate of every subsystem's metrics plus a coarse
/// memory estimate.
#[derive(Debug, Clone)]
pub struct ServerMetricsSnapshot {
    /// Runtime metrics (ticks, WAL, worlds, partitions, workers).
    pub runtime: RuntimeMetrics,
    /// Network metrics (connections, sessions, frames, rejections).
    pub network: NetworkMetrics,
    /// Game-server metrics (games, players, reducer calls).
    pub game: GameServerMetrics,
    /// Coarse live-memory estimate in bytes (rows × per-row + connections ×
    /// per-client + base), intended for trend detection, not accounting.
    pub memory_estimate_bytes: u64,
}

impl ServerMetricsSnapshot {
    /// Builds a snapshot from the three metric sources. `rows` is the
    /// approximate number of authoritative rows across worlds (used with the
    /// Phase 15 measured ≈88 bytes/row), and `connections` is the current
    /// connection count (≈2 KB per client estimate).
    pub fn capture(
        runtime: RuntimeMetrics,
        network: NetworkMetrics,
        game: GameServerMetrics,
        rows: u64,
        connections: usize,
    ) -> Self {
        const BYTES_PER_ROW: u64 = 88;
        const BYTES_PER_CONNECTION: u64 = 2 * 1024;
        const BASE_BYTES: u64 = 8 * 1024 * 1024;
        let memory_estimate_bytes = BASE_BYTES
            .saturating_add(rows.saturating_mul(BYTES_PER_ROW))
            .saturating_add((connections as u64).saturating_mul(BYTES_PER_CONNECTION));
        Self {
            runtime,
            network,
            game,
            memory_estimate_bytes,
        }
    }

    /// Formats the snapshot as one human-readable summary line (used by the
    /// periodic metrics log).
    pub fn summary_line(&self) -> String {
        let tick_avg_ns = self
            .runtime
            .tick_ns_total
            .checked_div(self.runtime.ticks_succeeded)
            .unwrap_or(0);
        format!(
            "ticks={} failed={} avg_tick={:.1}us | worlds={} partitions={} workers={} | \
             conns={} sessions={} subs={} | frames={} dropped={} rate_limited={} | \
             games={} players={} reducer_calls={} | mem~{}MB",
            self.runtime.ticks_succeeded,
            self.runtime.ticks_failed,
            tick_avg_ns as f64 / 1_000.0,
            self.runtime.running_worlds,
            self.runtime.partitions,
            self.runtime.workers,
            self.network.connections,
            self.network.sessions,
            self.network.subscriptions,
            self.network.frames_received,
            self.network.messages_dropped,
            self.network.rate_limited,
            self.game.games_active,
            self.game.players_active,
            self.game.reducer_calls,
            self.memory_estimate_bytes / (1024 * 1024),
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn logger_filters_by_level() {
        let logger = Logger::new(LogLevel::Warn, "test");
        // info must be suppressed at warn level (no panic, no output).
        logger.info("hidden", &[]);
        logger.warn("visible", &[("k", "v".to_string())]);
    }

    #[test]
    fn snapshot_summary_is_bounded() {
        let snapshot = ServerMetricsSnapshot::capture(
            RuntimeMetrics::empty(),
            NetworkMetrics::empty(),
            GameServerMetrics::default(),
            1_000_000,
            100,
        );
        let line = snapshot.summary_line();
        assert!(line.contains("mem~"), "{line}");
        assert!(!line.is_empty());
    }
}
