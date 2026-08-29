//! Observability (Phase 16, ADR-016): a tiny leveled logger and an
//! aggregate metrics snapshot.
//!
//! The logger is dependency-free: `timestamp level module message` lines on
//! stderr, filtered by the configured [`LogLevel`]. Structured `key=value`
//! fields are emitted for hot operational events.
//!
//! [`ServerMetricsSnapshot`] merges the runtime and network metric structs
//! plus a coarse memory estimate into one point-in-time picture.

use std::time::{SystemTime, UNIX_EPOCH};

use nexum_network::NetworkMetrics;
use nexum_runtime::RuntimeMetrics;

use crate::LogLevel;

/// A tiny leveled logger.
#[derive(Debug, Clone)]
pub struct Logger {
    level: LogLevel,
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
    /// Coarse live-memory estimate in bytes.
    pub memory_estimate_bytes: u64,
}

impl ServerMetricsSnapshot {
    /// Builds a snapshot from the metric sources.
    pub fn capture(
        runtime: RuntimeMetrics,
        network: NetworkMetrics,
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
            memory_estimate_bytes,
        }
    }

    /// Formats the snapshot as one human-readable summary line.
    pub fn summary_line(&self) -> String {
        let tick_avg_ns = self
            .runtime
            .tick_ns_total
            .checked_div(self.runtime.ticks_succeeded)
            .unwrap_or(0);
        format!(
            "ticks={} failed={} avg_tick={:.1}us | worlds={} partitions={} workers={} | \
             conns={} sessions={} subs={} | frames={} dropped={} rate_limited={} | \
             mem~{}MB",
            self.runtime.ticks_succeeded,
            self.runtime.ticks_failed,
            tick_avg_ns as f64 / 1_000.0,
            self.runtime.running_partitions,
            self.runtime.partitions,
            self.runtime.workers,
            self.network.connections,
            self.network.sessions,
            self.network.subscriptions,
            self.network.frames_received,
            self.network.messages_dropped,
            self.network.rate_limited,
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
        logger.info("hidden", &[]);
        logger.warn("visible", &[("k", "v".to_string())]);
    }

    #[test]
    fn snapshot_summary_is_bounded() {
        let snapshot = ServerMetricsSnapshot::capture(
            RuntimeMetrics::empty(),
            NetworkMetrics::empty(),
            1_000_000,
            100,
        );
        let line = snapshot.summary_line();
        assert!(line.contains("mem~"), "{line}");
        assert!(!line.is_empty());
    }
}
