//! Graceful shutdown (Phase 16, ADR-016 D2/D3).
//!
//! [`ShutdownHandle`] is a shared `Arc<AtomicBool>` that the server loop
//! polls. It can be triggered by:
//!
//! - a console signal (SIGINT/SIGTERM via `ctrlc`),
//! - a stop-file (path configured at startup; deleted after triggering), or
//! - a `--stop-after N` tick budget (scripted shutdown).
//!
//! Whichever source fires, the server loop runs the same deterministic
//! drain-then-flush path: stop accepting connections, drain inbound, flush
//! every world's WAL via `GameServer::shutdown()` (idempotent), then exit.

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// The shared shutdown flag.
#[derive(Debug, Clone)]
pub struct ShutdownHandle {
    flag: Arc<AtomicBool>,
    /// An optional stop-file: when it appears, shutdown is requested.
    stop_file: Option<PathBuf>,
}

impl ShutdownHandle {
    /// Creates a handle. When `stop_file` is `Some`, its appearance triggers
    /// shutdown (the file is removed once consumed).
    pub fn new(stop_file: Option<PathBuf>) -> Self {
        Self {
            flag: Arc::new(AtomicBool::new(false)),
            stop_file,
        }
    }

    /// Installs a SIGINT/SIGTERM handler that flips the flag (best-effort;
    /// platforms without signal support simply poll the stop-file).
    pub fn install_signal_handler(&self) {
        let flag = Arc::clone(&self.flag);
        let _ = ctrlc::set_handler(move || {
            flag.store(true, Ordering::SeqCst);
        });
    }

    /// Requests shutdown programmatically (idempotent).
    pub fn request(&self) {
        self.flag.store(true, Ordering::SeqCst);
    }

    /// Returns `true` once shutdown has been requested.
    pub fn is_requested(&self) -> bool {
        self.flag.load(Ordering::SeqCst)
    }

    /// Polls the stop-file (if any) and folds it into the flag. Called once
    /// per server-loop iteration.
    pub fn poll(&self) {
        if self.is_requested() {
            return;
        }
        if let Some(path) = &self.stop_file
            && path.exists()
        {
            let _ = std::fs::remove_file(path);
            self.request();
        }
    }

    /// Blocks (up to `timeout`) until shutdown is requested.
    pub fn wait(&self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while !self.is_requested() {
            self.poll();
            if Instant::now() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn request_is_idempotent() {
        let handle = ShutdownHandle::new(None);
        assert!(!handle.is_requested());
        handle.request();
        handle.request();
        assert!(handle.is_requested());
    }

    #[test]
    fn stop_file_triggers_shutdown_and_is_consumed() {
        let dir = std::env::temp_dir().join(format!("nexum-stop-{}", std::process::id()));
        let stop_file = dir.join("stop");
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(&stop_file, b"").unwrap();
        let handle = ShutdownHandle::new(Some(stop_file.clone()));
        assert!(!handle.is_requested());
        handle.poll();
        assert!(handle.is_requested());
        assert!(
            !stop_file.exists(),
            "stop-file is consumed after triggering"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn wait_returns_when_requested() {
        let handle = ShutdownHandle::new(None);
        let cloned = handle.clone();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(50));
            cloned.request();
        });
        assert!(handle.wait(Duration::from_secs(5)));
    }
}
