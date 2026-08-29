//! Nexum module manifest (nexum.toml) parsing.
//!
//! The manifest declares module metadata and runtime resource limits.
//! Tables, reducers, and subscriptions are defined in Rust via derive macros.

use nexum_core::{Error, Result};
use serde::Deserialize;
use std::fs;
use thiserror::Error;

/// Error during manifest parsing or validation.
#[derive(Debug, Error)]
pub enum ManifestError {
    #[error("failed to read manifest file: {0}")]
    Io(#[from] std::io::Error),
    #[error("failed to parse TOML: {0}")]
    Toml(#[from] toml::de::Error),
    #[error("validation error: {0}")]
    Validation(String),
}

impl From<ManifestError> for Error {
    fn from(e: ManifestError) -> Self {
        match e {
            ManifestError::Io(e) => Error::internal(format!("manifest io: {e}")),
            ManifestError::Toml(e) => Error::invalid_argument(format!("manifest parse: {e}")),
            ManifestError::Validation(s) => Error::invalid_argument(s),
        }
    }
}

/// Module manifest (nexum.toml).
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Manifest {
    #[serde(default)]
    pub module: ModuleConfig,
    #[serde(default)]
    pub runtime: RuntimeConfig,
}

/// Module identity and version.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModuleConfig {
    /// Unique module name (alphanumeric + hyphens, max 64 chars).
    #[serde(default)]
    pub name: String,

    /// Semantic version (informational).
    #[serde(default)]
    pub version: String,

    /// Human-readable description.
    #[serde(default)]
    pub description: String,

    /// Authors.
    #[serde(default)]
    pub authors: Vec<String>,
}

impl Default for ModuleConfig {
    fn default() -> Self {
        Self {
            name: String::new(),
            version: "0.1.0".to_string(),
            description: String::new(),
            authors: Vec::new(),
        }
    }
}

/// Runtime resource limits for the module's WASM sandbox.
#[derive(Debug, Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeConfig {
    /// Maximum Wasmtime fuel per reducer call (default: 100M).
    #[serde(default = "default_max_fuel")]
    pub max_fuel: u64,

    /// Maximum linear memory in bytes (default: 256 MB).
    #[serde(default = "default_max_memory")]
    pub max_memory_bytes: u64,

    /// Maximum module instance size in bytes (default: 64 MB).
    #[serde(default = "default_max_instance")]
    pub max_instance_bytes: u64,

    /// Enable deterministic execution (default: true).
    #[serde(default = "default_true")]
    pub deterministic: bool,

    /// Stack size limit in bytes (default: 512 KB).
    #[serde(default = "default_stack")]
    pub max_stack_bytes: u64,
}

impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            max_fuel: default_max_fuel(),
            max_memory_bytes: default_max_memory(),
            max_instance_bytes: default_max_instance(),
            deterministic: true,
            max_stack_bytes: default_stack(),
        }
    }
}

fn default_max_fuel() -> u64 {
    100_000_000
}
fn default_max_memory() -> u64 {
    268_435_456 // 256 MB
}
fn default_max_instance() -> u64 {
    67_108_864 // 64 MB
}
fn default_true() -> bool {
    true
}
fn default_stack() -> u64 {
    512 * 1024 // 512 KB
}

/// Parses a manifest from a file path.
pub fn parse_manifest(path: &str) -> Result<Manifest> {
    let content = fs::read_to_string(path)?;
    parse_manifest_str(&content)
}

/// Parses a manifest from a string.
pub fn parse_manifest_str(content: &str) -> Result<Manifest> {
    let manifest: Manifest = toml::from_str(content).map_err(ManifestError::Toml)?;
    manifest.validate()?;
    Ok(manifest)
}

impl Manifest {
    /// Validates the manifest contents.
    fn validate(&self) -> Result<()> {
        if self.module.name.is_empty() {
            return Err(ManifestError::Validation(
                "module.name is required and must not be empty".into(),
            )
            .into());
        }
        if self.module.name.len() > 64 {
            return Err(ManifestError::Validation(
                "module.name must be at most 64 characters".into(),
            )
            .into());
        }
        // Validate name: alphanumeric + hyphens
        if !self
            .module
            .name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-')
        {
            return Err(ManifestError::Validation(
                "module.name must contain only alphanumeric characters and hyphens".into(),
            )
            .into());
        }
        if self.module.name.starts_with('-') || self.module.name.ends_with('-') {
            return Err(ManifestError::Validation(
                "module.name must not start or end with a hyphen".into(),
            )
            .into());
        }
        Ok(())
    }

    /// Returns a reference to the module config.
    pub fn module(&self) -> &ModuleConfig {
        &self.module
    }

    /// Returns a reference to the runtime config.
    pub fn runtime(&self) -> &RuntimeConfig {
        &self.runtime
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_MANIFEST: &str = r#"
[module]
name = "my-game"
version = "0.1.0"
description = "A simple arena game"
authors = ["Alice <alice@example.com>"]

[runtime]
max_fuel = 50000000
max_memory_bytes = 134217728
max_instance_bytes = 33554432
deterministic = true
max_stack_bytes = 262144
"#;

    #[test]
    fn parse_valid_manifest() {
        let m = parse_manifest_str(VALID_MANIFEST).unwrap();
        assert_eq!(m.module.name, "my-game");
        assert_eq!(m.module.version, "0.1.0");
        assert_eq!(m.runtime.max_fuel, 50_000_000);
        assert_eq!(m.runtime.max_memory_bytes, 134_217_728);
        assert!(m.runtime.deterministic);
    }

    #[test]
    fn parse_minimal_manifest() {
        let minimal = r#"
[module]
name = "test"
"#;
        let m = parse_manifest_str(minimal).unwrap();
        assert_eq!(m.module.name, "test");
        assert_eq!(m.runtime.max_fuel, 100_000_000);
        assert_eq!(m.runtime.max_memory_bytes, 268_435_456);
    }

    #[test]
    fn reject_empty_name() {
        let bad = r#"
[module]
name = ""
"#;
        let err = parse_manifest_str(bad).unwrap_err();
        assert!(err.to_string().contains("name is required"));
    }

    #[test]
    fn reject_invalid_name() {
        let bad = r#"
[module]
name = "my game"
"#;
        let err = parse_manifest_str(bad).unwrap_err();
        assert!(err.to_string().contains("alphanumeric"));
    }

    #[test]
    fn reject_long_name() {
        let bad = format!(
            r#"
[module]
name = "{}"
"#,
            "a".repeat(65)
        );
        let err = parse_manifest_str(&bad).unwrap_err();
        assert!(err.to_string().contains("64 characters"));
    }
}
