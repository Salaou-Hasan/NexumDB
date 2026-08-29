//! The Nexum Database Server — the public-facing entry point.
//!
//! [`NexumServer`] composes a [`NetworkGateway`] (which owns the
//! [`Runtime`]) into a single database-server type. It provides the
//! clean API that game developers interact with.
//!
//! The server is the database. Modules contain application logic.
//! Reducers are server functions. Tables are authoritative state.

use std::sync::Arc;

use crate::NetworkError;
use crate::auth::Authenticator;
use crate::config::NetworkConfig;
use crate::gateway::NetworkGateway;
use crate::policy::GamePolicy;
use nexum_runtime::Runtime;

/// The Nexum Database Server.
///
/// This is the primary entry point for running a Nexum server. It owns
/// the [`NetworkGateway`] (which owns the [`Runtime`]) and provides a
/// clean API for module registration, server lifecycle, and access to
/// the underlying runtime and gateway.
pub struct NexumServer {
    gateway: NetworkGateway,
}

impl NexumServer {
    /// Creates a new Nexum Database Server.
    ///
    /// The runtime is configured with a world factory that creates
    /// partition executors. The gateway handles client connections,
    /// authentication, and protocol framing.
    pub fn new(
        runtime: Runtime,
        network_config: NetworkConfig,
        authenticator: Arc<dyn Authenticator>,
    ) -> Result<Self, NetworkError> {
        let gateway = NetworkGateway::new(runtime, network_config, authenticator)?;
        Ok(Self { gateway })
    }

    /// Creates a new Nexum Database Server with a custom authorization
    /// policy.
    pub fn with_policy(
        runtime: Runtime,
        network_config: NetworkConfig,
        authenticator: Arc<dyn Authenticator>,
        policy: Box<dyn GamePolicy>,
    ) -> Result<Self, NetworkError> {
        let mut gateway = NetworkGateway::new(runtime, network_config, authenticator)?;
        gateway.set_policy(policy);
        Ok(Self { gateway })
    }

    /// Returns a reference to the network gateway.
    pub fn gateway(&self) -> &NetworkGateway {
        &self.gateway
    }

    /// Returns a mutable reference to the network gateway.
    pub fn gateway_mut(&mut self) -> &mut NetworkGateway {
        &mut self.gateway
    }

    /// Returns a reference to the runtime.
    pub fn runtime(&self) -> &Runtime {
        self.gateway.runtime()
    }

    /// Returns a mutable reference to the runtime.
    pub fn runtime_mut(&mut self) -> &mut Runtime {
        self.gateway.runtime_mut()
    }

    /// Processes inbound client messages, ticks all worlds, fans out
    /// results to connected clients, and returns a step report.
    ///
    /// This is the canonical host-loop step: process -> tick -> fan-out.
    pub fn step(&mut self) -> Result<crate::gateway::StepReport, NetworkError> {
        self.gateway.process_inbound();
        let step = self.gateway.step_worlds()?;
        Ok(step)
    }

    /// Shuts down the server: flushes WALs, releases resources, and
    /// disconnects all clients.
    pub fn shutdown(&mut self) {
        let _ = self.gateway.runtime_mut().shutdown();
    }
}

impl std::fmt::Debug for NexumServer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("NexumServer")
            .field("gateway", &self.gateway)
            .finish()
    }
}
