//! # pos-api
//!
//! The ONE typed surface (L12): commands, queries, streams; ts-rs-generated TypeScript types; served identically over axum HTTP+SSE and Tauri IPC. Shells depend on this crate and nothing deeper.
//!
//! Skeleton created by m0-s01; filled by m0-s06. Charter: master plan §19.

#![forbid(unsafe_code)]

use pos_capabilities::{AccountId, CapabilityRegistry, LocalCapabilityConfig, WorkspaceId};
use std::path::PathBuf;

/// Conservative local-process composition used until account/project startup
/// configuration lands in m0-s06/m0-s08. Media and public ingress stay
/// unavailable unless a later typed configuration explicitly enables them.
pub struct LocalBootstrapConfig {
    pack_root: PathBuf,
}

impl LocalBootstrapConfig {
    #[must_use]
    pub fn isolated(pack_root: PathBuf) -> Self {
        Self { pack_root }
    }
}

/// Process-owned runtime state exposed to thin shell transports.
pub struct LocalRuntime {
    capabilities: CapabilityRegistry,
}

impl LocalRuntime {
    #[must_use]
    pub fn capability_count(&self) -> usize {
        self.capabilities.descriptors().len()
    }
}

/// Resolves all ten public capability sockets for a standalone process.
///
/// The fixed ids are process-local bootstrap identities, not durable ProjectOS
/// entity ids. m0-s06/m0-s08 replace them with values loaded through the typed
/// startup surface before any project state exists.
#[must_use]
pub fn bootstrap_local_runtime(config: LocalBootstrapConfig) -> LocalRuntime {
    LocalRuntime {
        capabilities: CapabilityRegistry::local(LocalCapabilityConfig {
            owner_account_id: AccountId::from_bytes([0; 16]),
            workspace_id: WorkspaceId::from_bytes([0; 16]),
            pack_root: config.pack_root,
            ffmpeg_available: false,
            ingress_reachable: false,
        }),
    }
}

/// Generates the M0 capability-card vocabulary through the single UI-facing
/// Rust surface. m0-s06 replaces the hand-rendered TypeScript shape with the
/// general ts-rs export pipeline without changing its source of truth.
#[must_use]
pub fn typescript_capability_catalog() -> String {
    pos_capabilities::typescript_catalog()
}

#[cfg(test)]
mod tests {
    use super::{LocalBootstrapConfig, bootstrap_local_runtime};
    use pos_capabilities::{CapabilityId, CapabilityMode};
    use std::path::PathBuf;

    #[test]
    fn isolated_startup_resolves_every_socket_with_honest_state() {
        let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-bootstrap-pack-root",
        )));
        assert_eq!(runtime.capability_count(), CapabilityId::COUNT);
        let descriptors = runtime.capabilities.descriptors();
        let connector = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::ConnectorHost)
            .expect("complete registry contains connector.host");
        assert!(matches!(connector.mode, CapabilityMode::Local));
        let ingress = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::RelayIngress)
            .expect("complete registry contains relay.ingress");
        assert!(matches!(
            ingress.mode,
            CapabilityMode::Unavailable(ref reason) if !reason.as_str().is_empty()
        ));
    }
}
