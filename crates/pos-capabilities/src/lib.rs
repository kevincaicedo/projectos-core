//! # pos-capabilities
//!
//! The frozen open-core socket (L11): ten capability traits, a runtime
//! registry, and honest local providers. Cloud may implement these traits but
//! cannot extend them privately. Request/response enums are non-exhaustive so
//! later milestones can add public operations without replacing the object-safe
//! registry signature.

#![forbid(unsafe_code)]

mod local;
mod traits;
mod types;

pub use local::{
    DirectSync, FilePackSource, KeychainBroker, LocalBus, LocalConnectorHost, LocalControlPlane,
    LocalIngress, LocalPool, LocalRenderer, NoopMeter,
};
pub use pos_foundation::{AccountId, ProjectId, RunId, WorkspaceId};
pub use traits::*;
pub use types::*;

use std::path::PathBuf;
use std::sync::Arc;
use std::{error::Error, fmt};

/// Version of the ten frozen provider signatures in ADR-0004.
pub const CAPABILITY_TRAIT_VERSION: u16 = 1;

/// All runtime providers. Construction requires every socket, so an absent
/// provider is represented by `Unavailable(reason)`, never a missing card.
pub struct CapabilityRegistry {
    control_plane: Arc<dyn ControlPlane>,
    credential_broker: Arc<dyn CredentialBroker>,
    sync_transport: Arc<dyn SyncTransport>,
    realtime_bus: Arc<dyn RealtimeBus>,
    worker_fleet: Arc<dyn WorkerFleet>,
    pack_source: Arc<dyn PackSource>,
    media_renderer: Arc<dyn MediaRenderer>,
    billing_meter: Arc<dyn BillingMeter>,
    ingress_relay: Arc<dyn IngressRelay>,
    connector_host: Arc<dyn ConnectorHost>,
}

/// Inputs that vary by local process; no secret value belongs here.
pub struct LocalCapabilityConfig {
    pub owner_account_id: pos_foundation::AccountId,
    pub workspace_id: pos_foundation::WorkspaceId,
    pub pack_root: PathBuf,
    pub ffmpeg_available: bool,
    pub ingress_reachable: bool,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RegistryError {
    MisreportedCapability {
        expected: CapabilityId,
        reported: CapabilityId,
    },
    EmptyProviderName {
        capability: CapabilityId,
    },
}

impl fmt::Display for RegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::MisreportedCapability { expected, reported } => write!(
                formatter,
                "provider for {} reported itself as {}",
                expected.as_str(),
                reported.as_str()
            ),
            Self::EmptyProviderName { capability } => {
                write!(
                    formatter,
                    "provider for {} has no name",
                    capability.as_str()
                )
            }
        }
    }
}

impl Error for RegistryError {}

impl CapabilityRegistry {
    /// Installs all ten standalone providers with bounded in-process stores.
    #[must_use]
    pub fn local(config: LocalCapabilityConfig) -> Self {
        Self {
            control_plane: Arc::new(LocalControlPlane::new(
                config.owner_account_id,
                config.workspace_id,
            )),
            credential_broker: Arc::new(KeychainBroker::default()),
            sync_transport: Arc::new(DirectSync::default()),
            realtime_bus: Arc::new(LocalBus::default()),
            worker_fleet: Arc::new(LocalPool::default()),
            pack_source: Arc::new(FilePackSource::new(config.pack_root)),
            media_renderer: Arc::new(LocalRenderer::new(config.ffmpeg_available)),
            billing_meter: Arc::new(NoopMeter::default()),
            ingress_relay: Arc::new(LocalIngress::new(config.ingress_reachable)),
            connector_host: Arc::new(LocalConnectorHost),
        }
    }

    /// Constructs a registry from hosted or third-party providers.
    /// Validates externally supplied providers before exposing the registry.
    ///
    /// # Errors
    ///
    /// Returns a typed error when a provider misreports its public socket or
    /// supplies an empty name, so startup stops closed instead of mislabeling a
    /// capability card.
    pub fn from_providers(providers: CapabilityProviders) -> Result<Self, RegistryError> {
        let registry = Self {
            control_plane: providers.control_plane,
            credential_broker: providers.credential_broker,
            sync_transport: providers.sync_transport,
            realtime_bus: providers.realtime_bus,
            worker_fleet: providers.worker_fleet,
            pack_source: providers.pack_source,
            media_renderer: providers.media_renderer,
            billing_meter: providers.billing_meter,
            ingress_relay: providers.ingress_relay,
            connector_host: providers.connector_host,
        };
        for (expected, descriptor) in CapabilityId::ALL.into_iter().zip(registry.descriptors()) {
            if descriptor.id != expected {
                return Err(RegistryError::MisreportedCapability {
                    expected,
                    reported: descriptor.id,
                });
            }
            if descriptor.provider_name.trim().is_empty() {
                return Err(RegistryError::EmptyProviderName {
                    capability: expected,
                });
            }
        }
        Ok(registry)
    }

    #[must_use]
    pub fn descriptors(&self) -> [CapabilityDescriptor; CapabilityId::COUNT] {
        [
            self.control_plane.descriptor(),
            self.credential_broker.descriptor(),
            self.sync_transport.descriptor(),
            self.realtime_bus.descriptor(),
            self.worker_fleet.descriptor(),
            self.pack_source.descriptor(),
            self.media_renderer.descriptor(),
            self.billing_meter.descriptor(),
            self.ingress_relay.descriptor(),
            self.connector_host.descriptor(),
        ]
    }

    #[must_use]
    pub fn control_plane(&self) -> &dyn ControlPlane {
        self.control_plane.as_ref()
    }

    #[must_use]
    pub fn credential_broker(&self) -> &dyn CredentialBroker {
        self.credential_broker.as_ref()
    }

    #[must_use]
    pub fn sync_transport(&self) -> &dyn SyncTransport {
        self.sync_transport.as_ref()
    }

    #[must_use]
    pub fn realtime_bus(&self) -> &dyn RealtimeBus {
        self.realtime_bus.as_ref()
    }

    #[must_use]
    pub fn worker_fleet(&self) -> &dyn WorkerFleet {
        self.worker_fleet.as_ref()
    }

    #[must_use]
    pub fn pack_source(&self) -> &dyn PackSource {
        self.pack_source.as_ref()
    }

    #[must_use]
    pub fn media_renderer(&self) -> &dyn MediaRenderer {
        self.media_renderer.as_ref()
    }

    #[must_use]
    pub fn billing_meter(&self) -> &dyn BillingMeter {
        self.billing_meter.as_ref()
    }

    #[must_use]
    pub fn ingress_relay(&self) -> &dyn IngressRelay {
        self.ingress_relay.as_ref()
    }

    #[must_use]
    pub fn connector_host(&self) -> &dyn ConnectorHost {
        self.connector_host.as_ref()
    }
}

/// Explicit provider bundle used by cloud/startup composition.
pub struct CapabilityProviders {
    pub control_plane: Arc<dyn ControlPlane>,
    pub credential_broker: Arc<dyn CredentialBroker>,
    pub sync_transport: Arc<dyn SyncTransport>,
    pub realtime_bus: Arc<dyn RealtimeBus>,
    pub worker_fleet: Arc<dyn WorkerFleet>,
    pub pack_source: Arc<dyn PackSource>,
    pub media_renderer: Arc<dyn MediaRenderer>,
    pub billing_meter: Arc<dyn BillingMeter>,
    pub ingress_relay: Arc<dyn IngressRelay>,
    pub connector_host: Arc<dyn ConnectorHost>,
}

/// A deliberately redundant catalog shape used by the honesty checker and its
/// malformed fixtures. The production catalog below fills every field.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CatalogEntry {
    pub id: CapabilityId,
    pub trait_name: Option<&'static str>,
    pub local_default_name: Option<&'static str>,
    pub ui_card_id: Option<&'static str>,
    pub unavailable_reason_required: bool,
    pub open_default_functional: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CatalogViolation {
    pub id: CapabilityId,
    pub field: &'static str,
}

#[must_use]
pub fn capability_catalog() -> [CatalogEntry; CapabilityId::COUNT] {
    CapabilityId::ALL.map(|id| CatalogEntry {
        id,
        trait_name: Some(id.trait_name()),
        local_default_name: Some(id.local_default_name()),
        ui_card_id: Some(id.ui_card_id()),
        unavailable_reason_required: true,
        open_default_functional: true,
    })
}

#[must_use]
pub fn catalog_violations(entries: &[CatalogEntry]) -> Vec<CatalogViolation> {
    let mut violations = Vec::new();
    for id in CapabilityId::ALL {
        let matches: Vec<&CatalogEntry> = entries.iter().filter(|entry| entry.id == id).collect();
        if matches.len() != 1 {
            violations.push(CatalogViolation {
                id,
                field: "exactly_one_entry",
            });
            continue;
        }
        let entry = matches[0];
        for (field, present) in [
            ("trait", non_empty(entry.trait_name)),
            ("local_default", non_empty(entry.local_default_name)),
            ("ui_card", non_empty(entry.ui_card_id)),
            ("unavailable_reason", entry.unavailable_reason_required),
            ("open_default_functional", entry.open_default_functional),
        ] {
            if !present {
                violations.push(CatalogViolation { id, field });
            }
        }
    }
    violations
}

fn non_empty(value: Option<&str>) -> bool {
    value.is_some_and(|value| !value.trim().is_empty())
}

/// Generates the UI catalog from the Rust source of truth. Runtime state still
/// arrives through `pos-api`; this file only freezes ids, labels, and cards.
#[must_use]
pub fn typescript_catalog() -> String {
    let mut output = String::from(
        "// @generated by `cargo run -p pos-api --bin export-capabilities`; do not edit.\n\n\
export const capabilityCards = [\n",
    );
    for id in CapabilityId::ALL {
        output.push_str(&format!(
            "  {{\n    id: \"{}\",\n    title: \"{}\",\n    localDefault: \"{}\",\n    uiCard: \"{}\",\n  }},\n",
            id.as_str(),
            id.title(),
            id.local_default_name(),
            id.ui_card_id()
        ));
    }
    output.push_str(
        "] as const;\n\nexport type CapabilityId = (typeof capabilityCards)[number][\"id\"];\nexport type CapabilityState =\n  { mode: \"local\" | \"hosted\" } | { mode: \"unavailable\"; reason: string };\n",
    );
    output
}

#[cfg(test)]
mod tests {
    use super::{
        CapabilityDescriptor, CapabilityError, CapabilityId, CapabilityMode, CapabilityProvider,
        CapabilityProviders, CapabilityRegistry, ControlPlane, ControlPlaneRequest,
        ControlPlaneResponse, LocalCapabilityConfig, ProviderFuture, RegistryError,
        capability_catalog, catalog_violations, typescript_catalog,
    };
    use pos_foundation::{AccountId, WorkspaceId};
    use std::path::PathBuf;
    use std::sync::Arc;

    #[test]
    fn production_catalog_has_all_ten_honest_entries() {
        assert_eq!(CapabilityId::ALL.len(), 10);
        assert!(catalog_violations(&capability_catalog()).is_empty());
    }

    #[test]
    fn missing_ui_card_and_reason_are_fixture_violations() {
        let mut fixture = capability_catalog();
        fixture[9].ui_card_id = None;
        fixture[9].unavailable_reason_required = false;
        let violations = catalog_violations(&fixture);
        assert_eq!(violations.len(), 2);
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "ui_card")
        );
        assert!(
            violations
                .iter()
                .any(|violation| violation.field == "unavailable_reason")
        );
    }

    #[test]
    fn generated_typescript_contains_every_id_once() {
        let generated = typescript_catalog();
        for id in CapabilityId::ALL {
            assert_eq!(
                generated.matches(id.as_str()).count(),
                2,
                "id and UI-card fields must both name {}",
                id.as_str()
            );
        }
    }

    #[test]
    fn external_registry_stops_closed_on_a_misreported_socket() {
        let providers = providers_with_control_plane(InvalidControlPlane {
            id: CapabilityId::ConnectorHost,
            name: "wrong-socket",
        });
        assert!(matches!(
            CapabilityRegistry::from_providers(providers),
            Err(RegistryError::MisreportedCapability {
                expected: CapabilityId::ControlPlane,
                reported: CapabilityId::ConnectorHost,
            })
        ));
    }

    #[test]
    fn external_registry_stops_closed_on_an_empty_provider_name() {
        let providers = providers_with_control_plane(InvalidControlPlane {
            id: CapabilityId::ControlPlane,
            name: "  ",
        });
        assert!(matches!(
            CapabilityRegistry::from_providers(providers),
            Err(RegistryError::EmptyProviderName {
                capability: CapabilityId::ControlPlane,
            })
        ));
    }

    struct InvalidControlPlane {
        id: CapabilityId,
        name: &'static str,
    }

    impl CapabilityProvider for InvalidControlPlane {
        fn descriptor(&self) -> CapabilityDescriptor {
            CapabilityDescriptor {
                id: self.id,
                provider_name: self.name,
                mode: CapabilityMode::Local,
            }
        }
    }

    impl ControlPlane for InvalidControlPlane {
        fn execute(
            &self,
            _request: ControlPlaneRequest,
        ) -> ProviderFuture<'_, Result<ControlPlaneResponse, CapabilityError>> {
            Box::pin(async {
                Err(CapabilityError::NotYetSupported {
                    capability: CapabilityId::ControlPlane,
                    operation: "invalid fixture",
                })
            })
        }
    }

    fn providers_with_control_plane(control_plane: InvalidControlPlane) -> CapabilityProviders {
        let local = CapabilityRegistry::local(LocalCapabilityConfig {
            owner_account_id: AccountId::from_bytes([1; 16]),
            workspace_id: WorkspaceId::from_bytes([2; 16]),
            pack_root: PathBuf::from("fixture-pack-root"),
            ffmpeg_available: false,
            ingress_reachable: false,
        });
        CapabilityProviders {
            control_plane: Arc::new(control_plane),
            credential_broker: Arc::clone(&local.credential_broker),
            sync_transport: Arc::clone(&local.sync_transport),
            realtime_bus: Arc::clone(&local.realtime_bus),
            worker_fleet: Arc::clone(&local.worker_fleet),
            pack_source: Arc::clone(&local.pack_source),
            media_renderer: Arc::clone(&local.media_renderer),
            billing_meter: Arc::clone(&local.billing_meter),
            ingress_relay: Arc::clone(&local.ingress_relay),
            connector_host: Arc::clone(&local.connector_host),
        }
    }
}
