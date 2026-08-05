use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;

/// Keeps connector/realtime test payloads far below the later 8 MiB ingest cap.
pub const CAPABILITY_PAYLOAD_SIZE_MAX: usize = 1_048_576;
pub const UNAVAILABLE_REASON_SIZE_MAX: usize = 240;
const SAFE_NAME_SIZE_MAX: usize = 128;
const SECRET_REFERENCE_SIZE_MAX: usize = 512;

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum CapabilityId {
    ControlPlane,
    IdentityBroker,
    SyncTransport,
    RealtimeBus,
    WorkerFleet,
    PackSource,
    MediaRender,
    BillingMeter,
    RelayIngress,
    ConnectorHost,
}

impl CapabilityId {
    pub const COUNT: usize = 10;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ControlPlane,
        Self::IdentityBroker,
        Self::SyncTransport,
        Self::RealtimeBus,
        Self::WorkerFleet,
        Self::PackSource,
        Self::MediaRender,
        Self::BillingMeter,
        Self::RelayIngress,
        Self::ConnectorHost,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ControlPlane => "control.plane",
            Self::IdentityBroker => "identity.broker",
            Self::SyncTransport => "sync.transport",
            Self::RealtimeBus => "realtime.bus",
            Self::WorkerFleet => "worker.fleet",
            Self::PackSource => "pack.source",
            Self::MediaRender => "media.render",
            Self::BillingMeter => "billing.meter",
            Self::RelayIngress => "relay.ingress",
            Self::ConnectorHost => "connector.host",
        }
    }

    #[must_use]
    pub const fn title(self) -> &'static str {
        match self {
            Self::ControlPlane => "Control plane",
            Self::IdentityBroker => "Identity broker",
            Self::SyncTransport => "Sync transport",
            Self::RealtimeBus => "Realtime bus",
            Self::WorkerFleet => "Worker fleet",
            Self::PackSource => "Pack source",
            Self::MediaRender => "Media renderer",
            Self::BillingMeter => "Billing meter",
            Self::RelayIngress => "Ingress relay",
            Self::ConnectorHost => "Connector host",
        }
    }

    #[must_use]
    pub const fn trait_name(self) -> &'static str {
        match self {
            Self::ControlPlane => "ControlPlane",
            Self::IdentityBroker => "CredentialBroker",
            Self::SyncTransport => "SyncTransport",
            Self::RealtimeBus => "RealtimeBus",
            Self::WorkerFleet => "WorkerFleet",
            Self::PackSource => "PackSource",
            Self::MediaRender => "MediaRenderer",
            Self::BillingMeter => "BillingMeter",
            Self::RelayIngress => "IngressRelay",
            Self::ConnectorHost => "ConnectorHost",
        }
    }

    #[must_use]
    pub const fn local_default_name(self) -> &'static str {
        match self {
            Self::ControlPlane => "LocalControlPlane",
            Self::IdentityBroker => "KeychainBroker",
            Self::SyncTransport => "DirectSync",
            Self::RealtimeBus => "LocalBus",
            Self::WorkerFleet => "LocalPool",
            Self::PackSource => "FilePackSource",
            Self::MediaRender => "LocalRenderer",
            Self::BillingMeter => "NoopMeter",
            Self::RelayIngress => "LocalIngress",
            Self::ConnectorHost => "LocalConnectorHost",
        }
    }

    #[must_use]
    pub const fn ui_card_id(self) -> &'static str {
        self.as_str()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CapabilityMode {
    Local,
    Hosted,
    Unavailable(UnavailableReason),
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityDescriptor {
    pub id: CapabilityId,
    pub provider_name: &'static str,
    pub mode: CapabilityMode,
}

pub trait CapabilityProvider: Send + Sync {
    fn descriptor(&self) -> CapabilityDescriptor;
}

pub type ProviderFuture<'a, T> = Pin<Box<dyn Future<Output = T> + Send + 'a>>;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct UnavailableReason(Box<str>);

impl UnavailableReason {
    pub fn new(reason: impl Into<Box<str>>) -> Result<Self, DefinitionError> {
        let reason = reason.into();
        if reason.trim().is_empty() {
            return Err(DefinitionError::EmptyUnavailableReason);
        }
        if reason.len() > UNAVAILABLE_REASON_SIZE_MAX {
            return Err(DefinitionError::UnavailableReasonTooLong);
        }
        Ok(Self(reason))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum DefinitionError {
    EmptyUnavailableReason,
    UnavailableReasonTooLong,
    EmptyName,
    NameTooLong,
    UnsafeName,
    InvalidSecretReference,
    PayloadTooLarge,
}

impl fmt::Display for DefinitionError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(match self {
            Self::EmptyUnavailableReason => "unavailable reason is empty",
            Self::UnavailableReasonTooLong => "unavailable reason exceeds its bound",
            Self::EmptyName => "identifier is empty",
            Self::NameTooLong => "identifier exceeds its bound",
            Self::UnsafeName => "identifier contains a forbidden character",
            Self::InvalidSecretReference => "secret reference has an unsupported shape",
            Self::PayloadTooLarge => "capability payload exceeds its bound",
        })
    }
}

impl Error for DefinitionError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum CapabilityError {
    InvalidRequest {
        field: &'static str,
        reason: &'static str,
    },
    NotFound {
        resource: &'static str,
    },
    Conflict {
        resource: &'static str,
    },
    ResourceExhausted {
        resource: &'static str,
        limit: u32,
    },
    NotYetSupported {
        capability: CapabilityId,
        operation: &'static str,
    },
    Io {
        operation: &'static str,
    },
}

impl fmt::Display for CapabilityError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidRequest { field, reason } => {
                write!(formatter, "invalid {field}: {reason}")
            }
            Self::NotFound { resource } => write!(formatter, "{resource} was not found"),
            Self::Conflict { resource } => {
                write!(formatter, "{resource} conflicts with current state")
            }
            Self::ResourceExhausted { resource, limit } => {
                write!(formatter, "{resource} reached its limit of {limit}")
            }
            Self::NotYetSupported {
                capability,
                operation,
            } => write!(
                formatter,
                "{} does not yet support {operation}",
                capability.as_str()
            ),
            Self::Io { operation } => write!(formatter, "I/O failed while attempting {operation}"),
        }
    }
}

impl Error for CapabilityError {}

macro_rules! safe_name {
    ($name:ident) => {
        #[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name(Box<str>);

        impl $name {
            pub fn new(value: impl Into<Box<str>>) -> Result<Self, DefinitionError> {
                let value = value.into();
                validate_safe_name(&value)?;
                Ok(Self(value))
            }

            #[must_use]
            pub fn as_str(&self) -> &str {
                &self.0
            }
        }
    };
}

safe_name!(ConnectorId);
safe_name!(PackId);

fn validate_safe_name(value: &str) -> Result<(), DefinitionError> {
    if value.is_empty() {
        return Err(DefinitionError::EmptyName);
    }
    if value.len() > SAFE_NAME_SIZE_MAX {
        return Err(DefinitionError::NameTooLong);
    }
    if !value
        .bytes()
        .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err(DefinitionError::UnsafeName);
    }
    Ok(())
}

macro_rules! opaque_id {
    ($name:ident) => {
        #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }
        }
    };
}

opaque_id!(CredentialLeaseId);
opaque_id!(UsageRecordId);
opaque_id!(WorkerLeaseId);

#[derive(Clone, Eq, PartialEq)]
pub struct SecretRef(Box<str>);

impl SecretRef {
    pub fn new(value: impl Into<Box<str>>) -> Result<Self, DefinitionError> {
        let value = value.into();
        let Some((scheme, locator)) = value.split_once(':') else {
            return Err(DefinitionError::InvalidSecretReference);
        };
        let supported_scheme = matches!(scheme, "keychain" | "vault" | "device");
        let safe_locator = !locator.is_empty()
            && !locator.starts_with('/')
            && !locator.contains('\\')
            && !locator.split('/').any(|segment| segment == "..")
            && locator.bytes().all(|byte| {
                byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.' | b'/' | b'@')
            });
        if value.len() > SECRET_REFERENCE_SIZE_MAX || !supported_scheme || !safe_locator {
            return Err(DefinitionError::InvalidSecretReference);
        }
        Ok(Self(value))
    }

    #[must_use]
    pub fn scheme(&self) -> &'static str {
        if self.0.starts_with("keychain:") {
            "keychain"
        } else if self.0.starts_with("vault:") {
            "vault"
        } else {
            "device"
        }
    }
}

impl fmt::Debug for SecretRef {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("SecretRef")
            .field("scheme", &self.scheme())
            .finish_non_exhaustive()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CapabilityPayload(Vec<u8>);

impl CapabilityPayload {
    pub fn new(bytes: Vec<u8>) -> Result<Self, DefinitionError> {
        if bytes.len() > CAPABILITY_PAYLOAD_SIZE_MAX {
            return Err(DefinitionError::PayloadTooLarge);
        }
        Ok(Self(bytes))
    }

    #[must_use]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }
}

#[cfg(test)]
mod tests {
    use super::{CapabilityPayload, DefinitionError, SecretRef, UnavailableReason};

    #[test]
    fn unavailable_requires_a_non_empty_bounded_reason() {
        assert_eq!(
            UnavailableReason::new("   "),
            Err(DefinitionError::EmptyUnavailableReason)
        );
    }

    #[test]
    fn secret_debug_output_never_contains_the_reference() {
        let secret_ref = SecretRef::new("keychain:provider/account").expect("fixture is valid");
        let debug = format!("{secret_ref:?}");
        assert!(!debug.contains("provider/account"));
        assert!(debug.contains("keychain"));
    }

    #[test]
    fn secret_reference_requires_a_safe_non_empty_locator() {
        for invalid in [
            "keychain:",
            "vault:/absolute",
            "device:../escape",
            "unknown:provider/account",
            "keychain:provider account",
        ] {
            assert_eq!(
                SecretRef::new(invalid),
                Err(DefinitionError::InvalidSecretReference),
                "{invalid} must not become a credential lookup"
            );
        }
    }

    #[test]
    fn payload_refuses_oversize_input_without_truncating() {
        let bytes = vec![0_u8; super::CAPABILITY_PAYLOAD_SIZE_MAX + 1];
        assert_eq!(
            CapabilityPayload::new(bytes),
            Err(DefinitionError::PayloadTooLarge)
        );
    }
}
