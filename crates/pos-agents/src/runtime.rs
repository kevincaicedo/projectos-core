//! Provider-neutral runtime registry seam (F49, M0 shape; M4 dispatch).
//!
//! Runtime identity, execution placement, and model provider remain separate.
//! M0 registers only the native worker, but every field required by the future
//! `ExecutionAdapter` conformance contract is already represented.

use pos_domain::{RunExecutor, RunRuntimeKind, RunRuntimeRef};
use std::collections::BTreeMap;
use std::fmt;

/// Process registry bound. A product process with hundreds of runtime
/// adapters has a discovery/configuration bug; refusal is visible (L8).
pub const RUNTIME_REGISTRY_COUNT_MAX: usize = 256;
const RUNTIME_ID_LEN_MAX: usize = 64;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct RuntimeId(String);

impl RuntimeId {
    pub fn new(value: impl Into<String>) -> Result<Self, RuntimeRegistryError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= RUNTIME_ID_LEN_MAX
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'));
        if valid {
            Ok(Self(value))
        } else {
            Err(RuntimeRegistryError::InvalidId { value })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeAuthState {
    NotRequired,
    Ready,
    Missing,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RuntimeHealth {
    Healthy,
    Degraded,
    Unavailable,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RuntimeControlCapabilities {
    pub status: bool,
    pub cancel: bool,
    pub pause: bool,
    pub checkpoint: bool,
    pub structured_events: bool,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RuntimeDescriptor {
    pub id: RuntimeId,
    pub kind: RunRuntimeKind,
    pub contract_version: u16,
    pub supported_executors: Vec<RunExecutor>,
    pub auth: RuntimeAuthState,
    pub health: RuntimeHealth,
    pub controls: RuntimeControlCapabilities,
}

impl RuntimeDescriptor {
    pub fn native() -> Result<Self, RuntimeRegistryError> {
        Ok(Self {
            id: RuntimeId::new("projectos.native")?,
            kind: RunRuntimeKind::Native,
            contract_version: 1,
            supported_executors: vec![RunExecutor::Device],
            auth: RuntimeAuthState::NotRequired,
            health: RuntimeHealth::Healthy,
            controls: RuntimeControlCapabilities {
                status: true,
                cancel: true,
                pause: true,
                checkpoint: true,
                structured_events: true,
            },
        })
    }

    #[must_use]
    pub fn reference(&self) -> RunRuntimeRef {
        RunRuntimeRef {
            kind: self.kind,
            runtime_id: self.id.as_str().to_owned(),
            contract_version: self.contract_version,
        }
    }
}

#[derive(Debug)]
pub enum RuntimeRegistryError {
    InvalidId { value: String },
    Duplicate { id: String },
    TooMany { maximum: usize },
    Unknown { id: String },
    UnsupportedExecutor { id: String, executor: RunExecutor },
    Unavailable { id: String },
}

impl fmt::Display for RuntimeRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidId { value } => write!(
                formatter,
                "invalid runtime id {value:?}: use 1..={RUNTIME_ID_LEN_MAX} ASCII id characters"
            ),
            Self::Duplicate { id } => write!(formatter, "runtime {id} is registered twice"),
            Self::TooMany { maximum } => {
                write!(
                    formatter,
                    "runtime registry exceeds its {maximum}-entry bound"
                )
            }
            Self::Unknown { id } => write!(formatter, "runtime {id} is not registered"),
            Self::UnsupportedExecutor { id, executor } => {
                write!(
                    formatter,
                    "runtime {id} does not support {executor:?} execution"
                )
            }
            Self::Unavailable { id } => write!(formatter, "runtime {id} is unavailable"),
        }
    }
}

impl std::error::Error for RuntimeRegistryError {}

#[derive(Debug)]
pub struct RuntimeRegistry {
    entries: BTreeMap<RuntimeId, RuntimeDescriptor>,
}

impl RuntimeRegistry {
    pub fn new(descriptors: Vec<RuntimeDescriptor>) -> Result<Self, RuntimeRegistryError> {
        if descriptors.len() > RUNTIME_REGISTRY_COUNT_MAX {
            return Err(RuntimeRegistryError::TooMany {
                maximum: RUNTIME_REGISTRY_COUNT_MAX,
            });
        }
        let mut entries = BTreeMap::new();
        for descriptor in descriptors {
            let id = descriptor.id.clone();
            if entries.insert(id.clone(), descriptor).is_some() {
                return Err(RuntimeRegistryError::Duplicate {
                    id: id.as_str().to_owned(),
                });
            }
        }
        Ok(Self { entries })
    }

    pub fn native_only() -> Result<Self, RuntimeRegistryError> {
        Self::new(vec![RuntimeDescriptor::native()?])
    }

    pub fn resolve(
        &self,
        id: &RuntimeId,
        executor: RunExecutor,
    ) -> Result<&RuntimeDescriptor, RuntimeRegistryError> {
        let descriptor = self
            .entries
            .get(id)
            .ok_or_else(|| RuntimeRegistryError::Unknown {
                id: id.as_str().to_owned(),
            })?;
        if descriptor.health == RuntimeHealth::Unavailable {
            return Err(RuntimeRegistryError::Unavailable {
                id: id.as_str().to_owned(),
            });
        }
        if !descriptor.supported_executors.contains(&executor) {
            return Err(RuntimeRegistryError::UnsupportedExecutor {
                id: id.as_str().to_owned(),
                executor,
            });
        }
        Ok(descriptor)
    }

    #[must_use]
    pub fn descriptors(&self) -> Vec<&RuntimeDescriptor> {
        self.entries.values().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::{RuntimeId, RuntimeRegistry};
    use pos_domain::RunExecutor;

    #[test]
    fn native_runtime_reports_only_the_executor_this_process_can_offer() {
        let registry = RuntimeRegistry::native_only().expect("native registry");
        let id = RuntimeId::new("projectos.native").expect("fixed id");
        let local = registry.resolve(&id, RunExecutor::Device).expect("device");
        assert_eq!(local.reference().runtime_id, "projectos.native");
        assert!(registry.resolve(&id, RunExecutor::Cloud).is_err());
        assert_eq!(registry.descriptors().len(), 1);
    }
}
