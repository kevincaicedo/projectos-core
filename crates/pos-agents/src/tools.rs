//! Capability- and taint-enforcing tool registry (L5/L6).
//!
//! An [`AuthorizedToolCall`] has no public constructor. The harness can
//! obtain one only after the registry has checked the per-Run allowlist,
//! central block/gate mode, permanent gates, taint, exact one-call receipt,
//! descriptor version, and bounded input. That type is the effect boundary.

use pos_domain::{RunToolCall, RunToolGrant, RunToolGrantMode};
use pos_foundation::{GateReceiptId, RunId, ToolCallId, UserId};
use std::collections::BTreeMap;
use std::fmt;

pub const TOOL_REGISTRY_COUNT_MAX: usize = 256;
pub const RUN_TOOL_GRANT_COUNT_MAX: usize = 64;
pub const TOOL_INPUT_BYTES_MAX: usize = 65_536;
const TOOL_ID_LEN_MAX: usize = 64;
const CAPABILITY_TARGET_LEN_MAX: usize = 128;
const GATE_REASON_LEN_MAX: usize = 512;

#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct ToolId(String);

impl ToolId {
    pub fn new(value: impl Into<String>) -> Result<Self, ToolRegistryError> {
        let value = value.into();
        let valid = !value.is_empty()
            && value.len() <= TOOL_ID_LEN_MAX
            && value
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'));
        if valid {
            Ok(Self(value))
        } else {
            Err(ToolRegistryError::InvalidToolId { value })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum CapabilityScope {
    ReadEvidence,
    ReadOperations,
    WriteProposal,
    WritePlan,
    WriteIncident,
    ExecLocal,
    ExecCloud,
    ChangeProduction,
    Egress(String),
    SecretUse(String),
}

impl CapabilityScope {
    pub fn egress(target: impl Into<String>) -> Result<Self, ToolRegistryError> {
        bounded_target("egress", target.into()).map(Self::Egress)
    }

    pub fn secret_use(reference: impl Into<String>) -> Result<Self, ToolRegistryError> {
        bounded_target("secret reference", reference.into()).map(Self::SecretUse)
    }

    #[must_use]
    pub const fn is_egress(&self) -> bool {
        matches!(self, Self::Egress(_))
    }

    #[must_use]
    pub const fn is_always_gated(&self) -> bool {
        matches!(self, Self::ChangeProduction | Self::Egress(_))
    }
}

fn bounded_target(kind: &'static str, value: String) -> Result<String, ToolRegistryError> {
    let valid = !value.is_empty()
        && value.len() <= CAPABILITY_TARGET_LEN_MAX
        && !value.chars().any(char::is_control);
    if valid {
        Ok(value)
    } else {
        Err(ToolRegistryError::InvalidCapabilityTarget { kind, value })
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolPolicyMode {
    Allow,
    Gate,
    Block,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolEffectClass {
    ReadOnly,
    Idempotent,
    NonIdempotent,
}

impl ToolEffectClass {
    /// The static label a trace carries for this step (m0-s15). The tool's
    /// own id is a registered identifier rather than a literal, and it is
    /// already in the durable step fact, so the span carries the class — the
    /// L5/L6 question — and the log carries the name.
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ReadOnly => "read_only",
            Self::Idempotent => "idempotent",
            Self::NonIdempotent => "non_idempotent",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolDescriptor {
    pub id: ToolId,
    pub version: u16,
    pub scope: CapabilityScope,
    pub mode: ToolPolicyMode,
    pub effect: ToolEffectClass,
    pub max_input_bytes: u32,
}

impl ToolDescriptor {
    pub fn new(
        id: ToolId,
        version: u16,
        scope: CapabilityScope,
        mode: ToolPolicyMode,
        effect: ToolEffectClass,
        max_input_bytes: u32,
    ) -> Result<Self, ToolRegistryError> {
        if version == 0 {
            return Err(ToolRegistryError::InvalidDescriptorVersion {
                id: id.as_str().to_owned(),
            });
        }
        if max_input_bytes == 0 || max_input_bytes as usize > TOOL_INPUT_BYTES_MAX {
            return Err(ToolRegistryError::InvalidInputBound {
                id: id.as_str().to_owned(),
                configured: max_input_bytes,
                maximum: TOOL_INPUT_BYTES_MAX,
            });
        }
        Ok(Self {
            id,
            version,
            scope,
            mode,
            effect,
            max_input_bytes,
        })
    }
}

#[derive(Debug)]
pub enum ToolRegistryError {
    InvalidToolId {
        value: String,
    },
    InvalidCapabilityTarget {
        kind: &'static str,
        value: String,
    },
    InvalidDescriptorVersion {
        id: String,
    },
    InvalidInputBound {
        id: String,
        configured: u32,
        maximum: usize,
    },
    DuplicateTool {
        id: String,
    },
    DuplicateGrant {
        id: String,
    },
    TooManyTools {
        maximum: usize,
    },
    TooManyGrants {
        maximum: usize,
    },
    InvalidAutonomyLevel {
        value: u8,
    },
    InvalidGateReason,
}

impl fmt::Display for ToolRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::InvalidToolId { value } => write!(
                formatter,
                "invalid tool id {value:?}: use 1..={TOOL_ID_LEN_MAX} ASCII id characters"
            ),
            Self::InvalidCapabilityTarget { kind, value } => write!(
                formatter,
                "invalid {kind} {value:?}: use 1..={CAPABILITY_TARGET_LEN_MAX} visible characters"
            ),
            Self::InvalidDescriptorVersion { id } => {
                write!(formatter, "tool {id} descriptor version must be positive")
            }
            Self::InvalidInputBound {
                id,
                configured,
                maximum,
            } => write!(
                formatter,
                "tool {id} input bound {configured} is outside 1..={maximum} bytes"
            ),
            Self::DuplicateTool { id } => write!(formatter, "tool {id} is registered twice"),
            Self::DuplicateGrant { id } => write!(formatter, "tool {id} is granted twice"),
            Self::TooManyTools { maximum } => {
                write!(formatter, "tool registry exceeds its {maximum}-entry bound")
            }
            Self::TooManyGrants { maximum } => {
                write!(formatter, "Run allowlist exceeds its {maximum}-grant bound")
            }
            Self::InvalidAutonomyLevel { value } => {
                write!(formatter, "autonomy level {value} is outside 0..=4")
            }
            Self::InvalidGateReason => write!(
                formatter,
                "gate reason must contain 1..={GATE_REASON_LEN_MAX} visible characters"
            ),
        }
    }
}

impl std::error::Error for ToolRegistryError {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct AutonomyLevel(u8);

impl AutonomyLevel {
    pub fn new(value: u8) -> Result<Self, ToolRegistryError> {
        if value <= 4 {
            Ok(Self(value))
        } else {
            Err(ToolRegistryError::InvalidAutonomyLevel { value })
        }
    }

    #[must_use]
    pub const fn value(self) -> u8 {
        self.0
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ToolGrantMode {
    Allow,
    Gate,
    Block,
}

#[derive(Clone, Debug)]
pub struct RunToolGrants {
    entries: BTreeMap<ToolId, ToolGrantMode>,
}

impl RunToolGrants {
    pub fn new(entries: Vec<(ToolId, ToolGrantMode)>) -> Result<Self, ToolRegistryError> {
        if entries.len() > RUN_TOOL_GRANT_COUNT_MAX {
            return Err(ToolRegistryError::TooManyGrants {
                maximum: RUN_TOOL_GRANT_COUNT_MAX,
            });
        }
        let mut grants = BTreeMap::new();
        for (id, mode) in entries {
            if grants.insert(id.clone(), mode).is_some() {
                return Err(ToolRegistryError::DuplicateGrant {
                    id: id.as_str().to_owned(),
                });
            }
        }
        Ok(Self { entries: grants })
    }

    fn mode(&self, id: &ToolId) -> Option<ToolGrantMode> {
        self.entries.get(id).copied()
    }

    pub(crate) fn from_domain(entries: &[RunToolGrant]) -> Result<Self, ToolRegistryError> {
        Self::new(
            entries
                .iter()
                .map(|grant| {
                    Ok((
                        ToolId::new(grant.tool_id.clone())?,
                        match grant.mode {
                            RunToolGrantMode::Allow => ToolGrantMode::Allow,
                            RunToolGrantMode::Gate => ToolGrantMode::Gate,
                            RunToolGrantMode::Block => ToolGrantMode::Block,
                        },
                    ))
                })
                .collect::<Result<Vec<_>, ToolRegistryError>>()?,
        )
    }

    pub(crate) fn domain_grants(&self) -> Vec<RunToolGrant> {
        self.entries
            .iter()
            .map(|(tool_id, mode)| RunToolGrant {
                tool_id: tool_id.as_str().to_owned(),
                mode: match mode {
                    ToolGrantMode::Allow => RunToolGrantMode::Allow,
                    ToolGrantMode::Gate => RunToolGrantMode::Gate,
                    ToolGrantMode::Block => RunToolGrantMode::Block,
                },
            })
            .collect()
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct GateReceipt {
    pub receipt_id: GateReceiptId,
    pub run_id: RunId,
    pub call_id: ToolCallId,
    pub tool_id: ToolId,
    pub approved_by: UserId,
    pub reason: String,
    pub expires_ts_ms: u64,
}

impl GateReceipt {
    pub fn new(
        receipt_id: GateReceiptId,
        run_id: RunId,
        call_id: ToolCallId,
        tool_id: ToolId,
        approved_by: UserId,
        reason: String,
        expires_ts_ms: u64,
    ) -> Result<Self, ToolRegistryError> {
        if reason.is_empty()
            || reason.len() > GATE_REASON_LEN_MAX
            || reason.chars().any(char::is_control)
        {
            return Err(ToolRegistryError::InvalidGateReason);
        }
        Ok(Self {
            receipt_id,
            run_id,
            call_id,
            tool_id,
            approved_by,
            reason,
            expires_ts_ms,
        })
    }
}

#[derive(Clone, Debug)]
pub struct ToolCallRequest {
    pub tool_id: ToolId,
    pub call_id: ToolCallId,
    pub input: Vec<u8>,
}

#[derive(Clone, Copy)]
pub struct AuthorizationContext<'a> {
    pub run_id: RunId,
    pub step_index: u32,
    pub tainted: bool,
    pub autonomy_level: AutonomyLevel,
    pub grants: &'a RunToolGrants,
    pub receipt: Option<&'a GateReceipt>,
    pub now_ts_ms: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AuthorizationError {
    UnknownTool {
        id: String,
    },
    OutsideAllowlist {
        id: String,
    },
    Blocked {
        id: String,
    },
    GateRequired {
        id: String,
    },
    ReceiptMismatch {
        id: String,
    },
    ReceiptExpired {
        id: String,
        expires_ts_ms: u64,
    },
    InputTooLarge {
        id: String,
        len: usize,
        maximum: u32,
    },
    DescriptorVersionChanged {
        id: String,
        committed: u16,
        current: u16,
    },
}

impl fmt::Display for AuthorizationError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::UnknownTool { id } => write!(formatter, "tool {id} is not registered"),
            Self::OutsideAllowlist { id } => {
                write!(formatter, "tool {id} is outside this Run's allowlist")
            }
            Self::Blocked { id } => write!(formatter, "tool {id} is blocked by policy"),
            Self::GateRequired { id } => write!(formatter, "tool {id} requires a human gate"),
            Self::ReceiptMismatch { id } => {
                write!(
                    formatter,
                    "gate receipt does not authorize this exact {id} call"
                )
            }
            Self::ReceiptExpired { id, expires_ts_ms } => write!(
                formatter,
                "gate receipt for {id} expired at {expires_ts_ms}"
            ),
            Self::InputTooLarge { id, len, maximum } => write!(
                formatter,
                "tool {id} input is {len} bytes, exceeding its {maximum}-byte bound"
            ),
            Self::DescriptorVersionChanged {
                id,
                committed,
                current,
            } => write!(
                formatter,
                "tool {id} was committed under descriptor v{committed}, current is v{current}; pause for compatibility review"
            ),
        }
    }
}

impl std::error::Error for AuthorizationError {}

/// The only value an effect executor accepts. Fields stay private so a call
/// cannot be manufactured by deserializing untrusted bytes.
#[derive(Clone, Debug)]
pub struct AuthorizedToolCall {
    run_id: RunId,
    step_index: u32,
    descriptor: ToolDescriptor,
    call_id: ToolCallId,
    idempotency_key: String,
    input: Vec<u8>,
}

impl AuthorizedToolCall {
    #[must_use]
    pub const fn run_id(&self) -> RunId {
        self.run_id
    }

    #[must_use]
    pub const fn step_index(&self) -> u32 {
        self.step_index
    }

    #[must_use]
    pub fn tool_id(&self) -> &ToolId {
        &self.descriptor.id
    }

    #[must_use]
    pub const fn call_id(&self) -> ToolCallId {
        self.call_id
    }

    #[must_use]
    pub fn idempotency_key(&self) -> &str {
        &self.idempotency_key
    }

    #[must_use]
    pub fn input(&self) -> &[u8] {
        &self.input
    }

    #[must_use]
    pub const fn effect_class(&self) -> ToolEffectClass {
        self.descriptor.effect
    }

    pub(crate) fn domain_call(&self) -> RunToolCall {
        RunToolCall {
            tool_id: self.descriptor.id.as_str().to_owned(),
            descriptor_version: self.descriptor.version,
            call_id: self.call_id,
            idempotency_key: self.idempotency_key.clone(),
            input: self.input.clone(),
        }
    }
}

#[derive(Debug)]
pub struct ToolRegistry {
    entries: BTreeMap<ToolId, ToolDescriptor>,
}

impl ToolRegistry {
    pub fn new(descriptors: Vec<ToolDescriptor>) -> Result<Self, ToolRegistryError> {
        if descriptors.len() > TOOL_REGISTRY_COUNT_MAX {
            return Err(ToolRegistryError::TooManyTools {
                maximum: TOOL_REGISTRY_COUNT_MAX,
            });
        }
        let mut entries = BTreeMap::new();
        for descriptor in descriptors {
            let id = descriptor.id.clone();
            if entries.insert(id.clone(), descriptor).is_some() {
                return Err(ToolRegistryError::DuplicateTool {
                    id: id.as_str().to_owned(),
                });
            }
        }
        Ok(Self { entries })
    }

    pub fn authorize(
        &self,
        request: ToolCallRequest,
        context: AuthorizationContext<'_>,
    ) -> Result<AuthorizedToolCall, AuthorizationError> {
        let descriptor =
            self.entries
                .get(&request.tool_id)
                .ok_or_else(|| AuthorizationError::UnknownTool {
                    id: request.tool_id.as_str().to_owned(),
                })?;
        let grant = context.grants.mode(&request.tool_id).ok_or_else(|| {
            AuthorizationError::OutsideAllowlist {
                id: request.tool_id.as_str().to_owned(),
            }
        })?;
        if descriptor.mode == ToolPolicyMode::Block || grant == ToolGrantMode::Block {
            return Err(AuthorizationError::Blocked {
                id: request.tool_id.as_str().to_owned(),
            });
        }
        if request.input.len() > descriptor.max_input_bytes as usize {
            return Err(AuthorizationError::InputTooLarge {
                id: request.tool_id.as_str().to_owned(),
                len: request.input.len(),
                maximum: descriptor.max_input_bytes,
            });
        }

        // Autonomy changes how much ordinary proposal work may proceed; it
        // deliberately never appears in the permanent-gate decision.
        let _ = context.autonomy_level.value();
        let taint_gate = context.tainted
            && (descriptor.scope.is_egress() || descriptor.effect != ToolEffectClass::ReadOnly);
        let gate_required = descriptor.mode == ToolPolicyMode::Gate
            || grant == ToolGrantMode::Gate
            || descriptor.scope.is_always_gated()
            || descriptor.effect == ToolEffectClass::NonIdempotent
            || taint_gate;
        if gate_required {
            validate_receipt(&request, &context)?;
        }
        Ok(AuthorizedToolCall {
            run_id: context.run_id,
            step_index: context.step_index,
            descriptor: descriptor.clone(),
            call_id: request.call_id,
            idempotency_key: idempotency_key(context.run_id, context.step_index, request.call_id),
            input: request.input,
        })
    }

    pub(crate) fn rehydrate(
        &self,
        run_id: RunId,
        step_index: u32,
        call: &RunToolCall,
    ) -> Result<AuthorizedToolCall, AuthorizationError> {
        let id =
            ToolId::new(call.tool_id.clone()).map_err(|_| AuthorizationError::UnknownTool {
                id: call.tool_id.clone(),
            })?;
        let descriptor = self
            .entries
            .get(&id)
            .ok_or_else(|| AuthorizationError::UnknownTool {
                id: call.tool_id.clone(),
            })?;
        if descriptor.version != call.descriptor_version {
            return Err(AuthorizationError::DescriptorVersionChanged {
                id: call.tool_id.clone(),
                committed: call.descriptor_version,
                current: descriptor.version,
            });
        }
        if call.input.len() > descriptor.max_input_bytes as usize {
            return Err(AuthorizationError::InputTooLarge {
                id: call.tool_id.clone(),
                len: call.input.len(),
                maximum: descriptor.max_input_bytes,
            });
        }
        Ok(AuthorizedToolCall {
            run_id,
            step_index,
            descriptor: descriptor.clone(),
            call_id: call.call_id,
            idempotency_key: call.idempotency_key.clone(),
            input: call.input.clone(),
        })
    }

    #[must_use]
    pub fn descriptors(&self) -> Vec<&ToolDescriptor> {
        self.entries.values().collect()
    }
}

fn validate_receipt(
    request: &ToolCallRequest,
    context: &AuthorizationContext<'_>,
) -> Result<(), AuthorizationError> {
    let id = request.tool_id.as_str().to_owned();
    let Some(receipt) = context.receipt else {
        return Err(AuthorizationError::GateRequired { id });
    };
    if receipt.run_id != context.run_id
        || receipt.call_id != request.call_id
        || receipt.tool_id != request.tool_id
    {
        return Err(AuthorizationError::ReceiptMismatch { id });
    }
    if receipt.expires_ts_ms < context.now_ts_ms {
        return Err(AuthorizationError::ReceiptExpired {
            id,
            expires_ts_ms: receipt.expires_ts_ms,
        });
    }
    Ok(())
}

fn idempotency_key(run_id: RunId, step_index: u32, call_id: ToolCallId) -> String {
    format!("{}:{step_index}:{}", run_id.to_hex(), call_id.to_hex())
}

#[cfg(test)]
mod tests {
    use super::{
        AuthorizationContext, AuthorizationError, AutonomyLevel, CapabilityScope, GateReceipt,
        RunToolGrants, ToolCallRequest, ToolDescriptor, ToolEffectClass, ToolGrantMode, ToolId,
        ToolPolicyMode, ToolRegistry,
    };
    use pos_foundation::{GateReceiptId, RunId, ToolCallId, UserId};

    fn descriptor(id: &str, scope: CapabilityScope, effect: ToolEffectClass) -> ToolDescriptor {
        ToolDescriptor::new(
            ToolId::new(id).expect("test id"),
            1,
            scope,
            ToolPolicyMode::Allow,
            effect,
            128,
        )
        .expect("test descriptor")
    }

    fn request(id: &str, call_byte: u8) -> ToolCallRequest {
        ToolCallRequest {
            tool_id: ToolId::new(id).expect("test id"),
            call_id: ToolCallId::from_bytes([call_byte; 16]),
            input: b"bounded".to_vec(),
        }
    }

    #[test]
    fn outside_allowlist_and_tainted_effects_stop_closed() {
        let registry = ToolRegistry::new(vec![
            descriptor(
                "evidence.read",
                CapabilityScope::ReadEvidence,
                ToolEffectClass::ReadOnly,
            ),
            descriptor(
                "plan.write",
                CapabilityScope::WritePlan,
                ToolEffectClass::Idempotent,
            ),
            descriptor(
                "network.read",
                CapabilityScope::egress("web").expect("scope"),
                ToolEffectClass::ReadOnly,
            ),
        ])
        .expect("registry");
        let empty = RunToolGrants::new(Vec::new()).expect("empty grants");
        let run_id = RunId::from_bytes([1; 16]);
        let error = registry
            .authorize(
                request("evidence.read", 2),
                AuthorizationContext {
                    run_id,
                    step_index: 0,
                    tainted: false,
                    autonomy_level: AutonomyLevel::new(0).expect("level"),
                    grants: &empty,
                    receipt: None,
                    now_ts_ms: 10,
                },
            )
            .expect_err("absent grant rejects");
        assert!(matches!(error, AuthorizationError::OutsideAllowlist { .. }));

        let grants = RunToolGrants::new(vec![(
            ToolId::new("plan.write").expect("id"),
            ToolGrantMode::Allow,
        )])
        .expect("grants");
        let error = registry
            .authorize(
                request("plan.write", 3),
                AuthorizationContext {
                    run_id,
                    step_index: 0,
                    tainted: true,
                    autonomy_level: AutonomyLevel::new(4).expect("level"),
                    grants: &grants,
                    receipt: None,
                    now_ts_ms: 10,
                },
            )
            .expect_err("tainted side effect gates even at level 4");
        assert!(matches!(error, AuthorizationError::GateRequired { .. }));

        let egress_grants = RunToolGrants::new(vec![(
            ToolId::new("network.read").expect("id"),
            ToolGrantMode::Allow,
        )])
        .expect("egress grants");
        let error = registry
            .authorize(
                request("network.read", 4),
                AuthorizationContext {
                    run_id,
                    step_index: 1,
                    tainted: true,
                    autonomy_level: AutonomyLevel::new(4).expect("level"),
                    grants: &egress_grants,
                    receipt: None,
                    now_ts_ms: 10,
                },
            )
            .expect_err("tainted egress requires a human gate");
        assert!(matches!(error, AuthorizationError::GateRequired { .. }));
    }

    #[test]
    fn every_egress_scope_stays_gated_at_level_four_and_receipts_are_one_call() {
        let egress = descriptor(
            "message.send",
            CapabilityScope::egress("message").expect("scope"),
            ToolEffectClass::Idempotent,
        );
        let registry = ToolRegistry::new(vec![egress]).expect("registry");
        let tool_id = ToolId::new("message.send").expect("id");
        let grants =
            RunToolGrants::new(vec![(tool_id.clone(), ToolGrantMode::Allow)]).expect("grants");
        let run_id = RunId::from_bytes([4; 16]);
        let call_id = ToolCallId::from_bytes([5; 16]);
        let receipt = GateReceipt::new(
            GateReceiptId::from_bytes([6; 16]),
            run_id,
            call_id,
            tool_id,
            UserId::from_bytes([7; 16]),
            "Approved exact message".to_owned(),
            100,
        )
        .expect("receipt");
        let call = registry
            .authorize(
                request("message.send", 5),
                AuthorizationContext {
                    run_id,
                    step_index: 2,
                    tainted: false,
                    autonomy_level: AutonomyLevel::new(4).expect("level"),
                    grants: &grants,
                    receipt: Some(&receipt),
                    now_ts_ms: 50,
                },
            )
            .expect("matching receipt authorizes");
        assert_eq!(call.call_id(), call_id);

        let error = registry
            .authorize(
                request("message.send", 8),
                AuthorizationContext {
                    run_id,
                    step_index: 3,
                    tainted: false,
                    autonomy_level: AutonomyLevel::new(4).expect("level"),
                    grants: &grants,
                    receipt: Some(&receipt),
                    now_ts_ms: 50,
                },
            )
            .expect_err("receipt cannot authorize a second call");
        assert!(matches!(error, AuthorizationError::ReceiptMismatch { .. }));
    }
}
