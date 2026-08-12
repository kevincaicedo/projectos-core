//! Disposable M0 Echo worker over the production harness and gateway.
//!
//! Echo is intentionally small but not a shortcut: three tool boundaries use
//! the ordinary roster, durable grants, ledger-before-effect protocol, hard
//! budgets, checkpoints, validation, CAS artifacts, and terminal Run fact.
//! Exactly one boundary invokes [`Gateway::complete`] on the fast tier. The
//! fixed marker is derived from the Run id, so a killed process can reconstruct
//! its work without hiding user content in process memory.

use crate::{
    ArtifactReport, HarnessError, RosterCharter, RunHarness, RunToolGrants, StepPlan,
    StepPreparation, ToolCallRequest, ToolDescriptor, ToolEffectClass, ToolEffectReport,
    ToolGrantMode, ToolId, ToolPolicyMode, ToolRegistry, ToolRegistryError, ValidationReport,
};
use pos_domain::{RunOutcome, RunState, RunStatus, RunStepPhase, RunUsage, RunValidationStatus};
use pos_foundation::{ArtifactId, RunId, ToolCallId, ValidationId, WallClock};
use pos_gateway::{
    CallAttribution, ChatMessage, CompletionRequest, Gateway, MessageRole, PromptFile,
    ReasoningEffort, RoutingTier, VecSink, Weather,
};
use pos_log::{Actor, ProjectLog};
use pos_store::{BlobHash, StoreError};
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::time::Duration;

pub const ECHO_AGENT_NAME: &str = "echo";
pub const ECHO_PREFLIGHT_TOOL_ID: &str = "echo.preflight";
pub const ECHO_MODEL_TOOL_ID: &str = "echo.complete";
pub const ECHO_REPORT_TOOL_ID: &str = "echo.report";

const ECHO_STEP_COUNT: u32 = 3;
const ECHO_OUTPUT_BYTES_MAX: usize = 64 * 1024;
const ECHO_OUTPUT_BYTES_BUDGET: u64 = 64 * 1024;
const ECHO_MAX_OUTPUT_TOKENS: u32 = 128;
const ECHO_CALL_TIMEOUT_MS: u32 = 30_000;
const ECHO_MODEL_TOKEN_RESERVATION: u64 = 4_096;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EchoFaultPoint {
    AfterCommit(u32),
    AfterCheckpoint(u32),
}

/// Explicit process-kill injection used by the m0-s13 shell chaos suites.
/// The marker must be an absolute test-owned file; reaching the point syncs
/// the marker and parks until the parent sends SIGKILL.
#[derive(Clone, Debug)]
pub struct EchoFaultPlan {
    point: EchoFaultPoint,
    marker: PathBuf,
}

impl EchoFaultPlan {
    pub fn new(point: EchoFaultPoint, marker: PathBuf) -> Result<Self, EchoError> {
        if !marker.is_absolute() {
            return Err(EchoError::InvalidFaultMarker);
        }
        Ok(Self { point, marker })
    }

    fn reach(&self, point: EchoFaultPoint) -> Result<(), EchoError> {
        if self.point != point {
            return Ok(());
        }
        let mut file = std::fs::File::create(&self.marker).map_err(|error| EchoError::FaultIo {
            path: self.marker.display().to_string(),
            reason: error.to_string(),
        })?;
        writeln!(file, "{point:?}").map_err(|error| EchoError::FaultIo {
            path: self.marker.display().to_string(),
            reason: error.to_string(),
        })?;
        file.sync_all().map_err(|error| EchoError::FaultIo {
            path: self.marker.display().to_string(),
            reason: error.to_string(),
        })?;
        loop {
            std::thread::park_timeout(Duration::from_secs(60));
        }
    }
}

#[derive(Debug)]
pub enum EchoError {
    Harness(HarnessError),
    Registry(ToolRegistryError),
    Gateway(Weather),
    Store(StoreError),
    InvalidPromptTier { tier: String },
    OutputTooLarge { bytes: usize },
    MissingValidation { run_id: RunId },
    ReconciliationRequired { run_id: RunId, step_index: u32 },
    InvalidFaultMarker,
    FaultIo { path: String, reason: String },
}

impl fmt::Display for EchoError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Harness(source) => write!(formatter, "{source}"),
            Self::Registry(source) => write!(formatter, "{source}"),
            Self::Gateway(source) => write!(formatter, "echo model call failed: {source}"),
            Self::Store(source) => write!(formatter, "echo artifact storage failed: {source}"),
            Self::InvalidPromptTier { tier } => {
                write!(formatter, "echo@1 requires the fast tier, found {tier:?}")
            }
            Self::OutputTooLarge { bytes } => write!(
                formatter,
                "echo output is {bytes} bytes, exceeding the {ECHO_OUTPUT_BYTES_MAX}-byte cap"
            ),
            Self::MissingValidation { run_id } => write!(
                formatter,
                "Echo Run {} reached its report step without durable validation",
                run_id.to_hex()
            ),
            Self::ReconciliationRequired { run_id, step_index } => write!(
                formatter,
                "Echo Run {} step {step_index} requires human reconciliation",
                run_id.to_hex()
            ),
            Self::InvalidFaultMarker => {
                write!(formatter, "Echo fault marker must be an absolute path")
            }
            Self::FaultIo { path, reason } => {
                write!(formatter, "Echo fault marker {path} failed: {reason}")
            }
        }
    }
}

impl std::error::Error for EchoError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Harness(source) => Some(source),
            Self::Registry(source) => Some(source),
            Self::Gateway(source) => Some(source),
            Self::Store(source) => Some(source),
            _ => None,
        }
    }
}

impl EchoError {
    #[must_use]
    pub fn code(&self) -> &'static str {
        match self {
            Self::Harness(_) => "harness_failure",
            Self::Registry(_) => "registry_failure",
            Self::Gateway(weather) => weather.code(),
            Self::Store(_) => "storage_failure",
            Self::InvalidPromptTier { .. } => "invalid_prompt_tier",
            Self::OutputTooLarge { .. } => "output_too_large",
            Self::MissingValidation { .. } => "missing_validation",
            Self::ReconciliationRequired { .. } => "reconciliation_required",
            Self::InvalidFaultMarker => "invalid_fault_marker",
            Self::FaultIo { .. } => "fault_io",
        }
    }
}

impl From<HarnessError> for EchoError {
    fn from(source: HarnessError) -> Self {
        Self::Harness(source)
    }
}

impl From<ToolRegistryError> for EchoError {
    fn from(source: ToolRegistryError) -> Self {
        Self::Registry(source)
    }
}

impl From<Weather> for EchoError {
    fn from(source: Weather) -> Self {
        Self::Gateway(source)
    }
}

impl From<StoreError> for EchoError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

pub struct EchoAgent<'runtime> {
    gateway: &'runtime Gateway<'runtime>,
    prompt: &'runtime PromptFile,
    log: &'runtime ProjectLog,
    clock: &'runtime dyn WallClock,
    boundary_delay: Duration,
    fault: Option<EchoFaultPlan>,
}

impl<'runtime> EchoAgent<'runtime> {
    #[must_use]
    pub fn new(
        gateway: &'runtime Gateway<'runtime>,
        prompt: &'runtime PromptFile,
        log: &'runtime ProjectLog,
        clock: &'runtime dyn WallClock,
    ) -> Self {
        Self {
            gateway,
            prompt,
            log,
            clock,
            boundary_delay: Duration::ZERO,
            fault: None,
        }
    }

    #[must_use]
    pub fn with_boundary_delay_ms(mut self, delay_ms: u64) -> Self {
        self.boundary_delay = Duration::from_millis(delay_ms);
        self
    }

    #[must_use]
    pub fn with_fault(mut self, fault: Option<EchoFaultPlan>) -> Self {
        self.fault = fault;
        self
    }

    /// Advances an Echo Run to a terminal, paused, or canceled state.
    pub fn run(&self, harness: &RunHarness<'_>, run_id: RunId) -> Result<RunState, EchoError> {
        if self.prompt.tier != "fast" {
            return Err(EchoError::InvalidPromptTier {
                tier: self.prompt.tier.clone(),
            });
        }
        loop {
            let state = harness
                .state(run_id)?
                .ok_or(EchoError::MissingValidation { run_id })?;
            if state.status.is_terminal() || state.status == RunStatus::Paused {
                return Ok(state);
            }
            if state.checkpointed_step_count >= ECHO_STEP_COUNT {
                let validation = state
                    .validation
                    .ok_or(EchoError::MissingValidation { run_id })?;
                let outcome = if validation.status == RunValidationStatus::Passed {
                    RunOutcome::Completed
                } else {
                    RunOutcome::Failed
                };
                return harness
                    .finish(run_id, outcome, Actor::Agent(run_id))
                    .map_err(EchoError::from);
            }
            self.advance_step(harness, &state)?;
        }
    }

    fn advance_step(&self, harness: &RunHarness<'_>, state: &RunState) -> Result<(), EchoError> {
        let plan = plan(state.run_id, state.checkpointed_step_count)?;
        let call = match harness.prepare_step(state.run_id, &plan, None)? {
            StepPreparation::Effect(call) => call,
            StepPreparation::Paused(_) | StepPreparation::ControlApplied(_) => return Ok(()),
            StepPreparation::ReconciliationRequired {
                run_id, step_index, ..
            } => {
                return Err(EchoError::ReconciliationRequired { run_id, step_index });
            }
        };
        if let Some(fault) = &self.fault {
            fault.reach(EchoFaultPoint::AfterCommit(call.step_index()))?;
        }
        if !self.boundary_delay.is_zero() {
            std::thread::sleep(self.boundary_delay);
        }
        let report = self.apply_effect(state, &call)?;
        harness.record_effect(&call, &report)?;
        if let Some(fault) = &self.fault {
            fault.reach(EchoFaultPoint::AfterCheckpoint(call.step_index()))?;
        }
        Ok(())
    }

    fn apply_effect(
        &self,
        state: &RunState,
        call: &crate::AuthorizedToolCall,
    ) -> Result<ToolEffectReport, EchoError> {
        let started = self.clock.now_ms();
        let mut report = match call.tool_id().as_str() {
            ECHO_PREFLIGHT_TOOL_ID => self.preflight_effect(call),
            ECHO_MODEL_TOOL_ID => self.model_effect(state, call),
            ECHO_REPORT_TOOL_ID => self.report_effect(state, call),
            _ => Err(EchoError::Registry(ToolRegistryError::InvalidToolId {
                value: call.tool_id().as_str().to_owned(),
            })),
        }?;
        report.spent.wall_ms = self.clock.now_ms().saturating_sub(started);
        Ok(report)
    }

    fn preflight_effect(
        &self,
        call: &crate::AuthorizedToolCall,
    ) -> Result<ToolEffectReport, EchoError> {
        let preflight = self.gateway.preflight(RoutingTier::Fast);
        if preflight.policy != "local_only" || preflight.endpoint_locality != "device_local" {
            return Err(EchoError::Gateway(Weather::PolicyViolation {
                policy: preflight.policy.to_owned(),
                requested: format!("Echo fast tier at {}", preflight.endpoint_locality),
            }));
        }
        let output = format!(
            "{}|{}|{}|{}",
            preflight.policy, preflight.endpoint_locality, preflight.provider, preflight.model
        );
        Ok(effect_report(
            call,
            output.as_bytes(),
            RunUsage::default(),
            None,
            None,
        ))
    }

    fn model_effect(
        &self,
        state: &RunState,
        call: &crate::AuthorizedToolCall,
    ) -> Result<ToolEffectReport, EchoError> {
        let marker = echo_marker(state.run_id);
        let expected = format!("ECHO: {marker}");
        let request = CompletionRequest {
            model: self.gateway.choice(RoutingTier::Fast).model.clone(),
            system: Some(self.prompt.body.clone()),
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: marker,
            }],
            tools_json: None,
            reasoning_effort: Some(ReasoningEffort::Disabled),
            max_output_tokens: ECHO_MAX_OUTPUT_TOKENS,
            timeout_ms: ECHO_CALL_TIMEOUT_MS,
        };
        let attribution = CallAttribution {
            project: self.log.store().manifest().project_id,
            feature: ECHO_AGENT_NAME.to_owned(),
            agent: Some(ECHO_AGENT_NAME.to_owned()),
        };
        let mut sink = VecSink::default();
        let usage = self
            .gateway
            .complete(RoutingTier::Fast, &attribution, &request, &mut sink)?;
        let output = sink.text();
        if output.len() > ECHO_OUTPUT_BYTES_MAX {
            return Err(EchoError::OutputTooLarge {
                bytes: output.len(),
            });
        }
        let output_bytes = u64::try_from(output.len()).map_err(|_| EchoError::OutputTooLarge {
            bytes: output.len(),
        })?;
        let content_hash = self.log.store().blobs().write_bytes(output.as_bytes())?;
        let validation_status = if output.contains(&expected) {
            RunValidationStatus::Passed
        } else {
            RunValidationStatus::Failed
        };
        let artifact = ArtifactReport {
            artifact_id: derived_artifact_id(call.call_id()),
            content_hash: content_hash.into_bytes(),
            media_type: "text/plain; charset=utf-8".to_owned(),
            size_bytes: output_bytes,
        };
        let validation = ValidationReport {
            validation_id: derived_validation_id(call.call_id()),
            status: validation_status,
            summary: if validation_status == RunValidationStatus::Passed {
                "Echo output contains the exact marker".to_owned()
            } else {
                "Echo output omitted the exact marker".to_owned()
            },
        };
        Ok(effect_report(
            call,
            output.as_bytes(),
            RunUsage {
                tokens: usage.tokens_in.saturating_add(usage.tokens_out),
                storage_bytes: output_bytes,
                ..RunUsage::default()
            },
            Some(artifact),
            Some(validation),
        ))
    }

    fn report_effect(
        &self,
        state: &RunState,
        call: &crate::AuthorizedToolCall,
    ) -> Result<ToolEffectReport, EchoError> {
        let validation = state.validation.ok_or(EchoError::MissingValidation {
            run_id: state.run_id,
        })?;
        Ok(effect_report(
            call,
            validation.status.as_str().as_bytes(),
            RunUsage::default(),
            None,
            None,
        ))
    }
}

#[must_use]
pub fn echo_marker(run_id: RunId) -> String {
    format!("PROJECTOS-ECHO-{}", run_id.to_hex())
}

pub fn echo_tool_registry() -> Result<ToolRegistry, ToolRegistryError> {
    ToolRegistry::new(vec![
        descriptor(
            ECHO_PREFLIGHT_TOOL_ID,
            crate::CapabilityScope::ReadOperations,
            ToolEffectClass::ReadOnly,
            128,
        )?,
        descriptor(
            ECHO_MODEL_TOOL_ID,
            crate::CapabilityScope::ExecLocal,
            ToolEffectClass::Idempotent,
            128,
        )?,
        descriptor(
            ECHO_REPORT_TOOL_ID,
            crate::CapabilityScope::ReadOperations,
            ToolEffectClass::ReadOnly,
            128,
        )?,
    ])
}

pub fn echo_tool_grants() -> Result<RunToolGrants, ToolRegistryError> {
    RunToolGrants::new(
        [
            ECHO_PREFLIGHT_TOOL_ID,
            ECHO_MODEL_TOOL_ID,
            ECHO_REPORT_TOOL_ID,
        ]
        .into_iter()
        .map(|id| ToolId::new(id).map(|id| (id, ToolGrantMode::Allow)))
        .collect::<Result<Vec<_>, _>>()?,
    )
}

fn descriptor(
    id: &'static str,
    scope: crate::CapabilityScope,
    effect: ToolEffectClass,
    max_input_bytes: u32,
) -> Result<ToolDescriptor, ToolRegistryError> {
    ToolDescriptor::new(
        ToolId::new(id)?,
        1,
        scope,
        ToolPolicyMode::Allow,
        effect,
        max_input_bytes,
    )
}

fn plan(run_id: RunId, step_index: u32) -> Result<StepPlan, EchoError> {
    let marker = echo_marker(run_id);
    let (phase, summary, tool_id, token_reservation, storage_reservation) = match step_index {
        0 => (
            RunStepPhase::Preflight,
            "Echo preflight checks the local-only fast tier",
            ECHO_PREFLIGHT_TOOL_ID,
            0,
            0,
        ),
        1 => (
            RunStepPhase::Tool,
            "Echo makes one fast-tier model call",
            ECHO_MODEL_TOOL_ID,
            ECHO_MODEL_TOKEN_RESERVATION,
            ECHO_OUTPUT_BYTES_BUDGET,
        ),
        2 => (
            RunStepPhase::Report,
            "Echo reports its durable validation",
            ECHO_REPORT_TOOL_ID,
            0,
            0,
        ),
        _ => {
            return Err(EchoError::MissingValidation { run_id });
        }
    };
    let input = marker.into_bytes();
    let mut digest_input = summary.as_bytes().to_vec();
    digest_input.extend_from_slice(&input);
    Ok(StepPlan {
        step_index,
        phase,
        summary: summary.to_owned(),
        digest: BlobHash::of_bytes(&digest_input).into_bytes(),
        tool_call: ToolCallRequest {
            tool_id: ToolId::new(tool_id)?,
            call_id: derived_call_id(run_id, step_index),
            input,
        },
        reserved: RunUsage {
            tokens: token_reservation,
            wall_ms: u64::from(ECHO_CALL_TIMEOUT_MS),
            storage_bytes: storage_reservation,
            tool_calls: 1,
            steps: 1,
            ..RunUsage::default()
        },
    })
}

fn effect_report(
    call: &crate::AuthorizedToolCall,
    output: &[u8],
    usage: RunUsage,
    artifact: Option<ArtifactReport>,
    validation: Option<ValidationReport>,
) -> ToolEffectReport {
    let output_digest = BlobHash::of_bytes(output).into_bytes();
    let mut checkpoint_input = call.idempotency_key().as_bytes().to_vec();
    checkpoint_input.extend_from_slice(&output_digest);
    ToolEffectReport {
        output_digest,
        checkpoint_digest: BlobHash::of_bytes(&checkpoint_input).into_bytes(),
        spent: RunUsage {
            tool_calls: 1,
            steps: 1,
            ..usage
        },
        artifact,
        validation,
    }
}

fn derived_call_id(run_id: RunId, step_index: u32) -> ToolCallId {
    let mut bytes = run_id.into_bytes();
    bytes[0] ^= 0xe1;
    bytes[12..].copy_from_slice(&step_index.to_be_bytes());
    ToolCallId::from_bytes(bytes)
}

fn derived_artifact_id(call_id: ToolCallId) -> ArtifactId {
    let mut bytes = call_id.into_bytes();
    bytes[0] ^= 0xa7;
    ArtifactId::from_bytes(bytes)
}

fn derived_validation_id(call_id: ToolCallId) -> ValidationId {
    let mut bytes = call_id.into_bytes();
    bytes[0] ^= 0x5d;
    ValidationId::from_bytes(bytes)
}

#[must_use]
pub const fn echo_charter() -> RosterCharter {
    RosterCharter::Echo
}
