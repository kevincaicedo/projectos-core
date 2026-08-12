//! Durable ProjectOS Run harness (F49, L5–L9).
//!
//! ## Invariant inventory
//!
//! - A Run has at most one committed, uncheckpointed step. Its index equals
//!   `checkpointed_step_count`; committed and checkpointed counts differ by
//!   only zero or one.
//! - Only [`AuthorizedToolCall`] crosses the effect boundary, and the step
//!   carrying its stable idempotency key is durable first (L5/L7).
//! - Effect receipt, optional artifact/validation, and checkpoint commit in
//!   one batch. A crash can expose intent without receipt, never receipt
//!   without checkpoint.
//! - Idempotent effects may be retried with the identical key. A possibly
//!   applied non-idempotent effect parks for human reconciliation instead of
//!   claiming exactly-once behavior.
//! - Budget admission happens before the step fact. Exhaustion appends a
//!   typed pause and applies no partial effect (L8).
//! - Pause/cancel requests may race an effect, but become effective only
//!   after the outstanding receipt + checkpoint boundary.
//! - Every optimistic transition compares the log head inside the writer
//!   transaction; on conflict the harness reloads projections and retries a
//!   fixed number of times.

use crate::budget::{
    BudgetConfigError, BudgetExceeded, admit, reservation_exceeded_dimension,
    usage_within_reservation, validate_budget,
};
use crate::roster::{RosterCharter, RosterRegistry};
use crate::runtime::{RuntimeId, RuntimeRegistry, RuntimeRegistryError};
use crate::tools::{
    AuthorizationContext, AuthorizationError, AuthorizedToolCall, AutonomyLevel, GateReceipt,
    RunToolGrants, ToolCallRequest, ToolEffectClass, ToolRegistry, ToolRegistryError,
};
use pos_domain::{
    DomainEvent, PendingRunControl, RunArtifactRecordedBody, RunBudget, RunCancelRequestedBody,
    RunCanceledBody, RunCheckpointRef, RunCheckpointSavedBody, RunExecutionLeaseRef, RunExecutor,
    RunFinishedBody, RunGateApprovedBody, RunOutcome, RunPauseCause, RunPauseRequestedBody,
    RunPausedBody, RunQuestionAnsweredBody, RunQuestionAskedBody, RunReadError, RunResumedBody,
    RunState, RunStatus, RunStepCommittedBody, RunStepPhase, RunStepState, RunTaintRaisedBody,
    RunToolEffectRecordedBody, RunTrigger, RunUsage, RunValidationRecordedBody, RunValidationRef,
    RunValidationStatus, read_run, read_run_step,
};
use pos_foundation::{
    ArtifactId, CheckpointId, DeviceId, QuestionId, RunId, ToolCallId, UserId, ValidationId,
    WallClock,
};
use pos_log::{Actor, LogError, ProjectLog};
use std::fmt;

/// Head races should resolve in one retry under the single writer. Four lets
/// a control burst settle while bounding contention work (L8).
const CONDITIONAL_APPEND_RETRY_MAX: u8 = 4;
const RUN_TEXT_LEN_MAX: usize = 512;
const MEDIA_TYPE_LEN_MAX: usize = 128;
/// A supervisor, worker, verifier, and a few bounded follow-ups fit well
/// inside eight edges; deeper chains are reaction storms, not delegation.
pub const RUN_LINEAGE_DEPTH_MAX: u8 = 8;

#[derive(Clone, Debug)]
pub struct RunStartSpec {
    pub run_id: RunId,
    pub worker: RosterCharter,
    pub runtime_id: RuntimeId,
    pub executor: RunExecutor,
    pub trigger: RunTrigger,
    pub autonomy_level: AutonomyLevel,
    pub budget: RunBudget,
    pub tool_grants: RunToolGrants,
    pub parent_run_id: Option<RunId>,
    pub checkpoint: Option<RunCheckpointRef>,
    pub validation: Option<RunValidationRef>,
    pub execution_lease: Option<RunExecutionLeaseRef>,
    pub tainted: bool,
}

#[derive(Clone, Debug)]
pub struct StepPlan {
    pub step_index: u32,
    pub phase: RunStepPhase,
    pub summary: String,
    pub digest: [u8; 32],
    pub tool_call: ToolCallRequest,
    pub reserved: RunUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ArtifactReport {
    pub artifact_id: ArtifactId,
    pub content_hash: [u8; 32],
    pub media_type: String,
    pub size_bytes: u64,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ValidationReport {
    pub validation_id: ValidationId,
    pub status: RunValidationStatus,
    pub summary: String,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ToolEffectReport {
    pub output_digest: [u8; 32],
    pub checkpoint_digest: [u8; 32],
    pub spent: RunUsage,
    pub artifact: Option<ArtifactReport>,
    pub validation: Option<ValidationReport>,
}

#[derive(Debug)]
pub enum StepPreparation {
    Effect(AuthorizedToolCall),
    Paused(BudgetExceeded),
    ControlApplied(RunStatus),
    ReconciliationRequired {
        run_id: RunId,
        step_index: u32,
        call_id: ToolCallId,
    },
}

#[derive(Debug)]
pub enum HarnessError {
    Log(LogError),
    Read(RunReadError),
    BudgetConfig(BudgetConfigError),
    Runtime(RuntimeRegistryError),
    ToolRegistry(ToolRegistryError),
    Authorization(AuthorizationError),
    RunNotFound { run_id: RunId },
    InvalidRunState { run_id: RunId, reason: String },
    InvalidStepPlan { reason: String },
    StepPlanChanged { run_id: RunId, step_index: u32 },
    EffectReportChanged { run_id: RunId, step_index: u32 },
    ConditionalAppendContention { run_id: RunId },
    InvalidControlReason,
    InvalidQuestion,
    InvalidArtifact,
    InvalidValidation,
}

impl fmt::Display for HarnessError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Log(source) => write!(formatter, "{source}"),
            Self::Read(source) => write!(formatter, "{source}"),
            Self::BudgetConfig(source) => write!(formatter, "{source}"),
            Self::Runtime(source) => write!(formatter, "{source}"),
            Self::ToolRegistry(source) => write!(formatter, "{source}"),
            Self::Authorization(source) => write!(formatter, "{source}"),
            Self::RunNotFound { run_id } => {
                write!(formatter, "Run {} does not exist", run_id.to_hex())
            }
            Self::InvalidRunState { run_id, reason } => {
                write!(
                    formatter,
                    "Run {} has invalid state: {reason}",
                    run_id.to_hex()
                )
            }
            Self::InvalidStepPlan { reason } => write!(formatter, "invalid step plan: {reason}"),
            Self::StepPlanChanged { run_id, step_index } => write!(
                formatter,
                "Run {} step {step_index} differs from its durable committed intent",
                run_id.to_hex()
            ),
            Self::EffectReportChanged { run_id, step_index } => write!(
                formatter,
                "Run {} step {step_index} already recorded a different effect report",
                run_id.to_hex()
            ),
            Self::ConditionalAppendContention { run_id } => write!(
                formatter,
                "Run {} changed during {CONDITIONAL_APPEND_RETRY_MAX} conditional append attempts",
                run_id.to_hex()
            ),
            Self::InvalidControlReason => write!(
                formatter,
                "control reason must contain 1..={RUN_TEXT_LEN_MAX} visible characters"
            ),
            Self::InvalidQuestion => write!(
                formatter,
                "question/answer must contain 1..={RUN_TEXT_LEN_MAX} visible characters"
            ),
            Self::InvalidArtifact => {
                write!(formatter, "artifact report violates its stated bounds")
            }
            Self::InvalidValidation => {
                write!(formatter, "validation summary violates its stated bounds")
            }
        }
    }
}

impl std::error::Error for HarnessError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Log(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::BudgetConfig(source) => Some(source),
            Self::Runtime(source) => Some(source),
            Self::ToolRegistry(source) => Some(source),
            Self::Authorization(source) => Some(source),
            _ => None,
        }
    }
}

impl From<LogError> for HarnessError {
    fn from(source: LogError) -> Self {
        Self::Log(source)
    }
}

impl From<RunReadError> for HarnessError {
    fn from(source: RunReadError) -> Self {
        Self::Read(source)
    }
}

impl From<BudgetConfigError> for HarnessError {
    fn from(source: BudgetConfigError) -> Self {
        Self::BudgetConfig(source)
    }
}

impl From<RuntimeRegistryError> for HarnessError {
    fn from(source: RuntimeRegistryError) -> Self {
        Self::Runtime(source)
    }
}

impl From<ToolRegistryError> for HarnessError {
    fn from(source: ToolRegistryError) -> Self {
        Self::ToolRegistry(source)
    }
}

impl From<AuthorizationError> for HarnessError {
    fn from(source: AuthorizationError) -> Self {
        Self::Authorization(source)
    }
}

pub struct RunHarness<'a> {
    log: &'a ProjectLog,
    clock: &'a dyn WallClock,
    device: DeviceId,
    tools: &'a ToolRegistry,
    runtimes: &'a RuntimeRegistry,
    roster: &'a RosterRegistry,
}

impl<'a> RunHarness<'a> {
    #[must_use]
    pub fn new(
        log: &'a ProjectLog,
        clock: &'a dyn WallClock,
        device: DeviceId,
        tools: &'a ToolRegistry,
        runtimes: &'a RuntimeRegistry,
        roster: &'a RosterRegistry,
    ) -> Self {
        Self {
            log,
            clock,
            device,
            tools,
            runtimes,
            roster,
        }
    }

    pub fn start(&self, spec: &RunStartSpec, actor: Actor) -> Result<RunState, HarnessError> {
        validate_budget(spec.budget)?;
        if !self.roster.contains(spec.worker) {
            return Err(HarnessError::InvalidRunState {
                run_id: spec.run_id,
                reason: "worker charter is not in the foundation roster".to_owned(),
            });
        }
        let runtime = self.runtimes.resolve(&spec.runtime_id, spec.executor)?;
        let lineage_depth = self.lineage_depth(spec)?;
        let event = DomainEvent::RunStarted(pos_domain::RunStartedBody::V2 {
            run_id: spec.run_id,
            project_id: self.log.store().manifest().project_id,
            worker: spec.worker.as_str().to_owned(),
            runtime: runtime.reference(),
            executor: spec.executor,
            trigger: spec.trigger,
            autonomy_level: spec.autonomy_level.value(),
            budget: spec.budget,
            tool_grants: spec.tool_grants.domain_grants(),
            parent_run_id: spec.parent_run_id,
            lineage_depth,
            checkpoint: spec.checkpoint,
            validation: spec.validation,
            execution_lease: spec.execution_lease,
            tainted: spec.tainted,
        });
        let head = self.log.head()?;
        self.log
            .append_at_head(head, event.into_request(self.device, actor)?, self.clock)?;
        self.state_required(spec.run_id)
    }

    pub fn state(&self, run_id: RunId) -> Result<Option<RunState>, HarnessError> {
        read_run(self.log, run_id).map_err(HarnessError::from)
    }

    pub fn prepare_step(
        &self,
        run_id: RunId,
        plan: &StepPlan,
        receipt: Option<&GateReceipt>,
    ) -> Result<StepPreparation, HarnessError> {
        validate_step_plan(plan)?;
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            validate_run_counts(&state)?;
            if let Some(control) = state.pending_control
                && state.committed_step_count == state.checkpointed_step_count
            {
                if self.settle_control_once(&state, control)? {
                    let status = self.state_required(run_id)?.status;
                    return Ok(StepPreparation::ControlApplied(status));
                }
                continue;
            }
            require_running(&state)?;
            if state.committed_step_count > state.checkpointed_step_count {
                return self.resume_committed_step(&state, plan);
            }
            if plan.step_index != state.committed_step_count {
                return Err(HarnessError::InvalidStepPlan {
                    reason: format!(
                        "step index {} is not the next durable index {}",
                        plan.step_index, state.committed_step_count
                    ),
                });
            }
            if let Err(exceeded) = admit(
                state.budget,
                state.spent,
                RunUsage::default(),
                plan.reserved,
            ) {
                if self.append_budget_pause(&state, exceeded)? {
                    return Ok(StepPreparation::Paused(exceeded));
                }
                continue;
            }
            let grants = RunToolGrants::from_domain(&state.tool_grants)?;
            let authorized = self.tools.authorize(
                plan.tool_call.clone(),
                AuthorizationContext {
                    run_id,
                    step_index: plan.step_index,
                    tainted: state.tainted,
                    autonomy_level: AutonomyLevel::new(state.autonomy_level).map_err(|error| {
                        HarnessError::InvalidRunState {
                            run_id,
                            reason: error.to_string(),
                        }
                    })?,
                    grants: &grants,
                    receipt,
                    now_ts_ms: self.clock.now_ms(),
                },
            )?;
            if self.append_step(&state, plan, &authorized, receipt)? {
                return Ok(StepPreparation::Effect(authorized));
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    pub fn record_effect(
        &self,
        call: &AuthorizedToolCall,
        report: &ToolEffectReport,
    ) -> Result<pos_domain::RunCheckpointState, HarnessError> {
        validate_effect_report(report)?;
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(call.run_id())?;
            let step = self.step_required(call.run_id(), call.step_index())?;
            validate_effect_target(call, &step)?;
            if let Some(checkpoint) = &step.checkpoint {
                validate_existing_effect(call, report, &step)?;
                return Ok(checkpoint.clone());
            }
            if step.effect.is_some() {
                return Err(HarnessError::InvalidRunState {
                    run_id: call.run_id(),
                    reason: "effect receipt exists without its atomic checkpoint".to_owned(),
                });
            }
            let events = effect_events(call, report, &state, &step)?;
            let head = self.log.head()?;
            let requests = events
                .into_iter()
                .map(|event| event.into_request(self.device, Actor::Agent(call.run_id())))
                .collect::<Result<Vec<_>, _>>()?;
            match self.log.append_batch_at_head(head, &requests, self.clock) {
                Ok(_) => {
                    let checkpoint = self
                        .step_required(call.run_id(), call.step_index())?
                        .checkpoint
                        .ok_or_else(|| HarnessError::InvalidRunState {
                            run_id: call.run_id(),
                            reason: "effect batch committed without a checkpoint projection"
                                .to_owned(),
                        })?;
                    self.settle_after_checkpoint(call.run_id())?;
                    return Ok(checkpoint);
                }
                Err(LogError::HeadChanged { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HarnessError::ConditionalAppendContention {
            run_id: call.run_id(),
        })
    }

    pub fn request_pause(
        &self,
        run_id: RunId,
        reason: &str,
        actor: Actor,
    ) -> Result<RunState, HarnessError> {
        self.request_control(run_id, reason, actor, PendingRunControl::Pause)
    }

    pub fn request_cancel(
        &self,
        run_id: RunId,
        reason: &str,
        actor: Actor,
    ) -> Result<RunState, HarnessError> {
        self.request_control(run_id, reason, actor, PendingRunControl::Cancel)
    }

    pub fn resume(&self, run_id: RunId, actor: Actor) -> Result<RunState, HarnessError> {
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            if state.status != RunStatus::Paused || state.pending_control.is_some() {
                return Err(HarnessError::InvalidRunState {
                    run_id,
                    reason: "resume requires a fully paused Run with no pending control".to_owned(),
                });
            }
            let event = DomainEvent::RunResumed(RunResumedBody::V1 {
                run_id,
                at_step_index: state.checkpointed_step_count,
            });
            let head = self.log.head()?;
            match self
                .log
                .append_at_head(head, event.into_request(self.device, actor)?, self.clock)
            {
                Ok(_) => return self.state_required(run_id),
                Err(LogError::HeadChanged { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    /// Appends the one terminal fact after every committed step has reached
    /// its checkpoint. A control request racing completion wins at the same
    /// boundary, so a user cancel can never be overwritten by stale success.
    pub fn finish(
        &self,
        run_id: RunId,
        outcome: RunOutcome,
        actor: Actor,
    ) -> Result<RunState, HarnessError> {
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            validate_run_counts(&state)?;
            if state.status.is_terminal() {
                return Ok(state);
            }
            if state.committed_step_count != state.checkpointed_step_count {
                return Err(HarnessError::InvalidRunState {
                    run_id,
                    reason: "finish requires every committed step to be checkpointed".to_owned(),
                });
            }
            if let Some(control) = state.pending_control {
                if self.settle_control_once(&state, control)? {
                    return self.state_required(run_id);
                }
                continue;
            }
            if state.status == RunStatus::Paused {
                return Err(HarnessError::InvalidRunState {
                    run_id,
                    reason: "a paused Run must resume or cancel before it can finish".to_owned(),
                });
            }
            let event = DomainEvent::RunFinished(RunFinishedBody::V2 {
                run_id,
                outcome,
                steps_total: state.checkpointed_step_count,
                spent: state.spent,
                validation: state.validation,
            });
            let head = self.log.head()?;
            match self
                .log
                .append_at_head(head, event.into_request(self.device, actor)?, self.clock)
            {
                Ok(_) => return self.state_required(run_id),
                Err(LogError::HeadChanged { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    pub fn raise_taint(&self, run_id: RunId, source: &str) -> Result<RunState, HarnessError> {
        validate_text(source).map_err(|_| HarnessError::InvalidQuestion)?;
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            if state.tainted {
                return Ok(state);
            }
            let event = DomainEvent::RunTaintRaised(RunTaintRaisedBody::V1 {
                run_id,
                source: source.to_owned(),
            });
            let head = self.log.head()?;
            match self.log.append_at_head(
                head,
                event.into_request(self.device, Actor::Agent(run_id))?,
                self.clock,
            ) {
                Ok(_) => return self.state_required(run_id),
                Err(LogError::HeadChanged { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    pub fn ask_question(
        &self,
        run_id: RunId,
        question_id: QuestionId,
        prompt: String,
    ) -> Result<RunState, HarnessError> {
        validate_text(&prompt).map_err(|_| HarnessError::InvalidQuestion)?;
        let state = self.state_required(run_id)?;
        require_running(&state)?;
        if state.committed_step_count != state.checkpointed_step_count {
            return Err(HarnessError::InvalidRunState {
                run_id,
                reason: "questions land only at a completed step boundary".to_owned(),
            });
        }
        let event = DomainEvent::RunQuestionAsked(RunQuestionAskedBody::V1 {
            run_id,
            question_id,
            prompt,
        });
        let head = self.log.head()?;
        self.log.append_at_head(
            head,
            event.into_request(self.device, Actor::Agent(run_id))?,
            self.clock,
        )?;
        self.state_required(run_id)
    }

    pub fn answer_question(
        &self,
        run_id: RunId,
        question_id: QuestionId,
        answered_by: UserId,
        answer: String,
    ) -> Result<RunState, HarnessError> {
        validate_text(&answer).map_err(|_| HarnessError::InvalidQuestion)?;
        let state = self.state_required(run_id)?;
        if state.status != RunStatus::WaitingInput {
            return Err(HarnessError::InvalidRunState {
                run_id,
                reason: "answer requires a Run waiting for input".to_owned(),
            });
        }
        let event = DomainEvent::RunQuestionAnswered(RunQuestionAnsweredBody::V1 {
            run_id,
            question_id,
            answered_by,
            answer,
        });
        let head = self.log.head()?;
        self.log.append_at_head(
            head,
            event.into_request(self.device, Actor::User(answered_by))?,
            self.clock,
        )?;
        self.state_required(run_id)
    }

    fn state_required(&self, run_id: RunId) -> Result<RunState, HarnessError> {
        self.state(run_id)?
            .ok_or(HarnessError::RunNotFound { run_id })
    }

    fn lineage_depth(&self, spec: &RunStartSpec) -> Result<u8, HarnessError> {
        let Some(parent_run_id) = spec.parent_run_id else {
            return Ok(0);
        };
        if parent_run_id == spec.run_id {
            return Err(HarnessError::InvalidRunState {
                run_id: spec.run_id,
                reason: "a Run cannot be its own parent".to_owned(),
            });
        }
        let parent = self.state_required(parent_run_id)?;
        let depth =
            parent
                .lineage_depth
                .checked_add(1)
                .ok_or_else(|| HarnessError::InvalidRunState {
                    run_id: spec.run_id,
                    reason: "parent lineage depth overflowed".to_owned(),
                })?;
        if depth > RUN_LINEAGE_DEPTH_MAX {
            return Err(HarnessError::InvalidRunState {
                run_id: spec.run_id,
                reason: format!(
                    "lineage depth {depth} exceeds the {RUN_LINEAGE_DEPTH_MAX}-edge cap"
                ),
            });
        }
        Ok(depth)
    }

    fn step_required(&self, run_id: RunId, step_index: u32) -> Result<RunStepState, HarnessError> {
        read_run_step(self.log, run_id, step_index)?.ok_or_else(|| HarnessError::InvalidRunState {
            run_id,
            reason: format!("committed step {step_index} is absent from its projection"),
        })
    }

    fn resume_committed_step(
        &self,
        state: &RunState,
        plan: &StepPlan,
    ) -> Result<StepPreparation, HarnessError> {
        let step_index = state.checkpointed_step_count;
        let step = self.step_required(state.run_id, step_index)?;
        validate_durable_plan(state.run_id, plan, &step)?;
        if step.effect.is_some() || step.checkpoint.is_some() {
            return Err(HarnessError::InvalidRunState {
                run_id: state.run_id,
                reason: "outstanding-step counters disagree with receipt/checkpoint columns"
                    .to_owned(),
            });
        }
        let domain_call = step
            .tool_call
            .as_ref()
            .ok_or_else(|| HarnessError::InvalidRunState {
                run_id: state.run_id,
                reason: "m0-s12 committed step has no tool call".to_owned(),
            })?;
        let call = self
            .tools
            .rehydrate(state.run_id, step_index, domain_call)?;
        if call.effect_class() == ToolEffectClass::NonIdempotent {
            return Ok(StepPreparation::ReconciliationRequired {
                run_id: state.run_id,
                step_index,
                call_id: call.call_id(),
            });
        }
        Ok(StepPreparation::Effect(call))
    }

    fn append_step(
        &self,
        state: &RunState,
        plan: &StepPlan,
        call: &AuthorizedToolCall,
        receipt: Option<&GateReceipt>,
    ) -> Result<bool, HarnessError> {
        let mut events = Vec::with_capacity(if receipt.is_some() { 2 } else { 1 });
        if let Some(receipt) = receipt {
            events.push(DomainEvent::RunGateApproved(RunGateApprovedBody::V1 {
                run_id: state.run_id,
                receipt_id: receipt.receipt_id,
                call_id: receipt.call_id,
                approved_by: receipt.approved_by,
                reason: receipt.reason.clone(),
                expires_ts_ms: receipt.expires_ts_ms,
            }));
        }
        events.push(DomainEvent::RunStepCommitted(RunStepCommittedBody::V2 {
            run_id: state.run_id,
            step_index: plan.step_index,
            phase: plan.phase,
            summary: plan.summary.clone(),
            digest: plan.digest,
            tool_call: Some(call.domain_call()),
            reserved: plan.reserved,
        }));
        let requests = events
            .into_iter()
            .map(|event| event.into_request(self.device, Actor::Agent(state.run_id)))
            .collect::<Result<Vec<_>, _>>()?;
        let head = self.log.head()?;
        match self.log.append_batch_at_head(head, &requests, self.clock) {
            Ok(_) => Ok(true),
            Err(LogError::HeadChanged { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn append_budget_pause(
        &self,
        state: &RunState,
        exceeded: BudgetExceeded,
    ) -> Result<bool, HarnessError> {
        let event = DomainEvent::RunPaused(RunPausedBody::V1 {
            run_id: state.run_id,
            at_step_index: state.checkpointed_step_count,
            cause: RunPauseCause::Budget {
                dimension: exceeded.dimension,
                limit: exceeded.limit,
                spent: exceeded.spent,
                pending: exceeded.pending,
                requested: exceeded.requested,
            },
            spent: state.spent,
        });
        let head = self.log.head()?;
        match self.log.append_at_head(
            head,
            event.into_request(self.device, Actor::Agent(state.run_id))?,
            self.clock,
        ) {
            Ok(_) => Ok(true),
            Err(LogError::HeadChanged { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }

    fn request_control(
        &self,
        run_id: RunId,
        reason: &str,
        actor: Actor,
        control: PendingRunControl,
    ) -> Result<RunState, HarnessError> {
        validate_text(reason).map_err(|_| HarnessError::InvalidControlReason)?;
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            if state.status.is_terminal() {
                return Err(HarnessError::InvalidRunState {
                    run_id,
                    reason: "terminal Runs reject new controls".to_owned(),
                });
            }
            if state.status == RunStatus::Paused && control == PendingRunControl::Pause {
                return Ok(state);
            }
            if state.pending_control == Some(control) {
                return Ok(state);
            }
            if state.pending_control.is_some() {
                return Err(HarnessError::InvalidRunState {
                    run_id,
                    reason: "a different control is already pending".to_owned(),
                });
            }
            let event = match control {
                PendingRunControl::Pause => {
                    DomainEvent::RunPauseRequested(RunPauseRequestedBody::V1 {
                        run_id,
                        reason: reason.to_owned(),
                    })
                }
                PendingRunControl::Cancel => {
                    DomainEvent::RunCancelRequested(RunCancelRequestedBody::V1 {
                        run_id,
                        reason: reason.to_owned(),
                    })
                }
            };
            let head = self.log.head()?;
            match self
                .log
                .append_at_head(head, event.into_request(self.device, actor)?, self.clock)
            {
                Ok(_) => {
                    let requested = self.state_required(run_id)?;
                    if requested.committed_step_count == requested.checkpointed_step_count {
                        self.settle_after_checkpoint(run_id)?;
                    }
                    return self.state_required(run_id);
                }
                Err(LogError::HeadChanged { .. }) => continue,
                Err(error) => return Err(error.into()),
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    fn settle_after_checkpoint(&self, run_id: RunId) -> Result<(), HarnessError> {
        for _ in 0..CONDITIONAL_APPEND_RETRY_MAX {
            let state = self.state_required(run_id)?;
            let Some(control) = state.pending_control else {
                return Ok(());
            };
            if self.settle_control_once(&state, control)? {
                return Ok(());
            }
        }
        Err(HarnessError::ConditionalAppendContention { run_id })
    }

    fn settle_control_once(
        &self,
        state: &RunState,
        control: PendingRunControl,
    ) -> Result<bool, HarnessError> {
        if state.committed_step_count != state.checkpointed_step_count {
            return Ok(false);
        }
        let events = match control {
            PendingRunControl::Pause => vec![DomainEvent::RunPaused(RunPausedBody::V1 {
                run_id: state.run_id,
                at_step_index: state.checkpointed_step_count,
                cause: RunPauseCause::Requested {
                    reason: state
                        .pending_control_reason
                        .clone()
                        .unwrap_or_else(|| "pause requested".to_owned()),
                },
                spent: state.spent,
            })],
            PendingRunControl::Cancel => vec![
                DomainEvent::RunCanceled(RunCanceledBody::V1 {
                    run_id: state.run_id,
                    at_step_index: state.checkpointed_step_count,
                }),
                DomainEvent::RunFinished(RunFinishedBody::V2 {
                    run_id: state.run_id,
                    outcome: RunOutcome::Canceled,
                    steps_total: state.checkpointed_step_count,
                    spent: state.spent,
                    validation: None,
                }),
            ],
        };
        let requests = events
            .into_iter()
            .map(|event| event.into_request(self.device, Actor::Agent(state.run_id)))
            .collect::<Result<Vec<_>, _>>()?;
        let head = self.log.head()?;
        match self.log.append_batch_at_head(head, &requests, self.clock) {
            Ok(_) => Ok(true),
            Err(LogError::HeadChanged { .. }) => Ok(false),
            Err(error) => Err(error.into()),
        }
    }
}

fn validate_step_plan(plan: &StepPlan) -> Result<(), HarnessError> {
    validate_text(&plan.summary).map_err(|_| HarnessError::InvalidStepPlan {
        reason: format!("summary must contain 1..={RUN_TEXT_LEN_MAX} visible characters"),
    })?;
    if plan.reserved.steps != 1 || plan.reserved.tool_calls != 1 {
        return Err(HarnessError::InvalidStepPlan {
            reason: "a tool step reserves exactly one step and one tool call".to_owned(),
        });
    }
    Ok(())
}

fn validate_effect_report(report: &ToolEffectReport) -> Result<(), HarnessError> {
    if report.spent.steps != 1 || report.spent.tool_calls != 1 {
        return Err(HarnessError::InvalidStepPlan {
            reason: "a tool effect records exactly one step and one tool call".to_owned(),
        });
    }
    if let Some(artifact) = &report.artifact {
        let valid = !artifact.media_type.is_empty()
            && artifact.media_type.len() <= MEDIA_TYPE_LEN_MAX
            && !artifact.media_type.chars().any(char::is_control);
        if !valid {
            return Err(HarnessError::InvalidArtifact);
        }
    }
    if let Some(validation) = &report.validation {
        validate_text(&validation.summary).map_err(|_| HarnessError::InvalidValidation)?;
    }
    Ok(())
}

fn validate_text(value: &str) -> Result<(), ()> {
    let valid = !value.is_empty()
        && value.len() <= RUN_TEXT_LEN_MAX
        && !value.chars().any(char::is_control);
    if valid { Ok(()) } else { Err(()) }
}

fn validate_run_counts(state: &RunState) -> Result<(), HarnessError> {
    let valid = state.committed_step_count >= state.checkpointed_step_count
        && state.committed_step_count - state.checkpointed_step_count <= 1;
    if valid {
        Ok(())
    } else {
        Err(HarnessError::InvalidRunState {
            run_id: state.run_id,
            reason: format!(
                "committed/checkpointed counts are {}/{}; expected a gap of zero or one",
                state.committed_step_count, state.checkpointed_step_count
            ),
        })
    }
}

fn require_running(state: &RunState) -> Result<(), HarnessError> {
    if matches!(
        state.status,
        RunStatus::Preflight | RunStatus::Running | RunStatus::Validating
    ) {
        Ok(())
    } else {
        Err(HarnessError::InvalidRunState {
            run_id: state.run_id,
            reason: format!(
                "step preparation requires running, found {:?}",
                state.status
            ),
        })
    }
}

fn validate_durable_plan(
    run_id: RunId,
    plan: &StepPlan,
    step: &RunStepState,
) -> Result<(), HarnessError> {
    let Some(call) = &step.tool_call else {
        return Err(HarnessError::StepPlanChanged {
            run_id,
            step_index: step.step_index,
        });
    };
    let same = plan.step_index == step.step_index
        && plan.summary == step.summary
        && plan.phase.as_str() == step.phase
        && step.digest == Some(plan.digest)
        && plan.reserved == step.reserved
        && plan.tool_call.tool_id.as_str() == call.tool_id
        && plan.tool_call.call_id == call.call_id
        && plan.tool_call.input == call.input;
    if same {
        Ok(())
    } else {
        Err(HarnessError::StepPlanChanged {
            run_id,
            step_index: step.step_index,
        })
    }
}

fn validate_effect_target(
    call: &AuthorizedToolCall,
    step: &RunStepState,
) -> Result<(), HarnessError> {
    let Some(durable) = &step.tool_call else {
        return Err(HarnessError::StepPlanChanged {
            run_id: call.run_id(),
            step_index: call.step_index(),
        });
    };
    let same = durable.call_id == call.call_id()
        && durable.idempotency_key == call.idempotency_key()
        && durable.tool_id == call.tool_id().as_str();
    if same {
        Ok(())
    } else {
        Err(HarnessError::StepPlanChanged {
            run_id: call.run_id(),
            step_index: call.step_index(),
        })
    }
}

fn validate_existing_effect(
    call: &AuthorizedToolCall,
    report: &ToolEffectReport,
    step: &RunStepState,
) -> Result<(), HarnessError> {
    let same = step.effect.as_ref().is_some_and(|effect| {
        effect.output_digest == report.output_digest && effect.spent == report.spent
    });
    if same {
        Ok(())
    } else {
        Err(HarnessError::EffectReportChanged {
            run_id: call.run_id(),
            step_index: call.step_index(),
        })
    }
}

fn effect_events(
    call: &AuthorizedToolCall,
    report: &ToolEffectReport,
    state: &RunState,
    step: &RunStepState,
) -> Result<Vec<DomainEvent>, HarnessError> {
    let checkpoint_id = checkpoint_id(call.call_id());
    let mut events = Vec::with_capacity(5);
    events.push(DomainEvent::RunToolEffectRecorded(
        RunToolEffectRecordedBody::V1 {
            run_id: call.run_id(),
            step_index: call.step_index(),
            call_id: call.call_id(),
            idempotency_key: call.idempotency_key().to_owned(),
            output_digest: report.output_digest,
            spent: report.spent,
        },
    ));
    if let Some(artifact) = &report.artifact {
        events.push(DomainEvent::RunArtifactRecorded(
            RunArtifactRecordedBody::V1 {
                run_id: call.run_id(),
                step_index: call.step_index(),
                artifact_id: artifact.artifact_id,
                content_hash: artifact.content_hash,
                media_type: artifact.media_type.clone(),
                size_bytes: artifact.size_bytes,
            },
        ));
    }
    if let Some(validation) = &report.validation {
        events.push(DomainEvent::RunValidationRecorded(
            RunValidationRecordedBody::V1 {
                run_id: call.run_id(),
                step_index: call.step_index(),
                validation_id: validation.validation_id,
                status: validation.status,
                summary: validation.summary.clone(),
            },
        ));
    }
    events.push(DomainEvent::RunCheckpointSaved(
        RunCheckpointSavedBody::V1 {
            run_id: call.run_id(),
            step_index: call.step_index(),
            checkpoint_id,
            state_digest: report.checkpoint_digest,
        },
    ));
    let exceeded_dimension = reservation_exceeded_dimension(report.spent, step.reserved);
    debug_assert_eq!(
        usage_within_reservation(report.spent, step.reserved),
        exceeded_dimension.is_none(),
        "reservation comparison helpers must agree"
    );
    if let Some(dimension) = exceeded_dimension {
        events.push(DomainEvent::RunPaused(RunPausedBody::V1 {
            run_id: call.run_id(),
            at_step_index: call.step_index().saturating_add(1),
            cause: RunPauseCause::ReservationExceeded { dimension },
            spent: saturating_usage_add(state.spent, report.spent),
        }));
    }
    Ok(events)
}

fn checkpoint_id(call_id: ToolCallId) -> CheckpointId {
    let mut bytes = call_id.into_bytes();
    bytes[0] ^= 0xc3;
    CheckpointId::from_bytes(bytes)
}

fn saturating_usage_add(left: RunUsage, right: RunUsage) -> RunUsage {
    RunUsage {
        tokens: left.tokens.saturating_add(right.tokens),
        usd_micros: left.usd_micros.saturating_add(right.usd_micros),
        wall_ms: left.wall_ms.saturating_add(right.wall_ms),
        storage_bytes: left.storage_bytes.saturating_add(right.storage_bytes),
        tool_calls: left.tool_calls.saturating_add(right.tool_calls),
        retries: left.retries.saturating_add(right.retries),
        steps: left.steps.saturating_add(right.steps),
    }
}
