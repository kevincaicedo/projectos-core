//! The v0 `EventKind` vocabulary (m0-s03): past-tense facts with versioned
//! CBOR bodies. Old events are eternal — a field is never removed or
//! re-typed; evolution adds a `V2` variant beside `V1` and the decoder
//! matches both (event-sourcing skill). Unknown kinds decode to `None` so a
//! newer project opens under an older build instead of failing closed.

use crate::ingest::{
    EvidenceAddedBody, EvidenceChunkedBody, EvidenceReprocessRequestedBody,
    EvidenceTranscribedBody, IngestStageFailedBody, IngestStageFinishedBody,
    IngestStageStartedBody, TranscriptSegmentSpeakerSetBody, TranscriptSpeakerNamedBody,
    TranscriptTextCorrectedBody,
};
use pos_foundation::{
    AccountId, ArtifactId, CheckpointId, CronId, ExecutionLeaseId, GateReceiptId, JobId, ProjectId,
    QuestionId, RunId, ToolCallId, UserId, ValidationId,
};
use pos_log::{Actor, AppendRequest, EntityRef, KindTag, LogError};
use serde::{Deserialize, Serialize};
use std::fmt;

/// A typed decode failure: the kind is known but the body does not parse —
/// real corruption or a forward-versioned body, named per seq by callers.
#[derive(Debug)]
pub struct DomainDecodeError {
    pub kind: &'static str,
    pub reason: String,
}

impl fmt::Display for DomainDecodeError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "{} body did not decode: {}",
            self.kind, self.reason
        )
    }
}

impl std::error::Error for DomainDecodeError {}

/// How a Run ended. Stored as text in projections; the words are UI copy.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunOutcome {
    Completed,
    Failed,
    Canceled,
}

impl RunOutcome {
    #[must_use]
    pub const fn as_status_str(self) -> &'static str {
        match self {
            // The V1 event variant is eternal; the current status vocabulary
            // follows master plan §11.2 (`Done`).
            Self::Completed => "done",
            Self::Failed => "failed",
            Self::Canceled => "canceled",
        }
    }
}

/// Where the worker implementation comes from. Runtime selection is kept
/// separate from execution placement and model-provider policy (F49/L9).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunRuntimeKind {
    Native,
    External,
}

impl RunRuntimeKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Native => "native",
            Self::External => "external",
        }
    }
}

/// A versioned runtime-registry reference, not the runtime implementation.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunRuntimeRef {
    pub kind: RunRuntimeKind,
    pub runtime_id: String,
    pub contract_version: u16,
}

/// Execution placement is independent of worker runtime and model provider.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunExecutor {
    Device,
    Cloud,
}

impl RunExecutor {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Device => "device",
            Self::Cloud => "cloud",
        }
    }
}

/// The typed source that requested a Run.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunTrigger {
    User,
    Schedule,
    Subscription,
    ParentRun,
    Retry,
}

impl RunTrigger {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Schedule => "schedule",
            Self::Subscription => "subscription",
            Self::ParentRun => "parent_run",
            Self::Retry => "retry",
        }
    }
}

/// Hard Run limits. Integer units avoid projection drift and make every
/// boundary explicit (L8).
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunBudget {
    pub tokens: u64,
    pub usd_micros: u64,
    pub wall_ms: u64,
    pub storage_bytes: u64,
    pub tool_calls: u32,
    pub retries: u32,
    pub steps: u32,
}

/// Durable actual or reserved usage in the same dimensions as [`RunBudget`].
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunUsage {
    pub tokens: u64,
    pub usd_micros: u64,
    pub wall_ms: u64,
    pub storage_bytes: u64,
    pub tool_calls: u32,
    pub retries: u32,
    pub steps: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunBudgetDimension {
    Tokens,
    UsdMicros,
    WallMs,
    StorageBytes,
    ToolCalls,
    Retries,
    Steps,
}

impl RunBudgetDimension {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Tokens => "tokens",
            Self::UsdMicros => "usd_micros",
            Self::WallMs => "wall_ms",
            Self::StorageBytes => "storage_bytes",
            Self::ToolCalls => "tool_calls",
            Self::Retries => "retries",
            Self::Steps => "steps",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "tokens" => Some(Self::Tokens),
            "usd_micros" => Some(Self::UsdMicros),
            "wall_ms" => Some(Self::WallMs),
            "storage_bytes" => Some(Self::StorageBytes),
            "tool_calls" => Some(Self::ToolCalls),
            "retries" => Some(Self::Retries),
            "steps" => Some(Self::Steps),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStepPhase {
    Preflight,
    Context,
    Tool,
    Validation,
    Report,
}

impl RunStepPhase {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Preflight => "preflight",
            Self::Context => "context",
            Self::Tool => "tool",
            Self::Validation => "validation",
            Self::Report => "report",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunToolCall {
    pub tool_id: String,
    pub descriptor_version: u16,
    pub call_id: ToolCallId,
    pub idempotency_key: String,
    pub input: Vec<u8>,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunToolGrantMode {
    Allow,
    Gate,
    Block,
}

impl RunToolGrantMode {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Allow => "allow",
            Self::Gate => "gate",
            Self::Block => "block",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "allow" => Some(Self::Allow),
            "gate" => Some(Self::Gate),
            "block" => Some(Self::Block),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunToolGrant {
    pub tool_id: String,
    pub mode: RunToolGrantMode,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunCheckpointRef {
    pub checkpoint_id: CheckpointId,
    pub step_index: u32,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunValidationStatus {
    Passed,
    Failed,
    Inconclusive,
}

impl RunValidationStatus {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Passed => "passed",
            Self::Failed => "failed",
            Self::Inconclusive => "inconclusive",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "passed" => Some(Self::Passed),
            "failed" => Some(Self::Failed),
            "inconclusive" => Some(Self::Inconclusive),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunValidationRef {
    pub validation_id: ValidationId,
    pub status: RunValidationStatus,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct RunExecutionLeaseRef {
    pub lease_id: ExecutionLeaseId,
    pub generation: u64,
}

/// Why the harness parked a Run at a step boundary.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunPauseCause {
    Budget {
        dimension: RunBudgetDimension,
        limit: u64,
        spent: u64,
        pending: u64,
        requested: u64,
    },
    Requested {
        reason: String,
    },
    ToolWeather {
        code: String,
    },
    ReservationExceeded {
        dimension: RunBudgetDimension,
    },
}

// Body enums are externally tagged (`{"V1": {...}}`): the variant name IS
// the version tag STYLE requires, and new versions are new variants.

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProjectCreatedBody {
    V1 {
        project_id: ProjectId,
        name: String,
        template: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ProjectRenamedBody {
    V1 { project_id: ProjectId, name: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStartedBody {
    V1 {
        run_id: RunId,
        worker: String,
        trigger: String,
    },
    V2 {
        run_id: RunId,
        project_id: ProjectId,
        worker: String,
        runtime: RunRuntimeRef,
        executor: RunExecutor,
        trigger: RunTrigger,
        autonomy_level: u8,
        budget: RunBudget,
        tool_grants: Vec<RunToolGrant>,
        parent_run_id: Option<RunId>,
        lineage_depth: u8,
        checkpoint: Option<RunCheckpointRef>,
        validation: Option<RunValidationRef>,
        execution_lease: Option<RunExecutionLeaseRef>,
        tainted: bool,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunStepCommittedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        summary: String,
    },
    V2 {
        run_id: RunId,
        step_index: u32,
        phase: RunStepPhase,
        summary: String,
        digest: [u8; 32],
        tool_call: Option<RunToolCall>,
        reserved: RunUsage,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunFinishedBody {
    V1 {
        run_id: RunId,
        outcome: RunOutcome,
        steps_total: u32,
    },
    V2 {
        run_id: RunId,
        outcome: RunOutcome,
        steps_total: u32,
        spent: RunUsage,
        validation: Option<RunValidationRef>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunToolEffectRecordedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        call_id: ToolCallId,
        idempotency_key: String,
        output_digest: [u8; 32],
        spent: RunUsage,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunCheckpointSavedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        checkpoint_id: CheckpointId,
        state_digest: [u8; 32],
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunArtifactRecordedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        artifact_id: ArtifactId,
        content_hash: [u8; 32],
        media_type: String,
        size_bytes: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunValidationRecordedBody {
    V1 {
        run_id: RunId,
        step_index: u32,
        validation_id: ValidationId,
        status: RunValidationStatus,
        summary: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunPauseRequestedBody {
    V1 { run_id: RunId, reason: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunPausedBody {
    V1 {
        run_id: RunId,
        at_step_index: u32,
        cause: RunPauseCause,
        spent: RunUsage,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunResumedBody {
    V1 { run_id: RunId, at_step_index: u32 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunCancelRequestedBody {
    V1 { run_id: RunId, reason: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunCanceledBody {
    V1 { run_id: RunId, at_step_index: u32 },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunTaintRaisedBody {
    V1 { run_id: RunId, source: String },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunQuestionAskedBody {
    V1 {
        run_id: RunId,
        question_id: QuestionId,
        prompt: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunQuestionAnsweredBody {
    V1 {
        run_id: RunId,
        question_id: QuestionId,
        answered_by: UserId,
        answer: String,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum RunGateApprovedBody {
    V1 {
        run_id: RunId,
        receipt_id: GateReceiptId,
        call_id: ToolCallId,
        approved_by: UserId,
        reason: String,
        expires_ts_ms: u64,
    },
}

/// Claim order inside a worker class (m0-s14). An enum rather than a raw
/// integer so the vocabulary cannot drift; the storage form is the discriminant
/// below, which sorts ascending in the claim query.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum JobPriority {
    High,
    Normal,
    Low,
}

impl JobPriority {
    /// Ascending claim rank — the number the claim query sorts on. Storing the
    /// rank rather than the name keeps `ORDER BY` a plain integer compare.
    #[must_use]
    pub const fn rank(self) -> u8 {
        match self {
            Self::High => 0,
            Self::Normal => 1,
            Self::Low => 2,
        }
    }

    #[must_use]
    pub const fn from_rank(rank: u8) -> Option<Self> {
        match rank {
            0 => Some(Self::High),
            1 => Some(Self::Normal),
            2 => Some(Self::Low),
            _ => None,
        }
    }
}

/// The worker class a job runs in. Weights protect interactive latency
/// (master plan §16: `foreground > ingest > maintenance`).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobClass {
    Foreground,
    Ingest,
    Maintenance,
}

impl JobClass {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Foreground => "foreground",
            Self::Ingest => "ingest",
            Self::Maintenance => "maintenance",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "foreground" => Some(Self::Foreground),
            "ingest" => Some(Self::Ingest),
            "maintenance" => Some(Self::Maintenance),
            _ => None,
        }
    }
}

/// What a cron firing does when its previous job has not finished.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CronOverlapPolicy {
    Skip,
    Queue,
    CancelPrevious,
}

impl CronOverlapPolicy {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Skip => "skip",
            Self::Queue => "queue",
            Self::CancelPrevious => "cancel_previous",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "skip" => Some(Self::Skip),
            "queue" => Some(Self::Queue),
            "cancel_previous" => Some(Self::CancelPrevious),
            _ => None,
        }
    }
}

/// The cron tick a job was fired for. `scheduled_ts_ms` is the *nominal* fire
/// instant, not the enqueue instant: it is what makes a catch-up firing
/// idempotent with the on-time firing it replaces.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct JobCronOrigin {
    pub cron_id: CronId,
    pub scheduled_ts_ms: u64,
}

/// Why a job reached the terminal `Dead` state. The DLQ is never a silent
/// drop (L8): every dead job carries a typed, renderable reason.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobDeadReason {
    /// The retry budget ran out; the last attempt's weather code is kept.
    RetriesExhausted { error_code: String },
    /// The handler refused permanently — retrying cannot change the outcome.
    Refused { error_code: String },
    /// A later firing of the same cron replaced this job (`CancelPrevious`).
    SupersededByCron { cron_id: CronId },
}

impl JobDeadReason {
    /// The stable code stored in the projection and rendered by `job.list`.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::RetriesExhausted { .. } => "retries_exhausted",
            Self::Refused { .. } => "refused",
            Self::SupersededByCron { .. } => "superseded_by_cron",
        }
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobEnqueuedBody {
    V1 {
        job_id: JobId,
        job_kind: String,
    },
    V2 {
        job_id: JobId,
        project_id: ProjectId,
        job_kind: String,
        idempotency_key: String,
        priority: JobPriority,
        class: JobClass,
        payload: Vec<u8>,
        /// Earliest instant a worker may claim this job.
        run_at_ts_ms: u64,
        attempt_count_max: u32,
        cron: Option<JobCronOrigin>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobCompletedBody {
    V1 {
        job_id: JobId,
        attempts: u32,
    },
    V2 {
        job_id: JobId,
        attempt_count: u32,
        wall_ms: u64,
    },
}

/// One failed attempt. A durable fact rather than a counter bump: it is what
/// makes the retry, its backoff, and the eventual DLQ reason explainable, and
/// it is bounded by the job's own `attempt_count_max` (L8).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobAttemptFailedBody {
    V1 {
        job_id: JobId,
        attempt_index: u32,
        error_code: String,
        error_detail: String,
        retry_at_ts_ms: u64,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum JobDeadBody {
    V1 {
        job_id: JobId,
        attempt_count: u32,
        reason: JobDeadReason,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CronRegisteredBody {
    V1 {
        cron_id: CronId,
        project_id: ProjectId,
        job_kind: String,
        expr: String,
        /// IANA zone name (`Europe/Berlin`); resolved against the tz database
        /// at evaluation time, never stored as a fixed offset.
        tz: String,
        overlap_policy: CronOverlapPolicy,
        enabled: bool,
        priority: JobPriority,
        class: JobClass,
        payload: Vec<u8>,
    },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum CronEnablementSetBody {
    V1 { cron_id: CronId, enabled: bool },
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum AccountAuditedBody {
    V1 {
        account_id: AccountId,
        action: String,
        target: String,
    },
}

/// One gateway model call (m0-s10): the honest cost ledger is a fact in the
/// log, so the billing meter is a projection and `pos export` carries it.
/// Money is integer micro-USD — floats accumulate error in projections
/// (event-sourcing skill). `outcome` is the gateway weather code or `"ok"`;
/// error paths are recorded calls too.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ModelCallCompletedBody {
    V1 {
        project_id: ProjectId,
        feature: String,
        agent: Option<String>,
        provider: String,
        credential_class: String,
        model: String,
        tokens_in: u64,
        tokens_out: u64,
        wall_ms: u64,
        provider_cost_kind: String,
        usd_micros: u64,
        outcome: String,
    },
}

/// The typed v0 vocabulary. The set grows additively; a tag, once shipped,
/// never changes meaning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum DomainEvent {
    ProjectCreated(ProjectCreatedBody),
    ProjectRenamed(ProjectRenamedBody),
    RunStarted(RunStartedBody),
    RunStepCommitted(RunStepCommittedBody),
    RunToolEffectRecorded(RunToolEffectRecordedBody),
    RunCheckpointSaved(RunCheckpointSavedBody),
    RunArtifactRecorded(RunArtifactRecordedBody),
    RunValidationRecorded(RunValidationRecordedBody),
    RunPauseRequested(RunPauseRequestedBody),
    RunPaused(RunPausedBody),
    RunResumed(RunResumedBody),
    RunCancelRequested(RunCancelRequestedBody),
    RunCanceled(RunCanceledBody),
    RunTaintRaised(RunTaintRaisedBody),
    RunQuestionAsked(RunQuestionAskedBody),
    RunQuestionAnswered(RunQuestionAnsweredBody),
    RunGateApproved(RunGateApprovedBody),
    RunFinished(RunFinishedBody),
    JobEnqueued(JobEnqueuedBody),
    JobAttemptFailed(JobAttemptFailedBody),
    JobCompleted(JobCompletedBody),
    JobDead(JobDeadBody),
    CronRegistered(CronRegisteredBody),
    CronEnablementSet(CronEnablementSetBody),
    AccountAudited(AccountAuditedBody),
    ModelCallCompleted(ModelCallCompletedBody),
    EvidenceAdded(EvidenceAddedBody),
    IngestStageStarted(IngestStageStartedBody),
    IngestStageFinished(IngestStageFinishedBody),
    IngestStageFailed(IngestStageFailedBody),
    EvidenceChunked(EvidenceChunkedBody),
    EvidenceReprocessRequested(EvidenceReprocessRequestedBody),
    EvidenceTranscribed(EvidenceTranscribedBody),
    TranscriptSpeakerNamed(TranscriptSpeakerNamedBody),
    TranscriptSegmentSpeakerSet(TranscriptSegmentSpeakerSetBody),
    TranscriptTextCorrected(TranscriptTextCorrectedBody),
}

impl DomainEvent {
    #[must_use]
    pub const fn kind_tag(&self) -> &'static str {
        match self {
            Self::ProjectCreated(_) => "ProjectCreated",
            Self::ProjectRenamed(_) => "ProjectRenamed",
            Self::RunStarted(_) => "RunStarted",
            Self::RunStepCommitted(_) => "RunStepCommitted",
            Self::RunToolEffectRecorded(_) => "RunToolEffectRecorded",
            Self::RunCheckpointSaved(_) => "RunCheckpointSaved",
            Self::RunArtifactRecorded(_) => "RunArtifactRecorded",
            Self::RunValidationRecorded(_) => "RunValidationRecorded",
            Self::RunPauseRequested(_) => "RunPauseRequested",
            Self::RunPaused(_) => "RunPaused",
            Self::RunResumed(_) => "RunResumed",
            Self::RunCancelRequested(_) => "RunCancelRequested",
            Self::RunCanceled(_) => "RunCanceled",
            Self::RunTaintRaised(_) => "RunTaintRaised",
            Self::RunQuestionAsked(_) => "RunQuestionAsked",
            Self::RunQuestionAnswered(_) => "RunQuestionAnswered",
            Self::RunGateApproved(_) => "RunGateApproved",
            Self::RunFinished(_) => "RunFinished",
            Self::JobEnqueued(_) => "JobEnqueued",
            Self::JobAttemptFailed(_) => "JobAttemptFailed",
            Self::JobCompleted(_) => "JobCompleted",
            Self::JobDead(_) => "JobDead",
            Self::CronRegistered(_) => "CronRegistered",
            Self::CronEnablementSet(_) => "CronEnablementSet",
            Self::AccountAudited(_) => "AccountAudited",
            Self::ModelCallCompleted(_) => "ModelCallCompleted",
            Self::EvidenceAdded(_) => "EvidenceAdded",
            Self::IngestStageStarted(_) => "IngestStageStarted",
            Self::IngestStageFinished(_) => "IngestStageFinished",
            Self::IngestStageFailed(_) => "IngestStageFailed",
            Self::EvidenceChunked(_) => "EvidenceChunked",
            Self::EvidenceReprocessRequested(_) => "EvidenceReprocessRequested",
            Self::EvidenceTranscribed(_) => "EvidenceTranscribed",
            Self::TranscriptSpeakerNamed(_) => "TranscriptSpeakerNamed",
            Self::TranscriptSegmentSpeakerSet(_) => "TranscriptSegmentSpeakerSet",
            Self::TranscriptTextCorrected(_) => "TranscriptTextCorrected",
        }
    }

    /// The L2 refs this event creates/touches — every entity id in the body,
    /// under its fixed domain noun. An event with missing refs orphans
    /// artifacts (event-sourcing skill), so refs derive from the body rather
    /// than trusting each call site.
    #[must_use]
    pub fn refs(&self) -> Vec<EntityRef> {
        match self {
            Self::ProjectCreated(ProjectCreatedBody::V1 { project_id, .. })
            | Self::ProjectRenamed(ProjectRenamedBody::V1 { project_id, .. }) => {
                vec![entity_ref("project", project_id.into_bytes())]
            }
            Self::RunStarted(RunStartedBody::V1 { run_id, .. })
            | Self::RunStepCommitted(RunStepCommittedBody::V1 { run_id, .. })
            | Self::RunFinished(RunFinishedBody::V1 { run_id, .. }) => run_refs(*run_id),
            Self::RunStarted(RunStartedBody::V2 {
                run_id,
                project_id,
                parent_run_id,
                checkpoint,
                validation,
                execution_lease,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("project", project_id.into_bytes()));
                if let Some(parent_run_id) = parent_run_id {
                    refs.push(entity_ref("run", parent_run_id.into_bytes()));
                }
                if let Some(checkpoint) = checkpoint {
                    refs.push(entity_ref(
                        "checkpoint",
                        checkpoint.checkpoint_id.into_bytes(),
                    ));
                }
                if let Some(validation) = validation {
                    refs.push(entity_ref(
                        "validation",
                        validation.validation_id.into_bytes(),
                    ));
                }
                if let Some(execution_lease) = execution_lease {
                    refs.push(entity_ref(
                        "execution_lease",
                        execution_lease.lease_id.into_bytes(),
                    ));
                }
                refs
            }
            Self::RunStepCommitted(RunStepCommittedBody::V2 {
                run_id, tool_call, ..
            }) => {
                let mut refs = run_refs(*run_id);
                if let Some(tool_call) = tool_call {
                    refs.push(entity_ref("tool_call", tool_call.call_id.into_bytes()));
                }
                refs
            }
            Self::RunToolEffectRecorded(RunToolEffectRecordedBody::V1 {
                run_id, call_id, ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("tool_call", call_id.into_bytes()));
                refs
            }
            Self::RunCheckpointSaved(RunCheckpointSavedBody::V1 {
                run_id,
                checkpoint_id,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("checkpoint", checkpoint_id.into_bytes()));
                refs
            }
            Self::RunArtifactRecorded(RunArtifactRecordedBody::V1 {
                run_id,
                artifact_id,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("artifact", artifact_id.into_bytes()));
                refs
            }
            Self::RunValidationRecorded(RunValidationRecordedBody::V1 {
                run_id,
                validation_id,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("validation", validation_id.into_bytes()));
                refs
            }
            Self::RunPauseRequested(RunPauseRequestedBody::V1 { run_id, .. })
            | Self::RunPaused(RunPausedBody::V1 { run_id, .. })
            | Self::RunResumed(RunResumedBody::V1 { run_id, .. })
            | Self::RunCancelRequested(RunCancelRequestedBody::V1 { run_id, .. })
            | Self::RunCanceled(RunCanceledBody::V1 { run_id, .. })
            | Self::RunTaintRaised(RunTaintRaisedBody::V1 { run_id, .. }) => run_refs(*run_id),
            Self::RunQuestionAsked(RunQuestionAskedBody::V1 {
                run_id,
                question_id,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("question", question_id.into_bytes()));
                refs
            }
            Self::RunQuestionAnswered(RunQuestionAnsweredBody::V1 {
                run_id,
                question_id,
                answered_by,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("question", question_id.into_bytes()));
                refs.push(entity_ref("user", answered_by.into_bytes()));
                refs
            }
            Self::RunGateApproved(RunGateApprovedBody::V1 {
                run_id,
                receipt_id,
                call_id,
                approved_by,
                ..
            }) => {
                let mut refs = run_refs(*run_id);
                refs.push(entity_ref("gate_receipt", receipt_id.into_bytes()));
                refs.push(entity_ref("tool_call", call_id.into_bytes()));
                refs.push(entity_ref("user", approved_by.into_bytes()));
                refs
            }
            Self::RunFinished(RunFinishedBody::V2 {
                run_id, validation, ..
            }) => {
                let mut refs = run_refs(*run_id);
                if let Some(validation) = validation {
                    refs.push(entity_ref(
                        "validation",
                        validation.validation_id.into_bytes(),
                    ));
                }
                refs
            }
            Self::JobEnqueued(JobEnqueuedBody::V1 { job_id, .. })
            | Self::JobCompleted(JobCompletedBody::V1 { job_id, .. })
            | Self::JobCompleted(JobCompletedBody::V2 { job_id, .. })
            | Self::JobAttemptFailed(JobAttemptFailedBody::V1 { job_id, .. }) => {
                vec![entity_ref("job", job_id.into_bytes())]
            }
            Self::JobEnqueued(JobEnqueuedBody::V2 {
                job_id,
                project_id,
                cron,
                ..
            }) => {
                let mut refs = vec![
                    entity_ref("job", job_id.into_bytes()),
                    entity_ref("project", project_id.into_bytes()),
                ];
                if let Some(cron) = cron {
                    refs.push(entity_ref("cron", cron.cron_id.into_bytes()));
                }
                refs
            }
            Self::JobDead(JobDeadBody::V1 { job_id, reason, .. }) => {
                let mut refs = vec![entity_ref("job", job_id.into_bytes())];
                if let JobDeadReason::SupersededByCron { cron_id } = reason {
                    refs.push(entity_ref("cron", cron_id.into_bytes()));
                }
                refs
            }
            Self::CronRegistered(CronRegisteredBody::V1 {
                cron_id,
                project_id,
                ..
            }) => {
                vec![
                    entity_ref("cron", cron_id.into_bytes()),
                    entity_ref("project", project_id.into_bytes()),
                ]
            }
            Self::CronEnablementSet(CronEnablementSetBody::V1 { cron_id, .. }) => {
                vec![entity_ref("cron", cron_id.into_bytes())]
            }
            Self::AccountAudited(AccountAuditedBody::V1 { account_id, .. }) => {
                vec![entity_ref("account", account_id.into_bytes())]
            }
            Self::ModelCallCompleted(ModelCallCompletedBody::V1 { project_id, .. }) => {
                vec![entity_ref("project", project_id.into_bytes())]
            }
            Self::EvidenceAdded(EvidenceAddedBody::V1 {
                evidence_id,
                source_id,
                ..
            }) => vec![
                entity_ref("evidence", evidence_id.into_bytes()),
                entity_ref("source", source_id.into_bytes()),
            ],
            Self::IngestStageStarted(IngestStageStartedBody::V1 {
                evidence_id,
                job_id,
                ..
            }) => vec![
                entity_ref("evidence", evidence_id.into_bytes()),
                entity_ref("job", job_id.into_bytes()),
            ],
            Self::IngestStageFinished(IngestStageFinishedBody::V1 { evidence_id, .. })
            | Self::IngestStageFailed(IngestStageFailedBody::V1 { evidence_id, .. })
            | Self::EvidenceReprocessRequested(EvidenceReprocessRequestedBody::V1 {
                evidence_id,
                ..
            })
            // Transcript facts and their edits all belong to one Evidence
            // item. Segments are not separate entities: they are positions
            // inside the recording, and a citation resolves through the chunk
            // that covers them (m1-s12), not through a segment id.
            | Self::EvidenceTranscribed(EvidenceTranscribedBody::V1 { evidence_id, .. })
            | Self::TranscriptSpeakerNamed(TranscriptSpeakerNamedBody::V1 { evidence_id, .. })
            | Self::TranscriptSegmentSpeakerSet(TranscriptSegmentSpeakerSetBody::V1 {
                evidence_id,
                ..
            })
            | Self::TranscriptTextCorrected(TranscriptTextCorrectedBody::V1 {
                evidence_id, ..
            }) => vec![entity_ref("evidence", evidence_id.into_bytes())],
            // A chunk batch touches every chunk it creates: the L2 why-chain
            // is what a citation walks backwards, and the batch is already
            // bounded by `CHUNK_BATCH_COUNT_MAX`, so the ref list is too.
            Self::EvidenceChunked(EvidenceChunkedBody::V1 {
                evidence_id,
                chunks,
                ..
            }) => {
                let mut refs = Vec::with_capacity(chunks.len() + 1);
                refs.push(entity_ref("evidence", evidence_id.into_bytes()));
                for chunk in chunks {
                    refs.push(entity_ref("chunk", chunk.chunk_id.into_bytes()));
                }
                refs
            }
        }
    }

    /// Versioned CBOR encoding of the body (the §7.1 `body` column).
    #[must_use]
    pub fn encode_body(&self) -> Vec<u8> {
        let mut body = Vec::new();
        let encoded = match self {
            Self::ProjectCreated(inner) => ciborium::into_writer(inner, &mut body),
            Self::ProjectRenamed(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunStarted(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunStepCommitted(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunToolEffectRecorded(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunCheckpointSaved(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunArtifactRecorded(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunValidationRecorded(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunPauseRequested(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunPaused(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunResumed(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunCancelRequested(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunCanceled(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunTaintRaised(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunQuestionAsked(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunQuestionAnswered(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunGateApproved(inner) => ciborium::into_writer(inner, &mut body),
            Self::RunFinished(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobEnqueued(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobAttemptFailed(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobCompleted(inner) => ciborium::into_writer(inner, &mut body),
            Self::JobDead(inner) => ciborium::into_writer(inner, &mut body),
            Self::CronRegistered(inner) => ciborium::into_writer(inner, &mut body),
            Self::CronEnablementSet(inner) => ciborium::into_writer(inner, &mut body),
            Self::AccountAudited(inner) => ciborium::into_writer(inner, &mut body),
            Self::ModelCallCompleted(inner) => ciborium::into_writer(inner, &mut body),
            Self::EvidenceAdded(inner) => ciborium::into_writer(inner, &mut body),
            Self::IngestStageStarted(inner) => ciborium::into_writer(inner, &mut body),
            Self::IngestStageFinished(inner) => ciborium::into_writer(inner, &mut body),
            Self::IngestStageFailed(inner) => ciborium::into_writer(inner, &mut body),
            Self::EvidenceChunked(inner) => ciborium::into_writer(inner, &mut body),
            Self::EvidenceTranscribed(inner) => ciborium::into_writer(inner, &mut body),
            Self::TranscriptSpeakerNamed(inner) => ciborium::into_writer(inner, &mut body),
            Self::TranscriptSegmentSpeakerSet(inner) => ciborium::into_writer(inner, &mut body),
            Self::TranscriptTextCorrected(inner) => ciborium::into_writer(inner, &mut body),
            Self::EvidenceReprocessRequested(inner) => ciborium::into_writer(inner, &mut body),
        };
        encoded.expect("CBOR encoding of typed bodies into a Vec cannot fail"); // INVARIANT: bodies contain only owned serde-friendly values and the writer is a Vec.
        body
    }

    /// Decodes a stored event. `Ok(None)` for a kind this build does not
    /// know — forward compatibility, never data loss (the raw event is still
    /// in the log).
    pub fn decode(kind: &KindTag, body: &[u8]) -> Result<Option<Self>, DomainDecodeError> {
        fn read<T: for<'de> Deserialize<'de>>(
            kind: &'static str,
            body: &[u8],
        ) -> Result<T, DomainDecodeError> {
            ciborium::from_reader(body).map_err(|error| DomainDecodeError {
                kind,
                reason: error.to_string(),
            })
        }
        let decoded = match kind.as_str() {
            "ProjectCreated" => Self::ProjectCreated(read("ProjectCreated", body)?),
            "ProjectRenamed" => Self::ProjectRenamed(read("ProjectRenamed", body)?),
            "RunStarted" => Self::RunStarted(read("RunStarted", body)?),
            "RunStepCommitted" => Self::RunStepCommitted(read("RunStepCommitted", body)?),
            "RunToolEffectRecorded" => {
                Self::RunToolEffectRecorded(read("RunToolEffectRecorded", body)?)
            }
            "RunCheckpointSaved" => Self::RunCheckpointSaved(read("RunCheckpointSaved", body)?),
            "RunArtifactRecorded" => Self::RunArtifactRecorded(read("RunArtifactRecorded", body)?),
            "RunValidationRecorded" => {
                Self::RunValidationRecorded(read("RunValidationRecorded", body)?)
            }
            "RunPauseRequested" => Self::RunPauseRequested(read("RunPauseRequested", body)?),
            "RunPaused" => Self::RunPaused(read("RunPaused", body)?),
            "RunResumed" => Self::RunResumed(read("RunResumed", body)?),
            "RunCancelRequested" => Self::RunCancelRequested(read("RunCancelRequested", body)?),
            "RunCanceled" => Self::RunCanceled(read("RunCanceled", body)?),
            "RunTaintRaised" => Self::RunTaintRaised(read("RunTaintRaised", body)?),
            "RunQuestionAsked" => Self::RunQuestionAsked(read("RunQuestionAsked", body)?),
            "RunQuestionAnswered" => Self::RunQuestionAnswered(read("RunQuestionAnswered", body)?),
            "RunGateApproved" => Self::RunGateApproved(read("RunGateApproved", body)?),
            "RunFinished" => Self::RunFinished(read("RunFinished", body)?),
            "JobEnqueued" => Self::JobEnqueued(read("JobEnqueued", body)?),
            "JobAttemptFailed" => Self::JobAttemptFailed(read("JobAttemptFailed", body)?),
            "JobCompleted" => Self::JobCompleted(read("JobCompleted", body)?),
            "JobDead" => Self::JobDead(read("JobDead", body)?),
            "CronRegistered" => Self::CronRegistered(read("CronRegistered", body)?),
            "CronEnablementSet" => Self::CronEnablementSet(read("CronEnablementSet", body)?),
            "AccountAudited" => Self::AccountAudited(read("AccountAudited", body)?),
            "ModelCallCompleted" => Self::ModelCallCompleted(read("ModelCallCompleted", body)?),
            "EvidenceAdded" => Self::EvidenceAdded(read("EvidenceAdded", body)?),
            "IngestStageStarted" => Self::IngestStageStarted(read("IngestStageStarted", body)?),
            "IngestStageFinished" => Self::IngestStageFinished(read("IngestStageFinished", body)?),
            "IngestStageFailed" => Self::IngestStageFailed(read("IngestStageFailed", body)?),
            "EvidenceChunked" => Self::EvidenceChunked(read("EvidenceChunked", body)?),
            "EvidenceTranscribed" => Self::EvidenceTranscribed(read("EvidenceTranscribed", body)?),
            "TranscriptSpeakerNamed" => {
                Self::TranscriptSpeakerNamed(read("TranscriptSpeakerNamed", body)?)
            }
            "TranscriptSegmentSpeakerSet" => {
                Self::TranscriptSegmentSpeakerSet(read("TranscriptSegmentSpeakerSet", body)?)
            }
            "TranscriptTextCorrected" => {
                Self::TranscriptTextCorrected(read("TranscriptTextCorrected", body)?)
            }
            "EvidenceReprocessRequested" => {
                Self::EvidenceReprocessRequested(read("EvidenceReprocessRequested", body)?)
            }
            _ => return Ok(None),
        };
        Ok(Some(decoded))
    }

    /// Builds the append request for this fact. The actor is the caller's to
    /// supply — it is never defaulted (event-sourcing skill).
    pub fn into_request(
        self,
        device: pos_foundation::DeviceId,
        actor: Actor,
    ) -> Result<AppendRequest, LogError> {
        Ok(AppendRequest {
            device,
            actor,
            kind: KindTag::new(self.kind_tag())?,
            body: self.encode_body(),
            refs: self.refs(),
        })
    }
}

fn entity_ref(entity: &str, id: [u8; 16]) -> EntityRef {
    EntityRef {
        entity: entity.to_owned(),
        id,
    }
}

fn run_refs(run_id: RunId) -> Vec<EntityRef> {
    vec![entity_ref("run", run_id.into_bytes())]
}

#[cfg(test)]
mod tests {
    use super::{DomainEvent, ProjectCreatedBody, RunFinishedBody, RunOutcome};
    use pos_foundation::{ProjectId, RunId};
    use pos_log::KindTag;

    #[test]
    fn bodies_round_trip_and_unknown_kinds_are_none() {
        let event = DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
            project_id: ProjectId::from_bytes([1; 16]),
            name: "Acme Widgets".to_owned(),
            template: "generic".to_owned(),
        });
        let body = event.encode_body();
        let decoded = DomainEvent::decode(&KindTag::new("ProjectCreated").expect("valid"), &body)
            .expect("body decodes");
        assert_eq!(decoded, Some(event));

        let unknown =
            DomainEvent::decode(&KindTag::new("HoloDeckCalibrated").expect("valid"), &body)
                .expect("unknown kinds are not errors");
        assert_eq!(unknown, None);
    }

    #[test]
    fn malformed_bodies_are_typed_errors_naming_the_kind() {
        let error = DomainEvent::decode(
            &KindTag::new("RunFinished").expect("valid"),
            &[0xff, 0x00, 0x01],
        )
        .expect_err("garbage must not decode");
        assert_eq!(error.kind, "RunFinished");
    }

    #[test]
    fn refs_carry_every_entity_the_event_touches() {
        let event = DomainEvent::RunFinished(RunFinishedBody::V1 {
            run_id: RunId::from_bytes([9; 16]),
            outcome: RunOutcome::Canceled,
            steps_total: 4,
        });
        let refs = event.refs();
        assert_eq!(refs.len(), 1);
        assert_eq!(refs[0].entity, "run");
        assert_eq!(refs[0].id, [9; 16]);
    }
}
