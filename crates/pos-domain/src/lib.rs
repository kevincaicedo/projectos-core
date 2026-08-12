//! # pos-domain
//!
//! Entities, events, projections, why-chain integrity (L1, L2). The typed EventKind vocabulary and the projection apply paths live here.
//!
//! Filled by m0-s03 (v0 kinds). Charter: master plan §19.
//!
//! Everything here is pure over the envelope: bodies are versioned CBOR
//! (`events`), projections return typed row writes that `pos-log`'s apply
//! chokepoint executes (`projections`), and the deterministic synthetic
//! generator (`synthetic`) feeds tests and `pos-bench` the same reproducible
//! corpora.

#![forbid(unsafe_code)]

pub mod events;
pub mod job_state;
pub mod projections;
pub mod run_state;
pub mod synthetic;

pub use events::{
    AccountAuditedBody, CronEnablementSetBody, CronOverlapPolicy, CronRegisteredBody,
    DomainDecodeError, DomainEvent, JobAttemptFailedBody, JobClass, JobCompletedBody,
    JobCronOrigin, JobDeadBody, JobDeadReason, JobEnqueuedBody, JobPriority,
    ModelCallCompletedBody, ProjectCreatedBody, ProjectRenamedBody, RunArtifactRecordedBody,
    RunBudget, RunBudgetDimension, RunCancelRequestedBody, RunCanceledBody, RunCheckpointRef,
    RunCheckpointSavedBody, RunExecutionLeaseRef, RunExecutor, RunFinishedBody,
    RunGateApprovedBody, RunOutcome, RunPauseCause, RunPauseRequestedBody, RunPausedBody,
    RunQuestionAnsweredBody, RunQuestionAskedBody, RunResumedBody, RunRuntimeKind, RunRuntimeRef,
    RunStartedBody, RunStepCommittedBody, RunStepPhase, RunTaintRaisedBody, RunToolCall,
    RunToolEffectRecordedBody, RunToolGrant, RunToolGrantMode, RunTrigger, RunUsage,
    RunValidationRecordedBody, RunValidationRef, RunValidationStatus,
};
pub use job_state::{
    CronRecord, JOB_LIST_ROW_COUNT_MAX, JobDurableState, JobListFilter, JobReadError, JobRecord,
    count_jobs_by_state, list_crons, list_jobs, read_cron, read_job,
};
pub use projections::v0_registry;
pub use run_state::{
    PendingRunControl, RunCheckpointState, RunEffectState, RunPauseState, RunReadError, RunState,
    RunStatus, RunStepState, read_run, read_run_step,
};
pub use synthetic::SyntheticEvents;
