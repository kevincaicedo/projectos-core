//! # pos-sched
//!
//! First-class scheduler (F36): SQLite job queue (idempotency keys, priorities, backoff, DLQ, fairness), tz-aware cron with overlap policies, weighted worker classes, capacity windows (F68 later).
//!
//! Skeleton created by m0-s01; filled by m0-s14. Charter: master plan §19.
//!
//! ## Where the queue lives (m0-s14 decision, ADR-0005)
//!
//! A job is a **fact in the project log**, not a row in a side database.
//! `JobEnqueued`, `JobAttemptFailed`, `JobCompleted`, and `JobDead` are typed
//! events; `proj_jobs`/`proj_crons` are their rebuildable projections. Three
//! consequences follow, and they are the reason for the design:
//!
//! 1. **Enqueue is transactional with domain writes** for free — an enqueue is
//!    an append, so "record the decision and schedule the work" is one commit
//!    (L1).
//! 2. **Queued work is portable** — it travels inside the `.pos` directory
//!    with everything else the project owns (L4).
//! 3. **Recovery is replay** — after `kill -9` the queue is exactly what the
//!    log says it is; nothing needs reconciling.
//!
//! The one thing that is *not* a fact is the claim. A lease is node-local
//! coordination with a wall-clock expiry — writing heartbeats into an eternal
//! log would be recording weather as history. Leases live in `sched_leases`,
//! an operational table this crate owns (never a `proj_*` table, so the L1
//! projection-write rule is untouched), and they are rebuilt-by-forgetting:
//! a lost lease simply makes its job claimable again.
//!
//! ## State machine and its invariant inventory (STYLE)
//!
//! The frozen §3.2 contract is `Queued → Running → Done | Failed | Dead`.
//! `Running` and the retry-waiting flavour of `Failed` are *derived*:
//!
//! ```text
//!   durable(proj_jobs)      lease(sched_leases)     live state
//!   queued                  live                    Running
//!   queued, attempts = 0    none                    Queued
//!   queued, attempts > 0    none                    Failed (retry at run_at_ts_ms)
//!   done                    any                     Done
//!   dead                    any                     Dead
//! ```
//!
//! Invariants, each asserted or type-enforced where it is established:
//!
//! - **I1 — exactly one job per (project, kind, idempotency key).** The job id
//!   is derived from that triple, so a duplicate enqueue collides on the
//!   projection primary key and fails the whole append; admission turns the
//!   expected case into a typed `Duplicate` before it gets that far.
//! - **I2 — at most one live lease per job.** `sched_leases` is keyed by job
//!   id, and claim + lease insert share one IMMEDIATE transaction.
//! - **I3 — attempts are counted from durable facts only.** A crash records
//!   nothing; the reaper turns the resulting expired lease into exactly one
//!   `JobAttemptFailed`, guarded by `attempt_count < lease.attempt_index` so a
//!   crash *after* the fact was appended cannot double-count it.
//! - **I4 — no terminal job is ever claimed.** The claim query filters on the
//!   durable `queued` state inside the same transaction that takes the lease.
//! - **I5 — every `Dead` job carries a typed reason** (L8: the DLQ is never a
//!   silent drop).
//! - **I6 — a cron tick fires at most one job per schedule**, keyed by the
//!   nominal fire instant, so a catch-up firing and the on-time firing it
//!   replaces are the same job.

#![forbid(unsafe_code)]

mod backoff;
mod cron;
mod job;
mod metrics;
mod pool;
mod queue;

pub use backoff::{
    BackoffPolicy, JOB_BACKOFF_BASE_MS_DEFAULT, JOB_BACKOFF_CAP_MS_DEFAULT,
    JOB_BACKOFF_FACTOR_DEFAULT, JitterSource, NoJitter, SplitMixJitter,
};
pub use cron::{
    CRON_PREVIEW_COUNT_MAX, CRON_SEARCH_STEP_COUNT_MAX, CronDriver, CronExpr, CronParseError,
    CronSchedule, CronSpec, CronTickReport, derive_cron_id, preview_registered,
};
pub use job::{
    ClaimedJob, JOB_IDEMPOTENCY_KEY_LEN_MAX, JOB_KIND_LEN_MAX, JOB_PAYLOAD_LEN_MAX,
    JOB_RETRY_COUNT_MAX, JobFailure, JobKind, JobKindError, JobLiveState, JobSpec,
    WORKER_NAME_LEN_MAX, WorkerName, derive_job_id,
};
pub use metrics::{
    HistogramSnapshot, METRIC_KIND_COUNT_MAX, QueueDepth, SchedulerMetrics,
    SchedulerMetricsSnapshot, latency_bucket_bounds_ms,
};
pub use pool::{
    ClassConfig, HANDLER_REGISTRY_COUNT_MAX, HandlerRegistry, HandlerRegistryError, JobHandler,
    PROJECT_REGISTRY_COUNT_MAX, ProjectRegistry, WorkerPool, WorkerPoolConfig, WorkerPoolHandle,
    class_defaults,
};
pub use queue::{
    EnqueueOutcome, FailureOutcome, JobQueue, LEASE_REAP_BATCH_COUNT_MAX, LIVE_LEASE_COUNT_MAX,
    QueueConfig, SCHED_LEASE_TTL_MS_DEFAULT, live_state_of,
};

use pos_domain::JobReadError;
use pos_log::LogError;
use pos_store::StoreError;
use std::fmt;

/// Typed failures of the scheduler. Everything here is operating weather —
/// a full disk, a malformed cron expression, an oversize payload — and is
/// handled by callers, never asserted.
#[derive(Debug)]
pub enum SchedError {
    Store(StoreError),
    Log(LogError),
    Read(JobReadError),
    /// A job spec violated a stated bound (kind/key/payload length, retry cap).
    InvalidSpec {
        field: &'static str,
        reason: String,
    },
    /// A cron expression or zone name did not parse.
    InvalidSchedule(CronParseError),
    /// The claim referenced a job whose durable row vanished — only reachable
    /// if the database was mutated outside the log.
    UnknownJob {
        job_id: String,
    },
}

impl fmt::Display for SchedError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::Log(source) => write!(formatter, "{source}"),
            Self::Read(source) => write!(formatter, "{source}"),
            Self::InvalidSpec { field, reason } => {
                write!(formatter, "job spec field {field} is invalid: {reason}")
            }
            Self::InvalidSchedule(source) => write!(formatter, "{source}"),
            Self::UnknownJob { job_id } => write!(
                formatter,
                "job {job_id} has no row in proj_jobs; the projection was mutated outside the log"
            ),
        }
    }
}

impl std::error::Error for SchedError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::Log(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::InvalidSchedule(source) => Some(source),
            Self::InvalidSpec { .. } | Self::UnknownJob { .. } => None,
        }
    }
}

impl From<StoreError> for SchedError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

impl From<LogError> for SchedError {
    fn from(source: LogError) -> Self {
        Self::Log(source)
    }
}

impl From<JobReadError> for SchedError {
    fn from(source: JobReadError) -> Self {
        Self::Read(source)
    }
}

impl From<CronParseError> for SchedError {
    fn from(source: CronParseError) -> Self {
        Self::InvalidSchedule(source)
    }
}
