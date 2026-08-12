//! The m0-s14 surface slice: `job.list` over the durable queue projection
//! joined with node-local leases, and `cron.preview` over the tz-aware cron
//! engine.
//!
//! Both are reads. The M0 scheduler has no user-facing write surface — job
//! history, cancel/retry, and the cron editor belong to the F36 UI in M4 —
//! so registering an enqueue command here would be scaffolding for a screen
//! that does not exist yet.

use crate::ApiError;
use crate::project_ops::{self};
use pos_domain::{
    JOB_LIST_ROW_COUNT_MAX, JobDurableState, JobListFilter, JobRecord, list_jobs, read_job,
};
use pos_foundation::{CronId, JobId, SystemWallClock, WallClock};
use pos_log::ProjectLog;
use pos_sched::{
    BackoffPolicy, CRON_PREVIEW_COUNT_MAX, CronSchedule, JobQueue, QueueConfig, SchedError,
    SchedulerMetrics, SplitMixJitter, live_state_of,
};
use serde::{Deserialize, Serialize};
use std::path::Path;
use std::sync::Arc;
use ts_rs::TS;

/// Default rows a `job.list` call answers with when the caller states no
/// bound of its own.
const JOB_LIST_ROW_COUNT_DEFAULT: u32 = 50;

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct JobListInput {
    pub path: String,
    /// `queued` | `done` | `dead` — the durable states. Live `running` is a
    /// lease overlay, so it is a property of a row, not a stored filter.
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub row_count_max: Option<u32>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct JobRow {
    pub job_id: String,
    pub job_kind: String,
    pub idempotency_key: String,
    /// The frozen §3.2 read view: `queued | running | failed | done | dead`.
    pub state: String,
    pub priority: String,
    pub class: String,
    #[ts(type = "number")]
    pub enqueued_seq: u64,
    #[ts(type = "number")]
    pub run_at_ts_ms: u64,
    pub attempt_count: u32,
    pub attempt_count_max: u32,
    pub last_error_code: Option<String>,
    pub dead_reason_code: Option<String>,
    pub cron_id: Option<String>,
    #[ts(type = "number | null")]
    pub wall_ms: Option<u64>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct JobListReport {
    pub jobs: Vec<JobRow>,
    /// Queue depth by durable state, read from the projection rather than
    /// counted over the returned page — a gauge must not depend on paging.
    #[ts(type = "number")]
    pub queued_count: u64,
    #[ts(type = "number")]
    pub done_count: u64,
    #[ts(type = "number")]
    pub dead_count: u64,
    /// The bound this answer honoured, in-band so a full page is
    /// distinguishable from a truncated one (L8).
    pub row_count_max: u32,
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CronPreviewInput {
    /// A five-field cron expression, previewed without registering anything —
    /// this is what a cron editor calls on every keystroke.
    pub expr: String,
    /// IANA zone name; an unknown zone is a typed error, never a UTC fallback.
    pub tz: String,
    /// Search origin. Absent means "now" on this process's clock.
    #[serde(default)]
    #[ts(type = "number | null")]
    pub after_ts_ms: Option<u64>,
    #[serde(default)]
    pub count: Option<u32>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CronPreviewReport {
    pub expr: String,
    pub tz: String,
    #[ts(type = "number")]
    pub after_ts_ms: u64,
    /// The next firings as UTC epoch milliseconds. Shorter than `count` means
    /// the expression has no further firing inside the engine's search bound.
    #[ts(type = "number[]")]
    pub runs: Vec<i64>,
    pub count_max: u32,
}

/// `job.list` — the durable queue joined with this node's leases.
pub fn job_list(input: &JobListInput) -> Result<String, ApiError> {
    let state = match input.state.as_deref() {
        None => None,
        Some(text) => Some(JobDurableState::parse(text).ok_or_else(|| ApiError {
            code: "invalid_input",
            message: format!(
                "job state {text:?} is not one of queued, done, dead \
                 (running and failed are live states, not stored ones)"
            ),
            retriable: false,
        })?),
    };
    let log = project_ops::open_log(Path::new(&input.path))?;
    let queue = read_only_queue();
    // The lease table belongs to this crate's scheduler, not to the project
    // format: a project opened by a build that never scheduled anything has
    // no such table until now.
    queue
        .ensure_schema(&log)
        .map_err(|error| sched_error(&error))?;
    let row_count_max = input
        .row_count_max
        .unwrap_or(JOB_LIST_ROW_COUNT_DEFAULT)
        .min(JOB_LIST_ROW_COUNT_MAX);
    let filter = JobListFilter {
        state,
        row_count_max: Some(row_count_max),
        ..JobListFilter::default()
    };
    let records = list_jobs(&log, filter).map_err(|error| ApiError {
        code: "storage_failure",
        message: error.to_string(),
        retriable: true,
    })?;
    let depth = queue.depth(&log).map_err(|error| sched_error(&error))?;
    // One lease read for the whole page rather than one per row: the durable
    // half is already in hand, and the live half is the same small set for
    // every row on the page.
    let live_leases = queue
        .live_lease_job_ids(&log, &SystemWallClock)
        .map_err(|error| sched_error(&error))?;
    let jobs = records
        .iter()
        .map(|record| job_row(record, live_leases.contains(&record.job_id.into_bytes())))
        .collect::<Vec<_>>();
    project_ops::to_json(&JobListReport {
        jobs,
        queued_count: depth.queued,
        done_count: depth.done,
        dead_count: depth.dead,
        row_count_max,
    })
}

/// `cron.preview` — the "next 10 runs" answer, over an expression the caller
/// supplies rather than a stored schedule, so an editor can preview before
/// anything is registered.
pub fn cron_preview(input: &CronPreviewInput) -> Result<String, ApiError> {
    let schedule = CronSchedule::new(&input.expr, &input.tz).map_err(|error| ApiError {
        code: "invalid_input",
        message: error.to_string(),
        retriable: false,
    })?;
    let after_ts_ms = input
        .after_ts_ms
        .unwrap_or_else(|| SystemWallClock.now_ms());
    let count = input.count.unwrap_or(10).min(CRON_PREVIEW_COUNT_MAX);
    let runs = schedule.preview(i64::try_from(after_ts_ms).unwrap_or(i64::MAX), count);
    project_ops::to_json(&CronPreviewReport {
        expr: input.expr.clone(),
        tz: input.tz.clone(),
        after_ts_ms,
        runs,
        count_max: CRON_PREVIEW_COUNT_MAX,
    })
}

/// Reads the live state of one job — used by the contract suite and by the
/// M4 job surface's detail view.
pub fn job_live_state(log: &ProjectLog, job_id: JobId) -> Result<Option<String>, ApiError> {
    let queue = read_only_queue();
    queue
        .ensure_schema(log)
        .map_err(|error| sched_error(&error))?;
    let Some(record) = read_job(log, job_id).map_err(|error| ApiError {
        code: "storage_failure",
        message: error.to_string(),
        retriable: true,
    })?
    else {
        return Ok(None);
    };
    let state = queue
        .live_state(log, record.job_id, &SystemWallClock)
        .map_err(|error| sched_error(&error))?;
    Ok(state.map(|state| state.as_str().to_owned()))
}

fn job_row(record: &JobRecord, lease_live: bool) -> JobRow {
    let live = live_state_of(record, lease_live);
    JobRow {
        job_id: record.job_id.to_hex(),
        job_kind: record.job_kind.clone(),
        idempotency_key: record.idempotency_key.clone(),
        state: live.as_str().to_owned(),
        priority: priority_name(record.priority).to_owned(),
        class: record.class.as_str().to_owned(),
        enqueued_seq: record.enqueued_seq.value(),
        run_at_ts_ms: record.run_at_ts_ms,
        attempt_count: record.attempt_count,
        attempt_count_max: record.attempt_count_max,
        last_error_code: record.last_error_code.clone(),
        dead_reason_code: record.dead_reason_code.clone(),
        cron_id: record.cron_id.map(CronId::to_hex),
        wall_ms: record.wall_ms,
    }
}

const fn priority_name(priority: pos_domain::JobPriority) -> &'static str {
    match priority {
        pos_domain::JobPriority::High => "high",
        pos_domain::JobPriority::Normal => "normal",
        pos_domain::JobPriority::Low => "low",
    }
}

/// A queue used only for its read paths. The device id is the read-side
/// identity: nothing on these paths appends, so it never reaches the log.
fn read_only_queue() -> JobQueue {
    let config = QueueConfig {
        device: pos_foundation::DeviceId::from_bytes([0; 16]),
        backoff: BackoffPolicy::default(),
        lease_ttl_ms: pos_sched::SCHED_LEASE_TTL_MS_DEFAULT,
    };
    JobQueue::new(
        config,
        Arc::new(SplitMixJitter::from_os_entropy()),
        Arc::new(SchedulerMetrics::default()),
    )
}

fn sched_error(error: &SchedError) -> ApiError {
    let code = match *error {
        SchedError::InvalidSpec { .. } | SchedError::InvalidSchedule(_) => "invalid_input",
        SchedError::UnknownJob { .. } => "state_mutated",
        SchedError::Store(_) | SchedError::Log(_) | SchedError::Read(_) => "storage_failure",
    };
    ApiError {
        code,
        message: error.to_string(),
        retriable: matches!(code, "storage_failure"),
    }
}
