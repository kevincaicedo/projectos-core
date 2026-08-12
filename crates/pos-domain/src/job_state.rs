//! Typed reads over the durable scheduler projections (m0-s14).
//!
//! `proj_jobs` and `proj_crons` are rebuildable views of the log, so these
//! functions are the only vocabulary the scheduler, the API, and future UIs
//! need in order to answer "what work exists". The *live* claim state (which
//! job a worker currently holds) is a node-local lease and lives in
//! `pos-sched`; nothing here reads or invents it.

use crate::{CronOverlapPolicy, JobClass, JobPriority};
use pos_foundation::{CronId, EventSeq, JobId, ProjectId};
use pos_log::ProjectLog;
use pos_store::StoreError;
use pos_store::rusqlite::{OptionalExtension, Row};
use std::fmt;

/// Rows one `job.list`/metrics read returns (L8). A queue legitimately holds
/// tens of thousands of rows; a read surface that tried to return all of them
/// would trade an honest bound for an unbounded allocation.
pub const JOB_LIST_ROW_COUNT_MAX: u32 = 500;

/// The durable job state — what the log alone can prove. `Running` is not a
/// member on purpose: a claim is a lease, not a fact (see `pos-sched::queue`,
/// which joins this state with the lease table to render the frozen
/// `Queued → Running → Done/Failed/Dead` contract).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobDurableState {
    Queued,
    Done,
    Dead,
}

impl JobDurableState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Done => "done",
            Self::Dead => "dead",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "queued" => Some(Self::Queued),
            "done" => Some(Self::Done),
            "dead" => Some(Self::Dead),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Dead)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobRecord {
    pub job_id: JobId,
    pub project_id: Option<ProjectId>,
    pub job_kind: String,
    pub idempotency_key: String,
    pub state: JobDurableState,
    pub priority: JobPriority,
    pub class: JobClass,
    pub payload: Vec<u8>,
    pub enqueued_seq: EventSeq,
    pub run_at_ts_ms: u64,
    pub attempt_count: u32,
    pub attempt_count_max: u32,
    pub last_error_code: Option<String>,
    pub last_error_detail: Option<String>,
    pub terminal_seq: Option<EventSeq>,
    pub wall_ms: Option<u64>,
    pub dead_reason_code: Option<String>,
    pub dead_reason_detail: Option<String>,
    pub cron_id: Option<CronId>,
}

impl JobRecord {
    /// Whether another attempt is still permitted. Read at failure time to
    /// decide retry-versus-DLQ, so the bound lives with the job rather than
    /// with whichever worker happens to fail it.
    #[must_use]
    pub const fn attempts_remain(&self) -> bool {
        self.attempt_count < self.attempt_count_max
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronRecord {
    pub cron_id: CronId,
    pub project_id: ProjectId,
    pub job_kind: String,
    pub expr: String,
    pub tz: String,
    pub overlap_policy: CronOverlapPolicy,
    pub enabled: bool,
    pub priority: JobPriority,
    pub class: JobClass,
    pub payload: Vec<u8>,
    pub registered_seq: EventSeq,
    pub registered_ts_ms: u64,
    pub last_fired_ts_ms: Option<u64>,
    pub last_job_id: Option<JobId>,
}

impl CronRecord {
    /// The instant the next fire is searched from: the last firing, or the
    /// registration if the schedule has never fired. Registration time is the
    /// honest baseline — a schedule cannot owe firings from before it existed.
    #[must_use]
    pub const fn watermark_ts_ms(&self) -> u64 {
        match self.last_fired_ts_ms {
            Some(fired) => fired,
            None => self.registered_ts_ms,
        }
    }
}

/// What a bounded job read selects. Every field narrows; `None` means "any".
#[derive(Clone, Copy, Debug, Default)]
pub struct JobListFilter {
    pub state: Option<JobDurableState>,
    pub class: Option<JobClass>,
    pub cron_id: Option<CronId>,
    /// Clamped to [`JOB_LIST_ROW_COUNT_MAX`] by the reader.
    pub row_count_max: Option<u32>,
}

#[derive(Debug)]
pub enum JobReadError {
    Store(StoreError),
    CorruptProjection { table: &'static str, reason: String },
}

impl fmt::Display for JobReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::CorruptProjection { table, reason } => {
                write!(formatter, "{table} is corrupt: {reason}")
            }
        }
    }
}

impl std::error::Error for JobReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::CorruptProjection { .. } => None,
        }
    }
}

impl From<StoreError> for JobReadError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

/// Column list shared by every `proj_jobs` read so the row decoder below can
/// stay a single function (positional decoding is the one thing that rots
/// when two queries drift).
const JOB_COLUMNS: &str = "job_id, project_id, job_kind, idempotency_key, state, priority_rank, \
     class, payload, enqueued_seq, run_at_ts_ms, attempt_count, attempt_count_max, \
     last_error_code, last_error_detail, terminal_seq, wall_ms, dead_reason_code, \
     dead_reason_detail, cron_id";

pub fn read_job(log: &ProjectLog, job_id: JobId) -> Result<Option<JobRecord>, JobReadError> {
    let raw = log.store().db().with_reader("read job row", |connection| {
        connection
            .query_row(
                &format!("SELECT {JOB_COLUMNS} FROM proj_jobs WHERE job_id = ?1"),
                [job_id.into_bytes().to_vec()],
                job_row,
            )
            .optional()
    })?;
    raw.map(|raw| raw.into_record()).transpose()
}

/// Bounded, deterministically ordered job listing: newest enqueue first, ties
/// broken by id so two runs of the same query cannot disagree.
pub fn list_jobs(log: &ProjectLog, filter: JobListFilter) -> Result<Vec<JobRecord>, JobReadError> {
    let limit = i64::from(
        filter
            .row_count_max
            .unwrap_or(JOB_LIST_ROW_COUNT_MAX)
            .min(JOB_LIST_ROW_COUNT_MAX),
    );
    let mut sql = format!("SELECT {JOB_COLUMNS} FROM proj_jobs WHERE 1 = 1");
    if filter.state.is_some() {
        sql.push_str(" AND state = :state");
    }
    if filter.class.is_some() {
        sql.push_str(" AND class = :class");
    }
    if filter.cron_id.is_some() {
        sql.push_str(" AND cron_id = :cron_id");
    }
    sql.push_str(" ORDER BY enqueued_seq DESC, job_id ASC LIMIT :limit");
    let raws = log
        .store()
        .db()
        .with_reader("list job rows", |connection| {
            let mut statement = connection.prepare(&sql)?;
            // Named parameters: the clause set varies, so positional indexes would
            // silently shift with the filter combination.
            if let Some(state) = filter.state {
                statement.raw_bind_parameter(
                    statement.parameter_index(":state")?.unwrap_or(usize::MAX),
                    state.as_str(),
                )?;
            }
            if let Some(class) = filter.class {
                statement.raw_bind_parameter(
                    statement.parameter_index(":class")?.unwrap_or(usize::MAX),
                    class.as_str(),
                )?;
            }
            if let Some(cron_id) = filter.cron_id {
                statement.raw_bind_parameter(
                    statement.parameter_index(":cron_id")?.unwrap_or(usize::MAX),
                    cron_id.into_bytes().to_vec(),
                )?;
            }
            statement.raw_bind_parameter(
                statement.parameter_index(":limit")?.unwrap_or(usize::MAX),
                limit,
            )?;
            let mut rows = statement.raw_query();
            let mut collected = Vec::new();
            while let Some(row) = rows.next()? {
                collected.push(job_row(row)?);
            }
            Ok(collected)
        })?;
    raws.into_iter().map(JobRaw::into_record).collect()
}

/// Queue depth per durable state — the gauge the scheduler metrics publish.
pub fn count_jobs_by_state(log: &ProjectLog) -> Result<[u64; 3], JobReadError> {
    let counts = log
        .store()
        .db()
        .with_reader("count jobs by state", |connection| {
            let mut statement = connection
                .prepare_cached("SELECT state, count(*) FROM proj_jobs GROUP BY state")?;
            let mut rows = statement.query([])?;
            let mut counts = [0_u64; 3];
            while let Some(row) = rows.next()? {
                let state: String = row.get(0)?;
                let count: i64 = row.get(1)?;
                let slot = match JobDurableState::parse(&state) {
                    Some(JobDurableState::Queued) => 0,
                    Some(JobDurableState::Done) => 1,
                    Some(JobDurableState::Dead) => 2,
                    // An unknown state string can only come from a newer build
                    // writing this database; counting it nowhere is honest.
                    None => continue,
                };
                counts[slot] = u64::try_from(count).unwrap_or(0);
            }
            Ok(counts)
        })?;
    Ok(counts)
}

const CRON_COLUMNS: &str = "cron_id, project_id, job_kind, expr, tz, overlap_policy, enabled, \
     priority_rank, class, payload, registered_seq, registered_ts_ms, last_fired_ts_ms, last_job_id";

pub fn read_cron(log: &ProjectLog, cron_id: CronId) -> Result<Option<CronRecord>, JobReadError> {
    let raw = log
        .store()
        .db()
        .with_reader("read cron row", |connection| {
            connection
                .query_row(
                    &format!("SELECT {CRON_COLUMNS} FROM proj_crons WHERE cron_id = ?1"),
                    [cron_id.into_bytes().to_vec()],
                    cron_row,
                )
                .optional()
        })?;
    raw.map(CronRaw::into_record).transpose()
}

/// Every registered schedule in cron-id order — the tick driver's work list.
pub fn list_crons(log: &ProjectLog) -> Result<Vec<CronRecord>, JobReadError> {
    let raws = log
        .store()
        .db()
        .with_reader("list cron rows", |connection| {
            let mut statement =
                connection.prepare_cached(&format!("SELECT {CRON_COLUMNS} FROM proj_crons"))?;
            let rows = statement.query_map([], cron_row)?;
            rows.collect::<Result<Vec<_>, _>>()
        })?;
    let mut records = raws
        .into_iter()
        .map(CronRaw::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by_key(|record| record.cron_id.into_bytes());
    Ok(records)
}

struct JobRaw {
    job_id: Vec<u8>,
    project_id: Option<Vec<u8>>,
    job_kind: String,
    idempotency_key: String,
    state: String,
    priority_rank: i64,
    class: String,
    payload: Vec<u8>,
    enqueued_seq: i64,
    run_at_ts_ms: i64,
    attempt_count: i64,
    attempt_count_max: i64,
    last_error_code: Option<String>,
    last_error_detail: Option<String>,
    terminal_seq: Option<i64>,
    wall_ms: Option<i64>,
    dead_reason_code: Option<String>,
    dead_reason_detail: Option<String>,
    cron_id: Option<Vec<u8>>,
}

fn job_row(row: &Row<'_>) -> Result<JobRaw, pos_store::rusqlite::Error> {
    Ok(JobRaw {
        job_id: row.get(0)?,
        project_id: row.get(1)?,
        job_kind: row.get(2)?,
        idempotency_key: row.get(3)?,
        state: row.get(4)?,
        priority_rank: row.get(5)?,
        class: row.get(6)?,
        payload: row.get(7)?,
        enqueued_seq: row.get(8)?,
        run_at_ts_ms: row.get(9)?,
        attempt_count: row.get(10)?,
        attempt_count_max: row.get(11)?,
        last_error_code: row.get(12)?,
        last_error_detail: row.get(13)?,
        terminal_seq: row.get(14)?,
        wall_ms: row.get(15)?,
        dead_reason_code: row.get(16)?,
        dead_reason_detail: row.get(17)?,
        cron_id: row.get(18)?,
    })
}

impl JobRaw {
    fn into_record(self) -> Result<JobRecord, JobReadError> {
        let corrupt = |reason: String| JobReadError::CorruptProjection {
            table: "proj_jobs",
            reason,
        };
        let job_id = JobId::from_bytes(id_bytes("proj_jobs", "job_id", &self.job_id)?);
        let state = JobDurableState::parse(&self.state)
            .ok_or_else(|| corrupt(format!("unknown job state {:?}", self.state)))?;
        let priority = u8::try_from(self.priority_rank)
            .ok()
            .and_then(JobPriority::from_rank)
            .ok_or_else(|| corrupt(format!("unknown priority rank {}", self.priority_rank)))?;
        let class = JobClass::parse(&self.class)
            .ok_or_else(|| corrupt(format!("unknown job class {:?}", self.class)))?;
        Ok(JobRecord {
            job_id,
            project_id: optional_id("proj_jobs", "project_id", self.project_id.as_deref())?
                .map(ProjectId::from_bytes),
            job_kind: self.job_kind,
            idempotency_key: self.idempotency_key,
            state,
            priority,
            class,
            payload: self.payload,
            enqueued_seq: EventSeq::new(non_negative(self.enqueued_seq)),
            run_at_ts_ms: non_negative(self.run_at_ts_ms),
            attempt_count: count_u32(self.attempt_count),
            attempt_count_max: count_u32(self.attempt_count_max),
            last_error_code: self.last_error_code,
            last_error_detail: self.last_error_detail,
            terminal_seq: self
                .terminal_seq
                .map(|seq| EventSeq::new(non_negative(seq))),
            wall_ms: self.wall_ms.map(non_negative),
            dead_reason_code: self.dead_reason_code,
            dead_reason_detail: self.dead_reason_detail,
            cron_id: optional_id("proj_jobs", "cron_id", self.cron_id.as_deref())?
                .map(CronId::from_bytes),
        })
    }
}

struct CronRaw {
    cron_id: Vec<u8>,
    project_id: Vec<u8>,
    job_kind: String,
    expr: String,
    tz: String,
    overlap_policy: String,
    enabled: i64,
    priority_rank: i64,
    class: String,
    payload: Vec<u8>,
    registered_seq: i64,
    registered_ts_ms: i64,
    last_fired_ts_ms: Option<i64>,
    last_job_id: Option<Vec<u8>>,
}

fn cron_row(row: &Row<'_>) -> Result<CronRaw, pos_store::rusqlite::Error> {
    Ok(CronRaw {
        cron_id: row.get(0)?,
        project_id: row.get(1)?,
        job_kind: row.get(2)?,
        expr: row.get(3)?,
        tz: row.get(4)?,
        overlap_policy: row.get(5)?,
        enabled: row.get(6)?,
        priority_rank: row.get(7)?,
        class: row.get(8)?,
        payload: row.get(9)?,
        registered_seq: row.get(10)?,
        registered_ts_ms: row.get(11)?,
        last_fired_ts_ms: row.get(12)?,
        last_job_id: row.get(13)?,
    })
}

impl CronRaw {
    fn into_record(self) -> Result<CronRecord, JobReadError> {
        let corrupt = |reason: String| JobReadError::CorruptProjection {
            table: "proj_crons",
            reason,
        };
        let overlap_policy = CronOverlapPolicy::parse(&self.overlap_policy)
            .ok_or_else(|| corrupt(format!("unknown overlap policy {:?}", self.overlap_policy)))?;
        let priority = u8::try_from(self.priority_rank)
            .ok()
            .and_then(JobPriority::from_rank)
            .ok_or_else(|| corrupt(format!("unknown priority rank {}", self.priority_rank)))?;
        let class = JobClass::parse(&self.class)
            .ok_or_else(|| corrupt(format!("unknown job class {:?}", self.class)))?;
        Ok(CronRecord {
            cron_id: CronId::from_bytes(id_bytes("proj_crons", "cron_id", &self.cron_id)?),
            project_id: ProjectId::from_bytes(id_bytes(
                "proj_crons",
                "project_id",
                &self.project_id,
            )?),
            job_kind: self.job_kind,
            expr: self.expr,
            tz: self.tz,
            overlap_policy,
            enabled: self.enabled != 0,
            priority,
            class,
            payload: self.payload,
            registered_seq: EventSeq::new(non_negative(self.registered_seq)),
            registered_ts_ms: non_negative(self.registered_ts_ms),
            last_fired_ts_ms: self.last_fired_ts_ms.map(non_negative),
            last_job_id: optional_id("proj_crons", "last_job_id", self.last_job_id.as_deref())?
                .map(JobId::from_bytes),
        })
    }
}

fn id_bytes(
    table: &'static str,
    column: &'static str,
    bytes: &[u8],
) -> Result<[u8; 16], JobReadError> {
    <[u8; 16]>::try_from(bytes).map_err(|_| JobReadError::CorruptProjection {
        table,
        reason: format!("{column} holds {} bytes, not 16", bytes.len()),
    })
}

fn optional_id(
    table: &'static str,
    column: &'static str,
    bytes: Option<&[u8]>,
) -> Result<Option<[u8; 16]>, JobReadError> {
    bytes
        .map(|bytes| id_bytes(table, column, bytes))
        .transpose()
}

/// SQLite integers are signed; projection counters never are. A negative value
/// is a hand-mutated database, and clamping to zero keeps the read total
/// rather than inventing a panic path in a read.
fn non_negative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn count_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(0)
}
