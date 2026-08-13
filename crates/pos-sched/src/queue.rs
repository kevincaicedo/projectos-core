//! The job queue: enqueue, claim-with-lease, heartbeat, terminal transitions,
//! and lease reaping (m0-s14).
//!
//! Every durable transition here is an append to the project log; the only
//! direct table this module writes is `sched_leases`, the node-local claim
//! ledger described in the crate doc. Read the invariant inventory there
//! before changing anything in this file — I1–I5 all live in these functions.

use crate::backoff::{BackoffPolicy, JitterSource};
use crate::job::{
    ClaimedJob, JobFailure, JobKind, JobLiveState, JobSpec, WorkerName, derive_job_id,
};
use crate::metrics::{QueueDepth, SchedulerMetrics};
use crate::{JobKindError, SchedError};
use pos_domain::{
    DomainEvent, JobAttemptFailedBody, JobClass, JobCompletedBody, JobDeadBody, JobDeadReason,
    JobDurableState, JobEnqueuedBody, JobRecord, count_jobs_by_state, read_job,
};
use pos_foundation::{CronId, DeviceId, JobId, ProjectId, WallClock};
use pos_log::{Actor, AppendRequest, LogError, ProjectLog};
use pos_store::rusqlite::{OptionalExtension, Transaction, params};
use std::collections::BTreeSet;
use std::sync::Arc;

/// How long a claim stays valid without a heartbeat. Long enough that a
/// normally-slow job is never stolen mid-flight, short enough that a killed
/// worker's work returns to the queue inside a user's patience.
pub const SCHED_LEASE_TTL_MS_DEFAULT: u64 = 30_000;

/// Expired leases handled per reap call. Bounded so recovery after a mass
/// crash is incremental instead of one unbounded transaction (L8).
pub const LEASE_REAP_BATCH_COUNT_MAX: usize = 64;

/// Live leases a bulk read will return. One lease exists per *running* worker,
/// and the pool's class caps are single digits, so exceeding this means a lease
/// leaked rather than that the product got busy.
pub const LIVE_LEASE_COUNT_MAX: usize = 1_024;

/// The node-local claim ledger. Deliberately not a `proj_*` table: a lease is
/// a wall-clock lock, not a fact about the project, and it must not travel
/// with an export (L4) or survive a replay (L1).
const SCHED_SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS sched_leases (
  job_id              BLOB    PRIMARY KEY,
  worker              TEXT    NOT NULL,
  attempt_index       INTEGER NOT NULL,
  claimed_ts_ms       INTEGER NOT NULL,
  heartbeat_ts_ms     INTEGER NOT NULL,
  lease_expires_ts_ms INTEGER NOT NULL
) WITHOUT ROWID;
CREATE INDEX IF NOT EXISTS idx_sched_leases_expiry
  ON sched_leases (lease_expires_ts_ms);
";

#[derive(Clone, Copy, Debug)]
pub struct QueueConfig {
    /// The device every scheduler-appended fact is attributed to.
    pub device: DeviceId,
    pub backoff: BackoffPolicy,
    pub lease_ttl_ms: u64,
}

impl QueueConfig {
    #[must_use]
    pub fn for_device(device: DeviceId) -> Self {
        Self {
            device,
            backoff: BackoffPolicy::default(),
            lease_ttl_ms: SCHED_LEASE_TTL_MS_DEFAULT,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EnqueueOutcome {
    /// A new job now exists in the log.
    Enqueued(JobId),
    /// This exact work was already enqueued; the id is the original's.
    Duplicate(JobId),
}

impl EnqueueOutcome {
    #[must_use]
    pub const fn job_id(self) -> JobId {
        match self {
            Self::Enqueued(id) | Self::Duplicate(id) => id,
        }
    }

    #[must_use]
    pub const fn is_duplicate(self) -> bool {
        matches!(self, Self::Duplicate(_))
    }
}

/// What a failed attempt turned into.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FailureOutcome {
    /// Another attempt is scheduled at this instant.
    Retrying { retry_at_ts_ms: u64 },
    /// The job entered the DLQ; `proj_jobs.dead_reason_code` says why.
    Dead,
}

/// The queue engine. One instance serves every project the process has open;
/// the project is a parameter, not state, so fairness stays the pool's
/// decision rather than something buried in here.
pub struct JobQueue {
    config: QueueConfig,
    jitter: Arc<dyn JitterSource>,
    metrics: Arc<SchedulerMetrics>,
}

impl JobQueue {
    #[must_use]
    pub fn new(
        config: QueueConfig,
        jitter: Arc<dyn JitterSource>,
        metrics: Arc<SchedulerMetrics>,
    ) -> Self {
        Self {
            config,
            jitter,
            metrics,
        }
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<SchedulerMetrics> {
        &self.metrics
    }

    #[must_use]
    pub const fn config(&self) -> &QueueConfig {
        &self.config
    }

    /// Creates the lease table. Idempotent; call once per opened project.
    pub fn ensure_schema(&self, log: &ProjectLog) -> Result<(), SchedError> {
        log.store()
            .db()
            .with_writer("ensure scheduler schema", |connection| {
                connection.execute_batch(SCHED_SCHEMA_SQL)
            })?;
        Ok(())
    }

    /// Appends the job as a durable fact. Exactly-once is enforced twice:
    /// this admission read answers the expected duplicate politely, and the
    /// derived id's primary key refuses one that races past it (I1).
    pub fn enqueue(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        spec: &JobSpec,
        clock: &dyn WallClock,
    ) -> Result<EnqueueOutcome, SchedError> {
        spec.validate()?;
        let job_id = derive_job_id(project_id, &spec.kind, &spec.idempotency_key);
        if read_job(log, job_id)?.is_some() {
            self.metrics.record_duplicate_enqueue();
            return Ok(EnqueueOutcome::Duplicate(job_id));
        }
        let request = enqueued_event(job_id, project_id, spec)
            .into_request(self.config.device, Actor::System(job_id))?;
        match log.append(request, clock) {
            Ok(_) => {
                self.metrics.record_enqueued();
                Ok(EnqueueOutcome::Enqueued(job_id))
            }
            // The insert the projection renders is strict, so a concurrent
            // enqueue of the same work fails the whole append atomically.
            // Re-reading is how we tell that from a genuine apply bug.
            Err(LogError::Apply { kind, seq, source }) => {
                if read_job(log, job_id)?.is_some() {
                    self.metrics.record_duplicate_enqueue();
                    return Ok(EnqueueOutcome::Duplicate(job_id));
                }
                Err(SchedError::Log(LogError::Apply { kind, seq, source }))
            }
            Err(error) => Err(error.into()),
        }
    }

    /// Builds the enqueue append *without* appending it, so a caller can
    /// commit "my fact and the work it schedules" in one transaction
    /// (m1-s01). This is the seam the crate docs already claimed — an enqueue
    /// is an append — and the ingestion pipeline needs it for correctness,
    /// not convenience: a stage that appended its completion and then
    /// enqueued the next stage separately would stall the whole item if the
    /// process died between the two commits.
    ///
    /// `Ok(None)` means the job already exists; the caller's own facts still
    /// need appending, and re-enqueueing would be the duplicate this returns
    /// instead of raising.
    pub fn enqueue_request(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        spec: &JobSpec,
    ) -> Result<(JobId, Option<AppendRequest>), SchedError> {
        spec.validate()?;
        let job_id = derive_job_id(project_id, &spec.kind, &spec.idempotency_key);
        if read_job(log, job_id)?.is_some() {
            self.metrics.record_duplicate_enqueue();
            return Ok((job_id, None));
        }
        let request = enqueued_event(job_id, project_id, spec)
            .into_request(self.config.device, Actor::System(job_id))?;
        Ok((job_id, Some(request)))
    }

    /// Records the metrics an [`Self::enqueue_request`] caller earned once it
    /// has committed the batch. Kept separate so the counter follows the
    /// durable append rather than the intention to append.
    pub fn record_enqueued(&self) {
        self.metrics.record_enqueued();
    }

    /// Takes the highest-priority runnable job in `class`, oldest first, and
    /// leases it. Claim and lease share one IMMEDIATE transaction, so two
    /// workers cannot hold the same job (I2), and terminal jobs are filtered
    /// inside that transaction rather than before it (I4).
    pub fn claim(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        class: JobClass,
        worker: &WorkerName,
        clock: &dyn WallClock,
    ) -> Result<Option<ClaimedJob>, SchedError> {
        let now_ms = clock.now_ms();
        let expires_ts_ms = now_ms.saturating_add(self.config.lease_ttl_ms);
        let claimed = log.store().db().write_transaction(
            "claim job",
            |transaction| -> Result<Option<ClaimedJob>, SchedError> {
                let Some(candidate) = select_claimable(transaction, class, now_ms)? else {
                    return Ok(None);
                };
                let attempt_index = candidate.attempt_count.saturating_add(1);
                insert_lease(
                    transaction,
                    candidate.job_id,
                    worker,
                    attempt_index,
                    now_ms,
                    expires_ts_ms,
                )?;
                let kind = JobKind::new(candidate.job_kind.clone())
                    .map_err(|error| invalid_kind(&error))?;
                Ok(Some(ClaimedJob {
                    job_id: candidate.job_id,
                    project_id,
                    kind,
                    payload: candidate.payload,
                    attempt_index,
                    retry_count_max: candidate.attempt_count_max,
                    enqueued_seq: pos_foundation::EventSeq::new(candidate.enqueued_seq),
                    worker: worker.clone(),
                    claimed_ts_ms: now_ms,
                    claim_latency_ms: now_ms.saturating_sub(candidate.run_at_ts_ms),
                }))
            },
        )?;
        if let Some(job) = &claimed {
            self.metrics.record_claim(job.claim_latency_ms);
        }
        Ok(claimed)
    }

    /// Extends a live lease. `false` means the lease is gone — the worker was
    /// declared dead and must stop touching the job.
    pub fn heartbeat(
        &self,
        log: &ProjectLog,
        job: &ClaimedJob,
        clock: &dyn WallClock,
    ) -> Result<bool, SchedError> {
        let now_ms = clock.now_ms();
        let expires_ts_ms = now_ms.saturating_add(self.config.lease_ttl_ms);
        let changed = log.store().db().write_transaction(
            "heartbeat job lease",
            |transaction| -> Result<usize, SchedError> {
                let changed = transaction
                    .prepare_cached(
                        "UPDATE sched_leases SET heartbeat_ts_ms = ?1, lease_expires_ts_ms = ?2
                         WHERE job_id = ?3 AND worker = ?4 AND attempt_index = ?5",
                    )
                    .and_then(|mut statement| {
                        statement.execute(params![
                            signed(now_ms),
                            signed(expires_ts_ms),
                            job.job_id.into_bytes().to_vec(),
                            job.worker.as_str(),
                            i64::from(job.attempt_index),
                        ])
                    })
                    .map_err(sqlite("heartbeat lease"))?;
                Ok(changed)
            },
        )?;
        Ok(changed == 1)
    }

    /// Records success and releases the lease. The fact lands first: a crash
    /// between the two leaves an expired lease over a terminal job, which the
    /// reaper resolves by deleting it.
    pub fn complete(
        &self,
        log: &ProjectLog,
        job: &ClaimedJob,
        wall_ms: u64,
        clock: &dyn WallClock,
    ) -> Result<(), SchedError> {
        let event = DomainEvent::JobCompleted(JobCompletedBody::V2 {
            job_id: job.job_id,
            attempt_count: job.attempt_index,
            wall_ms,
        });
        self.append_job_fact(log, job.job_id, event, clock)?;
        self.release_lease(log, job.job_id)?;
        self.metrics.record_completed(job.kind.as_str(), wall_ms);
        Ok(())
    }

    /// Records a failed attempt: another retry with backoff, or the DLQ with
    /// a typed reason (I5).
    pub fn fail(
        &self,
        log: &ProjectLog,
        job: &ClaimedJob,
        failure: &JobFailure,
        wall_ms: u64,
        clock: &dyn WallClock,
    ) -> Result<FailureOutcome, SchedError> {
        let now_ms = clock.now_ms();
        let attempt = FailedAttempt {
            job_id: job.job_id,
            attempt_index: job.attempt_index,
            retry_count_max: job.retry_count_max,
            now_ms,
        };
        let outcome = self.append_failure(log, attempt, failure, clock)?;
        self.release_lease(log, job.job_id)?;
        self.metrics
            .record_attempt_failed(job.kind.as_str(), wall_ms);
        if outcome == FailureOutcome::Dead {
            self.metrics.record_dead();
        }
        Ok(outcome)
    }

    /// Cron `CancelPrevious`: retires a still-running or queued job because a
    /// later firing replaced it. A typed dead reason, never a silent delete.
    pub fn supersede(
        &self,
        log: &ProjectLog,
        job_id: JobId,
        cron_id: CronId,
        clock: &dyn WallClock,
    ) -> Result<(), SchedError> {
        let Some(record) = read_job(log, job_id)? else {
            return Err(SchedError::UnknownJob {
                job_id: job_id.to_hex(),
            });
        };
        if record.state.is_terminal() {
            return Ok(());
        }
        let event = DomainEvent::JobDead(JobDeadBody::V1 {
            job_id,
            attempt_count: record.attempt_count,
            reason: JobDeadReason::SupersededByCron { cron_id },
        });
        self.append_job_fact(log, job_id, event, clock)?;
        self.release_lease(log, job_id)?;
        self.metrics.record_dead();
        Ok(())
    }

    /// Turns expired leases into durable failed attempts and frees their jobs.
    ///
    /// This is the crash-recovery path: a worker that died mid-job recorded
    /// nothing, so its attempt only becomes real here. The
    /// `attempt_count < attempt_index` guard is I3 — a crash *after* the fact
    /// was appended must not count the attempt twice.
    pub fn reap_expired_leases(
        &self,
        log: &ProjectLog,
        clock: &dyn WallClock,
    ) -> Result<u32, SchedError> {
        let now_ms = clock.now_ms();
        let expired = read_expired_leases(log, now_ms)?;
        let mut reaped = 0_u32;
        for lease in expired {
            let Some(record) = read_job(log, lease.job_id)? else {
                self.release_lease(log, lease.job_id)?;
                reaped += 1;
                continue;
            };
            if !record.state.is_terminal() && record.attempt_count < lease.attempt_index {
                let failure = JobFailure::retriable(
                    "lease_expired",
                    format!(
                        "worker {} stopped heartbeating attempt {}",
                        lease.worker, lease.attempt_index
                    ),
                );
                let attempt = FailedAttempt {
                    job_id: lease.job_id,
                    attempt_index: lease.attempt_index,
                    retry_count_max: record.attempt_count_max,
                    now_ms,
                };
                let outcome = self.append_failure(log, attempt, &failure, clock)?;
                if outcome == FailureOutcome::Dead {
                    self.metrics.record_dead();
                }
            }
            self.release_lease(log, lease.job_id)?;
            reaped += 1;
        }
        self.metrics.record_lease_reaped(u64::from(reaped));
        Ok(reaped)
    }

    /// The frozen §3.2 read view for one job (durable state ⋈ lease).
    pub fn live_state(
        &self,
        log: &ProjectLog,
        job_id: JobId,
        clock: &dyn WallClock,
    ) -> Result<Option<JobLiveState>, SchedError> {
        let Some(record) = read_job(log, job_id)? else {
            return Ok(None);
        };
        let lease_live = self.lease_is_live(log, job_id, clock.now_ms())?;
        Ok(Some(live_state_of(&record, lease_live)))
    }

    /// Queue depth by durable state — the gauge, read from the projection.
    pub fn depth(&self, log: &ProjectLog) -> Result<QueueDepth, SchedError> {
        Ok(QueueDepth::from_counts(count_jobs_by_state(log)?))
    }

    /// Every job currently under a live lease on this node, in one read.
    ///
    /// A list view needs the lease half for many rows at once; asking per row
    /// would make rendering a page O(rows) round trips against the same table
    /// (the per-item pattern STYLE forbids). Bounded because a live lease
    /// exists only per running worker, and the bound is asserted, not assumed.
    pub fn live_lease_job_ids(
        &self,
        log: &ProjectLog,
        clock: &dyn WallClock,
    ) -> Result<BTreeSet<[u8; 16]>, SchedError> {
        let now_ms = clock.now_ms();
        let ids = log
            .store()
            .db()
            .with_reader("read live leases", |connection| {
                let mut statement = connection.prepare_cached(
                    "SELECT job_id FROM sched_leases
                      WHERE lease_expires_ts_ms > ?1 LIMIT ?2",
                )?;
                let rows = statement.query_map(
                    params![
                        signed(now_ms),
                        i64::try_from(LIVE_LEASE_COUNT_MAX).unwrap_or(i64::MAX)
                    ],
                    |row| row.get::<_, Vec<u8>>(0),
                )?;
                rows.collect::<Result<Vec<_>, _>>()
            })?;
        debug_assert!(
            ids.len() < LIVE_LEASE_COUNT_MAX,
            "live leases hit the stated bound; a lease leaked or the pool is misconfigured"
        );
        Ok(ids
            .into_iter()
            .filter_map(|bytes| <[u8; 16]>::try_from(bytes.as_slice()).ok())
            .collect())
    }

    fn lease_is_live(
        &self,
        log: &ProjectLog,
        job_id: JobId,
        now_ms: u64,
    ) -> Result<bool, SchedError> {
        let expires: Option<i64> =
            log.store()
                .db()
                .with_reader("read job lease", |connection| {
                    connection
                        .query_row(
                            "SELECT lease_expires_ts_ms FROM sched_leases WHERE job_id = ?1",
                            [job_id.into_bytes().to_vec()],
                            |row| row.get(0),
                        )
                        .optional()
                })?;
        Ok(expires.is_some_and(|expires| unsigned(expires) > now_ms))
    }

    fn append_failure(
        &self,
        log: &ProjectLog,
        attempt: FailedAttempt,
        failure: &JobFailure,
        clock: &dyn WallClock,
    ) -> Result<FailureOutcome, SchedError> {
        let FailedAttempt {
            job_id,
            attempt_index,
            retry_count_max,
            now_ms,
        } = attempt;
        let exhausted = attempt_index > retry_count_max;
        if failure.permanent || exhausted {
            let reason = if failure.permanent {
                JobDeadReason::Refused {
                    error_code: failure.code.clone(),
                }
            } else {
                JobDeadReason::RetriesExhausted {
                    error_code: failure.code.clone(),
                }
            };
            let event = DomainEvent::JobDead(JobDeadBody::V1 {
                job_id,
                attempt_count: attempt_index,
                reason,
            });
            self.append_job_fact(log, job_id, event, clock)?;
            return Ok(FailureOutcome::Dead);
        }
        let retry_at_ts_ms = now_ms.saturating_add(
            self.config
                .backoff
                .delay_ms(attempt_index, self.jitter.as_ref()),
        );
        let event = DomainEvent::JobAttemptFailed(JobAttemptFailedBody::V1 {
            job_id,
            attempt_index,
            error_code: failure.code.clone(),
            error_detail: failure.detail.clone(),
            retry_at_ts_ms,
        });
        self.append_job_fact(log, job_id, event, clock)?;
        Ok(FailureOutcome::Retrying { retry_at_ts_ms })
    }

    fn append_job_fact(
        &self,
        log: &ProjectLog,
        job_id: JobId,
        event: DomainEvent,
        clock: &dyn WallClock,
    ) -> Result<(), SchedError> {
        let request = event.into_request(self.config.device, Actor::System(job_id))?;
        log.append(request, clock)?;
        Ok(())
    }

    fn release_lease(&self, log: &ProjectLog, job_id: JobId) -> Result<(), SchedError> {
        log.store()
            .db()
            .with_writer("release job lease", |connection| {
                connection.execute(
                    "DELETE FROM sched_leases WHERE job_id = ?1",
                    [job_id.into_bytes().to_vec()],
                )
            })?;
        Ok(())
    }
}

/// The read view rule, in one place so the API, the pool, and the tests
/// cannot disagree about what `Running` means.
#[must_use]
/// The one place a `JobEnqueued` body is built, so the appending and the
/// request-building paths cannot drift apart.
fn enqueued_event(job_id: JobId, project_id: ProjectId, spec: &JobSpec) -> DomainEvent {
    DomainEvent::JobEnqueued(JobEnqueuedBody::V2 {
        job_id,
        project_id,
        job_kind: spec.kind.as_str().to_owned(),
        idempotency_key: spec.idempotency_key.clone(),
        priority: spec.priority,
        class: spec.class,
        payload: spec.payload.clone(),
        run_at_ts_ms: spec.run_at_ts_ms,
        attempt_count_max: spec.retry_count_max,
        cron: spec.cron,
    })
}

pub fn live_state_of(record: &JobRecord, lease_live: bool) -> JobLiveState {
    match record.state {
        JobDurableState::Done => JobLiveState::Done,
        JobDurableState::Dead => JobLiveState::Dead,
        JobDurableState::Queued if lease_live => JobLiveState::Running,
        JobDurableState::Queued if record.attempt_count > 0 => JobLiveState::Failed,
        JobDurableState::Queued => JobLiveState::Queued,
    }
}

/// The four facts a failure transition needs, grouped so the transition
/// keeps one argument per idea instead of one per field.
#[derive(Clone, Copy, Debug)]
struct FailedAttempt {
    job_id: JobId,
    attempt_index: u32,
    retry_count_max: u32,
    now_ms: u64,
}

struct Candidate {
    job_id: JobId,
    job_kind: String,
    payload: Vec<u8>,
    attempt_count: u32,
    attempt_count_max: u32,
    enqueued_seq: u64,
    run_at_ts_ms: u64,
}

/// The hot read. Ordered by priority then enqueue order, filtered to jobs
/// that are durably queued, eligible now, and unleased — an expired lease
/// still blocks, because only the reaper may decide what a dead attempt cost
/// (I3).
fn select_claimable(
    transaction: &Transaction<'_>,
    class: JobClass,
    now_ms: u64,
) -> Result<Option<Candidate>, SchedError> {
    let candidate = transaction
        .prepare_cached(
            "SELECT j.job_id, j.job_kind, j.payload, j.attempt_count, j.attempt_count_max,
                    j.enqueued_seq, j.run_at_ts_ms
               FROM proj_jobs j
               LEFT JOIN sched_leases l ON l.job_id = j.job_id
              WHERE j.state = 'queued' AND j.class = ?1 AND j.run_at_ts_ms <= ?2
                AND l.job_id IS NULL
              ORDER BY j.priority_rank ASC, j.enqueued_seq ASC
              LIMIT 1",
        )
        .and_then(|mut statement| {
            statement
                .query_row(params![class.as_str(), signed(now_ms)], |row| {
                    let job_id: Vec<u8> = row.get(0)?;
                    Ok(Candidate {
                        job_id: JobId::from_bytes(
                            <[u8; 16]>::try_from(job_id.as_slice()).unwrap_or([0; 16]),
                        ),
                        job_kind: row.get(1)?,
                        payload: row.get(2)?,
                        attempt_count: count_u32(row.get(3)?),
                        attempt_count_max: count_u32(row.get(4)?),
                        enqueued_seq: unsigned(row.get(5)?),
                        run_at_ts_ms: unsigned(row.get(6)?),
                    })
                })
                .optional()
        })
        .map_err(sqlite("select claimable job"))?;
    Ok(candidate)
}

fn insert_lease(
    transaction: &Transaction<'_>,
    job_id: JobId,
    worker: &WorkerName,
    attempt_index: u32,
    now_ms: u64,
    expires_ts_ms: u64,
) -> Result<(), SchedError> {
    // Plain insert, not upsert: the claim query already excluded every job
    // that has a lease row, so a conflict here would mean I2 was violated.
    transaction
        .prepare_cached(
            "INSERT INTO sched_leases
               (job_id, worker, attempt_index, claimed_ts_ms, heartbeat_ts_ms, lease_expires_ts_ms)
             VALUES (?1, ?2, ?3, ?4, ?4, ?5)",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                job_id.into_bytes().to_vec(),
                worker.as_str(),
                i64::from(attempt_index),
                signed(now_ms),
                signed(expires_ts_ms),
            ])
        })
        .map_err(sqlite("insert job lease"))?;
    Ok(())
}

struct ExpiredLease {
    job_id: JobId,
    worker: String,
    attempt_index: u32,
}

fn read_expired_leases(log: &ProjectLog, now_ms: u64) -> Result<Vec<ExpiredLease>, SchedError> {
    let leases = log
        .store()
        .db()
        .with_reader("read expired leases", |connection| {
            let mut statement = connection.prepare_cached(
                "SELECT job_id, worker, attempt_index FROM sched_leases
                  WHERE lease_expires_ts_ms <= ?1
                  ORDER BY lease_expires_ts_ms ASC LIMIT ?2",
            )?;
            let rows = statement.query_map(
                params![
                    signed(now_ms),
                    i64::try_from(LEASE_REAP_BATCH_COUNT_MAX).unwrap_or(i64::MAX)
                ],
                |row| {
                    let job_id: Vec<u8> = row.get(0)?;
                    Ok(ExpiredLease {
                        job_id: JobId::from_bytes(
                            <[u8; 16]>::try_from(job_id.as_slice()).unwrap_or([0; 16]),
                        ),
                        worker: row.get(1)?,
                        attempt_index: count_u32(row.get(2)?),
                    })
                },
            )?;
            rows.collect::<Result<Vec<_>, _>>()
        })?;
    Ok(leases)
}

fn invalid_kind(error: &JobKindError) -> SchedError {
    SchedError::InvalidSpec {
        field: "job_kind",
        reason: error.to_string(),
    }
}

fn sqlite(context: &'static str) -> impl Fn(pos_store::rusqlite::Error) -> SchedError {
    move |source| SchedError::Store(pos_store::StoreError::Sqlite { context, source })
}

/// SQLite stores signed integers; scheduler instants and counters never are.
/// Saturating keeps the conversion total without inventing a panic path on a
/// value only a hand-edited database could produce.
fn signed(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}

fn unsigned(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn count_u32(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(0)
}
