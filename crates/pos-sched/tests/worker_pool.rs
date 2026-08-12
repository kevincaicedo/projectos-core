//! The weighted worker pool end to end (m0-s14 task 5): real tokio tasks,
//! real claims, real terminal writes.
//!
//! These use the system clock rather than the manual one, because the pool's
//! own timing (idle poll, heartbeat, reap) is part of what is under test.
//! Every assertion is on durable state after a bounded wait, never on a
//! sleep-and-hope.

#![forbid(unsafe_code)]

mod common;

use common::{RunLedger, ScriptedHandler, kind, open_project, queue, spec};
use pos_domain::{JobClass, JobDurableState, read_job};
use pos_foundation::{JobId, ProjectId, SystemWallClock};
use pos_log::ProjectLog;
use pos_sched::{
    ClaimedJob, ClassConfig, HandlerRegistry, HandlerRegistryError, JobFailure, JobHandler,
    JobKind, JobSpec, ProjectRegistry, WorkerPool, WorkerPoolConfig,
};
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROJECT: ProjectId = ProjectId::from_bytes([0xc1; 16]);

/// Upper bound on how long a pool test waits for durable state. Long enough
/// for a loaded CI machine, short enough that a hang fails instead of hanging.
const WAIT_MS_MAX: u64 = 10_000;

fn fast_pool_config() -> WorkerPoolConfig {
    WorkerPoolConfig {
        idle_poll_interval_ms: 10,
        heartbeat_interval_ms: 50,
        reap_interval_ms: 50,
        cron_tick_interval_ms: 50,
        ..WorkerPoolConfig::default()
    }
}

/// Polls durable state until `predicate` holds or the bound elapses.
async fn wait_until(mut predicate: impl FnMut() -> bool) -> bool {
    let deadline = Instant::now() + Duration::from_millis(WAIT_MS_MAX);
    while Instant::now() < deadline {
        if predicate() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    predicate()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_pool_drains_a_queue_and_records_per_kind_durations() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(SystemWallClock);
    let log = Arc::new(open_project(
        &directory.path().join("pool.pos"),
        PROJECT,
        &pos_foundation::ManualWallClock::starting_at(1_700_000_000_000),
    ));
    let queue = queue(30_000);
    let ledger = Arc::new(RunLedger::default());
    let mut handlers = HandlerRegistry::new();
    handlers
        .register(Arc::new(ScriptedHandler::always_ok(
            kind("noop"),
            Arc::clone(&ledger),
        )))
        .expect("register handler");
    let projects = Arc::new(ProjectRegistry::new());
    projects
        .register(&queue, PROJECT, Arc::clone(&log))
        .expect("register project");

    let mut ids = Vec::new();
    for index in 0..12 {
        ids.push(
            queue
                .enqueue(
                    &log,
                    PROJECT,
                    &spec("noop", &format!("job-{index}")),
                    clock.as_ref(),
                )
                .expect("enqueue")
                .job_id(),
        );
    }

    let pool = WorkerPool::start(
        &tokio::runtime::Handle::current(),
        Arc::clone(&queue),
        Arc::new(handlers),
        projects,
        clock,
        fast_pool_config(),
    );
    pool.wake();
    let drained = wait_until(|| all_done(&log, &ids)).await;
    pool.shutdown().await;

    assert!(drained, "the pool did not drain the queue inside the bound");
    assert_eq!(ledger.total(), 12);
    let snapshot = queue.metrics().snapshot();
    assert_eq!(snapshot.completed_total, 12);
    assert_eq!(snapshot.claimed_total, 12);
    assert_eq!(snapshot.queue_error_total, 0);
    assert_eq!(
        snapshot
            .run_duration_by_kind
            .get("noop")
            .map(|histogram| histogram.count),
        Some(12),
        "run durations are recorded per kind"
    );
    assert_eq!(snapshot.claim_latency.count, 12);
    assert!(log.verify_projections().expect("verify").is_clean());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_job_with_no_registered_handler_lands_in_the_dlq_with_a_reason() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(SystemWallClock);
    let log = Arc::new(open_project(
        &directory.path().join("orphan.pos"),
        PROJECT,
        &pos_foundation::ManualWallClock::starting_at(1_700_000_000_000),
    ));
    let queue = queue(30_000);
    let projects = Arc::new(ProjectRegistry::new());
    projects
        .register(&queue, PROJECT, Arc::clone(&log))
        .expect("register project");
    let job_id = queue
        .enqueue(
            &log,
            PROJECT,
            &spec("unknown.kind", "orphan"),
            clock.as_ref(),
        )
        .expect("enqueue")
        .job_id();

    let pool = WorkerPool::start(
        &tokio::runtime::Handle::current(),
        Arc::clone(&queue),
        Arc::new(HandlerRegistry::new()),
        projects,
        clock,
        fast_pool_config(),
    );
    pool.wake();
    let dead = wait_until(|| state_of(&log, job_id) == Some(JobDurableState::Dead)).await;
    pool.shutdown().await;

    assert!(dead, "an unhandled job must not sit queued forever");
    let record = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(record.dead_reason_code.as_deref(), Some("refused"));
    assert_eq!(record.dead_reason_detail.as_deref(), Some("no_handler"));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_handler_is_recorded_as_a_failed_attempt_not_a_dead_process() {
    struct Panicking(JobKind);
    impl JobHandler for Panicking {
        fn kind(&self) -> &JobKind {
            &self.0
        }
        fn run(&self, _job: &ClaimedJob) -> Result<(), JobFailure> {
            panic!("a handler bug");
        }
    }

    let directory = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(SystemWallClock);
    let log = Arc::new(open_project(
        &directory.path().join("panic.pos"),
        PROJECT,
        &pos_foundation::ManualWallClock::starting_at(1_700_000_000_000),
    ));
    let queue = queue(30_000);
    let mut handlers = HandlerRegistry::new();
    handlers
        .register(Arc::new(Panicking(kind("boom"))))
        .expect("register handler");
    let projects = Arc::new(ProjectRegistry::new());
    projects
        .register(&queue, PROJECT, Arc::clone(&log))
        .expect("register project");
    let job_id = queue
        .enqueue(
            &log,
            PROJECT,
            &JobSpec::new(kind("boom"), "explodes")
                .with_class(JobClass::Foreground)
                .with_retry_count_max(0),
            clock.as_ref(),
        )
        .expect("enqueue")
        .job_id();

    let pool = WorkerPool::start(
        &tokio::runtime::Handle::current(),
        Arc::clone(&queue),
        Arc::new(handlers),
        projects,
        clock,
        fast_pool_config(),
    );
    pool.wake();
    let dead = wait_until(|| state_of(&log, job_id) == Some(JobDurableState::Dead)).await;
    pool.shutdown().await;

    assert!(dead, "the panicking job never reached a terminal state");
    let record = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(
        record.dead_reason_code.as_deref(),
        Some("retries_exhausted")
    );
    assert_eq!(
        record.dead_reason_detail.as_deref(),
        Some("handler_panicked"),
        "a handler bug is visible, not swallowed"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_foreground_worker_never_runs_maintenance_work() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = Arc::new(SystemWallClock);
    let log = Arc::new(open_project(
        &directory.path().join("classes.pos"),
        PROJECT,
        &pos_foundation::ManualWallClock::starting_at(1_700_000_000_000),
    ));
    let queue = queue(30_000);
    let ledger = Arc::new(RunLedger::default());
    let mut handlers = HandlerRegistry::new();
    handlers
        .register(Arc::new(ScriptedHandler::always_ok(
            kind("noop"),
            Arc::clone(&ledger),
        )))
        .expect("register handler");
    let projects = Arc::new(ProjectRegistry::new());
    projects
        .register(&queue, PROJECT, Arc::clone(&log))
        .expect("register project");
    let maintenance = queue
        .enqueue(
            &log,
            PROJECT,
            &JobSpec::new(kind("noop"), "maintenance-work").with_class(JobClass::Maintenance),
            clock.as_ref(),
        )
        .expect("enqueue")
        .job_id();

    // Only the foreground class has workers, so the maintenance job must
    // remain queued rather than leaking across the class boundary.
    let config = WorkerPoolConfig {
        classes: [
            ClassConfig {
                class: JobClass::Foreground,
                worker_count_max: 2,
            },
            ClassConfig {
                class: JobClass::Ingest,
                worker_count_max: 0,
            },
            ClassConfig {
                class: JobClass::Maintenance,
                worker_count_max: 0,
            },
        ],
        ..fast_pool_config()
    };
    let pool = WorkerPool::start(
        &tokio::runtime::Handle::current(),
        Arc::clone(&queue),
        Arc::new(handlers),
        projects,
        clock,
        config,
    );
    pool.wake();
    tokio::time::sleep(Duration::from_millis(300)).await;
    pool.shutdown().await;

    assert_eq!(state_of(&log, maintenance), Some(JobDurableState::Queued));
    assert_eq!(ledger.total(), 0);
}

#[test]
fn the_handler_registry_refuses_duplicates_and_states_its_bound() {
    let ledger = Arc::new(RunLedger::default());
    let mut registry = HandlerRegistry::new();
    registry
        .register(Arc::new(ScriptedHandler::always_ok(
            kind("noop"),
            Arc::clone(&ledger),
        )))
        .expect("first registration");
    let duplicate = registry
        .register(Arc::new(ScriptedHandler::always_ok(
            kind("noop"),
            Arc::clone(&ledger),
        )))
        .expect_err("a second handler for one kind is ambiguous routing");
    assert_eq!(
        duplicate,
        HandlerRegistryError::Duplicate {
            kind: "noop".to_owned()
        }
    );
    assert_eq!(registry.count(), 1);
}

fn state_of(log: &ProjectLog, job_id: JobId) -> Option<JobDurableState> {
    read_job(log, job_id)
        .expect("read job")
        .map(|record| record.state)
}

fn all_done(log: &ProjectLog, ids: &[JobId]) -> bool {
    ids.iter()
        .all(|job_id| state_of(log, *job_id) == Some(JobDurableState::Done))
}
