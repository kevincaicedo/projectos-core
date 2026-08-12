//! m0-s14 AC 2 — "worker killed mid-job ⇒ lease expiry ⇒ retry succeeds;
//! `Dead` jobs carry their reason and are queryable".
//!
//! A killed worker is modelled the way a kill actually looks to the queue: a
//! job is claimed, the handler starts, and then *nothing else is ever
//! written*. The process is then dropped and the project reopened, so the
//! recovery under test is the durable one — reopen, reap, re-claim — and not
//! an in-memory shortcut.

#![forbid(unsafe_code)]

mod common;

use common::{RunLedger, ScriptedHandler, kind, open_project, queue, spec, worker};
use pos_domain::{JobClass, JobDurableState, JobListFilter, list_jobs, read_job};
use pos_foundation::{ManualWallClock, ProjectId, WallClock};
use pos_sched::{FailureOutcome, JobHandler, JobLiveState};
use std::sync::Arc;

const PROJECT: ProjectId = ProjectId::from_bytes([0x71; 16]);
const LEASE_TTL_MS: u64 = 30_000;

#[test]
fn a_killed_worker_loses_its_lease_and_the_retry_succeeds() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("kill.pos");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let queue = queue(LEASE_TTL_MS);
    let ledger = Arc::new(RunLedger::default());
    let handler = ScriptedHandler::always_ok(kind("noop"), Arc::clone(&ledger));

    let job_id = {
        let log = open_project(&root, PROJECT, &clock);
        queue.ensure_schema(&log).expect("lease schema");
        let job_id = queue
            .enqueue(&log, PROJECT, &spec("noop", "survives-a-kill"), &clock)
            .expect("enqueue")
            .job_id();
        let claimed = queue
            .claim(
                &log,
                PROJECT,
                JobClass::Foreground,
                &worker("victim-0"),
                &clock,
            )
            .expect("claim")
            .expect("a runnable job");
        assert_eq!(claimed.attempt_index, 1);
        assert_eq!(
            queue.live_state(&log, job_id, &clock).expect("state"),
            Some(JobLiveState::Running)
        );
        // The handler "starts" and the worker dies: nothing further is written.
        ledger.record(&claimed);
        job_id
    };

    // Reopen: the durable state alone must carry the recovery.
    let log = open_project(&root, PROJECT, &clock);
    queue.ensure_schema(&log).expect("lease schema");
    assert_eq!(
        queue.live_state(&log, job_id, &clock).expect("state"),
        Some(JobLiveState::Running),
        "a lease outlives the process that took it until it expires"
    );
    assert!(
        queue
            .claim(
                &log,
                PROJECT,
                JobClass::Foreground,
                &worker("next-0"),
                &clock
            )
            .expect("claim")
            .is_none(),
        "a job under an unexpired lease must not be claimed twice"
    );

    clock.advance_ms(LEASE_TTL_MS + 1);
    assert_eq!(queue.reap_expired_leases(&log, &clock).expect("reap"), 1);

    let after_reap = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(after_reap.attempt_count, 1, "the dead attempt was counted");
    assert_eq!(after_reap.last_error_code.as_deref(), Some("lease_expired"));
    assert_eq!(after_reap.state, JobDurableState::Queued);

    // Backoff applies to the recovered attempt too — a crash loop must not
    // become a hot loop.
    assert!(after_reap.run_at_ts_ms > clock.now_ms());
    clock.advance_ms(after_reap.run_at_ts_ms - clock.now_ms() + 1);

    let retried = queue
        .claim(
            &log,
            PROJECT,
            JobClass::Foreground,
            &worker("next-0"),
            &clock,
        )
        .expect("claim")
        .expect("the job is runnable again");
    assert_eq!(retried.attempt_index, 2);
    handler.run(&retried).expect("scripted success");
    queue.complete(&log, &retried, 5, &clock).expect("complete");

    let done = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(done.state, JobDurableState::Done);
    assert_eq!(done.attempt_count, 2);
    assert_eq!(ledger.total(), 2, "at-least-once delivered exactly twice");
    assert!(log.verify_projections().expect("verify").is_clean());
}

#[test]
fn reaping_twice_cannot_double_count_an_attempt() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("reap-twice.pos"), PROJECT, &clock);
    let queue = queue(LEASE_TTL_MS);
    queue.ensure_schema(&log).expect("lease schema");
    let job_id = queue
        .enqueue(&log, PROJECT, &spec("noop", "reaped"), &clock)
        .expect("enqueue")
        .job_id();
    let claimed = queue
        .claim(
            &log,
            PROJECT,
            JobClass::Foreground,
            &worker("victim-0"),
            &clock,
        )
        .expect("claim")
        .expect("a runnable job");
    assert_eq!(claimed.attempt_index, 1);

    clock.advance_ms(LEASE_TTL_MS + 1);
    assert_eq!(queue.reap_expired_leases(&log, &clock).expect("reap"), 1);
    assert_eq!(
        queue
            .reap_expired_leases(&log, &clock)
            .expect("second reap"),
        0,
        "the lease is gone, so the second sweep has nothing to do"
    );
    let record = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(record.attempt_count, 1);
}

#[test]
fn an_exhausted_retry_budget_lands_in_the_dlq_with_a_queryable_reason() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("dlq.pos"), PROJECT, &clock);
    let queue = queue(LEASE_TTL_MS);
    queue.ensure_schema(&log).expect("lease schema");
    let ledger = Arc::new(RunLedger::default());
    let handler = ScriptedHandler::failing(kind("noop"), Arc::clone(&ledger), u32::MAX);

    let job_spec = spec("noop", "always-fails").with_retry_count_max(2);
    let job_id = queue
        .enqueue(&log, PROJECT, &job_spec, &clock)
        .expect("enqueue")
        .job_id();

    let mut outcomes = Vec::new();
    for _ in 0..3 {
        let claimed = queue
            .claim(&log, PROJECT, JobClass::Foreground, &worker("w-0"), &clock)
            .expect("claim")
            .expect("job is runnable");
        let failure = handler.run(&claimed).expect_err("scripted failure");
        let outcome = queue
            .fail(&log, &claimed, &failure, 3, &clock)
            .expect("fail");
        outcomes.push(outcome);
        if let FailureOutcome::Retrying { retry_at_ts_ms } = outcome {
            clock.advance_ms(retry_at_ts_ms.saturating_sub(clock.now_ms()) + 1);
        }
    }

    assert!(matches!(outcomes[0], FailureOutcome::Retrying { .. }));
    assert!(matches!(outcomes[1], FailureOutcome::Retrying { .. }));
    assert_eq!(outcomes[2], FailureOutcome::Dead);

    let dead = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(dead.state, JobDurableState::Dead);
    assert_eq!(dead.attempt_count, 3);
    assert_eq!(dead.dead_reason_code.as_deref(), Some("retries_exhausted"));
    assert_eq!(dead.dead_reason_detail.as_deref(), Some("scripted"));
    assert_eq!(
        queue.live_state(&log, job_id, &clock).expect("state"),
        Some(JobLiveState::Dead)
    );

    // Queryable: the DLQ is a filter on the same read surface `job.list` uses.
    let dlq = list_jobs(
        &log,
        JobListFilter {
            state: Some(JobDurableState::Dead),
            ..JobListFilter::default()
        },
    )
    .expect("list dead jobs");
    assert_eq!(dlq.len(), 1);
    assert_eq!(dlq[0].job_id, job_id);

    // A dead job is never claimed again.
    assert!(
        queue
            .claim(&log, PROJECT, JobClass::Foreground, &worker("w-0"), &clock)
            .expect("claim")
            .is_none()
    );
    assert!(log.verify_projections().expect("verify").is_clean());
}

#[test]
fn a_permanent_refusal_skips_the_retry_budget_entirely() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("refused.pos"), PROJECT, &clock);
    let queue = queue(LEASE_TTL_MS);
    queue.ensure_schema(&log).expect("lease schema");
    let ledger = Arc::new(RunLedger::default());
    let handler = ScriptedHandler::refusing(kind("noop"), Arc::clone(&ledger));

    let job_id = queue
        .enqueue(
            &log,
            PROJECT,
            &spec("noop", "refused").with_retry_count_max(5),
            &clock,
        )
        .expect("enqueue")
        .job_id();
    let claimed = queue
        .claim(&log, PROJECT, JobClass::Foreground, &worker("w-0"), &clock)
        .expect("claim")
        .expect("job is runnable");
    let failure = handler.run(&claimed).expect_err("scripted refusal");
    assert_eq!(
        queue
            .fail(&log, &claimed, &failure, 1, &clock)
            .expect("fail"),
        FailureOutcome::Dead
    );

    let dead = read_job(&log, job_id).expect("read").expect("job exists");
    assert_eq!(dead.dead_reason_code.as_deref(), Some("refused"));
    assert_eq!(ledger.total(), 1, "a refusal is not retried");
}
