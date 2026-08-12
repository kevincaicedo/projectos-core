//! m0-s14 AC 1 — "duplicate enqueue with the same idempotency key executes
//! the handler exactly once".
//!
//! The property is proved over arbitrary interleavings of duplicate enqueues
//! and drain cycles against a real project log, so it covers the two
//! mechanisms independently: the admission read that answers duplicates
//! politely, and the derived-id primary key that refuses one anyway.

#![forbid(unsafe_code)]

mod common;

use common::{DEVICE, RunLedger, ScriptedHandler, kind, open_project, queue, spec, worker};
use pos_domain::{JobClass, JobDurableState, read_job};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_log::ProjectLog;
use pos_sched::{EnqueueOutcome, JobHandler, JobLiveState, JobQueue};
use proptest::prelude::*;
use std::sync::Arc;

const PROJECT: ProjectId = ProjectId::from_bytes([0x61; 16]);

/// One claim-run-complete cycle for every runnable job of the class.
fn drain(
    queue: &JobQueue,
    log: &ProjectLog,
    handler: &dyn JobHandler,
    ledger: &RunLedger,
    clock: &ManualWallClock,
) -> usize {
    let mut drained = 0;
    let worker = worker("drain-0");
    while let Some(job) = queue
        .claim(log, PROJECT, JobClass::Foreground, &worker, clock)
        .expect("claim")
    {
        let outcome = handler.run(&job);
        let _ = ledger;
        match outcome {
            Ok(()) => queue.complete(log, &job, 1, clock).expect("complete"),
            Err(failure) => {
                queue.fail(log, &job, &failure, 1, clock).expect("fail");
            }
        }
        drained += 1;
    }
    drained
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(24))]

    /// Arbitrary duplicate counts, arbitrary drain points: the handler still
    /// sees the work once, and every duplicate enqueue answers with the
    /// original job id rather than minting a second job.
    #[test]
    fn duplicate_enqueue_runs_the_handler_exactly_once(
        duplicate_count in 1_usize..8,
        drain_at in prop::collection::vec(any::<bool>(), 1..8),
    ) {
        let directory = tempfile::tempdir().expect("tempdir");
        let clock = ManualWallClock::starting_at(1_700_000_000_000);
        let log = open_project(&directory.path().join("idem.pos"), PROJECT, &clock);
        let queue = queue(30_000);
        queue.ensure_schema(&log).expect("lease schema");
        let ledger = Arc::new(RunLedger::default());
        let handler = ScriptedHandler::always_ok(kind("noop"), Arc::clone(&ledger));

        let job_spec = spec("noop", "the-one-unit-of-work");
        let mut ids = Vec::new();
        for index in 0..duplicate_count {
            let outcome = queue
                .enqueue(&log, PROJECT, &job_spec, &clock)
                .expect("enqueue");
            prop_assert_eq!(outcome.is_duplicate(), index > 0);
            ids.push(outcome.job_id());
            if drain_at.get(index).copied().unwrap_or(false) {
                drain(&queue, &log, &handler, &ledger, &clock);
            }
        }
        drain(&queue, &log, &handler, &ledger, &clock);

        let job_id = ids[0];
        prop_assert!(ids.iter().all(|candidate| *candidate == job_id));
        prop_assert_eq!(ledger.count_for(&job_id.to_hex()), 1);
        prop_assert_eq!(ledger.total(), 1);
        let record = read_job(&log, job_id).expect("read job").expect("job exists");
        prop_assert_eq!(record.state, JobDurableState::Done);
        prop_assert_eq!(record.attempt_count, 1);
        // The whole queue holds exactly one row for this work.
        let all = pos_domain::list_jobs(&log, pos_domain::JobListFilter::default())
            .expect("list jobs");
        prop_assert_eq!(all.len(), 1);
    }
}

#[test]
fn the_same_key_in_a_different_project_or_kind_is_different_work() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("scope.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");

    let first = queue
        .enqueue(&log, PROJECT, &spec("alpha", "shared-key"), &clock)
        .expect("enqueue alpha");
    let second = queue
        .enqueue(&log, PROJECT, &spec("beta", "shared-key"), &clock)
        .expect("enqueue beta");
    let other_project = ProjectId::from_bytes([0x62; 16]);
    let third = queue
        .enqueue(&log, other_project, &spec("alpha", "shared-key"), &clock)
        .expect("enqueue alpha for another project");

    assert!(matches!(first, EnqueueOutcome::Enqueued(_)));
    assert!(matches!(second, EnqueueOutcome::Enqueued(_)));
    assert!(matches!(third, EnqueueOutcome::Enqueued(_)));
    assert_ne!(first.job_id(), second.job_id());
    assert_ne!(first.job_id(), third.job_id());
}

#[test]
fn a_completed_job_stays_completed_when_its_key_is_enqueued_again() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("replay.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let ledger = Arc::new(RunLedger::default());
    let handler = ScriptedHandler::always_ok(kind("noop"), Arc::clone(&ledger));

    let job_spec = spec("noop", "once-ever");
    let job_id = queue
        .enqueue(&log, PROJECT, &job_spec, &clock)
        .expect("enqueue")
        .job_id();
    drain(&queue, &log, &handler, &ledger, &clock);

    // The key is permanent: recurrence is expressed by varying the key (cron
    // uses the nominal fire instant), never by re-using a spent one.
    let repeat = queue
        .enqueue(&log, PROJECT, &job_spec, &clock)
        .expect("enqueue again");
    assert_eq!(repeat, EnqueueOutcome::Duplicate(job_id));
    assert_eq!(drain(&queue, &log, &handler, &ledger, &clock), 0);
    assert_eq!(ledger.total(), 1);
    assert_eq!(
        queue
            .live_state(&log, job_id, &clock)
            .expect("live state")
            .expect("job exists"),
        JobLiveState::Done
    );
    let _ = DEVICE;
}

#[test]
fn replaying_the_log_rebuilds_the_queue_byte_for_byte() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("rebuild.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let ledger = Arc::new(RunLedger::default());
    let handler = ScriptedHandler::failing(kind("noop"), Arc::clone(&ledger), 1);

    for index in 0..4 {
        queue
            .enqueue(
                &log,
                PROJECT,
                &spec("noop", &format!("work-{index}")),
                &clock,
            )
            .expect("enqueue");
    }
    drain(&queue, &log, &handler, &ledger, &clock);

    // The queue is a projection: replaying the log must reproduce it exactly,
    // which is what makes crash recovery "reopen the project" and nothing else.
    let before = log.dump_projections().expect("dump before");
    log.rebuild_projections().expect("rebuild");
    let after = log.dump_projections().expect("dump after");
    assert_eq!(before, after);
    assert!(log.verify_projections().expect("verify").is_clean());
}
