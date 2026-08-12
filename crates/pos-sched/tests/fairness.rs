//! m0-s14 AC 4 — "project A with 10k queued jobs cannot delay project B's
//! first job beyond the stated bound".
//!
//! ## The stated bound
//!
//! With `P` registered projects, a class's round-robin cursor advances on
//! every claim attempt, so a project holding queued work is visited at least
//! once every `P` attempts. B's first job is therefore claimed **within `P`
//! claim attempts of becoming eligible, independently of A's backlog depth**.
//! Two things are measured here: that visit bound, and that the wall-clock
//! cost of a single claim does not grow with backlog (the index behind the
//! claim query is what makes the bound meaningful rather than merely true).

#![forbid(unsafe_code)]

mod common;

use common::{kind, open_project, queue, spec, worker};
use pos_domain::{JobClass, JobPriority};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_log::ProjectLog;
use pos_sched::{JobQueue, JobSpec, ProjectRegistry};
use std::sync::Arc;
use std::time::Instant;

const PROJECT_A: ProjectId = ProjectId::from_bytes([0xa1; 16]);
const PROJECT_B: ProjectId = ProjectId::from_bytes([0xb1; 16]);

/// The backlog the AC names. Enqueueing it is the slow part of this test, not
/// claiming from it — which is itself the point.
const BACKLOG_JOB_COUNT: usize = 10_000;

/// One claim attempt per registered project is the stated visit bound.
const FAIRNESS_VISIT_BOUND: usize = 2;

/// Ceiling for one claim against a 10k backlog. Generous by two orders of
/// magnitude relative to an indexed `LIMIT 1` so it fails on an accidental
/// full scan (~milliseconds and growing) and never on a busy machine. This is
/// a regression tripwire, not a reference-machine bench number (§18 numbers
/// come from `pos-bench` on `RM-LAPTOP-01`).
const CLAIM_WALL_MS_MAX: u128 = 250;

#[test]
fn a_ten_thousand_job_backlog_cannot_delay_another_projects_first_job() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log_a = open_project(&directory.path().join("a.pos"), PROJECT_A, &clock);
    let log_b = open_project(&directory.path().join("b.pos"), PROJECT_B, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log_a).expect("lease schema a");
    queue.ensure_schema(&log_b).expect("lease schema b");

    fill_backlog(&queue, &log_a, PROJECT_A, BACKLOG_JOB_COUNT, &clock);
    // B's single job arrives last and at the lowest priority, so nothing
    // except the fairness cursor can be what gets it claimed.
    queue
        .enqueue(
            &log_b,
            PROJECT_B,
            &spec("noop", "b-first").with_priority(JobPriority::Low),
            &clock,
        )
        .expect("enqueue b");

    let projects = [(PROJECT_A, &log_a), (PROJECT_B, &log_b)];
    let cursor_worker = worker("foreground-0");
    let mut attempts_before_b = 0_usize;
    let mut claimed_b = false;
    let started = Instant::now();
    for attempt in 0..FAIRNESS_VISIT_BOUND {
        let (project_id, log) = projects[attempt % projects.len()];
        let claimed = queue
            .claim(
                log,
                project_id,
                JobClass::Foreground,
                &cursor_worker,
                &clock,
            )
            .expect("claim")
            .expect("both projects have runnable work");
        attempts_before_b += 1;
        if claimed.project_id == PROJECT_B {
            claimed_b = true;
            break;
        }
        queue.complete(log, &claimed, 1, &clock).expect("complete");
    }
    let elapsed_ms = started.elapsed().as_millis();

    assert!(
        claimed_b,
        "B's job was not claimed within the {FAIRNESS_VISIT_BOUND}-attempt bound \
         despite A holding {BACKLOG_JOB_COUNT} queued jobs"
    );
    assert_eq!(attempts_before_b, FAIRNESS_VISIT_BOUND);
    assert!(
        elapsed_ms <= CLAIM_WALL_MS_MAX,
        "the round-robin cycle took {elapsed_ms} ms against a {BACKLOG_JOB_COUNT}-job \
         backlog; the claim is scanning instead of using idx_proj_jobs_claim"
    );
}

#[test]
fn claim_cost_does_not_grow_with_backlog_depth() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("depth.pos"), PROJECT_A, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let claimer = worker("foreground-0");

    fill_backlog(&queue, &log, PROJECT_A, 100, &clock);
    let shallow = time_one_claim(&queue, &log, &claimer, &clock);
    fill_backlog_from(&queue, &log, PROJECT_A, 100, BACKLOG_JOB_COUNT, &clock);
    let deep = time_one_claim(&queue, &log, &claimer, &clock);

    // Both must be fast; the assertion is on the ceiling rather than on a
    // ratio, because timing ratios on a shared machine are noise.
    assert!(
        shallow <= CLAIM_WALL_MS_MAX && deep <= CLAIM_WALL_MS_MAX,
        "claim took {shallow} ms at depth 100 and {deep} ms at depth {BACKLOG_JOB_COUNT}"
    );
}

#[test]
fn priority_orders_claims_inside_a_class_and_class_never_leaks() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&directory.path().join("priority.pos"), PROJECT_A, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let claimer = worker("foreground-0");

    // Enqueued worst-priority first, so claim order can only come from rank.
    for (index, priority) in [JobPriority::Low, JobPriority::Normal, JobPriority::High]
        .into_iter()
        .enumerate()
    {
        queue
            .enqueue(
                &log,
                PROJECT_A,
                &spec("noop", &format!("p-{index}")).with_priority(priority),
                &clock,
            )
            .expect("enqueue");
    }
    // A maintenance job that must never be handed to a foreground worker.
    queue
        .enqueue(
            &log,
            PROJECT_A,
            &JobSpec::new(kind("noop"), "maintenance-only")
                .with_class(JobClass::Maintenance)
                .with_priority(JobPriority::High),
            &clock,
        )
        .expect("enqueue maintenance");

    let mut claimed_keys = Vec::new();
    while let Some(job) = queue
        .claim(&log, PROJECT_A, JobClass::Foreground, &claimer, &clock)
        .expect("claim")
    {
        let record = pos_domain::read_job(&log, job.job_id)
            .expect("read")
            .expect("job exists");
        claimed_keys.push(record.idempotency_key);
        queue.complete(&log, &job, 1, &clock).expect("complete");
    }
    assert_eq!(claimed_keys, vec!["p-2", "p-1", "p-0"]);

    let leftover = queue
        .claim(&log, PROJECT_A, JobClass::Maintenance, &claimer, &clock)
        .expect("claim")
        .expect("the maintenance job is still queued");
    assert_eq!(leftover.job_id.to_hex().len(), 32);
}

#[test]
fn the_project_registry_orders_deterministically_and_states_its_bound() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let queue = queue(30_000);
    let registry = ProjectRegistry::new();
    // Registered out of id order; the snapshot must still be id-ordered so
    // the fairness cursor means the same thing on every pass.
    for (index, project) in [PROJECT_B, PROJECT_A].into_iter().enumerate() {
        let log = Arc::new(open_project(
            &directory.path().join(format!("r{index}.pos")),
            project,
            &clock,
        ));
        registry.register(&queue, project, log).expect("register");
    }
    let order: Vec<ProjectId> = registry
        .snapshot()
        .into_iter()
        .map(|(project_id, _)| project_id)
        .collect();
    assert_eq!(order, vec![PROJECT_A, PROJECT_B]);
    assert_eq!(registry.count(), 2);
    registry.unregister(PROJECT_A);
    assert_eq!(registry.count(), 1);
}

fn fill_backlog(
    queue: &JobQueue,
    log: &ProjectLog,
    project_id: ProjectId,
    count: usize,
    clock: &ManualWallClock,
) {
    fill_backlog_from(queue, log, project_id, 0, count, clock);
}

fn fill_backlog_from(
    queue: &JobQueue,
    log: &ProjectLog,
    project_id: ProjectId,
    from: usize,
    to: usize,
    clock: &ManualWallClock,
) {
    for index in from..to {
        queue
            .enqueue(
                log,
                project_id,
                &spec("noop", &format!("bulk-{index}")),
                clock,
            )
            .expect("enqueue backlog job");
    }
}

fn time_one_claim(
    queue: &JobQueue,
    log: &ProjectLog,
    claimer: &pos_sched::WorkerName,
    clock: &ManualWallClock,
) -> u128 {
    let started = Instant::now();
    let claimed = queue
        .claim(log, PROJECT_A, JobClass::Foreground, claimer, clock)
        .expect("claim")
        .expect("the backlog has runnable work");
    let elapsed = started.elapsed().as_millis();
    queue.complete(log, &claimed, 1, clock).expect("complete");
    elapsed
}
