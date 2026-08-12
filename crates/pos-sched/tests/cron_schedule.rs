//! m0-s14 AC 3 — "DST spring-forward/fall-back cases for two timezones; all
//! three overlap policies behave per spec; next-10-runs preview matches
//! actual firings in a simulated clock run".
//!
//! Expected instants are written as explicit UTC milliseconds computed from
//! the published transition rules, not from the library under test — a test
//! that asks the implementation what the answer is proves nothing.

#![forbid(unsafe_code)]

mod common;

use common::{kind, open_project, queue, worker};
use pos_domain::{CronOverlapPolicy, JobClass, JobDurableState, JobPriority, read_job};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_sched::{CronDriver, CronSchedule, CronSpec, derive_cron_id, preview_registered};
use std::sync::Arc;

const PROJECT: ProjectId = ProjectId::from_bytes([0x81; 16]);
const HOUR_MS: i64 = 3_600_000;
const DAY_MS: i64 = 24 * HOUR_MS;

// Local midnight on each 2026 transition day, as a UTC instant. Each is the
// epoch-day number times 86 400 s plus the zone's pre-transition offset, so
// the constant is checkable by hand without running anything.
/// 2026-03-08T00:00-05:00 (America/New_York): the US springs forward at 02:00
/// local, so 02:30 does not exist that day.
const NEW_YORK_SPRING_DAY_START_UTC_MS: i64 = 1_772_946_000_000;
/// 2026-11-01T00:00-04:00: the US falls back at 02:00 local, so 01:30 happens
/// twice that day.
const NEW_YORK_FALL_DAY_START_UTC_MS: i64 = 1_793_505_600_000;
/// 2026-03-29T00:00+01:00 (Europe/Berlin): springs forward at 02:00 local.
const BERLIN_SPRING_DAY_START_UTC_MS: i64 = 1_774_738_800_000;
/// 2026-10-25T00:00+02:00: falls back at 03:00 local, so 02:30 happens twice.
const BERLIN_FALL_DAY_START_UTC_MS: i64 = 1_792_879_200_000;

#[test]
fn spring_forward_fires_once_shifted_past_the_gap_in_two_zones() {
    // New York: 02:30 EST does not exist on 2026-03-08, so the firing shifts
    // forward by the gap to 03:30 EDT. Measured from local midnight that is
    // 2.5 absolute hours, not 3.5 — the missing hour is the whole point.
    let new_york = CronSchedule::new("30 2 * * *", "America/New_York").expect("valid");
    let fire = new_york
        .next_fire_after(NEW_YORK_SPRING_DAY_START_UTC_MS)
        .expect("a firing on the transition day");
    assert_eq!(
        fire,
        NEW_YORK_SPRING_DAY_START_UTC_MS + 2 * HOUR_MS + HOUR_MS / 2
    );
    // Exactly one firing that day: the next one is the following 02:30 EDT,
    // which is 23 hours later in absolute time.
    let next = new_york.next_fire_after(fire).expect("the next day");
    assert_eq!(next - fire, 23 * HOUR_MS);

    // Berlin: same shape, 02:30 CET → 03:30 CEST, again 2.5 absolute hours
    // after local midnight.
    let berlin = CronSchedule::new("30 2 * * *", "Europe/Berlin").expect("valid");
    let fire = berlin
        .next_fire_after(BERLIN_SPRING_DAY_START_UTC_MS)
        .expect("a firing on the transition day");
    assert_eq!(
        fire,
        BERLIN_SPRING_DAY_START_UTC_MS + 2 * HOUR_MS + HOUR_MS / 2
    );
    assert_eq!(
        berlin.next_fire_after(fire).expect("the next day") - fire,
        23 * HOUR_MS
    );
}

#[test]
fn fall_back_fires_once_on_the_first_occurrence_in_two_zones() {
    // New York: 01:30 happens twice on 2026-11-01. The earlier one (EDT) is
    // 05:30 UTC; the schedule must not fire again at 06:30 UTC.
    let new_york = CronSchedule::new("30 1 * * *", "America/New_York").expect("valid");
    let fire = new_york
        .next_fire_after(NEW_YORK_FALL_DAY_START_UTC_MS)
        .expect("a firing on the transition day");
    assert_eq!(fire, NEW_YORK_FALL_DAY_START_UTC_MS + HOUR_MS + HOUR_MS / 2);
    let next = new_york.next_fire_after(fire).expect("the next day");
    assert_eq!(
        next - fire,
        25 * HOUR_MS,
        "the repeated hour lengthens the day, it does not double the firing"
    );

    // Berlin falls back at 03:00 local, so 02:30 is the ambiguous civil time.
    // The earlier (CEST) occurrence is 2.5 hours after local midnight.
    let berlin = CronSchedule::new("30 2 * * *", "Europe/Berlin").expect("valid");
    let fire = berlin
        .next_fire_after(BERLIN_FALL_DAY_START_UTC_MS)
        .expect("a firing on the transition day");
    assert_eq!(
        fire,
        BERLIN_FALL_DAY_START_UTC_MS + 2 * HOUR_MS + HOUR_MS / 2
    );
    assert_eq!(
        berlin.next_fire_after(fire).expect("the next day") - fire,
        25 * HOUR_MS
    );
}

#[test]
fn a_daily_schedule_holds_its_local_hour_across_a_transition() {
    // The property a user actually cares about: "03:00 every day" stays at
    // 03:00 local, which means the absolute gap between firings changes.
    let berlin = CronSchedule::new("0 3 * * *", "Europe/Berlin").expect("valid");
    let before_spring = BERLIN_SPRING_DAY_START_UTC_MS - DAY_MS;
    let runs = berlin.preview(before_spring, 3);
    assert_eq!(runs.len(), 3);
    assert_eq!(
        runs[1] - runs[0],
        23 * HOUR_MS,
        "the night that springs forward is an hour shorter"
    );
    assert_eq!(runs[2] - runs[1], 24 * HOUR_MS);
}

#[test]
fn the_preview_matches_the_firings_a_simulated_clock_actually_produces() {
    let directory = tempfile::tempdir().expect("tempdir");
    let start_ms = 1_772_946_000_000_u64;
    let clock = ManualWallClock::starting_at(start_ms);
    let log = open_project(&directory.path().join("preview.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let driver = CronDriver::new(Arc::clone(&queue));

    let cron_id = derive_cron_id(PROJECT, "hourly-digest");
    let spec = CronSpec {
        cron_id,
        job_kind: kind("digest"),
        expr: "0 * * * *".to_owned(),
        tz: "America/New_York".to_owned(),
        overlap_policy: CronOverlapPolicy::Queue,
        enabled: true,
        priority: JobPriority::Normal,
        class: JobClass::Maintenance,
        payload: Vec::new(),
    };
    driver
        .register(&log, PROJECT, &spec, &clock)
        .expect("register");

    // The preview is taken before anything runs — it is a claim about the
    // future, and the simulated run is what tests that claim.
    let predicted = preview_registered(
        &log,
        cron_id,
        i64::try_from(start_ms).expect("in range"),
        10,
    )
    .expect("preview")
    .expect("the schedule is registered");
    assert_eq!(predicted.len(), 10);

    // Advance one hour at a time and record what each tick actually fired.
    let mut observed = Vec::new();
    for _ in 0..10 {
        clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
        let report = driver.tick(&log, PROJECT, &clock).expect("tick");
        for job_id in report.fired {
            let record = read_job(&log, job_id).expect("read").expect("job exists");
            observed.push(i64::try_from(record.run_at_ts_ms).expect("in range"));
        }
        assert_eq!(report.missed_tick_count, 0);
    }
    assert_eq!(observed, predicted);
}

#[test]
fn skip_does_not_stack_a_second_job_while_the_first_is_unfinished() {
    let (directory, clock, log, driver, queue) = cron_fixture(CronOverlapPolicy::Skip);
    let cron_id = derive_cron_id(PROJECT, "overlap");

    clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
    let first = driver.tick(&log, PROJECT, &clock).expect("tick");
    assert_eq!(first.fired.len(), 1);

    // The first job is still queued when the next hour comes round.
    clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
    let second = driver.tick(&log, PROJECT, &clock).expect("tick");
    assert!(second.fired.is_empty());
    assert_eq!(second.skipped_overlap_count, 1);

    // Once it finishes, the schedule resumes.
    let claimed = queue
        .claim(&log, PROJECT, JobClass::Maintenance, &worker("w-0"), &clock)
        .expect("claim")
        .expect("the first job");
    queue.complete(&log, &claimed, 1, &clock).expect("complete");
    clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
    let third = driver.tick(&log, PROJECT, &clock).expect("tick");
    assert_eq!(third.fired.len(), 1);
    assert_eq!(third.skipped_overlap_count, 0);
    let _ = (directory, cron_id);
}

#[test]
fn queue_stacks_every_firing_as_its_own_job() {
    let (directory, clock, log, driver, _queue) = cron_fixture(CronOverlapPolicy::Queue);
    for _ in 0..3 {
        clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
        let report = driver.tick(&log, PROJECT, &clock).expect("tick");
        assert_eq!(report.fired.len(), 1);
        assert_eq!(report.skipped_overlap_count, 0);
    }
    let queued = pos_domain::list_jobs(
        &log,
        pos_domain::JobListFilter {
            state: Some(JobDurableState::Queued),
            ..pos_domain::JobListFilter::default()
        },
    )
    .expect("list");
    assert_eq!(queued.len(), 3);
    let _ = directory;
}

#[test]
fn cancel_previous_retires_the_older_job_with_a_typed_reason() {
    let (directory, clock, log, driver, _queue) = cron_fixture(CronOverlapPolicy::CancelPrevious);

    clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
    let first = driver.tick(&log, PROJECT, &clock).expect("tick");
    let superseded_job = first.fired[0];

    clock.advance_ms(u64::try_from(HOUR_MS).expect("in range"));
    let second = driver.tick(&log, PROJECT, &clock).expect("tick");
    assert_eq!(second.superseded_count, 1);
    assert_eq!(second.fired.len(), 1);

    let old = read_job(&log, superseded_job)
        .expect("read")
        .expect("job exists");
    assert_eq!(old.state, JobDurableState::Dead);
    assert_eq!(old.dead_reason_code.as_deref(), Some("superseded_by_cron"));
    let fresh = read_job(&log, second.fired[0])
        .expect("read")
        .expect("job exists");
    assert_eq!(fresh.state, JobDurableState::Queued);
    assert!(log.verify_projections().expect("verify").is_clean());
    let _ = directory;
}

#[test]
fn a_disabled_schedule_fires_nothing() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_772_946_000_000);
    let log = open_project(&directory.path().join("disabled.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let driver = CronDriver::new(Arc::clone(&queue));
    let spec = CronSpec {
        cron_id: derive_cron_id(PROJECT, "off"),
        job_kind: kind("digest"),
        expr: "0 * * * *".to_owned(),
        tz: "UTC".to_owned(),
        overlap_policy: CronOverlapPolicy::Queue,
        enabled: false,
        priority: JobPriority::Normal,
        class: JobClass::Maintenance,
        payload: Vec::new(),
    };
    driver
        .register(&log, PROJECT, &spec, &clock)
        .expect("register");
    clock.advance_ms(u64::try_from(4 * HOUR_MS).expect("in range"));
    let report = driver.tick(&log, PROJECT, &clock).expect("tick");
    assert!(report.fired.is_empty());
}

#[test]
fn an_unparseable_schedule_is_refused_before_it_becomes_durable() {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_772_946_000_000);
    let log = open_project(&directory.path().join("bad-cron.pos"), PROJECT, &clock);
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let driver = CronDriver::new(Arc::clone(&queue));
    let head_before = log.head().expect("head");
    let spec = CronSpec {
        cron_id: derive_cron_id(PROJECT, "broken"),
        job_kind: kind("digest"),
        expr: "0 25 * * *".to_owned(),
        tz: "UTC".to_owned(),
        overlap_policy: CronOverlapPolicy::Skip,
        enabled: true,
        priority: JobPriority::Normal,
        class: JobClass::Maintenance,
        payload: Vec::new(),
    };
    assert!(driver.register(&log, PROJECT, &spec, &clock).is_err());
    assert_eq!(log.head().expect("head"), head_before);
}

type CronFixture = (
    tempfile::TempDir,
    ManualWallClock,
    pos_log::ProjectLog,
    CronDriver,
    Arc<pos_sched::JobQueue>,
);

fn cron_fixture(policy: CronOverlapPolicy) -> CronFixture {
    let directory = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_772_946_000_000);
    let log = open_project(
        &directory.path().join(format!("{}.pos", policy.as_str())),
        PROJECT,
        &clock,
    );
    let queue = queue(30_000);
    queue.ensure_schema(&log).expect("lease schema");
    let driver = CronDriver::new(Arc::clone(&queue));
    let spec = CronSpec {
        cron_id: derive_cron_id(PROJECT, "overlap"),
        job_kind: kind("digest"),
        expr: "0 * * * *".to_owned(),
        tz: "UTC".to_owned(),
        overlap_policy: policy,
        enabled: true,
        priority: JobPriority::Normal,
        class: JobClass::Maintenance,
        payload: Vec::new(),
    };
    driver
        .register(&log, PROJECT, &spec, &clock)
        .expect("register");
    (directory, clock, log, driver, queue)
}
