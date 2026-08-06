//! Domain-level determinism oracle (m0-s03, §18 replay gate): the real v0
//! kinds + projections under a seeded synthetic corpus large enough to cross
//! many snapshot boundaries. Incremental apply, full rebuild, and snapshot +
//! tail must all agree byte-for-byte; verify must stay green; and the run/
//! job state machines must land in queryable, sane projection rows.

#![forbid(unsafe_code)]

use pos_domain::{DomainEvent, ProjectCreatedBody, SyntheticEvents, v0_registry};
use pos_foundation::{DeviceId, ManualWallClock, UserId};
use pos_log::{Actor, AppendRequest, LogConfig, ProjectLog};
use pos_store::ProjectStore;

/// Enough events to cross several snapshot boundaries at the test cadence,
/// small enough to keep the PR lane fast; the 100k/1M scale runs in the CLI
/// e2e and `pos-bench` respectively.
const CORPUS_EVENT_COUNT: usize = 5_000;
const TEST_SNAPSHOT_CADENCE: u64 = 1_000;
const APPEND_BATCH_LEN: usize = 500;

fn synthetic_log(directory: &tempfile::TempDir) -> ProjectLog {
    let root = directory.path().join("domain.pos");
    let store = ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(1_000))
        .expect("create store");
    let project_id = store.manifest().project_id;
    let log = ProjectLog::open(
        store,
        v0_registry().expect("v0 registry is well-formed"),
        LogConfig {
            snapshot_cadence_events: TEST_SNAPSHOT_CADENCE,
        },
    )
    .expect("open log");

    let clock = ManualWallClock::starting_at(10_000);
    // The creation fact comes first, exactly as `project.create` appends it:
    // the generator only produces post-creation history.
    let created = DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
        project_id,
        name: "Synthetic Project".to_owned(),
        template: "generic".to_owned(),
    })
    .into_request(
        DeviceId::from_bytes([1; 16]),
        Actor::User(UserId::from_bytes([0xa1; 16])),
    )
    .expect("creation request");
    log.append(created, &clock).expect("append creation fact");

    let mut generator = SyntheticEvents::new(7, project_id);
    let mut appended = 0;
    while appended < CORPUS_EVENT_COUNT {
        let batch_len = APPEND_BATCH_LEN.min(CORPUS_EVENT_COUNT - appended);
        let requests: Vec<AppendRequest> = (0..batch_len)
            .map(|_| generator.next_request().expect("synthetic request"))
            .collect();
        log.append_batch(&requests, &clock).expect("append batch");
        appended += batch_len;
        clock.advance_ms(311);
    }
    log
}

#[test]
fn v0_projections_replay_byte_identical_across_all_three_paths() {
    let directory = tempfile::tempdir().expect("tempdir");
    let log = synthetic_log(&directory);
    let expected_events = CORPUS_EVENT_COUNT as u64 + 1;
    assert_eq!(log.event_count().expect("count"), expected_events);

    let incremental = log.dump_projections().expect("dump incremental");
    log.rebuild_projections().expect("full rebuild");
    let rebuilt = log.dump_projections().expect("dump rebuilt");
    assert_eq!(
        incremental, rebuilt,
        "full rebuild diverged from live apply"
    );

    log.restore_from_snapshot_and_tail()
        .expect("snapshot + tail");
    let restored = log.dump_projections().expect("dump restored");
    assert_eq!(incremental, restored, "snapshot + tail diverged");

    let report = log.verify_projections().expect("verify");
    assert!(report.is_clean(), "verify after equality: {report:?}");
    assert_eq!(report.events_replayed, expected_events);

    let snapshots = log.snapshot_state().expect("snapshot state");
    assert!(
        snapshots.snapshot_count > 0,
        "cadence must have produced snapshots"
    );
    assert!(
        snapshots.snapshot_count <= 2,
        "pruning must bound snapshot history"
    );
}

#[test]
fn v0_state_machines_land_in_sane_projection_rows() {
    let directory = tempfile::tempdir().expect("tempdir");
    let log = synthetic_log(&directory);

    let (project_rows, run_rows, orphan_running, step_rows, mismatched_finished): (
        i64,
        i64,
        i64,
        i64,
        i64,
    ) = log
        .store()
        .db()
        .with_reader("inspect projections", |connection| {
            let project_rows =
                connection.query_row("SELECT count(*) FROM proj_projects", [], |row| row.get(0))?;
            let run_rows =
                connection.query_row("SELECT count(*) FROM proj_runs", [], |row| row.get(0))?;
            // A run row must always have a started_seq; Increment-created
            // orphans would show up as NULL workers.
            let orphan_running = connection.query_row(
                "SELECT count(*) FROM proj_runs WHERE worker IS NULL",
                [],
                |row| row.get(0),
            )?;
            let step_rows =
                connection.query_row("SELECT count(*) FROM proj_run_steps", [], |row| row.get(0))?;
            // Finished runs agree with their step ledger: step_count equals
            // the number of proj_run_steps rows for that run.
            let mismatched_finished = connection.query_row(
                "SELECT count(*) FROM proj_runs r
                 WHERE r.status != 'running'
                   AND r.step_count != (SELECT count(*) FROM proj_run_steps s WHERE s.run_id = r.run_id)",
                [],
                |row| row.get(0),
            )?;
            Ok((project_rows, run_rows, orphan_running, step_rows, mismatched_finished))
        })
        .expect("projection queries");

    assert_eq!(project_rows, 1, "one project row for the project database");
    assert!(run_rows > 0, "the corpus starts runs");
    assert_eq!(orphan_running, 0, "no step arrived before its RunStarted");
    assert!(step_rows > 0, "the corpus commits steps");
    assert_eq!(
        mismatched_finished, 0,
        "every finished run's step_count equals its step rows"
    );
}
