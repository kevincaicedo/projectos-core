//! m0-s03/m0-s04 crash matrix, log side: `kill -9` (child abort) at the
//! log-owned fault points during an append batch, then restart and prove the
//! all-or-nothing contract — the committed prefix intact, the doomed batch
//! absent, projections exactly rebuildable (zero corruption).

#![forbid(unsafe_code)]

use pos_foundation::{DeviceId, ManualWallClock, UserId};
use pos_log::{
    Actor, AppendRequest, ColumnDef, ColumnKind, Event, KindTag, LogConfig, ProjectLog, Projection,
    ProjectionRegistry, RowWrite, SqlValue, TableDef,
};
use pos_store::{FaultAction, FaultPlan, FaultPoint, ProjectStore, StoreOptions};
use std::env;
use std::path::Path;
use std::process::Command;

const CRASH_SPEC_VARIABLE: &str = "POS_LOG_CRASH_SPEC";
/// Cadence 3 guarantees the doomed batch crosses a snapshot boundary, so the
/// `log-snapshot-written` point actually fires.
const CRASH_TEST_CADENCE: u64 = 3;
const COMMITTED_BATCH_LEN: u64 = 4;

struct TallyProjection;

const TALLY_TABLE: TableDef = TableDef {
    name: "proj_crash_tally",
    version: 1,
    key_columns: &[ColumnDef {
        name: "kind",
        kind: ColumnKind::Text,
    }],
    value_columns: &[ColumnDef {
        name: "count",
        kind: ColumnKind::Integer,
    }],
    indexes: &[],
};

impl Projection for TallyProjection {
    fn table(&self) -> &TableDef {
        &TALLY_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, pos_log::ApplyError> {
        Ok(vec![RowWrite::Increment {
            key: vec![SqlValue::Text(event.kind.as_str().to_owned())],
            column: "count",
            delta: 1,
        }])
    }
}

fn registry() -> ProjectionRegistry {
    ProjectionRegistry::new(vec![Box::new(TallyProjection)]).expect("registry is well-formed")
}

fn config() -> LogConfig {
    LogConfig {
        snapshot_cadence_events: CRASH_TEST_CADENCE,
    }
}

fn requests(count: u64) -> Vec<AppendRequest> {
    (0..count)
        .map(|index| AppendRequest {
            device: DeviceId::from_bytes([9; 16]),
            actor: Actor::User(UserId::from_bytes([3; 16])),
            kind: KindTag::new("PulseRecorded").expect("valid kind"),
            body: vec![u8::try_from(index % 251).unwrap_or(0)],
            refs: Vec::new(),
        })
        .collect()
}

/// Child: commit one batch cleanly, reopen with the armed abort, die inside
/// the second batch.
#[test]
fn log_crash_child() {
    let Ok(spec) = env::var(CRASH_SPEC_VARIABLE) else {
        return;
    };
    let (root, point) = spec.split_once('|').expect("spec is <root>|<point>");
    let point = FaultPoint::from_name(point).expect("spec names a fault point");
    let clock = ManualWallClock::starting_at(1_000);

    let store = ProjectStore::open(Path::new(root)).expect("child opens prepared store");
    let log = ProjectLog::open(store, registry(), config()).expect("open log");
    log.append_batch(&requests(COMMITTED_BATCH_LEN), &clock)
        .expect("committed batch");
    drop(log);

    let store = ProjectStore::open_with_options(
        Path::new(root),
        StoreOptions {
            faults: Some(FaultPlan {
                point,
                action: FaultAction::Abort,
            }),
        },
    )
    .expect("reopen with armed fault");
    let log = ProjectLog::open(store, registry(), config()).expect("open log with fault");
    let _ = log.append_batch(&requests(6), &clock);
    unreachable!("the armed fault plan must abort before the doomed batch returns");
}

#[test]
fn killed_appends_leave_the_committed_prefix_and_nothing_else() {
    let scenarios = [
        FaultPoint::LogEventInserted,
        FaultPoint::LogApplied,
        FaultPoint::LogSnapshotWritten,
        FaultPoint::WalCommit,
    ];
    for point in scenarios {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("crashlog.pos");
        let store = ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(0))
            .expect("create store");
        drop(ProjectLog::open(store, registry(), config()).expect("initialize schema"));

        let child = Command::new(env::current_exe().expect("test binary path"))
            .arg("log_crash_child")
            .arg("--exact")
            .arg("--nocapture")
            .env(
                CRASH_SPEC_VARIABLE,
                format!("{}|{}", root.display(), point.as_name()),
            )
            .output()
            .expect("spawn crash child");
        assert!(
            !child.status.success(),
            "{point}: child must die at the fault point; stdout: {}",
            String::from_utf8_lossy(&child.stdout)
        );

        // Restart. Open must succeed and the log must hold exactly the
        // committed prefix, with projections proven equal to a fresh replay.
        let store = ProjectStore::open(&root).expect("reopen after kill");
        let log = ProjectLog::open(store, registry(), config()).expect("reopen log after kill");
        // wal-commit trips during the reopen transaction in the child (before
        // any doomed append), so the committed prefix is the strict floor and
        // the doomed batch the strict ceiling in every scenario.
        let head = log.head().expect("head").value();
        assert_eq!(
            head, COMMITTED_BATCH_LEN,
            "{point}: exactly the committed batch survives"
        );
        assert_eq!(log.event_count().expect("count"), COMMITTED_BATCH_LEN);
        let report = log.verify_projections().expect("verify");
        assert!(
            report.is_clean(),
            "{point}: corruption after kill: {report:?}"
        );
    }
}
