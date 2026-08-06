//! m0-s04 crash-point harness: every store-owned fault point × `kill -9`
//! (process::abort in the child), then restart and verify nothing corrupted.
//! The child is this same test binary re-invoked with `POS_CRASH_SPEC` set;
//! without the variable the child test is a no-op, so normal runs skip it.
//!
//! The log-owned points (`log-applied`, `log-snapshot-written`) are exercised
//! by the pos-log crash suite on top of this same registry, keeping the
//! m0-s04 matrix complete across crates.

#![forbid(unsafe_code)]

use pos_foundation::ManualWallClock;
use pos_store::{FaultPlan, FaultPoint, ProjectStore, StoreError, StoreOptions};
use std::env;
use std::path::Path;
use std::process::Command;

const CRASH_SPEC_VARIABLE: &str = "POS_CRASH_SPEC";

/// Child entry: `POS_CRASH_SPEC="<project-root>|<point>"`. Runs the matching
/// operation with an armed Abort plan and therefore never returns normally.
#[test]
fn crash_matrix_child() {
    let Ok(spec) = env::var(CRASH_SPEC_VARIABLE) else {
        return;
    };
    let (root, point) = spec.split_once('|').expect("spec is <root>|<point>");
    let point = FaultPoint::from_name(point).expect("spec names a fault point");
    let store = ProjectStore::open_with_options(
        Path::new(root),
        StoreOptions {
            faults: Some(FaultPlan {
                point,
                action: pos_store::FaultAction::Abort,
            }),
        },
    )
    .expect("child opens the prepared store");

    match point {
        FaultPoint::CasTempWritten | FaultPoint::CasRenamed => {
            let content = vec![0xa5_u8; 256 * 1024];
            let _ = store.blobs().write_bytes(&content);
        }
        FaultPoint::WalCommit => {
            let _ = store
                .db()
                .write_transaction("doomed insert", |transaction| {
                    transaction
                        .execute_batch(
                            "INSERT INTO crash_probe(payload) VALUES ('must never be visible')",
                        )
                        .map_err(|source| StoreError::Sqlite {
                            context: "insert probe row",
                            source,
                        })
                });
        }
        FaultPoint::LogEventInserted | FaultPoint::LogApplied | FaultPoint::LogSnapshotWritten => {
            // Owned by the pos-log crash suite; reaching here is a bug.
        }
    }
    unreachable!("the armed fault plan must abort before the operation returns");
}

#[test]
fn every_store_fault_point_leaves_zero_corruption_after_kill() {
    let scenarios = [
        FaultPoint::CasTempWritten,
        FaultPoint::CasRenamed,
        FaultPoint::WalCommit,
    ];
    for point in scenarios {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("crash.pos");
        let store = ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(0))
            .expect("create project store");
        store
            .db()
            .with_writer("prepare probe table", |connection| {
                connection
                    .execute_batch("CREATE TABLE IF NOT EXISTS crash_probe(payload TEXT NOT NULL)")
            })
            .expect("probe table ready");
        drop(store);

        let child = Command::new(env::current_exe().expect("test binary path"))
            .arg("crash_matrix_child")
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

        // Restart: the directory must open cleanly and hold zero corruption.
        let reopened = ProjectStore::open(&root).expect("reopen after crash");
        let report = reopened.blobs().verify().expect("CAS sweep after crash");
        assert!(
            report.is_clean(),
            "{point}: CAS corrupt after kill: {report:?}"
        );
        assert_eq!(
            report.temp_leftover_count, 0,
            "{point}: open() must sweep interrupted writes"
        );
        let doomed_rows: i64 = reopened
            .db()
            .with_reader("probe row count", |connection| {
                connection.query_row("SELECT count(*) FROM crash_probe", [], |row| row.get(0))
            })
            .expect("probe table readable after crash");
        assert_eq!(
            doomed_rows, 0,
            "{point}: an uncommitted transaction became visible after the crash"
        );
    }
}
