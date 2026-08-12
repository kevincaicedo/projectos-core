//! m0-s03 property oracles (§18 replay-determinism gate, CI every PR):
//! arbitrary event sequences ⇒ byte-identical projections across incremental
//! apply, full rebuild, and snapshot + tail; seq contiguity and per-device
//! lamport monotonicity under arbitrary interleavings; append + apply
//! atomicity under injected failure between the two.
//!
//! The projections here are test-local on purpose: the property must hold
//! for ANY conforming projection, not just the v0 domain set (pos-domain
//! re-runs the same properties over the real kinds).

#![forbid(unsafe_code)]

use pos_foundation::{DeviceId, EventSeq, ManualWallClock, UserId};
use pos_log::{
    Actor, AppendRequest, ColumnDef, ColumnKind, EntityRef, Event, KindTag, LogConfig, LogError,
    ProjectLog, Projection, ProjectionRegistry, RowWrite, SqlValue, TableDef,
};
use pos_store::{FaultAction, FaultPlan, FaultPoint, ProjectStore, StoreOptions};
use proptest::prelude::*;

/// Counts per kind — exercises `Increment` (deterministic counters).
struct CountsProjection;

const COUNTS_TABLE: TableDef = TableDef {
    name: "proj_test_counts",
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

impl Projection for CountsProjection {
    fn table(&self) -> &TableDef {
        &COUNTS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, pos_log::ApplyError> {
        Ok(vec![RowWrite::Increment {
            key: vec![SqlValue::Text(event.kind.as_str().to_owned())],
            column: "count",
            delta: 1,
        }])
    }
}

/// Latest event per device — exercises `Upsert` overwrite semantics.
struct LatestProjection;

const LATEST_TABLE: TableDef = TableDef {
    name: "proj_test_latest",
    version: 1,
    key_columns: &[ColumnDef {
        name: "device",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "last_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_kind",
            kind: ColumnKind::Text,
        },
    ],
    indexes: &[],
};

impl Projection for LatestProjection {
    fn table(&self) -> &TableDef {
        &LATEST_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, pos_log::ApplyError> {
        Ok(vec![RowWrite::Upsert {
            key: vec![SqlValue::Blob(event.device.into_bytes().to_vec())],
            values: vec![
                SqlValue::Integer(i64::try_from(event.seq.value()).unwrap_or(i64::MAX)),
                SqlValue::Text(event.kind.as_str().to_owned()),
            ],
        }])
    }
}

/// Body ledger with tombstones — exercises `Upsert` + `Delete` (a removal
/// event deletes the row of the seq named in its body).
struct BodiesProjection;

const BODIES_TABLE: TableDef = TableDef {
    name: "proj_test_bodies",
    version: 1,
    key_columns: &[ColumnDef {
        name: "seq",
        kind: ColumnKind::Integer,
    }],
    value_columns: &[ColumnDef {
        name: "body",
        kind: ColumnKind::Blob,
    }],
    indexes: &[],
};

impl Projection for BodiesProjection {
    fn table(&self) -> &TableDef {
        &BODIES_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, pos_log::ApplyError> {
        if event.kind.as_str() == "EntryRemoved" {
            let target = event
                .body
                .first_chunk::<8>()
                .map_or(0, |bytes| u64::from_be_bytes(*bytes));
            return Ok(vec![RowWrite::Delete {
                key: vec![SqlValue::Integer(i64::try_from(target).unwrap_or(0))],
            }]);
        }
        Ok(vec![RowWrite::Upsert {
            key: vec![SqlValue::Integer(
                i64::try_from(event.seq.value()).unwrap_or(i64::MAX),
            )],
            values: vec![SqlValue::Blob(event.body.clone())],
        }])
    }
}

fn registry() -> ProjectionRegistry {
    ProjectionRegistry::new(vec![
        Box::new(CountsProjection),
        Box::new(LatestProjection),
        Box::new(BodiesProjection),
    ])
    .expect("test registry is well-formed")
}

/// Small cadence so every non-trivial case crosses snapshot boundaries.
const TEST_SNAPSHOT_CADENCE: u64 = 5;

fn open_log(root: &std::path::Path) -> ProjectLog {
    let store = if root.join("manifest.json").is_file() {
        ProjectStore::open(root).expect("reopen store")
    } else {
        ProjectStore::create(root, "generic", &ManualWallClock::starting_at(1_000))
            .expect("create store")
    };
    ProjectLog::open(
        store,
        registry(),
        LogConfig {
            snapshot_cadence_events: TEST_SNAPSHOT_CADENCE,
        },
    )
    .expect("open log")
}

#[derive(Clone, Debug)]
struct RequestSpec {
    device_index: u8,
    kind_index: u8,
    body: Vec<u8>,
}

fn request_from_spec(spec: &RequestSpec) -> AppendRequest {
    let kinds = ["AlphaHappened", "BetaHappened", "EntryRemoved"];
    let kind = kinds[usize::from(spec.kind_index) % kinds.len()];
    AppendRequest {
        device: DeviceId::from_bytes([spec.device_index % 3; 16]),
        actor: Actor::User(UserId::from_bytes([7; 16])),
        kind: KindTag::new(kind).expect("test kinds are valid"),
        body: spec.body.clone(),
        refs: vec![EntityRef {
            entity: "project".to_owned(),
            id: [1; 16],
        }],
    }
}

fn batches_strategy() -> impl Strategy<Value = Vec<Vec<RequestSpec>>> {
    let spec = (
        any::<u8>(),
        any::<u8>(),
        proptest::collection::vec(any::<u8>(), 0..24),
    )
        .prop_map(|(device_index, kind_index, body)| RequestSpec {
            device_index,
            kind_index,
            body,
        });
    proptest::collection::vec(proptest::collection::vec(spec, 1..12), 1..6)
}

#[test]
fn conditional_append_detects_a_stale_head_inside_the_writer_transaction() {
    let directory = tempfile::tempdir().expect("tempdir");
    let log = open_log(directory.path());
    let clock = ManualWallClock::starting_at(2_000);
    let request = request_from_spec(&RequestSpec {
        device_index: 1,
        kind_index: 0,
        body: vec![1],
    });
    let first = log
        .append_at_head(EventSeq::ZERO, request.clone(), &clock)
        .expect("empty head matches");
    assert_eq!(first, EventSeq::new(1));

    let error = log
        .append_at_head(EventSeq::ZERO, request, &clock)
        .expect_err("the stale compare must fail without appending");
    let LogError::HeadChanged { expected, actual } = error else {
        panic!("stale append returned the wrong typed error");
    };
    assert_eq!(expected, EventSeq::ZERO);
    assert_eq!(actual, EventSeq::new(1));
    assert_eq!(log.head().expect("head reads"), EventSeq::new(1));
}

proptest! {
    // Each case runs real SQLite in a tempdir; 32 cases keeps the suite in
    // CI budget while shrinking still works on failure.
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Same log ⇒ byte-identical projections, three ways: incremental apply,
    /// full rebuild (twice), snapshot + tail restore.
    #[test]
    fn replay_is_byte_identical_and_snapshot_tail_equals_full(batches in batches_strategy()) {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("prop.pos");
        let log = open_log(&root);
        let clock = ManualWallClock::starting_at(50_000);
        for batch in &batches {
            let requests: Vec<AppendRequest> = batch.iter().map(request_from_spec).collect();
            log.append_batch(&requests, &clock).expect("append batch");
            clock.advance_ms(13);
        }
        let incremental = log.dump_projections().expect("dump incremental");

        log.rebuild_projections().expect("full rebuild");
        let rebuilt_once = log.dump_projections().expect("dump rebuild 1");
        log.rebuild_projections().expect("full rebuild again");
        let rebuilt_twice = log.dump_projections().expect("dump rebuild 2");
        prop_assert_eq!(&incremental, &rebuilt_once);
        prop_assert_eq!(&rebuilt_once, &rebuilt_twice);

        log.restore_from_snapshot_and_tail().expect("snapshot + tail");
        let snapshot_tail = log.dump_projections().expect("dump snapshot+tail");
        prop_assert_eq!(&incremental, &snapshot_tail);

        let report = log.verify_projections().expect("verify");
        prop_assert!(report.is_clean(), "verify after equality: {report:?}");
    }

    /// Seq is contiguous from 1 and per-device lamport strictly increases in
    /// seq order, whatever the device interleaving.
    #[test]
    fn seq_contiguity_and_lamport_monotonicity(batches in batches_strategy()) {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("mono.pos");
        let log = open_log(&root);
        let clock = ManualWallClock::starting_at(1);
        let mut appended = 0_u64;
        for batch in &batches {
            let requests: Vec<AppendRequest> = batch.iter().map(request_from_spec).collect();
            let seqs = log.append_batch(&requests, &clock).expect("append batch");
            for seq in seqs {
                appended += 1;
                prop_assert_eq!(seq.value(), appended, "append returns contiguous seqs");
            }
        }
        let mut expected_seq = 0_u64;
        let mut lamport_by_device: std::collections::BTreeMap<[u8; 16], u64> =
            std::collections::BTreeMap::new();
        log.for_each_event(|event| {
            expected_seq += 1;
            assert_eq!(event.seq.value(), expected_seq, "seq gap in stored log");
            let device = event.device.into_bytes();
            let previous = lamport_by_device.get(&device).copied().unwrap_or(0);
            assert!(
                event.lamport > previous,
                "device lamport must strictly increase ({} then {})",
                previous,
                event.lamport
            );
            lamport_by_device.insert(device, event.lamport);
            Ok(())
        }).expect("scan events");
        prop_assert_eq!(expected_seq, appended);
    }
}

/// m0-s03 AC: a simulated failure between the event insert and the
/// projection apply/commit leaves NEITHER — the transaction rolls back.
#[test]
fn append_and_apply_are_atomic_under_injected_failure() {
    for point in [FaultPoint::LogEventInserted, FaultPoint::LogApplied] {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("atomic.pos");
        // Create cleanly first, then reopen with the armed fault.
        drop(open_log(&root));
        let store = ProjectStore::open_with_options(
            &root,
            StoreOptions {
                faults: Some(FaultPlan {
                    point,
                    action: FaultAction::FailOperation,
                }),
            },
        )
        .expect("reopen with fault");
        let log = ProjectLog::open(
            store,
            registry(),
            LogConfig {
                snapshot_cadence_events: TEST_SNAPSHOT_CADENCE,
            },
        )
        .expect("open log with fault");
        let clock = ManualWallClock::starting_at(9);
        let requests: Vec<AppendRequest> = (0..3)
            .map(|index| {
                request_from_spec(&RequestSpec {
                    device_index: index,
                    kind_index: index,
                    body: vec![index; 4],
                })
            })
            .collect();
        let error = log
            .append_batch(&requests, &clock)
            .expect_err("armed fault must fail the append");
        assert!(
            matches!(error, LogError::Store(_)),
            "{point}: expected an injected store failure, got {error:?}"
        );
        assert_eq!(
            log.head().expect("head").value(),
            0,
            "{point}: no event survived"
        );
        assert_eq!(log.event_count().expect("count"), 0);
        let report = log.verify_projections().expect("verify");
        assert!(
            report.is_clean(),
            "{point}: projections must equal an empty replay: {report:?}"
        );
    }
}

#[test]
fn as_of_is_a_reserved_typed_seam() {
    let directory = tempfile::tempdir().expect("tempdir");
    let log = open_log(&directory.path().join("asof.pos"));
    let error = log.as_of(EventSeq::new(1)).expect_err("reserved until M3");
    assert!(matches!(
        error,
        LogError::NotYetSupported { arrives: "M3", .. }
    ));
}

#[test]
fn verify_names_a_hand_mutated_projection_table() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("tamper.pos");
    let log = open_log(&root);
    let clock = ManualWallClock::starting_at(77);
    let requests: Vec<AppendRequest> = (0..7)
        .map(|index| {
            request_from_spec(&RequestSpec {
                device_index: 0,
                kind_index: index % 2,
                body: vec![index, index],
            })
        })
        .collect();
    log.append_batch(&requests, &clock).expect("append");

    // Mutate a projection row behind the log's back — the corruption class
    // `pos verify` exists to catch (L1: direct projection writes are
    // corruption; this test IS the attacker).
    log.store()
        .db()
        .with_writer("test tampering", |connection| {
            connection.execute_batch("UPDATE proj_test_counts SET count = count + 41")
        })
        .expect("tamper");

    let report = log.verify_projections().expect("verify runs");
    assert!(!report.is_clean());
    assert_eq!(report.mismatched_tables(), vec!["proj_test_counts"]);
}

#[test]
fn oversize_bodies_and_refs_are_refused_typed() {
    let directory = tempfile::tempdir().expect("tempdir");
    let log = open_log(&directory.path().join("bounds.pos"));
    let clock = ManualWallClock::starting_at(1);
    let oversize_body = AppendRequest {
        device: DeviceId::from_bytes([1; 16]),
        actor: Actor::User(UserId::from_bytes([1; 16])),
        kind: KindTag::new("AlphaHappened").expect("valid kind"),
        body: vec![0; 1_048_577],
        refs: Vec::new(),
    };
    assert!(matches!(
        log.append(oversize_body, &clock),
        Err(LogError::OversizeBody { .. })
    ));
    assert!(matches!(
        KindTag::new(""),
        Err(LogError::InvalidKindTag { .. })
    ));
    assert!(matches!(
        KindTag::new("has space"),
        Err(LogError::InvalidKindTag { .. })
    ));
}
