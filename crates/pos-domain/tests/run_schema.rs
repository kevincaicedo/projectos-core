//! m0-s12 schema-evolution oracle: legacy Run bodies remain readable while
//! the future runtime/executor/checkpoint/validation/parent seams replay.

#![forbid(unsafe_code)]

use pos_domain::{
    DomainEvent, RunBudget, RunCheckpointRef, RunExecutionLeaseRef, RunExecutor, RunFinishedBody,
    RunOutcome, RunRuntimeKind, RunRuntimeRef, RunStartedBody, RunStepCommittedBody, RunToolGrant,
    RunToolGrantMode, RunTrigger, RunUsage, RunValidationRef, RunValidationStatus, v0_registry,
};
use pos_foundation::{
    CheckpointId, DeviceId, ExecutionLeaseId, ManualWallClock, RunId, UserId, ValidationId,
};
use pos_log::{Actor, KindTag, LogConfig, ProjectLog};
use pos_store::ProjectStore;

struct FutureFields {
    runtime_kind: String,
    runtime_id: String,
    executor: String,
    contract_version: i64,
    parent_run_id: Vec<u8>,
    checkpoint_id: Vec<u8>,
    validation_id: Vec<u8>,
    lease_generation: i64,
    grant_mode: String,
}

#[test]
fn invalid_run_lifecycle_facts_fail_typed_and_atomically() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("invalid-lifecycle.pos");
    let clock = ManualWallClock::starting_at(60_000);
    let store = ProjectStore::create(&root, "generic", &clock).expect("create store");
    let log = ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open log");
    let actor = Actor::User(UserId::from_bytes([0x91; 16]));
    let device = DeviceId::from_bytes([0x92; 16]);
    let run_id = RunId::from_bytes([0x93; 16]);
    let step = DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 {
        run_id,
        step_index: 0,
        summary: "No Run exists".to_owned(),
    });
    let error = log
        .append(
            step.clone().into_request(device, actor).expect("request"),
            &clock,
        )
        .expect_err("a step without RunStarted must fail");
    assert!(
        error.to_string().contains("exactly one lifecycle row"),
        "unexpected typed error: {error}"
    );
    assert_eq!(log.head().expect("head after rejected append").value(), 0);

    log.append(
        DomainEvent::RunStarted(RunStartedBody::V1 {
            run_id,
            worker: "Navigator".to_owned(),
            trigger: "user".to_owned(),
        })
        .into_request(device, actor)
        .expect("start request"),
        &clock,
    )
    .expect("append RunStarted");
    log.append(
        step.clone()
            .into_request(device, actor)
            .expect("step request"),
        &clock,
    )
    .expect("append first step");
    let head_before_duplicate = log.head().expect("head before duplicate");
    let duplicate = log
        .append(
            step.into_request(device, actor).expect("step request"),
            &clock,
        )
        .expect_err("a duplicate step index must fail");
    assert!(
        duplicate.to_string().contains("UNIQUE constraint failed"),
        "unexpected duplicate error: {duplicate}"
    );
    assert_eq!(
        log.head().expect("head after duplicate"),
        head_before_duplicate,
        "failed projection apply must roll back the event append"
    );
}

#[test]
fn legacy_and_future_run_shapes_decode_and_replay_without_rewrite() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("schema.pos");
    let clock = ManualWallClock::starting_at(50_000);
    let store = ProjectStore::create(&root, "generic", &clock).expect("create store");
    let project_id = store.manifest().project_id;
    let log = ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open log");
    let actor = Actor::User(UserId::from_bytes([0x81; 16]));
    let device = DeviceId::from_bytes([0x82; 16]);
    let legacy_run = RunId::from_bytes([0x83; 16]);
    let future_run = RunId::from_bytes([0x84; 16]);

    let legacy = DomainEvent::RunStarted(RunStartedBody::V1 {
        run_id: legacy_run,
        worker: "Navigator".to_owned(),
        trigger: "user".to_owned(),
    });
    let legacy_bytes = legacy.encode_body();
    assert_eq!(
        DomainEvent::decode(&KindTag::new("RunStarted").expect("kind"), &legacy_bytes)
            .expect("decode legacy"),
        Some(legacy.clone())
    );
    log.append(
        legacy.into_request(device, actor).expect("legacy request"),
        &clock,
    )
    .expect("append legacy");
    log.append(
        DomainEvent::RunFinished(RunFinishedBody::V1 {
            run_id: legacy_run,
            outcome: RunOutcome::Completed,
            steps_total: 0,
        })
        .into_request(device, actor)
        .expect("legacy finish request"),
        &clock,
    )
    .expect("append legacy finish");

    let checkpoint = RunCheckpointRef {
        checkpoint_id: CheckpointId::from_bytes([0x85; 16]),
        step_index: 7,
    };
    let validation = RunValidationRef {
        validation_id: ValidationId::from_bytes([0x86; 16]),
        status: RunValidationStatus::Inconclusive,
    };
    let future = DomainEvent::RunStarted(RunStartedBody::V2 {
        run_id: future_run,
        project_id,
        worker: "Verifier".to_owned(),
        runtime: RunRuntimeRef {
            kind: RunRuntimeKind::External,
            runtime_id: "projectos.remote-worker".to_owned(),
            contract_version: 9,
        },
        executor: RunExecutor::Cloud,
        trigger: RunTrigger::ParentRun,
        autonomy_level: 4,
        budget: RunBudget {
            tokens: 1_000,
            usd_micros: 2_000,
            wall_ms: 3_000,
            storage_bytes: 4_000,
            tool_calls: 5,
            retries: 6,
            steps: 7,
        },
        tool_grants: vec![RunToolGrant {
            tool_id: "evidence.read".to_owned(),
            mode: RunToolGrantMode::Allow,
        }],
        parent_run_id: Some(legacy_run),
        lineage_depth: 1,
        checkpoint: Some(checkpoint),
        validation: Some(validation),
        execution_lease: Some(RunExecutionLeaseRef {
            lease_id: ExecutionLeaseId::from_bytes([0x87; 16]),
            generation: 11,
        }),
        tainted: true,
    });
    let future_bytes = future.encode_body();
    assert_eq!(
        DomainEvent::decode(&KindTag::new("RunStarted").expect("kind"), &future_bytes)
            .expect("decode future"),
        Some(future.clone())
    );
    log.append(
        future.into_request(device, actor).expect("future request"),
        &clock,
    )
    .expect("append future");
    log.append(
        DomainEvent::RunFinished(RunFinishedBody::V2 {
            run_id: future_run,
            outcome: RunOutcome::Completed,
            steps_total: 0,
            spent: RunUsage::default(),
            validation: Some(validation),
        })
        .into_request(device, actor)
        .expect("future finish request"),
        &clock,
    )
    .expect("append future finish");

    let fields = log
        .store()
        .db()
        .with_reader("inspect future Run schema", |connection| {
            connection.query_row(
                "SELECT runtime_kind, runtime_id, executor, runtime_contract_version, \
                    parent_run_id, checkpoint_id, validation_id, execution_lease_generation, \
                    (SELECT mode FROM proj_run_tool_grants g \
                     WHERE g.run_id = proj_runs.run_id AND g.tool_id = 'evidence.read') \
                 FROM proj_runs WHERE run_id = ?1",
                [future_run.into_bytes().to_vec()],
                |row| {
                    Ok(FutureFields {
                        runtime_kind: row.get(0)?,
                        runtime_id: row.get(1)?,
                        executor: row.get(2)?,
                        contract_version: row.get(3)?,
                        parent_run_id: row.get(4)?,
                        checkpoint_id: row.get(5)?,
                        validation_id: row.get(6)?,
                        lease_generation: row.get(7)?,
                        grant_mode: row.get(8)?,
                    })
                },
            )
        })
        .expect("query future fields");
    assert_eq!(fields.runtime_kind, "external");
    assert_eq!(fields.runtime_id, "projectos.remote-worker");
    assert_eq!(fields.executor, "cloud");
    assert_eq!(fields.contract_version, 9);
    assert_eq!(fields.parent_run_id, legacy_run.into_bytes());
    assert_eq!(fields.checkpoint_id, checkpoint.checkpoint_id.into_bytes());
    assert_eq!(fields.validation_id, validation.validation_id.into_bytes());
    assert_eq!(fields.lease_generation, 11);
    assert_eq!(fields.grant_mode, "allow");

    let incremental = log.dump_projections().expect("dump projections");
    log.rebuild_projections()
        .expect("rebuild from immutable events");
    let rebuilt = log.dump_projections().expect("dump rebuilt projections");
    assert_eq!(incremental, rebuilt);
    assert!(
        log.verify_projections()
            .expect("verify projections")
            .is_clean()
    );
}
