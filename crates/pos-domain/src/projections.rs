//! The v0 projections (m0-s03): pure `event → typed row writes` over the
//! domain vocabulary. No SQL lives here — `pos-log/src/apply/` renders and
//! executes the writes inside the append/replay transaction, which is what
//! keeps these functions deterministic by construction.
//!
//! Each projection matches its kinds by tag before decoding, so an event
//! only pays decode cost once per projection that actually cares about it.

use crate::events::{
    AccountAuditedBody, CronEnablementSetBody, CronRegisteredBody, DomainEvent,
    JobAttemptFailedBody, JobClass, JobCompletedBody, JobDeadBody, JobDeadReason, JobEnqueuedBody,
    JobPriority, ModelCallCompletedBody, ProjectCreatedBody, ProjectRenamedBody,
    RunArtifactRecordedBody, RunBudget, RunCancelRequestedBody, RunCanceledBody,
    RunCheckpointSavedBody, RunFinishedBody, RunPauseCause, RunPauseRequestedBody, RunPausedBody,
    RunQuestionAnsweredBody, RunQuestionAskedBody, RunResumedBody, RunStartedBody,
    RunStepCommittedBody, RunTaintRaisedBody, RunToolEffectRecordedBody, RunUsage,
    RunValidationRecordedBody,
};
use pos_log::{
    ApplyError, ColumnDef, ColumnKind, Event, IndexDef, LogError, Projection, ProjectionRegistry,
    RowWrite, SqlValue, TableDef,
};

/// Builds the registry every v0 shell opens projects with.
pub fn v0_registry() -> Result<ProjectionRegistry, LogError> {
    let mut projections: Vec<Box<dyn Projection>> = vec![
        Box::new(ProjectsProjection),
        Box::new(RunsProjection),
        Box::new(RunToolGrantsProjection),
        Box::new(RunStepsProjection),
        Box::new(JobsProjection),
        Box::new(CronsProjection),
        Box::new(AuditProjection),
        Box::new(ModelCallsProjection),
    ];
    // The ingestion tables live beside their own applies (m1-s01/m1-s02);
    // the registry stays the one place that says which projections a project
    // opens with, so a rebuild covers all of them or none.
    projections.extend(crate::ingest_projections::projections());
    ProjectionRegistry::new(projections)
}

fn decode_for(table: &TableDef, event: &Event) -> Result<Option<DomainEvent>, ApplyError> {
    DomainEvent::decode(&event.kind, &event.body).map_err(|error| ApplyError {
        reason: format!("{}: {error}", table.name),
    })
}

fn seq_value(event: &Event) -> SqlValue {
    SqlValue::Integer(i64::try_from(event.seq.value()).unwrap_or(i64::MAX))
}

fn integer_u64(value: u64) -> SqlValue {
    SqlValue::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn integer_u32(value: u32) -> SqlValue {
    SqlValue::Integer(i64::from(value))
}

fn id_blob(id: [u8; 16]) -> SqlValue {
    SqlValue::Blob(id.to_vec())
}

fn optional_id_blob(id: Option<[u8; 16]>) -> SqlValue {
    id.map_or(SqlValue::Null, id_blob)
}

fn budget_values(budget: RunBudget) -> Vec<SqlValue> {
    vec![
        integer_u64(budget.tokens),
        integer_u64(budget.usd_micros),
        integer_u64(budget.wall_ms),
        integer_u64(budget.storage_bytes),
        integer_u32(budget.tool_calls),
        integer_u32(budget.retries),
        integer_u32(budget.steps),
    ]
}

fn usage_values(usage: RunUsage) -> Vec<SqlValue> {
    vec![
        integer_u64(usage.tokens),
        integer_u64(usage.usd_micros),
        integer_u64(usage.wall_ms),
        integer_u64(usage.storage_bytes),
        integer_u32(usage.tool_calls),
        integer_u32(usage.retries),
        integer_u32(usage.steps),
    ]
}

fn usage_deltas(usage: RunUsage) -> Vec<(&'static str, i64)> {
    vec![
        (
            "spent_tokens",
            i64::try_from(usage.tokens).unwrap_or(i64::MAX),
        ),
        (
            "spent_usd_micros",
            i64::try_from(usage.usd_micros).unwrap_or(i64::MAX),
        ),
        (
            "spent_wall_ms",
            i64::try_from(usage.wall_ms).unwrap_or(i64::MAX),
        ),
        (
            "spent_storage_bytes",
            i64::try_from(usage.storage_bytes).unwrap_or(i64::MAX),
        ),
        ("spent_tool_calls", i64::from(usage.tool_calls)),
        ("spent_retries", i64::from(usage.retries)),
        ("spent_steps", i64::from(usage.steps)),
    ]
}

fn usage_assignments(usage: RunUsage) -> Vec<(&'static str, SqlValue)> {
    vec![
        ("spent_tokens", integer_u64(usage.tokens)),
        ("spent_usd_micros", integer_u64(usage.usd_micros)),
        ("spent_wall_ms", integer_u64(usage.wall_ms)),
        ("spent_storage_bytes", integer_u64(usage.storage_bytes)),
        ("spent_tool_calls", integer_u32(usage.tool_calls)),
        ("spent_retries", integer_u32(usage.retries)),
        ("spent_steps", integer_u32(usage.steps)),
    ]
}

/// The project row (one per project database today; the shape already
/// supports M5 multi-project workspaces).
struct ProjectsProjection;

const PROJECTS_TABLE: TableDef = TableDef {
    name: "proj_projects",
    version: 1,
    key_columns: &[ColumnDef {
        name: "project_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "name",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "template",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "created_ts_ms",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[],
};

impl Projection for ProjectsProjection {
    fn table(&self) -> &TableDef {
        &PROJECTS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "ProjectCreated" => {}
            "ProjectRenamed" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&PROJECTS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
                project_id,
                name,
                template,
            }) => vec![RowWrite::Upsert {
                key: vec![SqlValue::Blob(project_id.into_bytes().to_vec())],
                values: vec![
                    SqlValue::Text(name),
                    SqlValue::Text(template),
                    SqlValue::Integer(i64::try_from(event.ts_ms).unwrap_or(i64::MAX)),
                ],
            }],
            DomainEvent::ProjectRenamed(ProjectRenamedBody::V1 { project_id, name }) => {
                vec![RowWrite::Update {
                    key: vec![SqlValue::Blob(project_id.into_bytes().to_vec())],
                    assignments: vec![("name", SqlValue::Text(name))],
                }]
            }
            _ => Vec::new(),
        })
    }
}

/// Run lifecycle rows (master plan §11.2; the run feed and F23 meters read
/// these from M0-E5 on).
struct RunsProjection;

const RUNS_TABLE: TableDef = TableDef {
    name: "proj_runs",
    version: 2,
    key_columns: &[ColumnDef {
        name: "run_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "project_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "status",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "worker",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "runtime_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "runtime_id",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "runtime_contract_version",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "executor",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "trigger",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "autonomy_level",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "started_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "finished_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "committed_step_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "checkpointed_step_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_tokens",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_usd_micros",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_storage_bytes",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_tool_calls",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_retries",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "budget_steps",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_tokens",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_usd_micros",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_storage_bytes",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_tool_calls",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_retries",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_steps",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "tainted",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "parent_run_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "lineage_depth",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "checkpoint_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "checkpoint_step_index",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "validation_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "validation_status",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "execution_lease_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "execution_lease_generation",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "pending_control",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "control_reason",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "pause_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "pause_detail",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "pause_dimension",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "pause_limit",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "pause_spent",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "pause_pending",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "pause_requested",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "question_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "question_prompt",
            kind: ColumnKind::Text,
        },
    ],
    indexes: &[],
};

fn legacy_run_values(worker: String, trigger: String, event: &Event) -> Vec<SqlValue> {
    let unlimited = RunBudget {
        tokens: u64::MAX,
        usd_micros: u64::MAX,
        wall_ms: u64::MAX,
        storage_bytes: u64::MAX,
        tool_calls: u32::MAX,
        retries: u32::MAX,
        steps: u32::MAX,
    };
    let mut values = vec![
        SqlValue::Null,
        SqlValue::Text("running".to_owned()),
        SqlValue::Text(worker),
        SqlValue::Text("native".to_owned()),
        SqlValue::Text("legacy".to_owned()),
        SqlValue::Integer(1),
        SqlValue::Text("device".to_owned()),
        SqlValue::Text(trigger),
        SqlValue::Integer(0),
        seq_value(event),
        SqlValue::Null,
        SqlValue::Integer(0),
        SqlValue::Integer(0),
    ];
    values.extend(budget_values(unlimited));
    values.extend(usage_values(RunUsage::default()));
    values.extend([
        SqlValue::Integer(0),
        SqlValue::Null,
        SqlValue::Integer(0),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
    ]);
    values
}

struct RunStartProjectionFields {
    project_id: pos_foundation::ProjectId,
    worker: String,
    runtime: crate::events::RunRuntimeRef,
    executor: crate::events::RunExecutor,
    trigger: crate::events::RunTrigger,
    autonomy_level: u8,
    budget: RunBudget,
    parent_run_id: Option<pos_foundation::RunId>,
    lineage_depth: u8,
    checkpoint: Option<crate::events::RunCheckpointRef>,
    validation: Option<crate::events::RunValidationRef>,
    execution_lease: Option<crate::events::RunExecutionLeaseRef>,
    tainted: bool,
}

fn run_values_v2(fields: RunStartProjectionFields, event: &Event) -> Vec<SqlValue> {
    let RunStartProjectionFields {
        project_id,
        worker,
        runtime,
        executor,
        trigger,
        autonomy_level,
        budget,
        parent_run_id,
        lineage_depth,
        checkpoint,
        validation,
        execution_lease,
        tainted,
    } = fields;
    let mut values = vec![
        id_blob(project_id.into_bytes()),
        SqlValue::Text("preflight".to_owned()),
        SqlValue::Text(worker),
        SqlValue::Text(runtime.kind.as_str().to_owned()),
        SqlValue::Text(runtime.runtime_id),
        SqlValue::Integer(i64::from(runtime.contract_version)),
        SqlValue::Text(executor.as_str().to_owned()),
        SqlValue::Text(trigger.as_str().to_owned()),
        SqlValue::Integer(i64::from(autonomy_level)),
        seq_value(event),
        SqlValue::Null,
        SqlValue::Integer(0),
        SqlValue::Integer(0),
    ];
    values.extend(budget_values(budget));
    values.extend(usage_values(RunUsage::default()));
    values.extend([
        SqlValue::Integer(i64::from(tainted)),
        optional_id_blob(parent_run_id.map(pos_foundation::RunId::into_bytes)),
        SqlValue::Integer(i64::from(lineage_depth)),
        optional_id_blob(checkpoint.map(|value| value.checkpoint_id.into_bytes())),
        checkpoint.map_or(SqlValue::Null, |value| integer_u32(value.step_index)),
        optional_id_blob(validation.map(|value| value.validation_id.into_bytes())),
        validation.map_or(SqlValue::Null, |value| {
            SqlValue::Text(value.status.as_str().to_owned())
        }),
        optional_id_blob(execution_lease.map(|value| value.lease_id.into_bytes())),
        execution_lease.map_or(SqlValue::Null, |value| integer_u64(value.generation)),
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
    ]);
    values
}

fn pause_assignments(cause: &RunPauseCause) -> Vec<(&'static str, SqlValue)> {
    match cause {
        RunPauseCause::Budget {
            dimension,
            limit,
            spent,
            pending,
            requested,
        } => vec![
            ("pause_kind", SqlValue::Text("budget".to_owned())),
            ("pause_detail", SqlValue::Null),
            (
                "pause_dimension",
                SqlValue::Text(dimension.as_str().to_owned()),
            ),
            ("pause_limit", integer_u64(*limit)),
            ("pause_spent", integer_u64(*spent)),
            ("pause_pending", integer_u64(*pending)),
            ("pause_requested", integer_u64(*requested)),
        ],
        RunPauseCause::Requested { reason } => vec![
            ("pause_kind", SqlValue::Text("requested".to_owned())),
            ("pause_detail", SqlValue::Text(reason.clone())),
            ("pause_dimension", SqlValue::Null),
            ("pause_limit", SqlValue::Null),
            ("pause_spent", SqlValue::Null),
            ("pause_pending", SqlValue::Null),
            ("pause_requested", SqlValue::Null),
        ],
        RunPauseCause::ToolWeather { code } => vec![
            ("pause_kind", SqlValue::Text("tool_weather".to_owned())),
            ("pause_detail", SqlValue::Text(code.clone())),
            ("pause_dimension", SqlValue::Null),
            ("pause_limit", SqlValue::Null),
            ("pause_spent", SqlValue::Null),
            ("pause_pending", SqlValue::Null),
            ("pause_requested", SqlValue::Null),
        ],
        RunPauseCause::ReservationExceeded { dimension } => vec![
            (
                "pause_kind",
                SqlValue::Text("reservation_exceeded".to_owned()),
            ),
            ("pause_detail", SqlValue::Null),
            (
                "pause_dimension",
                SqlValue::Text(dimension.as_str().to_owned()),
            ),
            ("pause_limit", SqlValue::Null),
            ("pause_spent", SqlValue::Null),
            ("pause_pending", SqlValue::Null),
            ("pause_requested", SqlValue::Null),
        ],
    }
}

fn clear_pause_assignments() -> Vec<(&'static str, SqlValue)> {
    vec![
        ("pause_kind", SqlValue::Null),
        ("pause_detail", SqlValue::Null),
        ("pause_dimension", SqlValue::Null),
        ("pause_limit", SqlValue::Null),
        ("pause_spent", SqlValue::Null),
        ("pause_pending", SqlValue::Null),
        ("pause_requested", SqlValue::Null),
    ]
}

impl Projection for RunsProjection {
    fn table(&self) -> &TableDef {
        &RUNS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "RunStarted"
            | "RunStepCommitted"
            | "RunToolEffectRecorded"
            | "RunCheckpointSaved"
            | "RunValidationRecorded"
            | "RunPauseRequested"
            | "RunPaused"
            | "RunResumed"
            | "RunCancelRequested"
            | "RunCanceled"
            | "RunTaintRaised"
            | "RunQuestionAsked"
            | "RunQuestionAnswered"
            | "RunFinished" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&RUNS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::RunStarted(RunStartedBody::V1 {
                run_id,
                worker,
                trigger,
            }) => {
                vec![RowWrite::Insert {
                    key: vec![SqlValue::Blob(run_id.into_bytes().to_vec())],
                    values: legacy_run_values(worker, trigger, event),
                }]
            }
            DomainEvent::RunStarted(RunStartedBody::V2 {
                run_id,
                project_id,
                worker,
                runtime,
                executor,
                trigger,
                autonomy_level,
                budget,
                tool_grants: _,
                parent_run_id,
                lineage_depth,
                checkpoint,
                validation,
                execution_lease,
                tainted,
            }) => vec![RowWrite::Insert {
                key: vec![id_blob(run_id.into_bytes())],
                values: run_values_v2(
                    RunStartProjectionFields {
                        project_id,
                        worker,
                        runtime,
                        executor,
                        trigger,
                        autonomy_level,
                        budget,
                        parent_run_id,
                        lineage_depth,
                        checkpoint,
                        validation,
                        execution_lease,
                        tainted,
                    },
                    event,
                ),
            }],
            DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 { run_id, .. }) => {
                vec![RowWrite::IncrementOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    column: "committed_step_count",
                    delta: 1,
                }]
            }
            DomainEvent::RunStepCommitted(RunStepCommittedBody::V2 { run_id, phase, .. }) => vec![
                RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![(
                        "status",
                        SqlValue::Text(
                            match phase {
                                crate::events::RunStepPhase::Preflight => "preflight",
                                crate::events::RunStepPhase::Validation => "validating",
                                crate::events::RunStepPhase::Context
                                | crate::events::RunStepPhase::Tool
                                | crate::events::RunStepPhase::Report => "running",
                            }
                            .to_owned(),
                        ),
                    )],
                },
                RowWrite::IncrementOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    column: "committed_step_count",
                    delta: 1,
                },
            ],
            DomainEvent::RunToolEffectRecorded(RunToolEffectRecordedBody::V1 {
                run_id,
                spent,
                ..
            }) => vec![RowWrite::IncrementManyOne {
                key: vec![id_blob(run_id.into_bytes())],
                deltas: usage_deltas(spent),
            }],
            DomainEvent::RunCheckpointSaved(RunCheckpointSavedBody::V1 {
                run_id,
                step_index,
                checkpoint_id,
                ..
            }) => vec![
                RowWrite::IncrementOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    column: "checkpointed_step_count",
                    delta: 1,
                },
                RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![
                        ("checkpoint_id", id_blob(checkpoint_id.into_bytes())),
                        ("checkpoint_step_index", integer_u32(step_index)),
                    ],
                },
            ],
            DomainEvent::RunValidationRecorded(RunValidationRecordedBody::V1 {
                run_id,
                validation_id,
                status,
                ..
            }) => vec![RowWrite::UpdateOne {
                key: vec![id_blob(run_id.into_bytes())],
                assignments: vec![
                    ("validation_id", id_blob(validation_id.into_bytes())),
                    (
                        "validation_status",
                        SqlValue::Text(status.as_str().to_owned()),
                    ),
                ],
            }],
            DomainEvent::RunPauseRequested(RunPauseRequestedBody::V1 { run_id, reason }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![
                        ("pending_control", SqlValue::Text("pause".to_owned())),
                        ("control_reason", SqlValue::Text(reason)),
                    ],
                }]
            }
            DomainEvent::RunPaused(RunPausedBody::V1 {
                run_id,
                cause,
                spent,
                ..
            }) => {
                let mut assignments = vec![
                    ("status", SqlValue::Text("paused".to_owned())),
                    ("pending_control", SqlValue::Null),
                    ("control_reason", SqlValue::Null),
                ];
                assignments.extend(pause_assignments(&cause));
                assignments.extend(usage_assignments(spent));
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments,
                }]
            }
            DomainEvent::RunResumed(RunResumedBody::V1 { run_id, .. }) => {
                let mut assignments = vec![
                    ("status", SqlValue::Text("running".to_owned())),
                    ("pending_control", SqlValue::Null),
                    ("control_reason", SqlValue::Null),
                ];
                assignments.extend(clear_pause_assignments());
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments,
                }]
            }
            DomainEvent::RunCancelRequested(RunCancelRequestedBody::V1 { run_id, reason }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![
                        ("pending_control", SqlValue::Text("cancel".to_owned())),
                        ("control_reason", SqlValue::Text(reason)),
                    ],
                }]
            }
            DomainEvent::RunCanceled(RunCanceledBody::V1 { run_id, .. }) => {
                let mut assignments = vec![
                    ("status", SqlValue::Text("canceled".to_owned())),
                    ("pending_control", SqlValue::Null),
                    ("control_reason", SqlValue::Null),
                    ("finished_seq", seq_value(event)),
                ];
                assignments.extend(clear_pause_assignments());
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments,
                }]
            }
            DomainEvent::RunTaintRaised(RunTaintRaisedBody::V1 { run_id, .. }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![("tainted", SqlValue::Integer(1))],
                }]
            }
            DomainEvent::RunQuestionAsked(RunQuestionAskedBody::V1 {
                run_id,
                question_id,
                prompt,
            }) => vec![RowWrite::UpdateOne {
                key: vec![id_blob(run_id.into_bytes())],
                assignments: vec![
                    ("status", SqlValue::Text("waiting_input".to_owned())),
                    ("question_id", id_blob(question_id.into_bytes())),
                    ("question_prompt", SqlValue::Text(prompt)),
                ],
            }],
            DomainEvent::RunQuestionAnswered(RunQuestionAnsweredBody::V1 { run_id, .. }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments: vec![
                        ("status", SqlValue::Text("running".to_owned())),
                        ("question_id", SqlValue::Null),
                        ("question_prompt", SqlValue::Null),
                    ],
                }]
            }
            DomainEvent::RunFinished(RunFinishedBody::V1 {
                run_id, outcome, ..
            }) => vec![RowWrite::UpdateOne {
                key: vec![id_blob(run_id.into_bytes())],
                assignments: vec![
                    ("status", SqlValue::Text(outcome.as_status_str().to_owned())),
                    ("finished_seq", seq_value(event)),
                ],
            }],
            DomainEvent::RunFinished(RunFinishedBody::V2 {
                run_id,
                outcome,
                spent,
                validation,
                ..
            }) => {
                let mut assignments = vec![
                    ("status", SqlValue::Text(outcome.as_status_str().to_owned())),
                    ("finished_seq", seq_value(event)),
                ];
                assignments.extend(usage_assignments(spent));
                if let Some(validation) = validation {
                    assignments.push((
                        "validation_id",
                        id_blob(validation.validation_id.into_bytes()),
                    ));
                    assignments.push((
                        "validation_status",
                        SqlValue::Text(validation.status.as_str().to_owned()),
                    ));
                }
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(run_id.into_bytes())],
                    assignments,
                }]
            }
            _ => Vec::new(),
        })
    }
}

/// The per-Run capability allowlist is durable configuration, not a worker
/// argument that can grow after restart. One row per tool keeps lookup and
/// replay bounded by the 64-grant harness cap.
struct RunToolGrantsProjection;

const RUN_TOOL_GRANTS_TABLE: TableDef = TableDef {
    name: "proj_run_tool_grants",
    version: 1,
    key_columns: &[
        ColumnDef {
            name: "run_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "tool_id",
            kind: ColumnKind::Text,
        },
    ],
    value_columns: &[
        ColumnDef {
            name: "mode",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "started_seq",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[],
};

impl Projection for RunToolGrantsProjection {
    fn table(&self) -> &TableDef {
        &RUN_TOOL_GRANTS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        if event.kind.as_str() != "RunStarted" {
            return Ok(Vec::new());
        }
        let Some(decoded) = decode_for(&RUN_TOOL_GRANTS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        let DomainEvent::RunStarted(RunStartedBody::V2 {
            run_id,
            tool_grants,
            ..
        }) = decoded
        else {
            return Ok(Vec::new());
        };
        Ok(tool_grants
            .into_iter()
            .map(|grant| RowWrite::Insert {
                key: vec![id_blob(run_id.into_bytes()), SqlValue::Text(grant.tool_id)],
                values: vec![
                    SqlValue::Text(grant.mode.as_str().to_owned()),
                    seq_value(event),
                ],
            })
            .collect())
    }
}

/// One row per committed run step — the run feed's data (L7: the ledger IS
/// the activity feed).
struct RunStepsProjection;

const RUN_STEPS_TABLE: TableDef = TableDef {
    name: "proj_run_steps",
    version: 2,
    key_columns: &[
        ColumnDef {
            name: "run_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "step_index",
            kind: ColumnKind::Integer,
        },
    ],
    value_columns: &[
        ColumnDef {
            name: "summary",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "phase",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "step_digest",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "tool_id",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "tool_descriptor_version",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "tool_call_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "idempotency_key",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "input",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "reserved_tokens",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_usd_micros",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_storage_bytes",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_tool_calls",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_retries",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "reserved_steps",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "committed_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "effect_recorded_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "output_digest",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "spent_tokens",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_usd_micros",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_storage_bytes",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_tool_calls",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_retries",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "spent_steps",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "checkpoint_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "checkpoint_digest",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "checkpoint_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "artifact_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "artifact_hash",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "artifact_media_type",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "artifact_size_bytes",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "validation_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "validation_status",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "validation_summary",
            kind: ColumnKind::Text,
        },
    ],
    indexes: &[],
};

fn step_values(
    summary: String,
    phase: String,
    digest: Option<[u8; 32]>,
    tool_call: Option<crate::events::RunToolCall>,
    reserved: RunUsage,
    event: &Event,
) -> Vec<SqlValue> {
    let (tool_id, descriptor_version, call_id, idempotency_key, input) = tool_call.map_or(
        (
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
        ),
        |tool_call| {
            (
                SqlValue::Text(tool_call.tool_id),
                SqlValue::Integer(i64::from(tool_call.descriptor_version)),
                id_blob(tool_call.call_id.into_bytes()),
                SqlValue::Text(tool_call.idempotency_key),
                SqlValue::Blob(tool_call.input),
            )
        },
    );
    let mut values = vec![
        SqlValue::Text(summary),
        SqlValue::Text(phase),
        digest.map_or(SqlValue::Null, |value| SqlValue::Blob(value.to_vec())),
        tool_id,
        descriptor_version,
        call_id,
        idempotency_key,
        input,
    ];
    values.extend(usage_values(reserved));
    values.extend([seq_value(event), SqlValue::Null, SqlValue::Null]);
    values.extend(usage_values(RunUsage::default()));
    values.extend([
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
        SqlValue::Null,
    ]);
    values
}

impl Projection for RunStepsProjection {
    fn table(&self) -> &TableDef {
        &RUN_STEPS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "RunStepCommitted"
            | "RunToolEffectRecorded"
            | "RunCheckpointSaved"
            | "RunArtifactRecorded"
            | "RunValidationRecorded" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&RUN_STEPS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 {
                run_id,
                step_index,
                summary,
            }) => vec![RowWrite::Insert {
                key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                values: step_values(
                    summary,
                    "legacy".to_owned(),
                    None,
                    None,
                    RunUsage {
                        steps: 1,
                        ..RunUsage::default()
                    },
                    event,
                ),
            }],
            DomainEvent::RunStepCommitted(RunStepCommittedBody::V2 {
                run_id,
                step_index,
                phase,
                summary,
                digest,
                tool_call,
                reserved,
            }) => vec![RowWrite::Insert {
                key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                values: step_values(
                    summary,
                    phase.as_str().to_owned(),
                    Some(digest),
                    tool_call,
                    reserved,
                    event,
                ),
            }],
            DomainEvent::RunToolEffectRecorded(RunToolEffectRecordedBody::V1 {
                run_id,
                step_index,
                output_digest,
                spent,
                ..
            }) => {
                let mut assignments = vec![
                    ("effect_recorded_seq", seq_value(event)),
                    ("output_digest", SqlValue::Blob(output_digest.to_vec())),
                ];
                assignments.extend([
                    ("spent_tokens", integer_u64(spent.tokens)),
                    ("spent_usd_micros", integer_u64(spent.usd_micros)),
                    ("spent_wall_ms", integer_u64(spent.wall_ms)),
                    ("spent_storage_bytes", integer_u64(spent.storage_bytes)),
                    ("spent_tool_calls", integer_u32(spent.tool_calls)),
                    ("spent_retries", integer_u32(spent.retries)),
                    ("spent_steps", integer_u32(spent.steps)),
                ]);
                vec![RowWrite::UpdateOneWhenNull {
                    key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                    guard_column: "effect_recorded_seq",
                    assignments,
                }]
            }
            DomainEvent::RunCheckpointSaved(RunCheckpointSavedBody::V1 {
                run_id,
                step_index,
                checkpoint_id,
                state_digest,
            }) => vec![RowWrite::UpdateOneWhenNull {
                key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                guard_column: "checkpoint_seq",
                assignments: vec![
                    ("checkpoint_id", id_blob(checkpoint_id.into_bytes())),
                    ("checkpoint_digest", SqlValue::Blob(state_digest.to_vec())),
                    ("checkpoint_seq", seq_value(event)),
                ],
            }],
            DomainEvent::RunArtifactRecorded(RunArtifactRecordedBody::V1 {
                run_id,
                step_index,
                artifact_id,
                content_hash,
                media_type,
                size_bytes,
            }) => vec![RowWrite::UpdateOneWhenNull {
                key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                guard_column: "artifact_id",
                assignments: vec![
                    ("artifact_id", id_blob(artifact_id.into_bytes())),
                    ("artifact_hash", SqlValue::Blob(content_hash.to_vec())),
                    ("artifact_media_type", SqlValue::Text(media_type)),
                    ("artifact_size_bytes", integer_u64(size_bytes)),
                ],
            }],
            DomainEvent::RunValidationRecorded(RunValidationRecordedBody::V1 {
                run_id,
                step_index,
                validation_id,
                status,
                summary,
            }) => vec![RowWrite::UpdateOneWhenNull {
                key: vec![id_blob(run_id.into_bytes()), integer_u32(step_index)],
                guard_column: "validation_id",
                assignments: vec![
                    ("validation_id", id_blob(validation_id.into_bytes())),
                    (
                        "validation_status",
                        SqlValue::Text(status.as_str().to_owned()),
                    ),
                    ("validation_summary", SqlValue::Text(summary)),
                ],
            }],
            _ => Vec::new(),
        })
    }
}

/// Job rows — the durable half of the F36 queue (m0-s14).
///
/// This table is the queue's *truth*: what work exists, what it carries, what
/// it already attempted, and how it ended. It deliberately does **not** hold
/// `Running`: a claim is a node-local lease, not a durable fact, so the live
/// state a reader sees is this row joined against `sched_leases`
/// (`pos-sched::queue`). The durable `state` vocabulary is therefore
/// `queued | done | dead`, and the frozen `Queued → Running → Done/Failed/Dead`
/// contract is completed by that join — never by a projection write from
/// outside an apply path.
struct JobsProjection;

/// Durable job state written by this projection. `Running` and the
/// retry-waiting flavour of `Failed` are derived at read time.
const JOB_STATE_QUEUED: &str = "queued";
const JOB_STATE_DONE: &str = "done";
const JOB_STATE_DEAD: &str = "dead";

/// Defaults a legacy `JobEnqueued::V1` row takes. V1 carried no priority,
/// class, payload, or attempt bound; old events are eternal, so the projection
/// supplies the same values the V2 path would have written for a plain
/// maintenance job rather than refusing to replay them.
const JOB_ATTEMPT_COUNT_MAX_DEFAULT: u32 = 5;

const JOBS_TABLE: TableDef = TableDef {
    name: "proj_jobs",
    version: 2,
    key_columns: &[ColumnDef {
        name: "job_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "project_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "job_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "idempotency_key",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "state",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "priority_rank",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "class",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "payload",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "enqueued_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "run_at_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "attempt_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "attempt_count_max",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_error_code",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "last_error_detail",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "terminal_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "dead_reason_code",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "dead_reason_detail",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "cron_id",
            kind: ColumnKind::Blob,
        },
    ],
    // The claim query is the hot read of the whole subsystem: it runs once per
    // claim per class, against a table that legitimately holds tens of
    // thousands of queued rows (the m0-s14 fairness oracle uses 10k). Without
    // this index the claim degrades to a full scan plus sort and claim latency
    // becomes a function of backlog depth — exactly the starvation the
    // fairness bound forbids.
    indexes: &[
        IndexDef {
            name: "idx_proj_jobs_claim",
            // Equality on class/state, then the exact `ORDER BY` the claim
            // uses, so `LIMIT 1` stops at the first row instead of sorting a
            // backlog. `run_at_ts_ms` stays a residual filter: the pathological
            // case it would help (a queue made entirely of jobs scheduled in
            // the future) is a queue with nothing to claim.
            columns: &["class", "state", "priority_rank", "enqueued_seq"],
        },
        IndexDef {
            name: "idx_proj_jobs_cron",
            columns: &["cron_id", "state"],
        },
    ],
};

impl Projection for JobsProjection {
    fn table(&self) -> &TableDef {
        &JOBS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "JobEnqueued" | "JobAttemptFailed" | "JobCompleted" | "JobDead" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&JOBS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::JobEnqueued(body) => vec![enqueued_row(event, body)],
            DomainEvent::JobAttemptFailed(JobAttemptFailedBody::V1 {
                job_id,
                attempt_index,
                error_code,
                error_detail,
                retry_at_ts_ms,
            }) => {
                // Strict: an attempt for a job with no enqueue fact is durable
                // corruption, and a silent no-op would hide it behind a
                // permanently queued row.
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(job_id.into_bytes())],
                    assignments: vec![
                        ("attempt_count", integer_u32(attempt_index)),
                        ("last_error_code", SqlValue::Text(error_code)),
                        ("last_error_detail", SqlValue::Text(error_detail)),
                        ("run_at_ts_ms", integer_u64(retry_at_ts_ms)),
                    ],
                }]
            }
            DomainEvent::JobCompleted(JobCompletedBody::V1 { job_id, attempts }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(job_id.into_bytes())],
                    assignments: vec![
                        ("state", SqlValue::Text(JOB_STATE_DONE.to_owned())),
                        ("terminal_seq", seq_value(event)),
                        ("attempt_count", integer_u32(attempts)),
                    ],
                }]
            }
            DomainEvent::JobCompleted(JobCompletedBody::V2 {
                job_id,
                attempt_count,
                wall_ms,
            }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(job_id.into_bytes())],
                    assignments: vec![
                        ("state", SqlValue::Text(JOB_STATE_DONE.to_owned())),
                        ("terminal_seq", seq_value(event)),
                        ("attempt_count", integer_u32(attempt_count)),
                        ("wall_ms", integer_u64(wall_ms)),
                    ],
                }]
            }
            DomainEvent::JobDead(JobDeadBody::V1 {
                job_id,
                attempt_count,
                reason,
            }) => {
                let detail = match &reason {
                    JobDeadReason::RetriesExhausted { error_code }
                    | JobDeadReason::Refused { error_code } => error_code.clone(),
                    JobDeadReason::SupersededByCron { cron_id } => cron_id.to_hex(),
                };
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(job_id.into_bytes())],
                    assignments: vec![
                        ("state", SqlValue::Text(JOB_STATE_DEAD.to_owned())),
                        ("terminal_seq", seq_value(event)),
                        ("attempt_count", integer_u32(attempt_count)),
                        ("dead_reason_code", SqlValue::Text(reason.code().to_owned())),
                        ("dead_reason_detail", SqlValue::Text(detail)),
                    ],
                }]
            }
            _ => Vec::new(),
        })
    }
}

/// `Insert`, not `Upsert`: the job id is derived from
/// `(project, kind, idempotency_key)` by `pos-sched`, so a second enqueue of
/// the same logical work collides on the primary key and fails the whole
/// append transaction. That is the structural half of exactly-once — the
/// admission check in `pos-sched::queue` is the friendly half.
fn enqueued_row(event: &Event, body: JobEnqueuedBody) -> RowWrite {
    let (
        job_id,
        project_id,
        job_kind,
        idempotency_key,
        priority,
        class,
        payload,
        run_at_ts_ms,
        attempt_count_max,
        cron_id,
    ) = match body {
        JobEnqueuedBody::V1 { job_id, job_kind } => (
            job_id,
            SqlValue::Null,
            job_kind,
            job_id.to_hex(),
            JobPriority::Normal,
            JobClass::Maintenance,
            Vec::new(),
            event.ts_ms,
            JOB_ATTEMPT_COUNT_MAX_DEFAULT,
            SqlValue::Null,
        ),
        JobEnqueuedBody::V2 {
            job_id,
            project_id,
            job_kind,
            idempotency_key,
            priority,
            class,
            payload,
            run_at_ts_ms,
            attempt_count_max,
            cron,
        } => (
            job_id,
            id_blob(project_id.into_bytes()),
            job_kind,
            idempotency_key,
            priority,
            class,
            payload,
            run_at_ts_ms,
            attempt_count_max,
            optional_id_blob(cron.map(|cron| cron.cron_id.into_bytes())),
        ),
    };
    RowWrite::Insert {
        key: vec![id_blob(job_id.into_bytes())],
        values: vec![
            project_id,
            SqlValue::Text(job_kind),
            SqlValue::Text(idempotency_key),
            SqlValue::Text(JOB_STATE_QUEUED.to_owned()),
            SqlValue::Integer(i64::from(priority.rank())),
            SqlValue::Text(class.as_str().to_owned()),
            SqlValue::Blob(payload),
            seq_value(event),
            integer_u64(run_at_ts_ms),
            SqlValue::Integer(0),
            integer_u32(attempt_count_max),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            cron_id,
        ],
    }
}

/// Cron schedule rows (F36/F37). `last_fired_ts_ms` is advanced by the
/// `JobEnqueued` fact that the firing produced, never by the tick itself: the
/// two are one atomic append, so a crash between "decided to fire" and
/// "recorded the fire" is not representable.
struct CronsProjection;

const CRONS_TABLE: TableDef = TableDef {
    name: "proj_crons",
    version: 1,
    key_columns: &[ColumnDef {
        name: "cron_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "project_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "job_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "expr",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "tz",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "overlap_policy",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "enabled",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "priority_rank",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "class",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "payload",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "registered_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "registered_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_fired_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_job_id",
            kind: ColumnKind::Blob,
        },
    ],
    indexes: &[],
};

impl Projection for CronsProjection {
    fn table(&self) -> &TableDef {
        &CRONS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "CronRegistered" | "CronEnablementSet" | "JobEnqueued" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&CRONS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::CronRegistered(CronRegisteredBody::V1 {
                cron_id,
                project_id,
                job_kind,
                expr,
                tz,
                overlap_policy,
                enabled,
                priority,
                class,
                payload,
            }) => vec![RowWrite::Upsert {
                key: vec![id_blob(cron_id.into_bytes())],
                values: vec![
                    id_blob(project_id.into_bytes()),
                    SqlValue::Text(job_kind),
                    SqlValue::Text(expr),
                    SqlValue::Text(tz),
                    SqlValue::Text(overlap_policy.as_str().to_owned()),
                    SqlValue::Integer(i64::from(enabled)),
                    SqlValue::Integer(i64::from(priority.rank())),
                    SqlValue::Text(class.as_str().to_owned()),
                    SqlValue::Blob(payload),
                    seq_value(event),
                    integer_u64(event.ts_ms),
                    SqlValue::Null,
                    SqlValue::Null,
                ],
            }],
            DomainEvent::CronEnablementSet(CronEnablementSetBody::V1 { cron_id, enabled }) => {
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(cron_id.into_bytes())],
                    assignments: vec![("enabled", SqlValue::Integer(i64::from(enabled)))],
                }]
            }
            // A cron-originated job advances its schedule's watermark. Strict,
            // because a job naming a cron that was never registered is a
            // fabricated origin, not a tolerable gap.
            DomainEvent::JobEnqueued(JobEnqueuedBody::V2 {
                job_id,
                cron: Some(cron),
                ..
            }) => vec![RowWrite::UpdateOne {
                key: vec![id_blob(cron.cron_id.into_bytes())],
                assignments: vec![
                    ("last_fired_ts_ms", integer_u64(cron.scheduled_ts_ms)),
                    ("last_job_id", id_blob(job_id.into_bytes())),
                ],
            }],
            _ => Vec::new(),
        })
    }
}

/// The audit view: the event log filtered (master plan §7.1 — the audit log
/// is not a second system), materialized for cheap queries.
struct AuditProjection;

const AUDIT_TABLE: TableDef = TableDef {
    name: "proj_audit",
    version: 1,
    key_columns: &[ColumnDef {
        name: "seq",
        kind: ColumnKind::Integer,
    }],
    value_columns: &[
        ColumnDef {
            name: "action",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "target",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "account_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "ts_ms",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[],
};

/// The honest cost ledger rows (m0-s10): one row per gateway model call,
/// keyed by the event seq (the call's durable id). `cost.rollup` aggregates
/// this table; the F23 cost ticker reads it live from m0-s13 on.
struct ModelCallsProjection;

const MODEL_CALLS_TABLE: TableDef = TableDef {
    name: "proj_model_calls",
    version: 1,
    key_columns: &[ColumnDef {
        name: "seq",
        kind: ColumnKind::Integer,
    }],
    value_columns: &[
        ColumnDef {
            name: "project_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "feature",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "agent",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "provider",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "credential_class",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "model",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "tokens_in",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "tokens_out",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "provider_cost_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "usd_micros",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "outcome",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "ts_ms",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[],
};

impl Projection for ModelCallsProjection {
    fn table(&self) -> &TableDef {
        &MODEL_CALLS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        if event.kind.as_str() != "ModelCallCompleted" {
            return Ok(Vec::new());
        }
        let Some(DomainEvent::ModelCallCompleted(ModelCallCompletedBody::V1 {
            project_id,
            feature,
            agent,
            provider,
            credential_class,
            model,
            tokens_in,
            tokens_out,
            wall_ms,
            provider_cost_kind,
            usd_micros,
            outcome,
        })) = decode_for(&MODEL_CALLS_TABLE, event)?
        else {
            return Ok(Vec::new());
        };
        Ok(vec![RowWrite::Upsert {
            key: vec![seq_value(event)],
            values: vec![
                SqlValue::Blob(project_id.into_bytes().to_vec()),
                SqlValue::Text(feature),
                agent.map_or(SqlValue::Null, SqlValue::Text),
                SqlValue::Text(provider),
                SqlValue::Text(credential_class),
                SqlValue::Text(model),
                SqlValue::Integer(i64::try_from(tokens_in).unwrap_or(i64::MAX)),
                SqlValue::Integer(i64::try_from(tokens_out).unwrap_or(i64::MAX)),
                SqlValue::Integer(i64::try_from(wall_ms).unwrap_or(i64::MAX)),
                SqlValue::Text(provider_cost_kind),
                SqlValue::Integer(i64::try_from(usd_micros).unwrap_or(i64::MAX)),
                SqlValue::Text(outcome),
                SqlValue::Integer(i64::try_from(event.ts_ms).unwrap_or(i64::MAX)),
            ],
        }])
    }
}

impl Projection for AuditProjection {
    fn table(&self) -> &TableDef {
        &AUDIT_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        if event.kind.as_str() != "AccountAudited" {
            return Ok(Vec::new());
        }
        let Some(DomainEvent::AccountAudited(AccountAuditedBody::V1 {
            account_id,
            action,
            target,
        })) = decode_for(&AUDIT_TABLE, event)?
        else {
            return Ok(Vec::new());
        };
        Ok(vec![RowWrite::Upsert {
            key: vec![seq_value(event)],
            values: vec![
                SqlValue::Text(action),
                SqlValue::Text(target),
                SqlValue::Blob(account_id.into_bytes().to_vec()),
                SqlValue::Integer(i64::try_from(event.ts_ms).unwrap_or(i64::MAX)),
            ],
        }])
    }
}
