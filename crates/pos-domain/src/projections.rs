//! The v0 projections (m0-s03): pure `event → typed row writes` over the
//! domain vocabulary. No SQL lives here — `pos-log/src/apply/` renders and
//! executes the writes inside the append/replay transaction, which is what
//! keeps these functions deterministic by construction.
//!
//! Each projection matches its kinds by tag before decoding, so an event
//! only pays decode cost once per projection that actually cares about it.

use crate::events::{
    AccountAuditedBody, DomainEvent, JobCompletedBody, JobEnqueuedBody, ProjectCreatedBody,
    ProjectRenamedBody, RunFinishedBody, RunStartedBody, RunStepCommittedBody,
};
use pos_log::{
    ApplyError, ColumnDef, ColumnKind, Event, LogError, Projection, ProjectionRegistry, RowWrite,
    SqlValue, TableDef,
};

/// Builds the registry every v0 shell opens projects with.
pub fn v0_registry() -> Result<ProjectionRegistry, LogError> {
    ProjectionRegistry::new(vec![
        Box::new(ProjectsProjection),
        Box::new(RunsProjection),
        Box::new(RunStepsProjection),
        Box::new(JobsProjection),
        Box::new(AuditProjection),
    ])
}

fn decode_for(table: &TableDef, event: &Event) -> Result<Option<DomainEvent>, ApplyError> {
    DomainEvent::decode(&event.kind, &event.body).map_err(|error| ApplyError {
        reason: format!("{}: {error}", table.name),
    })
}

fn seq_value(event: &Event) -> SqlValue {
    SqlValue::Integer(i64::try_from(event.seq.value()).unwrap_or(i64::MAX))
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
    version: 1,
    key_columns: &[ColumnDef {
        name: "run_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "status",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "worker",
            kind: ColumnKind::Text,
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
            name: "step_count",
            kind: ColumnKind::Integer,
        },
    ],
};

impl Projection for RunsProjection {
    fn table(&self) -> &TableDef {
        &RUNS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "RunStarted" | "RunStepCommitted" | "RunFinished" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&RUNS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::RunStarted(RunStartedBody::V1 { run_id, worker, .. }) => {
                vec![RowWrite::Upsert {
                    key: vec![SqlValue::Blob(run_id.into_bytes().to_vec())],
                    values: vec![
                        SqlValue::Text("running".to_owned()),
                        SqlValue::Text(worker),
                        seq_value(event),
                        SqlValue::Null,
                        SqlValue::Integer(0),
                    ],
                }]
            }
            DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 { run_id, .. }) => {
                vec![RowWrite::Increment {
                    key: vec![SqlValue::Blob(run_id.into_bytes().to_vec())],
                    column: "step_count",
                    delta: 1,
                }]
            }
            DomainEvent::RunFinished(RunFinishedBody::V1 {
                run_id, outcome, ..
            }) => vec![RowWrite::Update {
                key: vec![SqlValue::Blob(run_id.into_bytes().to_vec())],
                assignments: vec![
                    ("status", SqlValue::Text(outcome.as_status_str().to_owned())),
                    ("finished_seq", seq_value(event)),
                ],
            }],
            _ => Vec::new(),
        })
    }
}

/// One row per committed run step — the run feed's data (L7: the ledger IS
/// the activity feed).
struct RunStepsProjection;

const RUN_STEPS_TABLE: TableDef = TableDef {
    name: "proj_run_steps",
    version: 1,
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
            name: "seq",
            kind: ColumnKind::Integer,
        },
    ],
};

impl Projection for RunStepsProjection {
    fn table(&self) -> &TableDef {
        &RUN_STEPS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        if event.kind.as_str() != "RunStepCommitted" {
            return Ok(Vec::new());
        }
        let Some(DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 {
            run_id,
            step_index,
            summary,
        })) = decode_for(&RUN_STEPS_TABLE, event)?
        else {
            return Ok(Vec::new());
        };
        Ok(vec![RowWrite::Upsert {
            key: vec![
                SqlValue::Blob(run_id.into_bytes().to_vec()),
                SqlValue::Integer(i64::from(step_index)),
            ],
            values: vec![SqlValue::Text(summary), seq_value(event)],
        }])
    }
}

/// Job rows (F36 substrate; m0-s14's scheduler registers real jobs through
/// the same kinds).
struct JobsProjection;

const JOBS_TABLE: TableDef = TableDef {
    name: "proj_jobs",
    version: 1,
    key_columns: &[ColumnDef {
        name: "job_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "job_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "state",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "enqueued_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "completed_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "attempts",
            kind: ColumnKind::Integer,
        },
    ],
};

impl Projection for JobsProjection {
    fn table(&self) -> &TableDef {
        &JOBS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "JobEnqueued" | "JobCompleted" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&JOBS_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::JobEnqueued(JobEnqueuedBody::V1 { job_id, job_kind }) => {
                vec![RowWrite::Upsert {
                    key: vec![SqlValue::Blob(job_id.into_bytes().to_vec())],
                    values: vec![
                        SqlValue::Text(job_kind),
                        SqlValue::Text("queued".to_owned()),
                        seq_value(event),
                        SqlValue::Null,
                        SqlValue::Integer(0),
                    ],
                }]
            }
            DomainEvent::JobCompleted(JobCompletedBody::V1 { job_id, attempts }) => {
                vec![RowWrite::Update {
                    key: vec![SqlValue::Blob(job_id.into_bytes().to_vec())],
                    assignments: vec![
                        ("state", SqlValue::Text("completed".to_owned())),
                        ("completed_seq", seq_value(event)),
                        ("attempts", SqlValue::Integer(i64::from(attempts))),
                    ],
                }]
            }
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
};

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
