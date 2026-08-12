//! Typed reads over the rebuildable Run projections (m0-s12).
//!
//! The harness never scans the event log for normal operation. It reloads
//! these rows after every conditional-append conflict or process restart;
//! replay remains the mechanism that recreates the rows from durable facts.

use crate::{
    RunBudget, RunBudgetDimension, RunToolCall, RunToolGrant, RunToolGrantMode, RunUsage,
    RunValidationRef, RunValidationStatus,
};
use pos_foundation::{CheckpointId, EventSeq, ProjectId, RunId, ToolCallId, ValidationId};
use pos_log::ProjectLog;
use pos_store::StoreError;
use pos_store::rusqlite::OptionalExtension;
use std::fmt;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum RunStatus {
    Requested,
    Preflight,
    Running,
    WaitingApproval,
    WaitingInput,
    Validating,
    Paused,
    Done,
    Failed,
    Canceled,
}

impl RunStatus {
    fn parse(value: &str) -> Option<Self> {
        match value {
            "requested" => Some(Self::Requested),
            "preflight" => Some(Self::Preflight),
            "running" => Some(Self::Running),
            "waiting_approval" => Some(Self::WaitingApproval),
            "waiting_input" => Some(Self::WaitingInput),
            "validating" => Some(Self::Validating),
            "paused" => Some(Self::Paused),
            "done" | "completed" => Some(Self::Done),
            "failed" => Some(Self::Failed),
            "canceled" => Some(Self::Canceled),
            _ => None,
        }
    }

    #[must_use]
    pub const fn is_terminal(self) -> bool {
        matches!(self, Self::Done | Self::Failed | Self::Canceled)
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PendingRunControl {
    Pause,
    Cancel,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum RunPauseState {
    Budget {
        dimension: RunBudgetDimension,
        limit: u64,
        spent: u64,
        pending: u64,
        requested: u64,
    },
    Requested {
        reason: String,
    },
    ToolWeather {
        code: String,
    },
    ReservationExceeded {
        dimension: RunBudgetDimension,
    },
}

impl PendingRunControl {
    fn parse(value: Option<&str>) -> Option<Self> {
        match value {
            Some("pause") => Some(Self::Pause),
            Some("cancel") => Some(Self::Cancel),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunState {
    pub run_id: RunId,
    pub project_id: Option<ProjectId>,
    pub status: RunStatus,
    pub worker: String,
    pub runtime_id: String,
    pub executor: String,
    pub autonomy_level: u8,
    pub started_seq: EventSeq,
    pub finished_seq: Option<EventSeq>,
    pub committed_step_count: u32,
    pub checkpointed_step_count: u32,
    pub budget: RunBudget,
    pub spent: RunUsage,
    pub tainted: bool,
    pub tool_grants: Vec<RunToolGrant>,
    pub parent_run_id: Option<RunId>,
    pub lineage_depth: u8,
    pub validation: Option<RunValidationRef>,
    pub pending_control: Option<PendingRunControl>,
    pub pending_control_reason: Option<String>,
    pub pause: Option<RunPauseState>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunEffectState {
    pub recorded_seq: EventSeq,
    pub output_digest: [u8; 32],
    pub spent: RunUsage,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunCheckpointState {
    pub checkpoint_id: CheckpointId,
    pub state_digest: [u8; 32],
    pub saved_seq: EventSeq,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct RunStepState {
    pub run_id: RunId,
    pub step_index: u32,
    pub summary: String,
    pub phase: String,
    pub digest: Option<[u8; 32]>,
    pub tool_call: Option<RunToolCall>,
    pub reserved: RunUsage,
    pub committed_seq: EventSeq,
    pub effect: Option<RunEffectState>,
    pub checkpoint: Option<RunCheckpointState>,
}

#[derive(Debug)]
pub enum RunReadError {
    Store(StoreError),
    CorruptProjection { table: &'static str, reason: String },
}

impl fmt::Display for RunReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::CorruptProjection { table, reason } => {
                write!(formatter, "{table} is corrupt: {reason}")
            }
        }
    }
}

impl std::error::Error for RunReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::CorruptProjection { .. } => None,
        }
    }
}

impl From<StoreError> for RunReadError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

struct RunRaw {
    project_id: Option<Vec<u8>>,
    status: String,
    worker: String,
    runtime_id: String,
    executor: String,
    autonomy_level: i64,
    started_seq: i64,
    finished_seq: Option<i64>,
    committed_step_count: i64,
    checkpointed_step_count: i64,
    budget: [i64; 7],
    spent: [i64; 7],
    tainted: i64,
    parent_run_id: Option<Vec<u8>>,
    lineage_depth: i64,
    validation_id: Option<Vec<u8>>,
    validation_status: Option<String>,
    pending_control: Option<String>,
    pending_control_reason: Option<String>,
    pause_kind: Option<String>,
    pause_detail: Option<String>,
    pause_dimension: Option<String>,
    pause_limit: Option<i64>,
    pause_spent: Option<i64>,
    pause_pending: Option<i64>,
    pause_requested: Option<i64>,
}

/// Reads one Run by id from `proj_runs`.
pub fn read_run(log: &ProjectLog, run_id: RunId) -> Result<Option<RunState>, RunReadError> {
    let raw = log
        .store()
        .db()
        .with_reader("read Run projection", |connection| {
            connection
                .query_row(
                    "SELECT project_id, status, worker, runtime_id, executor, autonomy_level, \
                        started_seq, finished_seq, committed_step_count, checkpointed_step_count, \
                        budget_tokens, budget_usd_micros, budget_wall_ms, budget_storage_bytes, \
                        budget_tool_calls, budget_retries, budget_steps, \
                        spent_tokens, spent_usd_micros, spent_wall_ms, spent_storage_bytes, \
                        spent_tool_calls, spent_retries, spent_steps, tainted, parent_run_id, \
                        lineage_depth, validation_id, validation_status, pending_control, control_reason, \
                        pause_kind, pause_detail, pause_dimension, pause_limit, pause_spent, pause_pending, pause_requested \
                   FROM proj_runs WHERE run_id = ?1",
                    [run_id.into_bytes().to_vec()],
                    |row| {
                        Ok(RunRaw {
                            project_id: row.get(0)?,
                            status: row.get(1)?,
                            worker: row.get(2)?,
                            runtime_id: row.get(3)?,
                            executor: row.get(4)?,
                            autonomy_level: row.get(5)?,
                            started_seq: row.get(6)?,
                            finished_seq: row.get(7)?,
                            committed_step_count: row.get(8)?,
                            checkpointed_step_count: row.get(9)?,
                            budget: [
                                row.get(10)?,
                                row.get(11)?,
                                row.get(12)?,
                                row.get(13)?,
                                row.get(14)?,
                                row.get(15)?,
                                row.get(16)?,
                            ],
                            spent: [
                                row.get(17)?,
                                row.get(18)?,
                                row.get(19)?,
                                row.get(20)?,
                                row.get(21)?,
                                row.get(22)?,
                                row.get(23)?,
                            ],
                            tainted: row.get(24)?,
                            parent_run_id: row.get(25)?,
                            lineage_depth: row.get(26)?,
                            validation_id: row.get(27)?,
                            validation_status: row.get(28)?,
                            pending_control: row.get(29)?,
                            pending_control_reason: row.get(30)?,
                            pause_kind: row.get(31)?,
                            pause_detail: row.get(32)?,
                            pause_dimension: row.get(33)?,
                            pause_limit: row.get(34)?,
                            pause_spent: row.get(35)?,
                            pause_pending: row.get(36)?,
                            pause_requested: row.get(37)?,
                        })
                    },
                )
                .optional()
        })?;
    let Some(raw) = raw else {
        return Ok(None);
    };
    let tool_grants = read_tool_grants(log, run_id)?;
    parse_run(run_id, raw, tool_grants).map(Some)
}

fn read_tool_grants(log: &ProjectLog, run_id: RunId) -> Result<Vec<RunToolGrant>, RunReadError> {
    log.store()
        .db()
        .with_reader("read Run tool grants", |connection| {
            let mut statement = connection.prepare_cached(
                "SELECT tool_id, mode FROM proj_run_tool_grants \
                 WHERE run_id = ?1 ORDER BY tool_id",
            )?;
            let rows = statement.query_map([run_id.into_bytes().to_vec()], |row| {
                Ok((row.get::<_, String>(0)?, row.get::<_, String>(1)?))
            })?;
            rows.collect::<Result<Vec<_>, _>>()
        })?
        .into_iter()
        .map(|(tool_id, mode)| {
            let mode = RunToolGrantMode::parse(&mode).ok_or_else(|| {
                corrupt(
                    "proj_run_tool_grants",
                    format!("unknown grant mode {mode:?} for tool {tool_id:?}"),
                )
            })?;
            Ok(RunToolGrant { tool_id, mode })
        })
        .collect()
}

fn parse_run(
    run_id: RunId,
    raw: RunRaw,
    tool_grants: Vec<RunToolGrant>,
) -> Result<RunState, RunReadError> {
    let status = RunStatus::parse(&raw.status)
        .ok_or_else(|| corrupt("proj_runs", format!("unknown status {:?}", raw.status)))?;
    let pending_control = PendingRunControl::parse(raw.pending_control.as_deref());
    if raw.pending_control.is_some() && pending_control.is_none() {
        return Err(corrupt(
            "proj_runs",
            format!("unknown pending control {:?}", raw.pending_control),
        ));
    }
    if pending_control.is_some() != raw.pending_control_reason.is_some() {
        return Err(corrupt(
            "proj_runs",
            "pending control and control reason must be populated together".to_owned(),
        ));
    }
    let pause = parse_pause(&raw)?;
    let validation = match (raw.validation_id, raw.validation_status) {
        (None, None) => None,
        (Some(validation_id), Some(status)) => {
            let status = RunValidationStatus::parse(&status).ok_or_else(|| {
                corrupt("proj_runs", format!("unknown validation status {status:?}"))
            })?;
            Some(RunValidationRef {
                validation_id: ValidationId::from_bytes(required_id(
                    validation_id,
                    "proj_runs",
                    "validation_id",
                )?),
                status,
            })
        }
        _ => {
            return Err(corrupt(
                "proj_runs",
                "validation id and status must be populated together".to_owned(),
            ));
        }
    };
    if (status == RunStatus::Paused) != pause.is_some() {
        return Err(corrupt(
            "proj_runs",
            format!("status {status:?} disagrees with typed pause columns"),
        ));
    }
    Ok(RunState {
        run_id,
        project_id: optional_id(raw.project_id, "proj_runs", "project_id")?
            .map(ProjectId::from_bytes),
        status,
        worker: raw.worker,
        runtime_id: raw.runtime_id,
        executor: raw.executor,
        autonomy_level: to_u8(raw.autonomy_level, "proj_runs", "autonomy_level")?,
        started_seq: EventSeq::new(to_u64(raw.started_seq, "proj_runs", "started_seq")?),
        finished_seq: raw
            .finished_seq
            .map(|value| to_u64(value, "proj_runs", "finished_seq").map(EventSeq::new))
            .transpose()?,
        committed_step_count: to_u32(
            raw.committed_step_count,
            "proj_runs",
            "committed_step_count",
        )?,
        checkpointed_step_count: to_u32(
            raw.checkpointed_step_count,
            "proj_runs",
            "checkpointed_step_count",
        )?,
        budget: budget_from(raw.budget, "proj_runs")?,
        spent: usage_from(raw.spent, "proj_runs")?,
        tainted: match raw.tainted {
            0 => false,
            1 => true,
            value => {
                return Err(corrupt(
                    "proj_runs",
                    format!("tainted must be 0 or 1, found {value}"),
                ));
            }
        },
        tool_grants,
        parent_run_id: optional_id(raw.parent_run_id, "proj_runs", "parent_run_id")?
            .map(RunId::from_bytes),
        lineage_depth: to_u8(raw.lineage_depth, "proj_runs", "lineage_depth")?,
        validation,
        pending_control,
        pending_control_reason: raw.pending_control_reason,
        pause,
    })
}

fn parse_pause(raw: &RunRaw) -> Result<Option<RunPauseState>, RunReadError> {
    let metric_count = [
        raw.pause_limit,
        raw.pause_spent,
        raw.pause_pending,
        raw.pause_requested,
    ]
    .into_iter()
    .flatten()
    .count();
    let dimension = raw
        .pause_dimension
        .as_deref()
        .map(|value| {
            RunBudgetDimension::parse(value)
                .ok_or_else(|| corrupt("proj_runs", format!("unknown pause dimension {value:?}")))
        })
        .transpose()?;
    let Some(kind) = raw.pause_kind.as_deref() else {
        if raw.pause_detail.is_none() && dimension.is_none() && metric_count == 0 {
            return Ok(None);
        }
        return Err(corrupt(
            "proj_runs",
            "pause detail exists without pause_kind".to_owned(),
        ));
    };
    let pause = match kind {
        "budget" => {
            let (Some(dimension), Some(limit), Some(spent), Some(pending), Some(requested)) = (
                dimension,
                raw.pause_limit,
                raw.pause_spent,
                raw.pause_pending,
                raw.pause_requested,
            ) else {
                return Err(corrupt(
                    "proj_runs",
                    "budget pause is missing a dimension or measurement".to_owned(),
                ));
            };
            if raw.pause_detail.is_some() {
                return Err(corrupt(
                    "proj_runs",
                    "budget pause cannot carry free-form detail".to_owned(),
                ));
            }
            RunPauseState::Budget {
                dimension,
                limit: to_u64(limit, "proj_runs", "pause_limit")?,
                spent: to_u64(spent, "proj_runs", "pause_spent")?,
                pending: to_u64(pending, "proj_runs", "pause_pending")?,
                requested: to_u64(requested, "proj_runs", "pause_requested")?,
            }
        }
        "requested" => {
            if dimension.is_some() || metric_count != 0 {
                return Err(corrupt(
                    "proj_runs",
                    "requested pause cannot carry budget measurements".to_owned(),
                ));
            }
            RunPauseState::Requested {
                reason: raw.pause_detail.clone().ok_or_else(|| {
                    corrupt("proj_runs", "requested pause has no reason".to_owned())
                })?,
            }
        }
        "tool_weather" => {
            if dimension.is_some() || metric_count != 0 {
                return Err(corrupt(
                    "proj_runs",
                    "tool-weather pause cannot carry budget measurements".to_owned(),
                ));
            }
            RunPauseState::ToolWeather {
                code: raw.pause_detail.clone().ok_or_else(|| {
                    corrupt("proj_runs", "tool-weather pause has no code".to_owned())
                })?,
            }
        }
        "reservation_exceeded" => {
            let Some(dimension) = dimension else {
                return Err(corrupt(
                    "proj_runs",
                    "reservation pause has no dimension".to_owned(),
                ));
            };
            if raw.pause_detail.is_some() || metric_count != 0 {
                return Err(corrupt(
                    "proj_runs",
                    "reservation pause has unexpected detail or measurements".to_owned(),
                ));
            }
            RunPauseState::ReservationExceeded { dimension }
        }
        _ => {
            return Err(corrupt("proj_runs", format!("unknown pause kind {kind:?}")));
        }
    };
    Ok(Some(pause))
}

struct StepRaw {
    summary: String,
    phase: String,
    digest: Option<Vec<u8>>,
    tool_id: Option<String>,
    tool_descriptor_version: Option<i64>,
    tool_call_id: Option<Vec<u8>>,
    idempotency_key: Option<String>,
    input: Option<Vec<u8>>,
    reserved: [i64; 7],
    committed_seq: i64,
    effect_recorded_seq: Option<i64>,
    output_digest: Option<Vec<u8>>,
    spent: [i64; 7],
    checkpoint_id: Option<Vec<u8>>,
    checkpoint_digest: Option<Vec<u8>>,
    checkpoint_seq: Option<i64>,
}

/// Reads one committed step. Absence means no ledger fact exists for that
/// index; callers must never manufacture an authorized tool call in that case.
pub fn read_run_step(
    log: &ProjectLog,
    run_id: RunId,
    step_index: u32,
) -> Result<Option<RunStepState>, RunReadError> {
    let raw = log.store().db().with_reader("read Run step projection", |connection| {
        connection
            .query_row(
                "SELECT summary, phase, step_digest, tool_id, tool_descriptor_version, tool_call_id, idempotency_key, input, \
                        reserved_tokens, reserved_usd_micros, reserved_wall_ms, \
                        reserved_storage_bytes, reserved_tool_calls, reserved_retries, \
                        reserved_steps, committed_seq, effect_recorded_seq, output_digest, \
                        spent_tokens, spent_usd_micros, spent_wall_ms, spent_storage_bytes, \
                        spent_tool_calls, spent_retries, spent_steps, checkpoint_id, \
                        checkpoint_digest, checkpoint_seq \
                   FROM proj_run_steps WHERE run_id = ?1 AND step_index = ?2",
                (run_id.into_bytes().to_vec(), i64::from(step_index)),
                |row| {
                    Ok(StepRaw {
                        summary: row.get(0)?,
                        phase: row.get(1)?,
                        digest: row.get(2)?,
                        tool_id: row.get(3)?,
                        tool_descriptor_version: row.get(4)?,
                        tool_call_id: row.get(5)?,
                        idempotency_key: row.get(6)?,
                        input: row.get(7)?,
                        reserved: [
                            row.get(8)?,
                            row.get(9)?,
                            row.get(10)?,
                            row.get(11)?,
                            row.get(12)?,
                            row.get(13)?,
                            row.get(14)?,
                        ],
                        committed_seq: row.get(15)?,
                        effect_recorded_seq: row.get(16)?,
                        output_digest: row.get(17)?,
                        spent: [
                            row.get(18)?,
                            row.get(19)?,
                            row.get(20)?,
                            row.get(21)?,
                            row.get(22)?,
                            row.get(23)?,
                            row.get(24)?,
                        ],
                        checkpoint_id: row.get(25)?,
                        checkpoint_digest: row.get(26)?,
                        checkpoint_seq: row.get(27)?,
                    })
                },
            )
            .optional()
    })?;
    raw.map(|raw| parse_step(run_id, step_index, raw))
        .transpose()
}

fn parse_step(run_id: RunId, step_index: u32, raw: StepRaw) -> Result<RunStepState, RunReadError> {
    let tool_call = match (
        raw.tool_id,
        raw.tool_descriptor_version,
        raw.tool_call_id,
        raw.idempotency_key,
        raw.input,
    ) {
        (None, None, None, None, None) => None,
        (
            Some(tool_id),
            Some(descriptor_version),
            Some(call_id),
            Some(idempotency_key),
            Some(input),
        ) => Some(RunToolCall {
            tool_id,
            descriptor_version: u16::try_from(descriptor_version).map_err(|_| {
                corrupt(
                    "proj_run_steps",
                    format!("tool_descriptor_version is outside u16: {descriptor_version}"),
                )
            })?,
            call_id: ToolCallId::from_bytes(required_id(
                call_id,
                "proj_run_steps",
                "tool_call_id",
            )?),
            idempotency_key,
            input,
        }),
        _ => {
            return Err(corrupt(
                "proj_run_steps",
                "tool call columns are only partially populated".to_owned(),
            ));
        }
    };
    let effect = match (raw.effect_recorded_seq, raw.output_digest) {
        (None, None) => None,
        (Some(seq), Some(digest)) => Some(RunEffectState {
            recorded_seq: EventSeq::new(to_u64(seq, "proj_run_steps", "effect_recorded_seq")?),
            output_digest: required_digest(digest, "output_digest")?,
            spent: usage_from(raw.spent, "proj_run_steps")?,
        }),
        _ => {
            return Err(corrupt(
                "proj_run_steps",
                "effect receipt columns are only partially populated".to_owned(),
            ));
        }
    };
    let checkpoint = match (raw.checkpoint_id, raw.checkpoint_digest, raw.checkpoint_seq) {
        (None, None, None) => None,
        (Some(id), Some(digest), Some(seq)) => Some(RunCheckpointState {
            checkpoint_id: CheckpointId::from_bytes(required_id(
                id,
                "proj_run_steps",
                "checkpoint_id",
            )?),
            state_digest: required_digest(digest, "checkpoint_digest")?,
            saved_seq: EventSeq::new(to_u64(seq, "proj_run_steps", "checkpoint_seq")?),
        }),
        _ => {
            return Err(corrupt(
                "proj_run_steps",
                "checkpoint columns are only partially populated".to_owned(),
            ));
        }
    };
    Ok(RunStepState {
        run_id,
        step_index,
        summary: raw.summary,
        phase: raw.phase,
        digest: raw
            .digest
            .map(|value| required_digest(value, "step_digest"))
            .transpose()?,
        tool_call,
        reserved: usage_from(raw.reserved, "proj_run_steps")?,
        committed_seq: EventSeq::new(to_u64(
            raw.committed_seq,
            "proj_run_steps",
            "committed_seq",
        )?),
        effect,
        checkpoint,
    })
}

fn budget_from(values: [i64; 7], table: &'static str) -> Result<RunBudget, RunReadError> {
    Ok(RunBudget {
        tokens: to_u64(values[0], table, "budget_tokens")?,
        usd_micros: to_u64(values[1], table, "budget_usd_micros")?,
        wall_ms: to_u64(values[2], table, "budget_wall_ms")?,
        storage_bytes: to_u64(values[3], table, "budget_storage_bytes")?,
        tool_calls: to_u32(values[4], table, "budget_tool_calls")?,
        retries: to_u32(values[5], table, "budget_retries")?,
        steps: to_u32(values[6], table, "budget_steps")?,
    })
}

fn usage_from(values: [i64; 7], table: &'static str) -> Result<RunUsage, RunReadError> {
    Ok(RunUsage {
        tokens: to_u64(values[0], table, "tokens")?,
        usd_micros: to_u64(values[1], table, "usd_micros")?,
        wall_ms: to_u64(values[2], table, "wall_ms")?,
        storage_bytes: to_u64(values[3], table, "storage_bytes")?,
        tool_calls: to_u32(values[4], table, "tool_calls")?,
        retries: to_u32(values[5], table, "retries")?,
        steps: to_u32(values[6], table, "steps")?,
    })
}

fn optional_id(
    value: Option<Vec<u8>>,
    table: &'static str,
    field: &'static str,
) -> Result<Option<[u8; 16]>, RunReadError> {
    value
        .map(|value| required_id(value, table, field))
        .transpose()
}

fn required_id(
    value: Vec<u8>,
    table: &'static str,
    field: &'static str,
) -> Result<[u8; 16], RunReadError> {
    value.try_into().map_err(|value: Vec<u8>| {
        corrupt(
            table,
            format!("{field} must contain 16 bytes, found {}", value.len()),
        )
    })
}

fn required_digest(value: Vec<u8>, field: &'static str) -> Result<[u8; 32], RunReadError> {
    value.try_into().map_err(|value: Vec<u8>| {
        corrupt(
            "proj_run_steps",
            format!("{field} must contain 32 bytes, found {}", value.len()),
        )
    })
}

fn to_u64(value: i64, table: &'static str, field: &'static str) -> Result<u64, RunReadError> {
    u64::try_from(value).map_err(|_| corrupt(table, format!("{field} is negative: {value}")))
}

fn to_u32(value: i64, table: &'static str, field: &'static str) -> Result<u32, RunReadError> {
    u32::try_from(value).map_err(|_| corrupt(table, format!("{field} is outside u32: {value}")))
}

fn to_u8(value: i64, table: &'static str, field: &'static str) -> Result<u8, RunReadError> {
    u8::try_from(value).map_err(|_| corrupt(table, format!("{field} is outside u8: {value}")))
}

fn corrupt(table: &'static str, reason: String) -> RunReadError {
    RunReadError::CorruptProjection { table, reason }
}
