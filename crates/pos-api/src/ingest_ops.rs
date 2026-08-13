//! The m1-s01/m1-s02 surface slice: evidence and stage reads, per-source
//! health, and the reprocess command.
//!
//! Three entries, and the split is deliberate. `evidence.list` and
//! `source.health` are what the browser and the source settings screen read;
//! `ingest.reprocess` is the one write, because re-running the pipeline is a
//! decision a human makes. Submission is *not* here: uploads and watch
//! folders are m1-s07's surface, and registering a half-designed intake
//! command now would freeze the wrong shape.

use crate::ApiError;
use crate::project_ops;
use pos_domain::{
    EVIDENCE_LIST_ROW_COUNT_MAX, EvidenceListFilter, EvidenceReadError, EvidenceRecord,
    EvidenceStatus, IngestStage, SourceHealthRecord, StageRecord, list_evidence,
    list_source_health, list_stages,
};
use pos_foundation::{DeviceId, EvidenceId, ProjectId, SourceId, SystemWallClock, UserId};
use pos_ingest::{
    IngestError, IngestPipeline, PipelineConfig, ReprocessRequest, StageRegistry,
    stage_registry_default,
};
use pos_log::{Actor, ProjectLog};
use pos_sched::{BackoffPolicy, JobQueue, QueueConfig, SchedulerMetrics, SplitMixJitter};
use serde::{Deserialize, Serialize};
use std::sync::Arc;
use ts_rs::TS;

/// Rows an `evidence.list` call answers with when the caller states no bound.
const EVIDENCE_LIST_ROW_COUNT_DEFAULT: u32 = 50;

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct EvidenceListInput {
    pub path: String,
    /// Hex source id; absent means every source.
    #[serde(default)]
    pub source_id: Option<String>,
    /// `raw` … `indexed` | `failed`.
    #[serde(default)]
    pub status: Option<String>,
    #[serde(default)]
    pub row_count_max: Option<u32>,
    /// Include the per-stage history of each returned item. Off by default:
    /// a browser list wants rows, and a stuck-item view wants the history.
    #[serde(default)]
    pub with_stages: bool,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceStageRow {
    pub stage: String,
    pub state: String,
    pub pass: u32,
    pub attempt_index: u32,
    #[ts(type = "number | null")]
    pub wall_ms: Option<u64>,
    #[ts(type = "number | null")]
    pub bytes_read: Option<u64>,
    #[ts(type = "number | null")]
    pub item_count: Option<u64>,
    pub last_error_code: Option<String>,
    pub last_error_detail: Option<String>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceRow {
    pub evidence_id: String,
    pub source_id: String,
    pub source_kind: String,
    pub external_id: String,
    pub external_url: Option<String>,
    pub media_kind: String,
    pub shape: String,
    pub status: String,
    pub canary_level: String,
    pub title: Option<String>,
    pub author: Option<String>,
    #[ts(type = "number")]
    pub occurred_ts_ms: u64,
    #[ts(type = "number")]
    pub byte_size: u64,
    #[ts(type = "number")]
    pub chunk_count: u64,
    pub pass: u32,
    /// The stage this item is waiting on, and whether this build implements
    /// it. An item that stops because a stage lands in a later story renders
    /// as *pending that story*, never as finished and never as failed.
    pub next_stage: Option<String>,
    pub next_stage_owner_story: Option<String>,
    pub next_stage_available: bool,
    pub stages: Vec<EvidenceStageRow>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct EvidenceListReport {
    pub evidence: Vec<EvidenceRow>,
    /// The bound this answer honoured, in-band so a full page is
    /// distinguishable from a truncated one (L8).
    pub row_count_max: u32,
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct SourceHealthInput {
    pub path: String,
    #[serde(default)]
    pub source_id: Option<String>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SourceHealthRow {
    pub source_id: String,
    pub stage: String,
    #[ts(type = "number")]
    pub ok_count: u64,
    #[ts(type = "number")]
    pub failed_count: u64,
    #[ts(type = "number")]
    pub dead_count: u64,
    #[ts(type = "number")]
    pub item_count: u64,
    #[ts(type = "number")]
    pub bytes_total: u64,
    #[ts(type = "number")]
    pub wall_ms_total: u64,
    #[ts(type = "number | null")]
    pub last_success_ts_ms: Option<u64>,
    #[ts(type = "number | null")]
    pub last_failure_ts_ms: Option<u64>,
    pub last_error_code: Option<String>,
    /// The ledger feature name this stage's model spend is recorded under, so
    /// a cost panel joins to `cost.rollup` instead of re-counting (m0-s15).
    pub cost_feature: String,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct SourceHealthReport {
    pub sources: Vec<SourceHealthRow>,
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestReprocessInput {
    pub path: String,
    /// The stage to re-run from. `raw` is refused: re-running it would mean
    /// re-fetching from the source.
    pub from_stage: String,
    /// One item, or every eligible item when absent.
    #[serde(default)]
    pub evidence_id: Option<String>,
    #[serde(default)]
    pub item_count_max: Option<u32>,
    /// Why, recorded on the event. A reprocess with no stated reason is an
    /// unexplained rewrite of derived state.
    pub reason: String,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct IngestReprocessReport {
    pub from_stage: String,
    pub requeued: Vec<String>,
    #[ts(type = "number")]
    pub requeued_count: u64,
    /// Items that never reached the target stage, so there is nothing to
    /// redo. Reported rather than counted as work (L8).
    pub skipped_not_reached: u32,
    pub item_count_max: u32,
    pub truncated: bool,
}

/// `evidence.list` — the browser and source-coverage read.
pub fn evidence_list(input: &EvidenceListInput) -> Result<String, ApiError> {
    let status = parse_optional(input.status.as_deref(), EvidenceStatus::parse, "status")?;
    let source_id =
        parse_optional_id(input.source_id.as_deref(), "sourceId")?.map(SourceId::from_bytes);
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    let row_count_max = input
        .row_count_max
        .unwrap_or(EVIDENCE_LIST_ROW_COUNT_DEFAULT)
        .min(EVIDENCE_LIST_ROW_COUNT_MAX);
    let records = list_evidence(
        &log,
        EvidenceListFilter {
            source_id,
            status,
            row_count_max: Some(row_count_max),
        },
    )
    .map_err(|error| read_error(&error))?;
    let stages = stage_registry_default();
    let mut evidence = Vec::with_capacity(records.len());
    for record in records {
        let history = if input.with_stages {
            list_stages(&log, record.evidence_id).map_err(|error| read_error(&error))?
        } else {
            Vec::new()
        };
        evidence.push(evidence_row(&record, &history, &stages));
    }
    project_ops::to_json(&EvidenceListReport {
        evidence,
        row_count_max,
    })
}

/// `source.health` — per-source, per-stage counters for the settings screen.
pub fn source_health(input: &SourceHealthInput) -> Result<String, ApiError> {
    let source_id =
        parse_optional_id(input.source_id.as_deref(), "sourceId")?.map(SourceId::from_bytes);
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    let rows = list_source_health(&log, source_id).map_err(|error| read_error(&error))?;
    project_ops::to_json(&SourceHealthReport {
        sources: rows.iter().map(source_health_row).collect(),
    })
}

/// `ingest.reprocess` — re-run the pipeline from a stage, never re-fetch.
pub fn ingest_reprocess(
    identity_device: DeviceId,
    identity_user: UserId,
    project_id: ProjectId,
    input: &IngestReprocessInput,
) -> Result<String, ApiError> {
    let from_stage = IngestStage::parse(&input.from_stage).ok_or_else(|| ApiError {
        code: "invalid_input",
        message: format!(
            "stage {:?} is not one of raw, normalize, transcribe, chunk, embed, extract, index",
            input.from_stage
        ),
        retriable: false,
    })?;
    let evidence_id =
        parse_optional_id(input.evidence_id.as_deref(), "evidenceId")?.map(EvidenceId::from_bytes);
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    let queue = ingest_queue(identity_device);
    queue
        .ensure_schema(&log)
        .map_err(|error| sched_error(&error))?;
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(identity_device),
        Arc::new(queue),
        stage_registry_default(),
    );
    let request = ReprocessRequest {
        evidence_id,
        from_stage,
        item_count_max: input
            .item_count_max
            .unwrap_or(pos_ingest::REPROCESS_ITEM_COUNT_MAX)
            .min(pos_ingest::REPROCESS_ITEM_COUNT_MAX),
    };
    let plan = pipeline
        .reprocess(
            &log,
            project_id,
            &SystemWallClock,
            &Actor::User(identity_user),
            request,
            &input.reason,
        )
        .map_err(|error| ingest_error(&error))?;
    project_ops::to_json(&IngestReprocessReport {
        from_stage: from_stage.as_str().to_owned(),
        requeued: plan
            .requeued
            .iter()
            .map(|(evidence_id, _)| evidence_id.to_hex())
            .collect(),
        requeued_count: plan.requeued_count() as u64,
        skipped_not_reached: plan.skipped_not_reached,
        item_count_max: plan.item_count_max,
        truncated: plan.is_truncated(),
    })
}

fn evidence_row(
    record: &EvidenceRecord,
    history: &[StageRecord],
    stages: &StageRegistry,
) -> EvidenceRow {
    let completed = IngestStage::ALL
        .into_iter()
        .find(|stage| EvidenceStatus::after(*stage) == record.status);
    let next = completed.and_then(|stage| stage.next_for(record.media_kind));
    EvidenceRow {
        evidence_id: record.evidence_id.to_hex(),
        source_id: record.source_id.to_hex(),
        source_kind: record.source_kind.clone(),
        external_id: record.external.external_id.clone(),
        external_url: record.external.external_url.clone(),
        media_kind: record.media_kind.as_str().to_owned(),
        shape: record.shape.as_str().to_owned(),
        status: record.status.as_str().to_owned(),
        canary_level: record.canary_level.as_str().to_owned(),
        title: record.title.clone(),
        author: record.author.clone(),
        occurred_ts_ms: record.occurred_ts_ms,
        byte_size: record.byte_size,
        chunk_count: record.chunk_count,
        pass: record.pass,
        next_stage: next.map(|stage| stage.as_str().to_owned()),
        next_stage_owner_story: next.map(|stage| stage.owner_story().to_owned()),
        next_stage_available: next.is_some_and(|stage| stages.contains(stage)),
        stages: history.iter().map(stage_row).collect(),
    }
}

fn stage_row(record: &StageRecord) -> EvidenceStageRow {
    EvidenceStageRow {
        stage: record.stage.as_str().to_owned(),
        state: record.state.as_str().to_owned(),
        pass: record.pass,
        attempt_index: record.attempt_index,
        wall_ms: record.wall_ms,
        bytes_read: record.bytes_read,
        item_count: record.item_count,
        last_error_code: record.last_error_code.clone(),
        last_error_detail: record.last_error_detail.clone(),
    }
}

fn source_health_row(record: &SourceHealthRecord) -> SourceHealthRow {
    SourceHealthRow {
        source_id: record.source_id.to_hex(),
        stage: record.stage.as_str().to_owned(),
        ok_count: record.ok_count,
        failed_count: record.failed_count,
        dead_count: record.dead_count,
        item_count: record.item_count,
        bytes_total: record.bytes_total,
        wall_ms_total: record.wall_ms_total,
        last_success_ts_ms: record.last_success_ts_ms,
        last_failure_ts_ms: record.last_failure_ts_ms,
        last_error_code: record.last_error_code.clone(),
        cost_feature: record.stage.cost_feature().to_owned(),
    }
}

/// A queue for the API's own appends. The read paths never append, and the
/// reprocess path appends through the same derived-id discipline every other
/// enqueue uses.
fn ingest_queue(device: DeviceId) -> JobQueue {
    JobQueue::new(
        QueueConfig {
            device,
            backoff: BackoffPolicy::default(),
            lease_ttl_ms: pos_sched::SCHED_LEASE_TTL_MS_DEFAULT,
        },
        Arc::new(SplitMixJitter::from_os_entropy()),
        Arc::new(SchedulerMetrics::default()),
    )
}

fn parse_optional<T>(
    value: Option<&str>,
    parse: impl Fn(&str) -> Option<T>,
    field: &str,
) -> Result<Option<T>, ApiError> {
    match value {
        None => Ok(None),
        Some(text) => parse(text).map(Some).ok_or_else(|| ApiError {
            code: "invalid_input",
            message: format!("{field} {text:?} is not a recognised value"),
            retriable: false,
        }),
    }
}

fn parse_optional_id(value: Option<&str>, field: &str) -> Result<Option<[u8; 16]>, ApiError> {
    match value {
        None => Ok(None),
        Some(text) => EvidenceId::from_hex(text)
            .map(|id| Some(id.into_bytes()))
            .ok_or_else(|| ApiError {
                code: "invalid_input",
                message: format!("{field} must be 32 lowercase hex characters"),
                retriable: false,
            }),
    }
}

fn read_error(error: &EvidenceReadError) -> ApiError {
    ApiError {
        code: match error {
            EvidenceReadError::Store(_) => "storage_failure",
            EvidenceReadError::CorruptProjection { .. } => "state_mutated",
        },
        message: error.to_string(),
        retriable: false,
    }
}

fn sched_error(error: &pos_sched::SchedError) -> ApiError {
    ApiError {
        code: "storage_failure",
        message: error.to_string(),
        retriable: true,
    }
}

fn ingest_error(error: &IngestError) -> ApiError {
    let code = match error {
        IngestError::StageNotReprocessable { .. } | IngestError::StalePass { .. } => {
            "invalid_input"
        }
        IngestError::UnknownEvidence { .. } => "not_found",
        _ if error.is_retriable() => "storage_failure",
        _ => "invalid_input",
    };
    ApiError {
        code,
        message: error.to_string(),
        retriable: code == "storage_failure",
    }
}

/// The project a shell attributes ingest writes to. Until M5's multi-project
/// workspaces, one `.pos` directory is one project, and the id is read from
/// the row the creation event wrote.
pub fn project_id_of(log: &ProjectLog) -> Result<ProjectId, ApiError> {
    project_ops::project_id(log)
}
