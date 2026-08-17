//! The ingest surface slice: evidence and stage reads, per-source health,
//! the reprocess command, and — since m1-s07 — the intake command that puts
//! bytes into a project in the first place.
//!
//! `evidence.list` and `source.health` are what the browser and the source
//! settings screen read. `ingest.reprocess` re-runs the pipeline, because
//! that is a decision a human makes. `ingest.submit` is the front door: a
//! drag-drop, a file picker, a folder import, and both `pos-bench` gate
//! scenarios all arrive here, which is the point — a gate that measured a
//! private fast path would be measuring something no user can do.

use crate::ApiError;
use crate::ingest_runtime;
use crate::project_ops;
use pos_domain::{
    DomainEvent, EVIDENCE_LIST_ROW_COUNT_MAX, EvidenceListFilter, EvidenceReadError,
    EvidenceRecord, EvidenceStatus, ExternalRef, IngestStage, SourceHealthRecord, StageRecord,
    TRANSCRIPT_SPEAKER_COUNT_MAX, TRANSCRIPT_SPEAKER_NAME_CHARS_MAX,
    TranscriptSegmentSpeakerSetBody, TranscriptSpeakerNamedBody, TranscriptTextCorrectedBody,
    list_evidence, list_source_health, list_stages, list_transcript_segments,
    list_transcript_speakers, read_evidence,
};
use pos_foundation::{
    DeviceId, EvidenceId, ProjectId, SourceId, SystemWallClock, UserId, WallClock,
};
use pos_ingest::{IngestError, IngestPipeline, PipelineConfig, ReprocessRequest, StageRegistry};
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

/// `transcript.get` — one page of a recording's transcript, in time order.
#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranscriptGetInput {
    pub path: String,
    pub evidence_id: String,
    /// Absent means the pass the evidence row is currently on — the reading a
    /// viewer wants. Naming a pass reads an older transcription, which is what
    /// makes a stored citation still resolvable after a re-transcription.
    #[serde(default)]
    pub pass: Option<u32>,
    /// Start after this segment index; absent starts at the beginning.
    #[serde(default)]
    pub after_segment_index: Option<u32>,
    #[serde(default)]
    pub row_count_max: Option<u32>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptSegmentRow {
    pub segment_index: u32,
    #[ts(type = "number")]
    pub start_ms: u64,
    #[ts(type = "number")]
    pub end_ms: u64,
    /// This segment opens a turn — a detected pause, never a claimed speaker.
    pub starts_turn: bool,
    pub speaker_index: u32,
    /// What a viewer renders: the correction when there is one, the model's
    /// own words otherwise.
    pub text: String,
    /// Exactly what the model produced, always. A viewer showing an edited
    /// segment can show the original beside it without a second round trip,
    /// which is what "the original ASR is recoverable" means to a user.
    pub asr_text: String,
    pub edited: bool,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptSpeakerRow {
    pub speaker_index: u32,
    pub name: String,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptReport {
    pub evidence_id: String,
    pub pass: u32,
    pub segments: Vec<TranscriptSegmentRow>,
    pub speakers: Vec<TranscriptSpeakerRow>,
    pub row_count_max: u32,
}

/// `transcript.correct` — a human fixes a word the model misheard.
#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranscriptCorrectInput {
    pub path: String,
    pub evidence_id: String,
    pub pass: u32,
    pub segment_index: u32,
    pub text: String,
}

/// `transcript.speaker-name` — a human names a voice.
#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranscriptSpeakerNameInput {
    pub path: String,
    pub evidence_id: String,
    pub speaker_index: u32,
    pub name: String,
}

/// `transcript.speaker-assign` — a human says who spoke a segment.
#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct TranscriptSpeakerAssignInput {
    pub path: String,
    pub evidence_id: String,
    pub pass: u32,
    pub segment_index: u32,
    pub speaker_index: u32,
}

/// What an edit returns: enough to re-render the one row that changed.
#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct TranscriptEditReport {
    pub evidence_id: String,
    pub pass: u32,
    pub segment_index: u32,
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

/// `ingest.submit` — a file, or a folder of files, becomes Evidence.
///
/// Two ways to say where the bytes are, and exactly one of them per call:
/// `filePath` names something on the machine the runtime is running on
/// (desktop drag-drop, the CLI, `pos-bench`), and *no* `filePath` means the
/// bytes rode in with the call itself over the upload route a browser uses.
/// Naming both is a caller bug, not a preference to resolve.
#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct IngestSubmitInput {
    pub path: String,
    /// A file or a directory. A directory is walked, bounded and ordered.
    #[serde(default)]
    pub file_path: Option<String>,
    /// What to call the item. For an upload that carries its bytes this is
    /// the browser's file name; for a `filePath` call it overrides the name
    /// on disk. Rendered as text, never used as a path (L6).
    #[serde(default)]
    pub file_name: Option<String>,
    /// The selection inside the upload connector these items belong to, so a
    /// watch folder and a drag-drop can be told apart on the source-health
    /// card. Defaults to [`UPLOAD_SOURCE_SCOPE_DEFAULT`].
    #[serde(default)]
    pub source_scope: Option<String>,
}

/// What one file's intake decided.
#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct IngestSubmitRow {
    /// The file's own name, echoed as data so a batch report is readable.
    /// Never the full path: a report is rendered and shared, and the rest of
    /// the path is the user's filesystem, not the project's evidence.
    pub file_name: String,
    pub evidence_id: Option<String>,
    /// What the sniffer decided the bytes are — content, never extension.
    pub media_kind: Option<String>,
    #[ts(type = "number")]
    pub byte_size: u64,
    /// `added`, `duplicate`, or `refused`.
    pub outcome: String,
    pub refused_code: Option<String>,
    pub refused_detail: Option<String>,
}

/// The batch summary. Counts are complete; [`IngestSubmitReport::items`] is
/// bounded, and says so.
#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct IngestSubmitReport {
    pub source_id: String,
    pub added_count: u32,
    /// Files whose exact bytes are already in this project. Not an error and
    /// not silence: re-dropping a file a partner already imported is the most
    /// common thing that happens to this command, and it must read as "you
    /// already have this" rather than as success or as failure.
    pub duplicate_count: u32,
    pub refused_count: u32,
    /// Entries the walk excluded by its own rules — dot-files, symlinks,
    /// nested projects.
    pub skipped_count: u32,
    /// Whether the walk stopped at [`pos_ingest::INTAKE_FILE_COUNT_MAX`] or
    /// [`pos_ingest::INTAKE_DEPTH_MAX`] short of the whole tree.
    pub truncated: bool,
    pub items: Vec<IngestSubmitRow>,
    /// The row bound this answer honoured, in-band so a caller can tell a
    /// full report from a trimmed one (L8).
    pub row_count_max: u32,
    /// Whether a pool in this process will claim what was just queued.
    pub background_workers_running: bool,
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
    /// Whether a pool in this process will claim what was just queued
    /// (m1-s01/ADR-0007). `false` is not a failure — a server shell may queue
    /// work another process runs — but a caller must be able to tell the two
    /// apart, because "requeued 12" with nothing running looks identical to
    /// success until nothing happens.
    pub background_workers_running: bool,
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
    let stages = ingest_runtime::stage_registry();
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

/// `transcript.get` — the transcript viewer's read.
///
/// # Errors
///
/// [`ApiError`] for an unreadable project, a malformed id, or an evidence id
/// this project does not hold.
pub fn transcript_get(input: &TranscriptGetInput) -> Result<String, ApiError> {
    let evidence_id = EvidenceId::from_bytes(parse_id(&input.evidence_id, "evidenceId")?);
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    let record = read_evidence(&log, evidence_id)
        .map_err(|error| read_error(&error))?
        .ok_or_else(|| ApiError {
            code: "not_found",
            message: format!("evidence {} is not in this project", input.evidence_id),
            retriable: false,
        })?;
    let pass = input.pass.unwrap_or(record.pass);
    let row_count_max = input
        .row_count_max
        .unwrap_or(TRANSCRIPT_ROW_COUNT_DEFAULT)
        .min(EVIDENCE_LIST_ROW_COUNT_MAX);
    let segments = list_transcript_segments(
        &log,
        evidence_id,
        pass,
        input.after_segment_index,
        row_count_max,
    )
    .map_err(|error| read_error(&error))?;
    let speakers =
        list_transcript_speakers(&log, evidence_id).map_err(|error| read_error(&error))?;
    project_ops::to_json(&TranscriptReport {
        evidence_id: evidence_id.to_hex(),
        pass,
        segments: segments
            .iter()
            .map(|segment| TranscriptSegmentRow {
                segment_index: segment.segment_index,
                start_ms: segment.start_ms,
                end_ms: segment.end_ms,
                starts_turn: segment.starts_turn,
                speaker_index: segment.speaker_index,
                text: segment.rendered_text().to_owned(),
                asr_text: segment.asr_text.clone(),
                edited: segment.is_edited(),
            })
            .collect(),
        speakers: speakers
            .into_iter()
            .map(|(speaker_index, name)| TranscriptSpeakerRow {
                speaker_index,
                name,
            })
            .collect(),
        row_count_max,
    })
}

/// Rows one `transcript.get` page answers with when the caller states no bound.
const TRANSCRIPT_ROW_COUNT_DEFAULT: u32 = 200;

/// Characters one correction may carry. A "fix a word" edit is a phrase; a
/// megabyte pasted into a transcript segment is not an edit (L8).
const TRANSCRIPT_EDIT_CHARS_MAX: usize = 4 * 1024;

fn checked_edit_text(text: &str, field: &'static str) -> Result<String, ApiError> {
    if text.chars().count() > TRANSCRIPT_EDIT_CHARS_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!("{field} is longer than {TRANSCRIPT_EDIT_CHARS_MAX} characters"),
            retriable: false,
        });
    }
    if text
        .chars()
        .any(|character| character.is_control() && character != '\n' && character != '\t')
    {
        return Err(ApiError {
            code: "invalid_input",
            message: format!("{field} carries control characters"),
            retriable: false,
        });
    }
    Ok(text.to_owned())
}

/// `transcript.correct` — the correction is an event; the ASR output is not
/// touched (m1-s03 invariant T1).
///
/// # Errors
///
/// [`ApiError`] for a malformed id, an over-long or control-bearing text, an
/// unreadable project, or a log append that fails.
pub fn transcript_correct(
    identity_device: DeviceId,
    identity_user: UserId,
    input: &TranscriptCorrectInput,
) -> Result<String, ApiError> {
    let evidence_id = EvidenceId::from_bytes(parse_id(&input.evidence_id, "evidenceId")?);
    let text = checked_edit_text(&input.text, "text")?;
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    append_transcript_event(
        &log,
        identity_device,
        identity_user,
        DomainEvent::TranscriptTextCorrected(TranscriptTextCorrectedBody::V1 {
            evidence_id,
            pass: input.pass,
            segment_index: input.segment_index,
            text,
        }),
    )?;
    project_ops::to_json(&TranscriptEditReport {
        evidence_id: evidence_id.to_hex(),
        pass: input.pass,
        segment_index: input.segment_index,
    })
}

/// `transcript.speaker-name` — names a voice for this recording.
///
/// # Errors
///
/// [`ApiError`] for a malformed id, an out-of-range speaker index, an
/// unusable name, or a log append that fails.
pub fn transcript_speaker_name(
    identity_device: DeviceId,
    identity_user: UserId,
    input: &TranscriptSpeakerNameInput,
) -> Result<String, ApiError> {
    let evidence_id = EvidenceId::from_bytes(parse_id(&input.evidence_id, "evidenceId")?);
    checked_speaker_index(input.speaker_index)?;
    let name = checked_edit_text(input.name.trim(), "name")?;
    if name.is_empty() || name.chars().count() > TRANSCRIPT_SPEAKER_NAME_CHARS_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "a speaker name is 1..={TRANSCRIPT_SPEAKER_NAME_CHARS_MAX} characters"
            ),
            retriable: false,
        });
    }
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    append_transcript_event(
        &log,
        identity_device,
        identity_user,
        DomainEvent::TranscriptSpeakerNamed(TranscriptSpeakerNamedBody::V1 {
            evidence_id,
            speaker_index: input.speaker_index,
            name,
        }),
    )?;
    project_ops::to_json(&TranscriptEditReport {
        evidence_id: evidence_id.to_hex(),
        // A speaker name is not pass-scoped: who was in the room does not
        // change when a better model re-reads the audio.
        pass: 0,
        segment_index: input.speaker_index,
    })
}

/// `transcript.speaker-assign` — says who spoke a segment.
///
/// # Errors
///
/// [`ApiError`] for a malformed id, an out-of-range speaker index, an
/// unreadable project, or a log append that fails.
pub fn transcript_speaker_assign(
    identity_device: DeviceId,
    identity_user: UserId,
    input: &TranscriptSpeakerAssignInput,
) -> Result<String, ApiError> {
    let evidence_id = EvidenceId::from_bytes(parse_id(&input.evidence_id, "evidenceId")?);
    checked_speaker_index(input.speaker_index)?;
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    append_transcript_event(
        &log,
        identity_device,
        identity_user,
        DomainEvent::TranscriptSegmentSpeakerSet(TranscriptSegmentSpeakerSetBody::V1 {
            evidence_id,
            pass: input.pass,
            segment_index: input.segment_index,
            speaker_index: input.speaker_index,
        }),
    )?;
    project_ops::to_json(&TranscriptEditReport {
        evidence_id: evidence_id.to_hex(),
        pass: input.pass,
        segment_index: input.segment_index,
    })
}

fn checked_speaker_index(speaker_index: u32) -> Result<(), ApiError> {
    if speaker_index >= TRANSCRIPT_SPEAKER_COUNT_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "speaker index {speaker_index} is past the {TRANSCRIPT_SPEAKER_COUNT_MAX}-speaker \
                 bound for one recording"
            ),
            retriable: false,
        });
    }
    Ok(())
}

/// Every transcript edit is one appended fact by the user who made it — the
/// same shape for all three, so the actor and the failure mapping cannot drift
/// between them.
fn append_transcript_event(
    log: &ProjectLog,
    identity_device: DeviceId,
    identity_user: UserId,
    event: DomainEvent,
) -> Result<(), ApiError> {
    let request = event
        .into_request(identity_device, Actor::User(identity_user))
        .map_err(|error| ApiError {
            code: "log_failure",
            message: error.to_string(),
            retriable: false,
        })?;
    log.append(request, &SystemWallClock)
        .map_err(|error| ApiError {
            code: "log_failure",
            message: error.to_string(),
            retriable: true,
        })?;
    Ok(())
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

/// The connector kind every intake item carries. One kind, so the upload
/// path has one source-health row per scope rather than one per screen that
/// happens to call it.
pub const UPLOAD_SOURCE_KIND: &str = "upload";

/// The scope an intake item lands in when the caller states none.
pub const UPLOAD_SOURCE_SCOPE_DEFAULT: &str = "uploads";

/// Characters a source scope may carry. It is part of the derived source id,
/// so it is identity, not prose.
const SOURCE_SCOPE_CHARS_MAX: usize = 120;

/// Rows one submit report renders. A folder import may cover up to
/// [`pos_ingest::INTAKE_FILE_COUNT_MAX`] files; the counts stay exact while
/// the per-file list stays a page (L8: the bound is visible, never silent).
const INGEST_SUBMIT_ROW_COUNT_MAX: u32 = 200;

/// `ingest.submit` — bytes become Evidence, and the pipeline starts.
///
/// `staged` is the upload route's half of the contract: the transport has
/// already streamed the request body to a file it owns, and hands the path
/// here rather than decoding the caller's JSON to inject it. A transport that
/// reshaped an input would be the L12 bug this seam exists to prevent.
pub fn ingest_submit(
    identity_device: DeviceId,
    identity_user: UserId,
    project_id: ProjectId,
    queue: &Arc<JobQueue>,
    background_workers_running: bool,
    input: &IngestSubmitInput,
    staged: Option<&std::path::Path>,
) -> Result<String, ApiError> {
    let root = intake_root(input, staged)?;
    let source_scope = source_scope(input)?;
    let plan = pos_ingest::plan_intake(&root).map_err(|error| ingest_error(&error))?;
    let log = project_ops::open_log(std::path::Path::new(&input.path))?;
    queue
        .ensure_schema(&log)
        .map_err(|error| sched_error(&error))?;
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(identity_device),
        Arc::clone(queue),
        ingest_runtime::stage_registry(),
    );
    let source_id = pos_ingest::derive_source_id(UPLOAD_SOURCE_KIND, &source_scope);
    let mut report = IngestSubmitReport {
        source_id: source_id.to_hex(),
        added_count: 0,
        duplicate_count: 0,
        refused_count: 0,
        skipped_count: plan.skipped_count,
        truncated: plan.truncated,
        items: Vec::new(),
        row_count_max: INGEST_SUBMIT_ROW_COUNT_MAX,
        background_workers_running,
    };
    // A stated file name belongs to a single-file call. Applying it to every
    // item of a folder import would name twelve recordings the same thing.
    let stated_name = (plan.files.len() == 1)
        .then_some(input.file_name.as_deref())
        .flatten();
    for file_path in &plan.files {
        let row = submit_one(SubmitOne {
            pipeline: &pipeline,
            log: &log,
            project_id,
            user: identity_user,
            source_scope: &source_scope,
            file_path,
            stated_name,
        });
        match row.outcome.as_str() {
            "added" => report.added_count = report.added_count.saturating_add(1),
            "duplicate" => report.duplicate_count = report.duplicate_count.saturating_add(1),
            _ => report.refused_count = report.refused_count.saturating_add(1),
        }
        if report.items.len() < INGEST_SUBMIT_ROW_COUNT_MAX as usize {
            report.items.push(row);
        }
    }
    project_ops::to_json(&report)
}

/// The arguments of one file's intake. Bundled because every one of them is
/// needed and a seven-argument function is a signature longer than its body.
#[derive(Clone, Copy)]
struct SubmitOne<'a> {
    pipeline: &'a IngestPipeline,
    log: &'a ProjectLog,
    project_id: ProjectId,
    user: UserId,
    source_scope: &'a str,
    file_path: &'a std::path::Path,
    stated_name: Option<&'a str>,
}

/// Ingests one file, turning every failure into a *row* rather than into a
/// failed batch. One unreadable file in a folder of two hundred must not cost
/// the other hundred and ninety-nine — and the reason it failed has to reach
/// the person who dropped it.
fn submit_one(request: SubmitOne<'_>) -> IngestSubmitRow {
    let file_name = request.stated_name.map_or_else(
        || {
            request
                .file_path
                .file_name()
                .map_or_else(String::new, |name| name.to_string_lossy().into_owned())
        },
        str::to_owned,
    );
    let file_name = pos_ingest::intake_title(&file_name);
    let mut intake = match pos_ingest::open_file(request.file_path) {
        Ok(intake) => intake,
        Err(error) => return refused_row(file_name, 0, &error),
    };
    let submission = pos_ingest::EvidenceSubmission {
        source_kind: UPLOAD_SOURCE_KIND.to_owned(),
        source_scope: request.source_scope.to_owned(),
        // An empty external id means "address this by its content", which is
        // what makes re-dropping the same file a visible duplicate instead of
        // a second copy (`pipeline::resolve_external_ref`).
        external: ExternalRef {
            external_id: String::new(),
            external_url: None,
            external_version: None,
        },
        media_kind: intake.media_kind,
        shape: intake.shape,
        occurred_ts_ms: intake
            .modified_ms
            .unwrap_or_else(|| SystemWallClock.now_ms()),
        author: None,
        title: Some(file_name.clone()),
        thread_ref: None,
        actor: Actor::User(request.user),
    };
    let byte_size = intake.byte_size;
    let media_kind = intake.media_kind.as_str().to_owned();
    match request.pipeline.submit(
        request.log,
        request.project_id,
        &SystemWallClock,
        &submission,
        &mut intake.content,
    ) {
        Ok(outcome) => IngestSubmitRow {
            file_name,
            evidence_id: Some(outcome.evidence_id().to_hex()),
            media_kind: Some(media_kind),
            byte_size,
            outcome: if outcome.is_duplicate() {
                "duplicate".to_owned()
            } else {
                "added".to_owned()
            },
            refused_code: None,
            refused_detail: None,
        },
        Err(error) => refused_row(file_name, byte_size, &error),
    }
}

fn refused_row(file_name: String, byte_size: u64, error: &IngestError) -> IngestSubmitRow {
    IngestSubmitRow {
        file_name,
        evidence_id: None,
        media_kind: None,
        byte_size,
        outcome: "refused".to_owned(),
        refused_code: Some(error.code().to_owned()),
        refused_detail: Some(error.to_string()),
    }
}

/// Resolves where the bytes are. Exactly one of the two ways, always.
fn intake_root(
    input: &IngestSubmitInput,
    staged: Option<&std::path::Path>,
) -> Result<std::path::PathBuf, ApiError> {
    match (staged, input.file_path.as_deref()) {
        (Some(path), None) => Ok(path.to_path_buf()),
        (None, Some(path)) if !path.is_empty() => Ok(std::path::PathBuf::from(path)),
        (Some(_), Some(_)) => Err(ApiError {
            code: "invalid_input",
            message: "a submit carries bytes or names a filePath, never both".to_owned(),
            retriable: false,
        }),
        _ => Err(ApiError {
            code: "invalid_input",
            message: "a submit needs a filePath, or bytes on the upload route".to_owned(),
            retriable: false,
        }),
    }
}

fn source_scope(input: &IngestSubmitInput) -> Result<String, ApiError> {
    let scope = input
        .source_scope
        .clone()
        .unwrap_or_else(|| UPLOAD_SOURCE_SCOPE_DEFAULT.to_owned());
    if scope.is_empty() || scope.chars().count() > SOURCE_SCOPE_CHARS_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "sourceScope is 1..={SOURCE_SCOPE_CHARS_MAX} characters; it is part of the \
                 derived source id"
            ),
            retriable: false,
        });
    }
    Ok(scope)
}

/// `ingest.reprocess` — re-run the pipeline from a stage, never re-fetch.
///
/// The queue is the runtime's own, shared with the worker pool, so the enqueue
/// this command commits and the claim that follows it are counted by one set
/// of metrics rather than by two that cannot be compared.
pub fn ingest_reprocess(
    identity_device: DeviceId,
    identity_user: UserId,
    project_id: ProjectId,
    queue: &Arc<JobQueue>,
    background_workers_running: bool,
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
    queue
        .ensure_schema(&log)
        .map_err(|error| sched_error(&error))?;
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(identity_device),
        Arc::clone(queue),
        ingest_runtime::stage_registry(),
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
        background_workers_running,
    })
}

/// The queue one runtime process appends and claims through. Built once per
/// runtime so the pool, the reprocess command, and the metrics snapshot are
/// all talking about the same queue.
pub(crate) fn runtime_queue(device: DeviceId) -> JobQueue {
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

fn parse_id(value: &str, field: &str) -> Result<[u8; 16], ApiError> {
    EvidenceId::from_hex(value)
        .map(EvidenceId::into_bytes)
        .ok_or_else(|| ApiError {
            code: "invalid_input",
            message: format!("{field} must be 32 lowercase hex characters"),
            retriable: false,
        })
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
