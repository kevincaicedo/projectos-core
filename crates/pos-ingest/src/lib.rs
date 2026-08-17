//! # pos-ingest
//!
//! Streaming ingestion pipeline: normalize, transcribe, chunk, embed, extract, index — bounded memory at GB scale (L8).
//!
//! Skeleton created by m0-s01; filled by M1. Charter: master plan §19.
//!
//! ## What this crate is
//!
//! The pipeline is **jobs, not a runtime**. Each stage of master plan §9 is an
//! idempotent `pos-sched` job keyed by `{evidence, stage, pass}`, so
//! crash-resume, retry with backoff, the DLQ, fairness across projects, and
//! per-job tracing all arrive from M0 rather than being rebuilt here (m1-s01).
//! What this crate adds is the stage vocabulary, the streaming discipline, the
//! chunkers, and the reprocess planner.
//!
//! ## Invariant inventory (STYLE: every state machine states its own)
//!
//! - **P1 — a stage never advances on its own.** Advancing means appending
//!   `IngestStageFinished` *and* the next stage's `JobEnqueued` in ONE
//!   transaction ([`JobQueue::enqueue_request`]). A crash between two commits
//!   would leave an item permanently half-ingested with nothing to notice it.
//! - **P2 — effects before facts, and every effect is content-addressed.**
//!   Stages write blobs to the CAS before appending anything. A crash in
//!   between leaves an unreferenced blob the CAS sweep collects; the reverse
//!   order would leave a row pointing at bytes that are not there.
//! - **P3 — a stage handler is a pure-ish function of its inputs.** Given the
//!   same evidence row and the same blobs, a re-run produces byte-identical
//!   events. That is what makes at-least-once delivery safe and what the
//!   kill-matrix digest oracle measures.
//! - **P4 — nothing reads user content into memory whole.** Every read goes
//!   through [`BoundedStream`] with a stated per-stage budget, and
//!   `read_to_end`/`read_to_string`/`fs::read` are mechanically denied in this
//!   crate by `check-discipline`. A 10 GB corpus and a 10 MB one have the same
//!   resident cost (§18).
//! - **P5 — a pass is monotonic.** Stage jobs carry the pass they were
//!   enqueued for; a job whose pass is behind the evidence row is stale work
//!   from a superseded reprocess and refuses permanently instead of writing
//!   over newer output.
//! - **P6 — the pipeline stops honestly.** If the next stage has no registered
//!   handler in this build, the item stops with its status naming the last
//!   completed stage, and [`IngestStage::owner_story`] names what implements
//!   the rest. It is never marked failed, and never claimed complete.

#![forbid(unsafe_code)]

mod audio;
mod budget;
mod captions;
mod chunk;
mod identity;
mod intake;
mod normalize;
mod pipeline;
mod reprocess;
mod segment;
mod transcribe;

pub use audio::{AudioError, AudioSource};
pub use budget::{
    BoundedStream, BufferResidency, PIPELINE_BUFFER_BYTES_MAX, STAGE_BUFFER_BYTES_MAX_DEFAULT,
    STAGE_READ_BYTES, StreamBudget, buffer_residency, reset_buffer_peak,
};
pub use captions::{CAPTION_BLOCK_BYTES_MAX, CAPTION_CUE_COUNT_MAX};
pub use chunk::{
    BoundaryRule, CHUNK_COUNT_MAX, ChunkParams, ChunkStage, TOKEN_BYTES_ESTIMATE, chunk_params_for,
};
pub use identity::{ContentHasher, derive_chunk_id, derive_evidence_id, derive_source_id};
pub use intake::{
    INTAKE_DEPTH_MAX, INTAKE_FILE_BYTES_MAX, INTAKE_FILE_COUNT_MAX, INTAKE_TEXT_BYTES_MAX,
    INTAKE_TITLE_CHARS_MAX, IntakeFile, IntakePlan, SNIFF_PREFIX_BYTES, intake_title, open_file,
    plan_intake, shape_for, sniff_intake,
};
pub use normalize::{NormalizeStage, RECORD_BYTES_MAX, sniff_media_kind};
pub use pipeline::{
    EvidenceSubmission, IngestPipeline, PipelineConfig, STAGE_RETRY_COUNT_MAX, StageContext,
    StageFailure, StageHandler, StageJobHandler, StageOutcome, StageProduct, StageRegistry,
    SubmitOutcome, stage_idempotency_key, stage_job_handlers, stage_registry_default,
};
pub use reprocess::{REPROCESS_ITEM_COUNT_MAX, ReprocessPlan, ReprocessRequest};
pub use segment::{SEGMENT_COUNT_MAX, SEGMENT_RECORD_BYTES, Segment, SegmentReader, SegmentWriter};
pub use transcribe::{
    StageLedgers, TRANSCRIBE_WINDOW_MS_DEFAULT, TRANSCRIPT_SEGMENT_COUNT_MAX, TranscribeRoute,
    TranscribeSetup, TranscribeStage, UnmeteredLedgers,
};

use pos_domain::{EvidenceReadError, IngestStage};
use pos_log::LogError;
use pos_sched::SchedError;
use pos_store::StoreError;
use std::fmt;

/// Typed failures of the ingestion pipeline. Everything here is operating
/// weather — a malformed upload, a missing blob, a full disk — and is handled
/// by callers, never asserted (STYLE: assertions are for programmer errors).
#[derive(Debug)]
pub enum IngestError {
    Store(StoreError),
    Log(LogError),
    Sched(SchedError),
    Read(EvidenceReadError),
    /// A stage asked for a window larger than its stated buffer budget. This
    /// is the streaming discipline refusing, not an allocation failing: the
    /// §18 RSS bound is a property of the code, not of how big the input was.
    BufferBudgetExceeded {
        stage: IngestStage,
        wanted_bytes: usize,
        budget_bytes: usize,
    },
    /// Reading or writing a CAS blob failed at the I/O layer.
    Io {
        operation: &'static str,
        source: std::io::Error,
    },
    /// The evidence id is not in this project.
    UnknownEvidence {
        evidence_id: String,
    },
    /// A second `EvidenceAdded` for an id that already exists. The id derives
    /// from `(source, external ref)`, so this means either a connector that
    /// should have deduped or a genuine content change that belongs in new,
    /// linked evidence — never an overwrite (m1-s02).
    EvidenceExists {
        evidence_id: String,
    },
    /// A stage job outlived the pass it was enqueued for (P5).
    StalePass {
        stage: IngestStage,
        job_pass: u32,
        evidence_pass: u32,
    },
    /// RAW cannot be reprocessed: re-running it would mean re-fetching from
    /// the source, which the milestone forbids outright.
    StageNotReprocessable {
        stage: IngestStage,
    },
    /// The stage's blob input has not been produced yet — the pipeline is
    /// being driven out of order.
    StageInputMissing {
        stage: IngestStage,
        missing: &'static str,
    },
    /// Content that should be UTF-8 text is not, and the media kind claimed
    /// it would be.
    NotUtf8 {
        byte_offset: u64,
    },
    /// A declared bound was exceeded by the input (segment count, chunk
    /// count, record size). Bounded work refuses rather than growing (L8).
    LimitExceeded {
        limit: &'static str,
        value: u64,
        limit_value: u64,
    },
}

impl fmt::Display for IngestError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::Log(source) => write!(formatter, "{source}"),
            Self::Sched(source) => write!(formatter, "{source}"),
            Self::Read(source) => write!(formatter, "{source}"),
            Self::BufferBudgetExceeded {
                stage,
                wanted_bytes,
                budget_bytes,
            } => write!(
                formatter,
                "stage {stage} asked for a {wanted_bytes}-byte window against a \
                 {budget_bytes}-byte buffer budget"
            ),
            Self::Io { operation, source } => write!(formatter, "{operation}: {source}"),
            Self::UnknownEvidence { evidence_id } => {
                write!(formatter, "evidence {evidence_id} is not in this project")
            }
            Self::EvidenceExists { evidence_id } => write!(
                formatter,
                "evidence {evidence_id} already exists; a correction is new linked evidence, \
                 never an overwrite"
            ),
            Self::StalePass {
                stage,
                job_pass,
                evidence_pass,
            } => write!(
                formatter,
                "stage {stage} job was enqueued for pass {job_pass}; the item is on pass \
                 {evidence_pass}"
            ),
            Self::StageNotReprocessable { stage } => write!(
                formatter,
                "stage {stage} cannot be reprocessed: it would re-fetch from the source"
            ),
            Self::StageInputMissing { stage, missing } => {
                write!(formatter, "stage {stage} needs {missing}, which is not set")
            }
            Self::NotUtf8 { byte_offset } => write!(
                formatter,
                "content is not valid UTF-8 at byte offset {byte_offset}"
            ),
            Self::LimitExceeded {
                limit,
                value,
                limit_value,
            } => write!(
                formatter,
                "{limit} is {value}, over the bound {limit_value}"
            ),
        }
    }
}

impl std::error::Error for IngestError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::Log(source) => Some(source),
            Self::Sched(source) => Some(source),
            Self::Read(source) => Some(source),
            Self::Io { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl IngestError {
    /// The stable code that reaches the DLQ, the source-health card, and the
    /// job failure. A typed reason is the whole point of the L8 DLQ rule.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Store(_) => "storage_failure",
            Self::Log(_) => "log_failure",
            Self::Sched(_) => "scheduler_failure",
            Self::Read(_) => "projection_read_failure",
            Self::BufferBudgetExceeded { .. } => "buffer_budget_exceeded",
            Self::Io { .. } => "io_failure",
            Self::UnknownEvidence { .. } => "unknown_evidence",
            Self::EvidenceExists { .. } => "evidence_exists",
            Self::StalePass { .. } => "stale_pass",
            Self::StageNotReprocessable { .. } => "stage_not_reprocessable",
            Self::StageInputMissing { .. } => "stage_input_missing",
            Self::NotUtf8 { .. } => "not_utf8",
            Self::LimitExceeded { .. } => "limit_exceeded",
        }
    }

    /// Whether another attempt could plausibly succeed. Malformed content and
    /// contract violations cannot be retried into working; storage and log
    /// failures can.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(
            self,
            Self::Store(_) | Self::Log(_) | Self::Sched(_) | Self::Read(_) | Self::Io { .. }
        )
    }
}

impl From<StoreError> for IngestError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

impl From<LogError> for IngestError {
    fn from(source: LogError) -> Self {
        Self::Log(source)
    }
}

impl From<SchedError> for IngestError {
    fn from(source: SchedError) -> Self {
        Self::Sched(source)
    }
}

impl From<EvidenceReadError> for IngestError {
    fn from(source: EvidenceReadError) -> Self {
        Self::Read(source)
    }
}
