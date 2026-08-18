//! The ingestion vocabulary and its event bodies (m1-s01, m1-s02).
//!
//! Master plan §9 fixes the stage order; the M1 §3.2 freeze fixes what the
//! rest of the product may depend on. Everything here is the *durable* half:
//! the words that appear in events, projections, and citations. The engine
//! that runs them lives in `pos-ingest`, which cannot reach a clock, a socket,
//! or the database from inside an apply path — the same dependency inversion
//! `pos-log`/`pos-domain` already use for projections.
//!
//! ## Three decisions that are forever
//!
//! 1. **A chunk id is content-derived** (m1-s02): `BLAKE3(evidence ‖ kind ‖
//!    occurrence ‖ normalized content)`. Citations point at chunk ids, so
//!    re-chunking with a better strategy in 2027 has to preserve the ids of
//!    unchanged content or every stored citation breaks. Deriving rather than
//!    minting is what makes that a property test instead of a migration.
//! 2. **[`ChunkKind::as_str`] values are inside that hash.** Renaming one
//!    silently re-ids every chunk it produced. They are frozen; adding a
//!    variant is additive and safe, renaming one never is.
//! 3. **Evidence granularity is the thread, document, or recording — never
//!    the individual message.** A Slack channel becomes one Evidence item per
//!    thread, not per message. This is load-bearing for the §18 GB gate: at
//!    per-message granularity a 3 GB Slack corpus would append tens of
//!    millions of pipeline events, and the log would cost more than the corpus
//!    it describes.

use pos_foundation::{ChunkId, EvidenceId, JobId, SourceId};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Chunk facts one [`EvidenceChunkedBody`] carries.
///
/// The number is the §7.1 envelope's own ref bound minus the evidence ref the
/// batch also carries. That is deliberate: every chunk keeps the L2 ref
/// saying which Evidence item produced it, and the batch is sized to fit
/// them rather than the refs being dropped to fit a bigger batch. Batching at
/// all is what keeps a corpus-sized item from accumulating its chunks in
/// memory before anything is durable (L8).
pub const CHUNK_BATCH_COUNT_MAX: usize = pos_log::EVENT_REFS_COUNT_MAX - 1;

/// The seven pipeline stages of master plan §9, in flow order.
///
/// `Raw` is a stage but never a job: it is performed by whoever owns the
/// bytes (the upload path, a connector fetch), because re-running it would
/// mean re-fetching from the source — exactly what reprocess must never do.
#[derive(Clone, Copy, Debug, Deserialize, Eq, Hash, Ord, PartialEq, PartialOrd, Serialize)]
pub enum IngestStage {
    Raw,
    Normalize,
    Transcribe,
    Chunk,
    Embed,
    Extract,
    Index,
}

impl IngestStage {
    pub const COUNT: usize = 7;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Raw,
        Self::Normalize,
        Self::Transcribe,
        Self::Chunk,
        Self::Embed,
        Self::Extract,
        Self::Index,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Normalize => "normalize",
            Self::Transcribe => "transcribe",
            Self::Chunk => "chunk",
            Self::Embed => "embed",
            Self::Extract => "extract",
            Self::Index => "index",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stage| stage.as_str() == value)
    }

    /// The registered `pos-sched` job kind that runs this stage. `None` for
    /// [`Self::Raw`], which has no job by construction (see the type docs).
    #[must_use]
    pub const fn job_kind(self) -> Option<&'static str> {
        match self {
            Self::Raw => None,
            Self::Normalize => Some("ingest.normalize"),
            Self::Transcribe => Some("ingest.transcribe"),
            Self::Chunk => Some("ingest.chunk"),
            Self::Embed => Some("ingest.embed"),
            Self::Extract => Some("ingest.extract"),
            Self::Index => Some("ingest.index"),
        }
    }

    /// The `feature` label this stage's model calls carry in the cost ledger,
    /// so per-stage cost is answered by the ledger rather than re-counted in a
    /// second place (the m0-s15 rule: one number, one owner).
    #[must_use]
    pub const fn cost_feature(self) -> &'static str {
        match self {
            Self::Raw => "ingest.raw",
            Self::Normalize => "ingest.normalize",
            Self::Transcribe => "ingest.transcribe",
            Self::Chunk => "ingest.chunk",
            Self::Embed => "ingest.embed",
            Self::Extract => "ingest.extract",
            Self::Index => "ingest.index",
        }
    }

    /// The milestone story that implements this stage, named in the honest
    /// refusal a not-yet-registered stage renders (the M0 `not_yet_supported`
    /// pattern: registered, typed, and explicit about its owner).
    #[must_use]
    pub const fn owner_story(self) -> &'static str {
        match self {
            Self::Raw | Self::Normalize | Self::Chunk => "m1-s01/m1-s02",
            Self::Transcribe => "m1-s03",
            Self::Embed => "m1-s04",
            Self::Extract => "m1-s11",
            Self::Index => "m1-s05",
        }
    }

    /// Whether this stage applies to `media`.
    ///
    /// Transcription is the only conditional stage, and it keys off the
    /// *media* rather than the shape: an interview recording needs decoding,
    /// a caption file or a pasted transcript is already text. Both are
    /// `Transcript`-shaped and both chunk into turn windows, so a
    /// shape-conditional plan would park every already-transcribed item
    /// behind a decoder it does not need.
    #[must_use]
    pub const fn applies_to(self, media: MediaKind) -> bool {
        match self {
            Self::Transcribe => matches!(media, MediaKind::Audio | MediaKind::Video),
            _ => true,
        }
    }

    /// The next stage in this item's plan, or `None` at [`Self::Index`].
    #[must_use]
    pub fn next_for(self, media: MediaKind) -> Option<Self> {
        let position = Self::ALL.iter().position(|stage| *stage == self)?;
        Self::ALL
            .into_iter()
            .skip(position + 1)
            .find(|stage| stage.applies_to(media))
    }

    /// Stage order as a rank, so "at or past stage N" is an integer compare
    /// in SQL and in the reprocess planner rather than a match arm per pair.
    #[must_use]
    pub fn rank(self) -> u8 {
        u8::try_from(
            Self::ALL
                .iter()
                .position(|stage| *stage == self)
                .unwrap_or(0),
        )
        .unwrap_or(0)
    }
}

impl fmt::Display for IngestStage {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_str())
    }
}

/// The normalized shape a chunker keys on — what the evidence *is* once
/// NORMALIZE (or TRANSCRIBE) has produced text and a segment index. Five
/// shapes, five chunk strategies, one windowing algorithm (m1-s02).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceShape {
    /// Speech with timestamps: interviews, meetings, voice notes.
    Transcript,
    /// A conversation of many messages: a Slack thread, a mail thread.
    Thread,
    /// A single authored message: one email, one issue comment.
    Message,
    /// Prose with headings: markdown, extracted document text, PR bodies.
    Document,
    /// Rows and columns: CSV exports, analytics dumps.
    Table,
}

impl EvidenceShape {
    pub const COUNT: usize = 5;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Transcript,
        Self::Thread,
        Self::Message,
        Self::Document,
        Self::Table,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Transcript => "transcript",
            Self::Thread => "thread",
            Self::Message => "message",
            Self::Document => "document",
            Self::Table => "table",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|shape| shape.as_str() == value)
    }

    /// The chunk kind this shape's chunker stamps into every chunk id.
    #[must_use]
    pub const fn chunk_kind(self) -> ChunkKind {
        match self {
            Self::Transcript => ChunkKind::TranscriptTurns,
            Self::Thread => ChunkKind::ThreadMessages,
            Self::Message => ChunkKind::MessageBody,
            Self::Document => ChunkKind::DocumentSection,
            Self::Table => ChunkKind::TableRows,
        }
    }
}

/// What the raw bytes are, decided by content sniffing rather than by file
/// extension (m1-s07's rule, stated here because RAW records it).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum MediaKind {
    PlainText,
    Markdown,
    Csv,
    /// WebVTT / SubRip captions — already-transcribed speech.
    Captions,
    Audio,
    Video,
    /// Structured connector payloads (a Slack thread, an email MIME part)
    /// whose normalization is the connector's, not a file decoder's.
    Structured,
    /// Anything we can store and cite but cannot yet read as text.
    Opaque,
}

impl MediaKind {
    pub const COUNT: usize = 8;
    pub const ALL: [Self; Self::COUNT] = [
        Self::PlainText,
        Self::Markdown,
        Self::Csv,
        Self::Captions,
        Self::Audio,
        Self::Video,
        Self::Structured,
        Self::Opaque,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::PlainText => "plain_text",
            Self::Markdown => "markdown",
            Self::Csv => "csv",
            Self::Captions => "captions",
            Self::Audio => "audio",
            Self::Video => "video",
            Self::Structured => "structured",
            Self::Opaque => "opaque",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// The chunk-kind vocabulary. **Frozen: these strings are hashed into chunk
/// ids** (see the module docs). Adding a variant is additive; renaming one
/// invalidates every citation that ever pointed at a chunk of that kind.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum ChunkKind {
    TranscriptTurns,
    ThreadMessages,
    MessageBody,
    DocumentSection,
    TableRows,
}

impl ChunkKind {
    pub const COUNT: usize = 5;
    pub const ALL: [Self; Self::COUNT] = [
        Self::TranscriptTurns,
        Self::ThreadMessages,
        Self::MessageBody,
        Self::DocumentSection,
        Self::TableRows,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::TranscriptTurns => "transcript_turns",
            Self::ThreadMessages => "thread_messages",
            Self::MessageBody => "message_body",
            Self::DocumentSection => "document_section",
            Self::TableRows => "table_rows",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|kind| kind.as_str() == value)
    }
}

/// Where a chunk sits in something a human can be shown. Every chunk carries
/// one, and the m1-s12 citation sweep gates on 100% of them resolving to a
/// rendered position — which is why the type has no "unknown" variant.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum Locator {
    /// Milliseconds from the start of the media. 10 ms resolution is the
    /// m1-s03 transcription contract; the citation UI renders whole seconds.
    TimeRange { start_ms: u64, end_ms: u64 },
    /// 1-based inclusive line range in the normalized text.
    LineRange { start: u64, end: u64 },
    /// 0-based inclusive message ordinal range inside the evidence thread.
    /// The external permalink for an ordinal is the connector's mapping
    /// (m1-s08/m1-s09); the ordinal alone already renders.
    MessageRange { start: u64, end: u64 },
}

impl Locator {
    #[must_use]
    pub const fn kind_str(self) -> &'static str {
        match self {
            Self::TimeRange { .. } => "time_range",
            Self::LineRange { .. } => "line_range",
            Self::MessageRange { .. } => "message_range",
        }
    }

    /// The two integers a projection stores beside [`Self::kind_str`], so a
    /// locator round-trips through three columns without a nested encoding.
    #[must_use]
    pub const fn bounds(self) -> (u64, u64) {
        match self {
            Self::TimeRange { start_ms, end_ms } => (start_ms, end_ms),
            Self::LineRange { start, end } | Self::MessageRange { start, end } => (start, end),
        }
    }

    /// Rebuilds a locator from its stored columns. `None` for an unknown kind
    /// — a corrupt projection is a typed read error, never a guessed position.
    #[must_use]
    pub fn from_columns(kind: &str, start: u64, end: u64) -> Option<Self> {
        match kind {
            "time_range" => Some(Self::TimeRange {
                start_ms: start,
                end_ms: end,
            }),
            "line_range" => Some(Self::LineRange { start, end }),
            "message_range" => Some(Self::MessageRange { start, end }),
            _ => None,
        }
    }
}

/// How hostile the normalized content looked to the m1-s14 canary detectors.
/// The level exists in the vocabulary from E1 so the evidence row, the
/// projections, and the taint boundary are shaped for it before the detectors
/// land — an armed boundary is cheaper than a retrofitted one (L6).
#[derive(Clone, Copy, Debug, Default, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
pub enum CanaryLevel {
    /// Nothing instruction-shaped was found — or no detector has run yet.
    #[default]
    Clean,
    /// Instruction-shaped content: renders with a warning, enters agent
    /// context only marked, and raises run taint.
    Suspect,
    /// Held back entirely until a human releases it with an audited event.
    Quarantined,
}

impl CanaryLevel {
    pub const COUNT: usize = 3;
    pub const ALL: [Self; Self::COUNT] = [Self::Clean, Self::Suspect, Self::Quarantined];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Clean => "clean",
            Self::Suspect => "suspect",
            Self::Quarantined => "quarantined",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|level| level.as_str() == value)
    }
}

/// The durable state of one Evidence item: the last stage that completed, or
/// the DLQ. `Indexed` is terminal and immutable — a mutation attempt past it
/// is a typed error and a correction is new, linked evidence (m1-s02).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceStatus {
    Raw,
    Normalized,
    Transcribed,
    Chunked,
    Embedded,
    Extracted,
    Indexed,
    /// A stage exhausted its retries or refused permanently; the stage row
    /// carries which one and why (L8: the DLQ is never a silent drop).
    Failed,
}

impl EvidenceStatus {
    pub const COUNT: usize = 8;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Raw,
        Self::Normalized,
        Self::Transcribed,
        Self::Chunked,
        Self::Embedded,
        Self::Extracted,
        Self::Indexed,
        Self::Failed,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Normalized => "normalized",
            Self::Transcribed => "transcribed",
            Self::Chunked => "chunked",
            Self::Embedded => "embedded",
            Self::Extracted => "extracted",
            Self::Indexed => "indexed",
            Self::Failed => "failed",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|status| status.as_str() == value)
    }

    /// The status an evidence item holds once `stage` has completed.
    #[must_use]
    pub const fn after(stage: IngestStage) -> Self {
        match stage {
            IngestStage::Raw => Self::Raw,
            IngestStage::Normalize => Self::Normalized,
            IngestStage::Transcribe => Self::Transcribed,
            IngestStage::Chunk => Self::Chunked,
            IngestStage::Embed => Self::Embedded,
            IngestStage::Extract => Self::Extracted,
            IngestStage::Index => Self::Indexed,
        }
    }

    /// Whether the evidence is sealed. Past INDEX the row is immutable: the
    /// citations that point into it must never describe a moving target.
    #[must_use]
    pub const fn is_immutable(self) -> bool {
        matches!(self, Self::Indexed)
    }
}

/// The identity an item has in the system it came from. m1-s06 grows this
/// into the full F87 `ExternalObjectRef`; the fields here are the subset
/// Evidence itself needs, and they are additive-only from now on.
#[derive(Clone, Debug, Default, Deserialize, Eq, PartialEq, Serialize)]
pub struct ExternalRef {
    /// Stable id in the origin system (a Slack `ts`, a message-id, a content
    /// hash for an upload). Together with the source it identifies the item,
    /// which is what makes re-fetching idempotent.
    pub external_id: String,
    /// Permalink for provenance rendering, when the source has one.
    pub external_url: Option<String>,
    /// Origin-side version/etag, so an edit is detectable without diffing.
    pub external_version: Option<String>,
}

/// One evidence item was accepted into the project — the RAW fact.
///
/// The blob is already in the CAS when this appends: content-addressed writes
/// are idempotent, so "effect then fact" is safe here, and a crash between
/// them leaves an unreferenced blob the CAS sweep collects, never a row
/// pointing at bytes that are not there.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceAddedBody {
    V1 {
        evidence_id: EvidenceId,
        source_id: SourceId,
        /// Connector kind that produced it (`upload`, `slack`, `email`, …).
        source_kind: String,
        external: ExternalRef,
        media_kind: MediaKind,
        /// Shape RAW believes this is; NORMALIZE confirms or corrects it.
        shape: EvidenceShape,
        /// BLAKE3 address of the original bytes, untouched and retained.
        content_blob: [u8; 32],
        byte_size: u64,
        /// When the item happened in its origin system, not when we fetched
        /// it — the ordering a human recognizes.
        occurred_ts_ms: u64,
        /// Author as the source names them; entity resolution is m1-s11.
        author: Option<String>,
        title: Option<String>,
        /// Thread this item belongs to, in the source's own identifiers.
        thread_ref: Option<String>,
    },
}

/// A stage attempt began. Carries the job and attempt it runs under so the
/// pipeline's history joins to the queue's without a second correlation id.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IngestStageStartedBody {
    V1 {
        evidence_id: EvidenceId,
        /// Denormalized out of `EvidenceAdded` on purpose: an apply function
        /// may not read another table, so per-source health can only be a
        /// projection if every stage event carries its source. Sixteen bytes
        /// per event buys an O(1) health read instead of a corpus scan.
        source_id: SourceId,
        stage: IngestStage,
        /// Pipeline pass: 0 for the original ingest, +1 per reprocess. The
        /// M1 §3.2 freeze calls this third key component `attempt`; the
        /// scheduler's per-job retry counter is a *different* number, and
        /// putting that one in the job key would destroy exactly-once.
        pass: u32,
        job_id: JobId,
        attempt_index: u32,
    },
}

/// What a finished stage produced. The output travels *with* the completion
/// rather than in a second event: a stage's completion is the fact that
/// produced its output, and two facts that must always agree can disagree.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IngestStageOutput {
    /// NORMALIZE / TRANSCRIBE: renderable text plus its segment index, both
    /// in the CAS. Identical input normalizes to identical bytes, so the CAS
    /// dedups the normalized form of a duplicated attachment for free.
    Normalized {
        shape: EvidenceShape,
        text_blob: [u8; 32],
        text_byte_size: u64,
        segments_blob: [u8; 32],
        segment_count: u64,
        canary_level: CanaryLevel,
    },
    /// CHUNK: the facts themselves are in [`EvidenceChunkedBody`] batches;
    /// this is the total the projection reconciles against.
    Chunked { chunk_count: u64 },
    /// EMBED / EXTRACT / INDEX in E1, and any stage whose durable output is
    /// entirely in its own projections.
    None,
}

/// A stage attempt finished successfully.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IngestStageFinishedBody {
    V1 {
        evidence_id: EvidenceId,
        /// See [`IngestStageStartedBody::V1::source_id`].
        source_id: SourceId,
        stage: IngestStage,
        pass: u32,
        wall_ms: u64,
        /// Bytes the stage streamed. With the per-stage buffer budget this is
        /// what proves streaming: bytes read may be gigabytes, resident bytes
        /// may not (§18 RSS gate).
        bytes_read: u64,
        /// Stage-specific unit count (segments, chunks, vectors).
        item_count: u64,
        output: IngestStageOutput,
    },
}

/// What happens to the item after a failed attempt. Typed rather than a pair
/// of booleans so "is this in the DLQ?" is one match arm at every read site
/// (L8: a dead item is never a silent drop, and never an inferred state).
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IngestStageDisposition {
    /// The queue will try again; the backoff is the scheduler's.
    Retrying { attempt_count_max: u32 },
    /// The handler refused permanently, or the retry budget is spent. The
    /// stage row and the source health card both show it.
    Dead { permanent: bool },
}

impl IngestStageDisposition {
    #[must_use]
    pub const fn is_dead(self) -> bool {
        matches!(self, Self::Dead { .. })
    }
}

/// A stage attempt failed.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum IngestStageFailedBody {
    V1 {
        evidence_id: EvidenceId,
        /// See [`IngestStageStartedBody::V1::source_id`].
        source_id: SourceId,
        stage: IngestStage,
        pass: u32,
        attempt_index: u32,
        code: String,
        detail: String,
        disposition: IngestStageDisposition,
    },
}

/// Transcript segments one [`EvidenceTranscribedBody`] carries.
///
/// Smaller than the chunk batch because a segment carries its *text* while a
/// chunk carries a byte range: 64 phrases of speech is a comfortable event,
/// 4096 would not be. The batch is also the durability grain — a `kill -9`
/// mid-transcription costs at most the window in flight (m1-s03).
pub const TRANSCRIPT_BATCH_COUNT_MAX: usize = 64;

/// Bytes one transcript segment may carry. The gateway's `Transcriber` seam
/// states the same bound; repeating it here is deliberate, because the log
/// must refuse an oversized fact even if some future adapter forgets to.
pub const TRANSCRIPT_SEGMENT_TEXT_BYTES_MAX: usize = 16 * 1024;

/// Speakers one recording may have. A meeting with more than this many
/// distinct voices is a conference, and a bound that is never hit is still
/// the difference between bounded and unbounded (L8).
pub const TRANSCRIPT_SPEAKER_COUNT_MAX: u32 = 64;

/// Characters a user-assigned speaker name may hold.
pub const TRANSCRIPT_SPEAKER_NAME_CHARS_MAX: usize = 128;

/// The speaker every segment starts life assigned to: unattributed.
///
/// v1 has no diarization, and inventing "Speaker A / Speaker B" from a pause
/// would be a fabricated attribution on evidence a citation points at (L3).
/// Turn *boundaries* are detected; who spoke is the user's to say.
pub const TRANSCRIPT_SPEAKER_UNASSIGNED: u32 = 0;

/// One decoded stretch of speech, as the log records it.
///
/// The ASR text is the durable fact and is never rewritten: a user's
/// correction is a separate event that projects *over* this one, so the
/// original output stays recoverable (m1-s03's editable-transcript AC).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct TranscriptSegmentFact {
    /// 0-based position in the recording, in time order. Stable within a
    /// pass, which is what makes an edit addressable.
    pub segment_index: u32,
    /// Milliseconds from the start of the media, at the 10 ms resolution the
    /// M1 §3.2 transcription contract states.
    pub start_ms: u64,
    pub end_ms: u64,
    /// The pause before this segment was long enough to read as a turn
    /// boundary. A detected boundary, never a claimed identity.
    pub starts_turn: bool,
    pub text: String,
}

/// A bounded batch of transcript segments from one TRANSCRIBE window.
///
/// Committing per window rather than per item is what makes transcription
/// resumable: the segments already in the log are facts the next attempt does
/// not redo, so a `kill -9` costs the window in flight and nothing else.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceTranscribedBody {
    V1 {
        evidence_id: EvidenceId,
        pass: u32,
        /// 0-based window index within this pass.
        batch_index: u32,
        segments: Vec<TranscriptSegmentFact>,
    },
}

/// A user named a speaker in a recording.
///
/// No `pass`: a name is about a person, and re-transcribing with a better
/// model does not change who was in the room. Contrast the two events below,
/// which are about specific decoded segments and therefore are pass-scoped.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TranscriptSpeakerNamedBody {
    V1 {
        evidence_id: EvidenceId,
        speaker_index: u32,
        name: String,
    },
}

/// A user attributed a segment to a speaker.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TranscriptSegmentSpeakerSetBody {
    V1 {
        evidence_id: EvidenceId,
        pass: u32,
        segment_index: u32,
        speaker_index: u32,
    },
}

/// A user corrected a segment's text.
///
/// Pass-scoped on purpose. A correction says "the model heard this wrong
/// here"; re-transcribing with a different model produces different segments,
/// and carrying the correction onto one of them would put a user's words on
/// audio they never checked. Reprocessing therefore drops corrections, which
/// is a stated limitation rather than a silent one.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum TranscriptTextCorrectedBody {
    V1 {
        evidence_id: EvidenceId,
        pass: u32,
        segment_index: u32,
        text: String,
    },
}

/// One chunk, as the log records it. `content_hash` is the untruncated
/// BLAKE3 of the normalized content *without* the evidence id, which is what
/// lets EMBED (m1-s04) embed identical content once no matter how many
/// sources delivered it.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkFact {
    pub chunk_id: ChunkId,
    /// 0-based position within the evidence, in reading order.
    pub ordinal: u32,
    pub kind: ChunkKind,
    /// Byte range into the normalized text blob — never a copy of the text.
    pub byte_start: u64,
    pub byte_end: u64,
    pub locator: Locator,
    pub content_hash: [u8; 32],
    /// Estimated tokens; the exact count belongs to the model's tokenizer and
    /// arrives with the embedder (m1-s04).
    pub token_count_estimate: u32,
}

/// A bounded batch of chunk facts from one CHUNK pass.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceChunkedBody {
    V1 {
        evidence_id: EvidenceId,
        pass: u32,
        /// 0-based batch index; with `chunks.len()` it gives the cumulative
        /// count as an *assignment*, so a replayed batch cannot double-count.
        batch_index: u32,
        chunks: Vec<ChunkFact>,
    },
}

/// Vectors one EMBED batch produced, as a fact.
///
/// ## Why the vectors are not in this event
///
/// A million-chunk corpus at 384 dimensions is 1.5 GB of `f32`. Putting that
/// in the log would make every replay decode it, every snapshot carry it, and
/// every sync ship it — for data that is *derivable*, and that the milestone
/// requires be **garbage-collectable** when a re-embed supersedes it. An
/// event cannot be collected; that is what makes it an event.
///
/// So this follows the shape NORMALIZE already set for text: the bytes go to
/// the **CAS** and the event carries their 32-byte identity. The blob is part
/// of the project directory, so export stays total (L4) and a fresh clone can
/// rebuild the vector index from the log plus the CAS — with no model, no
/// network, and no cost. See [ADR-0009].
///
/// [ADR-0009]: ../../../../docs/adr/0009-vectors-are-a-cas-backed-derived-index.md
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceEmbeddedBody {
    V1 {
        evidence_id: EvidenceId,
        /// See [`IngestStageStartedBody::V1::source_id`].
        source_id: SourceId,
        pass: u32,
        /// 0-based batch index, so a replayed batch assigns rather than adds.
        batch_index: u32,
        /// The model that produced these vectors. On every row, because a
        /// mixed-model index answers plausible nonsense and no shape check
        /// can catch it (the milestone's "mixed-model query = typed error").
        model_id: String,
        dim: u16,
        /// Which input shape produced them: `0` is content-only. Beside
        /// `model_id` because switching enrichment on changes the vectors
        /// exactly as much as switching models does ([05] §5).
        ///
        /// [05]: ../../../../docs/05-intelligence-context-and-data-architecture.md
        enrichment_version: u16,
        /// BLAKE3 of the packed little-endian `f32` batch in the CAS,
        /// `chunks.len() × dim` floats in `chunks` order.
        vectors_blob: [u8; 32],
        /// The chunks this batch embedded, in the batch's own order — which
        /// is also their row order inside `vectors_blob`.
        chunks: Vec<ChunkEmbeddingFact>,
    },
}

/// One chunk's place in an embedding batch.
#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct ChunkEmbeddingFact {
    pub chunk_id: ChunkId,
    /// Row index inside `vectors_blob`. Explicit rather than positional so a
    /// reader never depends on `Vec` order surviving a serde round trip.
    pub row: u32,
    /// The chunk content this vector is *of*. Carrying it makes the
    /// duplicate-embedding rule checkable after the fact: two chunks sharing
    /// a `content_hash` under one `(model_id, enrichment_version)` must share
    /// a vector, and this is what a checker compares (F6).
    pub content_hash: [u8; 32],
    /// Tokens the model actually consumed for this chunk, from the real
    /// tokenizer — which is what `ChunkFact::token_count_estimate` was an
    /// estimate *of* (m1-s02 named this story as its owner).
    pub token_count: u32,
    /// The content did not fit the model's sequence window and was cut.
    /// Never silent: a truncated chunk embedded as if whole is the
    /// silent-truncation lie L8 forbids (m1-s05 surfaces it in `explain`).
    pub truncated: bool,
}

/// Vectors one EMBED batch may carry. Bounded by the same event-ref budget as
/// a chunk batch, because each row contributes a ref.
pub const EMBED_BATCH_COUNT_MAX: usize = pos_log::EVENT_REFS_COUNT_MAX - 1;

/// A human or a migration asked for the pipeline to run again from a stage.
/// The pass increments here, which is what makes the re-enqueued stage jobs
/// new work rather than colliding with the completed ones.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum EvidenceReprocessRequestedBody {
    V1 {
        evidence_id: EvidenceId,
        from_stage: IngestStage,
        /// The pass the re-enqueued stages will run under.
        pass: u32,
        reason: String,
    },
}

#[cfg(test)]
mod tests {
    use super::{ChunkKind, EvidenceShape, EvidenceStatus, IngestStage, Locator, MediaKind};

    #[test]
    fn every_stage_and_shape_round_trips_through_its_string() {
        for stage in IngestStage::ALL {
            assert_eq!(IngestStage::parse(stage.as_str()), Some(stage));
        }
        for shape in EvidenceShape::ALL {
            assert_eq!(EvidenceShape::parse(shape.as_str()), Some(shape));
        }
        for kind in ChunkKind::ALL {
            assert_eq!(ChunkKind::parse(kind.as_str()), Some(kind));
        }
        for status in EvidenceStatus::ALL {
            assert_eq!(EvidenceStatus::parse(status.as_str()), Some(status));
        }
    }

    #[test]
    fn a_text_plan_skips_transcription_and_an_audio_plan_does_not() {
        let mut document = vec![IngestStage::Raw];
        while let Some(next) = document
            .last()
            .and_then(|stage| stage.next_for(MediaKind::Markdown))
        {
            document.push(next);
        }
        assert_eq!(
            document,
            vec![
                IngestStage::Raw,
                IngestStage::Normalize,
                IngestStage::Chunk,
                IngestStage::Embed,
                IngestStage::Extract,
                IngestStage::Index,
            ]
        );

        let mut recording = vec![IngestStage::Raw];
        while let Some(next) = recording
            .last()
            .and_then(|stage| stage.next_for(MediaKind::Audio))
        {
            recording.push(next);
        }
        assert_eq!(recording.len(), IngestStage::COUNT);
        assert!(recording.contains(&IngestStage::Transcribe));

        // An already-transcribed caption file is Transcript-shaped and still
        // skips the decoder.
        let mut captions = vec![IngestStage::Raw];
        while let Some(next) = captions
            .last()
            .and_then(|stage| stage.next_for(MediaKind::Captions))
        {
            captions.push(next);
        }
        assert!(!captions.contains(&IngestStage::Transcribe));
    }

    #[test]
    fn raw_has_no_job_kind_and_every_other_stage_does() {
        assert_eq!(IngestStage::Raw.job_kind(), None);
        for stage in IngestStage::ALL
            .into_iter()
            .filter(|s| *s != IngestStage::Raw)
        {
            assert!(stage.job_kind().is_some(), "{stage} needs a job kind");
        }
    }

    #[test]
    fn stage_rank_orders_the_pipeline() {
        assert!(IngestStage::Raw.rank() < IngestStage::Normalize.rank());
        assert!(IngestStage::Chunk.rank() < IngestStage::Index.rank());
        assert_eq!(IngestStage::Index.rank(), 6);
    }

    #[test]
    fn locators_round_trip_through_their_stored_columns() {
        let cases = [
            Locator::TimeRange {
                start_ms: 734_000,
                end_ms: 742_500,
            },
            Locator::LineRange { start: 1, end: 40 },
            Locator::MessageRange { start: 0, end: 11 },
        ];
        for locator in cases {
            let (start, end) = locator.bounds();
            assert_eq!(
                Locator::from_columns(locator.kind_str(), start, end),
                Some(locator)
            );
        }
        assert_eq!(Locator::from_columns("somewhere", 0, 0), None);
    }

    #[test]
    fn only_indexed_evidence_is_immutable() {
        for status in EvidenceStatus::ALL {
            assert_eq!(
                status.is_immutable(),
                status == EvidenceStatus::Indexed,
                "{status:?} disagrees with the immutability rule"
            );
        }
    }
}
