//! TRANSCRIBE (m1-s03): audio becomes text a citation can point at, to the
//! exact second.
//!
//! ## The shape of one attempt
//!
//! ```text
//!   CAS blob ─▶ symphonia decode ─▶ our resampler ─▶ 16 kHz mono f32
//!                                                        │
//!                       ┌────────────────────────────────┘
//!                       ▼  one window at a time (30 s by default)
//!   pos-gateway::transcribe  ─▶ segments ─▶ EvidenceTranscribed (durable)
//!                       │
//!                       └─▶ carry the tail past the last segment into the
//!                           next window, so no word is cut in half
//!   at the end: the committed segments become the normalized text blob and
//!   the segment index CHUNK already knows how to read
//! ```
//!
//! ## Why the window is the unit of everything
//!
//! One window bounds **memory** (30 s × 16 kHz × 4 B ≈ 1.9 MiB, plus at most
//! one window of carry — well inside the 64 MiB per-stage budget m1-s01
//! asserts), it bounds **durability** (each window's segments commit before
//! the next one starts, so `kill -9` costs the window in flight and nothing
//! that already finished), and it bounds **latency to first output** (a user
//! sees the first minute of a two-hour interview while the rest decodes).
//!
//! ## Resume, precisely
//!
//! A re-run reads the highest committed segment index and its end timestamp
//! for this `(evidence, pass)` and decodes from there. Committed segments are
//! facts; they are never transcribed twice. The decode itself restarts from
//! the beginning of the file and *skips* — decoding is two orders of magnitude
//! faster than transcription, so skipping costs seconds where a container seek
//! costs correctness on formats whose seek is approximate.
//!
//! ## Invariants this stage adds to the crate's P1–P6
//!
//! - **T1 — the ASR text is written once.** Corrections are separate events
//!   into a separate column ([`pos_domain::TranscriptTextCorrectedBody`]), so
//!   the model's original output is always recoverable.
//! - **T2 — segment indices are dense and monotonic within a pass, and the
//!   segments they number never overlap in time.** The resume path depends on
//!   `max(segment_index)` being the count committed so far, and on
//!   `max(end_ms)` being audio that is finished. A model that re-emits speech
//!   it already returned — which a window boundary invites — would break both,
//!   so a segment starting before the last committed end is dropped rather
//!   than appended.
//! - **T3 — the final blobs are a function of the committed segments only.**
//!   Not of how many attempts produced them, which is what makes a resumed
//!   item and an uninterrupted one produce identical output (P3, and the
//!   kill-matrix digest oracle).

use crate::IngestError;
use crate::audio::{AudioError, AudioSource};
use crate::pipeline::{StageContext, StageFailure, StageHandler, StageProduct};
use crate::segment::{Segment, SegmentWriter};
use pos_domain::{
    CanaryLevel, DomainEvent, EvidenceShape, EvidenceTranscribedBody, IngestStage,
    IngestStageOutput, Locator, TRANSCRIPT_BATCH_COUNT_MAX, TRANSCRIPT_SEGMENT_TEXT_BYTES_MAX,
    TranscriptSegmentFact, TranscriptSegmentRecord, list_transcript_segments,
    read_transcript_progress,
};
use pos_gateway::{
    AUDIO_SAMPLE_RATE_HZ, CallAttribution, CloudSttAdapter, CredentialClass, EndpointConfig,
    EndpointLocality, Gateway, GatewayConfig, LoopbackHttpTransport, ModelChoice, ModelPolicy,
    ModelRouting, ProviderFamily, SecretRef, SinkClosed, TlsHttpTransport, TranscribeRequest,
    Transcriber, TranscriptSegment, TranscriptSink, Transports, WINDOW_MS_MAX,
    WhisperLocalTranscriber,
};
use pos_log::Actor;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

/// Audio one model call decodes. Thirty seconds is whisper's own context
/// length, so a longer window buys nothing but a coarser checkpoint; a shorter
/// one pays the model's fixed per-call cost more often and hurts the ≥ 5×
/// realtime gate. It is configurable, and capped by the seam's own
/// [`WINDOW_MS_MAX`].
pub const TRANSCRIBE_WINDOW_MS_DEFAULT: u64 = 30_000;

/// The floor on how far a window advances the read position, as a fraction of
/// the window.
///
/// Normally the next window starts where the last segment ended, so a word
/// straddling the boundary is decoded whole. When a window produces almost
/// nothing — silence, music, a held tone — that rule would advance by a
/// fraction of a window and the stage would crawl through the file. Below this
/// fraction the stage skips ahead instead, which is safe because the model
/// already looked at the audio and found no speech in it.
const ADVANCE_FRACTION_MIN: u64 = 2;

/// How much of a skipped-ahead window is re-read.
///
/// The skip above has one failure mode worth paying for: a word can *begin* in
/// the last moment of a window the model otherwise found quiet, and jumping to
/// the window's end would cut that word's start off forever. Backing the next
/// window up by a second re-reads that boundary; a segment the model repeats
/// is dropped by the `committed_to_ms` guard, so the overlap costs decode time
/// and never a duplicate.
const BOUNDARY_OVERLAP_MS: u64 = 1_000;

/// Segments one recording may hold. At the ~4 s mean whisper produces, this is
/// a 40-hour recording — past anything the upload path accepts, and it keeps
/// the index arithmetic inside `u32` (L8: state the limit).
pub const TRANSCRIPT_SEGMENT_COUNT_MAX: u32 = 36_000;

/// Where a stage's model calls record their cost.
///
/// `pos-ingest` cannot construct `pos-api`'s `EventCostLedger` — that would be
/// an upward dependency — and writing a second one here would give the cost
/// meter two owners, which is exactly the drift the m0-s15 "one number, one
/// owner" rule exists to prevent. So the shell composes the ledger and the
/// pipeline borrows one per attempt through this seam. EMBED (m1-s04) and
/// EXTRACT (m1-s11) reach their gateways the same way.
pub trait StageLedgers: Send + Sync {
    /// Opens a ledger bound to this attempt's log, clock, and actor.
    fn open<'a>(
        &self,
        log: &'a pos_log::ProjectLog,
        clock: &'a dyn pos_foundation::WallClock,
        actor: Actor,
    ) -> Box<dyn pos_gateway::CostLedger + 'a>;
}

/// The ledger a pipeline composed without one gets.
///
/// It refuses rather than discarding: a model call that ran and was not
/// metered is an accounting hole, and the gateway already treats a ledger
/// failure as outranking a model success. Read-only pipelines (the reprocess
/// planner, the evidence browser) never reach it.
pub struct UnmeteredLedgers;

impl StageLedgers for UnmeteredLedgers {
    fn open<'a>(
        &self,
        _log: &'a pos_log::ProjectLog,
        _clock: &'a dyn pos_foundation::WallClock,
        _actor: Actor,
    ) -> Box<dyn pos_gateway::CostLedger + 'a> {
        Box::new(RefusingLedger)
    }
}

struct RefusingLedger;

impl pos_gateway::CostLedger for RefusingLedger {
    fn record(
        &self,
        _record: &pos_gateway::ModelCallRecord,
    ) -> Result<(), pos_gateway::LedgerError> {
        Err(pos_gateway::LedgerError {
            reason: "this pipeline was composed without a cost ledger; a model call it cannot \
                     meter must not run"
                .to_owned(),
        })
    }
}

/// Where transcription is routed. Selected by configuration and enforced by
/// the gateway's policy gate, exactly like every other model call (F43).
#[derive(Clone, Debug)]
pub enum TranscribeRoute {
    /// whisper.cpp in this process, via the `whisper-rs` adapter. The model
    /// artifact is loaded from `models_dir/model_name` on first use.
    LocalWhisper {
        models_dir: PathBuf,
        model_name: String,
    },
    /// An OpenAI-shaped `/v1/audio/transcriptions` endpoint.
    ///
    /// Composed and reachable, but no product surface writes a credential for
    /// it yet: the per-project encrypted secret store is m1-s06's. A
    /// deployment that puts a key in the pipeline's secret store today routes
    /// here; nothing in the UI does. Stated rather than implied.
    Cloud {
        base_url: String,
        model: String,
        supports_segments: bool,
        secret_ref: SecretRef,
    },
}

/// Everything TRANSCRIBE needs that is not in the log.
#[derive(Clone, Debug)]
pub struct TranscribeSetup {
    pub policy: ModelPolicy,
    pub route: TranscribeRoute,
    /// BCP-47 hint, or `None` to let the model detect. Absence is honest.
    pub language: Option<String>,
    pub window_ms: u64,
}

impl TranscribeSetup {
    /// The default every shell composes: local whisper, `local_only`, 30 s
    /// windows. Local-first is not a preference here — it is L9/F7, and a
    /// default that routed audio to a cloud endpoint would be a privacy
    /// decision made by a constant.
    #[must_use]
    pub fn local(models_dir: PathBuf, model_name: impl Into<String>) -> Self {
        Self {
            policy: ModelPolicy::LocalOnly,
            route: TranscribeRoute::LocalWhisper {
                models_dir,
                model_name: model_name.into(),
            },
            language: None,
            window_ms: TRANSCRIBE_WINDOW_MS_DEFAULT,
        }
    }

    fn window_ms_clamped(&self) -> u64 {
        self.window_ms.clamp(1_000, WINDOW_MS_MAX)
    }

    fn model_name(&self) -> &str {
        match &self.route {
            TranscribeRoute::LocalWhisper { model_name, .. } => model_name,
            TranscribeRoute::Cloud { model, .. } => model,
        }
    }

    fn choice(&self) -> ModelChoice {
        match &self.route {
            TranscribeRoute::LocalWhisper { model_name, .. } => ModelChoice {
                // The family names the wire shape, and an in-process model has
                // none; the ledger's engine label carries the honest answer
                // ("whisper-local") because the record's `provider` column is
                // the transcriber's label, not this field.
                family: ProviderFamily::OpenAiCompatible,
                endpoint: EndpointConfig::in_process("whisper-local"),
                model: model_name.clone(),
                credential: CredentialClass::DeviceSession {
                    adapter: "whisper".to_owned(),
                    device: pos_foundation::DeviceId::from_bytes([0; 16]),
                },
                is_pinned_family_base: false,
            },
            TranscribeRoute::Cloud {
                base_url,
                model,
                secret_ref,
                ..
            } => ModelChoice {
                family: ProviderFamily::OpenAi,
                endpoint: EndpointConfig::new(base_url.clone(), EndpointLocality::Remote)
                    .unwrap_or_else(|_| EndpointConfig::in_process("cloud-stt-misconfigured")),
                model: model.clone(),
                credential: CredentialClass::Byok {
                    secret_ref: secret_ref.clone(),
                },
                is_pinned_family_base: false,
            },
        }
    }
}

/// The TRANSCRIBE stage handler.
///
/// It caches the loaded whisper context: the weights are hundreds of megabytes
/// and loading them per window would dominate the ≥ 5× realtime gate. The
/// cache is keyed by model name so a configuration change loads the new model
/// and drops the old one rather than holding both.
pub struct TranscribeStage {
    setup: TranscribeSetup,
    loaded: Mutex<Option<(String, Arc<WhisperLocalTranscriber>)>>,
    /// An engine composed by the caller instead of resolved from the route.
    ///
    /// Not a test hook: it is how a peer implementation arrives. The vendored
    /// FFI leaf [ADR-0006] §2 defers is composed exactly here, beside the
    /// wrapper rather than instead of it, and so is any adapter a plugin ever
    /// contributes. That tests use it is a consequence of the seam being real.
    ///
    /// [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md
    composed: Option<Arc<dyn Transcriber + Send + Sync>>,
}

impl TranscribeStage {
    #[must_use]
    pub fn new(setup: TranscribeSetup) -> Self {
        Self {
            setup,
            loaded: Mutex::new(None),
            composed: None,
        }
    }

    /// Runs `engine` instead of resolving one from the route.
    #[must_use]
    pub fn with_engine(setup: TranscribeSetup, engine: Arc<dyn Transcriber + Send + Sync>) -> Self {
        Self {
            setup,
            loaded: Mutex::new(None),
            composed: Some(engine),
        }
    }

    #[must_use]
    pub const fn setup(&self) -> &TranscribeSetup {
        &self.setup
    }

    /// The loaded local engine, loading it on first use.
    fn local_engine(&self) -> Result<Arc<WhisperLocalTranscriber>, StageFailure> {
        let TranscribeRoute::LocalWhisper {
            models_dir,
            model_name,
        } = &self.setup.route
        else {
            return Err(StageFailure::permanent(
                "transcribe_route_mismatch",
                "the configured route is not local whisper",
            ));
        };
        let mut loaded = self.loaded.lock().unwrap_or_else(PoisonError::into_inner); // INVARIANT: a poisoned cache is replaced below, never read.
        if let Some((name, engine)) = loaded.as_ref()
            && name == model_name
        {
            return Ok(Arc::clone(engine));
        }
        let path = models_dir.join(model_name);
        if !path.is_file() {
            // Retriable on purpose: `pos models pull whisper-small` fixes it
            // without touching the item, and the DLQ reason says exactly that.
            return Err(StageFailure::retriable(
                "transcribe_model_missing",
                format!(
                    "the whisper artifact {model_name:?} is not at {}; pull it with \
                     `pos models pull {model_name}`",
                    path.display()
                ),
            ));
        }
        let engine = Arc::new(WhisperLocalTranscriber::load(model_name, &path).map_err(
            |weather| StageFailure::permanent("transcribe_model_unusable", weather.to_string()),
        )?);
        *loaded = Some((model_name.clone(), Arc::clone(&engine)));
        Ok(engine)
    }
}

impl StageHandler for TranscribeStage {
    fn stage(&self) -> IngestStage {
        IngestStage::Transcribe
    }

    fn run(&self, context: &StageContext<'_>) -> Result<StageProduct, StageFailure> {
        // The engine outlives the gateway that borrows it.
        let local;
        let cloud;
        let engine: &dyn Transcriber = match &self.setup.route {
            _ if self.composed.is_some() => {
                let Some(engine) = self.composed.as_ref() else {
                    return Err(StageFailure::permanent(
                        "transcribe_engine_missing",
                        "the composed engine vanished between the guard and the read",
                    ));
                };
                engine.as_ref()
            }
            TranscribeRoute::LocalWhisper { .. } => {
                local = self.local_engine()?;
                local.as_ref()
            }
            TranscribeRoute::Cloud {
                base_url,
                supports_segments,
                ..
            } => {
                cloud = CloudSttAdapter {
                    base_url: base_url.clone(),
                    supports_segments: *supports_segments,
                };
                &cloud
            }
        };
        let ledger = context.open_ledger();
        let loopback = LoopbackHttpTransport;
        let tls = TlsHttpTransport::new();
        let choice = self.setup.choice();
        let gateway = Gateway::new(
            GatewayConfig {
                policy: self.setup.policy.clone(),
                // Transcription needs no thinking tiers; routing them at the
                // same choice keeps the type total without inventing a
                // completion route this stage will never call.
                routing: ModelRouting::thinking_only(choice.clone(), choice)
                    .with_transcribe(self.setup.choice()),
            },
            Vec::new(),
            context.secrets(),
            ledger.as_ref(),
            Transports::new(&loopback, &tls),
            context.clock(),
        )
        .with_transcriber(engine);
        transcribe_item(context, &gateway, &self.setup)
    }
}

/// The decode/transcribe/commit loop. Split from the handler so the handler is
/// composition and this is control flow (STYLE: push `if`s up).
fn transcribe_item(
    context: &StageContext<'_>,
    gateway: &Gateway<'_>,
    setup: &TranscribeSetup,
) -> Result<StageProduct, StageFailure> {
    let evidence = context.evidence();
    let progress = read_transcript_progress(context.log(), evidence.evidence_id, context.pass())
        .map_err(IngestError::from)?;
    let (mut next_segment_index, resume_from_ms) = progress
        .map_or((0_u32, 0_u64), |(index, end_ms)| {
            (index.saturating_add(1), end_ms)
        });
    // Nothing may be committed that starts before this. It begins at the
    // resume point and advances with every committed segment (T2).
    let mut committed_to_ms = resume_from_ms;

    let source = context.open_content_file().map_err(StageFailure::from)?;
    let mut audio =
        AudioSource::open(Box::new(source)).map_err(|error| stage_failure_for_audio(&error))?;
    let window_samples = samples_for_ms(setup.window_ms_clamped());

    let mut window: Vec<f32> = Vec::with_capacity(window_samples);
    let mut window_start_ms = 0_u64;
    let mut batch_index = 0_u32;
    let mut audio_ms_transcribed = 0_u64;
    let mut ended = false;

    while !ended {
        // Fill the window. `decode_into` appends one packet at a time, so the
        // buffer never overshoots by more than a packet (P4).
        while window.len() < window_samples {
            let appended = audio
                .decode_into(&mut window)
                .map_err(|error| stage_failure_for_audio(&error))?;
            if appended == 0 && audio.is_ended() {
                ended = true;
                break;
            }
        }
        // Everything before the resume point is audio whose segments are
        // already durable facts. Drop it without a model call.
        if window_start_ms + ms_for_samples(window.len()) <= resume_from_ms {
            window_start_ms = window_start_ms.saturating_add(ms_for_samples(window.len()));
            window.clear();
            continue;
        }
        if window_start_ms < resume_from_ms {
            let skip = samples_for_ms(resume_from_ms - window_start_ms).min(window.len());
            window.drain(..skip);
            window_start_ms = resume_from_ms;
        }
        if window.is_empty() {
            break;
        }
        let (segments, advance_ms) =
            transcribe_window(gateway, context, setup, window_start_ms, &window)?;
        audio_ms_transcribed = audio_ms_transcribed.saturating_add(advance_ms);
        let committed = commit_segments(
            context,
            batch_index,
            next_segment_index,
            segments,
            committed_to_ms,
        )?;
        next_segment_index = committed.next_segment_index;
        committed_to_ms = committed.committed_to_ms.max(committed_to_ms);
        batch_index = batch_index.saturating_add(1);
        // Carry the tail past the last segment into the next window, so a word
        // straddling the boundary is decoded whole rather than twice.
        let consumed = samples_for_ms(advance_ms).min(window.len());
        window.drain(..consumed);
        window_start_ms = window_start_ms.saturating_add(advance_ms);
    }

    write_transcript(context, next_segment_index, audio_ms_transcribed)
}

/// One window through the gateway. Returns the segments and how far the read
/// position advances.
fn transcribe_window(
    gateway: &Gateway<'_>,
    context: &StageContext<'_>,
    setup: &TranscribeSetup,
    window_start_ms: u64,
    samples: &[f32],
) -> Result<(Vec<TranscriptSegment>, u64), StageFailure> {
    // Derived from the samples actually in hand, not from the configured
    // window: the last window of a file is short, and advancing by the
    // configured length there would step past audio that was never decoded.
    let window_ms = ms_for_samples(samples.len());
    let advance_floor_ms = window_ms / ADVANCE_FRACTION_MIN;
    let mut sink = CollectingSink::default();
    let attribution = CallAttribution {
        project: context.project_id(),
        feature: IngestStage::Transcribe.cost_feature().to_owned(),
        agent: None,
    };
    gateway
        .transcribe(
            &attribution,
            &TranscribeRequest {
                model: setup.model_name(),
                language: setup.language.as_deref(),
                offset_ms: window_start_ms,
                samples,
            },
            &mut sink,
        )
        .map_err(|weather| StageFailure {
            code: format!("transcribe_{}", weather.code()),
            detail: weather.to_string(),
            permanent: !weather.retriable(),
        })?;
    let last_end_ms = sink
        .segments
        .last()
        .map_or(window_start_ms, |segment| segment.end_ms);
    let spoken_ms = last_end_ms.saturating_sub(window_start_ms);
    // See ADVANCE_FRACTION_MIN and BOUNDARY_OVERLAP_MS: a window the model
    // found almost nothing in is skipped past, minus a second of overlap so a
    // word beginning at its very end is not cut off.
    let advance_ms = if spoken_ms >= advance_floor_ms {
        spoken_ms
    } else {
        window_ms
            .saturating_sub(BOUNDARY_OVERLAP_MS)
            .max(advance_floor_ms)
    };
    debug_assert!(
        advance_ms <= window_ms,
        "advancing past the decoded window would skip audio nothing examined"
    );
    Ok((sink.segments, advance_ms.max(1)))
}

/// What one window committed.
struct Committed {
    next_segment_index: u32,
    committed_to_ms: u64,
}

/// Commits one window's segments as a durable batch, in bounded sub-batches.
///
/// `committed_to_ms` is the boundary guard: a segment starting before it is
/// audio that already has a durable segment, which happens when a model
/// re-reads the carried tail at a window boundary. Dropping it keeps T2 true;
/// appending it would put the same sentence in the transcript twice and give a
/// citation two places to land.
fn commit_segments(
    context: &StageContext<'_>,
    batch_index: u32,
    first_segment_index: u32,
    segments: Vec<TranscriptSegment>,
    committed_to_ms: u64,
) -> Result<Committed, StageFailure> {
    let mut next_index = first_segment_index;
    let mut latest_end_ms = committed_to_ms;
    let fresh: Vec<TranscriptSegment> = segments
        .into_iter()
        .filter(|segment| segment.start_ms >= committed_to_ms)
        .collect();
    for (sub_batch, chunk) in fresh.chunks(TRANSCRIPT_BATCH_COUNT_MAX).enumerate() {
        let mut facts = Vec::with_capacity(chunk.len());
        for segment in chunk {
            if next_index >= TRANSCRIPT_SEGMENT_COUNT_MAX {
                return Err(IngestError::LimitExceeded {
                    limit: "transcript segments",
                    value: u64::from(next_index),
                    limit_value: u64::from(TRANSCRIPT_SEGMENT_COUNT_MAX),
                }
                .into());
            }
            if segment.text.len() > TRANSCRIPT_SEGMENT_TEXT_BYTES_MAX {
                return Err(IngestError::LimitExceeded {
                    limit: "transcript segment text",
                    value: segment.text.len() as u64,
                    limit_value: TRANSCRIPT_SEGMENT_TEXT_BYTES_MAX as u64,
                }
                .into());
            }
            let end_ms = segment.end_ms.max(segment.start_ms);
            facts.push(TranscriptSegmentFact {
                segment_index: next_index,
                start_ms: segment.start_ms,
                end_ms,
                starts_turn: segment.starts_turn,
                text: segment.text.clone(),
            });
            latest_end_ms = latest_end_ms.max(end_ms);
            next_index = next_index.saturating_add(1);
        }
        if facts.is_empty() {
            continue;
        }
        // The batch index has to stay unique across sub-batches, or two events
        // would claim the same window position.
        let index = batch_index
            .saturating_mul(u32::try_from(TRANSCRIPT_BATCH_COUNT_MAX).unwrap_or(1))
            .saturating_add(u32::try_from(sub_batch).unwrap_or(0));
        context.emit_transcript(index, facts)?;
    }
    Ok(Committed {
        next_segment_index: next_index,
        committed_to_ms: latest_end_ms,
    })
}

/// Streams the committed segments into the normalized text blob and the
/// segment index CHUNK reads (T3).
///
/// Built from the *projection*, not from what this attempt happened to
/// transcribe: an item that resumed after a crash and one that never crashed
/// therefore produce byte-identical blobs.
fn write_transcript(
    context: &StageContext<'_>,
    segment_count: u32,
    audio_ms: u64,
) -> Result<StageProduct, StageFailure> {
    let mut text = context.blob_writer().map_err(StageFailure::from)?;
    let mut segments = SegmentWriter::new(context.blob_writer().map_err(StageFailure::from)?);
    let mut byte_offset = 0_u64;
    let mut after: Option<u32> = None;
    let mut written = 0_u32;
    loop {
        let page = list_transcript_segments(
            context.log(),
            context.evidence().evidence_id,
            context.pass(),
            after,
            TRANSCRIPT_PAGE_ROWS,
        )
        .map_err(IngestError::from)?;
        if page.is_empty() {
            break;
        }
        for record in &page {
            byte_offset = write_one(&mut text, &mut segments, record, byte_offset)?;
            written = written.saturating_add(1);
        }
        after = page.last().map(|record| record.segment_index);
    }
    debug_assert_eq!(
        written, segment_count,
        "the transcript blob must contain exactly the committed segments (T2/T3)"
    );
    let text_blob = text.finish().map_err(IngestError::from)?.into_bytes();
    let (segments_blob, indexed_count) = segments.finish().map_err(IngestError::from)?;
    Ok(StageProduct {
        output: IngestStageOutput::Normalized {
            // TRANSCRIBE is what makes an audio item a transcript; NORMALIZE
            // wrote the empty placeholder this replaces.
            shape: EvidenceShape::Transcript,
            text_blob,
            text_byte_size: byte_offset,
            segments_blob,
            segment_count: indexed_count,
            // The m1-s14 canary detectors run at NORMALIZE. A transcript of
            // spoken words is still ingested content and will be scanned there
            // when they land; `Clean` by default is what the column means.
            canary_level: CanaryLevel::default(),
        },
        bytes_read: audio_ms,
        item_count: u64::from(segment_count),
    })
}

/// Rows one page of the transcript read pulls. Bounded because the blob write
/// above must not become the one place a whole transcript is resident (L8).
const TRANSCRIPT_PAGE_ROWS: u32 = 200;

fn write_one(
    text: &mut pos_store::BlobWriter<'_>,
    segments: &mut SegmentWriter<'_>,
    record: &TranscriptSegmentRecord,
    byte_offset: u64,
) -> Result<u64, StageFailure> {
    let rendered = record.rendered_text();
    text.append(rendered.as_bytes())
        .and_then(|()| text.append(b"\n"))
        .map_err(IngestError::from)?;
    let byte_start = byte_offset;
    let byte_end = byte_start.saturating_add(rendered.len() as u64);
    segments
        .push(Segment {
            byte_start,
            byte_end,
            locator: Locator::TimeRange {
                start_ms: record.start_ms,
                end_ms: record.end_ms,
            },
            // Transcripts have no heading depth; the turn boundary is carried
            // by the segment's own `starts_turn` in the projection, and the
            // chunker packs turns by token budget rather than by depth.
            depth: 0,
        })
        .map_err(StageFailure::from)?;
    // The newline separates segments in the rendered text and is not part of
    // any segment's span — a citation must not include it.
    Ok(byte_end.saturating_add(1))
}

const fn samples_for_ms(ms: u64) -> usize {
    ((ms * AUDIO_SAMPLE_RATE_HZ as u64) / 1_000) as usize
}

const fn ms_for_samples(samples: usize) -> u64 {
    (samples as u64) * 1_000 / AUDIO_SAMPLE_RATE_HZ as u64
}

fn stage_failure_for_audio(error: &AudioError) -> StageFailure {
    StageFailure {
        code: error.code().to_owned(),
        detail: error.to_string(),
        permanent: !error.is_retriable(),
    }
}

/// Collects one window's segments. Bounded by the window: 30 s of speech is
/// tens of segments, and the seam already caps each one's text.
#[derive(Default)]
struct CollectingSink {
    segments: Vec<TranscriptSegment>,
}

impl TranscriptSink for CollectingSink {
    fn on_segment(&mut self, segment: &TranscriptSegment) -> Result<(), SinkClosed> {
        self.segments.push(segment.clone());
        Ok(())
    }
}

/// Builds the `EvidenceTranscribed` event a stage commits.
pub(crate) fn transcribed_event(
    evidence_id: pos_foundation::EvidenceId,
    pass: u32,
    batch_index: u32,
    segments: Vec<TranscriptSegmentFact>,
) -> DomainEvent {
    DomainEvent::EvidenceTranscribed(EvidenceTranscribedBody::V1 {
        evidence_id,
        pass,
        batch_index,
        segments,
    })
}

#[cfg(test)]
mod tests {
    use super::{
        ADVANCE_FRACTION_MIN, TRANSCRIBE_WINDOW_MS_DEFAULT, TRANSCRIPT_SEGMENT_COUNT_MAX,
        TranscribeSetup, ms_for_samples, samples_for_ms,
    };
    use pos_gateway::{ModelPolicy, WINDOW_MS_MAX};
    use std::path::PathBuf;

    #[test]
    fn the_default_window_is_inside_the_seams_own_cap() {
        const { assert!(TRANSCRIBE_WINDOW_MS_DEFAULT <= WINDOW_MS_MAX) };
        // 30 s × 16 kHz × 4 B ≈ 1.9 MiB, the number the module doc promises.
        assert_eq!(samples_for_ms(TRANSCRIBE_WINDOW_MS_DEFAULT) * 4, 1_920_000);
    }

    #[test]
    fn a_configured_window_is_clamped_rather_than_trusted() {
        let mut setup = TranscribeSetup::local(PathBuf::from("/models"), "whisper-small");
        setup.window_ms = u64::MAX;
        assert_eq!(setup.window_ms_clamped(), WINDOW_MS_MAX);
        setup.window_ms = 1;
        assert_eq!(setup.window_ms_clamped(), 1_000);
    }

    #[test]
    fn the_default_route_is_local_only_because_privacy_is_not_a_constants_decision() {
        let setup = TranscribeSetup::local(PathBuf::from("/models"), "whisper-small");
        assert_eq!(setup.policy, ModelPolicy::LocalOnly);
        let choice = setup.choice();
        assert_eq!(
            choice.endpoint.locality(),
            pos_gateway::EndpointLocality::InProcess
        );
        assert!(
            ModelPolicy::LocalOnly.authorize(&choice).is_ok(),
            "the default route must be one a local_only project can take"
        );
    }

    #[test]
    fn sample_and_millisecond_conversions_round_trip_at_the_window_grain() {
        for ms in [1_000_u64, 5_000, 30_000, 120_000] {
            assert_eq!(ms_for_samples(samples_for_ms(ms)), ms);
        }
    }

    #[test]
    fn a_window_always_advances_and_never_past_itself() {
        // The two bounds that together make the loop terminate without
        // skipping audio: at least half a window of progress, at most the
        // window that was actually decoded.
        for window_ms in [1_u64, 900, 1_180, 5_000, 30_000] {
            let floor = window_ms / ADVANCE_FRACTION_MIN;
            let skipped = window_ms
                .saturating_sub(super::BOUNDARY_OVERLAP_MS)
                .max(floor);
            assert!(skipped <= window_ms, "{window_ms} ms window over-advanced");
            assert!(
                skipped >= floor,
                "{window_ms} ms window would crawl instead of progressing"
            );
        }
        const { assert!(TRANSCRIPT_SEGMENT_COUNT_MAX > 0) };
    }
}
