//! The m1-s03 TRANSCRIBE oracles.
//!
//! Three properties, each the mechanism an acceptance criterion names:
//!
//! 1. **An audio item becomes a transcript a citation can point at.** Real
//!    WAV bytes through the real decoder, the real resampler, the real stage,
//!    and on into CHUNK — every chunk carrying a `TimeRange` locator.
//! 2. **`kill -9` mid-transcription re-transcribes nothing that finished.**
//!    The engine records which window offsets it was asked to decode; after
//!    an interrupted run and a resume, no offset appears twice.
//! 3. **A local-only project hands the model no way to leave the device.**
//!    The engine asserts it received `None` for a transport on every call —
//!    the in-process selection is not a policy that could be misread, it is
//!    the absence of a socket.
//!
//! The engine is scripted rather than real whisper on purpose: these are
//! properties of the *pipeline*, and a suite whose outcome depended on what a
//! model heard would be measuring the model. The real engine is exercised by
//! the `whisper_local` qualification lane and the §18 realtime bench.

#![forbid(unsafe_code)]

mod common;

use common::{DEVICE, drain, open_project, queue, submission, submit};
use pos_domain::{
    ChunkKind, EvidenceShape, EvidenceStatus, IngestStage, Locator, MediaKind,
    TRANSCRIPT_SPEAKER_UNASSIGNED, list_chunks, list_transcript_segments, read_evidence,
    read_transcript_progress,
};
use pos_foundation::ManualWallClock;
use pos_gateway::{
    CallAuth, HttpTransport, TranscribeRequest, TranscribeUsage, Transcriber, TranscriptSegment,
    TranscriptSink,
};
use pos_ingest::{
    ChunkStage, IngestPipeline, NormalizeStage, PipelineConfig, StageRegistry, TranscribeSetup,
    TranscribeStage,
};
use pos_log::ProjectLog;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use tempfile::TempDir;

/// Words the scripted engine "hears", one per second of audio. Deterministic,
/// so the assembled transcript is an exact string rather than a shape.
const SPOKEN: [&str; 12] = [
    "the",
    "pricing",
    "page",
    "confused",
    "me",
    "completely",
    "so",
    "I",
    "asked",
    "support",
    "twice",
    "already",
];

/// A scripted transcriber: one segment per second of the window, plus a
/// deliberate pause before the seventh word so the turn heuristic has
/// something real to find.
struct ScriptedEngine {
    /// Every `(offset_ms, audio_ms)` this engine was asked to decode, in
    /// order. The resume property reads this.
    calls: Mutex<Vec<(u64, u64)>>,
    /// Set if any call ever arrived with a transport in hand.
    saw_transport: Mutex<bool>,
    /// Windows to serve before refusing, standing in for the process dying.
    fail_after_windows: Option<usize>,
}

impl ScriptedEngine {
    fn new() -> Self {
        Self {
            calls: Mutex::new(Vec::new()),
            saw_transport: Mutex::new(false),
            fail_after_windows: None,
        }
    }

    fn failing_after(windows: usize) -> Self {
        Self {
            fail_after_windows: Some(windows),
            ..Self::new()
        }
    }

    fn offsets(&self) -> Vec<u64> {
        self.calls
            .lock()
            .expect("test mutex")
            .iter()
            .map(|(offset, _)| *offset)
            .collect()
    }
}

impl Transcriber for ScriptedEngine {
    fn label(&self) -> &'static str {
        "scripted-test-engine"
    }

    fn transcribe(
        &self,
        _auth: &CallAuth,
        request: &TranscribeRequest<'_>,
        transport: Option<&dyn HttpTransport>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, pos_gateway::Weather> {
        if transport.is_some() {
            *self.saw_transport.lock().expect("test mutex") = true;
        }
        let mut calls = self.calls.lock().expect("test mutex");
        if self
            .fail_after_windows
            .is_some_and(|limit| calls.len() >= limit)
        {
            return Err(pos_gateway::Weather::Transport {
                reason: "the scripted engine is standing in for a killed process".to_owned(),
            });
        }
        calls.push((request.offset_ms, request.audio_ms()));
        drop(calls);

        // Words sit on absolute second boundaries, the way a real model's
        // segments sit on speech rather than on wherever a window happened to
        // start. Only whole seconds fully inside the window are emitted.
        let mut emitted = 0_u64;
        let window_end_ms = request.offset_ms + request.audio_ms();
        let first_second = request.offset_ms.div_ceil(1_000);
        for absolute_second in first_second.. {
            let start_ms = absolute_second * 1_000;
            if start_ms + 1_000 > window_end_ms {
                break;
            }
            let Some(word) = SPOKEN.get(usize::try_from(absolute_second).unwrap_or(usize::MAX))
            else {
                break;
            };
            let segment = TranscriptSegment {
                start_ms,
                end_ms: start_ms + 900,
                text: (*word).to_owned(),
                // A gap opens before "so" (index 6): 900 ms of speech then
                // 100 ms of silence is not a turn, but the extra second here
                // is. `mark_turns` in the gateway seam does the real work in
                // production; the flag is what the projection stores.
                starts_turn: absolute_second == 0 || absolute_second == 6,
            };
            if sink.on_segment(&segment).is_err() {
                break;
            }
            emitted += 1;
        }
        Ok(TranscribeUsage {
            audio_ms: request.audio_ms(),
            segment_count: emitted,
            measured: true,
        })
    }
}

/// A ledger that keeps every record, so the suite can assert transcription is
/// metered as a model call like any other.
#[derive(Default)]
struct RecordingLedgers {
    records: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl pos_ingest::StageLedgers for RecordingLedgers {
    fn open<'a>(
        &self,
        _log: &'a ProjectLog,
        _clock: &'a dyn pos_foundation::WallClock,
        _actor: pos_log::Actor,
    ) -> Box<dyn pos_gateway::CostLedger + 'a> {
        Box::new(RecordingLedger {
            records: Arc::clone(&self.records),
        })
    }
}

struct RecordingLedger {
    records: Arc<Mutex<Vec<(String, String, String)>>>,
}

impl pos_gateway::CostLedger for RecordingLedger {
    fn record(
        &self,
        record: &pos_gateway::ModelCallRecord,
    ) -> Result<(), pos_gateway::LedgerError> {
        self.records.lock().expect("test mutex").push((
            record.feature.clone(),
            record.provider.to_owned(),
            record.outcome.clone(),
        ));
        Ok(())
    }
}

/// 16-bit PCM mono WAV at `rate_hz`, `seconds` long, holding a quiet tone.
///
/// Real container bytes, not a stub: the point is that symphonia's WAV reader,
/// our resampler, and the window arithmetic all run. The *content* is
/// irrelevant because the engine is scripted — what matters is that the right
/// number of samples arrives at the right offsets.
#[expect(
    clippy::cast_possible_truncation,
    clippy::cast_sign_loss,
    reason = "fixture lengths are small positive literals"
)]
fn wav(rate_hz: u32, seconds: f64) -> Vec<u8> {
    let frame_count = (f64::from(rate_hz) * seconds) as u32;
    let data_bytes = frame_count * 2;
    let mut bytes = Vec::with_capacity(44 + data_bytes as usize);
    bytes.extend_from_slice(b"RIFF");
    bytes.extend_from_slice(&(36 + data_bytes).to_le_bytes());
    bytes.extend_from_slice(b"WAVEfmt ");
    bytes.extend_from_slice(&16_u32.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&1_u16.to_le_bytes());
    bytes.extend_from_slice(&rate_hz.to_le_bytes());
    bytes.extend_from_slice(&(rate_hz * 2).to_le_bytes());
    bytes.extend_from_slice(&2_u16.to_le_bytes());
    bytes.extend_from_slice(&16_u16.to_le_bytes());
    bytes.extend_from_slice(b"data");
    bytes.extend_from_slice(&data_bytes.to_le_bytes());
    for frame in 0..frame_count {
        let t = f64::from(frame) / f64::from(rate_hz);
        let value = (std::f64::consts::TAU * 220.0 * t).sin() * 0.3;
        bytes.extend_from_slice(&((value * f64::from(i16::MAX)) as i16).to_le_bytes());
    }
    bytes
}

/// The pipeline this suite runs: the three stages this build implements, with
/// transcription routed at a scripted engine and a real cost ledger seam.
fn pipeline_with(
    engine: Arc<ScriptedEngine>,
    ledgers: Arc<RecordingLedgers>,
    window_ms: u64,
    queue: Arc<pos_sched::JobQueue>,
) -> IngestPipeline {
    let mut setup = TranscribeSetup::local(PathBuf::from("unused-for-a-composed-engine"), "test");
    setup.window_ms = window_ms;
    IngestPipeline::new(
        PipelineConfig::for_device(DEVICE).with_ledgers(ledgers),
        queue,
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(TranscribeStage::with_engine(setup, engine)))
            .with(Arc::new(ChunkStage::new())),
    )
}

fn audio_item() -> pos_ingest::EvidenceSubmission {
    // The shape RAW guesses is deliberately wrong: NORMALIZE records
    // `Transcript` for audio and TRANSCRIBE confirms it, which is the plan
    // this suite is also checking.
    submission(
        "interview-01.wav",
        EvidenceShape::Document,
        MediaKind::Audio,
    )
}

#[test]
fn an_audio_item_becomes_a_transcript_chunks_can_cite_to_the_second() {
    let root = TempDir::new().expect("temp project");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let engine = Arc::new(ScriptedEngine::new());
    let ledgers = Arc::new(RecordingLedgers::default());
    let pipeline = pipeline_with(
        Arc::clone(&engine),
        Arc::clone(&ledgers),
        5_000,
        Arc::clone(&queue),
    );

    let item = audio_item();
    // 44.1 kHz stereo-rate-adjacent source: the resampler is on the hot path,
    // not bypassed by a fixture that happens to already be 16 kHz.
    let evidence_id = submit(&pipeline, &log, &clock, &item, &wav(44_100, 12.0)).evidence_id();
    let ran = drain(&pipeline, &queue, &log, &clock, 16);

    assert_eq!(
        ran,
        vec![
            (IngestStage::Normalize, true),
            (IngestStage::Transcribe, true),
            (IngestStage::Chunk, true),
        ],
        "audio must flow NORMALIZE → TRANSCRIBE → CHUNK, all succeeding"
    );

    let record = read_evidence(&log, evidence_id)
        .expect("read evidence")
        .expect("the item exists");
    assert_eq!(record.status, EvidenceStatus::Chunked);
    assert_eq!(
        record.shape,
        EvidenceShape::Transcript,
        "TRANSCRIBE decides the shape; RAW only guessed"
    );

    let segments = list_transcript_segments(&log, evidence_id, 0, None, 500).expect("segments");
    assert_eq!(
        segments.len(),
        SPOKEN.len(),
        "one segment per second of scripted speech"
    );
    assert_eq!(segments[0].asr_text, "the");
    assert_eq!(segments[0].start_ms, 0);
    assert_eq!(segments[0].end_ms, 900);
    assert!(segments[0].edited_text.is_none(), "nothing is edited yet");
    assert_eq!(segments[0].speaker_index, TRANSCRIPT_SPEAKER_UNASSIGNED);
    assert!(
        segments[6].starts_turn,
        "the pause before the seventh word is a turn boundary"
    );
    assert!(
        !segments[1].starts_turn,
        "consecutive speech is not a turn boundary"
    );
    // Indices are dense and monotonic (invariant T2) — the resume path
    // depends on it.
    for (position, segment) in segments.iter().enumerate() {
        assert_eq!(
            u32::try_from(position).expect("small"),
            segment.segment_index
        );
    }

    // Every chunk resolves to a *time*, which is what "cite to the exact
    // second" means downstream (m1-s12's resolution sweep).
    let chunks = list_chunks(&log, evidence_id, None, 500).expect("chunks");
    assert!(!chunks.is_empty(), "a transcript must produce chunks");
    for chunk in &chunks {
        assert_eq!(chunk.kind, ChunkKind::TranscriptTurns);
        assert!(
            matches!(chunk.locator, Locator::TimeRange { .. }),
            "a transcript chunk must carry a time range, not a line range"
        );
    }
    let Locator::TimeRange { start_ms, .. } = chunks[0].locator else {
        panic!("asserted above");
    };
    assert_eq!(start_ms, 0);

    // Transcription is a metered model call like any other (L9).
    let records = ledgers.records.lock().expect("test mutex");
    assert!(!records.is_empty(), "every window is a ledger row");
    for (feature, provider, outcome) in records.iter() {
        assert_eq!(feature, "ingest.transcribe");
        assert_eq!(provider, "scripted-test-engine");
        assert_eq!(outcome, "ok");
    }
}

#[test]
fn a_local_only_item_never_hands_the_model_a_transport() {
    // The zero-egress criterion at the pipeline level. The in-process
    // selection is not a rule someone could misread: the adapter is handed
    // `None`, so there is no socket to misuse (ADR-0006's compensating
    // control, one layer below the policy oracle that asserts the selection).
    let root = TempDir::new().expect("temp project");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let engine = Arc::new(ScriptedEngine::new());
    let pipeline = pipeline_with(
        Arc::clone(&engine),
        Arc::new(RecordingLedgers::default()),
        5_000,
        Arc::clone(&queue),
    );
    submit(&pipeline, &log, &clock, &audio_item(), &wav(16_000, 8.0));
    drain(&pipeline, &queue, &log, &clock, 16);

    assert!(
        !engine.offsets().is_empty(),
        "the engine must actually have been called, or the assertion below is vacuous"
    );
    assert!(
        !*engine.saw_transport.lock().expect("test mutex"),
        "a local-only transcription must never be handed a transport"
    );
}

#[test]
fn an_interrupted_transcription_resumes_without_re_transcribing_a_finished_window() {
    let root = TempDir::new().expect("temp project");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");

    // First run: two windows land, then the engine refuses — which is what a
    // killed process looks like from the pipeline's side, minus the part
    // where the assertions would die with it.
    let dying = Arc::new(ScriptedEngine::failing_after(2));
    let pipeline = pipeline_with(
        Arc::clone(&dying),
        Arc::new(RecordingLedgers::default()),
        5_000,
        Arc::clone(&queue),
    );
    let evidence_id =
        submit(&pipeline, &log, &clock, &audio_item(), &wav(16_000, 12.0)).evidence_id();
    let ran = drain(&pipeline, &queue, &log, &clock, 8);
    assert!(
        ran.iter()
            .any(|(stage, ok)| *stage == IngestStage::Transcribe && !ok),
        "the interrupted attempt must fail rather than silently finish: {ran:?}"
    );
    let first_offsets = dying.offsets();
    assert_eq!(
        first_offsets,
        vec![0, 4_900],
        "two windows decoded, the second starting where the first window's last \
         segment ended — that carry is what keeps a word off a boundary"
    );

    // The finished windows are durable facts, and the progress read is what
    // the resumed attempt will start from.
    let progress = read_transcript_progress(&log, evidence_id, 0)
        .expect("progress")
        .expect("two windows committed segments");
    assert_eq!(progress.0, 8, "nine segments, indices 0..=8");
    assert_eq!(progress.1, 8_900, "the last committed segment ends here");

    // The failed attempt is in the scheduler's retry backoff; a resume is
    // what happens after time passes, so time passes.
    clock.advance_ms(60_000);

    // Second run: a healthy engine, the same durable state. This is the
    // resume, and the assertion is that it does not redo finished work.
    let resumed = Arc::new(ScriptedEngine::new());
    let pipeline = pipeline_with(
        Arc::clone(&resumed),
        Arc::new(RecordingLedgers::default()),
        5_000,
        Arc::clone(&queue),
    );
    let ran = drain(&pipeline, &queue, &log, &clock, 8);
    assert!(
        ran.iter()
            .any(|(stage, ok)| *stage == IngestStage::Transcribe && *ok),
        "the resumed attempt must complete: {ran:?}"
    );

    let second_offsets = resumed.offsets();
    for offset in &second_offsets {
        assert!(
            !first_offsets.contains(offset),
            "window at {offset} ms was already transcribed; resuming must not redo it \
             (first: {first_offsets:?}, second: {second_offsets:?})"
        );
    }
    assert!(
        second_offsets.first().is_some_and(|first| *first >= 8_900),
        "the resume must start at the end of the last committed segment, not at zero: \
         {second_offsets:?}"
    );

    // And the item is whole: every second of scripted speech is present
    // exactly once, across both attempts.
    let segments = list_transcript_segments(&log, evidence_id, 0, None, 500).expect("segments");
    let text: Vec<&str> = segments
        .iter()
        .map(pos_domain::TranscriptSegmentRecord::rendered_text)
        .collect();
    assert_eq!(
        text,
        SPOKEN.to_vec(),
        "a resumed transcript must equal an uninterrupted one"
    );
}

#[test]
fn a_recording_with_no_speech_still_completes_rather_than_looping() {
    // The ADVANCE_FRACTION_MIN guard: a window the model finds nothing in
    // advances by the whole window. Without it a silent recording would
    // inch forward forever, which is a hang rather than an error and
    // therefore the worse failure (L8).
    let root = TempDir::new().expect("temp project");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let engine = Arc::new(ScriptedEngine::new());
    let pipeline = pipeline_with(
        Arc::clone(&engine),
        Arc::new(RecordingLedgers::default()),
        5_000,
        Arc::clone(&queue),
    );
    // Longer than the scripted engine has words for, so every window past the
    // twelfth second returns nothing at all.
    let evidence_id =
        submit(&pipeline, &log, &clock, &audio_item(), &wav(16_000, 40.0)).evidence_id();
    let ran = drain(&pipeline, &queue, &log, &clock, 16);
    assert!(
        ran.iter()
            .any(|(stage, ok)| *stage == IngestStage::Transcribe && *ok),
        "a mostly-silent recording must finish: {ran:?}"
    );
    let segments = list_transcript_segments(&log, evidence_id, 0, None, 500).expect("segments");
    assert_eq!(
        segments.len(),
        SPOKEN.len(),
        "silence produces no segments, and no fabricated ones"
    );
    assert!(
        engine.offsets().len() <= 12,
        "40 s of 5 s windows is 8 calls plus carry, not an unbounded crawl: {:?}",
        engine.offsets()
    );
}

#[test]
fn a_transcript_the_pipeline_produced_is_identical_on_a_re_run() {
    // P3 in the transcription path: same inputs, same durable output. This is
    // what makes at-least-once stage delivery safe and what the kill-matrix
    // digest oracle compares.
    let mut digests = Vec::new();
    for _ in 0..2 {
        let root = TempDir::new().expect("temp project");
        let clock = ManualWallClock::starting_at(1_700_000_000_000);
        let log = open_project(root.path(), &clock);
        let queue = queue();
        queue.ensure_schema(&log).expect("lease schema");
        let pipeline = pipeline_with(
            Arc::new(ScriptedEngine::new()),
            Arc::new(RecordingLedgers::default()),
            5_000,
            Arc::clone(&queue),
        );
        let evidence_id =
            submit(&pipeline, &log, &clock, &audio_item(), &wav(44_100, 12.0)).evidence_id();
        drain(&pipeline, &queue, &log, &clock, 16);
        let record = read_evidence(&log, evidence_id)
            .expect("read evidence")
            .expect("the item exists");
        digests.push((record.text_blob, record.segments_blob));
    }
    assert_eq!(
        digests[0], digests[1],
        "two identical runs must produce the same content-addressed blobs"
    );
    assert!(
        digests[0].0.is_some() && digests[0].1.is_some(),
        "TRANSCRIBE must have replaced NORMALIZE's empty placeholders"
    );
}
