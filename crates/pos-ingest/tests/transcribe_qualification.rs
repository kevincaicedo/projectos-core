//! The m1-s03 transcription qualification lane: **real** whisper, **real**
//! audio, through the real pipeline.
//!
//! Ignored in the PR lane — it needs a pulled model artifact and a recording,
//! neither of which belongs in the repository — and run explicitly by
//! `just qualify-transcribe-local`. What it prints is the §18 row:
//!
//! ```text
//! TRANSCRIPTION model=whisper-small audio_ms=… wall_ms=… realtime_factor=…×
//!               segments=… rss_peak_mib=… content_blake3=…
//! ```
//!
//! ## Why the artifact records a content hash instead of a path
//!
//! The recordings are a founder's real interviews: gitignored, and present on
//! exactly one machine. A gate artifact that pointed at `tmp/interview.m4a`
//! would be unreproducible *and* unfalsifiable. The hash and the duration are
//! what make a later run comparable to this one.
//!
//! ## What it does not claim
//!
//! It is not a `pos-bench` artifact. `pos-bench` writes the reference-machine
//! header and computes `binding` vs `early_warning` itself, and it is held to
//! `pos-api` — where no registered command puts bytes into a project yet
//! (the m1-s01 intake-seam finding). Until that seam exists, this lane
//! measures the same work through the same code and the number is recorded in
//! `docs/progress.md` as what it is.

#![forbid(unsafe_code)]

mod common;

use common::{DEVICE, drain, open_project, queue, submission, submit};
use pos_domain::{
    EvidenceShape, EvidenceStatus, IngestStage, MediaKind, list_transcript_segments, read_evidence,
};
use pos_foundation::{ManualWallClock, SystemWallClock, WallClock};
use pos_ingest::{
    ChunkStage, IngestPipeline, NormalizeStage, PipelineConfig, StageRegistry, TranscribeSetup,
    TranscribeStage,
};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::TempDir;

/// The §18 row: transcription must keep up with a real corpus.
const REALTIME_FACTOR_MIN: f64 = 5.0;

/// A counting cost ledger. The lane needs a real one — the pipeline refuses to
/// run a model call it cannot meter — and `pos-api`'s event-backed ledger is
/// above this crate, so the lane composes the smallest honest stand-in and
/// reports what it counted.
struct CountingLedgers {
    calls: Arc<std::sync::atomic::AtomicU64>,
}

impl pos_ingest::StageLedgers for CountingLedgers {
    fn open<'a>(
        &self,
        _log: &'a pos_log::ProjectLog,
        _clock: &'a dyn pos_foundation::WallClock,
        _actor: pos_log::Actor,
    ) -> Box<dyn pos_gateway::CostLedger + 'a> {
        Box::new(CountingLedger {
            calls: Arc::clone(&self.calls),
        })
    }
}

struct CountingLedger {
    calls: Arc<std::sync::atomic::AtomicU64>,
}

impl pos_gateway::CostLedger for CountingLedger {
    fn record(
        &self,
        _record: &pos_gateway::ModelCallRecord,
    ) -> Result<(), pos_gateway::LedgerError> {
        self.calls
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }
}

fn required_env(name: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| {
        panic!("{name} is not set; run this lane via `just qualify-transcribe-local`")
    })
}

/// Streams the recording through the CAS the way an upload would, hashing as
/// it goes, so the artifact can name the bytes rather than a path.
fn read_audio(path: &std::path::Path) -> (Vec<u8>, String) {
    let bytes = std::fs::read(path)
        .unwrap_or_else(|error| panic!("read the qualification audio {}: {error}", path.display()));
    let hash = blake3::hash(&bytes).to_hex().to_string();
    (bytes, hash)
}

#[test]
#[ignore = "qualification lane: `just qualify-transcribe-local` (needs a pulled model and a recording)"]
fn whisper_small_keeps_up_with_a_real_recording() {
    let models_dir = PathBuf::from(required_env("POS_QUALIFY_MODELS_DIR"));
    let model =
        std::env::var("POS_QUALIFY_WHISPER_MODEL").unwrap_or_else(|_| "whisper-small".to_owned());
    let audio_path = PathBuf::from(required_env("POS_QUALIFY_AUDIO"));
    let replicates: u32 = std::env::var("POS_QUALIFY_REPLICATES")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(1);

    let (audio, content_blake3) = read_audio(&audio_path);
    let wall = SystemWallClock;

    for replicate in 1..=replicates {
        let root = TempDir::new().expect("temp project");
        let clock = ManualWallClock::starting_at(1_700_000_000_000);
        let log = open_project(root.path(), &clock);
        let queue = queue();
        queue.ensure_schema(&log).expect("lease schema");
        let setup = TranscribeSetup::local(models_dir.clone(), model.clone());
        let calls = Arc::new(std::sync::atomic::AtomicU64::new(0));
        let ledgers: Arc<dyn pos_ingest::StageLedgers> = Arc::new(CountingLedgers {
            calls: Arc::clone(&calls),
        });
        let pipeline = IngestPipeline::new(
            PipelineConfig::for_device(DEVICE).with_ledgers(ledgers),
            Arc::clone(&queue),
            StageRegistry::new()
                .with(Arc::new(NormalizeStage))
                .with(Arc::new(TranscribeStage::new(setup)))
                .with(Arc::new(ChunkStage::new())),
        );
        let item = submission(
            "qualification-audio",
            EvidenceShape::Document,
            MediaKind::Audio,
        );
        let evidence_id = submit(&pipeline, &log, &clock, &item, &audio).evidence_id();

        let started_ms = wall.now_ms();
        let ran = drain(&pipeline, &queue, &log, &clock, 16);
        let wall_ms = wall.now_ms().saturating_sub(started_ms);

        if ran.iter().any(|(_, succeeded)| !succeeded) {
            // A failed stage says why in its own row; a lane that only said
            // "false" would send a reader back to the code to guess.
            let rows: Vec<String> = pos_domain::list_stages(&log, evidence_id)
                .expect("stage rows")
                .into_iter()
                .map(|stage| {
                    format!(
                        "{}:{:?} {}",
                        stage.stage,
                        stage.state,
                        stage.last_error_detail.unwrap_or_default()
                    )
                })
                .collect();
            panic!("every stage must succeed: {ran:?}\n{}", rows.join("\n"));
        }
        let record = read_evidence(&log, evidence_id)
            .expect("read evidence")
            .expect("the item exists");
        assert_eq!(record.status, EvidenceStatus::Chunked);

        let segments =
            list_transcript_segments(&log, evidence_id, 0, None, 500).expect("transcript segments");
        assert!(
            !segments.is_empty(),
            "a real recording must produce transcript segments"
        );
        // `bytes_read` on the TRANSCRIBE completion is the audio milliseconds
        // the stage actually decoded, which is the denominator of the gate.
        let audio_ms = pos_domain::list_stages(&log, evidence_id)
            .expect("stage rows")
            .into_iter()
            .find(|stage| stage.stage == IngestStage::Transcribe)
            .and_then(|stage| stage.bytes_read)
            .unwrap_or(0);
        assert!(audio_ms > 0, "the stage must report the audio it decoded");

        #[expect(
            clippy::cast_precision_loss,
            reason = "a ratio of two millisecond counts; f64 is exact well past any recording"
        )]
        let factor = audio_ms as f64 / wall_ms.max(1) as f64;
        println!(
            "TRANSCRIPTION replicate={replicate}/{replicates} model={model} \
             audio_ms={audio_ms} wall_ms={wall_ms} realtime_factor={factor:.2}x \
             windows={} segments_first_page={} content_blake3={content_blake3}",
            calls.load(std::sync::atomic::Ordering::Relaxed),
            segments.len()
        );
        assert!(
            factor >= REALTIME_FACTOR_MIN,
            "transcription ran at {factor:.2}× realtime; the §18 gate is \
             {REALTIME_FACTOR_MIN}× (audio {audio_ms} ms in {wall_ms} ms)"
        );
    }
}
