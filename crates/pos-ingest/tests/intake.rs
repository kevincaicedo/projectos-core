//! Intake oracles (m1-s07): the front door, and the memory bound it opened.
//!
//! Four properties, each with a failure it exists to catch:
//!
//! 1. **A re-drop of identical bytes is one item.** The failure it prevents is
//!    two Evidence items over one recording, which makes every citation into
//!    that recording ambiguous about which copy it meant.
//! 2. **A folder import is bounded and reports what it left out.** Silent
//!    truncation reads as completeness (L8).
//! 3. **A caption file becomes a transcript.** Same shape, same locators, same
//!    chunker as decoded audio — so an already-transcribed interview and a
//!    recording cite identically.
//! 4. **A GB-scale file streams.** [ADR-0008] bound 1 asserted against the
//!    process-wide meter, over a corpus many buffers deep. This is the
//!    in-code half of the §18 row `pos-bench` measures at 8 GiB.
//!
//! The meter is process-wide by construction (the bound sums across stages),
//! so the suites that read absolute values take one lock. Two tests measuring
//! one gauge would otherwise be measuring each other.
//!
//! [ADR-0008]: ../../../docs/adr/0008-ingest-memory-budget-splits-buffers-from-model-weights.md

#![forbid(unsafe_code)]

mod common;

use common::{DEVICE, PROJECT, USER, drain, open_project, pipeline, queue};
use pos_domain::{EvidenceShape, ExternalRef, IngestStage, Locator, MediaKind, read_evidence};
use pos_foundation::ManualWallClock;
use pos_ingest::{
    EvidenceSubmission, INTAKE_FILE_COUNT_MAX, IngestPipeline, PIPELINE_BUFFER_BYTES_MAX,
    SegmentReader, SubmitOutcome, buffer_residency, reset_buffer_peak,
};
use pos_log::{Actor, ProjectLog};
use pos_store::BlobHash;
use std::path::Path;
use std::sync::Mutex;

/// Serializes the tests that read the process-wide buffer meter.
static METER: Mutex<()> = Mutex::new(());

/// Submits a file through the same two calls the API command makes: classify
/// from the bytes, then stream into the CAS.
fn submit_file(
    pipeline: &IngestPipeline,
    log: &ProjectLog,
    clock: &ManualWallClock,
    path: &Path,
) -> SubmitOutcome {
    let mut intake = pos_ingest::open_file(path).expect("the intake opens a readable file");
    let submission = EvidenceSubmission {
        source_kind: "upload".to_owned(),
        source_scope: "uploads".to_owned(),
        external: ExternalRef {
            external_id: String::new(),
            external_url: None,
            external_version: None,
        },
        media_kind: intake.media_kind,
        shape: intake.shape,
        occurred_ts_ms: intake.modified_ms.unwrap_or(1_700_000_000_000),
        author: None,
        title: Some(pos_ingest::intake_title(
            &path.file_name().unwrap_or_default().to_string_lossy(),
        )),
        thread_ref: None,
        actor: Actor::User(USER),
    };
    pipeline
        .submit(log, PROJECT, clock, &submission, &mut intake.content)
        .expect("submit the intake file")
}

#[test]
fn the_same_bytes_dropped_twice_are_one_evidence_item() {
    let root = tempfile::tempdir().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&root.path().join("project.pos"), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(std::sync::Arc::clone(&queue));

    let first = root.path().join("kickoff.md");
    std::fs::write(
        &first,
        b"# Kickoff\n\nWe agreed to ship the evidence engine.\n",
    )
    .expect("write fixture");
    // The same bytes under a different name: a partner who renamed the export
    // must not get a second copy of the same interview.
    let renamed = root.path().join("kickoff-final.md");
    std::fs::copy(&first, &renamed).expect("copy fixture");

    let added = submit_file(&pipeline, &log, &clock, &first);
    assert!(matches!(added, SubmitOutcome::Added(_)));
    let again = submit_file(&pipeline, &log, &clock, &first);
    let renamed_outcome = submit_file(&pipeline, &log, &clock, &renamed);

    assert!(again.is_duplicate(), "an identical re-drop is a duplicate");
    assert!(
        renamed_outcome.is_duplicate(),
        "identity is the content, not the file name"
    );
    assert_eq!(
        added.evidence_id(),
        again.evidence_id(),
        "a duplicate names the item the caller already has"
    );
    assert_eq!(added.evidence_id(), renamed_outcome.evidence_id());
}

#[test]
fn a_folder_import_covers_its_files_and_says_what_it_left_out() {
    let root = tempfile::tempdir().expect("tempdir");
    let corpus = root.path().join("Interviews");
    std::fs::create_dir(&corpus).expect("mkdir");
    for index in 0..3 {
        std::fs::write(
            corpus.join(format!("note-{index}.md")),
            format!("# Note {index}\n\nBody.\n"),
        )
        .expect("write fixture");
    }
    std::fs::write(corpus.join(".DS_Store"), b"metadata").expect("write fixture");
    // A project directory inside the tree: walking into it would ingest
    // ProjectOS into ProjectOS.
    std::fs::create_dir(corpus.join("archive.pos")).expect("mkdir");
    std::fs::write(corpus.join("archive.pos").join("log.db"), b"no").expect("write fixture");

    let plan = pos_ingest::plan_intake(&corpus).expect("the walk succeeds");
    assert_eq!(plan.files.len(), 3);
    assert_eq!(plan.skipped_count, 2, "the dot-file and the nested project");
    assert!(!plan.truncated);
    assert!(
        plan.files.len() <= INTAKE_FILE_COUNT_MAX,
        "the walk is bounded by its stated file count"
    );

    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&root.path().join("project.pos"), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(std::sync::Arc::clone(&queue));
    for file in &plan.files {
        assert!(matches!(
            submit_file(&pipeline, &log, &clock, file),
            SubmitOutcome::Added(_)
        ));
    }
    let ran = drain(&pipeline, &queue, &log, &clock, 64);
    assert!(
        ran.iter().all(|(_, ok)| *ok),
        "every stage of every imported file succeeded: {ran:?}"
    );
}

/// A caption file is speech somebody already transcribed. It must come out of
/// NORMALIZE looking exactly like decoded audio — transcript shape, one
/// segment per utterance, a time range on each — because the chunker, the
/// viewer, and citation resolution are shared with the whisper path.
#[test]
fn a_caption_file_becomes_a_transcript_with_time_locators() {
    let root = tempfile::tempdir().expect("tempdir");
    let captions = root.path().join("kickoff.vtt");
    std::fs::write(
        &captions,
        b"WEBVTT\n\n\
          NOTE exported by hand\n\n\
          1\n00:00:01.000 --> 00:00:03.500\nWe agreed to ship it.\n\n\
          2\n00:00:03.500 --> 00:00:07.000\nThe evidence engine\ncomes first.\n\n\
          3\n00:00:02.000 --> 00:00:03.000\nAn overlapping repeat.\n",
    )
    .expect("write fixture");

    let intake = pos_ingest::open_file(&captions).expect("open the caption file");
    assert_eq!(intake.media_kind, MediaKind::Captions);
    assert_eq!(intake.shape, EvidenceShape::Transcript);
    // TRANSCRIBE is skipped for captions: there is nothing to decode, and
    // parking an offline item behind a model download would be absurd.
    assert!(!IngestStage::Transcribe.applies_to(MediaKind::Captions));

    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&root.path().join("project.pos"), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(std::sync::Arc::clone(&queue));
    let outcome = submit_file(&pipeline, &log, &clock, &captions);
    let ran = drain(&pipeline, &queue, &log, &clock, 16);
    assert!(ran.iter().all(|(_, ok)| *ok), "{ran:?}");

    let record = read_evidence(&log, outcome.evidence_id())
        .expect("read evidence")
        .expect("the item exists");
    assert_eq!(record.shape, EvidenceShape::Transcript);
    let segments_blob = record
        .segments_blob
        .expect("captions produce a segment index");
    let mut reader = SegmentReader::new(pos_ingest::BoundedStream::new(
        log.store()
            .blobs()
            .open_blob(BlobHash::from_bytes(segments_blob))
            .expect("open the segment index"),
        pos_ingest::StreamBudget::default_for(IngestStage::Chunk),
    ));
    let mut spans = Vec::new();
    while let Some(segment) = reader.next_segment().expect("read a segment") {
        spans.push(segment);
    }
    assert_eq!(
        spans.len(),
        2,
        "the overlapping repeat is dropped: {spans:?}"
    );
    assert_eq!(
        spans[0].locator,
        Locator::TimeRange {
            start_ms: 1_000,
            end_ms: 3_500
        }
    );
    assert_eq!(
        spans[1].locator,
        Locator::TimeRange {
            start_ms: 3_500,
            end_ms: 7_000
        }
    );
    // Segments tile the text without overlapping, which is what the citation
    // machinery and the chunker both assume (m1-s03's T2).
    assert!(spans[0].byte_end < spans[1].byte_start);
}

/// [ADR-0008] bound 1, in code: a corpus many stage buffers deep streams with
/// the process-wide resident total inside the stated bound.
///
/// `pos-bench` runs the same property at 8 GiB and writes the artifact; this
/// suite is the one that fails a pull request, which is why it uses a corpus a
/// CI runner can hold.
#[test]
fn a_corpus_many_buffers_deep_streams_inside_the_pipeline_bound() {
    let guard = METER.lock().expect("the meter lock is never poisoned");
    reset_buffer_peak();
    let root = tempfile::tempdir().expect("tempdir");
    let corpus = root.path().join("corpus.md");
    // Sixteen mebibytes of headed sections: sixty-four full stage reads, and
    // several thousand chunks, through a buffer that must not grow with it.
    let mut text = String::with_capacity(16 * 1024 * 1024);
    let mut section = 0_u32;
    while text.len() < 16 * 1024 * 1024 {
        text.push_str(&format!("## Section {section}\n\n"));
        for _ in 0..8 {
            text.push_str("The pipeline streams this sentence and never holds the file. ");
        }
        text.push_str("\n\n");
        section += 1;
    }
    std::fs::write(&corpus, text.as_bytes()).expect("write fixture");

    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(&root.path().join("project.pos"), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(std::sync::Arc::clone(&queue));
    let outcome = submit_file(&pipeline, &log, &clock, &corpus);
    let ran = drain(&pipeline, &queue, &log, &clock, 64);
    assert!(ran.iter().all(|(_, ok)| *ok), "{ran:?}");

    let residency = buffer_residency();
    assert!(
        residency.peak_bytes < PIPELINE_BUFFER_BYTES_MAX as u64,
        "the pipeline held {} bytes at its peak, over the ADR-0008 bound of {}",
        residency.peak_bytes,
        PIPELINE_BUFFER_BYTES_MAX
    );
    // Every stream released what it counted: a meter that only ever grew
    // would report the bound holding right up until it silently did not.
    assert_eq!(
        residency.resident_bytes, 0,
        "a quiescent pipeline holds no streaming buffers"
    );
    let record = read_evidence(&log, outcome.evidence_id())
        .expect("read evidence")
        .expect("the item exists");
    assert!(
        record.chunk_count > 1_000,
        "the corpus really did chunk: {} chunks",
        record.chunk_count
    );
    drop(guard);
}

/// The device id is part of every appended event; asserting it here keeps the
/// fixture honest about which identity ingested.
#[test]
fn intake_titles_are_text_not_markup_or_paths() {
    assert_eq!(pos_ingest::intake_title("kickoff.m4a"), "kickoff.m4a");
    assert_eq!(
        pos_ingest::intake_title("weird\u{0}\u{1}name.txt"),
        "weirdname.txt"
    );
    assert_eq!(pos_ingest::intake_title("   "), "Untitled upload");
    assert_eq!(DEVICE.into_bytes()[0], 0x71);
}
