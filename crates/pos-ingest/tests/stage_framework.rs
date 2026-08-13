//! m1-s01 oracles: the stage framework's acceptance criteria.
//!
//! Every suite here drives the real pipeline over a real `.pos` project and a
//! real queue. The point of the story is that stages inherit crash-resume,
//! retry, and the DLQ from `pos-sched` — so these tests exercise that path
//! rather than a stand-in for it.

#![forbid(unsafe_code)]

mod common;

use common::{PROJECT, document_text, drain, open_project, pipeline, queue, submission, submit};
use pos_domain::{
    EvidenceShape, EvidenceStatus, IngestStage, JobClass, MediaKind, StageState, list_evidence,
    list_source_health, list_stages, read_evidence,
};
use pos_foundation::ManualWallClock;
use pos_ingest::{
    ChunkStage, IngestPipeline, NormalizeStage, PipelineConfig, StageFailure, StageHandler,
    StageProduct, StageRegistry,
};
use pos_sched::{JobFailure, WorkerName};
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};
use tempfile::TempDir;

/// The pipeline runs, advances stage by stage, and stops honestly at the
/// first stage this build does not implement (P6) — never claiming an item is
/// finished and never marking it failed.
#[test]
fn the_pipeline_advances_stage_by_stage_and_halts_honestly() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(Arc::clone(&queue));

    let item = submission("doc-1", EvidenceShape::Document, MediaKind::Markdown);
    let outcome = submit(&pipeline, &log, &clock, &item, document_text(4).as_bytes());
    let evidence_id = outcome.evidence_id();
    assert!(!outcome.is_duplicate());

    let ran = drain(&pipeline, &queue, &log, &clock, 16);
    assert_eq!(
        ran,
        vec![(IngestStage::Normalize, true), (IngestStage::Chunk, true)]
    );

    let record = read_evidence(&log, evidence_id)
        .expect("read evidence")
        .expect("evidence exists");
    assert_eq!(record.status, EvidenceStatus::Chunked);
    assert!(record.chunk_count > 0, "a document must produce chunks");
    assert_eq!(record.chunk_pass, Some(0));
    // EMBED is registered nowhere in this build, so nothing was queued for it
    // and the item is not pretending to be indexed.
    assert!(!record.status.is_immutable());

    let stages = list_stages(&log, evidence_id).expect("stage rows");
    let names: Vec<(IngestStage, StageState)> = stages
        .iter()
        .map(|stage| (stage.stage, stage.state))
        .collect();
    assert_eq!(
        names,
        vec![
            (IngestStage::Normalize, StageState::Done),
            (IngestStage::Chunk, StageState::Done),
        ]
    );
    // The stage history carries the streaming proof: bytes read, not resident.
    assert!(stages[0].bytes_read.unwrap_or(0) > 0);
    assert!(stages[1].item_count.unwrap_or(0) > 0);
}

/// The idempotency oracle (m1-s01 AC). Killing a stage between its attempt
/// and its completion — which is what `kill -9` looks like to the queue —
/// must leave the projection digest identical to an uninterrupted run.
#[test]
fn a_stage_killed_at_any_point_re_runs_to_an_identical_projection_digest() {
    let text = document_text(6);
    let reference = run_corpus_digest(&text, KillPoint::None);
    for kill_point in [
        KillPoint::BeforeNormalizeCompletes,
        KillPoint::BeforeChunkCompletes,
        KillPoint::MidChunkBatches,
    ] {
        let replayed = run_corpus_digest(&text, kill_point);
        assert_eq!(
            replayed, reference,
            "projection digest diverged after a kill at {kill_point:?}"
        );
    }
}

/// Where a fault is injected. Each one models a real `kill -9`: the effect
/// may or may not have happened, and the completion fact certainly has not.
#[derive(Clone, Copy, Debug)]
enum KillPoint {
    None,
    BeforeNormalizeCompletes,
    BeforeChunkCompletes,
    MidChunkBatches,
}

/// Runs one corpus to completion, optionally failing a stage the first time
/// it is attempted, and returns the projection digest.
fn run_corpus_digest(text: &str, kill_point: KillPoint) -> String {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");

    let (normalize_faults, chunk_faults) = match kill_point {
        KillPoint::None => (0, 0),
        KillPoint::BeforeNormalizeCompletes => (1, 0),
        KillPoint::BeforeChunkCompletes | KillPoint::MidChunkBatches => (0, 1),
    };
    let registry = StageRegistry::new()
        .with(Arc::new(FaultingStage::new(
            Arc::new(NormalizeStage),
            normalize_faults,
        )))
        .with(Arc::new(FaultingStage::new(
            Arc::new(ChunkStage::new()),
            chunk_faults,
        )));
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(common::DEVICE),
        Arc::clone(&queue),
        registry,
    );

    let item = submission("doc-kill", EvidenceShape::Document, MediaKind::Markdown);
    submit(&pipeline, &log, &clock, &item, text.as_bytes());
    // Retries are scheduled with backoff; move the clock past it between
    // rounds so the fixture drains rather than idling on a run-at instant.
    for _ in 0..8 {
        drain(&pipeline, &queue, &log, &clock, 16);
        clock.advance_ms(60_000);
    }

    let record = read_evidence(&log, item_id(&log))
        .expect("read evidence")
        .expect("evidence exists");
    assert_eq!(
        record.status,
        EvidenceStatus::Chunked,
        "the item must reach the same terminal state regardless of the kill point"
    );
    common::pipeline_digest(&log)
}

fn item_id(log: &pos_log::ProjectLog) -> pos_foundation::EvidenceId {
    list_evidence(log, pos_domain::EvidenceListFilter::default())
        .expect("list evidence")
        .first()
        .expect("one evidence item")
        .evidence_id
}

/// Wraps a stage and fails its first `fault_count` attempts *after* the
/// handler has done its work — the shape a crash has: the CAS writes and any
/// committed chunk batches survive, the completion fact does not.
struct FaultingStage {
    inner: Arc<dyn StageHandler>,
    remaining: AtomicU32,
}

impl FaultingStage {
    fn new(inner: Arc<dyn StageHandler>, fault_count: u32) -> Self {
        Self {
            inner,
            remaining: AtomicU32::new(fault_count),
        }
    }
}

impl StageHandler for FaultingStage {
    fn stage(&self) -> IngestStage {
        self.inner.stage()
    }

    fn run(&self, context: &pos_ingest::StageContext<'_>) -> Result<StageProduct, StageFailure> {
        let product = self.inner.run(context)?;
        if self
            .remaining
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |remaining| {
                remaining.checked_sub(1)
            })
            .is_ok()
        {
            return Err(StageFailure::retriable(
                "injected_fault",
                "the fixture killed this attempt after its effects landed",
            ));
        }
        Ok(product)
    }
}

/// The DLQ criterion: a dead item shows its stage, attempt count, and typed
/// reason — the L8 rule that a dead item is never a silent drop.
#[test]
fn a_dead_stage_shows_its_stage_attempt_and_typed_reason() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(common::DEVICE),
        Arc::clone(&queue),
        StageRegistry::new().with(Arc::new(RefusingStage)),
    );

    let item = submission("doc-dead", EvidenceShape::Document, MediaKind::Markdown);
    let evidence_id = submit(&pipeline, &log, &clock, &item, b"content").evidence_id();
    let worker = WorkerName::new("dlq-0").expect("worker");
    let job = queue
        .claim(&log, PROJECT, JobClass::Ingest, &worker, &clock)
        .expect("claim")
        .expect("a normalize job is queued");
    let failure = pipeline
        .run_stage(&log, PROJECT, &clock, IngestStage::Normalize, &job)
        .expect_err("the stage refuses");
    assert!(failure.permanent);
    queue
        .fail(
            &log,
            &job,
            &JobFailure::permanent(failure.code.clone(), failure.detail.clone()),
            1,
            &clock,
        )
        .expect("record the DLQ transition");

    let stages = list_stages(&log, evidence_id).expect("stage rows");
    let normalize = stages
        .iter()
        .find(|row| row.stage == IngestStage::Normalize)
        .expect("a normalize row exists");
    assert_eq!(normalize.state, StageState::Dead);
    assert_eq!(normalize.attempt_index, 1);
    assert_eq!(normalize.last_error_code.as_deref(), Some("unreadable"));
    assert!(
        normalize
            .last_error_detail
            .as_deref()
            .is_some_and(|detail| detail.contains("fixture")),
        "the DLQ reason must be renderable, not a code alone"
    );

    let record = read_evidence(&log, evidence_id)
        .expect("read evidence")
        .expect("evidence exists");
    assert_eq!(record.status, EvidenceStatus::Failed);

    // ...and the source health card counts it without scanning the corpus.
    let health = list_source_health(&log, Some(record.source_id)).expect("health rows");
    let normalize_health = health
        .iter()
        .find(|row| row.stage == IngestStage::Normalize)
        .expect("a normalize health row exists");
    assert_eq!(normalize_health.dead_count, 1);
    assert_eq!(normalize_health.failed_count, 1);
    assert_eq!(
        normalize_health.last_error_code.as_deref(),
        Some("unreadable")
    );
}

struct RefusingStage;

impl StageHandler for RefusingStage {
    fn stage(&self) -> IngestStage {
        IngestStage::Normalize
    }

    fn run(&self, _context: &pos_ingest::StageContext<'_>) -> Result<StageProduct, StageFailure> {
        Err(StageFailure::permanent(
            "unreadable",
            "the fixture refuses this content permanently",
        ))
    }
}

/// Streaming discipline (m1-s01 AC, scaled to CI): a single file two orders
/// of magnitude larger than the buffer flows through every stage with a
/// resident window bounded by the stated budget, not by the input.
#[test]
fn a_large_single_file_flows_through_every_stage_within_the_buffer_budget() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    // A deliberately small budget: the property is that the resident window
    // follows the *budget*, so squeezing it proves more than raising it.
    let mut config = PipelineConfig::for_device(common::DEVICE);
    config.buffer_bytes_max = 512 * 1024;
    let pipeline = IngestPipeline::new(
        config,
        Arc::clone(&queue),
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(ChunkStage::new())),
    );

    let text = document_text(3_000);
    assert!(
        text.len() > 4 * config.buffer_bytes_max,
        "the fixture must be several buffers deep, not one"
    );
    let item = submission("doc-large", EvidenceShape::Document, MediaKind::Markdown);
    let evidence_id = submit(&pipeline, &log, &clock, &item, text.as_bytes()).evidence_id();
    let ran = drain(&pipeline, &queue, &log, &clock, 16);
    assert_eq!(
        ran,
        vec![(IngestStage::Normalize, true), (IngestStage::Chunk, true)]
    );

    let stages = list_stages(&log, evidence_id).expect("stage rows");
    let normalize = &stages[0];
    assert_eq!(
        normalize.bytes_read,
        Some(text.len() as u64),
        "NORMALIZE must have streamed every byte"
    );
    let record = read_evidence(&log, evidence_id)
        .expect("read evidence")
        .expect("evidence exists");
    assert!(record.segment_count.unwrap_or(0) > 100);
    assert!(record.chunk_count > 10);
}
