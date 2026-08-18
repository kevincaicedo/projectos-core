//! The m1-s04 EMBED oracles.
//!
//! Four properties, each the mechanism an acceptance criterion names:
//!
//! 1. **Adversarial chunk lengths never exceed the batch budget.** The engine
//!    records the padded shape of every batch it was handed; no batch may cost
//!    more than the stated budget, whatever the corpus looks like. ONNX
//!    Runtime's arena grows and never shrinks, so a single admitted overrun
//!    raises this process's RSS permanently — which is why the property is
//!    about *admission*, not about observed peak.
//! 2. **Identical content under one model embeds once.** Two Evidence items
//!    with the same bytes produce one model call for the shared content, and
//!    the second item's chunks point at the first item's vectors (F6). The
//!    ledger is what proves it — a "dedup" that still spent the call would be
//!    a comment, not a property.
//! 3. **Re-embedding to a second model is additive, and the old vectors are
//!    collectable as a set.** Both models' rows coexist keyed by model id, a
//!    query naming either gets only its own, and nothing has to guess which
//!    came first.
//! 4. **An interrupted pass recomputes nothing that finished.** Committed
//!    vectors are facts; the resumed attempt is asserted to embed no chunk the
//!    first attempt already committed.
//!
//! The engine is scripted rather than real ONNX on purpose: these are
//! properties of the *pipeline*, and a suite whose outcome depended on what a
//! model computed would be measuring the model. The real engine is exercised
//! by `wordpiece_reference` and the §18 embedding bench.

#![forbid(unsafe_code)]

mod common;

use common::{DEVICE, PROJECT, drain, open_project, queue, submission, submit};
use pos_domain::{
    EvidenceShape, EvidenceStatus, IngestStage, MediaKind, find_embedding_by_content,
    list_chunk_embeddings, list_chunks, list_embedding_models, read_evidence,
};
use pos_foundation::ManualWallClock;
use pos_gateway::{
    CallAuth, EMBED_BATCH_PADDED_TOKENS_MAX, EMBED_SEQUENCE_TOKENS_MAX, EmbedBatch, EmbedRequest,
    EmbedUsage, Embedder, HttpTransport, Weather,
};
use pos_ingest::{
    ChunkStage, EMBED_DIM_DEFAULT, EMBED_MODEL_DEFAULT, EmbedRoute, EmbedSetup, EmbedStage,
    IngestPipeline, NormalizeStage, PipelineConfig, StageRegistry,
};
use std::sync::{Arc, Mutex, PoisonError};
use tempfile::TempDir;

/// FNV-1a. A real hash rather than a byte sum: the first fixture summed bytes
/// and two model names that differed by a multiple of three produced identical
/// vectors, which made the re-embed property pass for the wrong reason.
fn fnv1a(bytes: &[u8]) -> u64 {
    let mut hash = 0xcbf2_9ce4_8422_2325_u64;
    for byte in bytes {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x1000_0000_01b3);
    }
    hash
}

/// A scripted embedder that records every batch it was asked to compute.
#[derive(Debug, Default)]
struct ScriptedEmbedder {
    model: String,
    dim: u16,
    calls: Mutex<Vec<BatchShape>>,
    /// Refuse from this call index onward, so a suite can interrupt a pass
    /// exactly where it wants to.
    refuse_from: Option<usize>,
}

/// What one dispatched batch cost, as the budget states it.
#[derive(Clone, Debug, Eq, PartialEq)]
struct BatchShape {
    count: usize,
    longest_tokens: usize,
    texts: Vec<String>,
}

impl BatchShape {
    const fn padded_tokens(&self) -> usize {
        self.count * self.longest_tokens
    }
}

impl ScriptedEmbedder {
    fn new(model: &str) -> Self {
        Self {
            model: model.to_owned(),
            dim: EMBED_DIM_DEFAULT,
            calls: Mutex::new(Vec::new()),
            refuse_from: None,
        }
    }

    fn refusing_from(model: &str, call_index: usize) -> Self {
        Self {
            refuse_from: Some(call_index),
            ..Self::new(model)
        }
    }

    fn calls(&self) -> Vec<BatchShape> {
        self.calls
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    fn call_count(&self) -> usize {
        self.calls().len()
    }

    /// Every text this engine was ever handed, across all batches.
    fn embedded_texts(&self) -> Vec<String> {
        self.calls()
            .into_iter()
            .flat_map(|shape| shape.texts)
            .collect()
    }
}

impl Embedder for ScriptedEmbedder {
    fn label(&self) -> &'static str {
        "scripted-embed"
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn dim(&self) -> u16 {
        self.dim
    }

    fn embed(
        &self,
        _auth: &CallAuth,
        request: &EmbedRequest<'_>,
        transport: Option<&dyn HttpTransport>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather> {
        assert!(
            transport.is_none(),
            "an in-process model is handed no socket"
        );
        let texts: Vec<String> = request
            .inputs
            .iter()
            .map(|input| input.content.to_owned())
            .collect();
        // The same over-estimate the planner uses, so the recorded shape is
        // the shape the budget was checked against.
        let longest_tokens = texts
            .iter()
            .map(|text| {
                text.chars()
                    .count()
                    .div_ceil(3)
                    .clamp(1, EMBED_SEQUENCE_TOKENS_MAX)
            })
            .max()
            .unwrap_or(1);
        let shape = BatchShape {
            count: texts.len(),
            longest_tokens,
            texts,
        };
        let mut calls = self.calls.lock().unwrap_or_else(PoisonError::into_inner);
        let index = calls.len();
        calls.push(shape);
        drop(calls);
        if self.refuse_from.is_some_and(|from| index >= from) {
            return Err(Weather::Timeout { timeout_ms: 1 });
        }
        let width = usize::from(self.dim);
        let count = request.inputs.len();
        let scale = 1.0_f32 / (width as f32).sqrt();
        let mut vectors = Vec::with_capacity(count * width);
        for input in request.inputs {
            // Deterministic in the content, so an identical chunk anywhere
            // produces an identical vector — which is what makes the dedup
            // property checkable end to end rather than by trusting a flag.
            // And in the model name, because a real second model produces
            // different bytes; without that the CAS would correctly dedup two
            // models' vectors into one blob and the re-embed property would
            // be asserting the fixture rather than the product.
            let seed = fnv1a(self.model.as_bytes()) ^ fnv1a(input.content.as_bytes());
            for index in 0..width {
                let sign = if (seed >> (index % 64)) & 1 == 1 {
                    1.0
                } else {
                    -1.0
                };
                vectors.push(sign * scale);
            }
        }
        let tokens = u64::try_from(count * longest_tokens).unwrap_or(u64::MAX);
        Ok((
            EmbedBatch::new(self.dim, vectors, count)?,
            EmbedUsage {
                tokens_in: tokens,
                padded_tokens: tokens,
                vector_count: count as u64,
                truncated_count: 0,
                measured: true,
            },
        ))
    }
}

/// A ledger that keeps every record, so a suite can assert what a pass
/// actually *spent* rather than what an engine happened to be asked.
///
/// The F6 criterion is worded in exactly these terms — "the ledger shows one
/// embed cost" — so this, not the engine's call log, is what proves it.
#[derive(Default)]
struct RecordingLedgers {
    records: Arc<Mutex<Vec<(String, String, u64)>>>,
}

impl RecordingLedgers {
    /// Successful EMBED rows, as `(provider, tokens_in)`.
    fn embed_calls(&self) -> Vec<(String, u64)> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .iter()
            .filter(|(feature, _, _)| feature == IngestStage::Embed.cost_feature())
            .map(|(_, provider, tokens)| (provider.clone(), *tokens))
            .collect()
    }
}

impl pos_ingest::StageLedgers for RecordingLedgers {
    fn open<'a>(
        &self,
        _log: &'a pos_log::ProjectLog,
        _clock: &'a dyn pos_foundation::WallClock,
        _actor: pos_log::Actor,
    ) -> Box<dyn pos_gateway::CostLedger + 'a> {
        Box::new(RecordingLedger {
            records: Arc::clone(&self.records),
        })
    }
}

struct RecordingLedger {
    records: Arc<Mutex<Vec<(String, String, u64)>>>,
}

impl pos_gateway::CostLedger for RecordingLedger {
    fn record(
        &self,
        record: &pos_gateway::ModelCallRecord,
    ) -> Result<(), pos_gateway::LedgerError> {
        self.records
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push((
                record.feature.clone(),
                record.provider.to_owned(),
                record.tokens_in,
            ));
        Ok(())
    }
}

/// The E1 pipeline plus EMBED, running `engine` and metering into `ledgers`.
fn pipeline_with(
    queue: Arc<pos_sched::JobQueue>,
    engine: Arc<ScriptedEmbedder>,
    ledgers: Arc<RecordingLedgers>,
) -> IngestPipeline {
    let setup = EmbedSetup {
        route: EmbedRoute::LocalOnnx {
            models_dir: std::path::PathBuf::from("unused-scripted-engine"),
            model_name: engine.model.clone(),
            dim: engine.dim,
            pooling: pos_gateway::Pooling::ClassifyToken,
        },
        ..EmbedSetup::local(std::path::PathBuf::from("unused-scripted-engine"))
    };
    IngestPipeline::new(
        PipelineConfig::for_device(DEVICE).with_ledgers(ledgers),
        queue,
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(ChunkStage::new()))
            .with(Arc::new(EmbedStage::with_engine(setup, engine))),
    )
}

/// Prose whose paragraph lengths vary wildly, so batches are not uniform.
fn corpus(paragraph_count: usize) -> String {
    let mut text = String::new();
    for index in 0..paragraph_count {
        // Lengths sweep from a few words to past the sequence cap, which is
        // the adversarial shape the batch-budget criterion names.
        let words = 3 + (index * 37) % 900;
        for word in 0..words {
            text.push_str(&format!("word{} ", (index * 17 + word) % 211));
        }
        text.push_str("\n\n");
    }
    text
}

#[test]
fn adversarial_chunk_lengths_never_exceed_the_batch_budget() {
    let directory = TempDir::new().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(directory.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("queue schema");
    let engine = Arc::new(ScriptedEmbedder::new(EMBED_MODEL_DEFAULT));
    let ledgers = Arc::new(RecordingLedgers::default());
    let pipeline = pipeline_with(Arc::clone(&queue), Arc::clone(&engine), ledgers);

    let submission = submission(
        "adversarial.md",
        EvidenceShape::Document,
        MediaKind::Markdown,
    );
    let outcome = submit(&pipeline, &log, &clock, &submission, corpus(40).as_bytes());
    let ran = drain(&pipeline, &queue, &log, &clock, 64);
    assert!(
        ran.iter().all(|(_, ok)| *ok),
        "every stage attempt succeeded: {ran:?}"
    );

    let calls = engine.calls();
    assert!(!calls.is_empty(), "the corpus produced batches");
    for shape in &calls {
        assert!(
            shape.count == 1 || shape.padded_tokens() <= EMBED_BATCH_PADDED_TOKENS_MAX,
            "a batch of {} padded to {} tokens costs {}, past the {EMBED_BATCH_PADDED_TOKENS_MAX} \
             budget — ONNX Runtime's arena would keep that peak for the process's life",
            shape.count,
            shape.longest_tokens,
            shape.padded_tokens()
        );
    }

    // Every chunk got exactly one vector, and the item says so.
    let evidence = read_evidence(&log, outcome.evidence_id())
        .expect("read evidence")
        .expect("the item exists");
    assert_eq!(evidence.status, EvidenceStatus::Embedded, "{evidence:?}");
    let chunks = list_chunks(&log, outcome.evidence_id(), None, 4_096).expect("chunks");
    let vectors = list_chunk_embeddings(&log, outcome.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
        .expect("vectors");
    assert_eq!(
        vectors.len(),
        chunks.len(),
        "a chunk missing from the index is invisible to search and to any check that counts \
         only what is present"
    );
    for vector in &vectors {
        assert_eq!(vector.dim, EMBED_DIM_DEFAULT);
        assert_eq!(vector.model_id, EMBED_MODEL_DEFAULT);
    }
}

#[test]
fn identical_content_across_two_sources_is_embedded_once() {
    let directory = TempDir::new().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(directory.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("queue schema");
    let engine = Arc::new(ScriptedEmbedder::new(EMBED_MODEL_DEFAULT));
    let ledgers = Arc::new(RecordingLedgers::default());
    let pipeline = pipeline_with(
        Arc::clone(&queue),
        Arc::clone(&engine),
        Arc::clone(&ledgers),
    );

    let content = corpus(6);
    let first = submit(
        &pipeline,
        &log,
        &clock,
        &submission(
            "attachment-a.md",
            EvidenceShape::Document,
            MediaKind::Markdown,
        ),
        content.as_bytes(),
    );
    drain(&pipeline, &queue, &log, &clock, 64);
    let after_first = engine.embedded_texts();
    let cost_after_first = ledgers.embed_calls();
    assert!(!after_first.is_empty(), "the first item was embedded");
    assert!(
        !cost_after_first.is_empty(),
        "the first item cost something"
    );

    // The same bytes arriving from a second source: two Evidence items with
    // their own provenance, one set of vectors.
    let second = submit(
        &pipeline,
        &log,
        &clock,
        &submission(
            "attachment-b.md",
            EvidenceShape::Document,
            MediaKind::Markdown,
        ),
        content.as_bytes(),
    );
    assert_ne!(first.evidence_id(), second.evidence_id());
    drain(&pipeline, &queue, &log, &clock, 64);

    assert_eq!(
        ledgers.embed_calls(),
        cost_after_first,
        "the duplicate attachment cost no model call — the ledger is what the criterion \
         names, and a 'dedup' that still spent the call would be a comment rather than a \
         property (F6)"
    );
    assert_eq!(
        engine.embedded_texts(),
        after_first,
        "and the engine was never asked to recompute it"
    );

    let first_vectors =
        list_chunk_embeddings(&log, first.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
            .expect("first vectors");
    let second_vectors =
        list_chunk_embeddings(&log, second.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
            .expect("second vectors");
    assert_eq!(first_vectors.len(), second_vectors.len());
    assert!(!second_vectors.is_empty());
    for vector in &second_vectors {
        // The second item's rows point into the first item's CAS blob.
        assert!(
            first_vectors
                .iter()
                .any(|other| other.vectors_blob == vector.vectors_blob
                    && other.row == vector.row
                    && other.content_hash == vector.content_hash),
            "a duplicate chunk reuses the vector that already exists"
        );
    }
}

#[test]
fn re_embedding_to_a_second_model_is_additive_and_names_both() {
    let directory = TempDir::new().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(directory.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("queue schema");

    let first_engine = Arc::new(ScriptedEmbedder::new(EMBED_MODEL_DEFAULT));
    let pipeline = pipeline_with(
        Arc::clone(&queue),
        Arc::clone(&first_engine),
        Arc::new(RecordingLedgers::default()),
    );
    let item = submit(
        &pipeline,
        &log,
        &clock,
        &submission("notes.md", EvidenceShape::Document, MediaKind::Markdown),
        corpus(8).as_bytes(),
    );
    drain(&pipeline, &queue, &log, &clock, 64);
    let original = list_chunk_embeddings(&log, item.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
        .expect("original vectors");
    assert!(!original.is_empty());

    // A second model over the same chunks, as a reprocess from EMBED.
    const SECOND_MODEL: &str = "bge-base-en-v1.5";
    let second_engine = Arc::new(ScriptedEmbedder::new(SECOND_MODEL));
    let second_pipeline = pipeline_with(
        Arc::clone(&queue),
        Arc::clone(&second_engine),
        Arc::new(RecordingLedgers::default()),
    );
    let plan = second_pipeline
        .reprocess(
            &log,
            PROJECT,
            &clock,
            &pos_log::Actor::User(common::USER),
            pos_ingest::ReprocessRequest {
                evidence_id: Some(item.evidence_id()),
                from_stage: IngestStage::Embed,
                item_count_max: pos_ingest::REPROCESS_ITEM_COUNT_MAX,
            },
            "re-embed to a second model",
        )
        .expect("reprocess from embed");
    assert_eq!(plan.requeued.len(), 1, "{plan:?}");
    drain(&second_pipeline, &queue, &log, &clock, 64);

    // Both coexist, keyed by model, and each query gets only its own.
    let models = list_embedding_models(&log).expect("models");
    let names: Vec<&str> = models.iter().map(|row| row.model_id.as_str()).collect();
    assert!(names.contains(&EMBED_MODEL_DEFAULT), "{names:?}");
    assert!(names.contains(&SECOND_MODEL), "{names:?}");

    let second = list_chunk_embeddings(&log, item.evidence_id(), SECOND_MODEL, 0, 4_096)
        .expect("second-model vectors");
    assert_eq!(
        second.len(),
        original.len(),
        "the second model covered the same chunks"
    );
    for vector in &second {
        assert_eq!(vector.model_id, SECOND_MODEL);
        assert!(
            original
                .iter()
                .all(|old| old.vectors_blob != vector.vectors_blob),
            "a second model's vectors are its own bytes, not the first model's"
        );
    }
    // The superseded set is addressable by name — which is what makes the old
    // vectors collectable rather than something to guess at.
    let superseded = models
        .iter()
        .find(|row| row.model_id == EMBED_MODEL_DEFAULT)
        .expect("the first model is still listed");
    assert_eq!(superseded.vector_count, original.len() as u64);
}

#[test]
fn an_interrupted_pass_recomputes_nothing_that_finished() {
    let directory = TempDir::new().expect("tempdir");
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let log = open_project(directory.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("queue schema");

    // Refuse from the third batch, so two are committed and the pass fails.
    let interrupted = Arc::new(ScriptedEmbedder::refusing_from(EMBED_MODEL_DEFAULT, 2));
    let pipeline = pipeline_with(
        Arc::clone(&queue),
        Arc::clone(&interrupted),
        Arc::new(RecordingLedgers::default()),
    );
    let item = submit(
        &pipeline,
        &log,
        &clock,
        &submission("long.md", EvidenceShape::Document, MediaKind::Markdown),
        corpus(30).as_bytes(),
    );
    let ran = drain(&pipeline, &queue, &log, &clock, 8);
    assert!(
        ran.iter()
            .any(|(stage, ok)| *stage == IngestStage::Embed && !*ok),
        "the interrupted pass failed at EMBED: {ran:?}"
    );
    let committed = list_chunk_embeddings(&log, item.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
        .expect("committed vectors");
    assert!(
        !committed.is_empty(),
        "the batches before the refusal are durable"
    );

    // Past the retry backoff, so the queue hands the job out again.
    clock.advance_ms(60_000);

    // A fresh engine resumes. Whatever it is asked to embed, it must not be
    // anything already committed.
    let resumed = Arc::new(ScriptedEmbedder::new(EMBED_MODEL_DEFAULT));
    let resumed_pipeline = pipeline_with(
        Arc::clone(&queue),
        Arc::clone(&resumed),
        Arc::new(RecordingLedgers::default()),
    );
    drain(&resumed_pipeline, &queue, &log, &clock, 64);
    assert!(resumed.call_count() > 0, "the resumed pass did work");

    let after = list_chunk_embeddings(&log, item.evidence_id(), EMBED_MODEL_DEFAULT, 0, 4_096)
        .expect("vectors after resume");
    let chunks = list_chunks(&log, item.evidence_id(), None, 4_096).expect("chunks");
    assert_eq!(after.len(), chunks.len(), "the resumed pass finished");

    // Committed vectors are facts: the resumed engine never saw their content.
    let recomputed = resumed.embedded_texts();
    for vector in &committed {
        assert!(
            find_embedding_by_content(&log, vector.content_hash, EMBED_MODEL_DEFAULT, 0)
                .expect("lookup")
                .is_some()
        );
    }
    let committed_hashes: Vec<[u8; 32]> = committed.iter().map(|row| row.content_hash).collect();
    for text in &recomputed {
        let mut hasher = pos_ingest::ContentHasher::new();
        hasher.update(text.as_bytes());
        assert!(
            !committed_hashes.contains(&hasher.finalize()),
            "the resumed pass recomputed a vector that was already a fact"
        );
    }
}
