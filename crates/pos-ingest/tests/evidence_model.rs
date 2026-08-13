//! m1-s02 oracles: the evidence model and structure-aware chunking.
//!
//! The three acceptance criteria here are the ones that make citations
//! survivable: chunk ids do not churn, every locator resolves to a position a
//! human can be shown, and identical content across sources is one unit of
//! work rather than two.

#![forbid(unsafe_code)]

mod common;

use common::{
    PROJECT, document_text, drain, mixed_section_document, open_project, pipeline, queue,
    submission, submit, transcript_text,
};
use pos_domain::{
    EvidenceShape, EvidenceStatus, IngestStage, Locator, MediaKind, count_chunks_by_content,
    list_chunks, read_evidence,
};
use pos_foundation::{EvidenceId, ManualWallClock};
use pos_ingest::{
    ChunkStage, IngestPipeline, NormalizeStage, PipelineConfig, ReprocessRequest, StageRegistry,
};
use pos_log::{Actor, ProjectLog};
use std::sync::Arc;
use tempfile::TempDir;

/// Re-chunking with the same strategy is a no-op: zero id churn (m1-s02 AC).
/// Anything else and every stored citation is one reprocess away from
/// dangling.
#[test]
fn re_chunking_with_the_same_strategy_churns_no_chunk_ids() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(Arc::clone(&queue));

    let item = submission(
        "interview-1",
        EvidenceShape::Transcript,
        MediaKind::PlainText,
    );
    let evidence_id = submit(
        &pipeline,
        &log,
        &clock,
        &item,
        transcript_text(120).as_bytes(),
    )
    .evidence_id();
    drain(&pipeline, &queue, &log, &clock, 16);
    let before = chunk_ids(&log, evidence_id, None);
    assert!(before.len() > 5, "the fixture must produce several chunks");

    // A second pass over the same normalized text with the same parameters.
    pipeline
        .reprocess(
            &log,
            PROJECT,
            &clock,
            &Actor::User(common::USER),
            ReprocessRequest::one(evidence_id, IngestStage::Chunk),
            "same-strategy re-chunk",
        )
        .expect("reprocess");
    drain(&pipeline, &queue, &log, &clock, 16);

    let record = read_evidence(&log, evidence_id)
        .expect("read evidence")
        .expect("evidence exists");
    assert_eq!(record.pass, 1, "the reprocess must have advanced the pass");
    // The status is not rewound while the new pass runs: the item genuinely
    // still holds its previous chunks, and saying otherwise would describe a
    // project that does not exist.
    assert_eq!(record.status, EvidenceStatus::Chunked);
    let after = chunk_ids(&log, evidence_id, None);
    assert_eq!(
        after, before,
        "re-chunking with the same strategy churned chunk ids"
    );
}

/// With a *changed* window size, chunks whose span is unchanged keep their
/// ids (m1-s02 AC). This is what makes "re-chunk with a better strategy in
/// 2027" a reprocess instead of a citation apocalypse.
#[test]
fn a_changed_window_size_preserves_the_ids_of_unchanged_chunks() {
    let text = document_text(40);
    let wide = chunk_facts_with_budget(&text, 512 * 1024);
    let narrow = chunk_facts_with_budget(&text, 64 * 1024);
    // The buffer budget must not change what is produced — only how much is
    // resident while producing it. Same spans, same ids.
    assert_eq!(wide, narrow, "the buffer budget changed the chunk output");

    // Now the real perturbation: the same corpus at two window targets.
    let mixed = mixed_section_document(24);
    let at_300 = chunk_facts_at_target(&mixed, 300);
    let at_800 = chunk_facts_at_target(&mixed, 800);
    assert_ne!(
        at_300.len(),
        at_800.len(),
        "the window size must actually have changed the chunking"
    );

    // Sections smaller than either window chunk identically at both targets;
    // those are the "unchanged content" the milestone protects, and their ids
    // must survive. Larger sections split differently and legitimately get
    // new ids — a citation into one of them still resolves, because the row
    // it points at keeps its own pass.
    let unchanged: Vec<&(String, u64, u64)> = at_300
        .iter()
        .filter(|(_, start, end)| {
            at_800
                .iter()
                .any(|(_, other_start, other_end)| other_start == start && other_end == end)
        })
        .collect();
    assert!(
        unchanged.len() >= 12,
        "the fixture must contain sections that chunk identically at both \
         targets; found {}",
        unchanged.len()
    );
    for (id, start, end) in unchanged {
        assert!(
            at_800.contains(&(id.clone(), *start, *end)),
            "a chunk covering the unchanged span {start}..{end} changed id"
        );
    }
}

/// Every chunk's locator resolves to a renderable position (m1-s02 AC): the
/// bound the m1-s12 citation sweep inherits. There is no "unknown" locator to
/// fall back to, which is why this is checkable at all.
#[test]
fn every_chunk_locator_resolves_to_a_renderable_position() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(Arc::clone(&queue));

    let corpora = [
        (
            "doc",
            EvidenceShape::Document,
            MediaKind::Markdown,
            document_text(40),
        ),
        (
            "thread",
            EvidenceShape::Thread,
            MediaKind::Structured,
            transcript_text(150),
        ),
        (
            "table",
            EvidenceShape::Table,
            MediaKind::Csv,
            "a,b,c\n1,2,3\n4,5,6\n7,8,9\n".repeat(400),
        ),
    ];
    let mut checked = 0_usize;
    for (name, shape, media, text) in corpora {
        let item = submission(name, shape, media);
        let evidence_id = submit(&pipeline, &log, &clock, &item, text.as_bytes()).evidence_id();
        drain(&pipeline, &queue, &log, &clock, 16);
        let record = read_evidence(&log, evidence_id)
            .expect("read evidence")
            .expect("evidence exists");
        assert_eq!(record.status, EvidenceStatus::Chunked, "{name} must chunk");
        let text_byte_size = record.text_byte_size.unwrap_or(0);
        let chunks = list_chunks(&log, evidence_id, None, 500).expect("chunks");
        assert!(!chunks.is_empty(), "{name} produced no chunks");
        for chunk in chunks {
            // The span must be inside the text it claims to index...
            assert!(chunk.byte_start < chunk.byte_end, "{name}: empty span");
            assert!(
                chunk.byte_end <= text_byte_size,
                "{name}: span {}..{} runs past {text_byte_size} bytes of text",
                chunk.byte_start,
                chunk.byte_end
            );
            // ...and the locator must be a position, not a placeholder.
            match chunk.locator {
                Locator::LineRange { start, end } => {
                    assert!(start >= 1 && end >= start, "{name}: bad line range");
                }
                Locator::MessageRange { start, end } => {
                    assert!(end >= start, "{name}: bad message range");
                }
                Locator::TimeRange { start_ms, end_ms } => {
                    assert!(end_ms >= start_ms, "{name}: bad time range");
                }
            }
            checked += 1;
        }
    }
    assert!(checked > 30, "the sweep must cover a real number of chunks");
}

/// The same bytes arriving from two different sources are two Evidence items
/// with their own provenance — and exactly one unit of embedding work
/// (m1-s02 AC / F6). The CAS deduplicates the blobs; the content hash
/// deduplicates the chunks.
#[test]
fn identical_content_from_two_sources_is_one_unit_of_embedding_work() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(Arc::clone(&queue));
    let text = document_text(8);

    let mut first = submission("attachment", EvidenceShape::Document, MediaKind::Markdown);
    first.source_scope = "inbox-a".to_owned();
    let mut second = submission("attachment", EvidenceShape::Document, MediaKind::Markdown);
    second.source_scope = "inbox-b".to_owned();

    let first_id = submit(&pipeline, &log, &clock, &first, text.as_bytes()).evidence_id();
    let second_id = submit(&pipeline, &log, &clock, &second, text.as_bytes()).evidence_id();
    assert_ne!(
        first_id, second_id,
        "two sources deliver two Evidence items — provenance is not deduplicated"
    );
    drain(&pipeline, &queue, &log, &clock, 32);

    let first_record = read_evidence(&log, first_id)
        .expect("read")
        .expect("exists");
    let second_record = read_evidence(&log, second_id)
        .expect("read")
        .expect("exists");
    // One blob, one normalized text, one segment index — the CAS did that.
    assert_eq!(first_record.content_blob, second_record.content_blob);
    assert_eq!(first_record.text_blob, second_record.text_blob);
    assert_eq!(first_record.segments_blob, second_record.segments_blob);

    // ...and one distinct content hash per chunk across both items, which is
    // what EMBED will group by (m1-s04): twice the rows, once the work.
    let (distinct_content, chunk_rows) = count_chunks_by_content(&log).expect("chunk counts");
    assert_eq!(chunk_rows, first_record.chunk_count * 2);
    assert_eq!(
        distinct_content, first_record.chunk_count,
        "identical content across two sources must embed once"
    );
}

/// Re-submitting the same file from the same source is a visible no-op — the
/// "already ingested" notice the upload path renders (m1-s07 consumes this).
#[test]
fn re_submitting_the_same_item_is_a_visible_no_op() {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = pipeline(Arc::clone(&queue));

    let item = submission("", EvidenceShape::Document, MediaKind::Markdown);
    let text = document_text(3);
    let first = submit(&pipeline, &log, &clock, &item, text.as_bytes());
    assert!(!first.is_duplicate());
    let second = submit(&pipeline, &log, &clock, &item, text.as_bytes());
    assert!(second.is_duplicate());
    assert_eq!(first.evidence_id(), second.evidence_id());

    // The second submission scheduled nothing: exactly one pipeline runs.
    let ran = drain(&pipeline, &queue, &log, &clock, 32);
    assert_eq!(
        ran,
        vec![(IngestStage::Normalize, true), (IngestStage::Chunk, true)]
    );
}

fn chunk_ids(log: &ProjectLog, evidence_id: EvidenceId, pass: Option<u32>) -> Vec<String> {
    list_chunks(log, evidence_id, pass, 500)
        .expect("chunks")
        .into_iter()
        .map(|chunk| chunk.chunk_id.to_hex())
        .collect()
}

/// Runs one corpus through a pipeline with the given buffer budget and
/// returns `(chunk id, span start, span end)` per chunk.
fn chunk_facts_with_budget(text: &str, buffer_bytes_max: usize) -> Vec<(String, u64, u64)> {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let mut config = PipelineConfig::for_device(common::DEVICE);
    config.buffer_bytes_max = buffer_bytes_max;
    let pipeline = IngestPipeline::new(
        config,
        Arc::clone(&queue),
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(ChunkStage::new())),
    );
    let item = submission("doc", EvidenceShape::Document, MediaKind::Markdown);
    let evidence_id = submit(&pipeline, &log, &clock, &item, text.as_bytes()).evidence_id();
    drain(&pipeline, &queue, &log, &clock, 16);
    facts(&log, evidence_id)
}

/// Runs one corpus through a pipeline whose CHUNK stage uses `target_tokens`.
fn chunk_facts_at_target(text: &str, target_tokens: u32) -> Vec<(String, u64, u64)> {
    let clock = ManualWallClock::starting_at(1_700_000_000_000);
    let root = TempDir::new().expect("temp project");
    let log = open_project(root.path(), &clock);
    let queue = queue();
    queue.ensure_schema(&log).expect("lease schema");
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(common::DEVICE),
        Arc::clone(&queue),
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(ChunkStage::with_target_tokens(target_tokens))),
    );
    let item = submission("doc", EvidenceShape::Document, MediaKind::Markdown);
    let evidence_id = submit(&pipeline, &log, &clock, &item, text.as_bytes()).evidence_id();
    drain(&pipeline, &queue, &log, &clock, 16);
    facts(&log, evidence_id)
}

fn facts(log: &ProjectLog, evidence_id: EvidenceId) -> Vec<(String, u64, u64)> {
    list_chunks(log, evidence_id, None, 500)
        .expect("chunks")
        .into_iter()
        .map(|chunk| (chunk.chunk_id.to_hex(), chunk.byte_start, chunk.byte_end))
        .collect()
}
