//! The ingestion projections (m1-s01, m1-s02): pure `event → row writes` over
//! the vocabulary in [`crate::ingest`].
//!
//! ## Why these four tables
//!
//! - `proj_evidence` — one row per Evidence item, provenance and status.
//! - `proj_evidence_stages` — one row per (evidence, stage): the pipeline's
//!   own history, which is what makes "why is this item stuck?" a query.
//! - `proj_chunks` — the atoms citations point at, forever.
//! - `proj_source_health` — per (source, stage) counters the settings screen
//!   renders without scanning evidence.
//!
//! ## Two rules these applies encode
//!
//! **Identity is written once.** The evidence row is an `Insert`, so a second
//! `EvidenceAdded` for the same id fails the whole append transaction rather
//! than overwriting provenance. Every later write touches only *derived*
//! columns; no assignment list in this file contains a provenance column.
//! That is the structural half of "evidence is immutable" — `pos-ingest`
//! refuses first with a typed error, and this is what happens if it ever
//! forgets.
//!
//! **Re-chunking never deletes a chunk row.** A pass that changes the window
//! size re-emits unchanged chunks under the same ids (they upsert) and emits
//! new ids for changed content; the rows it no longer produces keep their
//! older `pass` and stay resolvable. A citation stored against a retired
//! chunk therefore still renders its span instead of dropping the claim —
//! the whole reason chunk ids are content-derived in the first place. Reads
//! that must see only the current shape filter on the evidence's `chunk_pass`.

use crate::events::DomainEvent;
use crate::ingest::{
    ChunkFact, EvidenceAddedBody, EvidenceChunkedBody, EvidenceReprocessRequestedBody,
    EvidenceStatus, IngestStage, IngestStageDisposition, IngestStageFailedBody,
    IngestStageFinishedBody, IngestStageOutput, IngestStageStartedBody,
};
use pos_log::{
    ApplyError, ColumnDef, ColumnKind, Event, IndexDef, Projection, RowWrite, SqlValue, TableDef,
};

/// Stage-row lifecycle. `Running` is durable here (unlike the scheduler's,
/// which is a lease overlay) because a stage attempt appends its start: the
/// pipeline's history has to survive the process that produced it.
pub(crate) const STAGE_STATE_RUNNING: &str = "running";
pub(crate) const STAGE_STATE_DONE: &str = "done";
pub(crate) const STAGE_STATE_RETRYING: &str = "retrying";
pub(crate) const STAGE_STATE_DEAD: &str = "dead";

fn decode_for(table: &TableDef, event: &Event) -> Result<Option<DomainEvent>, ApplyError> {
    DomainEvent::decode(&event.kind, &event.body).map_err(|error| ApplyError {
        reason: format!("{}: {error}", table.name),
    })
}

fn seq_value(event: &Event) -> SqlValue {
    SqlValue::Integer(i64::try_from(event.seq.value()).unwrap_or(i64::MAX))
}

fn integer_u64(value: u64) -> SqlValue {
    SqlValue::Integer(i64::try_from(value).unwrap_or(i64::MAX))
}

fn integer_u32(value: u32) -> SqlValue {
    SqlValue::Integer(i64::from(value))
}

fn id_blob(id: [u8; 16]) -> SqlValue {
    SqlValue::Blob(id.to_vec())
}

fn hash_blob(hash: [u8; 32]) -> SqlValue {
    SqlValue::Blob(hash.to_vec())
}

fn text(value: &str) -> SqlValue {
    SqlValue::Text(value.to_owned())
}

fn optional_text(value: Option<String>) -> SqlValue {
    value.map_or(SqlValue::Null, SqlValue::Text)
}

fn stage_key(evidence_id: [u8; 16], stage: IngestStage) -> Vec<SqlValue> {
    vec![id_blob(evidence_id), text(stage.as_str())]
}

/// One Evidence item with complete provenance (L2's foundation).
struct EvidenceProjection;

const EVIDENCE_TABLE: TableDef = TableDef {
    name: "proj_evidence",
    version: 1,
    key_columns: &[ColumnDef {
        name: "evidence_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        // Provenance — written by `EvidenceAdded` and never assigned again.
        ColumnDef {
            name: "source_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "source_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "external_id",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "external_url",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "external_version",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "media_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "content_blob",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "byte_size",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "occurred_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "author",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "title",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "thread_ref",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "added_seq",
            kind: ColumnKind::Integer,
        },
        // Derived — every column below is a pipeline output.
        ColumnDef {
            name: "shape",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "status",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "pass",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "text_blob",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "text_byte_size",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "segments_blob",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "segment_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "canary_level",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "chunk_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "chunk_pass",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "updated_seq",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[
        // The source health screen and the connector coverage view both page
        // evidence by source; without this they scan the whole corpus.
        IndexDef {
            name: "idx_proj_evidence_source",
            columns: &["source_id", "status"],
        },
        // The browser's default ordering (m1-s12) and every date filter.
        IndexDef {
            name: "idx_proj_evidence_occurred",
            columns: &["occurred_ts_ms"],
        },
        // "Have we already stored these exact bytes?" — the dedup answer the
        // upload path needs before it decides to re-run the pipeline (F6).
        IndexDef {
            name: "idx_proj_evidence_content",
            columns: &["content_blob"],
        },
    ],
};

impl Projection for EvidenceProjection {
    fn table(&self) -> &TableDef {
        &EVIDENCE_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "EvidenceAdded"
            | "IngestStageFinished"
            | "IngestStageFailed"
            | "EvidenceReprocessRequested" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&EVIDENCE_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::EvidenceAdded(body) => vec![added_row(event, body)],
            DomainEvent::IngestStageFinished(IngestStageFinishedBody::V1 {
                evidence_id,
                stage,
                pass,
                output,
                ..
            }) => {
                let mut assignments = vec![
                    ("status", text(EvidenceStatus::after(stage).as_str())),
                    ("pass", integer_u32(pass)),
                    ("updated_seq", seq_value(event)),
                ];
                assignments.extend(output_assignments(&output, pass));
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(evidence_id.into_bytes())],
                    assignments,
                }]
            }
            DomainEvent::IngestStageFailed(IngestStageFailedBody::V1 {
                evidence_id,
                disposition,
                ..
            }) => {
                // A retry leaves the status alone: the item is still where
                // the last successful stage left it, and saying "failed"
                // between attempts would make the browser flicker a lie.
                if disposition.is_dead() {
                    vec![RowWrite::UpdateOne {
                        key: vec![id_blob(evidence_id.into_bytes())],
                        assignments: vec![
                            ("status", text(EvidenceStatus::Failed.as_str())),
                            ("updated_seq", seq_value(event)),
                        ],
                    }]
                } else {
                    Vec::new()
                }
            }
            DomainEvent::EvidenceReprocessRequested(EvidenceReprocessRequestedBody::V1 {
                evidence_id,
                pass,
                ..
            }) => {
                // The status is deliberately *not* rewound. Until the new
                // pass completes the item genuinely still holds its previous
                // derived state — the old chunks are there and still
                // resolvable — so moving the status backwards would describe
                // a project that does not exist. The bumped pass and the
                // stage rows are what say a reprocess is in flight.
                //
                // The rewind also could not be computed here honestly: an
                // apply function cannot read the item's media kind, and "the
                // stage before CHUNK" is TRANSCRIBE in the global order but
                // NORMALIZE for every text item, which never ran it.
                vec![RowWrite::UpdateOne {
                    key: vec![id_blob(evidence_id.into_bytes())],
                    assignments: vec![
                        ("pass", integer_u32(pass)),
                        ("updated_seq", seq_value(event)),
                    ],
                }]
            }
            _ => Vec::new(),
        })
    }
}

fn output_assignments(output: &IngestStageOutput, pass: u32) -> Vec<(&'static str, SqlValue)> {
    match output {
        IngestStageOutput::Normalized {
            shape,
            text_blob,
            text_byte_size,
            segments_blob,
            segment_count,
            canary_level,
        } => vec![
            ("shape", text(shape.as_str())),
            ("text_blob", hash_blob(*text_blob)),
            ("text_byte_size", integer_u64(*text_byte_size)),
            ("segments_blob", hash_blob(*segments_blob)),
            ("segment_count", integer_u64(*segment_count)),
            ("canary_level", text(canary_level.as_str())),
        ],
        // Assignment, never an increment: a replayed chunk batch must not
        // double-count, so the stage's own total is the one truth. `chunk_pass`
        // names which pass the current chunk shape belongs to; older rows keep
        // their own pass and stay resolvable for citations.
        IngestStageOutput::Chunked { chunk_count } => vec![
            ("chunk_count", integer_u64(*chunk_count)),
            ("chunk_pass", integer_u32(pass)),
        ],
        IngestStageOutput::None => Vec::new(),
    }
}

/// `Insert`, not `Upsert`: an evidence id is derived from `(source, external
/// ref)`, so a second `EvidenceAdded` is either a re-fetch the connector
/// should have deduped or durable corruption. Failing the append makes it
/// visible instead of quietly rewriting provenance.
fn added_row(event: &Event, body: EvidenceAddedBody) -> RowWrite {
    let EvidenceAddedBody::V1 {
        evidence_id,
        source_id,
        source_kind,
        external,
        media_kind,
        shape,
        content_blob,
        byte_size,
        occurred_ts_ms,
        author,
        title,
        thread_ref,
    } = body;
    RowWrite::Insert {
        key: vec![id_blob(evidence_id.into_bytes())],
        values: vec![
            id_blob(source_id.into_bytes()),
            SqlValue::Text(source_kind),
            SqlValue::Text(external.external_id),
            optional_text(external.external_url),
            optional_text(external.external_version),
            text(media_kind.as_str()),
            hash_blob(content_blob),
            integer_u64(byte_size),
            integer_u64(occurred_ts_ms),
            optional_text(author),
            optional_text(title),
            optional_text(thread_ref),
            seq_value(event),
            text(shape.as_str()),
            text(EvidenceStatus::Raw.as_str()),
            integer_u32(0),
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Null,
            SqlValue::Integer(0),
            SqlValue::Null,
            seq_value(event),
        ],
    }
}

/// The pipeline's own history: one row per (evidence, stage).
struct EvidenceStagesProjection;

const EVIDENCE_STAGES_TABLE: TableDef = TableDef {
    name: "proj_evidence_stages",
    version: 1,
    key_columns: &[
        ColumnDef {
            name: "evidence_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "stage",
            kind: ColumnKind::Text,
        },
    ],
    value_columns: &[
        ColumnDef {
            name: "pass",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "state",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "attempt_index",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "job_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "started_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "settled_seq",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "wall_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "bytes_read",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "item_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_error_code",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "last_error_detail",
            kind: ColumnKind::Text,
        },
    ],
    indexes: &[
        // "Show me everything in the DLQ, newest first" is the source-health
        // card's headline query and must not scan the corpus.
        IndexDef {
            name: "idx_proj_evidence_stages_state",
            columns: &["state", "stage", "settled_seq"],
        },
    ],
};

impl Projection for EvidenceStagesProjection {
    fn table(&self) -> &TableDef {
        &EVIDENCE_STAGES_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "IngestStageStarted" | "IngestStageFinished" | "IngestStageFailed" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&EVIDENCE_STAGES_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::IngestStageStarted(IngestStageStartedBody::V1 {
                evidence_id,
                stage,
                pass,
                job_id,
                attempt_index,
                ..
            }) => vec![RowWrite::Upsert {
                key: stage_key(evidence_id.into_bytes(), stage),
                values: vec![
                    integer_u32(pass),
                    text(STAGE_STATE_RUNNING),
                    integer_u32(attempt_index),
                    id_blob(job_id.into_bytes()),
                    seq_value(event),
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                    SqlValue::Null,
                ],
            }],
            DomainEvent::IngestStageFinished(IngestStageFinishedBody::V1 {
                evidence_id,
                stage,
                pass,
                wall_ms,
                bytes_read,
                item_count,
                ..
            }) => vec![RowWrite::UpdateOne {
                key: stage_key(evidence_id.into_bytes(), stage),
                assignments: vec![
                    ("pass", integer_u32(pass)),
                    ("state", text(STAGE_STATE_DONE)),
                    ("settled_seq", seq_value(event)),
                    ("wall_ms", integer_u64(wall_ms)),
                    ("bytes_read", integer_u64(bytes_read)),
                    ("item_count", integer_u64(item_count)),
                ],
            }],
            DomainEvent::IngestStageFailed(IngestStageFailedBody::V1 {
                evidence_id,
                stage,
                pass,
                attempt_index,
                code,
                detail,
                disposition,
                ..
            }) => {
                let state = if disposition.is_dead() {
                    STAGE_STATE_DEAD
                } else {
                    STAGE_STATE_RETRYING
                };
                vec![RowWrite::UpdateOne {
                    key: stage_key(evidence_id.into_bytes(), stage),
                    assignments: vec![
                        ("pass", integer_u32(pass)),
                        ("state", text(state)),
                        ("attempt_index", integer_u32(attempt_index)),
                        ("settled_seq", seq_value(event)),
                        ("last_error_code", SqlValue::Text(code)),
                        ("last_error_detail", SqlValue::Text(detail)),
                    ],
                }]
            }
            _ => Vec::new(),
        })
    }
}

/// The chunks citations point at. Ids are content-derived (m1-s02), so an
/// unchanged chunk re-chunks to the same row and upserts onto itself.
struct ChunksProjection;

const CHUNKS_TABLE: TableDef = TableDef {
    name: "proj_chunks",
    version: 1,
    key_columns: &[ColumnDef {
        name: "chunk_id",
        kind: ColumnKind::Blob,
    }],
    value_columns: &[
        ColumnDef {
            name: "evidence_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "ordinal",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "byte_start",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "byte_end",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "locator_kind",
            kind: ColumnKind::Text,
        },
        ColumnDef {
            name: "locator_start",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "locator_end",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "content_hash",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "token_count_estimate",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "pass",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "created_seq",
            kind: ColumnKind::Integer,
        },
    ],
    indexes: &[
        // Reading an evidence item's chunks in order: the transcript viewer,
        // the citation resolver, and the embed batcher all do exactly this.
        IndexDef {
            name: "idx_proj_chunks_evidence",
            columns: &["evidence_id", "pass", "ordinal"],
        },
        // Identical content across sources embeds once (F6): EMBED groups by
        // this column, so it has to be an index lookup, not a corpus scan.
        IndexDef {
            name: "idx_proj_chunks_content",
            columns: &["content_hash"],
        },
    ],
};

impl Projection for ChunksProjection {
    fn table(&self) -> &TableDef {
        &CHUNKS_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        if event.kind.as_str() != "EvidenceChunked" {
            return Ok(Vec::new());
        }
        let Some(DomainEvent::EvidenceChunked(EvidenceChunkedBody::V1 {
            evidence_id,
            pass,
            chunks,
            ..
        })) = decode_for(&CHUNKS_TABLE, event)?
        else {
            return Ok(Vec::new());
        };
        Ok(chunks
            .into_iter()
            .map(|chunk| chunk_row(event, evidence_id.into_bytes(), pass, &chunk))
            .collect())
    }
}

fn chunk_row(event: &Event, evidence_id: [u8; 16], pass: u32, chunk: &ChunkFact) -> RowWrite {
    let (locator_start, locator_end) = chunk.locator.bounds();
    RowWrite::Upsert {
        key: vec![id_blob(chunk.chunk_id.into_bytes())],
        values: vec![
            id_blob(evidence_id),
            integer_u32(chunk.ordinal),
            text(chunk.kind.as_str()),
            integer_u64(chunk.byte_start),
            integer_u64(chunk.byte_end),
            text(chunk.locator.kind_str()),
            integer_u64(locator_start),
            integer_u64(locator_end),
            hash_blob(chunk.content_hash),
            integer_u32(chunk.token_count_estimate),
            integer_u32(pass),
            seq_value(event),
        ],
    }
}

/// Per-source, per-stage counters the settings screen renders directly.
///
/// The source id is denormalized out of `EvidenceAdded` and *not* re-read
/// here — an apply function may not read another table (event-sourcing
/// skill), so every stage event carries the source it belongs to. That is one
/// 16-byte field per event to keep this projection pure.
struct SourceHealthProjection;

const SOURCE_HEALTH_TABLE: TableDef = TableDef {
    name: "proj_source_health",
    version: 1,
    key_columns: &[
        ColumnDef {
            name: "source_id",
            kind: ColumnKind::Blob,
        },
        ColumnDef {
            name: "stage",
            kind: ColumnKind::Text,
        },
    ],
    value_columns: &[
        ColumnDef {
            name: "ok_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "failed_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "dead_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "item_count",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "bytes_total",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "wall_ms_total",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_success_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_failure_ts_ms",
            kind: ColumnKind::Integer,
        },
        ColumnDef {
            name: "last_error_code",
            kind: ColumnKind::Text,
        },
    ],
    indexes: &[],
};

impl Projection for SourceHealthProjection {
    fn table(&self) -> &TableDef {
        &SOURCE_HEALTH_TABLE
    }

    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError> {
        match event.kind.as_str() {
            "EvidenceAdded" | "IngestStageFinished" | "IngestStageFailed" => {}
            _ => return Ok(Vec::new()),
        }
        let Some(decoded) = decode_for(&SOURCE_HEALTH_TABLE, event)? else {
            return Ok(Vec::new());
        };
        Ok(match decoded {
            DomainEvent::EvidenceAdded(EvidenceAddedBody::V1 {
                source_id,
                byte_size,
                ..
            }) => success_writes(
                event,
                stage_key(source_id.into_bytes(), IngestStage::Raw),
                byte_size,
                1,
                0,
            ),
            DomainEvent::IngestStageFinished(IngestStageFinishedBody::V1 {
                source_id,
                stage,
                wall_ms,
                bytes_read,
                item_count,
                ..
            }) => success_writes(
                event,
                stage_key(source_id.into_bytes(), stage),
                bytes_read,
                item_count,
                wall_ms,
            ),
            DomainEvent::IngestStageFailed(IngestStageFailedBody::V1 {
                source_id,
                stage,
                code,
                disposition,
                ..
            }) => failure_writes(
                event,
                stage_key(source_id.into_bytes(), stage),
                &code,
                disposition,
            ),
            _ => Vec::new(),
        })
    }
}

/// Counters are cumulative attempt history, not a snapshot of current state:
/// "this source failed 40 times last week and is fine now" is exactly what a
/// health card has to be able to say, and the stage table cannot say it —
/// it only remembers each stage's latest outcome.
fn success_writes(
    event: &Event,
    key: Vec<SqlValue>,
    bytes: u64,
    items: u64,
    wall_ms: u64,
) -> Vec<RowWrite> {
    vec![
        RowWrite::Increment {
            key: key.clone(),
            column: "ok_count",
            delta: 1,
        },
        RowWrite::Increment {
            key: key.clone(),
            column: "bytes_total",
            delta: i64::try_from(bytes).unwrap_or(i64::MAX),
        },
        RowWrite::Increment {
            key: key.clone(),
            column: "item_count",
            delta: i64::try_from(items).unwrap_or(i64::MAX),
        },
        RowWrite::Increment {
            key: key.clone(),
            column: "wall_ms_total",
            delta: i64::try_from(wall_ms).unwrap_or(i64::MAX),
        },
        RowWrite::Update {
            key,
            assignments: vec![("last_success_ts_ms", integer_u64(event.ts_ms))],
        },
    ]
}

fn failure_writes(
    event: &Event,
    key: Vec<SqlValue>,
    code: &str,
    disposition: IngestStageDisposition,
) -> Vec<RowWrite> {
    let mut writes = vec![RowWrite::Increment {
        key: key.clone(),
        column: "failed_count",
        delta: 1,
    }];
    if disposition.is_dead() {
        writes.push(RowWrite::Increment {
            key: key.clone(),
            column: "dead_count",
            delta: 1,
        });
    }
    writes.push(RowWrite::Update {
        key,
        assignments: vec![
            ("last_failure_ts_ms", integer_u64(event.ts_ms)),
            ("last_error_code", text(code)),
        ],
    });
    writes
}

/// The ingestion half of the v0 registry, kept beside its tables so adding a
/// projection is one edit in one file.
#[must_use]
pub fn projections() -> Vec<Box<dyn Projection>> {
    vec![
        Box::new(EvidenceProjection),
        Box::new(EvidenceStagesProjection),
        Box::new(ChunksProjection),
        Box::new(SourceHealthProjection),
    ]
}
