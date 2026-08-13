//! Typed reads over the durable ingestion projections (m1-s01, m1-s02).
//!
//! Same shape as [`crate::job_state`]: the projections are rebuildable views
//! of the log, and these functions are the only vocabulary `pos-ingest`, the
//! API, and the evidence browser need in order to ask what exists. Every read
//! is bounded and deterministically ordered — a listing whose order depends on
//! SQLite's row layout would make two shells disagree about the same corpus.

use crate::ingest::{
    CanaryLevel, ChunkKind, EvidenceShape, EvidenceStatus, ExternalRef, IngestStage, Locator,
    MediaKind,
};
use pos_foundation::{ChunkId, EventSeq, EvidenceId, JobId, SourceId};
use pos_log::ProjectLog;
use pos_store::StoreError;
use pos_store::rusqlite::{OptionalExtension, Row};
use std::fmt;

/// Rows one evidence or chunk listing returns (L8). A partner corpus holds
/// hundreds of thousands of items; a read surface that tried to return all of
/// them would trade an honest bound for an unbounded allocation.
pub const EVIDENCE_LIST_ROW_COUNT_MAX: u32 = 500;

/// Durable state of one (evidence, stage) row.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageState {
    Running,
    Done,
    /// An attempt failed and the queue will try again.
    Retrying,
    /// In the DLQ: no attempt remains, or the handler refused permanently.
    Dead,
}

impl StageState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Running => "running",
            Self::Done => "done",
            Self::Retrying => "retrying",
            Self::Dead => "dead",
        }
    }

    #[must_use]
    pub fn parse(value: &str) -> Option<Self> {
        match value {
            "running" => Some(Self::Running),
            "done" => Some(Self::Done),
            "retrying" => Some(Self::Retrying),
            "dead" => Some(Self::Dead),
            _ => None,
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EvidenceRecord {
    pub evidence_id: EvidenceId,
    pub source_id: SourceId,
    pub source_kind: String,
    pub external: ExternalRef,
    pub media_kind: MediaKind,
    pub shape: EvidenceShape,
    pub content_blob: [u8; 32],
    pub byte_size: u64,
    pub occurred_ts_ms: u64,
    pub author: Option<String>,
    pub title: Option<String>,
    pub thread_ref: Option<String>,
    pub status: EvidenceStatus,
    pub pass: u32,
    pub text_blob: Option<[u8; 32]>,
    pub text_byte_size: Option<u64>,
    pub segments_blob: Option<[u8; 32]>,
    pub segment_count: Option<u64>,
    pub canary_level: CanaryLevel,
    pub chunk_count: u64,
    pub chunk_pass: Option<u32>,
    pub added_seq: EventSeq,
    pub updated_seq: EventSeq,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageRecord {
    pub evidence_id: EvidenceId,
    pub stage: IngestStage,
    pub pass: u32,
    pub state: StageState,
    pub attempt_index: u32,
    pub job_id: Option<JobId>,
    pub started_seq: EventSeq,
    pub settled_seq: Option<EventSeq>,
    pub wall_ms: Option<u64>,
    pub bytes_read: Option<u64>,
    pub item_count: Option<u64>,
    pub last_error_code: Option<String>,
    pub last_error_detail: Option<String>,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChunkRecord {
    pub chunk_id: ChunkId,
    pub evidence_id: EvidenceId,
    pub ordinal: u32,
    pub kind: ChunkKind,
    pub byte_start: u64,
    pub byte_end: u64,
    pub locator: Locator,
    pub content_hash: [u8; 32],
    pub token_count_estimate: u32,
    pub pass: u32,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SourceHealthRecord {
    pub source_id: SourceId,
    pub stage: IngestStage,
    pub ok_count: u64,
    pub failed_count: u64,
    pub dead_count: u64,
    pub item_count: u64,
    pub bytes_total: u64,
    pub wall_ms_total: u64,
    pub last_success_ts_ms: Option<u64>,
    pub last_failure_ts_ms: Option<u64>,
    pub last_error_code: Option<String>,
}

/// What a bounded evidence read selects. Every field narrows; `None` is "any".
#[derive(Clone, Copy, Debug, Default)]
pub struct EvidenceListFilter {
    pub source_id: Option<SourceId>,
    pub status: Option<EvidenceStatus>,
    /// Clamped to [`EVIDENCE_LIST_ROW_COUNT_MAX`] by the reader.
    pub row_count_max: Option<u32>,
}

#[derive(Debug)]
pub enum EvidenceReadError {
    Store(StoreError),
    CorruptProjection { table: &'static str, reason: String },
}

impl fmt::Display for EvidenceReadError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::CorruptProjection { table, reason } => {
                write!(formatter, "{table} is corrupt: {reason}")
            }
        }
    }
}

impl std::error::Error for EvidenceReadError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::CorruptProjection { .. } => None,
        }
    }
}

impl From<StoreError> for EvidenceReadError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

fn corrupt(table: &'static str, reason: impl Into<String>) -> EvidenceReadError {
    EvidenceReadError::CorruptProjection {
        table,
        reason: reason.into(),
    }
}

const EVIDENCE_COLUMNS: &str = "evidence_id, source_id, source_kind, external_id, external_url, \
     external_version, media_kind, content_blob, byte_size, occurred_ts_ms, author, title, \
     thread_ref, added_seq, shape, status, pass, text_blob, text_byte_size, segments_blob, \
     segment_count, canary_level, chunk_count, chunk_pass, updated_seq";

pub fn read_evidence(
    log: &ProjectLog,
    evidence_id: EvidenceId,
) -> Result<Option<EvidenceRecord>, EvidenceReadError> {
    let raw = log.store().db().with_reader("read evidence row", |db| {
        db.query_row(
            &format!("SELECT {EVIDENCE_COLUMNS} FROM proj_evidence WHERE evidence_id = ?1"),
            [evidence_id.into_bytes().to_vec()],
            evidence_raw,
        )
        .optional()
    })?;
    raw.map(EvidenceRaw::into_record).transpose()
}

/// Bounded evidence listing, newest occurrence first, ties broken by id so
/// two runs of the same query cannot disagree.
pub fn list_evidence(
    log: &ProjectLog,
    filter: EvidenceListFilter,
) -> Result<Vec<EvidenceRecord>, EvidenceReadError> {
    let limit = i64::from(
        filter
            .row_count_max
            .unwrap_or(EVIDENCE_LIST_ROW_COUNT_MAX)
            .min(EVIDENCE_LIST_ROW_COUNT_MAX),
    );
    let mut sql = format!("SELECT {EVIDENCE_COLUMNS} FROM proj_evidence WHERE 1 = 1");
    if filter.source_id.is_some() {
        sql.push_str(" AND source_id = :source_id");
    }
    if filter.status.is_some() {
        sql.push_str(" AND status = :status");
    }
    sql.push_str(" ORDER BY occurred_ts_ms DESC, evidence_id ASC LIMIT :limit");
    let raws = log.store().db().with_reader("list evidence rows", |db| {
        let mut statement = db.prepare(&sql)?;
        // Named parameters: the clause set varies, so positional indexes would
        // silently shift with the filter combination.
        if let Some(source_id) = filter.source_id {
            statement.raw_bind_parameter(
                statement
                    .parameter_index(":source_id")?
                    .unwrap_or(usize::MAX),
                source_id.into_bytes().to_vec(),
            )?;
        }
        if let Some(status) = filter.status {
            statement.raw_bind_parameter(
                statement.parameter_index(":status")?.unwrap_or(usize::MAX),
                status.as_str(),
            )?;
        }
        statement.raw_bind_parameter(
            statement.parameter_index(":limit")?.unwrap_or(usize::MAX),
            limit,
        )?;
        let mut rows = statement.raw_query();
        let mut collected = Vec::new();
        while let Some(row) = rows.next()? {
            collected.push(evidence_raw(row)?);
        }
        Ok(collected)
    })?;
    raws.into_iter().map(EvidenceRaw::into_record).collect()
}

const STAGE_COLUMNS: &str = "evidence_id, stage, pass, state, attempt_index, job_id, started_seq, \
     settled_seq, wall_ms, bytes_read, item_count, last_error_code, last_error_detail";

/// Every stage row for one evidence item, in pipeline order.
pub fn list_stages(
    log: &ProjectLog,
    evidence_id: EvidenceId,
) -> Result<Vec<StageRecord>, EvidenceReadError> {
    let raws = log.store().db().with_reader("list stage rows", |db| {
        let mut statement = db.prepare_cached(&format!(
            "SELECT {STAGE_COLUMNS} FROM proj_evidence_stages WHERE evidence_id = ?1"
        ))?;
        let rows = statement.query_map([evidence_id.into_bytes().to_vec()], stage_raw)?;
        rows.collect::<Result<Vec<_>, _>>()
    })?;
    let mut records = raws
        .into_iter()
        .map(StageRaw::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    // Pipeline order, not the storage order: a stage table read is what a
    // human looks at when an item is stuck, and §9's order is the story.
    records.sort_by_key(|record| record.stage.rank());
    Ok(records)
}

/// Chunks of one evidence item, in reading order. `pass` selects a shape;
/// `None` means the current one, which is what every consumer except the
/// citation resolver wants.
pub fn list_chunks(
    log: &ProjectLog,
    evidence_id: EvidenceId,
    pass: Option<u32>,
    row_count_max: u32,
) -> Result<Vec<ChunkRecord>, EvidenceReadError> {
    let limit = i64::from(row_count_max.min(EVIDENCE_LIST_ROW_COUNT_MAX));
    let sql = match pass {
        Some(_) => format!(
            "SELECT {CHUNK_COLUMNS} FROM proj_chunks WHERE evidence_id = ?1 AND pass = ?2 \
             ORDER BY ordinal ASC, chunk_id ASC LIMIT ?3"
        ),
        None => format!(
            "SELECT {CHUNK_COLUMNS} FROM proj_chunks WHERE evidence_id = ?1 AND pass = \
             (SELECT chunk_pass FROM proj_evidence WHERE evidence_id = ?1) \
             ORDER BY ordinal ASC, chunk_id ASC LIMIT ?2"
        ),
    };
    let raws = log.store().db().with_reader("list chunk rows", |db| {
        let mut statement = db.prepare_cached(&sql)?;
        let key = evidence_id.into_bytes().to_vec();
        let rows = match pass {
            Some(pass) => statement.query_map(
                pos_store::rusqlite::params![key, i64::from(pass), limit],
                chunk_raw,
            )?,
            None => statement.query_map(pos_store::rusqlite::params![key, limit], chunk_raw)?,
        };
        rows.collect::<Result<Vec<_>, _>>()
    })?;
    raws.into_iter().map(ChunkRaw::into_record).collect()
}

const CHUNK_COLUMNS: &str = "chunk_id, evidence_id, ordinal, kind, byte_start, byte_end, \
     locator_kind, locator_start, locator_end, content_hash, token_count_estimate, pass";

/// How many chunk rows share each content hash — the F6 dedup answer.
/// Returns `(distinct_content_count, chunk_row_count)`, so "identical content
/// across two sources embeds once" is an assertion rather than a story.
pub fn count_chunks_by_content(log: &ProjectLog) -> Result<(u64, u64), EvidenceReadError> {
    let counts = log.store().db().with_reader("count chunk content", |db| {
        db.query_row(
            "SELECT count(DISTINCT content_hash), count(*) FROM proj_chunks",
            [],
            |row| Ok((row.get::<_, i64>(0)?, row.get::<_, i64>(1)?)),
        )
    })?;
    Ok((
        u64::try_from(counts.0).unwrap_or(0),
        u64::try_from(counts.1).unwrap_or(0),
    ))
}

const SOURCE_HEALTH_COLUMNS: &str = "source_id, stage, ok_count, failed_count, dead_count, \
     item_count, bytes_total, wall_ms_total, last_success_ts_ms, last_failure_ts_ms, \
     last_error_code";

/// Per-source, per-stage health. Ordered by source then pipeline stage, which
/// is the order the settings card renders.
pub fn list_source_health(
    log: &ProjectLog,
    source_id: Option<SourceId>,
) -> Result<Vec<SourceHealthRecord>, EvidenceReadError> {
    let sql = match source_id {
        Some(_) => {
            format!("SELECT {SOURCE_HEALTH_COLUMNS} FROM proj_source_health WHERE source_id = ?1")
        }
        None => format!("SELECT {SOURCE_HEALTH_COLUMNS} FROM proj_source_health"),
    };
    let raws = log.store().db().with_reader("list source health", |db| {
        let mut statement = db.prepare_cached(&sql)?;
        let rows = match source_id {
            Some(source_id) => {
                statement.query_map([source_id.into_bytes().to_vec()], source_health_raw)?
            }
            None => statement.query_map([], source_health_raw)?,
        };
        rows.collect::<Result<Vec<_>, _>>()
    })?;
    let mut records = raws
        .into_iter()
        .map(SourceHealthRaw::into_record)
        .collect::<Result<Vec<_>, _>>()?;
    records.sort_by_key(|record| (record.source_id.into_bytes(), record.stage.rank()));
    Ok(records)
}

struct EvidenceRaw {
    evidence_id: Vec<u8>,
    source_id: Vec<u8>,
    source_kind: String,
    external_id: String,
    external_url: Option<String>,
    external_version: Option<String>,
    media_kind: String,
    content_blob: Vec<u8>,
    byte_size: i64,
    occurred_ts_ms: i64,
    author: Option<String>,
    title: Option<String>,
    thread_ref: Option<String>,
    added_seq: i64,
    shape: String,
    status: String,
    pass: i64,
    text_blob: Option<Vec<u8>>,
    text_byte_size: Option<i64>,
    segments_blob: Option<Vec<u8>>,
    segment_count: Option<i64>,
    canary_level: Option<String>,
    chunk_count: i64,
    chunk_pass: Option<i64>,
    updated_seq: i64,
}

fn evidence_raw(row: &Row<'_>) -> Result<EvidenceRaw, pos_store::rusqlite::Error> {
    Ok(EvidenceRaw {
        evidence_id: row.get(0)?,
        source_id: row.get(1)?,
        source_kind: row.get(2)?,
        external_id: row.get(3)?,
        external_url: row.get(4)?,
        external_version: row.get(5)?,
        media_kind: row.get(6)?,
        content_blob: row.get(7)?,
        byte_size: row.get(8)?,
        occurred_ts_ms: row.get(9)?,
        author: row.get(10)?,
        title: row.get(11)?,
        thread_ref: row.get(12)?,
        added_seq: row.get(13)?,
        shape: row.get(14)?,
        status: row.get(15)?,
        pass: row.get(16)?,
        text_blob: row.get(17)?,
        text_byte_size: row.get(18)?,
        segments_blob: row.get(19)?,
        segment_count: row.get(20)?,
        canary_level: row.get(21)?,
        chunk_count: row.get(22)?,
        chunk_pass: row.get(23)?,
        updated_seq: row.get(24)?,
    })
}

impl EvidenceRaw {
    fn into_record(self) -> Result<EvidenceRecord, EvidenceReadError> {
        Ok(EvidenceRecord {
            evidence_id: EvidenceId::from_bytes(id_bytes("proj_evidence", &self.evidence_id)?),
            source_id: SourceId::from_bytes(id_bytes("proj_evidence", &self.source_id)?),
            source_kind: self.source_kind,
            external: ExternalRef {
                external_id: self.external_id,
                external_url: self.external_url,
                external_version: self.external_version,
            },
            media_kind: MediaKind::parse(&self.media_kind).ok_or_else(|| {
                corrupt(
                    "proj_evidence",
                    format!("unknown media kind {:?}", self.media_kind),
                )
            })?,
            shape: EvidenceShape::parse(&self.shape).ok_or_else(|| {
                corrupt("proj_evidence", format!("unknown shape {:?}", self.shape))
            })?,
            content_blob: hash_bytes("proj_evidence", &self.content_blob)?,
            byte_size: non_negative(self.byte_size),
            occurred_ts_ms: non_negative(self.occurred_ts_ms),
            author: self.author,
            title: self.title,
            thread_ref: self.thread_ref,
            status: EvidenceStatus::parse(&self.status).ok_or_else(|| {
                corrupt("proj_evidence", format!("unknown status {:?}", self.status))
            })?,
            pass: pass_value(self.pass),
            text_blob: self
                .text_blob
                .as_deref()
                .map(|bytes| hash_bytes("proj_evidence", bytes))
                .transpose()?,
            text_byte_size: self.text_byte_size.map(non_negative),
            segments_blob: self
                .segments_blob
                .as_deref()
                .map(|bytes| hash_bytes("proj_evidence", bytes))
                .transpose()?,
            segment_count: self.segment_count.map(non_negative),
            canary_level: self
                .canary_level
                .as_deref()
                .map_or(Some(CanaryLevel::Clean), CanaryLevel::parse)
                .ok_or_else(|| corrupt("proj_evidence", "unknown canary level"))?,
            chunk_count: non_negative(self.chunk_count),
            chunk_pass: self.chunk_pass.map(pass_value),
            added_seq: EventSeq::new(non_negative(self.added_seq)),
            updated_seq: EventSeq::new(non_negative(self.updated_seq)),
        })
    }
}

struct StageRaw {
    evidence_id: Vec<u8>,
    stage: String,
    pass: i64,
    state: String,
    attempt_index: i64,
    job_id: Option<Vec<u8>>,
    started_seq: i64,
    settled_seq: Option<i64>,
    wall_ms: Option<i64>,
    bytes_read: Option<i64>,
    item_count: Option<i64>,
    last_error_code: Option<String>,
    last_error_detail: Option<String>,
}

fn stage_raw(row: &Row<'_>) -> Result<StageRaw, pos_store::rusqlite::Error> {
    Ok(StageRaw {
        evidence_id: row.get(0)?,
        stage: row.get(1)?,
        pass: row.get(2)?,
        state: row.get(3)?,
        attempt_index: row.get(4)?,
        job_id: row.get(5)?,
        started_seq: row.get(6)?,
        settled_seq: row.get(7)?,
        wall_ms: row.get(8)?,
        bytes_read: row.get(9)?,
        item_count: row.get(10)?,
        last_error_code: row.get(11)?,
        last_error_detail: row.get(12)?,
    })
}

impl StageRaw {
    fn into_record(self) -> Result<StageRecord, EvidenceReadError> {
        Ok(StageRecord {
            evidence_id: EvidenceId::from_bytes(id_bytes(
                "proj_evidence_stages",
                &self.evidence_id,
            )?),
            stage: IngestStage::parse(&self.stage).ok_or_else(|| {
                corrupt(
                    "proj_evidence_stages",
                    format!("unknown stage {:?}", self.stage),
                )
            })?,
            pass: pass_value(self.pass),
            state: StageState::parse(&self.state).ok_or_else(|| {
                corrupt(
                    "proj_evidence_stages",
                    format!("unknown stage state {:?}", self.state),
                )
            })?,
            attempt_index: pass_value(self.attempt_index),
            job_id: self
                .job_id
                .as_deref()
                .map(|bytes| id_bytes("proj_evidence_stages", bytes))
                .transpose()?
                .map(JobId::from_bytes),
            started_seq: EventSeq::new(non_negative(self.started_seq)),
            settled_seq: self.settled_seq.map(|seq| EventSeq::new(non_negative(seq))),
            wall_ms: self.wall_ms.map(non_negative),
            bytes_read: self.bytes_read.map(non_negative),
            item_count: self.item_count.map(non_negative),
            last_error_code: self.last_error_code,
            last_error_detail: self.last_error_detail,
        })
    }
}

struct ChunkRaw {
    chunk_id: Vec<u8>,
    evidence_id: Vec<u8>,
    ordinal: i64,
    kind: String,
    byte_start: i64,
    byte_end: i64,
    locator_kind: String,
    locator_start: i64,
    locator_end: i64,
    content_hash: Vec<u8>,
    token_count_estimate: i64,
    pass: i64,
}

fn chunk_raw(row: &Row<'_>) -> Result<ChunkRaw, pos_store::rusqlite::Error> {
    Ok(ChunkRaw {
        chunk_id: row.get(0)?,
        evidence_id: row.get(1)?,
        ordinal: row.get(2)?,
        kind: row.get(3)?,
        byte_start: row.get(4)?,
        byte_end: row.get(5)?,
        locator_kind: row.get(6)?,
        locator_start: row.get(7)?,
        locator_end: row.get(8)?,
        content_hash: row.get(9)?,
        token_count_estimate: row.get(10)?,
        pass: row.get(11)?,
    })
}

impl ChunkRaw {
    fn into_record(self) -> Result<ChunkRecord, EvidenceReadError> {
        Ok(ChunkRecord {
            chunk_id: ChunkId::from_bytes(id_bytes("proj_chunks", &self.chunk_id)?),
            evidence_id: EvidenceId::from_bytes(id_bytes("proj_chunks", &self.evidence_id)?),
            ordinal: pass_value(self.ordinal),
            kind: ChunkKind::parse(&self.kind).ok_or_else(|| {
                corrupt("proj_chunks", format!("unknown chunk kind {:?}", self.kind))
            })?,
            byte_start: non_negative(self.byte_start),
            byte_end: non_negative(self.byte_end),
            // A locator that does not decode is corruption, never a guessed
            // position: the m1-s12 sweep gates on 100% of these resolving.
            locator: Locator::from_columns(
                &self.locator_kind,
                non_negative(self.locator_start),
                non_negative(self.locator_end),
            )
            .ok_or_else(|| {
                corrupt(
                    "proj_chunks",
                    format!("unknown locator kind {:?}", self.locator_kind),
                )
            })?,
            content_hash: hash_bytes("proj_chunks", &self.content_hash)?,
            token_count_estimate: pass_value(self.token_count_estimate),
            pass: pass_value(self.pass),
        })
    }
}

struct SourceHealthRaw {
    source_id: Vec<u8>,
    stage: String,
    ok_count: Option<i64>,
    failed_count: Option<i64>,
    dead_count: Option<i64>,
    item_count: Option<i64>,
    bytes_total: Option<i64>,
    wall_ms_total: Option<i64>,
    last_success_ts_ms: Option<i64>,
    last_failure_ts_ms: Option<i64>,
    last_error_code: Option<String>,
}

fn source_health_raw(row: &Row<'_>) -> Result<SourceHealthRaw, pos_store::rusqlite::Error> {
    Ok(SourceHealthRaw {
        source_id: row.get(0)?,
        stage: row.get(1)?,
        ok_count: row.get(2)?,
        failed_count: row.get(3)?,
        dead_count: row.get(4)?,
        item_count: row.get(5)?,
        bytes_total: row.get(6)?,
        wall_ms_total: row.get(7)?,
        last_success_ts_ms: row.get(8)?,
        last_failure_ts_ms: row.get(9)?,
        last_error_code: row.get(10)?,
    })
}

impl SourceHealthRaw {
    fn into_record(self) -> Result<SourceHealthRecord, EvidenceReadError> {
        Ok(SourceHealthRecord {
            source_id: SourceId::from_bytes(id_bytes("proj_source_health", &self.source_id)?),
            stage: IngestStage::parse(&self.stage).ok_or_else(|| {
                corrupt(
                    "proj_source_health",
                    format!("unknown stage {:?}", self.stage),
                )
            })?,
            // A counter column is NULL until its first increment creates the
            // row; zero is the honest reading, not missing data.
            ok_count: self.ok_count.map_or(0, non_negative),
            failed_count: self.failed_count.map_or(0, non_negative),
            dead_count: self.dead_count.map_or(0, non_negative),
            item_count: self.item_count.map_or(0, non_negative),
            bytes_total: self.bytes_total.map_or(0, non_negative),
            wall_ms_total: self.wall_ms_total.map_or(0, non_negative),
            last_success_ts_ms: self.last_success_ts_ms.map(non_negative),
            last_failure_ts_ms: self.last_failure_ts_ms.map(non_negative),
            last_error_code: self.last_error_code,
        })
    }
}

fn id_bytes(table: &'static str, bytes: &[u8]) -> Result<[u8; 16], EvidenceReadError> {
    <[u8; 16]>::try_from(bytes).map_err(|_| {
        corrupt(
            table,
            format!("id column holds {} bytes, not 16", bytes.len()),
        )
    })
}

fn hash_bytes(table: &'static str, bytes: &[u8]) -> Result<[u8; 32], EvidenceReadError> {
    <[u8; 32]>::try_from(bytes).map_err(|_| {
        corrupt(
            table,
            format!("hash column holds {} bytes, not 32", bytes.len()),
        )
    })
}

/// SQLite integers are signed; every count and size these tables hold is not.
/// A negative value could only come from a write outside the apply path, and
/// clamping keeps a read total instead of inventing a panic path.
fn non_negative(value: i64) -> u64 {
    u64::try_from(value).unwrap_or(0)
}

fn pass_value(value: i64) -> u32 {
    u32::try_from(value).unwrap_or(0)
}
