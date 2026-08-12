//! # pos-log
//!
//! The L1 substrate: append-only event log, projections applied in the same transaction, snapshots + tail replay, time-travel reads reserved. Append is the only write.
//!
//! Filled by m0-s03. Charter: master plan §19.
//!
//! ## The envelope (frozen at m0-s03 — additive evolution only, §3.2)
//!
//! `{seq, device, lamport, ts_ms, actor, kind, body: versioned CBOR, refs}` —
//! sync-ready fields (`device`, `lamport`) exist from day one so M5
//! replication is a feature, not a migration. `ts_ms` is informational only;
//! ordering is `seq`/`lamport`.
//!
//! ## Layering
//!
//! This crate owns the event tables and the *mechanics* of projections: the
//! only code that renders `proj_*` writes lives in `src/apply/` (grep-enforced
//! from m0-s02). Domain meaning — event kinds, bodies, projection shapes —
//! lives above in `pos-domain`, which implements [`Projection`] as pure
//! `event → typed row writes`. That split keeps apply deterministic by
//! construction: domain apply code cannot reach a clock, an RNG, or the
//! database; it can only return data.
//!
//! ## Append invariant inventory (STYLE: state machines document invariants)
//!
//! - `seq` is contiguous per project and assigned only here, under the single
//!   writer; head equals `log_state.applied_seq` outside a transaction.
//! - Per-device `lamport` is strictly monotonic in seq order.
//! - The event insert, every projection write, `log_state.applied_seq`, and
//!   any due snapshot commit in ONE SQLite transaction (paired assertions
//!   before append and after replay).
//! - Old events are eternal: nothing in this crate mutates or deletes an
//!   `events` row; there is no API to do so.

#![forbid(unsafe_code)]

pub mod apply;

pub use apply::{
    ApplyError, ColumnDef, ColumnKind, IndexDef, Projection, ProjectionRegistry, RowWrite,
    SqlValue, TableDef, VerifyReport,
};

use pos_foundation::{DeviceId, EventSeq, JobId, RunId, UserId, WallClock};
use pos_store::rusqlite::{OptionalExtension, Transaction, params};
use pos_store::{FaultPoint, ProjectStore, StoreError};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Default snapshot cadence (m0-s03: a stated parameter): every 10k events a
/// projection snapshot bounds tail replay, sized for the §18 project-open
/// gate (< 500 ms at 1M events ⇒ tail ≤ 10k events plus one restore).
pub const SNAPSHOT_CADENCE_EVENTS_DEFAULT: u64 = 10_000;

/// Snapshots kept per project. Older snapshots are pruned in the same
/// transaction that writes a new one: 1M events at default cadence would
/// otherwise retain ~100 full projection copies (L8 — bound everything).
const SNAPSHOT_KEEP_COUNT: u64 = 2;

/// Events decoded per batch during replay/export scans, bounding memory
/// while amortizing statement overhead.
const REPLAY_BATCH_EVENT_COUNT: usize = 4_096;

/// Upper bound on a kind tag; a tag is a name, not a payload.
const KIND_TAG_LEN_MAX: usize = 64;
/// Upper bound on refs per event; an event touching more entities than this
/// is modeling a batch, not a fact.
const EVENT_REFS_COUNT_MAX: usize = 64;
/// Upper bound on an encoded body. Bodies carry facts; bulk content belongs
/// in the CAS with a ref (L8).
const EVENT_BODY_LEN_MAX: usize = 1_048_576;

/// Who caused an event (§7.1): always known, never defaulted.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Actor {
    User(UserId),
    Agent(RunId),
    System(JobId),
}

impl Actor {
    const KIND_USER: i64 = 0;
    const KIND_AGENT: i64 = 1;
    const KIND_SYSTEM: i64 = 2;

    fn storage_kind(self) -> i64 {
        match self {
            Self::User(_) => Self::KIND_USER,
            Self::Agent(_) => Self::KIND_AGENT,
            Self::System(_) => Self::KIND_SYSTEM,
        }
    }

    fn storage_id(self) -> [u8; 16] {
        match self {
            Self::User(id) => id.into_bytes(),
            Self::Agent(id) => id.into_bytes(),
            Self::System(id) => id.into_bytes(),
        }
    }

    fn from_storage(kind: i64, id: [u8; 16]) -> Result<Self, String> {
        match kind {
            Self::KIND_USER => Ok(Self::User(UserId::from_bytes(id))),
            Self::KIND_AGENT => Ok(Self::Agent(RunId::from_bytes(id))),
            Self::KIND_SYSTEM => Ok(Self::System(JobId::from_bytes(id))),
            other => Err(format!("unknown actor kind {other}")),
        }
    }
}

/// A typed event-kind tag: past-tense fact names (`ProjectCreated`), ASCII,
/// bounded. The typed enum lives in `pos-domain`; this is its wire form.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct KindTag(String);

impl KindTag {
    pub fn new(tag: impl Into<String>) -> Result<Self, LogError> {
        let tag = tag.into();
        let valid = !tag.is_empty()
            && tag.len() <= KIND_TAG_LEN_MAX
            && tag.bytes().all(|byte| byte.is_ascii_alphanumeric());
        if valid {
            Ok(Self(tag))
        } else {
            Err(LogError::InvalidKindTag { tag })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for KindTag {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// An L2 link this event creates or touches; the why-chain is built from
/// these rows. Serialized as CBOR in the `refs` column.
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub struct EntityRef {
    /// Fixed domain noun in lowercase (`project`, `run`, `job`, `account`).
    pub entity: String,
    pub id: [u8; 16],
}

/// One appended event — the frozen §7.1 envelope.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Event {
    pub seq: EventSeq,
    pub device: DeviceId,
    pub lamport: u64,
    /// Wall clock at append, informational ONLY (ordering is seq/lamport).
    pub ts_ms: u64,
    pub actor: Actor,
    pub kind: KindTag,
    /// Versioned CBOR payload; decoding belongs to `pos-domain`.
    pub body: Vec<u8>,
    pub refs: Vec<EntityRef>,
}

/// What a caller submits; `seq`, `lamport`, and `ts_ms` are assigned at
/// append, under the single writer.
#[derive(Clone, Debug)]
pub struct AppendRequest {
    pub device: DeviceId,
    pub actor: Actor,
    pub kind: KindTag,
    pub body: Vec<u8>,
    pub refs: Vec<EntityRef>,
}

/// Stated log parameters (m0-s03).
#[derive(Clone, Copy, Debug)]
pub struct LogConfig {
    pub snapshot_cadence_events: u64,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            snapshot_cadence_events: SNAPSHOT_CADENCE_EVENTS_DEFAULT,
        }
    }
}

/// Typed failures of the log layer.
#[derive(Debug)]
pub enum LogError {
    Store(StoreError),
    InvalidKindTag {
        tag: String,
    },
    OversizeBody {
        len: usize,
    },
    OversizeRefs {
        count: usize,
    },
    DecodeEvent {
        seq: u64,
        reason: String,
    },
    Apply {
        kind: String,
        seq: u64,
        source: ApplyError,
    },
    /// Durable state claims more applied events than the log holds — a
    /// hand-mutated or mixed-up database, surfaced typed for `pos verify`.
    StateAhead {
        applied: u64,
        head: u64,
    },
    /// An optimistic caller prepared work against an older log head. The
    /// caller must reload durable state; silently appending would let two
    /// control/step transitions both claim the same boundary.
    HeadChanged {
        expected: EventSeq,
        actual: EventSeq,
    },
    /// Reserved seam (F4): time-travel reads arrive in M3; the signature
    /// exists now so nothing beneath it changes later.
    NotYetSupported {
        feature: &'static str,
        arrives: &'static str,
    },
}

impl fmt::Display for LogError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Store(source) => write!(formatter, "{source}"),
            Self::InvalidKindTag { tag } => write!(
                formatter,
                "invalid event kind tag {tag:?}: ASCII alphanumeric, 1..={KIND_TAG_LEN_MAX} bytes"
            ),
            Self::OversizeBody { len } => write!(
                formatter,
                "event body of {len} bytes exceeds the {EVENT_BODY_LEN_MAX}-byte bound; \
                 bulk content belongs in the blob store with a ref"
            ),
            Self::OversizeRefs { count } => write!(
                formatter,
                "{count} refs exceed the {EVENT_REFS_COUNT_MAX}-ref bound"
            ),
            Self::DecodeEvent { seq, reason } => {
                write!(formatter, "event {seq} cannot be decoded: {reason}")
            }
            Self::Apply { kind, seq, source } => {
                write!(
                    formatter,
                    "projection apply failed at event {seq} ({kind}): {source}"
                )
            }
            Self::StateAhead { applied, head } => write!(
                formatter,
                "projections claim seq {applied} but the log ends at {head}; \
                 the database has been mutated outside the log"
            ),
            Self::HeadChanged { expected, actual } => write!(
                formatter,
                "log head changed while preparing a conditional append: expected {expected}, actual {actual}"
            ),
            Self::NotYetSupported { feature, arrives } => {
                write!(formatter, "{feature} is reserved and arrives in {arrives}")
            }
        }
    }
}

impl std::error::Error for LogError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Store(source) => Some(source),
            Self::Apply { source, .. } => Some(source),
            _ => None,
        }
    }
}

impl From<StoreError> for LogError {
    fn from(source: StoreError) -> Self {
        Self::Store(source)
    }
}

/// The open project log: the one write surface of a project (L1).
pub struct ProjectLog {
    store: ProjectStore,
    registry: ProjectionRegistry,
    config: LogConfig,
}

impl ProjectLog {
    /// Opens the log over an open store, ensures schema, and brings
    /// projections current: fast no-op when `applied_seq == head`, snapshot +
    /// tail after an out-of-band copy, full rebuild when the registered
    /// projection set changed (schema digests differ).
    pub fn open(
        store: ProjectStore,
        registry: ProjectionRegistry,
        config: LogConfig,
    ) -> Result<Self, LogError> {
        assert!(
            config.snapshot_cadence_events > 0,
            "cadence is a positive parameter"
        );
        let log = Self {
            store,
            registry,
            config,
        };
        log.store
            .db()
            .with_writer("ensure log schema", |connection| {
                connection.execute_batch(LOG_SCHEMA_SQL)
            })?;
        apply::open_projections(&log.store, &log.registry, log.snapshot_policy())?;
        log.assert_consistent()?;
        Ok(log)
    }

    fn snapshot_policy(&self) -> apply::SnapshotPolicy {
        apply::SnapshotPolicy {
            cadence_events: self.config.snapshot_cadence_events,
            keep_count: SNAPSHOT_KEEP_COUNT,
        }
    }

    #[must_use]
    pub fn store(&self) -> &ProjectStore {
        &self.store
    }

    #[must_use]
    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    /// Appends one event. Prefer [`Self::append_batch`] for bulk work — one
    /// transaction per event is the per-item pattern STYLE forbids at scale.
    pub fn append(
        &self,
        request: AppendRequest,
        clock: &dyn WallClock,
    ) -> Result<EventSeq, LogError> {
        let seqs = self.append_batch(&[request], clock)?;
        Ok(*seqs
            .last()
            .expect("append_batch returns one seq per request")) // INVARIANT: a one-request batch yields exactly one seq.
    }

    /// Appends one event only when the durable head still equals
    /// `expected_head`. The comparison and append share the same IMMEDIATE
    /// transaction, so concurrent Run controls cannot race a prepared step.
    pub fn append_at_head(
        &self,
        expected_head: EventSeq,
        request: AppendRequest,
        clock: &dyn WallClock,
    ) -> Result<EventSeq, LogError> {
        let seqs = self.append_batch_at_head(expected_head, &[request], clock)?;
        Ok(*seqs
            .last()
            .expect("conditional one-event append returns exactly one seq")) // INVARIANT: the request slice above contains exactly one item.
    }

    /// Appends a batch in ONE transaction: events, projections, state, and
    /// any due snapshot commit atomically or not at all.
    pub fn append_batch(
        &self,
        requests: &[AppendRequest],
        clock: &dyn WallClock,
    ) -> Result<Vec<EventSeq>, LogError> {
        self.append_batch_inner(None, requests, clock)
    }

    /// Appends a batch only if the head still equals `expected_head`.
    pub fn append_batch_at_head(
        &self,
        expected_head: EventSeq,
        requests: &[AppendRequest],
        clock: &dyn WallClock,
    ) -> Result<Vec<EventSeq>, LogError> {
        self.append_batch_inner(Some(expected_head), requests, clock)
    }

    fn append_batch_inner(
        &self,
        expected_head: Option<EventSeq>,
        requests: &[AppendRequest],
        clock: &dyn WallClock,
    ) -> Result<Vec<EventSeq>, LogError> {
        if requests.is_empty() {
            return Ok(Vec::new());
        }
        for request in requests {
            validate_request(request)?;
        }
        let ts_ms = clock.now_ms();
        let cadence = self.config.snapshot_cadence_events;
        let registry = &self.registry;
        let store = &self.store;
        self.store.db().write_transaction(
            "append events",
            |transaction| -> Result<Vec<EventSeq>, LogError> {
                let mut head = read_head(transaction)?;
                let applied = apply::read_applied_seq(transaction)
                    .map_err(apply::sqlite("read applied seq"))?;
                // Paired assertion (pre-append): projections are exactly
                // current before the log grows; open/replay asserts the twin.
                assert_eq!(
                    head, applied,
                    "append found projections out of step with the log head"
                );
                if let Some(expected_head) = expected_head {
                    let actual = EventSeq::new(head);
                    if actual != expected_head {
                        return Err(LogError::HeadChanged {
                            expected: expected_head,
                            actual,
                        });
                    }
                }
                let mut seqs = Vec::with_capacity(requests.len());
                for request in requests {
                    let seq = EventSeq::new(head).next();
                    let lamport = next_device_lamport(transaction, request.device)?;
                    insert_event(transaction, seq, lamport, ts_ms, request)?;
                    store.trip_fault(FaultPoint::LogEventInserted)?;
                    let event = Event {
                        seq,
                        device: request.device,
                        lamport,
                        ts_ms,
                        actor: request.actor,
                        kind: request.kind.clone(),
                        body: request.body.clone(),
                        refs: request.refs.clone(),
                    };
                    apply::apply_event(transaction, registry, &event)?;
                    head = seq.value();
                    if head.is_multiple_of(cadence) {
                        apply::write_snapshot(
                            transaction,
                            registry,
                            seq,
                            ts_ms,
                            SNAPSHOT_KEEP_COUNT,
                        )?;
                        store.trip_fault(FaultPoint::LogSnapshotWritten)?;
                    }
                    seqs.push(seq);
                }
                store.trip_fault(FaultPoint::LogApplied)?;
                Ok(seqs)
            },
        )
    }

    /// The last assigned seq; `EventSeq::ZERO` for an empty log.
    pub fn head(&self) -> Result<EventSeq, LogError> {
        let head = self.store.db().with_reader("read log head", |connection| {
            connection.query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
        })?;
        Ok(EventSeq::new(u64::try_from(head).unwrap_or(0)))
    }

    pub fn event_count(&self) -> Result<u64, LogError> {
        let count = self.store.db().with_reader("count events", |connection| {
            connection.query_row("SELECT count(*) FROM events", [], |row| {
                row.get::<_, i64>(0)
            })
        })?;
        Ok(u64::try_from(count).unwrap_or(0))
    }

    /// Streams every event in seq order through `consumer` in bounded
    /// batches (export, verify, domain scans). Stops early on `Err`.
    pub fn for_each_event(
        &self,
        mut consumer: impl FnMut(&Event) -> Result<(), LogError>,
    ) -> Result<(), LogError> {
        let mut cursor = 0_u64;
        loop {
            let batch = self.read_batch_after(cursor)?;
            let Some(last) = batch.last() else {
                return Ok(());
            };
            cursor = last.seq.value();
            for event in &batch {
                consumer(event)?;
            }
            if batch.len() < REPLAY_BATCH_EVENT_COUNT {
                return Ok(());
            }
        }
    }

    fn read_batch_after(&self, cursor: u64) -> Result<Vec<Event>, LogError> {
        let rows = self
            .store
            .db()
            .with_reader("read event batch", |connection| {
                read_event_rows_after(connection, cursor, REPLAY_BATCH_EVENT_COUNT)
            })?;
        decode_event_rows(rows)
    }

    /// Full projection rebuild from the log alone — the recovery path and
    /// the determinism oracle's second run.
    pub fn rebuild_projections(&self) -> Result<(), LogError> {
        apply::rebuild_full(&self.store, &self.registry, self.snapshot_policy())?;
        self.assert_consistent()
    }

    /// The bounded open path (§18 project-open gate): restore the nearest
    /// usable snapshot, replay only the tail. Falls back to a full replay
    /// when no snapshot is usable.
    pub fn restore_from_snapshot_and_tail(&self) -> Result<(), LogError> {
        apply::restore_snapshot_and_tail(&self.store, &self.registry)?;
        self.assert_consistent()
    }

    /// Non-destructively re-derives every projection from the log into the
    /// `temp` schema and compares digests table by table (m0-s05 `pos
    /// verify`). The stored tables are never written.
    pub fn verify_projections(&self) -> Result<VerifyReport, LogError> {
        apply::verify_against_replay(&self.store, &self.registry)
    }

    /// Canonical bytes of every projection table (determinism oracles).
    pub fn dump_projections(&self) -> Result<Vec<u8>, LogError> {
        apply::dump_projections(&self.store, &self.registry)
    }

    /// Snapshot bookkeeping for `pos inspect`.
    pub fn snapshot_state(&self) -> Result<SnapshotState, LogError> {
        let (count, latest) = self
            .store
            .db()
            .with_reader("read snapshot state", |connection| {
                let count: i64 =
                    connection
                        .query_row("SELECT count(*) FROM log_snapshots", [], |row| row.get(0))?;
                let latest: Option<i64> = connection
                    .query_row("SELECT MAX(snapshot_seq) FROM log_snapshots", [], |row| {
                        row.get(0)
                    })
                    .optional()?
                    .flatten();
                Ok((count, latest))
            })?;
        Ok(SnapshotState {
            snapshot_count: u64::try_from(count).unwrap_or(0),
            latest_snapshot_seq: latest.and_then(|seq| u64::try_from(seq).ok()),
            cadence_events: self.config.snapshot_cadence_events,
        })
    }

    /// Time-travel reads (F4) — reserved seam until M3.
    pub fn as_of(&self, _seq: EventSeq) -> Result<AsOfProjections, LogError> {
        Err(LogError::NotYetSupported {
            feature: "as_of projection reads (F4)",
            arrives: "M3",
        })
    }

    /// Paired assertion (post-open/replay): durable state is internally
    /// consistent — projections track the head exactly.
    fn assert_consistent(&self) -> Result<(), LogError> {
        // Both values must come from one SQLite statement. Two autocommit
        // reads can straddle another process's atomic append and falsely pair
        // the old event head with the new applied seq during recovery.
        let (head, applied_bytes) =
            self.store
                .db()
                .with_reader("read log/projection consistency", |connection| {
                    connection.query_row(
                        "SELECT COALESCE((SELECT MAX(seq) FROM events), 0),
                            (SELECT value FROM log_state WHERE key = 'applied_seq')",
                        [],
                        |row| Ok((row.get::<_, i64>(0)?, row.get::<_, Option<Vec<u8>>>(1)?)),
                    )
                })?;
        let head = u64::try_from(head).unwrap_or(0);
        let applied = applied_bytes
            .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
            .map_or(0, u64::from_be_bytes);
        if applied > head {
            return Err(LogError::StateAhead { applied, head });
        }
        assert_eq!(
            applied, head,
            "open/replay must leave projections exactly at the log head"
        );
        Ok(())
    }
}

/// Opaque handle reserved for M3 time-travel (F4). No constructor exists on
/// purpose: the seam is the signature, not the feature.
#[derive(Debug)]
pub struct AsOfProjections {
    _reserved: (),
}

/// What `pos inspect` reports about snapshots.
#[derive(Clone, Copy, Debug)]
pub struct SnapshotState {
    pub snapshot_count: u64,
    pub latest_snapshot_seq: Option<u64>,
    pub cadence_events: u64,
}

const LOG_SCHEMA_SQL: &str = "
CREATE TABLE IF NOT EXISTS events (
  seq        INTEGER PRIMARY KEY,
  device     BLOB    NOT NULL,
  lamport    INTEGER NOT NULL,
  ts_ms      INTEGER NOT NULL,
  actor_kind INTEGER NOT NULL,
  actor_id   BLOB    NOT NULL,
  kind       TEXT    NOT NULL,
  body       BLOB    NOT NULL,
  refs       BLOB    NOT NULL
);
CREATE TABLE IF NOT EXISTS log_devices (
  device  BLOB PRIMARY KEY,
  lamport INTEGER NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS log_state (
  key   TEXT PRIMARY KEY,
  value BLOB NOT NULL
) WITHOUT ROWID;
CREATE TABLE IF NOT EXISTS log_snapshots (
  snapshot_seq  INTEGER PRIMARY KEY,
  schema_digest BLOB    NOT NULL,
  body          BLOB    NOT NULL,
  created_ts_ms INTEGER NOT NULL
);
";

fn validate_request(request: &AppendRequest) -> Result<(), LogError> {
    if request.body.len() > EVENT_BODY_LEN_MAX {
        return Err(LogError::OversizeBody {
            len: request.body.len(),
        });
    }
    if request.refs.len() > EVENT_REFS_COUNT_MAX {
        return Err(LogError::OversizeRefs {
            count: request.refs.len(),
        });
    }
    // The tag was validated at construction; re-assert the invariant at the
    // boundary rather than trusting every caller's copy.
    debug_assert!(!request.kind.as_str().is_empty());
    Ok(())
}

fn read_head(transaction: &Transaction<'_>) -> Result<u64, StoreError> {
    let head: i64 = transaction
        .query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| {
            row.get(0)
        })
        .map_err(|source| StoreError::Sqlite {
            context: "read log head",
            source,
        })?;
    Ok(u64::try_from(head).unwrap_or(0))
}

fn next_device_lamport(transaction: &Transaction<'_>, device: DeviceId) -> Result<u64, StoreError> {
    let device_bytes = device.into_bytes().to_vec();
    let current: Option<i64> = transaction
        .query_row(
            "SELECT lamport FROM log_devices WHERE device = ?1",
            params![device_bytes],
            |row| row.get(0),
        )
        .optional()
        .map_err(|source| StoreError::Sqlite {
            context: "read device lamport",
            source,
        })?;
    let next = u64::try_from(current.unwrap_or(0))
        .unwrap_or(0)
        .saturating_add(1);
    let next_storage = i64::try_from(next).unwrap_or(i64::MAX);
    transaction
        .execute(
            "INSERT INTO log_devices (device, lamport) VALUES (?1, ?2)
             ON CONFLICT(device) DO UPDATE SET lamport = excluded.lamport",
            params![device.into_bytes().to_vec(), next_storage],
        )
        .map_err(|source| StoreError::Sqlite {
            context: "advance device lamport",
            source,
        })?;
    Ok(next)
}

fn insert_event(
    transaction: &Transaction<'_>,
    seq: EventSeq,
    lamport: u64,
    ts_ms: u64,
    request: &AppendRequest,
) -> Result<(), StoreError> {
    let mut refs_cbor = Vec::new();
    ciborium::into_writer(&request.refs, &mut refs_cbor)
        .expect("CBOR encoding of refs into a Vec cannot fail"); // INVARIANT: EntityRef contains only owned serde-friendly values and the writer is a Vec.
    let mut statement = transaction
        .prepare_cached(
            "INSERT INTO events (seq, device, lamport, ts_ms, actor_kind, actor_id, kind, body, refs)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)",
        )
        .map_err(|source| StoreError::Sqlite {
            context: "prepare event insert",
            source,
        })?;
    statement
        .execute(params![
            i64::try_from(seq.value()).unwrap_or(i64::MAX),
            request.device.into_bytes().to_vec(),
            i64::try_from(lamport).unwrap_or(i64::MAX),
            i64::try_from(ts_ms).unwrap_or(i64::MAX),
            request.actor.storage_kind(),
            request.actor.storage_id().to_vec(),
            request.kind.as_str(),
            request.body,
            refs_cbor,
        ])
        .map_err(|source| StoreError::Sqlite {
            context: "insert event",
            source,
        })?;
    Ok(())
}

type EventRow = (
    i64,
    Vec<u8>,
    i64,
    i64,
    i64,
    Vec<u8>,
    String,
    Vec<u8>,
    Vec<u8>,
);

pub(crate) fn read_event_rows_after(
    connection: &pos_store::rusqlite::Connection,
    cursor: u64,
    limit: usize,
) -> Result<Vec<EventRow>, pos_store::rusqlite::Error> {
    let mut statement = connection.prepare_cached(
        "SELECT seq, device, lamport, ts_ms, actor_kind, actor_id, kind, body, refs
         FROM events WHERE seq > ?1 ORDER BY seq LIMIT ?2",
    )?;
    let rows = statement.query_map(
        params![
            i64::try_from(cursor).unwrap_or(i64::MAX),
            i64::try_from(limit).unwrap_or(i64::MAX)
        ],
        |row| {
            Ok((
                row.get(0)?,
                row.get(1)?,
                row.get(2)?,
                row.get(3)?,
                row.get(4)?,
                row.get(5)?,
                row.get(6)?,
                row.get(7)?,
                row.get(8)?,
            ))
        },
    )?;
    rows.collect()
}

fn decode_event_rows(rows: Vec<EventRow>) -> Result<Vec<Event>, LogError> {
    rows.into_iter().map(decode_event_row).collect()
}

pub(crate) fn decode_event_row(row: EventRow) -> Result<Event, LogError> {
    let (seq, device, lamport, ts_ms, actor_kind, actor_id, kind, body, refs_cbor) = row;
    let seq_u64 = u64::try_from(seq).map_err(|_| LogError::DecodeEvent {
        seq: 0,
        reason: format!("negative seq {seq}"),
    })?;
    let decode = |reason: String| LogError::DecodeEvent {
        seq: seq_u64,
        reason,
    };
    let device: [u8; 16] = device
        .try_into()
        .map_err(|bytes: Vec<u8>| decode(format!("device id has {} bytes", bytes.len())))?;
    let actor_id: [u8; 16] = actor_id
        .try_into()
        .map_err(|bytes: Vec<u8>| decode(format!("actor id has {} bytes", bytes.len())))?;
    let actor = Actor::from_storage(actor_kind, actor_id).map_err(decode)?;
    let refs: Vec<EntityRef> =
        ciborium::from_reader(refs_cbor.as_slice()).map_err(|error| decode(error.to_string()))?;
    Ok(Event {
        seq: EventSeq::new(seq_u64),
        device: DeviceId::from_bytes(device),
        lamport: u64::try_from(lamport)
            .map_err(|_| decode(format!("negative lamport {lamport}")))?,
        ts_ms: u64::try_from(ts_ms).unwrap_or(0),
        actor,
        kind: KindTag::new(kind)?,
        body,
        refs,
    })
}
