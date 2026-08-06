//! Replay drivers (m0-s03): full rebuild, snapshot + tail restore, the open
//! fast path, and the non-destructive verify that re-derives projections in
//! the `temp` schema. Replay of the same log is byte-identical by contract —
//! the §18 determinism gate rides on the property suites over this module.

use super::{
    ProjectionRegistry, SchemaTarget, apply_event_to_schema, create_table_sql, digest_table,
    drop_table_sql, snapshot, sqlite, write_applied_seq, write_schema_digest,
};
use crate::{LogError, decode_event_row, read_event_rows_after};
use pos_foundation::EventSeq;
use pos_store::ProjectStore;
use pos_store::rusqlite::{Transaction, params};

/// Events decoded per replay batch: bounds memory during 1M-event rebuilds
/// while amortizing statement overhead.
const REPLAY_BATCH_EVENT_COUNT: usize = 4_096;

/// Snapshot parameters threaded from `LogConfig` (the stated m0-s03 knobs).
#[derive(Clone, Copy, Debug)]
pub(crate) struct SnapshotPolicy {
    pub cadence_events: u64,
    pub keep_count: u64,
}

/// Per-table verify outcome plus overall state consistency.
#[derive(Debug)]
pub struct VerifyReport {
    pub events_replayed: u64,
    pub applied_seq: u64,
    pub head_seq: u64,
    pub tables: Vec<TableVerify>,
}

#[derive(Debug)]
pub struct TableVerify {
    pub name: &'static str,
    pub matches: bool,
}

impl VerifyReport {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.applied_seq == self.head_seq && self.tables.iter().all(|table| table.matches)
    }

    /// The names verify must surface (m0-s05 AC: "names the mismatch").
    #[must_use]
    pub fn mismatched_tables(&self) -> Vec<&'static str> {
        self.tables
            .iter()
            .filter(|table| !table.matches)
            .map(|table| table.name)
            .collect()
    }
}

/// Brings projections current at open: fast no-op when state tracks the
/// head, tail replay when behind, full rebuild when the registered schema
/// changed, typed error when durable state claims the impossible.
pub(crate) fn open_projections(
    store: &ProjectStore,
    registry: &ProjectionRegistry,
    policy: SnapshotPolicy,
) -> Result<(), LogError> {
    store
        .db()
        .write_transaction("open projections", |transaction| {
            ensure_tables(transaction, registry, SchemaTarget::Main)?;
            let head = read_head(transaction)?;
            let stored_digest =
                super::read_schema_digest(transaction).map_err(sqlite("read schema digest"))?;
            if stored_digest != Some(registry.schema_digest()) {
                return rebuild_in_transaction(transaction, registry, policy, head);
            }
            let applied =
                super::read_applied_seq(transaction).map_err(sqlite("read applied seq"))?;
            if applied > head {
                return Err(LogError::StateAhead { applied, head });
            }
            if applied < head {
                replay_range(
                    transaction,
                    registry,
                    applied,
                    head,
                    SchemaTarget::Main,
                    Some(policy),
                )?;
            }
            Ok(())
        })
}

/// Full rebuild from the log alone (recovery path; determinism oracle).
pub(crate) fn rebuild_full(
    store: &ProjectStore,
    registry: &ProjectionRegistry,
    policy: SnapshotPolicy,
) -> Result<(), LogError> {
    store
        .db()
        .write_transaction("rebuild projections", |transaction| {
            let head = read_head(transaction)?;
            rebuild_in_transaction(transaction, registry, policy, head)
        })
}

/// The bounded §18 open path: restore the newest usable snapshot, replay
/// only the tail; full replay when no snapshot fits.
pub(crate) fn restore_snapshot_and_tail(
    store: &ProjectStore,
    registry: &ProjectionRegistry,
) -> Result<(), LogError> {
    store
        .db()
        .write_transaction("restore snapshot + tail", |transaction| {
            ensure_tables(transaction, registry, SchemaTarget::Main)?;
            let head = read_head(transaction)?;
            recreate_tables(transaction, registry, SchemaTarget::Main)?;
            write_applied_seq(transaction, EventSeq::ZERO).map_err(sqlite("reset applied seq"))?;
            let restored = snapshot::restore_latest_snapshot(transaction, registry, head)?;
            let from = restored.map_or(0, EventSeq::value);
            // Snapshots are not rewritten on this path: the ones on disk are
            // still valid, and the point of restore is bounded work.
            replay_range(transaction, registry, from, head, SchemaTarget::Main, None)?;
            write_schema_digest(transaction, registry.schema_digest())
                .map_err(sqlite("write schema digest"))?;
            Ok(())
        })
}

/// Non-destructive verify: re-derive every projection into `temp` from the
/// full log and compare per-table digests against `main` (m0-s05 `pos
/// verify`). Stored projections are never written.
pub(crate) fn verify_against_replay(
    store: &ProjectStore,
    registry: &ProjectionRegistry,
) -> Result<VerifyReport, LogError> {
    store
        .db()
        .write_transaction("verify projections", |transaction| {
            ensure_tables(transaction, registry, SchemaTarget::Main)?;
            let head = read_head(transaction)?;
            let applied =
                super::read_applied_seq(transaction).map_err(sqlite("read applied seq"))?;
            recreate_tables(transaction, registry, SchemaTarget::Temp)?;
            let events_replayed =
                replay_range(transaction, registry, 0, head, SchemaTarget::Temp, None)?;
            let mut tables = Vec::with_capacity(registry.projections().len());
            for projection in registry.projections() {
                let table = projection.table();
                let stored = digest_table(transaction, table, SchemaTarget::Main)
                    .map_err(sqlite("digest stored projection"))?;
                let derived = digest_table(transaction, table, SchemaTarget::Temp)
                    .map_err(sqlite("digest derived projection"))?;
                tables.push(TableVerify {
                    name: table.name,
                    matches: stored == derived,
                });
            }
            drop_tables(transaction, registry, SchemaTarget::Temp)?;
            Ok(VerifyReport {
                events_replayed,
                applied_seq: applied,
                head_seq: head,
                tables,
            })
        })
}

fn rebuild_in_transaction(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    policy: SnapshotPolicy,
    head: u64,
) -> Result<(), LogError> {
    recreate_tables(transaction, registry, SchemaTarget::Main)?;
    write_applied_seq(transaction, EventSeq::ZERO).map_err(sqlite("reset applied seq"))?;
    // Snapshots taken under a different projection schema can never be
    // restored again; keeping them would only hide the real recovery cost.
    transaction
        .prepare_cached("DELETE FROM log_snapshots WHERE schema_digest != ?1")
        .and_then(|mut statement| statement.execute(params![registry.schema_digest().to_vec()]))
        .map_err(sqlite("prune stale snapshots"))?;
    replay_range(
        transaction,
        registry,
        0,
        head,
        SchemaTarget::Main,
        Some(policy),
    )?;
    write_schema_digest(transaction, registry.schema_digest())
        .map_err(sqlite("write schema digest"))?;
    Ok(())
}

/// Replays `(from, head]` in bounded batches. In `Main` with a policy, the
/// paired bookkeeping of a live append is reproduced: `applied_seq` advances
/// per event and snapshots land on cadence boundaries.
fn replay_range(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    from_exclusive: u64,
    head: u64,
    schema: SchemaTarget,
    policy: Option<SnapshotPolicy>,
) -> Result<u64, LogError> {
    let mut cursor = from_exclusive;
    let mut replayed = 0_u64;
    while cursor < head {
        let rows = read_event_rows_after(transaction, cursor, REPLAY_BATCH_EVENT_COUNT)
            .map_err(sqlite("read replay batch"))?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let event = decode_event_row(row)?;
            debug_assert_eq!(
                event.seq.value(),
                cursor + 1,
                "replay observed a seq gap — contiguity is assigned at append"
            );
            apply_event_to_schema(transaction, registry, &event, schema)?;
            cursor = event.seq.value();
            replayed += 1;
            if schema == SchemaTarget::Main {
                write_applied_seq(transaction, event.seq).map_err(sqlite("advance applied seq"))?;
                if let Some(policy) = policy
                    && cursor.is_multiple_of(policy.cadence_events)
                {
                    snapshot::write_snapshot(
                        transaction,
                        registry,
                        event.seq,
                        event.ts_ms,
                        policy.keep_count,
                    )?;
                }
            }
        }
    }
    Ok(replayed)
}

fn ensure_tables(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    schema: SchemaTarget,
) -> Result<(), LogError> {
    for projection in registry.projections() {
        transaction
            .execute_batch(&create_table_sql(projection.table(), schema))
            .map_err(sqlite("create projection table"))?;
    }
    Ok(())
}

fn recreate_tables(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    schema: SchemaTarget,
) -> Result<(), LogError> {
    drop_tables(transaction, registry, schema)?;
    ensure_tables(transaction, registry, schema)
}

fn drop_tables(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    schema: SchemaTarget,
) -> Result<(), LogError> {
    for projection in registry.projections() {
        transaction
            .execute_batch(&drop_table_sql(projection.table(), schema))
            .map_err(sqlite("drop projection table"))?;
    }
    Ok(())
}

fn read_head(transaction: &Transaction<'_>) -> Result<u64, LogError> {
    let head: i64 = transaction
        .query_row("SELECT COALESCE(MAX(seq), 0) FROM events", [], |row| {
            row.get(0)
        })
        .map_err(sqlite("read log head"))?;
    Ok(u64::try_from(head).unwrap_or(0))
}
