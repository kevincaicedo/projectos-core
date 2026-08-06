//! Projection snapshots (m0-s03): a periodic, typed copy of every projection
//! table stored in `log_snapshots`, so opening a project replays a bounded
//! tail instead of the whole log (§18 project-open gate). Snapshot restore +
//! tail replay must equal full replay byte-for-byte — property-tested.

use super::{ProjectionRegistry, SqlValue, TableDef, column_list, placeholders, sqlite};
use crate::LogError;
use pos_foundation::EventSeq;
use pos_store::rusqlite::types::ValueRef;
use pos_store::rusqlite::{OptionalExtension, Transaction, params};
use serde::{Deserialize, Serialize};

/// The stored snapshot body (CBOR): every projection table's rows in
/// primary-key order, with the same typed values apply writes.
#[derive(Deserialize, Serialize)]
struct SnapshotBody {
    tables: Vec<TableSnapshot>,
}

#[derive(Deserialize, Serialize)]
struct TableSnapshot {
    name: String,
    rows: Vec<Vec<SqlValue>>,
}

/// Serializes current projections and stores them under `snapshot_seq`,
/// pruning history beyond `keep_count` in the same transaction (L8: at the
/// default cadence an unpruned 1M-event project would hoard ~100 copies).
pub(crate) fn write_snapshot(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    snapshot_seq: EventSeq,
    created_ts_ms: u64,
    keep_count: u64,
) -> Result<(), LogError> {
    let mut tables = Vec::with_capacity(registry.projections().len());
    for projection in registry.projections() {
        tables.push(read_table_snapshot(transaction, projection.table())?);
    }
    let mut body_cbor = Vec::new();
    ciborium::into_writer(&SnapshotBody { tables }, &mut body_cbor)
        .expect("CBOR encoding of typed rows into a Vec cannot fail"); // INVARIANT: SnapshotBody contains only serde-friendly owned values.
    transaction
        .prepare_cached(
            "INSERT INTO log_snapshots (snapshot_seq, schema_digest, body, created_ts_ms)
             VALUES (?1, ?2, ?3, ?4)
             ON CONFLICT(snapshot_seq) DO UPDATE SET
               schema_digest = excluded.schema_digest,
               body = excluded.body,
               created_ts_ms = excluded.created_ts_ms",
        )
        .and_then(|mut statement| {
            statement.execute(params![
                i64::try_from(snapshot_seq.value()).unwrap_or(i64::MAX),
                registry.schema_digest().to_vec(),
                body_cbor,
                i64::try_from(created_ts_ms).unwrap_or(i64::MAX),
            ])
        })
        .map_err(sqlite("write snapshot"))?;
    transaction
        .prepare_cached(
            "DELETE FROM log_snapshots WHERE snapshot_seq NOT IN
             (SELECT snapshot_seq FROM log_snapshots ORDER BY snapshot_seq DESC LIMIT ?1)",
        )
        .and_then(|mut statement| {
            statement.execute(params![i64::try_from(keep_count).unwrap_or(i64::MAX)])
        })
        .map_err(sqlite("prune snapshots"))?;
    Ok(())
}

fn read_table_snapshot(
    transaction: &Transaction<'_>,
    table: &TableDef,
) -> Result<TableSnapshot, LogError> {
    let sql = format!(
        "SELECT * FROM main.{table} ORDER BY {keys}",
        table = table.name,
        keys = column_list(table.key_columns),
    );
    let mut statement = transaction
        .prepare(&sql)
        .map_err(sqlite("read snapshot rows"))?;
    let column_count = statement.column_count();
    let mut sql_rows = statement.query([]).map_err(sqlite("read snapshot rows"))?;
    let mut rows = Vec::new();
    while let Some(row) = sql_rows.next().map_err(sqlite("read snapshot rows"))? {
        let mut values = Vec::with_capacity(column_count);
        for index in 0..column_count {
            let value = match row.get_ref(index).map_err(sqlite("read snapshot value"))? {
                ValueRef::Null => SqlValue::Null,
                ValueRef::Integer(inner) => SqlValue::Integer(inner),
                ValueRef::Text(text) => SqlValue::Text(String::from_utf8_lossy(text).into_owned()),
                ValueRef::Blob(blob) => SqlValue::Blob(blob.to_vec()),
                ValueRef::Real(_) => {
                    // SqlValue excludes floats by design; a REAL here means
                    // the table was mutated outside the log.
                    return Err(LogError::Apply {
                        kind: "snapshot".to_owned(),
                        seq: 0,
                        source: super::ApplyError {
                            reason: format!(
                                "{}: REAL value found; projections never store floats",
                                table.name
                            ),
                        },
                    });
                }
            };
            values.push(value);
        }
        rows.push(values);
    }
    Ok(TableSnapshot {
        name: table.name.to_owned(),
        rows,
    })
}

/// Restores the newest snapshot whose schema digest matches the registry.
/// Returns the restored seq, or `None` when no snapshot is usable — the
/// caller then replays the full log (the documented recovery fallback).
pub(crate) fn restore_latest_snapshot(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    head: u64,
) -> Result<Option<EventSeq>, LogError> {
    let row: Option<(i64, Vec<u8>)> = transaction
        .query_row(
            "SELECT snapshot_seq, body FROM log_snapshots
             WHERE schema_digest = ?1 AND snapshot_seq <= ?2
             ORDER BY snapshot_seq DESC LIMIT 1",
            params![
                registry.schema_digest().to_vec(),
                i64::try_from(head).unwrap_or(i64::MAX)
            ],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )
        .optional()
        .map_err(sqlite("read latest snapshot"))?;
    let Some((snapshot_seq, body_cbor)) = row else {
        return Ok(None);
    };
    let Ok(body) = ciborium::from_reader::<SnapshotBody, _>(body_cbor.as_slice()) else {
        // Undecodable snapshot: unusable, not fatal — full replay recovers.
        return Ok(None);
    };
    let registry_tables: Vec<&TableDef> = registry
        .projections()
        .iter()
        .map(|projection| projection.table())
        .collect();
    if body.tables.len() != registry_tables.len()
        || body
            .tables
            .iter()
            .zip(&registry_tables)
            .any(|(snapshot, table)| snapshot.name != table.name)
    {
        return Ok(None);
    }
    for (snapshot, table) in body.tables.iter().zip(&registry_tables) {
        restore_table(transaction, table, snapshot)?;
    }
    let seq = EventSeq::new(u64::try_from(snapshot_seq).unwrap_or(0));
    super::write_applied_seq(transaction, seq).map_err(sqlite("set applied seq"))?;
    Ok(Some(seq))
}

fn restore_table(
    transaction: &Transaction<'_>,
    table: &TableDef,
    snapshot: &TableSnapshot,
) -> Result<(), LogError> {
    let column_count = table.key_columns.len() + table.value_columns.len();
    transaction
        .execute_batch(&format!("DELETE FROM main.{}", table.name))
        .map_err(sqlite("clear projection table"))?;
    let insert_sql = format!(
        "INSERT INTO main.{table} ({columns}) VALUES ({binds})",
        table = table.name,
        columns = format_all_columns(table),
        binds = placeholders(column_count),
    );
    let mut statement = transaction
        .prepare_cached(&insert_sql)
        .map_err(sqlite("prepare snapshot restore"))?;
    for row in &snapshot.rows {
        if row.len() != column_count {
            return Err(LogError::Apply {
                kind: "snapshot".to_owned(),
                seq: 0,
                source: super::ApplyError {
                    reason: format!(
                        "{}: snapshot row has {} values, table declares {column_count}",
                        table.name,
                        row.len()
                    ),
                },
            });
        }
        statement
            .execute(pos_store::rusqlite::params_from_iter(super::bind_values(
                row.iter(),
            )))
            .map_err(sqlite("restore snapshot row"))?;
    }
    Ok(())
}

fn format_all_columns(table: &TableDef) -> String {
    let keys = column_list(table.key_columns);
    if table.value_columns.is_empty() {
        keys
    } else {
        format!("{keys}, {}", column_list(table.value_columns))
    }
}
