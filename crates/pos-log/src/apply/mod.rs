//! The projection apply chokepoint (L1, m0-s02/m0-s03): the ONLY code in the
//! product allowed to render writes against `proj_*` tables — grep-enforced
//! by `check-discipline`, which pins `crates/pos-log/src/apply/`.
//!
//! Domain crates implement [`Projection`] as a pure function
//! `event → Vec<RowWrite>`: typed row mutations, no SQL, no I/O, no clock.
//! This module validates each write against the declared [`TableDef`] and
//! executes it inside the append/replay transaction. Determinism is therefore
//! structural: everything a projection can do is enumerable data.

mod replay;
mod snapshot;

pub(crate) use replay::{
    SnapshotPolicy, open_projections, rebuild_full, restore_snapshot_and_tail,
    verify_against_replay,
};
pub use replay::{TableVerify, VerifyReport};
pub(crate) use snapshot::write_snapshot;

use crate::{Event, LogError};
use pos_foundation::EventSeq;
use pos_store::StoreError;
use pos_store::blake3;
use pos_store::rusqlite::types::ValueRef;
use pos_store::rusqlite::{Connection, Transaction, params_from_iter};
use serde::{Deserialize, Serialize};
use std::fmt;

/// Projection tables a registry may hold. More than this is a design smell,
/// not a scale need — projections are per-purpose, not per-query.
const PROJECTION_COUNT_MAX: usize = 64;

/// Value shapes a projection may store. Deliberately excludes floats:
/// accumulated float state breaks byte-identical replay (event-sourcing
/// skill); money and measures live as integers (cents, micros, counts).
#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
pub enum SqlValue {
    Integer(i64),
    Text(String),
    Blob(Vec<u8>),
    Null,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ColumnKind {
    Integer,
    Text,
    Blob,
}

impl ColumnKind {
    const fn sql_type(self) -> &'static str {
        match self {
            Self::Integer => "INTEGER",
            Self::Text => "TEXT",
            Self::Blob => "BLOB",
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ColumnDef {
    pub name: &'static str,
    pub kind: ColumnKind,
}

/// A declared secondary index over a projection table. Read paths that scan a
/// projection at queue scale (m0-s14's claim query) state their index here
/// rather than issuing DDL from a feature crate — index definitions stay
/// beside the table they belong to, and the apply chokepoint stays the only
/// code that renders projection DDL.
#[derive(Clone, Copy, Debug)]
pub struct IndexDef {
    /// Globally unique in the database; by convention `idx_<table>_<purpose>`.
    pub name: &'static str,
    /// Indexed columns in order; each must be a declared key or value column.
    pub columns: &'static [&'static str],
}

/// A projection table's declared shape. The name must start with `proj_`
/// (the grep convention that makes illegal writes findable), key columns are
/// NOT NULL and form the primary key, and `version` participates in the
/// registry digest so bumping it forces a rebuild.
#[derive(Clone, Copy, Debug)]
pub struct TableDef {
    pub name: &'static str,
    pub version: u32,
    pub key_columns: &'static [ColumnDef],
    pub value_columns: &'static [ColumnDef],
    /// Secondary indexes created with the table and dropped with it. Included
    /// in the registry digest: adding one changes the read plan a story was
    /// measured against, so the rebuild that follows is the honest default.
    pub indexes: &'static [IndexDef],
}

/// A typed row mutation — everything a projection can do to its table.
#[derive(Clone, Debug)]
pub enum RowWrite {
    /// Insert a new row and fail if the key already exists. Lifecycle facts
    /// use this when a duplicate would hide durable corruption.
    Insert {
        key: Vec<SqlValue>,
        values: Vec<SqlValue>,
    },
    /// Insert or fully replace the row at `key`. `values` supplies every
    /// value column in declared order.
    Upsert {
        key: Vec<SqlValue>,
        values: Vec<SqlValue>,
    },
    /// Partial update; a missing row is a deterministic no-op.
    Update {
        key: Vec<SqlValue>,
        assignments: Vec<(&'static str, SqlValue)>,
    },
    /// Update exactly one existing row; zero rows is an invariant failure.
    UpdateOne {
        key: Vec<SqlValue>,
        assignments: Vec<(&'static str, SqlValue)>,
    },
    /// Update one existing row only while a declared value column is NULL.
    /// Durable receipts/checkpoints are single-assignment facts: a duplicate
    /// event must fail rather than overwrite the first proof.
    UpdateOneWhenNull {
        key: Vec<SqlValue>,
        guard_column: &'static str,
        assignments: Vec<(&'static str, SqlValue)>,
    },
    /// Add `delta` to an integer column, inserting the row (other value
    /// columns NULL) when absent — deterministic counters without reads.
    Increment {
        key: Vec<SqlValue>,
        column: &'static str,
        delta: i64,
    },
    /// Increment a column on exactly one existing row; never synthesizes a
    /// partial lifecycle row when its creation fact is missing.
    IncrementOne {
        key: Vec<SqlValue>,
        column: &'static str,
        delta: i64,
    },
    /// Increment several columns on exactly one row in one statement. Run
    /// usage has multiple integer dimensions but remains one atomic fact.
    IncrementManyOne {
        key: Vec<SqlValue>,
        deltas: Vec<(&'static str, i64)>,
    },
    /// Delete the row at `key`; missing rows are a deterministic no-op.
    Delete { key: Vec<SqlValue> },
}

/// A projection author's bug surfaced as data (wrong arity, unknown column).
/// Typed rather than panicking because replay must be able to name the
/// offending kind/seq instead of taking the process down mid-recovery.
#[derive(Debug)]
pub struct ApplyError {
    pub reason: String,
}

impl fmt::Display for ApplyError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.reason)
    }
}

impl std::error::Error for ApplyError {}

/// One rebuildable projection: a declared table and a pure apply function.
///
/// Purity contract (event-sourcing skill): no I/O, no clock, no randomness,
/// no hash-map iteration into output — the same event must always produce
/// the same writes, byte for byte.
pub trait Projection: Send + Sync {
    fn table(&self) -> &TableDef;
    fn apply(&self, event: &Event) -> Result<Vec<RowWrite>, ApplyError>;
}

/// The validated, ordered set of projections a log opens with.
pub struct ProjectionRegistry {
    projections: Vec<Box<dyn Projection>>,
}

impl ProjectionRegistry {
    /// Validates shape rules once, at construction: `proj_` prefix, unique
    /// names, at least one key column, bounded count. Registration order is
    /// normalized to table-name order so apply order is deterministic
    /// regardless of caller enumeration.
    pub fn new(mut projections: Vec<Box<dyn Projection>>) -> Result<Self, LogError> {
        assert!(
            projections.len() <= PROJECTION_COUNT_MAX,
            "projection count exceeds the stated bound"
        );
        projections.sort_by_key(|projection| projection.table().name);
        let mut previous_name = "";
        for projection in &projections {
            let table = projection.table();
            let shape_error = |reason: String| LogError::Apply {
                kind: String::new(),
                seq: 0,
                source: ApplyError { reason },
            };
            if !table.name.starts_with("proj_") {
                return Err(shape_error(format!(
                    "projection table {} must use the proj_ naming convention",
                    table.name
                )));
            }
            if table.name == previous_name {
                return Err(shape_error(format!(
                    "projection table {} is registered twice",
                    table.name
                )));
            }
            if table.key_columns.is_empty() {
                return Err(shape_error(format!(
                    "projection table {} declares no key columns",
                    table.name
                )));
            }
            for index in table.indexes {
                for column in index.columns {
                    let declared = table
                        .key_columns
                        .iter()
                        .chain(table.value_columns)
                        .any(|candidate| candidate.name == *column);
                    if !declared {
                        return Err(shape_error(format!(
                            "index {} on {} names undeclared column {column}",
                            index.name, table.name
                        )));
                    }
                }
            }
            previous_name = table.name;
        }
        Ok(Self { projections })
    }

    pub(crate) fn projections(&self) -> &[Box<dyn Projection>] {
        &self.projections
    }

    /// Digest of every table shape; stored in `log_state` and in snapshots.
    /// A changed registry (new table, bumped version, different columns)
    /// changes the digest, invalidating stale projections and snapshots.
    #[must_use]
    pub fn schema_digest(&self) -> [u8; 32] {
        let mut hasher = blake3_hasher();
        for projection in &self.projections {
            let table = projection.table();
            hasher.update(table.name.as_bytes());
            hasher.update(&table.version.to_be_bytes());
            for column in table.key_columns.iter().chain(table.value_columns) {
                hasher.update(column.name.as_bytes());
                hasher.update(column.kind.sql_type().as_bytes());
            }
            for index in table.indexes {
                hasher.update(index.name.as_bytes());
                for column in index.columns {
                    hasher.update(column.as_bytes());
                }
            }
        }
        *hasher.finalize().as_bytes()
    }
}

/// blake3 arrives through pos-store's re-export; the log reuses the same
/// digest primitive for schema/dump identity so the two crates cannot
/// disagree about content identity.
fn blake3_hasher() -> blake3::Hasher {
    blake3::Hasher::new()
}

/// The schema a write executes against: `main` for real projections, `temp`
/// for the non-destructive verify replay.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum SchemaTarget {
    Main,
    Temp,
}

impl SchemaTarget {
    const fn qualifier(self) -> &'static str {
        match self {
            Self::Main => "main",
            Self::Temp => "temp",
        }
    }
}

/// Applies one event through every registered projection and advances
/// `applied_seq` — always inside the caller's transaction.
pub(crate) fn apply_event(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    event: &Event,
) -> Result<(), LogError> {
    apply_event_to_schema(transaction, registry, event, SchemaTarget::Main)?;
    write_applied_seq(transaction, event.seq).map_err(sqlite("advance applied seq"))?;
    Ok(())
}

pub(crate) fn apply_event_to_schema(
    transaction: &Transaction<'_>,
    registry: &ProjectionRegistry,
    event: &Event,
    schema: SchemaTarget,
) -> Result<(), LogError> {
    for projection in registry.projections() {
        let writes = projection.apply(event).map_err(|source| LogError::Apply {
            kind: event.kind.as_str().to_owned(),
            seq: event.seq.value(),
            source,
        })?;
        for write in writes {
            execute_write(transaction, projection.table(), schema, &write).map_err(|source| {
                LogError::Apply {
                    kind: event.kind.as_str().to_owned(),
                    seq: event.seq.value(),
                    source,
                }
            })?;
        }
    }
    Ok(())
}

/// Renders and executes one typed write. This function and its snapshot/
/// replay siblings are the entire `proj_*` write surface of ProjectOS.
fn execute_write(
    transaction: &Transaction<'_>,
    table: &TableDef,
    schema: SchemaTarget,
    write: &RowWrite,
) -> Result<(), ApplyError> {
    let sql_error = |error: pos_store::rusqlite::Error| ApplyError {
        reason: format!("{}: {error}", table.name),
    };
    match write {
        RowWrite::Insert { key, values } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            require_arity(table, "values", values.len(), table.value_columns.len())?;
            let sql = insert_sql(table, schema);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            statement
                .execute(params_from_iter(bind_values(key.iter().chain(values))))
                .map_err(sql_error)?;
        }
        RowWrite::Upsert { key, values } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            require_arity(table, "values", values.len(), table.value_columns.len())?;
            let sql = upsert_sql(table, schema);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            statement
                .execute(params_from_iter(bind_values(key.iter().chain(values))))
                .map_err(sql_error)?;
        }
        RowWrite::Update { key, assignments } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            for (column, _) in assignments {
                require_value_column(table, column)?;
            }
            if assignments.is_empty() {
                return Err(ApplyError {
                    reason: format!("{}: update with no assignments", table.name),
                });
            }
            let sql = update_sql(table, schema, assignments);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let values = assignments.iter().map(|(_, value)| value).chain(key.iter());
            statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
        }
        RowWrite::UpdateOne { key, assignments } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            for (column, _) in assignments {
                require_value_column(table, column)?;
            }
            if assignments.is_empty() {
                return Err(ApplyError {
                    reason: format!("{}: strict update with no assignments", table.name),
                });
            }
            let sql = update_sql(table, schema, assignments);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let values = assignments.iter().map(|(_, value)| value).chain(key.iter());
            let changed = statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
            require_one_changed(table, "strict update", changed)?;
        }
        RowWrite::UpdateOneWhenNull {
            key,
            guard_column,
            assignments,
        } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            require_value_column(table, guard_column)?;
            for (column, _) in assignments {
                require_value_column(table, column)?;
            }
            if assignments.is_empty() {
                return Err(ApplyError {
                    reason: format!("{}: guarded update with no assignments", table.name),
                });
            }
            let sql = update_when_null_sql(table, schema, assignments, guard_column);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let values = assignments.iter().map(|(_, value)| value).chain(key.iter());
            let changed = statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
            require_one_changed(table, "guarded single-assignment update", changed)?;
        }
        RowWrite::Increment { key, column, delta } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            require_value_column(table, column)?;
            let sql = increment_sql(table, schema, column);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let delta_value = SqlValue::Integer(*delta);
            let values = key.iter().chain(std::iter::once(&delta_value));
            statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
        }
        RowWrite::IncrementOne { key, column, delta } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            require_value_column(table, column)?;
            let sql = increment_one_sql(table, schema, column);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let delta_value = SqlValue::Integer(*delta);
            let values = std::iter::once(&delta_value).chain(key.iter());
            let changed = statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
            require_one_changed(table, "strict increment", changed)?;
        }
        RowWrite::IncrementManyOne { key, deltas } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            if deltas.is_empty() {
                return Err(ApplyError {
                    reason: format!("{}: strict multi-increment has no deltas", table.name),
                });
            }
            for (column, _) in deltas {
                require_value_column(table, column)?;
            }
            let sql = increment_many_one_sql(table, schema, deltas);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            let delta_values = deltas
                .iter()
                .map(|(_, delta)| SqlValue::Integer(*delta))
                .collect::<Vec<_>>();
            let values = delta_values.iter().chain(key.iter());
            let changed = statement
                .execute(params_from_iter(bind_values(values)))
                .map_err(sql_error)?;
            require_one_changed(table, "strict multi-increment", changed)?;
        }
        RowWrite::Delete { key } => {
            require_arity(table, "key", key.len(), table.key_columns.len())?;
            let sql = delete_sql(table, schema);
            let mut statement = transaction.prepare_cached(&sql).map_err(sql_error)?;
            statement
                .execute(params_from_iter(bind_values(key.iter())))
                .map_err(sql_error)?;
        }
    }
    Ok(())
}

fn require_one_changed(
    table: &TableDef,
    operation: &str,
    changed: usize,
) -> Result<(), ApplyError> {
    if changed == 1 {
        Ok(())
    } else {
        Err(ApplyError {
            reason: format!(
                "{}: {operation} changed {changed} rows; exactly one lifecycle row must exist",
                table.name
            ),
        })
    }
}

fn require_arity(
    table: &TableDef,
    what: &str,
    got: usize,
    expected: usize,
) -> Result<(), ApplyError> {
    if got == expected {
        Ok(())
    } else {
        debug_assert_eq!(got, expected, "projection {} {what} arity", table.name);
        Err(ApplyError {
            reason: format!(
                "{}: {what} arity {got}, table declares {expected}",
                table.name
            ),
        })
    }
}

fn require_value_column(table: &TableDef, column: &str) -> Result<(), ApplyError> {
    if table.value_columns.iter().any(|c| c.name == column) {
        Ok(())
    } else {
        Err(ApplyError {
            reason: format!("{}: {column} is not a declared value column", table.name),
        })
    }
}

fn bind_values<'v>(
    values: impl Iterator<Item = &'v SqlValue>,
) -> impl Iterator<Item = pos_store::rusqlite::types::Value> {
    values.map(|value| match value {
        SqlValue::Integer(inner) => pos_store::rusqlite::types::Value::Integer(*inner),
        SqlValue::Text(inner) => pos_store::rusqlite::types::Value::Text(inner.clone()),
        SqlValue::Blob(inner) => pos_store::rusqlite::types::Value::Blob(inner.clone()),
        SqlValue::Null => pos_store::rusqlite::types::Value::Null,
    })
}

fn column_list(columns: &[ColumnDef]) -> String {
    columns
        .iter()
        .map(|column| column.name)
        .collect::<Vec<_>>()
        .join(", ")
}

fn placeholders(count: usize) -> String {
    vec!["?"; count].join(", ")
}

fn upsert_sql(table: &TableDef, schema: SchemaTarget) -> String {
    let keys = column_list(table.key_columns);
    let values = column_list(table.value_columns);
    let all = if table.value_columns.is_empty() {
        keys.clone()
    } else {
        format!("{keys}, {values}")
    };
    let update_clause = if table.value_columns.is_empty() {
        "NOTHING".to_owned()
    } else {
        let assignments = table
            .value_columns
            .iter()
            .map(|column| format!("{0} = excluded.{0}", column.name))
            .collect::<Vec<_>>()
            .join(", ");
        format!("UPDATE SET {assignments}")
    };
    format!(
        "INSERT INTO {schema}.{table} ({all}) VALUES ({binds}) ON CONFLICT({keys}) DO {update_clause}",
        schema = schema.qualifier(),
        table = table.name,
        binds = placeholders(table.key_columns.len() + table.value_columns.len()),
    )
}

fn insert_sql(table: &TableDef, schema: SchemaTarget) -> String {
    let keys = column_list(table.key_columns);
    let values = column_list(table.value_columns);
    let all = if table.value_columns.is_empty() {
        keys
    } else {
        format!("{keys}, {values}")
    };
    format!(
        "INSERT INTO {schema}.{table} ({all}) VALUES ({binds})",
        schema = schema.qualifier(),
        table = table.name,
        binds = placeholders(table.key_columns.len() + table.value_columns.len()),
    )
}

fn update_sql(
    table: &TableDef,
    schema: SchemaTarget,
    assignments: &[(&'static str, SqlValue)],
) -> String {
    let sets = assignments
        .iter()
        .map(|(column, _)| format!("{column} = ?"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE {schema}.{table} SET {sets} WHERE {keys}",
        schema = schema.qualifier(),
        table = table.name,
        keys = key_predicate(table),
    )
}

fn update_when_null_sql(
    table: &TableDef,
    schema: SchemaTarget,
    assignments: &[(&'static str, SqlValue)],
    guard_column: &str,
) -> String {
    let sets = assignments
        .iter()
        .map(|(column, _)| format!("{column} = ?"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE {schema}.{table} SET {sets} WHERE {keys} AND {guard_column} IS NULL",
        schema = schema.qualifier(),
        table = table.name,
        keys = key_predicate(table),
    )
}

fn increment_sql(table: &TableDef, schema: SchemaTarget, column: &str) -> String {
    format!(
        "INSERT INTO {schema}.{table} ({keys}, {column}) VALUES ({binds}, ?) \
         ON CONFLICT({keys}) DO UPDATE SET {column} = COALESCE({column}, 0) + excluded.{column}",
        schema = schema.qualifier(),
        table = table.name,
        keys = column_list(table.key_columns),
        binds = placeholders(table.key_columns.len()),
    )
}

fn increment_one_sql(table: &TableDef, schema: SchemaTarget, column: &str) -> String {
    format!(
        "UPDATE {schema}.{table} SET {column} = COALESCE({column}, 0) + ? WHERE {keys}",
        schema = schema.qualifier(),
        table = table.name,
        keys = key_predicate(table),
    )
}

fn increment_many_one_sql(
    table: &TableDef,
    schema: SchemaTarget,
    deltas: &[(&'static str, i64)],
) -> String {
    let assignments = deltas
        .iter()
        .map(|(column, _)| format!("{column} = COALESCE({column}, 0) + ?"))
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "UPDATE {schema}.{table} SET {assignments} WHERE {keys}",
        schema = schema.qualifier(),
        table = table.name,
        keys = key_predicate(table),
    )
}

fn delete_sql(table: &TableDef, schema: SchemaTarget) -> String {
    format!(
        "DELETE FROM {schema}.{table} WHERE {keys}",
        schema = schema.qualifier(),
        table = table.name,
        keys = key_predicate(table),
    )
}

fn key_predicate(table: &TableDef) -> String {
    table
        .key_columns
        .iter()
        .map(|column| format!("{} = ?", column.name))
        .collect::<Vec<_>>()
        .join(" AND ")
}

/// DDL for one projection table in a schema. `WITHOUT ROWID`: rows live in
/// primary-key order, which makes dumps and digests naturally deterministic.
pub(crate) fn create_table_sql(table: &TableDef, schema: SchemaTarget) -> String {
    let mut columns = Vec::new();
    for column in table.key_columns {
        columns.push(format!(
            "{} {} NOT NULL",
            column.name,
            column.kind.sql_type()
        ));
    }
    for column in table.value_columns {
        columns.push(format!("{} {}", column.name, column.kind.sql_type()));
    }
    let mut sql = format!(
        "CREATE TABLE IF NOT EXISTS {schema}.{table} ({columns}, PRIMARY KEY ({keys})) WITHOUT ROWID;",
        schema = schema.qualifier(),
        table = table.name,
        columns = columns.join(", "),
        keys = column_list(table.key_columns),
    );
    for index in table.indexes {
        // The index name is schema-qualified rather than suffixed: `temp` and
        // `main` hold the same table under the verify replay, and SQLite index
        // names are unique per schema.
        sql.push_str(&format!(
            "CREATE INDEX IF NOT EXISTS {schema}.{index} ON {table} ({columns});",
            schema = schema.qualifier(),
            index = index.name,
            table = table.name,
            columns = index.columns.join(", "),
        ));
    }
    sql
}

pub(crate) fn drop_table_sql(table: &TableDef, schema: SchemaTarget) -> String {
    format!(
        "DROP TABLE IF EXISTS {schema}.{table}",
        schema = schema.qualifier(),
        table = table.name
    )
}

/// `log_state.applied_seq`: the seq projections are current through. Read
/// via deref coercion from transactions and plain connections alike.
pub(crate) fn read_applied_seq(connection: &Connection) -> Result<u64, pos_store::rusqlite::Error> {
    use pos_store::rusqlite::OptionalExtension;
    let value: Option<Vec<u8>> = connection
        .query_row(
            "SELECT value FROM log_state WHERE key = 'applied_seq'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(value
        .and_then(|bytes| <[u8; 8]>::try_from(bytes.as_slice()).ok())
        .map_or(0, u64::from_be_bytes))
}

pub(crate) fn write_applied_seq(
    connection: &Connection,
    seq: EventSeq,
) -> Result<(), pos_store::rusqlite::Error> {
    connection
        .prepare_cached(
            "INSERT INTO log_state (key, value) VALUES ('applied_seq', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )?
        .execute([seq.value().to_be_bytes().to_vec()])?;
    Ok(())
}

pub(crate) fn read_schema_digest(
    connection: &Connection,
) -> Result<Option<[u8; 32]>, pos_store::rusqlite::Error> {
    use pos_store::rusqlite::OptionalExtension;
    let value: Option<Vec<u8>> = connection
        .query_row(
            "SELECT value FROM log_state WHERE key = 'schema_digest'",
            [],
            |row| row.get(0),
        )
        .optional()?;
    Ok(value.and_then(|bytes| <[u8; 32]>::try_from(bytes.as_slice()).ok()))
}

pub(crate) fn write_schema_digest(
    connection: &Connection,
    digest: [u8; 32],
) -> Result<(), pos_store::rusqlite::Error> {
    connection
        .prepare_cached(
            "INSERT INTO log_state (key, value) VALUES ('schema_digest', ?1)
             ON CONFLICT(key) DO UPDATE SET value = excluded.value",
        )?
        .execute([digest.to_vec()])?;
    Ok(())
}

pub(crate) fn sqlite(context: &'static str) -> impl Fn(pos_store::rusqlite::Error) -> LogError {
    move |source| LogError::Store(StoreError::Sqlite { context, source })
}

/// Canonical bytes of one table's rows in primary-key order: the unit both
/// the dump and the digest build on. Tag bytes keep value types unambiguous
/// so `1` (integer) can never collide with `"1"` (text).
pub(crate) fn fold_table_rows(
    connection: &Connection,
    table: &TableDef,
    schema: SchemaTarget,
    mut fold: impl FnMut(&[u8]),
) -> Result<(), pos_store::rusqlite::Error> {
    let sql = format!(
        "SELECT * FROM {schema}.{table} ORDER BY {keys}",
        schema = schema.qualifier(),
        table = table.name,
        keys = column_list(table.key_columns),
    );
    let mut statement = connection.prepare(&sql)?;
    let column_count = statement.column_count();
    let mut rows = statement.query([])?;
    let mut row_bytes = Vec::new();
    while let Some(row) = rows.next()? {
        row_bytes.clear();
        for index in 0..column_count {
            match row.get_ref(index)? {
                ValueRef::Null => row_bytes.push(0),
                ValueRef::Integer(value) => {
                    row_bytes.push(1);
                    row_bytes.extend_from_slice(&value.to_be_bytes());
                }
                ValueRef::Real(_) => {
                    // Floats are excluded from SqlValue by design; a REAL in
                    // a projection table means foreign mutation. Fold a
                    // distinct tag so verify sees the difference.
                    row_bytes.push(4);
                }
                ValueRef::Text(text) => {
                    row_bytes.push(2);
                    row_bytes
                        .extend_from_slice(&u64::try_from(text.len()).unwrap_or(0).to_be_bytes());
                    row_bytes.extend_from_slice(text);
                }
                ValueRef::Blob(blob) => {
                    row_bytes.push(3);
                    row_bytes
                        .extend_from_slice(&u64::try_from(blob.len()).unwrap_or(0).to_be_bytes());
                    row_bytes.extend_from_slice(blob);
                }
            }
        }
        fold(&row_bytes);
    }
    Ok(())
}

/// Canonical bytes of every projection table (small logs / tests).
pub(crate) fn dump_projections(
    store: &pos_store::ProjectStore,
    registry: &ProjectionRegistry,
) -> Result<Vec<u8>, LogError> {
    let mut dump = Vec::new();
    store.db().with_reader("dump projections", |connection| {
        for projection in registry.projections() {
            let table = projection.table();
            dump.extend_from_slice(table.name.as_bytes());
            dump.push(b'\n');
            fold_table_rows(connection, table, SchemaTarget::Main, |row| {
                dump.extend_from_slice(row);
                dump.push(b'\n');
            })?;
        }
        Ok(())
    })?;
    Ok(dump)
}

/// Streaming digest of one table (verify at 1M events without 1M rows in
/// memory).
pub(crate) fn digest_table(
    connection: &Connection,
    table: &TableDef,
    schema: SchemaTarget,
) -> Result<[u8; 32], pos_store::rusqlite::Error> {
    let mut hasher = blake3_hasher();
    fold_table_rows(connection, table, schema, |row| {
        hasher.update(row);
    })?;
    Ok(*hasher.finalize().as_bytes())
}
