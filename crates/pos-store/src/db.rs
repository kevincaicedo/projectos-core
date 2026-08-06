//! Per-project SQLite access (m0-s04): one writer, a small reader pool, WAL
//! mode with documented pragmas, and fail-stop durability — an I/O-class
//! failure at commit poisons the project store instead of degrading silently
//! (STYLE: never silently degrade durability).

use crate::fault::{FaultPlan, FaultPoint};
use crate::{StoreError, extensions};
use rusqlite::{Connection, OpenFlags, Transaction, TransactionBehavior};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, Ordering};

/// Readers a project keeps warm. M0 concurrency is one UI plus one CLI; the
/// m0-s08 server story revisits this with measured contention, not a guess.
const READER_CONNECTION_COUNT_MAX: usize = 4;

/// How long a connection waits on SQLite's file lock before failing typed.
/// Single-writer discipline makes real contention a bug, so the bound exists
/// to surface that bug quickly rather than to paper over it.
const BUSY_TIMEOUT_MS: u64 = 5_000;

/// WAL checkpoint threshold in pages (4 KiB default page size ⇒ ~4 MiB WAL).
/// SQLite's default; stated here so the choice is visible and tunable.
const WAL_AUTOCHECKPOINT_PAGES: u32 = 1_000;

/// The one SQLite database of a project directory (§7.2).
pub const PROJECT_DB_FILE_NAME: &str = "project.db";

/// Per-project database handle: a single mutex-serialized writer (per-project
/// single-writer discipline, master plan §8) plus a bounded read pool.
pub struct ProjectDb {
    path: PathBuf,
    writer: Mutex<Connection>,
    readers: Mutex<Vec<Connection>>,
    fail_stopped: AtomicBool,
    faults: Option<FaultPlan>,
}

impl ProjectDb {
    /// Opens (creating if absent) the project database and proves the
    /// extension surface: FTS5 and sqlite-vec load now, at open, because a
    /// store that discovers a missing index engine mid-ingest is corruption
    /// waiting for a write path.
    pub fn open(project_root: &Path, faults: Option<FaultPlan>) -> Result<Self, StoreError> {
        extensions::register_static_extensions();
        let path = project_root.join(PROJECT_DB_FILE_NAME);
        let writer = Connection::open(&path).map_err(|source| StoreError::Sqlite {
            context: "open project database",
            source,
        })?;
        configure_connection(&writer)?;
        probe_extensions(&writer)?;
        Ok(Self {
            path,
            writer: Mutex::new(writer),
            readers: Mutex::new(Vec::new()),
            fail_stopped: AtomicBool::new(false),
            faults,
        })
    }

    #[must_use]
    pub fn path(&self) -> &Path {
        &self.path
    }

    /// Runs `operation` on the single writer connection in autocommit mode —
    /// for DDL and one-statement writes. Multi-statement state changes go
    /// through [`Self::write_transaction`].
    pub fn with_writer<T>(
        &self,
        context: &'static str,
        operation: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StoreError> {
        self.ensure_live()?;
        let writer = self.lock_writer()?;
        operation(&writer).map_err(|source| self.classify(context, source))
    }

    /// Runs `operation` inside one IMMEDIATE transaction and commits durably.
    /// An operation error rolls back (transaction drop). A commit-time
    /// I/O-class failure — real or injected at [`FaultPoint::WalCommit`] —
    /// fail-stops this project: the typed error names it, and every later
    /// call refuses until the process reopens the store (STYLE).
    ///
    /// Generic over the caller's error so higher layers (`pos-log`) keep
    /// their typed errors through the transaction boundary.
    pub fn write_transaction<T, E: From<StoreError>>(
        &self,
        context: &'static str,
        operation: impl FnOnce(&Transaction<'_>) -> Result<T, E>,
    ) -> Result<T, E> {
        self.ensure_live().map_err(E::from)?;
        let mut writer = self.lock_writer().map_err(E::from)?;
        // IMMEDIATE takes the write lock at BEGIN: under single-writer
        // discipline a later upgrade conflict would be a hidden second writer.
        let transaction = writer
            .transaction_with_behavior(TransactionBehavior::Immediate)
            .map_err(|source| E::from(self.classify(context, source)))?;
        let output = operation(&transaction)?;
        if let Err(fault) = crate::fault::trip(self.faults.as_ref(), FaultPoint::WalCommit) {
            drop(fault);
            return Err(E::from(self.declare_durability_lost(context)));
        }
        transaction.commit().map_err(|source| {
            E::from(if is_durability_loss(&source) {
                self.declare_durability_lost(context)
            } else {
                StoreError::Sqlite { context, source }
            })
        })?;
        Ok(output)
    }

    /// Runs `operation` on a pooled read-only connection.
    pub fn with_reader<T>(
        &self,
        context: &'static str,
        operation: impl FnOnce(&Connection) -> Result<T, rusqlite::Error>,
    ) -> Result<T, StoreError> {
        self.ensure_live()?;
        let reader = self.take_reader()?;
        let output = operation(&reader).map_err(|source| self.classify(context, source));
        self.return_reader(reader)?;
        output
    }

    fn take_reader(&self) -> Result<Connection, StoreError> {
        let mut pool = self
            .readers
            .lock()
            .map_err(|_| self.fail_stop_error("reader pool poisoned by a panic"))?;
        if let Some(reader) = pool.pop() {
            return Ok(reader);
        }
        drop(pool);
        let reader = Connection::open_with_flags(
            &self.path,
            OpenFlags::SQLITE_OPEN_READ_ONLY | OpenFlags::SQLITE_OPEN_NO_MUTEX,
        )
        .map_err(|source| StoreError::Sqlite {
            context: "open read connection",
            source,
        })?;
        set_busy_timeout(&reader)?;
        Ok(reader)
    }

    fn return_reader(&self, reader: Connection) -> Result<(), StoreError> {
        let mut pool = self
            .readers
            .lock()
            .map_err(|_| self.fail_stop_error("reader pool poisoned by a panic"))?;
        if pool.len() < READER_CONNECTION_COUNT_MAX {
            pool.push(reader);
        }
        Ok(())
    }

    fn lock_writer(&self) -> Result<std::sync::MutexGuard<'_, Connection>, StoreError> {
        // A poisoned writer mutex means a panic escaped mid-write; the only
        // honest continuation is fail-stop, the same as durability loss.
        self.writer
            .lock()
            .map_err(|_| self.fail_stop_error("writer poisoned by a panic"))
    }

    fn ensure_live(&self) -> Result<(), StoreError> {
        if self.fail_stopped.load(Ordering::SeqCst) {
            return Err(StoreError::FailStopped {
                path: self.path.clone(),
            });
        }
        Ok(())
    }

    fn declare_durability_lost(&self, context: &'static str) -> StoreError {
        self.fail_stopped.store(true, Ordering::SeqCst);
        StoreError::DurabilityLost {
            context,
            path: self.path.clone(),
        }
    }

    fn fail_stop_error(&self, _reason: &'static str) -> StoreError {
        self.fail_stopped.store(true, Ordering::SeqCst);
        StoreError::FailStopped {
            path: self.path.clone(),
        }
    }

    fn classify(&self, context: &'static str, source: rusqlite::Error) -> StoreError {
        if is_durability_loss(&source) {
            self.declare_durability_lost(context)
        } else {
            StoreError::Sqlite { context, source }
        }
    }
}

/// An error class after which committed data can no longer be trusted to be
/// on disk. Busy/locked/constraint errors are operational weather; these are
/// not.
fn is_durability_loss(error: &rusqlite::Error) -> bool {
    let rusqlite::Error::SqliteFailure(inner, _) = error else {
        return false;
    };
    matches!(
        inner.code,
        rusqlite::ErrorCode::DiskFull
            | rusqlite::ErrorCode::SystemIoFailure
            | rusqlite::ErrorCode::DatabaseCorrupt
            | rusqlite::ErrorCode::NotADatabase
    )
}

fn configure_connection(connection: &Connection) -> Result<(), StoreError> {
    // WAL: readers proceed during writes and a crash tears nothing — the
    // journal mode the crash matrix assumes. The mode is persistent, but
    // setting it every open makes a hand-copied database heal itself.
    let mode: String = connection
        .query_row("PRAGMA journal_mode=WAL", [], |row| row.get(0))
        .map_err(|source| StoreError::Sqlite {
            context: "enable WAL journal mode",
            source,
        })?;
    if !mode.eq_ignore_ascii_case("wal") {
        return Err(StoreError::PragmaRejected {
            pragma: "journal_mode=WAL",
            detail: format!("database reports journal_mode={mode:?}"),
        });
    }
    // FULL, not NORMAL: an acked append is the project's truth (L1). NORMAL
    // in WAL can lose acked commits on power loss — a silent durability
    // downgrade STYLE forbids. Throughput comes from batching, not from
    // weakening fsync.
    run_pragmas(
        connection,
        &[
            "PRAGMA synchronous=FULL",
            "PRAGMA foreign_keys=ON",
            &format!("PRAGMA wal_autocheckpoint={WAL_AUTOCHECKPOINT_PAGES}"),
        ],
    )?;
    set_busy_timeout(connection)
}

fn run_pragmas(connection: &Connection, statements: &[&str]) -> Result<(), StoreError> {
    for statement in statements {
        connection
            .execute_batch(statement)
            .map_err(|source| StoreError::Sqlite {
                context: "apply connection pragma",
                source,
            })?;
    }
    Ok(())
}

fn set_busy_timeout(connection: &Connection) -> Result<(), StoreError> {
    connection
        .busy_timeout(std::time::Duration::from_millis(BUSY_TIMEOUT_MS))
        .map_err(|source| StoreError::Sqlite {
            context: "set busy timeout",
            source,
        })
}

/// Extension loading is the risky part of the storage story (m0-s04), so it
/// is proven at every open, not discovered at first use.
fn probe_extensions(connection: &Connection) -> Result<(), StoreError> {
    let fts5_present: i64 = connection
        .query_row(
            "SELECT count(*) FROM pragma_module_list WHERE name='fts5'",
            [],
            |row| row.get(0),
        )
        .map_err(|source| StoreError::Sqlite {
            context: "probe fts5 module",
            source,
        })?;
    if fts5_present != 1 {
        return Err(StoreError::ExtensionMissing { name: "fts5" });
    }
    connection
        .query_row("SELECT vec_version()", [], |row| row.get::<_, String>(0))
        .map_err(|_| StoreError::ExtensionMissing {
            name: "sqlite-vec (vec0)",
        })?;
    Ok(())
}
