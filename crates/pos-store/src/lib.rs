//! # pos-store
//!
//! The storage engine: SQLite (WAL) with FTS5 + sqlite-vec, and the BLAKE3 content-addressed blob store. One codepath for laptop and server (L12).
//!
//! Filled by m0-s04. Charter: master plan §19. The unsafe FFI leaf is
//! `src/extensions.rs` (see SAFETY.md); everything else denies unsafe.
//!
//! A [`ProjectStore`] is one `.pos` directory (§7.2): `project.db`, `blobs/`,
//! `manifest.json`. Layers above see typed operations and typed errors —
//! `rusqlite` is re-exported for exactly one consumer, `pos-log`, which owns
//! the event/projection schema on top of this engine (§6 layering).

#![deny(unsafe_code)]

mod cas;
mod db;
mod extensions;
pub mod fault;
mod manifest;

pub use cas::{
    BLOBS_DIRECTORY_NAME, BlobHash, BlobStore, BlobWriter, CAS_WRITE_BUFFER_CAP, CasVerifyReport,
};
pub use db::{PROJECT_DB_FILE_NAME, ProjectDb};
pub use fault::{FaultAction, FaultPlan, FaultPoint};
pub use manifest::{FORMAT_VERSION, MANIFEST_FILE_NAME, Manifest};
// The one sanctioned pass-through: pos-log speaks SQL to the engine it is
// built on. Shells and feature crates never see this (dep DAG).
pub use rusqlite;
// Shared digest primitive: the log's schema/dump digests use the same hash
// as the CAS so the two crates cannot disagree about content identity.
pub use blake3;

use pos_foundation::{ProjectId, WallClock};
use std::fmt;
use std::fs;
use std::path::{Path, PathBuf};

/// Typed operational failures of the storage engine. Panics are reserved for
/// violated internal invariants (STYLE); everything a disk, a user, or a
/// crash can cause arrives here.
#[derive(Debug)]
pub enum StoreError {
    Io {
        context: &'static str,
        path: PathBuf,
        source: std::io::Error,
    },
    Sqlite {
        context: &'static str,
        source: rusqlite::Error,
    },
    /// Commit-time I/O-class failure: committed data can no longer be
    /// trusted. The project store is fail-stopped (STYLE: fsync/WAL failure
    /// is fail-stop for the affected project, never silent degradation).
    DurabilityLost {
        context: &'static str,
        path: PathBuf,
    },
    /// The store was fail-stopped earlier in this process; reopen to recover.
    FailStopped {
        path: PathBuf,
    },
    ExtensionMissing {
        name: &'static str,
    },
    PragmaRejected {
        pragma: &'static str,
        detail: String,
    },
    ManifestInvalid {
        path: PathBuf,
        reason: String,
    },
    NotAProject {
        path: PathBuf,
        reason: &'static str,
    },
    AlreadyExists {
        path: PathBuf,
    },
    BlobMissing {
        hash: BlobHash,
    },
    BlobCorrupt {
        path: PathBuf,
        expected: BlobHash,
        actual: BlobHash,
    },
    /// An armed [`FaultPlan`] tripped with `FailOperation` — test-only paths.
    InjectedFault {
        point: FaultPoint,
    },
}

impl fmt::Display for StoreError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io {
                context,
                path,
                source,
            } => write!(formatter, "{context}: {}: {source}", path.display()),
            Self::Sqlite { context, source } => write!(formatter, "{context}: {source}"),
            Self::DurabilityLost { context, path } => write!(
                formatter,
                "durability lost during {context}: {} is fail-stopped for this process; \
                 reopen the project to recover",
                path.display()
            ),
            Self::FailStopped { path } => write!(
                formatter,
                "{} is fail-stopped after an earlier durability failure",
                path.display()
            ),
            Self::ExtensionMissing { name } => {
                write!(formatter, "required SQLite extension {name} did not load")
            }
            Self::PragmaRejected { pragma, detail } => {
                write!(formatter, "SQLite rejected {pragma}: {detail}")
            }
            Self::ManifestInvalid { path, reason } => {
                write!(formatter, "invalid manifest {}: {reason}", path.display())
            }
            Self::NotAProject { path, reason } => {
                write!(formatter, "{} is not a project: {reason}", path.display())
            }
            Self::AlreadyExists { path } => {
                write!(
                    formatter,
                    "{} already exists and is not empty",
                    path.display()
                )
            }
            Self::BlobMissing { hash } => write!(formatter, "blob {hash} is not in the store"),
            Self::BlobCorrupt {
                path,
                expected,
                actual,
            } => write!(
                formatter,
                "blob {} does not match its address (expected {expected}, hashed {actual})",
                path.display()
            ),
            Self::InjectedFault { point } => {
                write!(formatter, "injected fault tripped at {point}")
            }
        }
    }
}

impl std::error::Error for StoreError {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        match self {
            Self::Io { source, .. } => Some(source),
            Self::Sqlite { source, .. } => Some(source),
            _ => None,
        }
    }
}

/// Options for opening a store. Fault plans exist for the crash harness;
/// production callers use `Default`.
#[derive(Clone, Copy, Debug, Default)]
pub struct StoreOptions {
    pub faults: Option<FaultPlan>,
}

/// One open project directory: database, blobs, manifest.
pub struct ProjectStore {
    root: PathBuf,
    db: ProjectDb,
    blobs: BlobStore,
    manifest: Manifest,
    faults: Option<FaultPlan>,
}

impl fmt::Debug for ProjectStore {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("ProjectStore")
            .field("root", &self.root)
            .field("project_id", &self.manifest.project_id)
            .finish_non_exhaustive()
    }
}

impl ProjectStore {
    /// Creates a new project directory at `root`. Refuses a non-empty
    /// destination: creation never adopts or overwrites foreign data.
    ///
    /// The manifest is written **last**: its presence marks a complete
    /// directory, so a crash mid-create leaves debris, never a half-project.
    pub fn create(root: &Path, template: &str, clock: &dyn WallClock) -> Result<Self, StoreError> {
        if root.exists() && directory_is_occupied(root)? {
            return Err(StoreError::AlreadyExists {
                path: root.to_path_buf(),
            });
        }
        fs::create_dir_all(root).map_err(|source| StoreError::Io {
            context: "create project directory",
            path: root.to_path_buf(),
            source,
        })?;
        let db = ProjectDb::open(root, None)?;
        let blobs = BlobStore::open(root, None)?;
        // SQLite's PRNG is seeded from OS entropy and already in-process;
        // minting the 128-bit id here avoids a dependency whose only job
        // would be sixteen random bytes.
        let id_bytes: Vec<u8> = db.with_writer("mint project id", |connection| {
            connection.query_row("SELECT randomblob(16)", [], |row| row.get(0))
        })?;
        let id_bytes: [u8; 16] = id_bytes
            .try_into()
            .map_err(|_| StoreError::PragmaRejected {
                pragma: "randomblob(16)",
                detail: "returned a blob that is not 16 bytes".to_owned(),
            })?;
        let manifest = Manifest {
            format_version: FORMAT_VERSION,
            project_id: ProjectId::from_bytes(id_bytes),
            template: template.to_owned(),
            created_ts_ms: clock.now_ms(),
        };
        manifest.write(root)?;
        Ok(Self {
            root: root.to_path_buf(),
            db,
            blobs,
            manifest,
            faults: None,
        })
    }

    /// Opens an existing project directory.
    pub fn open(root: &Path) -> Result<Self, StoreError> {
        Self::open_with_options(root, StoreOptions::default())
    }

    /// Opens with explicit options (crash harness arms fault plans here).
    pub fn open_with_options(root: &Path, options: StoreOptions) -> Result<Self, StoreError> {
        let manifest = Manifest::read(root)?;
        let db = ProjectDb::open(root, options.faults)?;
        let blobs = BlobStore::open(root, options.faults)?;
        Ok(Self {
            root: root.to_path_buf(),
            db,
            blobs,
            manifest,
            faults: options.faults,
        })
    }

    #[must_use]
    pub fn root(&self) -> &Path {
        &self.root
    }

    #[must_use]
    pub fn db(&self) -> &ProjectDb {
        &self.db
    }

    #[must_use]
    pub fn blobs(&self) -> &BlobStore {
        &self.blobs
    }

    #[must_use]
    pub fn manifest(&self) -> &Manifest {
        &self.manifest
    }

    /// Trips an armed fault at a higher layer's point (`pos-log` transaction
    /// internals); `Ok` always in production (no plan armed).
    pub fn trip_fault(&self, point: FaultPoint) -> Result<(), StoreError> {
        fault::trip(self.faults.as_ref(), point)
    }
}

/// True when `path` is a directory with any entry in it.
fn directory_is_occupied(path: &Path) -> Result<bool, StoreError> {
    if !path.is_dir() {
        // A file at the destination counts as occupied.
        return Ok(path.exists());
    }
    let mut entries = fs::read_dir(path).map_err(|source| StoreError::Io {
        context: "inspect destination directory",
        path: path.to_path_buf(),
        source,
    })?;
    Ok(entries.next().is_some())
}
