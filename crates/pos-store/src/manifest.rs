//! `manifest.json` (§7.2, F2): the small plain-text file that makes a
//! project directory self-describing. Written atomically, last during
//! create — its presence marks a complete project directory, so a crashed
//! half-created directory can never be mistaken for a project.

use crate::StoreError;
use pos_foundation::ProjectId;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};

/// The on-disk format version this build reads and writes. Governed by
/// `docs/format-spec.md` (m0-s05); a CI check keeps constant and spec equal.
/// Version bumps are additive with a documented migration (L4, §3.2).
pub const FORMAT_VERSION: u32 = 0;

pub const MANIFEST_FILE_NAME: &str = "manifest.json";

/// The §7.2 manifest fields. `project_id` renders as 32-char lowercase hex —
/// the one textual id encoding (`pos-foundation`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Manifest {
    pub format_version: u32,
    pub project_id: ProjectId,
    pub template: String,
    pub created_ts_ms: u64,
}

/// The literal JSON shape. Kept separate from [`Manifest`] so the wire form
/// (hex id) and the typed form (ProjectId) cannot drift apart silently.
#[derive(Deserialize, Serialize)]
struct ManifestFile {
    format_version: u32,
    project_id: String,
    template: String,
    created_ts_ms: u64,
}

impl Manifest {
    /// Reads and validates `<root>/manifest.json`.
    pub fn read(project_root: &Path) -> Result<Self, StoreError> {
        let path = project_root.join(MANIFEST_FILE_NAME);
        let text = fs::read_to_string(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                StoreError::NotAProject {
                    path: project_root.to_path_buf(),
                    reason: "manifest.json is missing",
                }
            } else {
                StoreError::Io {
                    context: "read manifest",
                    path: path.clone(),
                    source,
                }
            }
        })?;
        let file: ManifestFile =
            serde_json::from_str(&text).map_err(|error| StoreError::ManifestInvalid {
                path: path.clone(),
                reason: format!("not valid manifest JSON: {error}"),
            })?;
        if file.format_version > FORMAT_VERSION {
            return Err(StoreError::ManifestInvalid {
                path,
                reason: format!(
                    "format_version {} is newer than this build understands ({FORMAT_VERSION}); \
                     upgrade ProjectOS instead of risking the data",
                    file.format_version
                ),
            });
        }
        let project_id =
            ProjectId::from_hex(&file.project_id).ok_or_else(|| StoreError::ManifestInvalid {
                path,
                reason: format!(
                    "project_id {:?} is not 32-char lowercase hex",
                    file.project_id
                ),
            })?;
        Ok(Self {
            format_version: file.format_version,
            project_id,
            template: file.template,
            created_ts_ms: file.created_ts_ms,
        })
    }

    /// Writes the manifest atomically (temp + rename + fsync).
    pub fn write(&self, project_root: &Path) -> Result<(), StoreError> {
        let path = project_root.join(MANIFEST_FILE_NAME);
        let file = ManifestFile {
            format_version: self.format_version,
            project_id: self.project_id.to_hex(),
            template: self.template.clone(),
            created_ts_ms: self.created_ts_ms,
        };
        let json =
            serde_json::to_string_pretty(&file).map_err(|error| StoreError::ManifestInvalid {
                path: path.clone(),
                reason: format!("manifest failed to serialize: {error}"),
            })?;
        let temp_path = temp_manifest_path(project_root);
        write_file_durably(&temp_path, json.as_bytes())?;
        fs::rename(&temp_path, &path).map_err(|source| StoreError::Io {
            context: "publish manifest",
            path: path.clone(),
            source,
        })?;
        Ok(())
    }
}

fn temp_manifest_path(project_root: &Path) -> PathBuf {
    project_root.join(format!(".{MANIFEST_FILE_NAME}.tmp-{}", std::process::id()))
}

fn write_file_durably(path: &Path, bytes: &[u8]) -> Result<(), StoreError> {
    let io_error = |context: &'static str| {
        let path = path.to_path_buf();
        move |source| StoreError::Io {
            context,
            path,
            source,
        }
    };
    fs::write(path, bytes).map_err(io_error("write manifest temp file"))?;
    let file = fs::File::open(path).map_err(io_error("open manifest temp file for sync"))?;
    file.sync_all().map_err(io_error("sync manifest temp file"))
}

#[cfg(test)]
mod tests {
    use super::{FORMAT_VERSION, Manifest};
    use crate::StoreError;
    use pos_foundation::ProjectId;

    fn manifest() -> Manifest {
        Manifest {
            format_version: FORMAT_VERSION,
            project_id: ProjectId::from_bytes([7; 16]),
            template: "generic".to_owned(),
            created_ts_ms: 1_700_000_000_000,
        }
    }

    #[test]
    fn manifest_round_trips() {
        let directory = tempfile::tempdir().expect("test tempdir");
        manifest().write(directory.path()).expect("write manifest");
        let read = Manifest::read(directory.path()).expect("read manifest");
        assert_eq!(read, manifest());
    }

    #[test]
    fn missing_manifest_is_not_a_project() {
        let directory = tempfile::tempdir().expect("test tempdir");
        let error = Manifest::read(directory.path()).expect_err("no manifest present");
        assert!(matches!(error, StoreError::NotAProject { .. }));
    }

    #[test]
    fn newer_format_version_is_refused_with_a_named_reason() {
        let directory = tempfile::tempdir().expect("test tempdir");
        let mut future = manifest();
        future.format_version = FORMAT_VERSION + 1;
        future.write(directory.path()).expect("write manifest");
        let error = Manifest::read(directory.path()).expect_err("future format must be refused");
        let StoreError::ManifestInvalid { reason, .. } = error else {
            panic!("expected ManifestInvalid, got {error:?}");
        };
        assert!(reason.contains("newer than this build"));
    }

    #[test]
    fn malformed_id_is_refused() {
        let directory = tempfile::tempdir().expect("test tempdir");
        std::fs::write(
            directory.path().join("manifest.json"),
            r#"{"format_version":0,"project_id":"XYZ","template":"generic","created_ts_ms":1}"#,
        )
        .expect("write raw manifest");
        let error = Manifest::read(directory.path()).expect_err("bad id must be refused");
        assert!(matches!(error, StoreError::ManifestInvalid { .. }));
    }
}
