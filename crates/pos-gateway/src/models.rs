//! The model manager v0 (m0-s11): local models are downloaded artifacts with
//! checksums — never bundled in the binary, never fetched without consent.
//! `pos models pull <name>` resolves a name against the in-repo manifest
//! (`models/manifest.json`), streams the artifact to a temp file while
//! hashing, verifies BLAKE3 + byte count, and renames atomically. A tampered
//! artifact is refused and the temp file removed; resume support is the
//! recorded m1-s03 cut line.

use crate::transport::{HttpHead, HttpRequestPlan, HttpTransport, ResponseHandler, StreamAbort};
use serde::Deserialize;
use std::fmt;
use std::io::Write;
use std::path::{Path, PathBuf};

/// Streaming write granularity for downloads; matches the transport's read
/// buffer so a pull is two copies, not many.
const DOWNLOAD_CHUNK_BYTES: usize = 16 * 1024;

/// A model artifact download gets generous time: whisper-class files are
/// hundreds of MB on user links.
const DOWNLOAD_TIMEOUT_MS: u32 = 10 * 60 * 1_000;

/// One manifest row: where a model comes from and exactly what bytes it is.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelManifestEntry {
    pub name: String,
    pub url: String,
    pub blake3: String,
    pub bytes: u64,
}

/// The in-repo manifest: the complete, reviewed catalog of pullable models.
#[derive(Clone, Debug, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ModelManifest {
    pub models: Vec<ModelManifestEntry>,
}

impl ModelManifest {
    /// # Errors
    ///
    /// [`ModelPullError::ManifestUnreadable`] when the file is missing or
    /// does not parse — a malformed catalog must not half-load.
    pub fn load(path: &Path) -> Result<Self, ModelPullError> {
        let text =
            std::fs::read_to_string(path).map_err(|error| ModelPullError::ManifestUnreadable {
                path: path.display().to_string(),
                reason: error.to_string(),
            })?;
        serde_json::from_str(&text).map_err(|error| ModelPullError::ManifestUnreadable {
            path: path.display().to_string(),
            reason: error.to_string(),
        })
    }

    /// # Errors
    ///
    /// [`ModelPullError::UnknownModel`] when the name is not in the catalog.
    pub fn entry(&self, name: &str) -> Result<&ModelManifestEntry, ModelPullError> {
        self.models
            .iter()
            .find(|entry| entry.name == name)
            .ok_or_else(|| ModelPullError::UnknownModel {
                name: name.to_owned(),
            })
    }
}

/// Explicit consent is an argument, not a default (L5-adjacent): the CLI
/// sets `Given` only after an interactive yes or `--yes`.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum PullConsent {
    Given,
    Withheld,
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelPullError {
    ManifestUnreadable {
        path: String,
        reason: String,
    },
    UnknownModel {
        name: String,
    },
    /// The refusal that makes "never auto-fetches" a mechanical property.
    ConsentRequired {
        name: String,
    },
    /// Wrong bytes: hash or size disagreed with the manifest. The temp file
    /// is already removed when this returns.
    ChecksumMismatch {
        name: String,
        expected_blake3: String,
        actual_blake3: String,
    },
    SizeMismatch {
        name: String,
        expected_bytes: u64,
        actual_bytes: u64,
    },
    /// The artifact is larger than the manifest claims — refused mid-stream
    /// rather than after the disk fills (L8).
    Overrun {
        name: String,
        expected_bytes: u64,
    },
    AlreadyPresent {
        path: String,
    },
    Source {
        reason: String,
    },
    Io {
        path: String,
        reason: String,
    },
}

impl fmt::Display for ModelPullError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::ManifestUnreadable { path, reason } => {
                write!(formatter, "model manifest unreadable at {path}: {reason}")
            }
            Self::UnknownModel { name } => {
                write!(formatter, "no model named {name:?} is in the manifest")
            }
            Self::ConsentRequired { name } => write!(
                formatter,
                "pulling {name:?} downloads a model artifact; re-run with explicit consent (--yes or answer y)"
            ),
            Self::ChecksumMismatch {
                name,
                expected_blake3,
                actual_blake3,
            } => write!(
                formatter,
                "{name:?} failed BLAKE3 verification: manifest {expected_blake3}, artifact {actual_blake3}; the download was discarded"
            ),
            Self::SizeMismatch {
                name,
                expected_bytes,
                actual_bytes,
            } => write!(
                formatter,
                "{name:?} is {actual_bytes} bytes, manifest says {expected_bytes}; the download was discarded"
            ),
            Self::Overrun {
                name,
                expected_bytes,
            } => write!(
                formatter,
                "{name:?} exceeded its manifest size {expected_bytes} mid-stream; the download was discarded"
            ),
            Self::AlreadyPresent { path } => {
                write!(formatter, "model already present at {path}")
            }
            Self::Source { reason } => write!(formatter, "model source failed: {reason}"),
            Self::Io { path, reason } => write!(formatter, "model I/O failed at {path}: {reason}"),
        }
    }
}

impl std::error::Error for ModelPullError {}

/// A verified pull's receipt.
#[derive(Clone, Debug)]
pub struct PullReport {
    pub name: String,
    pub path: PathBuf,
    pub bytes: u64,
    pub blake3: String,
}

struct HashingWriter {
    file: std::fs::File,
    hasher: blake3::Hasher,
    written: u64,
    limit: u64,
    head: Option<HttpHead>,
    overrun: bool,
    io_error: Option<String>,
}

impl ResponseHandler for HashingWriter {
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort> {
        self.head = Some(head.clone());
        Ok(())
    }

    fn on_chunk(&mut self, chunk: &[u8]) -> Result<(), StreamAbort> {
        if self.head.as_ref().is_none_or(|head| head.status >= 300) {
            // Error bodies are not artifacts; drop them.
            return Ok(());
        }
        if self.written.saturating_add(chunk.len() as u64) > self.limit {
            self.overrun = true;
            return Err(StreamAbort);
        }
        if let Err(error) = self.file.write_all(chunk) {
            self.io_error = Some(error.to_string());
            return Err(StreamAbort);
        }
        self.hasher.update(chunk);
        self.written += chunk.len() as u64;
        Ok(())
    }
}

/// Pulls one model by manifest entry into `dest_dir/<name>`.
///
/// # Errors
///
/// Typed [`ModelPullError`] for consent, source, size, hash, and I/O
/// failures. On any failure after bytes started landing, the temp file is
/// removed — a failed pull leaves nothing half-verified on disk.
pub fn pull_model(
    entry: &ModelManifestEntry,
    consent: PullConsent,
    dest_dir: &Path,
    transport: &dyn HttpTransport,
) -> Result<PullReport, ModelPullError> {
    if consent == PullConsent::Withheld {
        return Err(ModelPullError::ConsentRequired {
            name: entry.name.clone(),
        });
    }
    let final_path = dest_dir.join(&entry.name);
    if final_path.exists() {
        return Err(ModelPullError::AlreadyPresent {
            path: final_path.display().to_string(),
        });
    }
    std::fs::create_dir_all(dest_dir).map_err(|error| ModelPullError::Io {
        path: dest_dir.display().to_string(),
        reason: error.to_string(),
    })?;
    let temp_path = dest_dir.join(format!("{}.pulling", entry.name));
    let outcome = stream_to_temp(entry, &temp_path, transport);
    match outcome {
        Ok(report) => {
            std::fs::rename(&temp_path, &final_path).map_err(|error| ModelPullError::Io {
                path: final_path.display().to_string(),
                reason: error.to_string(),
            })?;
            Ok(PullReport {
                name: entry.name.clone(),
                path: final_path,
                bytes: report.0,
                blake3: report.1,
            })
        }
        Err(error) => {
            // Best-effort cleanup; the typed error is the primary signal.
            let _ = std::fs::remove_file(&temp_path);
            Err(error)
        }
    }
}

/// Streams the source into the temp file, returning `(bytes, blake3_hex)`
/// after verification against the manifest.
fn stream_to_temp(
    entry: &ModelManifestEntry,
    temp_path: &Path,
    transport: &dyn HttpTransport,
) -> Result<(u64, String), ModelPullError> {
    let io = |reason: String| ModelPullError::Io {
        path: temp_path.display().to_string(),
        reason,
    };
    if let Some(file_path) = entry.url.strip_prefix("file://") {
        // Local sources still stream through the same hasher/limit path.
        copy_file_source(entry, Path::new(file_path), temp_path)?;
    } else {
        let file = std::fs::File::create(temp_path).map_err(|error| io(error.to_string()))?;
        let mut writer = HashingWriter {
            file,
            hasher: blake3::Hasher::new(),
            written: 0,
            limit: entry.bytes,
            head: None,
            overrun: false,
            io_error: None,
        };
        let plan = HttpRequestPlan {
            method: crate::transport::HttpMethod::Get,
            url: entry.url.clone(),
            headers: vec![("accept", "application/octet-stream".to_owned())],
            body: Vec::new(),
            timeout_ms: DOWNLOAD_TIMEOUT_MS,
        };
        match transport.execute(&plan, &mut writer) {
            Ok(()) => {}
            Err(crate::transport::TransportError::Aborted) if writer.overrun => {
                return Err(ModelPullError::Overrun {
                    name: entry.name.clone(),
                    expected_bytes: entry.bytes,
                });
            }
            Err(crate::transport::TransportError::Aborted) => {
                return Err(io(writer
                    .io_error
                    .unwrap_or_else(|| "write aborted".to_owned())));
            }
            Err(error) => {
                return Err(ModelPullError::Source {
                    reason: error.to_string(),
                });
            }
        }
        let head = writer.head.as_ref().ok_or_else(|| ModelPullError::Source {
            reason: "source returned no response head".to_owned(),
        })?;
        if head.status >= 300 {
            return Err(ModelPullError::Source {
                reason: format!("source answered HTTP {}", head.status),
            });
        }
        writer.file.flush().map_err(|error| io(error.to_string()))?;
        return verify(entry, writer.written, writer.hasher.finalize());
    }
    // File sources verify from the landed temp file. Local fixtures are
    // small; streaming-hash unification with the HTTP path can ride the
    // m1-s03 resume work if file sources ever carry real model sizes.
    let bytes = std::fs::read(temp_path).map_err(|error| io(error.to_string()))?;
    verify(entry, bytes.len() as u64, blake3::hash(&bytes))
}

fn copy_file_source(
    entry: &ModelManifestEntry,
    source: &Path,
    temp_path: &Path,
) -> Result<(), ModelPullError> {
    let mut reader = std::fs::File::open(source).map_err(|error| ModelPullError::Source {
        reason: format!("{}: {error}", source.display()),
    })?;
    let mut writer = std::fs::File::create(temp_path).map_err(|error| ModelPullError::Io {
        path: temp_path.display().to_string(),
        reason: error.to_string(),
    })?;
    let mut written = 0_u64;
    let mut buffer = vec![0_u8; DOWNLOAD_CHUNK_BYTES];
    loop {
        use std::io::Read;
        let read = reader
            .read(&mut buffer)
            .map_err(|error| ModelPullError::Source {
                reason: error.to_string(),
            })?;
        if read == 0 {
            break;
        }
        written = written.saturating_add(read as u64);
        if written > entry.bytes {
            return Err(ModelPullError::Overrun {
                name: entry.name.clone(),
                expected_bytes: entry.bytes,
            });
        }
        writer
            .write_all(&buffer[..read])
            .map_err(|error| ModelPullError::Io {
                path: temp_path.display().to_string(),
                reason: error.to_string(),
            })?;
    }
    Ok(())
}

fn verify(
    entry: &ModelManifestEntry,
    bytes: u64,
    hash: blake3::Hash,
) -> Result<(u64, String), ModelPullError> {
    if bytes != entry.bytes {
        return Err(ModelPullError::SizeMismatch {
            name: entry.name.clone(),
            expected_bytes: entry.bytes,
            actual_bytes: bytes,
        });
    }
    let actual = hash.to_hex().to_string();
    if actual != entry.blake3.to_lowercase() {
        return Err(ModelPullError::ChecksumMismatch {
            name: entry.name.clone(),
            expected_blake3: entry.blake3.to_lowercase(),
            actual_blake3: actual,
        });
    }
    Ok((bytes, actual))
}

#[cfg(test)]
mod tests {
    use super::{ModelManifest, ModelManifestEntry, ModelPullError, PullConsent, pull_model};
    use crate::transport::{HttpRequestPlan, HttpTransport, ResponseHandler, TransportError};

    /// A transport that must never be reached: consent refusals happen
    /// before any source I/O.
    struct RefusingTransport;

    impl HttpTransport for RefusingTransport {
        fn execute(
            &self,
            _plan: &HttpRequestPlan,
            _handler: &mut dyn ResponseHandler,
        ) -> Result<(), TransportError> {
            panic!("consent must be checked before the source is touched");
        }
    }

    fn fixture_entry(
        dir: &std::path::Path,
        content: &[u8],
        lie: Option<&str>,
    ) -> ModelManifestEntry {
        let source = dir.join("artifact.bin");
        std::fs::write(&source, content).expect("fixture writes");
        ModelManifestEntry {
            name: "tiny-model".to_owned(),
            url: format!("file://{}", source.display()),
            blake3: lie
                .map(str::to_owned)
                .unwrap_or_else(|| blake3::hash(content).to_hex().to_string()),
            bytes: content.len() as u64,
        }
    }

    #[test]
    fn a_pull_without_consent_is_refused_before_any_source_io() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = fixture_entry(dir.path(), b"model-bytes", None);
        let error = pull_model(
            &entry,
            PullConsent::Withheld,
            &dir.path().join("models"),
            &RefusingTransport,
        )
        .expect_err("consent is never implicit");
        assert!(matches!(error, ModelPullError::ConsentRequired { .. }));
    }

    #[test]
    fn a_verified_pull_lands_atomically_and_a_second_is_refused() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = fixture_entry(dir.path(), b"model-bytes", None);
        let dest = dir.path().join("models");
        let report = pull_model(&entry, PullConsent::Given, &dest, &RefusingTransport)
            .expect("file source pulls without a network transport");
        assert_eq!(report.bytes, 11);
        assert!(report.path.exists());
        assert!(!dest.join("tiny-model.pulling").exists());
        let error = pull_model(&entry, PullConsent::Given, &dest, &RefusingTransport)
            .expect_err("an existing artifact is never silently overwritten");
        assert!(matches!(error, ModelPullError::AlreadyPresent { .. }));
    }

    #[test]
    fn a_tampered_artifact_is_rejected_and_the_temp_file_removed() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = fixture_entry(
            dir.path(),
            b"tampered-bytes",
            // The manifest pin for different (expected) content.
            Some(blake3::hash(b"original-bytes").to_hex().as_str()),
        );
        let dest = dir.path().join("models");
        let error = pull_model(&entry, PullConsent::Given, &dest, &RefusingTransport)
            .expect_err("wrong bytes must be refused");
        assert!(matches!(error, ModelPullError::ChecksumMismatch { .. }));
        assert!(!dest.join("tiny-model").exists());
        assert!(!dest.join("tiny-model.pulling").exists());
    }

    #[test]
    fn manifests_load_and_unknown_names_are_typed_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        let path = dir.path().join("manifest.json");
        std::fs::write(
            &path,
            r#"{"models":[{"name":"m","url":"file:///x","blake3":"aa","bytes":1}]}"#,
        )
        .expect("fixture");
        let manifest = ModelManifest::load(&path).expect("loads");
        assert!(manifest.entry("m").is_ok());
        assert!(matches!(
            manifest.entry("ghost"),
            Err(ModelPullError::UnknownModel { .. })
        ));
        std::fs::write(&path, "{not json").expect("fixture");
        assert!(matches!(
            ModelManifest::load(&path),
            Err(ModelPullError::ManifestUnreadable { .. })
        ));
    }
}
