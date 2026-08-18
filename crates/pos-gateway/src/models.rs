//! The model manager v0 (m0-s11): local models are downloaded artifacts with
//! checksums — never bundled in the binary, never fetched without consent.
//! `pos models pull <name>` resolves a name against the in-repo manifest
//! (`models/manifest.json`), streams the artifact to a temp file while
//! hashing, verifies BLAKE3 + byte count, and renames atomically. A tampered
//! artifact is refused and its temp file removed.
//!
//! ## Resume (m1-s03, the M0 debt)
//!
//! Whisper artifacts are hundreds of megabytes and users are on hotel wifi. A
//! pull that drops leaves its `.pulling` file in place; the next pull hashes
//! the bytes already on disk, asks the source for `Range: bytes=N-`, and
//! continues from there. Three rules keep that safe:
//!
//! 1. **The prefix is re-hashed, never trusted.** A partial file could be
//!    anything — a truncated download, an earlier version, a tampered file —
//!    so resuming re-reads it through the same hasher. The final BLAKE3 check
//!    then covers the whole artifact, resumed or not.
//! 2. **A source that ignores the range restarts the pull.** Answering `200`
//!    to a ranged request means the body is the whole file; appending it to a
//!    prefix would produce a corrupt artifact that only the hash would catch.
//! 3. **Only a *transport* failure leaves the partial behind.** A hash or size
//!    mismatch removes it: those bytes are known-wrong, and keeping them would
//!    make every later resume fail the same way.

use crate::transport::{HttpHead, HttpRequestPlan, HttpTransport, ResponseHandler, StreamAbort};
use serde::Deserialize;
use std::fmt;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

/// Streaming write granularity for downloads; matches the transport's read
/// buffer so a pull is two copies, not many.
const DOWNLOAD_CHUNK_BYTES: usize = 16 * 1024;

/// A model artifact download gets generous time: whisper-class files are
/// hundreds of MB on user links.
const DOWNLOAD_TIMEOUT_MS: u32 = 10 * 60 * 1_000;

/// Read granularity when re-hashing a partial file on resume.
const RESUME_HASH_CHUNK_BYTES: usize = 1024 * 1024;

/// Redirect hops one artifact pull may follow. Artifact hosts answer a stable
/// URL with a CDN location; three hops covers that and refuses a loop (L8).
const REDIRECT_HOPS_MAX: u32 = 3;

/// Default ceiling on one artifact. Larger than `whisper-large-v3` (~3.1 GB)
/// and smaller than anything that would fill a laptop by surprise. A stated
/// cap that refuses before the first byte beats a disk-full error after
/// twenty minutes (L8).
pub const MODEL_ARTIFACT_BYTES_MAX_DEFAULT: u64 = 8 * 1024 * 1024 * 1024;

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

    /// Every file one model artifact is made of, in manifest order.
    ///
    /// A whisper model is one `.bin`; an ONNX encoder is a graph *and* its
    /// vocabulary, and both must be present or neither is usable. Rather than
    /// give the entry a nested file list — which would fork the pull path,
    /// its resume rules, and its verification into single- and multi-file
    /// cases — a multi-file artifact is a **directory of names**:
    /// `bge-small-en-v1.5/model.onnx` and `bge-small-en-v1.5/vocab.txt` are
    /// two ordinary entries, each hash-pinned and resumable exactly like
    /// every other, and `pos models pull bge-small-en-v1.5` pulls both.
    ///
    /// The entry name is therefore the artifact's path under the models
    /// directory, which it already was.
    ///
    /// # Errors
    ///
    /// [`ModelPullError::UnknownModel`] when nothing matches, so a typo is a
    /// refusal rather than a silent zero-file pull.
    pub fn artifact(&self, name: &str) -> Result<Vec<&ModelManifestEntry>, ModelPullError> {
        let prefix = format!("{name}/");
        let files: Vec<&ModelManifestEntry> = self
            .models
            .iter()
            .filter(|entry| entry.name == name || entry.name.starts_with(&prefix))
            .collect();
        if files.is_empty() {
            return Err(ModelPullError::UnknownModel {
                name: name.to_owned(),
            });
        }
        Ok(files)
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
    /// The manifest's stated size is past the caller's stated budget. Refused
    /// before any I/O, so a disk that cannot hold the artifact costs nothing
    /// but the refusal (m1-s03).
    BudgetExceeded {
        name: String,
        bytes: u64,
        budget_bytes: u64,
    },
    /// The partial file on disk is longer than the manifest says the artifact
    /// is. Resuming from it would be nonsense; it is removed and the pull
    /// starts over.
    PartialTooLong {
        name: String,
        partial_bytes: u64,
        expected_bytes: u64,
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
            Self::BudgetExceeded {
                name,
                bytes,
                budget_bytes,
            } => write!(
                formatter,
                "{name:?} is {bytes} bytes, past the stated {budget_bytes}-byte artifact budget; \
                 nothing was downloaded"
            ),
            Self::PartialTooLong {
                name,
                partial_bytes,
                expected_bytes,
            } => write!(
                formatter,
                "the partial download of {name:?} is {partial_bytes} bytes but the manifest says \
                 {expected_bytes}; it was discarded and the pull will start over"
            ),
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
    /// Bytes already on disk when this request started; `0` for a fresh pull.
    resumed_from: u64,
    /// Set when a ranged request was answered with the whole file.
    range_ignored: bool,
    /// The `location` of a redirect the source answered with.
    redirect_to: Option<String>,
}

impl ResponseHandler for HashingWriter {
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort> {
        // A `200` to a ranged request means the body is the whole artifact.
        // Appending it to the prefix would build a corrupt file that only the
        // final hash would notice, so the transfer stops here and the caller
        // restarts from zero (module doc, rule 2).
        if (300..400).contains(&head.status)
            && let Some(location) = head.header("location")
        {
            self.redirect_to = Some(location.to_owned());
            self.head = Some(head.clone());
            return Err(StreamAbort);
        }
        if self.resumed_from > 0 && head.status == 200 {
            self.range_ignored = true;
            self.head = Some(head.clone());
            return Err(StreamAbort);
        }
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
/// Equivalent to [`pull_model_with_budget`] at
/// [`MODEL_ARTIFACT_BYTES_MAX_DEFAULT`].
///
/// # Errors
///
/// See [`pull_model_with_budget`].
pub fn pull_model(
    entry: &ModelManifestEntry,
    consent: PullConsent,
    dest_dir: &Path,
    transport: &dyn HttpTransport,
) -> Result<PullReport, ModelPullError> {
    pull_model_with_budget(
        entry,
        consent,
        dest_dir,
        transport,
        MODEL_ARTIFACT_BYTES_MAX_DEFAULT,
    )
}

/// Resolves a `Location` header against the URL that produced it.
///
/// Absolute `https` locations pass through; a path-absolute location keeps the
/// current origin; anything else — `http`, a scheme-relative `//host/path`, a
/// path-relative fragment — is refused rather than guessed at, because the
/// point of this check is that a redirect never downgrades the scheme and
/// never silently reaches a host the manifest did not name.
fn resolve_redirect(current: &str, location: &str) -> Option<String> {
    if location.starts_with("https://") {
        return Some(location.to_owned());
    }
    if !location.starts_with('/') || location.starts_with("//") {
        return None;
    }
    let origin_end = current.strip_prefix("https://")?.find('/')?;
    let origin = &current["https://".len().."https://".len() + origin_end];
    Some(format!("https://{origin}{location}"))
}

/// Pulls one model, refusing anything past `budget_bytes` before any I/O.
///
/// Resumes an interrupted pull from its `.pulling` file; see the module doc
/// for the three rules that make that safe.
///
/// # Errors
///
/// Typed [`ModelPullError`] for consent, budget, source, size, hash, and I/O
/// failures. A hash or size mismatch removes the partial file (those bytes are
/// known-wrong); a transport failure leaves it for the next resume.
pub fn pull_model_with_budget(
    entry: &ModelManifestEntry,
    consent: PullConsent,
    dest_dir: &Path,
    transport: &dyn HttpTransport,
    budget_bytes: u64,
) -> Result<PullReport, ModelPullError> {
    if consent == PullConsent::Withheld {
        return Err(ModelPullError::ConsentRequired {
            name: entry.name.clone(),
        });
    }
    if entry.bytes > budget_bytes {
        return Err(ModelPullError::BudgetExceeded {
            name: entry.name.clone(),
            bytes: entry.bytes,
            budget_bytes,
        });
    }
    let final_path = dest_dir.join(&entry.name);
    if final_path.exists() {
        return Err(ModelPullError::AlreadyPresent {
            path: final_path.display().to_string(),
        });
    }
    // The parent, not `dest_dir`: a multi-file artifact's entry name carries
    // a directory component (see `ModelManifest::artifact`).
    let parent = final_path.parent().unwrap_or(dest_dir);
    std::fs::create_dir_all(parent).map_err(|error| ModelPullError::Io {
        path: parent.display().to_string(),
        reason: error.to_string(),
    })?;
    let temp_path = dest_dir.join(format!("{}.pulling", entry.name));
    match stream_to_temp(entry, &temp_path, transport) {
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
            if discards_partial(&error) {
                // Best-effort cleanup; the typed error is the primary signal.
                let _ = std::fs::remove_file(&temp_path);
            }
            Err(error)
        }
    }
}

/// Whether this failure means the bytes on disk are worthless. Verification
/// failures do; a dropped connection does not, and deleting the prefix there
/// is what made "resume" impossible before m1-s03.
const fn discards_partial(error: &ModelPullError) -> bool {
    !matches!(
        error,
        ModelPullError::Source { .. } | ModelPullError::Io { .. }
    )
}

/// The bytes already on disk for this pull, and their hash state.
struct Partial {
    bytes: u64,
    hasher: blake3::Hasher,
}

/// Re-hashes an existing `.pulling` file. Returns a zero-length partial when
/// there is nothing to resume from.
fn read_partial(entry: &ModelManifestEntry, temp_path: &Path) -> Result<Partial, ModelPullError> {
    let mut hasher = blake3::Hasher::new();
    let Ok(mut file) = std::fs::File::open(temp_path) else {
        return Ok(Partial { bytes: 0, hasher });
    };
    let mut bytes = 0_u64;
    let mut buffer = vec![0_u8; RESUME_HASH_CHUNK_BYTES];
    loop {
        let read = file.read(&mut buffer).map_err(|error| ModelPullError::Io {
            path: temp_path.display().to_string(),
            reason: format!("re-hash the partial download: {error}"),
        })?;
        if read == 0 {
            break;
        }
        hasher.update(&buffer[..read]);
        bytes = bytes.saturating_add(read as u64);
    }
    if bytes > entry.bytes {
        return Err(ModelPullError::PartialTooLong {
            name: entry.name.clone(),
            partial_bytes: bytes,
            expected_bytes: entry.bytes,
        });
    }
    Ok(Partial { bytes, hasher })
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
        // File sources verify from the landed temp file. Local fixtures are
        // small; the streaming-hash path above is what real artifacts take.
        let bytes = std::fs::read(temp_path).map_err(|error| io(error.to_string()))?;
        return verify(entry, bytes.len() as u64, blake3::hash(&bytes));
    }

    // Iterative rather than recursive (STYLE): a redirect chain and a
    // restart-from-zero are both "try again with different state", and one
    // bounded loop states both bounds in one place.
    let mut url = entry.url.clone();
    let mut hops = 0_u32;
    let mut restarts = 0_u32;
    loop {
        // Rule 1: the prefix is re-hashed, never trusted.
        let partial = read_partial(entry, temp_path)?;
        let resuming = partial.bytes > 0 && partial.bytes < entry.bytes;
        let file = if resuming {
            std::fs::OpenOptions::new()
                .append(true)
                .open(temp_path)
                .map_err(|error| io(error.to_string()))?
        } else {
            std::fs::File::create(temp_path).map_err(|error| io(error.to_string()))?
        };
        let mut writer = HashingWriter {
            file,
            hasher: if resuming {
                partial.hasher
            } else {
                blake3::Hasher::new()
            },
            written: if resuming { partial.bytes } else { 0 },
            limit: entry.bytes,
            head: None,
            overrun: false,
            io_error: None,
            resumed_from: if resuming { partial.bytes } else { 0 },
            range_ignored: false,
            redirect_to: None,
        };
        let mut headers = vec![("accept", "application/octet-stream".to_owned())];
        if resuming {
            headers.push(("range", format!("bytes={}-", partial.bytes)));
        }
        let plan = HttpRequestPlan {
            method: crate::transport::HttpMethod::Get,
            url: url.clone(),
            headers,
            body: Vec::new(),
            timeout_ms: DOWNLOAD_TIMEOUT_MS,
            // Derived from the manifest's own declared size, not from the
            // transport's text-shaped default — every artifact in the catalog
            // is larger than that, so the default would refuse all of them
            // (m1-s04 found this: no HTTPS pull could finish). The `+1` lets
            // the transport deliver one byte past the declaration so the
            // size-mismatch check below reports an oversized artifact by name
            // rather than as a framing violation.
            response_bytes_max: Some(entry.bytes.saturating_add(1)),
        };
        // A redirect and an ignored range both abort the transfer on purpose;
        // both are handled below, from the head the writer kept.
        match transport.execute(&plan, &mut writer) {
            Ok(()) => {}
            Err(crate::transport::TransportError::Aborted)
                if writer.range_ignored || writer.redirect_to.is_some() => {}
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

        // Artifact hosts redirect: the manifest names a stable URL and the
        // host answers with a CDN location. Following it is safe *here* and
        // nowhere else — a model pull carries no credential, so a redirect
        // leaks nothing, and the artifact is BLAKE3-pinned, so a redirect to a
        // hostile host still cannot deliver different bytes. The transport
        // itself follows none (`tls.rs`), which is why this is explicit.
        if let Some(location) = writer.redirect_to.clone() {
            hops = hops.saturating_add(1);
            if hops > REDIRECT_HOPS_MAX {
                return Err(ModelPullError::Source {
                    reason: format!(
                        "artifact source redirected more than {REDIRECT_HOPS_MAX} times"
                    ),
                });
            }
            // A relative `Location` is resolved against the current origin.
            // That is *more* restrictive than an absolute one, not less: it
            // cannot change host, so the redirect stays inside the origin the
            // manifest named. HuggingFace answers with one of these, which is
            // how m1-s04 found this path refusing every artifact it hosts.
            url = match resolve_redirect(&url, &location) {
                Some(next) => next,
                None => {
                    return Err(ModelPullError::Source {
                        reason: format!(
                            "artifact source redirected to a non-https location: {location}"
                        ),
                    });
                }
            };
            continue;
        }
        if head.status >= 300 {
            return Err(ModelPullError::Source {
                reason: format!("source answered HTTP {}", head.status),
            });
        }
        // Rule 2: a source that ignored the range sent the whole file. The
        // writer refused to append it (see `on_head`); starting over is the
        // only correct answer.
        if writer.range_ignored {
            restarts = restarts.saturating_add(1);
            if restarts > 1 {
                return Err(ModelPullError::Source {
                    reason: "artifact source keeps ignoring Range; the pull cannot resume"
                        .to_owned(),
                });
            }
            std::fs::remove_file(temp_path).map_err(|error| io(error.to_string()))?;
            continue;
        }
        writer.file.flush().map_err(|error| io(error.to_string()))?;
        return verify(entry, writer.written, writer.hasher.finalize());
    }
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

    /// An HTTP source that honours `Range`, drops the connection partway
    /// through the first attempt, and serves the rest on the second — the
    /// exact shape of a pull over hotel wifi.
    struct FlakyRangeSource {
        content: Vec<u8>,
        /// Bytes to deliver before dropping, on the first request only.
        first_attempt_bytes: usize,
        attempts: std::cell::Cell<u32>,
        /// Every `range` header value the source was asked for.
        ranges: std::cell::RefCell<Vec<Option<String>>>,
    }

    impl FlakyRangeSource {
        fn new(content: &[u8], first_attempt_bytes: usize) -> Self {
            Self {
                content: content.to_vec(),
                first_attempt_bytes,
                attempts: std::cell::Cell::new(0),
                ranges: std::cell::RefCell::new(Vec::new()),
            }
        }
    }

    impl HttpTransport for FlakyRangeSource {
        fn execute(
            &self,
            plan: &HttpRequestPlan,
            handler: &mut dyn ResponseHandler,
        ) -> Result<(), TransportError> {
            let range = plan
                .headers
                .iter()
                .find(|(name, _)| *name == "range")
                .map(|(_, value)| value.clone());
            self.ranges.borrow_mut().push(range.clone());
            let attempt = self.attempts.get() + 1;
            self.attempts.set(attempt);
            let start: usize = range
                .as_deref()
                .and_then(|value| value.strip_prefix("bytes="))
                .and_then(|value| value.trim_end_matches('-').parse().ok())
                .unwrap_or(0);
            let status = if start > 0 { 206 } else { 200 };
            handler
                .on_head(&crate::transport::HttpHead {
                    status,
                    headers: Vec::new(),
                })
                .map_err(|_| TransportError::Aborted)?;
            let body = &self.content[start.min(self.content.len())..];
            let deliver = if attempt == 1 {
                body.len().min(self.first_attempt_bytes)
            } else {
                body.len()
            };
            handler
                .on_chunk(&body[..deliver])
                .map_err(|_| TransportError::Aborted)?;
            if deliver < body.len() {
                return Err(TransportError::Io {
                    reason: "connection dropped".to_owned(),
                });
            }
            Ok(())
        }
    }

    /// A source that ignores `Range` and always sends the whole artifact —
    /// common enough among CDNs and object stores to be worth a test.
    struct RangeIgnoringSource {
        content: Vec<u8>,
        requests: std::cell::Cell<u32>,
    }

    impl HttpTransport for RangeIgnoringSource {
        fn execute(
            &self,
            _plan: &HttpRequestPlan,
            handler: &mut dyn ResponseHandler,
        ) -> Result<(), TransportError> {
            self.requests.set(self.requests.get() + 1);
            handler
                .on_head(&crate::transport::HttpHead {
                    status: 200,
                    headers: Vec::new(),
                })
                .map_err(|_| TransportError::Aborted)?;
            handler
                .on_chunk(&self.content)
                .map_err(|_| TransportError::Aborted)?;
            Ok(())
        }
    }

    fn http_entry(content: &[u8]) -> ModelManifestEntry {
        ModelManifestEntry {
            name: "tiny-model".to_owned(),
            url: "http://127.0.0.1:1/artifact.bin".to_owned(),
            blake3: blake3::hash(content).to_hex().to_string(),
            bytes: content.len() as u64,
        }
    }

    #[test]
    fn a_dropped_pull_keeps_its_partial_and_the_next_one_resumes_from_it() {
        // The M0 debt this story closes. Two properties, and the second is the
        // one that matters: the resumed request asks for exactly the bytes it
        // is missing, so a 500 MB artifact is not downloaded twice.
        let content: Vec<u8> = (0..4_000_u32).map(|index| (index % 251) as u8).collect();
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("models");
        let entry = http_entry(&content);
        let source = FlakyRangeSource::new(&content, 1_500);

        let error = pull_model(&entry, PullConsent::Given, &dest, &source)
            .expect_err("the first attempt drops mid-stream");
        assert!(
            matches!(error, ModelPullError::Source { .. }),
            "a dropped connection is a source failure, not corrupt bytes: {error:?}"
        );
        let partial = dest.join("tiny-model.pulling");
        assert!(
            partial.is_file(),
            "a dropped transfer must leave its bytes for the resume"
        );
        assert_eq!(
            std::fs::metadata(&partial).expect("partial").len(),
            1_500,
            "the partial holds exactly what arrived"
        );

        let report = pull_model(&entry, PullConsent::Given, &dest, &source)
            .expect("the second attempt resumes and completes");
        assert_eq!(report.bytes, content.len() as u64);
        assert_eq!(report.blake3, entry.blake3);
        assert!(!partial.exists(), "a finished pull leaves no temp file");
        assert_eq!(
            source.ranges.borrow().as_slice(),
            &[None, Some("bytes=1500-".to_owned())],
            "the resume must request only the missing bytes"
        );
    }

    #[test]
    fn a_source_that_ignores_the_range_restarts_the_pull_rather_than_corrupting_it() {
        let content: Vec<u8> = (0..3_000_u32).map(|index| (index % 251) as u8).collect();
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("models");
        let entry = http_entry(&content);

        // Leave a plausible partial behind, as a dropped transfer would.
        std::fs::create_dir_all(&dest).expect("dest");
        std::fs::write(dest.join("tiny-model.pulling"), &content[..1_000]).expect("partial");

        let source = RangeIgnoringSource {
            content: content.clone(),
            requests: std::cell::Cell::new(0),
        };
        let report = pull_model(&entry, PullConsent::Given, &dest, &source)
            .expect("an ignored range must still produce a correct artifact");
        assert_eq!(report.blake3, entry.blake3);
        assert_eq!(
            source.requests.get(),
            2,
            "the ranged attempt is abandoned and the pull starts over exactly once"
        );
        assert_eq!(
            std::fs::read(&report.path).expect("artifact"),
            content,
            "the artifact must be the file, not the file appended to a prefix"
        );
    }

    #[test]
    fn a_verification_failure_discards_the_partial_so_a_resume_cannot_inherit_it() {
        let content = b"tampered-bytes";
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("models");
        let entry = ModelManifestEntry {
            name: "tiny-model".to_owned(),
            url: "http://127.0.0.1:1/artifact.bin".to_owned(),
            blake3: blake3::hash(b"original-bytes").to_hex().to_string(),
            bytes: content.len() as u64,
        };
        let source = FlakyRangeSource::new(content, content.len());
        let error = pull_model(&entry, PullConsent::Given, &dest, &source)
            .expect_err("wrong bytes are refused");
        assert!(matches!(error, ModelPullError::ChecksumMismatch { .. }));
        assert!(
            !dest.join("tiny-model.pulling").exists(),
            "known-wrong bytes must not be kept: every later resume would fail the same way"
        );
    }

    #[test]
    fn an_artifact_past_the_stated_budget_is_refused_before_any_io() {
        let dir = tempfile::tempdir().expect("tempdir");
        let entry = fixture_entry(dir.path(), b"model-bytes", None);
        let error = super::pull_model_with_budget(
            &entry,
            PullConsent::Given,
            &dir.path().join("models"),
            &RefusingTransport,
            4,
        )
        .expect_err("a stated disk budget refuses before the first byte");
        assert!(
            matches!(&error, ModelPullError::BudgetExceeded { bytes, budget_bytes, .. }
                if *bytes == 11 && *budget_bytes == 4),
            "got {error:?}"
        );
        assert!(!dir.path().join("models").exists(), "nothing was created");
    }

    #[test]
    fn a_partial_longer_than_the_manifest_is_discarded_rather_than_resumed() {
        let content = b"exactly-this";
        let dir = tempfile::tempdir().expect("tempdir");
        let dest = dir.path().join("models");
        std::fs::create_dir_all(&dest).expect("dest");
        std::fs::write(
            dest.join("tiny-model.pulling"),
            b"far too many bytes for this manifest",
        )
        .expect("partial");
        let entry = http_entry(content);
        let source = FlakyRangeSource::new(content, content.len());
        let error = pull_model(&entry, PullConsent::Given, &dest, &source)
            .expect_err("a partial longer than the artifact is nonsense");
        assert!(matches!(error, ModelPullError::PartialTooLong { .. }));
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
