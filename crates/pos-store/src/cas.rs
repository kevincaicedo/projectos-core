//! BLAKE3 content-addressed blob store (m0-s04, F2/F6 substrate): streaming
//! bounded writes into a temp file, atomic rename into a two-level fan-out,
//! dedup by construction, and a verify sweep that re-hashes what is on disk.
//! Large files never sit fully in memory (L8).

use crate::StoreError;
use crate::fault::{FaultPlan, FaultPoint};
use std::fmt;
use std::fs;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};

/// Streaming-write memory bound (L8, m0-s04 AC): a CAS write holds at most
/// this much content in memory regardless of blob size.
pub const CAS_WRITE_BUFFER_CAP: usize = 8 * 1024 * 1024;

/// Chunk size for streaming reads (verify, `write_stream`): large enough to
/// amortize syscalls, small enough to be irrelevant against the 8 MiB cap.
const CAS_READ_CHUNK_LEN: usize = 64 * 1024;

/// Verify reports name at most this many defective paths; the full count is
/// always exact. An unbounded defect list on a hostile disk is its own OOM.
const VERIFY_DEFECT_REPORT_MAX: usize = 64;

/// The blob directory inside a project (§7.2).
pub const BLOBS_DIRECTORY_NAME: &str = "blobs";
/// In-flight writes; contents are disposable by definition after a crash.
const TEMP_DIRECTORY_NAME: &str = "tmp";

/// A BLAKE3 content hash — the identity of a blob.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BlobHash(blake3::Hash);

impl BlobHash {
    #[must_use]
    pub fn to_hex(self) -> String {
        self.0.to_hex().to_string()
    }

    #[must_use]
    pub fn from_hex(text: &str) -> Option<Self> {
        if text.len() != 64
            || !text
                .bytes()
                .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
        {
            return None;
        }
        blake3::Hash::from_hex(text).ok().map(Self)
    }

    #[must_use]
    pub fn of_bytes(bytes: &[u8]) -> Self {
        Self(blake3::hash(bytes))
    }

    #[must_use]
    pub fn into_bytes(self) -> [u8; 32] {
        *self.0.as_bytes()
    }
}

impl fmt::Debug for BlobHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "BlobHash({})", self.to_hex())
    }
}

impl fmt::Display for BlobHash {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_hex())
    }
}

/// The verify sweep's honest result: exact counts, bounded detail (L8 — the
/// bound is visible, never a silent truncation).
#[derive(Debug, Default)]
pub struct CasVerifyReport {
    pub blob_count: u64,
    pub corrupt_count: u64,
    pub misplaced_count: u64,
    pub temp_leftover_count: u64,
    /// Up to [`VERIFY_DEFECT_REPORT_MAX`] defective paths, lexicographic.
    pub defect_paths: Vec<PathBuf>,
    pub defect_paths_truncated: bool,
}

impl CasVerifyReport {
    #[must_use]
    pub fn is_clean(&self) -> bool {
        self.corrupt_count == 0 && self.misplaced_count == 0
    }

    fn record_defect(&mut self, path: PathBuf) {
        if self.defect_paths.len() < VERIFY_DEFECT_REPORT_MAX {
            self.defect_paths.push(path);
        } else {
            self.defect_paths_truncated = true;
        }
    }
}

/// The content-addressed store rooted at `<project>/blobs`.
pub struct BlobStore {
    root: PathBuf,
    faults: Option<FaultPlan>,
}

impl BlobStore {
    /// Opens (creating if needed) the store and clears crash leftovers from
    /// the temp directory — an interrupted write must never look like data.
    pub fn open(project_root: &Path, faults: Option<FaultPlan>) -> Result<Self, StoreError> {
        let root = project_root.join(BLOBS_DIRECTORY_NAME);
        let temp = root.join(TEMP_DIRECTORY_NAME);
        fs::create_dir_all(&temp).map_err(|source| StoreError::Io {
            context: "create blob directories",
            path: temp.clone(),
            source,
        })?;
        let entries = fs::read_dir(&temp).map_err(|source| StoreError::Io {
            context: "sweep blob temp directory",
            path: temp.clone(),
            source,
        })?;
        let active_process_prefix = format!("write-{}-", process::id());
        for entry in entries {
            let entry = entry.map_err(|source| StoreError::Io {
                context: "sweep blob temp directory",
                path: temp.clone(),
                source,
            })?;
            // Another ProjectStore handle in this process may currently own
            // this writer. Crash leftovers come from a dead process id and
            // are swept; current-process files are removed by BlobWriter
            // finish/drop, never by an unrelated read-side reopen.
            if entry
                .file_name()
                .to_str()
                .is_some_and(|name| name.starts_with(&active_process_prefix))
            {
                continue;
            }
            // Best-effort: a leftover that cannot be removed is reported by
            // verify(), not a reason to refuse opening the project.
            let _ = fs::remove_file(entry.path());
        }
        Ok(Self { root, faults })
    }

    /// Starts a streaming write. Content is hashed as it arrives; the blob's
    /// identity exists only when [`BlobWriter::finish`] returns.
    pub fn writer(&self) -> Result<BlobWriter<'_>, StoreError> {
        static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);
        let unique = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let temp_path = self
            .root
            .join(TEMP_DIRECTORY_NAME)
            .join(format!("write-{}-{unique}", process::id()));
        let file = fs::File::create(&temp_path).map_err(|source| StoreError::Io {
            context: "create blob temp file",
            path: temp_path.clone(),
            source,
        })?;
        Ok(BlobWriter {
            store: self,
            hasher: blake3::Hasher::new(),
            file: Some(file),
            temp_path,
            buffer: Vec::new(),
            buffered_len_max: 0,
        })
    }

    /// One-call convenience for content already in memory.
    pub fn write_bytes(&self, bytes: &[u8]) -> Result<BlobHash, StoreError> {
        let mut writer = self.writer()?;
        writer.append(bytes)?;
        writer.finish()
    }

    /// Streams `reader` to a blob under the fixed read-chunk bound.
    pub fn write_stream(&self, reader: &mut impl Read) -> Result<BlobHash, StoreError> {
        let mut writer = self.writer()?;
        let mut chunk = vec![0_u8; CAS_READ_CHUNK_LEN];
        loop {
            let read_len = reader.read(&mut chunk).map_err(|source| StoreError::Io {
                context: "read blob input stream",
                path: self.root.clone(),
                source,
            })?;
            if read_len == 0 {
                break;
            }
            writer.append(&chunk[..read_len])?;
        }
        writer.finish()
    }

    #[must_use]
    pub fn contains(&self, hash: BlobHash) -> bool {
        self.blob_path(hash).is_file()
    }

    /// Opens a blob for streaming read.
    pub fn open_blob(&self, hash: BlobHash) -> Result<fs::File, StoreError> {
        let path = self.blob_path(hash);
        fs::File::open(&path).map_err(|source| {
            if source.kind() == std::io::ErrorKind::NotFound {
                StoreError::BlobMissing { hash }
            } else {
                StoreError::Io {
                    context: "open blob",
                    path,
                    source,
                }
            }
        })
    }

    /// Re-hashes one blob against its address.
    pub fn verify_blob(&self, hash: BlobHash) -> Result<(), StoreError> {
        let path = self.blob_path(hash);
        let actual = self.rehash_file(&path)?;
        if actual == hash {
            Ok(())
        } else {
            Err(StoreError::BlobCorrupt {
                path,
                expected: hash,
                actual,
            })
        }
    }

    /// Copies every stored blob into `destination_root` at its identical
    /// fan-out path (the export path, F45). Returns the blob count.
    pub fn copy_all_into(&self, destination_root: &Path) -> Result<u64, StoreError> {
        let mut copied = 0_u64;
        fs::create_dir_all(destination_root.join(TEMP_DIRECTORY_NAME)).map_err(|source| {
            StoreError::Io {
                context: "create export blob directories",
                path: destination_root.to_path_buf(),
                source,
            }
        })?;
        for (path, _) in self.sorted_blob_files()? {
            let relative = path.strip_prefix(&self.root).map_err(|_| StoreError::Io {
                context: "resolve blob export path",
                path: path.clone(),
                source: std::io::Error::other("blob escaped the store root"),
            })?;
            let destination = destination_root.join(relative);
            let parent = destination
                .parent()
                .expect("fan-out paths always have a parent"); // INVARIANT: sorted_blob_files yields paths two levels below the root.
            fs::create_dir_all(parent).map_err(|source| StoreError::Io {
                context: "create export fan-out directory",
                path: parent.to_path_buf(),
                source,
            })?;
            fs::copy(&path, &destination).map_err(|source| StoreError::Io {
                context: "copy blob into export",
                path: destination.clone(),
                source,
            })?;
            copied += 1;
        }
        Ok(copied)
    }

    /// Full integrity sweep: every stored blob re-hashed, misplaced files and
    /// temp leftovers counted. Deterministic (sorted) traversal order.
    pub fn verify(&self) -> Result<CasVerifyReport, StoreError> {
        let mut report = CasVerifyReport {
            temp_leftover_count: self.count_temp_leftovers()?,
            ..CasVerifyReport::default()
        };
        for (path, expected) in self.sorted_blob_files()? {
            report.blob_count += 1;
            let Some(expected) = expected else {
                report.misplaced_count += 1;
                report.record_defect(path);
                continue;
            };
            let actual = self.rehash_file(&path)?;
            if actual != expected {
                report.corrupt_count += 1;
                report.record_defect(path);
            }
        }
        Ok(report)
    }

    fn count_temp_leftovers(&self) -> Result<u64, StoreError> {
        let temp = self.root.join(TEMP_DIRECTORY_NAME);
        if !temp.is_dir() {
            return Ok(0);
        }
        let entries = fs::read_dir(&temp).map_err(|source| StoreError::Io {
            context: "list blob temp directory",
            path: temp,
            source,
        })?;
        Ok(entries.count() as u64)
    }

    /// Every regular file under the two-level fan-out, paired with the hash
    /// its location claims (`None` when the name or fan-out is malformed).
    fn sorted_blob_files(&self) -> Result<Vec<(PathBuf, Option<BlobHash>)>, StoreError> {
        let mut files = Vec::new();
        for first in sorted_directory_entries(&self.root)? {
            let first_name = file_name_string(&first);
            if first_name == TEMP_DIRECTORY_NAME || !first.is_dir() {
                continue;
            }
            for second in sorted_directory_entries(&first)? {
                if !second.is_dir() {
                    files.push((second, None));
                    continue;
                }
                let second_name = file_name_string(&second);
                for blob in sorted_directory_entries(&second)? {
                    let claimed = BlobHash::from_hex(&file_name_string(&blob)).filter(|hash| {
                        let hex = hash.to_hex();
                        hex.starts_with(&first_name) && hex[2..].starts_with(&second_name)
                    });
                    files.push((blob, claimed));
                }
            }
        }
        Ok(files)
    }

    fn rehash_file(&self, path: &Path) -> Result<BlobHash, StoreError> {
        let mut file = fs::File::open(path).map_err(|source| StoreError::Io {
            context: "open blob for verification",
            path: path.to_path_buf(),
            source,
        })?;
        let mut hasher = blake3::Hasher::new();
        let mut chunk = vec![0_u8; CAS_READ_CHUNK_LEN];
        loop {
            let read_len = file.read(&mut chunk).map_err(|source| StoreError::Io {
                context: "read blob for verification",
                path: path.to_path_buf(),
                source,
            })?;
            if read_len == 0 {
                break;
            }
            hasher.update(&chunk[..read_len]);
        }
        Ok(BlobHash(hasher.finalize()))
    }

    fn blob_path(&self, hash: BlobHash) -> PathBuf {
        let hex = hash.to_hex();
        // Two-level fan-out (§7.2): 256 × 256 directories keep listings small
        // at millions of blobs without a third level nobody needs yet.
        self.root.join(&hex[0..2]).join(&hex[2..4]).join(hex)
    }
}

/// A streaming blob write: bounded buffer, temp file, atomic publication.
pub struct BlobWriter<'store> {
    store: &'store BlobStore,
    hasher: blake3::Hasher,
    file: Option<fs::File>,
    temp_path: PathBuf,
    buffer: Vec<u8>,
    buffered_len_max: usize,
}

impl BlobWriter<'_> {
    /// Appends content, flushing to disk whenever the bounded buffer fills.
    pub fn append(&mut self, content: &[u8]) -> Result<(), StoreError> {
        for piece in content.chunks(CAS_WRITE_BUFFER_CAP) {
            if self.buffer.len() + piece.len() > CAS_WRITE_BUFFER_CAP {
                self.flush_buffer()?;
            }
            self.buffer.extend_from_slice(piece);
            self.buffered_len_max = self.buffered_len_max.max(self.buffer.len());
            debug_assert!(
                self.buffer.len() <= CAS_WRITE_BUFFER_CAP,
                "CAS write buffer exceeded its stated cap"
            );
        }
        Ok(())
    }

    /// Largest in-memory content the writer ever held — the observable side
    /// of the L8 cap, asserted by the m0-s04 memory test.
    #[must_use]
    pub fn buffered_len_max(&self) -> usize {
        self.buffered_len_max
    }

    /// Syncs the temp file and atomically publishes it at its content
    /// address. Identical content already present is a no-op (dedup).
    pub fn finish(mut self) -> Result<BlobHash, StoreError> {
        self.flush_buffer()?;
        let file = self
            .file
            .take()
            .expect("finish consumes self; the file is present until here"); // INVARIANT: `file` is only taken by finish/drop, and finish owns self.
        file.sync_all().map_err(|source| StoreError::Io {
            context: "sync blob temp file",
            path: self.temp_path.clone(),
            source,
        })?;
        drop(file);
        let hash = BlobHash(self.hasher.finalize());
        crate::fault::trip(self.store.faults.as_ref(), FaultPoint::CasTempWritten)?;

        let final_path = self.store.blob_path(hash);
        if final_path.is_file() {
            // Same content, same address: the earlier copy wins, this write
            // evaporates. Content-addressing makes this equality, not a race.
            let _ = fs::remove_file(&self.temp_path);
            return Ok(hash);
        }
        let parent = final_path
            .parent()
            .expect("blob paths always have a fan-out parent"); // INVARIANT: blob_path joins two directory levels below the store root.
        fs::create_dir_all(parent).map_err(|source| StoreError::Io {
            context: "create blob fan-out directory",
            path: parent.to_path_buf(),
            source,
        })?;
        fs::rename(&self.temp_path, &final_path).map_err(|source| StoreError::Io {
            context: "publish blob",
            path: final_path.clone(),
            source,
        })?;
        crate::fault::trip(self.store.faults.as_ref(), FaultPoint::CasRenamed)?;
        sync_directory(parent)?;
        Ok(hash)
    }

    fn flush_buffer(&mut self) -> Result<(), StoreError> {
        if self.buffer.is_empty() {
            return Ok(());
        }
        let file = self
            .file
            .as_mut()
            .expect("flush only runs while the writer owns its file"); // INVARIANT: `file` is only taken by finish/drop.
        self.hasher.update(&self.buffer);
        file.write_all(&self.buffer)
            .map_err(|source| StoreError::Io {
                context: "write blob temp file",
                path: self.temp_path.clone(),
                source,
            })?;
        self.buffer.clear();
        Ok(())
    }
}

impl Drop for BlobWriter<'_> {
    fn drop(&mut self) {
        // An unfinished write must not survive as a temp leftover for longer
        // than necessary; open() sweeps whatever a crash leaves anyway.
        if self.file.take().is_some() {
            let _ = fs::remove_file(&self.temp_path);
        }
    }
}

/// Directory fsync makes the rename itself durable. POSIX-only by design:
/// macOS and Linux are the M0 reference platforms; the Windows story arrives
/// with its packaging milestone and must revisit this.
fn sync_directory(path: &Path) -> Result<(), StoreError> {
    #[cfg(unix)]
    {
        let directory = fs::File::open(path).map_err(|source| StoreError::Io {
            context: "open directory for sync",
            path: path.to_path_buf(),
            source,
        })?;
        directory.sync_all().map_err(|source| StoreError::Io {
            context: "sync directory",
            path: path.to_path_buf(),
            source,
        })?;
    }
    #[cfg(not(unix))]
    {
        let _ = path;
    }
    Ok(())
}

fn sorted_directory_entries(path: &Path) -> Result<Vec<PathBuf>, StoreError> {
    let entries = fs::read_dir(path).map_err(|source| StoreError::Io {
        context: "list blob directory",
        path: path.to_path_buf(),
        source,
    })?;
    let mut paths = Vec::new();
    for entry in entries {
        let entry = entry.map_err(|source| StoreError::Io {
            context: "list blob directory",
            path: path.to_path_buf(),
            source,
        })?;
        paths.push(entry.path());
    }
    paths.sort();
    Ok(paths)
}

fn file_name_string(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}
