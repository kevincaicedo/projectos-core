//! Intake (m1-s07): a file on disk becomes Evidence.
//!
//! This is the front door of the whole pipeline — the seam a drag-drop, a
//! file picker, a batch import, and the `pos-bench` gate scenarios all enter
//! through. RAW itself lives in [`crate::pipeline::IngestPipeline::submit`];
//! what this module owns is everything that has to be decided *before* the
//! bytes are streamed: what the file is, how big it is allowed to be, and
//! which files a folder import actually covers.
//!
//! ## Two rules
//!
//! 1. **What a file *is* comes from its bytes, never from its name.** An
//!    extension is a claim made by whoever named the file, and ingested
//!    content is data rather than instruction (L6). A `notes.txt` holding an
//!    MP4 is transcribed; a `recording.mp4` holding prose is not handed to a
//!    decoder that would refuse it. Sniffing reads a bounded prefix, so
//!    classifying a 4 GB video costs exactly what classifying a note costs.
//! 2. **A batch is bounded before it starts.** The walk states a file count,
//!    a directory depth, and a per-file size, and it *reports* what it left
//!    out. Silent truncation reads as completeness and is therefore a lie
//!    (L8, and L3's engineering twin).
//!
//! ## Why there are two size caps
//!
//! [`INTAKE_FILE_BYTES_MAX`] bounds what the CAS will store. Text-shaped
//! media gets the stricter [`INTAKE_TEXT_BYTES_MAX`], because normalized text
//! becomes chunks and CHUNK's own [`crate::CHUNK_COUNT_MAX`] caps one item at
//! roughly 1.2 GB of text. Refusing an oversized text file here — where a
//! human is watching and can split it — beats accepting it and dead-lettering
//! it two stages later against a limit they never saw.

use crate::IngestError;
use crate::normalize::sniff_media_kind;
use pos_domain::{EvidenceShape, MediaKind};
use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};

/// Bytes read from the head of a file to decide what it is. Every magic
/// number this module knows lives in the first 16 bytes; the rest is slack
/// for the text sniffer, which wants whole lines.
pub const SNIFF_PREFIX_BYTES: usize = 4096;

/// Bytes one uploaded file may carry. The largest artifact the pipeline is
/// gated on is the 8 GB single file of the §18 buffer row, and the largest
/// thing a partner realistically drops is a multi-hour 4K recording; sixteen
/// gibibytes clears both without "no limit" being the answer (L8).
pub const INTAKE_FILE_BYTES_MAX: u64 = 16 * 1024 * 1024 * 1024;

/// Bytes a *text-shaped* file may carry.
///
/// Derived, not chosen: [`crate::CHUNK_COUNT_MAX`] is 4,000,000 chunks at a
/// 300-token target, which is about 1.2 GB of normalized text for one item.
/// One gibibytes sits under that with room for the newline the segment writer
/// adds per record, so a file this path accepts cannot dead-letter at CHUNK
/// against a limit the person who dropped it never saw.
pub const INTAKE_TEXT_BYTES_MAX: u64 = 1024 * 1024 * 1024;

/// Files one batch import covers. A folder import is a convenience over
/// drag-drop, not a migration tool: two thousand recordings is far past what
/// a partner drops at once, and the refusal names the count rather than
/// stopping quietly.
pub const INTAKE_FILE_COUNT_MAX: usize = 2_000;

/// Directory levels a batch import descends. Deep enough for
/// `Interviews/2026/Q3/`, shallow enough that a symlink-free walk of a home
/// directory cannot become the work itself.
pub const INTAKE_DEPTH_MAX: usize = 8;

/// What a batch import found, and what it deliberately left out.
#[derive(Clone, Debug, Default)]
pub struct IntakePlan {
    /// Regular files to submit, in a deterministic order: sorted by name
    /// within each directory, directories walked after their files. Two runs
    /// over the same tree therefore submit in the same order, which is what
    /// makes a re-import a visible sequence of duplicates rather than a
    /// reshuffle.
    pub files: Vec<PathBuf>,
    /// Entries skipped by the walk's own rules — dot-files, symlinks, and
    /// nested `.pos` projects. Counted rather than listed: the count is what
    /// tells a user "this folder held more than I took".
    pub skipped_count: u32,
    /// Whether [`INTAKE_FILE_COUNT_MAX`] or [`INTAKE_DEPTH_MAX`] stopped the
    /// walk short of the whole tree.
    pub truncated: bool,
}

/// Characters an intake-derived title keeps. A file name is untrusted text
/// that ends up on screen; two hundred characters is longer than any real one
/// and short enough that a crafted name cannot become the layout (L6).
pub const INTAKE_TITLE_CHARS_MAX: usize = 200;

/// One file, opened and classified from its own first bytes.
pub struct IntakeFile {
    /// Positioned at byte zero: the prefix read for sniffing is rewound, so
    /// the caller streams the whole file and not the tail of it.
    pub content: File,
    pub media_kind: MediaKind,
    pub shape: EvidenceShape,
    pub byte_size: u64,
    /// When the bytes last changed on disk, in Unix milliseconds. This is the
    /// honest `occurred_ts_ms` for an upload — an interview happened when it
    /// was recorded, not when somebody got around to dropping it in. `None`
    /// when the filesystem does not report one, which is the caller's cue to
    /// fall back to its own clock rather than invent a timestamp here.
    pub modified_ms: Option<u64>,
}

/// Opens `path`, decides what it holds from a bounded prefix of its own
/// bytes, and rewinds it for streaming.
///
/// # Errors
///
/// [`IngestError::Io`] when the path is not a readable regular file, and
/// [`IngestError::LimitExceeded`] when it is over the size cap its media kind
/// carries.
pub fn open_file(path: &Path) -> Result<IntakeFile, IngestError> {
    let metadata =
        std::fs::metadata(path).map_err(|source| io_error("stat an intake file", source))?;
    if !metadata.is_file() {
        return Err(io_error(
            "open an intake file",
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "only regular files are ingested; directories are walked and \
                 symlinks are skipped",
            ),
        ));
    }
    let byte_size = metadata.len();
    if byte_size > INTAKE_FILE_BYTES_MAX {
        return Err(IngestError::LimitExceeded {
            limit: "intake file bytes",
            value: byte_size,
            limit_value: INTAKE_FILE_BYTES_MAX,
        });
    }
    let mut content = File::open(path).map_err(|source| io_error("open an intake file", source))?;
    let mut prefix = [0_u8; SNIFF_PREFIX_BYTES];
    let filled = read_prefix(&mut content, &mut prefix)?;
    content
        .seek(SeekFrom::Start(0))
        .map_err(|source| io_error("rewind an intake file", source))?;
    let media_kind = sniff_intake(&prefix[..filled]);
    let shape = shape_for(media_kind);
    if is_text(media_kind) && byte_size > INTAKE_TEXT_BYTES_MAX {
        return Err(IngestError::LimitExceeded {
            limit: "intake text bytes",
            value: byte_size,
            limit_value: INTAKE_TEXT_BYTES_MAX,
        });
    }
    Ok(IntakeFile {
        content,
        media_kind,
        shape,
        byte_size,
        modified_ms: modified_ms(&metadata),
    })
}

/// The file's modification time as Unix milliseconds, or `None` when the
/// platform does not carry one or it predates the epoch.
fn modified_ms(metadata: &std::fs::Metadata) -> Option<u64> {
    let modified = metadata.modified().ok()?;
    let since_epoch = modified.duration_since(std::time::UNIX_EPOCH).ok()?;
    u64::try_from(since_epoch.as_millis()).ok()
}

/// Renders a file name as a title: control characters dropped, length capped.
///
/// A file name is text somebody else chose, arriving over a drag-drop or a
/// folder walk, and it is rendered next to evidence a person will trust. It
/// is data here, never markup and never a path (L6).
#[must_use]
pub fn intake_title(name: &str) -> String {
    let cleaned: String = name
        .chars()
        .filter(|character| !character.is_control())
        .take(INTAKE_TITLE_CHARS_MAX)
        .collect();
    let trimmed = cleaned.trim();
    if trimmed.is_empty() {
        "Untitled upload".to_owned()
    } else {
        trimmed.to_owned()
    }
}

/// Walks `root` into the bounded, ordered list of files a batch import
/// submits. A path that is itself a regular file is a one-item plan, so the
/// caller has one code path for "a file" and "a folder of files".
///
/// # Errors
///
/// [`IngestError::Io`] when the root cannot be read.
pub fn plan_intake(root: &Path) -> Result<IntakePlan, IngestError> {
    let metadata =
        std::fs::metadata(root).map_err(|source| io_error("stat an intake path", source))?;
    if metadata.is_file() {
        return Ok(IntakePlan {
            files: vec![root.to_path_buf()],
            skipped_count: 0,
            truncated: false,
        });
    }
    if !metadata.is_dir() {
        return Err(io_error(
            "open an intake path",
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "an intake path is a regular file or a directory",
            ),
        ));
    }
    walk(root)
}

/// Iterative, with an explicit stack and a stated depth — the crate rule
/// against recursion in ingestion code exists because a directory tree is
/// attacker-shaped input the moment a watch folder points at a shared drive
/// (STYLE §control flow, L6).
fn walk(root: &Path) -> Result<IntakePlan, IngestError> {
    let mut plan = IntakePlan::default();
    let mut stack = vec![(root.to_path_buf(), 0_usize)];
    while let Some((directory, depth)) = stack.pop() {
        let (files, directories, skipped) = read_directory(&directory)?;
        plan.skipped_count = plan.skipped_count.saturating_add(skipped);
        for file in files {
            if plan.files.len() >= INTAKE_FILE_COUNT_MAX {
                plan.truncated = true;
                return Ok(plan);
            }
            plan.files.push(file);
        }
        if depth + 1 > INTAKE_DEPTH_MAX {
            plan.truncated = plan.truncated || !directories.is_empty();
            continue;
        }
        // Pushed in reverse so the pop order matches the sorted order.
        for child in directories.into_iter().rev() {
            stack.push((child, depth + 1));
        }
    }
    Ok(plan)
}

/// Reads one directory into its sorted files and sub-directories, counting
/// what the walk's own rules exclude.
fn read_directory(directory: &Path) -> Result<(Vec<PathBuf>, Vec<PathBuf>, u32), IngestError> {
    let entries = std::fs::read_dir(directory)
        .map_err(|source| io_error("read an intake directory", source))?;
    let mut files = Vec::new();
    let mut directories = Vec::new();
    let mut skipped = 0_u32;
    for entry in entries {
        let entry = entry.map_err(|source| io_error("read an intake directory entry", source))?;
        let name = entry.file_name();
        let name = name.to_string_lossy();
        if is_skipped_name(&name) {
            skipped = skipped.saturating_add(1);
            continue;
        }
        // `file_type` does not follow symlinks, which is the point: a link is
        // a path out of the tree the user pointed at, and following one is how
        // a folder import becomes a walk of somebody's whole disk.
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("read an intake entry type", source))?;
        if file_type.is_symlink() {
            skipped = skipped.saturating_add(1);
        } else if file_type.is_dir() {
            directories.push(entry.path());
        } else if file_type.is_file() {
            files.push(entry.path());
        } else {
            skipped = skipped.saturating_add(1);
        }
    }
    files.sort();
    directories.sort();
    Ok((files, directories, skipped))
}

/// Names the walk never descends into or submits: dot-entries (`.DS_Store`,
/// `.git`, editor state) and `.pos` project directories, which hold a whole
/// other project's log and would ingest ProjectOS into ProjectOS.
fn is_skipped_name(name: &str) -> bool {
    name.starts_with('.') || name.ends_with(".pos")
}

/// Classifies a bounded prefix. Container magic numbers first, because a
/// media container is unambiguous where a text heuristic is not; the m1-s01
/// text sniffer decides everything that is not one.
#[must_use]
pub fn sniff_intake(prefix: &[u8]) -> MediaKind {
    if let Some(kind) = sniff_container(prefix) {
        return kind;
    }
    if let Some(kind) = sniff_captions(prefix) {
        return kind;
    }
    sniff_media_kind(prefix)
}

/// The containers this build recognizes. Deliberately a short list of exact
/// signatures rather than a general prober: a wrong guess sends a file to a
/// decoder that refuses it, and `Opaque` — stored, citable, honestly not read
/// — is a better answer than a confident wrong one (L3).
fn sniff_container(prefix: &[u8]) -> Option<MediaKind> {
    if prefix.starts_with(b"RIFF") && prefix.len() >= 12 && &prefix[8..12] == b"WAVE" {
        return Some(MediaKind::Audio);
    }
    if prefix.starts_with(b"OggS") || prefix.starts_with(b"fLaC") || prefix.starts_with(b"ID3") {
        return Some(MediaKind::Audio);
    }
    // MPEG audio frame sync: eleven set bits, then a layer that is not the
    // reserved one. This is what an MP3 with no ID3 tag starts with.
    if prefix.len() >= 2 && prefix[0] == 0xFF && (prefix[1] & 0xE6) >= 0xE2 {
        return Some(MediaKind::Audio);
    }
    if prefix.len() >= 12 && &prefix[4..8] == b"ftyp" {
        // ISO base media: the brand says whether the track is audio-only.
        // `M4A`/`M4B` are Apple's audio brands; everything else in the family
        // may carry video, and video is the safe assumption because the
        // decoder handles both.
        let brand = &prefix[8..12];
        if brand.starts_with(b"M4A") || brand.starts_with(b"M4B") {
            return Some(MediaKind::Audio);
        }
        return Some(MediaKind::Video);
    }
    if prefix.starts_with(&[0x1A, 0x45, 0xDF, 0xA3]) {
        // EBML: Matroska and WebM.
        return Some(MediaKind::Video);
    }
    if prefix.starts_with(&[0x00, 0x00, 0x01, 0xBA])
        || prefix.starts_with(&[0x00, 0x00, 0x01, 0xB3])
    {
        // MPEG program stream / video sequence header.
        return Some(MediaKind::Video);
    }
    if prefix.starts_with(b"%PDF-") {
        // Stored and citable; the text extractor is an M7 backlog item, so
        // claiming otherwise here would be the silent-limitation failure.
        return Some(MediaKind::Opaque);
    }
    None
}

/// WebVTT states itself; SubRip does not, so it is recognized by its shape —
/// a cue number, then a timing line with the `-->` arrow.
fn sniff_captions(prefix: &[u8]) -> Option<MediaKind> {
    let text = std::str::from_utf8(prefix)
        .ok()
        .or_else(|| first_valid_utf8(prefix))?;
    let text = text.strip_prefix('\u{feff}').unwrap_or(text);
    if text.starts_with("WEBVTT") {
        return Some(MediaKind::Captions);
    }
    let mut lines = text.lines().filter(|line| !line.trim().is_empty());
    let first = lines.next()?;
    let second = lines.next()?;
    if first.trim().parse::<u32>().is_ok() && second.contains("-->") {
        return Some(MediaKind::Captions);
    }
    None
}

/// A capped prefix can split a multi-byte character; that is not evidence of
/// binary, so the valid head is used and the split tail dropped.
fn first_valid_utf8(prefix: &[u8]) -> Option<&str> {
    let error = std::str::from_utf8(prefix).err()?;
    if error.error_len().is_some() || error.valid_up_to() == 0 {
        return None;
    }
    std::str::from_utf8(&prefix[..error.valid_up_to()]).ok()
}

/// The normalized shape a media kind produces, which is what the chunker
/// keys on. Audio, video, and captions are all `Transcript`: three ways of
/// arriving at timestamped speech, one chunker, one citation shape.
#[must_use]
pub const fn shape_for(media: MediaKind) -> EvidenceShape {
    match media {
        MediaKind::Audio | MediaKind::Video | MediaKind::Captions => EvidenceShape::Transcript,
        MediaKind::Csv => EvidenceShape::Table,
        MediaKind::Structured => EvidenceShape::Thread,
        MediaKind::PlainText | MediaKind::Markdown | MediaKind::Opaque => EvidenceShape::Document,
    }
}

/// Whether NORMALIZE will turn these bytes into text that CHUNK then windows
/// — which is what makes [`INTAKE_TEXT_BYTES_MAX`] apply.
const fn is_text(media: MediaKind) -> bool {
    matches!(
        media,
        MediaKind::PlainText | MediaKind::Markdown | MediaKind::Csv | MediaKind::Captions
    )
}

/// Fills as much of `prefix` as the file has, without a `read_to_end` and
/// without treating a short read as end of file.
fn read_prefix(content: &mut File, prefix: &mut [u8]) -> Result<usize, IngestError> {
    let mut filled = 0;
    while filled < prefix.len() {
        let count = content
            .read(&mut prefix[filled..])
            .map_err(|source| io_error("read an intake prefix", source))?;
        if count == 0 {
            break;
        }
        filled += count;
    }
    Ok(filled)
}

fn io_error(operation: &'static str, source: std::io::Error) -> IngestError {
    IngestError::Io { operation, source }
}

#[cfg(test)]
mod tests {
    use super::{INTAKE_DEPTH_MAX, INTAKE_TEXT_BYTES_MAX, plan_intake, shape_for, sniff_intake};
    use pos_domain::{EvidenceShape, MediaKind};

    #[test]
    fn containers_are_recognised_by_their_bytes_not_their_names() {
        assert_eq!(sniff_intake(b"RIFF\0\0\0\0WAVEfmt "), MediaKind::Audio);
        assert_eq!(sniff_intake(b"OggS\0\x02\0\0"), MediaKind::Audio);
        assert_eq!(sniff_intake(b"fLaC\0\0\0\x22"), MediaKind::Audio);
        assert_eq!(sniff_intake(b"ID3\x04\0\0\0\0\0\0"), MediaKind::Audio);
        assert_eq!(
            sniff_intake(b"\0\0\0\x18ftypM4A \0\0\0\0"),
            MediaKind::Audio
        );
        assert_eq!(
            sniff_intake(b"\0\0\0\x18ftypisom\0\0\x02\0"),
            MediaKind::Video
        );
        assert_eq!(
            sniff_intake(b"\x1a\x45\xdf\xa3\x01\0\0\0"),
            MediaKind::Video
        );
        assert_eq!(sniff_intake(b"%PDF-1.7\n"), MediaKind::Opaque);
    }

    #[test]
    fn captions_are_recognised_in_both_dialects() {
        assert_eq!(
            sniff_intake(b"WEBVTT\n\n00:00:00.000 --> 00:00:02.000\nHi\n"),
            MediaKind::Captions
        );
        assert_eq!(
            sniff_intake(b"1\n00:00:00,000 --> 00:00:02,000\nHi\n"),
            MediaKind::Captions
        );
    }

    #[test]
    fn a_transcript_shaped_extension_on_prose_does_not_win() {
        // The whole point of content sniffing: the bytes decide.
        assert_eq!(
            sniff_intake(b"# Notes\n\nWe agreed to ship."),
            MediaKind::Markdown
        );
        assert_eq!(sniff_intake(b"a,b,c\n1,2,3\n4,5,6\n"), MediaKind::Csv);
        assert_eq!(sniff_intake(b"just some prose"), MediaKind::PlainText);
    }

    #[test]
    fn every_media_kind_has_a_shape() {
        for media in MediaKind::ALL {
            let shape = shape_for(media);
            assert!(EvidenceShape::ALL.contains(&shape));
        }
        assert_eq!(shape_for(MediaKind::Captions), EvidenceShape::Transcript);
    }

    #[test]
    fn the_text_cap_stays_under_the_chunk_count_ceiling() {
        // A file this path accepts must not dead-letter at CHUNK: the chunker
        // caps one item at CHUNK_COUNT_MAX chunks of ~300 tokens.
        let chunk_ceiling_bytes = crate::CHUNK_COUNT_MAX
            * u64::from(crate::chunk_params_for(EvidenceShape::Document).target_tokens)
            * crate::TOKEN_BYTES_ESTIMATE;
        assert!(
            INTAKE_TEXT_BYTES_MAX < chunk_ceiling_bytes,
            "the intake text cap ({INTAKE_TEXT_BYTES_MAX}) must sit under the chunk ceiling \
             ({chunk_ceiling_bytes})"
        );
    }

    #[test]
    fn a_folder_walk_is_ordered_bounded_and_reports_what_it_skipped() {
        let root = tempfile::tempdir().expect("tempdir");
        std::fs::write(root.path().join("b.txt"), b"second").expect("write");
        std::fs::write(root.path().join("a.txt"), b"first").expect("write");
        std::fs::write(root.path().join(".DS_Store"), b"skip me").expect("write");
        std::fs::create_dir(root.path().join("nested.pos")).expect("mkdir");
        std::fs::write(root.path().join("nested.pos").join("log.db"), b"no").expect("write");
        std::fs::create_dir(root.path().join("deeper")).expect("mkdir");
        std::fs::write(root.path().join("deeper").join("c.txt"), b"third").expect("write");

        let plan = plan_intake(root.path()).expect("the walk succeeds");
        let names: Vec<String> = plan
            .files
            .iter()
            .map(|path| {
                path.file_name()
                    .unwrap_or_default()
                    .to_string_lossy()
                    .into_owned()
            })
            .collect();
        assert_eq!(names, vec!["a.txt", "b.txt", "c.txt"]);
        // The dot-file and the nested project directory, and nothing else.
        assert_eq!(plan.skipped_count, 2);
        assert!(!plan.truncated);
    }

    #[test]
    fn a_single_file_path_is_a_one_item_plan() {
        let root = tempfile::tempdir().expect("tempdir");
        let file = root.path().join("one.txt");
        std::fs::write(&file, b"content").expect("write");
        let plan = plan_intake(&file).expect("the walk succeeds");
        assert_eq!(plan.files, vec![file]);
        assert!(!plan.truncated);
    }

    #[test]
    fn a_tree_deeper_than_the_bound_says_so_instead_of_stopping_quietly() {
        let root = tempfile::tempdir().expect("tempdir");
        let mut path = root.path().to_path_buf();
        for level in 0..=INTAKE_DEPTH_MAX {
            path = path.join(format!("level-{level}"));
            std::fs::create_dir(&path).expect("mkdir");
            std::fs::write(path.join("note.txt"), b"deep").expect("write");
        }
        let plan = plan_intake(root.path()).expect("the walk succeeds");
        assert!(
            plan.truncated,
            "a walk that stopped at the depth bound must report it"
        );
        assert_eq!(plan.files.len(), INTAKE_DEPTH_MAX);
    }
}
