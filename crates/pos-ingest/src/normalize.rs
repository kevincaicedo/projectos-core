//! NORMALIZE (m1-s01): raw bytes become renderable text plus a segment index.
//!
//! The stage is one streaming pass. It writes the normalized text to a CAS
//! blob while emitting segment records for the structural units it crosses,
//! so neither the text nor the index is ever held whole (P4). Line endings
//! are normalized to `\n` and a UTF-8 BOM is dropped; nothing else about the
//! bytes changes, because the normalized text is what a citation renders and
//! a human has to recognize it.
//!
//! Both outputs are content-addressed. Two sources delivering byte-identical
//! attachments therefore produce the same normalized blob and the same index
//! without any dedup logic here — the CAS already did it (F6).
//!
//! ## What this stage does not do yet
//!
//! Caption files (VTT/SRT) arrive with the upload types in m1-s07, and audio
//! decoding is m1-s03's; both refuse typed and name their owner rather than
//! guessing. Connector payloads (`MediaKind::Structured`) normalize through
//! the connector's own `normalize()` once the m1-s06 contract exists; until
//! then a record-per-blank-line reading exercises the thread and message
//! chunkers with real bytes.

use crate::IngestError;
use crate::pipeline::{StageContext, StageFailure, StageHandler, StageProduct};
use crate::segment::{Segment, SegmentWriter};
use pos_domain::{CanaryLevel, EvidenceShape, IngestStage, IngestStageOutput, Locator, MediaKind};
use pos_store::BlobWriter;

/// Bytes one structural record may span before the stage refuses. A "line"
/// or "paragraph" larger than this is not structure, it is a blob that
/// happens to contain newlines, and treating it as a segment would put an
/// unbounded window inside a bounded stage (L8).
pub const RECORD_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Bytes of a record kept to decide its structural depth. A markdown heading
/// marker is at most `###### `; eight bytes is that plus slack.
const RECORD_PREFIX_BYTES: usize = 8;

/// How the scanner decides where one structural record ends.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum RecordRule {
    /// Blank-line separated: paragraphs, thread messages, mail bodies.
    Paragraph,
    /// One record per line, honouring RFC 4180 quoting so an embedded
    /// newline inside a quoted field does not split a row.
    CsvRecord,
}

/// The NORMALIZE stage handler.
pub struct NormalizeStage;

impl StageHandler for NormalizeStage {
    fn stage(&self) -> IngestStage {
        IngestStage::Normalize
    }

    fn run(&self, context: &StageContext<'_>) -> Result<StageProduct, StageFailure> {
        let evidence = context.evidence();
        match evidence.media_kind {
            // Media whose text only exists after decoding. NORMALIZE records
            // the shape and hands over an empty text blob; TRANSCRIBE
            // (m1-s03) replaces both outputs with the real transcript.
            MediaKind::Audio | MediaKind::Video => {
                empty_output(context, EvidenceShape::Transcript).map_err(StageFailure::from)
            }
            // Storable and citable, not readable as text. Honest zero chunks
            // beats a fabricated extraction.
            MediaKind::Opaque => empty_output(context, evidence.shape).map_err(StageFailure::from),
            MediaKind::Captions => Err(StageFailure::permanent(
                "media_not_supported",
                "caption files (VTT/SRT) are decoded by m1-s07's upload types",
            )),
            MediaKind::PlainText | MediaKind::Markdown | MediaKind::Csv | MediaKind::Structured => {
                let rule = match evidence.media_kind {
                    MediaKind::Csv => RecordRule::CsvRecord,
                    _ => RecordRule::Paragraph,
                };
                let markdown = evidence.media_kind == MediaKind::Markdown;
                normalize_text(context, rule, markdown).map_err(StageFailure::from)
            }
        }
    }
}

/// The empty-but-valid output: an addressable zero-byte text blob and an
/// empty index. Both still go through the CAS so later stages have real
/// hashes to open rather than a `None` every one of them must branch on.
fn empty_output(
    context: &StageContext<'_>,
    shape: EvidenceShape,
) -> Result<StageProduct, IngestError> {
    let text_blob = context.blob_writer()?.finish()?.into_bytes();
    let (segments_blob, segment_count) = SegmentWriter::new(context.blob_writer()?).finish()?;
    Ok(StageProduct {
        output: IngestStageOutput::Normalized {
            shape,
            text_blob,
            text_byte_size: 0,
            segments_blob,
            segment_count,
            // The m1-s14 detectors run here. Until they land the level is
            // `Clean` by default, which is exactly what `CanaryLevel::default`
            // means and why the column exists from E1.
            canary_level: CanaryLevel::default(),
        },
        bytes_read: 0,
        item_count: 0,
    })
}

/// One streaming pass: normalize, write, segment.
fn normalize_text(
    context: &StageContext<'_>,
    rule: RecordRule,
    markdown: bool,
) -> Result<StageProduct, IngestError> {
    let shape = context.evidence().shape;
    let mut stream = context.open_content()?;
    let mut text = context.blob_writer()?;
    let mut segments = SegmentWriter::new(context.blob_writer()?);
    let mut scan = ScanState::new(rule, markdown, shape);
    loop {
        let window = stream.window_max()?;
        if window.is_empty() {
            break;
        }
        let consumed = scan.absorb(window, &mut text, &mut segments)?;
        stream.advance(consumed);
    }
    scan.finish(&mut segments)?;
    let text_byte_size = scan.out_offset;
    let text_blob = text.finish()?.into_bytes();
    let (segments_blob, segment_count) = segments.finish()?;
    Ok(StageProduct {
        output: IngestStageOutput::Normalized {
            shape,
            text_blob,
            text_byte_size,
            segments_blob,
            segment_count,
            canary_level: CanaryLevel::default(),
        },
        bytes_read: stream.read_total(),
        item_count: segment_count,
    })
}

/// The scanner's carried state. Everything here is O(1): offsets, counters,
/// and a fixed-size prefix — a record is never buffered.
struct ScanState {
    rule: RecordRule,
    markdown: bool,
    shape: EvidenceShape,
    /// Offset in the *normalized* text, which is what segments index.
    out_offset: u64,
    record_start: u64,
    record_line_start: u64,
    content_line_last: u64,
    record_index: u64,
    line_index: u64,
    previous_was_newline: bool,
    in_quotes: bool,
    at_start: bool,
    prefix: [u8; RECORD_PREFIX_BYTES],
    prefix_len: usize,
    record_has_content: bool,
}

impl ScanState {
    const fn new(rule: RecordRule, markdown: bool, shape: EvidenceShape) -> Self {
        Self {
            rule,
            markdown,
            shape,
            out_offset: 0,
            record_start: 0,
            record_line_start: 1,
            content_line_last: 1,
            record_index: 0,
            line_index: 1,
            previous_was_newline: false,
            in_quotes: false,
            at_start: true,
            prefix: [0; RECORD_PREFIX_BYTES],
            prefix_len: 0,
            record_has_content: false,
        }
    }

    /// Consumes `window`, writing normalized bytes and emitting the records
    /// that ended inside it. Returns the input bytes consumed, which is the
    /// whole window: every byte is either written or deliberately dropped.
    fn absorb(
        &mut self,
        window: &[u8],
        text: &mut BlobWriter<'_>,
        segments: &mut SegmentWriter<'_>,
    ) -> Result<usize, IngestError> {
        let mut flush_from = 0;
        let mut index = 0;
        while index < window.len() {
            let byte = window[index];
            // A byte-order mark is metadata, not content: left in, it would
            // become the first bytes of the first chunk and of its hash.
            if self.at_start && window[index..].starts_with(&UTF8_BOM) {
                self.write(text, &window[flush_from..index])?;
                flush_from = index + UTF8_BOM.len();
                index = flush_from;
                self.at_start = false;
                continue;
            }
            if byte == b'\r' {
                // CRLF and lone CR both normalize away, so a citation renders
                // the same text on every platform.
                self.write(text, &window[flush_from..index])?;
                flush_from = index + 1;
                index += 1;
                continue;
            }
            self.at_start = false;
            if byte == b'"' && self.rule == RecordRule::CsvRecord {
                self.in_quotes = !self.in_quotes;
            }
            if !byte.is_ascii_whitespace() {
                if !self.record_has_content {
                    self.record_line_start = self.line_index;
                }
                self.record_has_content = true;
                self.content_line_last = self.line_index;
                self.push_prefix(byte);
            }
            if byte == b'\n' {
                // The newline belongs to the record it terminates.
                self.write(text, &window[flush_from..=index])?;
                flush_from = index + 1;
                self.line_index += 1;
                let ends = match self.rule {
                    RecordRule::CsvRecord => !self.in_quotes,
                    RecordRule::Paragraph => self.previous_was_newline,
                };
                self.previous_was_newline = true;
                if ends {
                    self.close_record(segments)?;
                }
                self.guard_record_size()?;
            } else {
                self.previous_was_newline = false;
            }
            index += 1;
        }
        self.write(text, &window[flush_from..])?;
        self.guard_record_size()?;
        Ok(window.len())
    }

    fn write(&mut self, text: &mut BlobWriter<'_>, bytes: &[u8]) -> Result<(), IngestError> {
        if bytes.is_empty() {
            return Ok(());
        }
        text.append(bytes)?;
        self.out_offset += bytes.len() as u64;
        Ok(())
    }

    fn push_prefix(&mut self, byte: u8) {
        if self.prefix_len < RECORD_PREFIX_BYTES {
            self.prefix[self.prefix_len] = byte;
            self.prefix_len += 1;
        }
    }

    fn guard_record_size(&self) -> Result<(), IngestError> {
        let span = self.out_offset.saturating_sub(self.record_start);
        if span > RECORD_BYTES_MAX as u64 {
            return Err(IngestError::LimitExceeded {
                limit: "structural record",
                value: span,
                limit_value: RECORD_BYTES_MAX as u64,
            });
        }
        Ok(())
    }

    /// Emits the record that just ended and rewinds the per-record state.
    /// A record with no non-whitespace content emits nothing — blank runs are
    /// separators, not segments — but still advances the span so segments
    /// tile the text with no gaps.
    fn close_record(&mut self, segments: &mut SegmentWriter<'_>) -> Result<(), IngestError> {
        let end = self.out_offset;
        if self.record_has_content {
            segments.push(Segment {
                byte_start: self.record_start,
                byte_end: end,
                locator: self.locator(),
                depth: self.depth(),
            })?;
            self.record_index += 1;
        }
        self.record_start = end;
        self.record_line_start = self.line_index;
        self.prefix_len = 0;
        self.record_has_content = false;
        Ok(())
    }

    fn locator(&self) -> Locator {
        match self.shape {
            EvidenceShape::Thread | EvidenceShape::Message => Locator::MessageRange {
                start: self.record_index,
                end: self.record_index,
            },
            _ => Locator::LineRange {
                start: self.record_line_start,
                end: self.content_line_last.max(self.record_line_start),
            },
        }
    }

    /// Markdown heading level, or 0. Only ATX headings (`## Title`) count:
    /// a setext underline is on the *next* line, which one forward pass does
    /// not have, and guessing is worse than a flat document.
    fn depth(&self) -> u8 {
        if !self.markdown {
            return 0;
        }
        let hashes = self.prefix[..self.prefix_len]
            .iter()
            .take_while(|byte| **byte == b'#')
            .count();
        if (1..=6).contains(&hashes) && self.prefix_len > hashes {
            u8::try_from(hashes).unwrap_or(0)
        } else {
            0
        }
    }

    /// Closes the trailing record of content that does not end in a blank
    /// line — the common case for a file that ends with its last sentence.
    fn finish(&mut self, segments: &mut SegmentWriter<'_>) -> Result<(), IngestError> {
        self.close_record(segments)
    }
}

const UTF8_BOM: [u8; 3] = [0xEF, 0xBB, 0xBF];

/// Content sniffing for the submission path: what these bytes are, decided by
/// looking at them rather than at a file name (m1-s07's rule, seeded here
/// because RAW has to record a media kind).
///
/// Deliberately conservative: bytes that are not valid UTF-8 are
/// [`MediaKind::Opaque`], which stores and cites the item without claiming to
/// have read it.
#[must_use]
pub fn sniff_media_kind(prefix: &[u8]) -> MediaKind {
    if prefix.is_empty() {
        return MediaKind::PlainText;
    }
    if prefix.contains(&0) {
        return MediaKind::Opaque;
    }
    let text = match std::str::from_utf8(prefix) {
        Ok(text) => text,
        Err(error) => {
            // A capped prefix can split a multi-byte character; that is not
            // evidence of binary. A genuinely invalid sequence is.
            if error.error_len().is_some() || error.valid_up_to() == 0 {
                return MediaKind::Opaque;
            }
            match std::str::from_utf8(&prefix[..error.valid_up_to()]) {
                Ok(text) => text,
                Err(_) => return MediaKind::Opaque,
            }
        }
    };
    let lines: Vec<&str> = text.lines().take(SNIFF_LINE_COUNT_MAX).collect();
    if lines
        .iter()
        .any(|line| line.starts_with("# ") || line.starts_with("## ") || line.starts_with("```"))
    {
        return MediaKind::Markdown;
    }
    // A CSV puts the same number of separators on every line, and at least one.
    let separators: Vec<usize> = lines
        .iter()
        .filter(|line| !line.is_empty())
        .map(|line| line.matches(',').count())
        .collect();
    if separators.len() >= 2
        && separators[0] > 0
        && separators.iter().all(|count| *count == separators[0])
    {
        return MediaKind::Csv;
    }
    MediaKind::PlainText
}

/// Lines the sniffer judges. Enough to see a CSV header plus rows, few enough
/// that sniffing a 4 GB video costs the same as sniffing a note (L8).
const SNIFF_LINE_COUNT_MAX: usize = 16;

#[cfg(test)]
mod tests {
    use super::{RECORD_PREFIX_BYTES, sniff_media_kind};
    use pos_domain::MediaKind;

    #[test]
    fn sniffing_reads_content_not_names() {
        assert_eq!(sniff_media_kind(b"# Title\n\nBody"), MediaKind::Markdown);
        assert_eq!(sniff_media_kind(b"a,b,c\n1,2,3\n4,5,6\n"), MediaKind::Csv);
        assert_eq!(sniff_media_kind(b"just some prose"), MediaKind::PlainText);
        assert_eq!(
            sniff_media_kind(&[0xff, 0xfe, 0x00, 0x01]),
            MediaKind::Opaque
        );
        assert_eq!(sniff_media_kind(b""), MediaKind::PlainText);
    }

    #[test]
    fn the_record_prefix_holds_a_full_heading_marker() {
        assert!(RECORD_PREFIX_BYTES >= "###### ".len());
    }
}
