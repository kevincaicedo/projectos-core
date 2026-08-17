//! Caption normalization (m1-s07): WebVTT and SubRip become a transcript.
//!
//! A caption file is speech that somebody already transcribed, with the
//! timestamps still attached. It therefore takes the *same* road as a whisper
//! transcript — normalized text, one segment per utterance, a
//! [`Locator::TimeRange`] on each — and comes out the far side identical in
//! shape to audio that went through TRANSCRIBE. One chunker, one citation
//! shape, one viewer, whether the words came from a model or from a caption
//! file a partner exported (m1-s03's `write_transcript` is this function's
//! twin, deliberately).
//!
//! ## Why this is a NORMALIZE concern and not a TRANSCRIBE one
//!
//! There is nothing to transcribe. `IngestStage::applies_to` already routes
//! [`MediaKind::Captions`] past TRANSCRIBE, because a caption file needs no
//! decoder and no model; parking it behind one would make an offline,
//! zero-cost item wait on a 488 MB download (L9's point, inverted).
//!
//! ## The invariant this shares with TRANSCRIBE
//!
//! **Segments are ordered and never overlap** — the property the segment
//! index, the chunker, and citation resolution all assume (m1-s03's T2). Real
//! caption files break it routinely: two speakers' cues overlap, and karaoke
//! styling repeats a line at shifting offsets. A cue that starts before the
//! previous one ended is clamped forward, and one that ends at or before it
//! is dropped. Dropping is safe because the *text* of an overlapping repeat
//! is already in the transcript; keeping it would put two citations on one
//! second of audio and make "the exact transcript second" ambiguous.

use crate::IngestError;
use crate::budget::BoundedStream;
use crate::pipeline::{StageContext, StageProduct};
use crate::segment::{Segment, SegmentWriter};
use crate::transcribe::TRANSCRIPT_SEGMENT_COUNT_MAX;
use pos_domain::{CanaryLevel, EvidenceShape, IngestStageOutput, Locator};
use pos_store::BlobWriter;
use std::io::Read;

/// Bytes one cue block may span. A cue is a line or two of speech plus its
/// timing; eight kibibytes is a hundred times the largest real one and small
/// enough that the parser's one buffered block stays a rounding error against
/// the stage budget (L8).
pub const CAPTION_BLOCK_BYTES_MAX: usize = 8 * 1024;

/// Cues one caption file may yield. Deliberately the same bound whisper
/// output carries: a transcript is a transcript, and one cap for both is one
/// number to keep true.
pub const CAPTION_CUE_COUNT_MAX: u32 = TRANSCRIPT_SEGMENT_COUNT_MAX;

/// One parsed cue: when it was said, and what was said.
#[derive(Clone, Debug, Eq, PartialEq)]
struct Cue {
    start_ms: u64,
    end_ms: u64,
    text: String,
}

/// Streams a caption file into the normalized text blob and the segment
/// index, exactly as `TRANSCRIBE` does for decoded audio.
pub(crate) fn normalize_captions(context: &StageContext<'_>) -> Result<StageProduct, IngestError> {
    let mut stream = context.open_content()?;
    let mut text = context.blob_writer()?;
    let mut segments = SegmentWriter::new(context.blob_writer()?);
    let mut reader = BlockReader::default();
    let mut byte_offset = 0_u64;
    let mut cue_count = 0_u32;
    let mut committed_to_ms = 0_u64;
    while let Some(block) = reader.next_block(&mut stream)? {
        let Some(cue) = parse_block(&block) else {
            continue;
        };
        let Some(cue) = order(cue, committed_to_ms) else {
            continue;
        };
        if cue_count >= CAPTION_CUE_COUNT_MAX {
            return Err(IngestError::LimitExceeded {
                limit: "caption cues",
                value: u64::from(cue_count) + 1,
                limit_value: u64::from(CAPTION_CUE_COUNT_MAX),
            });
        }
        committed_to_ms = cue.end_ms;
        byte_offset = write_cue(&mut text, &mut segments, &cue, byte_offset)?;
        cue_count = cue_count.saturating_add(1);
    }
    let text_byte_size = byte_offset;
    let text_blob = text.finish()?.into_bytes();
    let (segments_blob, segment_count) = segments.finish()?;
    Ok(StageProduct {
        output: IngestStageOutput::Normalized {
            shape: EvidenceShape::Transcript,
            text_blob,
            text_byte_size,
            segments_blob,
            segment_count,
            // The m1-s14 detectors run at this stage for every media kind;
            // `Clean` by default is what the column means until they land.
            canary_level: CanaryLevel::default(),
        },
        bytes_read: stream.read_total(),
        item_count: u64::from(cue_count),
    })
}

/// Appends one cue's rendered text and its segment. Byte-for-byte the same
/// layout `write_transcript` produces: the text, a newline that belongs to no
/// segment, and the next segment starting after it.
fn write_cue(
    text: &mut BlobWriter<'_>,
    segments: &mut SegmentWriter<'_>,
    cue: &Cue,
    byte_offset: u64,
) -> Result<u64, IngestError> {
    text.append(cue.text.as_bytes())?;
    text.append(b"\n")?;
    let byte_start = byte_offset;
    let byte_end = byte_start.saturating_add(cue.text.len() as u64);
    segments.push(Segment {
        byte_start,
        byte_end,
        locator: Locator::TimeRange {
            start_ms: cue.start_ms,
            end_ms: cue.end_ms,
        },
        depth: 0,
    })?;
    Ok(byte_end.saturating_add(1))
}

/// Enforces the non-overlap invariant. Returns `None` for a cue the
/// transcript already covers.
fn order(cue: Cue, committed_to_ms: u64) -> Option<Cue> {
    if cue.end_ms <= committed_to_ms {
        return None;
    }
    Some(Cue {
        start_ms: cue.start_ms.max(committed_to_ms),
        end_ms: cue.end_ms,
        text: cue.text,
    })
}

/// Turns one blank-line-separated block into a cue, or `None` for the blocks
/// a caption file carries that are not cues: the `WEBVTT` header, `NOTE`,
/// `STYLE`, and `REGION`. Unknown blocks are skipped rather than refused —
/// a caption dialect we do not know is not a reason to lose the ones we do.
fn parse_block(block: &str) -> Option<Cue> {
    let mut lines = block.lines().filter(|line| !line.trim().is_empty());
    let first = lines.next()?;
    // The timing line is either the first line of the block or the second,
    // depending on whether the dialect numbers its cues. Everything after it
    // is the spoken text; everything before it is an identifier we do not
    // need, because our own segment index numbers the segments.
    let timing = if first.contains("-->") {
        first
    } else {
        let second = lines.next()?;
        if !second.contains("-->") {
            return None;
        }
        second
    };
    let (start_text, rest) = timing.split_once("-->")?;
    // Cue settings (`line:0 align:start`) follow the end timestamp on the
    // same line, and they are presentation, not content.
    let end_text = rest.split_whitespace().next()?;
    let start_ms = parse_timestamp(start_text.trim())?;
    let end_ms = parse_timestamp(end_text.trim())?;
    if end_ms < start_ms {
        return None;
    }
    let mut text = String::new();
    for line in lines {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        if !text.is_empty() {
            text.push(' ');
        }
        text.push_str(line);
    }
    if text.is_empty() {
        return None;
    }
    Some(Cue {
        start_ms,
        end_ms,
        text,
    })
}

/// `HH:MM:SS.mmm`, `MM:SS.mmm`, and the SubRip comma spelling of both.
fn parse_timestamp(text: &str) -> Option<u64> {
    let text = text.trim();
    let (clock, fraction) = match text.rsplit_once(['.', ',']) {
        Some((clock, fraction)) => (clock, fraction),
        None => (text, "0"),
    };
    let millis: u64 = if fraction.is_empty() {
        0
    } else {
        // Milliseconds is the finest unit a locator carries; a longer
        // fraction is truncated rather than refused.
        let digits: String = fraction.chars().take(3).collect();
        let scale = 10_u64.pow(3 - u32::try_from(digits.len()).ok()?);
        digits.parse::<u64>().ok()? * scale
    };
    let mut total_ms = millis;
    let mut unit_ms = 1_000_u64;
    for part in clock.rsplit(':') {
        if part.is_empty() || unit_ms > 3_600_000 {
            return None;
        }
        total_ms = total_ms.checked_add(part.trim().parse::<u64>().ok()? * unit_ms)?;
        unit_ms *= 60;
    }
    Some(total_ms)
}

/// Reads blank-line-separated blocks out of a bounded stream without ever
/// holding more than one block.
#[derive(Default)]
struct BlockReader {
    /// The block being accumulated. Capped by [`CAPTION_BLOCK_BYTES_MAX`],
    /// so this is the only resident state and it is a stated size.
    pending: String,
    blank_run: u32,
}

impl BlockReader {
    fn next_block<R: Read>(
        &mut self,
        stream: &mut BoundedStream<R>,
    ) -> Result<Option<String>, IngestError> {
        loop {
            let Some(line) = read_line(stream)? else {
                let block = std::mem::take(&mut self.pending);
                return Ok((!block.trim().is_empty()).then_some(block));
            };
            if line.trim().is_empty() {
                self.blank_run = self.blank_run.saturating_add(1);
                if !self.pending.trim().is_empty() {
                    self.blank_run = 0;
                    return Ok(Some(std::mem::take(&mut self.pending)));
                }
                self.pending.clear();
                continue;
            }
            if self.pending.len() + line.len() > CAPTION_BLOCK_BYTES_MAX {
                return Err(IngestError::LimitExceeded {
                    limit: "caption block",
                    value: (self.pending.len() + line.len()) as u64,
                    limit_value: CAPTION_BLOCK_BYTES_MAX as u64,
                });
            }
            self.pending.push_str(&line);
            self.pending.push('\n');
        }
    }
}

/// One `\n`-terminated line, with the terminator stripped and a UTF-8 check
/// that refuses typed rather than replacing bytes a citation would render.
fn read_line<R: Read>(stream: &mut BoundedStream<R>) -> Result<Option<String>, IngestError> {
    let mut line = Vec::new();
    loop {
        let window = stream.window_max()?;
        if window.is_empty() {
            break;
        }
        let newline = window.iter().position(|byte| *byte == b'\n');
        let take = newline.map_or(window.len(), |index| index + 1);
        if line.len() + take > CAPTION_BLOCK_BYTES_MAX {
            return Err(IngestError::LimitExceeded {
                limit: "caption line",
                value: (line.len() + take) as u64,
                limit_value: CAPTION_BLOCK_BYTES_MAX as u64,
            });
        }
        line.extend_from_slice(&window[..take]);
        stream.advance(take);
        if newline.is_some() {
            break;
        }
    }
    if line.is_empty() {
        return Ok(None);
    }
    while matches!(line.last(), Some(b'\n' | b'\r')) {
        line.pop();
    }
    let offset = stream.offset().saturating_sub(line.len() as u64);
    String::from_utf8(line)
        .map(Some)
        .map_err(|_| IngestError::NotUtf8 {
            byte_offset: offset,
        })
}

#[cfg(test)]
mod tests {
    use super::{Cue, order, parse_block, parse_timestamp};

    #[test]
    fn both_timestamp_dialects_parse_to_the_same_milliseconds() {
        assert_eq!(parse_timestamp("00:00:02.500"), Some(2_500));
        assert_eq!(parse_timestamp("00:00:02,500"), Some(2_500));
        assert_eq!(parse_timestamp("01:02:03.004"), Some(3_723_004));
        assert_eq!(parse_timestamp("02:03.004"), Some(123_004));
        assert_eq!(parse_timestamp("00:00:01"), Some(1_000));
        assert_eq!(parse_timestamp("not a time"), None);
    }

    #[test]
    fn a_webvtt_cue_keeps_its_words_and_drops_its_settings() {
        let cue =
            parse_block("00:00:01.000 --> 00:00:03.500 line:0 align:start\nWe agreed\nto ship.\n")
                .expect("a timing line makes a cue");
        assert_eq!(
            cue,
            Cue {
                start_ms: 1_000,
                end_ms: 3_500,
                text: "We agreed to ship.".to_owned(),
            }
        );
    }

    #[test]
    fn a_subrip_cue_ignores_its_number() {
        let cue = parse_block("7\n00:00:01,000 --> 00:00:03,500\nHello.\n").expect("a cue");
        assert_eq!(cue.start_ms, 1_000);
        assert_eq!(cue.text, "Hello.");
    }

    #[test]
    fn headers_and_notes_are_not_cues() {
        assert!(parse_block("WEBVTT\n").is_none());
        assert!(parse_block("NOTE this file was exported by hand\n").is_none());
        assert!(parse_block("STYLE\n::cue { color: peachpuff }\n").is_none());
    }

    #[test]
    fn overlapping_cues_are_clamped_or_dropped_so_segments_never_overlap() {
        let clamped = order(
            Cue {
                start_ms: 900,
                end_ms: 2_000,
                text: "second speaker".to_owned(),
            },
            1_000,
        )
        .expect("a cue that extends past the committed end survives");
        assert_eq!(clamped.start_ms, 1_000);
        assert!(
            order(
                Cue {
                    start_ms: 500,
                    end_ms: 1_000,
                    text: "a karaoke repeat".to_owned(),
                },
                1_000,
            )
            .is_none(),
            "a cue the transcript already covers is dropped"
        );
    }
}
