//! The segment index (m1-s01, m1-s02): the bridge between NORMALIZE and
//! CHUNK, and the thing that makes a citation resolve to a *position*.
//!
//! NORMALIZE (or TRANSCRIBE) emits two CAS blobs per evidence item: the
//! renderable normalized text, and this index over it. A segment is the
//! smallest structural unit the source has — a transcript turn, a thread
//! message, a heading section, a CSV row — carrying its byte range in the
//! text and the human-facing locator that range corresponds to.
//!
//! ## Why fixed-width records
//!
//! Forty bytes, little-endian, no framing, no parser. Three reasons, in
//! order:
//!
//! 1. **There is no decoder to get wrong.** A variable-length encoding here
//!    would be a parser over untrusted-derived bytes on the hot path of every
//!    ingest and every citation resolution (L6's blast radius, plus a fuzz
//!    target we would rather not owe).
//! 2. **It supports seeking.** Resolving one citation reads exactly one
//!    40-byte record at a computed offset instead of streaming a prefix.
//! 3. **The size is predictable.** Index overhead is
//!    `40 / mean_segment_bytes`; at the ~500-byte paragraphs and turns real
//!    corpora produce, that is about 8% of the normalized text — comparable
//!    to the chunk table itself, and the price of never re-parsing raw bytes
//!    to re-chunk.
//!
//! Both blobs are content-addressed, so two sources that deliver identical
//! bytes share one normalized text *and* one segment index for free.

use crate::IngestError;
use crate::budget::BoundedStream;
use pos_domain::Locator;
use pos_store::{BlobWriter, StoreError};
use std::io::Read;

/// On-disk size of one segment record. Changing this changes the blob format
/// and is a format-version migration, not an edit.
pub const SEGMENT_RECORD_BYTES: usize = 40;

/// Segments one evidence item may hold. At the 500-byte mean a real corpus
/// produces this is a 5 GB single item — well past the largest thing the
/// upload path accepts — and it keeps every `segment_count` arithmetic inside
/// `u32` range for the record encoding (L8: state the limit).
pub const SEGMENT_COUNT_MAX: u64 = 10_000_000;

const LOCATOR_TAG_TIME: u8 = 1;
const LOCATOR_TAG_LINE: u8 = 2;
const LOCATOR_TAG_MESSAGE: u8 = 3;

/// One structural unit of normalized content.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct Segment {
    /// Byte range in the normalized text blob, half-open.
    pub byte_start: u64,
    pub byte_end: u64,
    pub locator: Locator,
    /// Structural depth: markdown heading level (1–6), or 0 for a segment
    /// that starts no section. The document chunker breaks windows on it;
    /// every other shape leaves it 0.
    pub depth: u8,
}

impl Segment {
    #[must_use]
    pub const fn byte_len(&self) -> u64 {
        self.byte_end.saturating_sub(self.byte_start)
    }

    /// Fixed layout: `start | end | tag | depth | reserved[6] | a | b`.
    /// The reserved bytes are written as zero and ignored on read, which is
    /// the only forward compatibility a fixed-width format can offer.
    #[must_use]
    pub fn encode(&self) -> [u8; SEGMENT_RECORD_BYTES] {
        let mut record = [0_u8; SEGMENT_RECORD_BYTES];
        record[0..8].copy_from_slice(&self.byte_start.to_le_bytes());
        record[8..16].copy_from_slice(&self.byte_end.to_le_bytes());
        record[16] = match self.locator {
            Locator::TimeRange { .. } => LOCATOR_TAG_TIME,
            Locator::LineRange { .. } => LOCATOR_TAG_LINE,
            Locator::MessageRange { .. } => LOCATOR_TAG_MESSAGE,
        };
        record[17] = self.depth;
        let (first, second) = self.locator.bounds();
        record[24..32].copy_from_slice(&first.to_le_bytes());
        record[32..40].copy_from_slice(&second.to_le_bytes());
        record
    }

    /// Decodes one record. `None` for a short slice or an unknown locator
    /// tag — a corrupt index is a typed error at the call site, never a
    /// guessed position (the m1-s12 resolution sweep gates on that).
    #[must_use]
    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < SEGMENT_RECORD_BYTES {
            return None;
        }
        let byte_start = u64::from_le_bytes(bytes[0..8].try_into().ok()?);
        let byte_end = u64::from_le_bytes(bytes[8..16].try_into().ok()?);
        if byte_end < byte_start {
            return None;
        }
        let first = u64::from_le_bytes(bytes[24..32].try_into().ok()?);
        let second = u64::from_le_bytes(bytes[32..40].try_into().ok()?);
        let locator = match bytes[16] {
            LOCATOR_TAG_TIME => Locator::TimeRange {
                start_ms: first,
                end_ms: second,
            },
            LOCATOR_TAG_LINE => Locator::LineRange {
                start: first,
                end: second,
            },
            LOCATOR_TAG_MESSAGE => Locator::MessageRange {
                start: first,
                end: second,
            },
            _ => return None,
        };
        Some(Self {
            byte_start,
            byte_end,
            locator,
            depth: bytes[17],
        })
    }
}

/// Streams segment records into a CAS blob. Records go out as they are
/// produced, so a normalizer never holds the whole index (P4).
pub struct SegmentWriter<'store> {
    writer: BlobWriter<'store>,
    count: u64,
}

impl<'store> SegmentWriter<'store> {
    #[must_use]
    pub const fn new(writer: BlobWriter<'store>) -> Self {
        Self { writer, count: 0 }
    }

    pub fn push(&mut self, segment: Segment) -> Result<(), IngestError> {
        if self.count >= SEGMENT_COUNT_MAX {
            return Err(IngestError::LimitExceeded {
                limit: "segment count",
                value: self.count.saturating_add(1),
                limit_value: SEGMENT_COUNT_MAX,
            });
        }
        self.writer.append(&segment.encode())?;
        self.count += 1;
        Ok(())
    }

    #[must_use]
    pub const fn count(&self) -> u64 {
        self.count
    }

    /// Seals the blob and returns its address with the record count.
    pub fn finish(self) -> Result<([u8; 32], u64), StoreError> {
        let count = self.count;
        Ok((self.writer.finish()?.into_bytes(), count))
    }
}

/// Streams segment records out of a CAS blob.
pub struct SegmentReader<R> {
    stream: BoundedStream<R>,
    read_count: u64,
}

impl<R: Read> SegmentReader<R> {
    #[must_use]
    pub const fn new(stream: BoundedStream<R>) -> Self {
        Self {
            stream,
            read_count: 0,
        }
    }

    /// The next record, or `None` at the end of the index.
    ///
    /// # Errors
    ///
    /// [`IngestError::LimitExceeded`] when the blob ends mid-record (a
    /// truncated index is corruption, not an end of input).
    pub fn next_segment(&mut self) -> Result<Option<Segment>, IngestError> {
        let window = self.stream.window(SEGMENT_RECORD_BYTES)?;
        if window.is_empty() {
            return Ok(None);
        }
        let decoded = Segment::decode(window);
        let visible = window.len();
        let Some(segment) = decoded else {
            return Err(IngestError::LimitExceeded {
                limit: "segment record",
                value: visible as u64,
                limit_value: SEGMENT_RECORD_BYTES as u64,
            });
        };
        self.stream.advance(SEGMENT_RECORD_BYTES);
        self.read_count += 1;
        Ok(Some(segment))
    }

    #[must_use]
    pub const fn read_count(&self) -> u64 {
        self.read_count
    }

    #[must_use]
    pub const fn peak_bytes(&self) -> usize {
        self.stream.peak_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::{SEGMENT_RECORD_BYTES, Segment, SegmentReader};
    use crate::budget::{BoundedStream, StreamBudget};
    use pos_domain::{IngestStage, Locator};
    use proptest::prelude::*;

    fn segment(locator: Locator, depth: u8) -> Segment {
        Segment {
            byte_start: 10,
            byte_end: 250,
            locator,
            depth,
        }
    }

    #[test]
    fn every_locator_shape_round_trips_through_the_record() {
        let cases = [
            segment(
                Locator::TimeRange {
                    start_ms: 734_000,
                    end_ms: 742_500,
                },
                0,
            ),
            segment(Locator::LineRange { start: 3, end: 19 }, 2),
            segment(Locator::MessageRange { start: 0, end: 7 }, 0),
        ];
        for original in cases {
            let encoded = original.encode();
            assert_eq!(encoded.len(), SEGMENT_RECORD_BYTES);
            assert_eq!(Segment::decode(&encoded), Some(original));
        }
    }

    #[test]
    fn a_stream_of_records_reads_back_in_order() {
        let written: Vec<Segment> = (0..64)
            .map(|index| Segment {
                byte_start: index * 100,
                byte_end: index * 100 + 90,
                locator: Locator::LineRange {
                    start: index + 1,
                    end: index + 2,
                },
                depth: u8::try_from(index % 7).unwrap_or(0),
            })
            .collect();
        let mut blob = Vec::new();
        for segment in &written {
            blob.extend_from_slice(&segment.encode());
        }
        let mut reader = SegmentReader::new(BoundedStream::new(
            blob.as_slice(),
            StreamBudget::new(IngestStage::Chunk, 4096),
        ));
        let mut read = Vec::new();
        while let Some(segment) = reader.next_segment().expect("well-formed index") {
            read.push(segment);
        }
        assert_eq!(read, written);
        assert_eq!(reader.read_count(), 64);
    }

    #[test]
    fn a_truncated_index_is_an_error_not_a_silent_end() {
        let mut blob = segment(Locator::LineRange { start: 1, end: 2 }, 0)
            .encode()
            .to_vec();
        blob.truncate(SEGMENT_RECORD_BYTES - 1);
        let mut reader = SegmentReader::new(BoundedStream::new(
            blob.as_slice(),
            StreamBudget::new(IngestStage::Chunk, 4096),
        ));
        assert!(reader.next_segment().is_err());
    }

    proptest! {
        /// The decoder is total over arbitrary bytes: every input either
        /// decodes to a segment with a coherent range or is refused. It never
        /// panics and never invents a locator. This is the structural-fuzz
        /// obligation every decoder carries (STYLE); a libFuzzer lane joins
        /// with the FTS sanitizer in m1-s05.
        #[test]
        fn the_decoder_is_total_over_arbitrary_bytes(
            bytes in proptest::collection::vec(any::<u8>(), 0..(SEGMENT_RECORD_BYTES * 3))
        ) {
            match Segment::decode(&bytes) {
                Some(segment) => {
                    prop_assert!(segment.byte_end >= segment.byte_start);
                    prop_assert!(bytes.len() >= SEGMENT_RECORD_BYTES);
                    prop_assert_eq!(Segment::decode(&segment.encode()), Some(segment));
                }
                None => prop_assert!(true),
            }
        }
    }
}
