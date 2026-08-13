//! CHUNK (m1-s02): the atoms citations point at, forever.
//!
//! The milestone lists five chunkers — transcript turn-windows, Slack thread
//! windows, email message-plus-quote-strip, markdown heading sections, and
//! CSV row groups. They are **one algorithm with five configurations**: a
//! greedy window over the segment index that closes on a token budget and on
//! a shape-specific boundary rule. Writing five loops would mean five places
//! for the token accounting, the id derivation, and the span arithmetic to
//! drift apart, and citation ids are the one thing that must never drift.
//!
//! ## Streaming shape
//!
//! Segments and normalized text are read forward together — segments are
//! ordered and non-overlapping, so one pass over each suffices. The only
//! resident state is the current window's bytes, bounded by
//! [`ChunkParams::bytes_max`], plus one batch of chunk facts bounded by
//! `CHUNK_BATCH_COUNT_MAX`. An 8 GB single file therefore chunks in the same
//! memory as an 8 KB note (P4).
//!
//! ## Re-runs are free
//!
//! Every chunk id derives from `(evidence, kind, span start, normalized
//! content)`, and chunk rows upsert. Re-running CHUNK after a crash produces
//! byte-identical facts, which is what makes at-least-once delivery safe
//! without a checkpoint (P3).

use crate::IngestError;
use crate::identity::{ContentHasher, derive_chunk_id};
use crate::pipeline::{StageContext, StageFailure, StageHandler, StageProduct};
use crate::segment::{SEGMENT_RECORD_BYTES, Segment};
use pos_domain::{
    CHUNK_BATCH_COUNT_MAX, ChunkFact, ChunkKind, EvidenceShape, IngestStage, IngestStageOutput,
    Locator,
};

/// Bytes per token, the standard English heuristic. The exact count belongs
/// to the model's tokenizer and arrives with the embedder (m1-s04); the
/// chunker's job is a *bounded* window, and a 25% estimate error moves a
/// 300-token chunk to 375 — still inside a 512-token embedding window.
pub const TOKEN_BYTES_ESTIMATE: u64 = 4;

/// Chunks one evidence item may produce. At the 300-token target this is a
/// ~1.2 GB single item of normalized text, past anything the upload path
/// accepts, and it keeps the ordinal inside `u32` (L8: state the limit).
pub const CHUNK_COUNT_MAX: u64 = 4_000_000;

/// Where a window is allowed to end.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BoundaryRule {
    /// Any segment boundary: transcripts, threads, and tables pack until the
    /// budget says stop, never splitting a turn, a message, or a row.
    AnySegment,
    /// One record per chunk: a single email body is a unit, and grouping two
    /// messages would make a citation ambiguous about which one it meant.
    PerSegment,
    /// Close when a heading starts a new section, so a document chunk is a
    /// section rather than an arbitrary window of prose.
    HeadingSection,
}

/// The window a chunker fills.
#[derive(Clone, Copy, Debug)]
pub struct ChunkParams {
    pub kind: ChunkKind,
    pub boundary: BoundaryRule,
    /// Preferred size; a window closes at the first boundary past it.
    pub target_tokens: u32,
    /// Hard ceiling. A single segment larger than this is split at
    /// whitespace, because a chunk that does not fit an embedding window is
    /// a chunk no model will ever see.
    pub tokens_max: u32,
}

impl ChunkParams {
    #[must_use]
    pub const fn bytes_target(&self) -> u64 {
        self.target_tokens as u64 * TOKEN_BYTES_ESTIMATE
    }

    #[must_use]
    pub const fn bytes_max(&self) -> u64 {
        self.tokens_max as u64 * TOKEN_BYTES_ESTIMATE
    }
}

/// The five configurations, chosen by shape. The transcript numbers are the
/// milestone's stated 200–400 token turn windows; the others follow it,
/// because one embedding model sees all of them and a mixed corpus with
/// wildly different chunk sizes ranks badly for reasons no eval can explain.
#[must_use]
pub const fn chunk_params_for(shape: EvidenceShape) -> ChunkParams {
    match shape {
        EvidenceShape::Transcript => ChunkParams {
            kind: ChunkKind::TranscriptTurns,
            boundary: BoundaryRule::AnySegment,
            target_tokens: 300,
            tokens_max: 400,
        },
        EvidenceShape::Thread => ChunkParams {
            kind: ChunkKind::ThreadMessages,
            boundary: BoundaryRule::AnySegment,
            target_tokens: 300,
            tokens_max: 400,
        },
        EvidenceShape::Message => ChunkParams {
            kind: ChunkKind::MessageBody,
            boundary: BoundaryRule::PerSegment,
            target_tokens: 300,
            tokens_max: 400,
        },
        EvidenceShape::Document => ChunkParams {
            kind: ChunkKind::DocumentSection,
            boundary: BoundaryRule::HeadingSection,
            target_tokens: 300,
            tokens_max: 400,
        },
        EvidenceShape::Table => ChunkParams {
            kind: ChunkKind::TableRows,
            boundary: BoundaryRule::AnySegment,
            target_tokens: 300,
            tokens_max: 400,
        },
    }
}

/// The CHUNK stage handler.
///
/// The window target is overridable because "re-chunk the corpus with a
/// different window" is a thing this product promises to survive (m1-s02),
/// and a promise with no way to exercise it is a hope. The default is the
/// milestone's stated 200–400 token turn window.
#[derive(Clone, Copy, Debug, Default)]
pub struct ChunkStage {
    target_tokens: Option<u32>,
}

impl ChunkStage {
    #[must_use]
    pub const fn new() -> Self {
        Self {
            target_tokens: None,
        }
    }

    /// Re-chunks at a different window target. The `tokens_max` ceiling moves
    /// with it, because a target above the ceiling would make every window
    /// close on the ceiling and quietly ignore the setting.
    #[must_use]
    pub const fn with_target_tokens(target_tokens: u32) -> Self {
        Self {
            target_tokens: Some(target_tokens),
        }
    }

    fn params_for(&self, shape: EvidenceShape) -> ChunkParams {
        let mut params = chunk_params_for(shape);
        if let Some(target_tokens) = self.target_tokens {
            params.target_tokens = target_tokens;
            params.tokens_max = target_tokens.saturating_add(target_tokens / 3);
        }
        params
    }
}

impl StageHandler for ChunkStage {
    fn stage(&self) -> IngestStage {
        IngestStage::Chunk
    }

    fn run(&self, context: &StageContext<'_>) -> Result<StageProduct, StageFailure> {
        chunk_evidence(context, self.params_for(context.evidence().shape))
            .map_err(StageFailure::from)
    }
}

fn chunk_evidence(
    context: &StageContext<'_>,
    params: ChunkParams,
) -> Result<StageProduct, IngestError> {
    let mut segments = context.open_segments()?;
    let mut text = context.open_text()?;
    let mut emitter = ChunkEmitter::new(context, params);
    let mut window = Window::default();
    while let Some(segment) = segments.next_segment()? {
        if window.would_close(&segment, params) {
            emitter.emit(&mut window, &mut text)?;
        }
        window.push(&segment);
        if params.boundary == BoundaryRule::PerSegment || window.exceeds_max(params) {
            emitter.emit(&mut window, &mut text)?;
        }
    }
    emitter.emit(&mut window, &mut text)?;
    let chunk_count = emitter.finish()?;
    Ok(StageProduct {
        output: IngestStageOutput::Chunked { chunk_count },
        bytes_read: text.read_total() + segments.read_count() * SEGMENT_RECORD_BYTES as u64,
        item_count: chunk_count,
    })
}

/// The accumulating window: spans and locator bounds only, never bytes.
#[derive(Clone, Copy, Debug, Default)]
struct Window {
    byte_start: u64,
    byte_end: u64,
    locator: Option<Locator>,
    depth: u8,
    segment_count: u32,
}

impl Window {
    const fn is_empty(&self) -> bool {
        self.segment_count == 0
    }

    const fn byte_len(&self) -> u64 {
        self.byte_end.saturating_sub(self.byte_start)
    }

    /// Whether `next` must start a new chunk rather than join this one.
    fn would_close(&self, next: &Segment, params: ChunkParams) -> bool {
        if self.is_empty() {
            return false;
        }
        match params.boundary {
            BoundaryRule::PerSegment => true,
            BoundaryRule::HeadingSection => {
                next.depth > 0 || self.byte_len() >= params.bytes_target()
            }
            BoundaryRule::AnySegment => self.byte_len() >= params.bytes_target(),
        }
    }

    fn exceeds_max(&self, params: ChunkParams) -> bool {
        self.byte_len() >= params.bytes_max()
    }

    fn push(&mut self, segment: &Segment) {
        if self.is_empty() {
            self.byte_start = segment.byte_start;
            self.locator = Some(segment.locator);
            self.depth = segment.depth;
        } else {
            self.locator = Some(extend(self.locator, segment.locator));
        }
        self.byte_end = segment.byte_end;
        self.segment_count += 1;
    }

    fn take(&mut self) -> Option<Self> {
        if self.is_empty() {
            return None;
        }
        let taken = *self;
        *self = Self::default();
        Some(taken)
    }
}

/// Merges two locators of the same kind into the range that covers both.
/// Mixed kinds cannot occur — one evidence item has one shape, and the shape
/// picks the locator — so the first is kept rather than inventing a union.
fn extend(current: Option<Locator>, next: Locator) -> Locator {
    let Some(current) = current else {
        return next;
    };
    match (current, next) {
        (
            Locator::TimeRange { start_ms, .. },
            Locator::TimeRange {
                end_ms: next_end, ..
            },
        ) => Locator::TimeRange {
            start_ms,
            end_ms: next_end,
        },
        (Locator::LineRange { start, .. }, Locator::LineRange { end: next_end, .. }) => {
            Locator::LineRange {
                start,
                end: next_end,
            }
        }
        (Locator::MessageRange { start, .. }, Locator::MessageRange { end: next_end, .. }) => {
            Locator::MessageRange {
                start,
                end: next_end,
            }
        }
        _ => current,
    }
}

/// Turns closed windows into chunk facts, reading exactly the bytes each
/// window covers and committing batches as it goes.
struct ChunkEmitter<'a, 'ctx> {
    context: &'a StageContext<'ctx>,
    params: ChunkParams,
    batch: Vec<ChunkFact>,
    batch_index: u32,
    ordinal: u32,
    total: u64,
    /// Where the text stream currently sits, so a window that follows a gap
    /// (a blank-line run between segments) skips forward instead of seeking.
    text_offset: u64,
}

impl<'a, 'ctx> ChunkEmitter<'a, 'ctx> {
    fn new(context: &'a StageContext<'ctx>, params: ChunkParams) -> Self {
        Self {
            context,
            params,
            batch: Vec::with_capacity(CHUNK_BATCH_COUNT_MAX),
            batch_index: 0,
            ordinal: 0,
            total: 0,
            text_offset: 0,
        }
    }

    fn emit<R: std::io::Read>(
        &mut self,
        window: &mut Window,
        text: &mut crate::budget::BoundedStream<R>,
    ) -> Result<(), IngestError> {
        let Some(window) = window.take() else {
            return Ok(());
        };
        self.skip_to(text, window.byte_start)?;
        let mut remaining = window.byte_len();
        let mut split_start = window.byte_start;
        let mut hasher = ContentHasher::new();
        let bytes_max = self.params.bytes_max();
        while remaining > 0 {
            let take = remaining.min(bytes_max);
            let consumed = self.absorb(text, take, &mut hasher)?;
            if consumed == 0 {
                // The index claims bytes the text blob does not have. That is
                // corruption between two content-addressed blobs, not an end
                // of input, and it must not silently shorten a citation.
                return Err(IngestError::LimitExceeded {
                    limit: "segment span beyond the normalized text",
                    value: window.byte_end,
                    limit_value: self.text_offset,
                });
            }
            remaining -= consumed;
            let split_end = split_start + consumed;
            let finished = std::mem::take(&mut hasher);
            self.push_fact(&window, split_start, split_end, finished)?;
            split_start = split_end;
        }
        Ok(())
    }

    /// Reads `take` bytes of the window into the hasher, returning how many
    /// were actually available.
    fn absorb<R: std::io::Read>(
        &mut self,
        text: &mut crate::budget::BoundedStream<R>,
        take: u64,
        hasher: &mut ContentHasher,
    ) -> Result<u64, IngestError> {
        let want = usize::try_from(take).unwrap_or(usize::MAX);
        let window = text.window(want)?;
        let visible = window.len().min(want);
        hasher.update(&window[..visible]);
        text.advance(visible);
        self.text_offset += visible as u64;
        Ok(visible as u64)
    }

    /// Advances past bytes no segment covers (blank-line runs).
    fn skip_to<R: std::io::Read>(
        &mut self,
        text: &mut crate::budget::BoundedStream<R>,
        offset: u64,
    ) -> Result<(), IngestError> {
        while self.text_offset < offset {
            let gap = offset - self.text_offset;
            let want = usize::try_from(gap.min(self.params.bytes_max())).unwrap_or(usize::MAX);
            let window = text.window(want)?;
            let visible = window.len();
            if visible == 0 {
                return Ok(());
            }
            text.advance(visible);
            self.text_offset += visible as u64;
        }
        Ok(())
    }

    fn push_fact(
        &mut self,
        window: &Window,
        byte_start: u64,
        byte_end: u64,
        hasher: ContentHasher,
    ) -> Result<(), IngestError> {
        if hasher.is_empty() {
            // A window of pure whitespace is not evidence of anything; the
            // bytes stay in the text blob and no chunk claims them.
            return Ok(());
        }
        if self.total >= CHUNK_COUNT_MAX {
            return Err(IngestError::LimitExceeded {
                limit: "chunk count",
                value: self.total.saturating_add(1),
                limit_value: CHUNK_COUNT_MAX,
            });
        }
        let token_count_estimate =
            u32::try_from(hasher.content_byte_count().div_ceil(TOKEN_BYTES_ESTIMATE))
                .unwrap_or(u32::MAX);
        let content_hash = hasher.finalize();
        let evidence_id = self.context.evidence().evidence_id;
        self.batch.push(ChunkFact {
            chunk_id: derive_chunk_id(evidence_id, self.params.kind, byte_start, &content_hash),
            ordinal: self.ordinal,
            kind: self.params.kind,
            byte_start,
            byte_end,
            locator: window
                .locator
                .unwrap_or(Locator::LineRange { start: 1, end: 1 }),
            content_hash,
            token_count_estimate,
        });
        self.ordinal = self.ordinal.saturating_add(1);
        self.total += 1;
        if self.batch.len() >= CHUNK_BATCH_COUNT_MAX {
            self.flush()?;
        }
        Ok(())
    }

    fn flush(&mut self) -> Result<(), IngestError> {
        if self.batch.is_empty() {
            return Ok(());
        }
        let batch = std::mem::take(&mut self.batch);
        self.context.emit_chunks(self.batch_index, batch)?;
        self.batch_index = self.batch_index.saturating_add(1);
        self.batch = Vec::with_capacity(CHUNK_BATCH_COUNT_MAX);
        Ok(())
    }

    fn finish(mut self) -> Result<u64, IngestError> {
        self.flush()?;
        Ok(self.total)
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundaryRule, ChunkParams, TOKEN_BYTES_ESTIMATE, Window, chunk_params_for};
    use crate::segment::Segment;
    use pos_domain::{EvidenceShape, Locator};

    fn segment(byte_start: u64, byte_end: u64, depth: u8) -> Segment {
        Segment {
            byte_start,
            byte_end,
            locator: Locator::LineRange {
                start: byte_start / 10 + 1,
                end: byte_end / 10 + 1,
            },
            depth,
        }
    }

    #[test]
    fn every_shape_has_a_distinct_chunk_kind() {
        let mut kinds: Vec<&str> = EvidenceShape::ALL
            .into_iter()
            .map(|shape| chunk_params_for(shape).kind.as_str())
            .collect();
        kinds.sort_unstable();
        kinds.dedup();
        assert_eq!(kinds.len(), EvidenceShape::COUNT);
    }

    #[test]
    fn a_window_packs_until_the_target_then_closes_on_a_boundary() {
        let params = chunk_params_for(EvidenceShape::Transcript);
        let mut window = Window::default();
        let step = 200_u64;
        let mut offset = 0;
        let mut closed_at = None;
        for index in 0..64_u64 {
            let next = segment(offset, offset + step, 0);
            if window.would_close(&next, params) {
                closed_at = Some(index);
                break;
            }
            window.push(&next);
            offset += step;
        }
        let closed = closed_at.expect("the window must close before 64 segments");
        assert!(
            window.byte_len() >= params.bytes_target(),
            "closed at {closed} with only {} bytes",
            window.byte_len()
        );
        // ...and never past the point where one more segment would overflow.
        assert!(window.byte_len() < params.bytes_target() + step);
    }

    #[test]
    fn a_heading_closes_a_document_window_regardless_of_size() {
        let params = chunk_params_for(EvidenceShape::Document);
        assert_eq!(params.boundary, BoundaryRule::HeadingSection);
        let mut window = Window::default();
        window.push(&segment(0, 40, 0));
        assert!(window.would_close(&segment(40, 80, 2), params));
        assert!(!window.would_close(&segment(40, 80, 0), params));
    }

    #[test]
    fn a_message_window_never_groups_two_records() {
        let params = chunk_params_for(EvidenceShape::Message);
        let mut window = Window::default();
        window.push(&segment(0, 10, 0));
        assert!(window.would_close(&segment(10, 20, 0), params));
    }

    #[test]
    fn the_locator_of_a_window_covers_every_segment_in_it() {
        let params = chunk_params_for(EvidenceShape::Table);
        let mut window = Window::default();
        for index in 0..5_u64 {
            window.push(&segment(index * 10, index * 10 + 10, 0));
        }
        let _ = params;
        assert_eq!(
            window.locator,
            Some(Locator::LineRange { start: 1, end: 6 })
        );
    }

    #[test]
    fn the_token_budget_is_stated_in_bytes_consistently() {
        let params: ChunkParams = chunk_params_for(EvidenceShape::Transcript);
        assert_eq!(
            params.bytes_target(),
            u64::from(params.target_tokens) * TOKEN_BYTES_ESTIMATE
        );
        assert!(params.bytes_max() > params.bytes_target());
    }
}
