//! The streaming discipline (m1-s01, P4).
//!
//! Every stage reads user content through [`BoundedStream`]: a forward-only
//! window over a reader with a *stated* resident cap. Asking for a window
//! larger than the budget is a typed refusal, not a large allocation — which
//! is what turns "a 10 GB corpus ingests in the same RSS as a 10 MB one" from
//! a hope into a property of the code.
//!
//! The companion half is mechanical: `check-discipline` denies
//! `read_to_end`, `read_to_string`, `fs::read`, and `fs::read_to_string`
//! anywhere in `crates/pos-ingest/`. A reviewer cannot forget, and a new
//! stage cannot quietly slurp.

use crate::IngestError;
use pos_domain::IngestStage;
use std::io::Read;

/// Resident bytes one stage may hold of the content it is streaming. 32 MiB
/// is the milestone's stated per-stage budget: generous enough that no real
/// window (a transcript turn, a heading section, a CSV row group) comes near
/// it, small enough that six concurrent stages stay far inside the 600 MB
/// pipeline RSS gate (§18).
pub const STAGE_BUFFER_BYTES_MAX_DEFAULT: usize = 32 * 1024 * 1024;

/// Bytes pulled from the underlying reader per syscall. 256 KiB amortizes the
/// syscall over a useful amount of work without making the resident window
/// jump in coarse steps.
pub const STAGE_READ_BYTES: usize = 256 * 1024;

/// A stage's declared streaming budget. Carrying the stage makes the refusal
/// name the offender instead of reporting an anonymous over-allocation.
#[derive(Clone, Copy, Debug)]
pub struct StreamBudget {
    pub stage: IngestStage,
    pub buffer_bytes_max: usize,
}

impl StreamBudget {
    #[must_use]
    pub const fn new(stage: IngestStage, buffer_bytes_max: usize) -> Self {
        Self {
            stage,
            buffer_bytes_max,
        }
    }

    #[must_use]
    pub const fn default_for(stage: IngestStage) -> Self {
        Self::new(stage, STAGE_BUFFER_BYTES_MAX_DEFAULT)
    }
}

/// A forward-only, capped window over a reader.
///
/// The contract is two calls: [`Self::window`] makes at least `want` bytes
/// visible (fewer only at end of input), and [`Self::advance`] consumes them.
/// Nothing else can see the buffer, so no caller can accidentally retain it.
pub struct BoundedStream<R> {
    reader: R,
    budget: StreamBudget,
    buffer: Vec<u8>,
    /// Bytes of `buffer` already consumed. Kept as a cursor rather than
    /// draining on every advance: a memmove per chunk would make chunking
    /// quadratic in window count for no benefit.
    start: usize,
    read_total: u64,
    consumed_total: u64,
    peak_bytes: usize,
    at_end: bool,
}

impl<R: Read> BoundedStream<R> {
    #[must_use]
    pub fn new(reader: R, budget: StreamBudget) -> Self {
        Self {
            reader,
            budget,
            buffer: Vec::new(),
            start: 0,
            read_total: 0,
            consumed_total: 0,
            peak_bytes: 0,
            at_end: false,
        }
    }

    /// Bytes visible without another read.
    #[must_use]
    pub fn available(&self) -> usize {
        self.buffer.len().saturating_sub(self.start)
    }

    /// Total bytes pulled from the reader — what a stage reports as
    /// `bytes_read`. With [`Self::peak_bytes`] this is the streaming proof:
    /// gigabytes read, kilobytes resident.
    #[must_use]
    pub const fn read_total(&self) -> u64 {
        self.read_total
    }

    /// Offset of the next unconsumed byte in the underlying content.
    #[must_use]
    pub const fn offset(&self) -> u64 {
        self.consumed_total
    }

    /// The high-water mark of the resident window. Asserted by the m1-s01
    /// buffer-budget suite and reported in the bench artifact.
    #[must_use]
    pub const fn peak_bytes(&self) -> usize {
        self.peak_bytes
    }

    #[must_use]
    pub const fn at_end(&self) -> bool {
        self.at_end && self.buffer.len() == self.start
    }

    /// Makes at least `want` bytes visible and returns the visible window,
    /// which is shorter than `want` only at end of input.
    ///
    /// # Errors
    ///
    /// [`IngestError::BufferBudgetExceeded`] when `want` exceeds the stated
    /// budget, and [`IngestError::Io`] when the reader fails.
    pub fn window(&mut self, want: usize) -> Result<&[u8], IngestError> {
        self.fill(want)?;
        let end = self.buffer.len().min(self.start + want);
        Ok(&self.buffer[self.start..end])
    }

    /// The whole visible window, after topping it up to the budget. Used by
    /// scanners that consume as much as they are given (a line splitter) and
    /// therefore have no fixed `want`.
    pub fn window_max(&mut self) -> Result<&[u8], IngestError> {
        let want = self.budget.buffer_bytes_max;
        self.fill(want)?;
        Ok(&self.buffer[self.start..])
    }

    /// Consumes `count` visible bytes. Consuming more than is visible is a
    /// caller bug, so it is clamped and asserted rather than panicking in a
    /// release build mid-ingest.
    pub fn advance(&mut self, count: usize) {
        debug_assert!(
            count <= self.available(),
            "advance past the visible window: {count} > {}",
            self.available()
        );
        let count = count.min(self.available());
        self.start += count;
        self.consumed_total += count as u64;
        if self.start == self.buffer.len() {
            self.buffer.clear();
            self.start = 0;
        }
    }

    fn fill(&mut self, want: usize) -> Result<(), IngestError> {
        if want > self.budget.buffer_bytes_max {
            return Err(IngestError::BufferBudgetExceeded {
                stage: self.budget.stage,
                wanted_bytes: want,
                budget_bytes: self.budget.buffer_bytes_max,
            });
        }
        while self.available() < want && !self.at_end {
            self.compact();
            let room = self
                .budget
                .buffer_bytes_max
                .saturating_sub(self.buffer.len());
            let read_len = STAGE_READ_BYTES.min(room);
            if read_len == 0 {
                // Unreachable while `want <= budget` holds, because compaction
                // leaves `buffer.len() == available() < want <= budget`. Typed
                // anyway: a silent break here would return a short window and
                // make a parser think it reached the end of the input.
                return Err(IngestError::BufferBudgetExceeded {
                    stage: self.budget.stage,
                    wanted_bytes: want,
                    budget_bytes: self.budget.buffer_bytes_max,
                });
            }
            let before = self.buffer.len();
            self.buffer.resize(before + read_len, 0);
            let count = self
                .reader
                .read(&mut self.buffer[before..])
                .map_err(|source| IngestError::Io {
                    operation: "read ingested content",
                    source,
                })?;
            self.buffer.truncate(before + count);
            if count == 0 {
                self.at_end = true;
            } else {
                self.read_total += count as u64;
            }
            self.peak_bytes = self.peak_bytes.max(self.buffer.len());
        }
        debug_assert!(
            self.buffer.len() <= self.budget.buffer_bytes_max,
            "resident window exceeded its stated budget"
        );
        Ok(())
    }

    /// Drops consumed bytes so the buffer measures live content rather than
    /// history. Only ever called before growing, so the cursor stays cheap on
    /// the hot path.
    fn compact(&mut self) {
        if self.start > 0 {
            self.buffer.drain(..self.start);
            self.start = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{BoundedStream, STAGE_READ_BYTES, StreamBudget};
    use crate::IngestError;
    use pos_domain::IngestStage;

    fn budget(bytes: usize) -> StreamBudget {
        StreamBudget::new(IngestStage::Chunk, bytes)
    }

    #[test]
    fn a_window_larger_than_the_budget_is_refused_not_allocated() {
        let content = vec![b'x'; 1024];
        let mut stream = BoundedStream::new(content.as_slice(), budget(512));
        let error = stream.window(513).expect_err("over budget must refuse");
        assert!(matches!(
            error,
            IngestError::BufferBudgetExceeded {
                wanted_bytes: 513,
                budget_bytes: 512,
                ..
            }
        ));
    }

    #[test]
    fn gigabyte_scale_reads_stay_inside_a_small_resident_window() {
        // Eight MiB through a 1 MiB budget in 4 KiB windows: the whole point
        // is that `read_total` and `peak_bytes` are unrelated numbers.
        let content = vec![b'a'; 8 * 1024 * 1024];
        let budget_bytes = 1024 * 1024;
        let mut stream = BoundedStream::new(content.as_slice(), budget(budget_bytes));
        let mut seen = 0_u64;
        loop {
            let window = stream.window(4096).expect("read within budget");
            if window.is_empty() {
                break;
            }
            let taken = window.len();
            stream.advance(taken);
            seen += taken as u64;
        }
        assert_eq!(seen, content.len() as u64);
        assert_eq!(stream.read_total(), content.len() as u64);
        assert!(
            stream.peak_bytes() <= budget_bytes,
            "peak {} exceeded the budget",
            stream.peak_bytes()
        );
        assert!(
            stream.peak_bytes() <= STAGE_READ_BYTES + 4096,
            "peak {} is far above one read plus one window",
            stream.peak_bytes()
        );
    }

    #[test]
    fn a_short_read_at_the_end_is_visible_as_a_short_window() {
        let content = b"twelve bytes";
        let mut stream = BoundedStream::new(&content[..], budget(1024));
        let window = stream.window(64).expect("short input is not an error");
        assert_eq!(window, content);
        let visible = window.len();
        stream.advance(visible);
        assert!(stream.at_end());
        assert_eq!(stream.offset(), content.len() as u64);
    }
}
