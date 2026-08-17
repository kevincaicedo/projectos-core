//! The `transcribe` slot (m1-s03, F7/L9): one trait, three implementations —
//! local whisper today, cloud STT today, a vendored FFI leaf later.
//!
//! ## Why every word here is ours
//!
//! [ADR-0006] §2 takes `whisper-rs` for the MVP instead of vendoring the
//! whisper.cpp FFI leaf, as stated technical debt. The obligation that makes
//! that debt payable is this file: the interface is written in *our*
//! vocabulary — bounded windows of 16 kHz mono samples in, timestamped
//! [`TranscriptSegment`]s out, typed [`Weather`] refusals, cancellation
//! through the sink — and never in a wrapper's types. A `check-discipline`
//! rule proves no `whisper_rs::` symbol appears outside the one adapter
//! module, which is what turns "we could swap it later" into a property.
//!
//! ## The memory budget, stated
//!
//! Speech recognition wants the whole utterance in memory, and an hour of
//! 16 kHz mono `f32` is 230 MB — past this milestone's entire RSS gate. So
//! the seam is **windowed**: the caller decodes at most
//! [`WINDOW_SAMPLE_COUNT_MAX`] samples ([`WINDOW_MS_MAX`] of audio) and calls
//! [`Transcriber::transcribe`] once per window, carrying the window's offset
//! in the source media. At the stated cap one window is
//! `120 s × 16 kHz × 4 B ≈ 7.7 MiB` — comfortably inside the 64 MiB per-stage
//! bound m1-s01 asserts, with the model's own working set on top.
//!
//! Windows do not cut words in half: the caller advances to the end of the
//! last segment the window produced, not to the end of the window (see
//! `pos-ingest`'s TRANSCRIBE stage). That also makes the seam resumable —
//! every returned segment is a durable fact the next attempt need not redo.
//!
//! [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md

use crate::credentials::CallAuth;
use crate::provider::SinkClosed;
use crate::transport::HttpTransport;
use crate::weather::Weather;

/// The one sample rate this seam speaks. Whisper is trained at 16 kHz mono,
/// every cloud STT endpoint accepts it, and fixing it here means resampling
/// happens once, in one place, instead of per adapter.
pub const AUDIO_SAMPLE_RATE_HZ: u32 = 16_000;

/// Longest window a caller may hand one [`Transcriber::transcribe`] call.
/// See the module doc for the arithmetic; the number is the budget.
pub const WINDOW_MS_MAX: u64 = 120_000;

/// [`WINDOW_MS_MAX`] as samples — the bound adapters actually check.
///
/// Written as a `usize` product rather than a cast from the `u64` millisecond
/// constant so the arithmetic is exact on every target this ships to; the
/// value is 1.92M samples, nowhere near a 32-bit ceiling.
pub const WINDOW_SAMPLE_COUNT_MAX: usize = 120 * (AUDIO_SAMPLE_RATE_HZ as usize);

/// Shortest window worth a model call. Below this the tail of a recording is
/// silence or a syllable, and whisper hallucinates confidently on both.
pub const WINDOW_MS_MIN: u64 = 100;

/// Longest text one segment may carry before the adapter refuses it as
/// malformed. Whisper emits a phrase per segment; a megabyte of "text" from
/// one 30-second window is a decoder in a loop, not speech (L8).
pub const SEGMENT_TEXT_BYTES_MAX: usize = 16 * 1024;

/// One decoded stretch of speech.
///
/// Timestamps are milliseconds from the start of the **source media**, not
/// from the start of the window — the adapter adds the caller's offset, so a
/// citation resolves against the recording a human plays. Whisper's native
/// resolution is 10 ms and the milestone's contract is 10 ms; a value that is
/// not a multiple of 10 is honest precision the caller may round, never a
/// promise this seam makes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TranscriptSegment {
    pub start_ms: u64,
    pub end_ms: u64,
    pub text: String,
    /// The pause before this segment was long enough that a human would hear
    /// a turn boundary. Speaker *identity* is not claimed — v1 has no
    /// diarization, and guessing "Speaker A / Speaker B" from a pause would
    /// be a fabricated attribution on evidence a citation points at (L3).
    /// The user assigns turns to speakers; the milestone's cut line already
    /// allows diarization quality to slip.
    pub starts_turn: bool,
}

impl TranscriptSegment {
    #[must_use]
    pub const fn duration_ms(&self) -> u64 {
        self.end_ms.saturating_sub(self.start_ms)
    }
}

/// Pause that reads as a turn boundary. 700 ms is above a breath and below a
/// thinking pause; it is the one tunable of the v1 heuristic, stated here
/// rather than buried in an adapter so every implementation agrees.
pub const TURN_GAP_MS: u64 = 700;

/// Where decoded segments land, in time order. `&mut dyn` for the same reason
/// [`crate::CompletionSink`] is: M1 shells are synchronous, and a trait object
/// keeps the caller free to commit batches, stream to a UI, or buffer.
pub trait TranscriptSink {
    /// # Errors
    ///
    /// [`SinkClosed`] asks the adapter to stop decoding — a cancel, not a
    /// failure. The adapter returns the usage it accumulated so far.
    fn on_segment(&mut self, segment: &TranscriptSegment) -> Result<(), SinkClosed>;
}

/// A sink that appends into a `Vec` — the shape every test wants.
#[derive(Debug, Default)]
pub struct VecTranscriptSink {
    pub segments: Vec<TranscriptSegment>,
}

impl TranscriptSink for VecTranscriptSink {
    fn on_segment(&mut self, segment: &TranscriptSegment) -> Result<(), SinkClosed> {
        self.segments.push(segment.clone());
        Ok(())
    }
}

impl VecTranscriptSink {
    /// The concatenated text, for asserts.
    #[must_use]
    pub fn text(&self) -> String {
        self.segments
            .iter()
            .map(|segment| segment.text.as_str())
            .collect::<Vec<_>>()
            .join(" ")
    }
}

/// One window of audio to transcribe.
#[derive(Clone, Copy, Debug)]
pub struct TranscribeRequest<'a> {
    /// The model this window decodes with: a whisper artifact name for the
    /// local adapter, a provider model id for a cloud one. Both are the
    /// `model` column of the ledger row, so cost is attributable either way.
    pub model: &'a str,
    /// BCP-47 hint, or `None` to let the model detect. Absence is honest.
    pub language: Option<&'a str>,
    /// Where this window starts in the source media. Added to every emitted
    /// segment's timestamps.
    pub offset_ms: u64,
    /// 16 kHz mono `f32` in `[-1.0, 1.0]`, at most
    /// [`WINDOW_SAMPLE_COUNT_MAX`] long.
    pub samples: &'a [f32],
}

impl TranscribeRequest<'_> {
    /// Audio this window covers.
    #[must_use]
    pub fn audio_ms(&self) -> u64 {
        (self.samples.len() as u64) * 1_000 / u64::from(AUDIO_SAMPLE_RATE_HZ)
    }

    /// The bound check every adapter runs first, so the refusal is identical
    /// across implementations rather than three slightly different messages.
    ///
    /// # Errors
    ///
    /// [`Weather::BudgetExhausted`] when the window is longer than the stated
    /// cap, [`Weather::InvalidRequest`] when it is too short to be speech.
    pub fn check_bounds(&self) -> Result<(), Weather> {
        if self.samples.len() > WINDOW_SAMPLE_COUNT_MAX {
            return Err(Weather::BudgetExhausted {
                limit: "transcribe_window_samples",
                message: format!(
                    "{} samples exceed the {WINDOW_SAMPLE_COUNT_MAX}-sample window budget \
                     ({WINDOW_MS_MAX} ms)",
                    self.samples.len()
                ),
            });
        }
        if self.audio_ms() < WINDOW_MS_MIN {
            return Err(Weather::InvalidRequest {
                reason: format!(
                    "a {} ms window is below the {WINDOW_MS_MIN} ms floor for a model call",
                    self.audio_ms()
                ),
            });
        }
        Ok(())
    }
}

/// What one finished window cost. Transcription is billed in audio seconds,
/// not tokens, so this is the honest unit; `measured` says whether the
/// provider reported it or the adapter counted the samples it sent.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TranscribeUsage {
    pub audio_ms: u64,
    pub segment_count: u64,
    pub measured: bool,
}

impl TranscribeUsage {
    #[must_use]
    pub const fn add(self, other: Self) -> Self {
        Self {
            audio_ms: self.audio_ms.saturating_add(other.audio_ms),
            segment_count: self.segment_count.saturating_add(other.segment_count),
            measured: self.measured && other.measured,
        }
    }
}

/// The transcription contract. Peers, not successors: the local adapter, the
/// cloud adapter, and a future vendored-FFI adapter all implement exactly
/// this, and the policy layer decides which one a project gets (F43).
pub trait Transcriber {
    /// Stable label for the ledger and preflight — `whisper-local`,
    /// `openai-stt`. Not a `ProviderFamily`, because "the model running in
    /// this process" is not a provider family and pretending otherwise would
    /// put a lie in the cost report.
    fn label(&self) -> &'static str;

    /// Decodes one window, pushing segments into `sink` in time order.
    ///
    /// `transport` is `None` for an in-process model — deliberately, so an
    /// adapter that should never reach a socket structurally cannot. A cloud
    /// adapter handed `None` refuses typed rather than guessing.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`] for every failure class: budget, malformed model
    /// output, transport, provider refusal. Never a panic (STYLE).
    fn transcribe(
        &self,
        auth: &CallAuth,
        request: &TranscribeRequest<'_>,
        transport: Option<&dyn HttpTransport>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, Weather>;
}

/// Applies the v1 turn heuristic to a run of segments in time order.
///
/// Pause-only, and that is the whole of it: the milestone's task names
/// "pause + pitch-change boundary guess", and pitch analysis is deliberately
/// **not** in v1 — it needs a pitch tracker over the decoded samples, it
/// improves a label the user can already fix, and the milestone's own cut
/// line puts diarization quality behind timestamp-exact citation. Recorded as
/// debt-forward rather than half-built.
///
/// Adapters call this so the boundary rule is one implementation, not one per
/// backend.
pub fn mark_turns(segments: &mut [TranscriptSegment]) {
    let mut previous_end_ms: Option<u64> = None;
    for segment in segments.iter_mut() {
        segment.starts_turn = match previous_end_ms {
            None => true,
            Some(end_ms) => segment.start_ms.saturating_sub(end_ms) >= TURN_GAP_MS,
        };
        previous_end_ms = Some(segment.end_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::{
        AUDIO_SAMPLE_RATE_HZ, TURN_GAP_MS, TranscribeRequest, TranscribeUsage, TranscriptSegment,
        WINDOW_MS_MAX, WINDOW_SAMPLE_COUNT_MAX, mark_turns,
    };
    use crate::weather::Weather;

    fn segment(start_ms: u64, end_ms: u64) -> TranscriptSegment {
        TranscriptSegment {
            start_ms,
            end_ms,
            text: "words".to_owned(),
            starts_turn: false,
        }
    }

    #[test]
    fn the_window_budget_is_the_stated_arithmetic() {
        assert_eq!(
            WINDOW_SAMPLE_COUNT_MAX,
            120 * AUDIO_SAMPLE_RATE_HZ as usize,
            "the sample cap must be WINDOW_MS_MAX of audio, not a rounded guess"
        );
        // 7.68 MiB of f32 — the number the module doc promises the caller.
        assert_eq!(WINDOW_SAMPLE_COUNT_MAX * 4, 7_680_000);
        assert_eq!(WINDOW_MS_MAX, 120_000);
    }

    #[test]
    fn an_oversized_window_is_refused_as_budget_not_accepted_and_truncated() {
        let samples = vec![0.0_f32; WINDOW_SAMPLE_COUNT_MAX + 1];
        let request = TranscribeRequest {
            model: "whisper-small",
            language: None,
            offset_ms: 0,
            samples: &samples,
        };
        let refused = request
            .check_bounds()
            .expect_err("a window past the budget must refuse, never truncate silently");
        assert!(
            matches!(&refused, Weather::BudgetExhausted { limit, .. } if *limit == "transcribe_window_samples"),
            "got {refused:?}"
        );
    }

    #[test]
    fn a_window_shorter_than_the_floor_is_a_caller_error() {
        let samples = vec![0.0_f32; 100];
        let request = TranscribeRequest {
            model: "whisper-small",
            language: None,
            offset_ms: 0,
            samples: &samples,
        };
        assert!(matches!(
            request.check_bounds(),
            Err(Weather::InvalidRequest { .. })
        ));
    }

    #[test]
    fn audio_ms_counts_samples_rather_than_wall_clock() {
        let samples = vec![0.0_f32; AUDIO_SAMPLE_RATE_HZ as usize * 3];
        let request = TranscribeRequest {
            model: "whisper-small",
            language: None,
            offset_ms: 90_000,
            samples: &samples,
        };
        assert_eq!(request.audio_ms(), 3_000);
        assert!(request.check_bounds().is_ok());
    }

    #[test]
    fn turns_break_on_a_pause_and_the_first_segment_always_starts_one() {
        let mut segments = vec![
            segment(0, 2_000),
            // 200 ms gap: a breath inside one person's sentence.
            segment(2_200, 4_000),
            // A gap at exactly the threshold is a turn — the bound is stated
            // inclusive so a fixture on the boundary has one right answer.
            segment(4_000 + TURN_GAP_MS, 7_000),
        ];
        mark_turns(&mut segments);
        assert!(segments[0].starts_turn, "the first segment opens a turn");
        assert!(!segments[1].starts_turn, "a breath is not a turn boundary");
        assert!(segments[2].starts_turn, "a {TURN_GAP_MS} ms pause is");
    }

    #[test]
    fn usage_adds_and_stays_honest_about_measurement() {
        let measured = TranscribeUsage {
            audio_ms: 1_000,
            segment_count: 2,
            measured: true,
        };
        let estimated = TranscribeUsage {
            audio_ms: 500,
            segment_count: 1,
            measured: false,
        };
        let total = measured.add(estimated);
        assert_eq!(total.audio_ms, 1_500);
        assert_eq!(total.segment_count, 3);
        assert!(
            !total.measured,
            "one estimated window makes the total an estimate; claiming otherwise \
             would launder a guess into a measurement"
        );
    }
}
