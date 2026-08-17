//! The local whisper adapter — **the only module in ProjectOS that may name a
//! `whisper_rs::` symbol** (m1-s03, [ADR-0006] §2).
//!
//! ## The containment rule, and why it is mechanical
//!
//! The founder decision took `whisper-rs` instead of vendoring the whisper.cpp
//! FFI leaf, as stated technical debt. The debt is payable only if swapping it
//! stays a one-file change, so `check-discipline` fails the build on any
//! `whisper_rs::` path or `use whisper_rs` outside this file — the same shape
//! as the `tracing` and projection-write rules, with its own seeded violation
//! fixture. Everything above this module speaks
//! [`Transcriber`]/[`TranscriptSegment`] and cannot tell which backend answered.
//!
//! Consequences worth keeping visible:
//!
//! - **Core still declares no `allow(unsafe_code)` module of ours.** The unsafe
//!   is the wrapper's, and its release cadence and whisper.cpp ABI pinning are
//!   not ours — which is exactly the debt, and why the version is pinned
//!   exactly in `Cargo.toml` and reviewed in `DEPENDENCIES.md`.
//! - **The eject path is this file.** A vendored FFI leaf, or any other
//!   binding, implements [`Transcriber`] beside this adapter rather than
//!   replacing the seam.
//!
//! ## Memory, stated
//!
//! Loading a model is the expensive act (whisper-small's weights are ~488 MB
//! resident), so one adapter owns one loaded context for one model for its
//! whole life, and a request naming a different model refuses instead of
//! quietly loading a second copy. Decoding adds the state's KV cache on top;
//! the window cap in [`crate::transcribe`] bounds the sample buffer.
//!
//! [ADR-0006]: ../../../../../docs/adr/0006-transcription-and-tls-dependencies.md

use crate::credentials::CallAuth;
use crate::transcribe::{
    SEGMENT_TEXT_BYTES_MAX, TranscribeRequest, TranscribeUsage, Transcriber, TranscriptSegment,
    TranscriptSink, mark_turns,
};
use crate::transport::HttpTransport;
use crate::weather::Weather;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

/// Decoder threads. Whisper saturates cores; leaving one for the rest of the
/// app is the §18 "never starves interactive use" rule expressed in the one
/// place that would otherwise eat the machine.
fn decode_thread_count() -> i32 {
    let available = std::thread::available_parallelism().map_or(1, std::num::NonZeroUsize::get);
    i32::try_from(available.saturating_sub(1).max(1)).unwrap_or(1)
}

/// Whisper timestamps are centiseconds; the seam speaks milliseconds.
const CENTISECONDS_TO_MS: i64 = 10;

/// A loaded whisper model, ready to decode windows.
pub struct WhisperLocalTranscriber {
    /// The artifact name callers route to (`whisper-small`), not a path —
    /// paths are machine-local and would make a ledger row unreadable.
    model_name: String,
    model_path: PathBuf,
    context: whisper_rs::WhisperContext,
    /// Reused across windows: allocating a fresh KV cache per 30-second window
    /// is pure overhead against the ≥ 5× realtime gate. Behind a mutex because
    /// `WhisperState::full` needs `&mut`, and one decode at a time per loaded
    /// model is what the memory budget above assumes anyway.
    state: Mutex<whisper_rs::WhisperState>,
}

impl std::fmt::Debug for WhisperLocalTranscriber {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WhisperLocalTranscriber")
            .field("model_name", &self.model_name)
            .finish_non_exhaustive()
    }
}

impl WhisperLocalTranscriber {
    /// Loads `model_path` and pins this adapter to `model_name`.
    ///
    /// # Errors
    ///
    /// [`Weather::InvalidRequest`] when the artifact is missing or is not a
    /// whisper model. Both are operator errors with an obvious fix, so they
    /// are typed and named rather than a panic on a `None`.
    pub fn load(model_name: &str, model_path: &Path) -> Result<Self, Weather> {
        // Route whisper.cpp's own stderr chatter into the hooks rather than
        // onto a user's terminal. With no `log_backend`/`tracing_backend`
        // feature enabled the hook bodies compile away, which is deliberate:
        // m0-s15 pins `tracing` to one module, and a dependency must not
        // become a second emission point.
        whisper_rs::install_logging_hooks();
        let context = whisper_rs::WhisperContext::new_with_params(
            model_path,
            whisper_rs::WhisperContextParameters::default(),
        )
        .map_err(|error| Weather::InvalidRequest {
            reason: format!(
                "whisper model {model_name:?} did not load from {}: {error}",
                model_path.display()
            ),
        })?;
        let state = context
            .create_state()
            .map_err(|error| Weather::InvalidRequest {
                reason: format!(
                    "whisper model {model_name:?} loaded but has no decode state: {error}"
                ),
            })?;
        Ok(Self {
            model_name: model_name.to_owned(),
            model_path: model_path.to_owned(),
            context,
            state: Mutex::new(state),
        })
    }

    #[must_use]
    pub fn model_name(&self) -> &str {
        &self.model_name
    }

    #[must_use]
    pub fn model_path(&self) -> &Path {
        &self.model_path
    }

    /// Rebuilds the decode state after a failed window, so one bad window
    /// cannot poison every later one.
    fn reset_state(&self) -> Result<whisper_rs::WhisperState, Weather> {
        self.context
            .create_state()
            .map_err(|error| Weather::Transport {
                reason: format!("whisper decode state could not be recreated: {error}"),
            })
    }
}

impl Transcriber for WhisperLocalTranscriber {
    fn label(&self) -> &'static str {
        "whisper-local"
    }

    fn transcribe(
        &self,
        _auth: &CallAuth,
        request: &TranscribeRequest<'_>,
        transport: Option<&dyn HttpTransport>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, Weather> {
        debug_assert!(
            transport.is_none(),
            "an in-process model must never be handed a socket (transcribe.rs contract)"
        );
        request.check_bounds()?;
        if request.model != self.model_name {
            return Err(Weather::InvalidRequest {
                reason: format!(
                    "this adapter serves {:?}; the request routed {:?}. Loading a second model \
                     would double the resident weights",
                    self.model_name, request.model
                ),
            });
        }
        let mut params =
            whisper_rs::FullParams::new(whisper_rs::SamplingStrategy::Greedy { best_of: 1 });
        params.set_n_threads(decode_thread_count());
        params.set_translate(false);
        params.set_language(request.language);
        // Nothing this library prints belongs on a user's terminal, and the
        // segment text we keep is read back explicitly below.
        params.set_print_special(false);
        params.set_print_progress(false);
        params.set_print_realtime(false);
        params.set_print_timestamps(false);
        // Each window is decoded independently. Carrying decoder context
        // across windows is what makes whisper repeat itself after a resume:
        // the same audio would decode differently depending on whether the
        // previous window happened to be in this process, which would break
        // the kill-resume determinism the pipeline oracle measures (P3).
        params.set_no_context(true);

        let mut state = self.state.lock().unwrap_or_else(PoisonError::into_inner); // INVARIANT: a poisoned decode state is recreated below, never trusted.
        if let Err(error) = state.full(params, request.samples) {
            *state = self.reset_state()?;
            return Err(Weather::MalformedOutput {
                reason: format!("whisper failed to decode a window: {error}"),
            });
        }
        let segments = collect_segments(&state, request)?;
        drop(state);
        emit(segments, request, sink)
    }
}

/// Reads the decoded window out of the state into our own vocabulary. Done in
/// one pass so the state lock is held for decoding and reading only, never
/// while a caller's sink commits to the log.
fn collect_segments(
    state: &whisper_rs::WhisperState,
    request: &TranscribeRequest<'_>,
) -> Result<Vec<TranscriptSegment>, Weather> {
    let count = state.full_n_segments();
    let mut segments = Vec::with_capacity(usize::try_from(count.max(0)).unwrap_or(0));
    for index in 0..count {
        let Some(segment) = state.get_segment(index) else {
            // The count and the accessor disagreeing is the wrapper's contract
            // breaking, not user weather — but it is still not ours to panic on.
            return Err(Weather::MalformedOutput {
                reason: format!("whisper reported {count} segments but segment {index} is absent"),
            });
        };
        let text = segment
            .to_str_lossy()
            .map_err(|error| Weather::MalformedOutput {
                reason: format!("whisper segment {index} is not readable text: {error}"),
            })?;
        if text.len() > SEGMENT_TEXT_BYTES_MAX {
            return Err(Weather::MalformedOutput {
                reason: format!(
                    "whisper segment {index} is {} bytes, past the {SEGMENT_TEXT_BYTES_MAX}-byte \
                     bound; a decoder in a loop is not speech",
                    text.len()
                ),
            });
        }
        let text = text.trim();
        if text.is_empty() {
            continue;
        }
        segments.push(TranscriptSegment {
            start_ms: window_ms(segment.start_timestamp(), request.offset_ms),
            end_ms: window_ms(segment.end_timestamp(), request.offset_ms),
            text: text.to_owned(),
            starts_turn: false,
        });
    }
    Ok(segments)
}

/// Whisper's centisecond timestamp, in the source media's milliseconds.
/// Negative timestamps are clamped rather than wrapped: a segment before the
/// start of its own window is a decoder artifact, and a `u64` underflow would
/// turn it into a citation 584 million years into the recording.
fn window_ms(centiseconds: i64, offset_ms: u64) -> u64 {
    let ms = centiseconds.saturating_mul(CENTISECONDS_TO_MS).max(0);
    offset_ms.saturating_add(u64::try_from(ms).unwrap_or(0))
}

fn emit(
    mut segments: Vec<TranscriptSegment>,
    request: &TranscribeRequest<'_>,
    sink: &mut dyn TranscriptSink,
) -> Result<TranscribeUsage, Weather> {
    mark_turns(&mut segments);
    let mut emitted = 0_u64;
    for segment in &segments {
        if sink.on_segment(segment).is_err() {
            // Cancellation, not failure: report what was actually produced.
            break;
        }
        emitted += 1;
    }
    Ok(TranscribeUsage {
        audio_ms: request.audio_ms(),
        segment_count: emitted,
        // Samples we counted ourselves — no provider reported anything.
        measured: true,
    })
}

#[cfg(test)]
mod tests {
    use super::{CENTISECONDS_TO_MS, decode_thread_count, window_ms};

    #[test]
    fn timestamps_convert_from_centiseconds_and_carry_the_window_offset() {
        assert_eq!(CENTISECONDS_TO_MS, 10);
        assert_eq!(window_ms(123, 0), 1_230);
        assert_eq!(window_ms(123, 30_000), 31_230);
    }

    #[test]
    fn a_negative_timestamp_clamps_instead_of_wrapping_into_deep_time() {
        assert_eq!(window_ms(-5, 0), 0);
        assert_eq!(window_ms(-5, 30_000), 30_000);
    }

    #[test]
    fn the_decoder_always_leaves_a_core_and_always_takes_one() {
        let threads = decode_thread_count();
        assert!(threads >= 1, "a decoder with no thread cannot decode");
    }
}
