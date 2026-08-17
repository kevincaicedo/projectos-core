//! Audio decoding and resampling for TRANSCRIBE (m1-s03, [ADR-0006] §3).
//!
//! Two jobs, split by the "could 50 lines of ours do it?" test:
//!
//! - **Demux and decode is `symphonia`'s.** MP3, AAC-in-MP4, ALAC, FLAC,
//!   Vorbis, WAV, and the MKV/WebM container are a decade of format edge cases
//!   and a fuzz surface we would rather buy than own.
//! - **Resampling is ours.** Whisper wants 16 kHz mono `f32` and real sources
//!   are 44.1/48 kHz stereo. A windowed-sinc resampler whose quality bar is
//!   "the ASR model cannot tell" is well inside 50 lines of ours, and adding a
//!   dependency for it would be a dependency we could not justify.
//!
//! ## Bounded by construction
//!
//! Nothing here holds a recording. [`AudioSource::decode_into`] appends one
//! packet's worth of resampled samples to a caller-owned buffer and returns;
//! the caller decides the window and therefore the memory. The resampler's own
//! state is a tap-width history — 64 samples — regardless of file size, so a
//! four-hour recording and a four-second one cost the same resident bytes
//! (§18 RSS gate).
//!
//! [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md

use pos_gateway::AUDIO_SAMPLE_RATE_HZ;
use std::fmt;
use symphonia::core::codecs::audio::AudioDecoderOptions;
use symphonia::core::errors::Error as SymphoniaError;
use symphonia::core::formats::probe::Hint;
use symphonia::core::formats::{FormatOptions, TrackType};
use symphonia::core::io::{MediaSource, MediaSourceStream, MediaSourceStreamOptions};
use symphonia::core::meta::MetadataOptions;

/// Half-width of the resampling kernel; 64 taps in total.
///
/// Measured rather than guessed (STYLE: measure, don't assume). At 32 taps a
/// 12 kHz tone in a 48 kHz source survived downsampling at 0.33 amplitude —
/// −9.6 dB, which folds to an audible 4 kHz artifact right inside the speech
/// band. Sixty-four taps and the guard band below put the same tone under
/// 0.02. The cost is 64 multiply-adds per output sample, about 1M ops per
/// second of audio, which is noise beside the model that consumes it.
const RESAMPLE_HALF_TAPS: usize = 32;

/// Fraction of the output Nyquist the passband reaches. A brick wall exactly
/// at Nyquist would put the filter's transition band *above* it, where it
/// aliases; pulling the corner down to 7.4 kHz costs nothing a speech model
/// uses — whisper's own mel filterbank stops at 8 kHz — and buys the guard
/// band that makes the stopband real.
const RESAMPLE_CUTOFF_GUARD: f64 = 0.92;

/// Sample rates outside this range are not speech recordings; refusing beats
/// allocating a resampler state for a header that claims 4 GHz (L8).
const SOURCE_RATE_HZ_MIN: u32 = 4_000;
const SOURCE_RATE_HZ_MAX: u32 = 768_000;

/// Channels one recording may carry. Above this the file is not an interview.
const SOURCE_CHANNEL_COUNT_MAX: usize = 32;

/// Typed decode failures. All of them are operating weather — a corrupt
/// upload, an unsupported codec, a truncated file — and none is an assertion.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum AudioError {
    /// The container or codec is not one this build decodes. Names what was
    /// found so the DLQ row is actionable rather than "audio failed".
    Unsupported {
        reason: String,
    },
    /// The stream carries no audio track at all (a silent screen recording).
    NoAudioTrack,
    /// The declared rate or channel count is outside the stated bounds.
    UnusableFormat {
        reason: String,
    },
    /// The bytes stopped making sense partway through.
    Corrupt {
        reason: String,
    },
    Io {
        reason: String,
    },
}

impl fmt::Display for AudioError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unsupported { reason } => write!(formatter, "unsupported audio: {reason}"),
            Self::NoAudioTrack => formatter.write_str("the media carries no audio track"),
            Self::UnusableFormat { reason } => write!(formatter, "unusable audio format: {reason}"),
            Self::Corrupt { reason } => write!(formatter, "audio stream is corrupt: {reason}"),
            Self::Io { reason } => write!(formatter, "audio read failed: {reason}"),
        }
    }
}

impl std::error::Error for AudioError {}

impl AudioError {
    /// The stable code the DLQ row and the source-health card carry.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Unsupported { .. } => "audio_unsupported",
            Self::NoAudioTrack => "audio_no_track",
            Self::UnusableFormat { .. } => "audio_format_unusable",
            Self::Corrupt { .. } => "audio_corrupt",
            Self::Io { .. } => "audio_io_failure",
        }
    }

    /// Whether another attempt could plausibly succeed. A codec we do not have
    /// will still not be there next time; a read that failed might.
    #[must_use]
    pub const fn is_retriable(&self) -> bool {
        matches!(self, Self::Io { .. })
    }
}

/// A decoded audio stream, delivered as 16 kHz mono `f32`.
pub struct AudioSource {
    format: Box<dyn symphonia::core::formats::FormatReader>,
    decoder: Box<dyn symphonia::core::codecs::audio::AudioDecoder>,
    track_id: u32,
    resampler: Resampler,
    /// Interleaved scratch for one decoded packet. Reused across packets so
    /// the decode loop allocates once, not once per packet.
    interleaved: Vec<f32>,
    channel_count: usize,
    /// Samples produced so far, which is also the position in the output
    /// timeline — the only clock this module trusts, because container
    /// timestamps disagree with sample counts often enough to matter.
    produced_samples: u64,
    ended: bool,
}

impl AudioSource {
    /// Probes `source` and prepares the decode chain.
    ///
    /// # Errors
    ///
    /// [`AudioError`] for an unknown container, a missing audio track, an
    /// undecodable codec, or a format outside the stated bounds.
    pub fn open(source: Box<dyn MediaSource>) -> Result<Self, AudioError> {
        let stream = MediaSourceStream::new(source, MediaSourceStreamOptions::default());
        // No extension hint: the pipeline sniffs content, never file names
        // (m1-s02's rule), and symphonia's probe reads magic bytes anyway.
        let format = symphonia::default::get_probe()
            .probe(
                &Hint::new(),
                stream,
                FormatOptions::default(),
                MetadataOptions::default(),
            )
            .map_err(|error| map_probe_error(&error))?;
        let track = format
            .default_track(TrackType::Audio)
            .ok_or(AudioError::NoAudioTrack)?;
        let track_id = track.id;
        let parameters = track
            .codec_params
            .as_ref()
            .and_then(symphonia::core::codecs::CodecParameters::audio)
            .ok_or(AudioError::NoAudioTrack)?
            .clone();
        let sample_rate = parameters
            .sample_rate
            .ok_or_else(|| AudioError::UnusableFormat {
                reason: "the track declares no sample rate".to_owned(),
            })?;
        if !(SOURCE_RATE_HZ_MIN..=SOURCE_RATE_HZ_MAX).contains(&sample_rate) {
            return Err(AudioError::UnusableFormat {
                reason: format!(
                    "{sample_rate} Hz is outside {SOURCE_RATE_HZ_MIN}..={SOURCE_RATE_HZ_MAX}"
                ),
            });
        }
        let channel_count = parameters
            .channels
            .as_ref()
            .map_or(1, |channels| channels.count().max(1));
        if channel_count > SOURCE_CHANNEL_COUNT_MAX {
            return Err(AudioError::UnusableFormat {
                reason: format!("{channel_count} channels exceeds {SOURCE_CHANNEL_COUNT_MAX}"),
            });
        }
        let decoder = symphonia::default::get_codecs()
            .make_audio_decoder(&parameters, &AudioDecoderOptions::default())
            .map_err(|error| AudioError::Unsupported {
                reason: error.to_string(),
            })?;
        Ok(Self {
            format,
            decoder,
            track_id,
            resampler: Resampler::new(sample_rate),
            interleaved: Vec::new(),
            channel_count,
            produced_samples: 0,
            ended: false,
        })
    }

    /// Milliseconds of 16 kHz output produced so far.
    #[must_use]
    pub const fn position_ms(&self) -> u64 {
        self.produced_samples * 1_000 / AUDIO_SAMPLE_RATE_HZ as u64
    }

    #[must_use]
    pub const fn is_ended(&self) -> bool {
        self.ended
    }

    /// Decodes the next packet and appends its 16 kHz mono samples to `out`.
    /// Returns how many samples were appended; `Ok(0)` with
    /// [`Self::is_ended`] means the stream is finished.
    ///
    /// A packet is the granularity on purpose: it is what the decoder already
    /// works in, and it keeps the caller — not this module — in charge of how
    /// much audio is resident (§18).
    ///
    /// # Errors
    ///
    /// [`AudioError::Corrupt`] for undecodable bytes, [`AudioError::Io`] for a
    /// read failure. A single skippable decode error is retried on the next
    /// packet rather than failing the item, because one damaged frame in an
    /// hour of speech should cost a frame.
    pub fn decode_into(&mut self, out: &mut Vec<f32>) -> Result<usize, AudioError> {
        if self.ended {
            return Ok(0);
        }
        let before = out.len();
        loop {
            let packet = match self.format.next_packet() {
                Ok(Some(packet)) => packet,
                Ok(None) => {
                    self.ended = true;
                    self.resampler.flush(out);
                    break;
                }
                Err(SymphoniaError::IoError(error))
                    if error.kind() == std::io::ErrorKind::UnexpectedEof =>
                {
                    self.ended = true;
                    self.resampler.flush(out);
                    break;
                }
                Err(error) => return Err(map_stream_error(&error)),
            };
            if packet.track_id != self.track_id {
                continue;
            }
            match self.decoder.decode(&packet) {
                Ok(buffer) => {
                    self.interleaved
                        .resize(buffer.samples_interleaved(), 0.0_f32);
                    buffer.copy_to_slice_interleaved(&mut self.interleaved);
                    self.resampler
                        .push_interleaved(&self.interleaved, self.channel_count, out);
                    break;
                }
                // Decode errors that symphonia marks recoverable are one bad
                // frame; skipping it is the honest behaviour for a recording
                // with a glitch in it.
                Err(SymphoniaError::DecodeError(_)) => continue,
                Err(error) => return Err(map_stream_error(&error)),
            }
        }
        let appended = out.len() - before;
        self.produced_samples = self.produced_samples.saturating_add(appended as u64);
        Ok(appended)
    }
}

fn map_probe_error(error: &SymphoniaError) -> AudioError {
    match error {
        SymphoniaError::Unsupported(reason) => AudioError::Unsupported {
            reason: (*reason).to_owned(),
        },
        SymphoniaError::IoError(io) => AudioError::Io {
            reason: io.to_string(),
        },
        other => AudioError::Unsupported {
            reason: other.to_string(),
        },
    }
}

fn map_stream_error(error: &SymphoniaError) -> AudioError {
    match error {
        SymphoniaError::IoError(io) => AudioError::Io {
            reason: io.to_string(),
        },
        SymphoniaError::Unsupported(reason) => AudioError::Unsupported {
            reason: (*reason).to_owned(),
        },
        other => AudioError::Corrupt {
            reason: other.to_string(),
        },
    }
}

/// Streaming windowed-sinc resampler to 16 kHz mono.
///
/// Downmixing is a channel average rather than a stereo-to-mono matrix:
/// interview recordings put one speaker per channel often enough that dropping
/// a channel would drop a person, and any weighting beyond equal is a mixing
/// decision we have no basis for.
struct Resampler {
    /// Input samples per output sample. `1.0` exactly when the source is
    /// already 16 kHz, and at that ratio the kernel is an identity — the
    /// no-resampling path costs one multiply per sample, not a special case.
    ratio: f64,
    /// Fractional input index of the next output sample, relative to
    /// `history[0]`.
    position: f64,
    /// Input samples still needed: the taps around `position`, and nothing
    /// else. Bounded by `2 * RESAMPLE_HALF_TAPS + ratio`.
    history: Vec<f32>,
    /// Normalized cutoff, `min(1, out/in)` — the anti-aliasing filter when
    /// downsampling, transparent when upsampling.
    cutoff: f64,
    /// Mono scratch for one packet.
    mono: Vec<f32>,
    passthrough: bool,
}

impl Resampler {
    fn new(source_rate_hz: u32) -> Self {
        let ratio = f64::from(source_rate_hz) / f64::from(AUDIO_SAMPLE_RATE_HZ);
        Self {
            ratio,
            position: 0.0,
            history: Vec::new(),
            cutoff: (1.0_f64 / ratio).min(1.0) * RESAMPLE_CUTOFF_GUARD,
            mono: Vec::new(),
            passthrough: source_rate_hz == AUDIO_SAMPLE_RATE_HZ,
        }
    }

    fn push_interleaved(&mut self, interleaved: &[f32], channel_count: usize, out: &mut Vec<f32>) {
        self.mono.clear();
        if channel_count <= 1 {
            self.mono.extend_from_slice(interleaved);
        } else {
            let scale = 1.0_f32 / channel_count as f32;
            for frame in interleaved.chunks_exact(channel_count) {
                self.mono.push(frame.iter().sum::<f32>() * scale);
            }
        }
        if self.passthrough {
            out.extend_from_slice(&self.mono);
            return;
        }
        self.history.extend_from_slice(&self.mono);
        self.emit(out, false);
    }

    /// Emits every output sample whose kernel is fully inside `history`, then
    /// drops the history that can no longer be reached.
    fn emit(&mut self, out: &mut Vec<f32>, draining: bool) {
        let half = RESAMPLE_HALF_TAPS as f64;
        loop {
            let last_needed = self.position + half;
            if !draining && last_needed >= self.history.len() as f64 {
                break;
            }
            if draining && self.position >= self.history.len() as f64 {
                break;
            }
            out.push(self.sample_at(self.position));
            self.position += self.ratio;
        }
        // Everything before the earliest tap the next output needs is dead.
        let keep_from = (self.position - half).floor().max(0.0);
        #[expect(
            clippy::cast_possible_truncation,
            clippy::cast_sign_loss,
            reason = "clamped non-negative and bounded by history.len()"
        )]
        let drop_count = (keep_from as usize).min(self.history.len());
        if drop_count > 0 {
            self.history.drain(..drop_count);
            self.position -= drop_count as f64;
        }
    }

    /// One output sample: windowed-sinc interpolation around `center`.
    fn sample_at(&self, center: f64) -> f32 {
        let half = RESAMPLE_HALF_TAPS as i64;
        #[expect(
            clippy::cast_possible_truncation,
            reason = "center is bounded by history.len(), far inside i64"
        )]
        let base = center.floor() as i64;
        let mut sum = 0.0_f64;
        let mut weight_total = 0.0_f64;
        for offset in (base - half + 1)..=(base + half) {
            let distance = center - offset as f64;
            let weight = sinc(distance * self.cutoff) * blackman(distance / half as f64);
            // The divisor counts every tap, including the ones hanging off the
            // ends of the stream. Normalizing by only the *present* taps looks
            // like it protects the gain at the edges, and it does — but it
            // also destroys the kernel's cancellation there, which turns the
            // first and last few milliseconds of a downsample into an
            // unattenuated copy of whatever was above Nyquist. A constant
            // divisor costs a few milliseconds of fade instead.
            weight_total += weight;
            let Ok(index) = usize::try_from(offset) else {
                continue;
            };
            let Some(sample) = self.history.get(index) else {
                continue;
            };
            sum += f64::from(*sample) * weight;
        }
        if weight_total.abs() < f64::EPSILON {
            return 0.0;
        }
        #[expect(
            clippy::cast_possible_truncation,
            reason = "audio samples are f32 by definition of the seam"
        )]
        let value = (sum / weight_total) as f32;
        value.clamp(-1.0, 1.0)
    }

    /// Emits the tail once the stream has ended, so the last fraction of a
    /// second is transcribed rather than silently dropped.
    fn flush(&mut self, out: &mut Vec<f32>) {
        if self.passthrough {
            return;
        }
        self.emit(out, true);
        self.history.clear();
    }
}

fn sinc(x: f64) -> f64 {
    if x.abs() < 1e-9 {
        return 1.0;
    }
    let scaled = std::f64::consts::PI * x;
    scaled.sin() / scaled
}

/// Blackman window over `[-1, 1]`; zero outside, which is what makes the tap
/// loop above a finite kernel rather than a truncated infinite one.
fn blackman(x: f64) -> f64 {
    if x.abs() >= 1.0 {
        return 0.0;
    }
    let phase = std::f64::consts::PI * (x + 1.0);
    0.42 - 0.5 * phase.cos() + 0.08 * (2.0 * phase).cos()
}

#[cfg(test)]
mod tests {
    use super::{AUDIO_SAMPLE_RATE_HZ, AudioError, RESAMPLE_HALF_TAPS, Resampler};

    /// A sine at `frequency_hz`, `seconds` long, at `rate_hz`.
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "fixture lengths are small positive literals"
    )]
    fn sine(rate_hz: u32, frequency_hz: f64, seconds: f64) -> Vec<f32> {
        let count = (f64::from(rate_hz) * seconds) as usize;
        (0..count)
            .map(|index| {
                let t = index as f64 / f64::from(rate_hz);
                (std::f64::consts::TAU * frequency_hz * t).sin() as f32
            })
            .collect()
    }

    /// Zero crossings per second, which is 2× the frequency of a clean sine —
    /// a frequency check that needs no FFT.
    fn zero_crossings_per_second(samples: &[f32], rate_hz: u32) -> f64 {
        let crossings = samples
            .windows(2)
            .filter(|pair| (pair[0] < 0.0) != (pair[1] < 0.0))
            .count();
        crossings as f64 * f64::from(rate_hz) / samples.len() as f64
    }

    fn resample(source_rate_hz: u32, input: &[f32]) -> Vec<f32> {
        let mut resampler = Resampler::new(source_rate_hz);
        let mut out = Vec::new();
        // Push in awkward packet sizes so the history/position bookkeeping is
        // exercised at boundaries rather than only on one big slice.
        for packet in input.chunks(1_021) {
            resampler.push_interleaved(packet, 1, &mut out);
        }
        resampler.flush(&mut out);
        out
    }

    #[test]
    fn a_16k_source_passes_through_sample_for_sample() {
        let input = sine(AUDIO_SAMPLE_RATE_HZ, 440.0, 0.25);
        let output = resample(AUDIO_SAMPLE_RATE_HZ, &input);
        assert_eq!(
            output, input,
            "a source already at the seam's rate must not be filtered at all"
        );
    }

    #[test]
    fn downsampling_48k_speech_preserves_its_frequency_and_its_length() {
        // 300 Hz is inside the speech band and far below the 8 kHz Nyquist of
        // the output, so a correct resampler moves it unchanged.
        let input = sine(48_000, 300.0, 1.0);
        let output = resample(48_000, &input);
        let expected = AUDIO_SAMPLE_RATE_HZ as usize;
        assert!(
            output.len().abs_diff(expected) <= RESAMPLE_HALF_TAPS * 2,
            "expected ~{expected} samples, got {}",
            output.len()
        );
        let measured = zero_crossings_per_second(&output, AUDIO_SAMPLE_RATE_HZ);
        assert!(
            (measured - 600.0).abs() < 6.0,
            "300 Hz should stay 300 Hz; measured {} Hz",
            measured / 2.0
        );
        let peak = output.iter().fold(0.0_f32, |peak, s| peak.max(s.abs()));
        assert!(
            (0.9..=1.01).contains(&peak),
            "resampling must not change loudness; peak {peak}"
        );
    }

    #[test]
    fn downsampling_44_1k_handles_a_non_integer_ratio() {
        let input = sine(44_100, 440.0, 0.5);
        let output = resample(44_100, &input);
        let expected = AUDIO_SAMPLE_RATE_HZ as usize / 2;
        assert!(
            output.len().abs_diff(expected) <= RESAMPLE_HALF_TAPS * 2,
            "expected ~{expected} samples, got {}",
            output.len()
        );
        let measured = zero_crossings_per_second(&output, AUDIO_SAMPLE_RATE_HZ);
        assert!(
            (measured - 880.0).abs() < 10.0,
            "440 Hz should stay 440 Hz; measured {} Hz",
            measured / 2.0
        );
    }

    #[test]
    fn a_tone_above_the_output_nyquist_is_attenuated_rather_than_aliased() {
        // 7 kHz at 48 kHz in, 16 kHz out: above the 8 kHz output Nyquist it
        // would fold down to an audible artifact without the anti-alias
        // kernel. 7 kHz is still legal output, so it survives; 12 kHz must not.
        let input = sine(48_000, 12_000.0, 0.5);
        let output = resample(48_000, &input);
        // The kernel needs its taps on both sides, so the first and last
        // RESAMPLE_HALF_TAPS output samples see zero-padding instead of audio
        // and cancel incompletely. That is ~2 ms at each end of a recording,
        // inherent to any finite filter at a stream boundary, and it happens
        // once per file rather than once per transcription window (the
        // resampler runs continuously across the whole stream). The steady
        // state is what a transcript is made of, so that is what is asserted.
        let edge = RESAMPLE_HALF_TAPS;
        let interior = &output[edge..output.len() - edge];
        let peak = interior.iter().fold(0.0_f32, |peak, s| peak.max(s.abs()));
        assert!(
            peak < 0.01,
            "a 12 kHz tone must be filtered out, not folded down to 4 kHz; peak {peak}"
        );
    }

    #[test]
    fn upsampling_from_8k_produces_twice_the_samples() {
        let input = sine(8_000, 200.0, 0.5);
        let output = resample(8_000, &input);
        let expected = AUDIO_SAMPLE_RATE_HZ as usize / 2;
        assert!(
            output.len().abs_diff(expected) <= RESAMPLE_HALF_TAPS * 2,
            "expected ~{expected} samples, got {}",
            output.len()
        );
    }

    #[test]
    fn stereo_frames_average_rather_than_dropping_a_channel() {
        // One speaker hard-left, one hard-right: dropping a channel would drop
        // a person, which is the failure this test exists to prevent.
        let mut resampler = Resampler::new(AUDIO_SAMPLE_RATE_HZ);
        let mut out = Vec::new();
        resampler.push_interleaved(&[1.0, 0.0, 0.0, 1.0, 0.5, 0.5], 2, &mut out);
        assert_eq!(out, vec![0.5, 0.5, 0.5]);
    }

    #[test]
    fn the_history_stays_bounded_no_matter_how_long_the_stream_is() {
        let mut resampler = Resampler::new(48_000);
        let mut out = Vec::new();
        let packet = vec![0.1_f32; 4_096];
        for _ in 0..200 {
            resampler.push_interleaved(&packet, 1, &mut out);
            assert!(
                resampler.history.len() <= 4_096 + RESAMPLE_HALF_TAPS * 4,
                "resampler state grew to {} samples; it must not depend on stream length",
                resampler.history.len()
            );
        }
    }

    #[test]
    fn error_codes_are_stable_and_only_io_invites_a_retry() {
        assert_eq!(AudioError::NoAudioTrack.code(), "audio_no_track");
        assert!(!AudioError::NoAudioTrack.is_retriable());
        assert!(
            AudioError::Io {
                reason: "disk".to_owned()
            }
            .is_retriable()
        );
        assert!(
            !AudioError::Unsupported {
                reason: "codec".to_owned()
            }
            .is_retriable(),
            "a codec we do not have will still not be there next time"
        );
    }
}
