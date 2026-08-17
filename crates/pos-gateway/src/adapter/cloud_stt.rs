//! The cloud STT adapter (m1-s03): OpenAI-shaped `/v1/audio/transcriptions`,
//! which OpenAI, Groq, Deepgram's compatibility layer, and every
//! OpenAI-compatible gateway speak.
//!
//! It is a peer of the local whisper adapter, not a fallback beneath it: a
//! user with a cloud key and no C++ toolchain routes here, and a `local_only`
//! project never can — the policy gate refuses the remote endpoint before this
//! file is reached (F43).
//!
//! ## Why it takes samples rather than the original file
//!
//! [`Transcriber`] speaks one input shape: 16 kHz mono `f32` windows. This
//! adapter re-encodes each window as a 16-bit PCM WAV — a 44-byte header and a
//! sample-width conversion, well inside "could 50 lines of ours do it?" — so
//! decoding, resampling, and windowing happen once in the pipeline instead of
//! once per backend. A window is also the unit of durability: uploading a
//! whole two-hour recording would make a mid-transfer failure cost everything,
//! where a window costs a window.
//!
//! At the seam's stated 120 s cap one upload is ~3.8 MB of WAV, so the request
//! body is bounded by the same constant that bounds the sample buffer (L8).

use crate::credentials::CallAuth;
use crate::transcribe::{
    AUDIO_SAMPLE_RATE_HZ, SEGMENT_TEXT_BYTES_MAX, TranscribeRequest, TranscribeUsage, Transcriber,
    TranscriptSegment, TranscriptSink, mark_turns,
};
use crate::transport::{
    HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, ResponseHandler, StreamAbort,
    TransportError,
};
use crate::weather::Weather;

/// Response bytes one transcription may return. A verbose transcript of a
/// two-minute window is kilobytes; the cap refuses a runaway peer rather than
/// growing a buffer to fit it (L8).
const RESPONSE_BYTES_MAX: usize = 4 * 1024 * 1024;

/// Fixed multipart boundary. Deterministic on purpose: a recorded-fixture
/// conformance row must be able to compare request bytes, and the collision
/// risk a random boundary buys off is a WAV frame reproducing this exact
/// 38-byte ASCII string — which is not the failure mode worth spending
/// non-determinism on.
const MULTIPART_BOUNDARY: &str = "projectos-transcribe-boundary-7f3c9a";

/// Bytes of WAV header before the sample data.
const WAV_HEADER_BYTES: usize = 44;

/// The transcription endpoint, its model, and how to authenticate. The base
/// URL is the *endpoint's*, so an OpenAI-compatible gateway is configuration
/// rather than a second adapter.
#[derive(Clone, Debug)]
pub struct CloudSttAdapter {
    pub base_url: String,
    /// Some deployments answer `verbose_json` (segments with timestamps) and
    /// some only `json` (one blob of text). Declared, never guessed: a
    /// deployment that cannot segment produces one segment spanning its
    /// window, and the citation lands on the window rather than on a
    /// fabricated second.
    pub supports_segments: bool,
}

impl CloudSttAdapter {
    fn url(&self) -> String {
        format!(
            "{}/v1/audio/transcriptions",
            self.base_url.trim_end_matches('/')
        )
    }
}

impl Transcriber for CloudSttAdapter {
    fn label(&self) -> &'static str {
        "cloud-stt"
    }

    fn transcribe(
        &self,
        auth: &CallAuth,
        request: &TranscribeRequest<'_>,
        transport: Option<&dyn HttpTransport>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, Weather> {
        request.check_bounds()?;
        let Some(transport) = transport else {
            return Err(Weather::TransportUnavailable {
                selection: "remote",
            });
        };
        let mut headers = vec![(
            "content-type",
            format!("multipart/form-data; boundary={MULTIPART_BOUNDARY}"),
        )];
        if let CallAuth::ApiKey(key) = auth {
            headers.push(("authorization", format!("Bearer {}", key.expose())));
        }
        let plan = HttpRequestPlan {
            method: HttpMethod::Post,
            url: self.url(),
            headers,
            body: multipart_body(request, self.supports_segments),
            timeout_ms: request_timeout_ms(request),
        };
        let mut collector = BoundedBody::default();
        transport
            .execute(&plan, &mut collector)
            .map_err(map_transport_error)?;
        let head = collector.head.ok_or_else(|| Weather::MalformedOutput {
            reason: "the endpoint returned no response head".to_owned(),
        })?;
        if collector.overflowed {
            return Err(Weather::MalformedOutput {
                reason: format!("the transcript response exceeded {RESPONSE_BYTES_MAX} bytes"),
            });
        }
        status_weather(&head, &collector.body)?;
        let mut segments = parse_segments(&collector.body, request)?;
        mark_turns(&mut segments);
        let mut emitted = 0_u64;
        for segment in &segments {
            if sink.on_segment(segment).is_err() {
                break;
            }
            emitted += 1;
        }
        Ok(TranscribeUsage {
            audio_ms: request.audio_ms(),
            segment_count: emitted,
            // We counted the samples we uploaded. No STT endpoint in this wire
            // shape reports billed audio, so claiming a measurement would be a
            // guess wearing a measurement's clothes.
            measured: false,
        })
    }
}

/// Upload plus decode time. Generous, because a cloud STT round trip on a
/// two-minute window over a domestic uplink is not a chat completion — and
/// still bounded, because "wait forever" is not a budget (L8).
fn request_timeout_ms(request: &TranscribeRequest<'_>) -> u32 {
    let audio_seconds = u32::try_from(request.audio_ms() / 1_000).unwrap_or(120);
    // 4× realtime plus a 30 s floor covers a slow uplink without unbounding.
    30_000_u32.saturating_add(audio_seconds.saturating_mul(4_000))
}

/// The multipart body: the WAV, the model, the response format, the language.
fn multipart_body(request: &TranscribeRequest<'_>, supports_segments: bool) -> Vec<u8> {
    let wav = wav_pcm16(request.samples);
    let mut body = Vec::with_capacity(wav.len() + 512);
    push_field(&mut body, "model", request.model);
    push_field(
        &mut body,
        "response_format",
        if supports_segments {
            "verbose_json"
        } else {
            "json"
        },
    );
    if let Some(language) = request.language {
        push_field(&mut body, "language", language);
    }
    body.extend_from_slice(format!("--{MULTIPART_BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(
        b"content-disposition: form-data; name=\"file\"; filename=\"window.wav\"\r\n\
          content-type: audio/wav\r\n\r\n",
    );
    body.extend_from_slice(&wav);
    body.extend_from_slice(format!("\r\n--{MULTIPART_BOUNDARY}--\r\n").as_bytes());
    body
}

fn push_field(body: &mut Vec<u8>, name: &str, value: &str) {
    body.extend_from_slice(format!("--{MULTIPART_BOUNDARY}\r\n").as_bytes());
    body.extend_from_slice(
        format!("content-disposition: form-data; name=\"{name}\"\r\n\r\n{value}\r\n").as_bytes(),
    );
}

/// 16 kHz mono 16-bit PCM WAV. The whole format, because the whole format is
/// a fixed header — see the module doc for why this is ours.
fn wav_pcm16(samples: &[f32]) -> Vec<u8> {
    let data_bytes = u32::try_from(samples.len().saturating_mul(2)).unwrap_or(u32::MAX);
    let mut wav = Vec::with_capacity(WAV_HEADER_BYTES + samples.len() * 2);
    wav.extend_from_slice(b"RIFF");
    wav.extend_from_slice(&(36_u32.saturating_add(data_bytes)).to_le_bytes());
    wav.extend_from_slice(b"WAVEfmt ");
    wav.extend_from_slice(&16_u32.to_le_bytes()); // PCM chunk size
    wav.extend_from_slice(&1_u16.to_le_bytes()); // format: PCM
    wav.extend_from_slice(&1_u16.to_le_bytes()); // channels: mono
    wav.extend_from_slice(&AUDIO_SAMPLE_RATE_HZ.to_le_bytes());
    wav.extend_from_slice(&(AUDIO_SAMPLE_RATE_HZ.saturating_mul(2)).to_le_bytes()); // byte rate
    wav.extend_from_slice(&2_u16.to_le_bytes()); // block align
    wav.extend_from_slice(&16_u16.to_le_bytes()); // bits per sample
    wav.extend_from_slice(b"data");
    wav.extend_from_slice(&data_bytes.to_le_bytes());
    for sample in samples {
        wav.extend_from_slice(&to_pcm16(*sample).to_le_bytes());
    }
    wav
}

/// Clamped conversion. A decoder that overshoots `[-1.0, 1.0]` — and they do,
/// on loud material — must saturate rather than wrap into the opposite sign,
/// which is audible as a click and reads to a model as a consonant.
fn to_pcm16(sample: f32) -> i16 {
    let clamped = sample.clamp(-1.0, 1.0);
    let scaled = clamped * f32::from(i16::MAX);
    // Rounding before the cast keeps the conversion symmetric; the clamp above
    // already guarantees the value is in range.
    #[expect(
        clippy::cast_possible_truncation,
        reason = "clamped to [-32767, 32767] on the line above"
    )]
    let value = scaled.round() as i16;
    value
}

/// Reads the transcript out of the response body.
fn parse_segments(
    body: &[u8],
    request: &TranscribeRequest<'_>,
) -> Result<Vec<TranscriptSegment>, Weather> {
    let parsed: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| Weather::MalformedOutput {
            reason: format!("transcription response is not JSON: {error}"),
        })?;
    if let Some(segments) = parsed.get("segments").and_then(serde_json::Value::as_array) {
        let mut collected = Vec::with_capacity(segments.len());
        for segment in segments {
            let Some(text) = segment.get("text").and_then(serde_json::Value::as_str) else {
                continue;
            };
            let text = text.trim();
            if text.is_empty() {
                continue;
            }
            if text.len() > SEGMENT_TEXT_BYTES_MAX {
                return Err(Weather::MalformedOutput {
                    reason: format!(
                        "a transcript segment is {} bytes, past the \
                         {SEGMENT_TEXT_BYTES_MAX}-byte bound",
                        text.len()
                    ),
                });
            }
            let start_ms = seconds_to_ms(segment.get("start"), request.offset_ms);
            let end_ms = seconds_to_ms(segment.get("end"), request.offset_ms);
            collected.push(TranscriptSegment {
                start_ms,
                end_ms: end_ms.max(start_ms),
                text: text.to_owned(),
                starts_turn: false,
            });
        }
        return Ok(collected);
    }
    // A `json`-only deployment. One segment spanning the window is the honest
    // reading: we know what was said, not when inside the window it was said.
    let Some(text) = parsed.get("text").and_then(serde_json::Value::as_str) else {
        return Err(Weather::MalformedOutput {
            reason: "transcription response carries neither `segments` nor `text`".to_owned(),
        });
    };
    let text = text.trim();
    if text.is_empty() {
        return Ok(Vec::new());
    }
    Ok(vec![TranscriptSegment {
        start_ms: request.offset_ms,
        end_ms: request.offset_ms.saturating_add(request.audio_ms()),
        text: text.to_owned(),
        starts_turn: false,
    }])
}

/// Seconds-as-float (the wire shape) into milliseconds from the media start.
fn seconds_to_ms(value: Option<&serde_json::Value>, offset_ms: u64) -> u64 {
    let seconds = value.and_then(serde_json::Value::as_f64).unwrap_or(0.0);
    if !seconds.is_finite() || seconds <= 0.0 {
        return offset_ms;
    }
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "guarded finite and positive; a transcript longer than u64 ms does not exist"
    )]
    let ms = (seconds * 1_000.0).round() as u64;
    offset_ms.saturating_add(ms)
}

/// Maps HTTP status onto weather, using the body only for its stated message.
fn status_weather(head: &HttpHead, body: &[u8]) -> Result<(), Weather> {
    if head.status < 300 {
        return Ok(());
    }
    let detail = String::from_utf8_lossy(&body[..body.len().min(512)]).into_owned();
    Err(match head.status {
        401 | 403 => Weather::AuthRejected {
            status: head.status,
        },
        429 => Weather::RateLimited {
            retry_after_ms: head
                .header("retry-after")
                .and_then(|value| value.parse::<u32>().ok())
                .map(|seconds| seconds.saturating_mul(1_000)),
        },
        400 => Weather::InvalidRequest {
            reason: format!("the endpoint rejected the request: {detail}"),
        },
        _ => Weather::Transport {
            reason: format!("the endpoint answered HTTP {}: {detail}", head.status),
        },
    })
}

fn map_transport_error(error: TransportError) -> Weather {
    match error {
        TransportError::Timeout { timeout_ms } => Weather::Timeout { timeout_ms },
        other => Weather::Transport {
            reason: other.to_string(),
        },
    }
}

/// Collects a bounded response body. Transcription responses are small and
/// arrive whole; there is nothing to stream, so the only discipline needed is
/// the cap.
#[derive(Default)]
struct BoundedBody {
    head: Option<HttpHead>,
    body: Vec<u8>,
    overflowed: bool,
}

impl ResponseHandler for BoundedBody {
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort> {
        self.head = Some(head.clone());
        Ok(())
    }

    fn on_chunk(&mut self, chunk: &[u8]) -> Result<(), StreamAbort> {
        if self.body.len().saturating_add(chunk.len()) > RESPONSE_BYTES_MAX {
            self.overflowed = true;
            return Err(StreamAbort);
        }
        self.body.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        CloudSttAdapter, MULTIPART_BOUNDARY, WAV_HEADER_BYTES, multipart_body, parse_segments,
        to_pcm16, wav_pcm16,
    };
    use crate::credentials::CallAuth;
    use crate::transcribe::{TranscribeRequest, Transcriber, VecTranscriptSink};
    use crate::weather::Weather;

    fn request<'a>(samples: &'a [f32]) -> TranscribeRequest<'a> {
        TranscribeRequest {
            model: "whisper-1",
            language: Some("en"),
            offset_ms: 30_000,
            samples,
        }
    }

    #[test]
    fn the_wav_header_is_exactly_the_format_and_the_samples_follow_it() {
        let wav = wav_pcm16(&[0.0, 1.0, -1.0]);
        assert_eq!(wav.len(), WAV_HEADER_BYTES + 6);
        assert_eq!(&wav[0..4], b"RIFF");
        assert_eq!(&wav[8..12], b"WAVE");
        assert_eq!(&wav[36..40], b"data");
        // 16 kHz, mono, 16-bit — the seam's one sample format.
        assert_eq!(
            u32::from_le_bytes([wav[24], wav[25], wav[26], wav[27]]),
            16_000
        );
        assert_eq!(u16::from_le_bytes([wav[22], wav[23]]), 1);
        assert_eq!(u16::from_le_bytes([wav[34], wav[35]]), 16);
    }

    #[test]
    fn loud_samples_saturate_rather_than_wrapping_into_the_opposite_sign() {
        assert_eq!(to_pcm16(0.0), 0);
        assert_eq!(to_pcm16(1.0), i16::MAX);
        assert_eq!(to_pcm16(-1.0), -i16::MAX);
        assert_eq!(to_pcm16(9.5), i16::MAX, "an overshooting decoder clips");
        assert_eq!(to_pcm16(-9.5), -i16::MAX);
    }

    #[test]
    fn the_multipart_body_carries_the_model_the_format_and_the_audio() {
        let samples = vec![0.25_f32; 16_000];
        let body = multipart_body(&request(&samples), true);
        let text = String::from_utf8_lossy(&body[..600]);
        assert!(text.contains(MULTIPART_BOUNDARY));
        assert!(text.contains("name=\"model\"\r\n\r\nwhisper-1"));
        assert!(text.contains("name=\"response_format\"\r\n\r\nverbose_json"));
        assert!(text.contains("name=\"language\"\r\n\r\nen"));
        assert!(text.contains("filename=\"window.wav\""));
        assert!(body.ends_with(format!("--{MULTIPART_BOUNDARY}--\r\n").as_bytes()));

        let body = multipart_body(&request(&samples), false);
        let text = String::from_utf8_lossy(&body[..600]);
        assert!(
            text.contains("name=\"response_format\"\r\n\r\njson"),
            "an endpoint that cannot segment must not be asked to"
        );
    }

    #[test]
    fn verbose_segments_are_offset_into_the_source_media() {
        let samples = vec![0.0_f32; 16_000];
        let body = br#"{"text":"a b","segments":[
            {"start":0.0,"end":1.2,"text":" a "},
            {"start":1.5,"end":2.0,"text":"b"},
            {"start":2.0,"end":2.1,"text":"   "}
        ]}"#;
        let segments = parse_segments(body, &request(&samples)).expect("verbose json parses");
        assert_eq!(segments.len(), 2, "an empty segment is not evidence");
        assert_eq!(segments[0].start_ms, 30_000);
        assert_eq!(segments[0].end_ms, 31_200);
        assert_eq!(segments[0].text, "a");
        assert_eq!(segments[1].start_ms, 31_500);
    }

    #[test]
    fn a_json_only_endpoint_produces_one_window_wide_segment_rather_than_a_guess() {
        let samples = vec![0.0_f32; 16_000 * 5];
        let segments =
            parse_segments(br#"{"text":"hello there"}"#, &request(&samples)).expect("json parses");
        assert_eq!(segments.len(), 1);
        assert_eq!(segments[0].start_ms, 30_000);
        assert_eq!(
            segments[0].end_ms, 35_000,
            "the citation lands on the window, which is what we actually know"
        );
    }

    #[test]
    fn a_response_that_is_neither_shape_is_malformed_rather_than_empty() {
        let samples = vec![0.0_f32; 16_000];
        let error = parse_segments(br#"{"error":"nope"}"#, &request(&samples))
            .expect_err("an unrecognized shape must not read as a silent empty transcript");
        assert!(matches!(error, Weather::MalformedOutput { .. }));
    }

    #[test]
    fn without_a_transport_the_cloud_adapter_refuses_instead_of_pretending() {
        let adapter = CloudSttAdapter {
            base_url: "https://api.openai.com".to_owned(),
            supports_segments: true,
        };
        let samples = vec![0.0_f32; 16_000];
        let mut sink = VecTranscriptSink::default();
        let refused = adapter
            .transcribe(&CallAuth::None, &request(&samples), None, &mut sink)
            .expect_err("a cloud adapter with no transport cannot transcribe");
        assert!(matches!(refused, Weather::TransportUnavailable { .. }));
        assert!(sink.segments.is_empty());
    }

    #[test]
    fn the_endpoint_url_is_the_base_plus_the_openai_shaped_path() {
        let adapter = CloudSttAdapter {
            base_url: "https://api.openai.com/".to_owned(),
            supports_segments: true,
        };
        assert_eq!(
            adapter.url(),
            "https://api.openai.com/v1/audio/transcriptions"
        );
    }
}
