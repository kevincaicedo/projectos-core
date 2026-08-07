//! The five adapter families (m0-s10): Anthropic, OpenAI, Google,
//! OpenRouter, and OpenAI-compatible (Ollama / LM Studio / vLLM / custom).
//! Every adapter is a pure codec over [`crate::transport::HttpTransport`]:
//! plan out, weather in. Shared machinery — error-status mapping, bounded
//! error bodies, usage estimation — lives here so the five families cannot
//! drift on failure semantics.

mod anthropic;
mod google;
mod openai_wire;

pub use anthropic::AnthropicAdapter;
pub use google::GoogleAdapter;
pub use openai_wire::{
    EndpointProfile, EndpointServer, OpenAiAdapter, OpenAiCompatibleAdapter, OpenRouterAdapter,
    QualificationReport, list_models, qualify_openai_compatible,
};

use crate::transport::{HttpHead, TransportError};
use crate::weather::Weather;

/// Provider error bodies are diagnostics, not payloads: 64 KiB is generous
/// for any real error document and stops a hostile endpoint from ballooning
/// memory through a failure path (L8).
pub(crate) const ERROR_BODY_BYTES_MAX: usize = 64 * 1024;

/// How much of a provider error message survives into a weather reason.
/// Enough to act on, small enough that a log line stays a log line.
pub(crate) const ERROR_REASON_CHARS_MAX: usize = 256;

/// Divisor for the explicit usage estimate when a provider reports none:
/// ~4 chars/token is the long-standing English-text heuristic. The estimate
/// is always labeled (`measured: false`) — the conformance row is
/// usage-or-*explicit*-estimate, never a silent guess.
pub(crate) const ESTIMATE_CHARS_PER_TOKEN: u64 = 4;

/// Request fields a compatible server most plausibly rejects; scanning the
/// error message for them turns an opaque 400 into a typed
/// [`Weather::UnsupportedField`] the capability profile can record.
pub(crate) const KNOWN_REJECTABLE_FIELDS: [&str; 4] =
    ["stream_options", "max_completion_tokens", "tools", "system"];

pub(crate) fn estimate_tokens(chars: u64) -> u64 {
    chars.div_ceil(ESTIMATE_CHARS_PER_TOKEN)
}

pub(crate) fn truncate_reason(text: &str) -> String {
    let mut reason: String = text.chars().take(ERROR_REASON_CHARS_MAX).collect();
    if text.chars().count() > ERROR_REASON_CHARS_MAX {
        reason.push('…');
    }
    reason
}

pub(crate) fn weather_from_transport(error: TransportError, timeout_ms: u32) -> Weather {
    match error {
        TransportError::Timeout { .. } => Weather::Timeout { timeout_ms },
        other => Weather::Transport {
            reason: other.to_string(),
        },
    }
}

fn retry_after_ms(head: &HttpHead) -> Option<u32> {
    head.header("retry-after")
        .and_then(|value| value.trim().parse::<u32>().ok())
        // The header is seconds; the weather field is milliseconds.
        .map(|seconds| seconds.saturating_mul(1_000))
}

/// Maps a non-2xx head + its bounded body into typed weather. Shared
/// verbatim by all five families so status semantics cannot drift.
pub(crate) fn weather_from_status(head: &HttpHead, error_body: &[u8]) -> Weather {
    let body_text = String::from_utf8_lossy(error_body);
    let message =
        provider_error_message(&body_text).unwrap_or_else(|| truncate_reason(body_text.trim()));
    match head.status {
        401 | 403 => Weather::AuthRejected {
            status: head.status,
        },
        429 => Weather::RateLimited {
            retry_after_ms: retry_after_ms(head),
        },
        400 | 404 | 422 => match rejected_field(&body_text) {
            Some(field) => Weather::UnsupportedField { field },
            None => Weather::MalformedOutput {
                reason: format!("HTTP {}: {}", head.status, truncate_reason(&message)),
            },
        },
        status if status >= 500 => Weather::Transport {
            reason: format!("HTTP {status}: {}", truncate_reason(&message)),
        },
        status => Weather::MalformedOutput {
            reason: format!("HTTP {status}: {}", truncate_reason(&message)),
        },
    }
}

/// Pulls `error.message` out of the OpenAI/Anthropic/Google error envelopes
/// without committing to one schema: any `"message"` string under an
/// `"error"` object counts.
fn provider_error_message(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    let error = value.get("error")?;
    error
        .get("message")
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
}

fn rejected_field(body: &str) -> Option<String> {
    // Prefer the structured `param` when the envelope carries one.
    if let Ok(value) = serde_json::from_str::<serde_json::Value>(body)
        && let Some(param) = value
            .get("error")
            .and_then(|error| error.get("param"))
            .and_then(serde_json::Value::as_str)
        && !param.is_empty()
    {
        return Some(param.to_owned());
    }
    KNOWN_REJECTABLE_FIELDS
        .iter()
        .find(|field| body.contains(**field))
        .map(|field| (*field).to_owned())
}

/// Collects a bounded error body from a transport stream.
pub(crate) struct BoundedErrorBody {
    bytes: Vec<u8>,
}

impl BoundedErrorBody {
    pub(crate) fn new() -> Self {
        Self { bytes: Vec::new() }
    }

    pub(crate) fn push(&mut self, chunk: &[u8]) {
        let remaining = ERROR_BODY_BYTES_MAX.saturating_sub(self.bytes.len());
        self.bytes
            .extend_from_slice(&chunk[..chunk.len().min(remaining)]);
    }

    pub(crate) fn bytes(&self) -> &[u8] {
        &self.bytes
    }
}

#[cfg(test)]
mod tests {
    use super::{estimate_tokens, rejected_field, weather_from_status};
    use crate::transport::HttpHead;
    use crate::weather::Weather;

    fn completed(status: u16, headers: &[(&str, &str)]) -> HttpHead {
        HttpHead {
            status,
            headers: headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                .collect(),
        }
    }

    #[test]
    fn statuses_map_to_the_shared_weather_classes() {
        assert!(matches!(
            weather_from_status(&completed(401, &[]), b"{}"),
            Weather::AuthRejected { status: 401 }
        ));
        assert_eq!(
            weather_from_status(&completed(429, &[("retry-after", "3")]), b"{}"),
            Weather::RateLimited {
                retry_after_ms: Some(3_000)
            }
        );
        assert!(matches!(
            weather_from_status(&completed(503, &[]), b"upstream sad"),
            Weather::Transport { .. }
        ));
        let unsupported = weather_from_status(
            &completed(400, &[]),
            br#"{"error":{"message":"Unrecognized request argument supplied","param":"stream_options"}}"#,
        );
        assert_eq!(
            unsupported,
            Weather::UnsupportedField {
                field: "stream_options".to_owned()
            }
        );
    }

    #[test]
    fn field_detection_prefers_param_then_falls_back_to_message_scan() {
        assert_eq!(
            rejected_field(r#"{"error":{"message":"x","param":"tools"}}"#),
            Some("tools".to_owned())
        );
        assert_eq!(
            rejected_field(r#"{"error":{"message":"unexpected keyword max_completion_tokens"}}"#),
            Some("max_completion_tokens".to_owned())
        );
        assert_eq!(rejected_field(r#"{"error":{"message":"nope"}}"#), None);
    }

    #[test]
    fn estimates_round_up_and_never_lose_a_short_string() {
        assert_eq!(estimate_tokens(0), 0);
        assert_eq!(estimate_tokens(1), 1);
        assert_eq!(estimate_tokens(9), 3);
    }
}
