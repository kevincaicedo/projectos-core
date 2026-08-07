//! The `Provider` trait (L9, F20): the one shape every model family
//! implements. Adapters are codecs over the transport seam — `complete`
//! streams through a caller-owned sink, tool use passes through as opaque
//! JSON (the harness interprets it; the gateway never does), and the
//! reserved capability slots answer with typed `NotYetSupported` naming the
//! story that implements them, exactly like the registered-but-later
//! `pos-api` entries.

use crate::credentials::CallAuth;
use crate::transport::HttpTransport;
use crate::weather::Weather;

/// Ceiling on `max_output_tokens` a single dispatch may request. Nothing in
/// M0 legitimately asks for more than a frontier model's largest published
/// output window; a bigger ask is a bug or a runaway, refused as typed
/// budget weather before any socket opens (L8).
pub const OUTPUT_TOKENS_REQUEST_MAX: u32 = 128_000;

/// The five adapter families m0-s10 ships. Cloud breadth is a conformance
/// claim: each family must pass its conformance rows, so this enum is also
/// the coverage checklist the suite iterates.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum ProviderFamily {
    Anthropic,
    OpenAi,
    Google,
    OpenRouter,
    /// Ollama, LM Studio, vLLM, and custom endpoints speaking the OpenAI
    /// wire shape; capability differences are a qualified profile, not a
    /// guess (see [`crate::adapter::EndpointProfile`]).
    OpenAiCompatible,
}

impl ProviderFamily {
    pub const COUNT: usize = 5;
    pub const ALL: [Self; Self::COUNT] = [
        Self::Anthropic,
        Self::OpenAi,
        Self::Google,
        Self::OpenRouter,
        Self::OpenAiCompatible,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Anthropic => "anthropic",
            Self::OpenAi => "openai",
            Self::Google => "google",
            Self::OpenRouter => "openrouter",
            Self::OpenAiCompatible => "openai-compatible",
        }
    }
}

/// Chat roles the v0 completion surface carries. The fixed vocabulary keeps
/// adapters from inventing role synonyms per provider.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MessageRole {
    User,
    Assistant,
}

impl MessageRole {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::User => "user",
            Self::Assistant => "assistant",
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ChatMessage {
    pub role: MessageRole,
    pub content: String,
}

/// One completion call, provider-neutral. `tools_json` is the pass-through
/// tool-use shape: a JSON array in the provider's native tool schema,
/// forwarded verbatim — the gateway routes bytes, the harness owns meaning.
#[derive(Clone, Debug)]
pub struct CompletionRequest {
    pub model: String,
    pub system: Option<String>,
    pub messages: Vec<ChatMessage>,
    pub tools_json: Option<String>,
    pub max_output_tokens: u32,
    /// Per-call transport deadline. A named default lives with the gateway
    /// config; the field is explicit so tests and the harness can tighten it.
    pub timeout_ms: u32,
}

/// Streamed completion items, pushed into a caller-owned sink in arrival
/// order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CompletionEvent {
    /// A fragment of assistant text.
    TextDelta(String),
    /// A provider tool-use block, passed through as the provider's own JSON.
    ToolCallPassThrough { json: String },
}

/// Returned by the sink to stop the stream early (user cancel). The adapter
/// aborts the transport read and reports whatever usage it saw.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct SinkClosed;

/// Where streamed events land. `&mut dyn` rather than a channel: M0 shells
/// are synchronous, and a trait object keeps the harness free to buffer,
/// forward over SSE, or drop into a test vector without an executor.
pub trait CompletionSink {
    /// # Errors
    ///
    /// [`SinkClosed`] tells the adapter to stop reading; it is a cancel
    /// signal, not a failure.
    fn on_event(&mut self, event: CompletionEvent) -> Result<(), SinkClosed>;
}

/// Token accounting for one finished call, and whether the provider measured
/// it or the adapter had to estimate (the `usage-or-explicit-estimate`
/// conformance row: an estimate must say it is one).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct CompletionUsage {
    pub tokens_in: u64,
    pub tokens_out: u64,
    /// `true` when the numbers came from the provider's own usage report;
    /// `false` when the adapter estimated (and the ledger row says so).
    pub measured: bool,
}

/// Embedding request shape, reserved: the engine lands with m1-s04.
#[derive(Clone, Debug)]
pub struct EmbedRequest {
    pub model: String,
    pub inputs: Vec<String>,
}

/// The provider contract. `complete` is the only slot with an M0 engine;
/// the reserved slots return typed weather naming their owning story, so a
/// caller wired today keeps compiling when the engines land.
pub trait Provider {
    fn family(&self) -> ProviderFamily;

    /// Streams one completion through `sink` and returns final usage. `auth`
    /// arrives already resolved by the gateway (policy first, credentials
    /// second, transport last); the adapter's only job with it is placing
    /// the value into this provider's auth header.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`] for every failure class — timeout, rate-limit,
    /// refusal, malformed output, transport — never a panic (STYLE).
    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather>;

    /// Reserved: batched embeddings (local ONNX or API) land with m1-s04.
    ///
    /// # Errors
    ///
    /// Always [`Weather::NotYetSupported`] until the owning story lands.
    fn embed(
        &self,
        _request: &EmbedRequest,
        _transport: &dyn HttpTransport,
    ) -> Result<(), Weather> {
        Err(Weather::NotYetSupported {
            capability: "embed",
            arrives_with: "the m1-s04 embedding engine",
        })
    }

    /// Reserved: local whisper.cpp + cloud STT land with m1-s03.
    ///
    /// # Errors
    ///
    /// Always [`Weather::NotYetSupported`] until the owning story lands.
    fn transcribe(&self, _transport: &dyn HttpTransport) -> Result<(), Weather> {
        Err(Weather::NotYetSupported {
            capability: "transcribe",
            arrives_with: "the m1-s03 transcription engine",
        })
    }

    /// Reserved: realtime voice sessions land with the M2 voice plane (§13).
    ///
    /// # Errors
    ///
    /// Always [`Weather::NotYetSupported`] until the owning story lands.
    fn voice(&self, _transport: &dyn HttpTransport) -> Result<(), Weather> {
        Err(Weather::NotYetSupported {
            capability: "voice",
            arrives_with: "the M2 voice plane",
        })
    }
}

/// A sink that appends into a `Vec` — the shape every unit/conformance test
/// uses, public because the CLI and eval runner want the same buffer.
#[derive(Debug, Default)]
pub struct VecSink {
    pub events: Vec<CompletionEvent>,
}

impl CompletionSink for VecSink {
    fn on_event(&mut self, event: CompletionEvent) -> Result<(), SinkClosed> {
        self.events.push(event);
        Ok(())
    }
}

impl VecSink {
    /// The concatenated assistant text, for asserts and the eval scorer.
    #[must_use]
    pub fn text(&self) -> String {
        let mut text = String::new();
        for event in &self.events {
            if let CompletionEvent::TextDelta(delta) = event {
                text.push_str(delta);
            }
        }
        text
    }
}

#[cfg(test)]
mod tests {
    use super::{CompletionEvent, CompletionSink, ProviderFamily, VecSink};

    #[test]
    fn the_family_checklist_is_complete_and_stable() {
        assert_eq!(ProviderFamily::ALL.len(), ProviderFamily::COUNT);
        let names: Vec<&str> = ProviderFamily::ALL
            .iter()
            .map(|family| family.as_str())
            .collect();
        assert_eq!(
            names,
            [
                "anthropic",
                "openai",
                "google",
                "openrouter",
                "openai-compatible"
            ]
        );
    }

    #[test]
    fn the_vec_sink_concatenates_text_and_keeps_tool_blocks() {
        let mut sink = VecSink::default();
        sink.on_event(CompletionEvent::TextDelta("hel".to_owned()))
            .expect("vec sink never closes");
        sink.on_event(CompletionEvent::ToolCallPassThrough {
            json: "{\"name\":\"read\"}".to_owned(),
        })
        .expect("vec sink never closes");
        sink.on_event(CompletionEvent::TextDelta("lo".to_owned()))
            .expect("vec sink never closes");
        assert_eq!(sink.text(), "hello");
        assert_eq!(sink.events.len(), 3);
    }
}
