//! The OpenAI chat-completions wire shape, shared by three families:
//! `openai`, `openrouter`, and `openai-compatible` (Ollama, LM Studio, vLLM,
//! custom endpoints). One codec, three configurations — the differences are
//! a capability *profile*, never a fork of the parse loop, which is what
//! keeps "cloud breadth is a conformance claim" true.

use crate::adapter::{
    BoundedErrorBody, estimate_tokens, truncate_reason, weather_from_status, weather_from_transport,
};
use crate::credentials::CallAuth;
use crate::provider::{
    CompletionEvent, CompletionRequest, CompletionSink, CompletionUsage, Provider, ProviderFamily,
};
use crate::sse::{SseDecoder, SseEvent};
use crate::transport::{
    BufferedResponse, HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, ResponseHandler,
    StreamAbort, TransportError,
};
use crate::weather::Weather;
use serde_json::{Value, json};

/// A `GET /v1/models` answer is a bounded catalog, not a stream; 1 MiB
/// covers thousands of model rows and stops a misbehaving server (L8).
const MODELS_BODY_BYTES_MAX: usize = 1024 * 1024;

/// Which server family an OpenAI-compatible endpoint claims/proved to be.
/// `Unknown` is honest for a custom endpoint that qualified generically.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointServer {
    Ollama,
    LmStudio,
    Vllm,
    Unknown,
}

impl EndpointServer {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Ollama => "ollama",
            Self::LmStudio => "lm-studio",
            Self::Vllm => "vllm",
            Self::Unknown => "unknown",
        }
    }
}

/// What a compatible endpoint actually supports — a qualified claim, not a
/// hope. The conformance suite exercises both values of each flag.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EndpointProfile {
    pub server: EndpointServer,
    /// Whether `stream_options: {"include_usage": true}` is accepted. When
    /// false the adapter omits the field and labels usage as an estimate.
    pub supports_stream_usage: bool,
}

impl EndpointProfile {
    /// The conservative starting profile for an unqualified endpoint: ask
    /// for nothing optional, estimate usage explicitly.
    #[must_use]
    pub const fn conservative() -> Self {
        Self {
            server: EndpointServer::Unknown,
            supports_stream_usage: false,
        }
    }
}

/// Wire-level knobs that distinguish the three OpenAI-shaped families.
struct WireConfig {
    base_url: String,
    /// `max_completion_tokens` (modern OpenAI) vs `max_tokens` (OpenRouter
    /// and every local server today).
    output_cap_field: &'static str,
    include_stream_usage: bool,
}

fn chat_completions_plan(
    config: &WireConfig,
    auth: &CallAuth,
    request: &CompletionRequest,
) -> Result<HttpRequestPlan, Weather> {
    if request.messages.is_empty() {
        return Err(Weather::InvalidRequest {
            reason: "a completion needs at least one message".to_owned(),
        });
    }
    let mut messages = Vec::with_capacity(request.messages.len() + 1);
    if let Some(system) = &request.system {
        messages.push(json!({"role": "system", "content": system}));
    }
    for message in &request.messages {
        messages.push(json!({"role": message.role.as_str(), "content": message.content}));
    }
    let mut body = json!({
        "model": request.model,
        "messages": messages,
        "stream": true,
        config.output_cap_field: request.max_output_tokens,
    });
    if config.include_stream_usage {
        body["stream_options"] = json!({"include_usage": true});
    }
    if let Some(tools_json) = &request.tools_json {
        let tools: Value =
            serde_json::from_str(tools_json).map_err(|error| Weather::InvalidRequest {
                reason: format!("tools_json is not valid JSON: {error}"),
            })?;
        body["tools"] = tools;
    }
    let mut headers: Vec<(&'static str, String)> = vec![
        ("content-type", "application/json".to_owned()),
        ("accept", "text/event-stream".to_owned()),
    ];
    if let CallAuth::ApiKey(key) = auth {
        headers.push(("authorization", format!("Bearer {}", key.expose())));
    }
    Ok(HttpRequestPlan {
        method: HttpMethod::Post,
        url: format!("{}/v1/chat/completions", config.base_url),
        headers,
        body: body.to_string().into_bytes(),
        timeout_ms: request.timeout_ms,
    })
}

/// Streaming parse state for one chat-completions response.
struct WireStream<'sink> {
    sink: &'sink mut dyn CompletionSink,
    decoder: SseDecoder,
    events: Vec<SseEvent>,
    error_body: BoundedErrorBody,
    head: Option<HttpHead>,
    usage: Option<(u64, u64)>,
    output_chars: u64,
    input_chars: u64,
    finish_refusal: Option<String>,
    parse_failure: Option<String>,
    /// Set when the caller's sink closed: the adapter stops reading and the
    /// partial result is returned as a cancel, never as an error.
    sink_closed: bool,
}

impl WireStream<'_> {
    fn is_success(&self) -> bool {
        self.head.as_ref().is_some_and(|head| head.status < 300)
    }

    fn consume_data(&mut self, data: &str) {
        if data.trim() == "[DONE]" {
            return;
        }
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            self.parse_failure = Some(truncate_reason(data));
            return;
        };
        if let Some(usage) = value.get("usage").filter(|usage| !usage.is_null()) {
            let tokens_in = usage.get("prompt_tokens").and_then(Value::as_u64);
            let tokens_out = usage.get("completion_tokens").and_then(Value::as_u64);
            if let (Some(tokens_in), Some(tokens_out)) = (tokens_in, tokens_out) {
                self.usage = Some((tokens_in, tokens_out));
            }
        }
        let Some(choice) = value
            .get("choices")
            .and_then(Value::as_array)
            .and_then(|choices| choices.first())
        else {
            return;
        };
        if let Some("content_filter") = choice.get("finish_reason").and_then(Value::as_str) {
            self.finish_refusal = Some("provider content filter ended the response".to_owned());
        }
        let Some(delta) = choice.get("delta") else {
            return;
        };
        if let Some(text) = delta.get("content").and_then(Value::as_str)
            && !text.is_empty()
        {
            self.output_chars += text.chars().count() as u64;
            if self
                .sink
                .on_event(CompletionEvent::TextDelta(text.to_owned()))
                .is_err()
            {
                self.sink_closed = true;
            }
        }
        if let Some(tool_calls) = delta.get("tool_calls").filter(|calls| !calls.is_null())
            && self
                .sink
                .on_event(CompletionEvent::ToolCallPassThrough {
                    json: tool_calls.to_string(),
                })
                .is_err()
        {
            self.sink_closed = true;
        }
    }
}

impl ResponseHandler for WireStream<'_> {
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort> {
        self.head = Some(head.clone());
        Ok(())
    }

    fn on_chunk(&mut self, chunk: &[u8]) -> Result<(), StreamAbort> {
        if !self.is_success() {
            self.error_body.push(chunk);
            return Ok(());
        }
        let mut completed = std::mem::take(&mut self.events);
        if self.decoder.feed(chunk, &mut completed).is_err() {
            self.parse_failure = Some("SSE event exceeded the size cap".to_owned());
            return Err(StreamAbort);
        }
        let mut aborted = false;
        for event in completed.drain(..) {
            if aborted {
                continue; // The drain must finish so the buffer stays reusable.
            }
            self.consume_data(&event.data);
            aborted = self.sink_closed;
        }
        self.events = completed;
        if aborted {
            return Err(StreamAbort);
        }
        Ok(())
    }
}

fn complete_over_wire(
    config: &WireConfig,
    auth: &CallAuth,
    request: &CompletionRequest,
    transport: &dyn HttpTransport,
    sink: &mut dyn CompletionSink,
) -> Result<CompletionUsage, Weather> {
    let plan = chat_completions_plan(config, auth, request)?;
    let input_chars: u64 = request
        .messages
        .iter()
        .map(|message| message.content.chars().count() as u64)
        .sum::<u64>()
        + request
            .system
            .as_ref()
            .map_or(0, |system| system.chars().count() as u64);
    let mut stream = WireStream {
        sink,
        decoder: SseDecoder::default(),
        events: Vec::new(),
        error_body: BoundedErrorBody::new(),
        head: None,
        usage: None,
        output_chars: 0,
        input_chars,
        finish_refusal: None,
        parse_failure: None,
        sink_closed: false,
    };
    match transport.execute(&plan, &mut stream) {
        Ok(()) | Err(TransportError::Aborted) => {}
        Err(error) => return Err(weather_from_transport(error, request.timeout_ms)),
    }
    let head = stream.head.as_ref().ok_or_else(|| Weather::Transport {
        reason: "transport returned without a response head".to_owned(),
    })?;
    if head.status >= 300 {
        return Err(weather_from_status(head, stream.error_body.bytes()));
    }
    if let Some(reason) = stream.finish_refusal {
        return Err(Weather::Refusal { reason });
    }
    if let Some(reason) = stream.parse_failure {
        return Err(Weather::MalformedOutput {
            reason: format!("stream payload did not parse: {reason}"),
        });
    }
    Ok(match stream.usage {
        Some((tokens_in, tokens_out)) => CompletionUsage {
            tokens_in,
            tokens_out,
            measured: true,
        },
        None => CompletionUsage {
            tokens_in: estimate_tokens(stream.input_chars),
            tokens_out: estimate_tokens(stream.output_chars),
            measured: false,
        },
    })
}

/// OpenAI (`api.openai.com` or a proxy of it).
pub struct OpenAiAdapter {
    pub base_url: String,
}

impl Provider for OpenAiAdapter {
    fn family(&self) -> ProviderFamily {
        ProviderFamily::OpenAi
    }

    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        complete_over_wire(
            &WireConfig {
                base_url: self.base_url.clone(),
                output_cap_field: "max_completion_tokens",
                include_stream_usage: true,
            },
            auth,
            request,
            transport,
            sink,
        )
    }
}

/// OpenRouter: the OpenAI wire at `<base>/v1` (canonical base ends in
/// `/api`), always with usage reporting.
pub struct OpenRouterAdapter {
    pub base_url: String,
}

impl Provider for OpenRouterAdapter {
    fn family(&self) -> ProviderFamily {
        ProviderFamily::OpenRouter
    }

    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        complete_over_wire(
            &WireConfig {
                base_url: self.base_url.clone(),
                output_cap_field: "max_tokens",
                include_stream_usage: true,
            },
            auth,
            request,
            transport,
            sink,
        )
    }
}

/// Ollama / LM Studio / vLLM / custom endpoints, driven by their qualified
/// [`EndpointProfile`].
pub struct OpenAiCompatibleAdapter {
    pub base_url: String,
    pub profile: EndpointProfile,
}

impl Provider for OpenAiCompatibleAdapter {
    fn family(&self) -> ProviderFamily {
        ProviderFamily::OpenAiCompatible
    }

    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        complete_over_wire(
            &WireConfig {
                base_url: self.base_url.clone(),
                output_cap_field: "max_tokens",
                include_stream_usage: self.profile.supports_stream_usage,
            },
            auth,
            request,
            transport,
            sink,
        )
    }
}

/// One qualification run's honest result: what the endpoint proved, not what
/// its docs promise. The live lane records this verbatim.
#[derive(Clone, Debug)]
pub struct QualificationReport {
    pub base_url: String,
    pub profile: EndpointProfile,
    pub models: Vec<String>,
    pub completion_text: String,
    pub usage: CompletionUsage,
}

/// Model discovery (`GET /v1/models`) — also the health probe: an endpoint
/// that cannot list models is not healthy.
///
/// # Errors
///
/// Typed weather for transport, auth, and shape failures.
pub fn list_models(
    base_url: &str,
    auth: &CallAuth,
    transport: &dyn HttpTransport,
    timeout_ms: u32,
) -> Result<Vec<String>, Weather> {
    let mut headers: Vec<(&'static str, String)> = vec![("accept", "application/json".to_owned())];
    if let CallAuth::ApiKey(key) = auth {
        headers.push(("authorization", format!("Bearer {}", key.expose())));
    }
    let plan = HttpRequestPlan {
        method: HttpMethod::Get,
        url: format!("{base_url}/v1/models"),
        headers,
        body: Vec::new(),
        timeout_ms,
    };
    let mut buffered = BufferedResponse::default();
    transport
        .execute(&plan, &mut buffered)
        .map_err(|error| weather_from_transport(error, timeout_ms))?;
    let head = buffered.head.as_ref().ok_or_else(|| Weather::Transport {
        reason: "transport returned without a response head".to_owned(),
    })?;
    if head.status >= 300 {
        return Err(weather_from_status(head, &buffered.body));
    }
    if buffered.body.len() > MODELS_BODY_BYTES_MAX {
        return Err(Weather::MalformedOutput {
            reason: format!("model catalog exceeds {MODELS_BODY_BYTES_MAX} bytes"),
        });
    }
    let value: Value =
        serde_json::from_slice(&buffered.body).map_err(|error| Weather::MalformedOutput {
            reason: format!("model catalog did not parse: {error}"),
        })?;
    let rows =
        value
            .get("data")
            .and_then(Value::as_array)
            .ok_or_else(|| Weather::MalformedOutput {
                reason: "model catalog has no data array".to_owned(),
            })?;
    Ok(rows
        .iter()
        .filter_map(|row| row.get("id").and_then(Value::as_str))
        .map(str::to_owned)
        .collect())
}

/// Qualifies one live OpenAI-compatible endpoint: model discovery, a
/// streamed completion, and usage-or-explicit-estimate. `server` names the
/// family being qualified so the report is a claim about a product, not a
/// URL.
///
/// # Errors
///
/// The first typed weather the endpoint produced; a failed qualification is
/// a recorded fact, not a retry loop.
pub fn qualify_openai_compatible(
    server: EndpointServer,
    base_url: &str,
    auth: &CallAuth,
    model: &str,
    transport: &dyn HttpTransport,
    timeout_ms: u32,
) -> Result<QualificationReport, Weather> {
    use crate::provider::{ChatMessage, MessageRole, VecSink};

    let models = list_models(base_url, auth, transport, timeout_ms)?;
    if !models.iter().any(|candidate| candidate == model) {
        return Err(Weather::InvalidRequest {
            reason: format!("endpoint does not serve the qualification model {model:?}"),
        });
    }
    let request = CompletionRequest {
        model: model.to_owned(),
        system: None,
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: "Reply with exactly: QUALIFY-OK".to_owned(),
        }],
        tools_json: None,
        max_output_tokens: 64,
        timeout_ms,
    };
    // First pass asks for stream usage; on a typed unsupported-field answer
    // the profile records the honest downgrade and retries without it.
    let mut profile = EndpointProfile {
        server,
        supports_stream_usage: true,
    };
    let mut sink = VecSink::default();
    let adapter = OpenAiCompatibleAdapter {
        base_url: base_url.to_owned(),
        profile,
    };
    let usage = match adapter.complete(auth, &request, transport, &mut sink) {
        Ok(usage) => usage,
        Err(Weather::UnsupportedField { .. }) => {
            profile.supports_stream_usage = false;
            let retry = OpenAiCompatibleAdapter {
                base_url: base_url.to_owned(),
                profile,
            };
            sink = VecSink::default();
            retry.complete(auth, &request, transport, &mut sink)?
        }
        Err(weather) => return Err(weather),
    };
    // A server that accepted stream_options but reported nothing measurable
    // gets the honest profile, not the requested one.
    if !usage.measured {
        profile.supports_stream_usage = false;
    }
    Ok(QualificationReport {
        base_url: base_url.to_owned(),
        profile,
        models,
        completion_text: sink.text(),
        usage,
    })
}
