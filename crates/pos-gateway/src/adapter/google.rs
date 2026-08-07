//! Google Gemini API codec (`POST /v1beta/models/{model}:streamGenerateContent
//! ?alt=sse`). Roles map `assistant → model`, auth is the `x-goog-api-key`
//! header, and function calls pass through as the provider's own part JSON.

use crate::adapter::{
    BoundedErrorBody, estimate_tokens, truncate_reason, weather_from_status, weather_from_transport,
};
use crate::credentials::CallAuth;
use crate::provider::{
    CompletionEvent, CompletionRequest, CompletionSink, CompletionUsage, MessageRole, Provider,
    ProviderFamily,
};
use crate::sse::{SseDecoder, SseEvent};
use crate::transport::{
    HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, ResponseHandler, StreamAbort,
    TransportError,
};
use crate::weather::Weather;
use serde_json::{Value, json};

pub struct GoogleAdapter {
    pub base_url: String,
}

fn generate_content_plan(
    base_url: &str,
    auth: &CallAuth,
    request: &CompletionRequest,
) -> Result<HttpRequestPlan, Weather> {
    if request.messages.is_empty() {
        return Err(Weather::InvalidRequest {
            reason: "a completion needs at least one message".to_owned(),
        });
    }
    let contents: Vec<Value> = request
        .messages
        .iter()
        .map(|message| {
            let role = match message.role {
                MessageRole::User => "user",
                // Gemini's wire name for the assistant role.
                MessageRole::Assistant => "model",
            };
            json!({"role": role, "parts": [{"text": message.content}]})
        })
        .collect();
    let mut body = json!({
        "contents": contents,
        "generationConfig": {"maxOutputTokens": request.max_output_tokens},
    });
    if let Some(system) = &request.system {
        body["systemInstruction"] = json!({"parts": [{"text": system}]});
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
        headers.push(("x-goog-api-key", key.expose().to_owned()));
    }
    Ok(HttpRequestPlan {
        method: HttpMethod::Post,
        url: format!(
            "{base_url}/v1beta/models/{}:streamGenerateContent?alt=sse",
            request.model
        ),
        headers,
        body: body.to_string().into_bytes(),
        timeout_ms: request.timeout_ms,
    })
}

struct GeminiStream<'sink> {
    sink: &'sink mut dyn CompletionSink,
    decoder: SseDecoder,
    events: Vec<SseEvent>,
    error_body: BoundedErrorBody,
    head: Option<HttpHead>,
    tokens_in: Option<u64>,
    tokens_out: Option<u64>,
    output_chars: u64,
    input_chars: u64,
    refusal: Option<String>,
    parse_failure: Option<String>,
    sink_closed: bool,
}

impl GeminiStream<'_> {
    fn is_success(&self) -> bool {
        self.head.as_ref().is_some_and(|head| head.status < 300)
    }

    fn consume_data(&mut self, data: &str) {
        let Ok(value) = serde_json::from_str::<Value>(data) else {
            self.parse_failure = Some(truncate_reason(data));
            return;
        };
        if let Some(usage) = value.get("usageMetadata") {
            if let Some(tokens_in) = usage.get("promptTokenCount").and_then(Value::as_u64) {
                self.tokens_in = Some(tokens_in);
            }
            if let Some(tokens_out) = usage.get("candidatesTokenCount").and_then(Value::as_u64) {
                self.tokens_out = Some(tokens_out);
            }
        }
        if let Some(block_reason) = value
            .pointer("/promptFeedback/blockReason")
            .and_then(Value::as_str)
        {
            self.refusal = Some(format!("prompt blocked: {}", truncate_reason(block_reason)));
            return;
        }
        let Some(candidate) = value
            .get("candidates")
            .and_then(Value::as_array)
            .and_then(|candidates| candidates.first())
        else {
            return;
        };
        if let Some("SAFETY" | "PROHIBITED_CONTENT" | "BLOCKLIST") =
            candidate.get("finishReason").and_then(Value::as_str)
        {
            self.refusal = Some("provider safety filter ended the response".to_owned());
        }
        let Some(parts) = candidate
            .pointer("/content/parts")
            .and_then(Value::as_array)
        else {
            return;
        };
        for part in parts {
            self.consume_part(part);
            if self.sink_closed {
                return;
            }
        }
    }

    fn consume_part(&mut self, part: &Value) {
        if let Some(text) = part.get("text").and_then(Value::as_str)
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
        if let Some(function_call) = part.get("functionCall").filter(|call| !call.is_null())
            && self
                .sink
                .on_event(CompletionEvent::ToolCallPassThrough {
                    json: function_call.to_string(),
                })
                .is_err()
        {
            self.sink_closed = true;
        }
    }
}

impl ResponseHandler for GeminiStream<'_> {
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

impl Provider for GoogleAdapter {
    fn family(&self) -> ProviderFamily {
        ProviderFamily::Google
    }

    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        let plan = generate_content_plan(&self.base_url, auth, request)?;
        let input_chars: u64 = request
            .messages
            .iter()
            .map(|message| message.content.chars().count() as u64)
            .sum::<u64>()
            + request
                .system
                .as_ref()
                .map_or(0, |system| system.chars().count() as u64);
        let mut stream = GeminiStream {
            sink,
            decoder: SseDecoder::default(),
            events: Vec::new(),
            error_body: BoundedErrorBody::new(),
            head: None,
            tokens_in: None,
            tokens_out: None,
            output_chars: 0,
            input_chars,
            refusal: None,
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
        if let Some(reason) = stream.refusal {
            return Err(Weather::Refusal { reason });
        }
        if let Some(reason) = stream.parse_failure {
            return Err(Weather::MalformedOutput {
                reason: format!("stream payload did not parse: {reason}"),
            });
        }
        Ok(match (stream.tokens_in, stream.tokens_out) {
            (Some(tokens_in), Some(tokens_out)) => CompletionUsage {
                tokens_in,
                tokens_out,
                measured: true,
            },
            _ => CompletionUsage {
                tokens_in: estimate_tokens(stream.input_chars),
                tokens_out: estimate_tokens(stream.output_chars),
                measured: false,
            },
        })
    }
}
