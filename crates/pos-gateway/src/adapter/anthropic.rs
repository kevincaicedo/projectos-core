//! Anthropic Messages API codec (`POST /v1/messages`, SSE streaming).
//! Auth is the `x-api-key` header plus a pinned `anthropic-version`; tool
//! use passes through as the provider's own `tool_use` block JSON.

use crate::adapter::{
    BoundedErrorBody, estimate_tokens, truncate_reason, weather_from_status, weather_from_transport,
};
use crate::credentials::CallAuth;
use crate::provider::{
    CompletionEvent, CompletionRequest, CompletionSink, CompletionUsage, Provider, ProviderFamily,
};
use crate::sse::{SseDecoder, SseEvent};
use crate::transport::{
    HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, ResponseHandler, StreamAbort,
    TransportError,
};
use crate::weather::Weather;
use serde_json::{Value, json};

/// The Messages API version this codec conforms to. Bumping it is a
/// conformance event (rerun the suite), not a config knob.
const ANTHROPIC_VERSION: &str = "2023-06-01";

pub struct AnthropicAdapter {
    pub base_url: String,
}

fn messages_plan(
    base_url: &str,
    auth: &CallAuth,
    request: &CompletionRequest,
) -> Result<HttpRequestPlan, Weather> {
    if request.messages.is_empty() {
        return Err(Weather::InvalidRequest {
            reason: "a completion needs at least one message".to_owned(),
        });
    }
    let messages: Vec<Value> = request
        .messages
        .iter()
        .map(|message| json!({"role": message.role.as_str(), "content": message.content}))
        .collect();
    let mut body = json!({
        "model": request.model,
        "max_tokens": request.max_output_tokens,
        "stream": true,
        "messages": messages,
    });
    if let Some(system) = &request.system {
        body["system"] = json!(system);
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
        ("anthropic-version", ANTHROPIC_VERSION.to_owned()),
    ];
    if let CallAuth::ApiKey(key) = auth {
        headers.push(("x-api-key", key.expose().to_owned()));
    }
    Ok(HttpRequestPlan {
        method: HttpMethod::Post,
        url: format!("{base_url}/v1/messages"),
        headers,
        body: body.to_string().into_bytes(),
        timeout_ms: request.timeout_ms,
    })
}

/// Streaming parse state for one Messages response. Tool-use blocks arrive
/// as a `content_block_start` plus `input_json_delta` fragments; the codec
/// assembles them and emits one pass-through block per `content_block_stop`.
struct MessagesStream<'sink> {
    sink: &'sink mut dyn CompletionSink,
    decoder: SseDecoder,
    events: Vec<SseEvent>,
    error_body: BoundedErrorBody,
    head: Option<HttpHead>,
    tokens_in: Option<u64>,
    tokens_out: Option<u64>,
    output_chars: u64,
    input_chars: u64,
    tool_block: Option<ToolBlock>,
    refusal: Option<String>,
    provider_error: Option<String>,
    parse_failure: Option<String>,
    sink_closed: bool,
}

struct ToolBlock {
    id: String,
    name: String,
    input_json: String,
}

impl MessagesStream<'_> {
    fn is_success(&self) -> bool {
        self.head.as_ref().is_some_and(|head| head.status < 300)
    }

    fn consume(&mut self, event: &SseEvent) {
        let Ok(value) = serde_json::from_str::<Value>(&event.data) else {
            self.parse_failure = Some(truncate_reason(&event.data));
            return;
        };
        match event.event.as_deref().unwrap_or("") {
            "message_start" => {
                self.tokens_in = value
                    .pointer("/message/usage/input_tokens")
                    .and_then(Value::as_u64);
            }
            "content_block_start" => self.block_start(&value),
            "content_block_delta" => self.block_delta(&value),
            "content_block_stop" => self.block_stop(),
            "message_delta" => {
                if let Some(tokens_out) = value
                    .pointer("/usage/output_tokens")
                    .and_then(Value::as_u64)
                {
                    self.tokens_out = Some(tokens_out);
                }
                if let Some("refusal") = value.pointer("/delta/stop_reason").and_then(Value::as_str)
                {
                    self.refusal =
                        Some("model declined to answer (stop_reason: refusal)".to_owned());
                }
            }
            "error" => {
                let message = value
                    .pointer("/error/message")
                    .and_then(Value::as_str)
                    .unwrap_or("provider sent an error event");
                self.provider_error = Some(truncate_reason(message));
            }
            // `message_stop` and `ping` carry nothing this codec needs.
            _ => {}
        }
    }

    fn block_start(&mut self, value: &Value) {
        if value.pointer("/content_block/type").and_then(Value::as_str) == Some("tool_use") {
            self.tool_block = Some(ToolBlock {
                id: value
                    .pointer("/content_block/id")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                name: value
                    .pointer("/content_block/name")
                    .and_then(Value::as_str)
                    .unwrap_or_default()
                    .to_owned(),
                input_json: String::new(),
            });
        }
    }

    fn block_delta(&mut self, value: &Value) {
        match value.pointer("/delta/type").and_then(Value::as_str) {
            Some("text_delta") => {
                if let Some(text) = value.pointer("/delta/text").and_then(Value::as_str)
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
            }
            Some("input_json_delta") => {
                if let (Some(block), Some(fragment)) = (
                    self.tool_block.as_mut(),
                    value.pointer("/delta/partial_json").and_then(Value::as_str),
                ) {
                    block.input_json.push_str(fragment);
                }
            }
            _ => {}
        }
    }

    fn block_stop(&mut self) {
        let Some(block) = self.tool_block.take() else {
            return;
        };
        let pass_through = json!({
            "type": "tool_use",
            "id": block.id,
            "name": block.name,
            "input_json": block.input_json,
        });
        if self
            .sink
            .on_event(CompletionEvent::ToolCallPassThrough {
                json: pass_through.to_string(),
            })
            .is_err()
        {
            self.sink_closed = true;
        }
    }
}

impl ResponseHandler for MessagesStream<'_> {
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
            self.consume(&event);
            aborted = self.sink_closed;
        }
        self.events = completed;
        if aborted {
            return Err(StreamAbort);
        }
        Ok(())
    }
}

impl Provider for AnthropicAdapter {
    fn family(&self) -> ProviderFamily {
        ProviderFamily::Anthropic
    }

    fn complete(
        &self,
        auth: &CallAuth,
        request: &CompletionRequest,
        transport: &dyn HttpTransport,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        let plan = messages_plan(&self.base_url, auth, request)?;
        let input_chars: u64 = request
            .messages
            .iter()
            .map(|message| message.content.chars().count() as u64)
            .sum::<u64>()
            + request
                .system
                .as_ref()
                .map_or(0, |system| system.chars().count() as u64);
        let mut stream = MessagesStream {
            sink,
            decoder: SseDecoder::default(),
            events: Vec::new(),
            error_body: BoundedErrorBody::new(),
            head: None,
            tokens_in: None,
            tokens_out: None,
            output_chars: 0,
            input_chars,
            tool_block: None,
            refusal: None,
            provider_error: None,
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
        if let Some(reason) = stream.provider_error {
            return Err(Weather::Transport {
                reason: format!("provider error event: {reason}"),
            });
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
