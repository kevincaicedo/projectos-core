//! The m0-s10 adapter conformance suite: every one of the five gateway
//! families passes its applicable rows against recorded fixtures — request
//! shape (auth in headers, never URLs), streamed text, tool-use
//! pass-through, usage-or-explicit-estimate, and the shared weather mapping
//! (rate limit, auth, timeout, refusal, malformed output, unsupported
//! field). The coverage test at the bottom iterates
//! [`ProviderFamily::ALL`], so a sixth family cannot merge without rows.

#![forbid(unsafe_code)]

mod common;

use common::{FixtureOutcome, FixtureTransport};
use pos_gateway::{
    AnthropicAdapter, CallAuth, ChatMessage, CompletionRequest, EndpointProfile, EndpointServer,
    GoogleAdapter, MessageRole, OpenAiAdapter, OpenAiCompatibleAdapter, OpenRouterAdapter,
    Provider, ProviderFamily, ReasoningEffort, SecretValue, TransportError, VecSink, Weather,
    qualify_openai_compatible,
};
use std::collections::BTreeSet;
use std::sync::Mutex;

/// Families exercised by this suite; the coverage test compares this set
/// against [`ProviderFamily::ALL`].
static COVERED: Mutex<BTreeSet<&'static str>> = Mutex::new(BTreeSet::new());

fn cover(family: ProviderFamily) {
    COVERED.lock().expect("test mutex").insert(family.as_str());
}

fn request() -> CompletionRequest {
    CompletionRequest {
        model: "test-model".to_owned(),
        system: Some("You are terse.".to_owned()),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: "Say hello world.".to_owned(),
        }],
        tools_json: Some(r#"[{"name":"read_file"}]"#.to_owned()),
        reasoning_effort: None,
        max_output_tokens: 128,
        timeout_ms: 5_000,
    }
}

fn api_key() -> CallAuth {
    CallAuth::ApiKey(SecretValue::new("sk-test-conformance-key"))
}

fn adapter_for(family: ProviderFamily) -> Box<dyn Provider> {
    match family {
        ProviderFamily::Anthropic => Box::new(AnthropicAdapter {
            base_url: "https://api.anthropic.com".to_owned(),
        }),
        ProviderFamily::OpenAi => Box::new(OpenAiAdapter {
            base_url: "https://api.openai.com".to_owned(),
        }),
        ProviderFamily::Google => Box::new(GoogleAdapter {
            base_url: "https://generativelanguage.googleapis.com".to_owned(),
        }),
        ProviderFamily::OpenRouter => Box::new(OpenRouterAdapter {
            base_url: "https://openrouter.ai/api".to_owned(),
        }),
        ProviderFamily::OpenAiCompatible => Box::new(OpenAiCompatibleAdapter {
            base_url: "http://localhost:11434".to_owned(),
            profile: EndpointProfile {
                server: EndpointServer::Ollama,
                supports_stream_usage: false,
            },
        }),
    }
}

// ---------------------------------------------------------------- fixtures

const ANTHROPIC_STREAM: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":12}}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"Hello \"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"text_delta\",\"text\":\"world\"}}\n\nevent: content_block_start\ndata: {\"type\":\"content_block_start\",\"content_block\":{\"type\":\"tool_use\",\"id\":\"tu_1\",\"name\":\"read_file\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"{\\\"path\\\":\"}}\n\nevent: content_block_delta\ndata: {\"type\":\"content_block_delta\",\"delta\":{\"type\":\"input_json_delta\",\"partial_json\":\"\\\"a.txt\\\"}\"}}\n\nevent: content_block_stop\ndata: {\"type\":\"content_block_stop\"}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"end_turn\"},\"usage\":{\"output_tokens\":7}}\n\nevent: message_stop\ndata: {\"type\":\"message_stop\"}\n\n";

const ANTHROPIC_REFUSAL: &str = "event: message_start\ndata: {\"type\":\"message_start\",\"message\":{\"usage\":{\"input_tokens\":3}}}\n\nevent: message_delta\ndata: {\"type\":\"message_delta\",\"delta\":{\"stop_reason\":\"refusal\"},\"usage\":{\"output_tokens\":0}}\n\n";

const OPENAI_STREAM: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"world\"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"read_file\",\"arguments\":\"{}\"}}]}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":11,\"completion_tokens\":5}}\n\ndata: [DONE]\n\n";

const OPENAI_REFUSAL: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"I\"},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n";

const GOOGLE_STREAM: &str = "data: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"Hello \"}]}}]}\n\ndata: {\"candidates\":[{\"content\":{\"parts\":[{\"text\":\"world\"},{\"functionCall\":{\"name\":\"read_file\",\"args\":{}}}]},\"finishReason\":\"STOP\"}],\"usageMetadata\":{\"promptTokenCount\":9,\"candidatesTokenCount\":4}}\n\n";

const GOOGLE_REFUSAL: &str = "data: {\"candidates\":[{\"finishReason\":\"SAFETY\"}]}\n\n";

/// Ollama-class stream: no usage anywhere — the adapter must return an
/// explicit estimate, never a silent zero or a fake measurement.
const COMPATIBLE_STREAM_NO_USAGE: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"Hello \"}}]}\n\ndata: {\"choices\":[{\"delta\":{\"content\":\"world\"}}]}\n\ndata: {\"choices\":[{\"delta\":{},\"finish_reason\":\"stop\"}]}\n\ndata: [DONE]\n\n";

const MODELS_CATALOG: &str =
    r#"{"data":[{"id":"llama3.2"},{"id":"test-model"},{"id":"qwen3"}],"object":"list"}"#;

// ------------------------------------------------------------ shape rows

#[test]
fn anthropic_plan_shape_key_in_header_version_pinned() {
    cover(ProviderFamily::Anthropic);
    let transport = FixtureTransport::respond(200, ANTHROPIC_STREAM);
    let mut sink = VecSink::default();
    adapter_for(ProviderFamily::Anthropic)
        .complete(&api_key(), &request(), &transport, &mut sink)
        .expect("fixture stream completes");
    let plan = transport.single_plan();
    assert_eq!(plan.method, "POST");
    assert_eq!(plan.url, "https://api.anthropic.com/v1/messages");
    assert!(!plan.url.contains("sk-test"), "keys never travel in URLs");
    let key_header = plan
        .headers
        .iter()
        .find(|(name, _)| name == "x-api-key")
        .expect("anthropic auth is the x-api-key header");
    assert_eq!(key_header.1, "sk-test-conformance-key");
    assert!(
        plan.headers
            .iter()
            .any(|(name, _)| name == "anthropic-version"),
        "the wire version must be pinned"
    );
    assert!(plan.body.contains("\"stream\":true"));
    assert!(plan.body.contains("\"system\":\"You are terse.\""));
    assert!(plan.body.contains("\"tools\":[{\"name\":\"read_file\"}]"));
}

#[test]
fn openai_and_openrouter_plan_shape_bearer_and_output_caps() {
    for (family, expected_url, cap_field) in [
        (
            ProviderFamily::OpenAi,
            "https://api.openai.com/v1/chat/completions",
            "max_completion_tokens",
        ),
        (
            ProviderFamily::OpenRouter,
            "https://openrouter.ai/api/v1/chat/completions",
            "max_tokens",
        ),
    ] {
        cover(family);
        let transport = FixtureTransport::respond(200, OPENAI_STREAM);
        let mut sink = VecSink::default();
        adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect("fixture stream completes");
        let plan = transport.single_plan();
        assert_eq!(plan.url, expected_url);
        assert!(!plan.url.contains("sk-test"), "keys never travel in URLs");
        let auth = plan
            .headers
            .iter()
            .find(|(name, _)| name == "authorization")
            .expect("bearer auth header");
        assert_eq!(auth.1, "Bearer sk-test-conformance-key");
        assert!(
            plan.body.contains(cap_field),
            "{} must cap output via {cap_field}",
            family.as_str()
        );
        assert!(plan.body.contains("\"stream\":true"));
    }
}

#[test]
fn google_plan_shape_model_in_path_key_in_header_roles_mapped() {
    cover(ProviderFamily::Google);
    let transport = FixtureTransport::respond(200, GOOGLE_STREAM);
    let mut sink = VecSink::default();
    let mut multi_turn = request();
    multi_turn.messages.push(ChatMessage {
        role: MessageRole::Assistant,
        content: "Earlier answer.".to_owned(),
    });
    adapter_for(ProviderFamily::Google)
        .complete(&api_key(), &multi_turn, &transport, &mut sink)
        .expect("fixture stream completes");
    let plan = transport.single_plan();
    assert_eq!(
        plan.url,
        "https://generativelanguage.googleapis.com/v1beta/models/test-model:streamGenerateContent?alt=sse"
    );
    assert!(!plan.url.contains("key="), "keys never travel in URLs");
    assert!(
        plan.headers
            .iter()
            .any(|(name, _)| name == "x-goog-api-key"),
        "google auth is the x-goog-api-key header"
    );
    assert!(
        plan.body.contains("\"role\":\"model\""),
        "assistant turns map to Gemini's `model` role"
    );
    assert!(plan.body.contains("systemInstruction"));
}

#[test]
fn ollama_reasoning_disable_is_explicit_and_unqualified_servers_refuse_it() {
    let mut no_reasoning = request();
    no_reasoning.reasoning_effort = Some(ReasoningEffort::Disabled);
    let transport = FixtureTransport::respond(200, COMPATIBLE_STREAM_NO_USAGE);
    let mut sink = VecSink::default();
    OpenAiCompatibleAdapter {
        base_url: "http://localhost:11434".to_owned(),
        profile: EndpointProfile {
            server: EndpointServer::Ollama,
            supports_stream_usage: false,
        },
    }
    .complete(&api_key(), &no_reasoning, &transport, &mut sink)
    .expect("qualified Ollama accepts explicit reasoning disable");
    assert!(
        transport
            .single_plan()
            .body
            .contains("\"reasoning_effort\":\"none\"")
    );

    let transport = FixtureTransport::respond(200, COMPATIBLE_STREAM_NO_USAGE);
    let refused = OpenAiCompatibleAdapter {
        base_url: "http://localhost:1234".to_owned(),
        profile: EndpointProfile::conservative(),
    }
    .complete(
        &api_key(),
        &no_reasoning,
        &transport,
        &mut VecSink::default(),
    )
    .expect_err("an unqualified compatible server cannot silently ignore reasoning control");
    assert_eq!(
        refused,
        Weather::UnsupportedField {
            field: "reasoning_effort".to_owned()
        }
    );
    assert!(
        transport.plans.lock().expect("test mutex").is_empty(),
        "unsupported reasoning control must fail before transport I/O"
    );
}

// -------------------------------------------------------- streaming rows

#[test]
fn every_family_streams_text_and_passes_tool_use_through() {
    for (family, fixture, expect_tool) in [
        (ProviderFamily::Anthropic, ANTHROPIC_STREAM, true),
        (ProviderFamily::OpenAi, OPENAI_STREAM, true),
        (ProviderFamily::Google, GOOGLE_STREAM, true),
        (ProviderFamily::OpenRouter, OPENAI_STREAM, true),
        (
            ProviderFamily::OpenAiCompatible,
            COMPATIBLE_STREAM_NO_USAGE,
            false,
        ),
    ] {
        cover(family);
        let transport = FixtureTransport::respond(200, fixture);
        let mut sink = VecSink::default();
        let usage = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .unwrap_or_else(|weather| {
                panic!(
                    "{} failed on its stream fixture: {weather}",
                    family.as_str()
                )
            });
        assert_eq!(
            sink.text(),
            "Hello world",
            "{} reassembled the wrong text",
            family.as_str()
        );
        let tool_events = sink
            .events
            .iter()
            .filter(|event| {
                matches!(
                    event,
                    pos_gateway::CompletionEvent::ToolCallPassThrough { .. }
                )
            })
            .count();
        if expect_tool {
            assert!(
                tool_events > 0,
                "{} dropped the tool-use pass-through block",
                family.as_str()
            );
        }
        assert!(usage.tokens_in > 0 && usage.tokens_out > 0);
    }
}

#[test]
fn usage_is_measured_when_reported_and_an_explicit_estimate_when_not() {
    let transport = FixtureTransport::respond(200, OPENAI_STREAM);
    let mut sink = VecSink::default();
    let measured = adapter_for(ProviderFamily::OpenAi)
        .complete(&api_key(), &request(), &transport, &mut sink)
        .expect("fixture stream completes");
    assert!(measured.measured);
    assert_eq!(measured.tokens_in, 11);
    assert_eq!(measured.tokens_out, 5);

    cover(ProviderFamily::OpenAiCompatible);
    let transport = FixtureTransport::respond(200, COMPATIBLE_STREAM_NO_USAGE);
    let mut sink = VecSink::default();
    let estimated = adapter_for(ProviderFamily::OpenAiCompatible)
        .complete(&api_key(), &request(), &transport, &mut sink)
        .expect("fixture stream completes");
    assert!(
        !estimated.measured,
        "an estimate must label itself as one (usage-or-explicit-estimate)"
    );
    assert!(
        estimated.tokens_out > 0,
        "the estimate covers streamed text"
    );
}

// ---------------------------------------------------------- weather rows

#[test]
fn the_shared_weather_mapping_holds_for_every_family() {
    for family in ProviderFamily::ALL {
        cover(family);
        // 429 + retry-after → RateLimited with the provider's own hint.
        let transport = FixtureTransport::new(FixtureOutcome::Response {
            status: 429,
            headers: vec![("retry-after", "2")],
            body: r#"{"error":{"message":"slow down"}}"#,
        });
        let mut sink = VecSink::default();
        let weather = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect_err("429 must be typed weather");
        assert_eq!(
            weather,
            Weather::RateLimited {
                retry_after_ms: Some(2_000)
            },
            "{} mapped 429 wrong",
            family.as_str()
        );

        // 401 → AuthRejected.
        let transport = FixtureTransport::respond(401, r#"{"error":{"message":"bad key"}}"#);
        let mut sink = VecSink::default();
        let weather = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect_err("401 must be typed weather");
        assert_eq!(weather, Weather::AuthRejected { status: 401 });

        // Transport timeout → Timeout carrying the request deadline.
        let transport = FixtureTransport::new(FixtureOutcome::Fail(TransportError::Timeout {
            timeout_ms: 5_000,
        }));
        let mut sink = VecSink::default();
        let weather = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect_err("a timeout must be typed weather");
        assert_eq!(weather, Weather::Timeout { timeout_ms: 5_000 });

        // Garbage SSE payload → MalformedOutput, never a panic or empty Ok.
        let transport = FixtureTransport::respond(200, "data: {not json}\n\n");
        let mut sink = VecSink::default();
        let weather = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect_err("garbage must be typed weather");
        assert!(
            matches!(weather, Weather::MalformedOutput { .. }),
            "{} mapped garbage to {weather:?}",
            family.as_str()
        );
    }
}

#[test]
fn refusals_are_typed_per_family() {
    for (family, fixture) in [
        (ProviderFamily::Anthropic, ANTHROPIC_REFUSAL),
        (ProviderFamily::OpenAi, OPENAI_REFUSAL),
        (ProviderFamily::Google, GOOGLE_REFUSAL),
    ] {
        cover(family);
        let transport = FixtureTransport::respond(200, fixture);
        let mut sink = VecSink::default();
        let weather = adapter_for(family)
            .complete(&api_key(), &request(), &transport, &mut sink)
            .expect_err("a refusal fixture must produce Refusal weather");
        assert!(
            matches!(weather, Weather::Refusal { .. }),
            "{} mapped its refusal to {weather:?}",
            family.as_str()
        );
    }
}

#[test]
fn an_unsupported_field_rejection_is_typed_for_capability_profiles() {
    cover(ProviderFamily::OpenAiCompatible);
    let transport = FixtureTransport::respond(
        400,
        r#"{"error":{"message":"unexpected keyword argument stream_options"}}"#,
    );
    let mut sink = VecSink::default();
    let adapter = OpenAiCompatibleAdapter {
        base_url: "http://localhost:8000".to_owned(),
        profile: EndpointProfile {
            server: EndpointServer::Vllm,
            supports_stream_usage: true,
        },
    };
    let weather = adapter
        .complete(&api_key(), &request(), &transport, &mut sink)
        .expect_err("field rejection must be typed");
    assert_eq!(
        weather,
        Weather::UnsupportedField {
            field: "stream_options".to_owned()
        }
    );
}

// ------------------------------------------- discovery and qualification

#[test]
fn model_discovery_parses_the_catalog_and_maps_failures() {
    cover(ProviderFamily::OpenAiCompatible);
    let transport = FixtureTransport::respond(200, MODELS_CATALOG);
    let models =
        pos_gateway::list_models("http://localhost:11434", &CallAuth::None, &transport, 2_000)
            .expect("catalog parses");
    assert_eq!(models, ["llama3.2", "test-model", "qwen3"]);
    let plan = transport.single_plan();
    assert_eq!(plan.method, "GET");
    assert_eq!(plan.url, "http://localhost:11434/v1/models");

    let transport = FixtureTransport::respond(200, "{\"object\":\"list\"}");
    let weather =
        pos_gateway::list_models("http://localhost:11434", &CallAuth::None, &transport, 2_000)
            .expect_err("a catalog without data is malformed");
    assert!(matches!(weather, Weather::MalformedOutput { .. }));
}

#[test]
fn qualification_records_the_honest_profile_from_fixture_behavior() {
    cover(ProviderFamily::OpenAiCompatible);
    // This fixture transport answers the models call and the completion with
    // the same script; the completion reports no usage, so the qualified
    // profile must downgrade `supports_stream_usage` to false.
    struct TwoStep {
        catalog: FixtureTransport,
        stream: FixtureTransport,
    }
    impl pos_gateway::HttpTransport for TwoStep {
        fn execute(
            &self,
            plan: &pos_gateway::HttpRequestPlan,
            handler: &mut dyn pos_gateway::ResponseHandler,
        ) -> Result<(), pos_gateway::TransportError> {
            if plan.url.ends_with("/v1/models") {
                self.catalog.execute(plan, handler)
            } else {
                self.stream.execute(plan, handler)
            }
        }
    }
    let transport = TwoStep {
        catalog: FixtureTransport::respond(200, MODELS_CATALOG),
        stream: FixtureTransport::respond(200, COMPATIBLE_STREAM_NO_USAGE),
    };
    let report = qualify_openai_compatible(
        EndpointServer::Ollama,
        "http://localhost:11434",
        &CallAuth::None,
        "test-model",
        &transport,
        2_000,
    )
    .expect("qualification runs on fixtures");
    assert_eq!(report.profile.server, EndpointServer::Ollama);
    assert!(
        !report.profile.supports_stream_usage,
        "a server that reported no usage must not be profiled as measuring it"
    );
    assert_eq!(report.completion_text, "Hello world");
    assert!(!report.usage.measured);
}

// --------------------------------------------------------------- coverage

/// Runs last alphabetically-ish but order does not matter: every family
/// must have been covered by at least one row in this binary.
#[test]
fn zz_every_family_has_conformance_rows() {
    // Execute the broadest row first so this test is order-independent.
    the_shared_weather_mapping_holds_for_every_family();
    let covered = COVERED.lock().expect("test mutex");
    for family in ProviderFamily::ALL {
        assert!(
            covered.contains(family.as_str()),
            "{} has no conformance rows",
            family.as_str()
        );
    }
}
