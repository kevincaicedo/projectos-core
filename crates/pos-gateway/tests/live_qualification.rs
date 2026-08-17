//! The m0-s10 release-qualification lane: live OpenAI-compatible endpoint
//! profiles (Ollama, LM Studio, vLLM). Ignored in the PR lane — recorded
//! fixtures own PR coverage — and run explicitly via the matching
//! `just qualify-gateway-*` recipe, which sets the endpoint env vars. Each run prints the honest
//! [`pos_gateway::QualificationReport`]; the recorded result lands in
//! `docs/progress.md` as the story's qualification evidence.
//!
//! The **cloud smoke** (m1-s03) is the same lane over the reviewed TLS
//! transport. It is secret-gated: the key arrives as an environment variable
//! through the ordinary credential path, is never a literal in this file, and
//! is never printed — [`pos_gateway::SecretValue`] cannot serialize and
//! `HttpRequestPlan`'s `Debug` redacts header values, so the assertions below
//! can be loud without the output being dangerous.

#![forbid(unsafe_code)]

use pos_gateway::{CallAuth, EndpointServer, LoopbackHttpTransport, qualify_openai_compatible};

/// One live qualification against `base_env`/`model_env`, skipped with a
/// visible message when the environment does not offer the endpoint.
fn qualify(server: EndpointServer, base_env: &str, model_env: &str) {
    let Ok(base_url) = std::env::var(base_env) else {
        panic!(
            "{base_env} is not set; run this lane via the matching `just qualify-gateway-*` recipe \
             with the endpoint available"
        );
    };
    let model = std::env::var(model_env)
        .unwrap_or_else(|_| panic!("{model_env} is not set; name the qualification model"));
    let report = qualify_openai_compatible(
        server,
        &base_url,
        &CallAuth::None,
        &model,
        &LoopbackHttpTransport,
        // Local first-token latency varies with machine load; a generous
        // deadline keeps the lane about capability, not speed.
        120_000,
    )
    .unwrap_or_else(|weather| panic!("{} qualification failed: {weather}", server.as_str()));
    println!(
        "QUALIFICATION {} base={} model={model} models_served={} stream_usage={} usage_measured={} tokens_in={} tokens_out={} text={:?}",
        server.as_str(),
        report.base_url,
        report.models.len(),
        report.profile.supports_stream_usage,
        report.usage.measured,
        report.usage.tokens_in,
        report.usage.tokens_out,
        report.completion_text
    );
    assert!(
        !report.completion_text.is_empty(),
        "a live completion produced no text"
    );
}

#[test]
#[ignore = "live-endpoint lane: `just qualify-gateway-local` (needs a running Ollama)"]
fn qualify_live_ollama() {
    qualify(
        EndpointServer::Ollama,
        "POS_QUALIFY_OLLAMA_BASE",
        "POS_QUALIFY_OLLAMA_MODEL",
    );
}

#[test]
#[ignore = "live-endpoint lane: pinned LM Studio endpoint (release qualification)"]
fn qualify_live_lm_studio() {
    qualify(
        EndpointServer::LmStudio,
        "POS_QUALIFY_LMSTUDIO_BASE",
        "POS_QUALIFY_LMSTUDIO_MODEL",
    );
}

#[test]
#[ignore = "live-endpoint lane: pinned vLLM endpoint (release qualification)"]
fn qualify_live_vllm() {
    qualify(
        EndpointServer::Vllm,
        "POS_QUALIFY_VLLM_BASE",
        "POS_QUALIFY_VLLM_MODEL",
    );
}

// ---------------------------------------------------------------------------
// m1-s03: the secret-gated cloud smoke over the reviewed TLS transport
// ---------------------------------------------------------------------------

/// One live cloud completion, end to end through the dispatch chokepoint.
///
/// This is the first test in the repository whose bytes leave the device, and
/// it exists to prove exactly that they can — the m0-s10 conformance suite
/// runs from recorded fixtures precisely so that *this* is the only place a
/// real cloud call happens.
#[test]
#[ignore = "live-cloud lane: `just qualify-gateway-cloud` (needs POS_QUALIFY_CLOUD_KEY)"]
fn qualify_live_cloud_over_the_tls_transport() {
    use pos_foundation::{ProjectId, SystemWallClock};
    use pos_gateway::{
        CallAttribution, ChatMessage, CompletionRequest, CredentialClass, EndpointConfig,
        EndpointLocality, Gateway, GatewayConfig, MemoryLedger, MemorySecretStore, MessageRole,
        ModelChoice, ModelPolicy, ModelRouting, OpenRouterAdapter, ProviderFamily, RoutingTier,
        SecretRef, TlsHttpTransport, Transports, VecSink,
    };

    let Ok(key) = std::env::var("POS_QUALIFY_CLOUD_KEY") else {
        panic!(
            "POS_QUALIFY_CLOUD_KEY is not set; run this lane via `just qualify-gateway-cloud` \
             with the key in the environment (never in a file this repository tracks)"
        );
    };
    let base_url = std::env::var("POS_QUALIFY_CLOUD_BASE")
        .unwrap_or_else(|_| "https://openrouter.ai/api".to_owned());
    let model = std::env::var("POS_QUALIFY_CLOUD_MODEL")
        .unwrap_or_else(|_| "openai/gpt-4o-mini".to_owned());

    let secret_ref = SecretRef::new("byok/openrouter/qualification");
    let secrets = MemorySecretStore::new();
    secrets.insert(&secret_ref, key);
    let ledger = MemoryLedger::new();
    let clock = SystemWallClock;
    let tls = TlsHttpTransport::new();
    let loopback = LoopbackHttpTransport;
    let choice = ModelChoice {
        family: ProviderFamily::OpenRouter,
        endpoint: EndpointConfig::new(base_url.clone(), EndpointLocality::Remote)
            .expect("a remote endpoint config"),
        model: model.clone(),
        credential: CredentialClass::Byok {
            secret_ref: secret_ref.clone(),
        },
        is_pinned_family_base: true,
    };
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::CloudAllowed,
            routing: ModelRouting::thinking_only(choice.clone(), choice),
        },
        vec![Box::new(OpenRouterAdapter {
            base_url: base_url.clone(),
        })],
        &secrets,
        &ledger,
        Transports::new(&loopback, &tls),
        &clock,
    );

    let mut sink = VecSink::default();
    let usage = gateway
        .complete(
            RoutingTier::Frontier,
            &CallAttribution {
                project: ProjectId::from_bytes([0x5c; 16]),
                feature: "qualification".to_owned(),
                agent: None,
            },
            &CompletionRequest {
                model: model.clone(),
                system: Some("Answer with one word.".to_owned()),
                messages: vec![ChatMessage {
                    role: MessageRole::User,
                    content: "In one word: what colour is a clear midday sky?".to_owned(),
                }],
                tools_json: None,
                reasoning_effort: None,
                max_output_tokens: 32,
                timeout_ms: 60_000,
            },
            &mut sink,
        )
        .unwrap_or_else(|weather| panic!("cloud qualification failed: {weather}"));

    let text = sink.text();
    println!(
        "QUALIFICATION cloud base={base_url} model={model} tokens_in={} tokens_out={} \
         usage_measured={} text={text:?}",
        usage.tokens_in, usage.tokens_out, usage.measured
    );
    assert!(!text.is_empty(), "a live completion produced no text");

    // The dispatch is exactly one attributed ledger row, like every other.
    let records = ledger.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, "ok");
    assert_eq!(records[0].credential_class, "byok");

    // And the key is nowhere a reader could find it: the preflight surface is
    // the one thing a UI renders about a credential.
    let preflight = gateway.preflight(RoutingTier::Frontier);
    let rendered = format!("{preflight:?}");
    assert!(
        preflight.egress_warning.is_some(),
        "a cloud dispatch must warn that bytes leave the device"
    );
    assert!(
        !rendered.contains("sk-"),
        "the preflight report must never carry key material"
    );
}
