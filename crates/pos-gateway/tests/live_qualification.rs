//! The m0-s10 release-qualification lane: live OpenAI-compatible endpoint
//! profiles (Ollama, LM Studio, vLLM). Ignored in the PR lane — recorded
//! fixtures own PR coverage — and run explicitly via the matching
//! `just qualify-gateway-*` recipe, which sets the endpoint env vars. Each run prints the honest
//! [`pos_gateway::QualificationReport`]; the recorded result lands in
//! `docs/progress.md` as the story's qualification evidence.
//!
//! Cloud smokes (Anthropic/OpenAI/Google/OpenRouter live) additionally need
//! the cloud-capable TLS transport core deliberately does not carry yet
//! (transport.rs module doc); they join this lane with that transport —
//! visible debt owned by the first live-cloud story (m0-s13 cloud leg /
//! m1-s03).

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
