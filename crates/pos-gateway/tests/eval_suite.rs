//! The m0-s11 eval CI job: runs the repository's real `evals/echo` golden
//! set through the real gateway dispatch path (prompt loaded by id from
//! `prompts/`, `Gateway::complete`, ledger rows written), writes the report
//! artifact, and blocks merge on regression — proven by the deliberately
//! broken fixture set, which must regress or the gate is decoration.

#![forbid(unsafe_code)]

mod common;

use common::{FixtureTransport, all_providers, local_fast_choice, routing};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_gateway::{
    CallAttribution, ChatMessage, CompletionRequest, EvalCase, Gateway, GatewayConfig,
    MemoryLedger, MemorySecretStore, MessageRole, ModelPolicy, PromptFile, PromptRegistry,
    RoutingTier, VecSink, load_cases, run_suite,
};
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root exists")
}

/// One echo-shaped SSE body per case input, in the OpenAI-compatible wire
/// the local fast tier speaks. Recorded-fixture stand-in for the live
/// echo model until m0-s13 binds the real agent to this same suite.
fn echo_fixture_body(input: &str) -> String {
    let payload = serde_json::json!({
        "choices": [{"delta": {"content": format!("ECHO: {input}")}}]
    });
    format!("data: {payload}\n\ndata: [DONE]\n\n")
}

fn dispatch_case(prompt: &PromptFile, case: &EvalCase) -> Result<String, pos_gateway::Weather> {
    let body: &'static str = Box::leak(echo_fixture_body(&case.input).into_boxed_str());
    let transport = FixtureTransport::respond(200, body);
    let secrets = MemorySecretStore::new();
    let ledger = MemoryLedger::new();
    let clock = ManualWallClock::starting_at(1_754_600_000_000);
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::LocalOnly,
            routing: routing(local_fast_choice(), local_fast_choice()),
        },
        all_providers(),
        &secrets,
        &ledger,
        &transport,
        &clock,
    );
    let request = CompletionRequest {
        model: "llama-test".to_owned(),
        system: Some(prompt.body.clone()),
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: case.input.clone(),
        }],
        tools_json: None,
        max_output_tokens: 128,
        timeout_ms: 5_000,
    };
    let attribution = CallAttribution {
        project: ProjectId::from_bytes([5; 16]),
        feature: "eval".to_owned(),
        agent: Some("echo".to_owned()),
    };
    let mut sink = VecSink::default();
    gateway.complete(RoutingTier::Fast, &attribution, &request, &mut sink)?;
    // The eval harness scores exactly one attributed call per case.
    assert_eq!(ledger.records().len(), 1);
    assert_eq!(ledger.records()[0].feature, "eval");
    Ok(sink.text())
}

#[test]
fn the_trivial_echo_suite_is_green_and_writes_its_artifact() {
    let root = repo_root();
    let registry = PromptRegistry::load_dir(&root.join("prompts")).expect("prompts load");
    let prompt = registry
        .get("echo", 1)
        .expect("echo@1 is registered")
        .clone();
    let cases = load_cases(&root.join("evals/echo/cases.jsonl")).expect("golden set loads");

    let mut complete = |prompt: &PromptFile, case: &EvalCase| dispatch_case(prompt, case);
    let report = run_suite("echo", "llama-test", &prompt, &cases, &mut complete);

    assert!(
        !report.regressed(),
        "the echo suite regressed: {:?}",
        report
            .outcomes
            .iter()
            .filter(|outcome| !outcome.passed)
            .map(|outcome| format!("{}: {}", outcome.case_id, outcome.detail))
            .collect::<Vec<_>>()
    );
    assert!(report.prompt_reference.starts_with("echo@1#"));

    let artifact_dir = root.join("target/eval-reports");
    std::fs::create_dir_all(&artifact_dir).expect("artifact dir");
    let artifact = artifact_dir.join("echo.json");
    report.write_artifact(&artifact).expect("artifact writes");
    assert!(artifact.exists());
}

/// The gate must be seen red: the deliberately broken fixture set expects a
/// marker no output contains, and the suite must report regression.
#[test]
fn the_deliberately_broken_fixture_regresses_the_suite() {
    let root = repo_root();
    let registry = PromptRegistry::load_dir(&root.join("prompts")).expect("prompts load");
    let prompt = registry
        .get("echo", 1)
        .expect("echo@1 is registered")
        .clone();
    let broken = load_cases(&root.join("evals/echo/cases-broken.jsonl")).expect("broken set loads");

    let mut complete = |prompt: &PromptFile, case: &EvalCase| dispatch_case(prompt, case);
    let report = run_suite("echo-broken", "llama-test", &prompt, &broken, &mut complete);
    assert!(
        report.regressed(),
        "a suite that cannot fail is decoration, not a gate"
    );
}
