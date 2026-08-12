//! The m0-s10 policy oracle (L9, F43): under `local_only`, a frontier-tier
//! request makes **zero network I/O** — the counting transport sees no
//! connection attempt — and returns typed `PolicyViolation`. The refusal is
//! still a ledger row, because a refused call is an attributed fact.

#![forbid(unsafe_code)]

mod common;

use common::{
    CountingTransport, all_providers, byok_store, cloud_frontier_choice, local_fast_choice, routing,
};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_gateway::{
    CallAttribution, ChatMessage, CompletionRequest, Gateway, GatewayConfig, MemoryLedger,
    MessageRole, ModelPolicy, RoutingTier, SecretRef, SecretStore, VecSink, Weather,
};

fn request() -> CompletionRequest {
    CompletionRequest {
        model: "claude-frontier-test".to_owned(),
        system: None,
        messages: vec![ChatMessage {
            role: MessageRole::User,
            content: "Summarize the evidence.".to_owned(),
        }],
        tools_json: None,
        reasoning_effort: None,
        max_output_tokens: 256,
        timeout_ms: 5_000,
    }
}

fn attribution() -> CallAttribution {
    CallAttribution {
        project: ProjectId::from_bytes([3; 16]),
        feature: "synthesis".to_owned(),
        agent: Some("analyst".to_owned()),
    }
}

#[test]
fn local_only_frontier_dispatch_makes_zero_network_io_and_types_the_refusal() {
    let secret_ref = SecretRef::new("byok/anthropic/policy-test");
    let secrets = byok_store(&secret_ref, "sk-must-never-be-touched");
    let ledger = MemoryLedger::new();
    let transport = CountingTransport::default();
    let clock = ManualWallClock::starting_at(1_754_000_000_000);
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::LocalOnly,
            routing: routing(cloud_frontier_choice(&secret_ref), local_fast_choice()),
        },
        all_providers(),
        &secrets,
        &ledger,
        &transport,
        &clock,
    );

    let mut sink = VecSink::default();
    let weather = gateway
        .complete(RoutingTier::Frontier, &attribution(), &request(), &mut sink)
        .expect_err("local_only must refuse a frontier cloud dispatch");

    // The typed refusal names the policy and the refused endpoint class.
    assert!(
        matches!(&weather, Weather::PolicyViolation { policy, .. } if policy == "local_only"),
        "got {weather:?}"
    );
    // Zero network I/O: the transport never saw a connection attempt.
    assert_eq!(transport.attempt_count(), 0);
    // Zero credential I/O either: policy runs before resolution, so the
    // secret was never read or marked used.
    assert_eq!(secrets.last_used_ts_ms(&secret_ref), None);
    // No output reached the sink.
    assert!(sink.events.is_empty());
    // The refusal is still exactly one attributed ledger row.
    let records = ledger.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].outcome, "policy_violation");
    assert_eq!(records[0].feature, "synthesis");
    assert_eq!(records[0].tokens_in, 0);
}

#[test]
fn the_same_gateway_under_cloud_allowed_reaches_the_transport() {
    // The positive control that keeps the zero above meaningful: with the
    // policy widened and nothing else changed, the dispatch reaches the
    // transport (which then refuses the connection, typed).
    let secret_ref = SecretRef::new("byok/anthropic/policy-test");
    let secrets = byok_store(&secret_ref, "sk-positive-control");
    let ledger = MemoryLedger::new();
    let transport = CountingTransport::default();
    let clock = ManualWallClock::starting_at(1_754_000_000_000);
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::CloudAllowed,
            routing: routing(cloud_frontier_choice(&secret_ref), local_fast_choice()),
        },
        all_providers(),
        &secrets,
        &ledger,
        &transport,
        &clock,
    );

    let mut sink = VecSink::default();
    let weather = gateway
        .complete(RoutingTier::Frontier, &attribution(), &request(), &mut sink)
        .expect_err("the counting transport refuses connections");
    assert_eq!(transport.attempt_count(), 1);
    assert!(matches!(weather, Weather::Transport { .. }));
    assert_eq!(ledger.records().len(), 1);
    assert_eq!(ledger.records()[0].outcome, "transport_failure");
}

#[test]
fn a_revoked_credential_blocks_the_dispatch_before_any_socket() {
    let secret_ref = SecretRef::new("byok/anthropic/revoked");
    let secrets = byok_store(&secret_ref, "sk-revoked-key");
    secrets.revoke(&secret_ref).expect("stored refs revoke");
    let ledger = MemoryLedger::new();
    let transport = CountingTransport::default();
    let clock = ManualWallClock::starting_at(1_754_000_000_000);
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::CloudAllowed,
            routing: routing(cloud_frontier_choice(&secret_ref), local_fast_choice()),
        },
        all_providers(),
        &secrets,
        &ledger,
        &transport,
        &clock,
    );
    let mut sink = VecSink::default();
    let weather = gateway
        .complete(RoutingTier::Frontier, &attribution(), &request(), &mut sink)
        .expect_err("a revoked credential must refuse immediately");
    assert!(matches!(weather, Weather::CredentialRevoked));
    assert_eq!(transport.attempt_count(), 0);
    assert_eq!(ledger.records()[0].outcome, "credential_revoked");

    // The preflight surface reports the revocation without resolving a use.
    let preflight = gateway.preflight(RoutingTier::Frontier);
    assert!(preflight.revoked);
    assert_eq!(preflight.credential_class, "byok");
    assert!(preflight.egress_warning.is_some(), "cloud egress must warn");
    let local = gateway.preflight(RoutingTier::Fast);
    assert!(
        local.egress_warning.is_none(),
        "loopback egress warns about nothing"
    );
}
