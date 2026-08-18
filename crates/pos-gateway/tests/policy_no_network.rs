//! The m0-s10 policy oracle (L9, F43), **extended by m1-s03**.
//!
//! ## What changed, and why the extension is not optional
//!
//! Until m1-s03 the gateway held one transport, and it was loopback-only by
//! construction. "A `local_only` project cannot egress" was therefore a
//! property of the *build*: there was nothing else to reach with.
//!
//! [ADR-0006] adds a TLS transport, and says so plainly — the guarantee
//! weakens to
//!
//! > under `local_only`, dispatch **selects** a transport that is structurally
//! > incapable of reaching a non-loopback host.
//!
//! The ADR makes extending this file a condition of that decision standing:
//! "without it this ADR has downgraded a §18 gate". So the suite now proves
//! four things rather than one:
//!
//! 1. a refused dispatch still makes zero network I/O, zero credential I/O,
//!    and exactly one attributed ledger row (the original criterion);
//! 2. every route a `local_only` project can hold **selects** `device_local`
//!    or `in_process` — asserted on the selection value, not inferred;
//! 3. the transport that selection names **refuses cloud hosts itself**, so
//!    the guarantee survives a policy bug;
//! 4. a gateway carrying *both* transports still sends a `local_only`
//!    dispatch to neither — the positive control that keeps (2) meaningful
//!    now that an alternative exists.
//!
//! [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md

#![forbid(unsafe_code)]

mod common;

use common::{
    CountingTransport, all_providers, byok_store, cloud_frontier_choice, cloud_transcribe_choice,
    in_process_transcribe_choice, local_fast_choice, routing,
};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_gateway::{
    BufferedResponse, CallAttribution, ChatMessage, CompletionRequest, Gateway, GatewayConfig,
    HttpMethod, HttpRequestPlan, HttpTransport, LoopbackHttpTransport, MemoryLedger, MessageRole,
    ModelPolicy, RoutingTier, SecretRef, SecretStore, TlsHttpTransport, TranscribeRequest,
    TransportError, TransportSelection, Transports, VecSink, VecTranscriptSink, Weather,
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
        Transports::device_local_only(&transport),
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
        Transports::new(&transport, &transport),
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
        Transports::new(&transport, &transport),
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

// ---------------------------------------------------------------------------
// m1-s03: the transport-selection half (ADR-0006's compensating control)
// ---------------------------------------------------------------------------

#[test]
fn every_route_a_local_only_project_can_hold_selects_a_transport_that_cannot_egress() {
    // Criterion 2. The assertion is on the *selection value*, so it fails if a
    // later change routes a device-local endpoint through the cloud transport
    // — which no amount of "the loopback one refuses anyway" would catch.
    let local = local_fast_choice();
    assert_eq!(
        Gateway::transport_selection(&local),
        TransportSelection::DeviceLocal
    );
    let whisper = in_process_transcribe_choice();
    assert_eq!(
        Gateway::transport_selection(&whisper),
        TransportSelection::InProcess,
        "an in-process model must be handed no transport at all"
    );

    // And the complement: the only selection that reaches TLS is the one the
    // `local_only` gate refuses outright.
    let secret_ref = SecretRef::new("byok/openai/stt");
    let cloud = cloud_transcribe_choice(&secret_ref);
    assert_eq!(
        Gateway::transport_selection(&cloud),
        TransportSelection::Remote
    );
    assert!(
        matches!(
            ModelPolicy::LocalOnly.authorize(&cloud),
            Err(Weather::PolicyViolation { .. })
        ),
        "local_only must refuse the one route whose selection is remote"
    );
}

#[test]
fn the_selected_local_transport_refuses_cloud_hosts_and_the_tls_one_refuses_cleartext() {
    // Criterion 3: the guarantee holds one level below the policy. Even if a
    // policy bug let a cloud URL through, the transport `device_local`
    // selects cannot reach it — and the TLS transport cannot be tricked into
    // sending a key in cleartext.
    let cloud_plan = HttpRequestPlan {
        method: HttpMethod::Post,
        url: "https://api.anthropic.com/v1/messages".to_owned(),
        headers: vec![("authorization", "Bearer sk-never-leaves".to_owned())],
        body: b"{}".to_vec(),
        timeout_ms: 1_000,
        response_bytes_max: None,
    };
    let mut buffered = BufferedResponse::default();
    let refused = LoopbackHttpTransport
        .execute(&cloud_plan, &mut buffered)
        .expect_err("the device-local transport must refuse a cloud host");
    assert!(matches!(refused, TransportError::HostRefused { .. }));

    let cleartext_plan = HttpRequestPlan {
        url: "http://api.anthropic.com/v1/messages".to_owned(),
        ..cloud_plan
    };
    let refused = TlsHttpTransport::new()
        .execute(&cleartext_plan, &mut buffered)
        .expect_err("the TLS transport must refuse to put a key on the wire in cleartext");
    assert!(matches!(refused, TransportError::HostRefused { .. }));
    assert!(buffered.head.is_none(), "no response was ever received");
}

#[test]
fn a_gateway_holding_both_transports_still_sends_a_local_only_dispatch_to_neither() {
    // Criterion 4. This is the case that did not exist before m1-s03: the
    // alternative is composed and reachable, and the refusal must still be
    // total. Both counters staying at zero is the whole point.
    let secret_ref = SecretRef::new("byok/anthropic/both-transports");
    let secrets = byok_store(&secret_ref, "sk-must-never-be-touched");
    let ledger = MemoryLedger::new();
    let device_local = CountingTransport::default();
    let remote = CountingTransport::default();
    let clock = ManualWallClock::starting_at(1_754_000_000_000);
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::LocalOnly,
            routing: routing(cloud_frontier_choice(&secret_ref), local_fast_choice())
                .with_transcribe(cloud_transcribe_choice(&secret_ref)),
        },
        all_providers(),
        &secrets,
        &ledger,
        Transports::new(&device_local, &remote),
        &clock,
    );

    let mut sink = VecSink::default();
    let weather = gateway
        .complete(RoutingTier::Frontier, &attribution(), &request(), &mut sink)
        .expect_err("local_only refuses the cloud route even with a cloud transport in hand");
    assert!(matches!(weather, Weather::PolicyViolation { .. }));

    // The transcription path is the same chokepoint, so it must refuse the
    // same way — a modality is not an exemption (F43).
    let samples = vec![0.0_f32; 16_000];
    let mut transcript = VecTranscriptSink::default();
    let weather = gateway
        .transcribe(
            &attribution(),
            &TranscribeRequest {
                model: "whisper-1",
                language: None,
                offset_ms: 0,
                samples: &samples,
            },
            &mut transcript,
        )
        .expect_err("local_only refuses a cloud STT route");
    assert!(
        matches!(&weather, Weather::PolicyViolation { policy, .. } if policy == "local_only"),
        "got {weather:?}"
    );

    assert_eq!(device_local.attempt_count(), 0);
    assert_eq!(
        remote.attempt_count(),
        0,
        "the cloud transport exists in this gateway and must still be untouched"
    );
    assert_eq!(secrets.last_used_ts_ms(&secret_ref), None);
    assert!(transcript.segments.is_empty());
    // Both refusals are attributed facts, not silent drops.
    let records = ledger.records();
    assert_eq!(records.len(), 2);
    assert!(
        records
            .iter()
            .all(|record| record.outcome == "policy_violation")
    );
}

#[test]
fn an_authorized_remote_dispatch_with_no_cloud_transport_refuses_typed() {
    // The wiring mistake that would otherwise be invisible: a deployment that
    // permits cloud models but composed no cloud transport. "Authorized and
    // then nothing happened" is the worst available answer (L8).
    let secret_ref = SecretRef::new("byok/anthropic/no-remote");
    let secrets = byok_store(&secret_ref, "sk-unused");
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
        Transports::device_local_only(&transport),
        &clock,
    );
    let mut sink = VecSink::default();
    let weather = gateway
        .complete(RoutingTier::Frontier, &attribution(), &request(), &mut sink)
        .expect_err("an authorized dispatch with nothing to travel on must say so");
    assert!(
        matches!(&weather, Weather::TransportUnavailable { selection } if *selection == "remote"),
        "got {weather:?}"
    );
    assert_eq!(transport.attempt_count(), 0);
    assert_eq!(ledger.records()[0].outcome, "transport_unavailable");
}
