//! The m0-s10 ledger property (honest cost ledger): for arbitrary dispatch
//! outcomes — success, refusal, rate limit, timeout, auth rejection,
//! malformed output, policy refusal, revoked credential — every call writes
//! **exactly one** fully attributed ledger row, and BYOK/device spend is
//! `customer_billed`, never fake ProjectOS model cost.

#![forbid(unsafe_code)]

mod common;

use common::{
    FixtureOutcome, FixtureTransport, all_providers, byok_store, cloud_frontier_choice,
    local_fast_choice, routing,
};
use pos_foundation::{ManualWallClock, ProjectId};
use pos_gateway::{
    CallAttribution, ChatMessage, CompletionRequest, Gateway, GatewayConfig, MemoryLedger,
    MessageRole, ModelPolicy, ProviderCostKind, RoutingTier, SecretRef, SecretStore,
    TransportError, Transports, VecSink,
};
use proptest::prelude::*;

/// The outcome classes the property ranges over. Each maps to a scripted
/// fixture/transport behavior and (for the pre-transport classes) a policy
/// or credential arrangement.
#[derive(Clone, Copy, Debug)]
enum OutcomeClass {
    Success,
    RateLimited,
    AuthRejected,
    Timeout,
    MalformedOutput,
    Refusal,
    PolicyRefused,
    CredentialRevoked,
    OverBudget,
}

const OPENAI_OK: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"fine\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":4,\"completion_tokens\":1}}\n\ndata: [DONE]\n\n";
const OPENAI_REFUSAL: &str =
    "data: {\"choices\":[{\"delta\":{},\"finish_reason\":\"content_filter\"}]}\n\ndata: [DONE]\n\n";

fn outcome_strategy() -> impl Strategy<Value = OutcomeClass> {
    prop_oneof![
        Just(OutcomeClass::Success),
        Just(OutcomeClass::RateLimited),
        Just(OutcomeClass::AuthRejected),
        Just(OutcomeClass::Timeout),
        Just(OutcomeClass::MalformedOutput),
        Just(OutcomeClass::Refusal),
        Just(OutcomeClass::PolicyRefused),
        Just(OutcomeClass::CredentialRevoked),
        Just(OutcomeClass::OverBudget),
    ]
}

fn transport_for(class: OutcomeClass) -> FixtureTransport {
    match class {
        OutcomeClass::Success => FixtureTransport::respond(200, OPENAI_OK),
        OutcomeClass::RateLimited => FixtureTransport::new(FixtureOutcome::Response {
            status: 429,
            headers: vec![("retry-after", "1")],
            body: r#"{"error":{"message":"slow down"}}"#,
        }),
        OutcomeClass::AuthRejected => {
            FixtureTransport::respond(401, r#"{"error":{"message":"bad key"}}"#)
        }
        OutcomeClass::Timeout => {
            FixtureTransport::new(FixtureOutcome::Fail(TransportError::Timeout {
                timeout_ms: 5_000,
            }))
        }
        OutcomeClass::MalformedOutput => FixtureTransport::respond(200, "data: {nope}\n\n"),
        OutcomeClass::Refusal => FixtureTransport::respond(200, OPENAI_REFUSAL),
        // Pre-transport refusals never reach the transport; the fixture
        // still exists so an accidental dispatch is visible as a plan.
        OutcomeClass::PolicyRefused
        | OutcomeClass::CredentialRevoked
        | OutcomeClass::OverBudget => FixtureTransport::respond(200, OPENAI_OK),
    }
}

fn expected_outcome_code(class: OutcomeClass) -> &'static str {
    match class {
        OutcomeClass::Success => "ok",
        OutcomeClass::RateLimited => "rate_limited",
        OutcomeClass::AuthRejected => "auth_rejected",
        OutcomeClass::Timeout => "timeout",
        OutcomeClass::MalformedOutput => "malformed_output",
        OutcomeClass::Refusal => "refusal",
        OutcomeClass::PolicyRefused => "policy_violation",
        OutcomeClass::CredentialRevoked => "credential_revoked",
        OutcomeClass::OverBudget => "budget_exhausted",
    }
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(64))]

    /// Exactly one fully attributed row per dispatch, for every outcome
    /// class, with the credential class deciding the cost kind.
    #[test]
    fn every_dispatch_writes_exactly_one_attributed_row(
        class in outcome_strategy(),
        use_byok_frontier in any::<bool>(),
        feature_index in 0_usize..3,
    ) {
        let features = ["synthesis", "extraction", "echo"];
        let feature = features[feature_index];
        let secret_ref = SecretRef::new("byok/openai/property");
        let secrets = byok_store(&secret_ref, "sk-property-key");
        if matches!(class, OutcomeClass::CredentialRevoked) {
            secrets.revoke(&secret_ref).expect("stored refs revoke");
        }
        let ledger = MemoryLedger::new();
        let transport = transport_for(class);
        let clock = ManualWallClock::starting_at(1_754_500_000_000);

        // BYOK cloud frontier vs device-local fast: both credential classes
        // are exercised across the property run.
        let tier = if use_byok_frontier { RoutingTier::Frontier } else { RoutingTier::Fast };
        let policy = if matches!(class, OutcomeClass::PolicyRefused) {
            // Policy refusal needs a policy the routed choice violates.
            prop_assume!(use_byok_frontier);
            ModelPolicy::LocalOnly
        } else {
            ModelPolicy::CloudAllowed
        };
        // Revocation only applies to the credentialed (BYOK) route.
        if matches!(class, OutcomeClass::CredentialRevoked) {
            prop_assume!(use_byok_frontier);
        }
        let mut frontier = cloud_frontier_choice(&secret_ref);
        // The frontier fixture speaks the OpenAI wire; route it there.
        frontier.family = pos_gateway::ProviderFamily::OpenAi;
        let gateway = Gateway::new(
            GatewayConfig { policy, routing: routing(frontier, local_fast_choice()) },
            all_providers(),
            &secrets,
            &ledger,
            Transports::new(&transport, &transport),
            &clock,
        );

        let request = CompletionRequest {
            model: "property-model".to_owned(),
            system: None,
            messages: vec![ChatMessage { role: MessageRole::User, content: "hi".to_owned() }],
            tools_json: None,
            reasoning_effort: None,
            max_output_tokens: if matches!(class, OutcomeClass::OverBudget) {
                pos_gateway::OUTPUT_TOKENS_REQUEST_MAX + 1
            } else {
                64
            },
            timeout_ms: 5_000,
        };
        let attribution = CallAttribution {
            project: ProjectId::from_bytes([9; 16]),
            feature: feature.to_owned(),
            agent: Some("prop-agent".to_owned()),
        };

        let mut sink = VecSink::default();
        let outcome = gateway.complete(tier, &attribution, &request, &mut sink);

        // Exactly one row, no matter what happened.
        let records = ledger.records();
        prop_assert_eq!(records.len(), 1, "outcome {:?} wrote {} rows", outcome, records.len());
        let record = &records[0];

        // Fully attributed, every path.
        prop_assert_eq!(record.project, ProjectId::from_bytes([9; 16]));
        prop_assert_eq!(record.feature.as_str(), feature);
        prop_assert_eq!(record.agent.as_deref(), Some("prop-agent"));
        prop_assert_eq!(record.model.as_str(), "property-model");
        prop_assert_eq!(record.outcome.as_str(), expected_outcome_code(class));

        // The credential class decides who pays: BYOK and device sessions
        // are customer_billed with zero ProjectOS cost, always.
        if use_byok_frontier {
            prop_assert_eq!(record.credential_class, "byok");
        } else {
            prop_assert_eq!(record.credential_class, "device_session");
        }
        prop_assert_eq!(record.provider_cost_kind, ProviderCostKind::CustomerBilled);
        prop_assert_eq!(record.usd_micros, 0);

        // Success carries the measured usage; failures carry zero tokens.
        match class {
            OutcomeClass::Success if use_byok_frontier => {
                prop_assert_eq!(record.tokens_in, 4);
                prop_assert_eq!(record.tokens_out, 1);
            }
            OutcomeClass::Success => {
                // The device-local route parses the same fixture.
                prop_assert!(record.tokens_in > 0);
            }
            _ => prop_assert_eq!(record.tokens_in, 0),
        }
    }
}
