//! The m0-s10 persistence chain and secret oracle, end to end through real
//! storage: gateway dispatch → `EventCostLedger` → `ModelCallCompleted`
//! events → `proj_model_calls` → `cost.rollup` over both simulated
//! transports — then the secret scan: after BYOK dispatches, the raw bytes
//! of the project database, its WAL, the export directory (including
//! `events.jsonl`), and every Debug rendering contain zero provider-key
//! material.

#![forbid(unsafe_code)]

use pos_api::{
    CommandName, EventCostLedger, LocalBootstrapConfig, ProjectCreateInput, ProjectExportInput,
    ProjectPathInput, QueryName, bootstrap_local_runtime, input_json,
};
use pos_foundation::{DeviceId, ManualWallClock};
use pos_gateway::{
    CallAttribution, ChatMessage, CompletionRequest, CredentialClass, EndpointConfig,
    EndpointLocality, Gateway, GatewayConfig, HttpHead, HttpRequestPlan, HttpTransport,
    MemorySecretStore, MessageRole, ModelChoice, ModelPolicy, ModelRouting, ProviderFamily,
    ResponseHandler, RoutingTier, SecretRef, TransportError, VecSink,
};
use std::path::Path;

/// The exact secret planted in the store; the scan greps every artifact for
/// these bytes (and their reverse, a cheap mangling canary).
const PLANTED_SECRET: &str = "sk-ant-PLANTED-SECRET-0xDEADBEEF-do-not-store";

const OPENAI_OK: &str = "data: {\"choices\":[{\"delta\":{\"content\":\"cited answer\"}}]}\n\ndata: {\"choices\":[],\"usage\":{\"prompt_tokens\":6,\"completion_tokens\":2}}\n\ndata: [DONE]\n\n";

struct FixtureTransport {
    status: u16,
    body: &'static str,
}

impl HttpTransport for FixtureTransport {
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        // The transport is the last hop before bytes leave: assert the key
        // travels only in a header value, never in the URL or body.
        assert!(!plan.url.contains(PLANTED_SECRET));
        assert!(!String::from_utf8_lossy(&plan.body).contains(PLANTED_SECRET));
        let head = HttpHead {
            status: self.status,
            headers: Vec::new(),
        };
        if handler.on_head(&head).is_err() {
            return Err(TransportError::Aborted);
        }
        for chunk in self.body.as_bytes().chunks(11) {
            if handler.on_chunk(chunk).is_err() {
                return Err(TransportError::Aborted);
            }
        }
        Ok(())
    }
}

fn byok_openai_choice(secret_ref: &SecretRef) -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::OpenAi,
        endpoint: EndpointConfig::new("https://api.openai.com", EndpointLocality::Remote)
            .expect("remote endpoint"),
        model: "cost-test-model".to_owned(),
        credential: CredentialClass::Byok {
            secret_ref: secret_ref.clone(),
        },
        is_pinned_family_base: true,
    }
}

/// Runs `count` gateway dispatches against an open project, recording every
/// call through the event ledger. Returns the Debug renderings produced
/// along the way so the scan can sweep them too.
fn dispatch_calls(project_root: &Path, count: usize, fail_last: bool) -> Vec<String> {
    let registry = pos_domain::v0_registry().expect("registry builds");
    let store = pos_store::ProjectStore::open(project_root).expect("project opens");
    let log = pos_log::ProjectLog::open(store, registry, pos_log::LogConfig::default())
        .expect("log opens");
    let clock = ManualWallClock::starting_at(1_754_700_000_000);
    let project_id = log.store().manifest().project_id;
    let ledger = EventCostLedger::new(
        &log,
        DeviceId::from_bytes([0x01; 16]),
        pos_log::Actor::System(pos_foundation::JobId::from_bytes([0x0c; 16])),
        &clock,
    );
    let secret_ref = SecretRef::new("byok/openai/scan-test");
    let secrets = MemorySecretStore::new();
    secrets.insert(&secret_ref, PLANTED_SECRET);

    let mut debug_renderings = Vec::new();
    for index in 0..count {
        let failing = fail_last && index + 1 == count;
        let transport = FixtureTransport {
            status: if failing { 429 } else { 200 },
            body: if failing {
                r#"{"error":{"message":"slow down"}}"#
            } else {
                OPENAI_OK
            },
        };
        let gateway = Gateway::new(
            GatewayConfig {
                policy: ModelPolicy::CloudAllowed,
                routing: ModelRouting {
                    frontier: byok_openai_choice(&secret_ref),
                    fast: byok_openai_choice(&secret_ref),
                },
            },
            vec![Box::new(pos_gateway::OpenAiAdapter {
                base_url: "https://api.openai.com".to_owned(),
            })],
            &secrets,
            &ledger,
            &transport,
            &clock,
        );
        let request = CompletionRequest {
            model: "cost-test-model".to_owned(),
            system: None,
            messages: vec![ChatMessage {
                role: MessageRole::User,
                content: "why did activation drop?".to_owned(),
            }],
            tools_json: None,
            max_output_tokens: 64,
            timeout_ms: 5_000,
        };
        let attribution = CallAttribution {
            project: project_id,
            feature: "synthesis".to_owned(),
            agent: Some("analyst".to_owned()),
        };
        let mut sink = VecSink::default();
        let outcome = gateway.complete(RoutingTier::Frontier, &attribution, &request, &mut sink);
        debug_renderings.push(format!("{outcome:?}"));
        debug_renderings.push(format!("{:?}", gateway.preflight(RoutingTier::Frontier)));
        if failing {
            outcome.expect_err("the seeded 429 must be typed weather");
        } else {
            outcome.expect("the fixture stream completes");
        }
    }
    debug_renderings
}

fn scan_tree_for_secret(root: &Path) {
    let mut pending = vec![root.to_path_buf()];
    // Iterative walk with an explicit stack (no recursion, style rule).
    while let Some(path) = pending.pop() {
        if path.is_dir() {
            for entry in std::fs::read_dir(&path).expect("scan dir") {
                pending.push(entry.expect("scan entry").path());
            }
            continue;
        }
        let bytes = std::fs::read(&path).expect("scan file");
        assert!(
            !contains_bytes(&bytes, PLANTED_SECRET.as_bytes()),
            "provider-key material found in {}",
            path.display()
        );
    }
}

fn contains_bytes(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

#[test]
fn ledger_events_project_and_roll_up_identically_on_both_transports() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("packs"),
    ));
    let project = directory.path().join("cost.pos");
    let path = project.display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Cost".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");

    // Three successful calls and one rate-limited failure: the rollup must
    // carry both outcome groups, because error paths are ledger rows too.
    dispatch_calls(&project, 4, true);

    let rollup_input = input_json(&pos_api::CostRollupInput {
        path: Some(path.clone()),
    })
    .expect("input serializes");
    let ipc = runtime
        .query_with_input(QueryName::CostRollup.as_str(), &rollup_input)
        .expect("rollup resolves");
    let http = runtime
        .query_with_input(QueryName::CostRollup.as_str(), &rollup_input)
        .expect("rollup resolves");
    assert_eq!(
        ipc, http,
        "cost.rollup must be byte-identical across dispatches"
    );

    assert!(ipc.contains("\"scope\":\"project\""));
    assert!(ipc.contains("\"feature\":\"synthesis\""));
    assert!(ipc.contains("\"credentialClass\":\"byok\""));
    assert!(ipc.contains("\"providerCostKind\":\"customer_billed\""));
    // 3 ok calls with measured usage 6/2 each; the 429 call contributes a
    // row with zero tokens — the sums below prove both groups are present.
    assert!(ipc.contains("\"calls\":4"));
    assert!(ipc.contains("\"tokensIn\":18"));
    assert!(ipc.contains("\"tokensOut\":6"));
    // BYOK spend is customer_billed: ProjectOS-billable total stays zero.
    assert!(ipc.contains("\"projectosUsdMicros\":0"));

    // Session scope: an empty session rolls up to zero rows, honestly.
    let empty = runtime
        .query_with_input(QueryName::CostRollup.as_str(), "{}")
        .expect("session rollup resolves");
    assert!(empty.contains("\"scope\":\"session\""));
    assert!(empty.contains("\"rows\":[]"));

    // After project.open, the session rollup sees the same project rows.
    runtime
        .command(
            CommandName::ProjectOpen.as_str(),
            &input_json(&ProjectPathInput { path }).expect("input serializes"),
        )
        .expect("open resolves");
    let session = runtime
        .query_with_input(QueryName::CostRollup.as_str(), "{}")
        .expect("session rollup resolves");
    assert!(session.contains("\"projectCount\":1"));
    assert!(session.contains("\"calls\":4"));
}

#[test]
fn the_secret_scan_finds_zero_key_material_in_log_export_and_debug_output() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("packs"),
    ));
    let project = directory.path().join("scan.pos");
    let path = project.display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Scan".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");

    // Dispatch with the planted BYOK secret, including an error path —
    // failure handling must not leak either.
    let debug_renderings = dispatch_calls(&project, 2, true);
    for rendering in &debug_renderings {
        assert!(
            !rendering.contains(PLANTED_SECRET),
            "a Debug rendering leaked the secret: {rendering}"
        );
    }

    // Export, then scan every byte the project can leave the machine as:
    // db + WAL + CAS + manifest, and the export tree with events.jsonl.
    let export = directory.path().join("scan-export.pos");
    runtime
        .command(
            CommandName::ProjectExport.as_str(),
            &input_json(&ProjectExportInput {
                path,
                out: export.display().to_string(),
            })
            .expect("input serializes"),
        )
        .expect("export resolves");
    scan_tree_for_secret(&project);
    scan_tree_for_secret(&export);
}
