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
    ResponseHandler, RoutingTier, SecretRef, TransportError, Transports, VecSink,
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
                routing: ModelRouting::thinking_only(
                    byok_openai_choice(&secret_ref),
                    byok_openai_choice(&secret_ref),
                ),
            },
            vec![Box::new(pos_gateway::OpenAiAdapter {
                base_url: "https://api.openai.com".to_owned(),
            })],
            &secrets,
            &ledger,
            Transports::new(&transport, &transport),
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
            reasoning_effort: None,
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

/// m0-s15 AC 2, arithmetic half: the rollup's totals — and every attribution
/// group — equal an **independent** sum read straight from
/// `proj_model_calls`. Summing the report's own rows would only prove the
/// report is self-consistent; the point is that the answer matches the
/// projection the ledger actually wrote.
#[test]
fn cost_rollup_totals_and_groups_equal_the_sum_of_the_model_call_rows() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("packs"),
    ));
    let project = directory.path().join("cost-groups.pos");
    let path = project.display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Cost groups".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");
    dispatch_calls(&project, 5, true);

    let report: serde_json::Value = serde_json::from_str(
        &runtime
            .query_with_input(
                QueryName::CostRollup.as_str(),
                &input_json(&pos_api::CostRollupInput {
                    path: Some(path.clone()),
                })
                .expect("input serializes"),
            )
            .expect("rollup resolves"),
    )
    .expect("rollup is JSON");

    let truth = model_call_truth(&project);
    let totals = &report["totals"];
    assert_eq!(totals["calls"].as_u64(), Some(truth.calls));
    assert_eq!(totals["tokensIn"].as_u64(), Some(truth.tokens_in));
    assert_eq!(totals["tokensOut"].as_u64(), Some(truth.tokens_out));
    assert_eq!(totals["usdMicros"].as_u64(), Some(truth.usd_micros));
    assert_eq!(
        totals["projectosUsdMicros"].as_u64(),
        Some(truth.projectos_usd_micros)
    );

    // Every dimension re-derives the same grand total, so a grouping bug
    // cannot hide behind a correct sum at the top.
    let groups = report["groups"].as_array().expect("groups is an array");
    assert!(!groups.is_empty(), "the rollup answers with its groups");
    for dimension in ["project", "feature", "agent"] {
        let in_dimension: Vec<&serde_json::Value> = groups
            .iter()
            .filter(|group| group["dimension"] == dimension)
            .collect();
        assert!(
            !in_dimension.is_empty(),
            "{dimension} is one of the three attribution grains the story names"
        );
        let calls: u64 = in_dimension
            .iter()
            .map(|group| group["calls"].as_u64().unwrap_or_default())
            .sum();
        let tokens_in: u64 = in_dimension
            .iter()
            .map(|group| group["tokensIn"].as_u64().unwrap_or_default())
            .sum();
        assert_eq!(
            calls, truth.calls,
            "{dimension} groups lost or gained calls"
        );
        assert_eq!(tokens_in, truth.tokens_in, "{dimension} tokensIn drifted");
    }
}

#[derive(Default)]
struct LedgerTruth {
    calls: u64,
    tokens_in: u64,
    tokens_out: u64,
    usd_micros: u64,
    projectos_usd_micros: u64,
}

/// Reads the projection directly — the independent side of the oracle.
fn model_call_truth(project_root: &Path) -> LedgerTruth {
    let store = pos_store::ProjectStore::open(project_root).expect("project store opens");
    store
        .db()
        .with_reader("model call truth", |connection| {
            let mut statement = connection.prepare(
                "SELECT COUNT(*), COALESCE(SUM(tokens_in), 0), COALESCE(SUM(tokens_out), 0), \
                        COALESCE(SUM(usd_micros), 0), \
                        COALESCE(SUM(CASE WHEN provider_cost_kind <> 'customer_billed' \
                                          THEN usd_micros ELSE 0 END), 0) \
                 FROM proj_model_calls",
            )?;
            statement.query_row([], |row| {
                Ok(LedgerTruth {
                    calls: row.get::<_, i64>(0)?.max(0).unsigned_abs(),
                    tokens_in: row.get::<_, i64>(1)?.max(0).unsigned_abs(),
                    tokens_out: row.get::<_, i64>(2)?.max(0).unsigned_abs(),
                    usd_micros: row.get::<_, i64>(3)?.max(0).unsigned_abs(),
                    projectos_usd_micros: row.get::<_, i64>(4)?.max(0).unsigned_abs(),
                })
            })
        })
        .expect("the projection reads")
}

/// m0-s15 AC 2, content half: the secret scan now covers span output, which
/// is the surface m0-s10 explicitly deferred to this story. The scan is the
/// second line of defence — the first is that [`SpanValue`] has no `String`
/// variant, so neither the planted key nor the marker below is *representable*
/// in a span field.
#[test]
fn span_output_carries_no_key_material_or_project_content() {
    const CONTENT_MARKER: &str = "CUSTOMER-SAID-WE-WILL-CHURN-IN-Q3";
    let directory = tempfile::tempdir().expect("tempdir");
    let span_log = directory.path().join("spans.jsonl");
    pos_api::install_telemetry(Some(&format!("file:{}", span_log.display())))
        .expect("the JSON-lines sink installs");

    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("packs"),
    ));
    // The marker rides in every place a careless implementation might reach
    // for a name: the project directory name, the project's display name,
    // and an unregistered query name a caller could hammer.
    let project = directory.path().join(format!("{CONTENT_MARKER}.pos"));
    let path = project.display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some(format!("{CONTENT_MARKER} {PLANTED_SECRET}")),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");
    dispatch_calls(&project, 2, true);
    let _ = runtime.query_with_input(&format!("query.{CONTENT_MARKER}"), "{}");
    let _ = runtime.command(&format!("cmd.{PLANTED_SECRET}"), "{}");
    runtime
        .query_with_input(
            QueryName::CostRollup.as_str(),
            &input_json(&pos_api::CostRollupInput { path: Some(path) }).expect("input serializes"),
        )
        .expect("rollup resolves");

    let spans = std::fs::read_to_string(&span_log).expect("the span log exists");
    assert!(
        spans.lines().count() >= 4,
        "the scan needs real span output to be evidence, got: {spans}"
    );
    // Every emitted line is well-formed JSON with only ids, numbers, and
    // static labels — a truncated line would be corruption, so the sink drops
    // instead, and it must not have had to.
    for line in spans.lines() {
        let value: serde_json::Value =
            serde_json::from_str(line).expect("every span line is canonical JSON");
        assert!(value["name"].is_string());
        assert!(value["fields"].is_object());
    }
    for needle in [
        CONTENT_MARKER,
        PLANTED_SECRET,
        "cited answer",
        directory.path().to_string_lossy().as_ref(),
    ] {
        assert!(
            !spans.contains(needle),
            "span output leaked {needle:?}:\n{spans}"
        );
    }
    let stats = pos_api::telemetry::stats();
    assert_eq!(stats.lines_dropped, 0, "no span line was refused");
    assert_eq!(stats.fields_dropped, 0, "no span field exceeded its budget");
    // Leave the process as it was found, so a parallel test binary is not
    // silently writing into a temporary directory that is about to vanish.
    pos_api::install_telemetry(None).expect("telemetry returns to off");
}
