//! Transport-parity contract suite (m0-s06; walking-skeleton evidence for
//! `public-builds-alone`).
//!
//! The parity claim this suite defends: a shell transport selects an operation
//! by `(name, input)` and forwards bytes. It never decodes, reshapes, filters,
//! or re-serializes a result. So the contract is checkable in one process,
//! without a webview, a WebDriver, or a network — and it stays checkable on a
//! runner that has no cloud submodule and no account.
//!
//! Every `QueryName`, `CommandName`, and `StreamName` variant needs a row
//! below. A new name without one fails
//! `the_contract_suite_covers_the_whole_surface`, which is the structural
//! version of remembering to test the new endpoint. The real axum transport
//! is contract-tested against this same registry in
//! `bins/pos-server/tests/http_contract.rs`.

#![forbid(unsafe_code)]

use pos_api::{
    API_SURFACE_VERSION, CommandName, LocalBootstrapConfig, LocalRuntime, OPEN_PROJECT_COUNT_MAX,
    ProjectCreateInput, ProjectPathInput, ProjectSeedInput, QueryName, RunBudgetWire,
    RunControlInput, RunResumeInput, RunStartInput, RunStepsInput, RunWorker, StreamName,
    bootstrap_local_runtime, input_json,
};
use std::path::PathBuf;

/// Input-free queries: dispatchable with `{}` from any transport. The two
/// registered-but-later entries answer with their typed envelope, which is
/// as much a part of the contract as a success body.
const INPUT_FREE_QUERIES: [QueryName; 4] = [
    QueryName::CapabilitySnapshot,
    QueryName::ProjectList,
    QueryName::CostRollup,
    QueryName::Health,
];

/// Commands owned by the agent harness. The coverage test keeps all four
/// names coupled to the lifecycle contract below.
const RUN_LIFECYCLE_COMMANDS: [CommandName; 4] = [
    CommandName::RunStart,
    CommandName::RunCancel,
    CommandName::RunPause,
    CommandName::RunResume,
];

fn runtime() -> LocalRuntime {
    bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
        "path-that-does-not-exist-in-the-test-checkout",
    )))
}

/// Stands in for the Tauri IPC command in `apps/desktop`: resolve a name, hand
/// back the bytes, serialize a failure through the shared envelope.
fn ipc_query(runtime: &LocalRuntime, name: &str, input: &str) -> Result<String, String> {
    runtime
        .query_with_input(name, input)
        .map_err(|error| error.to_json())
}

fn ipc_command(runtime: &LocalRuntime, name: &str, input: &str) -> Result<String, String> {
    runtime
        .command(name, input)
        .map_err(|error| error.to_json())
}

/// Stands in for the axum handlers: same registry, same bytes. The status
/// code the real transport chooses from the envelope is covered by the
/// `pos-server` contract suite; the bytes are covered here.
fn http_query(runtime: &LocalRuntime, name: &str, input: &str) -> Result<String, String> {
    runtime
        .query_with_input(name, input)
        .map_err(|error| error.to_json())
}

fn http_command(runtime: &LocalRuntime, name: &str, input: &str) -> Result<String, String> {
    runtime
        .command(name, input)
        .map_err(|error| error.to_json())
}

fn stream_subscribe(runtime: &LocalRuntime, name: &str, input: &str) -> Result<usize, String> {
    runtime
        .stream_subscribe(name, input, None)
        .map(|frames| frames.len())
        .map_err(|error| error.to_json())
}

/// A real project in a tempdir plus the input rows that exercise every
/// project operation against it, in dependency order.
fn project_rows(directory: &tempfile::TempDir) -> Vec<(&'static str, String, bool)> {
    let project = directory.path().join("parity.pos");
    let export = directory.path().join("parity-export.pos");
    let path = project.display().to_string();
    // A complete models.pull fixture: manifest + artifact, file:// sourced,
    // so the command row succeeds with zero network and full verification.
    let artifact = directory.path().join("tiny.gguf");
    std::fs::write(&artifact, b"tiny-model-bytes").expect("fixture artifact writes");
    let manifest_path = directory.path().join("manifest.json");
    std::fs::write(
        &manifest_path,
        format!(
            r#"{{"models":[{{"name":"tiny.gguf","url":"file://{}","blake3":"{}","bytes":16}}]}}"#,
            artifact.display(),
            blake3::hash(b"tiny-model-bytes").to_hex()
        ),
    )
    .expect("fixture manifest writes");
    vec![
        (
            CommandName::ProjectCreate.as_str(),
            input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Parity".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
            true,
        ),
        (
            CommandName::ProjectSeedSynthetic.as_str(),
            input_json(&ProjectSeedInput {
                path: path.clone(),
                event_count: 64,
                seed: 11,
            })
            .expect("input serializes"),
            true,
        ),
        (
            CommandName::ProjectOpen.as_str(),
            input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
            true,
        ),
        (
            QueryName::ProjectInspect.as_str(),
            input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
            false,
        ),
        (
            QueryName::ProjectVerify.as_str(),
            input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
            false,
        ),
        (
            QueryName::JobList.as_str(),
            input_json(&pos_api::JobListInput {
                path: path.clone(),
                state: None,
                row_count_max: Some(10),
            })
            .expect("input serializes"),
            false,
        ),
        (
            // A fixed origin instant: the preview is a pure function of
            // (expression, zone, origin), so the parity row is stable without
            // freezing the process clock.
            QueryName::CronPreview.as_str(),
            input_json(&pos_api::CronPreviewInput {
                expr: "*/15 9-17 * * 1-5".to_owned(),
                tz: "Europe/Berlin".to_owned(),
                after_ts_ms: Some(1_772_946_000_000),
                count: Some(10),
            })
            .expect("input serializes"),
            false,
        ),
        (
            CommandName::ProjectExport.as_str(),
            input_json(&pos_api::ProjectExportInput {
                path,
                out: export.display().to_string(),
            })
            .expect("input serializes"),
            true,
        ),
        (
            CommandName::ModelsPull.as_str(),
            input_json(&pos_api::ModelsPullInput {
                manifest_path: manifest_path.display().to_string(),
                name: "tiny.gguf".to_owned(),
                dest_dir: directory.path().join("models").display().to_string(),
                consent: true,
            })
            .expect("input serializes"),
            true,
        ),
    ]
}

#[test]
fn the_contract_suite_covers_the_whole_surface() {
    let directory = tempfile::tempdir().expect("tempdir");
    let rows = project_rows(&directory);
    for query in QueryName::ALL {
        let covered = INPUT_FREE_QUERIES.contains(&query)
            || rows
                .iter()
                .any(|(name, _, is_command)| !is_command && *name == query.as_str());
        assert!(
            covered,
            "{} has no contract row; add it before merging the query",
            query.as_str()
        );
    }
    for command in CommandName::ALL {
        let covered = RUN_LIFECYCLE_COMMANDS.contains(&command)
            || rows
                .iter()
                .any(|(name, _, is_command)| *is_command && *name == command.as_str());
        assert!(
            covered,
            "{} has no contract row; add it before merging the command",
            command.as_str()
        );
    }
    for stream in StreamName::ALL {
        // Today every stream resolves through the one subscribe row below;
        // a stream with real items must extend this match deliberately.
        assert_eq!(
            stream,
            StreamName::RunSteps,
            "{} has no contract row; add it before merging the stream",
            stream.as_str()
        );
    }
    assert_eq!(QueryName::ALL.len(), QueryName::COUNT);
    assert_eq!(CommandName::ALL.len(), CommandName::COUNT);
    assert_eq!(StreamName::ALL.len(), StreamName::COUNT);
}

#[test]
fn input_free_queries_are_byte_identical_across_transports() {
    let runtime = runtime();
    for query in INPUT_FREE_QUERIES {
        let name = query.as_str();
        let ipc = ipc_query(&runtime, name, "{}");
        let http = http_query(&runtime, name, "{}");
        assert_eq!(
            ipc, http,
            "{name} differs between transports; a transport reshaped a result"
        );
        // Every input-free query answers with a real body since m0-s14;
        // a typed envelope here would mean a registered surface regressed.
        let body = ipc.expect("an input-free query must resolve");
        assert!(body.starts_with('{'), "{name} returned a non-object");
    }
}

#[test]
fn run_lifecycle_commands_are_byte_identical_across_transports() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    let ipc_path = directory.path().join("run-ipc.pos").display().to_string();
    let http_path = directory.path().join("run-http.pos").display().to_string();
    for path in [&ipc_path, &http_path] {
        ipc_command(
            &runtime,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Run parity".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create project");
    }

    let budget = RunBudgetWire {
        tokens: 100,
        usd_micros: 100,
        wall_ms: 10_000,
        storage_bytes: 1_024,
        tool_calls: 4,
        retries: 2,
        steps: 4,
    };
    let start_input = |path: &str| {
        input_json(&RunStartInput {
            path: path.to_owned(),
            worker: RunWorker::Navigator,
            autonomy_level: 2,
            budget,
            tool_grants: Vec::new(),
            parent_run_id: None,
        })
        .expect("start input serializes")
    };
    let ipc_start = ipc_command(
        &runtime,
        CommandName::RunStart.as_str(),
        &start_input(&ipc_path),
    )
    .expect("run.start over IPC");
    let http_start = http_command(
        &runtime,
        CommandName::RunStart.as_str(),
        &start_input(&http_path),
    )
    .expect("run.start over HTTP");
    let ipc_run_id = run_field(&ipc_start, "runId");
    let http_run_id = run_field(&http_start, "runId");
    assert_run_bytes_equal(&ipc_start, &http_start);
    assert_eq!(run_field(&ipc_start, "status"), "preflight");

    let control_input = |path: &str, run_id: &str, reason: &str| {
        input_json(&RunControlInput {
            path: path.to_owned(),
            run_id: run_id.to_owned(),
            reason: reason.to_owned(),
        })
        .expect("control input serializes")
    };
    let ipc_pause = ipc_command(
        &runtime,
        CommandName::RunPause.as_str(),
        &control_input(&ipc_path, &ipc_run_id, "Parity pause"),
    )
    .expect("run.pause over IPC");
    let http_pause = http_command(
        &runtime,
        CommandName::RunPause.as_str(),
        &control_input(&http_path, &http_run_id, "Parity pause"),
    )
    .expect("run.pause over HTTP");
    assert_run_bytes_equal(&ipc_pause, &http_pause);
    assert_eq!(run_field(&ipc_pause, "status"), "paused");
    assert!(ipc_pause.contains("\"pause\":{\"kind\":\"requested\""));
    assert!(ipc_pause.contains("\"reason\":\"Parity pause\""));

    let resume_input = |path: &str, run_id: &str| {
        input_json(&RunResumeInput {
            path: path.to_owned(),
            run_id: run_id.to_owned(),
        })
        .expect("resume input serializes")
    };
    let ipc_resume = ipc_command(
        &runtime,
        CommandName::RunResume.as_str(),
        &resume_input(&ipc_path, &ipc_run_id),
    )
    .expect("run.resume over IPC");
    let http_resume = http_command(
        &runtime,
        CommandName::RunResume.as_str(),
        &resume_input(&http_path, &http_run_id),
    )
    .expect("run.resume over HTTP");
    assert_run_bytes_equal(&ipc_resume, &http_resume);
    assert_eq!(run_field(&ipc_resume, "status"), "running");

    let ipc_cancel = ipc_command(
        &runtime,
        CommandName::RunCancel.as_str(),
        &control_input(&ipc_path, &ipc_run_id, "Parity cancel"),
    )
    .expect("run.cancel over IPC");
    let http_cancel = http_command(
        &runtime,
        CommandName::RunCancel.as_str(),
        &control_input(&http_path, &http_run_id, "Parity cancel"),
    )
    .expect("run.cancel over HTTP");
    assert_run_bytes_equal(&ipc_cancel, &http_cancel);
    assert_eq!(run_field(&ipc_cancel, "status"), "canceled");
}

fn run_field(body: &str, field: &str) -> String {
    serde_json::from_str::<serde_json::Value>(body)
        .expect("Run report is JSON")
        .get(field)
        .and_then(serde_json::Value::as_str)
        .unwrap_or_else(|| panic!("Run report has string field {field}"))
        .to_owned()
}

fn normalize_run_bytes(body: &str) -> String {
    let path = run_field(body, "path");
    let run_id = run_field(body, "runId");
    let project_id = run_field(body, "projectId");
    body.replace(&path, "<path>")
        .replace(&run_id, "<runId>")
        .replace(&project_id, "<projectId>")
}

fn assert_run_bytes_equal(ipc: &str, http: &str) {
    assert_eq!(
        normalize_run_bytes(ipc),
        normalize_run_bytes(http),
        "Run lifecycle bytes differ beyond caller/project-minted identity"
    );
}

#[test]
fn the_stream_surface_is_registered_and_typed() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory.path().join("stream.pos").display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Stream".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create project");
    let started = runtime
        .command(
            CommandName::RunStart.as_str(),
            &input_json(&RunStartInput {
                path: path.clone(),
                worker: RunWorker::Navigator,
                autonomy_level: 2,
                budget: RunBudgetWire {
                    tokens: 1,
                    usd_micros: 0,
                    wall_ms: 1,
                    storage_bytes: 0,
                    tool_calls: 0,
                    retries: 0,
                    steps: 0,
                },
                tool_grants: Vec::new(),
                parent_run_id: None,
            })
            .expect("start input serializes"),
        )
        .expect("start Run");
    let input = input_json(&RunStepsInput {
        path,
        run_id: run_field(&started, "runId"),
    })
    .expect("stream input serializes");
    let subscribed = stream_subscribe(&runtime, StreamName::RunSteps.as_str(), &input)
        .expect("run.steps reads the durable projection");
    assert_eq!(subscribed, 0);

    let unknown = stream_subscribe(&runtime, "run.st3ps", "{}")
        .expect_err("an unregistered stream must not resolve");
    assert!(unknown.contains("\"code\":\"unknown_stream\""));
}

/// The session surface is real state, not a stub: opening a project changes
/// `project.list` and `health` on every transport identically.
#[test]
fn the_session_surface_reports_real_open_state() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    let project = directory.path().join("session.pos");
    let path = project.display().to_string();

    let empty_list =
        ipc_query(&runtime, QueryName::ProjectList.as_str(), "{}").expect("project.list resolves");
    assert!(empty_list.contains("\"projects\":[]"));
    assert!(empty_list.contains(&format!("\"openProjectCountMax\":{OPEN_PROJECT_COUNT_MAX}")));
    let health = ipc_query(&runtime, QueryName::Health.as_str(), "{}").expect("health resolves");
    assert!(health.contains("\"status\":\"ok\""));
    assert!(health.contains("\"openProjectCount\":0"));

    ipc_command(
        &runtime,
        CommandName::ProjectCreate.as_str(),
        &input_json(&ProjectCreateInput {
            path: path.clone(),
            name: Some("Session".to_owned()),
            template: "generic".to_owned(),
        })
        .expect("input serializes"),
    )
    .expect("create resolves");
    let opened = ipc_command(
        &runtime,
        CommandName::ProjectOpen.as_str(),
        &input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
    )
    .expect("open resolves");
    assert!(opened.contains("\"name\":\"Session\""));

    // Reopening is idempotent; the list has exactly one row either way.
    ipc_command(
        &runtime,
        CommandName::ProjectOpen.as_str(),
        &input_json(&ProjectPathInput { path }).expect("input serializes"),
    )
    .expect("reopen resolves");
    let ipc_list =
        ipc_query(&runtime, QueryName::ProjectList.as_str(), "{}").expect("project.list resolves");
    let http_list =
        http_query(&runtime, QueryName::ProjectList.as_str(), "{}").expect("project.list resolves");
    assert_eq!(ipc_list, http_list);
    assert_eq!(ipc_list.matches("\"projectId\"").count(), 1);
    let health = ipc_query(&runtime, QueryName::Health.as_str(), "{}").expect("health resolves");
    assert!(health.contains("\"openProjectCount\":1"));
}

/// The project operations against ONE project: every read dispatched through
/// both simulated transports must return byte-identical bytes with zero
/// normalization, and command results over identical state must differ only
/// by the caller-chosen output path. (Two separate projects cannot be
/// compared byte-for-byte — `create` mints a random project id.)
#[test]
fn project_operations_are_byte_identical_across_transports() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    let project = directory.path().join("parity.pos");
    let path = project.display().to_string();

    // Commands flow through the IPC-shaped entry; their effects are then
    // read back through BOTH entries and byte-compared.
    let create_input = input_json(&ProjectCreateInput {
        path: path.clone(),
        name: Some("Parity".to_owned()),
        template: "generic".to_owned(),
    })
    .expect("input serializes");
    ipc_command(&runtime, CommandName::ProjectCreate.as_str(), &create_input)
        .expect("create resolves");
    let seed_input = input_json(&ProjectSeedInput {
        path: path.clone(),
        event_count: 64,
        seed: 11,
    })
    .expect("input serializes");
    ipc_command(
        &runtime,
        CommandName::ProjectSeedSynthetic.as_str(),
        &seed_input,
    )
    .expect("seed resolves");

    let read_input =
        input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes");
    for query in [QueryName::ProjectInspect, QueryName::ProjectVerify] {
        let name = query.as_str();
        let ipc = ipc_query(&runtime, name, &read_input)
            .unwrap_or_else(|error| panic!("{name} failed over IPC: {error}"));
        let http = http_query(&runtime, name, &read_input)
            .unwrap_or_else(|error| panic!("{name} failed over HTTP: {error}"));
        assert_eq!(
            ipc, http,
            "{name} differs between transports; a transport reshaped a result"
        );
    }

    // The same export through both entries (distinct destinations, same
    // state): the runtime-chosen bytes must agree once the caller-chosen
    // destination token is normalized.
    let export_via = |label: &str| {
        input_json(&pos_api::ProjectExportInput {
            path: path.clone(),
            out: directory.path().join(label).display().to_string(),
        })
        .expect("input serializes")
    };
    let ipc_export = ipc_command(
        &runtime,
        CommandName::ProjectExport.as_str(),
        &export_via("export-ipc.pos"),
    )
    .expect("export resolves over IPC");
    let http_export = http_command(
        &runtime,
        CommandName::ProjectExport.as_str(),
        &export_via("export-http.pos"),
    )
    .expect("export resolves over HTTP");
    assert_eq!(
        ipc_export.replace("export-ipc.pos", "<out>"),
        http_export.replace("export-http.pos", "<out>"),
        "export differs between transports beyond the caller-chosen destination"
    );
}

#[test]
fn both_transports_return_the_identical_error_envelope() {
    let runtime = runtime();
    let unknown = "capability.snapshot/../secrets";
    let ipc =
        ipc_query(&runtime, unknown, "{}").expect_err("an unregistered name must not resolve");
    let http =
        http_query(&runtime, unknown, "{}").expect_err("an unregistered name must not resolve");
    assert_eq!(ipc, http);
    assert!(ipc.contains("\"code\":\"unknown_query\""));
    // The rejected name is echoed as escaped JSON data, never as a path.
    assert!(ipc.contains("capability.snapshot/../secrets"));

    let unknown_command = ipc_command(&runtime, "project.dr0p", "{}")
        .expect_err("an unregistered command must not resolve");
    assert!(unknown_command.contains("\"code\":\"unknown_command\""));

    let malformed = ipc_command(&runtime, "project.create", "{\"path\":42}")
        .expect_err("malformed input must be a typed error");
    assert!(malformed.contains("\"code\":\"invalid_input\""));
}

#[test]
fn the_snapshot_is_stable_across_repeated_reads() {
    let runtime = runtime();
    let name = QueryName::CapabilitySnapshot.as_str();
    let first = runtime.query(name).expect("the registered query resolves");
    let second = runtime.query(name).expect("the registered query resolves");
    assert_eq!(
        first, second,
        "a read surface that changes without a state change cannot be diffed in review"
    );
}

#[test]
fn the_snapshot_carries_live_state_rather_than_a_compile_time_claim() {
    let snapshot = runtime()
        .query(QueryName::CapabilitySnapshot.as_str())
        .expect("the registered query resolves");
    // The bounded connector-host tick actually executed against the provider.
    assert!(snapshot.contains("\"connectorHost\":{\"hostAvailable\":true"));
    // Sockets the isolated bootstrap cannot offer report a non-empty reason
    // instead of quietly reporting themselves as local.
    assert!(snapshot.contains("\"mode\":\"unavailable\",\"reason\":\""));
    assert!(!snapshot.contains("\"reason\":\"\""));
    assert!(snapshot.contains(&format!("\"surfaceVersion\":{API_SURFACE_VERSION}")));
}

#[test]
fn no_cloud_provider_can_appear_in_a_public_build_snapshot() {
    let snapshot = runtime()
        .query(QueryName::CapabilitySnapshot.as_str())
        .expect("the registered query resolves");
    // `public-builds-alone` in one assertion: the public runtime resolves to
    // local defaults, so no hosted provider can be announced without the cloud
    // repository that this build proves it does not have.
    assert!(!snapshot.contains("\"mode\":\"hosted\""));
    for local_default in [
        "LocalControlPlane",
        "KeychainBroker",
        "DirectSync",
        "LocalBus",
        "LocalPool",
        "FilePackSource",
        "LocalRenderer",
        "NoopMeter",
        "LocalIngress",
        "LocalConnectorHost",
    ] {
        assert!(
            snapshot.contains(local_default),
            "{local_default} is missing from the public-build snapshot"
        );
    }
}

/// The exported directory is itself a valid project: re-inspectable and
/// verify-clean (F2/F45 — no lock-in from the first commit).
#[test]
fn an_export_reopens_and_verifies_clean() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    for (name, input, is_command) in project_rows(&directory) {
        let result = if is_command {
            runtime.command(name, &input)
        } else {
            runtime.query_with_input(name, &input)
        };
        result.unwrap_or_else(|error| panic!("{name} failed: {error}"));
    }
    let export_path = directory.path().join("parity-export.pos");
    let verify = runtime
        .query_with_input(
            QueryName::ProjectVerify.as_str(),
            &input_json(&ProjectPathInput {
                path: export_path.display().to_string(),
            })
            .expect("input serializes"),
        )
        .expect("the export re-opens");
    assert!(
        verify.contains("\"clean\":true"),
        "export failed verification: {verify}"
    );
}
