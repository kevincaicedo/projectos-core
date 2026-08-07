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
    CommandName, LocalBootstrapConfig, LocalRuntime, OPEN_PROJECT_COUNT_MAX, ProjectCreateInput,
    ProjectPathInput, ProjectSeedInput, QueryName, StreamName, bootstrap_local_runtime, input_json,
};
use std::path::PathBuf;

/// Input-free queries: dispatchable with `{}` from any transport. The two
/// registered-but-later entries answer with their typed envelope, which is
/// as much a part of the contract as a success body.
const INPUT_FREE_QUERIES: [QueryName; 5] = [
    QueryName::CapabilitySnapshot,
    QueryName::ProjectList,
    QueryName::JobList,
    QueryName::CostRollup,
    QueryName::Health,
];

/// Commands whose engine lands with the agent harness (m0-s12/m0-s13); until
/// then their contract is the typed `not_yet_supported` envelope.
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

fn stream_subscribe(runtime: &LocalRuntime, name: &str) -> Result<usize, String> {
    runtime
        .stream_subscribe(name, "{}", None)
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
        // Both success bodies and typed envelopes travel unchanged.
        match ipc {
            Ok(body) => assert!(body.starts_with('{'), "{name} returned a non-object"),
            Err(envelope) => assert!(
                envelope.contains("\"code\":\"not_yet_supported\""),
                "{name} failed with an unexpected envelope: {envelope}"
            ),
        }
    }
}

#[test]
fn run_lifecycle_commands_answer_with_the_typed_envelope_on_both_transports() {
    let runtime = runtime();
    for command in RUN_LIFECYCLE_COMMANDS {
        let name = command.as_str();
        let ipc = ipc_command(&runtime, name, "{}")
            .expect_err("the run engine has not landed; success would be a lie");
        let http = http_command(&runtime, name, "{}")
            .expect_err("the run engine has not landed; success would be a lie");
        assert_eq!(ipc, http, "{name} envelope differs between transports");
        assert!(ipc.contains("\"code\":\"not_yet_supported\""));
        assert!(ipc.contains("m0-s12"), "{name} must name its owning story");
        assert!(ipc.contains("\"retriable\":false"));
    }
}

#[test]
fn the_stream_surface_is_registered_and_typed() {
    let runtime = runtime();
    let subscribed = stream_subscribe(&runtime, StreamName::RunSteps.as_str())
        .expect_err("run.steps has no producer until m0-s13; success would be a lie");
    assert!(subscribed.contains("\"code\":\"not_yet_supported\""));
    assert!(subscribed.contains("m0-s13"));

    let unknown = stream_subscribe(&runtime, "run.st3ps")
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
    assert!(snapshot.contains("\"surfaceVersion\":4"));
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
