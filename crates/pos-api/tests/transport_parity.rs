//! Transport-parity contract suite (m0-s06 slice; walking-skeleton evidence for
//! `public-builds-alone`).
//!
//! The parity claim this suite defends: a shell transport selects an operation
//! by `(name, input)` and forwards bytes. It never decodes, reshapes, filters,
//! or re-serializes a result. So the contract is checkable in one process,
//! without a webview, a WebDriver, or a network — and it stays checkable on a
//! runner that has no cloud submodule and no account.
//!
//! Every `QueryName` and `CommandName` variant needs a row below. A new name
//! without one fails `the_contract_suite_covers_the_whole_surface`, which is
//! the structural version of remembering to test the new endpoint.

#![forbid(unsafe_code)]

use pos_api::{
    CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, ProjectPathInput,
    ProjectSeedInput, QueryName, bootstrap_local_runtime, input_json,
};
use std::path::PathBuf;

/// Every input-free query the v0 read surface exposes.
const INPUT_FREE_SURFACE: [QueryName; 1] = [QueryName::CapabilitySnapshot];

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

/// Stands in for the axum handler that lands with `pos-server` in m0-s08: same
/// registry, same bytes, a status code chosen from the envelope rather than
/// from the payload.
fn http_query(runtime: &LocalRuntime, name: &str, input: &str) -> (u16, String) {
    match runtime.query_with_input(name, input) {
        Ok(body) => (200, body),
        Err(error) => (404, error.to_json()),
    }
}

fn http_command(runtime: &LocalRuntime, name: &str, input: &str) -> (u16, String) {
    match runtime.command(name, input) {
        Ok(body) => (200, body),
        Err(error) => (404, error.to_json()),
    }
}

/// A real project in a tempdir plus the input rows that exercise every
/// project operation against it, in dependency order.
fn project_rows(directory: &tempfile::TempDir) -> Vec<(&'static str, String, bool)> {
    let project = directory.path().join("parity.pos");
    let export = directory.path().join("parity-export.pos");
    let path = project.display().to_string();
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
    ]
}

#[test]
fn the_contract_suite_covers_the_whole_surface() {
    let directory = tempfile::tempdir().expect("tempdir");
    let rows = project_rows(&directory);
    for query in QueryName::ALL {
        let covered = INPUT_FREE_SURFACE.contains(&query)
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
        assert!(
            rows.iter()
                .any(|(name, _, is_command)| *is_command && *name == command.as_str()),
            "{} has no contract row; add it before merging the command",
            command.as_str()
        );
    }
    assert_eq!(QueryName::ALL.len(), QueryName::COUNT);
    assert_eq!(CommandName::ALL.len(), CommandName::COUNT);
}

#[test]
fn both_transports_return_byte_identical_results() {
    let runtime = runtime();
    for query in INPUT_FREE_SURFACE {
        let name = query.as_str();
        let ipc = ipc_query(&runtime, name, "{}").expect("the registered query resolves over IPC");
        let (status, http) = http_query(&runtime, name, "{}");
        assert_eq!(status, 200);
        assert_eq!(
            ipc, http,
            "{name} differs between transports; a transport reshaped a result"
        );
        assert_eq!(
            ipc,
            runtime.query(name).expect("the registered query resolves"),
            "{name} differs from the in-process registry result"
        );
    }
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
        let (status, http) = http_query(&runtime, name, &read_input);
        assert_eq!(status, 200, "{name} failed over HTTP: {http}");
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
    let (status, http_export) = http_command(
        &runtime,
        CommandName::ProjectExport.as_str(),
        &export_via("export-http.pos"),
    );
    assert_eq!(status, 200, "export failed over HTTP: {http_export}");
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
    let (status, http) = http_query(&runtime, unknown, "{}");
    assert_eq!(status, 404);
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
    assert!(snapshot.contains("\"surfaceVersion\":2"));
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
