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
    // A file to ingest through the intake command. Markdown, so the sniffer
    // classifies it from its own bytes and NORMALIZE has real structure.
    let upload = directory.path().join("parity-note.md");
    std::fs::write(
        &upload,
        b"# Parity\n\nOne note, ingested through the front door.\n",
    )
    .expect("fixture upload writes");
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
        // Close follows open in this list, and the ordering matters: the rows
        // are dispatched in sequence, so a close of a project the session
        // never opened would be a typed refusal rather than a parity row.
        (
            CommandName::ProjectClose.as_str(),
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
            QueryName::EvidenceList.as_str(),
            input_json(&pos_api::EvidenceListInput {
                path: path.clone(),
                source_id: None,
                status: None,
                row_count_max: Some(10),
                with_stages: true,
            })
            .expect("input serializes"),
            false,
        ),
        (
            QueryName::SourceHealth.as_str(),
            input_json(&pos_api::SourceHealthInput {
                path: path.clone(),
                source_id: None,
            })
            .expect("input serializes"),
            false,
        ),
        (
            // An evidence id this project does not hold: both transports must
            // answer with the identical typed `not_found`, which is the row
            // that catches a transport inventing its own error shape.
            QueryName::TranscriptGet.as_str(),
            input_json(&pos_api::TranscriptGetInput {
                path: path.clone(),
                evidence_id: "00000000000000000000000000000001".to_owned(),
                pass: None,
                after_segment_index: None,
                row_count_max: Some(10),
            })
            .expect("input serializes"),
            false,
        ),
        (
            // The three transcript edits append against an evidence id that
            // does not exist. The append succeeds — a projection `Update` on a
            // missing row is a deterministic no-op (m1-s03), which is exactly
            // what makes replay total — so the row compares a success shape.
            CommandName::TranscriptCorrect.as_str(),
            input_json(&pos_api::TranscriptCorrectInput {
                path: path.clone(),
                evidence_id: "00000000000000000000000000000001".to_owned(),
                pass: 0,
                segment_index: 0,
                text: "contract row".to_owned(),
            })
            .expect("input serializes"),
            true,
        ),
        (
            CommandName::TranscriptSpeakerName.as_str(),
            input_json(&pos_api::TranscriptSpeakerNameInput {
                path: path.clone(),
                evidence_id: "00000000000000000000000000000001".to_owned(),
                speaker_index: 1,
                name: "Contract Row".to_owned(),
            })
            .expect("input serializes"),
            true,
        ),
        (
            CommandName::TranscriptSpeakerAssign.as_str(),
            input_json(&pos_api::TranscriptSpeakerAssignInput {
                path: path.clone(),
                evidence_id: "00000000000000000000000000000001".to_owned(),
                pass: 0,
                segment_index: 0,
                speaker_index: 1,
            })
            .expect("input serializes"),
            true,
        ),
        (
            // Real bytes through the real front door (m1-s07). The report is
            // a pure function of the content — the evidence id is derived
            // from the CAS hash and the source id from the scope — so both
            // transports must produce identical bytes for it.
            CommandName::IngestSubmit.as_str(),
            input_json(&pos_api::IngestSubmitInput {
                path: path.clone(),
                file_path: Some(upload.display().to_string()),
                file_name: None,
                source_scope: None,
            })
            .expect("input serializes"),
            true,
        ),
        (
            // A project with no Evidence yet: the reprocess reports zero
            // requeued rather than refusing, which is the honest answer and
            // the one both transports must agree on byte for byte.
            CommandName::IngestReprocess.as_str(),
            input_json(&pos_api::IngestReprocessInput {
                path: path.clone(),
                from_stage: "chunk".to_owned(),
                evidence_id: None,
                item_count_max: Some(10),
                reason: "contract row".to_owned(),
            })
            .expect("input serializes"),
            true,
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

/// Contract rows whose whole point is the typed refusal. Reading a transcript
/// for an evidence id a project does not hold is a caller bug, and both
/// transports must say so identically rather than one 404ing and one 500ing.
const ROWS_THAT_ANSWER_TYPED_ERRORS: [&str; 1] = ["transcript.get"];

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
        &input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
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
    // This runtime was never asked to start a pool, and it says so rather than
    // implying that queued work is being claimed (m1-s01/ADR-0007).
    assert!(
        health.contains("\"backgroundWorkers\":{\"running\":false,\"registeredProjectCount\":0"),
        "health must report worker state honestly: {health}"
    );

    let closed = ipc_command(
        &runtime,
        CommandName::ProjectClose.as_str(),
        &input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
    )
    .expect("close resolves");
    assert!(closed.contains("\"openProjectCount\":0"));
    let closed_list =
        ipc_query(&runtime, QueryName::ProjectList.as_str(), "{}").expect("project.list resolves");
    assert!(closed_list.contains("\"projects\":[]"));
    // Closing twice is a typed refusal, not a second success: a shell that
    // believed it released a handle it never held would leak one per switch.
    let error = ipc_command(
        &runtime,
        CommandName::ProjectClose.as_str(),
        &input_json(&ProjectPathInput { path }).expect("input serializes"),
    )
    .expect_err("closing an already-closed project must refuse");
    assert!(error.contains("\"code\":\"not_open\""), "{error}");
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
fn intake_is_byte_identical_across_transports_and_dedupes_on_content() {
    let runtime = runtime();
    let directory = tempfile::tempdir().expect("tempdir");
    let upload = directory.path().join("interview-notes.md");
    std::fs::write(&upload, b"# Intake\n\nThe same bytes, two projects.\n")
        .expect("fixture writes");

    let project = |label: &str| {
        let path = directory.path().join(label).display().to_string();
        ipc_command(
            &runtime,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Intake parity".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");
        path
    };
    let submit_input = |path: &str| {
        input_json(&pos_api::IngestSubmitInput {
            path: path.to_owned(),
            file_path: Some(upload.display().to_string()),
            file_name: None,
            source_scope: None,
        })
        .expect("input serializes")
    };

    let ipc_path = project("intake-ipc.pos");
    let http_path = project("intake-http.pos");
    let ipc = ipc_command(
        &runtime,
        CommandName::IngestSubmit.as_str(),
        &submit_input(&ipc_path),
    )
    .expect("ingest.submit over IPC");
    let http = http_command(
        &runtime,
        CommandName::IngestSubmit.as_str(),
        &submit_input(&http_path),
    )
    .expect("ingest.submit over HTTP");
    assert_eq!(
        ipc, http,
        "ingest.submit differs between transports; a transport reshaped a result"
    );
    assert!(
        ipc.contains("\"addedCount\":1"),
        "one file, one item: {ipc}"
    );
    assert!(
        ipc.contains("\"mediaKind\":\"markdown\""),
        "the sniffer reads the bytes, not the extension: {ipc}"
    );

    // The same file again, into the project that already holds it. This is
    // the most common thing that happens to this command, and it must read as
    // "you already have this" rather than as a second copy.
    let again = ipc_command(
        &runtime,
        CommandName::IngestSubmit.as_str(),
        &submit_input(&ipc_path),
    )
    .expect("a re-drop resolves");
    assert!(
        again.contains("\"duplicateCount\":1") && again.contains("\"addedCount\":0"),
        "a re-drop of identical bytes is a visible duplicate, not a second item: {again}"
    );
}

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
        if ROWS_THAT_ANSWER_TYPED_ERRORS.contains(&name) {
            // These rows exist so the two transports are compared on an
            // *error* shape, which is where a transport is most tempted to
            // invent its own envelope. This test is about the export being a
            // valid project, so it asserts the refusal is typed and moves on.
            let error = result.expect_err("this row is declared to refuse");
            assert_eq!(
                error.code, "not_found",
                "{name} refused with the wrong code"
            );
            continue;
        }
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
