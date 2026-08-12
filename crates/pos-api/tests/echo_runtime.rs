//! m0-s13 Echo composition over the real local runtime and loopback HTTP
//! transport: background worker, durable step feed, cost attribution, and
//! cancellation at a checkpoint boundary.

#![forbid(unsafe_code)]

use pos_api::{
    CommandName, CostRollupInput, EchoRuntimeOptions, LocalBootstrapConfig, ProjectCreateInput,
    ProjectExportInput, ProjectPathInput, QueryName, RunBudgetWire, RunControlInput, RunStartInput,
    RunStepFrame, RunStepsInput, RunWorker, StreamName, bootstrap_local_runtime, input_json,
};
use serde_json::Value;
use std::io::{Read, Write};
use std::net::{TcpListener, TcpStream};
use std::sync::mpsc::{Receiver, Sender};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::thread::JoinHandle;
use std::time::Duration;

struct EchoEndpoint {
    base_url: String,
    requested: Receiver<()>,
    release: Sender<()>,
    thread: JoinHandle<()>,
}

impl EchoEndpoint {
    fn start(block_response: bool) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture binds loopback");
        let address = listener.local_addr().expect("fixture has address");
        let (requested_tx, requested) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        if !block_response {
            release.send(()).expect("immediate fixture releases");
        }
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Echo worker connects");
            let marker = read_marker(&mut stream);
            requested_tx.send(()).expect("test waits for model request");
            release_rx.recv().expect("test releases model response");
            write_echo_response(&mut stream, &marker);
        });
        Self {
            base_url: format!("http://{address}"),
            requested,
            release,
            thread,
        }
    }

    fn wait_for_request(&self) {
        self.requested
            .recv_timeout(Duration::from_secs(10))
            .expect("Echo reaches its one model request");
    }

    fn release(&self) {
        self.release.send(()).expect("release model fixture");
    }

    fn finish(self) {
        self.thread.join().expect("model fixture exits cleanly");
    }
}

#[test]
fn echo_background_run_streams_three_steps_and_one_cost_row() {
    let endpoint = EchoEndpoint::start(false);
    let fixture = RuntimeFixture::new(&endpoint.base_url, "echo-success");
    let run_id = fixture.start_echo();
    endpoint.wait_for_request();

    let frames = fixture.wait_for_terminal_frames(&run_id);
    assert_eq!(frames.len(), 3);
    assert_eq!(
        frames
            .iter()
            .map(|frame| frame.stream_seq)
            .collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    assert!(frames.iter().all(|frame| frame.run_id == run_id));
    assert_eq!(
        frames.last().map(|frame| frame.run_status.as_str()),
        Some("done")
    );
    assert_eq!(
        frames
            .last()
            .and_then(|frame| frame.validation_status.as_deref()),
        Some("passed")
    );
    fixture.assert_one_echo_cost();
    endpoint.finish();
}

#[test]
fn cancel_during_the_model_effect_lands_after_its_checkpoint() {
    let endpoint = EchoEndpoint::start(true);
    let fixture = RuntimeFixture::new(&endpoint.base_url, "echo-cancel");
    let run_id = fixture.start_echo();
    endpoint.wait_for_request();

    let pending = fixture
        .runtime
        .command(
            CommandName::RunCancel.as_str(),
            &input_json(&RunControlInput {
                path: fixture.project_path.clone(),
                run_id: run_id.clone(),
                reason: "Cancel while the model response is in flight".to_owned(),
            })
            .expect("cancel input serializes"),
        )
        .expect("cancel request appends");
    assert!(pending.contains("\"pendingControl\":\"cancel\""));
    endpoint.release();

    let frames = fixture.wait_for_terminal_frames(&run_id);
    assert_eq!(frames.len(), 2, "cancel settles after the model boundary");
    let terminal = frames.last().expect("the model boundary streams");
    assert_eq!(terminal.run_status, "canceled");
    assert!(terminal.terminal);
    assert_eq!(terminal.validation_status.as_deref(), Some("passed"));
    fixture.assert_one_echo_cost();
    endpoint.finish();
}

#[test]
fn two_local_only_projects_export_and_reopen_without_cross_project_state() {
    let endpoint = CountingEchoEndpoint::start(2);
    let directory = tempfile::tempdir().expect("tempdir");
    let alpha = directory.path().join("alpha.pos").display().to_string();
    let bravo = directory.path().join("bravo.pos").display().to_string();
    let runtime = bootstrap_local_runtime(
        LocalBootstrapConfig::isolated(directory.path().join("packs")).with_echo(
            EchoRuntimeOptions::loopback(&endpoint.base_url, "echo-isolation-fixture"),
        ),
    );
    let alpha_project = create_project(&runtime, &alpha, "Alpha");
    let bravo_project = create_project(&runtime, &bravo, "Bravo");
    assert_ne!(alpha_project, bravo_project);
    let alpha_run = start_echo(&runtime, &alpha);
    let bravo_run = start_echo(&runtime, &bravo);
    assert_ne!(alpha_run, bravo_run);
    let alpha_frames = wait_for_terminal_frames(&runtime, &alpha, &alpha_run, 400);
    let bravo_frames = wait_for_terminal_frames(&runtime, &bravo, &bravo_run, 400);
    assert_eq!(alpha_frames.len(), 3);
    assert_eq!(bravo_frames.len(), 3);
    assert!(
        alpha_frames
            .iter()
            .all(|frame| frame.project_id.as_deref() == Some(alpha_project.as_str()))
    );
    assert!(
        bravo_frames
            .iter()
            .all(|frame| frame.project_id.as_deref() == Some(bravo_project.as_str()))
    );
    assert_eq!(endpoint.finish(), 2, "one loopback model call per project");

    for (path, own_project, other_project) in [
        (&alpha, &alpha_project, &bravo_project),
        (&bravo, &bravo_project, &alpha_project),
    ] {
        let cost = runtime
            .query_with_input(
                QueryName::CostRollup.as_str(),
                &input_json(&CostRollupInput {
                    path: Some(path.clone()),
                })
                .expect("cost input serializes"),
            )
            .expect("cost rollup reads one project");
        let value: Value = serde_json::from_str(&cost).expect("cost report parses");
        assert_eq!(value["projectCount"], 1);
        assert_eq!(value["totals"]["calls"], 1);
        assert_eq!(value["rows"].as_array().map(Vec::len), Some(1));
        assert_eq!(value["rows"][0]["projectId"], own_project.as_str());
        assert_eq!(value["rows"][0]["feature"], "echo");
        assert_eq!(value["rows"][0]["agent"], "echo");
        assert!(!cost.contains(other_project.as_str()));
    }

    // A remote address cannot even be mislabeled device-local. Validation
    // happens before RunStarted, so this is a zero-I/O, zero-state-change
    // cloud block on the same Echo composition.
    let head_before = inspect_head(&runtime, &alpha);
    let cloud_blocked = bootstrap_local_runtime(
        LocalBootstrapConfig::isolated(directory.path().join("cloud-blocked-packs")).with_echo(
            EchoRuntimeOptions::loopback("https://api.openai.com", "must-not-connect"),
        ),
    );
    let denied = cloud_blocked
        .command(CommandName::RunStart.as_str(), &echo_start_input(&alpha))
        .expect_err("local_only Echo must refuse a cloud endpoint before I/O");
    assert_eq!(denied.code, "policy_denied");
    assert_eq!(inspect_head(&runtime, &alpha), head_before);

    let alpha_export = directory.path().join("alpha-export.pos");
    let bravo_export = directory.path().join("bravo-export.pos");
    for (path, export) in [(&alpha, &alpha_export), (&bravo, &bravo_export)] {
        runtime
            .command(
                CommandName::ProjectExport.as_str(),
                &input_json(&ProjectExportInput {
                    path: path.clone(),
                    out: export.display().to_string(),
                })
                .expect("export input serializes"),
            )
            .expect("completed Echo project exports");
    }
    let alpha_text = std::fs::read_to_string(alpha_export.join("events.jsonl"))
        .expect("Alpha export carries events");
    let bravo_text = std::fs::read_to_string(bravo_export.join("events.jsonl"))
        .expect("Bravo export carries events");
    assert!(alpha_text.contains(&alpha_project));
    assert!(alpha_text.contains(&alpha_run));
    assert!(!alpha_text.contains(&bravo_project));
    assert!(!alpha_text.contains(&bravo_run));
    assert!(bravo_text.contains(&bravo_project));
    assert!(bravo_text.contains(&bravo_run));
    assert!(!bravo_text.contains(&alpha_project));
    assert!(!bravo_text.contains(&alpha_run));

    let reopened = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("reopen-packs"),
    ));
    for export in [&alpha_export, &bravo_export] {
        let path = export.display().to_string();
        reopened
            .command(
                CommandName::ProjectOpen.as_str(),
                &input_json(&ProjectPathInput { path: path.clone() })
                    .expect("open input serializes"),
            )
            .expect("export reopens independently");
        let verify = reopened
            .query_with_input(
                QueryName::ProjectVerify.as_str(),
                &input_json(&ProjectPathInput { path }).expect("verify input serializes"),
            )
            .expect("reopened export verifies");
        assert!(verify.contains("\"clean\":true"), "dirty export: {verify}");
    }
}

#[test]
#[ignore = "requires local Ollama at 127.0.0.1:11434 with gemma4:12b"]
fn identical_echo_path_passes_live_ollama_under_local_only() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project = directory
        .path()
        .join("ollama-echo.pos")
        .display()
        .to_string();
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        directory.path().join("packs"),
    ));
    let project_id = create_project(&runtime, &project, "Live Ollama Echo");
    let run_id = start_echo(&runtime, &project);
    let frames = wait_for_terminal_frames(&runtime, &project, &run_id, 2_400);
    assert_eq!(frames.len(), 3);
    assert!(frames.iter().all(|frame| {
        frame.run_id == run_id && frame.project_id.as_deref() == Some(project_id.as_str())
    }));
    assert!(
        frames.last().is_some_and(|frame| {
            frame.terminal
                && frame.run_status == "done"
                && frame.validation_status.as_deref() == Some("passed")
        }),
        "live Ollama frames: {frames:#?}"
    );

    let cost = runtime
        .query_with_input(
            QueryName::CostRollup.as_str(),
            &input_json(&CostRollupInput {
                path: Some(project.clone()),
            })
            .expect("cost input serializes"),
        )
        .expect("live Ollama cost reads");
    let cost: Value = serde_json::from_str(&cost).expect("cost report parses");
    assert_eq!(cost["totals"]["calls"], 1);
    assert_eq!(cost["rows"][0]["feature"], "echo");
    assert_eq!(cost["rows"][0]["agent"], "echo");
    assert_eq!(cost["rows"][0]["provider"], "openai-compatible");
    assert_eq!(cost["rows"][0]["credentialClass"], "device_session");
    assert_eq!(cost["rows"][0]["model"], "gemma4:12b");
    let verify = runtime
        .query_with_input(
            QueryName::ProjectVerify.as_str(),
            &input_json(&ProjectPathInput { path: project }).expect("verify input serializes"),
        )
        .expect("live Ollama project verifies");
    assert!(
        verify.contains("\"clean\":true"),
        "dirty live project: {verify}"
    );
}

struct CountingEchoEndpoint {
    base_url: String,
    connections: Arc<AtomicUsize>,
    thread: JoinHandle<()>,
}

impl CountingEchoEndpoint {
    fn start(expected: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("fixture binds loopback");
        let address = listener.local_addr().expect("fixture has address");
        let connections = Arc::new(AtomicUsize::new(0));
        let observed = Arc::clone(&connections);
        let thread = std::thread::spawn(move || {
            for _ in 0..expected {
                let (mut stream, _) = listener.accept().expect("Echo worker connects");
                let marker = read_marker(&mut stream);
                observed.fetch_add(1, Ordering::SeqCst);
                write_echo_response(&mut stream, &marker);
            }
        });
        Self {
            base_url: format!("http://{address}"),
            connections,
            thread,
        }
    }

    fn finish(self) -> usize {
        self.thread.join().expect("counting fixture exits");
        self.connections.load(Ordering::SeqCst)
    }
}

struct RuntimeFixture {
    _directory: tempfile::TempDir,
    runtime: pos_api::LocalRuntime,
    project_path: String,
}

impl RuntimeFixture {
    fn new(base_url: &str, name: &str) -> Self {
        let directory = tempfile::tempdir().expect("tempdir");
        let project_path = directory
            .path()
            .join(format!("{name}.pos"))
            .display()
            .to_string();
        let runtime = bootstrap_local_runtime(
            LocalBootstrapConfig::isolated(directory.path().join("packs"))
                .with_echo(EchoRuntimeOptions::loopback(base_url, "echo-fixture")),
        );
        runtime
            .command(
                CommandName::ProjectCreate.as_str(),
                &input_json(&ProjectCreateInput {
                    path: project_path.clone(),
                    name: Some(name.to_owned()),
                    template: "generic".to_owned(),
                })
                .expect("create input serializes"),
            )
            .expect("create Echo project");
        Self {
            _directory: directory,
            runtime,
            project_path,
        }
    }

    fn start_echo(&self) -> String {
        let started = self
            .runtime
            .command(
                CommandName::RunStart.as_str(),
                &input_json(&RunStartInput {
                    path: self.project_path.clone(),
                    worker: RunWorker::Echo,
                    autonomy_level: 2,
                    budget: echo_budget(),
                    tool_grants: Vec::new(),
                    parent_run_id: None,
                })
                .expect("start input serializes"),
            )
            .expect("start Echo Run");
        field(&started, "runId")
    }

    fn wait_for_terminal_frames(&self, run_id: &str) -> Vec<RunStepFrame> {
        let input = input_json(&RunStepsInput {
            path: self.project_path.clone(),
            run_id: run_id.to_owned(),
        })
        .expect("stream input serializes");
        let mut last = Vec::new();
        for _ in 0..400 {
            let frames = self
                .runtime
                .stream_subscribe(StreamName::RunSteps.as_str(), &input, None)
                .expect("durable Run frames read");
            let decoded = frames
                .into_iter()
                .map(|frame| {
                    serde_json::from_str::<RunStepFrame>(&frame.data_json)
                        .expect("generated RunStepFrame decodes")
                })
                .collect::<Vec<_>>();
            if decoded.last().is_some_and(|frame| frame.terminal) {
                return decoded;
            }
            last = decoded;
            std::thread::sleep(Duration::from_millis(25));
        }
        panic!(
            "Echo Run {run_id} did not reach a terminal frame within 10 seconds; last frames: {last:?}"
        );
    }

    fn assert_one_echo_cost(&self) {
        let report = self
            .runtime
            .query_with_input(
                QueryName::CostRollup.as_str(),
                &input_json(&CostRollupInput {
                    path: Some(self.project_path.clone()),
                })
                .expect("cost input serializes"),
            )
            .expect("cost rollup reads ledger truth");
        let value: Value = serde_json::from_str(&report).expect("cost report parses");
        assert_eq!(value["totals"]["calls"], 1);
        assert_eq!(value["rows"][0]["feature"], "echo");
        assert_eq!(value["rows"][0]["agent"], "echo");
        assert_eq!(value["rows"][0]["credentialClass"], "device_session");
        assert_eq!(value["rows"][0]["providerCostKind"], "customer_billed");
    }
}

fn create_project(runtime: &pos_api::LocalRuntime, path: &str, name: &str) -> String {
    let created = runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.to_owned(),
                name: Some(name.to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create isolated Echo project");
    field(&created, "projectId")
}

fn echo_start_input(path: &str) -> String {
    input_json(&RunStartInput {
        path: path.to_owned(),
        worker: RunWorker::Echo,
        autonomy_level: 2,
        budget: echo_budget(),
        tool_grants: Vec::new(),
        parent_run_id: None,
    })
    .expect("start input serializes")
}

fn start_echo(runtime: &pos_api::LocalRuntime, path: &str) -> String {
    let started = runtime
        .command(CommandName::RunStart.as_str(), &echo_start_input(path))
        .expect("start isolated Echo Run");
    field(&started, "runId")
}

fn wait_for_terminal_frames(
    runtime: &pos_api::LocalRuntime,
    path: &str,
    run_id: &str,
    attempts: u32,
) -> Vec<RunStepFrame> {
    let input = input_json(&RunStepsInput {
        path: path.to_owned(),
        run_id: run_id.to_owned(),
    })
    .expect("stream input serializes");
    for _ in 0..attempts {
        let frames = runtime
            .stream_subscribe(StreamName::RunSteps.as_str(), &input, None)
            .expect("durable Run frames read")
            .into_iter()
            .map(|frame| {
                serde_json::from_str::<RunStepFrame>(&frame.data_json)
                    .expect("generated RunStepFrame decodes")
            })
            .collect::<Vec<_>>();
        if frames.last().is_some_and(|frame| frame.terminal) {
            return frames;
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    panic!("Echo Run {run_id} did not terminate within the bounded wait")
}

fn inspect_head(runtime: &pos_api::LocalRuntime, path: &str) -> u64 {
    let report = runtime
        .query_with_input(
            QueryName::ProjectInspect.as_str(),
            &input_json(&ProjectPathInput {
                path: path.to_owned(),
            })
            .expect("inspect input serializes"),
        )
        .expect("inspect project");
    serde_json::from_str::<Value>(&report).expect("inspect report parses")["headSeq"]
        .as_u64()
        .expect("inspect report carries headSeq")
}

const fn echo_budget() -> RunBudgetWire {
    RunBudgetWire {
        tokens: 4_096,
        usd_micros: 0,
        wall_ms: 90_000,
        storage_bytes: 64 * 1_024,
        tool_calls: 3,
        retries: 0,
        steps: 3,
    }
}

fn field(body: &str, name: &str) -> String {
    serde_json::from_str::<Value>(body)
        .expect("report parses")
        .get(name)
        .and_then(Value::as_str)
        .unwrap_or_else(|| panic!("report has string field {name}"))
        .to_owned()
}

fn read_marker(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1_024];
    let header_end = loop {
        let read = stream.read(&mut chunk).expect("read model request");
        assert!(read > 0, "request closed before its headers");
        bytes.extend_from_slice(&chunk[..read]);
        assert!(bytes.len() <= 1024 * 1024, "fixture request exceeds 1 MiB");
        if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
            break index + 4;
        }
    };
    let headers = String::from_utf8_lossy(&bytes[..header_end]);
    let content_length = headers
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-length")
                .then(|| value.trim().parse::<usize>().ok())
                .flatten()
        })
        .expect("model request carries content-length");
    while bytes.len() - header_end < content_length {
        let read = stream.read(&mut chunk).expect("read model request body");
        assert!(read > 0, "request closed before its body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    let request: Value = serde_json::from_slice(&bytes[header_end..header_end + content_length])
        .expect("model request is JSON");
    request["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|message| message["content"].as_str())
        .expect("model request has user marker")
        .to_owned()
}

fn write_echo_response(stream: &mut TcpStream, marker: &str) {
    let delta = serde_json::json!({
        "choices": [{"delta": {"content": format!("ECHO: {marker}")}}]
    });
    let usage = serde_json::json!({
        "choices": [],
        "usage": {"prompt_tokens": 7, "completion_tokens": 3}
    });
    let body = format!("data: {delta}\n\ndata: {usage}\n\ndata: [DONE]\n\n");
    let response = format!(
        "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
        body.len()
    );
    stream
        .write_all(response.as_bytes())
        .expect("write model response");
}
