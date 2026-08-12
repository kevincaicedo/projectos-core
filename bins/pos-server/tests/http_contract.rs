//! The m0-s06 contract suite over the REAL axum transport: every registry
//! entry is dispatched through an actual HTTP/1.1 socket and byte-compared
//! against direct registry dispatch — which is exactly what the Tauri IPC
//! command forwards (`apps/desktop` proves that half in its own tests).
//! One process, one shared runtime, zero cloud access, zero accounts —
//! checkable on the `public-builds-alone` runner.
//!
//! The client is a hand-rolled HTTP/1.1 reader over `std::net::TcpStream`
//! so no HTTP client library joins the tree for tests. `Connection: close`
//! keeps the reads simple; axum sets `content-length` for every body this
//! suite exercises.

#![forbid(unsafe_code)]

use pos_api::{
    CommandName, LocalBootstrapConfig, ProjectCreateInput, ProjectExportInput, ProjectPathInput,
    ProjectSeedInput, QueryName, RunBudgetWire, RunControlInput, RunResumeInput, RunStartInput,
    RunStepsInput, RunWorker, StreamName, bootstrap_local_runtime, input_json,
};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpStream};
use std::sync::Arc;

/// A served runtime on an ephemeral loopback port, torn down on drop.
struct ServedApi {
    addr: SocketAddr,
    runtime: Arc<pos_api::LocalRuntime>,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

impl ServedApi {
    fn start() -> Self {
        let runtime = Arc::new(bootstrap_local_runtime(LocalBootstrapConfig::isolated(
            "path-that-does-not-exist-in-the-test-checkout".into(),
        )));
        let served = Arc::clone(&runtime);
        let (shutdown, on_shutdown) = tokio::sync::oneshot::channel::<()>();
        let (addr_tx, addr_rx) = std::sync::mpsc::channel::<SocketAddr>();
        let thread = std::thread::spawn(move || {
            let tokio_runtime = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("test tokio runtime builds");
            tokio_runtime.block_on(async move {
                let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
                    .await
                    .expect("ephemeral loopback bind succeeds");
                addr_tx
                    .send(listener.local_addr().expect("bound socket has an addr"))
                    .expect("test main thread is waiting for the addr");
                pos_api::http::serve(listener, served, async {
                    let _ = on_shutdown.await;
                })
                .await
                .expect("serve runs until shutdown");
            });
        });
        let addr = addr_rx
            .recv_timeout(std::time::Duration::from_secs(10))
            .expect("server reports its address within 10s");
        Self {
            addr,
            runtime,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    /// One HTTP/1.1 exchange. Returns (status, content_type, body).
    fn request(
        &self,
        method: &str,
        target: &str,
        headers: &[(&str, &str)],
        body: &str,
    ) -> (u16, String, String) {
        let mut stream = TcpStream::connect(self.addr).expect("connect to the served API");
        let mut request = format!(
            "{method} {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.addr,
            body.len()
        );
        for (name, value) in headers {
            request.push_str(&format!("{name}: {value}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(body);
        stream
            .write_all(request.as_bytes())
            .expect("request writes");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("response reads");
        parse_http_response(&raw)
    }

    fn get(&self, target: &str) -> (u16, String, String) {
        self.request("GET", target, &[], "")
    }

    fn post(&self, target: &str, body: &str) -> (u16, String, String) {
        self.request("POST", target, &[], body)
    }
}

impl Drop for ServedApi {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

fn parse_http_response(raw: &[u8]) -> (u16, String, String) {
    let text = String::from_utf8_lossy(raw);
    let (head, body) = text
        .split_once("\r\n\r\n")
        .expect("response has a header/body separator");
    let status: u16 = head
        .lines()
        .next()
        .and_then(|line| line.split_whitespace().nth(1))
        .and_then(|code| code.parse().ok())
        .expect("status line parses");
    let content_type = head
        .lines()
        .find_map(|line| {
            let (name, value) = line.split_once(':')?;
            name.eq_ignore_ascii_case("content-type")
                .then(|| value.trim().to_owned())
        })
        .unwrap_or_default();
    (status, content_type, body.to_owned())
}

fn percent_encode(text: &str) -> String {
    let mut encoded = String::new();
    for byte in text.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(byte as char);
            }
            other => encoded.push_str(&format!("%{other:02X}")),
        }
    }
    encoded
}

/// Every input-free query, over the real socket vs direct dispatch.
#[test]
fn input_free_queries_are_byte_identical_over_the_real_transport() {
    let served = ServedApi::start();
    for query in [
        QueryName::CapabilitySnapshot,
        QueryName::ProjectList,
        // Real since m0-s10: an empty session rolls up to zero rows.
        QueryName::CostRollup,
        QueryName::Health,
    ] {
        let name = query.as_str();
        let direct = served
            .runtime
            .query_with_input(name, "{}")
            .expect("the registered query resolves");
        let (status, content_type, body) = served.get(&format!("/api/query/{name}"));
        assert_eq!(status, 200, "{name}: {body}");
        assert_eq!(content_type, "application/json");
        assert_eq!(body, direct, "{name} reshaped between transports");
    }
    // `cron.preview` is a pure function of its input, so it is contract-tested
    // here rather than in the project-scoped block below. A fixed origin
    // instant keeps the comparison stable without freezing the clock.
    let name = QueryName::CronPreview.as_str();
    let input =
        r#"{"expr":"0 3 * * *","tz":"America/New_York","afterTsMs":1772946000000,"count":10}"#;
    let direct = served
        .runtime
        .query_with_input(name, input)
        .expect("the cron engine resolves");
    let (status, content_type, body) = served.get(&format!(
        "/api/query/{name}?input={}",
        percent_encode(input)
    ));
    assert_eq!(status, 200, "{name}: {body}");
    assert_eq!(content_type, "application/json");
    assert_eq!(body, direct, "{name} reshaped between transports");
}

/// The full project lifecycle through the REAL transport, reads byte-compared
/// against direct dispatch on the same shared runtime.
#[test]
fn the_project_surface_is_byte_identical_over_the_real_transport() {
    let served = ServedApi::start();
    let directory = tempfile::tempdir().expect("tempdir");
    let project = directory.path().join("contract.pos");
    let export = directory.path().join("contract-export.pos");
    let path = project.display().to_string();

    let create = input_json(&ProjectCreateInput {
        path: path.clone(),
        name: Some("Contract".to_owned()),
        template: "generic".to_owned(),
    })
    .expect("input serializes");
    let (status, _, body) = served.post(
        &format!("/api/cmd/{}", CommandName::ProjectCreate.as_str()),
        &create,
    );
    assert_eq!(status, 200, "create failed: {body}");
    assert!(body.contains("\"headSeq\":1"));

    let seed = input_json(&ProjectSeedInput {
        path: path.clone(),
        event_count: 32,
        seed: 7,
    })
    .expect("input serializes");
    let (status, _, body) = served.post(
        &format!("/api/cmd/{}", CommandName::ProjectSeedSynthetic.as_str()),
        &seed,
    );
    assert_eq!(status, 200, "seed failed: {body}");

    let open = input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes");
    let (status, _, body) = served.post(
        &format!("/api/cmd/{}", CommandName::ProjectOpen.as_str()),
        &open,
    );
    assert_eq!(status, 200, "open failed: {body}");

    // Reads through the socket equal direct dispatch byte-for-byte.
    let read_input = input_json(&ProjectPathInput { path: path.clone() }).expect("serializes");
    for query in [
        QueryName::ProjectInspect,
        QueryName::ProjectVerify,
        QueryName::ProjectList,
        QueryName::Health,
    ] {
        let name = query.as_str();
        let (target, direct_input) = if matches!(query, QueryName::ProjectList | QueryName::Health)
        {
            (format!("/api/query/{name}"), "{}".to_owned())
        } else {
            (
                format!("/api/query/{name}?input={}", percent_encode(&read_input)),
                read_input.clone(),
            )
        };
        let direct = served
            .runtime
            .query_with_input(name, &direct_input)
            .expect("the registered query resolves");
        let (status, _, body) = served.get(&target);
        assert_eq!(status, 200, "{name} failed: {body}");
        assert_eq!(body, direct, "{name} reshaped between transports");
    }

    // `job.list` is project-scoped, so it belongs to this block: an empty
    // queue must answer with real zeros over the socket, not an envelope.
    let job_list_input = r#"{"path":"PATH","state":null,"rowCountMax":10}"#
        .replace("PATH", &path.replace('\\', "\\\\"));
    let name = QueryName::JobList.as_str();
    let direct = served
        .runtime
        .query_with_input(name, &job_list_input)
        .expect("job.list resolves against an open project");
    let (status, _, body) = served.get(&format!(
        "/api/query/{name}?input={}",
        percent_encode(&job_list_input)
    ));
    assert_eq!(status, 200, "{name} failed: {body}");
    assert_eq!(body, direct, "{name} reshaped between transports");
    // The seeded corpus carries legacy `JobEnqueued`/`JobCompleted` V1 facts,
    // so this row doubles as the eternal-events check over the real socket:
    // V1 jobs project and render through the v2 read surface with the
    // documented defaults rather than failing replay.
    assert!(
        body.contains("\"jobKind\":\"synthetic.tick\""),
        "the seeded legacy job facts did not render: {body}"
    );
    assert!(
        body.contains("\"class\":\"maintenance\"") && body.contains("\"priority\":\"normal\""),
        "legacy V1 jobs must take the documented defaults: {body}"
    );
    assert!(
        body.contains("\"rowCountMax\":10"),
        "the honoured bound must travel in-band: {body}"
    );

    let export_input = input_json(&ProjectExportInput {
        path,
        out: export.display().to_string(),
    })
    .expect("input serializes");
    let (status, _, body) = served.post(
        &format!("/api/cmd/{}", CommandName::ProjectExport.as_str()),
        &export_input,
    );
    assert_eq!(status, 200, "export failed: {body}");
    // The export is a valid project in its own right (F2/F45), through the
    // same socket.
    let verify_input = input_json(&ProjectPathInput {
        path: export.display().to_string(),
    })
    .expect("input serializes");
    let (status, _, body) = served.get(&format!(
        "/api/query/{}?input={}",
        QueryName::ProjectVerify.as_str(),
        percent_encode(&verify_input)
    ));
    assert_eq!(status, 200, "verify failed: {body}");
    assert!(body.contains("\"clean\":true"), "export dirty: {body}");
}

/// Run lifecycle and the durable step stream are real over the socket.
#[test]
fn run_lifecycle_is_real_and_the_later_stream_stays_honest() {
    let served = ServedApi::start();
    let directory = tempfile::tempdir().expect("tempdir");
    let path = directory
        .path()
        .join("run-contract.pos")
        .display()
        .to_string();
    let create = input_json(&ProjectCreateInput {
        path: path.clone(),
        name: Some("Run contract".to_owned()),
        template: "generic".to_owned(),
    })
    .expect("create input serializes");
    let (status, _, body) = served.post(
        &format!("/api/cmd/{}", CommandName::ProjectCreate.as_str()),
        &create,
    );
    assert_eq!(status, 200, "project.create failed: {body}");

    let start = input_json(&RunStartInput {
        path: path.clone(),
        worker: RunWorker::Navigator,
        autonomy_level: 2,
        budget: RunBudgetWire {
            tokens: 100,
            usd_micros: 100,
            wall_ms: 10_000,
            storage_bytes: 1_024,
            tool_calls: 4,
            retries: 2,
            steps: 4,
        },
        tool_grants: Vec::new(),
        parent_run_id: None,
    })
    .expect("Run start input serializes");
    let (status, content_type, started) = served.post(
        &format!("/api/cmd/{}", CommandName::RunStart.as_str()),
        &start,
    );
    assert_eq!(status, 200, "run.start failed: {started}");
    assert_eq!(content_type, "application/json");
    assert!(started.contains("\"status\":\"preflight\""));
    let run_id = serde_json::from_str::<serde_json::Value>(&started)
        .expect("Run report parses")
        .get("runId")
        .and_then(serde_json::Value::as_str)
        .expect("Run report has runId")
        .to_owned();

    let control = |reason: &str| {
        input_json(&RunControlInput {
            path: path.clone(),
            run_id: run_id.clone(),
            reason: reason.to_owned(),
        })
        .expect("control input serializes")
    };
    let (status, _, paused) = served.post(
        &format!("/api/cmd/{}", CommandName::RunPause.as_str()),
        &control("HTTP pause"),
    );
    assert_eq!(status, 200, "run.pause failed: {paused}");
    assert!(paused.contains("\"kind\":\"requested\""));

    let resume = input_json(&RunResumeInput {
        path: path.clone(),
        run_id: run_id.clone(),
    })
    .expect("resume input serializes");
    let (status, _, resumed) = served.post(
        &format!("/api/cmd/{}", CommandName::RunResume.as_str()),
        &resume,
    );
    assert_eq!(status, 200, "run.resume failed: {resumed}");
    assert!(resumed.contains("\"status\":\"running\""));

    let (status, _, canceled) = served.post(
        &format!("/api/cmd/{}", CommandName::RunCancel.as_str()),
        &control("HTTP cancel"),
    );
    assert_eq!(status, 200, "run.cancel failed: {canceled}");
    assert!(canceled.contains("\"status\":\"canceled\""));

    let name = StreamName::RunSteps.as_str();
    let stream_input =
        input_json(&RunStepsInput { path, run_id }).expect("stream input serializes");
    let (status, content_type, body) = served.get(&format!(
        "/api/stream/{name}?input={}",
        percent_encode(&stream_input)
    ));
    assert_eq!(status, 200);
    assert_eq!(content_type, "text/event-stream");
    assert!(body.contains("retry: 2000"));
}

/// Transport error behavior: unknown names, malformed input, malformed
/// resume cursors — each a typed envelope under a deliberate status.
#[test]
fn transport_errors_are_typed_envelopes_with_deliberate_statuses() {
    let served = ServedApi::start();

    let (status, _, body) = served.get("/api/query/capability.snapsh0t");
    assert_eq!(status, 404);
    assert!(body.contains("\"code\":\"unknown_query\""));

    let (status, _, body) = served.post("/api/cmd/project.dr0p", "{}");
    assert_eq!(status, 404);
    assert!(body.contains("\"code\":\"unknown_command\""));

    let (status, _, body) = served.get("/api/stream/run.st3ps");
    assert_eq!(status, 404);
    assert!(body.contains("\"code\":\"unknown_stream\""));

    let (status, _, body) = served.post("/api/cmd/project.create", "{\"path\":42}");
    assert_eq!(status, 400);
    assert!(body.contains("\"code\":\"invalid_input\""));

    // A malformed resume cursor is rejected before the stream resolves.
    let (status, _, body) = served.request(
        "GET",
        &format!("/api/stream/{}", StreamName::RunSteps.as_str()),
        &[("last-event-id", "seven")],
        "",
    );
    assert_eq!(status, 400);
    assert!(body.contains("\"code\":\"invalid_input\""));
    assert!(body.contains("Last-Event-ID"));

    // The ?from= fallback parses through the same framing function.
    let (status, _, body) = served.get(&format!(
        "/api/stream/{}?from=notanumber",
        StreamName::RunSteps.as_str()
    ));
    assert_eq!(status, 400);
    assert!(body.contains("\"code\":\"invalid_input\""));
}

/// The L8 transport cap: a body over the stated bound is refused by the
/// transport layer itself (axum's 413), never buffered without limit.
#[test]
fn an_oversized_command_body_is_refused_with_413() {
    let served = ServedApi::start();
    let oversized = "x".repeat(pos_api::http::API_HTTP_BODY_BYTES_MAX + 1);
    let (status, _, _) = served.post("/api/cmd/project.create", &oversized);
    assert_eq!(status, 413);
}
