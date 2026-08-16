//! The m0-s08 acceptance suite over the real served shell: cross-tenant
//! isolation on EVERY registered route (the suite iterates the registry, so
//! a new name is covered — or failed — automatically), the viewer RBAC
//! matrix, and the audit-log + zero-secret-material oracles.

#![forbid(unsafe_code)]

use pos_server::control::Role;
use pos_server::web::{ServerConfig, ServerState, serve};
use std::io::{Read, Write};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::sync::Arc;
use std::sync::mpsc::{Receiver, Sender};
use std::time::Duration;

use pos_api::{
    CommandName, CostRollupInput, EchoRuntimeOptions, ProjectCreateInput, ProjectId, QueryName,
    RunBudgetWire, RunControlInput, RunId, RunResumeInput, RunStartInput, RunStepsInput, RunWorker,
    StreamName, input_json, telemetry,
};

/// A served shell on an ephemeral loopback port with its own data root.
struct ServedShell {
    addr: SocketAddr,
    state: Arc<ServerState>,
    #[allow(
        dead_code,
        reason = "held for its Drop: the tempdir outlives the server"
    )]
    data_root: tempfile::TempDir,
    shutdown: Option<tokio::sync::oneshot::Sender<()>>,
    thread: Option<std::thread::JoinHandle<()>>,
}

/// One authenticated browser, as far as the server can tell.
struct Client {
    cookie: String,
    account_id: String,
    workspace_id: String,
    projects_root: String,
}

struct BlockingEchoEndpoint {
    base_url: String,
    requested: Receiver<()>,
    release: Sender<()>,
    thread: std::thread::JoinHandle<()>,
}

impl BlockingEchoEndpoint {
    fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Echo fixture binds");
        let address = listener.local_addr().expect("Echo fixture has address");
        let (requested_tx, requested) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Echo worker connects");
            let marker = read_echo_marker(&mut stream);
            requested_tx.send(()).expect("test waits for request");
            release_rx.recv().expect("test releases response");
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
            .expect("Echo reaches the model fixture");
    }

    fn release(&self) {
        self.release.send(()).expect("release Echo response");
    }

    fn finish(self) {
        self.thread.join().expect("Echo fixture exits");
    }
}

impl ServedShell {
    fn start() -> Self {
        Self::start_with_echo(None)
    }

    fn start_with_echo(echo: Option<EchoRuntimeOptions>) -> Self {
        let data_root = tempfile::tempdir().expect("tempdir");
        let state = ServerState::initialize(&ServerConfig {
            data_root: data_root.path().to_path_buf(),
            ui_dist: None,
            echo,
        })
        .expect("server state initializes");
        let served = Arc::clone(&state);
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
                serve(listener, served, async {
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
            state,
            data_root,
            shutdown: Some(shutdown),
            thread: Some(thread),
        }
    }

    fn request(
        &self,
        method: &str,
        target: &str,
        cookie: &str,
        body: &str,
    ) -> (u16, Vec<(String, String)>, String) {
        let mut stream = TcpStream::connect(self.addr).expect("connect to the served shell");
        let mut request = format!(
            "{method} {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncontent-length: {}\r\n",
            self.addr,
            body.len()
        );
        if !cookie.is_empty() {
            request.push_str(&format!("cookie: {cookie}\r\n"));
        }
        request.push_str("\r\n");
        request.push_str(body);
        stream
            .write_all(request.as_bytes())
            .expect("request writes");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("response reads");
        let text = String::from_utf8_lossy(&raw);
        let (head, body) = text
            .split_once("\r\n\r\n")
            .expect("response has a header/body separator");
        let status: u16 = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse().ok())
            .expect("status line parses");
        let headers = head
            .lines()
            .skip(1)
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            })
            .collect();
        (status, headers, body.to_owned())
    }

    fn signup(&self, email: &str, password: &str) -> Client {
        let body = format!("{{\"email\":\"{email}\",\"password\":\"{password}\"}}");
        let (status, headers, response) = self.request("POST", "/auth/signup", "", &body);
        assert_eq!(status, 200, "signup failed: {response}");
        let set_cookie = headers
            .iter()
            .find(|(name, _)| name == "set-cookie")
            .map(|(_, value)| value.clone())
            .expect("signup sets the session cookie");
        for attribute in ["HttpOnly", "SameSite=Lax", "Secure", "Path=/"] {
            assert!(
                set_cookie.contains(attribute),
                "session cookie is missing {attribute}: {set_cookie}"
            );
        }
        let cookie = set_cookie
            .split_once(';')
            .map(|(pair, _)| pair.to_owned())
            .expect("cookie has a name=value pair");
        Client {
            cookie,
            account_id: json_field(&response, "accountId"),
            workspace_id: json_field(&response, "workspaceId"),
            projects_root: json_field(&response, "projectsRoot"),
        }
    }
}

impl Drop for ServedShell {
    fn drop(&mut self) {
        if let Some(shutdown) = self.shutdown.take() {
            let _ = shutdown.send(());
        }
        if let Some(thread) = self.thread.take() {
            let _ = thread.join();
        }
    }
}

/// Minimal happy-path field extraction from the tiny auth bodies this suite
/// controls.
fn json_field(body: &str, field: &str) -> String {
    let value: serde_json::Value = serde_json::from_str(body).expect("auth body parses");
    value
        .get(field)
        .and_then(|v| v.as_str())
        .unwrap_or_else(|| panic!("{field} missing from {body}"))
        .to_owned()
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

fn project_path(client: &Client, name: &str) -> String {
    format!("{}/{name}.pos", client.projects_root)
}

fn path_input(path: &str) -> String {
    format!(
        "{{\"path\":{}}}",
        serde_json::to_string(path).expect("serializes")
    )
}

/// Dispatches `(name, input)` through the authenticated API surface.
fn api(shell: &ServedShell, client: &Client, kind: &str, name: &str, input: &str) -> (u16, String) {
    let (status, _, body) = match kind {
        "cmd" => shell.request("POST", &format!("/api/cmd/{name}"), &client.cookie, input),
        "query" => shell.request(
            "GET",
            &format!("/api/query/{name}?input={}", percent_encode(input)),
            &client.cookie,
            "",
        ),
        "stream" => shell.request(
            "GET",
            &format!("/api/stream/{name}?input={}", percent_encode(input)),
            &client.cookie,
            "",
        ),
        other => panic!("unknown dispatch kind {other}"),
    };
    (status, body)
}

/// Every registered route refuses an unauthenticated request with the typed
/// envelope — deny-by-default proven registry-wide.
#[test]
fn every_registered_route_requires_a_session() {
    let shell = ServedShell::start();
    for query in QueryName::ALL {
        let (status, _, body) =
            shell.request("GET", &format!("/api/query/{}", query.as_str()), "", "");
        assert_eq!(status, 401, "{} answered without a session", query.as_str());
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    for command in CommandName::ALL {
        let (status, _, body) =
            shell.request("POST", &format!("/api/cmd/{}", command.as_str()), "", "{}");
        assert_eq!(
            status,
            401,
            "{} answered without a session",
            command.as_str()
        );
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    for stream in StreamName::ALL {
        let (status, _, body) =
            shell.request("GET", &format!("/api/stream/{}", stream.as_str()), "", "");
        assert_eq!(
            status,
            401,
            "{} answered without a session",
            stream.as_str()
        );
        assert!(body.contains("\"code\":\"unauthenticated\""));
    }
    // A garbage cookie is unauthenticated, not an error.
    let (status, _, _) = shell.request("GET", "/api/query/health", "pos_session=deadbeef", "");
    assert_eq!(status, 401);
}

#[test]
fn authenticated_echo_frames_stream_before_the_run_finishes() {
    let endpoint = BlockingEchoEndpoint::start();
    let shell = ServedShell::start_with_echo(Some(EchoRuntimeOptions::loopback(
        &endpoint.base_url,
        "echo-http-fixture",
    )));
    let client = shell.signup("echo@example.com", "echo password 1");
    let project = project_path(&client, "echo-live");
    let (status, created) = api(
        &shell,
        &client,
        "cmd",
        CommandName::ProjectCreate.as_str(),
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("Echo live".to_owned()),
            template: "generic".to_owned(),
        })
        .expect("create input serializes"),
    );
    assert_eq!(status, 200, "create failed: {created}");
    let (status, started) = api(
        &shell,
        &client,
        "cmd",
        CommandName::RunStart.as_str(),
        &input_json(&RunStartInput {
            path: project.clone(),
            worker: RunWorker::Echo,
            autonomy_level: 2,
            budget: echo_budget(),
            tool_grants: Vec::new(),
            parent_run_id: None,
        })
        .expect("Run input serializes"),
    );
    assert_eq!(status, 200, "Echo start failed: {started}");
    let run_id = json_field(&started, "runId");
    endpoint.wait_for_request();

    let input = input_json(&RunStepsInput {
        path: project.clone(),
        run_id,
    })
    .expect("stream input serializes");
    let target = format!(
        "/api/stream/{}?input={}",
        StreamName::RunSteps.as_str(),
        percent_encode(&input)
    );
    let mut stream = TcpStream::connect(shell.addr).expect("connect SSE client");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set SSE timeout");
    let request = format!(
        "GET {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncookie: {}\r\n\r\n",
        shell.addr, client.cookie
    );
    stream
        .write_all(request.as_bytes())
        .expect("write SSE subscribe");
    let mut raw = Vec::new();
    read_until_contains(&mut stream, &mut raw, b"\"streamSeq\":1");
    let live = String::from_utf8_lossy(&raw);
    assert!(live.contains("content-type: text/event-stream"));
    assert!(live.contains("event: run.step"));
    assert!(
        !live.contains("\"streamSeq\":2"),
        "the blocked model boundary must not be announced as durable"
    );

    endpoint.release();
    stream
        .read_to_end(&mut raw)
        .expect("read terminal SSE tail");
    let complete = String::from_utf8_lossy(&raw);
    for needle in [
        "\"streamSeq\":2",
        "\"streamSeq\":3",
        "\"runStatus\":\"done\"",
        "\"validationStatus\":\"passed\"",
    ] {
        assert!(complete.contains(needle), "SSE response omitted {needle}");
    }

    let (status, cost) = api(
        &shell,
        &client,
        "query",
        QueryName::CostRollup.as_str(),
        &input_json(&CostRollupInput {
            path: Some(project),
        })
        .expect("cost input serializes"),
    );
    assert_eq!(status, 200, "cost rollup failed: {cost}");
    assert!(cost.contains("\"calls\":1"));
    assert!(cost.contains("\"feature\":\"echo\""));
    assert!(cost.contains("\"agent\":\"echo\""));
    endpoint.finish();
}

/// m0-s15 AC 1, server half. Identical assertion shape to the desktop test,
/// against a real authenticated socket: one Echo Run is one connected tree,
/// and the trace key is computed from the durable ids rather than scraped
/// from output.
#[test]
fn one_echo_run_produces_a_single_connected_span_tree_on_the_server() {
    let captured = telemetry::capture_any();
    let endpoint = BlockingEchoEndpoint::start();
    let shell = ServedShell::start_with_echo(Some(EchoRuntimeOptions::loopback(
        &endpoint.base_url,
        "echo-http-span-fixture",
    )));
    let client = shell.signup("echo-spans@example.com", "echo spans password 1");
    let project = project_path(&client, "echo-spans");
    let (status, created) = api(
        &shell,
        &client,
        "cmd",
        CommandName::ProjectCreate.as_str(),
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("Echo spans".to_owned()),
            template: "generic".to_owned(),
        })
        .expect("create input serializes"),
    );
    assert_eq!(status, 200, "create failed: {created}");
    let (status, started) = api(
        &shell,
        &client,
        "cmd",
        CommandName::RunStart.as_str(),
        &input_json(&RunStartInput {
            path: project.clone(),
            worker: RunWorker::Echo,
            autonomy_level: 2,
            budget: echo_budget(),
            tool_grants: Vec::new(),
            parent_run_id: None,
        })
        .expect("Run input serializes"),
    );
    assert_eq!(status, 200, "Echo start failed: {started}");
    let run_id = json_field(&started, "runId");
    let project_id = json_field(&started, "projectId");
    let trace = telemetry::TraceId::for_run(
        ProjectId::from_hex(&project_id).expect("project id is hex"),
        RunId::from_hex(&run_id).expect("Run id is hex"),
    );
    endpoint.wait_for_request();
    endpoint.release();

    // Drain the feed so every boundary has closed before the assertion.
    let input = input_json(&RunStepsInput {
        path: project.clone(),
        run_id,
    })
    .expect("stream input serializes");
    let target = format!(
        "/api/stream/{}?input={}",
        StreamName::RunSteps.as_str(),
        percent_encode(&input)
    );
    let mut stream = TcpStream::connect(shell.addr).expect("connect SSE client");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set SSE timeout");
    let request = format!(
        "GET {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncookie: {}\r\n\r\n",
        shell.addr, client.cookie
    );
    stream
        .write_all(request.as_bytes())
        .expect("write SSE subscribe");
    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).expect("read the SSE feed");
    assert!(String::from_utf8_lossy(&raw).contains("\"runStatus\":\"done\""));

    captured
        .assert_single_connected_tree(trace)
        .expect("the Echo Run is one connected tree on the server too");
    let spans = captured.spans_in(trace);
    let root = captured.root(trace).expect("the trace has a root");
    assert_eq!(root.taxonomy_name(), "api.cmd/run.start");
    let steps: Vec<&telemetry::FinishedSpan> = spans
        .iter()
        .filter(|span| span.name == telemetry::SpanName::AgentsStep)
        .collect();
    assert_eq!(steps.len(), 3);
    assert!(steps.iter().all(|step| step.parent == Some(root.span)));
    let gateway: Vec<&telemetry::FinishedSpan> = spans
        .iter()
        .filter(|span| span.name == telemetry::SpanName::GatewayCall)
        .collect();
    assert_eq!(gateway.len(), 1);
    assert!(
        steps
            .iter()
            .any(|step| Some(step.span) == gateway[0].parent)
    );
    // No span field anywhere in the tree carries free text: the value type
    // has no String variant, so this is a check that the *shape* held, not a
    // scan hoping to find nothing.
    for span in &spans {
        for (_, value) in span.fields() {
            assert!(
                !matches!(value, telemetry::SpanValue::Label(label) if label.len() > 64),
                "a span label grew past the closed vocabulary"
            );
        }
        assert_eq!(span.dropped_field_count(), 0);
    }
    endpoint.finish();
}

#[test]
fn authenticated_web_cancel_lands_at_the_next_streamed_checkpoint() {
    let endpoint = BlockingEchoEndpoint::start();
    let shell = ServedShell::start_with_echo(Some(EchoRuntimeOptions::loopback(
        &endpoint.base_url,
        "echo-http-cancel-fixture",
    )));
    let client = shell.signup("echo-cancel@example.com", "echo cancel password 1");
    let project = project_path(&client, "echo-cancel");
    let (status, created) = api(
        &shell,
        &client,
        "cmd",
        CommandName::ProjectCreate.as_str(),
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("Echo web cancel".to_owned()),
            template: "generic".to_owned(),
        })
        .expect("create input serializes"),
    );
    assert_eq!(status, 200, "create failed: {created}");
    let (status, started) = api(
        &shell,
        &client,
        "cmd",
        CommandName::RunStart.as_str(),
        &input_json(&RunStartInput {
            path: project.clone(),
            worker: RunWorker::Echo,
            autonomy_level: 2,
            budget: echo_budget(),
            tool_grants: Vec::new(),
            parent_run_id: None,
        })
        .expect("Run input serializes"),
    );
    assert_eq!(status, 200, "Echo start failed: {started}");
    let run_id = json_field(&started, "runId");
    endpoint.wait_for_request();

    let stream_input = input_json(&RunStepsInput {
        path: project.clone(),
        run_id: run_id.clone(),
    })
    .expect("stream input serializes");
    let target = format!(
        "/api/stream/{}?input={}",
        StreamName::RunSteps.as_str(),
        percent_encode(&stream_input)
    );
    let mut stream = TcpStream::connect(shell.addr).expect("connect SSE client");
    stream
        .set_read_timeout(Some(Duration::from_secs(10)))
        .expect("set SSE timeout");
    stream
        .write_all(
            format!(
                "GET {target} HTTP/1.1\r\nhost: {}\r\nconnection: close\r\ncookie: {}\r\n\r\n",
                shell.addr, client.cookie
            )
            .as_bytes(),
        )
        .expect("write SSE subscribe");
    let mut raw = Vec::new();
    read_until_contains(&mut stream, &mut raw, b"\"streamSeq\":1");
    assert!(!String::from_utf8_lossy(&raw).contains("\"streamSeq\":2"));

    let (status, pending) = api(
        &shell,
        &client,
        "cmd",
        CommandName::RunCancel.as_str(),
        &input_json(&RunControlInput {
            path: project.clone(),
            run_id,
            reason: "Web cancellation oracle".to_owned(),
        })
        .expect("cancel input serializes"),
    );
    assert_eq!(status, 200, "cancel failed: {pending}");
    assert!(pending.contains("\"pendingControl\":\"cancel\""));
    endpoint.release();
    stream
        .read_to_end(&mut raw)
        .expect("read canceled SSE tail");
    let canceled = String::from_utf8_lossy(&raw);
    for needle in [
        "\"streamSeq\":2",
        "\"runStatus\":\"canceled\"",
        "\"terminal\":true",
    ] {
        assert!(canceled.contains(needle), "canceled feed omitted {needle}");
    }
    assert!(
        !canceled.contains("\"streamSeq\":3"),
        "cancel must stop before the report step"
    );

    let (status, cost) = api(
        &shell,
        &client,
        "query",
        QueryName::CostRollup.as_str(),
        &input_json(&CostRollupInput {
            path: Some(project),
        })
        .expect("cost input serializes"),
    );
    assert_eq!(status, 200, "cost rollup failed: {cost}");
    assert!(cost.contains("\"calls\":1"));
    assert!(cost.contains("\"feature\":\"echo\""));
    assert!(cost.contains("\"agent\":\"echo\""));
    endpoint.finish();
}

/// The m0-s08 cross-tenant AC: user B, enumerating ids, cannot read or
/// mutate user A's projects through ANY registered API route. The suite
/// iterates the registry: every name is dispatched by B against A's
/// resources, and no response may carry A's data.
#[test]
fn cross_tenant_isolation_holds_on_every_registered_route() {
    let shell = ServedShell::start();
    let a = shell.signup("a@example.com", "alpha password 1");
    let b = shell.signup("b@example.com", "bravo password 1");

    // A builds real state: a created, seeded, opened project.
    let alpha = project_path(&a, "alpha");
    let (status, body) = api(
        &shell,
        &a,
        "cmd",
        "project.create",
        &format!(
            "{{\"path\":{},\"name\":\"Secret Alpha\"}}",
            serde_json::to_string(&alpha).expect("serializes")
        ),
    );
    assert_eq!(status, 200, "A cannot create: {body}");
    let a_project_id = json_field(&body, "projectId");
    let (status, _) = api(
        &shell,
        &a,
        "cmd",
        "project.seed-synthetic",
        &format!(
            "{{\"path\":{},\"eventCount\":16}}",
            serde_json::to_string(&alpha).expect("serializes")
        ),
    );
    assert_eq!(status, 200);
    let (status, _) = api(&shell, &a, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200);

    // B enumerates A's ids: workspace hex is known, project path is known.
    // Every registered name gets a row; path-bearing ops target A's project.
    let a_export = format!("{}/exfil.pos", a.projects_root);
    let enumerated_run_id = "11".repeat(16);
    let mut rows: Vec<(&str, String, String)> = Vec::new();
    for query in QueryName::ALL {
        let input = match query {
            QueryName::ProjectInspect | QueryName::ProjectVerify => path_input(&alpha),
            QueryName::CostRollup => input_json(&CostRollupInput {
                path: Some(alpha.clone()),
            })
            .expect("cost input serializes"),
            _ => "{}".to_owned(),
        };
        rows.push(("query", query.as_str().to_owned(), input));
    }
    for command in CommandName::ALL {
        let input = match command {
            CommandName::ProjectCreate => format!(
                "{{\"path\":{}}}",
                serde_json::to_string(&format!("{}/implant.pos", a.projects_root))
                    .expect("serializes")
            ),
            CommandName::ProjectSeedSynthetic => format!(
                "{{\"path\":{},\"eventCount\":1}}",
                serde_json::to_string(&alpha).expect("serializes")
            ),
            CommandName::ProjectExport => format!(
                "{{\"path\":{},\"out\":{}}}",
                serde_json::to_string(&alpha).expect("serializes"),
                serde_json::to_string(&a_export).expect("serializes")
            ),
            CommandName::ProjectOpen => path_input(&alpha),
            CommandName::RunStart => input_json(&RunStartInput {
                path: alpha.clone(),
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
            .expect("Run input serializes"),
            CommandName::RunCancel | CommandName::RunPause => input_json(&RunControlInput {
                path: alpha.clone(),
                run_id: enumerated_run_id.clone(),
                reason: "enumeration probe".to_owned(),
            })
            .expect("control input serializes"),
            CommandName::RunResume => input_json(&RunResumeInput {
                path: alpha.clone(),
                run_id: enumerated_run_id.clone(),
            })
            .expect("resume input serializes"),
            _ => "{}".to_owned(),
        };
        rows.push(("cmd", command.as_str().to_owned(), input));
    }
    for stream in StreamName::ALL {
        rows.push((
            "stream",
            stream.as_str().to_owned(),
            input_json(&RunStepsInput {
                path: alpha.clone(),
                run_id: enumerated_run_id.clone(),
            })
            .expect("stream input serializes"),
        ));
    }

    let b_client = &b;
    for (kind, name, input) in &rows {
        let (status, body) = api(&shell, b_client, kind, name, input);
        // The isolation property: nothing of A's ever reaches B.
        assert!(
            !body.contains(&a_project_id),
            "{name}: A's project id leaked to B: {body}"
        );
        assert!(
            !body.contains("Secret Alpha"),
            "{name}: A's project name leaked to B: {body}"
        );
        // Path-bearing rows must be refused outright.
        let targets_a = input.contains(&a.projects_root);
        if targets_a {
            assert_eq!(
                status, 403,
                "{name}: B reached A's workspace (status {status}): {body}"
            );
            assert!(body.contains("\"code\":\"forbidden\""), "{name}: {body}");
        }
    }

    // B's own session state saw none of it.
    let (_, list) = api(&shell, &b, "query", "project.list", "{}");
    assert!(
        list.contains("\"projects\":[]"),
        "B's list is not empty: {list}"
    );
    let (_, health) = api(&shell, &b, "query", "health", "{}");
    assert!(health.contains("\"openProjectCount\":0"));

    // A, the positive control, still sees exactly A's state.
    let (_, list) = api(&shell, &a, "query", "project.list", "{}");
    assert!(list.contains(&a_project_id));

    // Paths outside the placement grammar never reach the filesystem.
    for smuggled in ["/etc/passwd", "../../escape.pos", "relative.pos"] {
        let (status, body) = api(
            &shell,
            &b,
            "query",
            "project.inspect",
            &path_input(smuggled),
        );
        assert_eq!(status, 400, "{smuggled} was not refused: {body}");
        assert!(body.contains("\"code\":\"invalid_input\""));
    }
}

/// The m0-s08 RBAC matrix AC: a viewer-role account can read but cannot
/// invoke any mutating command, across the whole v0 surface.
#[test]
fn a_viewer_cannot_invoke_mutating_commands() {
    let shell = ServedShell::start();
    let a = shell.signup("owner@example.com", "owner password 1");
    let alpha = project_path(&a, "alpha");
    let (status, _) = api(&shell, &a, "cmd", "project.create", &path_input(&alpha));
    assert_eq!(status, 200);

    // C becomes a pure viewer: viewer in A's workspace, and their own
    // personal membership downgraded through the operator seam (the v0
    // registry deliberately has no invite/role routes — auth surface stays
    // tiny; the seam is the documented path until a later milestone).
    let c = shell.signup("viewer@example.com", "viewer password 1");
    let a_ws: [u8; 16] = hex_bytes(&a.workspace_id);
    let c_ws: [u8; 16] = hex_bytes(&c.workspace_id);
    let c_id: [u8; 16] = hex_bytes(&c.account_id);
    shell
        .state
        .control()
        .grant_membership(a_ws, c_id, Role::Viewer)
        .expect("grant viewer in A's workspace");
    shell
        .state
        .control()
        .grant_membership(c_ws, c_id, Role::Viewer)
        .expect("downgrade personal membership");

    // Reads on A's project: allowed for the viewer.
    for read in ["project.inspect", "project.verify"] {
        let (status, body) = api(&shell, &c, "query", read, &path_input(&alpha));
        assert_eq!(status, 200, "viewer read {read} refused: {body}");
    }
    let (status, body) = api(&shell, &c, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200, "viewer open refused: {body}");
    // Closing is the other half of viewing: a viewer that could take a handle
    // but never give it back would hold a scheduler registration forever.
    let (status, body) = api(&shell, &c, "cmd", "project.close", &path_input(&alpha));
    assert_eq!(status, 200, "viewer close refused: {body}");

    // Every mutating command in the registry: refused with `forbidden`.
    let viewer_target = format!("{}/viewer-write.pos", a.projects_root);
    for command in CommandName::ALL {
        let input = match command {
            // Read-shaped, both proven above.
            CommandName::ProjectOpen | CommandName::ProjectClose => continue,
            CommandName::ProjectCreate => path_input(&viewer_target),
            CommandName::ProjectSeedSynthetic => format!(
                "{{\"path\":{},\"eventCount\":1}}",
                serde_json::to_string(&alpha).expect("serializes")
            ),
            CommandName::ProjectExport => format!(
                "{{\"path\":{},\"out\":{}}}",
                serde_json::to_string(&alpha).expect("serializes"),
                serde_json::to_string(&viewer_target).expect("serializes")
            ),
            CommandName::RunStart => input_json(&RunStartInput {
                path: alpha.clone(),
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
            .expect("Run input serializes"),
            CommandName::RunCancel | CommandName::RunPause => input_json(&RunControlInput {
                path: alpha.clone(),
                run_id: "22".repeat(16),
                reason: "viewer probe".to_owned(),
            })
            .expect("control input serializes"),
            CommandName::RunResume => input_json(&RunResumeInput {
                path: alpha.clone(),
                run_id: "22".repeat(16),
            })
            .expect("resume input serializes"),
            // Any future command is presumed mutating until this suite gets
            // a deliberate row — deny-by-default extends to the test.
            _ => "{}".to_owned(),
        };
        let (status, body) = api(&shell, &c, "cmd", command.as_str(), &input);
        assert_eq!(status, 403, "viewer invoked {}: {body}", command.as_str());
        assert!(body.contains("\"code\":\"forbidden\""));
    }
}

/// The m0-s08 audit AC: every audited action lands a queryable row, and no
/// password or session-token material exists anywhere in the control
/// database or its WAL.
#[test]
fn audit_rows_exist_and_no_secret_material_is_stored() {
    let shell = ServedShell::start();
    let password = "hunter2 but actually long";
    let a = shell.signup("audit@example.com", password);

    // Login again (a second audited action), then the project actions.
    let (status, _, login_body) = shell.request(
        "POST",
        "/auth/login",
        "",
        &format!("{{\"email\":\"audit@example.com\",\"password\":\"{password}\"}}"),
    );
    assert_eq!(status, 200, "login failed: {login_body}");
    let alpha = project_path(&a, "audited");
    let (status, _) = api(&shell, &a, "cmd", "project.create", &path_input(&alpha));
    assert_eq!(status, 200);
    let (status, _) = api(&shell, &a, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200);
    // Run actions execute through the real m0-s12 harness; the audit row
    // names the same project path while the Run id remains typed data.
    let run_start = input_json(&RunStartInput {
        path: alpha.clone(),
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
    let (status, started) = api(&shell, &a, "cmd", "run.start", &run_start);
    assert_eq!(status, 200, "run.start failed: {started}");
    let run_id = json_field(&started, "runId");
    let run_cancel = input_json(&RunControlInput {
        path: alpha,
        run_id,
        reason: "Audit fixture cancellation".to_owned(),
    })
    .expect("Run cancel input serializes");
    let (status, canceled) = api(&shell, &a, "cmd", "run.cancel", &run_cancel);
    assert_eq!(status, 200, "run.cancel failed: {canceled}");

    let (status, _, audit) = shell.request("GET", "/auth/audit", &a.cookie, "");
    assert_eq!(status, 200, "audit query failed: {audit}");
    for action in [
        "auth.signup",
        "auth.login",
        "project.create",
        "project.open",
        "run.start",
        "run.cancel",
    ] {
        assert!(
            audit.contains(&format!("\"action\":\"{action}\"")),
            "audit trail is missing {action}: {audit}"
        );
    }
    // The project actions record their target path.
    assert!(
        audit.contains("audited.pos"),
        "audit rows lost their target"
    );

    // Zero secret material at rest: the raw password and the raw session
    // token appear nowhere in control.db or its WAL (only argon2id and
    // BLAKE3 hashes do). This is the grep half of the AC.
    let token = a
        .cookie
        .split_once('=')
        .map(|(_, value)| value.to_owned())
        .expect("cookie has a value");
    assert_eq!(token.len(), 64, "session token is the 64-hex cookie value");
    let mut stored = Vec::new();
    for file in ["control.db", "control.db-wal"] {
        let path = shell.data_root.path().join(file);
        if path.exists() {
            stored.extend(std::fs::read(&path).expect("control database file reads"));
        }
    }
    assert!(!stored.is_empty(), "control.db was never written");
    let stored_text = String::from_utf8_lossy(&stored);
    assert!(
        !stored_text.contains(password),
        "the raw password is stored somewhere in control.db"
    );
    assert!(
        !stored_text.contains(&token),
        "the raw session token is stored somewhere in control.db"
    );
}

/// The server shell composes a worker pool per account runtime, and the
/// projects each pool serves are that account's open projects — the isolation
/// argument that keeps session state per account applies to the scheduler's
/// registries too (m1-s01/ADR-0007).
#[test]
fn each_account_runtime_runs_its_own_background_workers() {
    let shell = ServedShell::start();
    let a = shell.signup("workers-a@example.com", "owner password 1");
    let b = shell.signup("workers-b@example.com", "owner password 2");
    let alpha = project_path(&a, "alpha");
    let (status, _) = api(&shell, &a, "cmd", "project.create", &path_input(&alpha));
    assert_eq!(status, 200);

    let (status, health) = api(&shell, &a, "query", "health", "{}");
    assert_eq!(status, 200);
    assert!(
        health.contains("\"running\":true"),
        "the server shell must start a pool for an account runtime: {health}"
    );
    assert!(health.contains("\"registeredProjectCount\":0"), "{health}");

    let (status, body) = api(&shell, &a, "cmd", "project.open", &path_input(&alpha));
    assert_eq!(status, 200, "{body}");
    let (_, health_a) = api(&shell, &a, "query", "health", "{}");
    assert!(
        health_a.contains("\"registeredProjectCount\":1"),
        "{health_a}"
    );
    // B's pool never saw A's project: the registries are per runtime, and the
    // runtime is per account.
    let (_, health_b) = api(&shell, &b, "query", "health", "{}");
    assert!(
        health_b.contains("\"registeredProjectCount\":0"),
        "{health_b}"
    );

    let (status, body) = api(&shell, &a, "cmd", "project.close", &path_input(&alpha));
    assert_eq!(status, 200, "{body}");
    let (_, closed) = api(&shell, &a, "query", "health", "{}");
    assert!(closed.contains("\"registeredProjectCount\":0"), "{closed}");
    assert!(
        shell.state.shutdown_background_workers(),
        "every account pool must stop inside the shutdown budget"
    );
}

/// The served UI bundle: hashed assets are immutable, the app shell is
/// revalidated, SPA routes fall back to it, and none of it requires a
/// session (the login page must load logged-out).
#[test]
fn the_ui_bundle_is_served_with_cache_discipline() {
    let dist = tempfile::tempdir().expect("tempdir");
    std::fs::write(
        dist.path().join("index.html"),
        "<!doctype html><title>pos</title>",
    )
    .expect("writes");
    std::fs::create_dir_all(dist.path().join("assets")).expect("mkdir");
    std::fs::write(dist.path().join("assets/app-abc123.js"), "console.log(1)").expect("writes");

    let data_root = tempfile::tempdir().expect("tempdir");
    let state = ServerState::initialize(&ServerConfig {
        data_root: data_root.path().to_path_buf(),
        ui_dist: Some(dist.path().to_path_buf()),
        echo: None,
    })
    .expect("server state initializes");
    let served = Arc::clone(&state);
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
                .expect("addr sends");
            serve(listener, served, async {
                let _ = on_shutdown.await;
            })
            .await
            .expect("serve runs until shutdown");
        });
    });
    let addr = addr_rx
        .recv_timeout(std::time::Duration::from_secs(10))
        .expect("addr within 10s");
    let request = |target: &str| {
        let mut stream = TcpStream::connect(addr).expect("connects");
        stream
            .write_all(
                format!("GET {target} HTTP/1.1\r\nhost: x\r\nconnection: close\r\n\r\n").as_bytes(),
            )
            .expect("writes");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("reads");
        String::from_utf8_lossy(&raw).into_owned()
    };

    let index = request("/");
    assert!(index.contains("200 OK") && index.contains("<!doctype html>"));
    assert!(index.contains("no-cache"));
    let asset = request("/assets/app-abc123.js");
    assert!(asset.contains("200 OK") && asset.contains("immutable"));
    assert!(asset.contains("text/javascript"));
    let spa = request("/projects/some-route");
    assert!(spa.contains("200 OK") && spa.contains("<!doctype html>"));
    let missing = request("/missing.map");
    assert!(missing.contains("404"));

    let _ = shutdown.send(());
    let _ = thread.join();
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

fn read_until_contains(stream: &mut TcpStream, bytes: &mut Vec<u8>, needle: &[u8]) {
    let mut chunk = [0_u8; 1_024];
    for _ in 0..1_024 {
        if bytes.windows(needle.len()).any(|window| window == needle) {
            return;
        }
        let read = stream.read(&mut chunk).expect("read live SSE bytes");
        assert!(read > 0, "SSE closed before the expected live frame");
        bytes.extend_from_slice(&chunk[..read]);
    }
    panic!("SSE did not contain the expected frame within the bounded read loop");
}

fn read_echo_marker(stream: &mut TcpStream) -> String {
    let mut bytes = Vec::new();
    let mut chunk = [0_u8; 1_024];
    let header_end = loop {
        let read = stream.read(&mut chunk).expect("read Echo request");
        assert!(read > 0, "Echo request closed before its headers");
        bytes.extend_from_slice(&chunk[..read]);
        assert!(bytes.len() <= 1024 * 1024, "Echo request exceeds 1 MiB");
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
        .expect("Echo request carries content-length");
    while bytes.len() - header_end < content_length {
        let read = stream.read(&mut chunk).expect("read Echo request body");
        assert!(read > 0, "Echo request closed before its body");
        bytes.extend_from_slice(&chunk[..read]);
    }
    let request: serde_json::Value =
        serde_json::from_slice(&bytes[header_end..header_end + content_length])
            .expect("Echo request is JSON");
    request["messages"]
        .as_array()
        .and_then(|messages| messages.last())
        .and_then(|message| message["content"].as_str())
        .expect("Echo request has its marker")
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
        .expect("write Echo response");
}

fn hex_bytes(text: &str) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        *byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).expect("hex id");
    }
    bytes
}
