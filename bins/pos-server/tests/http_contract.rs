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
    ProjectSeedInput, QueryName, StreamName, bootstrap_local_runtime, input_json,
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
    for query in [QueryName::JobList, QueryName::CostRollup] {
        let name = query.as_str();
        let direct = served
            .runtime
            .query_with_input(name, "{}")
            .expect_err("the engine has not landed; success would be a lie");
        let (status, _, body) = served.get(&format!("/api/query/{name}"));
        assert_eq!(status, 501, "{name} maps not_yet_supported to 501");
        assert_eq!(body, direct.to_json(), "{name} envelope reshaped");
    }
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

/// Run-lifecycle commands and the stream surface answer with their typed
/// envelopes and deliberate statuses over the real transport.
#[test]
fn later_story_surfaces_answer_honestly_over_the_real_transport() {
    let served = ServedApi::start();
    for command in [
        CommandName::RunStart,
        CommandName::RunCancel,
        CommandName::RunPause,
        CommandName::RunResume,
    ] {
        let name = command.as_str();
        let direct = served
            .runtime
            .command(name, "{}")
            .expect_err("the run engine has not landed");
        let (status, content_type, body) = served.post(&format!("/api/cmd/{name}"), "{}");
        assert_eq!(status, 501, "{name} maps not_yet_supported to 501");
        assert_eq!(content_type, "application/json");
        assert_eq!(body, direct.to_json());
    }
    let name = StreamName::RunSteps.as_str();
    let (status, content_type, body) = served.get(&format!("/api/stream/{name}"));
    assert_eq!(status, 501);
    assert_eq!(content_type, "application/json");
    assert!(body.contains("\"code\":\"not_yet_supported\""));
    assert!(body.contains("m0-s13"));
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
