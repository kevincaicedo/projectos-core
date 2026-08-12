//! m0-s13 web-process recovery oracle: the actual `pos-server` executable is
//! killed with SIGKILL at 20 seeded Echo boundaries, restarted over the same
//! control/project data, and driven through authenticated HTTP/SSE to proof.

#![forbid(unsafe_code)]

#[cfg(unix)]
mod unix {
    use pos_api::{
        CommandName, CostRollupInput, ProjectCreateInput, ProjectExportInput, ProjectPathInput,
        QueryName, RunBudgetWire, RunResumeInput, RunStartInput, RunStepsInput, RunWorker,
        StreamName, input_json,
    };
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::io::{Read, Write};
    use std::net::{SocketAddr, TcpListener, TcpStream};
    use std::path::{Path, PathBuf};
    use std::process::{Child, Command, Stdio};
    use std::thread::JoinHandle;
    use std::time::{Duration, Instant};

    const CASE_COUNT: u32 = 20;
    const WAIT_MAX: Duration = Duration::from_secs(15);

    #[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
    enum Boundary {
        AfterCommit(u32),
        DuringModelEffect,
        AfterCheckpoint(u32),
    }

    impl Boundary {
        fn server_fault(self) -> (&'static str, u32) {
            match self {
                Self::AfterCommit(step) => ("after-commit", step),
                Self::DuringModelEffect => ("none", 0),
                Self::AfterCheckpoint(step) => ("after-checkpoint", step),
            }
        }
    }

    #[test]
    fn twenty_seeded_server_sigkills_resume_over_authenticated_http_without_lost_steps() {
        let boundaries = (0..CASE_COUNT).map(seed_boundary).collect::<Vec<_>>();
        assert_eq!(boundaries.iter().copied().collect::<BTreeSet<_>>().len(), 7);
        for (seed, boundary) in boundaries.into_iter().enumerate() {
            run_case(u32::try_from(seed).expect("20 seeds fit u32"), boundary);
        }
    }

    fn run_case(seed: u32, boundary: Boundary) {
        let directory = tempfile::tempdir().expect("tempdir");
        let data_root = directory.path().join("server-data");
        let fault_marker = directory.path().join("fault-reached");
        let endpoint = ModelEndpoint::start(
            matches!(boundary, Boundary::DuringModelEffect),
            fault_marker.clone(),
        );
        let address = unused_address();
        let (fault_kind, step_index) = boundary.server_fault();
        let mut killed = spawn_server(
            address,
            &data_root,
            &endpoint.base_url,
            fault_kind,
            step_index,
            &fault_marker,
        );
        wait_for_server(&mut killed, address, boundary);
        let signup = request(
            address,
            "POST",
            "/auth/signup",
            "",
            &format!(
                "{{\"email\":\"chaos-{seed}@example.com\",\"password\":\"chaos password {seed} long\"}}"
            ),
        );
        assert_eq!(signup.status, 200, "signup failed: {}", signup.body);
        let cookie = signup
            .headers
            .iter()
            .find(|(name, _)| name == "set-cookie")
            .and_then(|(_, value)| value.split_once(';').map(|(pair, _)| pair.to_owned()))
            .expect("signup returns a session cookie");
        let projects_root = field(&signup.body, "projectsRoot");
        let project = format!("{projects_root}/server-{seed}.pos");
        let created = api_command(
            address,
            &cookie,
            CommandName::ProjectCreate,
            &input_json(&ProjectCreateInput {
                path: project.clone(),
                name: Some(format!("Server chaos {seed}")),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        );
        assert_eq!(created.status, 200, "create failed: {}", created.body);
        let started = api_command(
            address,
            &cookie,
            CommandName::RunStart,
            &input_json(&RunStartInput {
                path: project.clone(),
                worker: RunWorker::Echo,
                autonomy_level: 2,
                budget: echo_budget(),
                tool_grants: Vec::new(),
                parent_run_id: None,
            })
            .expect("start input serializes"),
        );
        assert_eq!(started.status, 200, "start failed: {}", started.body);
        let run_id = field(&started.body, "runId");
        wait_for_marker(&mut killed, &fault_marker, boundary);
        killed.kill().expect("SIGKILL the web server child");
        let killed_status = killed.wait().expect("reap killed web server child");
        assert!(!killed_status.success(), "SIGKILL cannot look successful");

        let mut resumed = spawn_server(
            address,
            &data_root,
            &endpoint.base_url,
            "none",
            0,
            &directory.path().join("unused-resume-marker"),
        );
        wait_for_server(&mut resumed, address, boundary);
        let resumed_report = api_command(
            address,
            &cookie,
            CommandName::RunResume,
            &input_json(&RunResumeInput {
                path: project.clone(),
                run_id: run_id.clone(),
            })
            .expect("resume input serializes"),
        );
        if resumed_report.status != 200 {
            let preserved = directory.keep();
            let _ = resumed.kill();
            let _ = resumed.wait();
            panic!(
                "resume failed for seed {seed} at {boundary:?}: {}; preserved {}",
                resumed_report.body,
                preserved.display()
            );
        }

        let stream_input = input_json(&RunStepsInput {
            path: project.clone(),
            run_id: run_id.clone(),
        })
        .expect("stream input serializes");
        let feed = request(
            address,
            "GET",
            &format!(
                "/api/stream/{}?input={}",
                StreamName::RunSteps.as_str(),
                percent_encode(&stream_input)
            ),
            &cookie,
            "",
        );
        assert_eq!(feed.status, 200, "Run feed failed: {}", feed.body);
        for seq in 1..=3 {
            assert_eq!(
                feed.body.matches(&format!("\"streamSeq\":{seq}")).count(),
                1,
                "seed {seed} lost or duplicated stream seq {seq}: {}",
                feed.body
            );
        }
        assert!(feed.body.contains("\"runStatus\":\"done\""));
        assert!(feed.body.contains("\"validationStatus\":\"passed\""));

        let cost = api_query(
            address,
            &cookie,
            QueryName::CostRollup,
            &input_json(&CostRollupInput {
                path: Some(project.clone()),
            })
            .expect("cost input serializes"),
        );
        assert_eq!(cost.status, 200, "cost failed: {}", cost.body);
        let cost_json: Value = serde_json::from_str(&cost.body).expect("cost report parses");
        assert_eq!(cost_json["totals"]["calls"], 1);
        assert_eq!(cost_json["rows"][0]["feature"], "echo");
        assert_eq!(cost_json["rows"][0]["agent"], "echo");

        let verify = api_query(
            address,
            &cookie,
            QueryName::ProjectVerify,
            &input_json(&ProjectPathInput {
                path: project.clone(),
            })
            .expect("verify input serializes"),
        );
        assert_eq!(verify.status, 200, "verify failed: {}", verify.body);
        assert!(verify.body.contains("\"clean\":true"));

        let export = format!("{projects_root}/server-{seed}-export.pos");
        let exported = api_command(
            address,
            &cookie,
            CommandName::ProjectExport,
            &input_json(&ProjectExportInput {
                path: project,
                out: export.clone(),
            })
            .expect("export input serializes"),
        );
        assert_eq!(exported.status, 200, "export failed: {}", exported.body);
        let events = std::fs::read_to_string(Path::new(&export).join("events.jsonl"))
            .expect("exported event history reads");
        for (kind, expected) in [
            ("RunStarted", 1),
            ("RunStepCommitted", 3),
            ("RunToolEffectRecorded", 3),
            ("RunCheckpointSaved", 3),
            ("RunFinished", 1),
            ("ModelCallCompleted", 1),
        ] {
            assert_eq!(
                events.matches(&format!("\"kind\":\"{kind}\"")).count(),
                expected,
                "seed {seed} has wrong {kind} cardinality"
            );
        }

        let expected_connections = if matches!(boundary, Boundary::DuringModelEffect) {
            2
        } else {
            1
        };
        assert_eq!(endpoint.finish(), expected_connections);
        stop_server(&mut resumed);
    }

    struct Response {
        status: u16,
        headers: Vec<(String, String)>,
        body: String,
    }

    fn request(
        address: SocketAddr,
        method: &str,
        target: &str,
        cookie: &str,
        body: &str,
    ) -> Response {
        let mut stream = TcpStream::connect(address).expect("connect to server child");
        stream
            .set_read_timeout(Some(WAIT_MAX))
            .expect("set HTTP read timeout");
        let mut wire = format!(
            "{method} {target} HTTP/1.1\r\nhost: {address}\r\nconnection: close\r\ncontent-length: {}\r\n",
            body.len()
        );
        if !cookie.is_empty() {
            wire.push_str(&format!("cookie: {cookie}\r\n"));
        }
        wire.push_str("\r\n");
        wire.push_str(body);
        stream
            .write_all(wire.as_bytes())
            .expect("write HTTP request");
        let mut raw = Vec::new();
        stream.read_to_end(&mut raw).expect("read HTTP response");
        let text = String::from_utf8_lossy(&raw);
        let (head, response_body) = text
            .split_once("\r\n\r\n")
            .expect("HTTP response has headers");
        let status = head
            .lines()
            .next()
            .and_then(|line| line.split_whitespace().nth(1))
            .and_then(|code| code.parse::<u16>().ok())
            .expect("HTTP status parses");
        let headers = head
            .lines()
            .skip(1)
            .filter_map(|line| {
                let (name, value) = line.split_once(':')?;
                Some((name.trim().to_ascii_lowercase(), value.trim().to_owned()))
            })
            .collect();
        Response {
            status,
            headers,
            body: response_body.to_owned(),
        }
    }

    fn api_command(
        address: SocketAddr,
        cookie: &str,
        command: CommandName,
        input: &str,
    ) -> Response {
        request(
            address,
            "POST",
            &format!("/api/cmd/{}", command.as_str()),
            cookie,
            input,
        )
    }

    fn api_query(address: SocketAddr, cookie: &str, query: QueryName, input: &str) -> Response {
        request(
            address,
            "GET",
            &format!(
                "/api/query/{}?input={}",
                query.as_str(),
                percent_encode(input)
            ),
            cookie,
            "",
        )
    }

    fn spawn_server(
        address: SocketAddr,
        data_root: &Path,
        base_url: &str,
        fault_kind: &str,
        step_index: u32,
        marker: &Path,
    ) -> Child {
        Command::new(env!("CARGO_BIN_EXE_pos-server"))
            .arg("--echo-chaos-server")
            .arg(base_url)
            .arg(fault_kind)
            .arg(step_index.to_string())
            .arg(marker)
            .env("POS_SERVER_ADDR", address.to_string())
            .env("POS_SERVER_DATA_DIR", data_root)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .spawn()
            .expect("spawn the real pos-server binary")
    }

    fn wait_for_server(child: &mut Child, address: SocketAddr, boundary: Boundary) {
        let deadline = Instant::now() + WAIT_MAX;
        while Instant::now() < deadline {
            if TcpStream::connect(address).is_ok() {
                return;
            }
            if let Some(status) = child.try_wait().expect("poll server child") {
                panic!("server child exited {status} before startup for {boundary:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("server child did not listen in 15 seconds for {boundary:?}");
    }

    fn wait_for_marker(child: &mut Child, marker: &Path, boundary: Boundary) {
        let deadline = Instant::now() + WAIT_MAX;
        while Instant::now() < deadline {
            if marker
                .metadata()
                .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
            {
                return;
            }
            if let Some(status) = child.try_wait().expect("poll faulted server child") {
                panic!("server child exited {status} before {boundary:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("server child did not reach {boundary:?} in 15 seconds");
    }

    fn stop_server(child: &mut Child) {
        let status = Command::new("kill")
            .arg("-TERM")
            .arg(child.id().to_string())
            .status()
            .expect("send SIGTERM to resumed server");
        assert!(status.success(), "SIGTERM command failed");
        let deadline = Instant::now() + Duration::from_secs(5);
        while Instant::now() < deadline {
            if child
                .try_wait()
                .expect("poll graceful server stop")
                .is_some()
            {
                return;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.kill().expect("kill server that ignored SIGTERM");
        let _ = child.wait();
        panic!("resumed server did not stop after SIGTERM");
    }

    fn unused_address() -> SocketAddr {
        let listener = TcpListener::bind("127.0.0.1:0").expect("reserve loopback port");
        listener.local_addr().expect("reserved port has address")
    }

    fn field(body: &str, name: &str) -> String {
        serde_json::from_str::<Value>(body)
            .expect("response parses")
            .get(name)
            .and_then(Value::as_str)
            .unwrap_or_else(|| panic!("response omitted {name}: {body}"))
            .to_owned()
    }

    fn percent_encode(text: &str) -> String {
        let mut encoded = String::new();
        for byte in text.bytes() {
            match byte {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    encoded.push(char::from(byte));
                }
                other => encoded.push_str(&format!("%{other:02X}")),
            }
        }
        encoded
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

    fn seed_boundary(seed: u32) -> Boundary {
        const ALL: [Boundary; 7] = [
            Boundary::AfterCommit(0),
            Boundary::AfterCommit(1),
            Boundary::AfterCommit(2),
            Boundary::DuringModelEffect,
            Boundary::AfterCheckpoint(0),
            Boundary::AfterCheckpoint(1),
            Boundary::AfterCheckpoint(2),
        ];
        let index =
            usize::try_from((seed.wrapping_mul(17).wrapping_add(3)) % 7).expect("index fits usize");
        ALL[index]
    }

    struct ModelEndpoint {
        base_url: String,
        thread: JoinHandle<usize>,
    }

    impl ModelEndpoint {
        fn start(interrupt_first: bool, effect_marker: PathBuf) -> Self {
            let listener = TcpListener::bind("127.0.0.1:0").expect("model fixture binds");
            let address = listener.local_addr().expect("fixture has address");
            let thread = std::thread::spawn(move || {
                let mut connections = 0;
                if interrupt_first {
                    let (mut stream, _) = listener.accept().expect("first model call connects");
                    let _ = read_marker(&mut stream);
                    connections += 1;
                    write_synced(&effect_marker, b"during-model-effect\n");
                    wait_for_disconnect(&mut stream);
                }
                let (mut stream, _) = listener.accept().expect("completing model call connects");
                let marker = read_marker(&mut stream);
                connections += 1;
                write_response(&mut stream, &marker);
                connections
            });
            Self {
                base_url: format!("http://{address}"),
                thread,
            }
        }

        fn finish(self) -> usize {
            self.thread.join().expect("model fixture exits")
        }
    }

    fn wait_for_disconnect(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_millis(100)))
            .expect("set disconnect timeout");
        let mut byte = [0_u8; 1];
        loop {
            match stream.read(&mut byte) {
                Ok(0) => return,
                Ok(_) => {}
                Err(error)
                    if error.kind() == std::io::ErrorKind::WouldBlock
                        || error.kind() == std::io::ErrorKind::TimedOut => {}
                Err(_) => return,
            }
        }
    }

    fn write_synced(path: &Path, bytes: &[u8]) {
        let mut file = std::fs::File::create(path).expect("create effect marker");
        file.write_all(bytes).expect("write effect marker");
        file.sync_all().expect("sync effect marker");
    }

    fn read_marker(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1_024];
        let header_end = loop {
            let read = stream.read(&mut chunk).expect("read model request");
            assert!(read > 0, "model request closed before headers");
            bytes.extend_from_slice(&chunk[..read]);
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
            .expect("model request has content-length");
        while bytes.len() - header_end < content_length {
            let read = stream.read(&mut chunk).expect("read model request body");
            assert!(read > 0, "model request closed before body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        let request: Value =
            serde_json::from_slice(&bytes[header_end..header_end + content_length])
                .expect("model request parses");
        request["messages"]
            .as_array()
            .and_then(|messages| messages.last())
            .and_then(|message| message["content"].as_str())
            .expect("model marker exists")
            .to_owned()
    }

    fn write_response(stream: &mut TcpStream, marker: &str) {
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
}
