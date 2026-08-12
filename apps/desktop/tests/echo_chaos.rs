//! m0-s13 desktop process-recovery oracle: the actual debug desktop binary
//! is killed with SIGKILL at 20 seeded boundaries, then a fresh process
//! resumes the same `.pos` Run to one clean terminal history.

#![forbid(unsafe_code)]

#[cfg(unix)]
mod unix {
    use pos_api::{
        CommandName, CostRollupInput, LocalBootstrapConfig, ProjectCreateInput, ProjectExportInput,
        ProjectPathInput, QueryName, RunStepFrame, RunStepsInput, StreamName,
        bootstrap_local_runtime, input_json,
    };
    use serde_json::Value;
    use std::collections::BTreeSet;
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
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
        fn child_fault(self) -> (&'static str, u32) {
            match self {
                Self::AfterCommit(step) => ("after-commit", step),
                Self::DuringModelEffect => ("none", 0),
                Self::AfterCheckpoint(step) => ("after-checkpoint", step),
            }
        }
    }

    #[test]
    fn twenty_seeded_desktop_sigkills_resume_without_lost_or_duplicate_steps() {
        let boundaries = (0..CASE_COUNT).map(seed_boundary).collect::<Vec<_>>();
        assert_eq!(boundaries.iter().copied().collect::<BTreeSet<_>>().len(), 7);

        for (seed, boundary) in boundaries.into_iter().enumerate() {
            run_case(u32::try_from(seed).expect("20 seeds fit u32"), boundary);
        }
    }

    fn run_case(seed: u32, boundary: Boundary) {
        let directory = tempfile::tempdir().expect("tempdir");
        let project = directory.path().join(format!("desktop-{seed}.pos"));
        let project_text = project.display().to_string();
        let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
            directory.path().join("parent-packs"),
        ));
        runtime
            .command(
                CommandName::ProjectCreate.as_str(),
                &input_json(&ProjectCreateInput {
                    path: project_text.clone(),
                    name: Some(format!("Desktop chaos {seed}")),
                    template: "generic".to_owned(),
                })
                .expect("create input serializes"),
            )
            .expect("parent creates the project");

        let fault_marker = directory.path().join("fault-reached");
        let run_id_file = directory.path().join("run-id");
        let endpoint = ModelEndpoint::start(
            matches!(boundary, Boundary::DuringModelEffect),
            fault_marker.clone(),
        );
        let (fault_kind, step_index) = boundary.child_fault();
        let mut killed = spawn_child(
            "start",
            &project,
            &run_id_file.display().to_string(),
            &endpoint.base_url,
            fault_kind,
            step_index,
            &fault_marker,
        );
        wait_for_files(&mut killed, &[&run_id_file, &fault_marker], boundary);
        let run_id = std::fs::read_to_string(&run_id_file)
            .expect("child syncs the Run id")
            .trim()
            .to_owned();
        killed.kill().expect("SIGKILL the desktop child");
        let killed_status = killed.wait().expect("reap killed desktop child");
        assert!(!killed_status.success(), "SIGKILL cannot look successful");

        let mut resumed = spawn_child(
            "resume",
            &project,
            &run_id,
            &endpoint.base_url,
            "none",
            0,
            &directory.path().join("unused-resume-marker"),
        );
        let status = wait_for_exit(&mut resumed, WAIT_MAX);
        if !status.success() {
            let preserved = directory.keep();
            panic!(
                "resume child failed for seed {seed} at {boundary:?}; preserved {}",
                preserved.display()
            );
        }
        let expected_connections = if matches!(boundary, Boundary::DuringModelEffect) {
            2
        } else {
            1
        };
        assert_eq!(endpoint.finish(), expected_connections);

        assert_recovered(&runtime, &project_text, &run_id, directory.path(), seed);
    }

    fn spawn_child(
        action: &str,
        project: &Path,
        run_value: &str,
        base_url: &str,
        fault_kind: &str,
        step_index: u32,
        marker: &Path,
    ) -> Child {
        Command::new(env!("CARGO_BIN_EXE_pos-desktop"))
            .arg("--echo-chaos-child")
            .arg(action)
            .arg(project)
            .arg(run_value)
            .arg(base_url)
            .arg(fault_kind)
            .arg(step_index.to_string())
            .arg(marker)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::inherit())
            .spawn()
            .expect("spawn the real desktop binary")
    }

    fn wait_for_files(child: &mut Child, files: &[&Path], boundary: Boundary) {
        let deadline = Instant::now() + WAIT_MAX;
        while Instant::now() < deadline {
            if files.iter().all(|path| {
                path.metadata()
                    .is_ok_and(|metadata| metadata.is_file() && metadata.len() > 0)
            }) {
                return;
            }
            if let Some(status) = child.try_wait().expect("poll desktop child") {
                panic!("desktop child exited {status} before {boundary:?}");
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        let _ = child.kill();
        let _ = child.wait();
        panic!("desktop child did not reach {boundary:?} in 15 seconds");
    }

    fn wait_for_exit(child: &mut Child, budget: Duration) -> std::process::ExitStatus {
        let deadline = Instant::now() + budget;
        while Instant::now() < deadline {
            if let Some(status) = child.try_wait().expect("poll resume child") {
                return status;
            }
            std::thread::sleep(Duration::from_millis(10));
        }
        child.kill().expect("kill stuck resume child");
        let status = child.wait().expect("reap stuck resume child");
        panic!("resume child exceeded {budget:?}: {status}");
    }

    fn assert_recovered(
        runtime: &pos_api::LocalRuntime,
        project: &str,
        run_id: &str,
        root: &Path,
        seed: u32,
    ) {
        let stream_input = input_json(&RunStepsInput {
            path: project.to_owned(),
            run_id: run_id.to_owned(),
        })
        .expect("stream input serializes");
        let frames = runtime
            .stream_subscribe(StreamName::RunSteps.as_str(), &stream_input, None)
            .expect("recovered stream reads")
            .into_iter()
            .map(|frame| {
                serde_json::from_str::<RunStepFrame>(&frame.data_json).expect("Run frame decodes")
            })
            .collect::<Vec<_>>();
        assert_eq!(frames.len(), 3);
        assert_eq!(
            frames
                .iter()
                .map(|frame| frame.stream_seq)
                .collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        assert!(frames.last().is_some_and(|frame| {
            frame.terminal
                && frame.run_status == "done"
                && frame.validation_status.as_deref() == Some("passed")
        }));

        let cost = runtime
            .query_with_input(
                QueryName::CostRollup.as_str(),
                &input_json(&CostRollupInput {
                    path: Some(project.to_owned()),
                })
                .expect("cost input serializes"),
            )
            .expect("cost rollup reads");
        let cost: Value = serde_json::from_str(&cost).expect("cost report parses");
        assert_eq!(cost["totals"]["calls"], 1);
        assert_eq!(cost["rows"][0]["feature"], "echo");
        assert_eq!(cost["rows"][0]["agent"], "echo");

        let verify = runtime
            .query_with_input(
                QueryName::ProjectVerify.as_str(),
                &input_json(&ProjectPathInput {
                    path: project.to_owned(),
                })
                .expect("verify input serializes"),
            )
            .expect("verify recovered project");
        assert!(
            verify.contains("\"clean\":true"),
            "dirty recovery: {verify}"
        );

        let export = root.join(format!("desktop-{seed}-export.pos"));
        runtime
            .command(
                CommandName::ProjectExport.as_str(),
                &input_json(&ProjectExportInput {
                    path: project.to_owned(),
                    out: export.display().to_string(),
                })
                .expect("export input serializes"),
            )
            .expect("recovered project exports");
        let events = std::fs::read_to_string(export.join("events.jsonl"))
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
            .expect("set disconnect polling timeout");
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
