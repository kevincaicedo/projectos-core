//! Debug-only child process used by the m0-s13 `kill -9` acceptance suite.
//!
//! It composes the same `LocalRuntime` and Echo worker as the desktop IPC
//! shell, then either starts and waits to be killed or resumes and waits for
//! the durable terminal frame. Release builds do not include this entrypoint.

use pos_api::{
    CommandName, EchoFaultInjection, EchoRuntimeOptions, LocalBootstrapConfig, RunBudgetWire,
    RunResumeInput, RunStartInput, RunStepFrame, RunStepsInput, RunWorker, StreamName,
    bootstrap_local_runtime, input_json,
};
use std::ffi::OsString;
use std::path::{Path, PathBuf};
use std::process::ExitCode;
use std::time::{Duration, Instant};

const TERMINAL_WAIT: Duration = Duration::from_secs(15);

pub(crate) fn run(arguments: impl Iterator<Item = OsString>) -> ExitCode {
    match parse(arguments).and_then(execute) {
        Ok(()) => ExitCode::SUCCESS,
        Err(message) => {
            eprintln!("pos-desktop echo chaos child: {message}");
            ExitCode::FAILURE
        }
    }
}

struct ChildInput {
    action: String,
    project: PathBuf,
    run_value: String,
    base_url: String,
    fault_kind: String,
    step_index: u32,
    marker: PathBuf,
}

fn parse(mut arguments: impl Iterator<Item = OsString>) -> Result<ChildInput, String> {
    let mut next = |name: &str| {
        arguments
            .next()
            .ok_or_else(|| format!("missing {name}"))?
            .into_string()
            .map_err(|_| format!("{name} is not UTF-8"))
    };
    let action = next("action")?;
    let project = PathBuf::from(next("project path")?);
    let run_value = next("run id or output path")?;
    let base_url = next("Echo base URL")?;
    let fault_kind = next("fault kind")?;
    let step_index = next("fault step")?
        .parse::<u32>()
        .map_err(|error| format!("fault step is not u32: {error}"))?;
    let marker = PathBuf::from(next("fault marker")?);
    if arguments.next().is_some() {
        return Err("unexpected trailing arguments".to_owned());
    }
    if action != "start" && action != "resume" {
        return Err(format!("unknown action {action:?}"));
    }
    Ok(ChildInput {
        action,
        project,
        run_value,
        base_url,
        fault_kind,
        step_index,
        marker,
    })
}

fn execute(input: ChildInput) -> Result<(), String> {
    let fault = match input.fault_kind.as_str() {
        "none" => None,
        "after-commit" => Some(EchoFaultInjection::AfterCommit {
            step_index: input.step_index,
            marker: input.marker,
        }),
        "after-checkpoint" => Some(EchoFaultInjection::AfterCheckpoint {
            step_index: input.step_index,
            marker: input.marker,
        }),
        other => return Err(format!("unknown fault kind {other:?}")),
    };
    let mut echo = EchoRuntimeOptions::loopback(input.base_url, "echo-chaos-fixture");
    if let Some(fault) = fault {
        echo = echo.with_fault(fault);
    }
    let pack_root = input
        .project
        .parent()
        .unwrap_or_else(|| Path::new("."))
        .join("chaos-packs");
    let runtime =
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(pack_root).with_echo(echo));
    if input.action == "start" {
        let started = runtime
            .command(
                CommandName::RunStart.as_str(),
                &input_json(&RunStartInput {
                    path: input.project.display().to_string(),
                    worker: RunWorker::Echo,
                    autonomy_level: 2,
                    budget: echo_budget(),
                    tool_grants: Vec::new(),
                    parent_run_id: None,
                })
                .map_err(|error| error.to_string())?,
            )
            .map_err(|error| error.to_string())?;
        let run_id = json_field(&started, "runId")?;
        write_synced(Path::new(&input.run_value), run_id.as_bytes())?;
        loop {
            std::thread::park_timeout(Duration::from_secs(60));
        }
    }

    let run_id = input.run_value;
    runtime
        .command(
            CommandName::RunResume.as_str(),
            &input_json(&RunResumeInput {
                path: input.project.display().to_string(),
                run_id: run_id.clone(),
            })
            .map_err(|error| error.to_string())?,
        )
        .map_err(|error| error.to_string())?;
    let stream_input = input_json(&RunStepsInput {
        path: input.project.display().to_string(),
        run_id,
    })
    .map_err(|error| error.to_string())?;
    let deadline = Instant::now() + TERMINAL_WAIT;
    while Instant::now() < deadline {
        let frames = runtime
            .stream_subscribe(StreamName::RunSteps.as_str(), &stream_input, None)
            .map_err(|error| error.to_string())?;
        if frames.last().is_some_and(|frame| {
            serde_json::from_str::<RunStepFrame>(&frame.data_json)
                .is_ok_and(|decoded| decoded.terminal)
        }) {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(25));
    }
    Err("resumed Echo Run did not reach a terminal frame in 15 seconds".to_owned())
}

fn write_synced(path: &Path, bytes: &[u8]) -> Result<(), String> {
    use std::io::Write;

    let mut file = std::fs::File::create(path)
        .map_err(|error| format!("create {}: {error}", path.display()))?;
    file.write_all(bytes)
        .and_then(|()| file.sync_all())
        .map_err(|error| format!("write {}: {error}", path.display()))
}

fn json_field(body: &str, name: &str) -> Result<String, String> {
    serde_json::from_str::<serde_json::Value>(body)
        .map_err(|error| format!("Run report is invalid JSON: {error}"))?
        .get(name)
        .and_then(serde_json::Value::as_str)
        .map(str::to_owned)
        .ok_or_else(|| format!("Run report omitted {name}"))
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
