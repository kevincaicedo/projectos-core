//! The three M0 gate scenarios and their datasets.
//!
//! Every scenario drives the product through `pos-api`, the same seam a shell
//! uses (L12). A scenario that measured a private fast path would produce a
//! number no user can experience, which is the failure mode the claim ledger
//! (master plan §24) exists to prevent.

use pos_api::{
    CommandName, LocalBootstrapConfig, ProjectCreateInput, ProjectPathInput, ProjectSeedInput,
    bootstrap_local_runtime, input_json,
};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::Instant;

/// The §18 project-open corpus. Fixed here rather than passed in: a gate whose
/// dataset size is an argument is a gate anyone can pass.
pub const PROJECT_OPEN_EVENT_COUNT: u64 = 1_000_000;

/// The §18 cold-start corpus.
pub const COLD_START_PROJECT_COUNT: u32 = 50;

/// Events per cold-start project. Small on purpose: the gate asks what
/// *fifty projects* cost at startup, not what one large one costs.
const COLD_START_EVENTS_PER_PROJECT: u64 = 200;

/// Deterministic seed shared by every dataset, so two runs on two days
/// measure the same bytes.
const DATASET_SEED: u64 = 0x504f_535f_4245_4e43;

/// A scenario refuses rather than measures when its inputs are wrong.
#[derive(Debug)]
pub struct ScenarioError(pub String);

impl std::fmt::Display for ScenarioError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for ScenarioError {}

fn fail(message: impl Into<String>) -> ScenarioError {
    ScenarioError(message.into())
}

/// Builds (or reuses) the 1M-event project. Reuse is keyed on the directory
/// existing: a dataset is deterministic, so rebuilding it would only cost
/// twenty seconds to produce identical bytes.
pub fn ensure_project_open_dataset(dataset: &Path) -> Result<PathBuf, ScenarioError> {
    let project = dataset.join("project-open-1m.pos");
    if project.is_dir() {
        return Ok(project);
    }
    std::fs::create_dir_all(dataset)
        .map_err(|error| fail(format!("create dataset directory: {error}")))?;
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(dataset.join("packs")));
    let path = project.display().to_string();
    runtime
        .command(
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("pos-bench project-open".to_owned()),
                template: "generic".to_owned(),
            })
            .map_err(|error| fail(error.to_json()))?,
        )
        .map_err(|error| fail(error.to_json()))?;
    runtime
        .command(
            CommandName::ProjectSeedSynthetic.as_str(),
            &input_json(&ProjectSeedInput {
                path,
                event_count: PROJECT_OPEN_EVENT_COUNT,
                seed: DATASET_SEED,
            })
            .map_err(|error| fail(error.to_json()))?,
        )
        .map_err(|error| fail(error.to_json()))?;
    Ok(project)
}

/// Builds (or reuses) the fifty-project cold-start corpus.
pub fn ensure_cold_start_dataset(dataset: &Path) -> Result<PathBuf, ScenarioError> {
    let root = dataset.join("cold-start-50");
    let marker = root.join(".complete");
    if marker.is_file() {
        return Ok(root);
    }
    std::fs::create_dir_all(&root)
        .map_err(|error| fail(format!("create dataset directory: {error}")))?;
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(dataset.join("packs")));
    for index in 0..COLD_START_PROJECT_COUNT {
        let path = root
            .join(format!("project-{index:02}.pos"))
            .display()
            .to_string();
        runtime
            .command(
                CommandName::ProjectCreate.as_str(),
                &input_json(&ProjectCreateInput {
                    path: path.clone(),
                    name: Some(format!("pos-bench cold start {index:02}")),
                    template: "generic".to_owned(),
                })
                .map_err(|error| fail(error.to_json()))?,
            )
            .map_err(|error| fail(error.to_json()))?;
        runtime
            .command(
                CommandName::ProjectSeedSynthetic.as_str(),
                &input_json(&ProjectSeedInput {
                    path,
                    event_count: COLD_START_EVENTS_PER_PROJECT,
                    seed: DATASET_SEED + u64::from(index),
                })
                .map_err(|error| fail(error.to_json()))?,
            )
            .map_err(|error| fail(error.to_json()))?;
    }
    std::fs::write(&marker, b"pos-bench cold-start dataset\n")
        .map_err(|error| fail(format!("write dataset marker: {error}")))?;
    Ok(root)
}

/// One replicate of the project-open gate, measured **in this process** — the
/// parent spawns a fresh child per replicate so no in-process cache survives
/// between them. Returns microseconds.
pub fn open_once(project: &Path) -> Result<u64, ScenarioError> {
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        project.parent().unwrap_or(project).join("packs"),
    ));
    let input = input_json(&ProjectPathInput {
        path: project.display().to_string(),
    })
    .map_err(|error| fail(error.to_json()))?;
    let started = Instant::now();
    runtime
        .command(CommandName::ProjectOpen.as_str(), &input)
        .map_err(|error| fail(error.to_json()))?;
    let elapsed = started.elapsed();
    u64::try_from(elapsed.as_micros())
        .map_err(|_| fail("an open took longer than u64 microseconds"))
}

/// Spawns one cold child per replicate. Page cache stays warm across
/// replicates — that is stated in the artifact rather than pretended away,
/// because the harness cannot purge it without privileges a bench must not
/// require.
pub fn measure_project_open(
    self_binary: &Path,
    project: &Path,
    replicates: u32,
) -> Result<Vec<f64>, ScenarioError> {
    let mut samples = Vec::new();
    for _ in 0..replicates {
        let output = Command::new(self_binary)
            .arg("replicate")
            .arg("--project")
            .arg(project)
            .output()
            .map_err(|error| fail(format!("spawn a cold replicate: {error}")))?;
        if !output.status.success() {
            return Err(fail(format!(
                "replicate failed: {}",
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        }
        let text = String::from_utf8_lossy(&output.stdout);
        let micros: u64 = text
            .trim()
            .strip_prefix("micros=")
            .and_then(|value| value.parse().ok())
            .ok_or_else(|| fail(format!("a replicate printed {text:?}, not micros=<n>")))?;
        #[expect(
            clippy::cast_precision_loss,
            reason = "a sample is milliseconds at f64 precision; the raw microseconds are recorded too"
        )]
        samples.push(micros as f64 / 1000.0);
    }
    Ok(samples)
}

/// Launches the packaged desktop shell under its startup probe and measures
/// `exec` → Tauri `Ready`: the window, its webview, and the in-process core
/// runtime all exist. That is the shell half of "time to interactive"; the
/// page half is measured in the page (see `--ui-measurements`).
pub fn measure_desktop_startup(
    desktop_binary: &Path,
    projects_root: &Path,
    replicates: u32,
) -> Result<Vec<f64>, ScenarioError> {
    if !desktop_binary.is_file() {
        return Err(fail(format!(
            "no desktop binary at {} — build it with `just package-unsigned` or \
             `cargo build --release -p pos-desktop`",
            desktop_binary.display()
        )));
    }
    let mut samples = Vec::new();
    for _ in 0..replicates {
        let started = Instant::now();
        let output = Command::new(desktop_binary)
            .env("POS_STARTUP_PROBE", "1")
            .env("POS_BENCH_PROJECTS_ROOT", projects_root)
            .output()
            .map_err(|error| fail(format!("launch the desktop shell: {error}")))?;
        let wall = started.elapsed();
        let text = String::from_utf8_lossy(&output.stdout);
        let reported = text
            .lines()
            .find_map(|line| line.trim().strip_prefix("pos-desktop: startup_probe_ms "))
            .and_then(|value| value.trim().parse::<u64>().ok());
        let Some(reported) = reported else {
            return Err(fail(format!(
                "the shell did not print its startup probe (exit {:?}); stderr: {}",
                output.status.code(),
                String::from_utf8_lossy(&output.stderr).trim()
            )));
        };
        // The shell reports the instant it became ready; the wall time is kept
        // as a sanity bound so a probe that lied would be visible.
        #[expect(
            clippy::cast_precision_loss,
            reason = "milliseconds at f64 precision; both values are recorded"
        )]
        let wall_ms = wall.as_millis() as f64;
        #[expect(clippy::cast_precision_loss, reason = "as above")]
        let reported_ms = reported as f64;
        if reported_ms > wall_ms + 1.0 {
            return Err(fail(
                "the startup probe reported more time than the process was alive".to_owned(),
            ));
        }
        samples.push(reported_ms);
    }
    Ok(samples)
}

/// Reads the measurements the Playwright suite writes from inside the page.
/// The in-page technique is not a preference: a per-step Playwright call
/// measures the WebDriver channel, which is how the first attempt at the §18
/// interaction gate read 400 ms for sub-frame work.
pub fn read_ui_measurements(path: &Path, key: &str) -> Result<Vec<f64>, ScenarioError> {
    let text = std::fs::read_to_string(path).map_err(|error| {
        fail(format!(
            "read {} : {error} — produce it with `just e2e`",
            path.display()
        ))
    })?;
    let value: serde_json::Value = serde_json::from_str(&text)
        .map_err(|error| fail(format!("parse measurements: {error}")))?;
    let samples = value
        .get(key)
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| fail(format!("{} has no {key} array", path.display())))?;
    samples
        .iter()
        .map(|sample| {
            sample
                .as_f64()
                .ok_or_else(|| fail(format!("{key} carries a non-numeric sample")))
        })
        .collect()
}
