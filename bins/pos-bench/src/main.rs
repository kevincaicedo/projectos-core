//! # pos-bench
//!
//! The measurement harness for the §18 M0 gate rows (m0-s16). It produces
//! machine-stamped artifacts — header, every raw replicate, aggregation
//! method, threshold, verdict — and it decides for itself whether a run is
//! binding evidence or an early warning ([`artifact`]).
//!
//! Charter: master plan §19/§24. A number without its artifact is a vibe.

#![forbid(unsafe_code)]

mod artifact;
mod scenarios;

use artifact::{ArtifactHeader, Classification, Environment};
use clap::{Parser, Subcommand, ValueEnum};
use std::path::{Path, PathBuf};
use std::process::ExitCode;

/// Replicates a §18 row must carry. Five is the smallest count that makes a
/// p95 mean anything at this sample size while keeping a 1M-event scenario
/// inside a coffee break.
const REPLICATE_COUNT_DEFAULT: u32 = 5;

#[derive(Parser)]
#[command(
    name = "pos-bench",
    about = "ProjectOS gate measurement harness (m0-s16)"
)]
struct Cli {
    #[command(subcommand)]
    command: BenchCommand,
}

#[derive(Subcommand)]
enum BenchCommand {
    /// Runs one scenario and writes its JSON + Markdown artifact.
    Run {
        #[arg(long)]
        scenario: ScenarioName,
        #[arg(long, default_value = "../docs/gates/m0")]
        out: PathBuf,
        #[arg(long, default_value = "RM-LAPTOP-01")]
        machine: String,
        #[arg(long, default_value_t = REPLICATE_COUNT_DEFAULT)]
        replicates: u32,
        /// Where datasets are built and cached. Deterministic, so a cached
        /// dataset and a rebuilt one are the same bytes.
        #[arg(long, default_value = "target/bench-data")]
        dataset: PathBuf,
        /// The packaged or release desktop executable, for the cold-start row.
        #[arg(long, default_value = "target/release/pos-desktop")]
        desktop_binary: PathBuf,
        /// The in-page measurements the Playwright suite writes.
        #[arg(long, default_value = "apps/ui/e2e-artifacts/ui-measurements.json")]
        ui_measurements: PathBuf,
        /// What else was running. Recorded verbatim in the artifact, because
        /// "nothing" is a claim and the reader deserves to judge it.
        #[arg(long, default_value = "developer shell only; no build running")]
        background_workload: String,
    },
    /// One cold replicate of the project-open row. Spawned by `run`; a fresh
    /// process per replicate is how "cold" is made true rather than asserted.
    Replicate {
        #[arg(long)]
        project: PathBuf,
    },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, ValueEnum)]
enum ScenarioName {
    /// §18: project open < 500 ms @ 1M synthetic events (snapshot + tail).
    ProjectOpen1m,
    /// §18: desktop cold start → interactive < 1.5 s @ 50 projects.
    DesktopColdStart50,
    /// §18: UI interaction p95 < 100 ms (palette open, project switch).
    UiInteractionP95,
}

impl ScenarioName {
    const fn gate_id(self) -> &'static str {
        match self {
            Self::ProjectOpen1m => "m0.project-open-1m",
            Self::DesktopColdStart50 => "m0.desktop-cold-start-50",
            Self::UiInteractionP95 => "m0.ui-interaction-p95",
        }
    }

    const fn story_id(self) -> &'static str {
        match self {
            // The cold-start row is m0-s07's acceptance criterion, measured by
            // this harness; the other two are m0-s16's own.
            Self::DesktopColdStart50 => "m0-s07",
            _ => "m0-s16",
        }
    }
}

/// One measured series inside an artifact: raw samples, how they were reduced,
/// and what the reduction is compared against.
struct Series {
    label: &'static str,
    unit: &'static str,
    samples: Vec<f64>,
    aggregation: &'static str,
    threshold_ms: Option<f64>,
}

impl Series {
    fn value(&self) -> f64 {
        match self.aggregation {
            "p95" => percentile(&self.samples, 0.95),
            _ => median(&self.samples),
        }
    }

    fn verdict(&self) -> &'static str {
        match self.threshold_ms {
            None => "informational",
            Some(threshold) if self.value() < threshold => "pass",
            Some(_) => "fail",
        }
    }
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    match cli.command {
        BenchCommand::Replicate { project } => match scenarios::open_once(&project) {
            Ok(micros) => {
                println!("micros={micros}");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("pos-bench: {error}");
                ExitCode::FAILURE
            }
        },
        BenchCommand::Run {
            scenario,
            out,
            machine,
            replicates,
            dataset,
            desktop_binary,
            ui_measurements,
            background_workload,
        } => {
            let request = RunRequest {
                scenario,
                out,
                machine,
                replicates,
                dataset,
                desktop_binary,
                ui_measurements,
                background_workload,
            };
            match run(&request) {
                Ok(verdict) => {
                    // A failed gate is a STOP, and the harness says so with its
                    // exit code rather than leaving it in a file nobody reads.
                    if verdict {
                        ExitCode::SUCCESS
                    } else {
                        ExitCode::FAILURE
                    }
                }
                Err(error) => {
                    eprintln!("pos-bench: {error}");
                    ExitCode::FAILURE
                }
            }
        }
    }
}

struct RunRequest {
    scenario: ScenarioName,
    out: PathBuf,
    machine: String,
    replicates: u32,
    dataset: PathBuf,
    desktop_binary: PathBuf,
    ui_measurements: PathBuf,
    background_workload: String,
}

fn run(request: &RunRequest) -> Result<bool, Box<dyn std::error::Error>> {
    if request.replicates == 0 {
        return Err(Box::new(scenarios::ScenarioError(
            "a gate row needs at least one replicate".to_owned(),
        )));
    }
    let root = std::env::current_dir()?;
    let registry = artifact::find_registry(&root).ok_or_else(|| {
        scenarios::ScenarioError("docs/reference-machines.md not found".to_owned())
    })?;
    let environment = Environment::probe(&root);
    let (series, dataset_hash) = measure(request)?;
    let header = ArtifactHeader {
        machine_id: request.machine.clone(),
        machine_registry_revision: artifact::registry_revision(&root, &registry),
        story_id: request.scenario.story_id(),
        gate_id: request.scenario.gate_id(),
        projectos_revision: environment.revision.clone(),
        harness_revision: environment.revision.clone(),
        rust: environment.rust.clone(),
        node: environment.node.clone(),
        pnpm: environment.pnpm.clone(),
        os_name: environment.os_name.clone(),
        os_version: environment.os_version.clone(),
        kernel: environment.kernel.clone(),
        started_at_utc: artifact::now_utc(&root),
        power_mode: environment.power_mode.clone(),
        thermal_state_before: environment.thermal_state_before.clone(),
        background_workload: request.background_workload.clone(),
        dataset_manifest_hash: dataset_hash,
        replicate_count: request.replicates,
        classification: artifact::classify(&request.machine, &registry, &environment),
    };
    let passed = series.iter().all(|row| row.verdict() != "fail");
    write_artifacts(&request.out, &header, &series, passed)?;
    Ok(passed)
}

fn measure(request: &RunRequest) -> Result<(Vec<Series>, String), Box<dyn std::error::Error>> {
    match request.scenario {
        ScenarioName::ProjectOpen1m => {
            let project = scenarios::ensure_project_open_dataset(&request.dataset)?;
            let self_binary = std::env::current_exe()?;
            let samples =
                scenarios::measure_project_open(&self_binary, &project, request.replicates)?;
            Ok((
                vec![Series {
                    label: "project open (snapshot + tail)",
                    unit: "ms",
                    samples,
                    aggregation: "p95",
                    threshold_ms: Some(500.0),
                }],
                format!(
                    "synthetic:{}-events:seeded",
                    scenarios::PROJECT_OPEN_EVENT_COUNT
                ),
            ))
        }
        ScenarioName::DesktopColdStart50 => {
            let projects = scenarios::ensure_cold_start_dataset(&request.dataset)?;
            let shell = scenarios::measure_desktop_startup(
                &request.desktop_binary,
                &projects,
                request.replicates,
            )?;
            let page =
                scenarios::read_ui_measurements(&request.ui_measurements, "timeToInteractiveMs")?;
            // The verdict rides on the sum, stated as an upper bound: the two
            // phases overlap in reality (the webview loads while the core
            // bootstraps), so a passing sum understates nothing.
            let combined = vec![percentile(&shell, 0.95) + percentile(&page, 0.95)];
            Ok((
                vec![
                    Series {
                        label: "native shell: exec → window+webview+runtime ready",
                        unit: "ms",
                        samples: shell,
                        aggregation: "p95",
                        threshold_ms: None,
                    },
                    Series {
                        label: "page: navigation start → project list painted",
                        unit: "ms",
                        samples: page,
                        aggregation: "p95",
                        threshold_ms: None,
                    },
                    Series {
                        label: "cold start → interactive (stated upper bound: shell p95 + page p95)",
                        unit: "ms",
                        samples: combined,
                        aggregation: "median",
                        threshold_ms: Some(1_500.0),
                    },
                ],
                format!(
                    "synthetic:{}-projects:seeded",
                    scenarios::COLD_START_PROJECT_COUNT
                ),
            ))
        }
        ScenarioName::UiInteractionP95 => {
            let palette =
                scenarios::read_ui_measurements(&request.ui_measurements, "paletteOpenMs")?;
            let switch =
                scenarios::read_ui_measurements(&request.ui_measurements, "projectSwitchMs")?;
            Ok((
                vec![
                    Series {
                        label: "palette open",
                        unit: "ms",
                        samples: palette,
                        aggregation: "p95",
                        threshold_ms: Some(100.0),
                    },
                    Series {
                        label: "project switch",
                        unit: "ms",
                        samples: switch,
                        aggregation: "p95",
                        threshold_ms: Some(100.0),
                    },
                ],
                "in-page measurement over the production bundle".to_owned(),
            ))
        }
    }
}

fn write_artifacts(
    out: &Path,
    header: &ArtifactHeader,
    series: &[Series],
    passed: bool,
) -> Result<(), Box<dyn std::error::Error>> {
    std::fs::create_dir_all(out)?;
    let day = header.started_at_utc.get(0..10).unwrap_or("undated");
    let stem = format!("{}-{day}", header.gate_id);
    let json = render_json(header, series, passed);
    std::fs::write(out.join(format!("{stem}.json")), json)?;
    let markdown = render_markdown(header, series, passed);
    std::fs::write(out.join(format!("{stem}.md")), &markdown)?;
    println!("{markdown}");
    println!("pos-bench: wrote {}/{stem}.{{json,md}}", out.display());
    Ok(())
}

fn render_json(header: &ArtifactHeader, series: &[Series], passed: bool) -> String {
    let reasons = match &header.classification {
        Classification::Binding => Vec::new(),
        Classification::EarlyWarning(reasons) => reasons.clone(),
    };
    let rows: Vec<serde_json::Value> = series
        .iter()
        .map(|row| {
            serde_json::json!({
                "label": row.label,
                "unit": row.unit,
                "aggregation": row.aggregation,
                "value": round2(row.value()),
                "thresholdMs": row.threshold_ms,
                "verdict": row.verdict(),
                "samples": row.samples.iter().map(|value| round2(*value)).collect::<Vec<_>>(),
            })
        })
        .collect();
    serde_json::to_string_pretty(&serde_json::json!({
        "schemaVersion": 1,
        "machineId": header.machine_id,
        "machineRegistryRevision": header.machine_registry_revision,
        "storyId": header.story_id,
        "gateId": header.gate_id,
        "projectosRevision": header.projectos_revision,
        "harnessRevision": header.harness_revision,
        "toolchain": {"rust": header.rust, "node": header.node, "pnpm": header.pnpm},
        "os": {"name": header.os_name, "version": header.os_version, "kernel": header.kernel},
        "run": {
            "startedAtUtc": header.started_at_utc,
            "powerMode": header.power_mode,
            "thermalStateBefore": header.thermal_state_before,
            "backgroundWorkload": header.background_workload,
            "datasetManifestHash": header.dataset_manifest_hash,
            "replicateCount": header.replicate_count,
        },
        "classification": header.classification.as_str(),
        "classificationReasons": reasons,
        "series": rows,
        "verdict": if passed { "pass" } else { "fail" },
    }))
    .unwrap_or_else(|_| "{}".to_owned()) // INVARIANT: the value is built from owned strings and finite numbers.
}

fn render_markdown(header: &ArtifactHeader, series: &[Series], passed: bool) -> String {
    let mut text = format!(
        "# {} — {}\n\n\
         **Machine:** `{}` · **Classification:** `{}` · **Verdict:** `{}`  \n\
         **Revision:** `{}` · **Started:** {} · **Replicates:** {}  \n\
         **Toolchain:** {} · Node {} · pnpm {}  \n\
         **OS:** {} {} (kernel {}) · **Power:** {}  \n\
         **Thermal before:** {}  \n\
         **Background:** {}  \n\
         **Dataset:** {}\n\n\
         | Series | Aggregation | Value | Threshold | Verdict |\n\
         |---|---|---|---|---|\n",
        header.gate_id,
        header.story_id,
        header.machine_id,
        header.classification.as_str(),
        if passed { "pass" } else { "fail" },
        header.projectos_revision,
        header.started_at_utc,
        header.replicate_count,
        header.rust,
        header.node,
        header.pnpm,
        header.os_name,
        header.os_version,
        header.kernel,
        header.power_mode,
        header.thermal_state_before,
        header.background_workload,
        header.dataset_manifest_hash,
    );
    for row in series {
        text.push_str(&format!(
            "| {} | {} | {:.2} {} | {} | {} |\n",
            row.label,
            row.aggregation,
            row.value(),
            row.unit,
            row.threshold_ms
                .map_or_else(|| "—".to_owned(), |threshold| format!("< {threshold} ms")),
            row.verdict(),
        ));
    }
    text.push_str("\n## Raw replicates\n\n");
    for row in series {
        let samples: Vec<String> = row
            .samples
            .iter()
            .map(|value| format!("{value:.2}"))
            .collect();
        text.push_str(&format!(
            "- **{}** ({}): {}\n",
            row.label,
            row.unit,
            samples.join(", ")
        ));
    }
    if let Classification::EarlyWarning(reasons) = &header.classification {
        text.push_str(
            "\n## Why this artifact is `early_warning`, not gate evidence\n\n\
             Every reason below is a `docs/reference-machines.md` §4 precondition the run did \
             not meet. The harness computes this — a development-machine number cannot be \
             promoted by forgetting to label it.\n\n",
        );
        for reason in reasons {
            text.push_str(&format!("- {reason}\n"));
        }
    }
    text
}

fn round2(value: f64) -> f64 {
    (value * 100.0).round() / 100.0
}

fn median(samples: &[f64]) -> f64 {
    percentile(samples, 0.5)
}

/// Nearest-rank percentile over a copy of the samples. Nearest-rank rather
/// than interpolation because a gate should quote a number a replicate
/// actually produced.
fn percentile(samples: &[f64], quantile: f64) -> f64 {
    if samples.is_empty() {
        return f64::NAN;
    }
    let mut sorted = samples.to_vec();
    sorted.sort_by(|left, right| left.partial_cmp(right).unwrap_or(std::cmp::Ordering::Equal));
    #[expect(
        clippy::cast_precision_loss,
        reason = "replicate counts are small integers; precision is irrelevant at this scale"
    )]
    let rank = (quantile * sorted.len() as f64).ceil().max(1.0);
    #[expect(
        clippy::cast_possible_truncation,
        clippy::cast_sign_loss,
        reason = "rank is clamped to 1..=len above"
    )]
    let index = (rank as usize).min(sorted.len()) - 1;
    sorted[index]
}

#[cfg(test)]
mod tests {
    use super::{ScenarioName, Series, percentile};

    #[test]
    fn nearest_rank_percentiles_quote_a_sample_that_actually_happened() {
        let samples = vec![10.0, 20.0, 30.0, 40.0, 100.0];
        assert!((percentile(&samples, 0.95) - 100.0).abs() < f64::EPSILON);
        assert!((percentile(&samples, 0.5) - 30.0).abs() < f64::EPSILON);
        // A single replicate is its own p95 rather than an error: a scenario
        // with one derived value (the cold-start sum) is still a real row.
        assert!((percentile(&[7.0], 0.95) - 7.0).abs() < f64::EPSILON);
        assert!(percentile(&[], 0.95).is_nan());
    }

    #[test]
    fn a_series_over_its_threshold_fails_and_says_so() {
        let over = Series {
            label: "project open",
            unit: "ms",
            samples: vec![400.0, 900.0],
            aggregation: "p95",
            threshold_ms: Some(500.0),
        };
        assert_eq!(over.verdict(), "fail");
        let under = Series {
            samples: vec![400.0, 410.0],
            ..over
        };
        assert_eq!(under.verdict(), "pass");
        let informational = Series {
            threshold_ms: None,
            ..under
        };
        assert_eq!(informational.verdict(), "informational");
    }

    #[test]
    fn every_scenario_names_a_gate_and_the_story_that_owns_it() {
        for scenario in [
            ScenarioName::ProjectOpen1m,
            ScenarioName::DesktopColdStart50,
            ScenarioName::UiInteractionP95,
        ] {
            assert!(scenario.gate_id().starts_with("m0."));
            assert!(scenario.story_id().starts_with("m0-s"));
        }
        // The cold-start row is m0-s07's acceptance criterion; the harness
        // records the owning story so the artifact closes the right box.
        assert_eq!(ScenarioName::DesktopColdStart50.story_id(), "m0-s07");
    }
}
