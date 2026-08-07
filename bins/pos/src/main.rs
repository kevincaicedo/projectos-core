//! # pos
//!
//! The CLI shell: create / open / inspect / verify / export over pos-api — no lock-in from the first commit (F45, L4).
//!
//! v0 lands with m0-s05. Every subcommand resolves a `(name, input)` pair
//! against the same registry the desktop webview and future server dispatch —
//! stdout carries the registry's canonical JSON bytes unchanged, so a
//! divergence between shells is visible in a diff without a GUI. Rendering
//! decisions (exit codes, stderr summaries) decode the bytes; they never
//! reshape them.

#![forbid(unsafe_code)]

use clap::{Parser, Subcommand};
use pos_api::{
    CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, ProjectExportInput,
    ProjectPathInput, ProjectSeedInput, QueryName, bootstrap_local_runtime, input_json,
};
use std::path::PathBuf;
use std::process::ExitCode;

#[derive(Parser)]
#[command(
    name = "pos",
    about = "ProjectOS project tool: a project is a portable directory you own (L4).",
    version
)]
struct Cli {
    #[command(subcommand)]
    command: CliCommand,
}

#[derive(Subcommand)]
enum CliCommand {
    /// Create a new project directory.
    Create {
        directory: PathBuf,
        /// Project template recorded in the manifest.
        #[arg(long, default_value = "generic")]
        template: String,
        /// Display name; defaults to the directory stem.
        #[arg(long)]
        name: Option<String>,
    },
    /// Open a project and confirm it is healthy (brings projections current).
    Open { directory: PathBuf },
    /// Report manifest, event count, head seq, and snapshot state.
    Inspect { directory: PathBuf },
    /// Re-derive projections from the log and sweep blob integrity.
    /// Exits non-zero and names every mismatch when verification fails.
    Verify { directory: PathBuf },
    /// Export the project: a portable copy of the directory plus
    /// `events.jsonl`, the documented text rendering of the log.
    Export {
        directory: PathBuf,
        /// Destination directory; must not exist yet.
        #[arg(long)]
        out: PathBuf,
    },
    /// Append deterministic synthetic events (test/bench scaffolding shared
    /// with pos-bench; identical seed + count ⇒ identical corpus).
    SeedSynthetic {
        directory: PathBuf,
        /// Number of events to append.
        #[arg(long)]
        events: u64,
        /// Generator seed.
        #[arg(long, default_value_t = 0)]
        seed: u64,
    },
    /// Print the live capability-registry snapshot (the walking-skeleton
    /// read; also the e2e fixture source).
    CapabilitySnapshot,
    /// Aggregate the model-call cost ledger for one project (m0-s10).
    CostRollup { directory: PathBuf },
    /// Local model management (m0-s11): checksummed downloads, never
    /// bundled, never fetched without consent.
    Models {
        #[command(subcommand)]
        command: ModelsCommand,
    },
}

#[derive(Subcommand)]
enum ModelsCommand {
    /// Download a model named in the manifest, verify its BLAKE3, and land
    /// it atomically in the models directory.
    Pull {
        name: String,
        /// The reviewed model catalog.
        #[arg(long, default_value = "models/manifest.json")]
        manifest: PathBuf,
        /// Where verified models land.
        #[arg(long, default_value = "models/pulled")]
        dest: PathBuf,
        /// Consent to the download without the interactive prompt.
        #[arg(long)]
        yes: bool,
    },
}

fn main() -> ExitCode {
    let cli = Cli::parse();
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    match run(&runtime, cli.command) {
        Ok(exit) => exit,
        Err(error_json) => {
            eprintln!("{error_json}");
            ExitCode::FAILURE
        }
    }
}

/// Dispatches one subcommand. `Err` carries the typed error envelope JSON.
fn run(runtime: &LocalRuntime, command: CliCommand) -> Result<ExitCode, String> {
    match command {
        CliCommand::Create {
            directory,
            template,
            name,
        } => {
            let input = ProjectCreateInput {
                path: path_text(&directory)?,
                name,
                template,
            };
            let report = dispatch_command(runtime, CommandName::ProjectCreate, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Open { directory } | CliCommand::Inspect { directory } => {
            let input = ProjectPathInput {
                path: path_text(&directory)?,
            };
            let report = dispatch_query(runtime, QueryName::ProjectInspect, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Verify { directory } => {
            let input = ProjectPathInput {
                path: path_text(&directory)?,
            };
            let report = dispatch_query(runtime, QueryName::ProjectVerify, &input)?;
            println!("{report}");
            Ok(render_verify_outcome(&report))
        }
        CliCommand::Export { directory, out } => {
            let input = ProjectExportInput {
                path: path_text(&directory)?,
                out: path_text(&out)?,
            };
            let report = dispatch_command(runtime, CommandName::ProjectExport, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::SeedSynthetic {
            directory,
            events,
            seed,
        } => {
            let input = ProjectSeedInput {
                path: path_text(&directory)?,
                event_count: events,
                seed,
            };
            let report = dispatch_command(runtime, CommandName::ProjectSeedSynthetic, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::CapabilitySnapshot => {
            let report = runtime
                .query(QueryName::CapabilitySnapshot.as_str())
                .map_err(|error| error.to_json())?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::CostRollup { directory } => {
            let input = pos_api::CostRollupInput {
                path: Some(path_text(&directory)?),
            };
            let report = dispatch_query(runtime, QueryName::CostRollup, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Models {
            command:
                ModelsCommand::Pull {
                    name,
                    manifest,
                    dest,
                    yes,
                },
        } => {
            // Consent is explicit, never implicit (m0-s11): `--yes`, or an
            // interactive `y` on a terminal. A non-interactive run without
            // `--yes` is refused by the registry with `consent_required`.
            let consent = yes || prompt_for_consent(&name);
            let input = pos_api::ModelsPullInput {
                manifest_path: path_text(&manifest)?,
                name,
                dest_dir: path_text(&dest)?,
                consent,
            };
            let report = dispatch_command(runtime, CommandName::ModelsPull, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
    }
}

/// Asks on stderr so stdout stays the machine-readable report. Anything but
/// an explicit `y`/`yes` line — including a closed or non-interactive
/// stdin — is a refusal.
fn prompt_for_consent(name: &str) -> bool {
    use std::io::IsTerminal;
    if !std::io::stdin().is_terminal() {
        return false;
    }
    eprint!("pull {name}? This downloads a model artifact. [y/N] ");
    let mut answer = String::new();
    if std::io::stdin().read_line(&mut answer).is_err() {
        return false;
    }
    matches!(answer.trim().to_ascii_lowercase().as_str(), "y" | "yes")
}

fn dispatch_command(
    runtime: &LocalRuntime,
    name: CommandName,
    input: &impl serde::Serialize,
) -> Result<String, String> {
    let input_document = input_json(input).map_err(|error| error.to_json())?;
    runtime
        .command(name.as_str(), &input_document)
        .map_err(|error| error.to_json())
}

fn dispatch_query(
    runtime: &LocalRuntime,
    name: QueryName,
    input: &impl serde::Serialize,
) -> Result<String, String> {
    let input_document = input_json(input).map_err(|error| error.to_json())?;
    runtime
        .query_with_input(name.as_str(), &input_document)
        .map_err(|error| error.to_json())
}

/// Renders the verify outcome: exit 0 only when clean, and every mismatch is
/// named on stderr (m0-s05 AC) — decoded from the same bytes stdout carries.
fn render_verify_outcome(report_json: &str) -> ExitCode {
    let Ok(report) = serde_json::from_str::<serde_json::Value>(report_json) else {
        eprintln!("pos verify: report is not valid JSON");
        return ExitCode::FAILURE;
    };
    if report["clean"] == serde_json::Value::Bool(true) {
        return ExitCode::SUCCESS;
    }
    for table in flatten_strings(&report["mismatchedTables"]) {
        eprintln!("pos verify: projection table {table} does not match the log");
    }
    for path in flatten_strings(&report["casDefectPaths"]) {
        eprintln!("pos verify: blob integrity defect at {path}");
    }
    if report["appliedSeq"] != report["headSeq"] {
        eprintln!(
            "pos verify: projections claim seq {} but the log head is {}",
            report["appliedSeq"], report["headSeq"]
        );
    }
    ExitCode::FAILURE
}

fn flatten_strings(value: &serde_json::Value) -> Vec<&str> {
    value
        .as_array()
        .map(|entries| {
            entries
                .iter()
                .filter_map(serde_json::Value::as_str)
                .collect()
        })
        .unwrap_or_default()
}

fn path_text(path: &std::path::Path) -> Result<String, String> {
    path.to_str().map(str::to_owned).ok_or_else(|| {
        "{\"code\":\"invalid_input\",\"message\":\"path is not valid UTF-8\",\"retriable\":false}"
            .to_owned()
    })
}
