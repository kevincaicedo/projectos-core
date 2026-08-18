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
    ProjectPathInput, ProjectSeedInput, QueryName, WORKER_DRAIN_MS_MAX_DEFAULT, WorkerConfig,
    WorkerDrainReport, bootstrap_local_runtime, input_json,
};
use std::path::PathBuf;
use std::process::ExitCode;

/// Milliseconds per second, for the `--drain-secs` budget. Seconds on the
/// flag because that is the unit a human waiting at a terminal thinks in.
const MS_PER_SEC: u64 = 1_000;

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
    /// Browse the Evidence a project holds and what the pipeline did to it
    /// (m1-s01/m1-s02).
    Evidence {
        #[command(subcommand)]
        command: EvidenceCommand,
    },
    /// Per-source, per-stage ingestion health (m1-s01).
    SourceHealth {
        directory: PathBuf,
        /// Hex source id; omitted means every source.
        #[arg(long)]
        source: Option<String>,
    },
    /// Ingestion pipeline control (m1-s01).
    Ingest {
        #[command(subcommand)]
        command: IngestCommand,
    },
}

#[derive(Subcommand)]
enum EvidenceCommand {
    /// List Evidence with its pipeline status.
    List {
        directory: PathBuf,
        #[arg(long)]
        source: Option<String>,
        /// `raw` … `indexed` | `failed`.
        #[arg(long)]
        status: Option<String>,
        #[arg(long, default_value_t = 50)]
        limit: u32,
        /// Include each item's per-stage history.
        #[arg(long)]
        with_stages: bool,
    },
}

#[derive(Subcommand)]
enum IngestCommand {
    /// Ingest a file, or every file in a folder, into a project (m1-s07).
    /// What each file *is* comes from its bytes, never from its name.
    Submit {
        directory: PathBuf,
        /// The file or folder to ingest.
        file: PathBuf,
        /// What to call a single file. Ignored for a folder: naming twelve
        /// recordings the same thing is worse than using their own names.
        #[arg(long)]
        title: Option<String>,
        /// The selection inside the upload connector these items belong to,
        /// so a batch import and a drag-drop are distinguishable on the
        /// source-health card.
        #[arg(long)]
        source_scope: Option<String>,
        /// Queue the work and exit without running it. The jobs stay durable
        /// in the project and the next shell to open it claims them.
        #[arg(long)]
        no_drain: bool,
        /// How long to run the queued work before giving up and saying what
        /// is left.
        #[arg(long, default_value_t = WORKER_DRAIN_MS_MAX_DEFAULT / MS_PER_SEC)]
        drain_secs: u64,
    },
    /// Re-embed a project under a different model (m1-s04).
    ///
    /// Sugar over `reprocess --from-stage embed`, and deliberately so: a
    /// re-embed *is* a managed reprocess, not a second mechanism. The model
    /// is named here rather than read from configuration so the recorded
    /// reason says which model, and so the command is reproducible from a
    /// shell history.
    Reembed {
        directory: PathBuf,
        /// The artifact to embed under, e.g. `bge-small-en-v1.5`. It must be
        /// pulled: vectors from a model that is not there cannot be computed.
        #[arg(long)]
        model: String,
        /// One item; omitted means every eligible item, up to `--limit`.
        #[arg(long)]
        evidence: Option<String>,
        #[arg(long, default_value_t = 500)]
        limit: u32,
        /// Queue the work and exit without running it.
        #[arg(long)]
        no_drain: bool,
        #[arg(long, default_value_t = WORKER_DRAIN_MS_MAX_DEFAULT / MS_PER_SEC)]
        drain_secs: u64,
    },
    /// Re-run the pipeline from a stage. Never re-fetches from the source:
    /// the bytes already stored are the Evidence.
    Reprocess {
        directory: PathBuf,
        /// `normalize` | `transcribe` | `chunk` | `embed` | `extract` | `index`.
        #[arg(long)]
        from_stage: String,
        /// One item; omitted means every eligible item, up to `--limit`.
        #[arg(long)]
        evidence: Option<String>,
        #[arg(long, default_value_t = 500)]
        limit: u32,
        /// Why. Recorded on the event — a reprocess with no stated reason is
        /// an unexplained rewrite of derived state.
        #[arg(long)]
        reason: String,
        /// Queue the work and exit without running it. The jobs stay durable
        /// in the project and the next shell to open it claims them.
        #[arg(long)]
        no_drain: bool,
        /// How long to run the queued work before giving up and saying what
        /// is left. A one-shot invocation must terminate whatever the corpus
        /// does, so this is a budget, not a promise.
        #[arg(long, default_value_t = WORKER_DRAIN_MS_MAX_DEFAULT / MS_PER_SEC)]
        drain_secs: u64,
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
    // Telemetry is opt-in and off by default (m0-s15). A spec we cannot honour
    // stops the process: running with export silently disabled would let an
    // empty collector read as "nothing happened".
    if let Err(error) = pos_api::install_telemetry(telemetry_spec().as_deref()) {
        eprintln!("{}", error.to_json());
        return ExitCode::FAILURE;
    }
    let mut runtime =
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    // Only the subcommands that queue work start a pool. A `pos inspect` that
    // spun up worker threads would pay for a scheduler to read one row, and
    // an idle pool in a one-shot process is a lie about what is running.
    if queues_background_work(&cli.command)
        && let Err(error) = runtime.start_background_workers(WorkerConfig::default())
    {
        eprintln!("{}", error.to_json());
        return ExitCode::FAILURE;
    }
    let exit = match run(&runtime, cli.command) {
        Ok(exit) => exit,
        Err(error_json) => {
            eprintln!("{error_json}");
            ExitCode::FAILURE
        }
    };
    if !runtime.shutdown_background_workers() {
        eprintln!(
            "pos: a background job outlived the shutdown budget; it was left running and its \
             lease will expire (the work is durable and resumes on the next run)"
        );
    }
    exit
}

/// Whether this invocation enqueues jobs somebody has to claim.
const fn queues_background_work(command: &CliCommand) -> bool {
    matches!(
        command,
        CliCommand::Ingest {
            command: IngestCommand::Reprocess { .. }
                | IngestCommand::Submit { .. }
                | IngestCommand::Reembed { .. }
        }
    )
}

/// The one telemetry configuration key every shell reads (m0-s15).
fn telemetry_spec() -> Option<String> {
    std::env::var("POS_TELEMETRY").ok()
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
        CliCommand::Evidence {
            command:
                EvidenceCommand::List {
                    directory,
                    source,
                    status,
                    limit,
                    with_stages,
                },
        } => {
            let input = pos_api::EvidenceListInput {
                path: path_text(&directory)?,
                source_id: source,
                status,
                row_count_max: Some(limit),
                with_stages,
            };
            let report = dispatch_query(runtime, QueryName::EvidenceList, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::SourceHealth { directory, source } => {
            let input = pos_api::SourceHealthInput {
                path: path_text(&directory)?,
                source_id: source,
            };
            let report = dispatch_query(runtime, QueryName::SourceHealth, &input)?;
            println!("{report}");
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Ingest {
            command:
                IngestCommand::Submit {
                    directory,
                    file,
                    title,
                    source_scope,
                    no_drain,
                    drain_secs,
                },
        } => {
            let path = path_text(&directory)?;
            let project = ProjectPathInput { path: path.clone() };
            // Open first, for the same reason reprocess does: the pool serves
            // the projects this process has open.
            dispatch_command(runtime, CommandName::ProjectOpen, &project)?;
            let input = pos_api::IngestSubmitInput {
                path,
                file_path: Some(path_text(&file)?),
                file_name: title,
                source_scope,
            };
            let report = dispatch_command(runtime, CommandName::IngestSubmit, &input)?;
            println!("{report}");
            if !no_drain {
                render_drain(&runtime.drain_background_workers(drain_secs * MS_PER_SEC));
            }
            if let Err(message) = dispatch_command(runtime, CommandName::ProjectClose, &project) {
                eprintln!("pos ingest submit: closing the project failed: {message}");
            }
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Ingest {
            command:
                IngestCommand::Reembed {
                    directory,
                    model,
                    evidence,
                    limit,
                    no_drain,
                    drain_secs,
                },
        } => {
            // One process, one answer for "which model does EMBED load".
            // Set before the project opens, because the worker pool composes
            // its stage handlers at open.
            pos_api::set_embed_model(model.clone())
                .map_err(|already| format!("this process already embeds under {already:?}"))?;
            let path = path_text(&directory)?;
            let project = ProjectPathInput { path: path.clone() };
            dispatch_command(runtime, CommandName::ProjectOpen, &project)?;
            let input = pos_api::IngestReprocessInput {
                path,
                from_stage: pos_api::IngestStage::Embed.as_str().to_owned(),
                evidence_id: evidence,
                item_count_max: Some(limit),
                reason: format!("re-embed under {model}"),
            };
            let report = dispatch_command(runtime, CommandName::IngestReprocess, &input)?;
            println!("{report}");
            if !no_drain {
                render_drain(&runtime.drain_background_workers(drain_secs * MS_PER_SEC));
            }
            if let Err(message) = dispatch_command(runtime, CommandName::ProjectClose, &project) {
                eprintln!("pos ingest reembed: closing the project failed: {message}");
            }
            Ok(ExitCode::SUCCESS)
        }
        CliCommand::Ingest {
            command:
                IngestCommand::Reprocess {
                    directory,
                    from_stage,
                    evidence,
                    limit,
                    reason,
                    no_drain,
                    drain_secs,
                },
        } => {
            let path = path_text(&directory)?;
            let project = ProjectPathInput { path: path.clone() };
            // Open first: the pool serves the projects this process has open,
            // so an invocation that queued work without opening the project
            // would enqueue jobs its own workers cannot see.
            dispatch_command(runtime, CommandName::ProjectOpen, &project)?;
            let input = pos_api::IngestReprocessInput {
                path,
                from_stage,
                evidence_id: evidence,
                item_count_max: Some(limit),
                reason,
            };
            let report = dispatch_command(runtime, CommandName::IngestReprocess, &input)?;
            println!("{report}");
            if !no_drain {
                render_drain(&runtime.drain_background_workers(drain_secs * MS_PER_SEC));
            }
            // Close is best-effort: the work is committed either way, and a
            // failure here must not turn a successful reprocess into one.
            if let Err(message) = dispatch_command(runtime, CommandName::ProjectClose, &project) {
                eprintln!("pos ingest reprocess: closing the project failed: {message}");
            }
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

/// Reports what the drain observed, on stderr — stdout stays the registry's
/// canonical bytes for the command that was asked for. An expired budget is
/// stated, never rounded up to success: the jobs are still durable, and the
/// next run resumes them.
fn render_drain(report: &WorkerDrainReport) {
    if let Some(error) = report.last_read_error.as_deref() {
        eprintln!("pos ingest reprocess: reading the queue failed during the drain: {error}");
    }
    if report.quiescent {
        eprintln!(
            "pos ingest reprocess: queue drained in {}ms ({} dead-lettered)",
            report.waited_ms, report.dead_total
        );
    } else {
        eprintln!(
            "pos ingest reprocess: the {}ms drain budget expired with {} job(s) still queued; \
             they stay durable and resume the next time this project is open",
            report.waited_ms, report.queued_remaining
        );
    }
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
