//! The three M0 gate scenarios and their datasets.
//!
//! Every scenario drives the product through `pos-api`, the same seam a shell
//! uses (L12). A scenario that measured a private fast path would produce a
//! number no user can experience, which is the failure mode the claim ledger
//! (master plan §24) exists to prevent.

use pos_api::{
    CommandName, IngestStage, LocalBootstrapConfig, ProjectCreateInput, ProjectPathInput,
    ProjectSeedInput, StageState, bootstrap_local_runtime, input_json,
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

// ---------------------------------------------------------------------------
// M1 gate scenarios (m1-s07 opened the seam these two measure through).
// ---------------------------------------------------------------------------

/// Bytes of the single synthetic file the buffer gate streams. The §18 row
/// says 8 GB; this is 8 GiB, which is more.
pub const INGEST_SINGLE_FILE_BYTES: u64 = 8 * 1024 * 1024 * 1024;

/// Bytes of the *text* file that runs beside it. The binary file proves RAW
/// streams; only text reaches NORMALIZE and CHUNK, and a buffer bound that
/// never exercised the chunker would be measuring a third of the pipeline.
///
/// Deliberately below `pos-ingest`'s own text intake cap, because a gate that
/// submitted a file the product refuses would measure the refusal.
pub const INGEST_TEXT_BYTES: u64 = 256 * 1024 * 1024;

/// Bytes written per call while building a dataset. Large enough to amortize
/// the write, small enough that building an 8 GiB file costs one buffer.
const DATASET_WRITE_BYTES: usize = 1024 * 1024;

/// Section length in the synthetic document, in bytes. Around four kibibytes
/// keeps the segment count inside `SEGMENT_COUNT_MAX` for a corpus this size
/// while still producing several chunks per section.
const DATASET_SECTION_BYTES: usize = 4 * 1024;

/// How long the buffer scenario waits for the queue to drain. A GB-scale
/// ingest is minutes, not seconds; the budget is stated so a stuck run ends
/// with a report rather than hanging (L8).
const INGEST_DRAIN_MS_MAX: u64 = 60 * 60 * 1000;

/// Drain budget for the embedding row. Longer than the ingest one because a
/// million chunks is a million forward passes: at the ~17k tokens/s the model
/// sustains on the reference laptop, ~1.2 KB per chunk is roughly four hours.
/// A budget, not a promise — the harness reports what is left rather than
/// waiting forever (L8).
const EMBED_DRAIN_MS_MAX: u64 = 8 * 60 * 60 * 1000;

/// How often the RSS sampler looks. One hertz is the §18 sampling rate and
/// costs one `ps` call per second against a run measured in minutes.
const RSS_SAMPLE_INTERVAL_MS: u64 = 1_000;

/// What one buffer-gate replicate measured.
pub struct IngestMeasurement {
    /// Peak resident bytes across every live pipeline stream, read from the
    /// runtime's own meter through `health` (ADR-0008 bound 1).
    pub buffer_peak_bytes: u64,
    /// Peak process RSS observed while the ingest ran (ADR-0008 bound 3).
    pub rss_peak_bytes: u64,
    pub wall_ms: u64,
    pub bytes_ingested: u64,
}

/// Builds (or reuses) the buffer-gate corpus: one large binary file and one
/// large text file, both deterministic.
pub fn ensure_ingest_dataset(
    dataset: &Path,
    single_file_bytes: u64,
    text_bytes: u64,
) -> Result<PathBuf, ScenarioError> {
    let root = dataset.join("ingest-buffers");
    let marker = root.join(format!(".complete-{single_file_bytes}-{text_bytes}"));
    if marker.is_file() {
        return Ok(root);
    }
    std::fs::create_dir_all(&root)
        .map_err(|error| fail(format!("create dataset directory: {error}")))?;
    write_opaque_file(&root.join("recording.bin"), single_file_bytes)?;
    write_document_file(&root.join("corpus.md"), text_bytes)?;
    std::fs::write(&marker, b"pos-bench ingest dataset\n")
        .map_err(|error| fail(format!("write dataset marker: {error}")))?;
    Ok(root)
}

/// A deterministic byte stream with a zero in every block, so the sniffer
/// classifies it `opaque` — which is what a video container looks like to a
/// text sniffer, without shipping a video in the repository.
fn write_opaque_file(path: &Path, bytes: u64) -> Result<(), ScenarioError> {
    if path.is_file() {
        return Ok(());
    }
    let mut block = vec![0_u8; DATASET_WRITE_BYTES];
    for (index, byte) in block.iter_mut().enumerate() {
        *byte = u8::try_from(index % 251).unwrap_or(0);
    }
    write_blocks(path, bytes, &block)
}

/// A markdown document with real headings, so CHUNK produces heading-section
/// windows rather than one undifferentiated block.
fn write_document_file(path: &Path, bytes: u64) -> Result<(), ScenarioError> {
    if path.is_file() {
        return Ok(());
    }
    let mut block = String::with_capacity(DATASET_WRITE_BYTES + DATASET_SECTION_BYTES);
    let mut section = 0_u32;
    while block.len() < DATASET_WRITE_BYTES {
        block.push_str(&format!("## Section {section}\n\n"));
        while !block.len().is_multiple_of(DATASET_SECTION_BYTES) {
            block.push_str("The pipeline streams this sentence and never holds the file. ");
        }
        block.push_str("\n\n");
        section += 1;
    }
    write_blocks(path, bytes, block.as_bytes())
}

fn write_blocks(path: &Path, bytes: u64, block: &[u8]) -> Result<(), ScenarioError> {
    use std::io::Write;
    let file = std::fs::File::create(path)
        .map_err(|error| fail(format!("create dataset file: {error}")))?;
    let mut writer = std::io::BufWriter::new(file);
    let mut written = 0_u64;
    while written < bytes {
        let take =
            usize::try_from((bytes - written).min(block.len() as u64)).unwrap_or(block.len());
        writer
            .write_all(&block[..take])
            .map_err(|error| fail(format!("write dataset file: {error}")))?;
        written += take as u64;
    }
    writer
        .flush()
        .map_err(|error| fail(format!("flush dataset file: {error}")))
}

/// One replicate of the buffer gate: a fresh project, the corpus submitted
/// through `ingest.submit`, and the pipeline drained to quiescence.
pub fn measure_ingest_buffers(
    dataset: &Path,
    corpus: &Path,
    replicate: u32,
) -> Result<IngestMeasurement, ScenarioError> {
    let project = dataset
        .join(format!("ingest-run-{replicate:02}.pos"))
        .display()
        .to_string();
    // A fresh project per replicate: a second ingest into the same project
    // would be deduplicated by the CAS and measure nothing.
    let _ = std::fs::remove_dir_all(&project);
    let mut runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        dataset.join("packs").join(format!("{replicate:02}")),
    ));
    command(
        &runtime,
        CommandName::ProjectCreate,
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("pos-bench ingest buffers".to_owned()),
            template: "generic".to_owned(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    runtime
        .start_background_workers(pos_api::WorkerConfig::default())
        .map_err(|error| fail(error.to_json()))?;
    command(
        &runtime,
        CommandName::ProjectOpen,
        &input_json(&ProjectPathInput {
            path: project.clone(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;

    let sampler = RssSampler::start();
    let started = Instant::now();
    let report = command(
        &runtime,
        CommandName::IngestSubmit,
        &input_json(&pos_api::IngestSubmitInput {
            path: project.clone(),
            file_path: Some(corpus.display().to_string()),
            file_name: None,
            source_scope: None,
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    let drain = runtime.drain_background_workers(INGEST_DRAIN_MS_MAX);
    let wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let rss_peak_bytes = sampler.finish();
    if !drain.quiescent {
        return Err(fail(format!(
            "the {INGEST_DRAIN_MS_MAX} ms drain budget expired with work still queued; \
             the measurement would understate the corpus"
        )));
    }
    let bytes_ingested = sum_u64_field(&report, "byteSize");
    let refused = number_field(&report, "refusedCount").unwrap_or(0);
    if refused > 0 {
        return Err(fail(format!("the corpus was not fully ingested: {report}")));
    }

    // The meter is read through the same `health` query a shell reads, after
    // the pipeline is quiescent: nothing else ran in this process, so the
    // peak is this scenario's peak.
    let health = runtime
        .query(pos_api::QueryName::Health.as_str())
        .map_err(|error| fail(error.to_json()))?;
    let buffer_peak_bytes = number_field(&health, "peakBytes")
        .ok_or_else(|| fail(format!("health carried no ingest buffer meter: {health}")))?;
    runtime.shutdown_background_workers();
    Ok(IngestMeasurement {
        buffer_peak_bytes,
        rss_peak_bytes,
        wall_ms,
        bytes_ingested,
    })
}

/// Chunks the §18 embedding row states. One million is the milestone's own
/// number, and it is a *corpus* size rather than a per-item one: the property
/// is that memory does not grow with the corpus.
pub const EMBED_CHUNK_COUNT: u64 = 1_000_000;

/// Bytes of text that produce [`EMBED_CHUNK_COUNT`] chunks.
///
/// The chunker targets 300 tokens at ~4 bytes each, so ~1.2 KB per chunk. The
/// dataset is written as one document per 4096 chunks rather than one giant
/// file, because m1-s07's intake cap refuses a text item past 1 GiB — a limit
/// *derived* from the chunker's own 4M-chunk ceiling, and one this gate must
/// respect rather than route around.
pub const EMBED_CHUNK_BYTES_ESTIMATE: u64 = 1_200;

/// Chunks per document in the embedding corpus.
const EMBED_CHUNKS_PER_DOCUMENT: u64 = 4_096;

/// What one embedding replicate measured.
pub struct EmbedMeasurement {
    pub buffer_peak_bytes: u64,
    pub rss_peak_bytes: u64,
    pub wall_ms: u64,
    pub chunk_count: u64,
    pub vector_count: u64,
}

/// Builds (or reuses) the 1M-chunk corpus as a folder of documents.
///
/// # Errors
///
/// [`ScenarioError`] when the dataset cannot be written.
pub fn ensure_embed_dataset(dataset: &Path, chunk_count: u64) -> Result<PathBuf, ScenarioError> {
    let root = dataset.join("embed-corpus");
    let marker = root.join(format!(".complete-{chunk_count}"));
    if marker.is_file() {
        return Ok(root);
    }
    std::fs::create_dir_all(&root)
        .map_err(|error| fail(format!("create dataset directory: {error}")))?;
    let document_count = chunk_count.div_ceil(EMBED_CHUNKS_PER_DOCUMENT);
    let document_bytes = EMBED_CHUNKS_PER_DOCUMENT * EMBED_CHUNK_BYTES_ESTIMATE;
    for index in 0..document_count {
        write_document_file(&root.join(format!("corpus-{index:04}.md")), document_bytes)?;
    }
    std::fs::write(&marker, b"pos-bench embed dataset\n")
        .map_err(|error| fail(format!("write dataset marker: {error}")))?;
    Ok(root)
}

/// One replicate of the embedding memory gate: a fresh project, the corpus
/// through the front door, and the meters read back afterwards.
///
/// The measurement that matters is **buffer residency**, and it is stated in
/// [ADR-0008]'s terms: bound 1 is what fails a pull request, because a peak
/// here means a stage stopped streaming. Bound 3 is the ceiling a user feels,
/// and unlike the 8 GiB row this one *does* load model weights — 226 MiB of
/// them — which is exactly why the two rows are separate.
///
/// # Errors
///
/// [`ScenarioError`] when the corpus is refused, the drain budget expires, or
/// the health meter is absent — each of which would make the number a lie
/// rather than a measurement.
///
/// [ADR-0008]: ../../../../docs/adr/0008-ingest-memory-budget-splits-buffers-from-model-weights.md
pub fn measure_embed_memory(
    dataset: &Path,
    corpus: &Path,
    replicate: u32,
) -> Result<EmbedMeasurement, ScenarioError> {
    let project = dataset
        .join(format!("embed-run-{replicate:02}.pos"))
        .display()
        .to_string();
    let _ = std::fs::remove_dir_all(&project);
    let mut runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        dataset.join("packs").join(format!("embed-{replicate:02}")),
    ));
    command(
        &runtime,
        CommandName::ProjectCreate,
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("pos-bench embed memory".to_owned()),
            template: "generic".to_owned(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    runtime
        .start_background_workers(pos_api::WorkerConfig::default())
        .map_err(|error| fail(error.to_json()))?;
    command(
        &runtime,
        CommandName::ProjectOpen,
        &input_json(&ProjectPathInput {
            path: project.clone(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;

    let sampler = RssSampler::start();
    let started = Instant::now();
    let report = command(
        &runtime,
        CommandName::IngestSubmit,
        &input_json(&pos_api::IngestSubmitInput {
            path: project.clone(),
            file_path: Some(corpus.display().to_string()),
            file_name: None,
            source_scope: None,
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    let drain = runtime.drain_background_workers(EMBED_DRAIN_MS_MAX);
    let wall_ms = u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX);
    let rss_peak_bytes = sampler.finish();
    if !drain.quiescent {
        return Err(fail(format!(
            "the {EMBED_DRAIN_MS_MAX} ms drain budget expired with work still queued; \
             the measurement would understate the corpus"
        )));
    }
    let refused = number_field(&report, "refusedCount").unwrap_or(0);
    if refused > 0 {
        return Err(fail(format!("the corpus was not fully ingested: {report}")));
    }
    let health = runtime
        .query(pos_api::QueryName::Health.as_str())
        .map_err(|error| fail(error.to_json()))?;
    let buffer_peak_bytes = number_field(&health, "peakBytes")
        .ok_or_else(|| fail(format!("health carried no ingest buffer meter: {health}")))?;
    let (chunk_count, vector_count) = embed_counts(&runtime, &project)?;
    runtime.shutdown_background_workers();
    if vector_count == 0 {
        return Err(fail(
            "no chunk was embedded — is bge-small-en-v1.5 pulled? \
             (`pos models pull bge-small-en-v1.5`)"
                .to_owned(),
        ));
    }
    Ok(EmbedMeasurement {
        buffer_peak_bytes,
        rss_peak_bytes,
        wall_ms,
        chunk_count,
        vector_count,
    })
}

/// Chunks produced and vectors committed, read back through the same
/// `evidence.list` a shell reads.
///
/// Both, rather than just the vector count: a run that embedded every chunk
/// it produced and a run that produced no chunks at all would report the same
/// zero, and the gate must be able to tell those apart.
fn embed_counts(
    runtime: &pos_api::LocalRuntime,
    project: &str,
) -> Result<(u64, u64), ScenarioError> {
    let listing = runtime
        .query_with_input(
            pos_api::QueryName::EvidenceList.as_str(),
            &input_json(&pos_api::EvidenceListInput {
                path: project.to_owned(),
                source_id: None,
                status: None,
                row_count_max: Some(pos_api::EVIDENCE_LIST_ROW_COUNT_MAX),
                with_stages: true,
            })
            .map_err(|error| fail(error.to_json()))?,
        )
        .map_err(|error| fail(error.to_json()))?;
    let parsed: serde_json::Value = serde_json::from_str(&listing)
        .map_err(|error| fail(format!("evidence.list was not JSON: {error}")))?;
    let rows = parsed
        .get("evidence")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| fail(format!("evidence.list carried no rows: {listing}")))?;
    let mut chunks = 0_u64;
    let mut vectors = 0_u64;
    for row in rows {
        chunks += row
            .get("chunkCount")
            .and_then(serde_json::Value::as_u64)
            .unwrap_or(0);
        let Some(stages) = row.get("stages").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for stage in stages {
            let embedded = stage.get("stage").and_then(serde_json::Value::as_str)
                == Some(IngestStage::Embed.as_str())
                && stage.get("state").and_then(serde_json::Value::as_str)
                    == Some(StageState::Done.as_str());
            if embedded {
                vectors += stage
                    .get("itemCount")
                    .and_then(serde_json::Value::as_u64)
                    .unwrap_or(0);
            }
        }
    }
    Ok((chunks, vectors))
}

/// What one transcription replicate measured, read back from the stage row
/// the pipeline itself wrote.
pub struct TranscribeMeasurement {
    pub audio_ms: u64,
    pub wall_ms: u64,
    /// Reproducible identity for a recording that is gitignored and lives on
    /// one machine: the evidence id, which derives from the content hash.
    pub evidence_id: String,
}

/// One replicate of the transcription gate: a fresh project, one recording
/// submitted through the front door, and the TRANSCRIBE stage row read back.
///
/// The numbers come from the stage row rather than from a stopwatch around
/// the call, because the stage row is what the product itself recorded — a
/// gate and a source-health card that disagreed would mean one of them is
/// measuring something else.
pub fn measure_transcription(
    dataset: &Path,
    audio: &Path,
    replicate: u32,
) -> Result<TranscribeMeasurement, ScenarioError> {
    if !audio.is_file() {
        return Err(fail(format!(
            "no recording at {} — pass one with --audio",
            audio.display()
        )));
    }
    let project = dataset
        .join(format!("transcribe-run-{replicate:02}.pos"))
        .display()
        .to_string();
    let _ = std::fs::remove_dir_all(&project);
    let mut runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        dataset.join("packs").join(format!("stt-{replicate:02}")),
    ));
    command(
        &runtime,
        CommandName::ProjectCreate,
        &input_json(&ProjectCreateInput {
            path: project.clone(),
            name: Some("pos-bench transcription".to_owned()),
            template: "generic".to_owned(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    runtime
        .start_background_workers(pos_api::WorkerConfig::default())
        .map_err(|error| fail(error.to_json()))?;
    command(
        &runtime,
        CommandName::ProjectOpen,
        &input_json(&ProjectPathInput {
            path: project.clone(),
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    let submitted = command(
        &runtime,
        CommandName::IngestSubmit,
        &input_json(&pos_api::IngestSubmitInput {
            path: project.clone(),
            file_path: Some(audio.display().to_string()),
            file_name: None,
            source_scope: None,
        })
        .map_err(|error| fail(error.to_json()))?,
    )?;
    let drain = runtime.drain_background_workers(INGEST_DRAIN_MS_MAX);
    if !drain.quiescent {
        return Err(fail(
            "the drain budget expired before transcription finished".to_owned(),
        ));
    }
    let listing = runtime
        .query_with_input(
            pos_api::QueryName::EvidenceList.as_str(),
            &input_json(&pos_api::EvidenceListInput {
                path: project,
                source_id: None,
                status: None,
                row_count_max: Some(10),
                with_stages: true,
            })
            .map_err(|error| fail(error.to_json()))?,
        )
        .map_err(|error| fail(error.to_json()))?;
    let stage = transcribe_stage(&listing)?;
    let audio_ms = stage
        .get("bytesRead")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| {
            fail(format!(
                "the transcribe row carried no audio duration: {stage}"
            ))
        })?;
    let wall_ms = stage
        .get("wallMs")
        .and_then(serde_json::Value::as_u64)
        .ok_or_else(|| fail(format!("the transcribe row carried no wall time: {stage}")))?;
    runtime.shutdown_background_workers();
    Ok(TranscribeMeasurement {
        audio_ms,
        wall_ms,
        evidence_id: string_field(&submitted, "evidenceId").unwrap_or_default(),
    })
}

/// The one finished TRANSCRIBE row in an `evidence.list --with-stages` answer.
fn transcribe_stage(listing: &str) -> Result<serde_json::Value, ScenarioError> {
    let parsed: serde_json::Value = serde_json::from_str(listing)
        .map_err(|error| fail(format!("evidence.list was not JSON: {error}")))?;
    let rows = parsed
        .get("evidence")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| fail(format!("evidence.list carried no rows: {listing}")))?;
    for row in rows {
        let Some(stages) = row.get("stages").and_then(serde_json::Value::as_array) else {
            continue;
        };
        for stage in stages {
            // Compared against the domain vocabulary, never a literal: the
            // first cut of this scenario looked for state `"ok"` and reported
            // an 18.8x transcription as a missing model.
            let is_transcribe = stage.get("stage").and_then(serde_json::Value::as_str)
                == Some(IngestStage::Transcribe.as_str())
                && stage.get("state").and_then(serde_json::Value::as_str)
                    == Some(StageState::Done.as_str());
            if is_transcribe {
                return Ok(stage.clone());
            }
        }
    }
    Err(fail(format!(
        "no completed TRANSCRIBE stage in the project — was the model pulled? {listing}"
    )))
}

/// Samples this process's resident set while a scenario runs.
///
/// `ps` rather than a platform API: the harness forbids unsafe code, both
/// reference platforms ship `ps`, and a gate that needed a new dependency to
/// read one number would be paying for it forever.
struct RssSampler {
    stop: std::sync::Arc<std::sync::atomic::AtomicBool>,
    peak: std::sync::Arc<std::sync::atomic::AtomicU64>,
    handle: std::thread::JoinHandle<()>,
}

impl RssSampler {
    fn start() -> Self {
        let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
        let peak = std::sync::Arc::new(std::sync::atomic::AtomicU64::new(0));
        let thread_stop = std::sync::Arc::clone(&stop);
        let thread_peak = std::sync::Arc::clone(&peak);
        let handle = std::thread::spawn(move || {
            while !thread_stop.load(std::sync::atomic::Ordering::Relaxed) {
                if let Some(bytes) = read_rss_bytes() {
                    thread_peak.fetch_max(bytes, std::sync::atomic::Ordering::Relaxed);
                }
                std::thread::sleep(std::time::Duration::from_millis(RSS_SAMPLE_INTERVAL_MS));
            }
        });
        Self { stop, peak, handle }
    }

    fn finish(self) -> u64 {
        self.stop.store(true, std::sync::atomic::Ordering::Relaxed);
        let _ = self.handle.join();
        self.peak.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// This process's resident set in bytes, or `None` when the platform did not
/// answer — an unmeasurable number is reported as absent, never as zero.
fn read_rss_bytes() -> Option<u64> {
    let output = Command::new("ps")
        .arg("-o")
        .arg("rss=")
        .arg("-p")
        .arg(std::process::id().to_string())
        .output()
        .ok()?;
    let text = String::from_utf8_lossy(&output.stdout);
    // `ps` reports kibibytes on both reference platforms.
    text.trim().parse::<u64>().ok().map(|kib| kib * 1024)
}

fn command(
    runtime: &pos_api::LocalRuntime,
    name: CommandName,
    input: &str,
) -> Result<String, ScenarioError> {
    runtime
        .command(name.as_str(), input)
        .map_err(|error| fail(error.to_json()))
}

/// Reads one unsigned field out of a report without decoding the whole shape.
/// The bench is a consumer of the surface, not a second definition of it.
fn number_field(report: &str, field: &str) -> Option<u64> {
    let parsed: serde_json::Value = serde_json::from_str(report).ok()?;
    find_number(&parsed, field)
}

fn find_number(value: &serde_json::Value, field: &str) -> Option<u64> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(found) = map.get(field).and_then(serde_json::Value::as_u64) {
                return Some(found);
            }
            map.values().find_map(|nested| find_number(nested, field))
        }
        serde_json::Value::Array(items) => items.iter().find_map(|item| find_number(item, field)),
        _ => None,
    }
}

fn string_field(report: &str, field: &str) -> Option<String> {
    let parsed: serde_json::Value = serde_json::from_str(report).ok()?;
    find_string(&parsed, field)
}

fn find_string(value: &serde_json::Value, field: &str) -> Option<String> {
    match value {
        serde_json::Value::Object(map) => {
            if let Some(found) = map.get(field).and_then(serde_json::Value::as_str) {
                return Some(found.to_owned());
            }
            map.values().find_map(|nested| find_string(nested, field))
        }
        serde_json::Value::Array(items) => items.iter().find_map(|item| find_string(item, field)),
        _ => None,
    }
}

/// Sums a field across every `items` row of a submit report.
fn sum_u64_field(report: &str, field: &str) -> u64 {
    let Ok(parsed) = serde_json::from_str::<serde_json::Value>(report) else {
        return 0;
    };
    parsed
        .get("items")
        .and_then(serde_json::Value::as_array)
        .map(|items| {
            items
                .iter()
                .filter_map(|item| item.get(field).and_then(serde_json::Value::as_u64))
                .sum()
        })
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::{IngestStage, StageState, transcribe_stage};

    /// The listing the scenario actually reads, rendered by `pos-api` from a
    /// `StageRecord`. Written through the same vocabulary the projection
    /// writes, so a rename of either spelling fails here rather than on a
    /// laptop three hours into a campaign.
    fn listing(state: StageState) -> String {
        let stage_row = pos_api::EvidenceStageRow {
            stage: IngestStage::Transcribe.as_str().to_owned(),
            state: state.as_str().to_owned(),
            pass: 0,
            attempt_index: 1,
            wall_ms: Some(184_683),
            bytes_read: Some(3_475_644),
            item_count: Some(656),
            last_error_code: None,
            last_error_detail: None,
        };
        serde_json::json!({ "evidence": [{ "stages": [stage_row] }] }).to_string()
    }

    #[test]
    fn a_finished_transcribe_row_is_the_row_the_gate_reads() {
        let found = transcribe_stage(&listing(StageState::Done))
            .expect("a done transcribe row is the measurement");
        assert_eq!(
            found.get("wallMs").and_then(serde_json::Value::as_u64),
            Some(184_683)
        );
        assert_eq!(
            found.get("bytesRead").and_then(serde_json::Value::as_u64),
            Some(3_475_644)
        );
    }

    #[test]
    fn an_unfinished_transcribe_row_is_not_a_measurement() {
        for state in [StageState::Running, StageState::Retrying, StageState::Dead] {
            assert!(
                transcribe_stage(&listing(state)).is_err(),
                "{} is not a completed transcription",
                state.as_str()
            );
        }
    }
}
