//! # pos-api
//!
//! The ONE typed surface (L12): commands, queries, streams; ts-rs-generated TypeScript types; served identically over axum HTTP+SSE and Tauri IPC. Shells depend on this crate and nothing deeper.
//!
//! Skeleton created by m0-s01; filled by m0-s06. Charter: master plan §19.
//!
//! This crate holds the registry and the wire bytes. It holds no transport: a
//! shell that owns a transport dispatches a name into [`LocalRuntime::query`]
//! and forwards the resulting bytes unchanged. That is what makes shell parity
//! checkable — a transport cannot reshape a result it never decodes.

#![forbid(unsafe_code)]

mod gateway_ops;
#[cfg(feature = "http")]
pub mod http;
mod ingest_ops;
mod ingest_runtime;
mod project_ops;
mod run_ops;
mod sched_ops;
mod session;
mod stream;
mod ts_export;
mod workers;

pub use gateway_ops::{
    CostGroupRow, CostRollupInput, CostRollupReport, CostRollupRow, CostRollupTotals,
    EventCostLedger, ModelsPullFileReport, ModelsPullInput, ModelsPullReport,
};
pub use ingest_ops::{
    EvidenceListInput, EvidenceListReport, EvidenceRow, EvidenceStageRow, IngestReprocessInput,
    IngestReprocessReport, IngestSubmitInput, IngestSubmitReport, IngestSubmitRow,
    SourceHealthInput, SourceHealthReport, SourceHealthRow, TranscriptCorrectInput,
    TranscriptEditReport, TranscriptGetInput, TranscriptReport, TranscriptSegmentRow,
    TranscriptSpeakerAssignInput, TranscriptSpeakerNameInput, TranscriptSpeakerRow,
    UPLOAD_SOURCE_KIND, UPLOAD_SOURCE_SCOPE_DEFAULT,
};
pub use project_ops::{ProjectCreateInput, ProjectExportInput, ProjectPathInput, ProjectSeedInput};
pub use run_ops::{
    EchoFaultInjection, EchoRuntimeOptions, RunBudgetDimensionWire, RunBudgetWire, RunControlInput,
    RunPauseReport, RunReport, RunResumeInput, RunStartInput, RunStepFrame, RunStepsInput,
    RunToolGrantInput, RunToolGrantModeWire, RunWorker,
};
pub use sched_ops::{
    CronPreviewInput, CronPreviewReport, JobListInput, JobListReport, JobRow, job_live_state,
};
// The pipeline-stage vocabulary that `EvidenceStageRow` serializes as strings.
// Re-exported so an api-only consumer (a shell, `pos-bench`) compares against
// the canonical spelling instead of a literal — m1-s07 shipped a bench that
// looked for state `"ok"` against a projection that writes `"done"`, which
// reported a passing gate as a missing model.
pub use ingest_runtime::{
    EMBED_MODEL_ENV, MODELS_DIR_DEFAULT, MODELS_DIR_ENV, TRANSCRIBE_LANGUAGE_ENV,
    WHISPER_MODEL_DEFAULT, WHISPER_MODEL_ENV, embed_setup, models_dir, set_embed_model,
    stage_registry, transcribe_setup,
};
pub use pos_domain::{EVIDENCE_LIST_ROW_COUNT_MAX, IngestStage, StageState};
// Shells construct runtimes and attribute actors through these foundation
// types; re-exported so a shell needs no direct pos-foundation edge (L12).
pub use pos_foundation::{ProjectId, RunId, SystemWallClock as FoundationClock, UserId, WallClock};
// The telemetry vocabulary shells configure and oracles assert against
// (m0-s15); re-exported so a shell still depends on `pos-api` alone (L12).
pub use pos_foundation::telemetry;
pub use session::{
    HealthReport, IngestBufferReport, OPEN_PROJECT_COUNT_MAX, OpenProjectRow, ProjectCloseReport,
    ProjectListReport,
};
pub use stream::{
    ResumeWindow, SSE_RETRY_MS, STREAM_RESUME_WINDOW_LEN, StreamFrame, parse_resume_cursor,
    sse_body,
};
pub use ts_export::{check_typescript_api, write_typescript_api};
pub use workers::{
    WORKER_DRAIN_MS_MAX_DEFAULT, WORKER_SHUTDOWN_MS_MAX_DEFAULT, WorkerConfig, WorkerDrainReport,
    WorkerStatusReport,
};

use pos_capabilities::{
    AccountId, CAPABILITY_TRAIT_VERSION, CapabilityMode, CapabilityRegistry, ConnectorHostRequest,
    ConnectorHostResponse, ConnectorId, LocalCapabilityConfig, ProviderFuture, WorkspaceId,
};
use pos_foundation::SystemWallClock;
use pos_foundation::telemetry::{Parent, SpanDetail, SpanField, SpanName, SpanValue};
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Version of the query/command names and their result shapes. A shape change
/// without a bump is what the m0-s06 contract suite exists to catch.
/// v2: m0-s05 adds the project operations (create/inspect/verify/export/seed).
/// v3: m0-s06 adds the session surface (`project.open`/`project.list`/
/// `health`), the run/job/cost registry entries, and the stream surface.
/// v4: m0-s10/m0-s11 — `cost.rollup` answers with the real ledger rollup
/// instead of `not_yet_supported`, and `models.pull` joins the commands.
/// v5: m0-s12 implements the typed Run start/pause/cancel/resume lifecycle.
/// v6: m0-s13 adds Echo and the durable `run.steps` item contract.
/// v7: m0-s14 implements `job.list` over the real queue and adds
/// `cron.preview`, the tz-aware next-runs answer a cron editor reads.
/// v8: m0-s15 adds the per-project/feature/agent `groups` to `cost.rollup`,
/// so the cost surfaces re-sum nothing in a shell.
/// v9: m1-s01/m1-s02 add the ingestion slice — `evidence.list`,
/// `source.health`, and the `ingest.reprocess` command.
/// v10: m1-s01 wires the m0-s14 worker pool into the shells (ADR-0007) —
/// `project.close` joins the commands, `health` reports whether this process
/// claims queued jobs, and `ingest.reprocess` says whether anything will run
/// what it queued.
/// v11: m1-s03 adds the transcript surface — `transcript.get` reads a
/// recording's decoded speech with any correction over it, and
/// `transcript.correct` / `transcript.speaker-name` /
/// `transcript.speaker-assign` are the three edits. The ASR output is never
/// rewritten: every row carries `asrText` beside `text`, so "the original is
/// recoverable" is something a shell can render rather than a claim.
/// v12: m1-s07 opens the front door — `ingest.submit` puts bytes into a
/// project from a path (desktop, CLI, bench) or from the request body (the
/// browser's upload route), and `health` reports what the ingest buffers
/// actually cost so [ADR-0008]'s bound 1 is readable rather than inferred.
///
/// v13: m1-s04 makes a model artifact a *set* of files — an ONNX encoder is a
/// graph and a vocabulary, and both must be present or neither is usable — so
/// `models.pull` answers one row per file, each with its own verified hash and
/// whether it was already on disk. "The pull succeeded" now means every file
/// was verified rather than the last one. See [ADR-0009].
///
/// [ADR-0008]: ../../../docs/adr/0008-ingest-memory-budget-splits-buffers-from-model-weights.md
/// [ADR-0009]: ../../../docs/adr/0009-vectors-are-a-cas-backed-derived-index.md
pub const API_SURFACE_VERSION: u16 = 13;

/// Bounded item budget for the M0 connector-host liveness tick (L8). The socket
/// itself caps this at 32; the runtime asks for less than it is allowed.
const CONNECTOR_TICK_ITEM_BUDGET: u16 = 8;

/// A provider future that is already resolved needs one poll. Four is slack for
/// a provider that yields once, and a bound instead of an unbounded spin.
const READY_POLL_COUNT_MAX: u8 = 4;

/// Every query in the v0 read surface.
///
/// Adding a variant without adding its row to the transport-parity contract
/// test fails that test, which is the structural version of "remember to test
/// the new endpoint".
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum QueryName {
    CapabilitySnapshot,
    /// Manifest, event count, head seq, snapshot state (m0-s05).
    ProjectInspect,
    /// Re-derives projections against the log + CAS integrity sweep (m0-s05).
    ProjectVerify,
    /// Projects this runtime session has opened (m0-s06).
    ProjectList,
    /// Job queue rows joined with this node's leases (m0-s14).
    JobList,
    /// Next firings of a cron expression in its zone (m0-s14).
    CronPreview,
    /// Model-call cost rollup — implemented by the pos-gateway ledger (m0-s10).
    CostRollup,
    /// Liveness + version identity of this runtime process (m0-s06).
    Health,
    /// Evidence rows with their pipeline status (m1-s01/m1-s02).
    EvidenceList,
    /// Per-source, per-stage ingestion health (m1-s01).
    SourceHealth,
    /// One page of a recording's transcript, with its speakers (m1-s03).
    TranscriptGet,
}

impl QueryName {
    pub const COUNT: usize = 11;
    pub const ALL: [Self; Self::COUNT] = [
        Self::CapabilitySnapshot,
        Self::ProjectInspect,
        Self::ProjectVerify,
        Self::ProjectList,
        Self::JobList,
        Self::CronPreview,
        Self::CostRollup,
        Self::Health,
        Self::EvidenceList,
        Self::SourceHealth,
        Self::TranscriptGet,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapabilitySnapshot => "capability.snapshot",
            Self::ProjectInspect => "project.inspect",
            Self::ProjectVerify => "project.verify",
            Self::ProjectList => "project.list",
            Self::JobList => "job.list",
            Self::CronPreview => "cron.preview",
            Self::CostRollup => "cost.rollup",
            Self::Health => "health",
            Self::EvidenceList => "evidence.list",
            Self::SourceHealth => "source.health",
            Self::TranscriptGet => "transcript.get",
        }
    }

    /// Resolves a transport-supplied name. Unknown names are `None` rather than
    /// a default, so a typo reaches the caller as a typed error instead of
    /// silently returning some other query's data.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|query| query.as_str() == name)
    }
}

/// Every state-changing command in the v0 surface (m0-s05 slice; the full
/// m0-s06 registry adds run/job commands and the ts-rs pipeline).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum CommandName {
    ProjectCreate,
    ProjectExport,
    /// Deterministic synthetic seeding — test/bench scaffolding shared with
    /// `pos-bench` (m0-s05/m0-s16), honest and documented, not a demo trick.
    ProjectSeedSynthetic,
    /// Opens (validates) a project directory into the session table (m0-s06)
    /// and registers it with this process's worker pool (m1-s01).
    ProjectOpen,
    /// Releases a project: the session row and the scheduler registration go
    /// together, so the projects a pool serves are exactly the ones the
    /// process has open (m1-s01).
    ProjectClose,
    /// Run lifecycle — registered now so the surface, contract rows, and
    /// transports are frozen; implemented by the agent harness (m0-s12/s13).
    RunStart,
    RunCancel,
    RunPause,
    RunResume,
    /// Checksummed, consent-gated model download (m0-s11).
    ModelsPull,
    /// A file or a folder of files becomes Evidence (m1-s07). The one way
    /// bytes enter a project, so every shell and both gate scenarios drive
    /// the same path.
    IngestSubmit,
    /// Re-run the ingestion pipeline from a stage (m1-s01). Never re-fetches
    /// from the source — that is the whole point of the command.
    IngestReprocess,
    /// The three transcript edits (m1-s03). Each is an appended fact that
    /// projects *over* the model's output; none of them rewrites it.
    TranscriptCorrect,
    TranscriptSpeakerName,
    TranscriptSpeakerAssign,
}

impl CommandName {
    pub const COUNT: usize = 15;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ProjectCreate,
        Self::ProjectExport,
        Self::ProjectSeedSynthetic,
        Self::ProjectOpen,
        Self::ProjectClose,
        Self::RunStart,
        Self::RunCancel,
        Self::RunPause,
        Self::RunResume,
        Self::ModelsPull,
        Self::IngestSubmit,
        Self::IngestReprocess,
        Self::TranscriptCorrect,
        Self::TranscriptSpeakerName,
        Self::TranscriptSpeakerAssign,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCreate => "project.create",
            Self::ProjectExport => "project.export",
            Self::ProjectSeedSynthetic => "project.seed-synthetic",
            Self::ProjectOpen => "project.open",
            Self::ProjectClose => "project.close",
            Self::RunStart => "run.start",
            Self::RunCancel => "run.cancel",
            Self::RunPause => "run.pause",
            Self::RunResume => "run.resume",
            Self::ModelsPull => "models.pull",
            Self::IngestSubmit => "ingest.submit",
            Self::IngestReprocess => "ingest.reprocess",
            Self::TranscriptCorrect => "transcript.correct",
            Self::TranscriptSpeakerName => "transcript.speaker-name",
            Self::TranscriptSpeakerAssign => "transcript.speaker-assign",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|command| command.as_str() == name)
    }
}

/// Every live item stream in the v0 surface. Streams share the command/query
/// registry discipline: a new variant without a contract row fails the suite.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum StreamName {
    /// Live run steps over SSE — framing frozen here (m0-s06); items arrive
    /// with the echo agent (m0-s13).
    RunSteps,
}

impl StreamName {
    pub const COUNT: usize = 1;
    pub const ALL: [Self; Self::COUNT] = [Self::RunSteps];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::RunSteps => "run.steps",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|stream| stream.as_str() == name)
    }
}

/// The typed error envelope every transport returns unchanged.
#[derive(Clone, Debug, Eq, PartialEq, ts_rs::TS)]
#[ts(rename = "ApiErrorEnvelope")]
pub struct ApiError {
    #[ts(type = "string")]
    pub code: &'static str,
    pub message: String,
    pub retriable: bool,
}

impl ApiError {
    #[must_use]
    pub fn unknown_query(name: &str) -> Self {
        Self {
            code: "unknown_query",
            // The name is echoed as data, never interpolated into a lookup.
            message: format!("no query is registered under the name {name:?}"),
            retriable: false,
        }
    }

    #[must_use]
    pub fn unknown_command(name: &str) -> Self {
        Self {
            code: "unknown_command",
            message: format!("no command is registered under the name {name:?}"),
            retriable: false,
        }
    }

    #[must_use]
    pub fn unknown_stream(name: &str) -> Self {
        Self {
            code: "unknown_stream",
            message: format!("no stream is registered under the name {name:?}"),
            retriable: false,
        }
    }

    /// The honest answer for a surface entry whose engine lands in a later
    /// story (capability-honesty pattern): registered, typed, and explicit
    /// about what implements it — never a fake empty success.
    #[must_use]
    pub fn not_yet_supported(operation: &str, arrives_with: &str) -> Self {
        Self {
            code: "not_yet_supported",
            message: format!(
                "{operation} is registered but not implemented yet; it lands with {arrives_with}"
            ),
            retriable: false,
        }
    }

    /// Serializes to the same envelope shape on every transport.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut json = String::from("{\"code\":");
        push_json_string(&mut json, self.code);
        json.push_str(",\"message\":");
        push_json_string(&mut json, &self.message);
        json.push_str(",\"retriable\":");
        json.push_str(if self.retriable { "true" } else { "false" });
        json.push('}');
        json
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

/// Conservative local-process composition. Media and public ingress stay
/// unavailable unless a later typed configuration explicitly enables them.
pub struct LocalBootstrapConfig {
    pack_root: PathBuf,
    user: Option<pos_foundation::UserId>,
    echo: EchoRuntimeOptions,
}

impl LocalBootstrapConfig {
    #[must_use]
    pub fn isolated(pack_root: PathBuf) -> Self {
        Self {
            pack_root,
            user: None,
            echo: EchoRuntimeOptions::default(),
        }
    }

    /// Attributes appended events to this user instead of the process-local
    /// bootstrap identity — the server shell passes each account's id so
    /// `actor` in the log names who actually acted (m0-s08).
    #[must_use]
    pub fn with_user(mut self, user: pos_foundation::UserId) -> Self {
        self.user = Some(user);
        self
    }

    /// Selects the loopback OpenAI-compatible endpoint used by Echo. The
    /// worker still validates device locality and enforces `local_only`.
    #[must_use]
    pub fn with_echo(mut self, options: EchoRuntimeOptions) -> Self {
        self.echo = options;
        self
    }
}

/// Process-owned runtime state exposed to thin shell transports.
pub struct LocalRuntime {
    capabilities: CapabilityRegistry,
    identity: project_ops::RuntimeIdentity,
    clock: SystemWallClock,
    queue: std::sync::Arc<pos_sched::JobQueue>,
    open_projects: session::OpenProjects,
    workers: Option<workers::BackgroundWorkers>,
    echo_supervisor: run_ops::EchoSupervisor,
}

impl LocalRuntime {
    #[must_use]
    pub fn capability_count(&self) -> usize {
        self.capabilities.descriptors().len()
    }

    /// Starts this process's background worker pool (m1-s01/ADR-0007).
    ///
    /// Explicit, and taken before the runtime is shared, because a pool that
    /// started itself on construction would make "is background work running?"
    /// a question with no honest answer — and because a shell that cannot
    /// start one should say so at startup rather than silently queue work
    /// nothing will claim. Calling it twice replaces the first pool, after
    /// stopping it.
    ///
    /// # Errors
    ///
    /// Returns the typed envelope when the worker thread or its runtime cannot
    /// start, or when the stage handlers cannot be composed.
    pub fn start_background_workers(&mut self, config: WorkerConfig) -> Result<(), ApiError> {
        if let Some(previous) = self.workers.take() {
            previous.shutdown();
        }
        self.workers = Some(workers::BackgroundWorkers::start(
            self.identity.device,
            std::sync::Arc::clone(&self.queue),
            config,
        )?);
        Ok(())
    }

    /// Stops the pool, waiting (bounded) for in-flight jobs. Returns whether
    /// it stopped inside the budget; queued work is durable either way.
    /// Idempotent, and a no-op when no pool was started.
    pub fn shutdown_background_workers(&self) -> bool {
        self.workers
            .as_ref()
            .is_none_or(workers::BackgroundWorkers::shutdown)
    }

    /// Waits, bounded, until every open project's queue is empty — the
    /// one-shot shape: a CLI invocation that queued work runs it and exits.
    /// A process with no pool reports the queue it is not draining rather
    /// than claiming quiescence.
    #[must_use]
    pub fn drain_background_workers(&self, budget_ms: u64) -> WorkerDrainReport {
        match self.workers.as_ref() {
            Some(workers) => workers.drain(budget_ms),
            None => WorkerDrainReport {
                quiescent: false,
                queued_remaining: 0,
                dead_total: 0,
                waited_ms: 0,
                last_read_error: Some(
                    "no background worker pool is running in this process".to_owned(),
                ),
            },
        }
    }

    #[must_use]
    pub fn background_worker_status(&self) -> WorkerStatusReport {
        self.workers
            .as_ref()
            .map_or_else(WorkerStatusReport::stopped, |workers| workers.status())
    }

    /// Dispatches a transport-supplied query name to its canonical JSON result.
    /// Sugar over [`Self::query_with_input`] for input-free queries.
    ///
    /// # Errors
    ///
    /// Returns the typed envelope when no query is registered under `name`.
    pub fn query(&self, name: &str) -> Result<String, ApiError> {
        self.query_with_input(name, "{}")
    }

    /// Dispatches a query with a JSON input document. Input-free queries
    /// accept (and ignore) an empty object so every transport can treat the
    /// pair `(name, input)` uniformly.
    pub fn query_with_input(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
        let span = api_span(
            SpanName::ApiQuery,
            QueryName::parse(name).map(QueryName::as_str),
        );
        let result = self.dispatch_query(name, input_json);
        finish_api_span(span, result.as_ref().err());
        result
    }

    fn dispatch_query(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
        match QueryName::parse(name) {
            Some(QueryName::CapabilitySnapshot) => Ok(self.capability_snapshot_json()),
            Some(QueryName::ProjectInspect) => {
                project_ops::inspect(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::ProjectVerify) => {
                project_ops::verify(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::ProjectList) => self.open_projects.list(),
            Some(QueryName::Health) => self.health_json(),
            Some(QueryName::CostRollup) => gateway_ops::cost_rollup(
                &self.open_projects,
                &project_ops::parse_input(input_json)?,
            ),
            Some(QueryName::JobList) => sched_ops::job_list(&project_ops::parse_input(input_json)?),
            Some(QueryName::CronPreview) => {
                sched_ops::cron_preview(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::EvidenceList) => {
                ingest_ops::evidence_list(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::SourceHealth) => {
                ingest_ops::source_health(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::TranscriptGet) => {
                ingest_ops::transcript_get(&project_ops::parse_input(input_json)?)
            }
            None => Err(ApiError::unknown_query(name)),
        }
    }

    /// Dispatches a state-changing command with a JSON input document.
    pub fn command(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
        let mut span = api_span(
            SpanName::ApiCommand,
            CommandName::parse(name).map(CommandName::as_str),
        );
        let result = self.dispatch_command(name, input_json, &mut span);
        finish_api_span(span, result.as_ref().err());
        result
    }

    /// Dispatches a command whose bytes arrived with the request rather than
    /// living on this machine — the browser upload path.
    ///
    /// The transport streams the request body to a file it owns and passes
    /// the path; it never decodes or rewrites the caller's input, which is
    /// what keeps "a transport selects an operation and forwards bytes" true
    /// for a request that carries four gigabytes (L12).
    ///
    /// # Errors
    ///
    /// [`ApiError::unknown_command`] for any name but `ingest.submit`, and
    /// whatever the command itself refuses with.
    pub fn command_with_upload(
        &self,
        name: &str,
        input_json: &str,
        staged: &std::path::Path,
    ) -> Result<String, ApiError> {
        let span = api_span(
            SpanName::ApiCommand,
            CommandName::parse(name).map(CommandName::as_str),
        );
        let result = match CommandName::parse(name) {
            Some(CommandName::IngestSubmit) => project_ops::parse_input(input_json)
                .and_then(|input| self.ingest_submit(&input, Some(staged))),
            // Every other command's input is a small typed document; a body
            // on one of them is a caller mistake, and answering it as if the
            // bytes were welcome would hide that.
            _ => Err(ApiError::unknown_command(name)),
        };
        finish_api_span(span, result.as_ref().err());
        result
    }

    /// The intake command needs the acting identity and the project id for
    /// the same reason reprocess does, and additionally wakes the pool: an
    /// upload whose first stage sat until the next idle poll would look
    /// broken for no reason a user could see.
    fn ingest_submit(
        &self,
        input: &ingest_ops::IngestSubmitInput,
        staged: Option<&std::path::Path>,
    ) -> Result<String, ApiError> {
        let log = project_ops::open_log(std::path::Path::new(&input.path))?;
        let project_id = ingest_ops::project_id_of(&log)?;
        let report = ingest_ops::ingest_submit(
            self.identity.device,
            self.identity.user,
            project_id,
            &self.queue,
            self.workers.is_some(),
            input,
            staged,
        )?;
        if let Some(workers) = self.workers.as_ref() {
            workers.wake();
        }
        Ok(report)
    }

    /// The reprocess command needs the acting identity and the project the
    /// directory holds, neither of which belongs in a wire input: the actor
    /// is the session's, and the project id is a fact in the log.
    ///
    /// The pool is woken after the enqueue commits, so the latency of a
    /// requeued item is the claim rather than the idle poll interval.
    fn ingest_reprocess(
        &self,
        input: &ingest_ops::IngestReprocessInput,
    ) -> Result<String, ApiError> {
        let log = project_ops::open_log(std::path::Path::new(&input.path))?;
        let project_id = ingest_ops::project_id_of(&log)?;
        let report = ingest_ops::ingest_reprocess(
            self.identity.device,
            self.identity.user,
            project_id,
            &self.queue,
            self.workers.is_some(),
            input,
        )?;
        if let Some(workers) = self.workers.as_ref() {
            workers.wake();
        }
        Ok(report)
    }

    /// Opens a project into the session table and registers it with the pool.
    ///
    /// Registration failing fails the open: a project that is tracked but not
    /// served would look open in `project.list` while every job queued against
    /// it sat unclaimed — precisely the silent half-wired state this seam
    /// exists to make impossible.
    fn open_project(&self, input: &ProjectPathInput) -> Result<String, ApiError> {
        let opened = self
            .open_projects
            .open(&self.identity, &self.clock, input)?;
        if let Some(workers) = self.workers.as_ref() {
            workers.register(opened.project_id, std::sync::Arc::clone(&opened.log))?;
        }
        Ok(opened.json)
    }

    /// Releases a project from the session table and the pool together.
    fn close_project(&self, input: &ProjectPathInput) -> Result<String, ApiError> {
        let (project_id, json) = self.open_projects.close(input)?;
        if let Some(workers) = self.workers.as_ref() {
            workers.unregister(project_id);
        }
        Ok(json)
    }

    fn dispatch_command(
        &self,
        name: &str,
        input_json: &str,
        span: &mut pos_foundation::telemetry::Span,
    ) -> Result<String, ApiError> {
        match CommandName::parse(name) {
            Some(CommandName::ProjectCreate) => project_ops::create(
                &self.identity,
                &self.clock,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::ProjectExport) => {
                project_ops::export(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::ProjectSeedSynthetic) => {
                project_ops::seed_synthetic(&self.clock, &project_ops::parse_input(input_json)?)
            }
            Some(CommandName::ProjectOpen) => {
                self.open_project(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::ProjectClose) => {
                self.close_project(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::ModelsPull) => {
                gateway_ops::models_pull(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::IngestSubmit) => {
                self.ingest_submit(&project_ops::parse_input(input_json)?, None)
            }
            Some(CommandName::IngestReprocess) => {
                self.ingest_reprocess(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::TranscriptCorrect) => ingest_ops::transcript_correct(
                self.identity.device,
                self.identity.user,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::TranscriptSpeakerName) => ingest_ops::transcript_speaker_name(
                self.identity.device,
                self.identity.user,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::TranscriptSpeakerAssign) => ingest_ops::transcript_speaker_assign(
                self.identity.device,
                self.identity.user,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::RunStart) => run_ops::start(
                &self.identity,
                &self.clock,
                &self.echo_supervisor,
                &project_ops::parse_input(input_json)?,
                span,
            ),
            Some(CommandName::RunCancel) => run_ops::cancel(
                &self.identity,
                &self.clock,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::RunPause) => run_ops::pause(
                &self.identity,
                &self.clock,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::RunResume) => run_ops::resume(
                &self.identity,
                &self.clock,
                &self.echo_supervisor,
                &project_ops::parse_input(input_json)?,
            ),
            None => Err(ApiError::unknown_command(name)),
        }
    }

    /// Subscribes to a stream, optionally resuming after a client-presented
    /// cursor, and returns the durable frames currently replayable. The m0-s13
    /// Echo producer also follows this cursor through [`Self::stream_follow`];
    /// both transports keep the same frozen framing and resume semantics.
    pub fn stream_subscribe(
        &self,
        name: &str,
        input_json: &str,
        resume_after: Option<u64>,
    ) -> Result<Vec<StreamFrame>, ApiError> {
        let span = api_span(
            SpanName::ApiStream,
            StreamName::parse(name).map(StreamName::as_str),
        );
        let result = match StreamName::parse(name) {
            Some(StreamName::RunSteps) => {
                run_ops::stream_subscribe(&project_ops::parse_input(input_json)?, resume_after)
            }
            None => Err(ApiError::unknown_stream(name)),
        };
        if let Ok(frames) = &result {
            span.set(
                SpanField::Frames,
                SpanValue::Count(frames.len().try_into().unwrap_or(u64::MAX)), // INVARIANT: the resume window caps a subscribe at 256 frames.
            );
        }
        finish_api_span(span, result.as_ref().err());
        result
    }

    /// Replays durable frames after `resume_after`, then tails checkpoint
    /// boundaries until the Run becomes terminal or the consumer disconnects.
    pub fn stream_follow(
        &self,
        name: &str,
        input_json: &str,
        resume_after: Option<u64>,
        mut consume: impl FnMut(StreamFrame) -> bool,
    ) -> Result<(), ApiError> {
        let span = api_span(
            SpanName::ApiStream,
            StreamName::parse(name).map(StreamName::as_str),
        );
        let mut frame_count: u64 = 0;
        let result = match StreamName::parse(name) {
            Some(StreamName::RunSteps) => run_ops::stream_follow(
                &project_ops::parse_input(input_json)?,
                resume_after,
                |frame| {
                    frame_count = frame_count.saturating_add(1);
                    consume(frame)
                },
            ),
            None => Err(ApiError::unknown_stream(name)),
        };
        span.set(SpanField::Frames, SpanValue::Count(frame_count));
        finish_api_span(span, result.as_ref().err());
        result
    }

    /// Liveness with version identity — real values read from this process,
    /// nothing hard-coded about runtime state.
    fn health_json(&self) -> Result<String, ApiError> {
        let count = u32::try_from(self.open_projects.count()).unwrap_or(u32::MAX); // INVARIANT: the session table is capped at OPEN_PROJECT_COUNT_MAX (64).
        input_json(&HealthReport {
            status: "ok",
            api_surface_version: API_SURFACE_VERSION,
            capability_trait_version: CAPABILITY_TRAIT_VERSION,
            format_version: pos_store::FORMAT_VERSION,
            open_project_count: count,
            background_workers: self.background_worker_status(),
            ingest_buffers: pos_ingest::buffer_residency().into(),
        })
    }

    /// Renders the live registry state the UI renders (m0-s17 capability
    /// honesty). Every field is read from the resolved providers at call time;
    /// nothing here is a compile-time literal about runtime availability.
    #[must_use]
    pub fn capability_snapshot_json(&self) -> String {
        let mut json = String::from("{\"surfaceVersion\":");
        json.push_str(&API_SURFACE_VERSION.to_string());
        json.push_str(",\"capabilityTraitVersion\":");
        json.push_str(&CAPABILITY_TRAIT_VERSION.to_string());
        json.push_str(",\"capabilities\":[");
        for (index, descriptor) in self.capabilities.descriptors().iter().enumerate() {
            if index > 0 {
                json.push(',');
            }
            json.push_str("{\"id\":");
            push_json_string(&mut json, descriptor.id.as_str());
            json.push_str(",\"provider\":");
            push_json_string(&mut json, descriptor.provider_name);
            json.push_str(",\"state\":");
            push_capability_state(&mut json, &descriptor.mode);
            json.push('}');
        }
        json.push_str("],\"connectorHost\":");
        match self.connector_host_tick() {
            Some(tick) => {
                json.push_str("{\"hostAvailable\":");
                json.push_str(if tick.host_available { "true" } else { "false" });
                json.push_str(",\"polledCount\":");
                json.push_str(&tick.polled_count.to_string());
                json.push_str(",\"nextCursor\":");
                json.push_str(&tick.next_cursor.to_string());
                json.push('}');
            }
            None => json.push_str("null"),
        }
        json.push('}');
        json
    }

    /// Runs the bounded local mock poll/health tick. `None` means the tick did
    /// not complete, which the UI renders as an unknown host rather than as a
    /// healthy one — a failed liveness check is not evidence of liveness.
    fn connector_host_tick(&self) -> Option<ConnectorHostTick> {
        let Ok(connector_id) = ConnectorId::new("mock") else {
            return None;
        };
        let request = ConnectorHostRequest::Tick {
            connector_id,
            cursor: 0,
            max_items: CONNECTOR_TICK_ITEM_BUDGET,
        };
        match poll_ready(self.capabilities.connector_host().execute(request))? {
            Ok(ConnectorHostResponse::Tick {
                polled_count,
                next_cursor,
                host_available,
            }) => Some(ConnectorHostTick {
                polled_count,
                next_cursor,
                host_available,
            }),
            _ => None,
        }
    }
}

/// Installs the telemetry pipeline from one shell-supplied spec (m0-s15).
///
/// Every shell calls this with the value of its own configuration key, so the
/// grammar is parsed once here rather than three times in three shells (L12).
/// Absent means **off**: a desktop that was not asked to export telemetry
/// writes none (L4 spirit).
///
/// Grammar: `off` · `stderr` · `file:<path>` · `otlp:<endpoint>`.
///
/// # Errors
///
/// A typed envelope for an unparseable spec, an unopenable file, or the
/// registered-but-unimplemented OTLP target. A shell reports it and stops
/// rather than starting with export silently disabled — an operator who asked
/// for traces and got none would be reading an empty collector as evidence.
pub fn install_telemetry(spec: Option<&str>) -> Result<(), ApiError> {
    let config = match spec.map(str::trim) {
        None | Some("") | Some("off") => pos_foundation::telemetry::TelemetryConfig::off(),
        Some("stderr") => pos_foundation::telemetry::TelemetryConfig::stderr(),
        Some(other) => match other.split_once(':') {
            Some(("file", path)) if !path.is_empty() => {
                pos_foundation::telemetry::TelemetryConfig::json_lines(PathBuf::from(path))
            }
            Some(("otlp", endpoint)) if !endpoint.is_empty() => {
                pos_foundation::telemetry::TelemetryConfig {
                    export: pos_foundation::telemetry::TelemetryExport::Otlp {
                        endpoint: endpoint.to_owned(),
                    },
                }
            }
            _ => {
                return Err(ApiError {
                    code: "invalid_input",
                    message: "telemetry must be one of: off, stderr, file:<path>, otlp:<endpoint>"
                        .to_owned(),
                    retriable: false,
                });
            }
        },
    };
    pos_foundation::telemetry::install(config).map_err(|error| ApiError {
        code: error.code,
        message: error.message,
        retriable: false,
    })
}

/// Opens the dispatch span for one API name (m0-s15). An unregistered name
/// still gets a span — a caller hammering a typo is exactly the thing a
/// trace should show — under the static label `unknown`.
fn api_span(name: SpanName, registered: Option<&'static str>) -> pos_foundation::telemetry::Span {
    pos_foundation::telemetry::Span::open(
        name,
        SpanDetail::from_static(registered.unwrap_or("unknown")),
        Parent::Current,
    )
}

/// Closes a dispatch span with the code the caller will actually receive.
/// `ApiError::code` is already `&'static str`, so the outcome vocabulary and
/// the wire vocabulary are the same list by construction.
fn finish_api_span(span: pos_foundation::telemetry::Span, error: Option<&ApiError>) {
    match error {
        Some(error) => span.finish(error.code),
        None => span.finish("ok"),
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectorHostTick {
    polled_count: u16,
    next_cursor: u64,
    host_available: bool,
}

/// Resolves a provider future without a runtime, within a fixed poll budget.
///
/// Local providers resolve immediately by construction. This keeps M0 shells
/// free of an async runtime while still driving the real socket; m0-s08 brings
/// the executor with the server that needs one.
fn poll_ready<T>(future: ProviderFuture<'_, T>) -> Option<T> {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..READY_POLL_COUNT_MAX {
        if let Poll::Ready(output) = Future::poll(future.as_mut(), &mut context) {
            return Some(output);
        }
    }
    None
}

fn push_capability_state(json: &mut String, mode: &CapabilityMode) {
    match mode {
        CapabilityMode::Local => json.push_str("{\"mode\":\"local\"}"),
        CapabilityMode::Hosted => json.push_str("{\"mode\":\"hosted\"}"),
        CapabilityMode::Unavailable(reason) => {
            json.push_str("{\"mode\":\"unavailable\",\"reason\":");
            push_json_string(json, reason.as_str());
            json.push('}');
        }
    }
}

/// Writes a JSON string literal. Escaping is done here, once, so no caller can
/// build a result by concatenating unescaped provider text.
fn push_json_string(json: &mut String, value: &str) {
    json.push('"');
    for character in value.chars() {
        match character {
            '"' => json.push_str("\\\""),
            '\\' => json.push_str("\\\\"),
            '\n' => json.push_str("\\n"),
            '\r' => json.push_str("\\r"),
            '\t' => json.push_str("\\t"),
            control if control < ' ' => {
                json.push_str("\\u00");
                let code = u32::from(control);
                json.push(hex_digit(code >> 4));
                json.push(hex_digit(code & 0xf));
            }
            other => json.push(other),
        }
    }
    json.push('"');
}

/// Callers pass a single nibble, so the fallback is unreachable. It is a `'0'`
/// rather than a panic because a malformed escape is a cosmetic defect and a
/// crashed shell is not.
fn hex_digit(nibble: u32) -> char {
    char::from_digit(nibble, 16).unwrap_or('0')
}

/// Resolves all ten public capability sockets for a standalone process.
///
/// The fixed ids are process-local bootstrap identities, not durable ProjectOS
/// entity ids. m0-s06/m0-s08 replace them with values loaded through the typed
/// startup surface before any project state exists.
#[must_use]
pub fn bootstrap_local_runtime(config: LocalBootstrapConfig) -> LocalRuntime {
    let identity = match config.user {
        Some(user) => project_ops::RuntimeIdentity::for_user(user),
        None => project_ops::RuntimeIdentity::bootstrap(),
    };
    LocalRuntime {
        capabilities: CapabilityRegistry::local(LocalCapabilityConfig {
            owner_account_id: AccountId::from_bytes([0; 16]),
            workspace_id: WorkspaceId::from_bytes([0; 16]),
            pack_root: config.pack_root,
            ffmpeg_available: false,
            ingress_reachable: false,
        }),
        identity,
        clock: SystemWallClock,
        queue: std::sync::Arc::new(ingest_ops::runtime_queue(identity.device)),
        open_projects: session::OpenProjects::default(),
        // Background work starts only when a shell asks for it
        // (`start_background_workers`); see `workers.rs`.
        workers: None,
        echo_supervisor: run_ops::EchoSupervisor::new(config.echo),
    }
}

/// Serializes a typed input for the `(name, input)` dispatch pair — the one
/// JSON encoder shells use, so a shell never hand-writes wire JSON.
pub fn input_json(input: &impl serde::Serialize) -> Result<String, ApiError> {
    project_ops::to_json(input)
}

/// Generates the M0 capability-card vocabulary through the single UI-facing
/// Rust surface. m0-s06 replaces the hand-rendered TypeScript shape with the
/// general ts-rs export pipeline without changing its source of truth.
#[must_use]
pub fn typescript_capability_catalog() -> String {
    pos_capabilities::typescript_catalog()
}

#[cfg(test)]
mod tests {
    use super::{
        ApiError, LocalBootstrapConfig, LocalRuntime, QueryName, bootstrap_local_runtime,
        push_json_string,
    };
    use pos_capabilities::{CapabilityId, CapabilityMode};
    use std::path::PathBuf;

    fn runtime() -> LocalRuntime {
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-bootstrap-pack-root",
        )))
    }

    #[test]
    fn isolated_startup_resolves_every_socket_with_honest_state() {
        let runtime = runtime();
        assert_eq!(runtime.capability_count(), CapabilityId::COUNT);
        let descriptors = runtime.capabilities.descriptors();
        let connector = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::ConnectorHost)
            .expect("complete registry contains connector.host");
        assert!(matches!(connector.mode, CapabilityMode::Local));
        let ingress = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::RelayIngress)
            .expect("complete registry contains relay.ingress");
        assert!(matches!(
            ingress.mode,
            CapabilityMode::Unavailable(ref reason) if !reason.as_str().is_empty()
        ));
    }

    #[test]
    fn every_query_name_round_trips_and_unknown_names_are_typed_errors() {
        for query in QueryName::ALL {
            assert_eq!(QueryName::parse(query.as_str()), Some(query));
        }
        assert_eq!(QueryName::parse("capability.snapshot "), None);
        assert_eq!(QueryName::ALL.len(), QueryName::COUNT);

        let error = runtime()
            .query("capability.snapshot; DROP")
            .expect_err("an unregistered name must not resolve to a registered query");
        assert_eq!(error.code, "unknown_query");
        assert!(!error.retriable);
    }

    #[test]
    fn the_snapshot_reports_live_state_for_all_ten_sockets() {
        let snapshot = runtime()
            .query(QueryName::CapabilitySnapshot.as_str())
            .expect("the registered query resolves");
        for id in CapabilityId::ALL {
            assert!(
                snapshot.contains(&format!("\"id\":\"{}\"", id.as_str())),
                "{} is missing from the live snapshot",
                id.as_str()
            );
        }
        // The bounded mock tick actually ran, rather than being asserted true
        // by the UI: an unavailable host would render as `null` here.
        assert!(snapshot.contains("\"connectorHost\":{\"hostAvailable\":true"));
        assert!(snapshot.contains("\"mode\":\"unavailable\",\"reason\":\""));
        assert!(!snapshot.contains("\"reason\":\"\""));
    }

    #[test]
    fn repeated_dispatch_is_byte_identical() {
        let runtime = runtime();
        let first = runtime.capability_snapshot_json();
        let second = runtime
            .query(QueryName::CapabilitySnapshot.as_str())
            .expect("the registered query resolves");
        assert_eq!(first, second, "transports must forward identical bytes");
    }

    #[test]
    fn provider_text_cannot_break_out_of_a_json_string() {
        let mut json = String::new();
        push_json_string(&mut json, "quote\" backslash\\ newline\n tab\t bell\u{7}");
        assert_eq!(
            json,
            "\"quote\\\" backslash\\\\ newline\\n tab\\t bell\\u0007\""
        );
    }

    #[test]
    fn the_error_envelope_has_all_three_fields() {
        let json = ApiError::unknown_query("nope").to_json();
        assert!(json.starts_with("{\"code\":\"unknown_query\""));
        assert!(json.contains("\"message\":"));
        assert!(json.ends_with("\"retriable\":false}"));
    }
}
