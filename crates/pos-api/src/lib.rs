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
mod project_ops;
mod session;
mod stream;
mod ts_export;

pub use gateway_ops::{
    CostRollupInput, CostRollupReport, CostRollupRow, CostRollupTotals, EventCostLedger,
    ModelsPullInput, ModelsPullReport,
};
pub use project_ops::{ProjectCreateInput, ProjectExportInput, ProjectPathInput, ProjectSeedInput};
// Shells construct runtimes and attribute actors through these foundation
// types; re-exported so a shell needs no direct pos-foundation edge (L12).
pub use pos_foundation::{SystemWallClock as FoundationClock, UserId, WallClock};
pub use session::{HealthReport, OPEN_PROJECT_COUNT_MAX, OpenProjectRow, ProjectListReport};
pub use stream::{
    ResumeWindow, SSE_RETRY_MS, STREAM_RESUME_WINDOW_LEN, StreamFrame, parse_resume_cursor,
    sse_body,
};
pub use ts_export::{check_typescript_api, write_typescript_api};

use pos_capabilities::{
    AccountId, CAPABILITY_TRAIT_VERSION, CapabilityMode, CapabilityRegistry, ConnectorHostRequest,
    ConnectorHostResponse, ConnectorId, LocalCapabilityConfig, ProviderFuture, WorkspaceId,
};
use pos_foundation::SystemWallClock;
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
pub const API_SURFACE_VERSION: u16 = 4;

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
    /// Job queue rows — registered now, implemented by pos-sched (m0-s14).
    JobList,
    /// Model-call cost rollup — implemented by the pos-gateway ledger (m0-s10).
    CostRollup,
    /// Liveness + version identity of this runtime process (m0-s06).
    Health,
}

impl QueryName {
    pub const COUNT: usize = 7;
    pub const ALL: [Self; Self::COUNT] = [
        Self::CapabilitySnapshot,
        Self::ProjectInspect,
        Self::ProjectVerify,
        Self::ProjectList,
        Self::JobList,
        Self::CostRollup,
        Self::Health,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapabilitySnapshot => "capability.snapshot",
            Self::ProjectInspect => "project.inspect",
            Self::ProjectVerify => "project.verify",
            Self::ProjectList => "project.list",
            Self::JobList => "job.list",
            Self::CostRollup => "cost.rollup",
            Self::Health => "health",
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
    /// Opens (validates) a project directory into the session table (m0-s06).
    ProjectOpen,
    /// Run lifecycle — registered now so the surface, contract rows, and
    /// transports are frozen; implemented by the agent harness (m0-s12/s13).
    RunStart,
    RunCancel,
    RunPause,
    RunResume,
    /// Checksummed, consent-gated model download (m0-s11).
    ModelsPull,
}

impl CommandName {
    pub const COUNT: usize = 9;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ProjectCreate,
        Self::ProjectExport,
        Self::ProjectSeedSynthetic,
        Self::ProjectOpen,
        Self::RunStart,
        Self::RunCancel,
        Self::RunPause,
        Self::RunResume,
        Self::ModelsPull,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCreate => "project.create",
            Self::ProjectExport => "project.export",
            Self::ProjectSeedSynthetic => "project.seed-synthetic",
            Self::ProjectOpen => "project.open",
            Self::RunStart => "run.start",
            Self::RunCancel => "run.cancel",
            Self::RunPause => "run.pause",
            Self::RunResume => "run.resume",
            Self::ModelsPull => "models.pull",
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
}

impl LocalBootstrapConfig {
    #[must_use]
    pub fn isolated(pack_root: PathBuf) -> Self {
        Self {
            pack_root,
            user: None,
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
}

/// Process-owned runtime state exposed to thin shell transports.
pub struct LocalRuntime {
    capabilities: CapabilityRegistry,
    identity: project_ops::RuntimeIdentity,
    clock: SystemWallClock,
    open_projects: session::OpenProjects,
}

impl LocalRuntime {
    #[must_use]
    pub fn capability_count(&self) -> usize {
        self.capabilities.descriptors().len()
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
            // Registered-but-later entries answer honestly instead of faking
            // an empty success; their input contracts belong to the stories
            // that implement the engines, so input is deliberately unparsed.
            Some(QueryName::JobList) => Err(ApiError::not_yet_supported(
                "job.list",
                "the pos-sched job queue (m0-s14)",
            )),
            None => Err(ApiError::unknown_query(name)),
        }
    }

    /// Dispatches a state-changing command with a JSON input document.
    pub fn command(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
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
            Some(CommandName::ProjectOpen) => self.open_projects.open(
                &self.identity,
                &self.clock,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::ModelsPull) => {
                gateway_ops::models_pull(&project_ops::parse_input(input_json)?)
            }
            Some(
                CommandName::RunStart
                | CommandName::RunCancel
                | CommandName::RunPause
                | CommandName::RunResume,
            ) => Err(ApiError::not_yet_supported(
                name,
                "the agent harness (m0-s12/m0-s13)",
            )),
            None => Err(ApiError::unknown_command(name)),
        }
    }

    /// Subscribes to a stream, optionally resuming after a client-presented
    /// cursor, and returns the frames currently replayable. Live tailing
    /// arrives with the first real stream producer (m0-s13); the framing and
    /// resume semantics are frozen now so that story changes no transport.
    pub fn stream_subscribe(
        &self,
        name: &str,
        _input_json: &str,
        resume_after: Option<u64>,
    ) -> Result<Vec<StreamFrame>, ApiError> {
        let _ = resume_after;
        match StreamName::parse(name) {
            Some(StreamName::RunSteps) => Err(ApiError::not_yet_supported(
                "run.steps",
                "the echo-agent run feed (m0-s13)",
            )),
            None => Err(ApiError::unknown_stream(name)),
        }
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
        open_projects: session::OpenProjects::default(),
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
