//! Typed `pos-api` composition for the m0-s12 Run lifecycle.
//!
//! The shell supplies JSON and forwards JSON. This module alone opens the
//! project log, mints the Run id, composes the default harness registries,
//! attributes the user actor, and maps harness failures into the shared API
//! envelope. No transport owns Run behavior (L12).

use crate::ApiError;
use crate::gateway_ops::EventCostLedger;
use crate::project_ops::{RuntimeIdentity, log_error, open_log, store_error, to_json};
use crate::stream::{ResumeWindow, STREAM_RESUME_WINDOW_LEN, StreamFrame};
use pos_agents::{
    AutonomyLevel, EchoAgent, EchoFaultPlan, EchoFaultPoint, HarnessError, RosterCharter,
    RosterRegistry, RunHarness, RunStartSpec, RunToolGrants, RuntimeId, RuntimeRegistry,
    ToolGrantMode, ToolId, ToolRegistry, echo_tool_grants, echo_tool_registry,
};
use pos_domain::{
    RunBudget, RunBudgetDimension, RunExecutor, RunPauseState, RunState, RunStatus, RunStepState,
    RunToolGrantMode, RunTrigger, RunUsage, read_run_step,
};
use pos_foundation::{ProjectId, RunId, SystemWallClock, WallClock};
use pos_gateway::{
    CredentialClass, EndpointConfig, EndpointLocality, EndpointProfile, EndpointServer, Gateway,
    GatewayConfig, LoopbackHttpTransport, MemorySecretStore, ModelChoice, ModelPolicy,
    ModelRouting, OpenAiCompatibleAdapter, PromptFile, ProviderFamily,
};
use pos_log::Actor;
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use ts_rs::TS;

const ECHO_DEFAULT_BASE_URL: &str = "http://127.0.0.1:11434";
const ECHO_DEFAULT_MODEL: &str = "gemma4:12b";
const ECHO_MODEL_NAME_LEN_MAX: usize = 128;
const RUN_STREAM_POLL_MS: u64 = 25;

/// Process composition for the disposable M0 Echo worker. The endpoint is
/// always revalidated as device-local before a worker starts; this type cannot
/// widen the hard `local_only` policy.
#[derive(Clone, Debug)]
pub struct EchoRuntimeOptions {
    base_url: String,
    model: String,
    boundary_delay_ms: u64,
    fault: Option<EchoFaultInjection>,
}

impl Default for EchoRuntimeOptions {
    fn default() -> Self {
        Self {
            base_url: ECHO_DEFAULT_BASE_URL.to_owned(),
            model: ECHO_DEFAULT_MODEL.to_owned(),
            boundary_delay_ms: 0,
            fault: None,
        }
    }
}

impl EchoRuntimeOptions {
    #[must_use]
    pub fn loopback(base_url: impl Into<String>, model: impl Into<String>) -> Self {
        Self {
            base_url: base_url.into(),
            model: model.into(),
            ..Self::default()
        }
    }

    #[must_use]
    pub const fn with_boundary_delay_ms(mut self, delay_ms: u64) -> Self {
        self.boundary_delay_ms = delay_ms;
        self
    }

    #[must_use]
    pub fn with_fault(mut self, fault: EchoFaultInjection) -> Self {
        self.fault = Some(fault);
        self
    }
}

/// Test-owned process-kill boundary. Normal product bootstrap never sets it;
/// the chaos child modes do so explicitly with an absolute marker path.
#[derive(Clone, Debug)]
pub enum EchoFaultInjection {
    AfterCommit { step_index: u32, marker: PathBuf },
    AfterCheckpoint { step_index: u32, marker: PathBuf },
}

impl EchoFaultInjection {
    fn plan(&self) -> Result<EchoFaultPlan, ApiError> {
        let (point, marker) = match self {
            Self::AfterCommit { step_index, marker } => {
                (EchoFaultPoint::AfterCommit(*step_index), marker.clone())
            }
            Self::AfterCheckpoint { step_index, marker } => {
                (EchoFaultPoint::AfterCheckpoint(*step_index), marker.clone())
            }
        };
        EchoFaultPlan::new(point, marker).map_err(|error| ApiError {
            code: "invalid_input",
            message: error.to_string(),
            retriable: false,
        })
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum RunWorker {
    Navigator,
    Analyst,
    Archivist,
    Planner,
    Foreman,
    Scout,
    Sentinel,
    IncidentCommander,
    Investigator,
    Verifier,
    Scribe,
    Echo,
}

impl RunWorker {
    const fn charter(self) -> RosterCharter {
        match self {
            Self::Navigator => RosterCharter::Navigator,
            Self::Analyst => RosterCharter::Analyst,
            Self::Archivist => RosterCharter::Archivist,
            Self::Planner => RosterCharter::Planner,
            Self::Foreman => RosterCharter::Foreman,
            Self::Scout => RosterCharter::Scout,
            Self::Sentinel => RosterCharter::Sentinel,
            Self::IncidentCommander => RosterCharter::IncidentCommander,
            Self::Investigator => RosterCharter::Investigator,
            Self::Verifier => RosterCharter::Verifier,
            Self::Scribe => RosterCharter::Scribe,
            Self::Echo => RosterCharter::Echo,
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunBudgetWire {
    #[ts(type = "number")]
    pub tokens: u64,
    #[ts(type = "number")]
    pub usd_micros: u64,
    #[ts(type = "number")]
    pub wall_ms: u64,
    #[ts(type = "number")]
    pub storage_bytes: u64,
    pub tool_calls: u32,
    pub retries: u32,
    pub steps: u32,
}

impl From<RunBudgetWire> for RunBudget {
    fn from(value: RunBudgetWire) -> Self {
        Self {
            tokens: value.tokens,
            usd_micros: value.usd_micros,
            wall_ms: value.wall_ms,
            storage_bytes: value.storage_bytes,
            tool_calls: value.tool_calls,
            retries: value.retries,
            steps: value.steps,
        }
    }
}

impl From<RunBudget> for RunBudgetWire {
    fn from(value: RunBudget) -> Self {
        Self {
            tokens: value.tokens,
            usd_micros: value.usd_micros,
            wall_ms: value.wall_ms,
            storage_bytes: value.storage_bytes,
            tool_calls: value.tool_calls,
            retries: value.retries,
            steps: value.steps,
        }
    }
}

impl From<RunUsage> for RunBudgetWire {
    fn from(value: RunUsage) -> Self {
        Self {
            tokens: value.tokens,
            usd_micros: value.usd_micros,
            wall_ms: value.wall_ms,
            storage_bytes: value.storage_bytes,
            tool_calls: value.tool_calls,
            retries: value.retries,
            steps: value.steps,
        }
    }
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunStartInput {
    pub path: String,
    pub worker: RunWorker,
    #[serde(default = "default_autonomy_level")]
    pub autonomy_level: u8,
    pub budget: RunBudgetWire,
    #[serde(default)]
    pub tool_grants: Vec<RunToolGrantInput>,
    #[serde(default)]
    pub parent_run_id: Option<String>,
}

#[derive(Clone, Copy, Debug, Deserialize, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum RunToolGrantModeWire {
    Allow,
    Gate,
    Block,
}

#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunToolGrantInput {
    pub tool_id: String,
    pub mode: RunToolGrantModeWire,
}

const fn default_autonomy_level() -> u8 {
    2
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunControlInput {
    pub path: String,
    pub run_id: String,
    pub reason: String,
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunResumeInput {
    pub path: String,
    pub run_id: String,
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunStepsInput {
    pub path: String,
    pub run_id: String,
}

/// One checkpoint-complete Run step. Frames are emitted only after the
/// receipt + checkpoint batch commits, so the UI never renders a tool effect
/// as durable before it is actually resumable.
#[derive(Clone, Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct RunStepFrame {
    pub run_id: String,
    pub project_id: Option<String>,
    #[ts(type = "number")]
    pub stream_seq: u64,
    pub step_index: u32,
    pub phase: String,
    pub summary: String,
    pub tool_id: Option<String>,
    #[ts(type = "number")]
    pub committed_seq: u64,
    #[ts(type = "number")]
    pub checkpoint_seq: u64,
    pub spent: RunBudgetWire,
    pub run_status: String,
    pub terminal: bool,
    pub validation_status: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub enum RunBudgetDimensionWire {
    Tokens,
    UsdMicros,
    WallMs,
    StorageBytes,
    ToolCalls,
    Retries,
    Steps,
}

impl From<RunBudgetDimension> for RunBudgetDimensionWire {
    fn from(value: RunBudgetDimension) -> Self {
        match value {
            RunBudgetDimension::Tokens => Self::Tokens,
            RunBudgetDimension::UsdMicros => Self::UsdMicros,
            RunBudgetDimension::WallMs => Self::WallMs,
            RunBudgetDimension::StorageBytes => Self::StorageBytes,
            RunBudgetDimension::ToolCalls => Self::ToolCalls,
            RunBudgetDimension::Retries => Self::Retries,
            RunBudgetDimension::Steps => Self::Steps,
        }
    }
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase", tag = "kind")]
pub enum RunPauseReport {
    Budget {
        dimension: RunBudgetDimensionWire,
        #[ts(type = "number")]
        limit: u64,
        #[ts(type = "number")]
        spent: u64,
        #[ts(type = "number")]
        pending: u64,
        #[ts(type = "number")]
        requested: u64,
    },
    Requested {
        reason: String,
    },
    ToolWeather {
        code: String,
    },
    ReservationExceeded {
        dimension: RunBudgetDimensionWire,
    },
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct RunReport {
    pub path: String,
    pub run_id: String,
    pub project_id: Option<String>,
    pub worker: String,
    pub runtime_id: String,
    pub executor: String,
    pub status: String,
    pub autonomy_level: u8,
    pub committed_step_count: u32,
    pub checkpointed_step_count: u32,
    pub budget: RunBudgetWire,
    pub spent: RunBudgetWire,
    pub tainted: bool,
    pub tool_grants: Vec<RunToolGrantInput>,
    pub parent_run_id: Option<String>,
    pub lineage_depth: u8,
    pub pending_control: Option<String>,
    pub pause: Option<RunPauseReport>,
}

#[derive(Clone)]
pub(crate) struct EchoSupervisor {
    options: EchoRuntimeOptions,
    active: ActiveEchoRuns,
}

type EchoRunKey = ([u8; 16], [u8; 16]);
type ActiveEchoRuns = Arc<Mutex<BTreeSet<EchoRunKey>>>;

impl EchoSupervisor {
    pub(crate) fn new(options: EchoRuntimeOptions) -> Self {
        Self {
            options,
            active: Arc::new(Mutex::new(BTreeSet::new())),
        }
    }

    fn validate(&self, identity: RuntimeIdentity) -> Result<(), ApiError> {
        let _ = echo_choice(&self.options, identity.device)?;
        PromptFile::from_embedded("echo@1.md", include_bytes!("../../../prompts/echo@1.md"))
            .map_err(|error| ApiError {
                code: "run_worker_failure",
                message: format!("the embedded Echo prompt is invalid: {error}"),
                retriable: false,
            })?;
        if let Some(fault) = &self.options.fault {
            let _ = fault.plan()?;
        }
        Ok(())
    }

    fn launch(
        &self,
        identity: RuntimeIdentity,
        path: String,
        project_id: ProjectId,
        run_id: RunId,
    ) -> Result<(), ApiError> {
        let choice = echo_choice(&self.options, identity.device)?;
        let prompt =
            PromptFile::from_embedded("echo@1.md", include_bytes!("../../../prompts/echo@1.md"))
                .map_err(|error| ApiError {
                    code: "run_worker_failure",
                    message: format!("the embedded Echo prompt is invalid: {error}"),
                    retriable: false,
                })?;
        let fault = self
            .options
            .fault
            .as_ref()
            .map(EchoFaultInjection::plan)
            .transpose()?;
        let key = (project_id.into_bytes(), run_id.into_bytes());
        {
            let mut active = active_runs(&self.active);
            if !active.insert(key) {
                return Ok(());
            }
        }
        let active = Arc::clone(&self.active);
        let base_url = self.options.base_url.trim_end_matches('/').to_owned();
        let boundary_delay_ms = self.options.boundary_delay_ms;
        let spawned = std::thread::Builder::new()
            .name(format!("pos-echo-{}", &run_id.to_hex()[..8]))
            .spawn(move || {
                let _active = ActiveRunGuard { active, key };
                if let Err(error) = execute_echo_worker(
                    identity,
                    &path,
                    run_id,
                    &base_url,
                    choice,
                    &prompt,
                    boundary_delay_ms,
                    fault,
                )
                {
                    // No provider text or project content belongs in process
                    // logs. Durable Run/model facts carry the diagnostic state.
                    eprintln!(
                        "pos-api: Echo Run {} stopped before a terminal boundary ({}); resume is available",
                        run_id.to_hex(),
                        error.code
                    );
                }
            });
        if let Err(error) = spawned {
            active_runs(&self.active).remove(&key);
            return Err(ApiError {
                code: "dispatch_failure",
                message: format!("could not start the Echo worker thread: {error}"),
                retriable: true,
            });
        }
        Ok(())
    }
}

struct ActiveRunGuard {
    active: ActiveEchoRuns,
    key: EchoRunKey,
}

impl Drop for ActiveRunGuard {
    fn drop(&mut self) {
        active_runs(&self.active).remove(&self.key);
    }
}

fn active_runs(
    active: &Mutex<BTreeSet<EchoRunKey>>,
) -> std::sync::MutexGuard<'_, BTreeSet<EchoRunKey>> {
    match active.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[allow(
    clippy::too_many_arguments,
    reason = "the worker boundary receives its fully resolved, owned process composition"
)]
fn execute_echo_worker(
    identity: RuntimeIdentity,
    path: &str,
    run_id: RunId,
    base_url: &str,
    choice: ModelChoice,
    prompt: &PromptFile,
    boundary_delay_ms: u64,
    fault: Option<EchoFaultPlan>,
) -> Result<(), ApiError> {
    let log = open_log(Path::new(path))?;
    let clock = SystemWallClock;
    let ledger = EventCostLedger::new(&log, identity.device, Actor::Agent(run_id), &clock);
    let secrets = MemorySecretStore::new();
    let transport = LoopbackHttpTransport;
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::LocalOnly,
            routing: ModelRouting {
                frontier: choice.clone(),
                fast: choice,
            },
        },
        vec![Box::new(OpenAiCompatibleAdapter {
            base_url: base_url.to_owned(),
            profile: EndpointProfile {
                server: EndpointServer::Ollama,
                supports_stream_usage: true,
            },
        })],
        &secrets,
        &ledger,
        &transport,
        &clock,
    );
    let tools = echo_tool_registry().map_err(registry_error)?;
    let runtimes = native_runtimes()?;
    let roster = RosterRegistry;
    let harness = RunHarness::new(&log, &clock, identity.device, &tools, &runtimes, &roster);
    EchoAgent::new(&gateway, prompt, &log, &clock)
        .with_boundary_delay_ms(boundary_delay_ms)
        .with_fault(fault)
        .run(&harness, run_id)
        .map(|_| ())
        .map_err(|error| ApiError {
            code: error.code(),
            message: error.to_string(),
            retriable: true,
        })
}

fn echo_choice(
    options: &EchoRuntimeOptions,
    device: pos_foundation::DeviceId,
) -> Result<ModelChoice, ApiError> {
    let base_url = options.base_url.trim_end_matches('/');
    if options.model.is_empty()
        || options.model.len() > ECHO_MODEL_NAME_LEN_MAX
        || options.model.chars().any(char::is_control)
    {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "Echo model names contain 1..={ECHO_MODEL_NAME_LEN_MAX} non-control characters"
            ),
            retriable: false,
        });
    }
    let endpoint =
        EndpointConfig::new(base_url, EndpointLocality::DeviceLocal).map_err(|error| ApiError {
            code: "policy_denied",
            message: error.to_string(),
            retriable: false,
        })?;
    Ok(ModelChoice {
        family: ProviderFamily::OpenAiCompatible,
        endpoint,
        model: options.model.clone(),
        credential: CredentialClass::DeviceSession {
            adapter: "ollama".to_owned(),
            device,
        },
        is_pinned_family_base: false,
    })
}

pub(crate) fn start(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    supervisor: &EchoSupervisor,
    input: &RunStartInput,
) -> Result<String, ApiError> {
    if matches!(input.worker, RunWorker::Echo) {
        supervisor.validate(*identity)?;
        if !input.tool_grants.is_empty() {
            return Err(ApiError {
                code: "invalid_input",
                message: "Echo uses its fixed three-tool grant set; omit toolGrants".to_owned(),
                retriable: false,
            });
        }
    }
    let log = open_log(std::path::Path::new(&input.path))?;
    let run_id = mint_run_id(&log)?;
    let parent_run_id = input
        .parent_run_id
        .as_deref()
        .map(parse_run_id)
        .transpose()?;
    let tool_grants = if matches!(input.worker, RunWorker::Echo) {
        echo_tool_grants().map_err(registry_error)?
    } else {
        input_tool_grants(&input.tool_grants)?
    };
    let tools = tools_for_worker(input.worker)?;
    let runtimes = native_runtimes()?;
    let roster = RosterRegistry;
    let harness = RunHarness::new(&log, clock, identity.device, &tools, &runtimes, &roster);
    let state = harness
        .start(
            &RunStartSpec {
                run_id,
                worker: input.worker.charter(),
                runtime_id: RuntimeId::new("projectos.native").map_err(registry_error)?,
                executor: RunExecutor::Device,
                trigger: RunTrigger::User,
                autonomy_level: AutonomyLevel::new(input.autonomy_level).map_err(registry_error)?,
                budget: input.budget.into(),
                tool_grants,
                parent_run_id,
                checkpoint: None,
                validation: None,
                execution_lease: None,
                tainted: false,
            },
            Actor::User(identity.user),
        )
        .map_err(harness_error)?;
    let body = to_json(&report(&input.path, &state))?;
    if matches!(input.worker, RunWorker::Echo) {
        let project_id = state.project_id.ok_or_else(|| ApiError {
            code: "state_corrupt",
            message: "an Echo Run is missing its project id".to_owned(),
            retriable: false,
        })?;
        supervisor.launch(*identity, input.path.clone(), project_id, run_id)?;
    }
    Ok(body)
}

pub(crate) fn pause(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    input: &RunControlInput,
) -> Result<String, ApiError> {
    control(identity, clock, input, Control::Pause)
}

pub(crate) fn cancel(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    input: &RunControlInput,
) -> Result<String, ApiError> {
    control(identity, clock, input, Control::Cancel)
}

pub(crate) fn resume(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    supervisor: &EchoSupervisor,
    input: &RunResumeInput,
) -> Result<String, ApiError> {
    let log = open_log(std::path::Path::new(&input.path))?;
    let run_id = parse_run_id(&input.run_id)?;
    let before = pos_domain::read_run(&log, run_id)
        .map_err(|error| harness_error(HarnessError::Read(error)))?
        .ok_or_else(|| harness_error(HarnessError::RunNotFound { run_id }))?;
    let is_echo = before.worker == RosterCharter::Echo.as_str();
    let tools = if is_echo {
        echo_tool_registry().map_err(registry_error)?
    } else {
        empty_tools()?
    };
    let runtimes = native_runtimes()?;
    let roster = RosterRegistry;
    let harness = RunHarness::new(&log, clock, identity.device, &tools, &runtimes, &roster);
    let state = if before.status == RunStatus::Paused {
        harness
            .resume(run_id, Actor::User(identity.user))
            .map_err(harness_error)?
    } else if before.status.is_terminal() {
        before
    } else {
        // A process crash leaves a Run in a resumable non-terminal state.
        // Relaunching its worker is idempotent; forcing a synthetic pause fact
        // first would falsify what happened.
        before
    };
    let body = to_json(&report(&input.path, &state))?;
    if is_echo && !state.status.is_terminal() {
        let project_id = state.project_id.ok_or_else(|| ApiError {
            code: "state_corrupt",
            message: "an Echo Run is missing its project id".to_owned(),
            retriable: false,
        })?;
        supervisor.validate(*identity)?;
        supervisor.launch(*identity, input.path.clone(), project_id, run_id)?;
    }
    Ok(body)
}

#[derive(Clone, Copy)]
enum Control {
    Pause,
    Cancel,
}

fn control(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    input: &RunControlInput,
    control: Control,
) -> Result<String, ApiError> {
    let log = open_log(std::path::Path::new(&input.path))?;
    let run_id = parse_run_id(&input.run_id)?;
    let tools = empty_tools()?;
    let runtimes = native_runtimes()?;
    let roster = RosterRegistry;
    let harness = RunHarness::new(&log, clock, identity.device, &tools, &runtimes, &roster);
    let actor = Actor::User(identity.user);
    let state = match control {
        Control::Pause => harness.request_pause(run_id, &input.reason, actor),
        Control::Cancel => harness.request_cancel(run_id, &input.reason, actor),
    }
    .map_err(harness_error)?;
    to_json(&report(&input.path, &state))
}

pub(crate) fn stream_subscribe(
    input: &RunStepsInput,
    resume_after: Option<u64>,
) -> Result<Vec<StreamFrame>, ApiError> {
    let log = open_log(Path::new(&input.path))?;
    let run_id = parse_run_id(&input.run_id)?;
    durable_frames(&log, run_id, resume_after)
}

pub(crate) fn stream_follow(
    input: &RunStepsInput,
    resume_after: Option<u64>,
    mut consume: impl FnMut(StreamFrame) -> bool,
) -> Result<(), ApiError> {
    let log = open_log(Path::new(&input.path))?;
    let run_id = parse_run_id(&input.run_id)?;
    let mut cursor = resume_after;
    loop {
        let frames = durable_frames(&log, run_id, cursor)?;
        for frame in frames {
            cursor = Some(frame.stream_seq);
            if !consume(frame) {
                return Ok(());
            }
        }
        let state = read_run_state(&log, run_id)?;
        let at_head = cursor.unwrap_or(0) >= u64::from(settled_frame_count(&state));
        if state.status.is_terminal() && at_head {
            return Ok(());
        }
        std::thread::sleep(Duration::from_millis(RUN_STREAM_POLL_MS));
    }
}

fn durable_frames(
    log: &pos_log::ProjectLog,
    run_id: RunId,
    resume_after: Option<u64>,
) -> Result<Vec<StreamFrame>, ApiError> {
    let state = read_run_state(log, run_id)?;
    let settled_count = settled_frame_count(&state);
    let available = u64::from(settled_count);
    if resume_after.is_some_and(|cursor| cursor > available) {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "Run step cursor {} is ahead of the durable head {available}",
                resume_after.unwrap_or(0)
            ),
            retriable: false,
        });
    }
    let retained = u32::try_from(STREAM_RESUME_WINDOW_LEN).unwrap_or(u32::MAX); // INVARIANT: the fixed window is 256 entries.
    let first_index = settled_count.saturating_sub(retained);
    let mut window = ResumeWindow::new();
    for step_index in first_index..settled_count {
        let step = read_run_step(log, run_id, step_index)
            .map_err(|error| harness_error(HarnessError::Read(error)))?
            .ok_or_else(|| ApiError {
                code: "state_corrupt",
                message: format!(
                    "Run {} is missing checkpointed step {step_index}",
                    run_id.to_hex()
                ),
                retriable: false,
            })?;
        window.push(step_stream_frame(&state, &step)?);
    }
    window.frames_after(resume_after)
}

fn settled_frame_count(state: &RunState) -> u32 {
    if state.status.is_terminal() || state.committed_step_count > state.checkpointed_step_count {
        state.checkpointed_step_count
    } else {
        // Hold the newest boundary until the next intent proves it was not
        // terminal, or a terminal fact proves it was. That keeps a given
        // stream sequence byte-identical across live delivery and replay.
        state.checkpointed_step_count.saturating_sub(1)
    }
}

fn read_run_state(log: &pos_log::ProjectLog, run_id: RunId) -> Result<RunState, ApiError> {
    pos_domain::read_run(log, run_id)
        .map_err(|error| harness_error(HarnessError::Read(error)))?
        .ok_or_else(|| harness_error(HarnessError::RunNotFound { run_id }))
}

fn step_stream_frame(state: &RunState, step: &RunStepState) -> Result<StreamFrame, ApiError> {
    let effect = step.effect.as_ref().ok_or_else(|| ApiError {
        code: "state_corrupt",
        message: format!(
            "Run {} step {} checkpoint exists without an effect receipt",
            state.run_id.to_hex(),
            step.step_index
        ),
        retriable: false,
    })?;
    let checkpoint = step.checkpoint.as_ref().ok_or_else(|| ApiError {
        code: "state_corrupt",
        message: format!(
            "Run {} step {} is counted as checkpointed without a checkpoint row",
            state.run_id.to_hex(),
            step.step_index
        ),
        retriable: false,
    })?;
    let stream_seq = u64::from(step.step_index).saturating_add(1);
    let terminal =
        state.status.is_terminal() && stream_seq == u64::from(state.checkpointed_step_count);
    let frame = RunStepFrame {
        run_id: state.run_id.to_hex(),
        project_id: state.project_id.map(|project_id| project_id.to_hex()),
        stream_seq,
        step_index: step.step_index,
        phase: step.phase.clone(),
        summary: step.summary.clone(),
        tool_id: step.tool_call.as_ref().map(|call| call.tool_id.clone()),
        committed_seq: step.committed_seq.value(),
        checkpoint_seq: checkpoint.saved_seq.value(),
        spent: effect.spent.into(),
        run_status: if terminal {
            status_name(state.status).to_owned()
        } else {
            "running".to_owned()
        },
        terminal,
        validation_status: if terminal {
            state
                .validation
                .map(|validation| validation.status.as_str().to_owned())
        } else {
            None
        },
    };
    Ok(StreamFrame {
        stream_seq,
        event_kind: "run.step",
        data_json: to_json(&frame)?,
    })
}

fn empty_tools() -> Result<ToolRegistry, ApiError> {
    ToolRegistry::new(Vec::new()).map_err(registry_error)
}

fn tools_for_worker(worker: RunWorker) -> Result<ToolRegistry, ApiError> {
    if matches!(worker, RunWorker::Echo) {
        echo_tool_registry().map_err(registry_error)
    } else {
        empty_tools()
    }
}

fn input_tool_grants(inputs: &[RunToolGrantInput]) -> Result<RunToolGrants, ApiError> {
    RunToolGrants::new(
        inputs
            .iter()
            .map(|grant| {
                Ok((
                    ToolId::new(grant.tool_id.clone()).map_err(registry_error)?,
                    match grant.mode {
                        RunToolGrantModeWire::Allow => ToolGrantMode::Allow,
                        RunToolGrantModeWire::Gate => ToolGrantMode::Gate,
                        RunToolGrantModeWire::Block => ToolGrantMode::Block,
                    },
                ))
            })
            .collect::<Result<Vec<_>, ApiError>>()?,
    )
    .map_err(registry_error)
}

fn native_runtimes() -> Result<RuntimeRegistry, ApiError> {
    RuntimeRegistry::native_only().map_err(registry_error)
}

fn mint_run_id(log: &pos_log::ProjectLog) -> Result<RunId, ApiError> {
    let bytes: Vec<u8> = log
        .store()
        .db()
        .with_reader("mint Run id", |connection| {
            connection.query_row("SELECT randomblob(16)", [], |row| row.get(0))
        })
        .map_err(|error| store_error(&error))?;
    let bytes: [u8; 16] = bytes.try_into().map_err(|bytes: Vec<u8>| ApiError {
        code: "storage_failure",
        message: format!(
            "SQLite randomblob(16) returned {} bytes while minting a Run id",
            bytes.len()
        ),
        retriable: false,
    })?;
    Ok(RunId::from_bytes(bytes))
}

fn parse_run_id(value: &str) -> Result<RunId, ApiError> {
    RunId::from_hex(value).ok_or_else(|| ApiError {
        code: "invalid_input",
        message: "runId must be exactly 32 lowercase hexadecimal characters".to_owned(),
        retriable: false,
    })
}

fn report(path: &str, state: &RunState) -> RunReport {
    RunReport {
        path: path.to_owned(),
        run_id: state.run_id.to_hex(),
        project_id: state.project_id.map(|project_id| project_id.to_hex()),
        worker: state.worker.clone(),
        runtime_id: state.runtime_id.clone(),
        executor: state.executor.clone(),
        status: status_name(state.status).to_owned(),
        autonomy_level: state.autonomy_level,
        committed_step_count: state.committed_step_count,
        checkpointed_step_count: state.checkpointed_step_count,
        budget: state.budget.into(),
        spent: state.spent.into(),
        tainted: state.tainted,
        tool_grants: state
            .tool_grants
            .iter()
            .map(|grant| RunToolGrantInput {
                tool_id: grant.tool_id.clone(),
                mode: match grant.mode {
                    RunToolGrantMode::Allow => RunToolGrantModeWire::Allow,
                    RunToolGrantMode::Gate => RunToolGrantModeWire::Gate,
                    RunToolGrantMode::Block => RunToolGrantModeWire::Block,
                },
            })
            .collect(),
        parent_run_id: state.parent_run_id.map(|run_id| run_id.to_hex()),
        lineage_depth: state.lineage_depth,
        pending_control: state
            .pending_control
            .map(|control| format!("{control:?}").to_ascii_lowercase()),
        pause: state.pause.as_ref().map(pause_report),
    }
}

fn pause_report(pause: &RunPauseState) -> RunPauseReport {
    match pause {
        RunPauseState::Budget {
            dimension,
            limit,
            spent,
            pending,
            requested,
        } => RunPauseReport::Budget {
            dimension: (*dimension).into(),
            limit: *limit,
            spent: *spent,
            pending: *pending,
            requested: *requested,
        },
        RunPauseState::Requested { reason } => RunPauseReport::Requested {
            reason: reason.clone(),
        },
        RunPauseState::ToolWeather { code } => RunPauseReport::ToolWeather { code: code.clone() },
        RunPauseState::ReservationExceeded { dimension } => RunPauseReport::ReservationExceeded {
            dimension: (*dimension).into(),
        },
    }
}

const fn status_name(status: RunStatus) -> &'static str {
    match status {
        RunStatus::Requested => "requested",
        RunStatus::Preflight => "preflight",
        RunStatus::Running => "running",
        RunStatus::WaitingApproval => "waitingApproval",
        RunStatus::WaitingInput => "waitingInput",
        RunStatus::Validating => "validating",
        RunStatus::Paused => "paused",
        RunStatus::Done => "done",
        RunStatus::Failed => "failed",
        RunStatus::Canceled => "canceled",
    }
}

fn registry_error(error: impl std::fmt::Display) -> ApiError {
    ApiError {
        code: "invalid_input",
        message: error.to_string(),
        retriable: false,
    }
}

fn harness_error(error: HarnessError) -> ApiError {
    let message = error.to_string();
    match error {
        HarnessError::Log(error) => log_error(error),
        HarnessError::Read(_) => ApiError {
            code: "state_corrupt",
            message,
            retriable: false,
        },
        HarnessError::BudgetConfig(_)
        | HarnessError::Runtime(_)
        | HarnessError::ToolRegistry(_)
        | HarnessError::InvalidStepPlan { .. }
        | HarnessError::InvalidControlReason
        | HarnessError::InvalidQuestion
        | HarnessError::InvalidArtifact
        | HarnessError::InvalidValidation => ApiError {
            code: "invalid_input",
            message,
            retriable: false,
        },
        HarnessError::Authorization(_) => ApiError {
            code: "policy_denied",
            message,
            retriable: false,
        },
        HarnessError::RunNotFound { .. } => ApiError {
            code: "run_not_found",
            message,
            retriable: false,
        },
        HarnessError::ConditionalAppendContention { .. } => ApiError {
            code: "state_changed",
            message,
            retriable: true,
        },
        HarnessError::InvalidRunState { .. }
        | HarnessError::StepPlanChanged { .. }
        | HarnessError::EffectReportChanged { .. } => ApiError {
            code: "run_conflict",
            message,
            retriable: false,
        },
    }
}
