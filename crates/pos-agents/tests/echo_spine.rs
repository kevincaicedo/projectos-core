//! m0-s13 deterministic Echo oracle over the production harness and gateway.

#![forbid(unsafe_code)]

use pos_agents::{
    AutonomyLevel, EchoAgent, RosterCharter, RosterRegistry, RunHarness, RunStartSpec, RuntimeId,
    RuntimeRegistry, echo_marker, echo_tool_grants, echo_tool_registry,
};
use pos_domain::{
    RunBudget, RunExecutor, RunStatus, RunTrigger, RunValidationStatus, read_run_step, v0_registry,
};
use pos_foundation::{DeviceId, ManualWallClock, RunId, UserId};
use pos_gateway::{
    CredentialClass, EndpointConfig, EndpointLocality, EndpointProfile, Gateway, GatewayConfig,
    HttpHead, HttpRequestPlan, HttpTransport, MemoryLedger, MemorySecretStore, ModelChoice,
    ModelPolicy, ModelRouting, OpenAiCompatibleAdapter, PromptFile, ProviderFamily,
    ResponseHandler, TransportError, Transports,
};
use pos_log::{Actor, LogConfig, ProjectLog};
use pos_store::{BlobHash, ProjectStore};
use std::path::Path;
use std::sync::Mutex;

const MODEL: &str = "echo-fixture";
const DEVICE: DeviceId = DeviceId::from_bytes([0x31; 16]);
const USER: UserId = UserId::from_bytes([0x41; 16]);

struct EchoTransport {
    expected_marker: String,
    attempts: Mutex<u32>,
}

impl EchoTransport {
    fn new(expected_marker: String) -> Self {
        Self {
            expected_marker,
            attempts: Mutex::new(0),
        }
    }

    fn attempt_count(&self) -> u32 {
        *self.attempts.lock().expect("test mutex")
    }
}

impl HttpTransport for EchoTransport {
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        *self.attempts.lock().expect("test mutex") += 1;
        let request = String::from_utf8_lossy(&plan.body);
        assert!(request.contains(&self.expected_marker));
        assert!(plan.url.starts_with("http://127.0.0.1:"));

        if handler
            .on_head(&HttpHead {
                status: 200,
                headers: Vec::new(),
            })
            .is_err()
        {
            return Err(TransportError::Aborted);
        }
        let body = format!(
            "data: {{\"choices\":[{{\"delta\":{{\"content\":\"ECHO: {}\"}}}}]}}\n\n\
             data: {{\"choices\":[],\"usage\":{{\"prompt_tokens\":7,\"completion_tokens\":3}}}}\n\n\
             data: [DONE]\n\n",
            self.expected_marker
        );
        for chunk in body.as_bytes().chunks(7) {
            if handler.on_chunk(chunk).is_err() {
                return Err(TransportError::Aborted);
            }
        }
        Ok(())
    }
}

#[test]
fn echo_runs_three_checkpointed_steps_with_one_attributed_model_call() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("echo.pos");
    let clock = ManualWallClock::starting_at(1_755_000_000_000);
    let log = open_log(&root, &clock);
    let run_id = RunId::from_bytes([0xe3; 16]);
    let marker = echo_marker(run_id);
    let transport = EchoTransport::new(marker.clone());
    let ledger = MemoryLedger::new();
    let secrets = MemorySecretStore::new();
    let choice = local_choice();
    let gateway = Gateway::new(
        GatewayConfig {
            policy: ModelPolicy::LocalOnly,
            routing: ModelRouting::thinking_only(choice.clone(), choice),
        },
        vec![Box::new(OpenAiCompatibleAdapter {
            base_url: "http://127.0.0.1:11434".to_owned(),
            profile: EndpointProfile {
                server: pos_gateway::EndpointServer::Ollama,
                supports_stream_usage: true,
            },
        })],
        &secrets,
        &ledger,
        Transports::device_local_only(&transport),
        &clock,
    );
    let prompt =
        PromptFile::from_embedded("echo@1.md", include_bytes!("../../../prompts/echo@1.md"))
            .expect("embedded Echo prompt");
    let tools = echo_tool_registry().expect("Echo tool registry");
    let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
    let roster = RosterRegistry;
    let harness = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster);
    harness
        .start(
            &RunStartSpec {
                run_id,
                worker: RosterCharter::Echo,
                runtime_id: RuntimeId::new("projectos.native").expect("native runtime id"),
                executor: RunExecutor::Device,
                trigger: RunTrigger::User,
                autonomy_level: AutonomyLevel::new(2).expect("autonomy level"),
                budget: echo_budget(),
                tool_grants: echo_tool_grants().expect("Echo grants"),
                parent_run_id: None,
                checkpoint: None,
                validation: None,
                execution_lease: None,
                tainted: false,
            },
            Actor::User(USER),
        )
        .expect("start Echo Run");

    let terminal = EchoAgent::new(&gateway, &prompt, &log, &clock)
        .run(&harness, run_id)
        .expect("Echo Run completes");

    assert_eq!(terminal.status, RunStatus::Done);
    assert_eq!(terminal.committed_step_count, 3);
    assert_eq!(terminal.checkpointed_step_count, 3);
    assert_eq!(
        terminal.validation.map(|validation| validation.status),
        Some(RunValidationStatus::Passed)
    );
    assert_eq!(transport.attempt_count(), 1);
    let records = ledger.records();
    assert_eq!(records.len(), 1);
    assert_eq!(records[0].project, log.store().manifest().project_id);
    assert_eq!(records[0].feature, "echo");
    assert_eq!(records[0].agent.as_deref(), Some("echo"));
    assert_eq!(records[0].outcome, "ok");

    for step_index in 0..3 {
        let step = read_run_step(&log, run_id, step_index)
            .expect("read Echo step")
            .expect("Echo step exists");
        assert!(step.effect.is_some());
        assert!(step.checkpoint.is_some());
    }
    let expected_output = format!("ECHO: {marker}");
    log.store()
        .blobs()
        .verify_blob(BlobHash::of_bytes(expected_output.as_bytes()))
        .expect("Echo output is present and hash-valid in CAS");

    let mut finished = 0_u32;
    log.for_each_event(|event| {
        if event.kind.as_str() == "RunFinished" {
            finished = finished.saturating_add(1);
        }
        Ok(())
    })
    .expect("scan Run facts");
    assert_eq!(finished, 1);
}

fn local_choice() -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::OpenAiCompatible,
        endpoint: EndpointConfig::new("http://127.0.0.1:11434", EndpointLocality::DeviceLocal)
            .expect("loopback endpoint"),
        model: MODEL.to_owned(),
        credential: CredentialClass::DeviceSession {
            adapter: "ollama".to_owned(),
            device: DEVICE,
        },
        is_pinned_family_base: false,
    }
}

fn open_log(root: &Path, clock: &ManualWallClock) -> ProjectLog {
    let store = ProjectStore::create(root, "generic", clock).expect("create project store");
    ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open project log")
}

const fn echo_budget() -> RunBudget {
    RunBudget {
        tokens: 4_096,
        usd_micros: 0,
        wall_ms: 90_000,
        storage_bytes: 64 * 1_024,
        tool_calls: 3,
        retries: 0,
        steps: 3,
    }
}
