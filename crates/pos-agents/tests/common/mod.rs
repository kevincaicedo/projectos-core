#![forbid(unsafe_code)]

use pos_agents::{
    AuthorizedToolCall, AutonomyLevel, CapabilityScope, RosterCharter, RunHarness, RunStartSpec,
    RunToolGrants, RuntimeId, RuntimeRegistry, StepPlan, StepPreparation, ToolCallRequest,
    ToolDescriptor, ToolEffectClass, ToolEffectReport, ToolGrantMode, ToolId, ToolPolicyMode,
    ToolRegistry,
};
use pos_domain::{RunBudget, RunExecutor, RunStepPhase, RunTrigger, RunUsage, v0_registry};
use pos_foundation::{DeviceId, ManualWallClock, RunId, ToolCallId, UserId};
use pos_log::{Actor, LogConfig, ProjectLog};
use pos_store::ProjectStore;
use std::fs;
use std::path::{Path, PathBuf};

pub const DEVICE: DeviceId = DeviceId::from_bytes([0x31; 16]);
pub const USER: UserId = UserId::from_bytes([0x41; 16]);

pub fn open_log(root: &Path, clock: &ManualWallClock) -> ProjectLog {
    let store = if root.join("manifest.json").is_file() {
        ProjectStore::open(root).expect("reopen project store")
    } else {
        ProjectStore::create(root, "generic", clock).expect("create project store")
    };
    ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open project log")
}

pub fn tool_registry(effect: ToolEffectClass) -> ToolRegistry {
    ToolRegistry::new(vec![
        ToolDescriptor::new(
            ToolId::new("counter.increment").expect("fixed tool id"),
            1,
            CapabilityScope::WritePlan,
            ToolPolicyMode::Allow,
            effect,
            64,
        )
        .expect("counter descriptor"),
    ])
    .expect("tool registry")
}

pub fn grants() -> RunToolGrants {
    RunToolGrants::new(vec![(
        ToolId::new("counter.increment").expect("fixed tool id"),
        ToolGrantMode::Allow,
    )])
    .expect("Run grants")
}

pub fn start_spec(run_id: RunId, budget: RunBudget, tool_grants: RunToolGrants) -> RunStartSpec {
    RunStartSpec {
        run_id,
        worker: RosterCharter::Navigator,
        runtime_id: RuntimeId::new("projectos.native").expect("native runtime id"),
        executor: RunExecutor::Device,
        trigger: RunTrigger::User,
        autonomy_level: AutonomyLevel::new(2).expect("autonomy level"),
        budget,
        tool_grants,
        parent_run_id: None,
        checkpoint: None,
        validation: None,
        execution_lease: None,
        tainted: false,
    }
}

pub fn standard_budget(step_count: u32) -> RunBudget {
    RunBudget {
        tokens: u64::from(step_count) * 10,
        usd_micros: u64::from(step_count) * 10,
        wall_ms: u64::from(step_count) * 100,
        storage_bytes: u64::from(step_count) * 100,
        tool_calls: step_count,
        retries: step_count,
        steps: step_count,
    }
}

pub fn step_plan(step_index: u32) -> StepPlan {
    StepPlan {
        step_index,
        phase: RunStepPhase::Tool,
        summary: format!("Increment durable counter at step {step_index}"),
        digest: [u8::try_from(step_index % 251).unwrap_or(0); 32],
        tool_call: ToolCallRequest {
            tool_id: ToolId::new("counter.increment").expect("fixed tool id"),
            call_id: ToolCallId::from_bytes(call_bytes(step_index)),
            input: step_index.to_be_bytes().to_vec(),
        },
        reserved: RunUsage {
            tokens: 1,
            usd_micros: 1,
            wall_ms: 1,
            storage_bytes: 0,
            tool_calls: 1,
            retries: 0,
            steps: 1,
        },
    }
}

pub fn effect_report(step_index: u32) -> ToolEffectReport {
    ToolEffectReport {
        output_digest: [u8::try_from(step_index % 251).unwrap_or(0); 32],
        checkpoint_digest: [u8::try_from((step_index + 1) % 251).unwrap_or(0); 32],
        spent: RunUsage {
            tokens: 1,
            usd_micros: 1,
            wall_ms: 1,
            storage_bytes: 0,
            tool_calls: 1,
            retries: 0,
            steps: 1,
        },
        artifact: None,
        validation: None,
    }
}

pub struct HarnessFixture<'a> {
    pub clock: &'a ManualWallClock,
    pub tools: &'a ToolRegistry,
    pub runtimes: &'a RuntimeRegistry,
    pub roster: &'a pos_agents::RosterRegistry,
    pub grants: &'a RunToolGrants,
}

impl<'a> HarnessFixture<'a> {
    pub const fn new(
        clock: &'a ManualWallClock,
        tools: &'a ToolRegistry,
        runtimes: &'a RuntimeRegistry,
        roster: &'a pos_agents::RosterRegistry,
        grants: &'a RunToolGrants,
    ) -> Self {
        Self {
            clock,
            tools,
            runtimes,
            roster,
            grants,
        }
    }
}

pub fn start_run(log: &ProjectLog, fixture: &HarnessFixture<'_>, run_id: RunId, budget: RunBudget) {
    RunHarness::new(
        log,
        fixture.clock,
        DEVICE,
        fixture.tools,
        fixture.runtimes,
        fixture.roster,
    )
    .start(
        &start_spec(run_id, budget, fixture.grants.clone()),
        Actor::User(USER),
    )
    .expect("start Run");
}

pub fn prepare(
    log: &ProjectLog,
    fixture: &HarnessFixture<'_>,
    run_id: RunId,
    plan: &StepPlan,
) -> StepPreparation {
    RunHarness::new(
        log,
        fixture.clock,
        DEVICE,
        fixture.tools,
        fixture.runtimes,
        fixture.roster,
    )
    .prepare_step(run_id, plan, None)
    .expect("prepare step")
}

pub fn record(
    log: &ProjectLog,
    fixture: &HarnessFixture<'_>,
    call: &AuthorizedToolCall,
    report: &ToolEffectReport,
) {
    RunHarness::new(
        log,
        fixture.clock,
        DEVICE,
        fixture.tools,
        fixture.runtimes,
        fixture.roster,
    )
    .record_effect(call, report)
    .expect("record effect and checkpoint");
}

pub struct IdempotentCounter {
    path: PathBuf,
}

impl IdempotentCounter {
    pub fn new(path: PathBuf) -> Self {
        Self { path }
    }

    pub fn apply(&self, call: &AuthorizedToolCall) {
        let mut keys = if self.path.is_file() {
            fs::read_to_string(&self.path).expect("read counter fixture")
        } else {
            String::new()
        };
        if keys.lines().any(|line| line == call.idempotency_key()) {
            return;
        }
        keys.push_str(call.idempotency_key());
        keys.push('\n');
        fs::write(&self.path, keys).expect("write counter fixture");
    }

    pub fn count(&self) -> usize {
        if self.path.is_file() {
            fs::read_to_string(&self.path)
                .expect("read counter fixture")
                .lines()
                .count()
        } else {
            0
        }
    }
}

fn call_bytes(step_index: u32) -> [u8; 16] {
    let mut bytes = [0x51; 16];
    bytes[12..].copy_from_slice(&step_index.to_be_bytes());
    bytes
}
