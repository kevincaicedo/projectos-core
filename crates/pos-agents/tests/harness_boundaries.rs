//! m0-s12 negative and budget oracles.

#![forbid(unsafe_code)]

mod common;

use common::{
    DEVICE, HarnessFixture, IdempotentCounter, USER, effect_report, grants, open_log, prepare,
    record, standard_budget, start_run, start_spec, step_plan, tool_registry,
};
use pos_agents::{
    GateReceipt, RosterRegistry, RunHarness, RuntimeRegistry, StepPreparation, ToolEffectClass,
    ToolId,
};
use pos_domain::{RunBudgetDimension, RunPauseState, RunStatus, read_run, read_run_step};
use pos_foundation::{GateReceiptId, ManualWallClock, RunId, WallClock};
use pos_log::Actor;

#[test]
fn hard_budget_pauses_before_the_next_effect() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("budget.pos");
    let counter = IdempotentCounter::new(directory.path().join("effect-keys.txt"));
    let clock = ManualWallClock::starting_at(20_000);
    let tools = tool_registry(ToolEffectClass::Idempotent);
    let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
    let roster = RosterRegistry;
    let grants = grants();
    let fixture = HarnessFixture::new(&clock, &tools, &runtimes, &roster, &grants);
    let run_id = RunId::from_bytes([0x71; 16]);
    let log = open_log(&root, &clock);
    let mut budget = standard_budget(3);
    budget.tool_calls = 2;
    start_run(&log, &fixture, run_id, budget);

    for step_index in 0..2 {
        let plan = step_plan(step_index);
        let call = match prepare(&log, &fixture, run_id, &plan) {
            StepPreparation::Effect(call) => call,
            other => panic!("budgeted step should run, got {other:?}"),
        };
        counter.apply(&call);
        record(&log, &fixture, &call, &effect_report(step_index));
    }

    let paused = prepare(&log, &fixture, run_id, &step_plan(2));
    let StepPreparation::Paused(exceeded) = paused else {
        panic!("third tool call must pause before effect: {paused:?}");
    };
    assert_eq!(exceeded.dimension, RunBudgetDimension::ToolCalls);
    assert_eq!(exceeded.limit, 2);
    assert_eq!(exceeded.spent, 2);
    assert_eq!(exceeded.requested, 1);
    assert_eq!(counter.count(), 2, "no third external effect ran");
    assert!(
        read_run_step(&log, run_id, 2)
            .expect("read absent step")
            .is_none(),
        "budget pause must not commit a partial step"
    );
    let state = read_run(&log, run_id)
        .expect("read Run")
        .expect("Run exists");
    assert_eq!(state.status, RunStatus::Paused);
    assert_eq!(state.committed_step_count, 2);
    assert_eq!(state.checkpointed_step_count, 2);
    assert_eq!(state.spent.tool_calls, 2);
    assert_eq!(
        state.pause,
        Some(RunPauseState::Budget {
            dimension: RunBudgetDimension::ToolCalls,
            limit: 2,
            spent: 2,
            pending: 0,
            requested: 1,
        })
    );
}

#[test]
fn non_idempotent_restart_requires_human_reconciliation() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("reconcile.pos");
    let clock = ManualWallClock::starting_at(30_000);
    let tools = tool_registry(ToolEffectClass::NonIdempotent);
    let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
    let roster = RosterRegistry;
    let grants = grants();
    let fixture = HarnessFixture::new(&clock, &tools, &runtimes, &roster, &grants);
    let run_id = RunId::from_bytes([0x72; 16]);
    let plan = step_plan(0);
    let log = open_log(&root, &clock);
    start_run(&log, &fixture, run_id, standard_budget(1));
    let receipt = GateReceipt::new(
        GateReceiptId::from_bytes([0x73; 16]),
        run_id,
        plan.tool_call.call_id,
        ToolId::new("counter.increment").expect("fixed tool id"),
        USER,
        "Approve this exact non-idempotent fixture call".to_owned(),
        clock.now_ms() + 1_000,
    )
    .expect("valid receipt");
    let first = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster)
        .prepare_step(run_id, &plan, Some(&receipt))
        .expect("gate authorizes initial intent");
    assert!(matches!(first, StepPreparation::Effect(_)));
    drop(log);

    let reopened = open_log(&root, &clock);
    let resumed = RunHarness::new(&reopened, &clock, DEVICE, &tools, &runtimes, &roster)
        .prepare_step(run_id, &plan, None)
        .expect("durable intent can be inspected after restart");
    let StepPreparation::ReconciliationRequired {
        run_id: parked_run,
        step_index,
        call_id,
    } = resumed
    else {
        panic!("non-idempotent effect must not auto-retry: {resumed:?}");
    };
    assert_eq!(parked_run, run_id);
    assert_eq!(step_index, 0);
    assert_eq!(call_id, plan.tool_call.call_id);
    assert_eq!(
        read_run(&reopened, run_id)
            .expect("read Run")
            .expect("Run exists")
            .committed_step_count,
        1
    );
    assert!(
        read_run_step(&reopened, run_id, 0)
            .expect("read step")
            .expect("step exists")
            .effect
            .is_none()
    );
}

#[test]
fn pause_and_cancel_settle_only_at_checkpoint_boundaries() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("controls.pos");
    let clock = ManualWallClock::starting_at(40_000);
    let tools = tool_registry(ToolEffectClass::Idempotent);
    let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
    let roster = RosterRegistry;
    let grants = grants();
    let fixture = HarnessFixture::new(&clock, &tools, &runtimes, &roster, &grants);
    let run_id = RunId::from_bytes([0x74; 16]);
    let log = open_log(&root, &clock);
    start_run(&log, &fixture, run_id, standard_budget(2));
    let call = match prepare(&log, &fixture, run_id, &step_plan(0)) {
        StepPreparation::Effect(call) => call,
        other => panic!("first step should run, got {other:?}"),
    };
    let pending = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster)
        .request_pause(run_id, "Operator requested pause", Actor::User(USER))
        .expect("request pause");
    assert_eq!(pending.status, RunStatus::Running);
    record(&log, &fixture, &call, &effect_report(0));
    let paused = read_run(&log, run_id)
        .expect("read Run")
        .expect("Run exists");
    assert_eq!(paused.status, RunStatus::Paused);
    assert_eq!(paused.committed_step_count, paused.checkpointed_step_count);

    let resumed = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster)
        .resume(run_id, Actor::User(USER))
        .expect("resume at checkpoint");
    assert_eq!(resumed.status, RunStatus::Running);
    let canceled = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster)
        .request_cancel(run_id, "Operator requested cancel", Actor::User(USER))
        .expect("cancel at boundary");
    assert_eq!(canceled.status, RunStatus::Canceled);
    assert!(canceled.status.is_terminal());
}

#[test]
fn grants_survive_restart_and_lineage_stops_at_the_named_cap() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("lineage.pos");
    let clock = ManualWallClock::starting_at(45_000);
    let tools = tool_registry(ToolEffectClass::Idempotent);
    let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
    let roster = RosterRegistry;
    let grants = grants();
    let log = open_log(&root, &clock);
    let mut parent = None;
    {
        let harness = RunHarness::new(&log, &clock, DEVICE, &tools, &runtimes, &roster);
        for depth in 0..=pos_agents::RUN_LINEAGE_DEPTH_MAX {
            let run_id = RunId::from_bytes([depth.saturating_add(1); 16]);
            let mut spec = start_spec(run_id, standard_budget(1), grants.clone());
            spec.parent_run_id = parent;
            let state = harness
                .start(&spec, Actor::User(USER))
                .expect("lineage at or below the cap starts");
            assert_eq!(state.lineage_depth, depth);
            assert_eq!(state.parent_run_id, parent);
            assert_eq!(state.tool_grants.len(), 1);
            assert_eq!(state.tool_grants[0].tool_id, "counter.increment");
            parent = Some(run_id);
        }

        let overflow_id = RunId::from_bytes([0xfe; 16]);
        let mut overflow = start_spec(overflow_id, standard_budget(1), grants);
        overflow.parent_run_id = parent;
        let error = harness
            .start(&overflow, Actor::User(USER))
            .expect_err("lineage beyond the cap must fail");
        assert!(error.to_string().contains("lineage depth"));
        assert!(
            read_run(&log, overflow_id)
                .expect("read rejected Run")
                .is_none()
        );
    }
    drop(log);
    let reopened = open_log(&root, &clock);
    let leaf = read_run(&reopened, parent.expect("leaf id"))
        .expect("read leaf")
        .expect("leaf exists");
    assert_eq!(leaf.lineage_depth, pos_agents::RUN_LINEAGE_DEPTH_MAX);
    assert_eq!(leaf.tool_grants.len(), 1);
}
