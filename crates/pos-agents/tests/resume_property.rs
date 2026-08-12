//! m0-s12 L7 oracle: arbitrary process boundaries around a durable intent
//! and an idempotent effect preserve exactly one effect and one checkpoint.

#![forbid(unsafe_code)]

mod common;

use common::{
    HarnessFixture, IdempotentCounter, effect_report, grants, open_log, prepare, record,
    standard_budget, start_run, step_plan, tool_registry,
};
use pos_agents::{RosterRegistry, RuntimeRegistry, StepPreparation, ToolEffectClass};
use pos_domain::{RunStatus, read_run, read_run_step};
use pos_foundation::{ManualWallClock, RunId};
use proptest::prelude::*;

proptest! {
    #![proptest_config(ProptestConfig::with_cases(32))]

    /// Each mask exercises three restart points: after intent commit, after
    /// the external effect but before its receipt, and after the atomic
    /// receipt + checkpoint batch. Re-executing at the middle point uses the
    /// same key, so the fixture proves zero loss and zero duplication.
    #[test]
    fn idempotent_steps_survive_arbitrary_restart_points(
        step_count in 1_u32..9,
        crash_masks in proptest::collection::vec(any::<u8>(), 1..9),
    ) {
        let directory = tempfile::tempdir().expect("tempdir");
        let root = directory.path().join("resume.pos");
        let counter = IdempotentCounter::new(directory.path().join("effect-keys.txt"));
        let clock = ManualWallClock::starting_at(10_000);
        let tools = tool_registry(ToolEffectClass::Idempotent);
        let runtimes = RuntimeRegistry::native_only().expect("native runtime registry");
        let roster = RosterRegistry;
        let grants = grants();
        let fixture = HarnessFixture::new(&clock, &tools, &runtimes, &roster, &grants);
        let run_id = RunId::from_bytes([0x61; 16]);
        let mut log = open_log(&root, &clock);
        start_run(&log, &fixture, run_id, standard_budget(step_count));

        for step_index in 0..step_count {
            let mask = crash_masks[usize::try_from(step_index).unwrap_or(0) % crash_masks.len()];
            let plan = step_plan(step_index);
            let mut call = match prepare(&log, &fixture, run_id, &plan) {
                StepPreparation::Effect(call) => call,
                other => panic!("new step should authorize its effect, got {other:?}"),
            };

            if mask & 0b001 != 0 {
                let key = call.idempotency_key().to_owned();
                drop(log);
                log = open_log(&root, &clock);
                call = match prepare(&log, &fixture, run_id, &plan) {
                    StepPreparation::Effect(resumed) => resumed,
                    other => panic!("intent restart should resume, got {other:?}"),
                };
                prop_assert_eq!(call.idempotency_key(), key);
            }

            counter.apply(&call);
            if mask & 0b010 != 0 {
                let key = call.idempotency_key().to_owned();
                drop(log);
                log = open_log(&root, &clock);
                call = match prepare(&log, &fixture, run_id, &plan) {
                    StepPreparation::Effect(resumed) => resumed,
                    other => panic!("effect-before-receipt restart should resume, got {other:?}"),
                };
                prop_assert_eq!(call.idempotency_key(), key);
                counter.apply(&call);
            }

            record(&log, &fixture, &call, &effect_report(step_index));
            if mask & 0b100 != 0 {
                drop(log);
                log = open_log(&root, &clock);
            }
        }

        let state = read_run(&log, run_id)
            .expect("read Run")
            .expect("Run exists");
        prop_assert_eq!(state.status, RunStatus::Running);
        prop_assert_eq!(state.committed_step_count, step_count);
        prop_assert_eq!(state.checkpointed_step_count, step_count);
        prop_assert_eq!(state.spent.steps, step_count);
        prop_assert_eq!(state.spent.tool_calls, step_count);
        prop_assert_eq!(counter.count(), usize::try_from(step_count).unwrap_or(usize::MAX));
        for step_index in 0..step_count {
            let step = read_run_step(&log, run_id, step_index)
                .expect("read step")
                .expect("step exists");
            prop_assert!(step.effect.is_some(), "step {step_index} lost its effect receipt");
            prop_assert!(step.checkpoint.is_some(), "step {step_index} lost its checkpoint");
        }

        let incremental = log.dump_projections().expect("dump incremental projections");
        log.rebuild_projections().expect("rebuild projections from events");
        let rebuilt = log.dump_projections().expect("dump rebuilt projections");
        prop_assert_eq!(incremental, rebuilt);
        prop_assert!(log.verify_projections().expect("verify projections").is_clean());
    }
}
