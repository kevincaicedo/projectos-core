//! How this process composes the ingestion pipeline (m1-s03).
//!
//! One module, because the answer must be the same everywhere. The worker pool
//! runs stages, the reprocess planner enqueues them, and the evidence browser
//! reports which stages this build can run — if those three disagreed about
//! whether TRANSCRIBE exists, `nextStageAvailable` would be a lie on one
//! surface and true on another (L12: parity is a property, not a habit).
//!
//! ## Configuration, honestly
//!
//! Transcription needs two things that are not in the log: where model
//! artifacts live, and which one to use. Both are read from the environment
//! with defaults that match `pos models pull`'s, so the command a user runs
//! and the stage that consumes its output agree by construction. A real
//! per-project settings surface is later work (m1-s06 brings the encrypted
//! store the cloud route also needs); an environment variable with a stated
//! default is the honest interim, not a hidden one.

use crate::gateway_ops::EventCostLedger;
use pos_foundation::{DeviceId, WallClock};
use pos_gateway::CostLedger;
use pos_ingest::{StageLedgers, StageRegistry, TranscribeSetup, stage_registry_default};
use pos_log::{Actor, ProjectLog};
use std::path::PathBuf;
use std::sync::Arc;

/// Overrides where verified model artifacts are looked up.
pub const MODELS_DIR_ENV: &str = "POS_MODELS_DIR";

/// Default artifact directory. Identical to `pos models pull --dest-dir`'s
/// default on purpose: a user who pulls a model and then ingests audio must
/// not have to know that two defaults exist.
pub const MODELS_DIR_DEFAULT: &str = "models/pulled";

/// Overrides which whisper artifact TRANSCRIBE loads.
pub const WHISPER_MODEL_ENV: &str = "POS_WHISPER_MODEL";

/// The §18 transcription gate is stated for whisper-small, so that is what the
/// product uses by default. A smaller artifact is a development convenience
/// and would quietly make the gate unmeasurable.
pub const WHISPER_MODEL_DEFAULT: &str = "whisper-small";

/// Optional BCP-47 language hint. Absent means the model detects it.
pub const TRANSCRIBE_LANGUAGE_ENV: &str = "POS_TRANSCRIBE_LANGUAGE";

#[must_use]
pub fn models_dir() -> PathBuf {
    std::env::var_os(MODELS_DIR_ENV)
        .map_or_else(|| PathBuf::from(MODELS_DIR_DEFAULT), PathBuf::from)
}

/// The transcription setup every surface in this process shares.
#[must_use]
pub fn transcribe_setup() -> TranscribeSetup {
    let model = std::env::var(WHISPER_MODEL_ENV)
        .ok()
        .filter(|name| !name.trim().is_empty())
        .unwrap_or_else(|| WHISPER_MODEL_DEFAULT.to_owned());
    let mut setup = TranscribeSetup::local(models_dir(), model);
    setup.language = std::env::var(TRANSCRIBE_LANGUAGE_ENV)
        .ok()
        .filter(|language| !language.trim().is_empty());
    setup
}

/// The stages this build can run. One answer, shared by every surface.
#[must_use]
pub fn stage_registry() -> StageRegistry {
    stage_registry_default(&transcribe_setup())
}

/// Opens an [`EventCostLedger`] per stage attempt.
///
/// The seam exists because `pos-ingest` may not depend on `pos-api`: the
/// pipeline needs a cost ledger bound to the attempt's own log, and the only
/// implementation that writes `ModelCallCompleted` into that log lives here.
pub struct EventStageLedgers {
    device: DeviceId,
}

impl EventStageLedgers {
    #[must_use]
    pub const fn new(device: DeviceId) -> Self {
        Self { device }
    }
}

impl StageLedgers for EventStageLedgers {
    fn open<'a>(
        &self,
        log: &'a ProjectLog,
        clock: &'a dyn WallClock,
        actor: Actor,
    ) -> Box<dyn CostLedger + 'a> {
        Box::new(EventCostLedger::new(log, self.device, actor, clock))
    }
}

/// The ledger seam, ready for a [`pos_ingest::PipelineConfig`].
#[must_use]
pub fn stage_ledgers(device: DeviceId) -> Arc<dyn StageLedgers> {
    Arc::new(EventStageLedgers::new(device))
}

#[cfg(test)]
mod tests {
    use super::{MODELS_DIR_DEFAULT, WHISPER_MODEL_DEFAULT, stage_registry};
    use pos_domain::IngestStage;

    #[test]
    fn the_default_artifact_directory_matches_the_cli_that_fills_it() {
        // `bins/pos`'s `models pull --dest-dir` default. Two defaults that
        // drift apart would make "I pulled the model and it still says it is
        // missing" a support ticket rather than a build failure.
        assert_eq!(MODELS_DIR_DEFAULT, "models/pulled");
        assert_eq!(WHISPER_MODEL_DEFAULT, "whisper-small");
    }

    #[test]
    fn every_surface_sees_the_same_stages_and_transcribe_is_among_them() {
        let stages = stage_registry().stages();
        assert!(
            stages.contains(&IngestStage::Transcribe),
            "TRANSCRIBE must be registered wherever this build reports its stages"
        );
        assert!(stages.contains(&IngestStage::Normalize));
        assert!(stages.contains(&IngestStage::Chunk));
    }
}
