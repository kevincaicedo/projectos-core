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
use pos_ingest::{
    EmbedSetup, StageLedgers, StageRegistry, TranscribeSetup, stage_registry_default,
};
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

/// Overrides which ONNX artifact EMBED loads.
pub const EMBED_MODEL_ENV: &str = "POS_EMBED_MODEL";

/// A process-local override for the embedding model, set before any worker
/// starts.
///
/// A `OnceLock` rather than `std::env::set_var`, which is `unsafe` for a real
/// reason — it mutates a process-global another thread may be reading — and
/// which core forbids outright. `pos ingest reembed --model X` needs to say
/// which model *this invocation* embeds under, and this is the safe way to
/// say it: written once, before `start_background_workers`, and read by every
/// surface through [`embed_setup`].
static EMBED_MODEL_OVERRIDE: std::sync::OnceLock<String> = std::sync::OnceLock::new();

/// Pins the embedding model for this process.
///
/// # Errors
///
/// The model already chosen, when something set it first. A second answer to
/// "which model does this process embed under" is a bug worth surfacing
/// rather than a value to overwrite.
pub fn set_embed_model(model: String) -> Result<(), String> {
    EMBED_MODEL_OVERRIDE
        .set(model)
        .map_err(|rejected| EMBED_MODEL_OVERRIDE.get().cloned().unwrap_or(rejected))
}

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

/// The embedding setup every surface in this process shares.
///
/// Local by default and `local_only` by policy: a default that sent every
/// chunk of a project to an API would be a privacy decision made by a
/// constant (L9/F7). The API route is composed by configuration, and needs
/// m1-s06's credential store before a user can reach it.
#[must_use]
pub fn embed_setup() -> EmbedSetup {
    let mut setup = EmbedSetup::local(models_dir());
    // The explicit override wins over the environment: a command that named a
    // model is a stronger statement than a variable in a shell profile.
    let chosen = EMBED_MODEL_OVERRIDE.get().cloned().or_else(|| {
        std::env::var(EMBED_MODEL_ENV)
            .ok()
            .filter(|model| !model.trim().is_empty())
    });
    if let Some(model) = chosen
        && let pos_ingest::EmbedRoute::LocalOnnx { model_name, .. } = &mut setup.route
    {
        *model_name = model;
    }
    setup
}

/// The stages this build can run. One answer, shared by every surface.
///
/// **EMBED registers only when its artifact is on disk**, and that is a
/// deliberate difference from every other stage. The distinction it preserves
/// is between two things the pipeline must never conflate:
///
/// - *this content failed* — a corrupt PDF, an unsupported codec. That is a
///   dead-letter, and the source-health card exists to show it.
/// - *this installation cannot run this stage* — no model has been pulled.
///   That is `nextStageAvailable: false` with the owning story named, which is
///   the vocabulary m1-s01 built for exactly this and which every shell
///   already renders.
///
/// Registering unconditionally would put a fresh install's entire corpus in
/// the DLQ for a reason that is not about the corpus. TRANSCRIBE does register
/// unconditionally, and the asymmetry is intentional rather than an
/// oversight: it applies only to audio and video, so a missing whisper model
/// parks the handful of items that need it, while EMBED applies to
/// *everything*.
///
/// The cost, stated: the worker pool composes its handlers once, so a model
/// pulled while a shell is running is picked up at next start. m1-s15's
/// onboarding is where pulling becomes part of first run.
#[must_use]
pub fn stage_registry() -> StageRegistry {
    let embed = embed_setup();
    let registry = stage_registry_default(&transcribe_setup(), &embed);
    if embed_artifact_present(&embed) {
        return registry;
    }
    registry.without(pos_domain::IngestStage::Embed)
}

/// Whether the local ONNX artifact this setup names is on disk.
///
/// A cloud route is always "present": its artifact is somebody else's, and
/// whether the credential resolves is the gateway's answer to give, not a
/// file check's.
fn embed_artifact_present(setup: &pos_ingest::EmbedSetup) -> bool {
    match &setup.route {
        pos_ingest::EmbedRoute::LocalOnnx {
            models_dir,
            model_name,
            ..
        } => models_dir.join(model_name).join("model.onnx").is_file(),
        pos_ingest::EmbedRoute::Cloud { .. } => true,
    }
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
