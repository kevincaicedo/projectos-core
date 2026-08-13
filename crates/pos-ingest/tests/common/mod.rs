//! Shared fixture for the M1-E1 oracles: a real `.pos` project, a real log, a
//! real queue, and the real pipeline. Nothing here stands in for a durable
//! path, because every property these suites claim is a property of that path.

#![forbid(unsafe_code)]
// Cargo compiles this module once per integration-test binary, so a helper
// every suite shares still reads as dead in the suites that do not call it.
#![allow(dead_code)]

use pos_domain::{
    DomainEvent, EvidenceShape, ExternalRef, IngestStage, JobClass, MediaKind, ProjectCreatedBody,
    v0_registry,
};
use pos_foundation::{DeviceId, EvidenceId, ManualWallClock, ProjectId, UserId};
use pos_ingest::{
    ChunkStage, EvidenceSubmission, IngestPipeline, NormalizeStage, PipelineConfig, StageRegistry,
    SubmitOutcome,
};
use pos_log::{Actor, LogConfig, ProjectLog};
use pos_sched::{
    BackoffPolicy, ClaimedJob, JobQueue, NoJitter, QueueConfig, SchedulerMetrics, WorkerName,
};
use pos_store::ProjectStore;
use std::path::Path;
use std::sync::Arc;

pub const DEVICE: DeviceId = DeviceId::from_bytes([0x71; 16]);
pub const USER: UserId = UserId::from_bytes([0x72; 16]);
pub const PROJECT: ProjectId = ProjectId::from_bytes([0x73; 16]);

/// Creates (or reopens) a project exactly as a shell would leave it.
pub fn open_project(root: &Path, clock: &ManualWallClock) -> ProjectLog {
    let fresh = !root.join("manifest.json").is_file();
    let store = if fresh {
        ProjectStore::create(root, "generic", clock).expect("create project store")
    } else {
        ProjectStore::open(root).expect("reopen project store")
    };
    let log = ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open project log");
    if fresh {
        let event = DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
            project_id: PROJECT,
            name: "ingest fixture".to_owned(),
            template: "generic".to_owned(),
        });
        let request = event
            .into_request(DEVICE, Actor::User(USER))
            .expect("project created request");
        log.append(request, clock).expect("append project created");
    }
    log
}

/// A queue with deterministic jitter, so retry instants are exact.
pub fn queue() -> Arc<JobQueue> {
    Arc::new(JobQueue::new(
        QueueConfig {
            device: DEVICE,
            backoff: BackoffPolicy::default(),
            lease_ttl_ms: 30_000,
        },
        Arc::new(NoJitter),
        Arc::new(SchedulerMetrics::default()),
    ))
}

/// The E1 pipeline: NORMALIZE and CHUNK are implemented; the rest of the plan
/// stops honestly at the first stage this build does not register (P6).
pub fn pipeline(queue: Arc<JobQueue>) -> IngestPipeline {
    IngestPipeline::new(
        PipelineConfig::for_device(DEVICE),
        queue,
        StageRegistry::new()
            .with(Arc::new(NormalizeStage))
            .with(Arc::new(ChunkStage::new())),
    )
}

pub fn submission(external_id: &str, shape: EvidenceShape, media: MediaKind) -> EvidenceSubmission {
    EvidenceSubmission {
        source_kind: "upload".to_owned(),
        source_scope: "fixture".to_owned(),
        external: ExternalRef {
            external_id: external_id.to_owned(),
            external_url: None,
            external_version: None,
        },
        media_kind: media,
        shape,
        occurred_ts_ms: 1_700_000_000_000,
        author: Some("fixture".to_owned()),
        title: Some("fixture item".to_owned()),
        thread_ref: None,
        actor: Actor::User(USER),
    }
}

/// Submits one item and returns its id.
pub fn submit(
    pipeline: &IngestPipeline,
    log: &ProjectLog,
    clock: &ManualWallClock,
    submission: &EvidenceSubmission,
    content: &[u8],
) -> SubmitOutcome {
    let mut reader = content;
    pipeline
        .submit(log, PROJECT, clock, submission, &mut reader)
        .expect("submit evidence")
}

/// Drains the ingest queue by claiming and running every stage job, exactly
/// as the worker pool would, but synchronously so a suite can assert between
/// steps. Returns `(stage, succeeded)` per attempt, in order — a suite that
/// only checked which stages ran would pass while every one of them failed.
pub fn drain(
    pipeline: &IngestPipeline,
    queue: &JobQueue,
    log: &ProjectLog,
    clock: &ManualWallClock,
    step_count_max: usize,
) -> Vec<(IngestStage, bool)> {
    let worker = WorkerName::new("fixture-0").expect("worker name");
    let mut ran = Vec::new();
    for _ in 0..step_count_max {
        let Some(job) = queue
            .claim(log, PROJECT, JobClass::Ingest, &worker, clock)
            .expect("claim stage job")
        else {
            break;
        };
        let stage = stage_of(&job);
        let outcome = pipeline.run_stage(log, PROJECT, clock, stage, &job);
        let succeeded = outcome.is_ok();
        match outcome {
            Ok(_) => {
                queue.complete(log, &job, 1, clock).expect("complete job");
            }
            Err(failure) => {
                let job_failure = if failure.permanent {
                    pos_sched::JobFailure::permanent(failure.code, failure.detail)
                } else {
                    pos_sched::JobFailure::retriable(failure.code, failure.detail)
                };
                queue
                    .fail(log, &job, &job_failure, 1, clock)
                    .expect("record failure");
            }
        }
        ran.push((stage, succeeded));
    }
    ran
}

pub fn stage_of(job: &ClaimedJob) -> IngestStage {
    IngestStage::ALL
        .into_iter()
        .find(|stage| stage.job_kind() == Some(job.kind.as_str()))
        .expect("every ingest job kind maps to a stage")
}

/// A deterministic transcript-shaped corpus: `turn_count` blank-line
/// separated turns whose lengths vary, so chunk windows are not all identical.
pub fn transcript_text(turn_count: usize) -> String {
    let mut text = String::new();
    for index in 0..turn_count {
        let words = 6 + (index * 7) % 40;
        text.push_str(&format!("Speaker {}:", index % 3));
        for word in 0..words {
            text.push_str(&format!(" word{}", (index * 13 + word) % 97));
        }
        text.push_str("\n\n");
    }
    text
}

/// A markdown document with headings at two depths.
pub fn document_text(section_count: usize) -> String {
    let mut text = String::new();
    for index in 0..section_count {
        text.push_str(&format!("## Section {index}\n\n"));
        for paragraph in 0..3 {
            for word in 0..40 {
                text.push_str(&format!(
                    "word{} ",
                    (index * 31 + paragraph * 7 + word) % 89
                ));
            }
            text.push_str("\n\n");
        }
    }
    text
}

/// A markdown document whose sections alternate small and large relative to
/// any plausible window: the small ones chunk identically at every window
/// size, the large ones split differently. That contrast is what makes
/// "unchanged chunks keep their ids" a testable claim rather than a tautology.
pub fn mixed_section_document(section_count: usize) -> String {
    let mut text = String::new();
    for index in 0..section_count {
        text.push_str(&format!("## Section {index}\n\n"));
        let paragraphs = if index % 2 == 0 { 1 } else { 12 };
        for paragraph in 0..paragraphs {
            for word in 0..40 {
                text.push_str(&format!(
                    "word{} ",
                    (index * 31 + paragraph * 7 + word) % 89
                ));
            }
            text.push_str("\n\n");
        }
    }
    text
}

pub fn evidence_id_of(outcome: SubmitOutcome) -> EvidenceId {
    outcome.evidence_id()
}

/// The digest the m1-s01 idempotency oracle compares: everything the pipeline
/// *derived*, and nothing about how many attempts it took to derive it.
///
/// A crashed-and-resumed run legitimately has more events, one more attempt
/// on the stage that died, and a `failed_count` on its source-health row.
/// Those are history, and a digest that hid them would be hiding exactly the
/// thing the DLQ criterion asks the pipeline to show. What must be identical
/// is the *output*: the evidence's derived columns and every chunk fact,
/// including the ids citations will point at forever.
pub fn pipeline_digest(log: &ProjectLog) -> String {
    let mut digest = String::new();
    let items = pos_domain::list_evidence(log, pos_domain::EvidenceListFilter::default())
        .expect("list evidence");
    for item in items {
        digest.push_str(&format!(
            "evidence {} shape={} status={} pass={} text={:?} segments={:?} segment_count={:?} \
             canary={} chunks={} chunk_pass={:?} content={:?} bytes={}\n",
            item.evidence_id.to_hex(),
            item.shape.as_str(),
            item.status.as_str(),
            item.pass,
            item.text_blob,
            item.segments_blob,
            item.segment_count,
            item.canary_level.as_str(),
            item.chunk_count,
            item.chunk_pass,
            item.content_blob,
            item.byte_size,
        ));
        let chunks =
            pos_domain::list_chunks(log, item.evidence_id, None, 500).expect("list chunks");
        for chunk in chunks {
            digest.push_str(&format!(
                "  chunk {} ordinal={} kind={} span={}..{} locator={:?} content={:?} tokens={} \
                 pass={}\n",
                chunk.chunk_id.to_hex(),
                chunk.ordinal,
                chunk.kind.as_str(),
                chunk.byte_start,
                chunk.byte_end,
                chunk.locator,
                chunk.content_hash,
                chunk.token_count_estimate,
                chunk.pass,
            ));
        }
    }
    digest
}
