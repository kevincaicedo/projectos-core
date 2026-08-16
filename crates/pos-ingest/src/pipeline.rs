//! The stage framework (m1-s01): stages are `pos-sched` jobs, and this is
//! everything that makes one run.
//!
//! Read the crate-level invariant inventory (P1–P6) before changing anything
//! here — all six live in this file.

use crate::IngestError;
use crate::budget::{BoundedStream, STAGE_BUFFER_BYTES_MAX_DEFAULT, StreamBudget};
use crate::identity::{derive_evidence_id, derive_source_id};
use crate::segment::SegmentReader;
use pos_domain::{
    CHUNK_BATCH_COUNT_MAX, ChunkFact, DomainEvent, EvidenceAddedBody, EvidenceChunkedBody,
    EvidenceRecord, EvidenceShape, ExternalRef, IngestStage, IngestStageDisposition,
    IngestStageFailedBody, IngestStageFinishedBody, IngestStageOutput, IngestStageStartedBody,
    JobClass, JobPriority, MediaKind, read_evidence,
};
use pos_foundation::telemetry::{
    Parent, Span, SpanContext, SpanDetail, SpanField, SpanName, SpanValue,
};
use pos_foundation::{DeviceId, EvidenceId, JobId, ProjectId, WallClock};
use pos_log::{Actor, AppendRequest, ProjectLog};
use pos_sched::{ClaimedJob, JobFailure, JobHandler, JobKind, JobQueue, JobSpec, ProjectRegistry};
use pos_store::{BlobHash, BlobWriter};
use std::collections::BTreeMap;
use std::fs::File;
use std::sync::Arc;

/// Retries a stage takes before the DLQ. Deliberately below the scheduler's
/// own cap: a stage that has failed four times is failing for a reason a
/// fifth attempt will not change, and a visible DLQ item beats an invisible
/// retry loop (L8).
pub const STAGE_RETRY_COUNT_MAX: u32 = 4;

/// The stage payload: `evidence_id ‖ pass`. Fixed width and 20 bytes, so
/// there is no payload parser to fuzz and no room for a payload to become the
/// work itself (the job-payload rule in `pos-sched`).
const STAGE_PAYLOAD_BYTES: usize = 20;

/// The idempotency key of one stage job. `{evidence, stage, pass}` is the
/// M1 §3.2 key; see [`IngestStageStartedBody`] for why the third component is
/// the pipeline pass rather than the scheduler's retry attempt.
#[must_use]
pub fn stage_idempotency_key(evidence_id: EvidenceId, stage: IngestStage, pass: u32) -> String {
    format!("{}:{}:{pass}", evidence_id.to_hex(), stage.as_str())
}

fn encode_stage_payload(evidence_id: EvidenceId, pass: u32) -> Vec<u8> {
    let mut payload = Vec::with_capacity(STAGE_PAYLOAD_BYTES);
    payload.extend_from_slice(&evidence_id.into_bytes());
    payload.extend_from_slice(&pass.to_le_bytes());
    payload
}

fn decode_stage_payload(payload: &[u8]) -> Option<(EvidenceId, u32)> {
    if payload.len() != STAGE_PAYLOAD_BYTES {
        return None;
    }
    let id = <[u8; 16]>::try_from(&payload[0..16]).ok()?;
    let pass = u32::from_le_bytes(<[u8; 4]>::try_from(&payload[16..20]).ok()?);
    Some((EvidenceId::from_bytes(id), pass))
}

/// What a caller hands the pipeline to create one Evidence item.
pub struct EvidenceSubmission {
    /// Connector kind (`upload`, `watch-folder`, `slack`, …).
    pub source_kind: String,
    /// The selection inside that connector this item came from.
    pub source_scope: String,
    /// Identity in the origin system. An empty `external_id` means "address
    /// this by its content", which is what the upload path wants: re-dropping
    /// the same file is then a visible no-op rather than a duplicate.
    pub external: ExternalRef,
    pub media_kind: MediaKind,
    pub shape: EvidenceShape,
    pub occurred_ts_ms: u64,
    pub author: Option<String>,
    pub title: Option<String>,
    pub thread_ref: Option<String>,
    /// Who caused this. Never defaulted — a user drag-drop and a connector
    /// fetch are different facts (event-sourcing skill).
    pub actor: Actor,
}

/// What [`IngestPipeline::submit`] decided.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SubmitOutcome {
    Added(EvidenceId),
    /// This exact `(source, external ref)` is already evidence. The bytes were
    /// still streamed and hashed — the CAS deduplicated them — and no second
    /// pipeline run was scheduled.
    Duplicate(EvidenceId),
}

impl SubmitOutcome {
    #[must_use]
    pub const fn evidence_id(self) -> EvidenceId {
        match self {
            Self::Added(id) | Self::Duplicate(id) => id,
        }
    }

    #[must_use]
    pub const fn is_duplicate(self) -> bool {
        matches!(self, Self::Duplicate(_))
    }
}

#[derive(Clone, Copy, Debug)]
pub struct PipelineConfig {
    pub device: DeviceId,
    pub buffer_bytes_max: usize,
    pub stage_retry_count_max: u32,
}

impl PipelineConfig {
    #[must_use]
    pub const fn for_device(device: DeviceId) -> Self {
        Self {
            device,
            buffer_bytes_max: STAGE_BUFFER_BYTES_MAX_DEFAULT,
            stage_retry_count_max: STAGE_RETRY_COUNT_MAX,
        }
    }
}

/// What a stage produced. The durable output rides with the completion fact
/// rather than in a second event, so the two can never disagree.
#[derive(Clone, Debug)]
pub struct StageProduct {
    pub output: IngestStageOutput,
    pub bytes_read: u64,
    pub item_count: u64,
}

/// A stage's typed refusal. `permanent` is the handler stating that retrying
/// cannot change the outcome — malformed content, not a full disk.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct StageFailure {
    pub code: String,
    pub detail: String,
    pub permanent: bool,
}

impl StageFailure {
    #[must_use]
    pub fn retriable(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
            permanent: false,
        }
    }

    #[must_use]
    pub fn permanent(code: impl Into<String>, detail: impl Into<String>) -> Self {
        Self {
            code: code.into(),
            detail: detail.into(),
            permanent: true,
        }
    }
}

impl From<IngestError> for StageFailure {
    fn from(error: IngestError) -> Self {
        Self {
            code: error.code().to_owned(),
            detail: error.to_string(),
            permanent: !error.is_retriable(),
        }
    }
}

/// How one stage attempt ended, from the pipeline's point of view.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum StageOutcome {
    /// The stage succeeded and the next one is queued.
    Advanced { next: IngestStage },
    /// The stage succeeded and the plan is complete for this build. `next` is
    /// `None` at INDEX and `Some(stage)` when the next stage has no handler
    /// registered here — an honest stop, never a claim of completeness (P6).
    Halted { next: Option<IngestStage> },
}

/// The four facts one stage attempt is about. Bundled because they always
/// travel together and every function below needs all of them; passing them
/// as loose arguments made the signatures longer than the bodies.
struct StageAttempt<'a> {
    stage: IngestStage,
    job: &'a ClaimedJob,
    evidence: &'a EvidenceRecord,
    pass: u32,
}

/// One stage's implementation.
pub trait StageHandler: Send + Sync {
    fn stage(&self) -> IngestStage;
    fn run(&self, context: &StageContext<'_>) -> Result<StageProduct, StageFailure>;
}

/// The registered stage implementations of this build.
#[derive(Default)]
pub struct StageRegistry {
    handlers: BTreeMap<IngestStage, Arc<dyn StageHandler>>,
}

impl StageRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a handler, replacing any previous one for the same stage.
    /// Replacement rather than refusal because a build composes its registry
    /// once at startup and a duplicate is a wiring bug the type system
    /// already prevents from being two different stages.
    #[must_use]
    pub fn with(mut self, handler: Arc<dyn StageHandler>) -> Self {
        self.handlers.insert(handler.stage(), handler);
        self
    }

    #[must_use]
    pub fn get(&self, stage: IngestStage) -> Option<Arc<dyn StageHandler>> {
        self.handlers.get(&stage).cloned()
    }

    #[must_use]
    pub fn contains(&self, stage: IngestStage) -> bool {
        self.handlers.contains_key(&stage)
    }

    /// The stages this build can run, in pipeline order — what the honest
    /// "the rest arrives with m1-sNN" message is derived from.
    #[must_use]
    pub fn stages(&self) -> Vec<IngestStage> {
        let mut stages: Vec<IngestStage> = self.handlers.keys().copied().collect();
        stages.sort_by_key(|stage| stage.rank());
        stages
    }
}

/// Everything a running stage is allowed to touch.
pub struct StageContext<'a> {
    log: &'a ProjectLog,
    clock: &'a dyn WallClock,
    device: DeviceId,
    job_id: JobId,
    evidence: &'a EvidenceRecord,
    stage: IngestStage,
    pass: u32,
    buffer_bytes_max: usize,
}

impl StageContext<'_> {
    #[must_use]
    pub const fn evidence(&self) -> &EvidenceRecord {
        self.evidence
    }

    #[must_use]
    pub const fn pass(&self) -> u32 {
        self.pass
    }

    #[must_use]
    pub const fn budget(&self) -> StreamBudget {
        StreamBudget::new(self.stage, self.buffer_bytes_max)
    }

    /// The original bytes, streamed. NORMALIZE and TRANSCRIBE read this.
    pub fn open_content(&self) -> Result<BoundedStream<File>, IngestError> {
        self.open_blob(self.evidence.content_blob)
    }

    /// The normalized text, streamed. CHUNK and every later stage read this.
    pub fn open_text(&self) -> Result<BoundedStream<File>, IngestError> {
        let hash = self
            .evidence
            .text_blob
            .ok_or(IngestError::StageInputMissing {
                stage: self.stage,
                missing: "the normalized text blob",
            })?;
        self.open_blob(hash)
    }

    /// The segment index over the normalized text, streamed.
    pub fn open_segments(&self) -> Result<SegmentReader<File>, IngestError> {
        let hash = self
            .evidence
            .segments_blob
            .ok_or(IngestError::StageInputMissing {
                stage: self.stage,
                missing: "the segment index blob",
            })?;
        Ok(SegmentReader::new(self.open_blob(hash)?))
    }

    fn open_blob(&self, hash: [u8; 32]) -> Result<BoundedStream<File>, IngestError> {
        let file = self
            .log
            .store()
            .blobs()
            .open_blob(BlobHash::from_bytes(hash))?;
        Ok(BoundedStream::new(file, self.budget()))
    }

    /// A CAS writer for a derived blob. Content-addressed, so writing it
    /// before any fact is committed is safe (P2): a crash leaves an
    /// unreferenced blob the sweep collects, not a dangling reference.
    pub fn blob_writer(&self) -> Result<BlobWriter<'_>, IngestError> {
        Ok(self.log.store().blobs().writer()?)
    }

    /// Commits one bounded batch of chunk facts as it is produced, so a
    /// corpus-sized item never accumulates its chunks in memory (P4).
    pub fn emit_chunks(&self, batch_index: u32, chunks: Vec<ChunkFact>) -> Result<(), IngestError> {
        if chunks.len() > CHUNK_BATCH_COUNT_MAX {
            return Err(IngestError::LimitExceeded {
                limit: "chunk batch",
                value: chunks.len() as u64,
                limit_value: CHUNK_BATCH_COUNT_MAX as u64,
            });
        }
        if chunks.is_empty() {
            return Ok(());
        }
        let event = DomainEvent::EvidenceChunked(EvidenceChunkedBody::V1 {
            evidence_id: self.evidence.evidence_id,
            pass: self.pass,
            batch_index,
            chunks,
        });
        let request = event.into_request(self.device, Actor::System(self.job_id))?;
        self.log.append(request, self.clock)?;
        Ok(())
    }
}

/// The pipeline: submission, stage execution, and the plan that connects them.
pub struct IngestPipeline {
    config: PipelineConfig,
    queue: Arc<JobQueue>,
    stages: StageRegistry,
}

impl IngestPipeline {
    #[must_use]
    pub fn new(config: PipelineConfig, queue: Arc<JobQueue>, stages: StageRegistry) -> Self {
        Self {
            config,
            queue,
            stages,
        }
    }

    #[must_use]
    pub const fn stages(&self) -> &StageRegistry {
        &self.stages
    }

    #[must_use]
    pub const fn config(&self) -> &PipelineConfig {
        &self.config
    }

    #[must_use]
    pub const fn queue(&self) -> &Arc<JobQueue> {
        &self.queue
    }

    /// RAW: streams the content into the CAS, then commits the evidence fact
    /// and the first stage job in one transaction (P1, P2).
    ///
    /// RAW is not a job by construction: whoever owns the bytes runs it, and
    /// re-running it would mean re-fetching from the source, which reprocess
    /// must never do.
    pub fn submit(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
        submission: &EvidenceSubmission,
        content: &mut impl std::io::Read,
    ) -> Result<SubmitOutcome, IngestError> {
        let hash = log.store().blobs().write_stream(content)?;
        let byte_size = blob_byte_size(log, hash)?;
        let source_id = derive_source_id(&submission.source_kind, &submission.source_scope);
        let external = resolve_external_ref(&submission.external, hash);
        let evidence_id = derive_evidence_id(source_id, &external.external_id);
        if read_evidence(log, evidence_id)?.is_some() {
            return Ok(SubmitOutcome::Duplicate(evidence_id));
        }
        let added = DomainEvent::EvidenceAdded(EvidenceAddedBody::V1 {
            evidence_id,
            source_id,
            source_kind: submission.source_kind.clone(),
            external,
            media_kind: submission.media_kind,
            shape: submission.shape,
            content_blob: hash.into_bytes(),
            byte_size,
            occurred_ts_ms: submission.occurred_ts_ms,
            author: submission.author.clone(),
            title: submission.title.clone(),
            thread_ref: submission.thread_ref.clone(),
        });
        let mut requests = vec![added.into_request(self.config.device, submission.actor)?];
        let next = IngestStage::Raw.next_for(submission.media_kind);
        let queued = self.stage_job_request(log, project_id, evidence_id, next, 0)?;
        let appended = queued.unwrap_or_default();
        let minted = !appended.is_empty();
        requests.extend(appended);
        log.append_batch(&requests, clock)?;
        if minted {
            self.queue.record_enqueued();
        }
        Ok(SubmitOutcome::Added(evidence_id))
    }

    /// Runs one stage attempt end to end: start fact, handler, then either
    /// the completion plus the next job in one transaction, or the typed
    /// failure. Every early return in here is a durable fact, never a silent
    /// exit — a stage that vanished would leave an item stuck with nothing to
    /// explain it.
    pub fn run_stage(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
        stage: IngestStage,
        job: &ClaimedJob,
    ) -> Result<StageOutcome, StageFailure> {
        let Some((evidence_id, pass)) = decode_stage_payload(&job.payload) else {
            return Err(StageFailure::permanent(
                "malformed_payload",
                "a stage job payload is 16 evidence-id bytes and a 4-byte pass",
            ));
        };
        let evidence = self.load_for_stage(log, evidence_id, stage, pass)?;
        // One trace per Evidence item, spanning every stage in every process
        // that ever works on it (m1-s01's correlated-trace criterion). The
        // job id rides as a field so the queue's own view stays reachable.
        let span = Span::open_detached(
            SpanName::IngestStage,
            SpanDetail::from_static(stage.as_str()),
            Parent::Root(SpanContext::for_evidence(project_id, evidence_id)),
        );
        span.set(SpanField::Project, SpanValue::Id(project_id.into_bytes()));
        span.set(SpanField::Evidence, SpanValue::Id(evidence_id.into_bytes()));
        span.set(SpanField::Job, SpanValue::Id(job.job_id.into_bytes()));
        span.set(
            SpanField::Attempt,
            SpanValue::Count(u64::from(job.attempt_index)),
        );
        let attempt = StageAttempt {
            stage,
            job,
            evidence: &evidence,
            pass,
        };
        let outcome = self.run_stage_inner(log, project_id, clock, &attempt);
        let label = match &outcome {
            Ok(StageOutcome::Advanced { .. }) => "ok",
            Ok(StageOutcome::Halted { .. }) => "halted",
            Err(failure) if failure.permanent => "failed_permanent",
            Err(_) => "failed",
        };
        span.finish(label);
        outcome
    }

    fn run_stage_inner(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
        attempt: &StageAttempt<'_>,
    ) -> Result<StageOutcome, StageFailure> {
        let StageAttempt {
            stage,
            job,
            evidence,
            pass,
        } = *attempt;
        let Some(handler) = self.stages.get(stage) else {
            return Err(StageFailure::permanent(
                "stage_not_registered",
                format!("stage {stage} is implemented by {}", stage.owner_story()),
            ));
        };
        let started = DomainEvent::IngestStageStarted(IngestStageStartedBody::V1 {
            evidence_id: evidence.evidence_id,
            source_id: evidence.source_id,
            stage,
            pass,
            job_id: job.job_id,
            attempt_index: job.attempt_index,
        });
        self.append_one(log, clock, started, job.job_id)?;
        let started_ts_ms = clock.now_ms();
        let context = StageContext {
            log,
            clock,
            device: self.config.device,
            job_id: job.job_id,
            evidence,
            stage,
            pass,
            buffer_bytes_max: self.config.buffer_bytes_max,
        };
        match handler.run(&context) {
            Ok(product) => {
                let wall_ms = clock.now_ms().saturating_sub(started_ts_ms);
                self.commit_finished(log, project_id, clock, attempt, product, wall_ms)
            }
            Err(failure) => {
                self.commit_failed(log, clock, attempt, &failure)?;
                Err(failure)
            }
        }
    }

    /// P1: the completion fact and the next stage's job commit together.
    fn commit_finished(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
        attempt: &StageAttempt<'_>,
        product: StageProduct,
        wall_ms: u64,
    ) -> Result<StageOutcome, StageFailure> {
        let StageAttempt {
            stage,
            job,
            evidence,
            pass,
        } = *attempt;
        let finished = DomainEvent::IngestStageFinished(IngestStageFinishedBody::V1 {
            evidence_id: evidence.evidence_id,
            source_id: evidence.source_id,
            stage,
            pass,
            wall_ms,
            bytes_read: product.bytes_read,
            item_count: product.item_count,
            output: product.output,
        });
        let mut requests = vec![
            finished
                .into_request(self.config.device, Actor::System(job.job_id))
                .map_err(IngestError::from)?,
        ];
        // The plan follows the media: NORMALIZE may correct the *shape* RAW
        // guessed, and that changes how the item chunks, not which stages run.
        let next = stage.next_for(evidence.media_kind);
        let queued = self.stage_job_request(log, project_id, evidence.evidence_id, next, pass)?;
        // `Some(empty)` is a redelivery whose job already exists — the queue
        // working as designed, and still an advance. Only `None` is a halt.
        let advanced = queued.is_some();
        let minted = queued.as_ref().is_some_and(|requests| !requests.is_empty());
        requests.extend(queued.unwrap_or_default());
        log.append_batch(&requests, clock)
            .map_err(IngestError::from)?;
        if minted {
            self.queue.record_enqueued();
        }
        Ok(match (advanced, next) {
            (true, Some(next)) => StageOutcome::Advanced { next },
            _ => StageOutcome::Halted { next },
        })
    }

    fn commit_failed(
        &self,
        log: &ProjectLog,
        clock: &dyn WallClock,
        attempt: &StageAttempt<'_>,
        failure: &StageFailure,
    ) -> Result<(), IngestError> {
        let StageAttempt {
            stage,
            job,
            evidence,
            pass,
        } = *attempt;
        let disposition = if failure.permanent || job.attempt_index >= job.retry_count_max {
            IngestStageDisposition::Dead {
                permanent: failure.permanent,
            }
        } else {
            IngestStageDisposition::Retrying {
                attempt_count_max: job.retry_count_max,
            }
        };
        let failed = DomainEvent::IngestStageFailed(IngestStageFailedBody::V1 {
            evidence_id: evidence.evidence_id,
            source_id: evidence.source_id,
            stage,
            pass,
            attempt_index: job.attempt_index,
            code: failure.code.clone(),
            detail: failure.detail.clone(),
            disposition,
        });
        self.append_one(log, clock, failed, job.job_id)
    }

    fn append_one(
        &self,
        log: &ProjectLog,
        clock: &dyn WallClock,
        event: DomainEvent,
        job_id: JobId,
    ) -> Result<(), IngestError> {
        let request = event.into_request(self.config.device, Actor::System(job_id))?;
        log.append(request, clock)?;
        Ok(())
    }

    /// The next stage's enqueue, as append requests the caller batches.
    ///
    /// `None` means the pipeline stops here — no next stage, or no handler
    /// for it in this build (P6). `Some(requests)` means the next stage is
    /// queued; the vector is empty when the job already existed, which is an
    /// at-least-once redelivery rather than a halt.
    fn stage_job_request(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        evidence_id: EvidenceId,
        next: Option<IngestStage>,
        pass: u32,
    ) -> Result<Option<Vec<AppendRequest>>, IngestError> {
        let Some(stage) = next.filter(|stage| self.stages.contains(*stage)) else {
            return Ok(None);
        };
        let spec = self.stage_job_spec(evidence_id, stage, pass)?;
        let (_, request) = self.queue.enqueue_request(log, project_id, &spec)?;
        Ok(Some(request.into_iter().collect()))
    }

    pub(crate) fn stage_job_spec(
        &self,
        evidence_id: EvidenceId,
        stage: IngestStage,
        pass: u32,
    ) -> Result<JobSpec, IngestError> {
        let Some(kind) = stage.job_kind() else {
            return Err(IngestError::StageNotReprocessable { stage });
        };
        let kind = JobKind::new(kind).map_err(|error| {
            IngestError::Sched(pos_sched::SchedError::InvalidSpec {
                field: "kind",
                reason: error.to_string(),
            })
        })?;
        Ok(
            JobSpec::new(kind, stage_idempotency_key(evidence_id, stage, pass))
                // Ingest never outranks interactive work: the §18 gate is that
                // the app stays usable *while* a 10 GB corpus ingests.
                .with_class(JobClass::Ingest)
                .with_priority(JobPriority::Normal)
                .with_payload(encode_stage_payload(evidence_id, pass))
                .with_retry_count_max(self.config.stage_retry_count_max),
        )
    }

    /// Loads the evidence row and refuses stale or sealed work (P5).
    fn load_for_stage(
        &self,
        log: &ProjectLog,
        evidence_id: EvidenceId,
        stage: IngestStage,
        pass: u32,
    ) -> Result<EvidenceRecord, StageFailure> {
        let record = read_evidence(log, evidence_id)
            .map_err(IngestError::from)?
            .ok_or(IngestError::UnknownEvidence {
                evidence_id: evidence_id.to_hex(),
            })?;
        if record.pass != pass {
            return Err(IngestError::StalePass {
                stage,
                job_pass: pass,
                evidence_pass: record.pass,
            }
            .into());
        }
        Ok(record)
    }
}

/// Fills in the content-addressed external id an upload leaves blank, so an
/// item is always identified by *something* stable in its origin.
fn resolve_external_ref(external: &ExternalRef, hash: BlobHash) -> ExternalRef {
    let mut resolved = external.clone();
    if resolved.external_id.is_empty() {
        resolved.external_id = hash.to_hex();
    }
    resolved
}

fn blob_byte_size(log: &ProjectLog, hash: BlobHash) -> Result<u64, IngestError> {
    let file = log.store().blobs().open_blob(hash)?;
    let metadata = file.metadata().map_err(|source| IngestError::Io {
        operation: "measure stored blob",
        source,
    })?;
    Ok(metadata.len())
}

/// The stages this build implements, in one place.
///
/// Every shell composes the same registry, so "what can this build actually
/// do to an Evidence item?" has one answer rather than one per surface. As
/// m1-s03/s04/s05/s11 land their stages they are added here, and every item
/// already in a project resumes with `pos ingest reprocess --from-stage`.
#[must_use]
pub fn stage_registry_default() -> StageRegistry {
    StageRegistry::new()
        .with(Arc::new(crate::normalize::NormalizeStage))
        .with(Arc::new(crate::chunk::ChunkStage::new()))
}

/// Every registered stage as a `pos-sched` handler, ready for a pool's
/// handler registry.
///
/// The composition lives here rather than in each shell because "which stages
/// can this process run?" must have exactly one answer per build (L12). A
/// shell that assembled its own subset would make background work a
/// per-surface behaviour, which is the drift [`StageRegistry`] exists to
/// prevent.
///
/// # Errors
///
/// Refuses if a registered stage has no job kind — only [`IngestStage::Raw`],
/// which is not a job by construction and cannot be in a stage registry.
pub fn stage_job_handlers(
    pipeline: &Arc<IngestPipeline>,
    projects: &Arc<ProjectRegistry>,
    clock: &Arc<dyn WallClock>,
) -> Result<Vec<Arc<dyn JobHandler>>, IngestError> {
    pipeline
        .stages()
        .stages()
        .into_iter()
        .map(|stage| {
            StageJobHandler::new(
                stage,
                Arc::clone(pipeline),
                Arc::clone(projects),
                Arc::clone(clock),
            )
            .map(|handler| Arc::new(handler) as Arc<dyn JobHandler>)
        })
        .collect()
}

/// The `pos-sched` adapter: one handler per stage job kind, resolving the
/// project from the registry the pool already keeps.
pub struct StageJobHandler {
    kind: JobKind,
    stage: IngestStage,
    pipeline: Arc<IngestPipeline>,
    projects: Arc<ProjectRegistry>,
    clock: Arc<dyn WallClock>,
}

impl StageJobHandler {
    /// # Errors
    ///
    /// Refuses [`IngestStage::Raw`], which has no job by construction.
    pub fn new(
        stage: IngestStage,
        pipeline: Arc<IngestPipeline>,
        projects: Arc<ProjectRegistry>,
        clock: Arc<dyn WallClock>,
    ) -> Result<Self, IngestError> {
        let name = stage
            .job_kind()
            .ok_or(IngestError::StageNotReprocessable { stage })?;
        let kind = JobKind::new(name).map_err(|error| {
            IngestError::Sched(pos_sched::SchedError::InvalidSpec {
                field: "kind",
                reason: error.to_string(),
            })
        })?;
        Ok(Self {
            kind,
            stage,
            pipeline,
            projects,
            clock,
        })
    }
}

impl JobHandler for StageJobHandler {
    fn kind(&self) -> &JobKind {
        &self.kind
    }

    fn run(&self, job: &ClaimedJob) -> Result<(), JobFailure> {
        let Some(log) = self.projects.get(job.project_id) else {
            // The project closed between claim and run. Retriable on purpose:
            // reopening it is a normal thing for a shell to do, and the lease
            // will simply be re-claimed by whoever holds it next.
            return Err(JobFailure::retriable(
                "project_not_open",
                "the project is not registered with this scheduler",
            ));
        };
        self.pipeline
            .run_stage(&log, job.project_id, self.clock.as_ref(), self.stage, job)
            .map(|_| ())
            .map_err(|failure| {
                if failure.permanent {
                    JobFailure::permanent(failure.code, failure.detail)
                } else {
                    JobFailure::retriable(failure.code, failure.detail)
                }
            })
    }
}

#[cfg(test)]
mod tests {
    use super::{decode_stage_payload, encode_stage_payload, stage_idempotency_key};
    use pos_domain::IngestStage;
    use pos_foundation::EvidenceId;
    use pos_sched::JOB_IDEMPOTENCY_KEY_LEN_MAX;

    #[test]
    fn the_stage_payload_round_trips_and_rejects_any_other_length() {
        let evidence = EvidenceId::from_bytes([5; 16]);
        let payload = encode_stage_payload(evidence, 7);
        assert_eq!(decode_stage_payload(&payload), Some((evidence, 7)));
        assert_eq!(decode_stage_payload(&payload[..19]), None);
        let mut longer = payload.clone();
        longer.push(0);
        assert_eq!(decode_stage_payload(&longer), None);
    }

    #[test]
    fn every_stage_key_fits_the_schedulers_stated_bound() {
        let evidence = EvidenceId::from_bytes([0xff; 16]);
        for stage in IngestStage::ALL {
            let key = stage_idempotency_key(evidence, stage, u32::MAX);
            assert!(
                key.len() <= JOB_IDEMPOTENCY_KEY_LEN_MAX,
                "{key} is {} bytes",
                key.len()
            );
        }
    }

    #[test]
    fn the_key_separates_stage_and_pass() {
        let evidence = EvidenceId::from_bytes([1; 16]);
        assert_ne!(
            stage_idempotency_key(evidence, IngestStage::Chunk, 0),
            stage_idempotency_key(evidence, IngestStage::Chunk, 1)
        );
        assert_ne!(
            stage_idempotency_key(evidence, IngestStage::Chunk, 0),
            stage_idempotency_key(evidence, IngestStage::Embed, 0)
        );
    }
}
