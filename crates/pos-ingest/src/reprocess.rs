//! Reprocess (m1-s01): re-run the pipeline from a stage, never re-fetch.
//!
//! `pos ingest reprocess --from-stage embed` is how a better chunker, a
//! better embedding model, or a fixed extractor reaches a corpus that is
//! already ingested. Three rules make it safe:
//!
//! 1. **RAW is never a target.** Re-running it would mean going back to the
//!    source, which is the one thing reprocess must not do — the bytes we
//!    hold are the evidence, and the source may have edited or deleted the
//!    original since.
//! 2. **The pass increments.** Stage jobs are keyed `{evidence, stage, pass}`,
//!    so the new work has new ids and cannot collide with the completed jobs
//!    it replaces, while any *in-flight* job from the old pass refuses
//!    typed on its next attempt (P5) instead of writing over newer output.
//! 3. **The rewind and the enqueue commit together.** One transaction holds
//!    `EvidenceReprocessRequested` and the first re-enqueued stage job, so an
//!    item can never be rewound without the work that moves it forward (P1).

use crate::IngestError;
use crate::pipeline::IngestPipeline;
use pos_domain::{
    DomainEvent, EvidenceListFilter, EvidenceReprocessRequestedBody, EvidenceStatus, IngestStage,
    list_evidence, read_evidence,
};
use pos_foundation::{EvidenceId, ProjectId, WallClock};
use pos_log::{Actor, ProjectLog};

/// Evidence items one reprocess call rewinds. A corpus-wide re-embed is a
/// legitimate thing to want and an unbounded transaction is not, so the
/// command pages and reports what it did (L8: the bound is in the result).
pub const REPROCESS_ITEM_COUNT_MAX: u32 = 500;

/// What to reprocess.
#[derive(Clone, Copy, Debug)]
pub struct ReprocessRequest {
    /// One item, or every item in the project when `None`.
    pub evidence_id: Option<EvidenceId>,
    pub from_stage: IngestStage,
    pub item_count_max: u32,
}

impl ReprocessRequest {
    #[must_use]
    pub const fn all(from_stage: IngestStage) -> Self {
        Self {
            evidence_id: None,
            from_stage,
            item_count_max: REPROCESS_ITEM_COUNT_MAX,
        }
    }

    #[must_use]
    pub const fn one(evidence_id: EvidenceId, from_stage: IngestStage) -> Self {
        Self {
            evidence_id: Some(evidence_id),
            from_stage,
            item_count_max: 1,
        }
    }
}

/// What a reprocess actually did. `skipped_not_reached` is the honest count
/// of items that never got as far as the target stage and therefore have
/// nothing to redo — reporting them as reprocessed would be a lie about work.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct ReprocessPlan {
    pub requeued: Vec<(EvidenceId, u32)>,
    pub skipped_not_reached: u32,
    pub item_count_max: u32,
}

impl ReprocessPlan {
    #[must_use]
    pub fn requeued_count(&self) -> usize {
        self.requeued.len()
    }

    /// Whether the bound cut the work short — the caller repeats until this
    /// is false, and a UI can say so instead of implying completeness.
    #[must_use]
    pub fn is_truncated(&self) -> bool {
        self.requeued_count() >= self.item_count_max as usize
    }
}

impl IngestPipeline {
    /// Rewinds matching evidence to `from_stage` under a new pass and queues
    /// that stage's job for each.
    pub fn reprocess(
        &self,
        log: &ProjectLog,
        project_id: ProjectId,
        clock: &dyn WallClock,
        actor: &Actor,
        request: ReprocessRequest,
        reason: &str,
    ) -> Result<ReprocessPlan, IngestError> {
        if request.from_stage == IngestStage::Raw {
            return Err(IngestError::StageNotReprocessable {
                stage: IngestStage::Raw,
            });
        }
        let targets = self.reprocess_targets(log, request)?;
        let mut plan = ReprocessPlan {
            item_count_max: request.item_count_max,
            ..ReprocessPlan::default()
        };
        for (evidence_id, current_pass, status) in targets {
            if !reached(status, request.from_stage) {
                plan.skipped_not_reached += 1;
                continue;
            }
            let pass = current_pass.saturating_add(1);
            let requested =
                DomainEvent::EvidenceReprocessRequested(EvidenceReprocessRequestedBody::V1 {
                    evidence_id,
                    from_stage: request.from_stage,
                    pass,
                    reason: reason.to_owned(),
                });
            let mut requests = vec![requested.into_request(self.config().device, *actor)?];
            let spec = self.stage_job_spec(evidence_id, request.from_stage, pass)?;
            let (_, enqueue) = self.queue().enqueue_request(log, project_id, &spec)?;
            requests.extend(enqueue);
            // One transaction per item rather than one for the batch: a
            // corpus-wide reprocess must be resumable, and a single failing
            // item must not roll back the hundred before it.
            log.append_batch(&requests, clock)?;
            self.queue().record_enqueued();
            plan.requeued.push((evidence_id, pass));
        }
        Ok(plan)
    }

    fn reprocess_targets(
        &self,
        log: &ProjectLog,
        request: ReprocessRequest,
    ) -> Result<Vec<(EvidenceId, u32, EvidenceStatus)>, IngestError> {
        if let Some(evidence_id) = request.evidence_id {
            let record = read_evidence(log, evidence_id)?.ok_or(IngestError::UnknownEvidence {
                evidence_id: evidence_id.to_hex(),
            })?;
            return Ok(vec![(record.evidence_id, record.pass, record.status)]);
        }
        let filter = EvidenceListFilter {
            row_count_max: Some(request.item_count_max),
            ..EvidenceListFilter::default()
        };
        Ok(list_evidence(log, filter)?
            .into_iter()
            .map(|record| (record.evidence_id, record.pass, record.status))
            .collect())
    }
}

/// Whether an item ever completed the stage *before* the reprocess target —
/// the precondition for that target having inputs to work on.
fn reached(status: EvidenceStatus, from_stage: IngestStage) -> bool {
    // A failed item is always eligible: retrying the stage that killed it is
    // exactly what a human reaches for after fixing the cause.
    if status == EvidenceStatus::Failed {
        return true;
    }
    let completed = IngestStage::ALL
        .into_iter()
        .find(|stage| EvidenceStatus::after(*stage) == status);
    completed.is_some_and(|completed| completed.rank() + 1 >= from_stage.rank())
}

#[cfg(test)]
mod tests {
    use super::{ReprocessPlan, reached};
    use pos_domain::{EvidenceStatus, IngestStage};
    use pos_foundation::EvidenceId;

    #[test]
    fn an_item_can_be_rewound_to_the_stage_after_the_one_it_completed() {
        assert!(reached(EvidenceStatus::Chunked, IngestStage::Chunk));
        assert!(reached(EvidenceStatus::Chunked, IngestStage::Embed));
        // ...but not to a stage it has no inputs for yet.
        assert!(!reached(EvidenceStatus::Normalized, IngestStage::Embed));
        assert!(!reached(EvidenceStatus::Raw, IngestStage::Chunk));
    }

    #[test]
    fn a_failed_item_is_always_eligible() {
        for stage in IngestStage::ALL {
            assert!(reached(EvidenceStatus::Failed, stage));
        }
    }

    #[test]
    fn truncation_is_visible_in_the_plan() {
        let plan = ReprocessPlan {
            requeued: vec![(EvidenceId::from_bytes([1; 16]), 1)],
            skipped_not_reached: 0,
            item_count_max: 1,
        };
        assert!(plan.is_truncated());
        let roomy = ReprocessPlan {
            item_count_max: 10,
            ..plan
        };
        assert!(!roomy.is_truncated());
    }
}
