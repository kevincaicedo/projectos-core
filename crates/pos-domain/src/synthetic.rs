//! The deterministic synthetic-event generator (m0-s05, shared with
//! `pos-bench` in m0-s16): a seeded, dependency-free stream of *valid* v0
//! domain facts, so `verify`/`export` are testable at 100k–1M events and
//! bench corpora are reproducible byte-for-byte from `(seed, count)` alone.
//! No RNG dependency: xorshift64* in ten lines is auditable and stable.

use crate::events::{
    AccountAuditedBody, DomainEvent, JobCompletedBody, JobEnqueuedBody, ProjectRenamedBody,
    RunFinishedBody, RunOutcome, RunStartedBody, RunStepCommittedBody,
};
use pos_foundation::{AccountId, DeviceId, JobId, ProjectId, RunId, UserId};
use pos_log::{Actor, AppendRequest, LogError};

/// Open runs the generator keeps in flight; more would model concurrency v0
/// does not have.
const OPEN_RUN_COUNT_MAX: usize = 4;
/// Queued jobs kept in flight.
const QUEUED_JOB_COUNT_MAX: usize = 8;
/// Devices the stream interleaves — exercises per-device lamport chains.
const DEVICE_COUNT: u8 = 3;

/// Deterministic generator state. `Iterator` yields append-ready requests;
/// identical `(seed, project_id)` yields identical streams forever (a bench
/// corpus is a claim, so it must be reproducible — master plan §24 spirit).
pub struct SyntheticEvents {
    rng_state: u64,
    project_id: ProjectId,
    next_entity: u64,
    open_runs: Vec<(RunId, u32)>,
    queued_jobs: Vec<JobId>,
}

impl SyntheticEvents {
    #[must_use]
    pub fn new(seed: u64, project_id: ProjectId) -> Self {
        Self {
            // xorshift needs a non-zero state; folding in a constant keeps
            // seed 0 valid for callers.
            rng_state: seed ^ 0x9e37_79b9_7f4a_7c15,
            project_id,
            next_entity: 1,
            open_runs: Vec::new(),
            queued_jobs: Vec::new(),
        }
    }

    /// xorshift64*: tiny, seedable, stable across platforms and releases.
    fn next_u64(&mut self) -> u64 {
        let mut state = self.rng_state;
        state ^= state >> 12;
        state ^= state << 25;
        state ^= state >> 27;
        self.rng_state = state;
        state.wrapping_mul(0x2545_f491_4f6c_dd1d)
    }

    fn mint_id(&mut self, discriminator: u8) -> [u8; 16] {
        let ordinal = self.next_entity;
        self.next_entity += 1;
        let mut bytes = [0_u8; 16];
        bytes[0] = discriminator;
        bytes[8..].copy_from_slice(&ordinal.to_be_bytes());
        bytes
    }

    fn device(&mut self) -> DeviceId {
        let index = u8::try_from(self.next_u64() % u64::from(DEVICE_COUNT)).unwrap_or(0);
        DeviceId::from_bytes([index + 1; 16])
    }

    fn actor(&mut self) -> Actor {
        match self.next_u64() % 3 {
            0 => Actor::User(UserId::from_bytes([0xa1; 16])),
            1 => Actor::Agent(RunId::from_bytes([0xa2; 16])),
            _ => Actor::System(JobId::from_bytes([0xa3; 16])),
        }
    }

    fn next_domain_event(&mut self) -> DomainEvent {
        // Weighted mix tuned for realistic shape: steps dominate (they do in
        // real projects), lifecycle events bracket them, audit trickles.
        let roll = self.next_u64() % 100;
        if roll < 45 && !self.open_runs.is_empty() {
            let index = usize::try_from(self.next_u64()).unwrap_or(0) % self.open_runs.len();
            let (run_id, steps) = &mut self.open_runs[index];
            let step_index = *steps;
            *steps += 1;
            let run_id = *run_id;
            return DomainEvent::RunStepCommitted(RunStepCommittedBody::V1 {
                run_id,
                step_index,
                summary: format!("synthetic step {step_index}"),
            });
        }
        if roll < 55 && self.open_runs.len() < OPEN_RUN_COUNT_MAX {
            let run_id = RunId::from_bytes(self.mint_id(2));
            self.open_runs.push((run_id, 0));
            return DomainEvent::RunStarted(RunStartedBody::V1 {
                run_id,
                worker: "synthetic".to_owned(),
                trigger: "generator".to_owned(),
            });
        }
        if roll < 65 && !self.open_runs.is_empty() {
            let index = usize::try_from(self.next_u64()).unwrap_or(0) % self.open_runs.len();
            let (run_id, steps) = self.open_runs.swap_remove(index);
            let outcome = match self.next_u64() % 10 {
                0 => RunOutcome::Canceled,
                1 => RunOutcome::Failed,
                _ => RunOutcome::Completed,
            };
            return DomainEvent::RunFinished(RunFinishedBody::V1 {
                run_id,
                outcome,
                steps_total: steps,
            });
        }
        if roll < 78 && self.queued_jobs.len() < QUEUED_JOB_COUNT_MAX {
            let job_id = JobId::from_bytes(self.mint_id(3));
            self.queued_jobs.push(job_id);
            return DomainEvent::JobEnqueued(JobEnqueuedBody::V1 {
                job_id,
                job_kind: "synthetic.tick".to_owned(),
            });
        }
        if roll < 88 && !self.queued_jobs.is_empty() {
            let index = usize::try_from(self.next_u64()).unwrap_or(0) % self.queued_jobs.len();
            let job_id = self.queued_jobs.swap_remove(index);
            let attempts = u32::try_from(self.next_u64() % 3).unwrap_or(0) + 1;
            return DomainEvent::JobCompleted(JobCompletedBody::V1 { job_id, attempts });
        }
        if roll < 94 {
            let ordinal = self.next_entity;
            return DomainEvent::ProjectRenamed(ProjectRenamedBody::V1 {
                project_id: self.project_id,
                name: format!("Synthetic Project {ordinal}"),
            });
        }
        DomainEvent::AccountAudited(AccountAuditedBody::V1 {
            account_id: AccountId::from_bytes([0xac; 16]),
            action: "synthetic.audited".to_owned(),
            target: "project".to_owned(),
        })
    }

    /// The next append request. Infallible in practice; typed because
    /// `into_request` validates the tag boundary like any other caller.
    pub fn next_request(&mut self) -> Result<AppendRequest, LogError> {
        let device = self.device();
        let actor = self.actor();
        self.next_domain_event().into_request(device, actor)
    }
}

#[cfg(test)]
mod tests {
    use super::SyntheticEvents;
    use pos_foundation::ProjectId;

    #[test]
    fn identical_seeds_yield_identical_streams() {
        let project = ProjectId::from_bytes([5; 16]);
        let mut left = SyntheticEvents::new(42, project);
        let mut right = SyntheticEvents::new(42, project);
        for _ in 0..500 {
            let a = left.next_request().expect("request");
            let b = right.next_request().expect("request");
            assert_eq!(a.kind, b.kind);
            assert_eq!(a.body, b.body);
            assert_eq!(a.device, b.device);
        }
    }

    #[test]
    fn different_seeds_diverge() {
        let project = ProjectId::from_bytes([5; 16]);
        let mut left = SyntheticEvents::new(1, project);
        let mut right = SyntheticEvents::new(2, project);
        let mut divergences = 0;
        for _ in 0..100 {
            let a = left.next_request().expect("request");
            let b = right.next_request().expect("request");
            if a.body != b.body || a.kind != b.kind {
                divergences += 1;
            }
        }
        assert!(divergences > 10, "streams barely diverged: {divergences}");
    }
}
