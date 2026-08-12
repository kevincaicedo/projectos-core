//! Job vocabulary: validated kind names, the enqueue spec, the derived job
//! id, and the live state a reader sees (m0-s14).
//!
//! Every bound here is stated as a named constant because a queue without
//! bounds is a memory leak with a schedule (L8).

use crate::SchedError;
use pos_domain::{JobClass, JobCronOrigin, JobPriority};
use pos_foundation::{EventSeq, JobId, ProjectId};
use pos_store::blake3;
use std::fmt;

/// A kind name is a routing key, not a payload. 64 bytes is generous for
/// `evidence.chunk` / `knowledge.reindex`-shaped names and keeps the claim
/// query's text compares cheap.
pub const JOB_KIND_LEN_MAX: usize = 64;

/// An idempotency key identifies a logical unit of work (`"cron:<id>:<tick>"`,
/// `"evidence:<blake3>"`). Longer than this is a payload in disguise.
pub const JOB_IDEMPOTENCY_KEY_LEN_MAX: usize = 128;

/// Payloads are references to work, never the work itself: bulk content lives
/// in the CAS behind a hash (L8, same rule the event body follows).
pub const JOB_PAYLOAD_LEN_MAX: usize = 16 * 1024;

/// Retries a single job may take before the DLQ. Each retry costs a durable
/// event and a backoff window, so an unbounded retry loop is both a log leak
/// and an invisible outage.
pub const JOB_RETRY_COUNT_MAX: u32 = 16;

/// Worker names appear in leases and metrics; they are identifiers, not prose.
pub const WORKER_NAME_LEN_MAX: usize = 64;

/// A validated job-kind name. Construction is the only validation point, so a
/// `JobKind` in hand is always routable.
#[derive(Clone, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JobKind(String);

#[derive(Debug, Eq, PartialEq)]
pub struct JobKindError {
    pub name: String,
}

impl fmt::Display for JobKindError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "invalid job kind {:?}: 1..={JOB_KIND_LEN_MAX} bytes of ASCII alphanumerics, '.', '-', or '_'",
            self.name
        )
    }
}

impl std::error::Error for JobKindError {}

impl JobKind {
    pub fn new(name: impl Into<String>) -> Result<Self, JobKindError> {
        let name = name.into();
        let valid = !name.is_empty()
            && name.len() <= JOB_KIND_LEN_MAX
            && name
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'));
        if valid {
            Ok(Self(name))
        } else {
            Err(JobKindError { name })
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for JobKind {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.0)
    }
}

/// A validated worker identity (`"foreground-0"`, `"reaper"`).
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct WorkerName(String);

impl WorkerName {
    pub fn new(name: impl Into<String>) -> Result<Self, SchedError> {
        let name = name.into();
        if name.is_empty() || name.len() > WORKER_NAME_LEN_MAX {
            return Err(SchedError::InvalidSpec {
                field: "worker",
                reason: format!(
                    "1..={WORKER_NAME_LEN_MAX} bytes required, got {}",
                    name.len()
                ),
            });
        }
        Ok(Self(name))
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// What a caller submits to `JobQueue::enqueue`.
#[derive(Clone, Debug)]
pub struct JobSpec {
    pub kind: JobKind,
    /// Scopes exactly-once. Callers derive it from the logical work — a cron
    /// tick, an evidence hash — so that "the same work" is a decidable
    /// question rather than a timing accident.
    pub idempotency_key: String,
    pub priority: JobPriority,
    pub class: JobClass,
    pub payload: Vec<u8>,
    /// Earliest claim instant. `0` means "as soon as a worker is free".
    pub run_at_ts_ms: u64,
    pub retry_count_max: u32,
    pub cron: Option<JobCronOrigin>,
}

impl JobSpec {
    /// A plain job: normal priority, maintenance class, runnable now.
    #[must_use]
    pub fn new(kind: JobKind, idempotency_key: impl Into<String>) -> Self {
        Self {
            kind,
            idempotency_key: idempotency_key.into(),
            priority: JobPriority::Normal,
            class: JobClass::Maintenance,
            payload: Vec::new(),
            run_at_ts_ms: 0,
            retry_count_max: 3,
            cron: None,
        }
    }

    #[must_use]
    pub fn with_class(mut self, class: JobClass) -> Self {
        self.class = class;
        self
    }

    #[must_use]
    pub fn with_priority(mut self, priority: JobPriority) -> Self {
        self.priority = priority;
        self
    }

    #[must_use]
    pub fn with_payload(mut self, payload: Vec<u8>) -> Self {
        self.payload = payload;
        self
    }

    #[must_use]
    pub fn with_run_at_ts_ms(mut self, run_at_ts_ms: u64) -> Self {
        self.run_at_ts_ms = run_at_ts_ms;
        self
    }

    #[must_use]
    pub fn with_retry_count_max(mut self, retry_count_max: u32) -> Self {
        self.retry_count_max = retry_count_max;
        self
    }

    /// Checks every stated bound before anything durable happens.
    pub(crate) fn validate(&self) -> Result<(), SchedError> {
        if self.idempotency_key.is_empty()
            || self.idempotency_key.len() > JOB_IDEMPOTENCY_KEY_LEN_MAX
        {
            return Err(SchedError::InvalidSpec {
                field: "idempotency_key",
                reason: format!(
                    "1..={JOB_IDEMPOTENCY_KEY_LEN_MAX} bytes required, got {}",
                    self.idempotency_key.len()
                ),
            });
        }
        if self.payload.len() > JOB_PAYLOAD_LEN_MAX {
            return Err(SchedError::InvalidSpec {
                field: "payload",
                reason: format!(
                    "{} bytes exceeds the {JOB_PAYLOAD_LEN_MAX}-byte bound; \
                     put bulk content in the blob store and carry its hash",
                    self.payload.len()
                ),
            });
        }
        if self.retry_count_max > JOB_RETRY_COUNT_MAX {
            return Err(SchedError::InvalidSpec {
                field: "retry_count_max",
                reason: format!(
                    "{} exceeds the {JOB_RETRY_COUNT_MAX}-retry bound",
                    self.retry_count_max
                ),
            });
        }
        Ok(())
    }
}

/// Domain separation for the derived id: a fixed prefix keeps job ids from
/// ever colliding with another BLAKE3-addressed identity in the product.
const JOB_ID_DOMAIN: &[u8] = b"projectos/job-id/v1";

/// The job id **is** the idempotency decision: same project, same kind, same
/// key ⇒ same id ⇒ the projection's primary key refuses the second insert.
/// Deriving rather than minting also keeps enqueue a pure function, so replay
/// and a re-run of the same caller agree byte for byte.
#[must_use]
pub fn derive_job_id(project_id: ProjectId, kind: &JobKind, idempotency_key: &str) -> JobId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(JOB_ID_DOMAIN);
    hasher.update(&project_id.into_bytes());
    // Length-prefixed so ("ab", "c") and ("a", "bc") cannot hash alike.
    hasher.update(
        &u32::try_from(kind.as_str().len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    hasher.update(kind.as_str().as_bytes());
    hasher.update(
        &u32::try_from(idempotency_key.len())
            .unwrap_or(u32::MAX)
            .to_be_bytes(),
    );
    hasher.update(idempotency_key.as_bytes());
    let digest = hasher.finalize();
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    JobId::from_bytes(id)
}

/// A job handed to a handler. Holding one proves a live lease was taken.
#[derive(Clone, Debug)]
pub struct ClaimedJob {
    pub job_id: JobId,
    pub project_id: ProjectId,
    pub kind: JobKind,
    pub payload: Vec<u8>,
    /// 1-based index of *this* attempt, already reserved in the lease.
    pub attempt_index: u32,
    pub retry_count_max: u32,
    pub enqueued_seq: EventSeq,
    pub worker: WorkerName,
    pub claimed_ts_ms: u64,
    /// How long the job waited between becoming eligible and being claimed —
    /// the claim-latency metric, measured where the truth is known.
    pub claim_latency_ms: u64,
}

/// What a handler reports when work does not succeed. `permanent` is the
/// handler's statement that retrying cannot change the outcome — it goes
/// straight to the DLQ with its reason rather than burning the retry budget.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct JobFailure {
    pub code: String,
    pub detail: String,
    pub permanent: bool,
}

impl JobFailure {
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

/// The frozen §3.2 read view: durable state joined with the lease.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum JobLiveState {
    Queued,
    Running,
    /// An attempt failed and the retry window has not opened yet.
    Failed,
    Done,
    Dead,
}

impl JobLiveState {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Failed => "failed",
            Self::Done => "done",
            Self::Dead => "dead",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{JobKind, JobSpec, derive_job_id};
    use pos_foundation::ProjectId;

    #[test]
    fn kind_names_are_validated_at_construction() {
        assert!(JobKind::new("evidence.chunk").is_ok());
        assert!(JobKind::new("").is_err());
        assert!(JobKind::new("has space").is_err());
        assert!(JobKind::new("x".repeat(super::JOB_KIND_LEN_MAX + 1)).is_err());
    }

    #[test]
    fn the_derived_id_separates_kind_from_key_and_project() {
        let project = ProjectId::from_bytes([7; 16]);
        let other_project = ProjectId::from_bytes([8; 16]);
        let kind = JobKind::new("ab").expect("valid");
        let short = JobKind::new("a").expect("valid");
        let same = derive_job_id(project, &kind, "c");
        assert_eq!(same, derive_job_id(project, &kind, "c"));
        // Length prefixes: ("ab","c") must not collide with ("a","bc").
        assert_ne!(same, derive_job_id(project, &short, "bc"));
        assert_ne!(same, derive_job_id(other_project, &kind, "c"));
    }

    #[test]
    fn bounds_are_refused_before_anything_durable_happens() {
        let kind = JobKind::new("noop").expect("valid");
        let oversize =
            JobSpec::new(kind.clone(), "key").with_payload(vec![0; super::JOB_PAYLOAD_LEN_MAX + 1]);
        assert!(oversize.validate().is_err());
        let too_many =
            JobSpec::new(kind.clone(), "key").with_retry_count_max(super::JOB_RETRY_COUNT_MAX + 1);
        assert!(too_many.validate().is_err());
        let empty_key = JobSpec::new(kind, "");
        assert!(empty_key.validate().is_err());
    }
}
