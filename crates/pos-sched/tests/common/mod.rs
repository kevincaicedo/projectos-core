//! Shared fixture for the m0-s14 scheduler oracles: a real `.pos` project,
//! a real log, and a real queue — no in-memory stand-ins, because every
//! property these suites claim is a property of the durable path.

#![forbid(unsafe_code)]
// Cargo compiles this module once per integration-test binary, so a helper
// every suite shares still reads as dead in the suites that do not call it.
// One fixture the suites agree on beats four that drift.
#![allow(dead_code)]

use pos_domain::{DomainEvent, JobClass, ProjectCreatedBody, v0_registry};
use pos_foundation::{DeviceId, ManualWallClock, ProjectId, UserId};
use pos_log::{Actor, LogConfig, ProjectLog};
use pos_sched::{
    BackoffPolicy, ClaimedJob, JitterSource, JobFailure, JobHandler, JobKind, JobQueue, JobSpec,
    NoJitter, QueueConfig, SchedulerMetrics, WorkerName,
};
use pos_store::ProjectStore;
use std::path::Path;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::{Arc, Mutex};

pub const DEVICE: DeviceId = DeviceId::from_bytes([0x51; 16]);
pub const USER: UserId = UserId::from_bytes([0x52; 16]);

/// Creates (or reopens) a project directory and records its creation fact so
/// the project row exists exactly as a shell would leave it.
pub fn open_project(root: &Path, project_id: ProjectId, clock: &ManualWallClock) -> ProjectLog {
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
            project_id,
            name: "scheduler fixture".to_owned(),
            template: "generic".to_owned(),
        });
        let request = event
            .into_request(DEVICE, Actor::User(USER))
            .expect("project created request");
        log.append(request, clock).expect("append project created");
    }
    log
}

/// A queue with deterministic jitter: retry instants are then exact, so a
/// suite asserting them is asserting the policy rather than a lucky draw.
pub fn queue(lease_ttl_ms: u64) -> Arc<JobQueue> {
    queue_with_jitter(lease_ttl_ms, Arc::new(NoJitter))
}

pub fn queue_with_jitter(lease_ttl_ms: u64, jitter: Arc<dyn JitterSource>) -> Arc<JobQueue> {
    let config = QueueConfig {
        device: DEVICE,
        backoff: BackoffPolicy::default(),
        lease_ttl_ms,
    };
    Arc::new(JobQueue::new(
        config,
        jitter,
        Arc::new(SchedulerMetrics::default()),
    ))
}

pub fn kind(name: &str) -> JobKind {
    JobKind::new(name).expect("fixture job kind")
}

pub fn worker(name: &str) -> WorkerName {
    WorkerName::new(name).expect("fixture worker name")
}

pub fn spec(name: &str, key: &str) -> JobSpec {
    JobSpec::new(kind(name), key).with_class(JobClass::Foreground)
}

/// Counts how often a handler actually ran, keyed by job — the oracle for
/// "executes the handler exactly once".
#[derive(Default)]
pub struct RunLedger {
    runs: Mutex<Vec<String>>,
}

impl RunLedger {
    pub fn record(&self, job: &ClaimedJob) {
        self.runs
            .lock()
            .expect("ledger lock")
            .push(job.job_id.to_hex());
    }

    pub fn count_for(&self, job_id_hex: &str) -> usize {
        self.runs
            .lock()
            .expect("ledger lock")
            .iter()
            .filter(|entry| entry.as_str() == job_id_hex)
            .count()
    }

    pub fn total(&self) -> usize {
        self.runs.lock().expect("ledger lock").len()
    }
}

/// A handler that records every run and then reports the scripted outcome.
pub struct ScriptedHandler {
    kind: JobKind,
    ledger: Arc<RunLedger>,
    /// Attempts that fail before the handler starts succeeding.
    failures_before_success: AtomicU32,
    permanent: bool,
}

impl ScriptedHandler {
    pub fn always_ok(kind: JobKind, ledger: Arc<RunLedger>) -> Self {
        Self {
            kind,
            ledger,
            failures_before_success: AtomicU32::new(0),
            permanent: false,
        }
    }

    pub fn failing(kind: JobKind, ledger: Arc<RunLedger>, failures_before_success: u32) -> Self {
        Self {
            kind,
            ledger,
            failures_before_success: AtomicU32::new(failures_before_success),
            permanent: false,
        }
    }

    pub fn refusing(kind: JobKind, ledger: Arc<RunLedger>) -> Self {
        Self {
            kind,
            ledger,
            failures_before_success: AtomicU32::new(0),
            permanent: true,
        }
    }
}

impl JobHandler for ScriptedHandler {
    fn kind(&self) -> &JobKind {
        &self.kind
    }

    fn run(&self, job: &ClaimedJob) -> Result<(), JobFailure> {
        self.ledger.record(job);
        if self.permanent {
            return Err(JobFailure::permanent("refused", "scripted refusal"));
        }
        let remaining = self.failures_before_success.load(Ordering::SeqCst);
        if remaining > 0 {
            self.failures_before_success
                .store(remaining - 1, Ordering::SeqCst);
            return Err(JobFailure::retriable("scripted", "scripted failure"));
        }
        Ok(())
    }
}
