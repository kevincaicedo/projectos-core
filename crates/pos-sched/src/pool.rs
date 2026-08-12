//! The worker pool (m0-s14): weighted tokio task classes over a
//! round-robin-fair set of projects, plus the lease reaper and cron tick that
//! keep the queue honest while it runs.
//!
//! ## Shape
//!
//! The core is synchronous — SQLite writes and job handlers both block — so
//! tokio's role here is *supervision*, not I/O: one task per worker slot, all
//! real work on `spawn_blocking`. That keeps the async surface confined to
//! this module and leaves handlers as plain functions M1's pipeline stages
//! can be written as.
//!
//! ## Fairness and its stated bound
//!
//! Each class shares one round-robin cursor over the registered projects,
//! advanced on **every** claim attempt rather than on every success. So with
//! `P` registered projects, a project with queued work is visited at least
//! once every `P` claim attempts, independently of how deep any other
//! project's backlog is. Backlog depth cannot buy claim order — that is the
//! whole fairness property, and `fairness.rs` measures it.

use crate::SchedError;
use crate::cron::CronDriver;
use crate::job::{ClaimedJob, JobFailure, JobKind, WorkerName};
use crate::metrics::SchedulerMetrics;
use crate::queue::JobQueue;
use pos_domain::JobClass;
use pos_foundation::telemetry::{
    Parent, Span, SpanContext, SpanDetail, SpanField, SpanName, SpanValue,
};
use pos_foundation::{ProjectId, WallClock};
use pos_log::ProjectLog;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::Notify;
use tokio::task::JoinHandle;

/// Handlers one process may register. A kind is a routing key; needing more
/// than this many is a sign the routing key is carrying data.
pub const HANDLER_REGISTRY_COUNT_MAX: usize = 64;

/// Projects one pool serves, matching the API session bound so the two
/// registries cannot disagree about how many projects a process holds.
pub const PROJECT_REGISTRY_COUNT_MAX: usize = 64;

/// Defaults: interactive work outranks ingest, which outranks maintenance.
/// These are slot counts, not shares — a class can never be starved by a
/// busier one because it owns its own tasks.
#[must_use]
pub fn class_defaults() -> [ClassConfig; 3] {
    [
        ClassConfig {
            class: JobClass::Foreground,
            worker_count_max: 4,
        },
        ClassConfig {
            class: JobClass::Ingest,
            worker_count_max: 2,
        },
        ClassConfig {
            class: JobClass::Maintenance,
            worker_count_max: 1,
        },
    ]
}

#[derive(Clone, Copy, Debug)]
pub struct ClassConfig {
    pub class: JobClass,
    pub worker_count_max: u16,
}

#[derive(Clone, Copy, Debug)]
pub struct WorkerPoolConfig {
    pub classes: [ClassConfig; 3],
    /// How long a worker sleeps when every project is empty. The enqueue
    /// wake-up makes this a backstop, not the normal latency path.
    pub idle_poll_interval_ms: u64,
    pub heartbeat_interval_ms: u64,
    pub reap_interval_ms: u64,
    pub cron_tick_interval_ms: u64,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            classes: class_defaults(),
            idle_poll_interval_ms: 250,
            // Comfortably inside the default 30 s lease so a slow-but-alive
            // job is never declared dead by one missed beat.
            heartbeat_interval_ms: 5_000,
            reap_interval_ms: 5_000,
            cron_tick_interval_ms: 30_000,
        }
    }
}

/// A unit of work the pool knows how to run. Handlers are **contractually
/// idempotent**: delivery is at-least-once, so seeing the same job twice
/// (after a crash, a lease expiry, or a retry) must be safe.
pub trait JobHandler: Send + Sync {
    fn kind(&self) -> &JobKind;
    fn run(&self, job: &ClaimedJob) -> Result<(), JobFailure>;
}

#[derive(Debug, Eq, PartialEq)]
pub enum HandlerRegistryError {
    Duplicate { kind: String },
    Full { count_max: usize },
}

impl fmt::Display for HandlerRegistryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Duplicate { kind } => write!(formatter, "job kind {kind} is already registered"),
            Self::Full { count_max } => {
                write!(formatter, "handler registry is full at {count_max} kinds")
            }
        }
    }
}

impl std::error::Error for HandlerRegistryError {}

#[derive(Default)]
pub struct HandlerRegistry {
    handlers: BTreeMap<String, Arc<dyn JobHandler>>,
}

impl HandlerRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register(&mut self, handler: Arc<dyn JobHandler>) -> Result<(), HandlerRegistryError> {
        let kind = handler.kind().as_str().to_owned();
        if self.handlers.contains_key(&kind) {
            return Err(HandlerRegistryError::Duplicate { kind });
        }
        if self.handlers.len() >= HANDLER_REGISTRY_COUNT_MAX {
            return Err(HandlerRegistryError::Full {
                count_max: HANDLER_REGISTRY_COUNT_MAX,
            });
        }
        self.handlers.insert(kind, handler);
        Ok(())
    }

    #[must_use]
    pub fn get(&self, kind: &str) -> Option<Arc<dyn JobHandler>> {
        self.handlers.get(kind).cloned()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.handlers.len()
    }
}

/// The projects a pool serves. Ordered by project id so the fairness cursor
/// means the same thing across snapshots even as projects open and close.
#[derive(Default)]
pub struct ProjectRegistry {
    projects: Mutex<BTreeMap<[u8; 16], Arc<ProjectLog>>>,
}

impl ProjectRegistry {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Registers a project and creates its lease table. Re-registering the
    /// same project replaces the handle, so a reopened project is one row.
    pub fn register(
        &self,
        queue: &JobQueue,
        project_id: ProjectId,
        log: Arc<ProjectLog>,
    ) -> Result<(), SchedError> {
        queue.ensure_schema(&log)?;
        let mut projects = lock_recovering(&self.projects);
        let key = project_id.into_bytes();
        if projects.len() >= PROJECT_REGISTRY_COUNT_MAX && !projects.contains_key(&key) {
            return Err(SchedError::InvalidSpec {
                field: "project",
                reason: format!(
                    "the scheduler already serves {PROJECT_REGISTRY_COUNT_MAX} projects"
                ),
            });
        }
        projects.insert(key, log);
        Ok(())
    }

    pub fn unregister(&self, project_id: ProjectId) {
        lock_recovering(&self.projects).remove(&project_id.into_bytes());
    }

    #[must_use]
    pub fn snapshot(&self) -> Vec<(ProjectId, Arc<ProjectLog>)> {
        lock_recovering(&self.projects)
            .iter()
            .map(|(key, log)| (ProjectId::from_bytes(*key), Arc::clone(log)))
            .collect()
    }

    #[must_use]
    pub fn count(&self) -> usize {
        lock_recovering(&self.projects).len()
    }
}

/// Everything the tasks share. One `Arc` rather than eight cloned fields.
struct PoolContext {
    queue: Arc<JobQueue>,
    cron: CronDriver,
    handlers: Arc<HandlerRegistry>,
    projects: Arc<ProjectRegistry>,
    clock: Arc<dyn WallClock>,
    metrics: Arc<SchedulerMetrics>,
    config: WorkerPoolConfig,
    shutdown: AtomicBool,
    wake: Notify,
    /// The most recent scheduler-level failure. The pool must not die because
    /// one project's database is unhappy, but a swallowed error would be the
    /// invisible degradation L8 forbids — so it is kept and readable.
    last_error: Mutex<Option<String>>,
}

impl PoolContext {
    fn note_error(&self, operation: &'static str, error: &SchedError) {
        self.metrics.record_queue_error();
        if let Ok(mut slot) = self.last_error.lock() {
            *slot = Some(format!("{operation}: {error}"));
        }
    }
}

pub struct WorkerPool;

impl WorkerPool {
    /// Spawns the class workers, the lease reaper, and the cron tick on
    /// `runtime`. Nothing runs until this is called: a pool that started
    /// itself on construction would make "is background work running?" a
    /// question with no honest answer.
    #[must_use]
    pub fn start(
        runtime: &tokio::runtime::Handle,
        queue: Arc<JobQueue>,
        handlers: Arc<HandlerRegistry>,
        projects: Arc<ProjectRegistry>,
        clock: Arc<dyn WallClock>,
        config: WorkerPoolConfig,
    ) -> WorkerPoolHandle {
        let metrics = Arc::clone(queue.metrics());
        let context = Arc::new(PoolContext {
            cron: CronDriver::new(Arc::clone(&queue)),
            queue,
            handlers,
            projects,
            clock,
            metrics,
            config,
            shutdown: AtomicBool::new(false),
            wake: Notify::new(),
            last_error: Mutex::new(None),
        });
        let mut tasks = Vec::new();
        for class_config in config.classes {
            let cursor = Arc::new(AtomicUsize::new(0));
            for slot in 0..class_config.worker_count_max {
                let context = Arc::clone(&context);
                let cursor = Arc::clone(&cursor);
                let name = format!("{}-{slot}", class_config.class.as_str());
                tasks.push(runtime.spawn(async move {
                    let Ok(worker) = WorkerName::new(name) else {
                        return;
                    };
                    class_worker(context, class_config.class, worker, cursor).await;
                }));
            }
        }
        tasks.push(runtime.spawn(maintenance_loop(Arc::clone(&context))));
        WorkerPoolHandle { context, tasks }
    }
}

pub struct WorkerPoolHandle {
    context: Arc<PoolContext>,
    tasks: Vec<JoinHandle<()>>,
}

impl WorkerPoolHandle {
    /// Wakes idle workers immediately — call after enqueueing so the queue's
    /// latency is the claim, not the poll interval.
    pub fn wake(&self) {
        self.context.wake.notify_waiters();
    }

    /// The most recent scheduler error, if any.
    #[must_use]
    pub fn last_error(&self) -> Option<String> {
        self.context
            .last_error
            .lock()
            .ok()
            .and_then(|slot| slot.clone())
    }

    #[must_use]
    pub fn metrics(&self) -> &Arc<SchedulerMetrics> {
        &self.context.metrics
    }

    /// Stops accepting claims and waits for in-flight jobs to finish. A job
    /// mid-handler is allowed to complete: killing it would leave a lease to
    /// expire and an attempt to burn for no reason.
    pub async fn shutdown(self) {
        self.context.shutdown.store(true, Ordering::Release);
        self.context.wake.notify_waiters();
        for task in self.tasks {
            let _ = task.await;
        }
    }
}

async fn class_worker(
    context: Arc<PoolContext>,
    class: JobClass,
    worker: WorkerName,
    cursor: Arc<AtomicUsize>,
) {
    while !context.shutdown.load(Ordering::Acquire) {
        match claim_round_robin(&context, class, &worker, &cursor).await {
            Some((log, job)) => run_job(&context, log, job).await,
            None => {
                let idle = Duration::from_millis(context.config.idle_poll_interval_ms);
                tokio::select! {
                    () = context.wake.notified() => {}
                    () = tokio::time::sleep(idle) => {}
                }
            }
        }
    }
}

/// One pass over the registered projects, starting where the class cursor
/// points. The cursor advances per attempt, which is what makes the visit
/// order independent of any project's backlog depth.
async fn claim_round_robin(
    context: &Arc<PoolContext>,
    class: JobClass,
    worker: &WorkerName,
    cursor: &Arc<AtomicUsize>,
) -> Option<(Arc<ProjectLog>, ClaimedJob)> {
    let projects = context.projects.snapshot();
    if projects.is_empty() {
        return None;
    }
    let start = cursor.fetch_add(1, Ordering::Relaxed);
    let context = Arc::clone(context);
    let worker = worker.clone();
    tokio::task::spawn_blocking(move || {
        for offset in 0..projects.len() {
            let index = start.wrapping_add(offset) % projects.len();
            let (project_id, log) = &projects[index];
            match context
                .queue
                .claim(log, *project_id, class, &worker, context.clock.as_ref())
            {
                Ok(Some(job)) => return Some((Arc::clone(log), job)),
                Ok(None) => {}
                Err(error) => context.note_error("claim", &error),
            }
        }
        None
    })
    .await
    .ok()
    .flatten()
}

async fn run_job(context: &Arc<PoolContext>, log: Arc<ProjectLog>, job: ClaimedJob) {
    // `sched.job/:kind` (m0-s15), rooted on the *derived* job context so every
    // attempt — including one retried in another process after a lease expiry
    // — lands in the trace the first attempt started. Detached because this
    // span crosses `await` points and may close on another worker thread.
    // This is the one call site the discipline checker admits for
    // `SpanDetail::from_registered_kind`.
    let span = Span::open_detached(
        SpanName::SchedJob,
        SpanDetail::from_registered_kind(job.kind.as_str()),
        Parent::Root(SpanContext::for_job(job.project_id, job.job_id)),
    );
    span.set(
        SpanField::Project,
        SpanValue::Id(job.project_id.into_bytes()),
    );
    span.set(SpanField::Job, SpanValue::Id(job.job_id.into_bytes()));
    span.set(
        SpanField::Attempt,
        SpanValue::Count(u64::from(job.attempt_index)),
    );
    let Some(handler) = context.handlers.get(job.kind.as_str()) else {
        // A queued job with no handler is a deployment mistake, not weather:
        // permanent, so it lands in the DLQ with a reason instead of
        // ping-ponging through the retry budget forever.
        let failure = JobFailure::permanent(
            "no_handler",
            format!("no handler is registered for job kind {}", job.kind),
        );
        finish_job(context, log, job, Err(failure), 0).await;
        span.finish("no_handler");
        return;
    };
    let started_ts_ms = context.clock.now_ms();
    let heartbeat = spawn_heartbeat(context, Arc::clone(&log), job.clone());
    let run_job = job.clone();
    let outcome = tokio::task::spawn_blocking(move || handler.run(&run_job)).await;
    heartbeat.abort();
    let wall_ms = context.clock.now_ms().saturating_sub(started_ts_ms);
    let outcome = outcome.unwrap_or_else(|_| {
        // A panicking handler must not take the shell down with it. It is
        // recorded as a failed attempt so the retry budget — and eventually
        // the DLQ — makes the bug visible instead of silent.
        Err(JobFailure::retriable(
            "handler_panicked",
            "the handler panicked; the attempt was recorded and will retry",
        ))
    });
    span.set(SpanField::DurationMs, SpanValue::Millis(wall_ms));
    // The outcome label is a closed set; the handler's own failure code is a
    // runtime string, and it is already durable in the `JobAttemptFailed`
    // fact. The span carries correlation, the log carries content.
    let label = match &outcome {
        Ok(()) => "ok",
        Err(failure) if failure.permanent => "failed_permanent",
        Err(_) => "failed",
    };
    finish_job(context, log, job, outcome, wall_ms).await;
    span.finish(label);
}

fn spawn_heartbeat(
    context: &Arc<PoolContext>,
    log: Arc<ProjectLog>,
    job: ClaimedJob,
) -> JoinHandle<()> {
    let context = Arc::clone(context);
    tokio::spawn(async move {
        let interval = Duration::from_millis(context.config.heartbeat_interval_ms);
        loop {
            tokio::time::sleep(interval).await;
            let context_inner = Arc::clone(&context);
            let log = Arc::clone(&log);
            let job = job.clone();
            let alive = tokio::task::spawn_blocking(move || {
                context_inner
                    .queue
                    .heartbeat(&log, &job, context_inner.clock.as_ref())
            })
            .await;
            match alive {
                Ok(Ok(true)) => {}
                // A lost lease means the reaper already declared this attempt
                // dead; there is nothing left to extend.
                Ok(Ok(false)) | Err(_) => break,
                Ok(Err(error)) => {
                    context.note_error("heartbeat", &error);
                    break;
                }
            }
        }
    })
}

async fn finish_job(
    context: &Arc<PoolContext>,
    log: Arc<ProjectLog>,
    job: ClaimedJob,
    outcome: Result<(), JobFailure>,
    wall_ms: u64,
) {
    let inner = Arc::clone(context);
    let finished = tokio::task::spawn_blocking(move || match outcome {
        Ok(()) => inner
            .queue
            .complete(&log, &job, wall_ms, inner.clock.as_ref()),
        Err(failure) => inner
            .queue
            .fail(&log, &job, &failure, wall_ms, inner.clock.as_ref())
            .map(|_| ()),
    })
    .await;
    // A failed terminal write is recorded, not swallowed: the lease then
    // expires and the reaper re-derives the state from durable facts, which
    // is the same recovery path a process kill takes.
    if let Ok(Err(error)) = finished {
        context.note_error("finish job", &error);
    }
}

/// The lease reaper and the cron tick share one task: both are periodic
/// sweeps over every registered project, and neither is hot enough to earn
/// its own worker.
async fn maintenance_loop(context: Arc<PoolContext>) {
    let reap = Duration::from_millis(context.config.reap_interval_ms);
    let cron = Duration::from_millis(context.config.cron_tick_interval_ms);
    let tick = reap.min(cron).max(Duration::from_millis(1));
    let mut since_cron = Duration::ZERO;
    while !context.shutdown.load(Ordering::Acquire) {
        tokio::select! {
            () = context.wake.notified() => {}
            () = tokio::time::sleep(tick) => {}
        }
        if context.shutdown.load(Ordering::Acquire) {
            break;
        }
        since_cron += tick;
        let run_cron = since_cron >= cron;
        if run_cron {
            since_cron = Duration::ZERO;
        }
        let context_inner = Arc::clone(&context);
        let swept = tokio::task::spawn_blocking(move || {
            sweep_projects(&context_inner, run_cron);
        })
        .await;
        if swept.is_err() {
            break;
        }
    }
}

fn sweep_projects(context: &Arc<PoolContext>, run_cron: bool) {
    for (project_id, log) in context.projects.snapshot() {
        if let Err(error) = context
            .queue
            .reap_expired_leases(&log, context.clock.as_ref())
        {
            context.note_error("reap", &error);
        }
        if run_cron && let Err(error) = context.cron.tick(&log, project_id, context.clock.as_ref())
        {
            context.note_error("cron tick", &error);
        }
    }
}

/// A poisoned registry lock means a caller panicked mid-insert; the map is
/// structurally valid either way, and refusing every later claim would turn
/// one bug into a dead scheduler.
fn lock_recovering<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
