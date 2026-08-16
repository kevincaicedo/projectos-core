//! Background workers: the process-owned half of the m0-s14 scheduler
//! (m1-s01 amendment, [ADR-0007](../../../docs/adr/0007-background-worker-lifecycle.md)).
//!
//! m0-s14 built the worker pool and m1-s01 built the stages that run on it,
//! and until this module **nothing started one**: `ingest.reprocess` appended
//! its facts, committed the next stage's `JobEnqueued`, and the job sat in the
//! projection because every test suite claimed and ran the queue itself. Two
//! well-tested halves with nothing covering the join. This module is the join.
//!
//! ## Where the pool runs
//!
//! One dedicated OS thread owns a **current-thread** tokio runtime, and the
//! pool runs on it. Two reasons, both structural:
//!
//! 1. The pool's async surface is supervision only — every claim, handler, and
//!    terminal write already goes through `spawn_blocking` — so a single
//!    driver thread plus tokio's blocking pool is the whole requirement. A
//!    multi-threaded work-stealing runtime would buy nothing here.
//! 2. Dropping a `tokio::runtime::Runtime` inside an async context panics, and
//!    `pos-server` builds one `LocalRuntime` per account *inside* an axum
//!    handler. Keeping the runtime on a thread this module owns means no shell
//!    can drop one from the wrong place.
//!
//! ## Lifecycle
//!
//! Start is explicit and per-shell ([`crate::LocalRuntime::start_background_workers`]):
//! a pool that started itself would make "is background work running?" a
//! question with no honest answer, which is also why [`WorkerStatusReport`]
//! rides on `health`. `project.open` registers a project with the pool and
//! `project.close` unregisters it, so the set of projects a pool serves is
//! exactly the set this process has open. Shutdown stops accepting claims and
//! waits, bounded, for in-flight jobs; queued work stays durable in the log
//! and resumes the next time any process opens the project (L1/L4).

use crate::ApiError;
use pos_foundation::{DeviceId, ProjectId, SystemWallClock, WallClock};
use pos_ingest::{IngestPipeline, PipelineConfig, stage_job_handlers, stage_registry_default};
use pos_log::ProjectLog;
use pos_sched::{
    HandlerRegistry, JobQueue, ProjectRegistry, WorkerPool, WorkerPoolConfig, WorkerPoolHandle,
};
use serde::Serialize;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use ts_rs::TS;

/// How long [`BackgroundWorkers::shutdown`] waits for in-flight jobs before
/// giving up and leaving the thread detached. A job that outlives this is not
/// lost: nothing terminal was written, its lease expires, and the reaper turns
/// that into exactly one counted attempt whenever a process next serves the
/// project (m0-s14 I3).
pub const WORKER_SHUTDOWN_MS_MAX_DEFAULT: u64 = 5_000;

/// The default budget for [`BackgroundWorkers::drain`] — the CLI's
/// "run the work I just queued, then exit" path. Generous because the caller
/// asked for the work to happen; bounded because a one-shot invocation must
/// terminate whatever the corpus does.
pub const WORKER_DRAIN_MS_MAX_DEFAULT: u64 = 120_000;

/// How long `start` waits for the worker thread to report its pool live. This
/// is thread spawn plus a runtime build; a machine that cannot do that in five
/// seconds has a problem the caller needs to hear about.
const WORKER_START_MS_MAX: u64 = 5_000;

/// Poll cadence for the two bounded waits. Drain reads one projection row per
/// registered project per tick, so it is deliberately slower than the shutdown
/// poll, which reads an in-process flag.
const DRAIN_POLL_MS: u64 = 100;
const SHUTDOWN_POLL_MS: u64 = 10;

/// What a shell chooses about its background workers. Everything else — class
/// weights, lease TTL, backoff — belongs to `pos-sched` and is the same in
/// every shell by construction (L12).
#[derive(Clone, Copy, Debug)]
pub struct WorkerConfig {
    /// The bounded wait at shutdown, before the shell stops waiting and says so.
    pub shutdown_ms_max: u64,
    /// How long an idle worker sleeps when every project is empty. The enqueue
    /// wake-up makes this a backstop rather than the normal latency path.
    pub idle_poll_interval_ms: u64,
}

impl Default for WorkerConfig {
    fn default() -> Self {
        Self {
            shutdown_ms_max: WORKER_SHUTDOWN_MS_MAX_DEFAULT,
            idle_poll_interval_ms: WorkerPoolConfig::default().idle_poll_interval_ms,
        }
    }
}

/// Whether this process claims queued jobs, and what it has to say about it.
/// Rendered on `health` so "the pipeline is stuck" is answerable from the
/// product instead of from a log file.
#[derive(Clone, Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct WorkerStatusReport {
    /// False means enqueued work stays queued in this process. It is never
    /// inferred from configuration — it is read from the live pool.
    pub running: bool,
    pub registered_project_count: u32,
    /// The most recent scheduler-level failure, kept rather than swallowed: a
    /// pool that survives a sick database must still say that it did (L8).
    pub last_error: Option<String>,
}

impl WorkerStatusReport {
    /// The honest answer for a process with no pool.
    #[must_use]
    pub const fn stopped() -> Self {
        Self {
            running: false,
            registered_project_count: 0,
            last_error: None,
        }
    }
}

/// What one bounded drain observed. `quiescent` is the only field a caller
/// should branch on; the rest exists so a shell can say *why* it is not.
#[derive(Clone, Debug)]
pub struct WorkerDrainReport {
    /// Every registered project's queue is empty — nothing queued, nothing
    /// leased, nothing waiting on a retry instant.
    pub quiescent: bool,
    pub queued_remaining: u64,
    pub dead_total: u64,
    pub waited_ms: u64,
    /// A queue read that failed during the drain. Reported rather than
    /// silently making the wait look successful.
    pub last_read_error: Option<String>,
}

/// The pool, its thread, and the registries they share.
pub struct BackgroundWorkers {
    queue: Arc<JobQueue>,
    projects: Arc<ProjectRegistry>,
    pool: Arc<Mutex<Option<WorkerPoolHandle>>>,
    stop: Arc<tokio::sync::Notify>,
    finished: Arc<AtomicBool>,
    join: Mutex<Option<std::thread::JoinHandle<()>>>,
    config: WorkerConfig,
}

impl BackgroundWorkers {
    /// Starts the pool on its own thread and returns once it is live.
    pub(crate) fn start(
        device: DeviceId,
        queue: Arc<JobQueue>,
        config: WorkerConfig,
    ) -> Result<Self, ApiError> {
        let projects = Arc::new(ProjectRegistry::new());
        let clock: Arc<dyn WallClock> = Arc::new(SystemWallClock);
        let pipeline = Arc::new(IngestPipeline::new(
            PipelineConfig::for_device(device),
            Arc::clone(&queue),
            stage_registry_default(),
        ));
        let handlers = Arc::new(handler_registry(&pipeline, &projects, &clock)?);
        let pool_config = WorkerPoolConfig {
            idle_poll_interval_ms: config.idle_poll_interval_ms,
            ..WorkerPoolConfig::default()
        };

        let pool: Arc<Mutex<Option<WorkerPoolHandle>>> = Arc::new(Mutex::new(None));
        let stop = Arc::new(tokio::sync::Notify::new());
        let finished = Arc::new(AtomicBool::new(false));
        let (ready_tx, ready_rx) = std::sync::mpsc::sync_channel::<Result<(), String>>(1);

        let thread_pool = Arc::clone(&pool);
        let thread_stop = Arc::clone(&stop);
        let thread_finished = Arc::clone(&finished);
        let thread_queue = Arc::clone(&queue);
        let thread_projects = Arc::clone(&projects);
        let join = std::thread::Builder::new()
            .name("pos-workers".to_owned())
            .spawn(move || {
                let built = tokio::runtime::Builder::new_current_thread()
                    .enable_time()
                    .build();
                let runtime = match built {
                    Ok(runtime) => runtime,
                    Err(error) => {
                        let _ = ready_tx.send(Err(format!("build the worker runtime: {error}")));
                        thread_finished.store(true, Ordering::Release);
                        return;
                    }
                };
                runtime.block_on(async move {
                    let handle = WorkerPool::start(
                        &tokio::runtime::Handle::current(),
                        thread_queue,
                        handlers,
                        thread_projects,
                        clock,
                        pool_config,
                    );
                    *lock_recovering(&thread_pool) = Some(handle);
                    let _ = ready_tx.send(Ok(()));
                    thread_stop.notified().await;
                    // Taken out of the slot so `status` reports a pool that is
                    // shutting down as stopped rather than as live.
                    let handle = lock_recovering(&thread_pool).take();
                    if let Some(handle) = handle {
                        handle.shutdown().await;
                    }
                });
                thread_finished.store(true, Ordering::Release);
            })
            .map_err(|error| ApiError {
                code: "worker_start_failed",
                message: format!("start the background worker thread: {error}"),
                retriable: true,
            })?;

        match ready_rx.recv_timeout(Duration::from_millis(WORKER_START_MS_MAX)) {
            Ok(Ok(())) => {}
            Ok(Err(message)) => {
                let _ = join.join();
                return Err(ApiError {
                    code: "worker_start_failed",
                    message,
                    retriable: true,
                });
            }
            Err(error) => {
                stop.notify_one();
                return Err(ApiError {
                    code: "worker_start_failed",
                    message: format!(
                        "the background worker pool did not come up within \
                         {WORKER_START_MS_MAX}ms: {error}"
                    ),
                    retriable: true,
                });
            }
        }

        Ok(Self {
            queue,
            projects,
            pool,
            stop,
            finished,
            join: Mutex::new(Some(join)),
            config,
        })
    }

    /// Registers an open project so the pool can claim its jobs, and ensures
    /// the lease table exists. Re-registering a reopened project replaces the
    /// handle rather than adding a second one.
    pub(crate) fn register(
        &self,
        project_id: ProjectId,
        log: Arc<ProjectLog>,
    ) -> Result<(), ApiError> {
        self.projects
            .register(&self.queue, project_id, log)
            .map_err(|error| ApiError {
                code: "worker_register_failed",
                message: format!("register the project with the scheduler: {error}"),
                retriable: true,
            })?;
        self.wake();
        Ok(())
    }

    /// Stops serving a project. A job claimed just before this refuses with a
    /// retriable `project_not_open` rather than touching a closed handle.
    pub(crate) fn unregister(&self, project_id: ProjectId) {
        self.projects.unregister(project_id);
    }

    /// Wakes idle workers — called after an enqueue so queue latency is the
    /// claim, not the poll interval.
    pub(crate) fn wake(&self) {
        if let Some(pool) = lock_recovering(&self.pool).as_ref() {
            pool.wake();
        }
    }

    #[must_use]
    pub fn status(&self) -> WorkerStatusReport {
        let pool = lock_recovering(&self.pool);
        let running = pool.is_some();
        let last_error = pool.as_ref().and_then(WorkerPoolHandle::last_error);
        drop(pool);
        WorkerStatusReport {
            running,
            registered_project_count: u32::try_from(self.projects.count()).unwrap_or(u32::MAX), // INVARIANT: the registry is capped at PROJECT_REGISTRY_COUNT_MAX (64).
            last_error,
        }
    }

    /// Waits, bounded, until every registered project's queue is empty.
    ///
    /// "Empty" is the durable answer — `proj_jobs` holds no queued row — which
    /// covers a job that is running (queued + live lease) and one waiting on a
    /// retry instant. That is what makes this an honest quiescence check for a
    /// one-shot CLI invocation rather than a guess about timing.
    #[must_use]
    pub fn drain(&self, budget_ms: u64) -> WorkerDrainReport {
        let started = Instant::now();
        loop {
            let (queued, dead, last_read_error) = self.queue_totals();
            let waited_ms = elapsed_ms(started);
            if queued == 0 || waited_ms >= budget_ms {
                return WorkerDrainReport {
                    quiescent: queued == 0,
                    queued_remaining: queued,
                    dead_total: dead,
                    waited_ms,
                    last_read_error,
                };
            }
            self.wake();
            std::thread::sleep(Duration::from_millis(DRAIN_POLL_MS));
        }
    }

    /// Stops claiming and waits for in-flight jobs, up to the configured
    /// budget. Returns whether the pool finished inside it; a `false` is worth
    /// reporting, because it means a handler outlived its expected duration.
    /// Idempotent — a second call on a stopped pool returns true.
    pub fn shutdown(&self) -> bool {
        self.stop.notify_one();
        let deadline = Duration::from_millis(self.config.shutdown_ms_max);
        let started = Instant::now();
        while !self.finished.load(Ordering::Acquire) {
            if started.elapsed() >= deadline {
                return false;
            }
            std::thread::sleep(Duration::from_millis(SHUTDOWN_POLL_MS));
        }
        if let Some(join) = lock_recovering(&self.join).take() {
            let _ = join.join();
        }
        true
    }

    /// Queue depth across every registered project, plus the first read error
    /// if one occurred. Failing to read one project's queue must not abandon
    /// the wait on the others.
    fn queue_totals(&self) -> (u64, u64, Option<String>) {
        let mut queued: u64 = 0;
        let mut dead: u64 = 0;
        let mut last_read_error = None;
        for (_, log) in self.projects.snapshot() {
            match self.queue.depth(&log) {
                Ok(depth) => {
                    queued = queued.saturating_add(depth.queued);
                    dead = dead.saturating_add(depth.dead);
                }
                Err(error) => last_read_error = Some(error.to_string()),
            }
        }
        (queued, dead, last_read_error)
    }
}

impl Drop for BackgroundWorkers {
    /// Signals the thread and returns immediately. Drop must not block: an
    /// account runtime is dropped inside `pos-server`'s async context, and
    /// waiting there would stall the executor. A shell that wants to know the
    /// pool stopped calls [`BackgroundWorkers::shutdown`].
    fn drop(&mut self) {
        self.stop.notify_one();
    }
}

/// The stage handlers this build runs, as one registry.
fn handler_registry(
    pipeline: &Arc<IngestPipeline>,
    projects: &Arc<ProjectRegistry>,
    clock: &Arc<dyn WallClock>,
) -> Result<HandlerRegistry, ApiError> {
    let handlers = stage_job_handlers(pipeline, projects, clock).map_err(|error| ApiError {
        code: "worker_start_failed",
        message: format!("compose the ingestion stage handlers: {error}"),
        retriable: false,
    })?;
    let mut registry = HandlerRegistry::new();
    for handler in handlers {
        registry.register(handler).map_err(|error| ApiError {
            code: "worker_start_failed",
            message: format!("register a stage handler: {error}"),
            retriable: false,
        })?;
    }
    Ok(registry)
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX) // INVARIANT: saturation, matching the telemetry clock policy.
}

/// A poisoned lock means a caller panicked mid-update; both slots hold a
/// single value, so recovery is safe and refusing would convert one bug into
/// a scheduler that can never be woken or stopped.
fn lock_recovering<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
