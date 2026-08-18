//! The join between the m0-s14 worker pool and the m1-s01 stage framework
//! (ADR-0007).
//!
//! Both halves were tested before this suite existed, and the product still
//! did nothing: every ingestion suite claimed and ran its own jobs, so
//! "the pipeline works" was proven about a loop no shell contained. The rule
//! this file follows is therefore literal — **nothing here claims a job**.
//! Work is submitted, and the only thing the test does afterwards is read the
//! registry's own answers until they change or a bounded wait expires.

#![forbid(unsafe_code)]

use pos_api::{
    CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, ProjectPathInput,
    QueryName, WorkerConfig, bootstrap_local_runtime, input_json,
};
use pos_domain::{EvidenceShape, ExternalRef, MediaKind, v0_registry};
use pos_ingest::{EvidenceSubmission, IngestPipeline, PipelineConfig, stage_registry_default};
use pos_log::{Actor, LogConfig, ProjectLog};
use pos_store::ProjectStore;
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

/// The bounded wait for an asynchronous assertion. Generous relative to the
/// work (two stages over a few kilobytes on an idle pool) so a slow machine
/// does not make this flaky, and finite so a broken wiring fails instead of
/// hanging a CI runner.
const OBSERVE_MS_MAX: u64 = 20_000;

/// How long the "nothing runs it" case watches before concluding that nothing
/// is going to. Short on purpose: it is asserting a *negative*, and the only
/// thing a longer wait buys is a slower suite.
const IDLE_OBSERVE_MS: u64 = 750;

const OBSERVE_POLL_MS: u64 = 25;

/// The fixture corpus: several markdown sections, which NORMALIZE turns into
/// text and CHUNK turns into more than one chunk.
fn corpus() -> Vec<u8> {
    let mut text = String::new();
    for section in 0..8 {
        text.push_str(&format!("# Section {section}\n\n"));
        for line in 0..12 {
            text.push_str(&format!(
                "Interview note {section}.{line}: the participant described their workflow.\n"
            ));
        }
        text.push('\n');
    }
    text.into_bytes()
}

/// Creates a project through the registry — the same bytes a user's `pos
/// create` writes — and returns its path.
fn create_project(runtime: &LocalRuntime, root: &Path, name: &str) -> String {
    let path = root.join(format!("{name}.pos")).display().to_string();
    let input = input_json(&ProjectCreateInput {
        path: path.clone(),
        name: Some(name.to_owned()),
        template: "generic".to_owned(),
    })
    .expect("input serializes");
    runtime
        .command(CommandName::ProjectCreate.as_str(), &input)
        .expect("project.create resolves");
    path
}

/// Puts one Evidence item into a project the way m1-s07's upload path will:
/// stream the bytes through RAW, which commits `EvidenceAdded` and the first
/// stage's `JobEnqueued` in one transaction.
///
/// This runs beside the runtime rather than through it because submission is
/// deliberately not on the API surface yet (m1-s07 owns it). What it proves is
/// unaffected: the job is a durable fact in the project either way, and who
/// claims it is exactly the question this suite asks.
fn submit_one(path: &str, content: &[u8]) {
    let store = ProjectStore::open(Path::new(path)).expect("open the project store");
    let project_id = store.manifest().project_id;
    let log = ProjectLog::open(
        store,
        v0_registry().expect("domain registry"),
        LogConfig::default(),
    )
    .expect("open the project log");
    let device = pos_foundation::DeviceId::from_bytes([0x5a; 16]);
    let queue = Arc::new(pos_sched::JobQueue::new(
        pos_sched::QueueConfig {
            device,
            backoff: pos_sched::BackoffPolicy::default(),
            lease_ttl_ms: pos_sched::SCHED_LEASE_TTL_MS_DEFAULT,
        },
        Arc::new(pos_sched::SplitMixJitter::from_os_entropy()),
        Arc::new(pos_sched::SchedulerMetrics::default()),
    ));
    queue.ensure_schema(&log).expect("ensure the queue schema");
    let pipeline = IngestPipeline::new(
        PipelineConfig::for_device(device),
        queue,
        stage_registry_default(
            &pos_ingest::TranscribeSetup::local(
                std::path::PathBuf::from("models/pulled"),
                "whisper-small",
            ),
            &pos_ingest::EmbedSetup::local(std::path::PathBuf::from("models/pulled")),
        ),
    );
    let submission = EvidenceSubmission {
        source_kind: "upload".to_owned(),
        source_scope: "worker-fixture".to_owned(),
        external: ExternalRef {
            external_id: "interview-01.md".to_owned(),
            external_url: None,
            external_version: None,
        },
        media_kind: MediaKind::Markdown,
        shape: EvidenceShape::Document,
        occurred_ts_ms: 1_700_000_000_000,
        author: Some("fixture".to_owned()),
        title: Some("Interview 01".to_owned()),
        thread_ref: None,
        actor: Actor::User(pos_foundation::UserId::from_bytes([0x5b; 16])),
    };
    let mut reader = content;
    pipeline
        .submit(
            &log,
            project_id,
            &pos_foundation::SystemWallClock,
            &submission,
            &mut reader,
        )
        .expect("submit the fixture item");
}

fn evidence_list(runtime: &LocalRuntime, path: &str) -> String {
    let input = input_json(&pos_api::EvidenceListInput {
        path: path.to_owned(),
        source_id: None,
        status: None,
        row_count_max: Some(10),
        with_stages: true,
    })
    .expect("input serializes");
    runtime
        .query_with_input(QueryName::EvidenceList.as_str(), &input)
        .expect("evidence.list resolves")
}

fn open_project(runtime: &LocalRuntime, path: &str) {
    let input = input_json(&ProjectPathInput {
        path: path.to_owned(),
    })
    .expect("input serializes");
    runtime
        .command(CommandName::ProjectOpen.as_str(), &input)
        .expect("project.open resolves");
}

fn close_project(runtime: &LocalRuntime, path: &str) {
    let input = input_json(&ProjectPathInput {
        path: path.to_owned(),
    })
    .expect("input serializes");
    runtime
        .command(CommandName::ProjectClose.as_str(), &input)
        .expect("project.close resolves");
}

fn health(runtime: &LocalRuntime) -> String {
    runtime
        .query(QueryName::Health.as_str())
        .expect("health resolves")
}

/// Polls a registry read until it satisfies `done`, or the budget expires.
/// Returns the last answer either way, so a failure message carries the state
/// the assertion actually saw.
fn observe_until(
    budget_ms: u64,
    mut read: impl FnMut() -> String,
    done: impl Fn(&str) -> bool,
) -> String {
    let started = Instant::now();
    loop {
        let answer = read();
        if done(&answer) {
            return answer;
        }
        if u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX) >= budget_ms {
            return answer;
        }
        std::thread::sleep(Duration::from_millis(OBSERVE_POLL_MS));
    }
}

fn runtime_with_workers() -> LocalRuntime {
    let mut runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        std::path::PathBuf::from("worker-suite-has-no-pack-root"),
    ));
    runtime
        .start_background_workers(WorkerConfig {
            // Tighter than the product default so the suite's idle path is
            // measured in tens of milliseconds instead of a quarter second.
            idle_poll_interval_ms: 20,
            ..WorkerConfig::default()
        })
        .expect("the worker pool starts");
    runtime
}

/// **The oracle for the whole change.** An item submitted into an open project
/// reaches the last stage this build implements with nothing in this test
/// claiming, running, or advancing a job.
#[test]
fn an_open_project_runs_its_queued_stages_with_nothing_here_claiming_them() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = runtime_with_workers();
    let path = create_project(&runtime, directory.path(), "wired");
    open_project(&runtime, &path);
    submit_one(&path, &corpus());

    let listed = observe_until(
        OBSERVE_MS_MAX,
        || evidence_list(&runtime, &path),
        |answer| answer.contains("\"status\":\"chunked\""),
    );
    assert!(
        listed.contains("\"status\":\"chunked\""),
        "the pipeline did not advance on its own: {listed}"
    );
    // Both stages ran, in order, and left their durable history — not just a
    // status field somebody could have written directly.
    assert!(
        listed.contains("\"stage\":\"normalize\",\"state\":\"done\""),
        "{listed}"
    );
    assert!(
        listed.contains("\"stage\":\"chunk\",\"state\":\"done\""),
        "{listed}"
    );
    assert!(!listed.contains("\"chunkCount\":0"), "{listed}");
    // And it stopped honestly. *Where* it stops depends on whether an
    // embedding model is pulled on this machine (m1-s04's `stage_registry`
    // explains why that is a registration question rather than a failure),
    // so the assertion is the property that holds either way: the run either
    // named a next stage it cannot run, or it ran EMBED too and named the
    // stage after it. What it must never do is claim a stage is available and
    // then not have run it.
    let stopped_honestly = listed.contains("\"nextStageAvailable\":false")
        || listed.contains("\"stage\":\"embed\",\"state\":\"done\"");
    assert!(stopped_honestly, "{listed}");
    assert!(
        !listed.contains("\"state\":\"dead\""),
        "nothing correct may dead-letter: {listed}"
    );

    let drained = runtime.drain_background_workers(OBSERVE_MS_MAX);
    assert!(drained.quiescent, "the queue is not quiescent: {drained:?}");
    assert_eq!(drained.queued_remaining, 0);
    assert_eq!(
        drained.dead_total, 0,
        "no item should have died: {drained:?}"
    );
    assert!(runtime.shutdown_background_workers());
    // Idempotent: a second stop on a stopped pool is a no-op, not a hang.
    assert!(runtime.shutdown_background_workers());
}

/// The inverse, and the M0 behaviour this change fixes: with no pool started,
/// the same submission stays exactly where it was queued. Without this case
/// the suite above could pass for the wrong reason — some other thread, some
/// synchronous fallback — and the regression would be invisible.
#[test]
fn without_a_pool_the_same_work_stays_queued_and_health_says_so() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        std::path::PathBuf::from("worker-suite-has-no-pack-root"),
    ));
    let path = create_project(&runtime, directory.path(), "unwired");
    open_project(&runtime, &path);
    submit_one(&path, &corpus());

    let listed = observe_until(
        IDLE_OBSERVE_MS,
        || evidence_list(&runtime, &path),
        |answer| answer.contains("\"status\":\"chunked\""),
    );
    assert!(
        listed.contains("\"status\":\"raw\""),
        "nothing should have advanced this item: {listed}"
    );
    assert!(listed.contains("\"nextStage\":\"normalize\""), "{listed}");
    // The stage exists in this build — the item is waiting on a *worker*, not
    // on a story. That distinction is the whole point of the honest stop.
    assert!(listed.contains("\"nextStageAvailable\":true"), "{listed}");
    let health = health(&runtime);
    assert!(health.contains("\"running\":false"), "{health}");
    assert!(
        health.contains("\"registeredProjectCount\":0"),
        "a stopped pool serves nothing: {health}"
    );
}

/// Open and close are the pool's registration boundary, so the projects it
/// serves are exactly the ones this process has open.
#[test]
fn open_registers_a_project_with_the_pool_and_close_releases_it() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = runtime_with_workers();
    let first = create_project(&runtime, directory.path(), "first");
    let second = create_project(&runtime, directory.path(), "second");

    assert!(health(&runtime).contains("\"registeredProjectCount\":0"));
    open_project(&runtime, &first);
    open_project(&runtime, &second);
    let opened = health(&runtime);
    assert!(opened.contains("\"running\":true"), "{opened}");
    assert!(opened.contains("\"registeredProjectCount\":2"), "{opened}");

    close_project(&runtime, &first);
    let after_close = health(&runtime);
    assert!(
        after_close.contains("\"registeredProjectCount\":1"),
        "{after_close}"
    );
    assert!(
        after_close.contains("\"openProjectCount\":1"),
        "{after_close}"
    );
    // Nothing that ran should have gone wrong quietly.
    assert!(after_close.contains("\"lastError\":null"), "{after_close}");
    assert!(runtime.shutdown_background_workers());
}

/// `ingest.reprocess` re-runs a completed item, and the pool that the same
/// process holds is what runs it. The report says whether anything will.
#[test]
fn reprocess_says_whether_a_worker_will_run_what_it_queued() {
    let directory = tempfile::tempdir().expect("tempdir");
    let runtime = runtime_with_workers();
    let path = create_project(&runtime, directory.path(), "reprocessed");
    open_project(&runtime, &path);
    submit_one(&path, &corpus());
    let listed = observe_until(
        OBSERVE_MS_MAX,
        || evidence_list(&runtime, &path),
        |answer| answer.contains("\"status\":\"chunked\""),
    );
    assert!(listed.contains("\"status\":\"chunked\""), "{listed}");

    let input = input_json(&pos_api::IngestReprocessInput {
        path: path.clone(),
        from_stage: "chunk".to_owned(),
        evidence_id: None,
        item_count_max: Some(10),
        reason: "re-chunk with the same strategy".to_owned(),
    })
    .expect("input serializes");
    let report = runtime
        .command(CommandName::IngestReprocess.as_str(), &input)
        .expect("ingest.reprocess resolves");
    assert!(report.contains("\"requeuedCount\":1"), "{report}");
    assert!(
        report.contains("\"backgroundWorkersRunning\":true"),
        "the report must state whether anything claims this: {report}"
    );

    let drained = runtime.drain_background_workers(OBSERVE_MS_MAX);
    assert!(drained.quiescent, "{drained:?}");
    let relisted = evidence_list(&runtime, &path);
    assert!(
        relisted.contains("\"status\":\"chunked\"") || relisted.contains("\"status\":\"embedded\""),
        "the reprocess pass reached the last stage this machine can run: {relisted}"
    );
    assert!(
        relisted.contains("\"pass\":1"),
        "the reprocess pass ran: {relisted}"
    );
    assert!(runtime.shutdown_background_workers());
}
