//! The telemetry spine (m0-s15): one correlated span tree per unit of work,
//! across `pos-api` dispatch, `pos-agents` steps, `pos-gateway` calls, and
//! `pos-sched` jobs.
//!
//! ## Invariant inventory (STYLE — state machine in prose)
//!
//! 1. **A span field cannot hold a string.** [`SpanValue`] has no `String`
//!    variant and no `Display`/`Debug` escape hatch, so ingested content,
//!    prompts, model output, paths, and key material are not *representable*
//!    in a span (L6, and the secret rule). The variable half of a span name is
//!    a [`SpanDetail`], constructible only from a `&'static str` or from the
//!    bounded registered-identifier grammar the scheduler's job kinds already
//!    satisfy.
//! 2. **This module is the only emission point.** `check-discipline` fails any
//!    use of `tracing` outside `pos-foundation/src/telemetry/`, which is what
//!    keeps invariant 1 a property of the build rather than of everyone's
//!    memory. `tracing` therefore appears in exactly one crate manifest.
//! 3. **Correlation ids are derived, not minted.** A Run that resumes in a new
//!    process after `kill -9` (m0-s13) and a job that retries in another
//!    process (m0-s14) are each *one* piece of work, so their trace id and
//!    root span id are derived from ids that are already durable. No process
//!    hands a trace context to any other process.
//! 4. **Telemetry reads the real clock.** Domain code takes an injected
//!    [`crate::WallClock`] so replay is deterministic (L1). A span is an
//!    observation *of* the process, not a fact in the log, so it stamps itself
//!    from `SystemTime`/`Instant` — deliberately, here, and nowhere else. This
//!    is ADR-0005's rule one layer up: a heartbeat is weather, not history.
//! 5. **Off by default.** With no [`install`] call the macros short-circuit on
//!    a dispatcher check, so instrumentation may sit on the dispatch path of
//!    every query. Desktop ships with export off (L4 spirit).
//! 6. **Bounded, and honest when it truncates.** Detail length, field count,
//!    parent-stack depth, and serialized line length all have named caps; a
//!    dropped field is counted and the count is exported (L8).

mod emit;
mod sink;

pub use sink::{
    CaptureHandle, FinishedSpan, JsonLinesSink, NullSink, SPAN_LINE_LEN_MAX, SpanSink,
    TelemetryConfig, TelemetryError, TelemetryExport, TelemetryStats,
};

use std::cell::RefCell;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Longest `:detail` suffix a span name carries. Job kinds — the only
/// non-static source — are already capped at 64 bytes by their own grammar.
pub const SPAN_DETAIL_LEN_MAX: usize = 64;

/// How deep the per-thread parent stack may go before a span becomes a root.
/// The taxonomy nests at most three levels (api → step → gateway call); 16 is
/// slack, not a design allowance for unbounded nesting.
pub const SPAN_STACK_DEPTH_MAX: usize = 16;

/// Fields one span may carry. The taxonomy's widest span uses seven.
pub const SPAN_FIELD_COUNT_MAX: usize = 12;

/// Outcome recorded for a span that ended without one — a panic, an early
/// return that skipped `finish`, or a process kill between the two. It is a
/// visible state rather than a silently missing field (L8).
pub const OUTCOME_INCOMPLETE: &str = "incomplete";

/// W3C-shaped 128-bit trace id. Derived from durable ids (invariant 3), so
/// the same logical Run or job produces the same trace in any process.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct TraceId([u8; 16]);

/// W3C-shaped 64-bit span id.
#[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SpanId([u8; 8]);

/// The pair a child needs to attach to a parent that may live in another
/// thread — or, for a derived root, in another process.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct SpanContext {
    pub trace: TraceId,
    pub span: SpanId,
}

/// Domain separators. Distinct constants keep the Run and job id spaces from
/// colliding into one trace even if the underlying ids ever did.
const DOMAIN_RUN: u64 = 0x706f_735f_7275_6e31; // "pos_run1"
const DOMAIN_JOB: u64 = 0x706f_735f_6a6f_6231; // "pos_job1"
const DOMAIN_PROCESS: u64 = 0x706f_735f_7072_6331; // "pos_prc1"

impl TraceId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 16]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 16] {
        self.0
    }

    /// The trace every span of one Run belongs to, in every process that ever
    /// works on it (invariant 3).
    #[must_use]
    pub fn for_run(project: crate::ProjectId, run: crate::RunId) -> Self {
        Self(derive_128(
            DOMAIN_RUN,
            project.into_bytes(),
            run.into_bytes(),
        ))
    }

    /// The trace every attempt of one job belongs to, so a retry after a lease
    /// expiry joins the trace its first attempt started.
    #[must_use]
    pub fn for_job(project: crate::ProjectId, job: crate::JobId) -> Self {
        Self(derive_128(
            DOMAIN_JOB,
            project.into_bytes(),
            job.into_bytes(),
        ))
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex(&self.0)
    }
}

impl SpanId {
    #[must_use]
    pub const fn from_bytes(bytes: [u8; 8]) -> Self {
        Self(bytes)
    }

    #[must_use]
    pub const fn into_bytes(self) -> [u8; 8] {
        self.0
    }

    /// The Run's root span id, derived so a resumed process can parent its
    /// steps to a span it never saw created.
    #[must_use]
    pub fn root_for_run(project: crate::ProjectId, run: crate::RunId) -> Self {
        Self(first8(derive_128(
            DOMAIN_RUN ^ 1,
            project.into_bytes(),
            run.into_bytes(),
        )))
    }

    /// The job's root span id; the same argument as [`Self::root_for_run`],
    /// for attempts spread across processes.
    #[must_use]
    pub fn root_for_job(project: crate::ProjectId, job: crate::JobId) -> Self {
        Self(first8(derive_128(
            DOMAIN_JOB ^ 1,
            project.into_bytes(),
            job.into_bytes(),
        )))
    }

    #[must_use]
    pub fn to_hex(self) -> String {
        hex(&self.0)
    }
}

impl SpanContext {
    /// The context every span of this Run attaches to.
    #[must_use]
    pub fn for_run(project: crate::ProjectId, run: crate::RunId) -> Self {
        Self {
            trace: TraceId::for_run(project, run),
            span: SpanId::root_for_run(project, run),
        }
    }

    /// The context every attempt of this job attaches to.
    #[must_use]
    pub fn for_job(project: crate::ProjectId, job: crate::JobId) -> Self {
        Self {
            trace: TraceId::for_job(project, job),
            span: SpanId::root_for_job(project, job),
        }
    }
}

impl std::fmt::Debug for TraceId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "TraceId({})", self.to_hex())
    }
}

impl std::fmt::Debug for SpanId {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SpanId({})", self.to_hex())
    }
}

fn hex(bytes: &[u8]) -> String {
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        use std::fmt::Write as _;
        let _ = write!(text, "{byte:02x}");
    }
    text
}

/// Domain-separated mixing over two ids. Deliberately **not** cryptographic:
/// a correlation id joins spans, it does not authenticate anything, and the
/// bottom crate of the workspace should not grow a hash dependency to build
/// one. The same reasoning the scheduler used for its retry jitter.
fn derive_128(domain: u64, left: [u8; 16], right: [u8; 16]) -> [u8; 16] {
    let mut state = mix(domain);
    for chunk in [&left[0..8], &left[8..16], &right[0..8], &right[8..16]] {
        state = mix(state ^ u64_from(chunk));
    }
    let high = state;
    let low = mix(state ^ domain.rotate_left(17));
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&high.to_be_bytes());
    bytes[8..16].copy_from_slice(&low.to_be_bytes());
    // An all-zero trace id is reserved as "absent" by the W3C/OTel shape.
    if bytes == [0_u8; 16] {
        bytes[15] = 1;
    }
    bytes
}

fn first8(bytes: [u8; 16]) -> [u8; 8] {
    let mut out = [0_u8; 8];
    out.copy_from_slice(&bytes[0..8]);
    if out == [0_u8; 8] {
        out[7] = 1;
    }
    out
}

fn u64_from(chunk: &[u8]) -> u64 {
    let mut bytes = [0_u8; 8];
    let len = chunk.len().min(8);
    bytes[..len].copy_from_slice(&chunk[..len]);
    u64::from_be_bytes(bytes)
}

/// SplitMix64's finalizer — the same avalanche the scheduler's jitter uses.
const fn mix(seed: u64) -> u64 {
    let mut z = seed.wrapping_add(0x9E37_79B9_7F4A_7C15);
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
    z ^ (z >> 31)
}

/// Every span stem in the taxonomy. Closed on purpose: a span name outside
/// this list is unrepresentable, so the taxonomy is the type.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SpanName {
    ApiCommand,
    ApiQuery,
    ApiStream,
    AgentsStep,
    GatewayCall,
    SchedJob,
}

impl SpanName {
    pub const COUNT: usize = 6;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ApiCommand,
        Self::ApiQuery,
        Self::ApiStream,
        Self::AgentsStep,
        Self::GatewayCall,
        Self::SchedJob,
    ];

    /// The static stem. `tracing` fixes a span's name at its callsite, so the
    /// stem is what it sees; the `:detail` half rides as a field and the sink
    /// renders the taxonomy name (invariant 1 keeps that half bounded).
    #[must_use]
    pub const fn stem(self) -> &'static str {
        match self {
            Self::ApiCommand => "api.cmd",
            Self::ApiQuery => "api.query",
            Self::ApiStream => "api.stream",
            Self::AgentsStep => "agents.step",
            Self::GatewayCall => "gateway.call",
            Self::SchedJob => "sched.job",
        }
    }

    #[must_use]
    pub fn parse(stem: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|name| name.stem() == stem)
    }
}

/// The variable half of a span name (`api.cmd/:name`). Copy, inline, bounded:
/// no allocation, and no constructor that accepts arbitrary text.
#[derive(Clone, Copy, Eq, PartialEq)]
pub struct SpanDetail {
    bytes: [u8; SPAN_DETAIL_LEN_MAX],
    len: u8,
}

impl SpanDetail {
    /// The normal path. Every caller in core passes an `as_str()`/`label()`/
    /// `code()` that is already `&'static str`, so a compile-time literal is
    /// the only thing that can reach a span name.
    #[must_use]
    pub fn from_static(text: &'static str) -> Self {
        Self::from_bounded(text)
    }

    /// The one non-static source: `pos-sched` job kinds. The grammar is
    /// `JobKind`'s own — 1..=64 bytes of ASCII alphanumerics, `.`, `-`, `_` —
    /// which is a registered identifier, never user or ingested text. Anything
    /// outside the grammar becomes the static label `unregistered` rather than
    /// a panic or a truncated fragment of whatever it actually was.
    ///
    /// `check-discipline` restricts this call site to `pos-sched/src/pool.rs`,
    /// the same way projection writes are restricted to `pos-log/src/apply/`.
    #[must_use]
    pub fn from_registered_kind(text: &str) -> Self {
        let admissible = !text.is_empty()
            && text.len() <= SPAN_DETAIL_LEN_MAX
            && text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'-' | b'_'));
        if admissible {
            Self::from_bounded(text)
        } else {
            Self::from_bounded("unregistered")
        }
    }

    fn from_bounded(text: &str) -> Self {
        let mut bytes = [0_u8; SPAN_DETAIL_LEN_MAX];
        let mut len = text.len().min(SPAN_DETAIL_LEN_MAX);
        while len > 0 && !text.is_char_boundary(len) {
            len -= 1;
        }
        bytes[..len].copy_from_slice(&text.as_bytes()[..len]);
        Self {
            bytes,
            len: u8::try_from(len).unwrap_or(0), // INVARIANT: len <= SPAN_DETAIL_LEN_MAX (64).
        }
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        let len = usize::from(self.len);
        std::str::from_utf8(&self.bytes[..len]).unwrap_or("") // INVARIANT: from_bounded only stores whole UTF-8 prefixes.
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len == 0
    }
}

impl Default for SpanDetail {
    fn default() -> Self {
        Self {
            bytes: [0_u8; SPAN_DETAIL_LEN_MAX],
            len: 0,
        }
    }
}

impl std::fmt::Debug for SpanDetail {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "SpanDetail({:?})", self.as_str())
    }
}

/// Every field key the taxonomy uses. Closed like the names, so the exported
/// shape is reviewable in one place.
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub enum SpanField {
    Project,
    Run,
    Job,
    StepIndex,
    Attempt,
    /// Whether a tool step was read-only, idempotent, or not — the L5/L6
    /// question a trace is asked. The tool's own id is a registered
    /// identifier, not a static label, and lives in the durable step fact
    /// where the ledger already carries it (L7).
    EffectClass,
    Tier,
    CredentialClass,
    TokensIn,
    TokensOut,
    Frames,
    Rows,
    Outcome,
    DurationMs,
}

impl SpanField {
    #[must_use]
    pub const fn key(self) -> &'static str {
        match self {
            Self::Project => "project",
            Self::Run => "run",
            Self::Job => "job",
            Self::StepIndex => "step_index",
            Self::Attempt => "attempt",
            Self::EffectClass => "effect_class",
            Self::Tier => "tier",
            Self::CredentialClass => "credential_class",
            Self::TokensIn => "tokens_in",
            Self::TokensOut => "tokens_out",
            Self::Frames => "frames",
            Self::Rows => "rows",
            Self::Outcome => "outcome",
            Self::DurationMs => "duration_ms",
        }
    }
}

/// What a span field may hold. There is no `String` variant, no `Display`
/// bound, and no constructor from a runtime `&str` — invariant 1.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SpanValue {
    Id([u8; 16]),
    Count(u64),
    Millis(u64),
    Flag(bool),
    Label(&'static str),
}

/// Where a new span attaches.
#[derive(Clone, Copy, Debug)]
pub enum Parent {
    /// The innermost span open on this thread, or a fresh process-local root
    /// when there is none.
    Current,
    /// This span *is* a derived root (`SpanContext::for_run`/`for_job`): a
    /// Run's `run.start` command and a job's execution span both take this,
    /// which is what makes a resumed process join the original trace.
    Root(SpanContext),
    /// An explicit remote parent — used when work crosses a thread (the Echo
    /// worker) or a process (a resumed Run's steps).
    Of(SpanContext),
}

thread_local! {
    /// The per-thread parent stack. `tracing`'s own current-span stack is not
    /// used: our parents are explicit ids that must survive a process
    /// boundary, and mixing the two would give one span two notions of
    /// "parent" that disagree exactly when it matters.
    static PARENTS: RefCell<Vec<SpanContext>> = const { RefCell::new(Vec::new()) };
}

/// Process nonce for spans that have no durable id to derive from. Documented
/// as non-cryptographic: it correlates a request, it does not authenticate one.
static PROCESS_NONCE: std::sync::OnceLock<u64> = std::sync::OnceLock::new();
static ROOT_COUNTER: AtomicU64 = AtomicU64::new(0);

fn process_root() -> SpanContext {
    let nonce = *PROCESS_NONCE.get_or_init(|| {
        let since_epoch = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_nanos()).unwrap_or(u64::MAX)
            });
        mix(since_epoch ^ u64::from(std::process::id()).rotate_left(32))
    });
    let counter = ROOT_COUNTER.fetch_add(1, Ordering::Relaxed);
    let mut left = [0_u8; 16];
    left[0..8].copy_from_slice(&nonce.to_be_bytes());
    let mut right = [0_u8; 16];
    right[0..8].copy_from_slice(&counter.to_be_bytes());
    let trace = derive_128(DOMAIN_PROCESS, left, right);
    SpanContext {
        trace: TraceId(trace),
        span: SpanId(first8(derive_128(DOMAIN_PROCESS ^ 1, left, right))),
    }
}

/// A live span. Dropping it closes the span; [`Self::finish`] is the same
/// thing with the outcome stated, which is what every call site should use.
#[must_use = "a span that is dropped immediately measures nothing"]
pub struct Span {
    inner: Option<Box<SpanInner>>,
}

struct SpanInner {
    context: SpanContext,
    parent: Option<SpanId>,
    name: SpanName,
    detail: SpanDetail,
    tracing_span: tracing::Span,
    fields: Mutex<emit::FieldSet>,
    start_unix_ms: u64,
    started: Instant,
    pushed: bool,
}

impl Span {
    /// Opens a span. Returns a no-op handle when no telemetry pipeline is
    /// installed, which is the shipped desktop configuration (invariant 5).
    pub fn open(name: SpanName, detail: SpanDetail, parent: Parent) -> Self {
        if !emit::enabled() {
            return Self { inner: None };
        }
        let (context, parent_span) = match parent {
            Parent::Root(context) => (context, None),
            Parent::Of(context) => (
                SpanContext {
                    trace: context.trace,
                    span: SpanId(first8(derive_128(
                        DOMAIN_PROCESS,
                        context.trace.0,
                        seed_from(context.span, ROOT_COUNTER.fetch_add(1, Ordering::Relaxed)),
                    ))),
                },
                Some(context.span),
            ),
            Parent::Current => match current() {
                Some(context) => (
                    SpanContext {
                        trace: context.trace,
                        span: SpanId(first8(derive_128(
                            DOMAIN_PROCESS,
                            context.trace.0,
                            seed_from(context.span, ROOT_COUNTER.fetch_add(1, Ordering::Relaxed)),
                        ))),
                    },
                    Some(context.span),
                ),
                None => (process_root(), None),
            },
        };
        let tracing_span = emit::new_span(name, detail, context, parent_span);
        let pushed = push_parent(context);
        Self {
            inner: Some(Box::new(SpanInner {
                context,
                parent: parent_span,
                name,
                detail,
                tracing_span,
                fields: Mutex::new(emit::FieldSet::default()),
                start_unix_ms: unix_ms_now(),
                started: Instant::now(),
                pushed,
            })),
        }
    }

    /// Opens a span that does **not** become this thread's current parent.
    ///
    /// For spans that cross an `await`: a task may resume on a different
    /// worker thread, so a stack push and its matching pop could land on two
    /// different stacks — leaving one entry stranded and mis-parenting
    /// whatever opened next on that thread. Detached spans state their parent
    /// explicitly (every async span in core does), so nothing is lost.
    pub fn open_detached(name: SpanName, detail: SpanDetail, parent: Parent) -> Self {
        let mut span = Self::open(name, detail, parent);
        if let Some(inner) = span.inner.as_mut()
            && inner.pushed
        {
            pop_parent(inner.context);
            inner.pushed = false;
        }
        span
    }

    /// Records one typed field. Fields past [`SPAN_FIELD_COUNT_MAX`] are
    /// dropped and counted rather than silently lost (L8).
    pub fn set(&self, field: SpanField, value: SpanValue) {
        let Some(inner) = &self.inner else {
            return;
        };
        emit::record(&inner.tracing_span, field, value);
        if let Ok(mut fields) = inner.fields.lock() {
            fields.set(field, value);
        }
    }

    /// The context a child on another thread or in another process attaches
    /// to. `None` when telemetry is off.
    #[must_use]
    pub fn context(&self) -> Option<SpanContext> {
        self.inner.as_ref().map(|inner| inner.context)
    }

    /// Re-roots this span onto a derived context.
    ///
    /// The one command that needs this is `run.start`: its trace is derived
    /// from a RunId the command itself mints, so the span is open before its
    /// identity exists. Adoption is safe because nothing is emitted until
    /// close — the span's identity is decided exactly once, just later than
    /// its creation. Every other call site knows its context up front and
    /// uses [`Parent`].
    pub fn adopt_root(&mut self, context: SpanContext) {
        let Some(inner) = self.inner.as_mut() else {
            return;
        };
        let previous = inner.context;
        inner.context = context;
        inner.parent = None;
        if inner.pushed {
            replace_parent(previous, context);
        }
        emit::rewrite_identity(&inner.tracing_span, context);
    }

    /// Closes the span with a stated outcome. The label is `&'static str`
    /// because every outcome in core already is: `ApiError::code`,
    /// `Weather::code`, a job's terminal reason.
    pub fn finish(mut self, outcome: &'static str) {
        self.set(SpanField::Outcome, SpanValue::Label(outcome));
        self.close();
    }

    fn close(&mut self) {
        let Some(inner) = self.inner.take() else {
            return;
        };
        if inner.pushed {
            pop_parent(inner.context);
        }
        let mut field_set = inner
            .fields
            .into_inner()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        // A span that ended without a stated outcome says so; the alternative
        // is a missing field that reads as "nothing went wrong" (L8).
        field_set.set_if_absent(SpanField::Outcome, SpanValue::Label(OUTCOME_INCOMPLETE));
        let (fields, dropped_field_count) = field_set.into_parts();
        let duration_us = u64::try_from(inner.started.elapsed().as_micros()).unwrap_or(u64::MAX); // INVARIANT: a span longer than 584 000 years saturates rather than wrapping.
        emit::export(&FinishedSpan {
            trace: inner.context.trace,
            span: inner.context.span,
            parent: inner.parent,
            name: inner.name,
            detail: inner.detail,
            start_unix_ms: inner.start_unix_ms,
            duration_us,
            fields,
            dropped_field_count,
        });
        drop(inner.tracing_span);
    }
}

/// The one system-clock read in the telemetry plane (invariant 4).
fn unix_ms_now() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |elapsed| {
            u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
        }) // INVARIANT: saturation is the documented policy, matching SystemWallClock.
}

impl Drop for Span {
    fn drop(&mut self) {
        self.close();
    }
}

fn seed_from(span: SpanId, counter: u64) -> [u8; 16] {
    let mut bytes = [0_u8; 16];
    bytes[0..8].copy_from_slice(&span.0);
    bytes[8..16].copy_from_slice(&counter.to_be_bytes());
    bytes
}

/// The innermost span open on this thread, if any.
#[must_use]
pub fn current() -> Option<SpanContext> {
    PARENTS
        .try_with(|stack| stack.borrow().last().copied())
        .ok()
        .flatten()
}

fn push_parent(context: SpanContext) -> bool {
    PARENTS
        .try_with(|stack| {
            let mut stack = stack.borrow_mut();
            if stack.len() >= SPAN_STACK_DEPTH_MAX {
                return false;
            }
            stack.push(context);
            true
        })
        .unwrap_or(false)
}

fn replace_parent(previous: SpanContext, next: SpanContext) {
    let _ = PARENTS.try_with(|stack| {
        let mut stack = stack.borrow_mut();
        if let Some(entry) = stack.iter_mut().rev().find(|entry| **entry == previous) {
            *entry = next;
        }
    });
}

fn pop_parent(context: SpanContext) {
    let _ = PARENTS.try_with(|stack| {
        let mut stack = stack.borrow_mut();
        // Spans need not close in LIFO order (a handle may be moved), so this
        // removes *this* context rather than assuming it is on top.
        if let Some(index) = stack.iter().rposition(|entry| *entry == context) {
            stack.remove(index);
        }
    });
}

/// Installs the process-wide telemetry pipeline. Shells call this once at
/// startup from typed configuration; calling it again swaps the primary sink.
///
/// # Errors
///
/// [`TelemetryError`] when the configured export target cannot be opened, or
/// when another library already claimed the global `tracing` subscriber.
pub fn install(config: TelemetryConfig) -> Result<(), TelemetryError> {
    emit::install(config)
}

/// Registers an in-memory capture for one trace, for the span-tree oracles.
/// Capture is keyed by trace id, so parallel tests in one process never see
/// each other's spans — and because trace ids are derived (invariant 3), a
/// test computes the key from the RunId instead of scraping it from output.
#[must_use]
pub fn capture(trace: TraceId) -> CaptureHandle {
    emit::capture(Some(trace))
}

/// Registers a capture for **every** trace, for the oracles whose trace id is
/// derived from an id the work itself mints (a Run's, minted inside
/// `run.start`). Such a test cannot know its key before the work starts, so it
/// captures everything and scopes with
/// [`CaptureHandle::assert_single_connected_tree`] afterwards.
#[must_use]
pub fn capture_any() -> CaptureHandle {
    emit::capture(None)
}

/// Counters the pipeline itself publishes: exported spans, dropped fields,
/// dropped lines. A bounded sink that silently discarded would read as
/// completeness (L8).
#[must_use]
pub fn stats() -> TelemetryStats {
    emit::stats()
}

#[cfg(test)]
mod tests {
    use super::{
        OUTCOME_INCOMPLETE, Parent, SPAN_DETAIL_LEN_MAX, SpanContext, SpanDetail, SpanField,
        SpanName, SpanValue, TraceId, capture, current,
    };
    use crate::{JobId, ProjectId, RunId};

    #[test]
    fn a_run_trace_is_the_same_in_any_process() {
        let project = ProjectId::from_bytes([7; 16]);
        let run = RunId::from_bytes([9; 16]);
        // Two "processes" derive the identical context from durable ids alone.
        assert_eq!(SpanContext::for_run(project, run), {
            SpanContext {
                trace: TraceId::for_run(project, run),
                span: super::SpanId::root_for_run(project, run),
            }
        });
        // Different Runs, and Run vs job, stay in different traces.
        let other = RunId::from_bytes([10; 16]);
        assert_ne!(
            TraceId::for_run(project, run),
            TraceId::for_run(project, other)
        );
        assert_ne!(
            TraceId::for_run(project, run).into_bytes(),
            TraceId::for_job(project, JobId::from_bytes([9; 16])).into_bytes()
        );
        assert_eq!(TraceId::for_run(project, run).to_hex().len(), 32);
    }

    #[test]
    fn a_detail_can_only_come_from_a_literal_or_the_registered_grammar() {
        assert_eq!(
            SpanDetail::from_static("project.create").as_str(),
            "project.create"
        );
        assert_eq!(
            SpanDetail::from_registered_kind("ingest.chunk-1").as_str(),
            "ingest.chunk-1"
        );
        // Anything outside the registered-identifier grammar becomes a static
        // label instead of a fragment of whatever it actually was.
        for hostile in [
            "sk-ant-api03-secret value",
            "the customer said: we will churn",
            "",
            "/Users/someone/private.pos",
        ] {
            assert_eq!(
                SpanDetail::from_registered_kind(hostile).as_str(),
                "unregistered",
                "{hostile:?} must not reach a span name"
            );
        }
        let long = "a".repeat(SPAN_DETAIL_LEN_MAX + 40);
        assert_eq!(
            SpanDetail::from_registered_kind(&long).as_str(),
            "unregistered"
        );
        assert_eq!(SpanDetail::from_static("").as_str(), "");
    }

    #[test]
    fn every_stem_and_field_key_round_trips() {
        for name in SpanName::ALL {
            assert_eq!(SpanName::parse(name.stem()), Some(name));
        }
        assert_eq!(SpanName::ALL.len(), SpanName::COUNT);
        assert_eq!(SpanName::parse("api.cmd/project.create"), None);
        // Keys are distinct: a duplicate would make two fields overwrite.
        let mut keys: Vec<&str> = [
            SpanField::Project,
            SpanField::Run,
            SpanField::Job,
            SpanField::StepIndex,
            SpanField::Attempt,
            SpanField::EffectClass,
            SpanField::Tier,
            SpanField::CredentialClass,
            SpanField::TokensIn,
            SpanField::TokensOut,
            SpanField::Frames,
            SpanField::Rows,
            SpanField::Outcome,
            SpanField::DurationMs,
        ]
        .iter()
        .map(|field| field.key())
        .collect();
        keys.sort_unstable();
        let count = keys.len();
        keys.dedup();
        assert_eq!(keys.len(), count);
    }

    #[test]
    fn spans_nest_under_the_derived_root_and_report_their_outcome() {
        let project = ProjectId::from_bytes([1; 16]);
        let run = RunId::from_bytes([2; 16]);
        let root = SpanContext::for_run(project, run);
        let captured = capture(root.trace);

        {
            let outer = super::Span::open(
                SpanName::ApiCommand,
                SpanDetail::from_static("run.start"),
                Parent::Root(root),
            );
            outer.set(SpanField::Project, SpanValue::Id(project.into_bytes()));
            assert_eq!(current(), Some(root));
            {
                let inner = super::Span::open(
                    SpanName::GatewayCall,
                    SpanDetail::from_static("openai-compatible"),
                    Parent::Current,
                );
                inner.set(SpanField::TokensIn, SpanValue::Count(12));
                inner.finish("ok");
            }
            assert_eq!(current(), Some(root));
            outer.finish("ok");
        }
        assert_eq!(current(), None);

        let spans = captured.spans();
        assert_eq!(spans.len(), 2, "both spans reached the capture");
        captured
            .assert_single_connected_tree(root.trace)
            .expect("the two spans form one connected tree");
        let outer = spans
            .iter()
            .find(|span| span.name == SpanName::ApiCommand)
            .expect("the command span was captured");
        let inner = spans
            .iter()
            .find(|span| span.name == SpanName::GatewayCall)
            .expect("the gateway span was captured");
        assert_eq!(outer.trace, root.trace);
        assert_eq!(outer.span, root.span);
        assert_eq!(outer.parent, None);
        assert_eq!(inner.parent, Some(root.span));
        assert_eq!(inner.taxonomy_name(), "gateway.call/openai-compatible");
        assert_eq!(inner.outcome(), Some("ok"));
        assert_eq!(inner.field(SpanField::TokensIn), Some(SpanValue::Count(12)));
    }

    #[test]
    fn a_span_dropped_without_finishing_records_that_it_was_incomplete() {
        let project = ProjectId::from_bytes([3; 16]);
        let run = RunId::from_bytes([4; 16]);
        let root = SpanContext::for_run(project, run);
        let captured = capture(root.trace);
        drop(super::Span::open(
            SpanName::AgentsStep,
            SpanDetail::from_static("echo.complete"),
            Parent::Root(root),
        ));
        let spans = captured.spans();
        assert_eq!(spans.len(), 1);
        assert_eq!(spans[0].outcome(), Some(OUTCOME_INCOMPLETE));
    }
}
