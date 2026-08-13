//! The emission half: `tracing` spans out, the process sink registry in.
//!
//! `tracing` is the master plan §20 observability facade, and it is used here
//! for exactly what a facade is for — every ProjectOS span is a real
//! `tracing` span, at the taxonomy stem, carrying its trace/parent ids and
//! its typed fields, so any `tracing`-aware tool (a development `fmt` layer,
//! the M1 OTLP bridge) sees the same tree ProjectOS's own sink sees.
//!
//! What it deliberately is **not** used for is storage. Our parents are
//! explicit ids that must survive a process boundary (module invariant 3),
//! and the typed field set already lives in the [`super::Span`] handle, so
//! routing finished spans through a subscriber registry would mean encoding
//! typed values, decoding them back, and maintaining a second notion of
//! "parent" that disagrees with the first exactly when a Run resumes. The
//! encode side is therefore one-way, and `tracing-subscriber` is not a
//! dependency.

use super::sink::{
    CaptureHandle, FinishedSpan, JsonLinesSink, NullSink, SpanSink, TelemetryConfig,
    TelemetryError, TelemetryExport, TelemetryStats,
};
use super::{
    SPAN_FIELD_COUNT_MAX, SpanContext, SpanDetail, SpanField, SpanId, SpanName, SpanValue,
};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock, RwLock, Weak};

/// One `tracing` target for the whole taxonomy, so an operator filters
/// ProjectOS spans with `pos=info` and nothing else has to be guessed.
const TARGET: &str = "pos";

/// How many concurrent trace captures the process keeps. Captures are a test
/// mechanism; the cap stops a leak from growing without bound (L8).
const CAPTURE_COUNT_MAX: usize = 256;

static ENABLED: AtomicBool = AtomicBool::new(false);
static PRIMARY_RECORDS: AtomicBool = AtomicBool::new(false);
static CAPTURE_COUNT: AtomicUsize = AtomicUsize::new(0);
static SPANS_EXPORTED: AtomicU64 = AtomicU64::new(0);
static FIELDS_DROPPED: AtomicU64 = AtomicU64::new(0);

type CaptureSlot = (Option<super::TraceId>, Weak<Mutex<Vec<FinishedSpan>>>);

fn primary() -> &'static RwLock<Arc<dyn SpanSink>> {
    static PRIMARY: OnceLock<RwLock<Arc<dyn SpanSink>>> = OnceLock::new();
    PRIMARY.get_or_init(|| RwLock::new(Arc::new(NullSink)))
}

fn captures() -> &'static Mutex<Vec<CaptureSlot>> {
    static CAPTURES: OnceLock<Mutex<Vec<CaptureSlot>>> = OnceLock::new();
    CAPTURES.get_or_init(|| Mutex::new(Vec::new()))
}

/// One relaxed atomic load. This is what makes it acceptable to instrument
/// the dispatch path of every query (module invariant 5).
pub(super) fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn refresh_enabled() {
    let live = PRIMARY_RECORDS.load(Ordering::Relaxed) || CAPTURE_COUNT.load(Ordering::Relaxed) > 0;
    ENABLED.store(live, Ordering::Relaxed);
}

pub(super) fn install(config: TelemetryConfig) -> Result<(), TelemetryError> {
    let (sink, records): (Arc<dyn SpanSink>, bool) = match config.export {
        TelemetryExport::Off => (Arc::new(NullSink), false),
        TelemetryExport::Stderr => (Arc::new(JsonLinesSink::to_stderr()), true),
        TelemetryExport::JsonLinesFile(path) => (Arc::new(JsonLinesSink::to_file(&path)?), true),
        TelemetryExport::Otlp { .. } => {
            // Capability honesty, the m0-s06 pattern: the target is a
            // registered, typed configuration that refuses and names its
            // owner, never a silent no-op that reads as "exporting". The
            // endpoint is not echoed — a collector URL can carry a token.
            return Err(TelemetryError {
                code: "not_yet_supported",
                message: "OTLP export is configured but not implemented yet; it lands with \
                          m1-s03, alongside the reviewed TLS transport it shares a supply-chain \
                          review with (M0-E7 design record §2.3)"
                    .to_owned(),
            });
        }
    };
    match primary().write() {
        Ok(mut slot) => *slot = sink,
        Err(_) => {
            return Err(TelemetryError {
                code: "telemetry_unavailable",
                message: "the telemetry sink lock was poisoned by an earlier panic".to_owned(),
            });
        }
    }
    PRIMARY_RECORDS.store(records, Ordering::Relaxed);
    refresh_enabled();
    Ok(())
}

pub(super) fn capture(trace: Option<super::TraceId>) -> CaptureHandle {
    let spans = Arc::new(Mutex::new(Vec::new()));
    if let Ok(mut slots) = captures().lock() {
        slots.retain(|(_, weak)| weak.strong_count() > 0);
        if slots.len() < CAPTURE_COUNT_MAX {
            slots.push((trace, Arc::downgrade(&spans)));
            CAPTURE_COUNT.store(slots.len(), Ordering::Relaxed);
        }
    }
    refresh_enabled();
    CaptureHandle { spans }
}

pub(super) fn stats() -> TelemetryStats {
    let lines_dropped = primary()
        .read()
        .ok()
        .and_then(|sink| sink.dropped_line_count())
        .unwrap_or(0);
    TelemetryStats {
        spans_exported: SPANS_EXPORTED.load(Ordering::Relaxed),
        fields_dropped: FIELDS_DROPPED.load(Ordering::Relaxed),
        lines_dropped,
    }
}

/// Hands one finished span to the primary sink and to every live capture for
/// its trace.
pub(super) fn export(span: &FinishedSpan) {
    SPANS_EXPORTED.fetch_add(1, Ordering::Relaxed);
    FIELDS_DROPPED.fetch_add(u64::from(span.dropped_field_count()), Ordering::Relaxed);
    if let Ok(sink) = primary().read() {
        sink.export(span);
    }
    let Ok(mut slots) = captures().lock() else {
        return;
    };
    slots.retain(|(_, weak)| weak.strong_count() > 0);
    for (trace, weak) in slots.iter() {
        if trace.is_some_and(|wanted| wanted != span.trace) {
            continue;
        }
        if let Some(buffer) = weak.upgrade()
            && let Ok(mut spans) = buffer.lock()
        {
            spans.push(span.clone());
        }
    }
    CAPTURE_COUNT.store(slots.len(), Ordering::Relaxed);
}

/// The declared field vocabulary, repeated per callsite because `tracing`
/// fixes a span's name and its field names at the callsite. One macro so the
/// six stems cannot drift apart.
macro_rules! taxonomy_span {
    ($stem:literal) => {
        tracing::span!(
            target: TARGET,
            tracing::Level::INFO,
            $stem,
            detail = tracing::field::Empty,
            trace_hi = tracing::field::Empty,
            trace_lo = tracing::field::Empty,
            span_id = tracing::field::Empty,
            parent_id = tracing::field::Empty,
            project_hi = tracing::field::Empty,
            project_lo = tracing::field::Empty,
            run_hi = tracing::field::Empty,
            run_lo = tracing::field::Empty,
            job_hi = tracing::field::Empty,
            job_lo = tracing::field::Empty,
            evidence_hi = tracing::field::Empty,
            evidence_lo = tracing::field::Empty,
            step_index = tracing::field::Empty,
            attempt = tracing::field::Empty,
            tokens_in = tracing::field::Empty,
            tokens_out = tracing::field::Empty,
            frames = tracing::field::Empty,
            rows = tracing::field::Empty,
            duration_ms = tracing::field::Empty,
            effect_class = tracing::field::Empty,
            tier = tracing::field::Empty,
            credential_class = tracing::field::Empty,
            outcome = tracing::field::Empty,
        )
    };
}

pub(super) fn new_span(
    name: SpanName,
    detail: SpanDetail,
    context: SpanContext,
    parent: Option<SpanId>,
) -> tracing::Span {
    // `parent: None` is deliberate: ProjectOS parents are explicit ids that
    // survive a process boundary, and letting `tracing` also infer one from
    // its thread-local stack would give a resumed Run two disagreeing parents.
    let span = match name {
        SpanName::ApiCommand => taxonomy_span!("api.cmd"),
        SpanName::ApiQuery => taxonomy_span!("api.query"),
        SpanName::ApiStream => taxonomy_span!("api.stream"),
        SpanName::AgentsStep => taxonomy_span!("agents.step"),
        SpanName::GatewayCall => taxonomy_span!("gateway.call"),
        SpanName::SchedJob => taxonomy_span!("sched.job"),
        SpanName::IngestStage => taxonomy_span!("ingest.stage"),
    };
    span.record("detail", detail.as_str());
    let trace = context.trace.into_bytes();
    span.record("trace_hi", u64_at(&trace, 0));
    span.record("trace_lo", u64_at(&trace, 8));
    span.record("span_id", u64::from_be_bytes(context.span.into_bytes()));
    span.record(
        "parent_id",
        parent.map_or(0, |id| u64::from_be_bytes(id.into_bytes())),
    );
    span
}

/// Re-stamps the `tracing` mirror after [`super::Span::adopt_root`], so the
/// mirror and the ProjectOS sink never disagree about which trace a span is
/// in. `record` overwrites, so the last value is the one a subscriber sees.
pub(super) fn rewrite_identity(span: &tracing::Span, context: SpanContext) {
    let trace = context.trace.into_bytes();
    span.record("trace_hi", u64_at(&trace, 0));
    span.record("trace_lo", u64_at(&trace, 8));
    span.record("span_id", u64::from_be_bytes(context.span.into_bytes()));
    span.record("parent_id", 0_u64);
}

/// Mirrors one typed field into the `tracing` span. Ids travel as two `u64`
/// halves rather than as a rendered string: no allocation, and the only
/// `&str` a span field can carry stays a `&'static str` label.
pub(super) fn record(span: &tracing::Span, field: SpanField, value: SpanValue) {
    match (field, value) {
        (SpanField::Project, SpanValue::Id(bytes)) => {
            span.record("project_hi", u64_at(&bytes, 0));
            span.record("project_lo", u64_at(&bytes, 8));
        }
        (SpanField::Run, SpanValue::Id(bytes)) => {
            span.record("run_hi", u64_at(&bytes, 0));
            span.record("run_lo", u64_at(&bytes, 8));
        }
        (SpanField::Job, SpanValue::Id(bytes)) => {
            span.record("job_hi", u64_at(&bytes, 0));
            span.record("job_lo", u64_at(&bytes, 8));
        }
        (SpanField::Evidence, SpanValue::Id(bytes)) => {
            span.record("evidence_hi", u64_at(&bytes, 0));
            span.record("evidence_lo", u64_at(&bytes, 8));
        }
        (SpanField::StepIndex, SpanValue::Count(count)) => {
            span.record("step_index", count);
        }
        (SpanField::Attempt, SpanValue::Count(count)) => {
            span.record("attempt", count);
        }
        (SpanField::TokensIn, SpanValue::Count(count)) => {
            span.record("tokens_in", count);
        }
        (SpanField::TokensOut, SpanValue::Count(count)) => {
            span.record("tokens_out", count);
        }
        (SpanField::Frames, SpanValue::Count(count)) => {
            span.record("frames", count);
        }
        (SpanField::Rows, SpanValue::Count(count)) => {
            span.record("rows", count);
        }
        (SpanField::DurationMs, SpanValue::Millis(millis)) => {
            span.record("duration_ms", millis);
        }
        (SpanField::EffectClass, SpanValue::Label(label)) => {
            span.record("effect_class", label);
        }
        (SpanField::Tier, SpanValue::Label(label)) => {
            span.record("tier", label);
        }
        (SpanField::CredentialClass, SpanValue::Label(label)) => {
            span.record("credential_class", label);
        }
        (SpanField::Outcome, SpanValue::Label(label)) => {
            span.record("outcome", label);
        }
        // A key/value shape the taxonomy does not pair (a count in a label
        // slot) is not mirrored. The typed field set still carries it, so the
        // ProjectOS sink stays complete and only the `tracing` mirror is
        // partial — the direction that cannot lose data.
        _ => {}
    }
}

fn u64_at(bytes: &[u8; 16], offset: usize) -> u64 {
    let mut chunk = [0_u8; 8];
    chunk.copy_from_slice(&bytes[offset..offset + 8]);
    u64::from_be_bytes(chunk)
}

/// The bounded, ordered field set a live span accumulates.
#[derive(Clone, Copy)]
pub(super) struct FieldSet {
    entries: [Option<(SpanField, SpanValue)>; SPAN_FIELD_COUNT_MAX],
    len: usize,
    dropped: u16,
}

impl Default for FieldSet {
    fn default() -> Self {
        Self {
            entries: [None; SPAN_FIELD_COUNT_MAX],
            len: 0,
            dropped: 0,
        }
    }
}

impl FieldSet {
    /// Re-setting a key overwrites it in place, so the outcome recorded at
    /// `finish` replaces any provisional one instead of consuming a slot.
    pub(super) fn set(&mut self, field: SpanField, value: SpanValue) {
        for slot in self.entries.iter_mut().take(self.len) {
            if let Some((key, existing)) = slot
                && *key == field
            {
                *existing = value;
                return;
            }
        }
        if self.len == SPAN_FIELD_COUNT_MAX {
            self.dropped = self.dropped.saturating_add(1);
            return;
        }
        self.entries[self.len] = Some((field, value));
        self.len += 1;
    }

    /// Sets a key only when it is absent. `close` uses this so a stated
    /// outcome is never overwritten by the "incomplete" default.
    pub(super) fn set_if_absent(&mut self, field: SpanField, value: SpanValue) {
        let present = self
            .entries
            .iter()
            .take(self.len)
            .flatten()
            .any(|(key, _)| *key == field);
        if !present {
            self.set(field, value);
        }
    }

    pub(super) const fn into_parts(
        self,
    ) -> ([Option<(SpanField, SpanValue)>; SPAN_FIELD_COUNT_MAX], u16) {
        (self.entries, self.dropped)
    }
}
