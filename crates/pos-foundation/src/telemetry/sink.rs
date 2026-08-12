//! What a finished span *is*, and the three places one can go: nowhere
//! (the default), a bounded JSON-lines stream, or a test capture.
//!
//! The OTLP wire exporter is deliberately absent — see the M0-E7 design
//! record §2.3. Importing an OTLP client would give core a TLS-capable HTTP
//! stack and silently destroy the m0-s10 property that core is *structurally*
//! incapable of cloud egress, for a feature no M0 acceptance criterion asks
//! for. [`TelemetryExport::Otlp`] is registered and refuses honestly, naming
//! the story that implements it.

use super::{
    OUTCOME_INCOMPLETE, SPAN_FIELD_COUNT_MAX, SpanDetail, SpanField, SpanId, SpanName, SpanValue,
    TraceId,
};
use std::fmt;
use std::io::Write;
use std::path::PathBuf;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

/// Longest serialized span line. A span carries ids, counts and static
/// labels, so the real lines are ~200 bytes; the cap exists so a future field
/// cannot turn the sink into an unbounded writer (L8).
pub const SPAN_LINE_LEN_MAX: usize = 1024;

/// One completed span, as the sink sees it. Fixed-size: a span allocates its
/// box once and never grows a field vector.
#[derive(Clone)]
pub struct FinishedSpan {
    pub trace: TraceId,
    pub span: SpanId,
    pub parent: Option<SpanId>,
    pub name: SpanName,
    pub detail: SpanDetail,
    /// Wall-clock start, milliseconds since the Unix epoch. Read from the
    /// system clock rather than an injected one — a span measures the process
    /// (module invariant 4).
    pub start_unix_ms: u64,
    pub duration_us: u64,
    pub(crate) fields: [Option<(SpanField, SpanValue)>; SPAN_FIELD_COUNT_MAX],
    pub(crate) dropped_field_count: u16,
}

impl FinishedSpan {
    /// The taxonomy name the milestone specifies (`api.cmd/project.create`).
    /// `tracing` fixes a span's name at its callsite, so this is where the
    /// stem and the detail are rejoined — one function, tested.
    #[must_use]
    pub fn taxonomy_name(&self) -> String {
        if self.detail.is_empty() {
            self.name.stem().to_owned()
        } else {
            format!("{}/{}", self.name.stem(), self.detail.as_str())
        }
    }

    #[must_use]
    pub fn field(&self, field: SpanField) -> Option<SpanValue> {
        self.fields
            .iter()
            .flatten()
            .find(|(key, _)| *key == field)
            .map(|(_, value)| *value)
    }

    /// The outcome label. Every span has one: a span that ended without a
    /// stated outcome reports [`OUTCOME_INCOMPLETE`], never nothing.
    #[must_use]
    pub fn outcome(&self) -> Option<&'static str> {
        match self.field(SpanField::Outcome) {
            Some(SpanValue::Label(label)) => Some(label),
            _ => Some(OUTCOME_INCOMPLETE),
        }
    }

    /// Fields in the order they were recorded.
    pub fn fields(&self) -> impl Iterator<Item = (SpanField, SpanValue)> + '_ {
        self.fields.iter().flatten().copied()
    }

    /// How many fields this span could not carry. Visible, not silent (L8).
    #[must_use]
    pub const fn dropped_field_count(&self) -> u16 {
        self.dropped_field_count
    }

    /// One canonical JSON object. Every value is an id, a number, a boolean,
    /// or a static label, so this writer cannot render content it was not
    /// statically given.
    #[must_use]
    pub fn to_json_line(&self) -> String {
        let mut json = String::with_capacity(256);
        json.push_str("{\"trace\":\"");
        json.push_str(&self.trace.to_hex());
        json.push_str("\",\"span\":\"");
        json.push_str(&self.span.to_hex());
        json.push_str("\",\"parent\":");
        match self.parent {
            Some(parent) => {
                json.push('"');
                json.push_str(&parent.to_hex());
                json.push('"');
            }
            None => json.push_str("null"),
        }
        json.push_str(",\"name\":");
        push_json_string(&mut json, &self.taxonomy_name());
        json.push_str(",\"startUnixMs\":");
        json.push_str(&self.start_unix_ms.to_string());
        json.push_str(",\"durationUs\":");
        json.push_str(&self.duration_us.to_string());
        json.push_str(",\"droppedFields\":");
        json.push_str(&self.dropped_field_count.to_string());
        json.push_str(",\"fields\":{");
        for (index, (field, value)) in self.fields().enumerate() {
            if index > 0 {
                json.push(',');
            }
            push_json_string(&mut json, field.key());
            json.push(':');
            push_value(&mut json, value);
        }
        json.push_str("}}");
        json
    }
}

impl fmt::Debug for FinishedSpan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.to_json_line())
    }
}

fn push_value(json: &mut String, value: SpanValue) {
    match value {
        SpanValue::Id(bytes) => {
            json.push('"');
            for byte in bytes {
                use fmt::Write as _;
                let _ = write!(json, "{byte:02x}");
            }
            json.push('"');
        }
        SpanValue::Count(count) => json.push_str(&count.to_string()),
        SpanValue::Millis(millis) => json.push_str(&millis.to_string()),
        SpanValue::Flag(flag) => json.push_str(if flag { "true" } else { "false" }),
        SpanValue::Label(label) => push_json_string(json, label),
    }
}

fn push_json_string(json: &mut String, value: &str) {
    json.push('"');
    for character in value.chars() {
        match character {
            '"' => json.push_str("\\\""),
            '\\' => json.push_str("\\\\"),
            '\n' => json.push_str("\\n"),
            '\r' => json.push_str("\\r"),
            '\t' => json.push_str("\\t"),
            control if control < ' ' => {
                use fmt::Write as _;
                let _ = write!(json, "\\u{:04x}", u32::from(control));
            }
            other => json.push(other),
        }
    }
    json.push('"');
}

/// Where finished spans go. Implementations must not block for long: the
/// caller is on the dispatch path of whatever it measured.
pub trait SpanSink: Send + Sync {
    fn export(&self, span: &FinishedSpan);

    /// Lines this sink refused to write. `None` for sinks that cannot lose
    /// one; the pipeline reports it through [`TelemetryStats`] so a bounded
    /// writer never reads as a complete one (L8).
    fn dropped_line_count(&self) -> Option<u64> {
        None
    }
}

/// The default. Costs nothing and keeps the shipped desktop honest about
/// sending no telemetry anywhere (L4 spirit).
#[derive(Clone, Copy, Debug, Default)]
pub struct NullSink;

impl SpanSink for NullSink {
    fn export(&self, _span: &FinishedSpan) {}
}

/// One bounded JSON object per line, to a file or to stderr. This is the M1
/// pipeline-debugging story, and it is the artifact the m0-s15 secret/content
/// scan sweeps — a scan over output that does not exist proves nothing.
pub struct JsonLinesSink {
    writer: Mutex<Box<dyn Write + Send>>,
    dropped_line_count: AtomicU64,
}

impl JsonLinesSink {
    #[must_use]
    pub fn new(writer: Box<dyn Write + Send>) -> Self {
        Self {
            writer: Mutex::new(writer),
            dropped_line_count: AtomicU64::new(0),
        }
    }

    /// Opens (creating, appending) the target file.
    ///
    /// # Errors
    ///
    /// [`TelemetryError`] when the path cannot be opened for append.
    pub fn to_file(path: &std::path::Path) -> Result<Self, TelemetryError> {
        let file = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .map_err(|error| TelemetryError {
                code: "telemetry_target_unavailable",
                message: format!("could not open the span log for append: {error}"),
            })?;
        Ok(Self::new(Box::new(file)))
    }

    #[must_use]
    pub fn to_stderr() -> Self {
        Self::new(Box::new(std::io::stderr()))
    }

    #[must_use]
    pub fn dropped_lines(&self) -> u64 {
        self.dropped_line_count.load(Ordering::Relaxed)
    }
}

impl SpanSink for JsonLinesSink {
    fn export(&self, span: &FinishedSpan) {
        let line = span.to_json_line();
        if line.len() > SPAN_LINE_LEN_MAX {
            // Truncating a JSON object produces malformed JSON, which reads as
            // corruption downstream. Dropping and counting is the honest
            // degradation (L8).
            self.dropped_line_count.fetch_add(1, Ordering::Relaxed);
            return;
        }
        let Ok(mut writer) = self.writer.lock() else {
            self.dropped_line_count.fetch_add(1, Ordering::Relaxed);
            return;
        };
        if writeln!(writer, "{line}").is_err() {
            // A telemetry write failure must never take down the work it was
            // measuring; it is counted and reported through `stats()`.
            self.dropped_line_count.fetch_add(1, Ordering::Relaxed);
        }
    }

    fn dropped_line_count(&self) -> Option<u64> {
        Some(self.dropped_lines())
    }
}

/// An in-memory capture for the span-tree oracles.
#[derive(Clone)]
pub struct CaptureHandle {
    // Which traces reach this buffer is decided at registration; the handle
    // itself only needs the buffer. A capture may be scoped to one trace, or
    // (for a test whose trace id is derived from an id the work itself mints,
    // like a Run's) capture everything and scope afterwards.
    pub(crate) spans: Arc<Mutex<Vec<FinishedSpan>>>,
}

impl CaptureHandle {
    /// Every span this capture received, in close order.
    #[must_use]
    pub fn spans(&self) -> Vec<FinishedSpan> {
        self.spans
            .lock()
            .map(|spans| spans.clone())
            .unwrap_or_default()
    }

    /// Only the spans of one trace — how a capture-everything handle scopes
    /// itself to its own work when tests share a process.
    #[must_use]
    pub fn spans_in(&self, trace: TraceId) -> Vec<FinishedSpan> {
        self.spans()
            .into_iter()
            .filter(|span| span.trace == trace)
            .collect()
    }

    /// The root span of a trace — the one with no parent.
    #[must_use]
    pub fn root(&self, trace: TraceId) -> Option<FinishedSpan> {
        self.spans_in(trace)
            .into_iter()
            .find(|span| span.parent.is_none())
    }

    /// `Ok(())` when this trace's spans form one connected tree rooted at a
    /// single parentless span: exactly the m0-s15 acceptance criterion, as a
    /// reusable assertion rather than a bespoke check per shell.
    ///
    /// # Errors
    ///
    /// A human-readable reason naming the first defect found.
    pub fn assert_single_connected_tree(&self, trace: TraceId) -> Result<(), String> {
        let spans = self.spans_in(trace);
        if spans.is_empty() {
            return Err("no spans were captured for this trace".to_owned());
        }
        let ids: Vec<SpanId> = spans.iter().map(|span| span.span).collect();
        let roots: Vec<&FinishedSpan> = spans.iter().filter(|span| span.parent.is_none()).collect();
        if roots.len() != 1 {
            return Err(format!(
                "expected exactly one root span, found {} among [{}]",
                roots.len(),
                spans
                    .iter()
                    .map(FinishedSpan::taxonomy_name)
                    .collect::<Vec<_>>()
                    .join(", ")
            ));
        }
        for span in &spans {
            if let Some(parent) = span.parent
                && !ids.contains(&parent)
            {
                return Err(format!(
                    "{} is parented to a span outside the capture",
                    span.taxonomy_name()
                ));
            }
        }
        Ok(())
    }
}

/// Where the pipeline sends finished spans.
#[derive(Clone, Debug, Default)]
pub enum TelemetryExport {
    /// The shipped default on every shell.
    #[default]
    Off,
    Stderr,
    JsonLinesFile(PathBuf),
    /// Registered, not implemented. See the module doc and the M0-E7 design
    /// record §2.3; the wire exporter lands with m1-s03's reviewed TLS
    /// transport, which it shares a supply-chain review with.
    Otlp {
        endpoint: String,
    },
}

/// Typed startup configuration. Shells build this from their own config
/// surface; there is no environment-variable magic inside core.
#[derive(Clone, Debug, Default)]
pub struct TelemetryConfig {
    pub export: TelemetryExport,
}

impl TelemetryConfig {
    #[must_use]
    pub const fn off() -> Self {
        Self {
            export: TelemetryExport::Off,
        }
    }

    #[must_use]
    pub const fn stderr() -> Self {
        Self {
            export: TelemetryExport::Stderr,
        }
    }

    #[must_use]
    pub const fn json_lines(path: PathBuf) -> Self {
        Self {
            export: TelemetryExport::JsonLinesFile(path),
        }
    }
}

/// The typed refusal envelope, shaped like `pos-api`'s so a shell can forward
/// it without inventing a second error vocabulary.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct TelemetryError {
    pub code: &'static str,
    pub message: String,
}

impl fmt::Display for TelemetryError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for TelemetryError {}

/// What the pipeline itself reports about its own losses.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TelemetryStats {
    pub spans_exported: u64,
    pub fields_dropped: u64,
    pub lines_dropped: u64,
}

#[cfg(test)]
mod tests {
    use super::{FinishedSpan, JsonLinesSink, SPAN_LINE_LEN_MAX, SpanSink};
    use crate::telemetry::{
        SPAN_FIELD_COUNT_MAX, SpanDetail, SpanField, SpanId, SpanName, SpanValue, TraceId,
    };

    fn span() -> FinishedSpan {
        let mut fields = [None; SPAN_FIELD_COUNT_MAX];
        fields[0] = Some((SpanField::Project, SpanValue::Id([0xab; 16])));
        fields[1] = Some((SpanField::TokensIn, SpanValue::Count(41)));
        fields[2] = Some((SpanField::Outcome, SpanValue::Label("ok")));
        FinishedSpan {
            trace: TraceId::from_bytes([1; 16]),
            span: SpanId::from_bytes([2; 8]),
            parent: Some(SpanId::from_bytes([3; 8])),
            name: SpanName::GatewayCall,
            detail: SpanDetail::from_static("openai-compatible"),
            start_unix_ms: 1_700_000_000_000,
            duration_us: 12_345,
            fields,
            dropped_field_count: 0,
        }
    }

    #[test]
    fn a_span_line_is_canonical_json_with_only_ids_numbers_and_labels() {
        let json = span().to_json_line();
        assert!(json.starts_with("{\"trace\":\"01010101"));
        assert!(json.contains("\"name\":\"gateway.call/openai-compatible\""));
        assert!(json.contains("\"project\":\"abababababababababababababababab\""));
        assert!(json.contains("\"tokens_in\":41"));
        assert!(json.contains("\"outcome\":\"ok\""));
        assert!(json.ends_with("}}"));
        assert!(json.len() <= SPAN_LINE_LEN_MAX);
    }

    #[test]
    fn an_oversized_line_is_dropped_and_counted_rather_than_truncated() {
        // A truncated JSON object reads as corruption downstream; the sink
        // refuses to emit one and says how often it refused.
        let sink = JsonLinesSink::new(Box::new(std::io::sink()));
        let mut oversized = span();
        oversized.detail = SpanDetail::from_static(
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        );
        for slot in &mut oversized.fields {
            *slot = Some((
                SpanField::EffectClass,
                SpanValue::Label("x".repeat(200).leak()),
            ));
        }
        sink.export(&oversized);
        assert_eq!(sink.dropped_lines(), 1);
        sink.export(&span());
        assert_eq!(sink.dropped_lines(), 1, "a normal line still writes");
    }

    #[test]
    fn a_span_without_a_stated_outcome_reports_incomplete() {
        let mut bare = span();
        bare.fields = [None; SPAN_FIELD_COUNT_MAX];
        assert_eq!(bare.outcome(), Some(super::OUTCOME_INCOMPLETE));
    }
}
