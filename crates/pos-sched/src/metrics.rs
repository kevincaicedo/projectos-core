//! Scheduler metrics (m0-s14): queue depth, claim latency, and per-kind run
//! durations — the numbers m0-s15 turns into spans and M4 turns into the job
//! surface.
//!
//! In-process and bounded on purpose: no metric here allocates per event, no
//! histogram grows with traffic, and the per-kind map states its cap so a
//! misbehaving caller registering kinds in a loop degrades visibly into an
//! `other` bucket instead of eating the heap (L8).

use std::collections::BTreeMap;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Distinct kinds tracked separately. Beyond this, samples land in `other`
/// and `kind_overflow_count` says so.
pub const METRIC_KIND_COUNT_MAX: usize = 64;

/// Upper edges of the latency/duration buckets in milliseconds; the final
/// bucket is everything above the last edge.
const BUCKET_BOUNDS_MS: [u64; 15] = [
    1, 2, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000, 30_000, 60_000,
];

/// The bucket edges, exported so a reader can label a snapshot without
/// duplicating the constant.
#[must_use]
pub const fn latency_bucket_bounds_ms() -> [u64; 15] {
    BUCKET_BOUNDS_MS
}

/// Fixed-bucket histogram. Counts are approximate under concurrency (each
/// bucket is its own atomic) and exact in the sequential tests that assert
/// them — the honest trade for a lock-free hot path.
#[derive(Debug, Default)]
struct Histogram {
    buckets: [AtomicU64; BUCKET_BOUNDS_MS.len() + 1],
    sum_ms: AtomicU64,
    count: AtomicU64,
}

impl Histogram {
    fn observe(&self, value_ms: u64) {
        let index = BUCKET_BOUNDS_MS
            .iter()
            .position(|bound| value_ms <= *bound)
            .unwrap_or(BUCKET_BOUNDS_MS.len());
        self.buckets[index].fetch_add(1, Ordering::Relaxed);
        self.sum_ms.fetch_add(value_ms, Ordering::Relaxed);
        self.count.fetch_add(1, Ordering::Relaxed);
    }

    fn snapshot(&self) -> HistogramSnapshot {
        let mut buckets = [0_u64; BUCKET_BOUNDS_MS.len() + 1];
        for (slot, bucket) in buckets.iter_mut().zip(&self.buckets) {
            *slot = bucket.load(Ordering::Relaxed);
        }
        HistogramSnapshot {
            buckets,
            sum_ms: self.sum_ms.load(Ordering::Relaxed),
            count: self.count.load(Ordering::Relaxed),
        }
    }
}

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct HistogramSnapshot {
    pub buckets: [u64; BUCKET_BOUNDS_MS.len() + 1],
    pub sum_ms: u64,
    pub count: u64,
}

impl HistogramSnapshot {
    /// Upper bound of the bucket holding the `quantile`-th sample — a
    /// histogram cannot report an exact percentile, and pretending otherwise
    /// would be the silent-precision lie L3 forbids in its own domain.
    #[must_use]
    pub fn quantile_upper_ms(&self, quantile: f64) -> Option<u64> {
        if self.count == 0 {
            return None;
        }
        #[expect(
            clippy::cast_precision_loss,
            reason = "sample counts below 2^53 are exact in f64; this is a bucket lookup, not accounting"
        )]
        let target = (self.count as f64 * quantile).ceil().max(1.0);
        let mut cumulative = 0_u64;
        for (index, count) in self.buckets.iter().enumerate() {
            cumulative += count;
            #[expect(
                clippy::cast_precision_loss,
                reason = "same bound as above; the comparison only needs bucket resolution"
            )]
            let reached = cumulative as f64 >= target;
            if reached {
                return Some(BUCKET_BOUNDS_MS.get(index).copied().unwrap_or(u64::MAX));
            }
        }
        Some(u64::MAX)
    }
}

/// Every counter the scheduler publishes. Shared by `Arc` between the queue,
/// the cron driver, and the worker pool.
#[derive(Debug, Default)]
pub struct SchedulerMetrics {
    enqueued_total: AtomicU64,
    duplicate_enqueue_total: AtomicU64,
    claimed_total: AtomicU64,
    completed_total: AtomicU64,
    attempt_failed_total: AtomicU64,
    dead_total: AtomicU64,
    lease_reaped_total: AtomicU64,
    cron_fired_total: AtomicU64,
    cron_skipped_overlap_total: AtomicU64,
    cron_missed_tick_total: AtomicU64,
    kind_overflow_count: AtomicU64,
    queue_error_total: AtomicU64,
    claim_latency: Histogram,
    run_duration_by_kind: Mutex<BTreeMap<String, Histogram>>,
    run_duration_other: Histogram,
}

impl SchedulerMetrics {
    pub fn record_enqueued(&self) {
        self.enqueued_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_duplicate_enqueue(&self) {
        self.duplicate_enqueue_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_claim(&self, latency_ms: u64) {
        self.claimed_total.fetch_add(1, Ordering::Relaxed);
        self.claim_latency.observe(latency_ms);
    }

    pub fn record_completed(&self, kind: &str, wall_ms: u64) {
        self.completed_total.fetch_add(1, Ordering::Relaxed);
        self.observe_run(kind, wall_ms);
    }

    pub fn record_attempt_failed(&self, kind: &str, wall_ms: u64) {
        self.attempt_failed_total.fetch_add(1, Ordering::Relaxed);
        self.observe_run(kind, wall_ms);
    }

    pub fn record_dead(&self) {
        self.dead_total.fetch_add(1, Ordering::Relaxed);
    }

    /// A scheduler-level failure the pool absorbed (claim, heartbeat, reap,
    /// terminal write). Counted so "the pool kept running" never means "the
    /// pool kept failing quietly".
    pub fn record_queue_error(&self) {
        self.queue_error_total.fetch_add(1, Ordering::Relaxed);
    }

    pub fn record_lease_reaped(&self, count: u64) {
        self.lease_reaped_total.fetch_add(count, Ordering::Relaxed);
    }

    pub fn record_cron_tick(&self, fired: u64, skipped_overlap: u64, missed: u64) {
        self.cron_fired_total.fetch_add(fired, Ordering::Relaxed);
        self.cron_skipped_overlap_total
            .fetch_add(skipped_overlap, Ordering::Relaxed);
        self.cron_missed_tick_total
            .fetch_add(missed, Ordering::Relaxed);
    }

    fn observe_run(&self, kind: &str, wall_ms: u64) {
        let Ok(mut kinds) = self.run_duration_by_kind.lock() else {
            // A poisoned metrics lock must never take a scheduler down: the
            // measurement is lost, the work is not.
            self.run_duration_other.observe(wall_ms);
            return;
        };
        if let Some(histogram) = kinds.get(kind) {
            histogram.observe(wall_ms);
            return;
        }
        if kinds.len() >= METRIC_KIND_COUNT_MAX {
            drop(kinds);
            self.kind_overflow_count.fetch_add(1, Ordering::Relaxed);
            self.run_duration_other.observe(wall_ms);
            return;
        }
        kinds.entry(kind.to_owned()).or_default().observe(wall_ms);
    }

    /// A consistent-enough view for reporting: each counter is read once, so
    /// a snapshot taken mid-flight can straddle one job's transitions. Stated
    /// rather than papered over with a global lock on the hot path.
    #[must_use]
    pub fn snapshot(&self) -> SchedulerMetricsSnapshot {
        let by_kind = match self.run_duration_by_kind.lock() {
            Ok(kinds) => kinds
                .iter()
                .map(|(kind, histogram)| (kind.clone(), histogram.snapshot()))
                .collect(),
            Err(_) => BTreeMap::new(),
        };
        SchedulerMetricsSnapshot {
            enqueued_total: self.enqueued_total.load(Ordering::Relaxed),
            duplicate_enqueue_total: self.duplicate_enqueue_total.load(Ordering::Relaxed),
            claimed_total: self.claimed_total.load(Ordering::Relaxed),
            completed_total: self.completed_total.load(Ordering::Relaxed),
            attempt_failed_total: self.attempt_failed_total.load(Ordering::Relaxed),
            dead_total: self.dead_total.load(Ordering::Relaxed),
            lease_reaped_total: self.lease_reaped_total.load(Ordering::Relaxed),
            cron_fired_total: self.cron_fired_total.load(Ordering::Relaxed),
            cron_skipped_overlap_total: self.cron_skipped_overlap_total.load(Ordering::Relaxed),
            cron_missed_tick_total: self.cron_missed_tick_total.load(Ordering::Relaxed),
            kind_overflow_count: self.kind_overflow_count.load(Ordering::Relaxed),
            queue_error_total: self.queue_error_total.load(Ordering::Relaxed),
            claim_latency: self.claim_latency.snapshot(),
            run_duration_by_kind: by_kind,
            run_duration_other: self.run_duration_other.snapshot(),
        }
    }
}

#[derive(Clone, Debug, Default)]
pub struct SchedulerMetricsSnapshot {
    pub enqueued_total: u64,
    pub duplicate_enqueue_total: u64,
    pub claimed_total: u64,
    pub completed_total: u64,
    pub attempt_failed_total: u64,
    pub dead_total: u64,
    pub lease_reaped_total: u64,
    pub cron_fired_total: u64,
    pub cron_skipped_overlap_total: u64,
    pub cron_missed_tick_total: u64,
    /// Times a run duration landed in `other` because the kind cap was hit —
    /// the visible half of a bounded metric (L8).
    pub kind_overflow_count: u64,
    pub queue_error_total: u64,
    pub claim_latency: HistogramSnapshot,
    pub run_duration_by_kind: BTreeMap<String, HistogramSnapshot>,
    pub run_duration_other: HistogramSnapshot,
}

/// Queue depth by durable state, read from the projection rather than
/// counted in memory: a gauge derived from truth cannot drift from it.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct QueueDepth {
    pub queued: u64,
    pub done: u64,
    pub dead: u64,
}

impl QueueDepth {
    #[must_use]
    pub const fn from_counts(counts: [u64; 3]) -> Self {
        Self {
            queued: counts[0],
            done: counts[1],
            dead: counts[2],
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{METRIC_KIND_COUNT_MAX, SchedulerMetrics};

    #[test]
    fn run_durations_land_in_buckets_and_report_a_quantile_bound() {
        let metrics = SchedulerMetrics::default();
        for wall_ms in [1_u64, 7, 40, 900, 120_000] {
            metrics.record_completed("noop", wall_ms);
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.completed_total, 5);
        let histogram = snapshot
            .run_duration_by_kind
            .get("noop")
            .copied()
            .expect("the kind was recorded");
        assert_eq!(histogram.count, 5);
        assert_eq!(histogram.sum_ms, 1 + 7 + 40 + 900 + 120_000);
        assert_eq!(histogram.quantile_upper_ms(1.0), Some(u64::MAX));
        assert_eq!(histogram.quantile_upper_ms(0.2), Some(1));
    }

    #[test]
    fn the_kind_cap_degrades_visibly_instead_of_growing() {
        let metrics = SchedulerMetrics::default();
        for index in 0..METRIC_KIND_COUNT_MAX + 5 {
            metrics.record_completed(&format!("kind-{index}"), 10);
        }
        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.run_duration_by_kind.len(), METRIC_KIND_COUNT_MAX);
        assert_eq!(snapshot.kind_overflow_count, 5);
        assert_eq!(snapshot.run_duration_other.count, 5);
    }

    #[test]
    fn an_empty_histogram_reports_no_quantile_instead_of_zero() {
        let metrics = SchedulerMetrics::default();
        assert_eq!(
            metrics.snapshot().claim_latency.quantile_upper_ms(0.95),
            None
        );
    }
}
