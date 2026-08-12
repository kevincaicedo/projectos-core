//! Retry backoff and its jitter source (m0-s14).
//!
//! Full jitter (`sleep ∈ [0, min(cap, base·factor^attempt)]`) rather than
//! plain exponential: a fleet of workers that all failed on the same
//! provider outage must not retry in lockstep, and the uniform draw is the
//! variant with the best measured recovery behaviour in the literature the
//! master plan's retry row cites.

use std::sync::atomic::{AtomicU64, Ordering};

/// First retry window (master plan §16 / m0-s14 task list).
pub const JOB_BACKOFF_BASE_MS_DEFAULT: u64 = 2_000;
/// Growth factor per attempt.
pub const JOB_BACKOFF_FACTOR_DEFAULT: u32 = 2;
/// Ceiling on any single retry window: 15 minutes. Beyond this the delay is
/// no longer congestion control, it is an outage nobody was told about.
pub const JOB_BACKOFF_CAP_MS_DEFAULT: u64 = 15 * 60 * 1_000;

/// Exponent bound. `2^32` already saturates any plausible cap; clamping keeps
/// the shift total instead of relying on `checked_pow` at every call site.
const BACKOFF_EXPONENT_MAX: u32 = 32;

#[derive(Clone, Copy, Debug)]
pub struct BackoffPolicy {
    pub base_ms: u64,
    pub factor: u32,
    pub cap_ms: u64,
}

impl Default for BackoffPolicy {
    fn default() -> Self {
        Self {
            base_ms: JOB_BACKOFF_BASE_MS_DEFAULT,
            factor: JOB_BACKOFF_FACTOR_DEFAULT,
            cap_ms: JOB_BACKOFF_CAP_MS_DEFAULT,
        }
    }
}

impl BackoffPolicy {
    /// Deterministic upper bound of the window for `attempt_index` (1-based).
    /// Exposed so tests can assert the envelope without asserting the draw.
    #[must_use]
    pub fn window_ms(&self, attempt_index: u32) -> u64 {
        let exponent = attempt_index.saturating_sub(1).min(BACKOFF_EXPONENT_MAX);
        let growth = u64::from(self.factor)
            .checked_pow(exponent)
            .unwrap_or(u64::MAX);
        self.base_ms.saturating_mul(growth).min(self.cap_ms)
    }

    /// The actual delay: a uniform draw inside the window.
    #[must_use]
    pub fn delay_ms(&self, attempt_index: u32, jitter: &dyn JitterSource) -> u64 {
        let window = self.window_ms(attempt_index);
        jitter.sample_below(window.saturating_add(1))
    }
}

/// Where jitter comes from. Injected like the clock so a retry schedule is
/// reproducible in tests and unpredictable in production.
pub trait JitterSource: Send + Sync {
    /// Uniform in `[0, bound_exclusive)`; `0` when the bound is zero.
    fn sample_below(&self, bound_exclusive: u64) -> u64;
}

/// The production source: SplitMix64 seeded once from OS entropy.
///
/// Deliberately not a cryptographic generator and deliberately not a syscall
/// per draw — jitter only has to decorrelate retries, and a per-retry
/// `getrandom` would be a syscall on the failure path of every job in a
/// stampede. Seeding from the OS is what keeps two processes from sharing a
/// schedule.
pub struct SplitMixJitter {
    state: AtomicU64,
}

impl SplitMixJitter {
    /// Seeds from OS entropy, falling back to a fixed seed if the syscall
    /// refuses. A degraded seed weakens decorrelation; it can never affect
    /// correctness, so refusing to schedule retries would be the worse trade.
    #[must_use]
    pub fn from_os_entropy() -> Self {
        let mut seed = [0_u8; 8];
        let seed = match getrandom::fill(&mut seed) {
            Ok(()) => u64::from_le_bytes(seed),
            Err(_) => 0x9E37_79B9_7F4A_7C15,
        };
        Self::from_seed(seed)
    }

    #[must_use]
    pub const fn from_seed(seed: u64) -> Self {
        Self {
            state: AtomicU64::new(seed),
        }
    }

    fn next_u64(&self) -> u64 {
        // SplitMix64 (Steele et al.), the standard seeding generator: fast,
        // stateless apart from the counter, and good enough for jitter.
        let z = self
            .state
            .fetch_add(0x9E37_79B9_7F4A_7C15, Ordering::Relaxed)
            .wrapping_add(0x9E37_79B9_7F4A_7C15);
        let z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        let z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_1331_11EB);
        z ^ (z >> 31)
    }
}

impl JitterSource for SplitMixJitter {
    fn sample_below(&self, bound_exclusive: u64) -> u64 {
        if bound_exclusive == 0 {
            return 0;
        }
        // Modulo bias is immaterial for a delay in milliseconds and the
        // alternative (rejection sampling) buys nothing here.
        self.next_u64() % bound_exclusive
    }
}

/// The deterministic source: always the top of the window. Tests that assert
/// exact retry instants use it, and it is the worst case for latency, so a
/// suite written against it never accidentally depends on a lucky small draw.
pub struct NoJitter;

impl JitterSource for NoJitter {
    fn sample_below(&self, bound_exclusive: u64) -> u64 {
        bound_exclusive.saturating_sub(1)
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BackoffPolicy, JOB_BACKOFF_CAP_MS_DEFAULT, JitterSource, NoJitter, SplitMixJitter,
    };

    #[test]
    fn the_window_grows_exponentially_and_stops_at_the_cap() {
        let policy = BackoffPolicy::default();
        assert_eq!(policy.window_ms(1), 2_000);
        assert_eq!(policy.window_ms(2), 4_000);
        assert_eq!(policy.window_ms(3), 8_000);
        assert_eq!(policy.window_ms(20), JOB_BACKOFF_CAP_MS_DEFAULT);
        // No overflow panic at absurd attempt indexes.
        assert_eq!(policy.window_ms(u32::MAX), JOB_BACKOFF_CAP_MS_DEFAULT);
    }

    #[test]
    fn full_jitter_stays_inside_the_window_and_varies() {
        let policy = BackoffPolicy::default();
        let jitter = SplitMixJitter::from_seed(42);
        let mut seen = std::collections::BTreeSet::new();
        for _ in 0..64 {
            let delay = policy.delay_ms(4, &jitter);
            assert!(delay <= policy.window_ms(4));
            seen.insert(delay);
        }
        assert!(seen.len() > 1, "full jitter must not be constant");
        assert_eq!(policy.delay_ms(4, &NoJitter), policy.window_ms(4));
    }

    #[test]
    fn a_zero_bound_never_divides_by_zero() {
        assert_eq!(SplitMixJitter::from_seed(1).sample_below(0), 0);
        assert_eq!(NoJitter.sample_below(0), 0);
    }
}
