//! The injected wall clock (m0-s03). Domain code never reads `SystemTime`
//! directly (AGENTS.md non-negotiable); it receives a `&dyn WallClock` from
//! its caller so tests and replay stay deterministic. This module is the one
//! blessed place in core that touches the operating-system clock.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch. Informational only in event envelopes:
/// ordering is `seq`/`lamport`, never `ts_ms` (event-sourcing skill).
pub trait WallClock: Send + Sync {
    fn now_ms(&self) -> u64;
}

/// The production clock.
#[derive(Clone, Copy, Debug, Default)]
pub struct SystemWallClock;

impl WallClock for SystemWallClock {
    fn now_ms(&self) -> u64 {
        // A host clock before 1970 or beyond u64 milliseconds is a broken
        // environment; `ts_ms` is informational, so saturating beats a panic
        // path that could take an append down with it.
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_or(0, |elapsed| {
                u64::try_from(elapsed.as_millis()).unwrap_or(u64::MAX)
            }) // INVARIANT: try_from only fails past year ~584M; saturation is the documented policy.
    }
}

/// A deterministic clock for tests and property suites: starts at a fixed
/// instant and only moves when told to, so replayed fixtures are stable.
#[derive(Debug, Default)]
pub struct ManualWallClock {
    now_ms: AtomicU64,
}

impl ManualWallClock {
    #[must_use]
    pub fn starting_at(now_ms: u64) -> Self {
        Self {
            now_ms: AtomicU64::new(now_ms),
        }
    }

    pub fn advance_ms(&self, delta_ms: u64) {
        self.now_ms.fetch_add(delta_ms, Ordering::SeqCst);
    }
}

impl WallClock for ManualWallClock {
    fn now_ms(&self) -> u64 {
        self.now_ms.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::{ManualWallClock, SystemWallClock, WallClock};

    #[test]
    fn manual_clock_only_moves_when_advanced() {
        let clock = ManualWallClock::starting_at(1_000);
        assert_eq!(clock.now_ms(), 1_000);
        assert_eq!(clock.now_ms(), 1_000);
        clock.advance_ms(250);
        assert_eq!(clock.now_ms(), 1_250);
    }

    #[test]
    fn system_clock_reports_a_post_epoch_instant() {
        // 2020-01-01 in ms; a machine reporting earlier than this is broken.
        assert!(SystemWallClock.now_ms() > 1_577_836_800_000);
    }
}
