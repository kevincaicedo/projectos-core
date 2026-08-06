//! Injected fault points (m0-s04): the crash-point harness arms exactly one
//! point per process run; production stores carry `None` and pay one branch
//! per site. Points are named, finite, and parseable so the kill-matrix
//! subprocess can be told where to die from the outside.

use crate::StoreError;
use std::fmt;
use std::process;

/// Every place the crash matrix can interrupt. Store-owned points cover the
/// CAS write path; the `Log*` points are tripped by `pos-log`, which owns the
/// append/snapshot transactions but injects faults through this one registry
/// so the matrix stays a single enumerable table.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultPoint {
    /// Temp blob fully written and synced, before the atomic rename.
    CasTempWritten,
    /// Blob renamed into its fan-out path, before the parent directory sync.
    CasRenamed,
    /// Immediately before a write transaction commits.
    WalCommit,
    /// Append transaction: event row inserted, before projections apply —
    /// the point that proves append + apply are one atom (m0-s03 AC).
    LogEventInserted,
    /// Append transaction: event row and projections written, before commit.
    LogApplied,
    /// Snapshot row written inside the append transaction, before commit.
    LogSnapshotWritten,
}

impl FaultPoint {
    pub const ALL: [Self; 6] = [
        Self::CasTempWritten,
        Self::CasRenamed,
        Self::WalCommit,
        Self::LogEventInserted,
        Self::LogApplied,
        Self::LogSnapshotWritten,
    ];

    #[must_use]
    pub const fn as_name(self) -> &'static str {
        match self {
            Self::CasTempWritten => "cas-temp-written",
            Self::CasRenamed => "cas-renamed",
            Self::WalCommit => "wal-commit",
            Self::LogEventInserted => "log-event-inserted",
            Self::LogApplied => "log-applied",
            Self::LogSnapshotWritten => "log-snapshot-written",
        }
    }

    #[must_use]
    pub fn from_name(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|point| point.as_name() == name)
    }
}

impl fmt::Display for FaultPoint {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(self.as_name())
    }
}

/// What happens when the armed point is reached.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum FaultAction {
    /// Die exactly like `kill -9`: no destructors, no flushes.
    Abort,
    /// Surface a typed operational failure instead of dying, so error paths
    /// (fail-stop durability, CAS cleanup) are testable in-process.
    FailOperation,
}

/// One armed fault. A single point per plan keeps every scenario in the
/// matrix independently reproducible.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FaultPlan {
    pub point: FaultPoint,
    pub action: FaultAction,
}

impl FaultPlan {
    /// Parses `"<point>:abort"` / `"<point>:fail"` — the wire form the crash
    /// harness passes to its subprocess.
    #[must_use]
    pub fn parse(spec: &str) -> Option<Self> {
        let (point, action) = spec.split_once(':')?;
        let action = match action {
            "abort" => FaultAction::Abort,
            "fail" => FaultAction::FailOperation,
            _ => return None,
        };
        Some(Self {
            point: FaultPoint::from_name(point)?,
            action,
        })
    }

    /// Trips the plan if `point` is armed. `Abort` never returns.
    pub(crate) fn trip(plan: Option<&Self>, point: FaultPoint) -> Result<(), StoreError> {
        let Some(plan) = plan else {
            return Ok(());
        };
        if plan.point != point {
            return Ok(());
        }
        match plan.action {
            // The harness reads this marker from stderr to confirm the death
            // site; abort() is the closest in-process stand-in for kill -9.
            FaultAction::Abort => {
                eprintln!("pos-store: injected crash at fault point {point}");
                process::abort();
            }
            FaultAction::FailOperation => Err(StoreError::InjectedFault { point }),
        }
    }
}

/// Public entry for higher layers (`pos-log`) that own transactions but
/// inject through the store's registry.
pub fn trip(plan: Option<&FaultPlan>, point: FaultPoint) -> Result<(), StoreError> {
    FaultPlan::trip(plan, point)
}

#[cfg(test)]
mod tests {
    use super::{FaultAction, FaultPlan, FaultPoint};

    #[test]
    fn specs_round_trip_and_reject_garbage() {
        for point in FaultPoint::ALL {
            let spec = format!("{}:abort", point.as_name());
            assert_eq!(
                FaultPlan::parse(&spec),
                Some(FaultPlan {
                    point,
                    action: FaultAction::Abort
                })
            );
        }
        assert_eq!(
            FaultPlan::parse("wal-commit:fail"),
            Some(FaultPlan {
                point: FaultPoint::WalCommit,
                action: FaultAction::FailOperation
            })
        );
        assert_eq!(FaultPlan::parse("wal-commit"), None);
        assert_eq!(FaultPlan::parse("nowhere:abort"), None);
        assert_eq!(FaultPlan::parse("wal-commit:explode"), None);
    }

    #[test]
    fn unarmed_points_do_not_trip() {
        let plan = FaultPlan {
            point: FaultPoint::CasRenamed,
            action: FaultAction::FailOperation,
        };
        assert!(FaultPlan::trip(Some(&plan), FaultPoint::WalCommit).is_ok());
        assert!(FaultPlan::trip(None, FaultPoint::CasRenamed).is_ok());
        assert!(FaultPlan::trip(Some(&plan), FaultPoint::CasRenamed).is_err());
    }
}
