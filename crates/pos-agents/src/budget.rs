//! Hard Run-budget admission (L8).
//!
//! Invariants:
//! - Every planned step is admitted against durable spent usage plus the one
//!   outstanding reservation before any step fact is appended.
//! - Checked arithmetic fails closed; integer overflow can never become extra
//!   budget.
//! - The first exceeded dimension is stable and typed, so UI/API degradation
//!   is deterministic rather than dependent on hash or field iteration order.

use pos_domain::{RunBudget, RunBudgetDimension, RunUsage};
use std::fmt;

/// The snapshot cadence already bounds replay at 10,000 events. A Run with
/// more steps is a workflow that must create a successor Run with lineage.
pub const RUN_STEP_COUNT_MAX: u32 = 10_000;
/// A single Run cannot create more tool effects than steps.
pub const RUN_TOOL_CALL_COUNT_MAX: u32 = RUN_STEP_COUNT_MAX;
/// Retries beyond this are a loop, not resilience; the visible pause lets a
/// human repair the cause instead of spending indefinitely.
pub const RUN_RETRY_COUNT_MAX: u32 = 1_000;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct BudgetExceeded {
    pub dimension: RunBudgetDimension,
    pub limit: u64,
    pub spent: u64,
    pub pending: u64,
    pub requested: u64,
}

impl fmt::Display for BudgetExceeded {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "Run {:?} budget exceeded: limit {}, spent {}, pending {}, requested {}",
            self.dimension, self.limit, self.spent, self.pending, self.requested
        )
    }
}

impl std::error::Error for BudgetExceeded {}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BudgetConfigError {
    StepLimitTooLarge { configured: u32, maximum: u32 },
    ToolCallLimitTooLarge { configured: u32, maximum: u32 },
    RetryLimitTooLarge { configured: u32, maximum: u32 },
}

impl fmt::Display for BudgetConfigError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::StepLimitTooLarge {
                configured,
                maximum,
            } => write!(
                formatter,
                "Run step budget {configured} exceeds the {maximum}-step hard cap"
            ),
            Self::ToolCallLimitTooLarge {
                configured,
                maximum,
            } => write!(
                formatter,
                "Run tool-call budget {configured} exceeds the {maximum}-call hard cap"
            ),
            Self::RetryLimitTooLarge {
                configured,
                maximum,
            } => write!(
                formatter,
                "Run retry budget {configured} exceeds the {maximum}-retry hard cap"
            ),
        }
    }
}

impl std::error::Error for BudgetConfigError {}

pub(crate) fn validate_budget(budget: RunBudget) -> Result<(), BudgetConfigError> {
    if budget.steps > RUN_STEP_COUNT_MAX {
        return Err(BudgetConfigError::StepLimitTooLarge {
            configured: budget.steps,
            maximum: RUN_STEP_COUNT_MAX,
        });
    }
    if budget.tool_calls > RUN_TOOL_CALL_COUNT_MAX {
        return Err(BudgetConfigError::ToolCallLimitTooLarge {
            configured: budget.tool_calls,
            maximum: RUN_TOOL_CALL_COUNT_MAX,
        });
    }
    if budget.retries > RUN_RETRY_COUNT_MAX {
        return Err(BudgetConfigError::RetryLimitTooLarge {
            configured: budget.retries,
            maximum: RUN_RETRY_COUNT_MAX,
        });
    }
    Ok(())
}

pub(crate) fn admit(
    budget: RunBudget,
    spent: RunUsage,
    pending: RunUsage,
    requested: RunUsage,
) -> Result<(), BudgetExceeded> {
    check_u64(
        RunBudgetDimension::Tokens,
        budget.tokens,
        spent.tokens,
        pending.tokens,
        requested.tokens,
    )?;
    check_u64(
        RunBudgetDimension::UsdMicros,
        budget.usd_micros,
        spent.usd_micros,
        pending.usd_micros,
        requested.usd_micros,
    )?;
    check_u64(
        RunBudgetDimension::WallMs,
        budget.wall_ms,
        spent.wall_ms,
        pending.wall_ms,
        requested.wall_ms,
    )?;
    check_u64(
        RunBudgetDimension::StorageBytes,
        budget.storage_bytes,
        spent.storage_bytes,
        pending.storage_bytes,
        requested.storage_bytes,
    )?;
    check_u64(
        RunBudgetDimension::ToolCalls,
        u64::from(budget.tool_calls),
        u64::from(spent.tool_calls),
        u64::from(pending.tool_calls),
        u64::from(requested.tool_calls),
    )?;
    check_u64(
        RunBudgetDimension::Retries,
        u64::from(budget.retries),
        u64::from(spent.retries),
        u64::from(pending.retries),
        u64::from(requested.retries),
    )?;
    check_u64(
        RunBudgetDimension::Steps,
        u64::from(budget.steps),
        u64::from(spent.steps),
        u64::from(pending.steps),
        u64::from(requested.steps),
    )
}

pub(crate) fn usage_within_reservation(actual: RunUsage, reserved: RunUsage) -> bool {
    actual.tokens <= reserved.tokens
        && actual.usd_micros <= reserved.usd_micros
        && actual.wall_ms <= reserved.wall_ms
        && actual.storage_bytes <= reserved.storage_bytes
        && actual.tool_calls <= reserved.tool_calls
        && actual.retries <= reserved.retries
        && actual.steps <= reserved.steps
}

pub(crate) fn reservation_exceeded_dimension(
    actual: RunUsage,
    reserved: RunUsage,
) -> Option<RunBudgetDimension> {
    if actual.tokens > reserved.tokens {
        Some(RunBudgetDimension::Tokens)
    } else if actual.usd_micros > reserved.usd_micros {
        Some(RunBudgetDimension::UsdMicros)
    } else if actual.wall_ms > reserved.wall_ms {
        Some(RunBudgetDimension::WallMs)
    } else if actual.storage_bytes > reserved.storage_bytes {
        Some(RunBudgetDimension::StorageBytes)
    } else if actual.tool_calls > reserved.tool_calls {
        Some(RunBudgetDimension::ToolCalls)
    } else if actual.retries > reserved.retries {
        Some(RunBudgetDimension::Retries)
    } else if actual.steps > reserved.steps {
        Some(RunBudgetDimension::Steps)
    } else {
        None
    }
}

fn check_u64(
    dimension: RunBudgetDimension,
    limit: u64,
    spent: u64,
    pending: u64,
    requested: u64,
) -> Result<(), BudgetExceeded> {
    let admitted = spent
        .checked_add(pending)
        .and_then(|value| value.checked_add(requested));
    if admitted.is_some_and(|value| value <= limit) {
        return Ok(());
    }
    Err(BudgetExceeded {
        dimension,
        limit,
        spent,
        pending,
        requested,
    })
}

#[cfg(test)]
mod tests {
    use super::{admit, usage_within_reservation};
    use pos_domain::{RunBudget, RunBudgetDimension, RunUsage};

    #[test]
    fn admission_is_checked_and_dimension_order_is_stable() {
        let budget = RunBudget {
            tokens: 10,
            usd_micros: 20,
            wall_ms: 30,
            storage_bytes: 40,
            tool_calls: 2,
            retries: 1,
            steps: 2,
        };
        let error = admit(
            budget,
            RunUsage {
                tokens: 9,
                ..RunUsage::default()
            },
            RunUsage::default(),
            RunUsage {
                tokens: 2,
                usd_micros: 30,
                ..RunUsage::default()
            },
        )
        .expect_err("tokens is the first exceeded dimension");
        assert_eq!(error.dimension, RunBudgetDimension::Tokens);

        let overflow = admit(
            RunBudget {
                tokens: u64::MAX,
                ..budget
            },
            RunUsage {
                tokens: u64::MAX,
                ..RunUsage::default()
            },
            RunUsage::default(),
            RunUsage {
                tokens: 1,
                ..RunUsage::default()
            },
        )
        .expect_err("overflow fails closed");
        assert_eq!(overflow.dimension, RunBudgetDimension::Tokens);
    }

    #[test]
    fn actual_usage_cannot_claim_more_than_the_durable_reservation() {
        let reservation = RunUsage {
            tokens: 10,
            tool_calls: 1,
            steps: 1,
            ..RunUsage::default()
        };
        assert!(usage_within_reservation(reservation, reservation));
        assert!(!usage_within_reservation(
            RunUsage {
                tokens: 11,
                ..reservation
            },
            reservation
        ));
    }
}
