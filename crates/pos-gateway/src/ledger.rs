//! The honest cost ledger (m0-s10): exactly one fully attributed record per
//! dispatch, including every error path. The gateway defines the record and
//! the sink seam; persistence is the composition layer's choice — `pos-api`
//! appends each record as a `ModelCallCompleted` project event, which makes
//! the billing meter a projection of the log (L1) and `cost.rollup` a
//! projection query.

use crate::credentials::CredentialClass;
use pos_foundation::ProjectId;

/// Who pays for a call and how sure we are about the number (m0-s10):
/// `Measured` came from the provider, `Estimated` is our labeled estimate,
/// `CustomerBilled` is BYOK/device spend that is never ProjectOS model cost.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum ProviderCostKind {
    Measured,
    Estimated,
    CustomerBilled,
}

impl ProviderCostKind {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Measured => "measured",
            Self::Estimated => "estimated",
            Self::CustomerBilled => "customer_billed",
        }
    }
}

/// The attribution every dispatch must carry before it runs: which project
/// pays, which feature asked, and which agent (if any) is running. Unit
/// economics per feature are queries because these fields are never blank.
#[derive(Clone, Debug)]
pub struct CallAttribution {
    pub project: ProjectId,
    pub feature: String,
    pub agent: Option<String>,
}

/// One ledger row — the `model_calls` shape from the milestone, with money
/// in integer micro-USD because floats accumulate error in projections
/// (event-sourcing skill).
#[derive(Clone, Debug)]
pub struct ModelCallRecord {
    pub project: ProjectId,
    pub feature: String,
    pub agent: Option<String>,
    /// The engine label: a [`ProviderFamily`] name for a wire call, or a
    /// [`crate::Transcriber`]'s label for an in-process model. A `&'static str`
    /// rather than the family enum because transcription can run inside this
    /// process, and "openai-compatible" would be a lie in the cost report
    /// (m1-s03). The durable cost event already stores it as text.
    pub provider: &'static str,
    pub credential_class: &'static str,
    pub model: String,
    pub tokens_in: u64,
    pub tokens_out: u64,
    pub wall_ms: u64,
    pub provider_cost_kind: ProviderCostKind,
    pub usd_micros: u64,
    /// The weather code, or `"ok"`. Error paths get rows too — a refused or
    /// failed call is still an attributed fact.
    pub outcome: String,
    pub ts_ms: u64,
}

impl ModelCallRecord {
    /// The cost kind a credential class dictates. BYOK and device-local
    /// sessions are `customer_billed` — their spend must never appear as
    /// ProjectOS model cost (m0-s10 AC). Managed calls are `measured` only
    /// when the provider reported usage; the M0 price table is empty, so
    /// managed USD stays 0 and `estimated` until billing (M6) owns pricing.
    #[must_use]
    pub const fn cost_kind_for(
        credential: &CredentialClass,
        usage_measured: bool,
    ) -> ProviderCostKind {
        match credential {
            CredentialClass::Byok { .. } | CredentialClass::DeviceSession { .. } => {
                ProviderCostKind::CustomerBilled
            }
            CredentialClass::Managed { .. } => {
                if usage_measured {
                    ProviderCostKind::Measured
                } else {
                    ProviderCostKind::Estimated
                }
            }
        }
    }
}

/// Typed sink failure. The gateway surfaces it loudly ([`crate::Weather::LedgerFailure`]):
/// an unmetered call is an accounting hole, not a warning.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct LedgerError {
    pub reason: String,
}

impl std::fmt::Display for LedgerError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "ledger write failed: {}", self.reason)
    }
}

impl std::error::Error for LedgerError {}

/// Where records land. Implementations must be exactly-once per call to
/// `record`; the gateway guarantees exactly one `record` call per dispatch
/// (property-tested in `tests/ledger_property.rs`).
pub trait CostLedger {
    /// # Errors
    ///
    /// [`LedgerError`] when the record could not be durably accepted.
    fn record(&self, record: &ModelCallRecord) -> Result<(), LedgerError>;
}

/// Bounded in-memory ledger for tests and the eval runner. Refuses beyond
/// its cap instead of growing silently (L8).
pub struct MemoryLedger {
    /// Test/eval runs record hundreds of calls, not millions; a run that
    /// hits this cap is a runaway, and refusing beats swallowing.
    records: std::sync::Mutex<Vec<ModelCallRecord>>,
}

/// See [`MemoryLedger`]: the cap that turns a runaway loop into a typed
/// refusal instead of unbounded memory.
pub const MEMORY_LEDGER_RECORDS_MAX: usize = 4_096;

impl Default for MemoryLedger {
    fn default() -> Self {
        Self {
            records: std::sync::Mutex::new(Vec::new()),
        }
    }
}

impl MemoryLedger {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Snapshot of everything recorded so far.
    #[must_use]
    pub fn records(&self) -> Vec<ModelCallRecord> {
        self.records
            .lock()
            .expect("ledger mutex is never poisoned: critical sections are panic-free") // INVARIANT: push/clone only below.
            .clone()
    }
}

impl CostLedger for MemoryLedger {
    fn record(&self, record: &ModelCallRecord) -> Result<(), LedgerError> {
        let mut records = self
            .records
            .lock()
            .expect("ledger mutex is never poisoned: critical sections are panic-free"); // INVARIANT: push/clone only.
        if records.len() >= MEMORY_LEDGER_RECORDS_MAX {
            return Err(LedgerError {
                reason: format!("memory ledger is full ({MEMORY_LEDGER_RECORDS_MAX} records)"),
            });
        }
        records.push(record.clone());
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{CredentialClass, ModelCallRecord, ProviderCostKind};
    use crate::credentials::SecretRef;

    #[test]
    fn byok_and_device_sessions_are_always_customer_billed() {
        let byok = CredentialClass::Byok {
            secret_ref: SecretRef::new("byok/x"),
        };
        assert_eq!(
            ModelCallRecord::cost_kind_for(&byok, true),
            ProviderCostKind::CustomerBilled
        );
        let device = CredentialClass::DeviceSession {
            adapter: "ollama".to_owned(),
            device: pos_foundation::DeviceId::from_bytes([2; 16]),
        };
        assert_eq!(
            ModelCallRecord::cost_kind_for(&device, false),
            ProviderCostKind::CustomerBilled
        );
    }

    #[test]
    fn managed_cost_kind_tracks_whether_usage_was_measured() {
        let managed = CredentialClass::Managed {
            secret_ref: SecretRef::new("managed/x"),
        };
        assert_eq!(
            ModelCallRecord::cost_kind_for(&managed, true),
            ProviderCostKind::Measured
        );
        assert_eq!(
            ModelCallRecord::cost_kind_for(&managed, false),
            ProviderCostKind::Estimated
        );
    }
}
