//! The dispatch chokepoint (L9): every model call in ProjectOS flows through
//! [`Gateway::complete`], in this fixed order — attribution in hand, policy
//! gate, credential resolution, transport, then exactly one ledger record.
//!
//! ## Invariant inventory (STYLE — state machine in prose)
//!
//! 1. **Policy before I/O.** `ModelPolicy::authorize` runs before credential
//!    resolution and before the transport is touched. A refused dispatch
//!    performs zero network I/O (proven by `tests/policy_no_network.rs`).
//! 2. **Exactly one ledger record per dispatch**, on success and on every
//!    weather path, always fully attributed (proven by
//!    `tests/ledger_property.rs`).
//! 3. **Secrets flow one way:** store → [`CallAuth`] → an auth header inside
//!    an adapter. No record, weather, preflight report, or Debug output can
//!    carry key material (proven by the pos-api secret-scan suite).
//! 4. **A ledger write failure outranks a model success**: the caller gets
//!    [`Weather::LedgerFailure`], because an unmetered call is an accounting
//!    hole (module doc in `ledger.rs`).

use crate::credentials::{CallAuth, CredentialClass, SecretError, SecretStore};
use crate::ledger::{CallAttribution, CostLedger, ModelCallRecord, ProviderCostKind};
use crate::policy::{ModelChoice, ModelPolicy, ModelRouting, RoutingTier};
use crate::provider::{
    CompletionRequest, CompletionSink, CompletionUsage, OUTPUT_TOKENS_REQUEST_MAX, Provider,
};
use crate::transport::HttpTransport;
use crate::weather::Weather;
use pos_foundation::WallClock;

/// Default per-call transport deadline. Two minutes covers a long frontier
/// synthesis stream; anything slower should be a job, not a blocking call.
pub const CALL_TIMEOUT_MS_DEFAULT: u32 = 120_000;

/// What one project's gateway is built from. The shell composes this once
/// per project from typed config; nothing here is global state.
pub struct GatewayConfig {
    pub policy: ModelPolicy,
    pub routing: ModelRouting,
}

/// The per-project gateway. Borrows its collaborators so shells decide
/// lifetimes; the gateway owns only the dispatch order.
pub struct Gateway<'runtime> {
    config: GatewayConfig,
    providers: Vec<Box<dyn Provider + 'runtime>>,
    secrets: &'runtime dyn SecretStore,
    ledger: &'runtime dyn CostLedger,
    transport: &'runtime dyn HttpTransport,
    clock: &'runtime dyn WallClock,
}

/// What the preflight surface shows before a credential is used (m0-s10):
/// provider, class, policy scope, egress warning, last use. Ids and labels
/// only — rendering this struct can never leak a key.
#[derive(Clone, Debug)]
pub struct PreflightReport {
    pub provider: &'static str,
    pub credential_class: &'static str,
    pub policy: &'static str,
    pub model: String,
    pub endpoint_locality: &'static str,
    /// Present exactly when bytes would leave the device — the UI's
    /// data-egress warning is this field, not a heuristic.
    pub egress_warning: Option<String>,
    pub last_used_ts_ms: Option<u64>,
    pub revoked: bool,
}

impl<'runtime> Gateway<'runtime> {
    pub fn new(
        config: GatewayConfig,
        providers: Vec<Box<dyn Provider + 'runtime>>,
        secrets: &'runtime dyn SecretStore,
        ledger: &'runtime dyn CostLedger,
        transport: &'runtime dyn HttpTransport,
        clock: &'runtime dyn WallClock,
    ) -> Self {
        Self {
            config,
            providers,
            secrets,
            ledger,
            transport,
            clock,
        }
    }

    #[must_use]
    pub const fn policy(&self) -> &ModelPolicy {
        &self.config.policy
    }

    /// The tier a caller asked for, resolved against this project's routing.
    #[must_use]
    pub fn choice(&self, tier: RoutingTier) -> &ModelChoice {
        self.config.routing.choice(tier)
    }

    /// One streamed completion through the full chokepoint. See the module
    /// invariant inventory for the order this method must never reorder.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`] for every refusal and failure class; each of them
    /// has already written its ledger record when this returns.
    pub fn complete(
        &self,
        tier: RoutingTier,
        attribution: &CallAttribution,
        request: &CompletionRequest,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        let choice = self.config.routing.choice(tier);
        let started_ts_ms = self.clock.now_ms();
        let outcome = self.run_call(choice, request, sink);
        let wall_ms = self.clock.now_ms().saturating_sub(started_ts_ms);
        self.record_outcome(
            attribution,
            choice,
            request,
            &outcome,
            wall_ms,
            started_ts_ms,
        )?;
        outcome
    }

    /// Policy → budget → credentials → adapter → transport. Extracted so the
    /// ledger write in [`Self::complete`] wraps every path uniformly.
    fn run_call(
        &self,
        choice: &ModelChoice,
        request: &CompletionRequest,
        sink: &mut dyn CompletionSink,
    ) -> Result<CompletionUsage, Weather> {
        // Invariant 1: policy first, before credentials and transport.
        self.config.policy.authorize(choice)?;
        if request.max_output_tokens > OUTPUT_TOKENS_REQUEST_MAX {
            return Err(Weather::BudgetExhausted {
                limit: "max_output_tokens_request",
                message: format!(
                    "{} exceeds the per-call output cap {OUTPUT_TOKENS_REQUEST_MAX}",
                    request.max_output_tokens
                ),
            });
        }
        let auth = self.resolve_auth(&choice.credential)?;
        let provider = self
            .providers
            .iter()
            .find(|provider| provider.family() == choice.family)
            .ok_or_else(|| Weather::InvalidRequest {
                reason: format!(
                    "no provider is registered for family {}",
                    choice.family.as_str()
                ),
            })?;
        provider.complete(&auth, request, self.transport, sink)
    }

    fn resolve_auth(&self, credential: &CredentialClass) -> Result<CallAuth, Weather> {
        let secret_ref = match credential {
            CredentialClass::DeviceSession { .. } => return Ok(CallAuth::None),
            CredentialClass::Managed { secret_ref } | CredentialClass::Byok { secret_ref } => {
                secret_ref
            }
        };
        match self.secrets.resolve(secret_ref) {
            Ok(value) => {
                self.secrets.note_used(secret_ref, self.clock.now_ms());
                Ok(CallAuth::ApiKey(value))
            }
            Err(SecretError::Revoked { .. }) => Err(Weather::CredentialRevoked),
            Err(error) => Err(Weather::CredentialUnavailable {
                reason: error.to_string(),
            }),
        }
    }

    /// Invariant 2: exactly one record per dispatch, every path. On a ledger
    /// failure the whole call reports [`Weather::LedgerFailure`] (invariant 4).
    fn record_outcome(
        &self,
        attribution: &CallAttribution,
        choice: &ModelChoice,
        request: &CompletionRequest,
        outcome: &Result<CompletionUsage, Weather>,
        wall_ms: u64,
        ts_ms: u64,
    ) -> Result<(), Weather> {
        let (tokens_in, tokens_out, measured, outcome_code) = match outcome {
            Ok(usage) => (
                usage.tokens_in,
                usage.tokens_out,
                usage.measured,
                "ok".to_owned(),
            ),
            Err(weather) => (0, 0, false, weather.code().to_owned()),
        };
        let record = ModelCallRecord {
            project: attribution.project,
            feature: attribution.feature.clone(),
            agent: attribution.agent.clone(),
            provider: choice.family,
            credential_class: choice.credential.label(),
            model: request.model.clone(),
            tokens_in,
            tokens_out,
            wall_ms,
            provider_cost_kind: ModelCallRecord::cost_kind_for(&choice.credential, measured),
            // The M0 managed price table is deliberately empty (ledger.rs);
            // customer-billed spend is structurally 0 ProjectOS cost.
            usd_micros: 0,
            outcome: outcome_code,
            ts_ms,
        };
        debug_assert!(
            !matches!(record.provider_cost_kind, ProviderCostKind::Measured)
                || matches!(choice.credential, CredentialClass::Managed { .. }),
            "only managed calls may claim measured ProjectOS cost"
        );
        self.ledger
            .record(&record)
            .map_err(|error| Weather::LedgerFailure {
                reason: error.reason,
            })
    }

    /// The preflight surface for one tier's credential (m0-s10). Read-only:
    /// it never resolves the secret value, so preflight cannot leak or burn
    /// a use.
    #[must_use]
    pub fn preflight(&self, tier: RoutingTier) -> PreflightReport {
        let choice = self.config.routing.choice(tier);
        let (last_used_ts_ms, revoked) = match &choice.credential {
            CredentialClass::DeviceSession { .. } => (None, false),
            CredentialClass::Managed { secret_ref } | CredentialClass::Byok { secret_ref } => (
                self.secrets.last_used_ts_ms(secret_ref),
                matches!(
                    self.secrets.resolve(secret_ref),
                    Err(SecretError::Revoked { .. })
                ),
            ),
        };
        let egress_warning = match choice.endpoint.locality() {
            crate::policy::EndpointLocality::DeviceLocal => None,
            crate::policy::EndpointLocality::Remote => Some(format!(
                "prompt and evidence bytes leave this device for {}",
                choice.endpoint.base_url()
            )),
        };
        PreflightReport {
            provider: choice.family.as_str(),
            credential_class: choice.credential.label(),
            policy: self.config.policy.label(),
            model: choice.model.clone(),
            endpoint_locality: choice.endpoint.locality().as_str(),
            egress_warning,
            last_used_ts_ms,
            revoked,
        }
    }
}
