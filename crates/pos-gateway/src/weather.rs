//! The typed model-failure vocabulary (STYLE panic policy: model failures are
//! weather, not incidents). Every gateway call site returns one of these
//! variants; none of them is ever a panic, and none of them may carry secret
//! material — messages name codes, models, and policies, never key bytes.

use std::fmt;

/// Why a dispatch produced no (or partial) model output. The variants are the
/// exact weather classes m0-s10 requires every call site to handle: timeout,
/// rate-limit, refusal, malformed output, and budget exhaustion — plus the
/// pre-transport refusals (policy, credential, transport) that the dispatch
/// chokepoint can produce before any socket opens.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Weather {
    /// The provider did not answer inside the request's stated deadline.
    Timeout { timeout_ms: u32 },
    /// The provider asked us to slow down. `retry_after_ms` is the provider's
    /// own hint when it sent one; absence is honest, never a default.
    RateLimited { retry_after_ms: Option<u32> },
    /// The model declined the request. The provider's stated reason is data
    /// for the caller's UI, not an instruction.
    Refusal { reason: String },
    /// The provider answered bytes the adapter could not parse into the wire
    /// contract it conforms to. Always names what failed to parse.
    MalformedOutput { reason: String },
    /// A stated cap refused the call before any spend (L8). Names the cap so
    /// degradation is visible, never silent.
    BudgetExhausted {
        limit: &'static str,
        message: String,
    },
    /// The per-project model policy refused the dispatch before any socket
    /// opened (L9/F43). `requested` names the endpoint class that was denied.
    PolicyViolation { policy: String, requested: String },
    /// The credential reference could not be resolved into a usable secret.
    CredentialUnavailable { reason: String },
    /// The credential was revoked; revocation blocks new dispatches
    /// immediately (m0-s10 preflight contract).
    CredentialRevoked,
    /// The provider rejected our authentication.
    AuthRejected { status: u16 },
    /// The provider rejected a request field it does not support — the
    /// OpenAI-compatible conformance class for capability-profile honesty.
    UnsupportedField { field: String },
    /// The caller's request violated the gateway contract before any I/O
    /// (unparseable pass-through tool JSON, empty message list). A caller
    /// bug, named as one — never sent to a provider to fail remotely.
    InvalidRequest { reason: String },
    /// The byte transport failed below HTTP semantics (connect, TLS-less
    /// socket, mid-stream drop). Carries no provider payload.
    Transport { reason: String },
    /// The cost ledger could not record the call. Surfaced loudly even when
    /// the model answered, because an unmetered call is an accounting hole
    /// the honest-ledger AC exists to prevent.
    LedgerFailure { reason: String },
    /// A reserved capability slot whose engine lands with a named story —
    /// registered and typed, never a fake empty success.
    NotYetSupported {
        capability: &'static str,
        arrives_with: &'static str,
    },
}

impl Weather {
    /// Stable machine-readable code — the ledger's `outcome` column and every
    /// UI badge key off this, so the words are frozen vocabulary.
    #[must_use]
    pub const fn code(&self) -> &'static str {
        match self {
            Self::Timeout { .. } => "timeout",
            Self::RateLimited { .. } => "rate_limited",
            Self::Refusal { .. } => "refusal",
            Self::MalformedOutput { .. } => "malformed_output",
            Self::BudgetExhausted { .. } => "budget_exhausted",
            Self::PolicyViolation { .. } => "policy_violation",
            Self::CredentialUnavailable { .. } => "credential_unavailable",
            Self::CredentialRevoked => "credential_revoked",
            Self::AuthRejected { .. } => "auth_rejected",
            Self::UnsupportedField { .. } => "unsupported_field",
            Self::InvalidRequest { .. } => "invalid_request",
            Self::Transport { .. } => "transport_failure",
            Self::LedgerFailure { .. } => "ledger_failure",
            Self::NotYetSupported { .. } => "not_yet_supported",
        }
    }

    /// Whether an unchanged retry can plausibly succeed. Policy, budget,
    /// credential, and contract errors are deterministic refusals — retrying
    /// them is spend without hope.
    #[must_use]
    pub const fn retriable(&self) -> bool {
        matches!(
            self,
            Self::Timeout { .. } | Self::RateLimited { .. } | Self::Transport { .. }
        )
    }
}

impl fmt::Display for Weather {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Timeout { timeout_ms } => {
                write!(formatter, "provider timed out after {timeout_ms} ms")
            }
            Self::RateLimited { retry_after_ms } => match retry_after_ms {
                Some(ms) => write!(formatter, "provider rate-limited; retry after {ms} ms"),
                None => write!(formatter, "provider rate-limited"),
            },
            Self::Refusal { reason } => write!(formatter, "model refused: {reason}"),
            Self::MalformedOutput { reason } => {
                write!(formatter, "provider output did not parse: {reason}")
            }
            Self::BudgetExhausted { limit, message } => {
                write!(formatter, "budget exhausted ({limit}): {message}")
            }
            Self::PolicyViolation { policy, requested } => {
                write!(formatter, "model policy {policy} refuses {requested}")
            }
            Self::CredentialUnavailable { reason } => {
                write!(formatter, "credential unavailable: {reason}")
            }
            Self::CredentialRevoked => formatter.write_str("credential revoked"),
            Self::AuthRejected { status } => {
                write!(
                    formatter,
                    "provider rejected authentication (HTTP {status})"
                )
            }
            Self::UnsupportedField { field } => {
                write!(formatter, "endpoint does not support the field {field:?}")
            }
            Self::InvalidRequest { reason } => {
                write!(formatter, "request violates the gateway contract: {reason}")
            }
            Self::Transport { reason } => write!(formatter, "transport failure: {reason}"),
            Self::LedgerFailure { reason } => {
                write!(formatter, "cost ledger write failed: {reason}")
            }
            Self::NotYetSupported {
                capability,
                arrives_with,
            } => write!(
                formatter,
                "{capability} is registered but not implemented yet; it lands with {arrives_with}"
            ),
        }
    }
}

impl std::error::Error for Weather {}

#[cfg(test)]
mod tests {
    use super::Weather;

    #[test]
    fn codes_are_stable_and_retriability_matches_the_class() {
        let deterministic = Weather::PolicyViolation {
            policy: "local_only".to_owned(),
            requested: "cloud endpoint".to_owned(),
        };
        assert_eq!(deterministic.code(), "policy_violation");
        assert!(!deterministic.retriable());

        let transient = Weather::RateLimited {
            retry_after_ms: Some(250),
        };
        assert_eq!(transient.code(), "rate_limited");
        assert!(transient.retriable());
    }
}
