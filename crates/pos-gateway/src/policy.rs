//! Per-project model policy (F43) and tier routing, enforced at gateway
//! dispatch — before credentials resolve and before any transport is
//! touched. A `local_only` project cannot leak a byte to a cloud API by
//! construction: the policy check is the first statement of the dispatch
//! chokepoint, and the zero-network-I/O test proves the transport is never
//! reached (L9).

use crate::credentials::CredentialClass;
use crate::provider::ProviderFamily;
use crate::weather::Weather;

/// Where an endpoint's bytes go. Declared by configuration and validated at
/// construction — never sniffed from hostnames at dispatch time.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum EndpointLocality {
    /// The model runs inside this process (local whisper, m1-s03). There is
    /// no URL, no socket, and therefore nothing a policy could leak through —
    /// the strongest local-only statement the vocabulary can make.
    InProcess,
    /// A same-device server (Ollama, LM Studio, vLLM on localhost). The
    /// constructor refuses this label for a non-loopback URL, so the label
    /// cannot lie.
    DeviceLocal,
    /// Anything whose bytes leave the device.
    Remote,
}

impl EndpointLocality {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::DeviceLocal => "device_local",
            Self::Remote => "remote",
        }
    }

    /// Whether bytes leave this device. The one question the egress warning
    /// and the `local_only` gate both ask.
    #[must_use]
    pub const fn leaves_device(self) -> bool {
        matches!(self, Self::Remote)
    }
}

/// A dispatchable endpoint: base URL plus its validated locality.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointConfig {
    base_url: String,
    locality: EndpointLocality,
}

/// Typed construction failure: the config asked for a label the URL cannot
/// honestly carry.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct EndpointConfigError {
    pub reason: String,
}

impl std::fmt::Display for EndpointConfigError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(formatter, "endpoint config rejected: {}", self.reason)
    }
}

impl std::error::Error for EndpointConfigError {}

fn url_host(base_url: &str) -> Option<&str> {
    let rest = base_url
        .strip_prefix("http://")
        .or_else(|| base_url.strip_prefix("https://"))?;
    let authority = rest.split('/').next().unwrap_or(rest);
    Some(
        authority
            .rsplit_once(':')
            .map_or(authority, |(host, _)| host),
    )
}

fn is_loopback_host(host: &str) -> bool {
    host == "localhost" || host == "127.0.0.1" || host == "[::1]" || host == "::1"
}

impl EndpointConfig {
    /// # Errors
    ///
    /// [`EndpointConfigError`] when a `DeviceLocal` label names a
    /// non-loopback host — a config that lies about locality would turn the
    /// policy gate into decoration.
    pub fn new(
        base_url: impl Into<String>,
        locality: EndpointLocality,
    ) -> Result<Self, EndpointConfigError> {
        let base_url = base_url.into();
        if locality == EndpointLocality::InProcess {
            return Err(EndpointConfigError {
                reason: "an in-process endpoint has no URL; use EndpointConfig::in_process"
                    .to_owned(),
            });
        }
        let host = url_host(&base_url).ok_or_else(|| EndpointConfigError {
            reason: format!("base URL did not parse: {base_url:?}"),
        })?;
        if locality == EndpointLocality::DeviceLocal && !is_loopback_host(host) {
            return Err(EndpointConfigError {
                reason: format!("{host:?} is not a loopback host; it cannot be device_local"),
            });
        }
        Ok(Self { base_url, locality })
    }

    /// An endpoint that *is* this process — a model loaded into our own
    /// address space (m1-s03's local whisper adapter).
    ///
    /// It is a separate constructor rather than a URL with a special scheme
    /// because there is no URL to get wrong: `component` names the adapter
    /// for preflight and the ledger, and nothing ever parses or dials it.
    #[must_use]
    pub fn in_process(component: &str) -> Self {
        Self {
            base_url: format!("in-process:{component}"),
            locality: EndpointLocality::InProcess,
        }
    }

    #[must_use]
    pub fn base_url(&self) -> &str {
        &self.base_url
    }

    #[must_use]
    pub const fn locality(&self) -> EndpointLocality {
        self.locality
    }
}

/// The three per-project policies (F43). Each is strictly wider than the
/// previous; the enum order is the trust order.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ModelPolicy {
    /// Device-local endpoints only. Zero cloud I/O by construction.
    LocalOnly,
    /// Device-local plus the five known families at their pinned bases.
    CloudAllowed,
    /// `CloudAllowed` plus explicitly allowlisted custom base URLs.
    CustomEndpoints { allowed_base_urls: Vec<String> },
}

impl ModelPolicy {
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::LocalOnly => "local_only",
            Self::CloudAllowed => "cloud_allowed",
            Self::CustomEndpoints { .. } => "custom_endpoints",
        }
    }

    /// The dispatch gate. Runs before credential resolution and transport
    /// selection; a refusal here is the typed `PolicyViolation` the AC
    /// names, produced with zero I/O of any kind.
    ///
    /// # Errors
    ///
    /// [`Weather::PolicyViolation`] naming the policy and the refused
    /// endpoint class.
    pub fn authorize(&self, choice: &ModelChoice) -> Result<(), Weather> {
        let violation = |requested: String| Weather::PolicyViolation {
            policy: self.label().to_owned(),
            requested,
        };
        match self {
            Self::LocalOnly => match choice.endpoint.locality() {
                EndpointLocality::InProcess | EndpointLocality::DeviceLocal => Ok(()),
                EndpointLocality::Remote => Err(violation(format!(
                    "remote endpoint for family {}",
                    choice.family.as_str()
                ))),
            },
            Self::CloudAllowed => match choice.endpoint.locality() {
                EndpointLocality::InProcess | EndpointLocality::DeviceLocal => Ok(()),
                // Known families at their pinned bases are exactly what the
                // gateway constructor registers; a custom remote base under
                // plain cloud_allowed is refused.
                EndpointLocality::Remote if choice.is_pinned_family_base => Ok(()),
                EndpointLocality::Remote => Err(violation(format!(
                    "custom remote endpoint {}",
                    choice.endpoint.base_url()
                ))),
            },
            Self::CustomEndpoints { allowed_base_urls } => match choice.endpoint.locality() {
                EndpointLocality::InProcess | EndpointLocality::DeviceLocal => Ok(()),
                EndpointLocality::Remote => {
                    if choice.is_pinned_family_base
                        || allowed_base_urls
                            .iter()
                            .any(|allowed| allowed == choice.endpoint.base_url())
                    {
                        Ok(())
                    } else {
                        Err(violation(format!(
                            "unlisted remote endpoint {}",
                            choice.endpoint.base_url()
                        )))
                    }
                }
            },
        }
    }
}

/// Routing tiers (master plan §12): `frontier` for synthesis/specs/agents,
/// `fast` for extraction/classification.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum RoutingTier {
    Frontier,
    Fast,
}

impl RoutingTier {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Frontier => "frontier",
            Self::Fast => "fast",
        }
    }
}

/// A fully resolved dispatch target: family, endpoint, model, credential.
#[derive(Clone, Debug)]
pub struct ModelChoice {
    pub family: ProviderFamily,
    pub endpoint: EndpointConfig,
    pub model: String,
    pub credential: CredentialClass,
    /// True when `endpoint` is the family's pinned canonical base (set by
    /// the gateway constructor, not by callers) — what lets `cloud_allowed`
    /// admit Anthropic's API while refusing an arbitrary remote URL.
    pub is_pinned_family_base: bool,
}

/// The per-tier routing table a project configures.
///
/// Transcription and embedding are fields rather than extra tiers because
/// tiers are about *how hard the thinking is* and these are different
/// modalities: an interview routes to whisper or to a cloud STT endpoint, and
/// a chunk routes to bge or to an embeddings endpoint, regardless of what
/// synthesis costs. Both are optional because most gateways do neither, and a
/// `None` that refuses typed beats a placeholder route that dials something
/// unexpected (m1-s03, m1-s04).
#[derive(Clone, Debug)]
pub struct ModelRouting {
    pub frontier: ModelChoice,
    pub fast: ModelChoice,
    pub transcribe: Option<ModelChoice>,
    /// Embeddings, for the same reason transcription is a field: it is a
    /// different modality, not a harder tier. A chunk routes to the local
    /// ONNX model or to an API endpoint regardless of what synthesis costs
    /// (m1-s04).
    pub embed: Option<ModelChoice>,
}

impl ModelRouting {
    /// The two thinking tiers with no transcription route — every gateway
    /// composed before m1-s03, stated once instead of at each call site.
    #[must_use]
    pub const fn thinking_only(frontier: ModelChoice, fast: ModelChoice) -> Self {
        Self {
            frontier,
            fast,
            transcribe: None,
            embed: None,
        }
    }

    #[must_use]
    pub fn with_transcribe(mut self, choice: ModelChoice) -> Self {
        self.transcribe = Some(choice);
        self
    }

    #[must_use]
    pub fn with_embed(mut self, choice: ModelChoice) -> Self {
        self.embed = Some(choice);
        self
    }

    #[must_use]
    pub const fn choice(&self, tier: RoutingTier) -> &ModelChoice {
        match tier {
            RoutingTier::Frontier => &self.frontier,
            RoutingTier::Fast => &self.fast,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{EndpointConfig, EndpointLocality, ModelChoice, ModelPolicy, RoutingTier};
    use crate::credentials::{CredentialClass, SecretRef};
    use crate::provider::ProviderFamily;
    use crate::weather::Weather;

    fn cloud_choice(pinned: bool) -> ModelChoice {
        ModelChoice {
            family: ProviderFamily::Anthropic,
            endpoint: EndpointConfig::new("https://api.anthropic.com", EndpointLocality::Remote)
                .expect("remote endpoint config"),
            model: "claude-x".to_owned(),
            credential: CredentialClass::Byok {
                secret_ref: SecretRef::new("byok/anthropic/1"),
            },
            is_pinned_family_base: pinned,
        }
    }

    fn local_choice() -> ModelChoice {
        ModelChoice {
            family: ProviderFamily::OpenAiCompatible,
            endpoint: EndpointConfig::new("http://localhost:11434", EndpointLocality::DeviceLocal)
                .expect("loopback endpoint config"),
            model: "llama3.2".to_owned(),
            credential: CredentialClass::DeviceSession {
                adapter: "ollama".to_owned(),
                device: pos_foundation::DeviceId::from_bytes([1; 16]),
            },
            is_pinned_family_base: false,
        }
    }

    #[test]
    fn a_device_local_label_cannot_name_a_remote_host() {
        let error = EndpointConfig::new("http://models.example.com", EndpointLocality::DeviceLocal)
            .expect_err("a non-loopback device_local label is a lie");
        assert!(error.reason.contains("models.example.com"));
        assert!(
            EndpointConfig::new("http://127.0.0.1:8000", EndpointLocality::DeviceLocal).is_ok()
        );
    }

    #[test]
    fn local_only_refuses_every_remote_endpoint_and_admits_loopback() {
        let policy = ModelPolicy::LocalOnly;
        assert!(policy.authorize(&local_choice()).is_ok());
        let refused = policy
            .authorize(&cloud_choice(true))
            .expect_err("local_only must refuse a pinned cloud base too");
        assert!(matches!(refused, Weather::PolicyViolation { .. }));
        assert_eq!(refused.code(), "policy_violation");
    }

    #[test]
    fn cloud_allowed_admits_pinned_bases_but_not_arbitrary_remotes() {
        let policy = ModelPolicy::CloudAllowed;
        assert!(policy.authorize(&cloud_choice(true)).is_ok());
        let refused = policy
            .authorize(&cloud_choice(false))
            .expect_err("an unpinned remote base needs custom_endpoints");
        assert!(matches!(refused, Weather::PolicyViolation { .. }));
    }

    #[test]
    fn custom_endpoints_admits_exactly_the_allowlist() {
        let policy = ModelPolicy::CustomEndpoints {
            allowed_base_urls: vec!["https://api.anthropic.com".to_owned()],
        };
        assert!(policy.authorize(&cloud_choice(false)).is_ok());
        let policy = ModelPolicy::CustomEndpoints {
            allowed_base_urls: vec!["https://other.example.com".to_owned()],
        };
        assert!(policy.authorize(&cloud_choice(false)).is_err());
    }

    #[test]
    fn routing_resolves_tiers_to_their_configured_choice() {
        let routing = super::ModelRouting::thinking_only(cloud_choice(true), local_choice());
        assert_eq!(
            routing.choice(RoutingTier::Frontier).family,
            ProviderFamily::Anthropic
        );
        assert_eq!(
            routing.choice(RoutingTier::Fast).family,
            ProviderFamily::OpenAiCompatible
        );
    }
}
