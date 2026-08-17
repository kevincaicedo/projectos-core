//! Shared fixtures for the pos-gateway integration suites: a recorded-
//! fixture transport (conformance), a refusing/counting transport (the
//! zero-network-I/O policy oracle), and gateway builders.

#![allow(dead_code)] // Shared test helpers: each test binary uses a subset.

use pos_gateway::{
    AnthropicAdapter, CredentialClass, EndpointConfig, EndpointLocality, EndpointProfile,
    GoogleAdapter, HttpHead, HttpRequestPlan, HttpTransport, MemorySecretStore, ModelChoice,
    ModelPolicy, ModelRouting, OpenAiAdapter, OpenAiCompatibleAdapter, OpenRouterAdapter, Provider,
    ProviderFamily, ResponseHandler, SecretRef, TransportError,
};
use std::sync::Mutex;

/// What one fixture exchange answers with.
pub enum FixtureOutcome {
    /// status, headers, body — body is delivered in deliberately awkward
    /// chunk sizes so SSE reassembly is exercised on every row.
    Response {
        status: u16,
        headers: Vec<(&'static str, &'static str)>,
        body: &'static str,
    },
    /// A transport-level failure (timeout, drop) before any response head.
    Fail(TransportError),
}

/// Recorded-fixture transport: answers each `execute` with the scripted
/// outcome and keeps every plan it saw for header/URL assertions.
pub struct FixtureTransport {
    outcome: FixtureOutcome,
    pub plans: Mutex<Vec<PlanSnapshot>>,
}

/// The parts of a plan the conformance rows assert on. Header values are
/// kept here (tests need to check where the key landed); this type never
/// leaves the test binary.
pub struct PlanSnapshot {
    pub method: &'static str,
    pub url: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

/// Chunk size chosen to split SSE lines mid-token: exercises reassembly
/// without being so small the test crawls.
const FIXTURE_CHUNK_BYTES: usize = 7;

impl FixtureTransport {
    pub fn new(outcome: FixtureOutcome) -> Self {
        Self {
            outcome,
            plans: Mutex::new(Vec::new()),
        }
    }

    pub fn respond(status: u16, body: &'static str) -> Self {
        Self::new(FixtureOutcome::Response {
            status,
            headers: Vec::new(),
            body,
        })
    }

    pub fn single_plan(&self) -> PlanSnapshot {
        let mut plans = self.plans.lock().expect("test mutex");
        assert_eq!(plans.len(), 1, "expected exactly one dispatch");
        plans.pop().expect("one plan")
    }
}

impl HttpTransport for FixtureTransport {
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        self.plans.lock().expect("test mutex").push(PlanSnapshot {
            method: plan.method.as_str(),
            url: plan.url.clone(),
            headers: plan
                .headers
                .iter()
                .map(|(name, value)| ((*name).to_owned(), value.clone()))
                .collect(),
            body: String::from_utf8_lossy(&plan.body).into_owned(),
        });
        match &self.outcome {
            FixtureOutcome::Fail(error) => Err(error.clone()),
            FixtureOutcome::Response {
                status,
                headers,
                body,
            } => {
                let head = HttpHead {
                    status: *status,
                    headers: headers
                        .iter()
                        .map(|(name, value)| ((*name).to_owned(), (*value).to_owned()))
                        .collect(),
                };
                if handler.on_head(&head).is_err() {
                    return Err(TransportError::Aborted);
                }
                for chunk in body.as_bytes().chunks(FIXTURE_CHUNK_BYTES) {
                    if handler.on_chunk(chunk).is_err() {
                        return Err(TransportError::Aborted);
                    }
                }
                Ok(())
            }
        }
    }
}

/// The zero-network-I/O oracle: counts connection attempts and panics the
/// test if one slips through when `expect_zero` is set.
#[derive(Default)]
pub struct CountingTransport {
    pub attempts: Mutex<u32>,
}

impl CountingTransport {
    pub fn attempt_count(&self) -> u32 {
        *self.attempts.lock().expect("test mutex")
    }
}

impl HttpTransport for CountingTransport {
    fn execute(
        &self,
        _plan: &HttpRequestPlan,
        _handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        *self.attempts.lock().expect("test mutex") += 1;
        // Refuse instead of pretending: a test that reaches this transport
        // asserts on the attempt count, not on a fake response.
        Err(TransportError::Connect {
            reason: "counting transport accepts no connections".to_owned(),
        })
    }
}

pub fn all_providers() -> Vec<Box<dyn Provider>> {
    vec![
        Box::new(AnthropicAdapter {
            base_url: "https://api.anthropic.com".to_owned(),
        }),
        Box::new(OpenAiAdapter {
            base_url: "https://api.openai.com".to_owned(),
        }),
        Box::new(GoogleAdapter {
            base_url: "https://generativelanguage.googleapis.com".to_owned(),
        }),
        Box::new(OpenRouterAdapter {
            base_url: "https://openrouter.ai/api".to_owned(),
        }),
        Box::new(OpenAiCompatibleAdapter {
            base_url: "http://localhost:11434".to_owned(),
            profile: EndpointProfile::conservative(),
        }),
    ]
}

pub fn cloud_frontier_choice(secret_ref: &SecretRef) -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::Anthropic,
        endpoint: EndpointConfig::new("https://api.anthropic.com", EndpointLocality::Remote)
            .expect("remote endpoint"),
        model: "claude-frontier-test".to_owned(),
        credential: CredentialClass::Byok {
            secret_ref: secret_ref.clone(),
        },
        is_pinned_family_base: true,
    }
}

pub fn local_fast_choice() -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::OpenAiCompatible,
        endpoint: EndpointConfig::new("http://localhost:11434", EndpointLocality::DeviceLocal)
            .expect("loopback endpoint"),
        model: "llama-test".to_owned(),
        credential: CredentialClass::DeviceSession {
            adapter: "ollama".to_owned(),
            device: pos_foundation::DeviceId::from_bytes([7; 16]),
        },
        is_pinned_family_base: false,
    }
}

pub fn routing(frontier: ModelChoice, fast: ModelChoice) -> ModelRouting {
    ModelRouting::thinking_only(frontier, fast)
}

/// A transcription route that runs inside this process — the local whisper
/// shape, without loading a model.
pub fn in_process_transcribe_choice() -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::OpenAiCompatible,
        endpoint: EndpointConfig::in_process("whisper-local"),
        model: "whisper-small".to_owned(),
        credential: CredentialClass::DeviceSession {
            adapter: "whisper".to_owned(),
            device: pos_foundation::DeviceId::from_bytes([9; 16]),
        },
        is_pinned_family_base: false,
    }
}

/// A cloud STT route at a remote endpoint.
pub fn cloud_transcribe_choice(secret_ref: &SecretRef) -> ModelChoice {
    ModelChoice {
        family: ProviderFamily::OpenAi,
        endpoint: EndpointConfig::new("https://api.openai.com", EndpointLocality::Remote)
            .expect("remote endpoint"),
        model: "whisper-1".to_owned(),
        credential: CredentialClass::Byok {
            secret_ref: secret_ref.clone(),
        },
        is_pinned_family_base: true,
    }
}

pub fn byok_store(secret_ref: &SecretRef, value: &str) -> MemorySecretStore {
    let store = MemorySecretStore::new();
    store.insert(secret_ref, value);
    store
}

pub fn local_only() -> ModelPolicy {
    ModelPolicy::LocalOnly
}
