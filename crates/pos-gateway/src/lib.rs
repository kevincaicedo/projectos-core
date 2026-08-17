//! # pos-gateway
//!
//! All model access (L9): provider adapters (Anthropic, OpenAI, Google, OpenRouter, OpenAI-compatible local), managed/BYOK/device-local credential references, per-project model policy enforced at dispatch, cost ledger, prompt registry, eval scaffold, model manager.
//!
//! Skeleton created by m0-s01; filled by m0-s10/s11. Charter: master plan §19.
//!
//! The one rule that organizes this crate: [`Gateway::complete`] is the only
//! path to a model, and its order is frozen — policy gate, credential
//! resolution, transport, exactly one ledger record. Adapters are codecs
//! over the [`HttpTransport`] seam; the only live transport in core is
//! loopback-only by construction (see `transport.rs`), so `local_only` is a
//! structural guarantee today, not a filtered one.

#![forbid(unsafe_code)]

mod adapter;
mod credentials;
mod dispatch;
mod eval;
mod ledger;
mod models;
mod policy;
mod prompts;
mod provider;
mod sse;
mod tls;
mod transcribe;
mod transport;
mod weather;

pub use adapter::{
    AnthropicAdapter, CloudSttAdapter, EndpointProfile, EndpointServer, GoogleAdapter,
    OpenAiAdapter, OpenAiCompatibleAdapter, OpenRouterAdapter, QualificationReport,
    WhisperLocalTranscriber, list_models, qualify_openai_compatible,
};
pub use credentials::{
    CallAuth, CredentialClass, MemorySecretStore, SecretError, SecretRef, SecretStore, SecretValue,
};
pub use dispatch::{CALL_TIMEOUT_MS_DEFAULT, Gateway, GatewayConfig, PreflightReport};
pub use eval::{
    EVAL_CASES_MAX, EvalCase, EvalError, EvalOutcome, EvalReport, load_cases, run_suite,
};
pub use ledger::{
    CallAttribution, CostLedger, LedgerError, MEMORY_LEDGER_RECORDS_MAX, MemoryLedger,
    ModelCallRecord, ProviderCostKind,
};
pub use models::{
    ModelManifest, ModelManifestEntry, ModelPullError, PullConsent, PullReport, pull_model,
};
pub use policy::{
    EndpointConfig, EndpointConfigError, EndpointLocality, ModelChoice, ModelPolicy, ModelRouting,
    RoutingTier,
};
pub use prompts::{PROMPT_LOCK_FILE_NAME, PromptError, PromptFile, PromptRegistry};
pub use provider::{
    ChatMessage, CompletionEvent, CompletionRequest, CompletionSink, CompletionUsage, EmbedRequest,
    MessageRole, OUTPUT_TOKENS_REQUEST_MAX, Provider, ProviderFamily, ReasoningEffort, SinkClosed,
    VecSink,
};
pub use sse::{SseDecoder, SseEvent, SseParseError};
pub use tls::TlsHttpTransport;
pub use transcribe::{
    AUDIO_SAMPLE_RATE_HZ, SEGMENT_TEXT_BYTES_MAX, TURN_GAP_MS, TranscribeRequest, TranscribeUsage,
    Transcriber, TranscriptSegment, TranscriptSink, VecTranscriptSink, WINDOW_MS_MAX,
    WINDOW_MS_MIN, WINDOW_SAMPLE_COUNT_MAX, mark_turns,
};
pub use transport::{
    BufferedResponse, HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, LoopbackHttpTransport,
    ResponseHandler, StreamAbort, TransportError, TransportSelection, Transports,
};
pub use weather::Weather;
