//! The dispatch chokepoint (L9): every model call in ProjectOS flows through
//! [`Gateway::complete`], in this fixed order — attribution in hand, policy
//! gate, credential resolution, transport, then exactly one ledger record.
//!
//! ## Invariant inventory (STYLE — state machine in prose)
//!
//! 1. **Policy before I/O.** `ModelPolicy::authorize` runs before credential
//!    resolution and before the transport is *selected*. A refused dispatch
//!    performs zero network I/O (proven by `tests/policy_no_network.rs`).
//!    **Selection follows the declared locality** (m1-s03, ADR-0006): the
//!    gateway holds a [`Transports`] set, and which one a dispatch gets is
//!    [`TransportSelection::for_locality`] of the choice's endpoint — never
//!    the URL, never a default. Under `local_only` the policy gate has already
//!    refused every `Remote` choice, so the selection is provably
//!    `device_local` or `in_process`, and the same oracle asserts both halves.
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
use crate::embed::{EmbedBatch, EmbedRequest, EmbedUsage, Embedder};
use crate::ledger::{CallAttribution, CostLedger, ModelCallRecord, ProviderCostKind};
use crate::policy::{ModelChoice, ModelPolicy, ModelRouting, RoutingTier};
use crate::provider::{
    CompletionRequest, CompletionSink, CompletionUsage, OUTPUT_TOKENS_REQUEST_MAX, Provider,
};
use crate::transcribe::{TranscribeRequest, TranscribeUsage, Transcriber, TranscriptSink};
use crate::transport::{TransportSelection, Transports};
use crate::weather::Weather;
use pos_foundation::WallClock;
use pos_foundation::telemetry::{Parent, Span, SpanDetail, SpanField, SpanName, SpanValue};

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
    transports: Transports<'runtime>,
    transcriber: Option<&'runtime dyn Transcriber>,
    embedder: Option<&'runtime dyn Embedder>,
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
        transports: Transports<'runtime>,
        clock: &'runtime dyn WallClock,
    ) -> Self {
        Self {
            config,
            providers,
            secrets,
            ledger,
            transports,
            transcriber: None,
            embedder: None,
            clock,
        }
    }

    /// Composes the transcription engine this gateway routes to (m1-s03).
    /// Separate from [`Self::new`] because most gateways never transcribe, and
    /// a `None` that refuses typed beats a stub that silently answers nothing.
    #[must_use]
    pub const fn with_transcriber(mut self, transcriber: &'runtime dyn Transcriber) -> Self {
        self.transcriber = Some(transcriber);
        self
    }

    /// Composes the embedding engine this gateway routes to (m1-s04). Same
    /// shape and same reason as [`Self::with_transcriber`]: most gateways
    /// never embed, and a `None` that refuses typed beats a stub that
    /// silently answers nothing.
    #[must_use]
    pub const fn with_embedder(mut self, embedder: &'runtime dyn Embedder) -> Self {
        self.embedder = Some(embedder);
        self
    }

    /// The transport this choice would select, as a value.
    ///
    /// Public because it is the m0-s10 policy oracle's assertion target: after
    /// [ADR-0006] the `local_only` guarantee is "dispatch selects a transport
    /// that cannot egress", and a guarantee nothing can read is a comment.
    ///
    /// [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md
    #[must_use]
    pub const fn transport_selection(choice: &ModelChoice) -> TransportSelection {
        TransportSelection::for_locality(choice.endpoint.locality())
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
        // `gateway.call/:provider` (m0-s15). The parent is whatever step is
        // open on this thread, so a model call always lands under the agent
        // step that asked for it. The model name is deliberately absent: a
        // span field cannot hold a string, and the ledger row already carries
        // it against the money.
        let span = Span::open(
            SpanName::GatewayCall,
            SpanDetail::from_static(choice.family.as_str()),
            Parent::Current,
        );
        span.set(
            SpanField::Project,
            SpanValue::Id(attribution.project.into_bytes()),
        );
        span.set(SpanField::Tier, SpanValue::Label(tier.as_str()));
        span.set(
            SpanField::CredentialClass,
            SpanValue::Label(choice.credential.label()),
        );
        let started_ts_ms = self.clock.now_ms();
        let outcome = self.run_call(choice, request, sink);
        let wall_ms = self.clock.now_ms().saturating_sub(started_ts_ms);
        span.set(SpanField::DurationMs, SpanValue::Millis(wall_ms));
        match &outcome {
            Ok(usage) => {
                span.set(SpanField::TokensIn, SpanValue::Count(usage.tokens_in));
                span.set(SpanField::TokensOut, SpanValue::Count(usage.tokens_out));
            }
            Err(weather) => span.set(SpanField::Outcome, SpanValue::Label(weather.code())),
        }
        let ledgered = self.record_outcome(
            attribution,
            choice,
            request,
            &outcome,
            wall_ms,
            started_ts_ms,
        );
        match &ledgered {
            Ok(()) if outcome.is_ok() => span.finish("ok"),
            // A ledger failure outranks a model success (invariant 4), so the
            // span reports what the caller will actually receive.
            Err(weather) => span.finish(weather.code()),
            Ok(()) => drop(span),
        }
        ledgered?;
        outcome
    }

    /// One window of audio through the same chokepoint (m1-s03).
    ///
    /// Identical order to [`Self::complete`] — policy, credentials, transport
    /// selection, exactly one ledger row — because transcription is a model
    /// call and the four invariants above are not per-modality. The route is
    /// `routing.transcribe`, not a tier: whisper-vs-cloud is a modality and a
    /// privacy decision, never a thinking-effort one.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`]. [`Weather::NotYetSupported`] when this gateway
    /// composed no transcriber, and [`Weather::InvalidRequest`] when it has an
    /// engine but no route — two different wiring mistakes, named apart.
    pub fn transcribe(
        &self,
        attribution: &CallAttribution,
        request: &TranscribeRequest<'_>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, Weather> {
        let Some(choice) = self.config.routing.transcribe.as_ref() else {
            // No route at all is a caller/wiring bug before any policy
            // question exists — there is nothing to authorize and nothing to
            // attribute, so it is not a ledger row.
            return Err(Weather::InvalidRequest {
                reason: "this project has no transcription route configured".to_owned(),
            });
        };
        // The engine's label when there is one, the route's family when there
        // is not. Resolved before dispatch so a missing engine still produces
        // an attributed ledger row — invariant 2 has no exceptions.
        let engine = self
            .transcriber
            .map_or_else(|| choice.family.as_str(), Transcriber::label);
        let span = Span::open(
            SpanName::GatewayCall,
            SpanDetail::from_static(engine),
            Parent::Current,
        );
        span.set(
            SpanField::Project,
            SpanValue::Id(attribution.project.into_bytes()),
        );
        span.set(
            SpanField::CredentialClass,
            SpanValue::Label(choice.credential.label()),
        );
        let started_ts_ms = self.clock.now_ms();
        let outcome = self.run_transcribe(choice, request, sink);
        let wall_ms = self.clock.now_ms().saturating_sub(started_ts_ms);
        span.set(SpanField::DurationMs, SpanValue::Millis(wall_ms));
        if let Err(weather) = &outcome {
            span.set(SpanField::Outcome, SpanValue::Label(weather.code()));
        }
        let ledgered = self.record_transcribe(
            attribution,
            choice,
            engine,
            &outcome,
            wall_ms,
            started_ts_ms,
        );
        match &ledgered {
            Ok(()) if outcome.is_ok() => span.finish("ok"),
            Err(weather) => span.finish(weather.code()),
            Ok(()) => drop(span),
        }
        ledgered?;
        outcome
    }

    fn run_transcribe(
        &self,
        choice: &ModelChoice,
        request: &TranscribeRequest<'_>,
        sink: &mut dyn TranscriptSink,
    ) -> Result<TranscribeUsage, Weather> {
        // Invariant 1: policy first — ahead of the engine check too, so a
        // `local_only` project routed at a cloud STT endpoint is refused for
        // the reason that matters rather than for whatever the wiring happens
        // to be missing.
        self.config.policy.authorize(choice)?;
        let Some(transcriber) = self.transcriber else {
            return Err(Weather::NotYetSupported {
                capability: "transcribe",
                arrives_with: "a transcription engine composed into this gateway",
            });
        };
        let auth = self.resolve_auth(&choice.credential)?;
        let selection = Self::transport_selection(choice);
        let transport = self.transports.resolve(selection)?;
        transcriber.transcribe(&auth, request, transport, sink)
    }

    /// Embeds one bounded batch through the frozen dispatch order (m1-s04).
    ///
    /// The same five steps as [`Self::complete`] and [`Self::transcribe`]:
    /// policy, credential, transport selection, engine, exactly one ledger
    /// record. Batching is the caller's — `EmbedBatchPlan` decides what one
    /// call carries — because the memory budget is stated in padded tokens
    /// and only the caller knows the token counts.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`] for every failure class, and
    /// [`Weather::LedgerFailure`] outranks a model success, because an
    /// unmetered call is an accounting hole (invariant 4).
    pub fn embed(
        &self,
        attribution: &CallAttribution,
        request: &EmbedRequest<'_>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather> {
        let Some(choice) = self.config.routing.embed.as_ref() else {
            return Err(Weather::InvalidRequest {
                reason: "this project has no embedding route configured".to_owned(),
            });
        };
        let engine = self
            .embedder
            .map_or_else(|| choice.family.as_str(), Embedder::label);
        let span = Span::open(
            SpanName::GatewayCall,
            SpanDetail::from_static(engine),
            Parent::Current,
        );
        span.set(
            SpanField::Project,
            SpanValue::Id(attribution.project.into_bytes()),
        );
        span.set(
            SpanField::CredentialClass,
            SpanValue::Label(choice.credential.label()),
        );
        let started_ts_ms = self.clock.now_ms();
        let outcome = self.run_embed(choice, request);
        let wall_ms = self.clock.now_ms().saturating_sub(started_ts_ms);
        span.set(SpanField::DurationMs, SpanValue::Millis(wall_ms));
        if let Err(weather) = &outcome {
            span.set(SpanField::Outcome, SpanValue::Label(weather.code()));
        }
        let ledgered = self.record_embed(
            attribution,
            choice,
            engine,
            &outcome,
            wall_ms,
            started_ts_ms,
        );
        match &ledgered {
            Ok(()) if outcome.is_ok() => span.finish("ok"),
            Err(weather) => span.finish(weather.code()),
            Ok(()) => drop(span),
        }
        ledgered?;
        outcome
    }

    fn run_embed(
        &self,
        choice: &ModelChoice,
        request: &EmbedRequest<'_>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather> {
        // Invariant 1: policy first — ahead of the engine check too, so a
        // `local_only` project routed at an embeddings API is refused for the
        // reason that matters rather than for whatever wiring is missing.
        self.config.policy.authorize(choice)?;
        let Some(embedder) = self.embedder else {
            return Err(Weather::NotYetSupported {
                capability: "embed",
                arrives_with: "an embedding engine composed into this gateway",
            });
        };
        let auth = self.resolve_auth(&choice.credential)?;
        let selection = Self::transport_selection(choice);
        let transport = self.transports.resolve(selection)?;
        embedder.embed(&auth, request, transport)
    }

    /// Invariant 2, for the embedding path.
    ///
    /// Unlike transcription, embedding *does* have an honest token count on
    /// both routes — a local forward pass and API pricing are both measured
    /// in tokens — so `tokens_in` is real here rather than zero. `tokens_out`
    /// is zero because an embedding produces no tokens; the vectors it does
    /// produce are counted by the pipeline's own `IngestStageFinished`.
    fn record_embed(
        &self,
        attribution: &CallAttribution,
        choice: &ModelChoice,
        engine: &'static str,
        outcome: &Result<(EmbedBatch, EmbedUsage), Weather>,
        wall_ms: u64,
        ts_ms: u64,
    ) -> Result<(), Weather> {
        let (outcome_code, tokens_in) = match outcome {
            Ok((_batch, usage)) => ("ok".to_owned(), usage.tokens_in),
            Err(weather) => (weather.code().to_owned(), 0),
        };
        let record = ModelCallRecord {
            project: attribution.project,
            feature: attribution.feature.clone(),
            agent: attribution.agent.clone(),
            provider: engine,
            credential_class: choice.credential.label(),
            model: choice.model.clone(),
            tokens_in,
            tokens_out: 0,
            wall_ms,
            provider_cost_kind: ModelCallRecord::cost_kind_for(&choice.credential, false),
            usd_micros: 0,
            outcome: outcome_code,
            ts_ms,
        };
        self.ledger
            .record(&record)
            .map_err(|error| Weather::LedgerFailure {
                reason: error.reason,
            })
    }

    /// Invariant 2, for the transcription path.
    ///
    /// Tokens are zero because audio has none, and the honest unit — audio
    /// duration — is already metered by the pipeline's own
    /// `IngestStageFinished` (`wall_ms`, `bytes_read`, `item_count`). One
    /// number, one owner (m0-s15). When managed STT pricing lands, that is a
    /// `ModelCallCompleted` v2 with an audio field, not a second meter.
    fn record_transcribe(
        &self,
        attribution: &CallAttribution,
        choice: &ModelChoice,
        engine: &'static str,
        outcome: &Result<TranscribeUsage, Weather>,
        wall_ms: u64,
        ts_ms: u64,
    ) -> Result<(), Weather> {
        let outcome_code = match outcome {
            Ok(_) => "ok".to_owned(),
            Err(weather) => weather.code().to_owned(),
        };
        let record = ModelCallRecord {
            project: attribution.project,
            feature: attribution.feature.clone(),
            agent: attribution.agent.clone(),
            provider: engine,
            credential_class: choice.credential.label(),
            model: choice.model.clone(),
            tokens_in: 0,
            tokens_out: 0,
            wall_ms,
            provider_cost_kind: ModelCallRecord::cost_kind_for(&choice.credential, false),
            usd_micros: 0,
            outcome: outcome_code,
            ts_ms,
        };
        self.ledger
            .record(&record)
            .map_err(|error| Weather::LedgerFailure {
                reason: error.reason,
            })
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
        // Invariant 1a: selection follows the declared locality, and it
        // happens *after* the policy gate — so a refused dispatch never even
        // names a transport, let alone touches one.
        let selection = Self::transport_selection(choice);
        let transport = self.transports.resolve(selection)?.ok_or({
            Weather::TransportUnavailable {
                selection: selection.as_str(),
            }
        })?;
        provider.complete(&auth, request, transport, sink)
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
            provider: choice.family.as_str(),
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
        let egress_warning = choice.endpoint.locality().leaves_device().then(|| {
            format!(
                "prompt and evidence bytes leave this device for {}",
                choice.endpoint.base_url()
            )
        });
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
