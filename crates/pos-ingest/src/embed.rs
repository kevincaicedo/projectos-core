//! EMBED (m1-s04): chunks become vectors a search can rank, without costing
//! API dollars or connectivity (L9/F7).
//!
//! ## The shape of one attempt
//!
//! ```text
//!   proj_chunks ──▶ batch planner (padded-token budget)
//!                        │
//!                        ▼  one bounded batch at a time
//!            content-hash lookup ──▶ already embedded? reuse the vector (F6)
//!                        │ no
//!                        ▼
//!            pos-gateway::embed ──▶ vectors ──▶ CAS blob
//!                                                 │
//!                        EvidenceEmbedded (durable) ◀┘
//! ```
//!
//! ## Why the batch is the unit of everything
//!
//! One batch bounds **memory** (the padded-token budget in
//! [`pos_gateway::EMBED_BATCH_PADDED_TOKENS_MAX`], measured rather than
//! guessed — ONNX Runtime's arena grows and never shrinks, so an admitted
//! overrun is permanent), it bounds **durability** (each batch's vectors
//! commit before the next one starts, so `kill -9` costs the batch in flight),
//! and it bounds **the blob** (one CAS object per batch rather than one per
//! vector, which would turn a million chunks into a million files).
//!
//! ## Resume, precisely
//!
//! A re-run reads which chunks already have a row under this
//! `(model_id, enrichment_version)` and skips them. Committed vectors are
//! facts; they are never recomputed. Because [`EmbedBatchPlan`] preserves
//! input order and never depends on a chunk's neighbours, a resumed pass
//! plans the same batches an uninterrupted one did.
//!
//! ## Invariants this stage adds to the crate's P1–P6
//!
//! - **E1 — a vector is written once per `(chunk, model, enrichment)`.** The
//!   projection key is exactly that triple, so a re-embed to a second model is
//!   *additive* rather than destructive: both coexist, and retrieval names
//!   which one it wants.
//! - **E2 — identical content under one model embeds once.** Two chunks with
//!   the same `content_hash` share a vector, and the second costs an index
//!   lookup rather than a model call (F6). The ledger is what proves it.
//! - **E3 — the vector count equals the chunk count for the pass.** A batch
//!   that produced fewer vectors than it was given inputs is a refusal, never
//!   a short write: a chunk silently missing from the index is invisible to
//!   search and to every check that only counts what is present.

use crate::IngestError;
use crate::pipeline::{StageContext, StageFailure, StageHandler, StageProduct};
use pos_domain::{
    ChunkEmbeddingFact, ChunkRecord, EMBED_BATCH_COUNT_MAX, EVIDENCE_LIST_ROW_COUNT_MAX,
    IngestStage, IngestStageOutput, find_embedding_by_content, list_chunk_embeddings, list_chunks,
};
use pos_gateway::{
    CallAttribution, CloudEmbedAdapter, CredentialClass, EMBED_SEQUENCE_TOKENS_MAX,
    ENRICHMENT_VERSION_CONTENT_ONLY, EmbedBatchPlan, EmbedInput, EmbedRequest, Embedder,
    EndpointConfig, EndpointLocality, Gateway, GatewayConfig, LoopbackHttpTransport, ModelChoice,
    ModelPolicy, ModelRouting, OnnxEmbedder, Pooling, ProviderFamily, SecretRef, TlsHttpTransport,
    Transports, Weather,
};
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, PoisonError};

/// Chunks one EMBED pass may cover. At the chunker's own 4M-chunk ceiling per
/// item this is never the binding limit for a real item; it is the L8
/// admission bound that keeps the planner's working set stated.
pub const EMBED_CHUNK_COUNT_MAX: u32 = EVIDENCE_LIST_ROW_COUNT_MAX;

/// Where embedding is routed. Selected by configuration and enforced by the
/// gateway's policy gate, exactly like every other model call (F43).
#[derive(Clone, Debug)]
pub enum EmbedRoute {
    /// An ONNX encoder in this process, via the `ort` adapter. The artifact
    /// is loaded from `models_dir/model_name/` on first use.
    LocalOnnx {
        models_dir: PathBuf,
        model_name: String,
        dim: u16,
        pooling: Pooling,
    },
    /// An OpenAI-shaped `/v1/embeddings` endpoint.
    ///
    /// Composed and reachable, but no product surface writes a credential for
    /// it yet: the per-project encrypted secret store is m1-s06's. Stated
    /// rather than implied — the same position the cloud STT route is in.
    Cloud {
        base_url: String,
        model: String,
        dim: u16,
        secret_ref: SecretRef,
    },
}

/// Everything EMBED needs that is not in the log.
#[derive(Clone, Debug)]
pub struct EmbedSetup {
    pub policy: ModelPolicy,
    pub route: EmbedRoute,
    /// Which input shape the vectors are of. `0` (content only) at M1;
    /// contextual enrichment is M2 and arrives as a new version plus a
    /// reprocess, never as a silent change to what `0` meant.
    pub enrichment_version: u16,
}

/// The reference local model: bge-small-en-v1.5, 384 dimensions, CLS-pooled.
pub const EMBED_MODEL_DEFAULT: &str = "bge-small-en-v1.5";

/// [`EMBED_MODEL_DEFAULT`]'s width. Declared here as well as in the manifest
/// because a mismatch must fail at load rather than after a corpus is indexed.
pub const EMBED_DIM_DEFAULT: u16 = 384;

impl EmbedSetup {
    /// The default every shell composes: the local ONNX model, `local_only`,
    /// content-only inputs. Local-first is L9/F7, not a preference — a default
    /// that sent every chunk of a project to an API would be a privacy
    /// decision made by a constant.
    #[must_use]
    pub fn local(models_dir: PathBuf) -> Self {
        Self {
            policy: ModelPolicy::LocalOnly,
            route: EmbedRoute::LocalOnnx {
                models_dir,
                model_name: EMBED_MODEL_DEFAULT.to_owned(),
                dim: EMBED_DIM_DEFAULT,
                pooling: Pooling::ClassifyToken,
            },
            enrichment_version: ENRICHMENT_VERSION_CONTENT_ONLY,
        }
    }

    #[must_use]
    pub fn model_name(&self) -> &str {
        match &self.route {
            EmbedRoute::LocalOnnx { model_name, .. } => model_name,
            EmbedRoute::Cloud { model, .. } => model,
        }
    }

    #[must_use]
    pub const fn dim(&self) -> u16 {
        match &self.route {
            EmbedRoute::LocalOnnx { dim, .. } | EmbedRoute::Cloud { dim, .. } => *dim,
        }
    }

    fn choice(&self) -> ModelChoice {
        match &self.route {
            EmbedRoute::LocalOnnx { model_name, .. } => ModelChoice {
                // The family names the wire shape, and an in-process model has
                // none; the ledger's engine label carries the honest answer
                // ("onnx-local") because the record's `provider` column is the
                // embedder's label, not this field.
                family: ProviderFamily::OpenAiCompatible,
                endpoint: EndpointConfig::in_process("onnx-local"),
                model: model_name.clone(),
                credential: CredentialClass::DeviceSession {
                    adapter: "onnx".to_owned(),
                    device: pos_foundation::DeviceId::from_bytes([0; 16]),
                },
                is_pinned_family_base: false,
            },
            EmbedRoute::Cloud {
                base_url,
                model,
                secret_ref,
                ..
            } => ModelChoice {
                family: ProviderFamily::OpenAi,
                endpoint: EndpointConfig::new(base_url.clone(), EndpointLocality::Remote)
                    .unwrap_or_else(|_| EndpointConfig::in_process("cloud-embed-misconfigured")),
                model: model.clone(),
                credential: CredentialClass::Byok {
                    secret_ref: secret_ref.clone(),
                },
                is_pinned_family_base: false,
            },
        }
    }
}

/// The EMBED stage handler.
///
/// It caches the loaded ONNX session: the weights are 226 MiB resident and
/// loading them per batch would dominate the throughput gate. The cache is
/// keyed by model name, so a configuration change loads the new model and
/// drops the old rather than holding both.
pub struct EmbedStage {
    setup: EmbedSetup,
    loaded: Mutex<Option<(String, Arc<OnnxEmbedder>)>>,
    /// An engine composed by the caller instead of resolved from the route.
    /// Not a test hook: it is how a peer implementation arrives, and that
    /// tests use it is a consequence of the seam being real.
    composed: Option<Arc<dyn Embedder + Send + Sync>>,
}

impl EmbedStage {
    #[must_use]
    pub fn new(setup: EmbedSetup) -> Self {
        Self {
            setup,
            loaded: Mutex::new(None),
            composed: None,
        }
    }

    /// Runs `engine` instead of resolving one from the route.
    #[must_use]
    pub fn with_engine(setup: EmbedSetup, engine: Arc<dyn Embedder + Send + Sync>) -> Self {
        Self {
            setup,
            loaded: Mutex::new(None),
            composed: Some(engine),
        }
    }

    #[must_use]
    pub const fn setup(&self) -> &EmbedSetup {
        &self.setup
    }

    /// The loaded local engine, loading it on first use.
    fn local_engine(&self) -> Result<Arc<OnnxEmbedder>, StageFailure> {
        let EmbedRoute::LocalOnnx {
            models_dir,
            model_name,
            dim,
            pooling,
        } = &self.setup.route
        else {
            return Err(StageFailure::permanent(
                "embed_route_mismatch",
                "the configured route is not a local ONNX model",
            ));
        };
        let mut loaded = self.loaded.lock().unwrap_or_else(PoisonError::into_inner); // INVARIANT: a poisoned cache is replaced below, never read.
        if let Some((name, engine)) = loaded.as_ref()
            && name == model_name
        {
            return Ok(Arc::clone(engine));
        }
        let dir = OnnxEmbedder::artifact_dir(models_dir, model_name);
        if !dir.join("model.onnx").is_file() {
            // Retriable on purpose: `pos models pull bge-small-en-v1.5` fixes
            // it without touching the item, and the DLQ reason says exactly
            // that.
            return Err(StageFailure::retriable(
                "embed_model_missing",
                format!(
                    "the embedding artifact {model_name:?} is not at {}; pull it with \
                     `pos models pull {model_name}`",
                    dir.display()
                ),
            ));
        }
        let engine = Arc::new(
            OnnxEmbedder::load(model_name, &dir, *dim, *pooling)
                .map_err(|weather| weather_failure("embed_model_load", &weather))?,
        );
        *loaded = Some((model_name.clone(), Arc::clone(&engine)));
        Ok(engine)
    }
}

impl StageHandler for EmbedStage {
    fn stage(&self) -> IngestStage {
        IngestStage::Embed
    }

    fn run(&self, context: &StageContext<'_>) -> Result<StageProduct, StageFailure> {
        // The engine outlives the gateway that borrows it.
        let local;
        let cloud;
        let engine: &dyn Embedder = match &self.setup.route {
            _ if self.composed.is_some() => {
                let Some(engine) = self.composed.as_ref() else {
                    return Err(StageFailure::permanent(
                        "embed_engine_missing",
                        "the composed engine vanished between the guard and the read",
                    ));
                };
                engine.as_ref()
            }
            EmbedRoute::LocalOnnx { .. } => {
                local = self.local_engine()?;
                local.as_ref()
            }
            EmbedRoute::Cloud {
                base_url,
                model,
                dim,
                ..
            } => {
                cloud = CloudEmbedAdapter {
                    base_url: base_url.clone(),
                    model: model.clone(),
                    dim: *dim,
                };
                &cloud
            }
        };
        let ledger = context.open_ledger();
        let loopback = LoopbackHttpTransport;
        let tls = TlsHttpTransport::new();
        let choice = self.setup.choice();
        let gateway = Gateway::new(
            GatewayConfig {
                policy: self.setup.policy.clone(),
                // Embedding needs no thinking tiers; routing them at the same
                // choice keeps the type total without inventing a completion
                // route this stage will never call.
                routing: ModelRouting::thinking_only(choice.clone(), choice.clone())
                    .with_embed(choice),
            },
            Vec::new(),
            context.secrets(),
            ledger.as_ref(),
            Transports::new(&loopback, &tls),
            context.clock(),
        )
        .with_embedder(engine);
        embed_item(context, &gateway, &self.setup, engine.dim())
    }
}

/// The plan/embed/commit loop. Split from the handler so the handler is
/// composition and this is control flow (STYLE: push `if`s up).
fn embed_item(
    context: &StageContext<'_>,
    gateway: &Gateway<'_>,
    setup: &EmbedSetup,
    dim: u16,
) -> Result<StageProduct, StageFailure> {
    if dim != setup.dim() {
        return Err(StageFailure::permanent(
            "embed_dim_mismatch",
            format!(
                "the engine is {dim}-dimensional but the route declares {} — one index \
                 cannot hold both",
                setup.dim()
            ),
        ));
    }
    // The evidence's *current* chunk pass, not this stage's pass. They differ
    // exactly when someone reprocesses from EMBED — `pos ingest reembed
    // --model X` is that command — where the stage advances to pass 1 while
    // the chunks it must embed are still the ones CHUNK wrote at pass 0.
    // Reading by stage pass would find nothing and report a re-embed that
    // silently embedded zero chunks.
    let chunks = list_chunks(
        context.log(),
        context.evidence().evidence_id,
        None,
        EMBED_CHUNK_COUNT_MAX,
    )
    .map_err(|error| read_failure("embed_chunks_unreadable", &error))?;
    if chunks.is_empty() {
        // Not a failure: an item whose CHUNK pass produced nothing has
        // nothing to embed, and refusing would dead-letter a correct item.
        return Ok(StageProduct {
            output: IngestStageOutput::None,
            bytes_read: 0,
            item_count: 0,
        });
    }
    let model_id = setup.model_name().to_owned();
    let done = already_embedded(context, &model_id, setup.enrichment_version)?;
    let attribution = CallAttribution {
        project: context.project_id(),
        feature: IngestStage::Embed.cost_feature().to_owned(),
        agent: None,
    };
    let mut writer = Batcher::new(context, setup, &model_id, dim);
    for chunk in &chunks {
        if done.contains_key(&chunk.chunk_id.into_bytes()) {
            // E1: a committed vector is a fact. Never recomputed.
            writer.skip_committed();
            continue;
        }
        writer.push(chunk, gateway, &attribution)?;
    }
    writer.finish(gateway, &attribution)
}

/// Chunk ids of this item that already have a vector under this model, so a
/// resumed pass recomputes nothing.
fn already_embedded(
    context: &StageContext<'_>,
    model_id: &str,
    enrichment_version: u16,
) -> Result<HashMap<[u8; 16], ()>, StageFailure> {
    let rows = list_chunk_embeddings(
        context.log(),
        context.evidence().evidence_id,
        model_id,
        enrichment_version,
        EMBED_CHUNK_COUNT_MAX,
    )
    .map_err(|error| read_failure("embed_rows_unreadable", &error))?;
    Ok(rows
        .into_iter()
        .map(|row| (row.chunk_id.into_bytes(), ()))
        .collect())
}

/// Accumulates chunks into batches under the padded-token budget, dispatches
/// each one, and commits its vectors before the next begins.
struct Batcher<'a, 'b> {
    context: &'a StageContext<'b>,
    setup: &'a EmbedSetup,
    model_id: &'a str,
    dim: u16,
    plan: EmbedBatchPlan,
    /// The most items one batch may carry: the stricter of the event-ref
    /// bound (a fact per chunk) and the gateway's own per-call bound.
    batch_count_max: usize,
    pending: Vec<PendingChunk>,
    /// Token counts of `pending` plus the chunk being considered, so the
    /// planner is asked about the batch that *would* exist.
    candidate_tokens: Vec<usize>,
    longest_tokens: usize,
    batch_index: u32,
    vector_count: u64,
    bytes_read: u64,
}

/// One chunk waiting for its batch to fill.
struct PendingChunk {
    chunk_id: [u8; 16],
    content_hash: [u8; 32],
    text: String,
    token_count: usize,
    truncated: bool,
    /// A vector already computed for this exact content under this exact
    /// model (F6) — reused rather than recomputed, so the ledger shows one
    /// embed cost for a duplicate attachment.
    reuse: Option<ReusedVector>,
}

/// Where an identical chunk's vector already lives.
struct ReusedVector {
    vectors_blob: [u8; 32],
    row: u32,
}

impl<'a, 'b> Batcher<'a, 'b> {
    fn new(
        context: &'a StageContext<'b>,
        setup: &'a EmbedSetup,
        model_id: &'a str,
        dim: u16,
    ) -> Self {
        Self {
            context,
            setup,
            model_id,
            dim,
            plan: EmbedBatchPlan::default(),
            batch_count_max: EMBED_BATCH_COUNT_MAX.min(pos_gateway::EMBED_BATCH_COUNT_MAX),
            pending: Vec::new(),
            candidate_tokens: Vec::new(),
            longest_tokens: 0,
            batch_index: 0,
            vector_count: 0,
            bytes_read: 0,
        }
    }

    /// A chunk whose vector is already committed under this model.
    const fn skip_committed(&mut self) {
        self.vector_count = self.vector_count.saturating_add(1);
    }

    /// Adds one chunk, flushing first when it would not fit the budget.
    fn push(
        &mut self,
        chunk: &ChunkRecord,
        gateway: &Gateway<'_>,
        attribution: &CallAttribution,
    ) -> Result<(), StageFailure> {
        let text = self.chunk_text(chunk)?;
        self.bytes_read = self.bytes_read.saturating_add(text.len() as u64);
        // The tokenizer's own count, not the chunker's estimate: the budget is
        // stated in tokens the model will actually compute.
        let token_count = self
            .estimate_tokens(&text)
            .clamp(1, EMBED_SEQUENCE_TOKENS_MAX);
        let reuse = find_embedding_by_content(
            self.context.log(),
            chunk.content_hash,
            self.model_id,
            self.setup.enrichment_version,
        )
        .map_err(|error| read_failure("embed_content_lookup", &error))?
        .map(|row| ReusedVector {
            vectors_blob: row.vectors_blob,
            row: row.row,
        });
        let pending = PendingChunk {
            chunk_id: chunk.chunk_id.into_bytes(),
            content_hash: chunk.content_hash,
            truncated: token_count >= EMBED_SEQUENCE_TOKENS_MAX,
            text,
            token_count,
            reuse,
        };
        // A reused vector costs no model call, so it never enters the budget —
        // it is committed on its own, pointing at the blob that already holds
        // it (E2).
        if pending.reuse.is_some() {
            return self.commit_reused(pending);
        }
        // The planner owns the budget rule; asking it whether the *candidate*
        // batch is admissible keeps one implementation of "what fits" instead
        // of a second copy that could drift from the one the adapter checks.
        self.candidate_tokens.push(pending.token_count);
        let admissible = self.candidate_tokens.len() <= self.batch_count_max
            && self.plan.check(&self.candidate_tokens).is_ok();
        if !self.pending.is_empty() && !admissible {
            self.flush(gateway, attribution)?;
            self.candidate_tokens.push(pending.token_count);
        }
        self.longest_tokens = self.longest_tokens.max(pending.token_count);
        self.pending.push(pending);
        Ok(())
    }

    /// The chunk's text, read from the normalized blob by byte range.
    fn chunk_text(&self, chunk: &ChunkRecord) -> Result<String, StageFailure> {
        let text = self
            .context
            .read_text_range(chunk.byte_start, chunk.byte_end)
            .map_err(|error| ingest_failure("embed_text_unreadable", &error))?;
        Ok(text)
    }

    /// Tokens this text will cost, without loading a vocabulary.
    ///
    /// The exact count comes from the adapter and is what lands in the fact;
    /// this is the planner's *admission* estimate, and it is deliberately an
    /// over-estimate — planning a batch too small wastes nothing measurable
    /// (throughput is flat in batch size), where planning one too large would
    /// grow an arena that never shrinks.
    fn estimate_tokens(&self, text: &str) -> usize {
        // ~3 characters per WordPiece token on English prose, rounded down to
        // over-estimate; the 4-chars-per-token rule of thumb is for byte-level
        // BPE and under-counts WordPiece's subword splits.
        text.chars().count().div_ceil(3).max(1)
    }

    /// Commits a chunk whose vector another chunk already computed.
    fn commit_reused(&mut self, pending: PendingChunk) -> Result<(), StageFailure> {
        let Some(reuse) = pending.reuse else {
            return Err(StageFailure::permanent(
                "embed_reuse_missing",
                "a reused vector vanished between the guard and the read",
            ));
        };
        let fact = ChunkEmbeddingFact {
            chunk_id: pos_foundation::ChunkId::from_bytes(pending.chunk_id),
            row: reuse.row,
            content_hash: pending.content_hash,
            token_count: u32::try_from(pending.token_count).unwrap_or(u32::MAX),
            truncated: pending.truncated,
        };
        self.context
            .emit_embeddings(
                self.batch_index,
                self.model_id,
                self.dim,
                self.setup.enrichment_version,
                reuse.vectors_blob,
                vec![fact],
            )
            .map_err(|error| ingest_failure("embed_commit", &error))?;
        self.batch_index = self.batch_index.saturating_add(1);
        self.vector_count = self.vector_count.saturating_add(1);
        Ok(())
    }

    /// Dispatches the pending batch, writes its vectors to the CAS, and
    /// commits the fact — in that order, so a crash leaves an unreferenced
    /// blob the sweep collects rather than a dangling reference (P2).
    fn flush(
        &mut self,
        gateway: &Gateway<'_>,
        attribution: &CallAttribution,
    ) -> Result<(), StageFailure> {
        if self.pending.is_empty() {
            return Ok(());
        }
        let pending = std::mem::take(&mut self.pending);
        self.candidate_tokens.clear();
        self.longest_tokens = 0;
        let inputs: Vec<EmbedInput<'_>> = pending
            .iter()
            .map(|chunk| EmbedInput {
                // Always `None` at M1; the seam exists so M2 can fill it.
                context_prefix: None,
                content: &chunk.text,
            })
            .collect();
        let request = EmbedRequest {
            model: self.model_id,
            inputs: &inputs,
            enrichment_version: self.setup.enrichment_version,
        };
        let (batch, usage) = gateway
            .embed(attribution, &request)
            .map_err(|weather| weather_failure("embed_call", &weather))?;
        // E3: a short answer is a refusal, never a silent partial index.
        if batch.count() != pending.len() {
            return Err(StageFailure::permanent(
                "embed_count_mismatch",
                format!(
                    "the engine returned {} vectors for {} chunks",
                    batch.count(),
                    pending.len()
                ),
            ));
        }
        let vectors_blob = self.write_vectors(&batch)?;
        let facts: Vec<ChunkEmbeddingFact> = pending
            .iter()
            .enumerate()
            .map(|(row, chunk)| ChunkEmbeddingFact {
                chunk_id: pos_foundation::ChunkId::from_bytes(chunk.chunk_id),
                row: u32::try_from(row).unwrap_or(u32::MAX),
                content_hash: chunk.content_hash,
                token_count: u32::try_from(chunk.token_count).unwrap_or(u32::MAX),
                truncated: chunk.truncated,
            })
            .collect();
        let count = facts.len() as u64;
        self.context
            .emit_embeddings(
                self.batch_index,
                self.model_id,
                self.dim,
                self.setup.enrichment_version,
                vectors_blob,
                facts,
            )
            .map_err(|error| ingest_failure("embed_commit", &error))?;
        self.batch_index = self.batch_index.saturating_add(1);
        self.vector_count = self.vector_count.saturating_add(count);
        debug_assert_eq!(
            usage.vector_count, count,
            "the usage and the committed facts count the same batch"
        );
        Ok(())
    }

    /// Writes the packed batch to the CAS as little-endian `f32`.
    ///
    /// Little-endian explicitly rather than `to_ne_bytes`: a project directory
    /// is portable (L4), and a blob whose meaning depended on the byte order
    /// of the machine that wrote it would silently produce garbage vectors on
    /// the machine that read it.
    fn write_vectors(&self, batch: &pos_gateway::EmbedBatch) -> Result<[u8; 32], StageFailure> {
        let mut writer = self
            .context
            .blob_writer()
            .map_err(|error| ingest_failure("embed_blob_open", &error))?;
        for value in batch.as_flat() {
            writer
                .append(&value.to_le_bytes())
                .map_err(|error| store_failure("embed_blob_write", &error))?;
        }
        let hash = writer
            .finish()
            .map_err(|error| store_failure("embed_blob_finish", &error))?;
        Ok(hash.into_bytes())
    }

    /// Flushes the last batch and reports what the pass produced.
    fn finish(
        mut self,
        gateway: &Gateway<'_>,
        attribution: &CallAttribution,
    ) -> Result<StageProduct, StageFailure> {
        self.flush(gateway, attribution)?;
        Ok(StageProduct {
            // The durable output is entirely in this stage's own projection
            // and the CAS; there is nothing for the evidence row to assign.
            output: IngestStageOutput::None,
            bytes_read: self.bytes_read,
            item_count: self.vector_count,
        })
    }
}

fn weather_failure(code: &'static str, weather: &Weather) -> StageFailure {
    // A policy refusal, a missing credential, and a wrong dimension cannot be
    // fixed by retrying; weather can. The gateway already classifies this, so
    // the stage reads its answer rather than inventing a second taxonomy.
    let permanent = matches!(
        weather,
        Weather::PolicyViolation { .. }
            | Weather::InvalidRequest { .. }
            | Weather::Refusal { .. }
            | Weather::UnsupportedField { .. }
            | Weather::CredentialRevoked
            | Weather::NotYetSupported { .. }
    );
    let detail = format!("{code}: {}", weather.code());
    if permanent {
        StageFailure::permanent(weather.code(), detail)
    } else {
        StageFailure::retriable(weather.code(), detail)
    }
}

fn read_failure(code: &'static str, error: &pos_domain::EvidenceReadError) -> StageFailure {
    StageFailure::retriable(code, error.to_string())
}

fn ingest_failure(code: &'static str, error: &IngestError) -> StageFailure {
    StageFailure::retriable(code, error.to_string())
}

fn store_failure(code: &'static str, error: &pos_store::StoreError) -> StageFailure {
    StageFailure::retriable(code, error.to_string())
}
