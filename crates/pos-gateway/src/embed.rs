//! The `embed` slot (m1-s04, F8/L9): one trait, a local ONNX adapter and an
//! API adapter behind it, and a batch planner whose cap is a measurement.
//!
//! ## The measurement that shaped this file
//!
//! The milestone task said `batch = min(64, budget/est_tokens)`. Running
//! bge-small-en-v1.5 under ONNX Runtime on `RM-LAPTOP-01` says that is the
//! wrong shape, and the numbers are worth keeping where the constants are:
//!
//! | batch × seq | padded tokens | activation | throughput |
//! |---|---|---|---|
//! | 1 × 512 | 512 | 43 MiB | 16.9k tok/s |
//! | 8 × 512 | 4 096 | 302 MiB | 16.8k tok/s |
//! | 16 × 512 | 8 192 | 605 MiB | 17.2k tok/s |
//! | 32 × 512 | 16 384 | 1 016 MiB | 17.1k tok/s |
//! | 64 × 512 | 32 768 | 1 920 MiB | 17.0k tok/s |
//!
//! Two facts fall out. **Throughput is flat** — ONNX Runtime already spreads
//! one sequence across cores, so a batch of 64 computes no faster per token
//! than a batch of 1. And **activation grows linearly in _padded_ tokens** at
//! ≈74 KiB each. A batch of 64 would therefore have spent 1.9 GB — past
//! [ADR-0008]'s entire 1.0 GB process ceiling — to buy nothing.
//!
//! So the budget here is stated in **padded tokens, not items**, and it is
//! small. [`EMBED_BATCH_PADDED_TOKENS_MAX`] is the cap; the item count is
//! whatever fits under it.
//!
//! ## Why the cap is a hard admission bound, not a target
//!
//! ONNX Runtime allocates activations from an arena that **grows and never
//! shrinks**. One oversized batch therefore raises this process's RSS for its
//! whole remaining life — the gate would pass on a corpus of short chunks and
//! fail forever after one long one. That is why [`EmbedBatchPlan`] refuses
//! rather than adapts, and why there is no "grow the batch when there is
//! headroom" path anywhere: headroom measured after the fact is headroom
//! already spent (L8).
//!
//! ## The input seam, versioned before it is used
//!
//! The embedded text is `optional context prefix + chunk content`, carried as
//! [`EmbedInput`] with an `enrichment_version` beside the model id. At M1 the
//! prefix is always `None` — contextual enrichment and its golden-set A/B are
//! M2 scope ([05] §5). The shape exists now so switching it on is a reprocess
//! against a new `enrichment_version`, never a schema change.
//!
//! [ADR-0008]: ../../../../docs/adr/0008-ingest-memory-budget-splits-buffers-from-model-weights.md
//! [05]: ../../../../docs/05-intelligence-context-and-data-architecture.md

use crate::credentials::CallAuth;
use crate::transport::HttpTransport;
use crate::weather::Weather;

/// Widest vector this seam will carry. 1536 is the largest dimension among
/// the API models we route to; a provider answering wider is a contract
/// change, not a bigger row, so it refuses.
pub const EMBED_DIM_MAX: u16 = 1_536;

/// Longest single input, in tokens. BERT-class encoders are trained to 512
/// and the chunker targets well under it; a longer input is truncated by the
/// adapter with the truncation *stated* in the usage, never silently (L8).
pub const EMBED_SEQUENCE_TOKENS_MAX: usize = 512;

/// The batch budget, in **padded** tokens: `item_count × longest_item`.
///
/// 1024 padded tokens ≈ 77 MiB of activation by the table above, which leaves
/// [ADR-0008] bound 3 room for whisper-small (488 MB) and the embedding
/// weights (226 MiB resident) to be loaded at the same time — the shape of
/// the full audio + text run the gate actually measures.
pub const EMBED_BATCH_PADDED_TOKENS_MAX: usize = 1_024;

/// Measured activation cost of one padded token on the reference laptop —
/// the constant behind [`EMBED_BATCH_PADDED_TOKENS_MAX`], kept beside it so a
/// future change to either has to face the other. See the module table.
pub const EMBED_ACTIVATION_BYTES_PER_PADDED_TOKEN: usize = 76_800;

/// Most items one batch may carry regardless of length. Short chunks would
/// otherwise let the padded-token budget admit thousands of rows, and per-item
/// overhead — tensor construction, the response vector — is real even when
/// the token budget is not the binding constraint.
pub const EMBED_BATCH_COUNT_MAX: usize = 64;

/// What one input to the model is. Two fields rather than one string so the
/// prefix is a *seam* and not a formatting convention some caller can skip.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct EmbedInput<'a> {
    /// M2's contextual enrichment. Always `None` at M1 — see the module doc.
    pub context_prefix: Option<&'a str>,
    pub content: &'a str,
}

impl<'a> EmbedInput<'a> {
    /// The M1 shape: content with no enrichment.
    #[must_use]
    pub const fn plain(content: &'a str) -> Self {
        Self {
            context_prefix: None,
            content,
        }
    }
}

/// `enrichment_version` for "the input is the chunk content, unmodified".
/// Stored on every vector so a later version is a reprocess with a visible
/// boundary rather than a silent mix of two input shapes in one index.
pub const ENRICHMENT_VERSION_CONTENT_ONLY: u16 = 0;

/// One request for a bounded batch of vectors.
#[derive(Clone, Copy, Debug)]
pub struct EmbedRequest<'a> {
    /// Artifact name (`bge-small-en-v1.5`) or API model id. The adapter
    /// refuses a name it is not loaded for rather than answering with the
    /// wrong model's vectors, which no downstream check could catch.
    pub model: &'a str,
    pub inputs: &'a [EmbedInput<'a>],
    pub enrichment_version: u16,
}

/// Vectors for one finished batch, row-major and flat.
///
/// Flat rather than `Vec<Vec<f32>>` because every consumer wants it flat: the
/// CAS blob is this buffer's bytes, and sqlite-vec takes a contiguous row.
/// One allocation per batch instead of one per vector.
#[derive(Clone, Debug, PartialEq)]
pub struct EmbedBatch {
    dim: u16,
    vectors: Vec<f32>,
}

impl EmbedBatch {
    /// # Errors
    ///
    /// [`Weather::MalformedOutput`] when the buffer is not exactly
    /// `count × dim` floats, or the dimension is zero or past
    /// [`EMBED_DIM_MAX`]. An adapter that miscounts would otherwise silently
    /// shift every subsequent vector by one row.
    pub fn new(dim: u16, vectors: Vec<f32>, count: usize) -> Result<Self, Weather> {
        if dim == 0 || dim > EMBED_DIM_MAX {
            return Err(Weather::MalformedOutput {
                reason: format!("embedding dimension {dim} is outside 1..={EMBED_DIM_MAX}"),
            });
        }
        let expected = count * usize::from(dim);
        if vectors.len() != expected {
            return Err(Weather::MalformedOutput {
                reason: format!(
                    "{} floats for {count} inputs at dimension {dim} (expected {expected})",
                    vectors.len()
                ),
            });
        }
        Ok(Self { dim, vectors })
    }

    #[must_use]
    pub const fn dim(&self) -> u16 {
        self.dim
    }

    #[must_use]
    pub fn count(&self) -> usize {
        self.vectors.len() / usize::from(self.dim)
    }

    /// One vector by row index, or `None` past the end.
    #[must_use]
    pub fn vector(&self, index: usize) -> Option<&[f32]> {
        let width = usize::from(self.dim);
        self.vectors.get(index * width..(index + 1) * width)
    }

    #[must_use]
    pub fn as_flat(&self) -> &[f32] {
        &self.vectors
    }

    #[must_use]
    pub fn into_flat(self) -> Vec<f32> {
        self.vectors
    }
}

/// What one finished batch cost. Tokens are the honest unit for both a local
/// forward pass and API pricing, so unlike transcription this seam meters the
/// same number everywhere.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct EmbedUsage {
    pub tokens_in: u64,
    /// Padded tokens the model actually computed — always ≥ `tokens_in`, and
    /// the number the memory budget is stated in. Reported so the padding
    /// waste a corpus causes is visible rather than inferred.
    pub padded_tokens: u64,
    pub vector_count: u64,
    /// Inputs whose token count hit [`EMBED_SEQUENCE_TOKENS_MAX`] and were
    /// cut. Never silent: L8's "visible degradation" for this seam.
    pub truncated_count: u64,
    /// `true` when the provider reported the token count; `false` when the
    /// adapter counted what it sent.
    pub measured: bool,
}

impl EmbedUsage {
    #[must_use]
    pub const fn add(self, other: Self) -> Self {
        Self {
            tokens_in: self.tokens_in.saturating_add(other.tokens_in),
            padded_tokens: self.padded_tokens.saturating_add(other.padded_tokens),
            vector_count: self.vector_count.saturating_add(other.vector_count),
            truncated_count: self.truncated_count.saturating_add(other.truncated_count),
            measured: self.measured && other.measured,
        }
    }
}

/// The embedding contract. Peers, not successors: the local ONNX adapter and
/// the API adapter both implement exactly this, and policy decides which one
/// a project gets (F43).
pub trait Embedder {
    /// Stable label for the ledger and preflight — `onnx-local`,
    /// `openai-embed`. Not a `ProviderFamily`: "the model running in this
    /// process" is not one, and saying otherwise would put a lie in the cost
    /// report.
    fn label(&self) -> &'static str;

    /// The model this embedder answers for. A request naming another model is
    /// refused rather than served.
    fn model_name(&self) -> &str;

    /// Vector width, known before the first call so a caller can reject a
    /// mixed-dimension index without spending anything.
    fn dim(&self) -> u16;

    /// Embeds one bounded batch.
    ///
    /// `transport` is `None` for an in-process model — deliberately, so an
    /// adapter that should never reach a socket structurally cannot. An API
    /// adapter handed `None` refuses typed rather than guessing.
    ///
    /// # Errors
    ///
    /// Typed [`Weather`] for every failure class: budget, malformed output,
    /// transport, provider refusal. Never a panic (STYLE).
    fn embed(
        &self,
        auth: &CallAuth,
        request: &EmbedRequest<'_>,
        transport: Option<&dyn HttpTransport>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather>;
}

/// How a run of inputs divides into batches under the padded-token budget.
///
/// Separate from any adapter because the answer must be identical whichever
/// backend runs: a local run and an API run of the same corpus produce the
/// same batches, so their costs and their failure boundaries compare.
#[derive(Clone, Copy, Debug)]
pub struct EmbedBatchPlan {
    padded_tokens_max: usize,
    count_max: usize,
}

impl Default for EmbedBatchPlan {
    fn default() -> Self {
        Self::new(EMBED_BATCH_PADDED_TOKENS_MAX, EMBED_BATCH_COUNT_MAX)
    }
}

impl EmbedBatchPlan {
    #[must_use]
    pub const fn new(padded_tokens_max: usize, count_max: usize) -> Self {
        Self {
            padded_tokens_max,
            count_max,
        }
    }

    /// Activation this plan's worst-case batch may cost, by the measured
    /// per-token constant. What a caller asserts its budget against.
    #[must_use]
    pub const fn activation_bytes_max(&self) -> usize {
        self.padded_tokens_max * EMBED_ACTIVATION_BYTES_PER_PADDED_TOKEN
    }

    /// Splits `token_counts` — one entry per input, in order — into batch
    /// lengths whose padded cost never exceeds the budget.
    ///
    /// Order is preserved rather than sorted by length. Sorting would cut
    /// padding waste, and it would also make the batch an item's vector was
    /// computed in depend on its neighbours, so re-embedding a corpus after
    /// adding one chunk would re-shuffle every batch boundary. Deterministic
    /// batches keep a resumed EMBED pass identical to an uninterrupted one
    /// (the same property TRANSCRIBE's windows have).
    ///
    /// An input longer than the whole budget still gets its own batch: the
    /// adapter truncates it to [`EMBED_SEQUENCE_TOKENS_MAX`] and says so, and
    /// a chunk that cannot be embedded at all is worse than a truncated one.
    #[must_use]
    pub fn split(&self, token_counts: &[usize]) -> Vec<usize> {
        let mut batches = Vec::new();
        let mut count = 0usize;
        let mut longest = 0usize;
        for &tokens in token_counts {
            let tokens = tokens.clamp(1, EMBED_SEQUENCE_TOKENS_MAX);
            let next_longest = longest.max(tokens);
            let next_padded = (count + 1) * next_longest;
            let fits = next_padded <= self.padded_tokens_max && count < self.count_max;
            if count > 0 && !fits {
                batches.push(count);
                count = 1;
                longest = tokens;
                continue;
            }
            count += 1;
            longest = next_longest;
        }
        if count > 0 {
            batches.push(count);
        }
        batches
    }

    /// The admission check an adapter runs before allocating anything.
    ///
    /// # Errors
    ///
    /// [`Weather::BudgetExhausted`] naming the cap that refused. A batch of
    /// one input past the sequence cap is *not* refused here — the adapter
    /// truncates it — but a batch whose padded shape exceeds the budget is,
    /// because admitting it would permanently raise this process's arena.
    pub fn check(&self, token_counts: &[usize]) -> Result<(), Weather> {
        if token_counts.is_empty() {
            return Err(Weather::InvalidRequest {
                reason: "an embedding batch with no inputs".to_owned(),
            });
        }
        if token_counts.len() > self.count_max {
            return Err(Weather::BudgetExhausted {
                limit: "embed_batch_count",
                message: format!(
                    "{} inputs exceed the {}-input batch budget",
                    token_counts.len(),
                    self.count_max
                ),
            });
        }
        let longest = token_counts
            .iter()
            .map(|tokens| (*tokens).clamp(1, EMBED_SEQUENCE_TOKENS_MAX))
            .max()
            .unwrap_or(1);
        let padded = token_counts.len() * longest;
        // One input is always admissible: truncation already bounds it to the
        // sequence cap, which is smaller than the batch budget by construction.
        if token_counts.len() > 1 && padded > self.padded_tokens_max {
            return Err(Weather::BudgetExhausted {
                limit: "embed_batch_padded_tokens",
                message: format!(
                    "{} inputs padded to {longest} tokens is {padded} padded tokens, past the \
                     {}-token batch budget (≈{} MiB of activation)",
                    token_counts.len(),
                    self.padded_tokens_max,
                    self.activation_bytes_max() / (1024 * 1024)
                ),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        EMBED_BATCH_COUNT_MAX, EMBED_BATCH_PADDED_TOKENS_MAX, EMBED_SEQUENCE_TOKENS_MAX,
        EmbedBatch, EmbedBatchPlan,
    };

    /// The AC's adversarial case: whatever the corpus looks like, no planned
    /// batch may cost more than the stated budget. The arena never shrinks, so
    /// a single admitted overrun is permanent.
    #[test]
    fn no_planned_batch_exceeds_the_padded_token_budget() {
        let plan = EmbedBatchPlan::default();
        let corpora: [Vec<usize>; 6] = [
            vec![512; 64],
            vec![1; 4096],
            (0..512).map(|index| index * 7 % 900).collect(),
            (0..512).map(|index| 512 - (index % 512)).collect(),
            vec![100_000; 8],
            vec![1, 512, 1, 512, 1, 512, 1, 512],
        ];
        for counts in corpora {
            let mut offset = 0usize;
            for length in plan.split(&counts) {
                let batch = &counts[offset..offset + length];
                offset += length;
                plan.check(batch).expect("a planned batch is admissible");
                let longest = batch
                    .iter()
                    .map(|tokens| (*tokens).clamp(1, EMBED_SEQUENCE_TOKENS_MAX))
                    .max()
                    .unwrap_or(1);
                let padded = batch.len() * longest;
                assert!(
                    batch.len() == 1 || padded <= EMBED_BATCH_PADDED_TOKENS_MAX,
                    "a batch of {} padded to {longest} costs {padded} tokens",
                    batch.len()
                );
                assert!(batch.len() <= EMBED_BATCH_COUNT_MAX);
            }
            assert_eq!(
                offset,
                counts.len(),
                "every input lands in exactly one batch"
            );
        }
    }

    /// A batch plan that depended on its neighbours would make a resumed
    /// EMBED pass produce different batches than an uninterrupted one.
    #[test]
    fn a_prefix_of_a_corpus_plans_the_same_leading_batches() {
        let plan = EmbedBatchPlan::default();
        let counts: Vec<usize> = (0..400).map(|index| (index * 13) % 500 + 1).collect();
        let whole = plan.split(&counts);
        let prefix = plan.split(&counts[..200]);
        let shared = prefix.len().saturating_sub(1);
        assert_eq!(whole[..shared], prefix[..shared]);
    }

    #[test]
    fn a_batch_refuses_a_buffer_that_is_not_count_times_dim() {
        assert!(EmbedBatch::new(4, vec![0.0; 8], 2).is_ok());
        assert!(EmbedBatch::new(4, vec![0.0; 7], 2).is_err());
        assert!(EmbedBatch::new(0, vec![], 0).is_err());
        assert!(EmbedBatch::new(u16::MAX, vec![0.0; 2], 2).is_err());
    }

    #[test]
    fn a_batch_hands_back_the_rows_it_was_given() {
        let batch = EmbedBatch::new(3, vec![1.0, 2.0, 3.0, 4.0, 5.0, 6.0], 2).expect("valid");
        assert_eq!(batch.count(), 2);
        assert_eq!(batch.vector(0), Some(&[1.0, 2.0, 3.0][..]));
        assert_eq!(batch.vector(1), Some(&[4.0, 5.0, 6.0][..]));
        assert_eq!(batch.vector(2), None);
    }
}
