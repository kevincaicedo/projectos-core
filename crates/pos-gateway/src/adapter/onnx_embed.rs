//! The local ONNX embedding adapter — **the only module in ProjectOS that may
//! name an `ort::` symbol** (m1-s04, [ADR-0009] §1).
//!
//! ## The containment rule, and why it is mechanical
//!
//! Same shape as the `whisper_rs` rule m1-s03 established: `check-discipline`
//! fails the build on any `ort::` path or `use ort` outside this file, with
//! its own seeded violation fixture. Everything above here speaks
//! [`Embedder`]/[`EmbedBatch`] and cannot tell which backend answered. The
//! debt is real — `ort` is a pre-1.0 release candidate — and it is payable
//! only if swapping it stays a one-file change.
//!
//! ## Memory, stated
//!
//! Loading the model is the expensive act: bge-small-en-v1.5's 133 MB
//! artifact is **226 MiB resident** once ONNX Runtime materializes it. One
//! adapter owns one loaded session for one model for its whole life; a
//! request naming a different model refuses rather than quietly loading a
//! second copy.
//!
//! Activation is on top, and it is why [`crate::embed`]'s budget is stated in
//! padded tokens. **ONNX Runtime allocates from an arena that grows and never
//! shrinks**, so the peak is set by the largest batch this process ever ran —
//! not by the current one. A single admitted overrun is permanent, which is
//! why the plan's cap is checked before the tensors are built rather than
//! after.
//!
//! ## Pooling
//!
//! bge is a CLS-pooled model: the sentence vector is position 0 of
//! `last_hidden_state`, L2-normalized. Mean pooling — the other common
//! convention, and what e5 wants — would produce plausible-looking vectors
//! that retrieve measurably worse, which is the failure class this whole
//! story is built to make impossible to ship silently. The convention is
//! therefore a declared property of the loaded model, not a guess.
//!
//! [ADR-0009]: ../../../../../docs/adr/0009-vectors-are-a-cas-backed-derived-index.md

use crate::credentials::CallAuth;
use crate::embed::{
    EMBED_SEQUENCE_TOKENS_MAX, EmbedBatch, EmbedBatchPlan, EmbedRequest, EmbedUsage, Embedder,
};
use crate::transport::HttpTransport;
use crate::weather::Weather;
use crate::wordpiece::WordPiece;
use std::path::{Path, PathBuf};
use std::sync::{Mutex, PoisonError};

/// How a model turns token states into one sentence vector. Declared per
/// model rather than inferred, because both conventions produce a
/// well-formed vector and only one of them is correct for a given model.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Pooling {
    /// Position 0 (`[CLS]`), then L2-normalize. bge-class.
    ClassifyToken,
    /// Attention-masked mean over positions, then L2-normalize. e5-class.
    Mean,
}

/// Inference threads. Leaving one core for the rest of the app is the §18
/// "never starves interactive use" rule, in the one place that would
/// otherwise eat the machine — the same choice the whisper adapter makes.
fn inference_thread_count() -> usize {
    std::thread::available_parallelism()
        .map_or(1, std::num::NonZeroUsize::get)
        .saturating_sub(1)
        .max(1)
}

/// A loaded ONNX encoder and its tokenizer.
pub struct OnnxEmbedder {
    /// The artifact name callers route to (`bge-small-en-v1.5`), not a path —
    /// paths are machine-local and would make a ledger row unreadable.
    model_name: String,
    dim: u16,
    pooling: Pooling,
    vocab: WordPiece,
    /// Behind a mutex because `Session::run` needs `&mut`, and one forward
    /// pass at a time per loaded model is what the memory budget above
    /// already assumes.
    session: Mutex<ort::session::Session>,
}

impl std::fmt::Debug for OnnxEmbedder {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("OnnxEmbedder")
            .field("model_name", &self.model_name)
            .field("dim", &self.dim)
            .field("pooling", &self.pooling)
            .finish_non_exhaustive()
    }
}

impl OnnxEmbedder {
    /// Loads `model.onnx` and `vocab.txt` from `model_dir` and pins this
    /// adapter to `model_name`.
    ///
    /// # Errors
    ///
    /// [`Weather::InvalidRequest`] when either artifact is missing or is not
    /// what it claims to be. Operator errors with an obvious fix, so they are
    /// typed and named rather than a panic on a `None`.
    pub fn load(
        model_name: &str,
        model_dir: &Path,
        dim: u16,
        pooling: Pooling,
    ) -> Result<Self, Weather> {
        let vocab_path = model_dir.join("vocab.txt");
        let vocab_text =
            std::fs::read_to_string(&vocab_path).map_err(|error| Weather::InvalidRequest {
                reason: format!(
                    "embedding model {model_name:?} has no vocabulary at {}: {error}",
                    vocab_path.display()
                ),
            })?;
        let vocab =
            WordPiece::from_vocab_text(&vocab_text).map_err(|error| Weather::InvalidRequest {
                reason: format!("embedding model {model_name:?} vocabulary is unusable: {error}"),
            })?;
        let graph_path = model_dir.join("model.onnx");
        let load = || -> Result<ort::session::Session, String> {
            let builder = ort::session::Session::builder().map_err(|error| error.to_string())?;
            let mut builder = builder
                .with_intra_threads(inference_thread_count())
                .map_err(|error| error.to_string())?;
            builder
                .commit_from_file(&graph_path)
                .map_err(|error| error.to_string())
        };
        let session = load().map_err(|error| Weather::InvalidRequest {
            reason: format!(
                "embedding model {model_name:?} did not load from {}: {error}",
                graph_path.display()
            ),
        })?;
        Ok(Self {
            model_name: model_name.to_owned(),
            dim,
            pooling,
            vocab,
            session: Mutex::new(session),
        })
    }

    /// The directory `pos models pull` puts an embedding artifact in.
    #[must_use]
    pub fn artifact_dir(models_root: &Path, model_name: &str) -> PathBuf {
        models_root.join(model_name)
    }

    /// Encodes and pads one batch into the three parallel arrays every
    /// BERT-family graph takes, returning the padded shape it built.
    fn encode_batch(&self, request: &EmbedRequest<'_>) -> Result<EncodedBatch, Weather> {
        let mut encodings = Vec::with_capacity(request.inputs.len());
        let mut token_counts = Vec::with_capacity(request.inputs.len());
        let mut tokens_in = 0u64;
        let mut truncated_count = 0u64;
        for input in request.inputs {
            // The versioned input seam: prefix and content are joined here,
            // in one place, so M2 can switch enrichment on without any caller
            // learning a formatting convention.
            let text = match input.context_prefix {
                Some(prefix) => format!("{prefix}\n\n{}", input.content),
                None => input.content.to_owned(),
            };
            let encoding = self.vocab.encode(&text, EMBED_SEQUENCE_TOKENS_MAX);
            tokens_in += encoding.len() as u64;
            if encoding.truncated {
                truncated_count += 1;
            }
            token_counts.push(encoding.len());
            encodings.push(encoding);
        }
        // Admission before allocation: past this point the arena grows.
        EmbedBatchPlan::default().check(&token_counts)?;
        let width = token_counts.iter().copied().max().unwrap_or(0).max(1);
        let count = encodings.len();
        let mut input_ids = vec![self.vocab.id_pad(); count * width];
        let mut attention_mask = vec![0i64; count * width];
        let token_type_ids = vec![0i64; count * width];
        for (row, encoding) in encodings.iter().enumerate() {
            let start = row * width;
            input_ids[start..start + encoding.len()].copy_from_slice(&encoding.input_ids);
            attention_mask[start..start + encoding.len()].copy_from_slice(&encoding.attention_mask);
        }
        Ok(EncodedBatch {
            count,
            width,
            input_ids,
            attention_mask,
            token_type_ids,
            usage: EmbedUsage {
                tokens_in,
                padded_tokens: (count * width) as u64,
                vector_count: count as u64,
                truncated_count,
                measured: true,
            },
        })
    }

    /// Pools `last_hidden_state` into one L2-normalized vector per row.
    fn pool(&self, batch: &EncodedBatch, states: &[f32]) -> Result<Vec<f32>, Weather> {
        let width = usize::from(self.dim);
        let expected = batch.count * batch.width * width;
        if states.len() != expected {
            return Err(Weather::MalformedOutput {
                reason: format!(
                    "the model answered {} floats for a [{}, {}, {width}] hidden state \
                     (expected {expected}) — is the declared dimension right?",
                    states.len(),
                    batch.count,
                    batch.width
                ),
            });
        }
        let mut pooled = vec![0.0f32; batch.count * width];
        for row in 0..batch.count {
            let target = &mut pooled[row * width..(row + 1) * width];
            match self.pooling {
                Pooling::ClassifyToken => {
                    let start = row * batch.width * width;
                    target.copy_from_slice(&states[start..start + width]);
                }
                Pooling::Mean => {
                    let mut counted = 0u32;
                    for position in 0..batch.width {
                        if batch.attention_mask[row * batch.width + position] == 0 {
                            continue;
                        }
                        counted += 1;
                        let start = (row * batch.width + position) * width;
                        for (slot, value) in target.iter_mut().zip(&states[start..start + width]) {
                            *slot += *value;
                        }
                    }
                    let divisor = f32::from(u16::try_from(counted.max(1)).unwrap_or(u16::MAX));
                    for slot in target.iter_mut() {
                        *slot /= divisor;
                    }
                }
            }
            normalize_l2(target)?;
        }
        Ok(pooled)
    }
}

/// One padded batch, ready for the graph.
struct EncodedBatch {
    count: usize,
    width: usize,
    input_ids: Vec<i64>,
    attention_mask: Vec<i64>,
    token_type_ids: Vec<i64>,
    usage: EmbedUsage,
}

/// Scales a vector to unit length.
///
/// Cosine similarity over normalized vectors is a dot product, which is what
/// sqlite-vec's distance functions assume; normalizing once here means every
/// later comparison is one multiply-add per dimension instead of two norms.
///
/// # Errors
///
/// [`Weather::MalformedOutput`] on a zero or non-finite vector. Both are the
/// model having failed, and a normalized NaN would poison every distance it
/// ever participates in — silently, and forever, because the vector is stored.
fn normalize_l2(vector: &mut [f32]) -> Result<(), Weather> {
    let norm = vector
        .iter()
        .map(|value| f64::from(*value) * f64::from(*value))
        .sum::<f64>()
        .sqrt();
    if !norm.is_finite() || norm <= 0.0 {
        return Err(Weather::MalformedOutput {
            reason: format!("the model produced a vector with L2 norm {norm}"),
        });
    }
    #[expect(
        clippy::cast_possible_truncation,
        reason = "the accumulator is f64 for precision; the stored vector is f32 by design"
    )]
    let scale = (1.0 / norm) as f32;
    for value in vector.iter_mut() {
        *value *= scale;
        if !value.is_finite() {
            return Err(Weather::MalformedOutput {
                reason: "the model produced a non-finite vector component".to_owned(),
            });
        }
    }
    Ok(())
}

impl Embedder for OnnxEmbedder {
    fn label(&self) -> &'static str {
        "onnx-local"
    }

    fn model_name(&self) -> &str {
        &self.model_name
    }

    fn dim(&self) -> u16 {
        self.dim
    }

    fn embed(
        &self,
        _auth: &CallAuth,
        request: &EmbedRequest<'_>,
        transport: Option<&dyn HttpTransport>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather> {
        // An in-process model is handed no transport by construction. Being
        // handed one means the gateway routed this call as if it were remote,
        // which is a wiring bug worth refusing loudly rather than ignoring.
        if transport.is_some() {
            return Err(Weather::InvalidRequest {
                reason: "the local embedder was handed a transport; it never opens a socket"
                    .to_owned(),
            });
        }
        if request.model != self.model_name {
            return Err(Weather::InvalidRequest {
                reason: format!(
                    "this embedder is loaded for {:?}, not {:?} — a vector from the wrong \
                     model is undetectable downstream",
                    self.model_name, request.model
                ),
            });
        }
        let batch = self.encode_batch(request)?;
        let mut session = self.session.lock().unwrap_or_else(PoisonError::into_inner);
        let shape = [batch.count, batch.width];
        let outputs = session
            .run(ort::inputs![
                "input_ids" => ort::value::Tensor::from_array((shape, batch.input_ids.clone()))
                    .map_err(|error| tensor_failed(&error))?,
                "attention_mask" => ort::value::Tensor::from_array((shape, batch.attention_mask.clone()))
                    .map_err(|error| tensor_failed(&error))?,
                "token_type_ids" => ort::value::Tensor::from_array((shape, batch.token_type_ids.clone()))
                    .map_err(|error| tensor_failed(&error))?,
            ])
            .map_err(|error| Weather::MalformedOutput {
                reason: format!("the embedding graph refused the batch: {error}"),
            })?;
        let (_shape, states) =
            outputs[0]
                .try_extract_tensor::<f32>()
                .map_err(|error| Weather::MalformedOutput {
                    reason: format!("the embedding graph produced no float hidden state: {error}"),
                })?;
        let pooled = self.pool(&batch, states)?;
        drop(outputs);
        drop(session);
        let vectors = EmbedBatch::new(self.dim, pooled, batch.count)?;
        Ok((vectors, batch.usage))
    }
}

fn tensor_failed(error: &ort::Error) -> Weather {
    Weather::InvalidRequest {
        reason: format!("the batch could not be shaped into a tensor: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::normalize_l2;

    #[test]
    fn normalizing_scales_to_unit_length() {
        let mut vector = [3.0f32, 4.0];
        normalize_l2(&mut vector).expect("a finite non-zero vector normalizes");
        assert!((vector[0] - 0.6).abs() < 1e-6);
        assert!((vector[1] - 0.8).abs() < 1e-6);
    }

    /// A stored NaN poisons every distance it ever participates in, silently
    /// and forever. It must never reach the index.
    #[test]
    fn a_degenerate_vector_is_a_typed_refusal_not_a_stored_nan() {
        assert!(normalize_l2(&mut [0.0f32, 0.0]).is_err());
        assert!(normalize_l2(&mut [f32::NAN, 1.0]).is_err());
        assert!(normalize_l2(&mut [f32::INFINITY, 1.0]).is_err());
        assert!(normalize_l2(&mut []).is_err());
    }
}
