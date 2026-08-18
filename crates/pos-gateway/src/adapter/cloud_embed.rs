//! The API embedding adapter (m1-s04): OpenAI-shaped `/v1/embeddings`, which
//! OpenAI, Voyage, Together, and every OpenAI-compatible gateway speak.
//!
//! It is a peer of the local ONNX adapter, not a fallback beneath it: a
//! server deployment with no model artifact routes here, and a `local_only`
//! project never can — the policy gate refuses the remote endpoint before this
//! file is reached (F43).
//!
//! ## The dimension is declared, not discovered
//!
//! An index is only coherent if every vector in it is the same width from the
//! same model. Reading the width off the first response would mean a provider
//! silently changing its default model corrupts an index that still passes
//! every shape check. So the width is configuration, and a response of any
//! other width is [`Weather::MalformedOutput`].
//!
//! ## Token counts come from the provider when it reports them
//!
//! Cost attribution keys off `usage.prompt_tokens`. When an endpoint omits it
//! the adapter estimates from characters and marks the usage `measured:
//! false`, which is the `usage-or-explicit-estimate` conformance rule this
//! crate already holds every family to — an estimate must say it is one.

use super::{BoundedBody, estimate_tokens, weather_from_status, weather_from_transport};
use crate::credentials::CallAuth;
use crate::embed::{EmbedBatch, EmbedRequest, EmbedUsage, Embedder};
use crate::transport::{HttpMethod, HttpRequestPlan, HttpTransport};
use crate::weather::Weather;

/// Per-call deadline. An embedding batch is small and fast everywhere; a
/// minute is generous and, unlike no timeout, is a number (L8).
const EMBED_TIMEOUT_MS: u32 = 60_000;

/// The embedding endpoint, its model, and its declared width.
#[derive(Clone, Debug)]
pub struct CloudEmbedAdapter {
    pub base_url: String,
    pub model: String,
    /// Declared rather than discovered — see the module doc.
    pub dim: u16,
}

impl CloudEmbedAdapter {
    fn url(&self) -> String {
        format!("{}/v1/embeddings", self.base_url.trim_end_matches('/'))
    }
}

impl Embedder for CloudEmbedAdapter {
    fn label(&self) -> &'static str {
        "cloud-embed"
    }

    fn model_name(&self) -> &str {
        &self.model
    }

    fn dim(&self) -> u16 {
        self.dim
    }

    fn embed(
        &self,
        auth: &CallAuth,
        request: &EmbedRequest<'_>,
        transport: Option<&dyn HttpTransport>,
    ) -> Result<(EmbedBatch, EmbedUsage), Weather> {
        let Some(transport) = transport else {
            return Err(Weather::TransportUnavailable {
                selection: "remote",
            });
        };
        if request.inputs.is_empty() {
            return Err(Weather::InvalidRequest {
                reason: "an embedding batch with no inputs".to_owned(),
            });
        }
        let mut headers = vec![("content-type", "application/json".to_owned())];
        if let CallAuth::ApiKey(key) = auth {
            headers.push(("authorization", format!("Bearer {}", key.expose())));
        }
        let (body, character_count) = request_body(&self.model, request);
        let plan = HttpRequestPlan {
            method: HttpMethod::Post,
            url: self.url(),
            headers,
            body,
            timeout_ms: EMBED_TIMEOUT_MS,
            // A provider answer is text; the transport default applies.
            response_bytes_max: None,
        };
        let mut collector = BoundedBody::default();
        transport
            .execute(&plan, &mut collector)
            .map_err(|error| weather_from_transport(error, EMBED_TIMEOUT_MS))?;
        let head = collector.head().ok_or_else(|| Weather::MalformedOutput {
            reason: "the endpoint returned no response head".to_owned(),
        })?;
        if collector.overflowed() {
            return Err(Weather::MalformedOutput {
                reason: "the embedding response exceeded the bounded body budget".to_owned(),
            });
        }
        if head.status >= 300 {
            return Err(weather_from_status(&head, collector.body()));
        }
        parse_embeddings(
            collector.body(),
            self.dim,
            request.inputs.len(),
            character_count,
        )
    }
}

/// The request body, plus the character count the token estimate falls back
/// to when a provider reports no usage.
fn request_body(model: &str, request: &EmbedRequest<'_>) -> (Vec<u8>, u64) {
    let mut character_count = 0u64;
    let inputs: Vec<String> = request
        .inputs
        .iter()
        .map(|input| {
            // The versioned input seam, joined exactly as the local adapter
            // joins it — two backends that framed the prefix differently
            // would produce two incomparable indexes from one config.
            let text = match input.context_prefix {
                Some(prefix) => format!("{prefix}\n\n{}", input.content),
                None => input.content.to_owned(),
            };
            character_count += text.chars().count() as u64;
            text
        })
        .collect();
    let body = serde_json::json!({ "model": model, "input": inputs });
    (body.to_string().into_bytes(), character_count)
}

/// Decodes `{"data":[{"index":n,"embedding":[…]}],"usage":{…}}`.
///
/// The `index` field is honoured rather than assumed: the wire contract
/// permits any order, and a provider that returns them shuffled would
/// otherwise attach every vector to the wrong chunk — undetectably, because
/// the shapes all still line up.
fn parse_embeddings(
    body: &[u8],
    dim: u16,
    count: usize,
    character_count: u64,
) -> Result<(EmbedBatch, EmbedUsage), Weather> {
    let parsed: serde_json::Value =
        serde_json::from_slice(body).map_err(|error| Weather::MalformedOutput {
            reason: format!("the embedding response was not JSON: {error}"),
        })?;
    let rows = parsed
        .get("data")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| Weather::MalformedOutput {
            reason: "the embedding response carried no `data` array".to_owned(),
        })?;
    if rows.len() != count {
        return Err(Weather::MalformedOutput {
            reason: format!("{} vectors for {count} inputs", rows.len()),
        });
    }
    let width = usize::from(dim);
    let mut vectors = vec![0.0f32; count * width];
    let mut seen = vec![false; count];
    for row in rows {
        let index = row
            .get("index")
            .and_then(serde_json::Value::as_u64)
            .and_then(|value| usize::try_from(value).ok())
            .filter(|value| *value < count)
            .ok_or_else(|| Weather::MalformedOutput {
                reason: "an embedding row carried no usable `index`".to_owned(),
            })?;
        let values = row
            .get("embedding")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| Weather::MalformedOutput {
                reason: format!("embedding row {index} carried no `embedding` array"),
            })?;
        if values.len() != width {
            return Err(Weather::MalformedOutput {
                reason: format!(
                    "embedding row {index} is {} wide, not the declared {dim}",
                    values.len()
                ),
            });
        }
        if std::mem::replace(&mut seen[index], true) {
            return Err(Weather::MalformedOutput {
                reason: format!("the endpoint returned index {index} twice"),
            });
        }
        for (slot, value) in vectors[index * width..(index + 1) * width]
            .iter_mut()
            .zip(values)
        {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "JSON numbers are f64; vectors are stored as f32 by design"
            )]
            let component = value
                .as_f64()
                .filter(|number| number.is_finite())
                .ok_or_else(|| Weather::MalformedOutput {
                    reason: format!("embedding row {index} carried a non-finite component"),
                })? as f32;
            *slot = component;
        }
    }
    let reported = parsed
        .get("usage")
        .and_then(|usage| usage.get("prompt_tokens"))
        .and_then(serde_json::Value::as_u64);
    let usage = EmbedUsage {
        tokens_in: reported.unwrap_or_else(|| estimate_tokens(character_count)),
        // A remote model's padding is its own business and it does not consume
        // our arena, so the honest answer for the memory meter is the tokens
        // we sent — not a number we would be inventing.
        padded_tokens: reported.unwrap_or_else(|| estimate_tokens(character_count)),
        vector_count: count as u64,
        truncated_count: 0,
        measured: reported.is_some(),
    };
    Ok((EmbedBatch::new(dim, vectors, count)?, usage))
}

#[cfg(test)]
mod tests {
    use super::{CloudEmbedAdapter, parse_embeddings, request_body};
    use crate::credentials::CallAuth;
    use crate::embed::{EmbedInput, EmbedRequest, Embedder};
    use crate::weather::Weather;

    fn adapter() -> CloudEmbedAdapter {
        CloudEmbedAdapter {
            base_url: "https://api.openai.com/".to_owned(),
            model: "text-embedding-3-small".to_owned(),
            dim: 4,
        }
    }

    #[test]
    fn the_endpoint_url_is_the_base_plus_the_openai_shaped_path() {
        assert_eq!(adapter().url(), "https://api.openai.com/v1/embeddings");
    }

    #[test]
    fn without_a_transport_the_cloud_adapter_refuses_instead_of_pretending() {
        let inputs = [EmbedInput::plain("hello")];
        let refused = adapter()
            .embed(
                &CallAuth::None,
                &EmbedRequest {
                    model: "text-embedding-3-small",
                    inputs: &inputs,
                    enrichment_version: 0,
                },
                None,
            )
            .expect_err("a cloud adapter with no transport cannot embed");
        assert!(matches!(refused, Weather::TransportUnavailable { .. }));
    }

    /// The wire contract permits any order. A provider that shuffled would
    /// otherwise attach every vector to the wrong chunk — undetectably.
    #[test]
    fn rows_are_placed_by_their_stated_index_not_their_arrival_order() {
        let body = br#"{"data":[
            {"index":1,"embedding":[5.0,6.0,7.0,8.0]},
            {"index":0,"embedding":[1.0,2.0,3.0,4.0]}
        ],"usage":{"prompt_tokens":9}}"#;
        let (batch, usage) = parse_embeddings(body, 4, 2, 0).expect("two rows parse");
        assert_eq!(batch.vector(0), Some(&[1.0, 2.0, 3.0, 4.0][..]));
        assert_eq!(batch.vector(1), Some(&[5.0, 6.0, 7.0, 8.0][..]));
        assert_eq!(usage.tokens_in, 9);
        assert!(usage.measured, "the provider reported its own count");
    }

    #[test]
    fn an_estimate_says_it_is_one() {
        let body = br#"{"data":[{"index":0,"embedding":[1.0,2.0,3.0,4.0]}]}"#;
        let (_batch, usage) = parse_embeddings(body, 4, 1, 40).expect("one row parses");
        assert!(!usage.measured, "an unreported count is an estimate");
        assert!(usage.tokens_in > 0);
    }

    #[test]
    fn a_wrong_width_or_count_or_duplicate_is_malformed_rather_than_stored() {
        let narrow = br#"{"data":[{"index":0,"embedding":[1.0,2.0]}]}"#;
        assert!(parse_embeddings(narrow, 4, 1, 0).is_err(), "wrong width");
        let short = br#"{"data":[]}"#;
        assert!(parse_embeddings(short, 4, 1, 0).is_err(), "wrong count");
        let duplicate = br#"{"data":[
            {"index":0,"embedding":[1.0,2.0,3.0,4.0]},
            {"index":0,"embedding":[1.0,2.0,3.0,4.0]}
        ]}"#;
        assert!(
            parse_embeddings(duplicate, 4, 2, 0).is_err(),
            "duplicate index"
        );
        let infinite = br#"{"data":[{"index":0,"embedding":[1.0,2.0,3.0,null]}]}"#;
        assert!(parse_embeddings(infinite, 4, 1, 0).is_err(), "non-finite");
    }

    /// Two backends that framed the prefix differently would produce two
    /// incomparable indexes from one configuration.
    #[test]
    fn the_context_prefix_is_joined_the_same_way_the_local_adapter_joins_it() {
        let inputs = [EmbedInput {
            context_prefix: Some("about widgets"),
            content: "the body",
        }];
        let (body, characters) = request_body(
            "m",
            &EmbedRequest {
                model: "m",
                inputs: &inputs,
                enrichment_version: 1,
            },
        );
        let text = String::from_utf8(body).expect("json is utf-8");
        assert!(text.contains("about widgets\\n\\nthe body"), "{text}");
        assert_eq!(
            characters,
            "about widgets\n\nthe body".chars().count() as u64
        );
    }
}
