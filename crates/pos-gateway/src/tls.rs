//! The reviewed TLS transport (m1-s03, [ADR-0006] §1): the one module in core
//! that can reach a host off this device.
//!
//! ## What this module is, and what it is not
//!
//! It is a second implementation of [`HttpTransport`], nothing more. Adapters
//! do not know it exists; they build an [`HttpRequestPlan`] and parse bytes
//! exactly as they do against the loopback transport, and the gateway decides
//! which of the two a dispatch gets from the endpoint's declared locality.
//! That is the whole eject path: swapping `ureq` for anything else is this
//! file, and nothing above it changes.
//!
//! It is **not** a general HTTP client for the rest of the codebase. Two rules
//! keep it narrow:
//!
//! 1. **`https` only.** A plan naming `http://` is refused with a typed
//!    [`TransportError::HostRefused`] before any connection. Every plan that
//!    reaches here may carry an API key in a header value, and cleartext is
//!    not a thing we let a caller opt into by typo.
//! 2. **HTTP statuses are not transport errors.** `ureq` is configured with
//!    `http_status_as_error(false)` so a 429 or a 500 completes normally and
//!    the adapter maps it to [`crate::Weather`] from the head and body it
//!    received — the same contract `LoopbackHttpTransport` honours.
//!
//! ## Why `rustls` + `ureq`
//!
//! `HttpTransport::execute` is blocking, so the client must be too: a
//! `reqwest`-shaped choice would put an async runtime into `pos` and
//! `apps/desktop` to make one outbound call. The full comparison, and the
//! consequence for the `local_only` guarantee, are in [ADR-0006].
//!
//! [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md

use crate::transport::{
    HttpHead, HttpMethod, HttpRequestPlan, HttpTransport, ResponseHandler, TransportError,
};
use std::fmt;
use std::time::Duration;

/// Read granularity while streaming a response body. Matches the loopback
/// transport's, so SSE deltas flow at the same rate on both paths and neither
/// buffers a whole response (L8).
const STREAM_CHUNK_BYTES: usize = 16 * 1024;

/// Response bytes one call may stream before the transport refuses. Cloud
/// completions are text; 64 MiB is far past any legitimate answer and far
/// below anything that could exhaust a laptop. A stated cap beats discovering
/// the real one under memory pressure (L8).
const RESPONSE_BODY_BYTES_MAX: u64 = 64 * 1024 * 1024;

/// The cloud-capable transport. Construct one per runtime and share it: the
/// agent owns a connection pool, and building one per call would pay a TLS
/// handshake for every token stream.
pub struct TlsHttpTransport {
    agent: ureq::Agent,
}

impl fmt::Debug for TlsHttpTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("TlsHttpTransport")
    }
}

impl Default for TlsHttpTransport {
    fn default() -> Self {
        Self::new()
    }
}

impl TlsHttpTransport {
    /// # Panics
    ///
    /// Never. The configuration is a literal; `ureq`'s builder validates
    /// nothing that a constant can get wrong.
    #[must_use]
    pub fn new() -> Self {
        let config = ureq::Agent::config_builder()
            // Statuses are the adapter's to interpret (module doc rule 2).
            .http_status_as_error(false)
            // Redirects would silently move a request carrying an API key to
            // a host the policy layer never authorized. A provider that wants
            // us elsewhere says so in its base URL.
            .max_redirects(0)
            .build();
        Self {
            agent: config.new_agent(),
        }
    }
}

impl HttpTransport for TlsHttpTransport {
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        let host = require_https(&plan.url)?;
        let timeout = Duration::from_millis(u64::from(plan.timeout_ms));
        // `ureq` types the body-carrying and body-less builders apart, so the
        // two arms cannot be one expression. The shared half is `with_headers`.
        let response = match plan.method {
            HttpMethod::Get => with_headers(
                self.agent
                    .get(&plan.url)
                    .config()
                    .timeout_global(Some(timeout))
                    .build(),
                plan,
            )
            .call(),
            HttpMethod::Post => with_headers(
                self.agent
                    .post(&plan.url)
                    .config()
                    .timeout_global(Some(timeout))
                    .build(),
                plan,
            )
            .send(&plan.body[..]),
        }
        .map_err(|error| map_send_error(&error, &host, plan.timeout_ms))?;
        let head = HttpHead {
            status: response.status().as_u16(),
            headers: response
                .headers()
                .iter()
                .filter_map(|(name, value)| {
                    value
                        .to_str()
                        .ok()
                        .map(|value| (name.as_str().to_ascii_lowercase(), value.to_owned()))
                })
                .collect(),
        };
        if handler.on_head(&head).is_err() {
            return Err(TransportError::Aborted);
        }
        stream_body(response, plan.timeout_ms, handler)
    }
}

/// Copies the plan's headers onto either builder typestate.
fn with_headers<S>(
    mut request: ureq::RequestBuilder<S>,
    plan: &HttpRequestPlan,
) -> ureq::RequestBuilder<S> {
    for (name, value) in &plan.headers {
        request = request.header(*name, value);
    }
    request
}

/// Rule 1 of the module doc, as code: the host, or a typed refusal.
fn require_https(url: &str) -> Result<String, TransportError> {
    let Some(rest) = url.strip_prefix("https://") else {
        if let Some(rest) = url.strip_prefix("http://") {
            let host = rest.split(['/', ':']).next().unwrap_or(rest);
            return Err(TransportError::HostRefused {
                host: host.to_owned(),
                reason: "the TLS transport speaks https only; a cleartext plan may carry a key",
            });
        }
        return Err(TransportError::UrlInvalid {
            url: url.to_owned(),
        });
    };
    let authority = rest.split('/').next().unwrap_or(rest);
    let host = authority
        .rsplit_once(':')
        .map_or(authority, |(host, _)| host);
    if host.is_empty() {
        return Err(TransportError::UrlInvalid {
            url: url.to_owned(),
        });
    }
    Ok(host.to_owned())
}

/// Streams the body to the handler in bounded windows, refusing past the
/// stated cap rather than growing to fit whatever the peer sends.
fn stream_body(
    mut response: ureq::http::Response<ureq::Body>,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    use std::io::Read;
    let mut reader = response.body_mut().as_reader();
    let mut buffer = [0_u8; STREAM_CHUNK_BYTES];
    let mut streamed = 0_u64;
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| map_io(&error, timeout_ms))?;
        if read == 0 {
            return Ok(());
        }
        streamed = streamed.saturating_add(read as u64);
        if streamed > RESPONSE_BODY_BYTES_MAX {
            return Err(TransportError::Protocol {
                reason: format!("response body exceeds {RESPONSE_BODY_BYTES_MAX} bytes"),
            });
        }
        if handler.on_chunk(&buffer[..read]).is_err() {
            return Err(TransportError::Aborted);
        }
    }
}

/// Maps a send failure onto the seam's own vocabulary. The message is the
/// crate's `Display`, which never carries request headers — the plan's own
/// `Debug` redacts them, and this path never formats the plan at all.
fn map_send_error(error: &ureq::Error, host: &str, timeout_ms: u32) -> TransportError {
    match error {
        ureq::Error::Timeout(_) => TransportError::Timeout { timeout_ms },
        ureq::Error::HostNotFound => TransportError::HostRefused {
            host: host.to_owned(),
            reason: "the host did not resolve",
        },
        ureq::Error::Io(io) => map_io(io, timeout_ms),
        ureq::Error::Tls(_) => TransportError::Connect {
            reason: format!("TLS handshake with {host} failed: {error}"),
        },
        ureq::Error::TooManyRedirects => TransportError::Protocol {
            reason: "the peer redirected; this transport follows none (module doc)".to_owned(),
        },
        _ => TransportError::Connect {
            reason: format!("{host}: {error}"),
        },
    }
}

fn map_io(error: &std::io::Error, timeout_ms: u32) -> TransportError {
    if matches!(
        error.kind(),
        std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
    ) {
        return TransportError::Timeout { timeout_ms };
    }
    TransportError::Io {
        reason: format!("read response body: {error}"),
    }
}

#[cfg(test)]
mod tests {
    use super::{RESPONSE_BODY_BYTES_MAX, TlsHttpTransport, require_https};
    use crate::transport::{
        BufferedResponse, HttpMethod, HttpRequestPlan, HttpTransport, TransportError,
    };

    fn plan(url: &str) -> HttpRequestPlan {
        HttpRequestPlan {
            method: HttpMethod::Post,
            url: url.to_owned(),
            headers: vec![(
                "authorization",
                "Bearer sk-must-not-go-cleartext".to_owned(),
            )],
            body: b"{}".to_vec(),
            timeout_ms: 2_000,
        }
    }

    #[test]
    fn cleartext_and_unparseable_plans_are_refused_before_any_connection() {
        let transport = TlsHttpTransport::new();
        let mut buffered = BufferedResponse::default();
        let refused = transport
            .execute(&plan("http://api.example.com/v1"), &mut buffered)
            .expect_err("http must be refused: the plan can carry a key");
        assert!(
            matches!(&refused, TransportError::HostRefused { host, .. } if host == "api.example.com"),
            "got {refused:?}"
        );
        let refused = transport
            .execute(&plan("ftp://api.example.com/v1"), &mut buffered)
            .expect_err("a non-HTTP scheme does not parse");
        assert!(matches!(refused, TransportError::UrlInvalid { .. }));
        assert!(buffered.head.is_none(), "no response was ever received");
    }

    #[test]
    fn the_host_is_extracted_without_the_port_or_path() {
        assert_eq!(
            require_https("https://api.anthropic.com/v1/messages").expect("parses"),
            "api.anthropic.com"
        );
        assert_eq!(
            require_https("https://localhost:8443/v1").expect("parses"),
            "localhost"
        );
        assert!(require_https("https://").is_err());
    }

    #[test]
    fn the_body_cap_is_stated_rather_than_discovered() {
        // The bound exists so an oversized response is a typed refusal, not a
        // memory event. Asserting it here keeps a future edit from quietly
        // removing the only limit on peer-controlled bytes (L8).
        assert_eq!(RESPONSE_BODY_BYTES_MAX, 64 * 1024 * 1024);
    }
}
