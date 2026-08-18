//! The byte-transport seam every adapter speaks through. Adapters are pure
//! codecs: they build an [`HttpRequestPlan`] and parse response bytes; they
//! never open sockets. That split is what makes the m0-s10 conformance suite
//! runnable from recorded fixtures and the zero-network-I/O policy test
//! provable — a dispatch the policy refuses never reaches a transport at all.
//!
//! ## Two transports, and why the choice is a value
//!
//! [`LoopbackHttpTransport`] is loopback-only *by construction*: it speaks
//! `http` and refuses any host that does not resolve to a loopback address,
//! before connecting. [`TlsHttpTransport`] is its mirror image: it speaks
//! `https` only, so no credential ever crosses the wire in cleartext.
//!
//! Until m1-s03 the loopback transport was the *only* one, which made
//! "`local_only` cannot egress" a property of the build. [ADR-0006] adds the
//! TLS transport and states the weakening plainly: the guarantee becomes
//!
//! > under `local_only`, dispatch **selects** a transport that is structurally
//! > incapable of reaching a non-loopback host.
//!
//! [`TransportSelection`] is what makes that sentence checkable. The gateway
//! holds a [`Transports`] set rather than one transport, resolves the
//! selection from the endpoint's declared locality, and the m0-s10 policy
//! oracle asserts the *selection* — not merely the absence of an alternative.
//!
//! [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md

use crate::policy::EndpointLocality;
use crate::weather::Weather;
use std::fmt;
use std::io::{BufRead, BufReader, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::time::Duration;

/// Response headers + status line must fit here (32 KiB): a local model
/// server that sends more is misbehaving, and an unbounded header read is an
/// allocation attack surface (L8).
const RESPONSE_HEAD_BYTES_MAX: usize = 32 * 1024;
/// Streaming read buffer. 16 KiB keeps SSE deltas flowing without per-byte
/// syscalls and without buffering whole responses.
const STREAM_CHUNK_BYTES: usize = 16 * 1024;
/// A chunked-encoding size line is a hex number plus CRLF; 34 bytes allows
/// extensions we ignore while refusing pathological lines (L8).
const CHUNK_SIZE_LINE_BYTES_MAX: usize = 34;

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum HttpMethod {
    Get,
    Post,
}

impl HttpMethod {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Get => "GET",
            Self::Post => "POST",
        }
    }
}

/// One provider call, fully described before any I/O. Headers carry
/// credential material only inside header values (never in `url`), which is
/// what lets the conformance suite assert keys stay out of URLs and logs:
/// `Debug` for this type redacts every header value.
pub struct HttpRequestPlan {
    pub method: HttpMethod,
    pub url: String,
    pub headers: Vec<(&'static str, String)>,
    pub body: Vec<u8>,
    pub timeout_ms: u32,
    /// Response bytes this call may stream before the transport refuses.
    ///
    /// Per-call rather than a transport constant, because the two things that
    /// use this seam have budgets three orders of magnitude apart: a cloud
    /// completion is text, and a model artifact is hundreds of megabytes.
    /// A single constant sized for one silently breaks the other — which it
    /// did, until m1-s04: every artifact in `models/manifest.json` is larger
    /// than the 64 MiB completion cap, so no HTTPS pull could ever finish.
    ///
    /// `None` takes [`RESPONSE_BODY_BYTES_DEFAULT`]. A caller with a *declared*
    /// size (the model manifest states exact byte counts) sets it from that,
    /// so the cap is a reviewed number rather than a global guess (L8).
    pub response_bytes_max: Option<u64>,
}

impl fmt::Debug for HttpRequestPlan {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Header values may hold API keys; a Debug print must never be the
        // reason the m0-s10 secret scan fires.
        formatter
            .debug_struct("HttpRequestPlan")
            .field("method", &self.method.as_str())
            .field("url", &self.url)
            .field(
                "headers",
                &self
                    .headers
                    .iter()
                    .map(|(name, _)| *name)
                    .collect::<Vec<_>>(),
            )
            .field("body_len", &self.body.len())
            .field("timeout_ms", &self.timeout_ms)
            .field("response_bytes_max", &self.response_bytes_max)
            .finish()
    }
}

/// The response head: status plus lower-cased header names with verbatim
/// values. Delivered to the handler *before* the first body chunk, so an
/// adapter can choose between live SSE parsing and bounded error collection.
#[derive(Clone, Debug)]
pub struct HttpHead {
    pub status: u16,
    pub headers: Vec<(String, String)>,
}

impl HttpHead {
    #[must_use]
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(header, _)| header == name)
            .map(|(_, value)| value.as_str())
    }
}

/// Returned by a handler to stop the stream early (client-side cancel).
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct StreamAbort;

/// Receives one response: head first, then body chunks in arrival order.
pub trait ResponseHandler {
    /// # Errors
    ///
    /// [`StreamAbort`] stops the transfer before the body streams.
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort>;

    /// # Errors
    ///
    /// [`StreamAbort`] stops the transfer mid-body; it is a cancel signal,
    /// not a failure of the peer.
    fn on_chunk(&mut self, chunk: &[u8]) -> Result<(), StreamAbort>;
}

/// Typed transport failure below HTTP semantics. Never carries payload bytes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum TransportError {
    /// The plan named a host this transport refuses to reach. The loopback
    /// transport returns this for every non-loopback host — that refusal is
    /// a design property, not an error condition to work around.
    HostRefused {
        host: String,
        reason: &'static str,
    },
    /// The URL did not parse into scheme/host/port/path.
    UrlInvalid {
        url: String,
    },
    Connect {
        reason: String,
    },
    Timeout {
        timeout_ms: u32,
    },
    /// The peer's bytes violated HTTP framing (status line, headers,
    /// chunked encoding). Distinct from provider-payload parse failures,
    /// which are the adapter's [`crate::Weather::MalformedOutput`].
    Protocol {
        reason: String,
    },
    Io {
        reason: String,
    },
    /// The handler asked to stop; not a failure of the peer.
    Aborted,
}

impl fmt::Display for TransportError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::HostRefused { host, reason } => {
                write!(formatter, "refusing host {host:?}: {reason}")
            }
            Self::UrlInvalid { url } => write!(formatter, "URL did not parse: {url:?}"),
            Self::Connect { reason } => write!(formatter, "connect failed: {reason}"),
            Self::Timeout { timeout_ms } => {
                write!(formatter, "transport timed out after {timeout_ms} ms")
            }
            Self::Protocol { reason } => write!(formatter, "HTTP framing violation: {reason}"),
            Self::Io { reason } => write!(formatter, "transport I/O failed: {reason}"),
            Self::Aborted => formatter.write_str("handler aborted the stream"),
        }
    }
}

impl std::error::Error for TransportError {}

/// The seam. `execute` drives one request to completion, delivering the head
/// and then body bytes to the handler.
pub trait HttpTransport {
    /// # Errors
    ///
    /// Returns a typed [`TransportError`]; HTTP error *statuses* are not
    /// transport errors — they complete normally so the adapter can map them
    /// to weather from the head + body it received.
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError>;
}

/// Which transport a dispatch selected, as a value.
///
/// The gateway used to hold exactly one transport, so "`local_only` cannot
/// egress" was true because nothing else existed. [ADR-0006] adds a TLS
/// transport and replaces that with a typed selection; this enum is the thing
/// the policy oracle asserts, which is the compensating control the ADR makes
/// non-optional.
///
/// [ADR-0006]: ../../../../docs/adr/0006-transcription-and-tls-dependencies.md
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum TransportSelection {
    /// No transport at all — the model runs inside this process (local
    /// whisper). The strongest local-only statement available: there is no
    /// socket to refuse.
    InProcess,
    /// [`LoopbackHttpTransport`], which refuses every non-loopback host.
    DeviceLocal,
    /// [`TlsHttpTransport`]. Only a policy-authorized remote endpoint reaches
    /// it, and only over `https`.
    Remote,
}

impl TransportSelection {
    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InProcess => "in_process",
            Self::DeviceLocal => "device_local",
            Self::Remote => "remote",
        }
    }

    /// The selection an endpoint's declared locality implies. Total, and
    /// derived from the *declaration* rather than from the URL, so a config
    /// that lied about locality would have been refused at construction
    /// (`EndpointConfig::new`) rather than sniffed here.
    #[must_use]
    pub const fn for_locality(locality: EndpointLocality) -> Self {
        match locality {
            EndpointLocality::InProcess => Self::InProcess,
            EndpointLocality::DeviceLocal => Self::DeviceLocal,
            EndpointLocality::Remote => Self::Remote,
        }
    }
}

/// The transports one gateway may select between.
///
/// `remote` is an `Option` because a build or a deployment may compose no
/// cloud transport at all — and when it does not, a remote dispatch that got
/// past the policy gate refuses typed instead of silently doing nothing.
pub struct Transports<'runtime> {
    device_local: &'runtime dyn HttpTransport,
    remote: Option<&'runtime dyn HttpTransport>,
}

impl<'runtime> Transports<'runtime> {
    /// A gateway that can only reach this device.
    #[must_use]
    pub const fn device_local_only(device_local: &'runtime dyn HttpTransport) -> Self {
        Self {
            device_local,
            remote: None,
        }
    }

    #[must_use]
    pub const fn new(
        device_local: &'runtime dyn HttpTransport,
        remote: &'runtime dyn HttpTransport,
    ) -> Self {
        Self {
            device_local,
            remote: Some(remote),
        }
    }

    #[must_use]
    pub const fn has_remote(&self) -> bool {
        self.remote.is_some()
    }

    /// The transport for one selection. [`TransportSelection::InProcess`]
    /// resolves to `None` on purpose: an in-process model must not be handed
    /// a socket it could accidentally use.
    ///
    /// # Errors
    ///
    /// [`Weather::TransportUnavailable`] when a remote dispatch is authorized
    /// but this gateway composed no cloud transport.
    pub fn resolve(
        &self,
        selection: TransportSelection,
    ) -> Result<Option<&'runtime dyn HttpTransport>, Weather> {
        match selection {
            TransportSelection::InProcess => Ok(None),
            TransportSelection::DeviceLocal => Ok(Some(self.device_local)),
            TransportSelection::Remote => self.remote.map(Some).ok_or({
                Weather::TransportUnavailable {
                    selection: TransportSelection::Remote.as_str(),
                }
            }),
        }
    }
}

/// The scheme/host/port/path of a plan URL. Only `http` parses here: `https`
/// is [`TlsHttpTransport`]'s, and naming that split beats a confusing connect
/// error.
struct ParsedUrl {
    host: String,
    port: u16,
    path: String,
}

fn parse_http_url(url: &str) -> Result<ParsedUrl, TransportError> {
    let invalid = || TransportError::UrlInvalid {
        url: url.to_owned(),
    };
    if let Some(rest) = url.strip_prefix("https://") {
        let host = rest.split(['/', ':']).next().unwrap_or(rest);
        return Err(TransportError::HostRefused {
            host: host.to_owned(),
            reason: "https is the TLS transport's; this one reaches loopback only",
        });
    }
    let rest = url.strip_prefix("http://").ok_or_else(invalid)?;
    let (authority, path) = match rest.find('/') {
        Some(index) => (&rest[..index], &rest[index..]),
        None => (rest, "/"),
    };
    if authority.is_empty() {
        return Err(invalid());
    }
    let (host, port) = match authority.rsplit_once(':') {
        Some((host, port_text)) => (host, port_text.parse::<u16>().map_err(|_| invalid())?),
        None => (authority, 80),
    };
    Ok(ParsedUrl {
        host: host.to_owned(),
        port,
        path: path.to_owned(),
    })
}

/// The only socket-opening transport in core (see module doc). Loopback-only
/// by construction: `execute` resolves the host and refuses any address that
/// is not a loopback interface, before connecting.
#[derive(Clone, Copy, Debug, Default)]
pub struct LoopbackHttpTransport;

impl LoopbackHttpTransport {
    fn connect(host: &str, port: u16, timeout: Duration) -> Result<TcpStream, TransportError> {
        let addresses: Vec<SocketAddr> = (host, port)
            .to_socket_addrs()
            .map_err(|error| TransportError::Connect {
                reason: format!("resolve {host}:{port}: {error}"),
            })?
            .collect();
        let loopback: Vec<SocketAddr> = addresses
            .into_iter()
            .filter(|address| match address.ip() {
                IpAddr::V4(v4) => v4.is_loopback(),
                IpAddr::V6(v6) => v6.is_loopback(),
            })
            .collect();
        let Some(address) = loopback.first() else {
            return Err(TransportError::HostRefused {
                host: host.to_owned(),
                reason: "resolves to no loopback address; this transport cannot reach it",
            });
        };
        TcpStream::connect_timeout(address, timeout).map_err(|error| TransportError::Connect {
            reason: format!("connect {address}: {error}"),
        })
    }
}

impl HttpTransport for LoopbackHttpTransport {
    fn execute(
        &self,
        plan: &HttpRequestPlan,
        handler: &mut dyn ResponseHandler,
    ) -> Result<(), TransportError> {
        let url = parse_http_url(&plan.url)?;
        let timeout = Duration::from_millis(u64::from(plan.timeout_ms));
        let stream = Self::connect(&url.host, url.port, timeout)?;
        stream
            .set_read_timeout(Some(timeout))
            .and_then(|()| stream.set_write_timeout(Some(timeout)))
            .map_err(|error| TransportError::Io {
                reason: format!("set socket timeouts: {error}"),
            })?;
        write_request(&stream, plan, &url)?;
        read_response(stream, plan.timeout_ms, handler)
    }
}

fn write_request(
    mut stream: &TcpStream,
    plan: &HttpRequestPlan,
    url: &ParsedUrl,
) -> Result<(), TransportError> {
    let mut head = format!(
        "{} {} HTTP/1.1\r\nhost: {}:{}\r\nconnection: close\r\ncontent-length: {}\r\n",
        plan.method.as_str(),
        url.path,
        url.host,
        url.port,
        plan.body.len()
    );
    for (name, value) in &plan.headers {
        head.push_str(name);
        head.push_str(": ");
        head.push_str(value);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    stream
        .write_all(head.as_bytes())
        .and_then(|()| stream.write_all(&plan.body))
        .map_err(|error| map_io(&error, "write request", plan.timeout_ms))
}

/// Reads status line + headers, hands the head to the handler, then streams
/// the body per its framing (content-length, chunked, or read-to-close).
fn read_response(
    stream: TcpStream,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    let mut reader = BufReader::new(stream);
    let status = read_status_line(&mut reader, timeout_ms)?;
    let mut headers = Vec::new();
    let mut head_bytes = 0_usize;
    loop {
        let line = read_crlf_line(&mut reader, timeout_ms)?;
        head_bytes = head_bytes.saturating_add(line.len());
        if head_bytes > RESPONSE_HEAD_BYTES_MAX {
            return Err(TransportError::Protocol {
                reason: format!("response head exceeds {RESPONSE_HEAD_BYTES_MAX} bytes"),
            });
        }
        if line.is_empty() {
            break;
        }
        if let Some((name, value)) = line.split_once(':') {
            headers.push((name.trim().to_ascii_lowercase(), value.trim().to_owned()));
        }
    }
    let head = HttpHead { status, headers };
    if handler.on_head(&head).is_err() {
        return Err(TransportError::Aborted);
    }
    stream_body(&head, &mut reader, timeout_ms, handler)
}

fn read_status_line(
    reader: &mut BufReader<TcpStream>,
    timeout_ms: u32,
) -> Result<u16, TransportError> {
    let line = read_crlf_line(reader, timeout_ms)?;
    // "HTTP/1.1 200 OK" — the status is the second whitespace token.
    let status_text = line
        .split(' ')
        .nth(1)
        .ok_or_else(|| TransportError::Protocol {
            reason: format!("status line did not parse: {line:?}"),
        })?;
    status_text
        .parse::<u16>()
        .map_err(|_| TransportError::Protocol {
            reason: format!("status code did not parse: {status_text:?}"),
        })
}

fn read_crlf_line(
    reader: &mut BufReader<TcpStream>,
    timeout_ms: u32,
) -> Result<String, TransportError> {
    let mut line = String::new();
    let read = reader
        .read_line(&mut line)
        .map_err(|error| map_io(&error, "read response", timeout_ms))?;
    if read == 0 {
        return Err(TransportError::Protocol {
            reason: "connection closed before the response head completed".to_owned(),
        });
    }
    if line.len() > RESPONSE_HEAD_BYTES_MAX {
        return Err(TransportError::Protocol {
            reason: format!("head line exceeds {RESPONSE_HEAD_BYTES_MAX} bytes"),
        });
    }
    while line.ends_with('\n') || line.ends_with('\r') {
        line.pop();
    }
    Ok(line)
}

fn map_io(error: &std::io::Error, context: &'static str, timeout_ms: u32) -> TransportError {
    if error.kind() == std::io::ErrorKind::WouldBlock
        || error.kind() == std::io::ErrorKind::TimedOut
    {
        return TransportError::Timeout { timeout_ms };
    }
    TransportError::Io {
        reason: format!("{context}: {error}"),
    }
}

fn stream_body(
    head: &HttpHead,
    reader: &mut BufReader<TcpStream>,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    if head
        .header("transfer-encoding")
        .is_some_and(|value| value.eq_ignore_ascii_case("chunked"))
    {
        return stream_chunked(reader, timeout_ms, handler);
    }
    if let Some(length_text) = head.header("content-length") {
        let length: u64 = length_text.parse().map_err(|_| TransportError::Protocol {
            reason: format!("content-length did not parse: {length_text:?}"),
        })?;
        return stream_exact(reader, length, timeout_ms, handler);
    }
    // No framing header: `connection: close` semantics — read until EOF.
    stream_to_close(reader, timeout_ms, handler)
}

fn stream_exact(
    reader: &mut BufReader<TcpStream>,
    mut remaining: u64,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    let mut buffer = [0_u8; STREAM_CHUNK_BYTES];
    while remaining > 0 {
        let want = usize::try_from(remaining.min(buffer.len() as u64)).unwrap_or(buffer.len());
        let read = reader
            .read(&mut buffer[..want])
            .map_err(|error| map_io(&error, "read body", timeout_ms))?;
        if read == 0 {
            return Err(TransportError::Protocol {
                reason: format!("body ended {remaining} bytes early"),
            });
        }
        remaining -= read as u64;
        if handler.on_chunk(&buffer[..read]).is_err() {
            return Err(TransportError::Aborted);
        }
    }
    Ok(())
}

fn stream_to_close(
    reader: &mut BufReader<TcpStream>,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    let mut buffer = [0_u8; STREAM_CHUNK_BYTES];
    loop {
        let read = reader
            .read(&mut buffer)
            .map_err(|error| map_io(&error, "read body", timeout_ms))?;
        if read == 0 {
            return Ok(());
        }
        if handler.on_chunk(&buffer[..read]).is_err() {
            return Err(TransportError::Aborted);
        }
    }
}

/// RFC 9112 chunked decoding, iterative, with a stated cap on the size line.
fn stream_chunked(
    reader: &mut BufReader<TcpStream>,
    timeout_ms: u32,
    handler: &mut dyn ResponseHandler,
) -> Result<(), TransportError> {
    loop {
        let size_line = read_crlf_line(reader, timeout_ms)?;
        if size_line.len() > CHUNK_SIZE_LINE_BYTES_MAX {
            return Err(TransportError::Protocol {
                reason: "chunk size line exceeds the cap".to_owned(),
            });
        }
        let size_text = size_line.split(';').next().unwrap_or("").trim();
        let size = u64::from_str_radix(size_text, 16).map_err(|_| TransportError::Protocol {
            reason: format!("chunk size did not parse: {size_text:?}"),
        })?;
        if size == 0 {
            // Trailer section: consume through the final blank line.
            loop {
                if read_crlf_line(reader, timeout_ms)?.is_empty() {
                    return Ok(());
                }
            }
        }
        stream_exact(reader, size, timeout_ms, handler)?;
        let terminator = read_crlf_line(reader, timeout_ms)?;
        if !terminator.is_empty() {
            return Err(TransportError::Protocol {
                reason: "chunk data not followed by CRLF".to_owned(),
            });
        }
    }
}

/// Buffers one whole response — the shape simple GET callers (model
/// discovery, model downloads use their own sink) and tests want.
#[derive(Default)]
pub struct BufferedResponse {
    pub head: Option<HttpHead>,
    pub body: Vec<u8>,
}

impl ResponseHandler for BufferedResponse {
    fn on_head(&mut self, head: &HttpHead) -> Result<(), StreamAbort> {
        self.head = Some(head.clone());
        Ok(())
    }

    fn on_chunk(&mut self, chunk: &[u8]) -> Result<(), StreamAbort> {
        self.body.extend_from_slice(chunk);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{
        BufferedResponse, HttpMethod, HttpRequestPlan, HttpTransport, LoopbackHttpTransport,
        TransportError, parse_http_url,
    };
    use std::io::{Read, Write};
    use std::net::TcpListener;

    fn plan(url: &str) -> HttpRequestPlan {
        HttpRequestPlan {
            method: HttpMethod::Post,
            url: url.to_owned(),
            headers: vec![("authorization", "Bearer sk-super-secret".to_owned())],
            body: b"{}".to_vec(),
            timeout_ms: 2_000,
            response_bytes_max: None,
        }
    }

    #[test]
    fn https_and_non_loopback_hosts_are_refused_with_typed_errors() {
        let mut buffered = BufferedResponse::default();
        let refused = LoopbackHttpTransport
            .execute(
                &plan("https://api.anthropic.com/v1/messages"),
                &mut buffered,
            )
            .expect_err("https must be refused by the loopback transport");
        assert!(matches!(refused, TransportError::HostRefused { .. }));

        let refused = LoopbackHttpTransport
            .execute(&plan("http://93.184.216.34/v1"), &mut buffered)
            .expect_err("a public address must be refused");
        assert!(matches!(refused, TransportError::HostRefused { .. }));
    }

    #[test]
    fn plan_debug_never_prints_header_values() {
        let rendered = format!("{:?}", plan("http://127.0.0.1:1/x"));
        assert!(!rendered.contains("sk-super-secret"));
        assert!(rendered.contains("authorization"));
    }

    #[test]
    fn url_parsing_extracts_authority_and_defaults() {
        let parsed = parse_http_url("http://localhost:11434/v1/models").expect("parses");
        assert_eq!(parsed.host, "localhost");
        assert_eq!(parsed.port, 11434);
        assert_eq!(parsed.path, "/v1/models");
        let parsed = parse_http_url("http://127.0.0.1").expect("parses");
        assert_eq!(parsed.port, 80);
        assert_eq!(parsed.path, "/");
        assert!(parse_http_url("ftp://x").is_err());
    }

    /// A real socket round-trip against an in-test loopback server covering
    /// both framings the transport must decode.
    #[test]
    fn loopback_round_trip_decodes_content_length_and_chunked_bodies() {
        let listener = TcpListener::bind("127.0.0.1:0").expect("bind test listener");
        let address = listener.local_addr().expect("listener address");
        let server = std::thread::spawn(move || {
            for (index, connection) in listener.incoming().take(2).enumerate() {
                let mut connection = connection.expect("accept");
                let mut request = [0_u8; 1024];
                let _ = connection.read(&mut request).expect("read request");
                let response: &[u8] = if index == 0 {
                    b"HTTP/1.1 200 OK\r\ncontent-length: 5\r\n\r\nhello"
                } else {
                    b"HTTP/1.1 429 Too Many\r\nretry-after: 7\r\ntransfer-encoding: chunked\r\n\r\n3\r\nabc\r\n2\r\nde\r\n0\r\n\r\n"
                };
                connection.write_all(response).expect("write response");
            }
        });

        let url = format!("http://127.0.0.1:{}/echo", address.port());
        let mut buffered = BufferedResponse::default();
        LoopbackHttpTransport
            .execute(&plan(&url), &mut buffered)
            .expect("content-length response decodes");
        let head = buffered.head.expect("head delivered before body");
        assert_eq!(head.status, 200);
        assert_eq!(buffered.body, b"hello");

        let mut buffered = BufferedResponse::default();
        LoopbackHttpTransport
            .execute(&plan(&url), &mut buffered)
            .expect("chunked response decodes");
        let head = buffered.head.expect("head delivered before body");
        assert_eq!(head.status, 429);
        assert_eq!(buffered.body, b"abcde");
        assert_eq!(
            head.header("retry-after"),
            Some("7"),
            "headers must be readable for rate-limit hints"
        );
        server.join().expect("server thread");
    }
}
