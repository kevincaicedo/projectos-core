//! The axum HTTP+SSE transport (m0-s06, `http` feature).
//!
//! Thin dispatch, zero logic (L12): every handler resolves `(name, input)`
//! through the same [`LocalRuntime`] registry the Tauri IPC transport calls
//! and forwards the resulting bytes unchanged. The only transport-owned
//! decisions are the HTTP status (a pure function of the envelope code) and
//! the SSE content type — both contract-tested against the IPC transport in
//! `bins/pos-server/tests/http_contract.rs`.

use crate::{
    ApiError, LocalRuntime, SSE_RETRY_MS, STREAM_RESUME_WINDOW_LEN, stream::parse_resume_cursor,
};
use axum::Router;
use axum::body::{Body, Bytes};
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use serde::Deserialize;
use std::convert::Infallible;
use std::future::Future;
use std::sync::Arc;
use tokio_stream::wrappers::ReceiverStream;

/// Command/query inputs are small typed JSON documents (paths, counts, ids).
/// Blob and export payloads never travel this transport — they move through
/// the store — so anything above this cap is a defect or an attack (L8).
pub const API_HTTP_BODY_BYTES_MAX: usize = 1024 * 1024;

/// Bytes one upload request may carry. The same bound intake states for a
/// file on disk, because a browser upload and a desktop drag-drop must accept
/// the same corpus — a limit that depended on which shell you happened to use
/// would be exactly the parity failure L12 forbids.
pub const UPLOAD_BYTES_MAX: u64 = pos_ingest::INTAKE_FILE_BYTES_MAX;

/// Where the upload route parks a request body while it streams. Defaults to
/// the OS temp directory; a deployment whose temp filesystem is small or
/// memory-backed points this somewhere with room.
pub const UPLOAD_STAGING_DIR_ENV: &str = "POS_UPLOAD_STAGING_DIR";

/// Builds the API router over the shared registry. `pos-server` (m0-s08)
/// layers auth, static assets, and the control plane around this.
pub fn router(runtime: Arc<LocalRuntime>) -> Router {
    Router::new()
        .route("/api/cmd/{name}", post(dispatch_command))
        .route("/api/query/{name}", get(dispatch_query))
        .route("/api/stream/{name}", get(dispatch_stream))
        .layer(DefaultBodyLimit::max(API_HTTP_BODY_BYTES_MAX))
        // Added *after* the body-limit layer, so it is not covered by it: a
        // recording is gigabytes and a command input is kilobytes, and one
        // cap for both would mean choosing which of the two to get wrong.
        // The upload route states its own bound and counts it while
        // streaming, so nothing is ever buffered to find out how big it was.
        .route(
            "/api/upload/{name}",
            post(dispatch_upload).layer(DefaultBodyLimit::disable()),
        )
        .with_state(runtime)
}

/// Serves the router until `shutdown` resolves. Graceful: in-flight dispatch
/// completes before the listener closes (the WAL-flush half of shutdown is
/// the store's own drop discipline).
///
/// # Errors
///
/// Returns the underlying I/O error when the listener fails.
pub async fn serve(
    listener: tokio::net::TcpListener,
    runtime: Arc<LocalRuntime>,
    shutdown: impl Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(runtime))
        .with_graceful_shutdown(shutdown)
        .await
}

/// The status a given envelope code travels under. Pure and total so the
/// mapping is unit-testable and a new code cannot crash a response path —
/// unmapped codes are server-side failures by default.
#[must_use]
pub fn status_for_error_code(code: &str) -> StatusCode {
    match code {
        "unknown_query" | "unknown_command" | "unknown_stream" | "not_a_project" => {
            StatusCode::NOT_FOUND
        }
        "invalid_input" => StatusCode::BAD_REQUEST,
        // The upload route's own bound, refused mid-stream rather than after
        // the body is resident (m1-s07).
        "limit_exceeded" => StatusCode::PAYLOAD_TOO_LARGE,
        "unauthenticated" => StatusCode::UNAUTHORIZED,
        "forbidden" => StatusCode::FORBIDDEN,
        "already_exists" | "open_project_limit" | "resume_window_exceeded" | "state_mutated" => {
            StatusCode::CONFLICT
        }
        "not_yet_supported" => StatusCode::NOT_IMPLEMENTED,
        _ => StatusCode::INTERNAL_SERVER_ERROR,
    }
}

/// Optional inputs a read/stream request may carry in its query string.
/// Unknown parameters are ignored — a transport does not police inputs the
/// registry will type-check anyway.
#[derive(Deserialize)]
struct ReadParams {
    input: Option<String>,
    /// Resume cursor fallback for clients that cannot set `Last-Event-ID`.
    from: Option<String>,
}

async fn dispatch_command(
    Path(name): Path<String>,
    State(runtime): State<Arc<LocalRuntime>>,
    body: String,
) -> Response {
    respond_command(runtime, name, body).await
}

async fn dispatch_query(
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    State(runtime): State<Arc<LocalRuntime>>,
) -> Response {
    respond_query(runtime, name, params.input).await
}

async fn dispatch_stream(
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    headers: HeaderMap,
    State(runtime): State<Arc<LocalRuntime>>,
) -> Response {
    // The browser-standard reconnect header wins; `?from=` serves clients
    // that cannot set headers. Both parse through the one framing function.
    let last_event_id = headers
        .get("last-event-id")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
        .or(params.from);
    respond_stream(runtime, name, params.input, last_event_id).await
}

async fn dispatch_upload(
    Path(name): Path<String>,
    Query(params): Query<ReadParams>,
    State(runtime): State<Arc<LocalRuntime>>,
    body: Body,
) -> Response {
    respond_upload(runtime, name, params.input, body).await
}

/// The one HTTP rendering of an upload: stream the body to a file this
/// transport owns, then dispatch the named command against it.
///
/// The transport never looks inside `input_json`. It carries bytes and a
/// name, exactly as every other route does; the only difference is that here
/// "bytes" means the request body instead of a JSON document (L12).
pub async fn respond_upload(
    runtime: Arc<LocalRuntime>,
    name: String,
    input_json: Option<String>,
    body: Body,
) -> Response {
    let Some(input_json) = input_json else {
        return error_response(&ApiError {
            code: "invalid_input",
            message: "an upload carries its typed input in the `input` query parameter".to_owned(),
            retriable: false,
        });
    };
    let staged = match StagedUpload::create().await {
        Ok(staged) => staged,
        Err(error) => return error_response(&error),
    };
    if let Err(error) = staged.write_body(body).await {
        return error_response(&error);
    }
    let path = staged.path().to_path_buf();
    let outcome =
        run_blocking(move || runtime.command_with_upload(&name, &input_json, &path)).await;
    // The staging file is dropped here, whether the command succeeded or not:
    // the CAS already holds whatever was accepted, and a temp file that
    // outlived its request would be user content sitting outside the project
    // directory the user owns (L4).
    drop(staged);
    json_response(outcome)
}

/// A request body parked on disk for exactly as long as one dispatch takes.
struct StagedUpload {
    path: tempfile::TempPath,
}

impl StagedUpload {
    async fn create() -> Result<Self, ApiError> {
        let directory = std::env::var_os(UPLOAD_STAGING_DIR_ENV)
            .map_or_else(std::env::temp_dir, std::path::PathBuf::from);
        let file = tempfile::Builder::new()
            .prefix("pos-upload-")
            .tempfile_in(&directory)
            .map_err(|error| ApiError {
                code: "storage_failure",
                message: format!(
                    "staging an upload in {} failed: {error}",
                    directory.display()
                ),
                retriable: true,
            })?;
        Ok(Self {
            path: file.into_temp_path(),
        })
    }

    fn path(&self) -> &std::path::Path {
        &self.path
    }

    /// Streams the body through, counting as it goes. Nothing is buffered to
    /// discover the size: the refusal happens at the byte that crosses the
    /// bound, which is what makes the cap real at sixteen gibibytes (L8).
    async fn write_body(&self, body: Body) -> Result<(), ApiError> {
        use tokio::io::AsyncWriteExt;
        use tokio_stream::StreamExt;
        let mut file = tokio::fs::File::create(self.path())
            .await
            .map_err(|error| storage_failure("open the upload staging file", &error))?;
        let mut stream = body.into_data_stream();
        let mut written = 0_u64;
        while let Some(chunk) = stream.next().await {
            let chunk = chunk.map_err(|error| ApiError {
                code: "invalid_input",
                message: format!("the upload body ended early: {error}"),
                retriable: true,
            })?;
            written = written.saturating_add(chunk.len() as u64);
            if written > UPLOAD_BYTES_MAX {
                return Err(ApiError {
                    code: "limit_exceeded",
                    message: format!("an upload is at most {UPLOAD_BYTES_MAX} bytes"),
                    retriable: false,
                });
            }
            file.write_all(&chunk)
                .await
                .map_err(|error| storage_failure("write the upload staging file", &error))?;
        }
        file.flush()
            .await
            .map_err(|error| storage_failure("flush the upload staging file", &error))
    }
}

fn storage_failure(operation: &'static str, error: &std::io::Error) -> ApiError {
    ApiError {
        code: "storage_failure",
        message: format!("{operation}: {error}"),
        retriable: true,
    }
}

/// Builds the one HTTP rendering of a command dispatch. Public so the
/// authenticated `pos-server` routes (m0-s08) reuse the exact same bytes,
/// statuses, and blocking discipline — transport glue exists once.
pub async fn respond_command(
    runtime: Arc<LocalRuntime>,
    name: String,
    input_json: String,
) -> Response {
    let outcome = run_blocking(move || runtime.command(&name, &input_json)).await;
    json_response(outcome)
}

/// The one HTTP rendering of a query dispatch (`None` input dispatches `{}`).
pub async fn respond_query(
    runtime: Arc<LocalRuntime>,
    name: String,
    input_json: Option<String>,
) -> Response {
    let input = input_json.unwrap_or_else(|| "{}".to_owned());
    let outcome = run_blocking(move || runtime.query_with_input(&name, &input)).await;
    json_response(outcome)
}

/// The one HTTP rendering of a stream subscribe, including resume-cursor
/// parsing and SSE framing.
pub async fn respond_stream(
    runtime: Arc<LocalRuntime>,
    name: String,
    input_json: Option<String>,
    last_event_id: Option<String>,
) -> Response {
    let input = input_json.unwrap_or_else(|| "{}".to_owned());
    let subscribed_runtime = Arc::clone(&runtime);
    let subscribed_name = name.clone();
    let subscribed_input = input.clone();
    let outcome = run_blocking(move || {
        let cursor = parse_resume_cursor(last_event_id.as_deref())?;
        let frames =
            subscribed_runtime.stream_subscribe(&subscribed_name, &subscribed_input, cursor)?;
        Ok((cursor, frames))
    })
    .await;
    let (cursor, frames) = match outcome {
        Ok(subscribed) => subscribed,
        Err(error) => return error_response(&error),
    };

    // One complete replay window plus the live slot. Backpressure can block
    // only this subscriber's feeder thread; it never blocks the Run worker or
    // grows an unbounded broker queue.
    let queue_len = STREAM_RESUME_WINDOW_LEN.saturating_add(1);
    let (sender, receiver) = tokio::sync::mpsc::channel::<Result<Bytes, Infallible>>(queue_len);
    let tail_cursor = frames.last().map_or(cursor, |frame| Some(frame.stream_seq));
    let _stream_task = tokio::task::spawn_blocking(move || {
        let send = |body: String| sender.blocking_send(Ok(Bytes::from(body))).is_ok();
        if !send(format!("retry: {SSE_RETRY_MS}\n\n")) {
            return;
        }
        for frame in frames {
            if !send(frame.to_sse()) {
                return;
            }
        }
        let followed =
            runtime.stream_follow(&name, &input, tail_cursor, |frame| send(frame.to_sse()));
        if let Err(error) = followed {
            let _ = send(format!(
                "event: stream.error\ndata: {}\n\n",
                error.to_json()
            ));
        }
    });
    stream_response(ReceiverStream::new(receiver))
}

/// Renders a typed envelope under its deliberate status — the shared error
/// path for server-shell layers (auth, ACL) that refuse before dispatch.
#[must_use]
pub fn envelope_response(error: &ApiError) -> Response {
    error_response(error)
}

/// Registry dispatch does file I/O, so it runs on the blocking pool. A
/// panicked dispatch surfaces as a typed envelope rather than a hung socket.
async fn run_blocking<T: Send + 'static>(
    dispatch: impl FnOnce() -> Result<T, ApiError> + Send + 'static,
) -> Result<T, ApiError> {
    match tokio::task::spawn_blocking(dispatch).await {
        Ok(result) => result,
        Err(join_error) => Err(ApiError {
            code: "dispatch_failure",
            message: format!("registry dispatch did not complete: {join_error}"),
            retriable: true,
        }),
    }
}

fn json_response(outcome: Result<String, ApiError>) -> Response {
    match outcome {
        Ok(body) => response_with(StatusCode::OK, "application/json", body),
        Err(error) => error_response(&error),
    }
}

fn error_response(error: &ApiError) -> Response {
    response_with(
        status_for_error_code(error.code),
        "application/json",
        error.to_json(),
    )
}

fn response_with(status: StatusCode, content_type: &'static str, body: String) -> Response {
    let built = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, content_type)
        .body(Body::from(body));
    match built {
        Ok(response) => response,
        // Static status + static header cannot fail to build; a broken
        // response is still answered, not dropped.
        Err(_) => Response::new(Body::from(
            "{\"code\":\"dispatch_failure\",\"message\":\"response assembly failed\",\"retriable\":true}",
        )),
    }
}

fn stream_response(stream: ReceiverStream<Result<Bytes, Infallible>>) -> Response {
    let built = Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, "text/event-stream")
        .header(header::CACHE_CONTROL, "no-cache")
        .body(Body::from_stream(stream));
    match built {
        Ok(response) => response,
        Err(_) => Response::new(Body::from(
            "{\"code\":\"dispatch_failure\",\"message\":\"stream response assembly failed\",\"retriable\":true}",
        )),
    }
}

#[cfg(test)]
mod tests {
    use super::status_for_error_code;
    use axum::http::StatusCode;

    #[test]
    fn every_known_envelope_code_has_a_deliberate_status() {
        for (code, status) in [
            ("unknown_query", StatusCode::NOT_FOUND),
            ("unknown_command", StatusCode::NOT_FOUND),
            ("unknown_stream", StatusCode::NOT_FOUND),
            ("not_a_project", StatusCode::NOT_FOUND),
            ("invalid_input", StatusCode::BAD_REQUEST),
            ("unauthenticated", StatusCode::UNAUTHORIZED),
            ("forbidden", StatusCode::FORBIDDEN),
            ("already_exists", StatusCode::CONFLICT),
            ("open_project_limit", StatusCode::CONFLICT),
            ("resume_window_exceeded", StatusCode::CONFLICT),
            ("state_mutated", StatusCode::CONFLICT),
            ("not_yet_supported", StatusCode::NOT_IMPLEMENTED),
            ("durability_failure", StatusCode::INTERNAL_SERVER_ERROR),
            ("something_unmapped", StatusCode::INTERNAL_SERVER_ERROR),
        ] {
            assert_eq!(status_for_error_code(code), status, "code {code}");
        }
    }
}
