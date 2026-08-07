//! The axum HTTP+SSE transport (m0-s06, `http` feature).
//!
//! Thin dispatch, zero logic (L12): every handler resolves `(name, input)`
//! through the same [`LocalRuntime`] registry the Tauri IPC transport calls
//! and forwards the resulting bytes unchanged. The only transport-owned
//! decisions are the HTTP status (a pure function of the envelope code) and
//! the SSE content type — both contract-tested against the IPC transport in
//! `bins/pos-server/tests/http_contract.rs`.

use crate::{ApiError, LocalRuntime, sse_body, stream::parse_resume_cursor};
use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use serde::Deserialize;
use std::future::Future;
use std::sync::Arc;

/// Command/query inputs are small typed JSON documents (paths, counts, ids).
/// Blob and export payloads never travel this transport — they move through
/// the store — so anything above this cap is a defect or an attack (L8).
pub const API_HTTP_BODY_BYTES_MAX: usize = 1024 * 1024;

/// Builds the API router over the shared registry. `pos-server` (m0-s08)
/// layers auth, static assets, and the control plane around this.
pub fn router(runtime: Arc<LocalRuntime>) -> Router {
    Router::new()
        .route("/api/cmd/{name}", post(dispatch_command))
        .route("/api/query/{name}", get(dispatch_query))
        .route("/api/stream/{name}", get(dispatch_stream))
        .layer(DefaultBodyLimit::max(API_HTTP_BODY_BYTES_MAX))
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
    let outcome = run_blocking(move || {
        parse_resume_cursor(last_event_id.as_deref())
            .and_then(|cursor| runtime.stream_subscribe(&name, &input, cursor))
    })
    .await;
    match outcome {
        Ok(frames) => response_with(StatusCode::OK, "text/event-stream", sse_body(&frames)),
        Err(error) => error_response(&error),
    }
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
