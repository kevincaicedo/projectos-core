//! Router assembly (m0-s08): `/auth/*` (the deployment surface), `/api/*`
//! (the pos-api transport behind session + ACL + audit), and the static
//! `apps/ui` bundle.
//!
//! The API handlers here add exactly three things before the shared
//! `pos_api::http` responders run: authentication (session cookie → account),
//! authorization (deny-by-default ACL, `acl.rs`), and the audit row. They
//! never touch dispatch results — transport parity stays a property of
//! `pos-api`, not a discipline of this file.

use crate::acl;
use crate::auth;
use crate::control::{ControlDb, hex};
use axum::Router;
use axum::body::Body;
use axum::extract::{DefaultBodyLimit, Path as UrlPath, Query, State};
use axum::http::{HeaderMap, StatusCode, header};
use axum::response::Response;
use axum::routing::{get, post};
use pos_api::{
    ApiError, CommandName, FoundationClock, LocalBootstrapConfig, LocalRuntime, QueryName,
    StreamName, UserId, WallClock, bootstrap_local_runtime,
};
use serde::Deserialize;
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

/// Accounts served concurrently from one process (L8). Each entry is a
/// registry + session table (~KBs); the refusal names the bound.
const ACCOUNT_RUNTIME_COUNT_MAX: usize = 256;

/// Static bundle budget (L8): the production UI bundle is ~1 MB; 64 MiB
/// refuses a mispointed directory instead of memory-mapping a mistake.
const STATIC_ASSET_BYTES_MAX: u64 = 64 * 1024 * 1024;

/// Auth request bodies are one email + one password.
const AUTH_BODY_BYTES_MAX: usize = 4 * 1024;

/// `created_device` column budget: the user-agent is untrusted data; it is
/// stored (as data) truncated, never parsed.
const CREATED_DEVICE_LEN_MAX: usize = 128;

pub struct ServerConfig {
    pub data_root: PathBuf,
    /// The built `apps/ui` bundle; `None` serves the API only (CI harnesses).
    pub ui_dist: Option<PathBuf>,
}

pub struct ServerState {
    control: ControlDb,
    data_root: PathBuf,
    clock: FoundationClock,
    /// One isolated runtime per authenticated account: session state
    /// (`project.list`) cannot cross accounts because it never shares an
    /// instance. Keyed by account id bytes for deterministic iteration.
    runtimes: Mutex<BTreeMap<[u8; 16], Arc<LocalRuntime>>>,
    assets: StaticAssets,
}

impl ServerState {
    /// # Errors
    ///
    /// Returns the typed envelope when control.db or the UI bundle cannot be
    /// loaded.
    pub fn initialize(config: &ServerConfig) -> Result<Arc<Self>, ApiError> {
        std::fs::create_dir_all(&config.data_root).map_err(|error| ApiError {
            code: "control_failure",
            message: format!("create {}: {error}", config.data_root.display()),
            retriable: true,
        })?;
        let control = ControlDb::open(&config.data_root.join("control.db"))?;
        let assets = match &config.ui_dist {
            Some(dist) => StaticAssets::load(dist)?,
            None => StaticAssets::empty(),
        };
        Ok(Arc::new(Self {
            control,
            data_root: config.data_root.clone(),
            clock: FoundationClock,
            runtimes: Mutex::new(BTreeMap::new()),
            assets,
        }))
    }

    /// Direct control-database access for the RBAC test seam and operator
    /// tooling. Not reachable from any route.
    #[must_use]
    pub fn control(&self) -> &ControlDb {
        &self.control
    }

    fn runtime_for_account(&self, account: [u8; 16]) -> Result<Arc<LocalRuntime>, ApiError> {
        let mut runtimes = match self.runtimes.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        if let Some(existing) = runtimes.get(&account) {
            return Ok(Arc::clone(existing));
        }
        if runtimes.len() >= ACCOUNT_RUNTIME_COUNT_MAX {
            return Err(ApiError {
                code: "control_failure",
                message: format!(
                    "this process already serves {ACCOUNT_RUNTIME_COUNT_MAX} account runtimes; \
                     restart the server or raise the bound deliberately"
                ),
                retriable: true,
            });
        }
        let runtime = Arc::new(bootstrap_local_runtime(
            LocalBootstrapConfig::isolated(self.data_root.join("packs"))
                .with_user(UserId::from_bytes(account)),
        ));
        runtimes.insert(account, Arc::clone(&runtime));
        Ok(runtime)
    }
}

/// Builds the complete server router.
pub fn router(state: Arc<ServerState>) -> Router {
    Router::new()
        .route("/auth/signup", post(signup))
        .route("/auth/login", post(login))
        .route("/auth/logout", post(logout))
        .route("/auth/me", get(me))
        .route("/auth/audit", get(audit_trail))
        .route("/api/cmd/{name}", post(api_command))
        .route("/api/query/{name}", get(api_query))
        .route("/api/stream/{name}", get(api_stream))
        .fallback(static_asset)
        .layer(DefaultBodyLimit::max(
            pos_api::http::API_HTTP_BODY_BYTES_MAX,
        ))
        .with_state(state)
}

/// Serves until `shutdown` resolves (same discipline as the bare transport).
///
/// # Errors
///
/// Returns the underlying I/O error when the listener fails.
pub async fn serve(
    listener: tokio::net::TcpListener,
    state: Arc<ServerState>,
    shutdown: impl std::future::Future<Output = ()> + Send + 'static,
) -> std::io::Result<()> {
    axum::serve(listener, router(state))
        .with_graceful_shutdown(shutdown)
        .await
}

// ---------------------------------------------------------------- auth

#[derive(Deserialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
struct CredentialsBody {
    email: String,
    password: String,
}

async fn signup(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if body.len() > AUTH_BODY_BYTES_MAX {
        return refuse(&invalid("auth request body exceeds the bound"));
    }
    let credentials: CredentialsBody = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(error) => return refuse(&invalid(&error.to_string())),
    };
    if !credentials.email.contains('@') || credentials.email.len() > 254 {
        return refuse(&invalid("email must contain @ and fit in 254 bytes"));
    }
    let hashed = match auth::hash_password(&credentials.password) {
        Ok(hashed) => hashed,
        Err(error) => return refuse(&error),
    };
    let now_ms = state.clock.now_ms();
    let (account_id, workspace_id) = match (random_id(), random_id()) {
        (Ok(account), Ok(workspace)) => (account, workspace),
        (Err(error), _) | (_, Err(error)) => return refuse(&error),
    };
    if let Err(error) = state.control.create_account(
        account_id,
        workspace_id,
        &credentials.email,
        &hashed,
        now_ms,
    ) {
        return refuse(&error);
    }
    if let Err(error) = state.control.audit(account_id, "auth.signup", "-", now_ms) {
        return refuse(&error);
    }
    install_session(&state, account_id, workspace_id, &headers, now_ms)
}

async fn login(
    State(state): State<Arc<ServerState>>,
    headers: HeaderMap,
    body: String,
) -> Response {
    if body.len() > AUTH_BODY_BYTES_MAX {
        return refuse(&invalid("auth request body exceeds the bound"));
    }
    let credentials: CredentialsBody = match serde_json::from_str(&body) {
        Ok(parsed) => parsed,
        Err(error) => return refuse(&invalid(&error.to_string())),
    };
    // One uniform failure for unknown email and wrong password: login must
    // not be an account-existence oracle.
    let rejected = ApiError {
        code: "unauthenticated",
        message: "invalid email or password".to_owned(),
        retriable: false,
    };
    let Ok(Some((account_id, stored_hash))) = state
        .control
        .account_by_email(&credentials.email)
        .map_err(|error| refuse(&error))
    else {
        return refuse(&rejected);
    };
    if !auth::password_matches(&credentials.password, &stored_hash) {
        return refuse(&rejected);
    }
    let now_ms = state.clock.now_ms();
    let workspace_id = match state.control.owned_workspace(account_id) {
        Ok(Some(workspace)) => workspace,
        Ok(None) => [0; 16],
        Err(error) => return refuse(&error),
    };
    if let Err(error) = state.control.audit(account_id, "auth.login", "-", now_ms) {
        return refuse(&error);
    }
    install_session(&state, account_id, workspace_id, &headers, now_ms)
}

async fn logout(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    let cookie_header = header_str(&headers, header::COOKIE);
    if let Some(value) = cookie_header
        .and_then(cookie_pair)
        .and_then(|value| auth::presented_token_hash(&value))
        && let Err(error) = state.control.delete_session(value)
    {
        return refuse(&error);
    }
    json_with_cookie(StatusCode::OK, "{}", &auth::clearing_cookie())
}

async fn me(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    let (identity, _) = match authenticate(&state, &headers) {
        Ok(authenticated) => authenticated,
        Err(error) => return refuse(&error),
    };
    let workspace_id = match state.control.owned_workspace(identity) {
        Ok(Some(workspace)) => workspace,
        Ok(None) => [0; 16],
        Err(error) => return refuse(&error),
    };
    session_body(&state, StatusCode::OK, identity, workspace_id, None)
}

async fn audit_trail(State(state): State<Arc<ServerState>>, headers: HeaderMap) -> Response {
    let (identity, _) = match authenticate(&state, &headers) {
        Ok(authenticated) => authenticated,
        Err(error) => return refuse(&error),
    };
    match state.control.audit_rows_json(identity) {
        Ok(body) => json_with_cookie(StatusCode::OK, &body, ""),
        Err(error) => refuse(&error),
    }
}

// ---------------------------------------------------------------- api

#[derive(Deserialize)]
struct ReadParams {
    input: Option<String>,
    from: Option<String>,
}

async fn api_command(
    State(state): State<Arc<ServerState>>,
    UrlPath(name): UrlPath<String>,
    headers: HeaderMap,
    body: String,
) -> Response {
    let (account, _) = match authenticate(&state, &headers) {
        Ok(authenticated) => authenticated,
        Err(error) => return refuse(&error),
    };
    let Some(command) = CommandName::parse(&name) else {
        return refuse(&ApiError::unknown_command(&name));
    };
    if let Err(error) =
        acl::authorize_command(&state.control, &state.data_root, account, command, &body)
    {
        return refuse(&error);
    }
    if let Some(action) = acl::audited_action(command)
        && let Err(error) =
            state
                .control
                .audit(account, action, &audit_target(&body), state.clock.now_ms())
    {
        return refuse(&error);
    }
    let runtime = match state.runtime_for_account(account) {
        Ok(runtime) => runtime,
        Err(error) => return refuse(&error),
    };
    pos_api::http::respond_command(runtime, name, body).await
}

async fn api_query(
    State(state): State<Arc<ServerState>>,
    UrlPath(name): UrlPath<String>,
    Query(params): Query<ReadParams>,
    headers: HeaderMap,
) -> Response {
    let (account, _) = match authenticate(&state, &headers) {
        Ok(authenticated) => authenticated,
        Err(error) => return refuse(&error),
    };
    let Some(query) = QueryName::parse(&name) else {
        return refuse(&ApiError::unknown_query(&name));
    };
    let input = params.input.clone().unwrap_or_else(|| "{}".to_owned());
    if let Err(error) =
        acl::authorize_query(&state.control, &state.data_root, account, query, &input)
    {
        return refuse(&error);
    }
    let runtime = match state.runtime_for_account(account) {
        Ok(runtime) => runtime,
        Err(error) => return refuse(&error),
    };
    pos_api::http::respond_query(runtime, name, params.input).await
}

async fn api_stream(
    State(state): State<Arc<ServerState>>,
    UrlPath(name): UrlPath<String>,
    Query(params): Query<ReadParams>,
    headers: HeaderMap,
) -> Response {
    let (account, _) = match authenticate(&state, &headers) {
        Ok(authenticated) => authenticated,
        Err(error) => return refuse(&error),
    };
    if StreamName::parse(&name).is_none() {
        return refuse(&ApiError::unknown_stream(&name));
    }
    // All v0 streams are reads with no typed workspace context (m0-s13 adds
    // the run input, and with it a real per-workspace check).
    let runtime = match state.runtime_for_account(account) {
        Ok(runtime) => runtime,
        Err(error) => return refuse(&error),
    };
    let last_event_id = header_str(&headers, header::HeaderName::from_static("last-event-id"))
        .map(str::to_owned)
        .or(params.from.clone());
    pos_api::http::respond_stream(runtime, name, params.input, last_event_id).await
}

// ---------------------------------------------------------------- static

struct StaticAssets {
    files: BTreeMap<String, (&'static str, Vec<u8>)>,
}

impl StaticAssets {
    fn empty() -> Self {
        Self {
            files: BTreeMap::new(),
        }
    }

    fn load(dist: &Path) -> Result<Self, ApiError> {
        let mut files = BTreeMap::new();
        let mut total: u64 = 0;
        let mut pending = vec![dist.to_path_buf()];
        while let Some(directory) = pending.pop() {
            let entries = std::fs::read_dir(&directory).map_err(|error| ApiError {
                code: "control_failure",
                message: format!("read UI bundle {}: {error}", directory.display()),
                retriable: false,
            })?;
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    pending.push(path);
                    continue;
                }
                let bytes = std::fs::read(&path).map_err(|error| ApiError {
                    code: "control_failure",
                    message: format!("read {}: {error}", path.display()),
                    retriable: false,
                })?;
                total += bytes.len() as u64;
                if total > STATIC_ASSET_BYTES_MAX {
                    return Err(ApiError {
                        code: "control_failure",
                        message: format!(
                            "UI bundle exceeds the {STATIC_ASSET_BYTES_MAX}-byte budget; \
                             is POS_SERVER_UI_DIST pointing at the right directory?"
                        ),
                        retriable: false,
                    });
                }
                let relative = path
                    .strip_prefix(dist)
                    .map_or_else(|_| path.display().to_string(), |p| p.display().to_string());
                files.insert(format!("/{relative}"), (content_type_for(&relative), bytes));
            }
        }
        Ok(Self { files })
    }

    fn response_for(&self, request_path: &str) -> Option<Response> {
        let (path, cache_control) = if self.files.contains_key(request_path) {
            // Vite emits content-hashed names under /assets — immutable.
            let cache = if request_path.starts_with("/assets/") {
                "public, max-age=31536000, immutable"
            } else {
                "no-cache"
            };
            (request_path, cache)
        } else if !request_path.contains('.') && self.files.contains_key("/index.html") {
            // SPA fallback: extensionless routes render the app shell.
            ("/index.html", "no-cache")
        } else {
            return None;
        };
        let (content_type, bytes) = self.files.get(path)?;
        Response::builder()
            .status(StatusCode::OK)
            .header(header::CONTENT_TYPE, *content_type)
            .header(header::CACHE_CONTROL, cache_control)
            .body(Body::from(bytes.clone()))
            .ok()
    }
}

async fn static_asset(
    State(state): State<Arc<ServerState>>,
    request: axum::extract::Request,
) -> Response {
    let path = request.uri().path().to_owned();
    match state.assets.response_for(&path) {
        Some(response) => response,
        None => refuse(&ApiError {
            code: "unknown_query",
            message: format!("no route or asset at {path:?}"),
            retriable: false,
        }),
    }
}

fn content_type_for(path: &str) -> &'static str {
    match path.rsplit_once('.').map(|(_, extension)| extension) {
        Some("html") => "text/html; charset=utf-8",
        Some("js") => "text/javascript",
        Some("css") => "text/css",
        Some("svg") => "image/svg+xml",
        Some("json") => "application/json",
        Some("png") => "image/png",
        Some("ico") => "image/x-icon",
        Some("woff2") => "font/woff2",
        _ => "application/octet-stream",
    }
}

// ---------------------------------------------------------------- shared

fn authenticate(state: &ServerState, headers: &HeaderMap) -> Result<([u8; 16], String), ApiError> {
    let cookie_header = header_str(headers, header::COOKIE);
    let identity =
        auth::identity_from_cookie_header(&state.control, cookie_header, state.clock.now_ms())?;
    match identity {
        Some(session) => Ok((session.account_id, session.email)),
        None => Err(ApiError {
            code: "unauthenticated",
            message: "sign in to use this server".to_owned(),
            retriable: false,
        }),
    }
}

fn install_session(
    state: &ServerState,
    account_id: [u8; 16],
    workspace_id: [u8; 16],
    headers: &HeaderMap,
    now_ms: u64,
) -> Response {
    let minted = match auth::mint_session_token() {
        Ok(minted) => minted,
        Err(error) => return refuse(&error),
    };
    let device: String = header_str(headers, header::USER_AGENT)
        .unwrap_or("unknown")
        .chars()
        .take(CREATED_DEVICE_LEN_MAX)
        .collect();
    if let Err(error) = state
        .control
        .insert_session(minted.token_hash, account_id, &device, now_ms)
    {
        return refuse(&error);
    }
    session_body(
        state,
        StatusCode::OK,
        account_id,
        workspace_id,
        Some(&auth::session_cookie(&minted.cookie_value)),
    )
}

fn session_body(
    state: &ServerState,
    status: StatusCode,
    account_id: [u8; 16],
    workspace_id: [u8; 16],
    cookie: Option<&str>,
) -> Response {
    let body = format!(
        "{{\"accountId\":\"{}\",\"workspaceId\":\"{}\",\"projectsRoot\":{}}}",
        hex(&account_id),
        hex(&workspace_id),
        serde_json::to_string(&acl::workspace_projects_root(
            &state.data_root,
            workspace_id
        ))
        .unwrap_or_else(|_| "\"\"".to_owned()),
    );
    json_with_cookie(status, &body, cookie.unwrap_or(""))
}

/// The audit `target` column: the path the request names, or `-`. Parsed
/// loosely — the target is descriptive, the ACL already validated it.
fn audit_target(input_json: &str) -> String {
    serde_json::from_str::<serde_json::Value>(input_json)
        .ok()
        .and_then(|value| value.get("path")?.as_str().map(str::to_owned))
        .unwrap_or_else(|| "-".to_owned())
}

fn header_str<K: axum::http::header::AsHeaderName>(headers: &HeaderMap, name: K) -> Option<&str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn cookie_pair(header: &str) -> Option<String> {
    header.split(';').find_map(|pair| {
        let (key, value) = pair.split_once('=')?;
        (key.trim() == auth::SESSION_COOKIE_NAME).then(|| value.trim().to_owned())
    })
}

fn refuse(error: &ApiError) -> Response {
    pos_api::http::envelope_response(error)
}

fn json_with_cookie(status: StatusCode, body: &str, set_cookie: &str) -> Response {
    let mut builder = Response::builder()
        .status(status)
        .header(header::CONTENT_TYPE, "application/json");
    if !set_cookie.is_empty() {
        builder = builder.header(header::SET_COOKIE, set_cookie);
    }
    match builder.body(Body::from(body.to_owned())) {
        Ok(response) => response,
        Err(_) => refuse(&invalid("response assembly failed")),
    }
}

fn invalid(message: &str) -> ApiError {
    ApiError {
        code: "invalid_input",
        message: message.to_owned(),
        retriable: false,
    }
}

fn random_id() -> Result<[u8; 16], ApiError> {
    let mut id = [0_u8; 16];
    getrandom::fill(&mut id).map_err(|error| ApiError {
        code: "auth_failure",
        message: format!("entropy source unavailable: {error}"),
        retriable: true,
    })?;
    Ok(id)
}
