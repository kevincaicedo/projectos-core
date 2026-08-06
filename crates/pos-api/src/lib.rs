//! # pos-api
//!
//! The ONE typed surface (L12): commands, queries, streams; ts-rs-generated TypeScript types; served identically over axum HTTP+SSE and Tauri IPC. Shells depend on this crate and nothing deeper.
//!
//! Skeleton created by m0-s01; filled by m0-s06. Charter: master plan §19.
//!
//! This crate holds the registry and the wire bytes. It holds no transport: a
//! shell that owns a transport dispatches a name into [`LocalRuntime::query`]
//! and forwards the resulting bytes unchanged. That is what makes shell parity
//! checkable — a transport cannot reshape a result it never decodes.

#![forbid(unsafe_code)]

mod project_ops;

pub use project_ops::{ProjectCreateInput, ProjectExportInput, ProjectPathInput, ProjectSeedInput};

use pos_capabilities::{
    AccountId, CAPABILITY_TRAIT_VERSION, CapabilityMode, CapabilityRegistry, ConnectorHostRequest,
    ConnectorHostResponse, ConnectorId, LocalCapabilityConfig, ProviderFuture, WorkspaceId,
};
use pos_foundation::SystemWallClock;
use std::fmt;
use std::future::Future;
use std::path::PathBuf;
use std::pin::pin;
use std::task::{Context, Poll, Waker};

/// Version of the query/command names and their result shapes. A shape change
/// without a bump is what the m0-s06 contract suite exists to catch.
/// v2: m0-s05 adds the project operations (create/inspect/verify/export/seed).
pub const API_SURFACE_VERSION: u16 = 2;

/// Bounded item budget for the M0 connector-host liveness tick (L8). The socket
/// itself caps this at 32; the runtime asks for less than it is allowed.
const CONNECTOR_TICK_ITEM_BUDGET: u16 = 8;

/// A provider future that is already resolved needs one poll. Four is slack for
/// a provider that yields once, and a bound instead of an unbounded spin.
const READY_POLL_COUNT_MAX: u8 = 4;

/// Every query in the v0 read surface.
///
/// Adding a variant without adding its row to the transport-parity contract
/// test fails that test, which is the structural version of "remember to test
/// the new endpoint".
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum QueryName {
    CapabilitySnapshot,
    /// Manifest, event count, head seq, snapshot state (m0-s05).
    ProjectInspect,
    /// Re-derives projections against the log + CAS integrity sweep (m0-s05).
    ProjectVerify,
}

impl QueryName {
    pub const COUNT: usize = 3;
    pub const ALL: [Self; Self::COUNT] = [
        Self::CapabilitySnapshot,
        Self::ProjectInspect,
        Self::ProjectVerify,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::CapabilitySnapshot => "capability.snapshot",
            Self::ProjectInspect => "project.inspect",
            Self::ProjectVerify => "project.verify",
        }
    }

    /// Resolves a transport-supplied name. Unknown names are `None` rather than
    /// a default, so a typo reaches the caller as a typed error instead of
    /// silently returning some other query's data.
    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL.into_iter().find(|query| query.as_str() == name)
    }
}

/// Every state-changing command in the v0 surface (m0-s05 slice; the full
/// m0-s06 registry adds run/job commands and the ts-rs pipeline).
#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
#[non_exhaustive]
pub enum CommandName {
    ProjectCreate,
    ProjectExport,
    /// Deterministic synthetic seeding — test/bench scaffolding shared with
    /// `pos-bench` (m0-s05/m0-s16), honest and documented, not a demo trick.
    ProjectSeedSynthetic,
}

impl CommandName {
    pub const COUNT: usize = 3;
    pub const ALL: [Self; Self::COUNT] = [
        Self::ProjectCreate,
        Self::ProjectExport,
        Self::ProjectSeedSynthetic,
    ];

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::ProjectCreate => "project.create",
            Self::ProjectExport => "project.export",
            Self::ProjectSeedSynthetic => "project.seed-synthetic",
        }
    }

    #[must_use]
    pub fn parse(name: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|command| command.as_str() == name)
    }
}

/// The typed error envelope every transport returns unchanged.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct ApiError {
    pub code: &'static str,
    pub message: String,
    pub retriable: bool,
}

impl ApiError {
    #[must_use]
    pub fn unknown_query(name: &str) -> Self {
        Self {
            code: "unknown_query",
            // The name is echoed as data, never interpolated into a lookup.
            message: format!("no query is registered under the name {name:?}"),
            retriable: false,
        }
    }

    #[must_use]
    pub fn unknown_command(name: &str) -> Self {
        Self {
            code: "unknown_command",
            message: format!("no command is registered under the name {name:?}"),
            retriable: false,
        }
    }

    /// Serializes to the same envelope shape on every transport.
    #[must_use]
    pub fn to_json(&self) -> String {
        let mut json = String::from("{\"code\":");
        push_json_string(&mut json, self.code);
        json.push_str(",\"message\":");
        push_json_string(&mut json, &self.message);
        json.push_str(",\"retriable\":");
        json.push_str(if self.retriable { "true" } else { "false" });
        json.push('}');
        json
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}: {}", self.code, self.message)
    }
}

impl std::error::Error for ApiError {}

/// Conservative local-process composition used until account/project startup
/// configuration lands in m0-s06/m0-s08. Media and public ingress stay
/// unavailable unless a later typed configuration explicitly enables them.
pub struct LocalBootstrapConfig {
    pack_root: PathBuf,
}

impl LocalBootstrapConfig {
    #[must_use]
    pub fn isolated(pack_root: PathBuf) -> Self {
        Self { pack_root }
    }
}

/// Process-owned runtime state exposed to thin shell transports.
pub struct LocalRuntime {
    capabilities: CapabilityRegistry,
    identity: project_ops::RuntimeIdentity,
    clock: SystemWallClock,
}

impl LocalRuntime {
    #[must_use]
    pub fn capability_count(&self) -> usize {
        self.capabilities.descriptors().len()
    }

    /// Dispatches a transport-supplied query name to its canonical JSON result.
    /// Sugar over [`Self::query_with_input`] for input-free queries.
    ///
    /// # Errors
    ///
    /// Returns the typed envelope when no query is registered under `name`.
    pub fn query(&self, name: &str) -> Result<String, ApiError> {
        self.query_with_input(name, "{}")
    }

    /// Dispatches a query with a JSON input document. Input-free queries
    /// accept (and ignore) an empty object so every transport can treat the
    /// pair `(name, input)` uniformly.
    pub fn query_with_input(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
        match QueryName::parse(name) {
            Some(QueryName::CapabilitySnapshot) => Ok(self.capability_snapshot_json()),
            Some(QueryName::ProjectInspect) => {
                project_ops::inspect(&project_ops::parse_input(input_json)?)
            }
            Some(QueryName::ProjectVerify) => {
                project_ops::verify(&project_ops::parse_input(input_json)?)
            }
            None => Err(ApiError::unknown_query(name)),
        }
    }

    /// Dispatches a state-changing command with a JSON input document.
    pub fn command(&self, name: &str, input_json: &str) -> Result<String, ApiError> {
        match CommandName::parse(name) {
            Some(CommandName::ProjectCreate) => project_ops::create(
                &self.identity,
                &self.clock,
                &project_ops::parse_input(input_json)?,
            ),
            Some(CommandName::ProjectExport) => {
                project_ops::export(&project_ops::parse_input(input_json)?)
            }
            Some(CommandName::ProjectSeedSynthetic) => {
                project_ops::seed_synthetic(&self.clock, &project_ops::parse_input(input_json)?)
            }
            None => Err(ApiError::unknown_command(name)),
        }
    }

    /// Renders the live registry state the UI renders (m0-s17 capability
    /// honesty). Every field is read from the resolved providers at call time;
    /// nothing here is a compile-time literal about runtime availability.
    #[must_use]
    pub fn capability_snapshot_json(&self) -> String {
        let mut json = String::from("{\"surfaceVersion\":");
        json.push_str(&API_SURFACE_VERSION.to_string());
        json.push_str(",\"capabilityTraitVersion\":");
        json.push_str(&CAPABILITY_TRAIT_VERSION.to_string());
        json.push_str(",\"capabilities\":[");
        for (index, descriptor) in self.capabilities.descriptors().iter().enumerate() {
            if index > 0 {
                json.push(',');
            }
            json.push_str("{\"id\":");
            push_json_string(&mut json, descriptor.id.as_str());
            json.push_str(",\"provider\":");
            push_json_string(&mut json, descriptor.provider_name);
            json.push_str(",\"state\":");
            push_capability_state(&mut json, &descriptor.mode);
            json.push('}');
        }
        json.push_str("],\"connectorHost\":");
        match self.connector_host_tick() {
            Some(tick) => {
                json.push_str("{\"hostAvailable\":");
                json.push_str(if tick.host_available { "true" } else { "false" });
                json.push_str(",\"polledCount\":");
                json.push_str(&tick.polled_count.to_string());
                json.push_str(",\"nextCursor\":");
                json.push_str(&tick.next_cursor.to_string());
                json.push('}');
            }
            None => json.push_str("null"),
        }
        json.push('}');
        json
    }

    /// Runs the bounded local mock poll/health tick. `None` means the tick did
    /// not complete, which the UI renders as an unknown host rather than as a
    /// healthy one — a failed liveness check is not evidence of liveness.
    fn connector_host_tick(&self) -> Option<ConnectorHostTick> {
        let Ok(connector_id) = ConnectorId::new("mock") else {
            return None;
        };
        let request = ConnectorHostRequest::Tick {
            connector_id,
            cursor: 0,
            max_items: CONNECTOR_TICK_ITEM_BUDGET,
        };
        match poll_ready(self.capabilities.connector_host().execute(request))? {
            Ok(ConnectorHostResponse::Tick {
                polled_count,
                next_cursor,
                host_available,
            }) => Some(ConnectorHostTick {
                polled_count,
                next_cursor,
                host_available,
            }),
            _ => None,
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct ConnectorHostTick {
    polled_count: u16,
    next_cursor: u64,
    host_available: bool,
}

/// Resolves a provider future without a runtime, within a fixed poll budget.
///
/// Local providers resolve immediately by construction. This keeps M0 shells
/// free of an async runtime while still driving the real socket; m0-s08 brings
/// the executor with the server that needs one.
fn poll_ready<T>(future: ProviderFuture<'_, T>) -> Option<T> {
    let mut future = pin!(future);
    let waker = Waker::noop();
    let mut context = Context::from_waker(waker);
    for _ in 0..READY_POLL_COUNT_MAX {
        if let Poll::Ready(output) = Future::poll(future.as_mut(), &mut context) {
            return Some(output);
        }
    }
    None
}

fn push_capability_state(json: &mut String, mode: &CapabilityMode) {
    match mode {
        CapabilityMode::Local => json.push_str("{\"mode\":\"local\"}"),
        CapabilityMode::Hosted => json.push_str("{\"mode\":\"hosted\"}"),
        CapabilityMode::Unavailable(reason) => {
            json.push_str("{\"mode\":\"unavailable\",\"reason\":");
            push_json_string(json, reason.as_str());
            json.push('}');
        }
    }
}

/// Writes a JSON string literal. Escaping is done here, once, so no caller can
/// build a result by concatenating unescaped provider text.
fn push_json_string(json: &mut String, value: &str) {
    json.push('"');
    for character in value.chars() {
        match character {
            '"' => json.push_str("\\\""),
            '\\' => json.push_str("\\\\"),
            '\n' => json.push_str("\\n"),
            '\r' => json.push_str("\\r"),
            '\t' => json.push_str("\\t"),
            control if control < ' ' => {
                json.push_str("\\u00");
                let code = u32::from(control);
                json.push(hex_digit(code >> 4));
                json.push(hex_digit(code & 0xf));
            }
            other => json.push(other),
        }
    }
    json.push('"');
}

/// Callers pass a single nibble, so the fallback is unreachable. It is a `'0'`
/// rather than a panic because a malformed escape is a cosmetic defect and a
/// crashed shell is not.
fn hex_digit(nibble: u32) -> char {
    char::from_digit(nibble, 16).unwrap_or('0')
}

/// Resolves all ten public capability sockets for a standalone process.
///
/// The fixed ids are process-local bootstrap identities, not durable ProjectOS
/// entity ids. m0-s06/m0-s08 replace them with values loaded through the typed
/// startup surface before any project state exists.
#[must_use]
pub fn bootstrap_local_runtime(config: LocalBootstrapConfig) -> LocalRuntime {
    LocalRuntime {
        capabilities: CapabilityRegistry::local(LocalCapabilityConfig {
            owner_account_id: AccountId::from_bytes([0; 16]),
            workspace_id: WorkspaceId::from_bytes([0; 16]),
            pack_root: config.pack_root,
            ffmpeg_available: false,
            ingress_reachable: false,
        }),
        identity: project_ops::RuntimeIdentity::bootstrap(),
        clock: SystemWallClock,
    }
}

/// Serializes a typed input for the `(name, input)` dispatch pair — the one
/// JSON encoder shells use, so a shell never hand-writes wire JSON.
pub fn input_json(input: &impl serde::Serialize) -> Result<String, ApiError> {
    project_ops::to_json(input)
}

/// Generates the M0 capability-card vocabulary through the single UI-facing
/// Rust surface. m0-s06 replaces the hand-rendered TypeScript shape with the
/// general ts-rs export pipeline without changing its source of truth.
#[must_use]
pub fn typescript_capability_catalog() -> String {
    pos_capabilities::typescript_catalog()
}

#[cfg(test)]
mod tests {
    use super::{
        ApiError, LocalBootstrapConfig, LocalRuntime, QueryName, bootstrap_local_runtime,
        push_json_string,
    };
    use pos_capabilities::{CapabilityId, CapabilityMode};
    use std::path::PathBuf;

    fn runtime() -> LocalRuntime {
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-bootstrap-pack-root",
        )))
    }

    #[test]
    fn isolated_startup_resolves_every_socket_with_honest_state() {
        let runtime = runtime();
        assert_eq!(runtime.capability_count(), CapabilityId::COUNT);
        let descriptors = runtime.capabilities.descriptors();
        let connector = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::ConnectorHost)
            .expect("complete registry contains connector.host");
        assert!(matches!(connector.mode, CapabilityMode::Local));
        let ingress = descriptors
            .iter()
            .find(|descriptor| descriptor.id == CapabilityId::RelayIngress)
            .expect("complete registry contains relay.ingress");
        assert!(matches!(
            ingress.mode,
            CapabilityMode::Unavailable(ref reason) if !reason.as_str().is_empty()
        ));
    }

    #[test]
    fn every_query_name_round_trips_and_unknown_names_are_typed_errors() {
        for query in QueryName::ALL {
            assert_eq!(QueryName::parse(query.as_str()), Some(query));
        }
        assert_eq!(QueryName::parse("capability.snapshot "), None);
        assert_eq!(QueryName::ALL.len(), QueryName::COUNT);

        let error = runtime()
            .query("capability.snapshot; DROP")
            .expect_err("an unregistered name must not resolve to a registered query");
        assert_eq!(error.code, "unknown_query");
        assert!(!error.retriable);
    }

    #[test]
    fn the_snapshot_reports_live_state_for_all_ten_sockets() {
        let snapshot = runtime()
            .query(QueryName::CapabilitySnapshot.as_str())
            .expect("the registered query resolves");
        for id in CapabilityId::ALL {
            assert!(
                snapshot.contains(&format!("\"id\":\"{}\"", id.as_str())),
                "{} is missing from the live snapshot",
                id.as_str()
            );
        }
        // The bounded mock tick actually ran, rather than being asserted true
        // by the UI: an unavailable host would render as `null` here.
        assert!(snapshot.contains("\"connectorHost\":{\"hostAvailable\":true"));
        assert!(snapshot.contains("\"mode\":\"unavailable\",\"reason\":\""));
        assert!(!snapshot.contains("\"reason\":\"\""));
    }

    #[test]
    fn repeated_dispatch_is_byte_identical() {
        let runtime = runtime();
        let first = runtime.capability_snapshot_json();
        let second = runtime
            .query(QueryName::CapabilitySnapshot.as_str())
            .expect("the registered query resolves");
        assert_eq!(first, second, "transports must forward identical bytes");
    }

    #[test]
    fn provider_text_cannot_break_out_of_a_json_string() {
        let mut json = String::new();
        push_json_string(&mut json, "quote\" backslash\\ newline\n tab\t bell\u{7}");
        assert_eq!(
            json,
            "\"quote\\\" backslash\\\\ newline\\n tab\\t bell\\u0007\""
        );
    }

    #[test]
    fn the_error_envelope_has_all_three_fields() {
        let json = ApiError::unknown_query("nope").to_json();
        assert!(json.starts_with("{\"code\":\"unknown_query\""));
        assert!(json.contains("\"message\":"));
        assert!(json.ends_with("\"retriable\":false}"));
    }
}
