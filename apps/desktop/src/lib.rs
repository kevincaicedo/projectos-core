//! Thin Tauri v2 desktop boot shell (L12).
//!
//! The shell owns the native window and transport selection only. Domain logic
//! remains behind `pos-api`; m0-s07 adds dialogs, menus, packaging, and its boot
//! smoke without changing this cut.
//!
//! The IPC transport added by the m0-s06 slice is deliberately incapable of
//! shaping a result: it resolves a name through the shared registry and returns
//! the bytes it receives. Anything else would make shell parity a matter of
//! discipline instead of a property a test can check.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, LocalRuntime, bootstrap_local_runtime};
use std::path::PathBuf;

/// The one dispatch path both the IPC command and its test use.
fn dispatch_query(runtime: &LocalRuntime, name: &str) -> Result<String, String> {
    runtime.query(name).map_err(|error| error.to_json())
}

/// Tauri IPC transport for the typed read surface.
///
/// Both the success and error payloads are already-serialized `pos-api` bytes,
/// so the webview receives exactly what any other transport would send.
#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_query(name: String, runtime: tauri::State<'_, LocalRuntime>) -> Result<String, String> {
    dispatch_query(runtime.inner(), &name)
}

/// Runs the native event loop and returns startup/runtime failures to `main`.
///
/// # Errors
///
/// Returns Tauri's typed error when configuration, webview startup, or the
/// native event loop fails.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() -> Result<(), tauri::Error> {
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    tauri::Builder::default()
        .manage(runtime)
        .invoke_handler(tauri::generate_handler![api_query])
        .run(tauri::generate_context!())
}

#[cfg(test)]
mod tests {
    use super::dispatch_query;
    use pos_api::{LocalBootstrapConfig, QueryName, bootstrap_local_runtime};
    use std::path::PathBuf;

    #[test]
    fn the_ipc_transport_forwards_registry_bytes_unchanged() {
        let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-desktop-pack-root",
        )));
        let name = QueryName::CapabilitySnapshot.as_str();
        let through_ipc = dispatch_query(&runtime, name).expect("the registered query resolves");
        let direct = runtime.query(name).expect("the registered query resolves");
        assert_eq!(through_ipc, direct);
    }

    #[test]
    fn an_unknown_query_reaches_the_webview_as_the_typed_envelope() {
        let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-desktop-pack-root",
        )));
        let error = dispatch_query(&runtime, "capability.snapsh0t")
            .expect_err("an unregistered name must not resolve");
        assert!(error.contains("\"code\":\"unknown_query\""));
        assert!(error.contains("\"retriable\":false"));
    }
}
