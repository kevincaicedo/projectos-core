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

/// The dispatch paths the IPC commands and their tests share. Inputs default
/// to the empty document so `(name)` and `(name, input)` dispatch uniformly.
fn dispatch_query(
    runtime: &LocalRuntime,
    name: &str,
    input: Option<&str>,
) -> Result<String, String> {
    runtime
        .query_with_input(name, input.unwrap_or("{}"))
        .map_err(|error| error.to_json())
}

fn dispatch_command(runtime: &LocalRuntime, name: &str, input: &str) -> Result<String, String> {
    runtime
        .command(name, input)
        .map_err(|error| error.to_json())
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
fn api_query(
    name: String,
    input: Option<String>,
    runtime: tauri::State<'_, LocalRuntime>,
) -> Result<String, String> {
    dispatch_query(runtime.inner(), &name, input.as_deref())
}

/// Tauri IPC transport for the state-changing command surface (m0-s06).
#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_command(
    name: String,
    input: String,
    runtime: tauri::State<'_, LocalRuntime>,
) -> Result<String, String> {
    dispatch_command(runtime.inner(), &name, &input)
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
        .invoke_handler(tauri::generate_handler![api_query, api_command])
        .run(tauri::generate_context!())
}

#[cfg(test)]
mod tests {
    use super::{dispatch_command, dispatch_query};
    use pos_api::{
        CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, QueryName,
        bootstrap_local_runtime, input_json,
    };
    use std::path::PathBuf;

    fn runtime() -> LocalRuntime {
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-desktop-pack-root",
        )))
    }

    #[test]
    fn the_ipc_transport_forwards_registry_bytes_unchanged() {
        let runtime = runtime();
        let name = QueryName::CapabilitySnapshot.as_str();
        let through_ipc =
            dispatch_query(&runtime, name, None).expect("the registered query resolves");
        let direct = runtime.query(name).expect("the registered query resolves");
        assert_eq!(through_ipc, direct);
    }

    #[test]
    fn the_ipc_command_transport_forwards_bytes_and_effects() {
        let runtime = runtime();
        let directory = tempfile::tempdir().expect("tempdir");
        let path = directory.path().join("ipc.pos").display().to_string();
        let input = input_json(&ProjectCreateInput {
            path: path.clone(),
            name: Some("Ipc".to_owned()),
            template: "generic".to_owned(),
        })
        .expect("input serializes");
        let created = dispatch_command(&runtime, CommandName::ProjectCreate.as_str(), &input)
            .expect("create resolves over IPC");
        assert!(created.contains("\"headSeq\":1"));
        let open_input = input_json(&pos_api::ProjectPathInput { path }).expect("serializes");
        dispatch_command(&runtime, CommandName::ProjectOpen.as_str(), &open_input)
            .expect("open resolves over IPC");
        // The effect is visible through the input-bearing read surface.
        let listed = dispatch_query(&runtime, QueryName::ProjectList.as_str(), Some("{}"))
            .expect("project.list resolves");
        assert!(listed.contains("\"name\":\"Ipc\""));
    }

    #[test]
    fn an_unknown_query_reaches_the_webview_as_the_typed_envelope() {
        let runtime = runtime();
        let error = dispatch_query(&runtime, "capability.snapsh0t", None)
            .expect_err("an unregistered name must not resolve");
        assert!(error.contains("\"code\":\"unknown_query\""));
        assert!(error.contains("\"retriable\":false"));

        let error = dispatch_command(&runtime, "run.start", "{}")
            .expect_err("the run engine has not landed; success would be a lie");
        assert!(error.contains("\"code\":\"not_yet_supported\""));
    }
}
