//! Thin Tauri v2 desktop shell (L12): the Tauri process *is* the core — no
//! server, no network. The shell owns the native window, menus, tray, native
//! dialogs, and transport selection; domain logic stays behind `pos-api`.
//!
//! Lifecycle (m0-s07):
//! - **Single instance.** A second launch focuses the running window instead
//!   of opening a second writer against the same project directory — the
//!   single-writer discipline (§8) made a shell property, not a hope.
//! - **Graceful shutdown.** Project operations are short and each commits
//!   with `synchronous=FULL` before returning, so nothing durable waits in a
//!   buffer at exit. What shutdown must not do is kill a dispatch *mid*
//!   transaction, so close waits (bounded) for in-flight dispatches to drain.
//! - **Recent projects** live in the app config directory, never in a
//!   project (L4) — see `recents.rs`.
//!
//! The IPC transport cannot shape a result: it resolves a name through the
//! shared registry and returns the bytes it receives. Anything else would
//! make shell parity a matter of discipline instead of a checkable property.

#![forbid(unsafe_code)]

mod recents;

pub use recents::{RECENT_PROJECT_COUNT_MAX, Recents};

use pos_api::{
    CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, ProjectPathInput,
    QueryName, bootstrap_local_runtime, input_json,
};
use std::path::{Path, PathBuf};
use std::sync::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};
use tauri::Manager;
use tauri::menu::{MenuBuilder, MenuItemBuilder, SubmenuBuilder};
use tauri::tray::TrayIconBuilder;

/// How long close waits for in-flight dispatches before exiting anyway. A
/// dispatch that outlives this is either a bug or a pathological corpus; the
/// wait exists so a normal operation is never cut mid-transaction, not to
/// hold a window hostage.
const SHUTDOWN_DRAIN_MS_MAX: u64 = 5_000;
const SHUTDOWN_POLL_MS: u64 = 25;

/// Shell-owned state beside the runtime: the recents list and the in-flight
/// dispatch count that shutdown drains.
pub struct ShellState {
    config_dir: PathBuf,
    recents: Mutex<Recents>,
    in_flight: AtomicUsize,
}

/// Exercises the packaged executable's real project path without starting a
/// webview. The release smoke pairs this with a normal bundle launch: together
/// they prove both native boot and the statically linked FTS5/sqlite-vec path.
///
/// # Errors
///
/// Returns a stable diagnostic when the harness supplies an unsafe path, the
/// typed API refuses create/verify, or verification reports a dirty project.
pub fn packaging_smoke(project_root: &Path) -> Result<(), String> {
    if !project_root.is_absolute() {
        return Err("packaging smoke requires an absolute project path".to_owned());
    }
    let project_path = project_root
        .to_str()
        .ok_or_else(|| "packaging smoke project path is not valid UTF-8".to_owned())?;
    let pack_root = project_root
        .parent()
        .ok_or_else(|| "packaging smoke project path has no parent".to_owned())?
        .join("packs");
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(pack_root));

    let create_input = input_json(&ProjectCreateInput {
        path: project_path.to_owned(),
        name: Some("Packaging Smoke".to_owned()),
        template: "generic".to_owned(),
    })
    .map_err(|error| error.to_json())?;
    runtime
        .command(CommandName::ProjectCreate.as_str(), &create_input)
        .map_err(|error| error.to_json())?;

    let verify_input = input_json(&ProjectPathInput {
        path: project_path.to_owned(),
    })
    .map_err(|error| error.to_json())?;
    let report = runtime
        .query_with_input(QueryName::ProjectVerify.as_str(), &verify_input)
        .map_err(|error| error.to_json())?;
    let report: serde_json::Value = serde_json::from_str(&report)
        .map_err(|error| format!("packaging smoke verify report is invalid JSON: {error}"))?;
    if report.get("clean").and_then(serde_json::Value::as_bool) != Some(true) {
        return Err("packaging smoke project verification reported a defect".to_owned());
    }
    Ok(())
}

impl ShellState {
    #[must_use]
    pub fn new(config_dir: PathBuf) -> Self {
        let recents = Recents::load(&config_dir);
        Self {
            config_dir,
            recents: Mutex::new(recents),
            in_flight: AtomicUsize::new(0),
        }
    }

    /// Records a project the user just created or opened. A recents-write
    /// failure is reported, never fatal: losing the list must not lose the
    /// project operation that succeeded.
    pub fn record_recent(&self, path: &str) -> Option<String> {
        let mut recents = match self.recents.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        recents
            .record(&self.config_dir, std::path::Path::new(path))
            .err()
    }

    /// The project the shell restores on launch, as its JSON path string.
    #[must_use]
    pub fn last_open_json(&self) -> String {
        let recents = match self.recents.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        };
        let list: Vec<&str> = recents.paths.iter().map(String::as_str).collect();
        serde_json::json!({
            "lastOpen": recents.last_open().map(|path| path.display().to_string()),
            "recents": list,
            "recentProjectCountMax": RECENT_PROJECT_COUNT_MAX,
        })
        .to_string()
    }

    fn enter_dispatch(&self) -> DispatchGuard<'_> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        DispatchGuard { state: self }
    }

    /// Blocks until no dispatch is in flight, or the drain budget expires.
    /// Returns whether the drain completed — a false here is worth logging,
    /// because it means something outlived its expected duration.
    pub fn drain_in_flight(&self) -> bool {
        let deadline_polls = SHUTDOWN_DRAIN_MS_MAX / SHUTDOWN_POLL_MS;
        for _ in 0..deadline_polls {
            if self.in_flight.load(Ordering::SeqCst) == 0 {
                return true;
            }
            std::thread::sleep(std::time::Duration::from_millis(SHUTDOWN_POLL_MS));
        }
        self.in_flight.load(Ordering::SeqCst) == 0
    }
}

/// Decrements the in-flight count however the dispatch ends, including a
/// panic — a leaked count would hang every later shutdown.
struct DispatchGuard<'state> {
    state: &'state ShellState,
}

impl Drop for DispatchGuard<'_> {
    fn drop(&mut self) {
        self.state.in_flight.fetch_sub(1, Ordering::SeqCst);
    }
}

/// The one query dispatch path the IPC command and its tests share.
fn dispatch_query(
    runtime: &LocalRuntime,
    name: &str,
    input: Option<&str>,
) -> Result<String, String> {
    runtime
        .query_with_input(name, input.unwrap_or("{}"))
        .map_err(|error| error.to_json())
}

/// The one command dispatch path. Successful project create/open also
/// records the path in the shell's recents list — shell state derived from a
/// registry result, never a second source of truth about projects.
fn dispatch_command(
    runtime: &LocalRuntime,
    shell: &ShellState,
    name: &str,
    input: &str,
) -> Result<String, String> {
    let result = runtime
        .command(name, input)
        .map_err(|error| error.to_json());
    if result.is_ok()
        && (name == "project.create" || name == "project.open")
        && let Some(path) = path_from_input(input)
        && let Some(message) = shell.record_recent(&path)
    {
        eprintln!("pos-desktop: recents list not updated: {message}");
    }
    result
}

/// Reads the caller-supplied `path` field. The value is used only as a
/// recents entry — it never becomes a shell command or an unchecked path
/// operation (L6).
fn path_from_input(input_json: &str) -> Option<String> {
    serde_json::from_str::<serde_json::Value>(input_json)
        .ok()?
        .get("path")?
        .as_str()
        .map(str::to_owned)
}

#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_query(
    name: String,
    input: Option<String>,
    runtime: tauri::State<'_, LocalRuntime>,
    shell: tauri::State<'_, ShellState>,
) -> Result<String, String> {
    let _guard = shell.enter_dispatch();
    dispatch_query(runtime.inner(), &name, input.as_deref())
}

#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_command(
    name: String,
    input: String,
    runtime: tauri::State<'_, LocalRuntime>,
    shell: tauri::State<'_, ShellState>,
) -> Result<String, String> {
    let _guard = shell.enter_dispatch();
    dispatch_command(runtime.inner(), shell.inner(), &name, &input)
}

/// The shell surface the UI reads at startup to restore its last project.
#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri resolves managed state into an owned guard before invoking the command"
)]
fn shell_recents(shell: tauri::State<'_, ShellState>) -> String {
    shell.last_open_json()
}

/// Runs the native event loop and returns startup/runtime failures to `main`.
///
/// # Errors
///
/// Returns Tauri's typed error when configuration, webview startup, or the
/// native event loop fails.
#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() -> Result<(), tauri::Error> {
    tauri::Builder::default()
        .plugin(tauri_plugin_dialog::init())
        // Single instance must be registered first so a second launch is
        // rejected before it can touch any project directory.
        .plugin(tauri_plugin_single_instance::init(|app, _argv, _cwd| {
            if let Some(window) = app.get_webview_window("main") {
                let _ = window.unminimize();
                let _ = window.set_focus();
            }
        }))
        .setup(|app| {
            let config_dir = app
                .path()
                .app_config_dir()
                .unwrap_or_else(|_| PathBuf::from("."));
            let packs_root = app
                .path()
                .app_data_dir()
                .unwrap_or_else(|_| PathBuf::from("."))
                .join("packs");
            app.manage(bootstrap_local_runtime(LocalBootstrapConfig::isolated(
                packs_root,
            )));
            app.manage(ShellState::new(config_dir));
            install_menu(app)?;
            install_tray(app)?;
            Ok(())
        })
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { .. } = event {
                let shell = window.state::<ShellState>();
                if !shell.drain_in_flight() {
                    eprintln!(
                        "pos-desktop: a dispatch outlived the {SHUTDOWN_DRAIN_MS_MAX}ms shutdown \
                         drain; exiting anyway (its transaction either committed or rolled back — \
                         synchronous=FULL leaves no partial state)"
                    );
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            api_query,
            api_command,
            shell_recents
        ])
        .run(tauri::generate_context!())
}

/// App/edit/view menus with standard platform shortcuts. Menu items emit
/// events the UI handles, so the palette and the menu drive one code path.
fn install_menu(app: &tauri::App) -> Result<(), tauri::Error> {
    let handle = app.handle();
    let create = MenuItemBuilder::with_id("project.create", "New Project…")
        .accelerator("CmdOrCtrl+N")
        .build(handle)?;
    let open = MenuItemBuilder::with_id("project.open", "Open Project…")
        .accelerator("CmdOrCtrl+O")
        .build(handle)?;
    let palette = MenuItemBuilder::with_id("shell.palette", "Command Palette")
        .accelerator("CmdOrCtrl+K")
        .build(handle)?;
    let theme = MenuItemBuilder::with_id("shell.theme", "Toggle Theme").build(handle)?;

    let app_menu = SubmenuBuilder::new(handle, "ProjectOS")
        .item(&create)
        .item(&open)
        .separator()
        .quit()
        .build()?;
    let edit_menu = SubmenuBuilder::new(handle, "Edit")
        .undo()
        .redo()
        .separator()
        .cut()
        .copy()
        .paste()
        .select_all()
        .build()?;
    let view_menu = SubmenuBuilder::new(handle, "View")
        .item(&palette)
        .item(&theme)
        .build()?;
    let menu = MenuBuilder::new(handle)
        .items(&[&app_menu, &edit_menu, &view_menu])
        .build()?;
    app.set_menu(menu)?;
    app.on_menu_event(|app, event| {
        if let Some(window) = app.get_webview_window("main") {
            // The UI owns what each command does; the menu only names it, so
            // menu and palette cannot diverge.
            let _ = window.emit_shell_command(event.id().0.as_str());
        }
    });
    Ok(())
}

/// Tray with the background-work indicator stub. The indicator earns real
/// state when background work exists (cut line: → M2); today it is a static
/// presence marker and says so, rather than animating a lie.
fn install_tray(app: &tauri::App) -> Result<(), tauri::Error> {
    let quit = MenuItemBuilder::with_id("tray.quit", "Quit ProjectOS").build(app.handle())?;
    let menu = MenuBuilder::new(app.handle()).item(&quit).build()?;
    TrayIconBuilder::new()
        .tooltip("ProjectOS — no background work yet (the indicator lands with M2)")
        .menu(&menu)
        .on_menu_event(|app, event| {
            if event.id().0.as_str() == "tray.quit" {
                app.exit(0);
            }
        })
        .build(app)?;
    Ok(())
}

/// Emitting a menu selection to the webview, in one place.
trait EmitShellCommand {
    fn emit_shell_command(&self, id: &str) -> Result<(), tauri::Error>;
}

impl<R: tauri::Runtime> EmitShellCommand for tauri::WebviewWindow<R> {
    fn emit_shell_command(&self, id: &str) -> Result<(), tauri::Error> {
        tauri::Emitter::emit(self, "shell://command", id)
    }
}

#[cfg(test)]
mod tests {
    use super::{ShellState, dispatch_command, dispatch_query, packaging_smoke, path_from_input};
    use pos_api::{
        CommandName, LocalBootstrapConfig, LocalRuntime, ProjectCreateInput, ProjectPathInput,
        QueryName, bootstrap_local_runtime, input_json,
    };
    use std::path::PathBuf;

    fn runtime() -> LocalRuntime {
        bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from(
            "missing-desktop-pack-root",
        )))
    }

    #[test]
    fn packaging_smoke_creates_and_verifies_through_the_typed_surface() {
        let directory = tempfile::tempdir().expect("the test owns its temporary directory");
        let project_root = directory.path().join("packaging-smoke.pos");
        packaging_smoke(&project_root).expect("the packaged core path is healthy");
        assert!(project_root.join("project.db").is_file());
    }

    #[test]
    fn packaging_smoke_rejects_a_relative_path_before_state_changes() {
        let error = packaging_smoke(PathBuf::from("relative.pos").as_path())
            .expect_err("a harness path must be absolute");
        assert_eq!(error, "packaging smoke requires an absolute project path");
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

    /// The m0-s07 lifecycle AC, minus the webview: create a project through
    /// the same dispatch path the native dialog feeds, confirm it is a valid
    /// `.pos` directory by running the registry's own verify, and confirm a
    /// fresh shell state (a relaunch) restores it.
    #[test]
    fn create_then_relaunch_restores_a_verifiable_project() {
        let runtime = runtime();
        let config = tempfile::tempdir().expect("tempdir");
        let projects = tempfile::tempdir().expect("tempdir");
        let project = projects.path().join("desktop.pos");
        let path = project.display().to_string();
        let shell = ShellState::new(config.path().to_path_buf());

        let created = dispatch_command(
            &runtime,
            &shell,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Desktop".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves over IPC");
        assert!(created.contains("\"headSeq\":1"));

        // On disk and valid, checked by the registry rather than by asserting
        // file names.
        let verify = dispatch_query(
            &runtime,
            QueryName::ProjectVerify.as_str(),
            Some(&input_json(&ProjectPathInput { path: path.clone() }).expect("serializes")),
        )
        .expect("verify resolves");
        assert!(verify.contains("\"clean\":true"), "{verify}");

        // Relaunch: a fresh shell state reads the persisted config.
        let relaunched = ShellState::new(config.path().to_path_buf());
        let restored = relaunched.last_open_json();
        assert!(restored.contains(&path), "restore payload was {restored}");
        let reopened = dispatch_command(
            &runtime,
            &relaunched,
            CommandName::ProjectOpen.as_str(),
            &input_json(&ProjectPathInput { path }).expect("serializes"),
        )
        .expect("the restored project reopens");
        assert!(reopened.contains("\"name\":\"Desktop\""));
    }

    #[test]
    fn a_failed_command_records_no_recent_and_reaches_the_webview_typed() {
        let runtime = runtime();
        let config = tempfile::tempdir().expect("tempdir");
        let shell = ShellState::new(config.path().to_path_buf());

        let error = dispatch_command(
            &runtime,
            &shell,
            CommandName::ProjectOpen.as_str(),
            "{\"path\":\"missing-project-directory.pos\"}",
        )
        .expect_err("a missing directory must not open");
        assert!(error.contains("\"code\":\"not_a_project\""));
        assert!(
            !shell.last_open_json().contains("missing-project-directory"),
            "a failed open must not enter the recents list"
        );

        let error = dispatch_query(&runtime, "capability.snapsh0t", None)
            .expect_err("an unregistered name must not resolve");
        assert!(error.contains("\"code\":\"unknown_query\""));
        assert!(error.contains("\"retriable\":false"));
    }

    #[test]
    fn shutdown_drains_when_nothing_is_in_flight() {
        let config = tempfile::tempdir().expect("tempdir");
        let shell = ShellState::new(config.path().to_path_buf());
        assert!(shell.drain_in_flight());
        {
            let _guard = shell.enter_dispatch();
            // The guard releases on drop even if the dispatch panics.
        }
        assert!(shell.drain_in_flight());
    }

    #[test]
    fn only_a_string_path_becomes_a_recents_entry() {
        assert_eq!(
            path_from_input("{\"path\":\"/tmp/x.pos\"}").as_deref(),
            Some("/tmp/x.pos")
        );
        assert_eq!(path_from_input("{\"path\":42}"), None);
        assert_eq!(path_from_input("not json"), None);
    }
}
