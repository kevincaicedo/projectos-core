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
    QueryName, SSE_RETRY_MS, StreamFrame, WorkerConfig, bootstrap_local_runtime, input_json,
};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use tauri::Manager;
use tauri::ipc::Channel;
use tauri::menu::{MenuBuilder, MenuItemBuilder, SubmenuBuilder};
use tauri::tray::TrayIconBuilder;

/// Set by the `pos-bench` cold-start scenario (m0-s16). Present only when a
/// measurement asked for it, so the shipped app has no probe behaviour.
pub const STARTUP_PROBE_ENV: &str = "POS_STARTUP_PROBE";

/// The single line the probe prints, parsed by `pos-bench`. A stable marker
/// beats scraping a log format.
pub const STARTUP_PROBE_MARKER: &str = "pos-desktop: startup_probe_ms";

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
    in_flight: Arc<AtomicUsize>,
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
            in_flight: Arc::new(AtomicUsize::new(0)),
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

    fn enter_dispatch(&self) -> DispatchGuard {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        DispatchGuard {
            in_flight: Arc::clone(&self.in_flight),
        }
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
struct DispatchGuard {
    in_flight: Arc<AtomicUsize>,
}

impl Drop for DispatchGuard {
    fn drop(&mut self) {
        self.in_flight.fetch_sub(1, Ordering::SeqCst);
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

fn dispatch_stream(
    runtime: Arc<LocalRuntime>,
    name: String,
    input: String,
    resume_after: Option<u64>,
    channel: Channel<String>,
    guard: DispatchGuard,
) -> Result<(), String> {
    let frames = runtime
        .stream_subscribe(&name, &input, resume_after)
        .map_err(|error| error.to_json())?;
    let tail_cursor = frames
        .last()
        .map_or(resume_after, |frame| Some(frame.stream_seq));
    std::thread::Builder::new()
        .name("pos-desktop-run-stream".to_owned())
        .spawn(move || {
            let _guard = guard;
            if channel.send(format!("retry: {SSE_RETRY_MS}\n\n")).is_err() {
                return;
            }
            for frame in frames {
                if !send_stream_frame(&channel, &frame) {
                    return;
                }
            }
            let followed = runtime.stream_follow(&name, &input, tail_cursor, |frame| {
                send_stream_frame(&channel, &frame)
            });
            if let Err(error) = followed {
                let _ = channel.send(format!(
                    "event: stream.error\ndata: {}\n\n",
                    error.to_json()
                ));
            }
        })
        .map(|_| ())
        .map_err(|error| format!("start desktop Run stream: {error}"))
}

fn send_stream_frame(channel: &Channel<String>, frame: &StreamFrame) -> bool {
    channel.send(frame.to_sse()).is_ok()
}

#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_query(
    name: String,
    input: Option<String>,
    runtime: tauri::State<'_, Arc<LocalRuntime>>,
    shell: tauri::State<'_, ShellState>,
) -> Result<String, String> {
    let _guard = shell.enter_dispatch();
    dispatch_query(runtime.inner().as_ref(), &name, input.as_deref())
}

#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments into owned values before invoking the command"
)]
fn api_command(
    name: String,
    input: String,
    runtime: tauri::State<'_, Arc<LocalRuntime>>,
    shell: tauri::State<'_, ShellState>,
) -> Result<String, String> {
    let _guard = shell.enter_dispatch();
    dispatch_command(runtime.inner().as_ref(), shell.inner(), &name, &input)
}

#[tauri::command]
#[allow(
    clippy::needless_pass_by_value,
    reason = "Tauri deserializes IPC arguments and the channel into owned values"
)]
fn api_stream(
    name: String,
    input: String,
    resume_after: Option<u64>,
    channel: Channel<String>,
    runtime: tauri::State<'_, Arc<LocalRuntime>>,
    shell: tauri::State<'_, ShellState>,
) -> Result<(), String> {
    dispatch_stream(
        Arc::clone(runtime.inner()),
        name,
        input,
        resume_after,
        channel,
        shell.enter_dispatch(),
    )
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
    let launched = std::time::Instant::now();
    // Telemetry is opt-in and **off by default on desktop** (L4 spirit): a
    // local-only project produces zero bytes outside the machine unless its
    // owner asked for them. A spec we cannot honour stops the shell rather
    // than starting with export silently disabled.
    if let Err(error) = pos_api::install_telemetry(std::env::var("POS_TELEMETRY").ok().as_deref()) {
        eprintln!("pos-desktop: {}", error.to_json());
        let boxed: Box<dyn std::error::Error> = Box::new(error);
        return Err(tauri::Error::Setup(boxed.into()));
    }
    let startup_probe = std::env::var_os(STARTUP_PROBE_ENV).is_some();
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
            let mut runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(packs_root));
            // The desktop app is the long-running shell, so it is the one that
            // actually runs the pipeline: ingestion advances while the window
            // is open, and `project.open` registers each project with the pool
            // (m1-s01/ADR-0007). Failing to start is fatal rather than silent —
            // a shell that queues work nothing claims looks identical to one
            // that is merely slow.
            runtime.start_background_workers(WorkerConfig::default())?;
            app.manage(Arc::new(runtime));
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
                // Then the background pool: stop claiming, and let whatever is
                // mid-handler finish. Queued work is not waited for — it is a
                // durable fact in the project and resumes on the next open.
                let runtime = window.state::<Arc<LocalRuntime>>();
                if !runtime.shutdown_background_workers() {
                    eprintln!(
                        "pos-desktop: a background job outlived the shutdown budget; exiting \
                         anyway (nothing terminal was written, so its lease expires and the \
                         attempt is re-counted from durable facts on the next open)"
                    );
                }
            }
        })
        .invoke_handler(tauri::generate_handler![
            api_query,
            api_command,
            api_stream,
            shell_recents
        ])
        .build(tauri::generate_context!())?
        .run(move |handle, event| {
            // The `pos-bench` cold-start probe (m0-s16): `Ready` is the first
            // instant the native window, its webview, and the in-process core
            // runtime all exist, which is the shell half of "time to
            // interactive". The UI half is measured in the page, and the gate
            // adds them as a stated upper bound — the two phases overlap in
            // reality, so their sum can only overstate the real cold start.
            if startup_probe && matches!(event, tauri::RunEvent::Ready) {
                let elapsed_ms = u64::try_from(launched.elapsed().as_millis()).unwrap_or(u64::MAX); // INVARIANT: saturation, matching the telemetry clock policy.
                println!("{STARTUP_PROBE_MARKER} {elapsed_ms}");
                handle.exit(0);
            }
        });
    Ok(())
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
    use super::{
        ShellState, dispatch_command, dispatch_query, dispatch_stream, packaging_smoke,
        path_from_input,
    };
    use pos_api::{
        CommandName, CostRollupInput, EchoRuntimeOptions, LocalBootstrapConfig, LocalRuntime,
        ProjectCreateInput, ProjectId, ProjectPathInput, QueryName, RunBudgetWire, RunControlInput,
        RunId, RunStartInput, RunStepsInput, RunWorker, StreamName, bootstrap_local_runtime,
        input_json, telemetry,
    };
    use std::io::{Read, Write};
    use std::net::{TcpListener, TcpStream};
    use std::path::PathBuf;
    use std::sync::Arc;
    use std::time::Duration;
    use tauri::ipc::Channel;

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

    /// The desktop is the long-running shell, so it is the one that actually
    /// runs the pipeline: opening a project through the IPC dispatch path must
    /// register it with the pool, closing must release it, and shutdown must
    /// stop it inside the budget (m1-s01/ADR-0007).
    #[test]
    fn the_shell_runs_background_work_and_releases_projects_on_close() {
        let directory = tempfile::tempdir().expect("tempdir");
        let config = tempfile::tempdir().expect("config tempdir");
        let path = directory
            .path()
            .join("desktop-workers.pos")
            .display()
            .to_string();
        let mut runtime = runtime();
        runtime
            .start_background_workers(pos_api::WorkerConfig::default())
            .expect("the desktop pool starts");
        let shell = ShellState::new(config.path().to_path_buf());
        dispatch_command(
            &runtime,
            &shell,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Desktop Workers".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("input serializes"),
        )
        .expect("create resolves");
        dispatch_command(
            &runtime,
            &shell,
            CommandName::ProjectOpen.as_str(),
            &input_json(&ProjectPathInput { path: path.clone() }).expect("input serializes"),
        )
        .expect("open resolves");
        let opened =
            dispatch_query(&runtime, QueryName::Health.as_str(), None).expect("health resolves");
        assert!(opened.contains("\"running\":true"), "{opened}");
        assert!(opened.contains("\"registeredProjectCount\":1"), "{opened}");

        dispatch_command(
            &runtime,
            &shell,
            CommandName::ProjectClose.as_str(),
            &input_json(&ProjectPathInput { path }).expect("input serializes"),
        )
        .expect("close resolves");
        let closed =
            dispatch_query(&runtime, QueryName::Health.as_str(), None).expect("health resolves");
        assert!(closed.contains("\"registeredProjectCount\":0"), "{closed}");
        assert!(
            runtime.shutdown_background_workers(),
            "the pool must stop inside the shutdown budget"
        );
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
    fn the_tauri_channel_forwards_the_exact_durable_echo_frame_bytes() {
        let (base_url, model_thread) = echo_endpoint();
        let directory = tempfile::tempdir().expect("tempdir");
        let config = tempfile::tempdir().expect("config tempdir");
        let path = directory
            .path()
            .join("echo-channel.pos")
            .display()
            .to_string();
        let runtime = Arc::new(bootstrap_local_runtime(
            LocalBootstrapConfig::isolated(directory.path().join("packs")).with_echo(
                EchoRuntimeOptions::loopback(base_url, "echo-desktop-fixture"),
            ),
        ));
        let shell = ShellState::new(config.path().to_path_buf());
        dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Echo channel".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create project over IPC path");
        let started = dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::RunStart.as_str(),
            &input_json(&RunStartInput {
                path: path.clone(),
                worker: RunWorker::Echo,
                autonomy_level: 2,
                budget: echo_budget(),
                tool_grants: Vec::new(),
                parent_run_id: None,
            })
            .expect("Run input serializes"),
        )
        .expect("start Echo over IPC path");
        let run_id = serde_json::from_str::<serde_json::Value>(&started)
            .expect("Run report parses")
            .get("runId")
            .and_then(serde_json::Value::as_str)
            .expect("Run report has runId")
            .to_owned();
        let stream_input = input_json(&RunStepsInput {
            path: path.clone(),
            run_id,
        })
        .expect("stream input serializes");
        let (message_tx, message_rx) = std::sync::mpsc::channel::<String>();
        let channel = Channel::<String>::new(move |body| {
            if let Ok(message) = body.deserialize::<String>() {
                let _ = message_tx.send(message);
            }
            Ok(())
        });
        dispatch_stream(
            Arc::clone(&runtime),
            StreamName::RunSteps.as_str().to_owned(),
            stream_input.clone(),
            None,
            channel,
            shell.enter_dispatch(),
        )
        .expect("Tauri channel stream starts");

        let mut channel_frames = Vec::new();
        loop {
            let message = message_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("channel receives the terminal Echo feed");
            if message.starts_with("id:") {
                let terminal = message.contains("\"terminal\":true");
                channel_frames.push(message);
                if terminal {
                    break;
                }
            }
        }
        assert_eq!(channel_frames.len(), 3);
        let durable = runtime
            .stream_subscribe(StreamName::RunSteps.as_str(), &stream_input, None)
            .expect("read durable frames")
            .into_iter()
            .map(|frame| frame.to_sse())
            .collect::<Vec<_>>();
        assert_eq!(channel_frames, durable);
        assert_one_echo_cost(runtime.as_ref(), &path);
        model_thread.join().expect("Echo fixture exits");
        assert!(shell.drain_in_flight());
    }

    /// m0-s15 AC 1, desktop half: one Echo Run produces a single connected
    /// span tree. The trace id is *derived* from the durable project and Run
    /// ids, so the assertion computes its own key rather than scraping one
    /// out of output — which is also why the worker thread's steps join the
    /// tree the command opened without anything being handed between threads.
    #[test]
    fn one_echo_run_produces_a_single_connected_span_tree_on_desktop() {
        let captured = telemetry::capture_any();
        let (base_url, model_thread) = echo_endpoint();
        let directory = tempfile::tempdir().expect("tempdir");
        let config = tempfile::tempdir().expect("config tempdir");
        let path = directory
            .path()
            .join("echo-spans.pos")
            .display()
            .to_string();
        let runtime = Arc::new(bootstrap_local_runtime(
            LocalBootstrapConfig::isolated(directory.path().join("packs")).with_echo(
                EchoRuntimeOptions::loopback(base_url, "echo-desktop-span-fixture"),
            ),
        ));
        let shell = ShellState::new(config.path().to_path_buf());
        dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Echo spans".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create project over IPC path");
        let started = dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::RunStart.as_str(),
            &input_json(&RunStartInput {
                path: path.clone(),
                worker: RunWorker::Echo,
                autonomy_level: 2,
                budget: echo_budget(),
                tool_grants: Vec::new(),
                parent_run_id: None,
            })
            .expect("Run input serializes"),
        )
        .expect("start Echo over IPC path");
        let report: serde_json::Value = serde_json::from_str(&started).expect("Run report parses");
        let run_id = report["runId"].as_str().expect("Run report has runId");
        let project_id = report["projectId"]
            .as_str()
            .expect("Run report has projectId");
        let trace = telemetry::TraceId::for_run(
            ProjectId::from_hex(project_id).expect("project id is hex"),
            RunId::from_hex(run_id).expect("Run id is hex"),
        );

        // Drain the durable feed so the worker has finished all three steps.
        let stream_input = input_json(&RunStepsInput {
            path: path.clone(),
            run_id: run_id.to_owned(),
        })
        .expect("stream input serializes");
        let (message_tx, message_rx) = std::sync::mpsc::channel::<String>();
        let channel = Channel::<String>::new(move |body| {
            if let Ok(message) = body.deserialize::<String>() {
                let _ = message_tx.send(message);
            }
            Ok(())
        });
        dispatch_stream(
            Arc::clone(&runtime),
            StreamName::RunSteps.as_str().to_owned(),
            stream_input,
            None,
            channel,
            shell.enter_dispatch(),
        )
        .expect("Tauri channel stream starts");
        loop {
            let message = message_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("channel receives the terminal Echo feed");
            if message.starts_with("id:") && message.contains("\"terminal\":true") {
                break;
            }
        }
        model_thread.join().expect("Echo fixture exits");
        assert!(shell.drain_in_flight());

        captured
            .assert_single_connected_tree(trace)
            .expect("the Echo Run is one connected tree");
        let spans = captured.spans_in(trace);
        let root = captured.root(trace).expect("the trace has a root");
        assert_eq!(root.taxonomy_name(), "api.cmd/run.start");
        assert_eq!(root.outcome(), Some("ok"));
        let steps: Vec<&telemetry::FinishedSpan> = spans
            .iter()
            .filter(|span| span.name == telemetry::SpanName::AgentsStep)
            .collect();
        assert_eq!(steps.len(), 3, "Echo has exactly three tool boundaries");
        assert!(
            steps
                .iter()
                .all(|step| step.parent == Some(root.span) && step.outcome() == Some("ok"))
        );
        let gateway: Vec<&telemetry::FinishedSpan> = spans
            .iter()
            .filter(|span| span.name == telemetry::SpanName::GatewayCall)
            .collect();
        assert_eq!(gateway.len(), 1, "Echo makes exactly one model call");
        assert_eq!(gateway[0].taxonomy_name(), "gateway.call/openai-compatible");
        // The model call nests under the step that asked for it, not under
        // the command — that nesting is the whole point of the tree.
        assert!(
            steps
                .iter()
                .any(|step| Some(step.span) == gateway[0].parent)
        );
    }

    #[test]
    fn desktop_cancel_lands_after_the_blocked_model_checkpoint() {
        let (base_url, requested, release, model_thread) = blocking_echo_endpoint();
        let directory = tempfile::tempdir().expect("tempdir");
        let config = tempfile::tempdir().expect("config tempdir");
        let path = directory
            .path()
            .join("echo-cancel-channel.pos")
            .display()
            .to_string();
        let runtime = Arc::new(bootstrap_local_runtime(
            LocalBootstrapConfig::isolated(directory.path().join("packs")).with_echo(
                EchoRuntimeOptions::loopback(base_url, "echo-desktop-cancel-fixture"),
            ),
        ));
        let shell = ShellState::new(config.path().to_path_buf());
        dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::ProjectCreate.as_str(),
            &input_json(&ProjectCreateInput {
                path: path.clone(),
                name: Some("Echo desktop cancel".to_owned()),
                template: "generic".to_owned(),
            })
            .expect("create input serializes"),
        )
        .expect("create project over IPC path");
        let started = dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::RunStart.as_str(),
            &input_json(&RunStartInput {
                path: path.clone(),
                worker: RunWorker::Echo,
                autonomy_level: 2,
                budget: echo_budget(),
                tool_grants: Vec::new(),
                parent_run_id: None,
            })
            .expect("Run input serializes"),
        )
        .expect("start Echo over IPC path");
        let run_id = serde_json::from_str::<serde_json::Value>(&started)
            .expect("Run report parses")["runId"]
            .as_str()
            .expect("Run report has runId")
            .to_owned();
        requested
            .recv_timeout(Duration::from_secs(10))
            .expect("Echo reaches the blocked model effect");

        let stream_input = input_json(&RunStepsInput {
            path: path.clone(),
            run_id: run_id.clone(),
        })
        .expect("stream input serializes");
        let (message_tx, message_rx) = std::sync::mpsc::channel::<String>();
        let channel = Channel::<String>::new(move |body| {
            if let Ok(message) = body.deserialize::<String>() {
                let _ = message_tx.send(message);
            }
            Ok(())
        });
        dispatch_stream(
            Arc::clone(&runtime),
            StreamName::RunSteps.as_str().to_owned(),
            stream_input.clone(),
            None,
            channel,
            shell.enter_dispatch(),
        )
        .expect("Tauri channel stream starts");
        let first = loop {
            let message = message_rx
                .recv_timeout(Duration::from_secs(10))
                .expect("preflight frame streams while the model is blocked");
            if message.starts_with("id:") {
                break message;
            }
        };
        assert!(first.contains("\"streamSeq\":1"));

        let pending = dispatch_command(
            runtime.as_ref(),
            &shell,
            CommandName::RunCancel.as_str(),
            &input_json(&RunControlInput {
                path: path.clone(),
                run_id,
                reason: "Desktop cancellation oracle".to_owned(),
            })
            .expect("cancel input serializes"),
        )
        .expect("cancel appends over IPC path");
        assert!(pending.contains("\"pendingControl\":\"cancel\""));
        release.send(()).expect("release model fixture");

        let terminal = message_rx
            .recv_timeout(Duration::from_secs(10))
            .expect("canceled checkpoint streams");
        assert!(terminal.contains("\"streamSeq\":2"));
        assert!(terminal.contains("\"runStatus\":\"canceled\""));
        assert!(terminal.contains("\"terminal\":true"));
        let durable = runtime
            .stream_subscribe(StreamName::RunSteps.as_str(), &stream_input, None)
            .expect("read durable canceled frames")
            .into_iter()
            .map(|frame| frame.to_sse())
            .collect::<Vec<_>>();
        assert_eq!(durable, vec![first, terminal]);
        assert_one_echo_cost(runtime.as_ref(), &path);
        model_thread.join().expect("Echo fixture exits");
        assert!(shell.drain_in_flight());
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

    const fn echo_budget() -> RunBudgetWire {
        RunBudgetWire {
            tokens: 4_096,
            usd_micros: 0,
            wall_ms: 90_000,
            storage_bytes: 64 * 1_024,
            tool_calls: 3,
            retries: 0,
            steps: 3,
        }
    }

    fn assert_one_echo_cost(runtime: &LocalRuntime, path: &str) {
        let input = input_json(&CostRollupInput {
            path: Some(path.to_owned()),
        })
        .expect("cost input serializes");
        let cost = dispatch_query(runtime, QueryName::CostRollup.as_str(), Some(&input))
            .expect("desktop cost rollup reads");
        let cost: serde_json::Value = serde_json::from_str(&cost).expect("cost report parses");
        assert_eq!(cost["totals"]["calls"], 1);
        assert_eq!(cost["rows"][0]["feature"], "echo");
        assert_eq!(cost["rows"][0]["agent"], "echo");
    }

    fn echo_endpoint() -> (String, std::thread::JoinHandle<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Echo fixture binds");
        let address = listener.local_addr().expect("Echo fixture has address");
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Echo worker connects");
            let marker = read_echo_marker(&mut stream);
            let delta = serde_json::json!({
                "choices": [{"delta": {"content": format!("ECHO: {marker}")}}]
            });
            let usage = serde_json::json!({
                "choices": [],
                "usage": {"prompt_tokens": 7, "completion_tokens": 3}
            });
            let body = format!("data: {delta}\n\ndata: {usage}\n\ndata: [DONE]\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write Echo response");
        });
        (format!("http://{address}"), thread)
    }

    fn blocking_echo_endpoint() -> (
        String,
        std::sync::mpsc::Receiver<()>,
        std::sync::mpsc::Sender<()>,
        std::thread::JoinHandle<()>,
    ) {
        let listener = TcpListener::bind("127.0.0.1:0").expect("Echo fixture binds");
        let address = listener.local_addr().expect("Echo fixture has address");
        let (requested_tx, requested) = std::sync::mpsc::channel();
        let (release, release_rx) = std::sync::mpsc::channel();
        let thread = std::thread::spawn(move || {
            let (mut stream, _) = listener.accept().expect("Echo worker connects");
            let marker = read_echo_marker(&mut stream);
            requested_tx.send(()).expect("test waits for request");
            release_rx.recv().expect("test releases response");
            let delta = serde_json::json!({
                "choices": [{"delta": {"content": format!("ECHO: {marker}")}}]
            });
            let usage = serde_json::json!({
                "choices": [],
                "usage": {"prompt_tokens": 7, "completion_tokens": 3}
            });
            let body = format!("data: {delta}\n\ndata: {usage}\n\ndata: [DONE]\n\n");
            let response = format!(
                "HTTP/1.1 200 OK\r\ncontent-type: text/event-stream\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
                body.len()
            );
            stream
                .write_all(response.as_bytes())
                .expect("write Echo response");
        });
        (format!("http://{address}"), requested, release, thread)
    }

    fn read_echo_marker(stream: &mut TcpStream) -> String {
        let mut bytes = Vec::new();
        let mut chunk = [0_u8; 1_024];
        let header_end = loop {
            let read = stream.read(&mut chunk).expect("read Echo request");
            assert!(read > 0, "Echo request closed before headers");
            bytes.extend_from_slice(&chunk[..read]);
            if let Some(index) = bytes.windows(4).position(|window| window == b"\r\n\r\n") {
                break index + 4;
            }
        };
        let headers = String::from_utf8_lossy(&bytes[..header_end]);
        let content_length = headers
            .lines()
            .find_map(|line| {
                let (name, value) = line.split_once(':')?;
                name.eq_ignore_ascii_case("content-length")
                    .then(|| value.trim().parse::<usize>().ok())
                    .flatten()
            })
            .expect("Echo request has content-length");
        while bytes.len() - header_end < content_length {
            let read = stream.read(&mut chunk).expect("read Echo body");
            assert!(read > 0, "Echo request closed before body");
            bytes.extend_from_slice(&chunk[..read]);
        }
        let request: serde_json::Value =
            serde_json::from_slice(&bytes[header_end..header_end + content_length])
                .expect("Echo request parses");
        request["messages"]
            .as_array()
            .and_then(|messages| messages.last())
            .and_then(|message| message["content"].as_str())
            .expect("Echo marker exists")
            .to_owned()
    }
}
