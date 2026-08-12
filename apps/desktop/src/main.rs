//! Native process entry point for the thin Tauri desktop shell.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![forbid(unsafe_code)]

use std::process::ExitCode;

#[cfg(debug_assertions)]
mod chaos_child;

fn main() -> ExitCode {
    let mut arguments = std::env::args_os().skip(1);
    let first = arguments.next();
    #[cfg(debug_assertions)]
    if first.as_deref() == Some(std::ffi::OsStr::new("--echo-chaos-child")) {
        return chaos_child::run(arguments);
    }
    if first.as_deref() == Some(std::ffi::OsStr::new("--packaging-smoke")) {
        let Some(project_root) = arguments.next() else {
            eprintln!("usage: pos-desktop --packaging-smoke <absolute-project-path>");
            return ExitCode::FAILURE;
        };
        if arguments.next().is_some() {
            eprintln!("usage: pos-desktop --packaging-smoke <absolute-project-path>");
            return ExitCode::FAILURE;
        }
        return match pos_desktop_lib::packaging_smoke(std::path::Path::new(&project_root)) {
            Ok(()) => {
                println!("packaging-smoke: native project create and verify are clean");
                ExitCode::SUCCESS
            }
            Err(error) => {
                eprintln!("ProjectOS packaging smoke failed: {error}");
                ExitCode::FAILURE
            }
        };
    }
    match pos_desktop_lib::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ProjectOS desktop failed to run: {error}");
            ExitCode::FAILURE
        }
    }
}
