//! Native process entry point for the thin Tauri desktop shell.

#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]
#![forbid(unsafe_code)]

use std::process::ExitCode;

fn main() -> ExitCode {
    match pos_desktop_lib::run() {
        Ok(()) => ExitCode::SUCCESS,
        Err(error) => {
            eprintln!("ProjectOS desktop failed to run: {error}");
            ExitCode::FAILURE
        }
    }
}
