//! Thin Tauri v2 desktop boot shell (L12).
//!
//! The shell owns the native window and transport selection only. Domain logic
//! remains behind `pos-api`; m0-s07 adds dialogs, menus, packaging, and its boot
//! smoke without changing this cut.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, bootstrap_local_runtime};
use std::path::PathBuf;

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
        .run(tauri::generate_context!())
}
