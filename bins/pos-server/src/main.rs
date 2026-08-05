//! # pos-server
//!
//! The web shell: axum server serving the apps/ui bundle and the pos-api HTTP+SSE transport; control.db (accounts, workspaces, RBAC), audit log.
//!
//! Stub created by m0-s01; real shell lands in m0-s08.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, bootstrap_local_runtime};
use std::path::PathBuf;

fn main() {
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    println!(
        "pos-server: {} capability providers resolved; walking-skeleton web transport lands in m0-s08.",
        runtime.capability_count()
    );
}
