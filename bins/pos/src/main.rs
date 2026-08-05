//! # pos
//!
//! The CLI shell: create / open / inspect / verify / export over pos-api — no lock-in from the first commit (F45, L4).
//!
//! Stub created by m0-s01; real shell lands in m0-s05.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, bootstrap_local_runtime};
use std::path::PathBuf;

fn main() {
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    println!(
        "pos: {} capability providers resolved; CLI v0 lands in m0-s05 (create/inspect/verify/export).",
        runtime.capability_count()
    );
}
