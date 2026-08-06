//! # pos-server
//!
//! The web shell: axum server serving the apps/ui bundle and the pos-api HTTP+SSE transport; control.db (accounts, workspaces, RBAC), audit log.
//!
//! Stub created by m0-s01; real shell lands in m0-s08.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, QueryName, bootstrap_local_runtime};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let runtime = bootstrap_local_runtime(LocalBootstrapConfig::isolated(PathBuf::from("packs")));
    // No socket is opened here yet. m0-s08 brings axum, and with it the HTTP
    // and SSE transports; announcing a server that does not listen would be the
    // dishonesty the capability gates exist to prevent.
    match runtime.query(QueryName::CapabilitySnapshot.as_str()) {
        Ok(snapshot) => {
            println!("{snapshot}");
            eprintln!(
                "pos-server: registry resolved; the HTTP+SSE transport for /api/query/{} lands in m0-s08.",
                QueryName::CapabilitySnapshot.as_str()
            );
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("{}", error.to_json());
            ExitCode::FAILURE
        }
    }
}
