//! # pos-server
//!
//! The web shell binary: serves the apps/ui bundle and the pos-api HTTP+SSE
//! transport behind auth v0, control.db RBAC, and the audit log (m0-s08).
//! Composition lives in the library half (`web.rs`); this file is
//! configuration, the listener, and signals.

#![forbid(unsafe_code)]

use pos_server::web::{ServerConfig, ServerState, serve};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::process::ExitCode;

/// Loopback by default: TLS termination and non-local exposure are a reverse
/// proxy's job (deploy story, with Litestream for control.db — documented,
/// not built, in M0).
const BIND_ADDR_DEFAULT: &str = "127.0.0.1:7420";

/// Server state lives here unless overridden; one directory holds control.db
/// and every workspace's project directories, so backup is one path.
const DATA_ROOT_DEFAULT: &str = "pos-server-data";

fn main() -> ExitCode {
    let bind_addr: SocketAddr = match env_or("POS_SERVER_ADDR", BIND_ADDR_DEFAULT).parse() {
        Ok(addr) => addr,
        Err(error) => {
            eprintln!("pos-server: POS_SERVER_ADDR is not a socket address: {error}");
            return ExitCode::FAILURE;
        }
    };
    let config = ServerConfig {
        data_root: PathBuf::from(env_or("POS_SERVER_DATA_DIR", DATA_ROOT_DEFAULT)),
        ui_dist: std::env::var_os("POS_SERVER_UI_DIST")
            .map(PathBuf::from)
            .or_else(|| {
                let default_dist = PathBuf::from("apps/ui/dist");
                default_dist.is_dir().then_some(default_dist)
            }),
    };
    let state = match ServerState::initialize(&config) {
        Ok(state) => state,
        Err(error) => {
            eprintln!("pos-server: {}", error.to_json());
            return ExitCode::FAILURE;
        }
    };
    let tokio_runtime = match tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
    {
        Ok(built) => built,
        Err(error) => {
            eprintln!("pos-server: tokio runtime: {error}");
            return ExitCode::FAILURE;
        }
    };
    let served = tokio_runtime.block_on(async move {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|error| format!("bind {bind_addr}: {error}"))?;
        let local = listener
            .local_addr()
            .map_err(|error| format!("local addr: {error}"))?;
        eprintln!("pos-server: serving on http://{local} (auth v0, audit on)");
        serve(listener, state, shutdown_signal())
            .await
            .map_err(|error| format!("serve: {error}"))
    });
    match served {
        Ok(()) => {
            eprintln!("pos-server: shut down cleanly");
            ExitCode::SUCCESS
        }
        Err(message) => {
            eprintln!("pos-server: {message}");
            ExitCode::FAILURE
        }
    }
}

fn env_or(name: &str, fallback: &str) -> String {
    std::env::var(name).unwrap_or_else(|_| fallback.to_owned())
}

/// Resolves on Ctrl-C/SIGTERM; axum then drains in-flight dispatch. A signal
/// wiring failure aborts startup loudly rather than leaving an unstoppable
/// process.
async fn shutdown_signal() {
    let interrupted = async {
        if let Err(error) = tokio::signal::ctrl_c().await {
            eprintln!("pos-server: ctrl-c handler failed: {error}; shutting down");
        }
    };
    #[cfg(unix)]
    {
        let mut terminate =
            match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
                Ok(stream) => stream,
                Err(error) => {
                    eprintln!("pos-server: SIGTERM handler failed: {error}; shutting down");
                    return;
                }
            };
        tokio::select! {
            () = interrupted => {}
            _ = terminate.recv() => {}
        }
    }
    #[cfg(not(unix))]
    interrupted.await;
}
