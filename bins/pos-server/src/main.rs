//! # pos-server
//!
//! The web shell: axum server serving the apps/ui bundle and the pos-api
//! HTTP+SSE transport; control.db (accounts, workspaces, RBAC), audit log.
//!
//! m0-s06 lands the API transport: this binary binds a listener and serves
//! the `pos-api` router — thin dispatch, zero logic (L12). Static assets,
//! auth v0, control.db, and the audit log land in m0-s08.

#![forbid(unsafe_code)]

use pos_api::{LocalBootstrapConfig, bootstrap_local_runtime};
use std::net::SocketAddr;
use std::process::ExitCode;
use std::sync::Arc;

/// Loopback by default: without auth (m0-s08) this process must not be
/// reachable from another machine. The env override exists for CI harnesses,
/// not for deployment.
const BIND_ADDR_DEFAULT: &str = "127.0.0.1:7420";

fn main() -> ExitCode {
    let bind_addr = match resolve_bind_addr() {
        Ok(addr) => addr,
        Err(message) => {
            eprintln!("pos-server: {message}");
            return ExitCode::FAILURE;
        }
    };
    let runtime = Arc::new(bootstrap_local_runtime(LocalBootstrapConfig::isolated(
        "packs".into(),
    )));
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
        eprintln!("pos-server: serving the pos-api transport on http://{local}");
        pos_api::http::serve(listener, runtime, shutdown_signal())
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

fn resolve_bind_addr() -> Result<SocketAddr, String> {
    let text = std::env::var("POS_SERVER_ADDR").unwrap_or_else(|_| BIND_ADDR_DEFAULT.to_owned());
    text.parse()
        .map_err(|error| format!("POS_SERVER_ADDR {text:?} is not a socket address: {error}"))
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
