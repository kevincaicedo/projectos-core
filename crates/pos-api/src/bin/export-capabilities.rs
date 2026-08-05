//! Deterministic pos-api-to-TypeScript capability catalog exporter (m0-s17).

#![forbid(unsafe_code)]

use pos_api::typescript_capability_catalog;
use std::env;
use std::fs;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let Some(mode) = args.next() else {
        eprintln!("usage: export-capabilities --check|--write <output-path>");
        return ExitCode::FAILURE;
    };
    let Some(path) = args.next() else {
        eprintln!("export-capabilities: output path is required");
        return ExitCode::FAILURE;
    };
    if args.next().is_some() {
        eprintln!("export-capabilities: too many arguments");
        return ExitCode::FAILURE;
    }
    let path = PathBuf::from(path);
    let generated = typescript_capability_catalog();
    match mode.to_str() {
        Some("--check") => check(&path, &generated),
        Some("--write") => write(&path, &generated),
        _ => {
            eprintln!("export-capabilities: mode must be --check or --write");
            ExitCode::FAILURE
        }
    }
}

fn check(path: &PathBuf, generated: &str) -> ExitCode {
    match fs::read_to_string(path) {
        Ok(current) if current == generated => {
            println!("export-capabilities: generated UI catalog is current");
            ExitCode::SUCCESS
        }
        Ok(_) => {
            eprintln!(
                "export-capabilities: {} is stale; run `just generate-capabilities`",
                path.display()
            );
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("export-capabilities: read {}: {error}", path.display());
            ExitCode::FAILURE
        }
    }
}

fn write(path: &PathBuf, generated: &str) -> ExitCode {
    if let Some(parent) = path.parent()
        && let Err(error) = fs::create_dir_all(parent)
    {
        eprintln!("export-capabilities: create {}: {error}", parent.display());
        return ExitCode::FAILURE;
    }
    match fs::write(path, generated) {
        Ok(()) => {
            println!("export-capabilities: wrote {}", path.display());
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("export-capabilities: write {}: {error}", path.display());
            ExitCode::FAILURE
        }
    }
}
