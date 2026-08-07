//! Generates (or staleness-checks) the ts-rs API types under
//! `apps/ui/src/api/gen/api/` (m0-s06). Same contract as
//! `export-capabilities`: `--write` regenerates, `--check` fails naming every
//! stale, missing, or orphaned file without touching the checkout.

#![forbid(unsafe_code)]

use pos_api::{check_typescript_api, write_typescript_api};
use std::env;
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let (Some(mode), Some(path), None) = (args.next(), args.next(), args.next()) else {
        eprintln!("usage: export-api-types --check|--write <output-directory>");
        return ExitCode::FAILURE;
    };
    let directory = PathBuf::from(path);
    match mode.to_str() {
        Some("--write") => match write_typescript_api(&directory) {
            Ok(()) => {
                println!("export-api-types: wrote {}", directory.display());
                ExitCode::SUCCESS
            }
            Err(message) => {
                eprintln!("export-api-types: {message}");
                ExitCode::FAILURE
            }
        },
        Some("--check") => match check_typescript_api(&directory) {
            Ok(()) => {
                println!("export-api-types: generated API types are current");
                ExitCode::SUCCESS
            }
            Err(defects) => {
                for defect in defects {
                    eprintln!("export-api-types: {defect}");
                }
                eprintln!("export-api-types: run `just generate-api-types`");
                ExitCode::FAILURE
            }
        },
        _ => {
            eprintln!("export-api-types: mode must be --check or --write");
            ExitCode::FAILURE
        }
    }
}
