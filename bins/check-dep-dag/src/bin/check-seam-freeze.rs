//! Frozen capability-seam pull-request gate (m0-s17).
//!
//! A change to the public capability ids, request/response envelopes, or trait
//! signatures must raise `CAPABILITY_TRAIT_VERSION` and link an ADR in the PR
//! body. The GitHub event is parsed as untrusted data and never reaches a shell.

#![forbid(unsafe_code)]

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};

const EVENT_SIZE_MAX: u64 = 1_048_576;
const FROZEN_PATHS: [&str; 2] = [
    "crates/pos-capabilities/src/traits.rs",
    "crates/pos-capabilities/src/types.rs",
];
const VERSION_PATH: &str = "crates/pos-capabilities/src/lib.rs";

fn main() -> ExitCode {
    match arguments().and_then(|arguments| check(&arguments)) {
        Ok(()) => {
            println!("seam-freeze: frozen capability contract is valid");
            ExitCode::SUCCESS
        }
        Err(error) => {
            eprintln!("seam-freeze: {error}");
            ExitCode::FAILURE
        }
    }
}

struct Arguments {
    root: PathBuf,
    base: String,
    event: PathBuf,
}

fn arguments() -> Result<Arguments, String> {
    let mut args = env::args_os().skip(1);
    if args.next().as_deref() != Some(OsStr::new("--root")) {
        return Err("expected --root <path> --base <sha> --event <path>".to_owned());
    }
    let root = args
        .next()
        .ok_or_else(|| "root path is required".to_owned())?;
    if args.next().as_deref() != Some(OsStr::new("--base")) {
        return Err("--base <sha> is required".to_owned());
    }
    let base = args
        .next()
        .and_then(|value| value.into_string().ok())
        .ok_or_else(|| "base sha must be UTF-8".to_owned())?;
    if args.next().as_deref() != Some(OsStr::new("--event")) {
        return Err("--event <path> is required".to_owned());
    }
    let event = args
        .next()
        .ok_or_else(|| "event path is required".to_owned())?;
    if args.next().is_some() {
        return Err("unexpected trailing arguments".to_owned());
    }
    if base.len() != 40 || !base.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return Err("base must be a 40-character git commit sha".to_owned());
    }
    let root = PathBuf::from(root)
        .canonicalize()
        .map_err(|error| format!("resolve root: {error}"))?;
    Ok(Arguments {
        root,
        base,
        event: PathBuf::from(event),
    })
}

fn check(arguments: &Arguments) -> Result<(), String> {
    if !frozen_paths_changed(&arguments.root, &arguments.base)? {
        return Ok(());
    }
    let current_version = version_from_file(&arguments.root.join(VERSION_PATH))?;
    let base_version =
        version_from_source(&git_file(&arguments.root, &arguments.base, VERSION_PATH)?)?;
    if current_version <= base_version {
        return Err(format!(
            "frozen contract changed but CAPABILITY_TRAIT_VERSION did not increase ({base_version} -> {current_version})"
        ));
    }
    let body = pull_request_body(&arguments.event)?;
    if !contains_adr_link(&body) {
        return Err(
            "frozen contract changed but the PR body has no docs/adr/NNNN-*.md link".to_owned(),
        );
    }
    Ok(())
}

fn frozen_paths_changed(root: &Path, base: &str) -> Result<bool, String> {
    let status = Command::new("git")
        .current_dir(root)
        .arg("diff")
        .arg("--quiet")
        .arg(base)
        .arg("--")
        .args(FROZEN_PATHS)
        .status()
        .map_err(|error| format!("run git diff: {error}"))?;
    match status.code() {
        Some(0) => Ok(false),
        Some(1) => Ok(true),
        Some(code) => Err(format!("git diff failed with exit code {code}")),
        None => Err("git diff terminated without an exit code".to_owned()),
    }
}

fn git_file(root: &Path, base: &str, path: &str) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(root)
        .arg("show")
        .arg(format!("{base}:{path}"))
        .output()
        .map_err(|error| format!("run git show for {path}: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git show failed for {path}: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| format!("{path} is not UTF-8: {error}"))
}

fn version_from_file(path: &Path) -> Result<u16, String> {
    let source =
        fs::read_to_string(path).map_err(|error| format!("read {}: {error}", path.display()))?;
    version_from_source(&source)
}

fn version_from_source(source: &str) -> Result<u16, String> {
    let syntax =
        syn::parse_file(source).map_err(|error| format!("parse capability lib: {error}"))?;
    let mut found = None;
    for item in syntax.items {
        let syn::Item::Const(item) = item else {
            continue;
        };
        if item.ident != "CAPABILITY_TRAIT_VERSION" {
            continue;
        }
        let syn::Expr::Lit(expression) = item.expr.as_ref() else {
            return Err("CAPABILITY_TRAIT_VERSION must be an integer literal".to_owned());
        };
        let syn::Lit::Int(value) = &expression.lit else {
            return Err("CAPABILITY_TRAIT_VERSION must be an integer literal".to_owned());
        };
        let version = value
            .base10_parse::<u16>()
            .map_err(|error| format!("parse CAPABILITY_TRAIT_VERSION: {error}"))?;
        if found.replace(version).is_some() {
            return Err("CAPABILITY_TRAIT_VERSION is declared more than once".to_owned());
        }
    }
    found.ok_or_else(|| "CAPABILITY_TRAIT_VERSION is missing".to_owned())
}

fn pull_request_body(path: &Path) -> Result<String, String> {
    let file = fs::File::open(path)
        .map_err(|error| format!("open GitHub event {}: {error}", path.display()))?;
    let bytes = read_bounded(file, EVENT_SIZE_MAX, "GitHub event")?;
    let event: serde_json::Value =
        serde_json::from_slice(&bytes).map_err(|error| format!("decode GitHub event: {error}"))?;
    Ok(event
        .pointer("/pull_request/body")
        .and_then(serde_json::Value::as_str)
        .unwrap_or_default()
        .to_owned())
}

fn read_bounded(mut reader: impl Read, limit: u64, description: &str) -> Result<Vec<u8>, String> {
    let mut bytes = Vec::new();
    reader
        .by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {description}: {error}"))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(format!("{description} exceeds {limit} bytes"));
    }
    Ok(bytes)
}

fn contains_adr_link(body: &str) -> bool {
    body.match_indices("docs/adr/").any(|(index, _)| {
        let suffix = &body[index + "docs/adr/".len()..];
        let mut bytes = suffix.bytes();
        let four_digits = (0..4).all(|_| bytes.next().is_some_and(|byte| byte.is_ascii_digit()));
        four_digits
            && matches!(bytes.next(), Some(b'-'))
            && suffix
                .split_ascii_whitespace()
                .next()
                .is_some_and(|link| link.contains(".md"))
    })
}

#[cfg(test)]
mod tests {
    use super::{contains_adr_link, read_bounded, version_from_source};
    use std::io::Cursor;

    #[test]
    fn version_requires_the_named_integer_constant() {
        assert_eq!(
            version_from_source("pub const CAPABILITY_TRAIT_VERSION: u16 = 7;")
                .expect("fixture is valid"),
            7
        );
        assert!(version_from_source("pub const OTHER: u16 = 7;").is_err());
    }

    #[test]
    fn adr_evidence_requires_a_link_not_a_bare_number() {
        assert!(contains_adr_link(
            "Reason: [ADR-0004](docs/adr/0004-open-core-repository-topology.md)"
        ));
        assert!(!contains_adr_link("ADR-0004"));
        assert!(!contains_adr_link("docs/adr/4-too-short.md"));
        assert!(!contains_adr_link("docs/adr/0004-no-extension"));
    }

    #[test]
    fn event_reader_refuses_oversize_input_without_reading_unbounded_bytes() {
        assert_eq!(
            read_bounded(Cursor::new(b"1234"), 4, "fixture").expect("fixture is bounded"),
            b"1234"
        );
        assert!(read_bounded(Cursor::new(b"12345"), 4, "fixture").is_err());
    }
}
