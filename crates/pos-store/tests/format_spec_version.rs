//! m0-s05 AC: the format-version constant in code and `docs/format-spec.md`
//! move together — this check fails CI when they disagree, and the parser is
//! proven on a mismatch fixture (a checker that has never failed is
//! decoration).

#![forbid(unsafe_code)]

use pos_store::FORMAT_VERSION;
use std::path::Path;

/// Extracts the declared version from the spec's `**Format version:** `N``
/// marker line. `None` when the marker is missing or malformed.
fn declared_format_version(spec_text: &str) -> Option<u32> {
    let marker = "**Format version:**";
    let line = spec_text.lines().find(|line| line.starts_with(marker))?;
    let value = line[marker.len()..].trim().trim_matches('`');
    value.parse().ok()
}

#[test]
fn the_spec_and_the_code_constant_agree() {
    let spec_path = Path::new(env!("CARGO_MANIFEST_DIR")).join("../../docs/format-spec.md");
    let spec_text = std::fs::read_to_string(&spec_path)
        .unwrap_or_else(|error| panic!("read {}: {error}", spec_path.display()));
    let declared = declared_format_version(&spec_text)
        .expect("docs/format-spec.md must declare `**Format version:** `N``");
    assert_eq!(
        declared, FORMAT_VERSION,
        "docs/format-spec.md declares v{declared} but pos_store::FORMAT_VERSION is \
         {FORMAT_VERSION}; bump them together with the documented migration"
    );
}

#[test]
fn the_parser_fails_on_a_mismatch_fixture() {
    // Clean fixture parses.
    assert_eq!(
        declared_format_version("# Spec\n\n**Format version:** `0`\n"),
        Some(0)
    );
    // A drifted spec is a detectable mismatch, not a silent pass.
    let drifted = declared_format_version("**Format version:** `999`\n").expect("marker parses");
    assert_ne!(drifted, FORMAT_VERSION);
    // A missing or mangled marker is caught rather than defaulting.
    assert_eq!(declared_format_version("no marker here"), None);
    assert_eq!(declared_format_version("**Format version:** `abc`"), None);
}
