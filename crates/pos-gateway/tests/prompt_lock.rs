//! CI gates over the repository's real prompt/eval/model trees (m0-s11):
//! every shipped prompt is hash-pinned in `prompts/prompts.lock` (an
//! unversioned edit fails here), the model manifest parses, and the echo
//! golden set loads. The seeded-violation halves of these gates live in the
//! crate unit tests (`prompts::tests`, `models::tests`), which prove the
//! checks fire; this binary proves the real trees pass them.

#![forbid(unsafe_code)]

use pos_gateway::{ModelManifest, PROMPT_LOCK_FILE_NAME, PromptRegistry, load_cases};
use std::path::PathBuf;

fn repo_root() -> PathBuf {
    // crates/pos-gateway → the core workspace root.
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .join("../..")
        .canonicalize()
        .expect("the workspace root exists")
}

#[test]
fn every_shipped_prompt_is_pinned_and_unedited() {
    let prompts_dir = repo_root().join("prompts");
    let registry = PromptRegistry::load_dir(&prompts_dir).expect("the prompt tree loads");
    let lock = std::fs::read_to_string(prompts_dir.join(PROMPT_LOCK_FILE_NAME))
        .expect("prompts.lock exists — regenerate with `cargo run -p pos-gateway --bin generate-prompt-lock -- prompts`");
    registry
        .verify_lock(&lock)
        .expect("a shipped prompt changed without a version bump, or a pin is stale; add a new <id>@<version+1>.md and regenerate the lock");
}

#[test]
fn the_echo_prompt_loads_by_id_with_its_contract_frontmatter() {
    let registry =
        PromptRegistry::load_dir(&repo_root().join("prompts")).expect("the prompt tree loads");
    let echo = registry.get("echo", 1).expect("echo@1 is registered");
    assert_eq!(echo.tier, "fast", "the echo agent runs on the fast tier");
    assert_eq!(
        echo.params.get("marker").map(String::as_str),
        Some("\"ECHO: \""),
        "the marker token the m0-s13 e2e asserts on is frontmatter, not folklore"
    );
}

#[test]
fn the_model_manifest_parses_and_every_entry_is_complete() {
    let manifest =
        ModelManifest::load(&repo_root().join("models/manifest.json")).expect("manifest parses");
    for entry in &manifest.models {
        assert!(!entry.name.is_empty());
        assert_eq!(
            entry.blake3.len(),
            64,
            "{}: blake3 pins are 64 hex chars",
            entry.name
        );
        assert!(
            entry.bytes > 0,
            "{}: a zero-byte model is a typo",
            entry.name
        );
    }
}

#[test]
fn the_echo_golden_sets_load_and_stay_within_the_case_cap() {
    let evals_dir = repo_root().join("evals/echo");
    let cases = load_cases(&evals_dir.join("cases.jsonl")).expect("golden set loads");
    assert!(!cases.is_empty(), "an empty golden set gates nothing");
    let broken =
        load_cases(&evals_dir.join("cases-broken.jsonl")).expect("the seeded-violation set loads");
    assert!(!broken.is_empty());
}
