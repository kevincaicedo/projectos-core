//! Differential test: our WordPiece against the reference implementation's
//! recorded answers (m1-s04).
//!
//! `wordpiece.rs` states why the tokenizer is ours. This is the obligation
//! that makes that safe. A hand-written pre-tokenizer is easy to get *almost*
//! right, and almost is invisible — the ids stay plausible, the vectors stay
//! 384-wide, and retrieval just quietly gets worse. So the ids HuggingFace
//! `tokenizers` produces over bge-small's real 30 522-entry vocabulary are
//! recorded in `fixtures/wordpiece-reference.json`, and this asserts we match
//! them.
//!
//! Writing that file found two real defects the unit tests had not asked
//! about: `？` (U+FF1F) was not punctuation, and `ø` was accent-stripped to
//! `o` when its canonical decomposition leaves it alone.
//!
//! **The dependency is not in this workspace; its answers are.** That is the
//! whole point — we get the confidence without the 29 crates, and the
//! recorded ids cannot drift under a stored index the way a live dependency
//! could.
//!
//! The vocabulary itself is a pulled model artifact, so this test skips with
//! a message when it is absent rather than failing a clean checkout.

#![forbid(unsafe_code)]

use pos_gateway::WordPiece;
use std::path::PathBuf;

const SEQUENCE_TOKENS_MAX: usize = 512;

/// Where `pos models pull` puts artifacts, mirroring the whisper adapter's
/// resolution so one environment variable configures both.
fn vocab_path() -> PathBuf {
    let root = std::env::var("POS_MODELS_DIR").unwrap_or_else(|_| "models/pulled".to_owned());
    PathBuf::from(root)
        .join("bge-small-en-v1.5")
        .join("vocab.txt")
}

#[test]
fn our_tokenizer_matches_the_recorded_reference_ids() {
    let path = vocab_path();
    let Ok(vocab_text) = std::fs::read_to_string(&path) else {
        eprintln!(
            "skipping: no vocabulary at {} — `just pull-embed-model` first",
            path.display()
        );
        return;
    };
    let vocab = WordPiece::from_vocab_text(&vocab_text).expect("the bge vocabulary loads");
    assert_eq!(
        vocab.entry_count(),
        30_522,
        "the recorded ids are for bge-small-en-v1.5's vocabulary"
    );

    let fixture = include_str!("fixtures/wordpiece-reference.json");
    let parsed: serde_json::Value = serde_json::from_str(fixture).expect("the fixture is JSON");
    let cases = parsed["cases"].as_array().expect("the fixture has cases");
    assert!(cases.len() >= 40, "the fixture is not a token sample");

    let mut mismatched = Vec::new();
    for case in cases {
        let text = case["text"].as_str().expect("a case has text");
        let expected: Vec<i64> = case["ids"]
            .as_array()
            .expect("a case has ids")
            .iter()
            .filter_map(serde_json::Value::as_i64)
            .collect();
        let ours = vocab.encode(text, SEQUENCE_TOKENS_MAX);
        if ours.input_ids != expected {
            mismatched.push(format!(
                "  {text:?}\n    reference: {expected:?}\n    ours:      {:?}",
                ours.input_ids
            ));
        }
    }
    assert!(
        mismatched.is_empty(),
        "{} of {} cases diverge from the reference tokenizer:\n{}",
        mismatched.len(),
        cases.len(),
        mismatched.join("\n")
    );
}
