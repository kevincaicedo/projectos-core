//! The eval harness scaffold (m0-s11, master plan §12): a golden-set runner
//! — `cases.jsonl` → run → score → report artifact — pinned to an exact
//! prompt reference. Trivial today, load-bearing from M1: the wiring (load,
//! run, score, artifact, regression gate) is what this story proves, and
//! `tests/eval_suite.rs` runs the real `evals/echo` suite through the real
//! gateway dispatch path in CI, with a deliberately broken fixture proving
//! the gate actually fails.

use crate::prompts::PromptFile;
use crate::weather::Weather;
use serde::{Deserialize, Serialize};
use std::path::Path;

/// A golden set is curated, not scraped: 10k cases is far beyond any M-era
/// suite and stops a malformed JSONL from ballooning a CI job (L8).
pub const EVAL_CASES_MAX: usize = 10_000;

/// How much model output an outcome row keeps for the report artifact —
/// enough to debug a failure, small enough to diff.
const OUTPUT_EXCERPT_CHARS_MAX: usize = 400;

/// One golden case: an input and the substring its output must contain.
/// v0 scoring is containment; richer scorers arrive with the suites that
/// need them (M1 retrieval evals), behind this same case shape.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct EvalCase {
    pub id: String,
    pub input: String,
    pub expect_contains: String,
}

/// One case's result inside the report artifact.
#[derive(Clone, Debug, Serialize)]
pub struct EvalOutcome {
    pub case_id: String,
    pub passed: bool,
    pub detail: String,
}

/// The report artifact CI archives: which suite, which pinned prompt, which
/// model, and every outcome. `regressed()` is the merge gate.
#[derive(Debug, Serialize)]
pub struct EvalReport {
    pub suite: String,
    /// `<id>@<version>#<hash-prefix>` — the pin that makes a regression
    /// bisect to a prompt change.
    pub prompt_reference: String,
    pub model: String,
    pub total: u32,
    pub passed: u32,
    pub failed: u32,
    pub outcomes: Vec<EvalOutcome>,
}

impl EvalReport {
    /// True when any case failed — the condition that blocks a merge.
    #[must_use]
    pub const fn regressed(&self) -> bool {
        self.failed > 0
    }

    /// Writes the JSON artifact CI attaches to the run.
    ///
    /// # Errors
    ///
    /// [`EvalError::Io`] when the artifact cannot be written.
    pub fn write_artifact(&self, path: &Path) -> Result<(), EvalError> {
        let json = serde_json::to_string_pretty(self).map_err(|error| EvalError::Io {
            path: path.display().to_string(),
            reason: error.to_string(),
        })?;
        std::fs::write(path, json).map_err(|error| EvalError::Io {
            path: path.display().to_string(),
            reason: error.to_string(),
        })
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum EvalError {
    Io { path: String, reason: String },
    BadCase { line_number: usize, reason: String },
    TooManyCases { count: usize },
}

impl std::fmt::Display for EvalError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Io { path, reason } => write!(formatter, "eval I/O failed at {path}: {reason}"),
            Self::BadCase {
                line_number,
                reason,
            } => write!(formatter, "cases.jsonl line {line_number}: {reason}"),
            Self::TooManyCases { count } => write!(
                formatter,
                "{count} cases exceeds the {EVAL_CASES_MAX}-case suite cap"
            ),
        }
    }
}

impl std::error::Error for EvalError {}

/// Loads a `cases.jsonl` golden set.
///
/// # Errors
///
/// Typed [`EvalError`] naming the offending line; a partially loaded suite
/// would silently shrink coverage.
pub fn load_cases(path: &Path) -> Result<Vec<EvalCase>, EvalError> {
    let text = std::fs::read_to_string(path).map_err(|error| EvalError::Io {
        path: path.display().to_string(),
        reason: error.to_string(),
    })?;
    let mut cases = Vec::new();
    for (index, line) in text.lines().enumerate() {
        if line.trim().is_empty() {
            continue;
        }
        let case: EvalCase = serde_json::from_str(line).map_err(|error| EvalError::BadCase {
            line_number: index + 1,
            reason: error.to_string(),
        })?;
        cases.push(case);
        if cases.len() > EVAL_CASES_MAX {
            return Err(EvalError::TooManyCases { count: cases.len() });
        }
    }
    Ok(cases)
}

fn excerpt(text: &str) -> String {
    let mut cut: String = text.chars().take(OUTPUT_EXCERPT_CHARS_MAX).collect();
    if text.chars().count() > OUTPUT_EXCERPT_CHARS_MAX {
        cut.push('…');
    }
    cut
}

/// Runs one suite. `complete` is the system under test: it receives the
/// pinned prompt body and one case input, and returns the model text — the
/// eval CI test binds it to a real gateway dispatch; unit tests bind fakes.
pub fn run_suite(
    suite: &str,
    model: &str,
    prompt: &PromptFile,
    cases: &[EvalCase],
    complete: &mut dyn FnMut(&PromptFile, &EvalCase) -> Result<String, Weather>,
) -> EvalReport {
    let mut outcomes = Vec::with_capacity(cases.len());
    let mut passed = 0_u32;
    for case in cases {
        let outcome = match complete(prompt, case) {
            Ok(output) if output.contains(&case.expect_contains) => EvalOutcome {
                case_id: case.id.clone(),
                passed: true,
                detail: excerpt(&output),
            },
            Ok(output) => EvalOutcome {
                case_id: case.id.clone(),
                passed: false,
                detail: format!(
                    "expected {:?} in output; got: {}",
                    case.expect_contains,
                    excerpt(&output)
                ),
            },
            Err(weather) => EvalOutcome {
                case_id: case.id.clone(),
                passed: false,
                detail: format!("weather: {weather}"),
            },
        };
        if outcome.passed {
            passed += 1;
        }
        outcomes.push(outcome);
    }
    let total = u32::try_from(cases.len()).unwrap_or(u32::MAX); // INVARIANT: EVAL_CASES_MAX bounds the suite far below u32::MAX.
    EvalReport {
        suite: suite.to_owned(),
        prompt_reference: prompt.reference(),
        model: model.to_owned(),
        total,
        passed,
        failed: total.saturating_sub(passed),
        outcomes,
    }
}

#[cfg(test)]
mod tests {
    use super::{EvalCase, load_cases, run_suite};
    use crate::prompts::PromptRegistry;

    fn echo_prompt_dir() -> tempfile::TempDir {
        let dir = tempfile::tempdir().expect("tempdir");
        std::fs::write(
            dir.path().join("echo@1.md"),
            "---\ntier: fast\n---\nRepeat the input after `ECHO:`.\n",
        )
        .expect("fixture");
        dir
    }

    #[test]
    fn the_runner_scores_pins_and_gates() {
        let dir = echo_prompt_dir();
        let registry = PromptRegistry::load_dir(dir.path()).expect("loads");
        let prompt = registry.get("echo", 1).expect("registered").clone();
        let cases = vec![
            EvalCase {
                id: "greets".to_owned(),
                input: "hello".to_owned(),
                expect_contains: "ECHO: hello".to_owned(),
            },
            EvalCase {
                id: "numbers".to_owned(),
                input: "42".to_owned(),
                expect_contains: "ECHO: 42".to_owned(),
            },
        ];
        let mut fake =
            |_: &crate::prompts::PromptFile, case: &EvalCase| Ok(format!("ECHO: {}", case.input));
        let report = run_suite("echo-unit", "fake-echo", &prompt, &cases, &mut fake);
        assert_eq!(report.total, 2);
        assert_eq!(report.failed, 0);
        assert!(!report.regressed());
        assert!(report.prompt_reference.starts_with("echo@1#"));

        // The deliberately broken fixture: an impossible expectation must
        // flip the gate — a gate never seen red is decoration.
        let broken = vec![EvalCase {
            id: "broken".to_owned(),
            input: "hello".to_owned(),
            expect_contains: "THIS-MARKER-NEVER-APPEARS".to_owned(),
        }];
        let report = run_suite("echo-broken", "fake-echo", &prompt, &broken, &mut fake);
        assert!(report.regressed());
    }

    #[test]
    fn artifacts_write_and_malformed_cases_are_typed_errors() {
        let dir = echo_prompt_dir();
        let registry = PromptRegistry::load_dir(dir.path()).expect("loads");
        let prompt = registry.get("echo", 1).expect("registered").clone();
        let report = run_suite("empty", "fake", &prompt, &[], &mut |_, _| Ok(String::new()));
        let artifact = dir.path().join("report.json");
        report.write_artifact(&artifact).expect("artifact writes");
        let written = std::fs::read_to_string(&artifact).expect("artifact readable");
        assert!(written.contains("\"suite\": \"empty\""));

        let bad = dir.path().join("cases.jsonl");
        std::fs::write(&bad, "{\"id\":\"x\"}\n").expect("fixture");
        assert!(load_cases(&bad).is_err());
    }
}
