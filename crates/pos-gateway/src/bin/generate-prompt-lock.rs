//! Regenerates `prompts/prompts.lock` from the prompt tree (m0-s11). The
//! lock pins every `<id>@<version>.md` to its BLAKE3 so an in-place edit of
//! a shipped prompt fails CI (`tests/prompt_lock.rs`); this bin is the one
//! blessed way to re-pin after adding a new prompt version.

#![forbid(unsafe_code)]

use pos_gateway::{PROMPT_LOCK_FILE_NAME, PromptRegistry};
use std::path::PathBuf;
use std::process::ExitCode;

fn main() -> ExitCode {
    let mut arguments = std::env::args().skip(1);
    let Some(prompts_dir) = arguments.next().map(PathBuf::from) else {
        eprintln!("usage: generate-prompt-lock <prompts-dir>");
        return ExitCode::FAILURE;
    };
    let registry = match PromptRegistry::load_dir(&prompts_dir) {
        Ok(registry) => registry,
        Err(error) => {
            eprintln!("generate-prompt-lock: {error}");
            return ExitCode::FAILURE;
        }
    };
    let lock_path = prompts_dir.join(PROMPT_LOCK_FILE_NAME);
    if let Err(error) = std::fs::write(&lock_path, registry.render_lock()) {
        eprintln!(
            "generate-prompt-lock: write {}: {error}",
            lock_path.display()
        );
        return ExitCode::FAILURE;
    }
    println!("wrote {}", lock_path.display());
    ExitCode::SUCCESS
}
