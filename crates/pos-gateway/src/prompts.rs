//! The prompt registry (m0-s11, F25): prompts are versioned files —
//! `prompts/<id>@<version>.md` with a small frontmatter — loaded by id and
//! pinned by content hash in `prompts.lock`. Editing a file without bumping
//! its version fails CI (`tests/prompt_lock.rs` runs [`PromptRegistry::verify_lock`]
//! against the repository's real prompt tree), because a prompt change is a
//! code change: versioned, reviewed, evaluated (doc 05 §7).

use std::collections::BTreeMap;
use std::fmt;
use std::path::Path;

/// A prompt file is instructions, not a novel: 256 KiB is far above any
/// sane prompt and refuses a runaway file (L8).
const PROMPT_FILE_BYTES_MAX: u64 = 256 * 1024;

/// The lock file beside the prompt files: one `id@version blake3-hex` line
/// per prompt, sorted, newline-terminated.
pub const PROMPT_LOCK_FILE_NAME: &str = "prompts.lock";

/// Typed registry failure. Every variant names the offending file so a CI
/// failure is a fix instruction, not a scavenger hunt.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum PromptError {
    Io {
        path: String,
        reason: String,
    },
    BadName {
        file_name: String,
    },
    BadFrontmatter {
        file_name: String,
        reason: String,
    },
    FileTooLarge {
        file_name: String,
        bytes: u64,
    },
    /// The hash in `prompts.lock` does not match the file: someone edited a
    /// prompt without bumping its version.
    DriftedWithoutBump {
        file_name: String,
    },
    /// A prompt file exists with no lock line: bump = add the line.
    MissingFromLock {
        file_name: String,
    },
    /// A lock line names a file that does not exist.
    OrphanLockLine {
        line: String,
    },
    BadLockLine {
        line: String,
    },
    UnknownPrompt {
        id: String,
        version: u32,
    },
}

impl fmt::Display for PromptError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Io { path, reason } => write!(formatter, "prompt I/O failed at {path}: {reason}"),
            Self::BadName { file_name } => write!(
                formatter,
                "{file_name}: prompt files are named <id>@<version>.md"
            ),
            Self::BadFrontmatter { file_name, reason } => {
                write!(formatter, "{file_name}: bad frontmatter: {reason}")
            }
            Self::FileTooLarge { file_name, bytes } => write!(
                formatter,
                "{file_name}: {bytes} bytes exceeds the {PROMPT_FILE_BYTES_MAX}-byte prompt cap"
            ),
            Self::DriftedWithoutBump { file_name } => write!(
                formatter,
                "{file_name} changed without a version bump: its content no longer matches prompts.lock; add a new <id>@<version+1>.md instead of editing a shipped prompt"
            ),
            Self::MissingFromLock { file_name } => write!(
                formatter,
                "{file_name} has no prompts.lock line; add `<id>@<version> <blake3>` to pin it"
            ),
            Self::OrphanLockLine { line } => write!(
                formatter,
                "prompts.lock pins {line:?} but no such prompt file exists"
            ),
            Self::BadLockLine { line } => {
                write!(formatter, "prompts.lock line did not parse: {line:?}")
            }
            Self::UnknownPrompt { id, version } => {
                write!(formatter, "no prompt is registered as {id}@{version}")
            }
        }
    }
}

impl std::error::Error for PromptError {}

/// One loaded prompt: identity, frontmatter, body, and the content hash the
/// `PromptManifest` (doc 05 §3) pins.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct PromptFile {
    pub id: String,
    pub version: u32,
    /// Routing tier the prompt expects (`frontier` / `fast`) — frontmatter
    /// `tier:`, required so a prompt cannot silently run on the wrong tier.
    pub tier: String,
    /// Remaining frontmatter pairs, order-preserving by key.
    pub params: BTreeMap<String, String>,
    pub body: String,
    /// Lowercase BLAKE3 of the full file bytes (frontmatter included): the
    /// pin `prompts.lock` and eval reports carry.
    pub blake3_hex: String,
}

impl PromptFile {
    #[must_use]
    pub fn reference(&self) -> String {
        format!(
            "{}@{}#{}",
            self.id,
            self.version,
            &self.blake3_hex[..16.min(self.blake3_hex.len())]
        )
    }
}

/// All prompts under one directory, loaded eagerly (the tree is small by
/// the cap above) and indexed by `(id, version)`.
pub struct PromptRegistry {
    prompts: BTreeMap<(String, u32), PromptFile>,
}

fn parse_file_name(file_name: &str) -> Option<(String, u32)> {
    let stem = file_name.strip_suffix(".md")?;
    let (id, version_text) = stem.rsplit_once('@')?;
    if id.is_empty() {
        return None;
    }
    let version: u32 = version_text.parse().ok()?;
    Some((id.to_owned(), version))
}

fn parse_frontmatter(
    file_name: &str,
    text: &str,
) -> Result<(BTreeMap<String, String>, String), PromptError> {
    let bad = |reason: &str| PromptError::BadFrontmatter {
        file_name: file_name.to_owned(),
        reason: reason.to_owned(),
    };
    let rest = text
        .strip_prefix("---\n")
        .ok_or_else(|| bad("file must start with a `---` frontmatter block"))?;
    let (front, body) = rest
        .split_once("\n---\n")
        .ok_or_else(|| bad("frontmatter block is not closed by `---`"))?;
    let mut pairs = BTreeMap::new();
    for line in front.lines() {
        if line.trim().is_empty() {
            continue;
        }
        let (key, value) = line
            .split_once(':')
            .ok_or_else(|| bad(&format!("line is not `key: value`: {line:?}")))?;
        pairs.insert(key.trim().to_owned(), value.trim().to_owned());
    }
    Ok((pairs, body.to_owned()))
}

impl PromptRegistry {
    /// Loads every `*.md` prompt under `dir`.
    ///
    /// # Errors
    ///
    /// The first typed [`PromptError`] encountered; a partially valid prompt
    /// tree is a broken build, not a warning.
    pub fn load_dir(dir: &Path) -> Result<Self, PromptError> {
        let io = |reason: String| PromptError::Io {
            path: dir.display().to_string(),
            reason,
        };
        let mut prompts = BTreeMap::new();
        let entries = std::fs::read_dir(dir).map_err(|error| io(error.to_string()))?;
        for entry in entries {
            let entry = entry.map_err(|error| io(error.to_string()))?;
            let file_name = entry.file_name().to_string_lossy().into_owned();
            if !file_name.ends_with(".md") {
                continue;
            }
            let (id, version) = parse_file_name(&file_name).ok_or(PromptError::BadName {
                file_name: file_name.clone(),
            })?;
            let metadata = entry.metadata().map_err(|error| io(error.to_string()))?;
            if metadata.len() > PROMPT_FILE_BYTES_MAX {
                return Err(PromptError::FileTooLarge {
                    file_name,
                    bytes: metadata.len(),
                });
            }
            let bytes = std::fs::read(entry.path()).map_err(|error| io(error.to_string()))?;
            let text = String::from_utf8_lossy(&bytes).into_owned();
            let (mut params, body) = parse_frontmatter(&file_name, &text)?;
            let tier = params
                .remove("tier")
                .ok_or_else(|| PromptError::BadFrontmatter {
                    file_name: file_name.clone(),
                    reason: "missing required `tier:`".to_owned(),
                })?;
            prompts.insert(
                (id.clone(), version),
                PromptFile {
                    id,
                    version,
                    tier,
                    params,
                    body,
                    blake3_hex: blake3::hash(&bytes).to_hex().to_string(),
                },
            );
        }
        Ok(Self { prompts })
    }

    /// # Errors
    ///
    /// [`PromptError::UnknownPrompt`] when nothing is registered under the id.
    pub fn get(&self, id: &str, version: u32) -> Result<&PromptFile, PromptError> {
        self.prompts
            .get(&(id.to_owned(), version))
            .ok_or_else(|| PromptError::UnknownPrompt {
                id: id.to_owned(),
                version,
            })
    }

    /// The registry's canonical lock rendering: sorted, one line per prompt.
    #[must_use]
    pub fn render_lock(&self) -> String {
        let mut lock = String::new();
        for ((id, version), prompt) in &self.prompts {
            lock.push_str(&format!("{id}@{version} {}\n", prompt.blake3_hex));
        }
        lock
    }

    /// Verifies this registry against the lock file text: every prompt is
    /// pinned, every pin matches, every pin has a file.
    ///
    /// # Errors
    ///
    /// [`PromptError::DriftedWithoutBump`] on an edited-in-place prompt,
    /// [`PromptError::MissingFromLock`] / [`PromptError::OrphanLockLine`] on
    /// set drift, [`PromptError::BadLockLine`] on a malformed pin.
    pub fn verify_lock(&self, lock_text: &str) -> Result<(), PromptError> {
        let mut pinned: BTreeMap<(String, u32), String> = BTreeMap::new();
        for line in lock_text.lines() {
            if line.trim().is_empty() {
                continue;
            }
            let (name, hash) = line
                .split_once(' ')
                .ok_or_else(|| PromptError::BadLockLine {
                    line: line.to_owned(),
                })?;
            let (id, version) =
                parse_file_name(&format!("{name}.md")).ok_or_else(|| PromptError::BadLockLine {
                    line: line.to_owned(),
                })?;
            pinned.insert((id, version), hash.trim().to_owned());
        }
        for ((id, version), prompt) in &self.prompts {
            let file_name = format!("{id}@{version}.md");
            match pinned.remove(&(id.clone(), *version)) {
                None => return Err(PromptError::MissingFromLock { file_name }),
                Some(hash) if hash != prompt.blake3_hex => {
                    return Err(PromptError::DriftedWithoutBump { file_name });
                }
                Some(_) => {}
            }
        }
        if let Some(((id, version), _)) = pinned.into_iter().next() {
            return Err(PromptError::OrphanLockLine {
                line: format!("{id}@{version}"),
            });
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::{PromptError, PromptRegistry};
    use std::path::Path;

    fn write_prompt(dir: &Path, name: &str, tier: &str, body: &str) {
        std::fs::write(dir.join(name), format!("---\ntier: {tier}\n---\n{body}"))
            .expect("test fixture writes");
    }

    #[test]
    fn prompts_load_by_id_with_frontmatter_and_stable_hashes() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_prompt(dir.path(), "echo@1.md", "fast", "Echo the marker.\n");
        let registry = PromptRegistry::load_dir(dir.path()).expect("loads");
        let prompt = registry.get("echo", 1).expect("registered");
        assert_eq!(prompt.tier, "fast");
        assert_eq!(prompt.body, "Echo the marker.\n");
        let again = PromptRegistry::load_dir(dir.path()).expect("loads");
        assert_eq!(
            again.get("echo", 1).expect("registered").blake3_hex,
            prompt.blake3_hex,
            "the pin must be a pure content hash"
        );
        assert!(registry.get("echo", 2).is_err());
    }

    #[test]
    fn an_unversioned_edit_fails_the_lock_check_naming_the_file() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_prompt(dir.path(), "echo@1.md", "fast", "Original body.\n");
        let registry = PromptRegistry::load_dir(dir.path()).expect("loads");
        let lock = registry.render_lock();
        registry
            .verify_lock(&lock)
            .expect("freshly rendered lock verifies");

        // The seeded violation: edit the shipped prompt in place.
        write_prompt(dir.path(), "echo@1.md", "fast", "Sneakily edited body.\n");
        let drifted = PromptRegistry::load_dir(dir.path()).expect("loads");
        let error = drifted
            .verify_lock(&lock)
            .expect_err("an in-place edit must fail the lock");
        assert_eq!(
            error,
            PromptError::DriftedWithoutBump {
                file_name: "echo@1.md".to_owned()
            }
        );

        // The legitimate path: a new version beside the old one, pinned.
        write_prompt(dir.path(), "echo@1.md", "fast", "Original body.\n");
        write_prompt(dir.path(), "echo@2.md", "fast", "Improved body.\n");
        let bumped = PromptRegistry::load_dir(dir.path()).expect("loads");
        let unpinned = bumped
            .verify_lock(&lock)
            .expect_err("the new version must be pinned before it ships");
        assert_eq!(
            unpinned,
            PromptError::MissingFromLock {
                file_name: "echo@2.md".to_owned()
            }
        );
        bumped
            .verify_lock(&bumped.render_lock())
            .expect("re-pinned lock verifies");
    }

    #[test]
    fn orphan_pins_and_malformed_files_are_typed_errors() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_prompt(dir.path(), "echo@1.md", "fast", "Body.\n");
        let registry = PromptRegistry::load_dir(dir.path()).expect("loads");
        let lock_with_ghost = format!("{}ghost@3 ffff\n", registry.render_lock());
        let error = registry
            .verify_lock(&lock_with_ghost)
            .expect_err("a pin without a file is an error");
        assert!(matches!(error, PromptError::OrphanLockLine { .. }));

        std::fs::write(dir.path().join("noversion.md"), "---\ntier: fast\n---\nx")
            .expect("fixture");
        assert!(matches!(
            PromptRegistry::load_dir(dir.path()),
            Err(PromptError::BadName { .. })
        ));
    }
}
