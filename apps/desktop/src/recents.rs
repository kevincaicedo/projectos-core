//! Recent projects and last-open restore (m0-s07).
//!
//! This list lives in the **app config directory**, never inside any project
//! (L4: a project is a portable directory a user can copy, and one machine's
//! window state is not part of it). Copying a `.pos` directory to another
//! machine therefore carries no trace of who opened it or when.
//!
//! The file is a bounded JSON array of paths, most-recent first. It is a
//! cache of user intent, so every read is defensive: a corrupt or truncated
//! file degrades to an empty list rather than failing app startup, and paths
//! that no longer exist are dropped on read instead of being offered as
//! broken menu entries.

use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

/// Recent entries retained (L8). Ten is the menu-sized list every desktop
/// app converges on; beyond that the palette's project switcher is the
/// right surface.
pub const RECENT_PROJECT_COUNT_MAX: usize = 10;

/// Refuses a config file that is not a plausible recents list — a bounded
/// read, so a corrupted or hostile file cannot allocate unbounded memory.
const RECENTS_FILE_BYTES_MAX: u64 = 64 * 1024;

const FILE_NAME: &str = "recent-projects.json";

#[derive(Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
pub struct Recents {
    /// Most-recent first. Absolute paths to `.pos` directories.
    #[serde(default)]
    pub paths: Vec<String>,
}

impl Recents {
    /// Reads the list, dropping entries whose directory no longer exists.
    /// Never fails: an unreadable or malformed file is an empty list, which
    /// is exactly as true as it is useful.
    #[must_use]
    pub fn load(config_dir: &Path) -> Self {
        let path = config_dir.join(FILE_NAME);
        let within_bound = std::fs::metadata(&path)
            .map(|metadata| metadata.len() <= RECENTS_FILE_BYTES_MAX)
            .unwrap_or(false);
        if !within_bound {
            return Self::default();
        }
        let parsed: Self = std::fs::read_to_string(&path)
            .ok()
            .and_then(|text| serde_json::from_str(&text).ok())
            .unwrap_or_default();
        Self {
            paths: parsed
                .paths
                .into_iter()
                .filter(|entry| Path::new(entry).is_dir())
                .take(RECENT_PROJECT_COUNT_MAX)
                .collect(),
        }
    }

    /// Moves `path` to the front (deduplicating) and persists.
    ///
    /// # Errors
    ///
    /// Returns a message naming the path when the config directory or file
    /// cannot be written. Callers surface this rather than failing the
    /// project operation it accompanies — losing the recents list must never
    /// lose a project.
    pub fn record(&mut self, config_dir: &Path, path: &Path) -> Result<(), String> {
        let text = path.display().to_string();
        self.paths.retain(|entry| entry != &text);
        self.paths.insert(0, text);
        self.paths.truncate(RECENT_PROJECT_COUNT_MAX);
        self.save(config_dir)
    }

    /// The project to restore on relaunch: the most recent still-present
    /// entry, or `None` for a first run (which opens the workspace home).
    #[must_use]
    pub fn last_open(&self) -> Option<PathBuf> {
        self.paths.first().map(PathBuf::from)
    }

    fn save(&self, config_dir: &Path) -> Result<(), String> {
        std::fs::create_dir_all(config_dir)
            .map_err(|error| format!("create {}: {error}", config_dir.display()))?;
        let path = config_dir.join(FILE_NAME);
        let json = serde_json::to_string_pretty(self)
            .map_err(|error| format!("serialize recents: {error}"))?;
        std::fs::write(&path, json).map_err(|error| format!("write {}: {error}", path.display()))
    }
}

#[cfg(test)]
mod tests {
    use super::{RECENT_PROJECT_COUNT_MAX, Recents};

    #[test]
    fn recording_dedupes_orders_and_bounds() {
        let config = tempfile::tempdir().expect("tempdir");
        let projects = tempfile::tempdir().expect("tempdir");
        let mut recents = Recents::default();
        // More projects than the bound, so eviction is exercised.
        let created: Vec<_> = (0..RECENT_PROJECT_COUNT_MAX + 3)
            .map(|index| {
                let path = projects.path().join(format!("p{index}.pos"));
                std::fs::create_dir_all(&path).expect("mkdir");
                path
            })
            .collect();
        for path in &created {
            recents.record(config.path(), path).expect("records");
        }
        assert_eq!(recents.paths.len(), RECENT_PROJECT_COUNT_MAX);
        // Most recent first.
        assert_eq!(
            recents.last_open().expect("a recent project"),
            *created.last().expect("created is non-empty")
        );

        // Re-recording an existing entry moves it to the front without
        // growing the list.
        let first = created.first().expect("created is non-empty");
        std::fs::create_dir_all(first).expect("mkdir");
        recents.record(config.path(), first).expect("records");
        assert_eq!(recents.paths.len(), RECENT_PROJECT_COUNT_MAX);
        assert_eq!(recents.last_open().as_deref(), Some(first.as_path()));
    }

    /// Relaunch restore, without a webview: the persisted list is what a
    /// fresh process reads back.
    #[test]
    fn a_fresh_process_restores_the_last_open_project() {
        let config = tempfile::tempdir().expect("tempdir");
        let projects = tempfile::tempdir().expect("tempdir");
        let project = projects.path().join("restored.pos");
        std::fs::create_dir_all(&project).expect("mkdir");
        Recents::default()
            .record(config.path(), &project)
            .expect("records");

        let reloaded = Recents::load(config.path());
        assert_eq!(reloaded.last_open(), Some(project.clone()));

        // A project the user deleted between launches is dropped rather than
        // offered as a broken entry.
        std::fs::remove_dir_all(&project).expect("remove");
        assert_eq!(Recents::load(config.path()).last_open(), None);
    }

    #[test]
    fn corrupt_and_oversized_config_files_degrade_to_an_empty_list() {
        let config = tempfile::tempdir().expect("tempdir");
        std::fs::write(config.path().join("recent-projects.json"), "{not json")
            .expect("write corrupt file");
        assert_eq!(Recents::load(config.path()), Recents::default());

        std::fs::write(
            config.path().join("recent-projects.json"),
            "x".repeat(128 * 1024),
        )
        .expect("write oversized file");
        assert_eq!(Recents::load(config.path()), Recents::default());
    }
}
