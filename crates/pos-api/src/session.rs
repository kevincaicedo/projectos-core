//! Session-scoped project registry (m0-s06): which projects this runtime
//! process has opened, backing `project.open`, `project.list`, and `health`.
//!
//! This is view/session state, not domain truth — the project itself is the
//! directory and its log (L1/L4). A row here records that this process
//! validated and opened the directory; nothing is persisted, and every
//! operation still opens the store fresh, so the single-writer discipline
//! stays exactly where SQLite's own locking enforces it. Shell-durable
//! recency (the desktop recent-projects list) belongs to m0-s07 app config.

use crate::project_ops::{self, RuntimeIdentity};
use crate::{ApiError, ProjectPathInput};
use pos_foundation::{ProjectId, WallClock};
use pos_log::ProjectLog;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use ts_rs::TS;

/// Projects one runtime process will track at once (L8). A session that hits
/// this is pathological for M0 (the UI lists a handful); the refusal names the
/// bound instead of evicting silently. `u32` because the bound is wire
/// metadata (`ProjectListReport`), sized like every other wire counter.
pub const OPEN_PROJECT_COUNT_MAX: u32 = 64;

/// One tracked project. Keyed and sorted by project id bytes so `project.list`
/// renders in a deterministic order regardless of open order.
#[derive(Clone, Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct OpenProjectRow {
    pub project_id: String,
    pub path: String,
    pub name: Option<String>,
    pub template: String,
    pub format_version: u32,
    #[ts(type = "number")]
    pub head_seq: u64,
    /// Wall-clock open time, informational only (ordering is the id sort).
    #[ts(type = "number")]
    pub opened_ts_ms: u64,
}

#[derive(Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ProjectListReport {
    pub projects: Vec<OpenProjectRow>,
    /// The session bound, in-band so a full list is distinguishable from a
    /// truncated one (L8: the cap appears in the result metadata).
    pub open_project_count_max: u32,
}

/// What `project.close` answers with: the project that was released and what
/// the session still holds. Closing something already closed is a typed
/// refusal, not a silent success — a shell that thinks it released a handle it
/// never held would leak one every switch.
#[derive(Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ProjectCloseReport {
    pub project_id: String,
    pub path: String,
    pub open_project_count: u32,
}

#[derive(Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct HealthReport {
    /// Fixed vocabulary: `ok` is the only value this process ever reports —
    /// a runtime that cannot answer does not answer (capability honesty).
    #[ts(type = "string")]
    pub status: &'static str,
    pub api_surface_version: u16,
    pub capability_trait_version: u16,
    pub format_version: u32,
    pub open_project_count: u32,
    /// Whether this process claims queued jobs (m1-s01/ADR-0007). A shell that
    /// enqueues work into a runtime with no pool is the exact silence this
    /// field exists to break.
    pub background_workers: crate::workers::WorkerStatusReport,
}

/// Bounded, deterministic session table. Interior mutability because the
/// registry dispatch surface is `&self` across every transport.
#[derive(Default)]
pub struct OpenProjects {
    rows: Mutex<BTreeMap<[u8; 16], OpenProjectRow>>,
}

/// One `project.open`: the bytes the caller receives, plus the handle the
/// scheduler needs. The log is opened exactly once here — a second open just
/// to register the project with the pool would pay the schema/projection
/// catch-up twice for one user action.
pub(crate) struct OpenedProject {
    pub json: String,
    pub project_id: ProjectId,
    pub log: Arc<ProjectLog>,
}

impl OpenProjects {
    /// Opens (validates) the project directory and tracks it. Reopening an
    /// already-tracked project refreshes its row — idempotent, so a shell can
    /// call open on every activation without bookkeeping.
    pub(crate) fn open(
        &self,
        identity: &RuntimeIdentity,
        clock: &dyn WallClock,
        input: &ProjectPathInput,
    ) -> Result<OpenedProject, ApiError> {
        let _ = identity; // Identity joins the row when accounts land (m0-s08).
        let opened = project_ops::open_for_session(Path::new(&input.path))?;
        let row = OpenProjectRow {
            project_id: opened.project_id.to_hex(),
            path: input.path.clone(),
            name: opened.name,
            template: opened.template,
            format_version: opened.format_version,
            head_seq: opened.head_seq,
            opened_ts_ms: clock.now_ms(),
        };
        let mut rows = lock_recovering(&self.rows);
        let key = opened.project_id.into_bytes();
        if rows.len() >= OPEN_PROJECT_COUNT_MAX as usize && !rows.contains_key(&key) {
            return Err(ApiError {
                code: "open_project_limit",
                message: format!(
                    "this session already tracks {OPEN_PROJECT_COUNT_MAX} projects; \
                     close one before opening another"
                ),
                retriable: false,
            });
        }
        rows.insert(key, row.clone());
        drop(rows);
        Ok(OpenedProject {
            json: project_ops::to_json(&row)?,
            project_id: opened.project_id,
            log: opened.log,
        })
    }

    /// Releases a tracked project: the session row goes, and the caller
    /// unregisters it from the scheduler. Matching is by resolved path, so
    /// `./demo.pos` and an absolute path name the same project; an untracked
    /// path is a typed refusal rather than a fake success.
    pub(crate) fn close(&self, input: &ProjectPathInput) -> Result<(ProjectId, String), ApiError> {
        let wanted = resolved(&input.path);
        let mut rows = lock_recovering(&self.rows);
        let found = rows
            .iter()
            .find(|(_, row)| resolved(&row.path) == wanted)
            .map(|(key, _)| *key);
        let Some(key) = found else {
            return Err(ApiError {
                code: "not_open",
                message: format!(
                    "this session does not have {:?} open; nothing was closed",
                    input.path
                ),
                retriable: false,
            });
        };
        rows.remove(&key);
        let count = u32::try_from(rows.len()).unwrap_or(u32::MAX); // INVARIANT: the table is capped at OPEN_PROJECT_COUNT_MAX (64).
        drop(rows);
        let project_id = ProjectId::from_bytes(key);
        let json = project_ops::to_json(&ProjectCloseReport {
            project_id: project_id.to_hex(),
            path: input.path.clone(),
            open_project_count: count,
        })?;
        Ok((project_id, json))
    }

    /// The tracked rows, in project-id order.
    pub fn list(&self) -> Result<String, ApiError> {
        let rows = lock_recovering(&self.rows);
        let report = ProjectListReport {
            projects: rows.values().cloned().collect(),
            open_project_count_max: OPEN_PROJECT_COUNT_MAX,
        };
        drop(rows);
        project_ops::to_json(&report)
    }

    #[must_use]
    pub fn count(&self) -> usize {
        lock_recovering(&self.rows).len()
    }

    /// The tracked project paths in project-id order — the `cost.rollup`
    /// session scope walks exactly this list.
    #[must_use]
    pub(crate) fn paths(&self) -> Vec<String> {
        lock_recovering(&self.rows)
            .values()
            .map(|row| row.path.clone())
            .collect()
    }
}

/// The path as the filesystem resolves it, so two spellings of one directory
/// are one project. A directory that cannot be resolved (deleted between open
/// and close) falls back to its literal text rather than refusing the close.
fn resolved(path: &str) -> std::path::PathBuf {
    std::fs::canonicalize(path).unwrap_or_else(|_| std::path::PathBuf::from(path))
}

/// A poisoned lock means some earlier caller panicked mid-insert; the map
/// itself is always structurally valid (single insert/read operations), so
/// recovery is safe and refusing every later session read would only convert
/// one bug into a dead shell.
fn lock_recovering<T>(mutex: &Mutex<T>) -> std::sync::MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

#[cfg(test)]
mod tests {
    use super::{OPEN_PROJECT_COUNT_MAX, OpenProjects};
    use crate::ProjectPathInput;
    use crate::project_ops::RuntimeIdentity;
    use pos_foundation::ManualWallClock;

    #[test]
    fn opening_a_missing_directory_is_a_typed_error_and_tracks_nothing() {
        let projects = OpenProjects::default();
        let clock = ManualWallClock::starting_at(1_000);
        let input = ProjectPathInput {
            path: "missing-project-directory.pos".to_owned(),
        };
        let Err(error) = projects.open(&RuntimeIdentity::bootstrap(), &clock, &input) else {
            panic!("a missing directory must not open");
        };
        assert_eq!(error.code, "not_a_project");
        assert_eq!(projects.count(), 0);
        let list = projects.list().expect("list serializes");
        assert!(list.contains("\"projects\":[]"));
        assert!(list.contains(&format!("\"openProjectCountMax\":{OPEN_PROJECT_COUNT_MAX}")));
    }

    #[test]
    fn closing_a_project_this_session_never_opened_is_a_typed_refusal() {
        let projects = OpenProjects::default();
        let error = projects
            .close(&ProjectPathInput {
                path: "never-opened.pos".to_owned(),
            })
            .expect_err("closing an untracked path must refuse");
        assert_eq!(error.code, "not_open");
        assert!(!error.retriable);
    }
}
