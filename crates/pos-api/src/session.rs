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
use pos_foundation::WallClock;
use serde::Serialize;
use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Mutex;
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
}

/// Bounded, deterministic session table. Interior mutability because the
/// registry dispatch surface is `&self` across every transport.
#[derive(Default)]
pub struct OpenProjects {
    rows: Mutex<BTreeMap<[u8; 16], OpenProjectRow>>,
}

impl OpenProjects {
    /// Opens (validates) the project directory and tracks it. Reopening an
    /// already-tracked project refreshes its row — idempotent, so a shell can
    /// call open on every activation without bookkeeping.
    pub fn open(
        &self,
        identity: &RuntimeIdentity,
        clock: &dyn WallClock,
        input: &ProjectPathInput,
    ) -> Result<String, ApiError> {
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
                     close the shell (session state is not persisted) before opening more"
                ),
                retriable: false,
            });
        }
        rows.insert(key, row.clone());
        project_ops::to_json(&row)
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
        let error = projects
            .open(
                &RuntimeIdentity::bootstrap(),
                &clock,
                &ProjectPathInput {
                    path: "missing-project-directory.pos".to_owned(),
                },
            )
            .expect_err("a missing directory must not open");
        assert_eq!(error.code, "not_a_project");
        assert_eq!(projects.count(), 0);
        let list = projects.list().expect("list serializes");
        assert!(list.contains("\"projects\":[]"));
        assert!(list.contains(&format!("\"openProjectCountMax\":{OPEN_PROJECT_COUNT_MAX}")));
    }
}
