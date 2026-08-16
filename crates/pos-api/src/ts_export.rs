//! The ts-rs generation pipeline (m0-s06, frozen by §3.2): every wire type
//! the UI consumes is generated from the Rust structs in this crate into
//! `apps/ui/src/api/gen/api/`. The UI imports from that directory only
//! (eslint-enforced); a hand-declared server type is the L12 bug class this
//! module exists to kill.
//!
//! `write` regenerates the tree; `check` regenerates into a scratch
//! directory and byte-compares, so CI fails on drift without mutating the
//! checkout. The registry name constants are emitted alongside the ts-rs
//! output from the same enums the dispatcher matches on — one source of
//! truth for names on both sides of the wire.

use crate::gateway_ops::{
    CostGroupRow, CostRollupInput, CostRollupReport, CostRollupRow, CostRollupTotals,
    ModelsPullInput, ModelsPullReport,
};
use crate::ingest_ops::{
    EvidenceListInput, EvidenceListReport, EvidenceRow, EvidenceStageRow, IngestReprocessInput,
    IngestReprocessReport, SourceHealthInput, SourceHealthReport, SourceHealthRow,
};
use crate::project_ops::{
    ProjectCreateInput, ProjectCreateReport, ProjectExportInput, ProjectExportReport,
    ProjectInspectReport, ProjectPathInput, ProjectSeedInput, ProjectSeedReport,
    ProjectVerifyReport,
};
use crate::run_ops::{
    RunBudgetDimensionWire, RunBudgetWire, RunControlInput, RunPauseReport, RunReport,
    RunResumeInput, RunStartInput, RunStepFrame, RunStepsInput, RunToolGrantInput,
    RunToolGrantModeWire, RunWorker,
};
use crate::sched_ops::{CronPreviewInput, CronPreviewReport, JobListInput, JobListReport, JobRow};
use crate::session::{HealthReport, OpenProjectRow, ProjectCloseReport, ProjectListReport};
use crate::stream::{SSE_RETRY_MS, STREAM_RESUME_WINDOW_LEN};
use crate::workers::WorkerStatusReport;
use crate::{API_SURFACE_VERSION, ApiError, CommandName, QueryName, StreamName};
use std::collections::BTreeMap;
use std::fs;
use std::path::Path;
use ts_rs::TS;

/// Every exported root type. A new wire type joins this list or it never
/// reaches the UI — which is the point: the list is the reviewed surface.
macro_rules! for_each_exported_type {
    ($macro:ident) => {
        $macro!(
            ApiError,
            ProjectCreateInput,
            ProjectPathInput,
            ProjectExportInput,
            ProjectSeedInput,
            ProjectCreateReport,
            ProjectInspectReport,
            ProjectVerifyReport,
            ProjectExportReport,
            ProjectSeedReport,
            OpenProjectRow,
            ProjectListReport,
            ProjectCloseReport,
            WorkerStatusReport,
            HealthReport,
            CostRollupInput,
            CostRollupRow,
            CostGroupRow,
            CostRollupTotals,
            CostRollupReport,
            ModelsPullInput,
            ModelsPullReport,
            RunWorker,
            RunBudgetDimensionWire,
            RunBudgetWire,
            RunToolGrantModeWire,
            RunToolGrantInput,
            RunStartInput,
            RunControlInput,
            RunResumeInput,
            RunStepsInput,
            RunStepFrame,
            RunPauseReport,
            RunReport,
            JobListInput,
            JobRow,
            JobListReport,
            CronPreviewInput,
            CronPreviewReport,
            EvidenceListInput,
            EvidenceStageRow,
            EvidenceRow,
            EvidenceListReport,
            SourceHealthInput,
            SourceHealthRow,
            SourceHealthReport,
            IngestReprocessInput,
            IngestReprocessReport
        )
    };
}

/// Writes the complete generated tree into `dir`, replacing what is there.
///
/// # Errors
///
/// Returns a path-naming message when any file cannot be written.
pub fn write_typescript_api(dir: &Path) -> Result<(), String> {
    fs::create_dir_all(dir).map_err(|error| format!("create {}: {error}", dir.display()))?;
    // Clear stale files first so a renamed/removed type cannot survive as an
    // orphan the UI keeps importing.
    for entry in fs::read_dir(dir).map_err(|error| format!("read {}: {error}", dir.display()))? {
        let entry = entry.map_err(|error| format!("read {}: {error}", dir.display()))?;
        let path = entry.path();
        if path.extension().is_some_and(|extension| extension == "ts") {
            fs::remove_file(&path)
                .map_err(|error| format!("remove {}: {error}", path.display()))?;
        }
    }
    for (name, content) in generated_files(dir)? {
        let path = dir.join(&name);
        fs::write(&path, content).map_err(|error| format!("write {}: {error}", path.display()))?;
    }
    Ok(())
}

/// Compares the on-disk tree against a fresh generation.
///
/// # Errors
///
/// Returns one line per stale, missing, or orphaned file — the m0-s06
/// staleness gate output.
pub fn check_typescript_api(dir: &Path) -> Result<(), Vec<String>> {
    let scratch = tempfile::tempdir().map_err(|error| vec![format!("scratch dir: {error}")])?;
    let expected = generated_files(scratch.path()).map_err(|error| vec![error])?;
    let mut defects = Vec::new();
    for (name, content) in &expected {
        let path = dir.join(name);
        match fs::read_to_string(&path) {
            Ok(current) if current == *content => {}
            Ok(_) => defects.push(format!("{} is stale", path.display())),
            Err(_) => defects.push(format!("{} is missing", path.display())),
        }
    }
    match fs::read_dir(dir) {
        Ok(entries) => {
            for entry in entries.flatten() {
                let file_name = entry.file_name().to_string_lossy().into_owned();
                if file_name.ends_with(".ts") && !expected.contains_key(&file_name) {
                    defects.push(format!(
                        "{} is orphaned (no generating type)",
                        entry.path().display()
                    ));
                }
            }
        }
        Err(error) => defects.push(format!("read {}: {error}", dir.display())),
    }
    if defects.is_empty() {
        Ok(())
    } else {
        Err(defects)
    }
}

/// Produces the full generated file set, deterministically ordered by name.
/// ts-rs writes through the filesystem, so generation runs against `scratch`
/// and the results are read back — the caller decides where they land.
fn generated_files(scratch: &Path) -> Result<BTreeMap<String, String>, String> {
    // Deliberately not `Config::from_env()`: environment variables must not
    // be able to reshape the checked-in wire types.
    let config = ts_rs::Config::new().with_out_dir(scratch);
    macro_rules! export_each {
        ($($type:ty),+) => {
            $(
                <$type as TS>::export_all(&config)
                    .map_err(|error| format!("ts-rs export {}: {error}", stringify!($type)))?;
            )+
        };
    }
    for_each_exported_type!(export_each);

    let mut files = BTreeMap::new();
    for entry in
        fs::read_dir(scratch).map_err(|error| format!("read {}: {error}", scratch.display()))?
    {
        let entry = entry.map_err(|error| format!("read {}: {error}", scratch.display()))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if !name.ends_with(".ts") {
            continue;
        }
        let content = fs::read_to_string(entry.path())
            .map_err(|error| format!("read {}: {error}", entry.path().display()))?;
        files.insert(name, content);
    }
    files.insert("names.ts".to_owned(), names_ts());
    files.insert("index.ts".to_owned(), index_ts(&files));
    Ok(files)
}

/// The registry vocabulary, emitted from the same enums the dispatcher
/// matches on. `as const` arrays give the UI literal unions for free.
fn names_ts() -> String {
    let mut out = String::from(
        "// @generated by `cargo run -p pos-api --bin export-api-types`; do not edit.\n\n",
    );
    out.push_str(&format!(
        "export const API_SURFACE_VERSION = {API_SURFACE_VERSION};\n\n"
    ));
    push_name_array(
        &mut out,
        "QUERY_NAMES",
        QueryName::ALL.map(QueryName::as_str),
    );
    out.push_str("export type ApiQueryName = (typeof QUERY_NAMES)[number];\n\n");
    push_name_array(
        &mut out,
        "COMMAND_NAMES",
        CommandName::ALL.map(CommandName::as_str),
    );
    out.push_str("export type ApiCommandName = (typeof COMMAND_NAMES)[number];\n\n");
    push_name_array(
        &mut out,
        "STREAM_NAMES",
        StreamName::ALL.map(StreamName::as_str),
    );
    out.push_str("export type ApiStreamName = (typeof STREAM_NAMES)[number];\n\n");
    out.push_str(&format!(
        "// SSE stream framing constants (pos-api stream.rs).\n\
         export const STREAM_RESUME_WINDOW_LEN = {STREAM_RESUME_WINDOW_LEN};\n\
         export const SSE_RETRY_MS = {SSE_RETRY_MS};\n"
    ));
    out
}

fn push_name_array<const N: usize>(out: &mut String, constant: &str, names: [&str; N]) {
    out.push_str(&format!("export const {constant} = [\n"));
    for name in names {
        out.push_str(&format!("  \"{name}\",\n"));
    }
    out.push_str("] as const;\n");
}

/// One import surface for the UI: `import {{ ... }} from "../gen/api"`.
fn index_ts(files: &BTreeMap<String, String>) -> String {
    let mut out = String::from(
        "// @generated by `cargo run -p pos-api --bin export-api-types`; do not edit.\n\n",
    );
    for name in files.keys() {
        let stem = name.trim_end_matches(".ts");
        out.push_str(&format!("export * from \"./{stem}\";\n"));
    }
    out
}

#[cfg(test)]
mod tests {
    use super::{check_typescript_api, write_typescript_api};
    use std::fs;

    #[test]
    fn a_fresh_generation_passes_its_own_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_typescript_api(dir.path()).expect("generation succeeds");
        check_typescript_api(dir.path()).expect("a fresh tree is current");
        // The tree actually contains the surface: envelope, inputs, names.
        for expected in [
            "ApiErrorEnvelope.ts",
            "ProjectCreateInput.ts",
            "names.ts",
            "index.ts",
        ] {
            assert!(
                dir.path().join(expected).is_file(),
                "{expected} missing from the generated tree"
            );
        }
    }

    /// The m0-s06 staleness AC: a drifted, deleted, or orphaned file fails
    /// the check, each named in the output.
    #[test]
    fn drift_deletion_and_orphans_each_fail_the_check() {
        let dir = tempfile::tempdir().expect("tempdir");
        write_typescript_api(dir.path()).expect("generation succeeds");

        let envelope = dir.path().join("ApiErrorEnvelope.ts");
        let pristine = fs::read_to_string(&envelope).expect("generated file exists");
        fs::write(&envelope, format!("{pristine}\n// hand edit\n")).expect("mutate");
        let defects = check_typescript_api(dir.path()).expect_err("drift must fail");
        assert!(
            defects
                .iter()
                .any(|line| line.contains("ApiErrorEnvelope.ts"))
        );
        assert!(defects.iter().any(|line| line.contains("stale")));

        fs::remove_file(&envelope).expect("delete");
        let defects = check_typescript_api(dir.path()).expect_err("deletion must fail");
        assert!(defects.iter().any(|line| line.contains("missing")));

        write_typescript_api(dir.path()).expect("regeneration heals the tree");
        fs::write(dir.path().join("HandDeclared.ts"), "export type X = 1;\n").expect("orphan");
        let defects = check_typescript_api(dir.path()).expect_err("an orphan must fail");
        assert!(defects.iter().any(|line| line.contains("orphaned")));
    }

    #[test]
    fn generation_is_deterministic_across_runs() {
        let first = tempfile::tempdir().expect("tempdir");
        let second = tempfile::tempdir().expect("tempdir");
        write_typescript_api(first.path()).expect("generation succeeds");
        write_typescript_api(second.path()).expect("generation succeeds");
        let read = |dir: &std::path::Path| {
            let mut all = Vec::new();
            for entry in fs::read_dir(dir).expect("readable") {
                let entry = entry.expect("entry");
                all.push((
                    entry.file_name().to_string_lossy().into_owned(),
                    fs::read_to_string(entry.path()).expect("readable file"),
                ));
            }
            all.sort();
            all
        };
        assert_eq!(read(first.path()), read(second.path()));
    }
}
