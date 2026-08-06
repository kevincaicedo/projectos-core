//! m0-s05 CLI e2e oracles, driven entirely through the built binary:
//! create → append 100k synthetic events → export → re-open the export →
//! verify green; verify detects a deliberately corrupted blob and a
//! hand-mutated projection row, exits non-zero, and names the mismatch.

#![forbid(unsafe_code)]

use std::path::Path;
use std::process::{Command, Output};

/// The AC names 100k events explicitly; the 1M-scale run belongs to
/// `pos-bench` (m0-s16) with the same generator and seed discipline.
const E2E_EVENT_COUNT: u64 = 100_000;

fn pos(arguments: &[&str]) -> Output {
    Command::new(env!("CARGO_BIN_EXE_pos"))
        .args(arguments)
        .output()
        .expect("run pos binary")
}

fn expect_success(arguments: &[&str]) -> String {
    let output = pos(arguments);
    assert!(
        output.status.success(),
        "pos {} failed: {}",
        arguments.join(" "),
        String::from_utf8_lossy(&output.stderr)
    );
    String::from_utf8(output.stdout).expect("pos output is UTF-8")
}

fn path_text(path: &Path) -> String {
    path.to_str().expect("test paths are UTF-8").to_owned()
}

#[test]
fn create_seed_100k_export_reopen_verify_green() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project = path_text(&directory.path().join("acme.pos"));
    let export = path_text(&directory.path().join("acme-export.pos"));

    let created = expect_success(&["create", &project, "--name", "Acme Widgets"]);
    assert!(created.contains("\"name\":\"Acme Widgets\""));
    assert!(created.contains("\"headSeq\":1"));

    let seeded = expect_success(&[
        "seed-synthetic",
        &project,
        "--events",
        &E2E_EVENT_COUNT.to_string(),
        "--seed",
        "42",
    ]);
    assert!(seeded.contains(&format!("\"appended\":{E2E_EVENT_COUNT}")));

    let exported = expect_success(&["export", &project, "--out", &export]);
    assert!(exported.contains(&format!("\"eventCount\":{}", E2E_EVENT_COUNT + 1)));

    // The export must itself be a healthy project: inspect + full verify.
    // (The synthetic corpus contains ProjectRenamed facts, so the display
    // name is whatever the log last said — source and export must agree.)
    let source_inspected = expect_success(&["inspect", &project]);
    let inspected = expect_success(&["inspect", &export]);
    assert!(inspected.contains(&format!("\"eventCount\":{}", E2E_EVENT_COUNT + 1)));
    let name_of = |report: &str| {
        serde_json::from_str::<serde_json::Value>(report).expect("inspect JSON")["name"].clone()
    };
    assert_eq!(name_of(&source_inspected), name_of(&inspected));
    assert!(name_of(&inspected).is_string());
    let verified = expect_success(&["verify", &export]);
    assert!(verified.contains("\"clean\":true"));

    // The JSONL rendering exists and holds one line per event.
    let jsonl = std::fs::read_to_string(Path::new(&export).join("events.jsonl"))
        .expect("events.jsonl exists in the export");
    assert_eq!(jsonl.lines().count() as u64, E2E_EVENT_COUNT + 1);
    let first_line = jsonl.lines().next().expect("at least one line");
    assert!(first_line.contains("\"kind\":\"ProjectCreated\""));
    assert!(first_line.contains("\"seq\":1"));
}

#[test]
fn verify_names_a_corrupted_blob_and_exits_nonzero() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project_path = directory.path().join("blobby.pos");
    let project = path_text(&project_path);
    expect_success(&["create", &project]);
    expect_success(&["seed-synthetic", &project, "--events", "50", "--seed", "7"]);

    // Plant a blob whose address does not match its content — exactly what
    // on-disk corruption (or tampering) looks like to the sweep.
    let fake_hash = "ab".repeat(32);
    let blob_dir = project_path.join("blobs").join("ab").join("ab");
    std::fs::create_dir_all(&blob_dir).expect("create fan-out dir");
    std::fs::write(blob_dir.join(&fake_hash), b"not the hashed content").expect("plant blob");

    let output = pos(&["verify", &project]);
    assert!(
        !output.status.success(),
        "verify must exit non-zero on a corrupted blob"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("\"clean\":false"));
    assert!(stdout.contains("\"casCorruptCount\":1"));
    assert!(
        stderr.contains(&fake_hash),
        "the defect path must be named on stderr: {stderr}"
    );
}

#[test]
fn verify_names_a_hand_mutated_projection_row_and_exits_nonzero() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project_path = directory.path().join("tampered.pos");
    let project = path_text(&project_path);
    expect_success(&["create", &project]);
    expect_success(&["seed-synthetic", &project, "--events", "200", "--seed", "3"]);

    // Mutate a projection row behind the log's back — the corruption class
    // L1 calls "corruption with extra steps".
    let connection = rusqlite::Connection::open(project_path.join("project.db"))
        .expect("open project.db directly");
    connection
        .execute_batch("UPDATE proj_projects SET name = 'Renamed By Hand'")
        .expect("mutate projection row");
    drop(connection);

    let output = pos(&["verify", &project]);
    assert!(
        !output.status.success(),
        "verify must exit non-zero on a mutated projection"
    );
    let stdout = String::from_utf8_lossy(&output.stdout);
    let stderr = String::from_utf8_lossy(&output.stderr);
    assert!(stdout.contains("\"clean\":false"));
    assert!(stdout.contains("proj_projects"));
    assert!(
        stderr.contains("proj_projects"),
        "the mismatched table must be named on stderr: {stderr}"
    );

    // Recovery is a rebuild, not archaeology: open repairs nothing silently,
    // but an explicit re-verify after rebuild-by-reopen must still fail until
    // someone rebuilds. (The rebuild command arrives with m0-s06's surface.)
    let second = pos(&["verify", &project]);
    assert!(!second.status.success(), "verify stays red until rebuilt");
}

#[test]
fn open_and_inspect_report_the_same_healthy_project() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project = path_text(&directory.path().join("tiny.pos"));
    expect_success(&["create", &project, "--template", "generic"]);
    let opened = expect_success(&["open", &project]);
    let inspected = expect_success(&["inspect", &project]);
    assert_eq!(
        opened, inspected,
        "open and inspect share one registry read"
    );
    assert!(inspected.contains("\"formatVersion\":0"));
    assert!(inspected.contains("\"headSeq\":1"));
}

#[test]
fn export_refuses_an_existing_destination() {
    let directory = tempfile::tempdir().expect("tempdir");
    let project = path_text(&directory.path().join("src.pos"));
    let out = directory.path().join("occupied");
    std::fs::create_dir_all(&out).expect("occupy destination");
    expect_success(&["create", &project]);
    let output = pos(&["export", &project, "--out", &path_text(&out)]);
    assert!(!output.status.success());
    assert!(String::from_utf8_lossy(&output.stderr).contains("already_exists"));
}
