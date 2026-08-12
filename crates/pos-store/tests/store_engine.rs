//! m0-s04 oracles: extension loading proven at open (the risky FFI path),
//! CAS round-trip/dedup/bounded-buffer properties, verify sweeps that catch
//! deliberate corruption, and typed fail-stop on injected commit failure.

#![forbid(unsafe_code)]

use pos_foundation::ManualWallClock;
use pos_store::{
    CAS_WRITE_BUFFER_CAP, FaultAction, FaultPlan, FaultPoint, ProjectStore, StoreError,
    StoreOptions,
};
use std::fs;
use std::io::Read;

fn created_store(directory: &tempfile::TempDir) -> Result<ProjectStore, StoreError> {
    let root = directory.path().join("probe.pos");
    ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(1_000))
}

/// Deterministic pseudo-content: enough structure to defeat accidental
/// equality, no RNG dependency.
fn synthetic_bytes(len: usize, salt: u8) -> Vec<u8> {
    (0..len)
        .map(|index| {
            let mixed = index
                .wrapping_mul(31)
                .wrapping_add(usize::from(salt).wrapping_mul(97));
            u8::try_from(mixed & 0xff).unwrap_or(0)
        })
        .collect()
}

#[test]
fn fts5_and_sqlite_vec_actually_work_not_just_load() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = created_store(&directory).expect("create project store");

    // FTS5: virtual table + MATCH round trip.
    let matches: i64 = store
        .db()
        .with_writer("fts5 probe", |connection| {
            connection.execute_batch(
                "CREATE VIRTUAL TABLE fts_probe USING fts5(content);
                 INSERT INTO fts_probe(content) VALUES ('evidence before opinions');",
            )?;
            connection.query_row(
                "SELECT count(*) FROM fts_probe WHERE fts_probe MATCH 'evidence'",
                [],
                |row| row.get(0),
            )
        })
        .expect("fts5 is compiled in and queryable");
    assert_eq!(matches, 1);

    // sqlite-vec: vec0 virtual table + KNN query round trip.
    let nearest: i64 = store
        .db()
        .with_writer("vec0 probe", |connection| {
            connection.execute_batch(
                "CREATE VIRTUAL TABLE vec_probe USING vec0(embedding float[4]);
                 INSERT INTO vec_probe(rowid, embedding) VALUES (1, '[1,1,1,1]');
                 INSERT INTO vec_probe(rowid, embedding) VALUES (2, '[9,9,9,9]');",
            )?;
            connection.query_row(
                "SELECT rowid FROM vec_probe WHERE embedding MATCH '[1,1,1,2]' \
                 ORDER BY distance LIMIT 1",
                [],
                |row| row.get(0),
            )
        })
        .expect("vec0 is registered and queryable");
    assert_eq!(nearest, 1);
}

#[test]
fn wal_mode_and_reader_pool_serve_queries() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = created_store(&directory).expect("create project store");
    let mode: String = store
        .db()
        .with_reader("journal mode", |connection| {
            connection.query_row("PRAGMA journal_mode", [], |row| row.get(0))
        })
        .expect("reader connection works");
    assert_eq!(mode, "wal");
    let synchronous: i64 = store
        .db()
        .with_reader("synchronous level", |connection| {
            connection.query_row("PRAGMA synchronous", [], |row| row.get(0))
        })
        .expect("reader connection works");
    assert_eq!(synchronous, 2, "FULL is the documented durability choice");
}

#[test]
fn cas_round_trips_dedups_and_stays_under_the_buffer_cap() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = created_store(&directory).expect("create project store");
    // 3× the cap forces multiple flushes; odd chunk sizes cross boundaries.
    let content = synthetic_bytes(3 * CAS_WRITE_BUFFER_CAP + 12_345, 7);

    let mut writer = store.blobs().writer().expect("start blob write");
    for chunk in content.chunks(1_048_573) {
        writer.append(chunk).expect("append chunk");
    }
    assert!(
        writer.buffered_len_max() <= CAS_WRITE_BUFFER_CAP,
        "streaming write exceeded the stated 8 MiB memory cap (L8)"
    );
    let hash = writer.finish().expect("publish blob");

    // Round trip.
    let mut read_back = Vec::new();
    store
        .blobs()
        .open_blob(hash)
        .expect("blob is present")
        .read_to_end(&mut read_back)
        .expect("read blob");
    assert_eq!(read_back, content);

    // Dedup: identical content converges on one file, byte-for-byte once.
    let duplicate_hash = store.blobs().write_bytes(&content).expect("rewrite blob");
    assert_eq!(duplicate_hash.to_hex(), hash.to_hex());
    let report = store.blobs().verify().expect("verify sweep");
    assert_eq!(report.blob_count, 1, "same content stored exactly once");
    assert!(report.is_clean());
    assert_eq!(report.temp_leftover_count, 0);
}

#[test]
fn reopening_a_project_cannot_sweep_an_active_same_process_blob_write() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("concurrent-open.pos");
    let store = ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(1_000))
        .expect("create project store");
    let mut writer = store.blobs().writer().expect("start active blob write");
    writer
        .append(b"content remains owned by the active writer")
        .expect("buffer content");

    // A live-feed/read handle opens the same project while the worker owns
    // the CAS temp file. Recovery may sweep dead-process debris, not this
    // process's active write.
    let reopened = ProjectStore::open(&root).expect("concurrent read handle opens");
    let hash = writer.finish().expect("active writer still publishes");
    reopened
        .blobs()
        .verify_blob(hash)
        .expect("published content verifies through the other handle");
}

#[test]
fn verify_names_a_corrupted_blob_and_a_misplaced_file() {
    let directory = tempfile::tempdir().expect("tempdir");
    let store = created_store(&directory).expect("create project store");
    let hash = store
        .blobs()
        .write_bytes(b"authentic content")
        .expect("write blob");

    // Corrupt the stored bytes behind the store's back.
    let hex = hash.to_hex();
    let blob_path = store
        .root()
        .join("blobs")
        .join(&hex[0..2])
        .join(&hex[2..4])
        .join(&hex);
    fs::write(&blob_path, b"tampered content").expect("overwrite blob file");
    let error = store
        .blobs()
        .verify_blob(hash)
        .expect_err("tampering must be detected");
    assert!(matches!(error, StoreError::BlobCorrupt { .. }));

    // A file whose name is not its content's address is misplaced.
    fs::write(blob_path.with_file_name("not-a-hash"), b"debris").expect("plant misplaced file");
    let report = store.blobs().verify().expect("verify sweep");
    assert_eq!(report.corrupt_count, 1);
    assert_eq!(report.misplaced_count, 1);
    assert!(!report.is_clean());
    assert_eq!(report.defect_paths.len(), 2);
}

#[test]
fn injected_commit_failure_is_typed_fail_stop_never_silent_success() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("failstop.pos");
    ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(1_000))
        .expect("create project store");
    let store = ProjectStore::open_with_options(
        &root,
        StoreOptions {
            faults: Some(FaultPlan {
                point: FaultPoint::WalCommit,
                action: FaultAction::FailOperation,
            }),
        },
    )
    .expect("reopen with armed fault");

    let error = store
        .db()
        .write_transaction("crash-probe insert", |transaction| {
            transaction
                .execute_batch("CREATE TABLE fail_probe(x INTEGER)")
                .map_err(|source| StoreError::Sqlite {
                    context: "create probe table",
                    source,
                })
        })
        .expect_err("injected fsync failure must surface");
    assert!(
        matches!(error, StoreError::DurabilityLost { .. }),
        "expected DurabilityLost, got {error:?}"
    );

    // The project is fail-stopped for the rest of the process: no silent
    // continuation after a durability lie.
    let follow_up = store
        .db()
        .with_writer("post-failure write", |connection| {
            connection.execute_batch("CREATE TABLE after_failure(x INTEGER)")
        })
        .expect_err("fail-stopped store must refuse");
    assert!(matches!(follow_up, StoreError::FailStopped { .. }));

    // Reopening recovers: the failed transaction never became durable state.
    let reopened = ProjectStore::open(&root).expect("reopen after fail-stop");
    let table_count: i64 = reopened
        .db()
        .with_reader("probe table count", |connection| {
            connection.query_row(
                "SELECT count(*) FROM sqlite_master WHERE name IN ('fail_probe','after_failure')",
                [],
                |row| row.get(0),
            )
        })
        .expect("read after reopen");
    assert_eq!(table_count, 0, "no partial transaction survived");
}

#[test]
fn create_refuses_an_occupied_destination() {
    let directory = tempfile::tempdir().expect("tempdir");
    let root = directory.path().join("occupied.pos");
    fs::create_dir_all(&root).expect("make destination");
    fs::write(root.join("keep.txt"), b"user data").expect("occupy destination");
    let error = ProjectStore::create(&root, "generic", &ManualWallClock::starting_at(0))
        .expect_err("must not adopt foreign directories");
    assert!(matches!(error, StoreError::AlreadyExists { .. }));
}
