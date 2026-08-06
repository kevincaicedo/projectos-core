//! The m0-s05 project operations behind the typed surface: create, inspect,
//! verify, export, and the deterministic synthetic seeder the CLI e2e and
//! `pos-bench` share. Inputs and outputs are serde types with camelCase wire
//! names; every result serializes deterministically (struct field order), so
//! transports can forward bytes without reshaping them (L12).

use crate::ApiError;
use pos_domain::{DomainEvent, ProjectCreatedBody, SyntheticEvents, v0_registry};
use pos_foundation::{DeviceId, ProjectId, UserId, WallClock};
use pos_log::{Actor, AppendRequest, Event, LogConfig, LogError, ProjectLog};
use pos_store::{Manifest, ProjectStore, StoreError};
use serde::{Deserialize, Serialize};
use std::fs;
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};

/// Synthetic events appended per transaction: large enough to amortize the
/// FULL-sync commit, small enough to keep memory flat at 1M events.
const SEED_BATCH_LEN: usize = 1_000;
/// Hard cap per seed call (L8): a bigger corpus is several calls, and a typo
/// like `--events 100000000000` fails fast instead of filling the disk.
const SEED_EVENT_COUNT_MAX: u64 = 2_000_000;

/// Process-local bootstrap identity for CLI/desktop operation before real
/// account/device identity lands (m0-s07/m0-s08). Stable so per-device
/// lamport chains stay monotonic across invocations; never all-zero so a
/// zeroed row is visibly distinct from a real append.
pub(crate) struct RuntimeIdentity {
    pub device: DeviceId,
    pub user: UserId,
}

impl RuntimeIdentity {
    pub(crate) fn bootstrap() -> Self {
        Self {
            device: DeviceId::from_bytes([0x01; 16]),
            user: UserId::from_bytes([0x01; 16]),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectCreateInput {
    pub path: String,
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default = "default_template")]
    pub template: String,
}

fn default_template() -> String {
    "generic".to_owned()
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectPathInput {
    pub path: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectExportInput {
    pub path: String,
    pub out: String,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ProjectSeedInput {
    pub path: String,
    pub event_count: u64,
    #[serde(default)]
    pub seed: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectCreateReport {
    path: String,
    project_id: String,
    name: String,
    template: String,
    head_seq: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectInspectReport {
    path: String,
    project_id: String,
    format_version: u32,
    template: String,
    created_ts_ms: u64,
    name: Option<String>,
    event_count: u64,
    head_seq: u64,
    snapshot_count: u64,
    latest_snapshot_seq: Option<u64>,
    snapshot_cadence_events: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectVerifyReport {
    clean: bool,
    events_replayed: u64,
    applied_seq: u64,
    head_seq: u64,
    mismatched_tables: Vec<String>,
    cas_blob_count: u64,
    cas_corrupt_count: u64,
    cas_misplaced_count: u64,
    cas_temp_leftover_count: u64,
    cas_defect_paths: Vec<String>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectExportReport {
    out: String,
    event_count: u64,
    blob_count: u64,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectSeedReport {
    appended: u64,
    head_seq: u64,
}

/// One JSONL line of the exported log (`events.jsonl`; format-spec §5).
/// Bytes render as lowercase hex — the export is a portability document, so
/// every field must survive a text pipeline unmangled.
#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportedEventLine {
    seq: u64,
    device: String,
    lamport: u64,
    ts_ms: u64,
    actor_kind: &'static str,
    actor_id: String,
    kind: String,
    body_cbor_hex: String,
    refs: Vec<ExportedRef>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ExportedRef {
    entity: String,
    id: String,
}

pub(crate) fn create(
    identity: &RuntimeIdentity,
    clock: &dyn WallClock,
    input: &ProjectCreateInput,
) -> Result<String, ApiError> {
    let path = PathBuf::from(&input.path);
    let name = input.name.clone().unwrap_or_else(|| default_name(&path));
    let store =
        ProjectStore::create(&path, &input.template, clock).map_err(|error| store_error(&error))?;
    let project_id = store.manifest().project_id;
    let template = store.manifest().template.clone();
    let log = open_log_over(store)?;
    let created = DomainEvent::ProjectCreated(ProjectCreatedBody::V1 {
        project_id,
        name: name.clone(),
        template: template.clone(),
    })
    .into_request(identity.device, Actor::User(identity.user))
    .map_err(log_error)?;
    let head = log.append(created, clock).map_err(log_error)?;
    to_json(&ProjectCreateReport {
        path: input.path.clone(),
        project_id: project_id.to_hex(),
        name,
        template,
        head_seq: head.value(),
    })
}

pub(crate) fn inspect(input: &ProjectPathInput) -> Result<String, ApiError> {
    let log = open_log(Path::new(&input.path))?;
    let manifest = log.store().manifest().clone();
    let head = log.head().map_err(log_error)?;
    let event_count = log.event_count().map_err(log_error)?;
    let snapshots = log.snapshot_state().map_err(log_error)?;
    let name = read_project_name(&log, manifest.project_id)?;
    to_json(&ProjectInspectReport {
        path: input.path.clone(),
        project_id: manifest.project_id.to_hex(),
        format_version: manifest.format_version,
        template: manifest.template,
        created_ts_ms: manifest.created_ts_ms,
        name,
        event_count,
        head_seq: head.value(),
        snapshot_count: snapshots.snapshot_count,
        latest_snapshot_seq: snapshots.latest_snapshot_seq,
        snapshot_cadence_events: snapshots.cadence_events,
    })
}

pub(crate) fn verify(input: &ProjectPathInput) -> Result<String, ApiError> {
    let log = open_log(Path::new(&input.path))?;
    let projections = log.verify_projections().map_err(log_error)?;
    let cas = log
        .store()
        .blobs()
        .verify()
        .map_err(|error| store_error(&error))?;
    to_json(&ProjectVerifyReport {
        clean: projections.is_clean() && cas.is_clean(),
        events_replayed: projections.events_replayed,
        applied_seq: projections.applied_seq,
        head_seq: projections.head_seq,
        mismatched_tables: projections
            .mismatched_tables()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        cas_blob_count: cas.blob_count,
        cas_corrupt_count: cas.corrupt_count,
        cas_misplaced_count: cas.misplaced_count,
        cas_temp_leftover_count: cas.temp_leftover_count,
        cas_defect_paths: cas
            .defect_paths
            .iter()
            .map(|path| path.display().to_string())
            .collect(),
    })
}

/// Export = a copy that is itself a valid project directory (F2/F45: the
/// export must re-open and re-verify) plus `events.jsonl`, the documented
/// text rendering of the log. The manifest is written last as the
/// completeness marker, mirroring create.
pub(crate) fn export(input: &ProjectExportInput) -> Result<String, ApiError> {
    let log = open_log(Path::new(&input.path))?;
    let out_root = PathBuf::from(&input.out);
    if out_root.exists() {
        return Err(store_error(&StoreError::AlreadyExists { path: out_root }));
    }
    fs::create_dir_all(&out_root)
        .map_err(|source| io_error("create export directory", &out_root, &source))?;

    // 1. A consistent single-file database copy (VACUUM INTO reads through
    //    the WAL, so no checkpoint dance is needed).
    let db_out = out_root.join(pos_store::PROJECT_DB_FILE_NAME);
    let db_out_text = db_out
        .to_str()
        .ok_or_else(|| ApiError {
            code: "invalid_input",
            message: "export destination path is not valid UTF-8".to_owned(),
            retriable: false,
        })?
        .to_owned();
    log.store()
        .db()
        .with_writer("vacuum into export", move |connection| {
            connection.execute("VACUUM INTO ?1", [db_out_text])?;
            Ok(())
        })
        .map_err(|error| store_error(&error))?;

    // 2. Blobs, byte-for-byte at the same addresses.
    let blob_count = log
        .store()
        .blobs()
        .copy_all_into(&out_root.join(pos_store::BLOBS_DIRECTORY_NAME))
        .map_err(|error| store_error(&error))?;

    // 3. The JSONL rendering of the log, streamed.
    let events_path = out_root.join("events.jsonl");
    let events_file = fs::File::create(&events_path)
        .map_err(|source| io_error("create events.jsonl", &events_path, &source))?;
    let mut writer = BufWriter::new(events_file);
    let mut event_count = 0_u64;
    log.for_each_event(|event| {
        let line = exported_line(event)?;
        writer
            .write_all(line.as_bytes())
            .and_then(|()| writer.write_all(b"\n"))
            .map_err(|source| {
                LogError::Store(StoreError::Io {
                    context: "write events.jsonl",
                    path: events_path.clone(),
                    source,
                })
            })?;
        event_count += 1;
        Ok(())
    })
    .map_err(log_error)?;
    writer
        .flush()
        .map_err(|source| io_error("flush events.jsonl", &events_path, &source))?;

    // 4. Manifest last: an interrupted export is debris, never a project.
    Manifest::read(Path::new(&input.path))
        .and_then(|manifest| manifest.write(&out_root))
        .map_err(|error| store_error(&error))?;

    to_json(&ProjectExportReport {
        out: input.out.clone(),
        event_count,
        blob_count,
    })
}

pub(crate) fn seed_synthetic(
    clock: &dyn WallClock,
    input: &ProjectSeedInput,
) -> Result<String, ApiError> {
    if input.event_count > SEED_EVENT_COUNT_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "eventCount {} exceeds the per-call bound {SEED_EVENT_COUNT_MAX}",
                input.event_count
            ),
            retriable: false,
        });
    }
    let log = open_log(Path::new(&input.path))?;
    let project_id = log.store().manifest().project_id;
    let mut generator = SyntheticEvents::new(input.seed, project_id);
    let mut appended = 0_u64;
    while appended < input.event_count {
        let batch_len = SEED_BATCH_LEN
            .min(usize::try_from(input.event_count - appended).unwrap_or(SEED_BATCH_LEN));
        let requests: Vec<AppendRequest> = (0..batch_len)
            .map(|_| generator.next_request())
            .collect::<Result<_, _>>()
            .map_err(log_error)?;
        log.append_batch(&requests, clock).map_err(log_error)?;
        appended += batch_len as u64;
    }
    let head = log.head().map_err(log_error)?;
    to_json(&ProjectSeedReport {
        appended,
        head_seq: head.value(),
    })
}

fn open_log(root: &Path) -> Result<ProjectLog, ApiError> {
    let store = ProjectStore::open(root).map_err(|error| store_error(&error))?;
    open_log_over(store)
}

fn open_log_over(store: ProjectStore) -> Result<ProjectLog, ApiError> {
    let registry = v0_registry().map_err(log_error)?;
    ProjectLog::open(store, registry, LogConfig::default()).map_err(log_error)
}

fn read_project_name(log: &ProjectLog, project_id: ProjectId) -> Result<Option<String>, ApiError> {
    log.store()
        .db()
        .with_reader("read project name", |connection| {
            use pos_store::rusqlite::OptionalExtension;
            connection
                .query_row(
                    "SELECT name FROM proj_projects WHERE project_id = ?1",
                    [project_id.into_bytes().to_vec()],
                    |row| row.get(0),
                )
                .optional()
        })
        .map_err(|error| store_error(&error))
}

fn exported_line(event: &Event) -> Result<String, LogError> {
    let (actor_kind, actor_id) = match event.actor {
        Actor::User(id) => ("user", id.to_hex()),
        Actor::Agent(id) => ("agent", id.to_hex()),
        Actor::System(id) => ("system", id.to_hex()),
    };
    let line = ExportedEventLine {
        seq: event.seq.value(),
        device: event.device.to_hex(),
        lamport: event.lamport,
        ts_ms: event.ts_ms,
        actor_kind,
        actor_id,
        kind: event.kind.as_str().to_owned(),
        body_cbor_hex: hex_encode(&event.body),
        refs: event
            .refs
            .iter()
            .map(|entity_ref| ExportedRef {
                entity: entity_ref.entity.clone(),
                id: hex_encode(&entity_ref.id),
            })
            .collect(),
    };
    serde_json::to_string(&line).map_err(|error| LogError::DecodeEvent {
        seq: event.seq.value(),
        reason: format!("export serialization failed: {error}"),
    })
}

fn hex_encode(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut hex = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(hex, "{byte:02x}").expect("writing hex into a String cannot fail"); // INVARIANT: fmt::Write on String is infallible.
    }
    hex
}

fn default_name(path: &Path) -> String {
    path.file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .filter(|stem| !stem.is_empty())
        .unwrap_or_else(|| "Untitled Project".to_owned())
}

pub(crate) fn to_json(value: &impl Serialize) -> Result<String, ApiError> {
    serde_json::to_string(value).map_err(|error| ApiError {
        code: "serialization_failure",
        message: error.to_string(),
        retriable: false,
    })
}

pub(crate) fn parse_input<'de, T: Deserialize<'de>>(input_json: &'de str) -> Result<T, ApiError> {
    serde_json::from_str(input_json).map_err(|error| ApiError {
        code: "invalid_input",
        // serde's message carries field/position; the raw input is echoed
        // nowhere so a caller cannot reflect content through an error body.
        message: error.to_string(),
        retriable: false,
    })
}

fn io_error(context: &'static str, path: &Path, source: &std::io::Error) -> ApiError {
    ApiError {
        code: "storage_failure",
        message: format!("{context}: {}: {source}", path.display()),
        retriable: false,
    }
}

pub(crate) fn store_error(error: &StoreError) -> ApiError {
    let code = match error {
        StoreError::AlreadyExists { .. } => "already_exists",
        StoreError::NotAProject { .. } => "not_a_project",
        StoreError::ManifestInvalid { .. } => "manifest_invalid",
        StoreError::DurabilityLost { .. } | StoreError::FailStopped { .. } => "durability_failure",
        StoreError::ExtensionMissing { .. } => "engine_incomplete",
        StoreError::BlobMissing { .. } | StoreError::BlobCorrupt { .. } => "blob_integrity",
        _ => "storage_failure",
    };
    ApiError {
        code,
        message: error.to_string(),
        retriable: false,
    }
}

pub(crate) fn log_error(error: LogError) -> ApiError {
    match error {
        LogError::Store(store) => store_error(&store),
        LogError::StateAhead { .. } => ApiError {
            code: "state_mutated",
            message: error.to_string(),
            retriable: false,
        },
        LogError::NotYetSupported { .. } => ApiError {
            code: "not_yet_supported",
            message: error.to_string(),
            retriable: false,
        },
        other => ApiError {
            code: "log_failure",
            message: other.to_string(),
            retriable: false,
        },
    }
}
