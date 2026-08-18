//! The m0-s10/m0-s11 surface slice: the event-appending cost-ledger sink
//! (the gateway's `CostLedger` persisted as `ModelCallCompleted` facts, L1),
//! the `cost.rollup` query over `proj_model_calls`, and the `models.pull`
//! command over the gateway model manager.

use crate::project_ops::{self};
use crate::{ApiError, session::OpenProjects};
use pos_domain::{DomainEvent, ModelCallCompletedBody};
use pos_foundation::WallClock;
use pos_gateway::{
    CostLedger, LedgerError, LoopbackHttpTransport, ModelCallRecord, ModelManifest, PullConsent,
    TlsHttpTransport, pull_model,
};
use pos_log::{Actor, ProjectLog};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use ts_rs::TS;

/// Appends every gateway ledger record to an open project log as a
/// `ModelCallCompleted` event — the billing meter is a projection of the
/// log, so replay, export, and `cost.rollup` all see the same rows. The
/// owner of the open log (the m0-s12 harness; tests today) composes this
/// per run.
pub struct EventCostLedger<'log> {
    log: &'log ProjectLog,
    device: pos_foundation::DeviceId,
    actor: Actor,
    clock: &'log dyn WallClock,
}

impl<'log> EventCostLedger<'log> {
    #[must_use]
    pub fn new(
        log: &'log ProjectLog,
        device: pos_foundation::DeviceId,
        actor: Actor,
        clock: &'log dyn WallClock,
    ) -> Self {
        Self {
            log,
            device,
            actor,
            clock,
        }
    }
}

impl CostLedger for EventCostLedger<'_> {
    fn record(&self, record: &ModelCallRecord) -> Result<(), LedgerError> {
        let event = DomainEvent::ModelCallCompleted(ModelCallCompletedBody::V1 {
            project_id: record.project,
            feature: record.feature.clone(),
            agent: record.agent.clone(),
            provider: record.provider.to_owned(),
            credential_class: record.credential_class.to_owned(),
            model: record.model.clone(),
            tokens_in: record.tokens_in,
            tokens_out: record.tokens_out,
            wall_ms: record.wall_ms,
            provider_cost_kind: record.provider_cost_kind.as_str().to_owned(),
            usd_micros: record.usd_micros,
            outcome: record.outcome.clone(),
        });
        let request = event
            .into_request(self.device, self.actor)
            .map_err(|error| LedgerError {
                reason: error.to_string(),
            })?;
        self.log
            .append(request, self.clock)
            .map(|_| ())
            .map_err(|error| LedgerError {
                reason: error.to_string(),
            })
    }
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct CostRollupInput {
    /// One project directory, or absent for every project this session has
    /// open (the UI cost surface's scope).
    #[serde(default)]
    pub path: Option<String>,
}

/// One aggregated ledger group. Money stays integer micro-USD end to end.
#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CostRollupRow {
    pub project_id: String,
    pub feature: String,
    pub agent: Option<String>,
    pub provider: String,
    pub credential_class: String,
    pub model: String,
    pub provider_cost_kind: String,
    #[ts(type = "number")]
    pub calls: u64,
    #[ts(type = "number")]
    pub tokens_in: u64,
    #[ts(type = "number")]
    pub tokens_out: u64,
    #[ts(type = "number")]
    pub wall_ms_total: u64,
    #[ts(type = "number")]
    pub usd_micros: u64,
}

#[derive(Debug, Default, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CostRollupTotals {
    #[ts(type = "number")]
    pub calls: u64,
    #[ts(type = "number")]
    pub tokens_in: u64,
    #[ts(type = "number")]
    pub tokens_out: u64,
    #[ts(type = "number")]
    pub usd_micros: u64,
    /// ProjectOS-billable spend only (measured/estimated); BYOK and
    /// device-session spend is `customer_billed` and never counted here —
    /// the m0-s10 honesty rule as a wire field.
    #[ts(type = "number")]
    pub projectos_usd_micros: u64,
}

/// One attribution dimension's totals (m0-s15). The three dimensions the
/// story names — project, feature, agent — are all computed in one pass over
/// the same detail rows, so the cost surfaces F23/F32 will render never
/// re-sum anything in TypeScript. Domain arithmetic stays in the core, which
/// is also the only way two shells cannot disagree about a number (L12).
#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CostGroupRow {
    /// `project`, `feature`, or `agent`.
    #[ts(type = "string")]
    pub dimension: &'static str,
    /// `null` only in the `agent` dimension, for spend a feature incurred
    /// outside any Run — a real bucket, not a missing value.
    pub key: Option<String>,
    #[ts(type = "number")]
    pub calls: u64,
    #[ts(type = "number")]
    pub tokens_in: u64,
    #[ts(type = "number")]
    pub tokens_out: u64,
    #[ts(type = "number")]
    pub wall_ms_total: u64,
    #[ts(type = "number")]
    pub usd_micros: u64,
    #[ts(type = "number")]
    pub projectos_usd_micros: u64,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct CostRollupReport {
    /// `project` when the input named a path, `session` otherwise.
    #[ts(type = "string")]
    pub scope: &'static str,
    pub project_count: u32,
    pub rows: Vec<CostRollupRow>,
    /// Per-project, per-feature, and per-agent rollups over the same rows.
    pub groups: Vec<CostGroupRow>,
    pub totals: CostRollupTotals,
}

/// The `cost.rollup` engine (fills the entry registered and contract-tested
/// since surface v3). Aggregation happens in SQL over `proj_model_calls`
/// with every group key in the ORDER BY, so repeated dispatches are
/// byte-identical across transports.
pub(crate) fn cost_rollup(
    open_projects: &OpenProjects,
    input: &CostRollupInput,
) -> Result<String, ApiError> {
    let targets: Vec<String> = match &input.path {
        Some(path) => vec![path.clone()],
        None => open_projects.paths(),
    };
    let mut rows = Vec::new();
    for path in &targets {
        rollup_one_project(Path::new(path), &mut rows)?;
    }
    // Cross-project determinism: session scope walks projects in the
    // session table's id order already; the final sort makes the contract
    // independent of that detail.
    rows.sort_by(|left, right| {
        (
            &left.project_id,
            &left.feature,
            &left.agent,
            &left.provider,
            &left.model,
            &left.credential_class,
            &left.provider_cost_kind,
        )
            .cmp(&(
                &right.project_id,
                &right.feature,
                &right.agent,
                &right.provider,
                &right.model,
                &right.credential_class,
                &right.provider_cost_kind,
            ))
    });
    let mut totals = CostRollupTotals::default();
    for row in &rows {
        totals.calls += row.calls;
        totals.tokens_in += row.tokens_in;
        totals.tokens_out += row.tokens_out;
        totals.usd_micros += row.usd_micros;
        if row.provider_cost_kind != "customer_billed" {
            totals.projectos_usd_micros += row.usd_micros;
        }
    }
    let groups = group_rows(&rows);
    project_ops::to_json(&CostRollupReport {
        scope: if input.path.is_some() {
            "project"
        } else {
            "session"
        },
        project_count: u32::try_from(targets.len()).unwrap_or(u32::MAX), // INVARIANT: the session table is capped at 64 projects.
        rows,
        groups,
        totals,
    })
}

/// Folds the detail rows three ways in one pass. Deterministic order —
/// dimension, then key — so both transports render byte-identical bytes.
fn group_rows(rows: &[CostRollupRow]) -> Vec<CostGroupRow> {
    let mut buckets: BTreeMap<(&'static str, Option<&str>), CostGroupRow> = BTreeMap::new();
    for row in rows {
        for (dimension, key) in [
            ("project", Some(row.project_id.as_str())),
            ("feature", Some(row.feature.as_str())),
            ("agent", row.agent.as_deref()),
        ] {
            let bucket = buckets
                .entry((dimension, key))
                .or_insert_with(|| CostGroupRow {
                    dimension,
                    key: key.map(str::to_owned),
                    calls: 0,
                    tokens_in: 0,
                    tokens_out: 0,
                    wall_ms_total: 0,
                    usd_micros: 0,
                    projectos_usd_micros: 0,
                });
            bucket.calls += row.calls;
            bucket.tokens_in += row.tokens_in;
            bucket.tokens_out += row.tokens_out;
            bucket.wall_ms_total += row.wall_ms_total;
            bucket.usd_micros += row.usd_micros;
            // The m0-s10 honesty rule holds at every grain: BYOK and
            // device-session spend is the customer's, never ProjectOS cost.
            if row.provider_cost_kind != "customer_billed" {
                bucket.projectos_usd_micros += row.usd_micros;
            }
        }
    }
    buckets.into_values().collect()
}

fn rollup_one_project(root: &Path, rows: &mut Vec<CostRollupRow>) -> Result<(), ApiError> {
    let log = project_ops::open_log(root)?;
    let grouped: Vec<CostRollupRow> = log
        .store()
        .db()
        .with_reader("cost rollup", |connection| {
            let mut statement = connection.prepare(
                "SELECT lower(hex(project_id)), feature, agent, provider, credential_class, model, \
                        provider_cost_kind, COUNT(*), SUM(tokens_in), SUM(tokens_out), \
                        SUM(wall_ms), SUM(usd_micros) \
                 FROM proj_model_calls \
                 GROUP BY project_id, feature, agent, provider, credential_class, model, provider_cost_kind \
                 ORDER BY project_id, feature, agent, provider, model, credential_class, provider_cost_kind",
            )?;
            let mapped = statement.query_map([], |row| {
                Ok(CostRollupRow {
                    project_id: row.get(0)?,
                    feature: row.get(1)?,
                    agent: row.get(2)?,
                    provider: row.get(3)?,
                    credential_class: row.get(4)?,
                    model: row.get(5)?,
                    provider_cost_kind: row.get(6)?,
                    calls: row.get::<_, i64>(7)?.max(0).unsigned_abs(),
                    tokens_in: row.get::<_, i64>(8)?.max(0).unsigned_abs(),
                    tokens_out: row.get::<_, i64>(9)?.max(0).unsigned_abs(),
                    wall_ms_total: row.get::<_, i64>(10)?.max(0).unsigned_abs(),
                    usd_micros: row.get::<_, i64>(11)?.max(0).unsigned_abs(),
                })
            })?;
            mapped.collect()
        })
        .map_err(|error| project_ops::store_error(&error))?;
    rows.extend(grouped);
    Ok(())
}

#[derive(Debug, Deserialize, Serialize, TS)]
#[serde(deny_unknown_fields, rename_all = "camelCase")]
pub struct ModelsPullInput {
    /// The reviewed catalog file (`models/manifest.json` in the repository;
    /// shells pass their installed copy's path).
    pub manifest_path: String,
    pub name: String,
    pub dest_dir: String,
    /// Explicit consent, supplied by the shell after its prompt or `--yes`.
    /// `false` is a typed refusal — a pull is never implicit (m0-s11).
    #[serde(default)]
    pub consent: bool,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ModelsPullReport {
    pub name: String,
    /// Total across every file, so a caller shows one number for one pull.
    #[ts(type = "number")]
    pub bytes: u64,
    /// One row per file (m1-s04: an ONNX artifact is a graph and a
    /// vocabulary). Each carries its own verified hash, because "the pull
    /// succeeded" must mean every file was verified, not the last one.
    pub files: Vec<ModelsPullFileReport>,
}

#[derive(Debug, Serialize, TS)]
#[serde(rename_all = "camelCase")]
pub struct ModelsPullFileReport {
    pub name: String,
    pub path: String,
    #[ts(type = "number")]
    pub bytes: u64,
    pub blake3: String,
    /// This file was on disk from an earlier attempt and was not re-fetched.
    /// Stated rather than hidden: a "pull succeeded" that downloaded nothing
    /// should say so.
    pub already_present: bool,
}

/// The `models.pull` command: manifest lookup, consent gate, BLAKE3-verified
/// streaming download over the loopback-only transport (file:// and local
/// HTTP sources today; remote HTTPS names its owning debt, m1-s03).
pub(crate) fn models_pull(input: &ModelsPullInput) -> Result<String, ApiError> {
    let manifest =
        ModelManifest::load(Path::new(&input.manifest_path)).map_err(|error| pull_error(&error))?;
    // Every file the artifact is made of: one for a whisper `.bin`, a graph
    // plus a vocabulary for an ONNX encoder (m1-s04).
    let files = manifest
        .artifact(&input.name)
        .map_err(|error| pull_error(&error))?;
    let consent = if input.consent {
        PullConsent::Given
    } else {
        PullConsent::Withheld
    };
    // The transport follows the URL scheme, which is the same selection rule
    // the gateway's dispatch uses (m1-s03/ADR-0006): a `file://` or loopback
    // source never touches the TLS stack, and an `https://` artifact source is
    // the one thing in the model manager that leaves the device.
    let tls = TlsHttpTransport::new();
    let loopback = LoopbackHttpTransport;
    let dest = PathBuf::from(&input.dest_dir);
    let mut reports = Vec::with_capacity(files.len());
    for entry in files {
        let transport: &dyn pos_gateway::HttpTransport = if entry.url.starts_with("https://") {
            &tls
        } else {
            &loopback
        };
        // Sequential, and it stops at the first real failure: a half-pulled
        // ONNX artifact is not a usable one, and continuing would leave a
        // directory that looks present to the stage's `is_file` check.
        //
        // A file that is *already* present is not a failure of the artifact,
        // though — it is the normal state when an earlier attempt got part
        // way. Refusing the whole pull for it would make a resumed multi-file
        // artifact permanently unpullable, which is exactly the state m1-s04
        // hit on its first run.
        match pull_model(entry, consent, &dest, transport) {
            Ok(report) => reports.push(ModelsPullFileReport {
                name: report.name,
                path: report.path.display().to_string(),
                bytes: report.bytes,
                blake3: report.blake3,
                already_present: false,
            }),
            Err(pos_gateway::ModelPullError::AlreadyPresent { path }) => {
                reports.push(ModelsPullFileReport {
                    name: entry.name.clone(),
                    path,
                    bytes: entry.bytes,
                    // The manifest's pin, which the earlier pull verified
                    // before renaming the file into place. Restated rather
                    // than recomputed: re-hashing 133 MB to tell a user
                    // nothing changed is a cost with no answer attached, and
                    // `pos verify` is the command that re-checks artifacts.
                    blake3: entry.blake3.clone(),
                    already_present: true,
                });
            }
            Err(error) => return Err(pull_error(&error)),
        }
    }
    let bytes = reports.iter().map(|file| file.bytes).sum();
    project_ops::to_json(&ModelsPullReport {
        name: input.name.clone(),
        bytes,
        files: reports,
    })
}

fn pull_error(error: &pos_gateway::ModelPullError) -> ApiError {
    use pos_gateway::ModelPullError;
    let code = match error {
        ModelPullError::ConsentRequired { .. } => "consent_required",
        ModelPullError::UnknownModel { .. } => "unknown_model",
        ModelPullError::ChecksumMismatch { .. }
        | ModelPullError::SizeMismatch { .. }
        | ModelPullError::Overrun { .. } => "artifact_rejected",
        ModelPullError::AlreadyPresent { .. } => "already_present",
        ModelPullError::BudgetExceeded { .. } => "budget_exceeded",
        ModelPullError::PartialTooLong { .. } => "artifact_rejected",
        ModelPullError::ManifestUnreadable { .. } => "manifest_invalid",
        ModelPullError::Source { .. } => "source_failure",
        ModelPullError::Io { .. } => "storage_failure",
    };
    ApiError {
        code,
        message: error.to_string(),
        retriable: matches!(error, ModelPullError::Source { .. }),
    }
}
