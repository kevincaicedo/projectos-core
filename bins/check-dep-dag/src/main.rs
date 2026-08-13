//! # check-dep-dag — crate-boundary checker (m0-s01)
//!
//! Enforces the master plan §19 dependency direction over `cargo metadata`:
//! `pos-foundation` at the bottom; `pos-store`, then `pos-log`, then
//! `pos-domain` (the §6 layer diagram); `pos-capabilities` and `pos-gateway`
//! beside domain; feature crates above; `pos-api` on top of everything;
//! shells (`pos-server`, `pos`, `pos-desktop`) depend on `pos-api` only (L12).
//!
//! Extending an allowed edge is a deliberate, reviewed edit to
//! [`allowed_deps`] — never "add the dependency and see if CI minds".
//! A crate absent from the map is itself a violation: new crates enter the
//! workspace through master plan §19 + an ADR, not through a new folder.

#![forbid(unsafe_code)]

use std::collections::{BTreeMap, BTreeSet};
use std::process::{Command, ExitCode};

/// The four base layers every internal crate may build on.
const BASE: [&str; 4] = ["pos-foundation", "pos-store", "pos-log", "pos-domain"];

/// Infrastructure crates beside the domain layer.
const BESIDE: [&str; 2] = ["pos-capabilities", "pos-gateway"];

/// The allowed internal-dependency map. Keys are every workspace crate;
/// values are the complete set of internal crates each may depend on.
/// Feature-crate cross-edges (e.g. a future `pos-agents -> pos-knowledge`)
/// are added HERE, explicitly, in the PR that needs them.
fn allowed_deps() -> BTreeMap<&'static str, BTreeSet<&'static str>> {
    let base: BTreeSet<&str> = BASE.into_iter().collect();
    let mut base_and_beside = base.clone();
    base_and_beside.extend(BESIDE);

    let feature_crates = [
        "pos-ingest",
        "pos-connect",
        "pos-knowledge",
        "pos-agents",
        "pos-planning",
        "pos-exec",
        "pos-sched",
        "pos-sync",
        "pos-plugins",
    ];

    let mut map: BTreeMap<&str, BTreeSet<&str>> = BTreeMap::new();
    map.insert("pos-foundation", BTreeSet::new());
    map.insert("pos-store", ["pos-foundation"].into_iter().collect());
    map.insert(
        "pos-log",
        ["pos-foundation", "pos-store"].into_iter().collect(),
    );
    map.insert(
        "pos-domain",
        base.iter()
            .copied()
            .filter(|c| *c != "pos-domain")
            .collect(),
    );
    map.insert("pos-capabilities", ["pos-foundation"].into_iter().collect());
    map.insert("pos-gateway", ["pos-foundation"].into_iter().collect());
    for crate_name in feature_crates {
        map.insert(crate_name, base_and_beside.clone());
    }

    // Feature-crate cross-edge, added deliberately by m1-s01: the ingestion
    // pipeline *is* `pos-sched` jobs (master plan §9), so `pos-ingest` needs
    // the queue's enqueue seam and its handler/registry types. Recorded here
    // rather than silently: if a second and third feature crate need the same
    // edge, `pos-sched` is infrastructure beside the domain rather than a
    // feature plane, and that is a §19 change with an ADR — not another line.
    if let Some(ingest) = map.get_mut("pos-ingest") {
        ingest.insert("pos-sched");
    }

    // pos-api sits on top of every library crate.
    let mut api_deps = base_and_beside.clone();
    api_deps.extend(feature_crates);
    map.insert("pos-api", api_deps);

    // The public SDK may bind every public contract, including pos-api, but
    // product crates never depend back on the SDK facade.
    let mut sdk_deps = map
        .get("pos-api")
        .cloned()
        .expect("pos-api is inserted immediately above"); // INVARIANT: the static map constructs pos-api before pos-sdk.
    sdk_deps.insert("pos-api");
    map.insert("pos-sdk", sdk_deps);

    // Shells depend on pos-api only (L12).
    let api_only: BTreeSet<&str> = ["pos-api"].into_iter().collect();
    map.insert("pos-server", api_only.clone());
    map.insert("pos", api_only.clone());
    map.insert("pos-desktop", api_only.clone());
    // The gate harness is held to the shell rule on purpose: a bench that
    // reached past `pos-api` would measure a path no user can take (m0-s16).
    map.insert("pos-bench", api_only);

    // The checker itself uses no internal crates.
    map.insert("check-dep-dag", BTreeSet::new());
    map
}

/// Returns one human-readable violation per forbidden or unknown edge.
fn violations(metadata: &serde_json::Value) -> Vec<String> {
    let allowed = allowed_deps();
    let packages = metadata["packages"].as_array().cloned().unwrap_or_default();

    let workspace_names: BTreeSet<String> = packages
        .iter()
        .filter_map(|p| p["name"].as_str().map(str::to_owned))
        .collect();

    let mut found = Vec::new();
    for missing in allowed
        .keys()
        .filter(|name| !workspace_names.contains(**name))
    {
        found.push(format!(
            "{missing}: planned §19 crate is absent from the workspace"
        ));
    }
    for package in &packages {
        let Some(name) = package["name"].as_str() else {
            continue;
        };
        let Some(allowed_for_crate) = allowed.get(name) else {
            found.push(format!(
                "{name}: crate is not in the allowed-deps map — new crates enter \
                 through master plan §19 + an ADR + a deliberate map edit"
            ));
            continue;
        };
        let deps = package["dependencies"]
            .as_array()
            .cloned()
            .unwrap_or_default();
        for dep in &deps {
            let Some(dep_name) = dep["name"].as_str() else {
                continue;
            };
            let is_internal = workspace_names.contains(dep_name);
            if is_internal && !allowed_for_crate.contains(dep_name) {
                found.push(format!(
                    "{name} -> {dep_name}: edge not in the allowed-deps map \
                     (upward or unapproved import — see .agents/skills/crate-boundaries)"
                ));
            }
        }
    }
    found
}

fn main() -> ExitCode {
    let output = match Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .output()
    {
        Ok(output) => output,
        Err(error) => {
            eprintln!("check-dep-dag: failed to run `cargo metadata`: {error}");
            return ExitCode::FAILURE;
        }
    };
    if !output.status.success() {
        eprintln!(
            "check-dep-dag: `cargo metadata` failed:\n{}",
            String::from_utf8_lossy(&output.stderr)
        );
        return ExitCode::FAILURE;
    }
    let metadata: serde_json::Value = match serde_json::from_slice(&output.stdout) {
        Ok(value) => value,
        Err(error) => {
            eprintln!("check-dep-dag: metadata is not valid JSON: {error}");
            return ExitCode::FAILURE;
        }
    };

    let found = violations(&metadata);
    if found.is_empty() {
        println!("check-dep-dag: ok — every internal edge is in the allowed map");
        ExitCode::SUCCESS
    } else {
        eprintln!("check-dep-dag: {} violation(s):", found.len());
        for violation in &found {
            eprintln!("  {violation}");
        }
        ExitCode::FAILURE
    }
}

#[cfg(test)]
mod tests {
    use super::{allowed_deps, violations};
    use std::collections::BTreeMap;

    fn metadata_with(edges: &[(&str, &[&str])]) -> serde_json::Value {
        let mut graph: BTreeMap<&str, Vec<&str>> = allowed_deps()
            .into_keys()
            .map(|name| (name, Vec::new()))
            .collect();
        for (name, dependencies) in edges {
            graph.insert(name, dependencies.to_vec());
        }
        let packages: Vec<serde_json::Value> = graph
            .into_iter()
            .map(|(name, deps)| {
                let dep_objs: Vec<serde_json::Value> = deps
                    .into_iter()
                    .map(|d| serde_json::json!({ "name": d }))
                    .collect();
                serde_json::json!({ "name": name, "dependencies": dep_objs })
            })
            .collect();
        serde_json::json!({ "packages": packages })
    }

    /// AC (m0-s01): a deliberately introduced upward import fails the checker.
    #[test]
    fn upward_import_is_a_violation() {
        let metadata = metadata_with(&[
            ("pos-store", &["pos-foundation", "pos-domain"]),
            ("pos-foundation", &[]),
            ("pos-domain", &[]),
        ]);
        let found = violations(&metadata);
        assert_eq!(
            found.len(),
            1,
            "exactly the upward edge is flagged: {found:?}"
        );
        assert!(found[0].contains("pos-store -> pos-domain"));
    }

    #[test]
    fn shell_reaching_past_pos_api_is_a_violation() {
        let metadata = metadata_with(&[
            ("pos-server", &["pos-api", "pos-store"]),
            ("pos-api", &[]),
            ("pos-store", &["pos-foundation"]),
            ("pos-foundation", &[]),
        ]);
        let found = violations(&metadata);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("pos-server -> pos-store"));
    }

    #[test]
    fn unknown_crate_is_a_violation() {
        let metadata = metadata_with(&[("pos-mystery", &[])]);
        let found = violations(&metadata);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("not in the allowed-deps map"));
    }

    #[test]
    fn missing_planned_crate_is_a_violation() {
        let mut metadata = metadata_with(&[]);
        let packages = metadata["packages"]
            .as_array_mut()
            .expect("fixture packages are an array");
        packages.retain(|package| package["name"] != "pos-sdk");
        let found = violations(&metadata);
        assert_eq!(found.len(), 1, "{found:?}");
        assert!(found[0].contains("pos-sdk: planned §19 crate is absent"));
    }

    #[test]
    fn clean_workspace_shape_passes() {
        let metadata = metadata_with(&[
            ("pos-foundation", &[]),
            ("pos-store", &["pos-foundation"]),
            ("pos-log", &["pos-foundation", "pos-store"]),
            ("pos-domain", &["pos-foundation", "pos-store", "pos-log"]),
            ("pos-api", &["pos-domain", "pos-gateway"]),
            ("pos-sdk", &["pos-api", "pos-capabilities"]),
            ("pos-gateway", &["pos-foundation"]),
            ("pos-server", &["pos-api"]),
            ("check-dep-dag", &[]),
        ]);
        assert!(violations(&metadata).is_empty());
    }

    /// External (crates.io) dependencies are not internal edges.
    #[test]
    fn external_dependencies_are_ignored() {
        let metadata = metadata_with(&[("check-dep-dag", &["serde_json"])]);
        assert!(violations(&metadata).is_empty());
    }
}
