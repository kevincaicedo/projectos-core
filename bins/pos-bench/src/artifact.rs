//! The gate-artifact header, its environment probe, and the rule that decides
//! whether a result may be called binding.
//!
//! [`docs/reference-machines.md`] §3 says a missing header field is a failed
//! artifact, not a nullable value, and §5 says only a pinned machine under the
//! §4 protocol produces binding evidence. Both are enforced *here*, by the
//! harness, rather than by the discipline of whoever runs it: a
//! development-machine number cannot be promoted to a gate result by
//! forgetting to label it.

use std::path::{Path, PathBuf};
use std::process::Command;

/// Longest registry document the harness will read. The file is prose; the
/// cap exists so a mistaken path cannot make the harness slurp a corpus (L8).
const REGISTRY_FILE_SIZE_MAX: u64 = 1_048_576;

#[derive(Clone, Debug, Eq, PartialEq)]
pub enum Classification {
    /// Every §4 precondition held; this row may be cited as gate evidence.
    Binding,
    /// Something the protocol requires was not true. The reasons are carried
    /// in the artifact so a reader knows exactly what to fix.
    EarlyWarning(Vec<String>),
}

impl Classification {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Binding => "binding",
            Self::EarlyWarning(_) => "early_warning",
        }
    }
}

pub struct ArtifactHeader {
    pub machine_id: String,
    pub machine_registry_revision: String,
    pub story_id: &'static str,
    pub gate_id: &'static str,
    pub projectos_revision: String,
    pub harness_revision: String,
    pub rust: String,
    pub node: String,
    pub pnpm: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel: String,
    pub started_at_utc: String,
    pub power_mode: String,
    pub thermal_state_before: String,
    pub background_workload: String,
    pub dataset_manifest_hash: String,
    pub replicate_count: u32,
    pub classification: Classification,
}

pub struct Environment {
    pub revision: String,
    pub tree_clean: bool,
    pub rust: String,
    pub node: String,
    pub pnpm: String,
    pub os_name: String,
    pub os_version: String,
    pub kernel: String,
    pub power_mode: String,
    pub thermal_state_before: String,
    pub release_profile: bool,
}

impl Environment {
    /// Probes everything the header needs. Every value is present: a probe
    /// that cannot answer records `unavailable`, which is a fact about the
    /// environment rather than a hole in the artifact.
    pub fn probe(root: &Path) -> Self {
        let revision = capture(root, "git", &["rev-parse", "HEAD"]);
        let dirty = capture(root, "git", &["status", "--porcelain"]);
        Self {
            revision,
            tree_clean: dirty.trim().is_empty() && dirty != UNAVAILABLE,
            rust: first_line(&capture(root, "rustc", &["--version"])),
            node: first_line(&capture(root, "node", &["--version"])),
            pnpm: first_line(&capture(root, "pnpm", &["--version"])),
            os_name: os_name(root),
            os_version: os_version(root),
            kernel: first_line(&capture(root, "uname", &["-r"])),
            power_mode: power_mode(root),
            thermal_state_before: thermal_state(root),
            // A debug-profile timing is not a product timing. The harness
            // reads its own build rather than trusting an operator's flag.
            release_profile: !cfg!(debug_assertions),
        }
    }
}

const UNAVAILABLE: &str = "unavailable";

fn capture(root: &Path, program: &str, args: &[&str]) -> String {
    Command::new(program)
        .args(args)
        .current_dir(root)
        .output()
        .ok()
        .filter(|output| output.status.success())
        .map_or_else(
            || UNAVAILABLE.to_owned(),
            |output| String::from_utf8_lossy(&output.stdout).trim().to_owned(),
        )
}

fn first_line(text: &str) -> String {
    text.lines().next().unwrap_or(UNAVAILABLE).trim().to_owned()
}

fn os_name(root: &Path) -> String {
    let name = first_line(&capture(root, "uname", &["-s"]));
    match name.as_str() {
        "Darwin" => "macOS".to_owned(),
        other => other.to_owned(),
    }
}

fn os_version(root: &Path) -> String {
    let product = capture(root, "sw_vers", &["-productVersion"]);
    if product != UNAVAILABLE {
        let build = capture(root, "sw_vers", &["-buildVersion"]);
        return format!("{product} build {build}");
    }
    // Linux: the release line of /etc/os-release, without shelling out to a
    // parser we would then have to trust.
    std::fs::read_to_string("/etc/os-release")
        .ok()
        .and_then(|text| {
            text.lines()
                .find_map(|line| line.strip_prefix("PRETTY_NAME=").map(str::to_owned))
        })
        .map_or_else(
            || UNAVAILABLE.to_owned(),
            |value| value.trim_matches('"').to_owned(),
        )
}

fn power_mode(root: &Path) -> String {
    let battery = capture(root, "pmset", &["-g", "batt"]);
    if battery.contains("AC Power") {
        "ac".to_owned()
    } else if battery == UNAVAILABLE {
        UNAVAILABLE.to_owned()
    } else {
        "battery".to_owned()
    }
}

fn thermal_state(root: &Path) -> String {
    let thermal = capture(root, "pmset", &["-g", "therm"]);
    if thermal == UNAVAILABLE {
        return UNAVAILABLE.to_owned();
    }
    // macOS reports "no warning level recorded" on a cool machine; that is the
    // reading, and recording it verbatim beats inventing a scale.
    thermal
        .lines()
        .map(str::trim)
        .collect::<Vec<_>>()
        .join("; ")
}

/// Decides whether this run may be cited. The rules are
/// [`docs/reference-machines.md`] §4/§5, one predicate each, so a failure
/// names exactly what was violated.
pub fn classify(machine_id: &str, registry: &Path, environment: &Environment) -> Classification {
    let mut reasons = Vec::new();
    if !machine_has_committed_fingerprint(machine_id, registry) {
        reasons.push(format!(
            "{machine_id} has no committed fingerprint in {} (§4 step 1)",
            registry.display()
        ));
    }
    if !environment.tree_clean {
        reasons.push("the working tree is dirty (§4 step 1)".to_owned());
    }
    if !environment.release_profile {
        reasons.push("the harness was built without --release (§4 step 1)".to_owned());
    }
    if environment.power_mode != "ac" {
        reasons.push(format!(
            "the machine reported power mode {} rather than ac (§4 step 2)",
            environment.power_mode
        ));
    }
    if reasons.is_empty() {
        Classification::Binding
    } else {
        Classification::EarlyWarning(reasons)
    }
}

/// A machine is pinned when §6 carries its committed fingerprint block, not
/// when someone typed its id on the command line.
fn machine_has_committed_fingerprint(machine_id: &str, registry: &Path) -> bool {
    let Ok(metadata) = std::fs::metadata(registry) else {
        return false;
    };
    if metadata.len() > REGISTRY_FILE_SIZE_MAX {
        return false;
    }
    let Ok(text) = std::fs::read_to_string(registry) else {
        return false;
    };
    let Some((_, after)) = text.split_once("## 6. Committed fingerprints") else {
        return false;
    };
    after.contains(&format!("machine_id: {machine_id}"))
}

/// Walks up from `start` for the documentation registry, so the harness works
/// from the workspace, the superproject, or a scenario's own directory.
pub fn find_registry(start: &Path) -> Option<PathBuf> {
    let mut cursor = Some(start);
    while let Some(directory) = cursor {
        let candidate = directory.join("docs/reference-machines.md");
        if candidate.is_file() {
            return Some(candidate);
        }
        cursor = directory.parent();
    }
    None
}

pub fn registry_revision(root: &Path, registry: &Path) -> String {
    let path = registry.to_string_lossy().to_string();
    let revision = capture(root, "git", &["log", "-1", "--format=%H", "--", &path]);
    if revision.is_empty() {
        UNAVAILABLE.to_owned()
    } else {
        revision
    }
}

/// RFC 3339 in UTC, from `date` rather than a time dependency: the harness
/// stamps an artifact once per run, and adding a crate for one string would
/// not survive the `DEPENDENCIES.md` question.
pub fn now_utc(root: &Path) -> String {
    capture(root, "date", &["-u", "+%Y-%m-%dT%H:%M:%SZ"])
}

#[cfg(test)]
mod tests {
    use super::{Classification, Environment, classify, machine_has_committed_fingerprint};
    use std::io::Write as _;

    fn environment() -> Environment {
        Environment {
            revision: "abc".to_owned(),
            tree_clean: true,
            rust: "rustc 1.95.0".to_owned(),
            node: "v24.16.0".to_owned(),
            pnpm: "11.20.0".to_owned(),
            os_name: "macOS".to_owned(),
            os_version: "26.6.1".to_owned(),
            kernel: "25.6.0".to_owned(),
            power_mode: "ac".to_owned(),
            thermal_state_before: "nominal".to_owned(),
            release_profile: true,
        }
    }

    fn registry(body: &str) -> tempfile::NamedTempFile {
        let mut file = tempfile::NamedTempFile::new().expect("fixture file");
        file.write_all(body.as_bytes()).expect("fixture writes");
        file
    }

    /// The classification is computed, so a development-machine run cannot be
    /// promoted to gate evidence by forgetting to label it.
    #[test]
    fn every_protocol_violation_is_named_and_downgrades_the_artifact() {
        let pinned = registry(
            "## 6. Committed fingerprints\n\n### `RM-LAPTOP-01`\n\nmachine_id: RM-LAPTOP-01\n",
        );
        assert_eq!(
            classify("RM-LAPTOP-01", pinned.path(), &environment()),
            Classification::Binding
        );

        let mut dirty = environment();
        dirty.tree_clean = false;
        dirty.release_profile = false;
        dirty.power_mode = "battery".to_owned();
        let Classification::EarlyWarning(reasons) = classify("RM-LAPTOP-01", pinned.path(), &dirty)
        else {
            panic!("a dirty debug battery run cannot be binding");
        };
        assert_eq!(reasons.len(), 3, "each violation is named: {reasons:?}");

        // An unpinned machine id is the fourth way to fail, and the most
        // important one: identity is not a claim, it is a committed block.
        let Classification::EarlyWarning(reasons) =
            classify("RM-SERVER-01", pinned.path(), &environment())
        else {
            panic!("a machine without a committed fingerprint cannot be binding");
        };
        assert_eq!(reasons.len(), 1);
        assert!(reasons[0].contains("RM-SERVER-01"));
    }

    /// A fingerprint mentioned outside §6 is prose, not evidence.
    #[test]
    fn only_the_committed_fingerprint_section_pins_a_machine() {
        let prose = registry("## 1. Machine registry\n\nmachine_id: RM-LAPTOP-01\n");
        assert!(!machine_has_committed_fingerprint(
            "RM-LAPTOP-01",
            prose.path()
        ));
        let missing = registry("## 6. Committed fingerprints\n\n_Not yet captured._\n");
        assert!(!machine_has_committed_fingerprint(
            "RM-LAPTOP-01",
            missing.path()
        ));
    }
}
