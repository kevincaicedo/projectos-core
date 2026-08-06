//! Open-core boundary checks and seeded fixture oracles (m0-s17).
//!
//! Repository orchestration supplies live build/tag/submodule evidence. This
//! binary owns the policy decisions so every named gate has a positive and a
//! negative unit fixture instead of relying on an untested shell grep.

#![forbid(unsafe_code)]

use proc_macro2::TokenTree;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use syn::visit::Visit;

const SOURCE_FILE_COUNT_MAX: usize = 50_000;
const DIRECTORY_DEPTH_MAX: usize = 64;
const SOURCE_FILE_SIZE_MAX: u64 = 4 * 1_048_576;
const POLICY_FILE_SIZE_MAX: u64 = 1_048_576;
const MIRROR_FILE_SIZE_MAX: u64 = 16 * 1_048_576;
const LEGAL_FILE_SIZE_MIN: usize = 200;

/// The accepted ADR-0003 identity. These strings are public by design: they are
/// the copyright holder and the single security intake the public repository
/// promises. Keeping them here makes "the legal files agree with each other" a
/// machine check instead of a review habit.
const PUBLIC_COPYRIGHT_HOLDER: &str = "Private AI Inc.";
const PUBLIC_SECURITY_INTAKE: &str = "ing.sys.kevincaicedo@gmail.com";

/// ADR-0003 §Verification: each public legal file, plus the phrase that proves
/// it is the accepted text rather than a placeholder that happens to exist.
const REQUIRED_PUBLIC_LEGAL_FILES: [(&str, &str); 6] = [
    ("LICENSE", "Apache License"),
    ("NOTICE", PUBLIC_COPYRIGHT_HOLDER),
    ("SECURITY.md", PUBLIC_SECURITY_INTAKE),
    ("TRADEMARK.md", PUBLIC_COPYRIGHT_HOLDER),
    ("CONTRIBUTING.md", "Signed-off-by"),
    (".github/workflows/dco.yml", "Signed-off-by"),
];

#[derive(Clone, Debug, Eq, PartialEq)]
struct BoundaryViolation {
    gate: &'static str,
    detail: String,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct BuildEvidence {
    builds: bool,
    tests: bool,
    packages_desktop: bool,
    packages_web: bool,
    packages_cli: bool,
    walking_skeleton: bool,
    cloud_absent: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct CorePinEvidence {
    expected_matches_checkout: bool,
    source_matches_tag: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct CapabilityEvidence {
    trait_present: bool,
    default_present: bool,
    ui_card_present: bool,
    unavailable_reason_required: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct OpenCapabilityEvidence {
    functional: bool,
    gated: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct SeamEvidence {
    changed: bool,
    version_bumped: bool,
    adr_linked: bool,
}

#[cfg(test)]
#[derive(Clone, Copy)]
struct SubmoduleEvidence {
    core_is_gitlink: bool,
    cloud_is_gitlink: bool,
    core_tag_signed: bool,
    cloud_tag_signed: bool,
    floating_branch: bool,
    cloud_core_not_ahead: bool,
}

fn main() -> ExitCode {
    let mut args = env::args_os().skip(1);
    let Some(scope) = args.next() else {
        eprintln!("usage: check-boundaries core|cloud|docs|umbrella --root <path>");
        return ExitCode::FAILURE;
    };
    let Some(flag) = args.next() else {
        eprintln!("check-boundaries: --root is required");
        return ExitCode::FAILURE;
    };
    let Some(root) = args.next() else {
        eprintln!("check-boundaries: root path is required");
        return ExitCode::FAILURE;
    };
    if flag != OsStr::new("--root") || args.next().is_some() {
        eprintln!("check-boundaries: expected exactly `--root <path>`");
        return ExitCode::FAILURE;
    }
    let root = match PathBuf::from(root).canonicalize() {
        Ok(root) => root,
        Err(error) => {
            eprintln!("check-boundaries: resolve root: {error}");
            return ExitCode::FAILURE;
        }
    };
    let result = match scope.to_str() {
        Some("core") => scan_repository(&root, RepositoryKind::Core),
        Some("cloud") => scan_repository(&root, RepositoryKind::Cloud),
        Some("docs") => docs_mirror_violations(&root),
        Some("umbrella") => scan_umbrella(&root),
        _ => {
            eprintln!("check-boundaries: scope must be core, cloud, docs, or umbrella");
            return ExitCode::FAILURE;
        }
    };
    match result {
        Ok(violations) if violations.is_empty() => {
            println!(
                "check-boundaries: {} boundary is clean",
                scope.to_string_lossy()
            );
            ExitCode::SUCCESS
        }
        Ok(violations) => {
            eprintln!("check-boundaries: {} violation(s):", violations.len());
            for violation in violations {
                eprintln!("  {}: {}", violation.gate, violation.detail);
            }
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("check-boundaries: could not complete: {error}");
            ExitCode::FAILURE
        }
    }
}

#[derive(Clone, Copy)]
enum RepositoryKind {
    Core,
    Cloud,
}

fn scan_repository(
    root: &Path,
    repository_kind: RepositoryKind,
) -> Result<Vec<BoundaryViolation>, String> {
    let mut violations = dependency_boundary_violations(root, repository_kind)?;
    if matches!(repository_kind, RepositoryKind::Core) {
        violations.extend(public_legal_file_violations(root)?);
    }
    for path in rust_files(root)? {
        let source = read_bounded_text(&path, SOURCE_FILE_SIZE_MAX, "Rust source")?;
        let syntax = syn::parse_file(&source)
            .map_err(|error| format!("parse {}: {error}", path.display()))?;
        let mut visitor = SourceBoundaryVisitor {
            repository_kind,
            path: &path,
            violations: &mut violations,
        };
        visitor.visit_file(&syntax);
    }
    if matches!(repository_kind, RepositoryKind::Cloud) {
        for path in files_with_extension(root, "sql")? {
            let sql = read_bounded_text(&path, SOURCE_FILE_SIZE_MAX, "SQL source")?;
            if projection_write(&sql) {
                violations.push(BoundaryViolation {
                    gate: "no-domain-in-cloud",
                    detail: format!("{}: contains a write to a proj_* table", path.display()),
                });
            }
        }
    }
    Ok(violations)
}

fn rust_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    files_with_extension(root, "rs")
}

fn files_with_extension(root: &Path, extension: &str) -> Result<Vec<PathBuf>, String> {
    let mut stack = vec![(root.to_path_buf(), 0_usize)];
    let mut files = Vec::new();
    while let Some((directory, depth)) = stack.pop() {
        if depth > DIRECTORY_DEPTH_MAX {
            return Err(format!("directory nesting exceeds {DIRECTORY_DEPTH_MAX}"));
        }
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("read {}: {error}", directory.display()))?
        {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| format!("inspect {}: {error}", path.display()))?;
            if file_type.is_symlink() || ignored_path(&path) {
                continue;
            }
            if file_type.is_dir() {
                stack.push((path, depth + 1));
            } else if path.extension() == Some(OsStr::new(extension)) {
                files.push(path);
                if files.len() > SOURCE_FILE_COUNT_MAX {
                    return Err(format!("source file count exceeds {SOURCE_FILE_COUNT_MAX}"));
                }
            }
        }
    }
    files.sort();
    Ok(files)
}

fn dependency_boundary_violations(
    root: &Path,
    repository_kind: RepositoryKind,
) -> Result<Vec<BoundaryViolation>, String> {
    let manifest_path = root.join("Cargo.toml");
    if !manifest_path.is_file() {
        return Err(format!("{} is missing", manifest_path.display()));
    }
    let output = Command::new("cargo")
        .args([
            "metadata",
            "--format-version",
            "1",
            "--no-deps",
            "--manifest-path",
        ])
        .arg(&manifest_path)
        .output()
        .map_err(|error| format!("run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed for {}: {}",
            manifest_path.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    let metadata: serde_json::Value = serde_json::from_slice(&output.stdout)
        .map_err(|error| format!("decode cargo metadata: {error}"))?;
    dependency_violations_from_metadata(&metadata, root, repository_kind)
}

fn dependency_violations_from_metadata(
    metadata: &serde_json::Value,
    root: &Path,
    repository_kind: RepositoryKind,
) -> Result<Vec<BoundaryViolation>, String> {
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no packages array".to_owned())?;
    let workspace_members = metadata
        .get("workspace_members")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no workspace_members array".to_owned())?;
    let workspace_ids: std::collections::BTreeSet<&str> = workspace_members
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    let mut violations = Vec::new();
    for package in packages {
        let Some(package_id) = package.get("id").and_then(serde_json::Value::as_str) else {
            return Err("cargo metadata package has no id".to_owned());
        };
        if !workspace_ids.contains(package_id) {
            continue;
        }
        let package_name = package
            .get("name")
            .and_then(serde_json::Value::as_str)
            .unwrap_or("<unnamed-package>");
        let dependencies = package
            .get("dependencies")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("cargo metadata package {package_name} has no dependencies"))?;
        for dependency in dependencies {
            let Some(dependency_name) = dependency.get("name").and_then(serde_json::Value::as_str)
            else {
                return Err(format!(
                    "cargo metadata dependency in {package_name} has no name"
                ));
            };
            let dependency_path = dependency.get("path").and_then(serde_json::Value::as_str);
            let outside_path =
                dependency_path.is_some_and(|path| !Path::new(path).starts_with(root));
            let violation = match repository_kind {
                RepositoryKind::Core => outside_path,
                RepositoryKind::Cloud => {
                    if dependency_name == "pos-capabilities" {
                        let expected_socket = root
                            .parent()
                            .map(|parent| parent.join("core/crates/pos-capabilities"));
                        expected_socket.as_deref() != dependency_path.map(Path::new)
                    } else {
                        outside_path || dependency_name.starts_with("pos-")
                    }
                }
            };
            if violation {
                let (gate, rule) = match repository_kind {
                    RepositoryKind::Core => (
                        "no-build-time-cloud-cfg",
                        "core cannot depend on a path outside its public repository",
                    ),
                    RepositoryKind::Cloud => (
                        "no-domain-in-cloud",
                        "cloud may consume only the public pos-capabilities socket",
                    ),
                };
                violations.push(BoundaryViolation {
                    gate,
                    detail: format!("{package_name} -> {dependency_name}: {rule}"),
                });
            }
        }
    }
    Ok(violations)
}

fn scan_umbrella(root: &Path) -> Result<Vec<BoundaryViolation>, String> {
    let mut violations = Vec::new();
    let gitmodules_path = root.join(".gitmodules");
    let gitmodules = read_bounded_text(&gitmodules_path, POLICY_FILE_SIZE_MAX, ".gitmodules")?;
    if gitmodules
        .lines()
        .map(str::trim_start)
        .any(|line| line.starts_with("branch"))
    {
        violations.push(BoundaryViolation {
            gate: "submodule-pin",
            detail: ".gitmodules contains a floating branch setting".to_owned(),
        });
    }
    for child in ["core", "cloud"] {
        if gitlink_mode(root, child)?.as_deref() != Some("160000") {
            violations.push(BoundaryViolation {
                gate: "submodule-pin",
                detail: format!("{child} is not a gitlink (mode 160000)"),
            });
        }
    }

    let core_root = root.join("core");
    let cloud_root = root.join("cloud");
    let core_pin_path = cloud_root.join("core-pin.json");
    let core_pin_bytes = read_bounded_bytes(&core_pin_path, POLICY_FILE_SIZE_MAX, "core pin")?;
    let core_pin: serde_json::Value = serde_json::from_slice(&core_pin_bytes)
        .map_err(|error| format!("decode {}: {error}", core_pin_path.display()))?;
    let pinned_core_tag = core_pin
        .get("core_tag")
        .and_then(serde_json::Value::as_str)
        .filter(|tag| !tag.trim().is_empty());
    match pinned_core_tag {
        Some(tag) => {
            if !tag_points_at_head(&core_root, tag)? {
                violations.push(BoundaryViolation {
                    gate: "no-core-drift",
                    detail: format!("cloud pin {tag} does not name the core gitlink commit"),
                });
            }
            if !tag_is_signed(&core_root, tag)? {
                violations.push(BoundaryViolation {
                    gate: "submodule-pin",
                    detail: format!("core tag {tag} has no verifiable signature"),
                });
            }
        }
        None => violations.push(BoundaryViolation {
            gate: "no-core-drift",
            detail: "cloud/core-pin.json has no non-empty core_tag".to_owned(),
        }),
    }
    let cloud_tags = tags_at_head(&cloud_root, "cloud-v*")?;
    if cloud_tags.len() != 1 {
        violations.push(BoundaryViolation {
            gate: "submodule-pin",
            detail: format!(
                "cloud gitlink needs exactly one cloud-v* tag; found {}",
                cloud_tags.len()
            ),
        });
    } else if !tag_is_signed(&cloud_root, &cloud_tags[0])? {
        violations.push(BoundaryViolation {
            gate: "submodule-pin",
            detail: format!("cloud tag {} has no verifiable signature", cloud_tags[0]),
        });
    }

    violations.extend(docs_mirror_violations(root)?);
    Ok(violations)
}

/// Reads the public legal files from a live checkout and applies the policy.
///
/// A missing file and an empty file are the same failure here on purpose: a
/// zero-byte `LICENSE` satisfies a shell `test -f` and satisfies nobody else.
fn public_legal_file_violations(root: &Path) -> Result<Vec<BoundaryViolation>, String> {
    let mut files = Vec::with_capacity(REQUIRED_PUBLIC_LEGAL_FILES.len());
    for (name, _) in REQUIRED_PUBLIC_LEGAL_FILES {
        let path = root.join(name);
        let text = if path.is_file() {
            Some(read_bounded_text(
                &path,
                POLICY_FILE_SIZE_MAX,
                "legal file",
            )?)
        } else {
            None
        };
        files.push((name, text));
    }
    Ok(public_legal_violations(&files))
}

fn public_legal_violations(files: &[(&str, Option<String>)]) -> Vec<BoundaryViolation> {
    let mut violations = Vec::new();
    for (name, required_phrase) in REQUIRED_PUBLIC_LEGAL_FILES {
        let found = files
            .iter()
            .find(|(candidate, _)| *candidate == name)
            .and_then(|(_, text)| text.as_deref());
        let Some(text) = found else {
            violations.push(BoundaryViolation {
                gate: "public-legal-files",
                detail: format!("{name} is missing from the public repository"),
            });
            continue;
        };
        if text.len() < LEGAL_FILE_SIZE_MIN {
            violations.push(BoundaryViolation {
                gate: "public-legal-files",
                detail: format!("{name} is too short to be the accepted text"),
            });
            continue;
        }
        if !text.contains(required_phrase) {
            violations.push(BoundaryViolation {
                gate: "public-legal-files",
                detail: format!("{name} does not contain the accepted phrase {required_phrase:?}"),
            });
        }
    }
    violations
}

fn gitlink_mode(root: &Path, child: &str) -> Result<Option<String>, String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(["ls-files", "--stage", "--", child])
        .output()
        .map_err(|error| format!("inspect {child} gitlink: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "git ls-files failed for {child}: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    Ok(String::from_utf8_lossy(&output.stdout)
        .split_whitespace()
        .next()
        .map(str::to_owned))
}

fn tag_points_at_head(repository: &Path, tag: &str) -> Result<bool, String> {
    let head = git_stdout(repository, &["rev-parse", "HEAD"])?;
    let tag_commit = git_stdout(repository, &["rev-parse", &format!("{tag}^{{commit}}")])?;
    Ok(head.trim() == tag_commit.trim())
}

fn tag_is_signed(repository: &Path, tag: &str) -> Result<bool, String> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(["verify-tag", tag])
        .output()
        .map_err(|error| format!("verify tag {tag}: {error}"))?;
    Ok(output.status.success())
}

fn tags_at_head(repository: &Path, pattern: &str) -> Result<Vec<String>, String> {
    let output = git_stdout(
        repository,
        &["tag", "--points-at", "HEAD", "--list", pattern],
    )?;
    Ok(output
        .lines()
        .map(str::trim)
        .filter(|tag| !tag.is_empty())
        .map(str::to_owned)
        .collect())
}

fn git_stdout(repository: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(repository)
        .args(args)
        .output()
        .map_err(|error| format!("run git in {}: {error}", repository.display()))?;
    if !output.status.success() {
        return Err(format!(
            "git {} failed in {}: {}",
            args.join(" "),
            repository.display(),
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    String::from_utf8(output.stdout).map_err(|error| format!("git output is not UTF-8: {error}"))
}

fn docs_mirror_violations(root: &Path) -> Result<Vec<BoundaryViolation>, String> {
    const PAIR_COUNT_MAX: usize = 64;
    let manifest_path = root.join("docs/public-mirrors.json");
    let manifest_bytes =
        read_bounded_bytes(&manifest_path, POLICY_FILE_SIZE_MAX, "mirror manifest")?;
    let manifest: serde_json::Value = serde_json::from_slice(&manifest_bytes)
        .map_err(|error| format!("decode {}: {error}", manifest_path.display()))?;
    let pairs = manifest
        .get("pairs")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "public mirror manifest has no pairs array".to_owned())?;
    if pairs.is_empty() || pairs.len() > PAIR_COUNT_MAX {
        return Err(format!(
            "public mirror pair count must be between 1 and {PAIR_COUNT_MAX}"
        ));
    }
    let mut violations = Vec::new();
    for pair in pairs {
        let source = mirror_path(pair, "source")?;
        let mirror = mirror_path(pair, "mirror")?;
        if !source.starts_with(Path::new("docs")) || !mirror.starts_with(Path::new("core/docs")) {
            return Err("mirror paths must stay under docs and core/docs".to_owned());
        }
        let source_path = root.join(&source);
        let mirror_path = root.join(&mirror);
        let source_bytes = read_bounded_bytes(&source_path, MIRROR_FILE_SIZE_MAX, "mirror source")?;
        let mirror_bytes = read_bounded_bytes(&mirror_path, MIRROR_FILE_SIZE_MAX, "public mirror")?;
        if source_bytes != mirror_bytes {
            violations.push(BoundaryViolation {
                gate: "docs-mirror-fresh",
                detail: format!("{} differs from {}", mirror.display(), source.display()),
            });
        }
    }
    Ok(violations)
}

fn mirror_path(pair: &serde_json::Value, field: &str) -> Result<PathBuf, String> {
    let value = pair
        .get(field)
        .and_then(serde_json::Value::as_str)
        .filter(|value| !value.is_empty())
        .ok_or_else(|| format!("public mirror pair has no {field}"))?;
    let path = PathBuf::from(value);
    if path.is_absolute()
        || path
            .components()
            .any(|component| !matches!(component, std::path::Component::Normal(_)))
    {
        return Err(format!("public mirror {field} is not a safe relative path"));
    }
    Ok(path)
}

fn read_bounded_text(path: &Path, limit: u64, description: &str) -> Result<String, String> {
    let bytes = read_bounded_bytes(path, limit, description)?;
    String::from_utf8(bytes)
        .map_err(|error| format!("{description} {} is not UTF-8: {error}", path.display()))
}

fn read_bounded_bytes(path: &Path, limit: u64, description: &str) -> Result<Vec<u8>, String> {
    let mut file = fs::File::open(path)
        .map_err(|error| format!("open {description} {}: {error}", path.display()))?;
    let mut bytes = Vec::new();
    file.by_ref()
        .take(limit.saturating_add(1))
        .read_to_end(&mut bytes)
        .map_err(|error| format!("read {description} {}: {error}", path.display()))?;
    if u64::try_from(bytes.len()).unwrap_or(u64::MAX) > limit {
        return Err(format!(
            "{description} {} exceeds {limit} bytes",
            path.display()
        ));
    }
    Ok(bytes)
}

fn ignored_path(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some(".git" | "target" | "node_modules" | "dist")
        )
    })
}

struct SourceBoundaryVisitor<'a> {
    repository_kind: RepositoryKind,
    path: &'a Path,
    violations: &'a mut Vec<BoundaryViolation>,
}

impl SourceBoundaryVisitor<'_> {
    fn report(&mut self, gate: &'static str, detail: &str) {
        self.violations.push(BoundaryViolation {
            gate,
            detail: format!("{}: {detail}", self.path.display()),
        });
    }
}

impl<'ast> Visit<'ast> for SourceBoundaryVisitor<'_> {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if item_is_test_only(item) {
            return;
        }
        if matches!(self.repository_kind, RepositoryKind::Cloud) {
            let forbidden = match item {
                syn::Item::Enum(item) if item.ident == "EventKind" => Some("declares EventKind"),
                syn::Item::Struct(item)
                    if matches!(
                        item.ident.to_string().as_str(),
                        "GateRule" | "AutonomyRule" | "CitationRule"
                    ) =>
                {
                    Some("declares a core-owned domain rule")
                }
                _ => None,
            };
            if let Some(detail) = forbidden {
                self.report("no-domain-in-cloud", detail);
            }
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_path(&mut self, path: &'ast syn::Path) {
        if matches!(self.repository_kind, RepositoryKind::Cloud)
            && path
                .segments
                .iter()
                .any(|segment| segment.ident == "EventKind")
        {
            self.report("no-domain-in-cloud", "imports or uses core-owned EventKind");
        }
        syn::visit::visit_path(self, path);
    }

    fn visit_macro(&mut self, item_macro: &'ast syn::Macro) {
        if matches!(self.repository_kind, RepositoryKind::Cloud)
            && tokens_contain_identifier(item_macro.tokens.clone(), "EventKind")
        {
            self.report(
                "no-domain-in-cloud",
                "hides core-owned EventKind inside macro tokens",
            );
        }
        syn::visit::visit_macro(self, item_macro);
    }

    fn visit_attribute(&mut self, attribute: &'ast syn::Attribute) {
        if matches!(self.repository_kind, RepositoryKind::Core)
            && attribute.path().is_ident("cfg")
            && attribute_tokens_contain_cloud(attribute)
        {
            self.report(
                "no-build-time-cloud-cfg",
                "contains a cloud-shaped cfg attribute",
            );
        }
        syn::visit::visit_attribute(self, attribute);
    }

    fn visit_lit_str(&mut self, literal: &'ast syn::LitStr) {
        let value = literal.value();
        match self.repository_kind {
            RepositoryKind::Core => {
                let lower = value.to_ascii_lowercase();
                let cloud_build_marker = ["projectos", "cloud"].join("_");
                let cloud_host_marker = ["cloud", "hostname"].join("_");
                if lower.contains(&cloud_build_marker) || lower.contains(&cloud_host_marker) {
                    self.report(
                        "no-build-time-cloud-cfg",
                        "contains a cloud-build or hostname discriminator",
                    );
                }
            }
            RepositoryKind::Cloud => {
                if projection_write(&value) {
                    self.report("no-domain-in-cloud", "contains a write to a proj_* table");
                }
            }
        }
        syn::visit::visit_lit_str(self, literal);
    }
}

fn attribute_tokens_contain_cloud(attribute: &syn::Attribute) -> bool {
    let syn::Meta::List(list) = &attribute.meta else {
        return false;
    };
    tokens_contain_cloud(list.tokens.clone())
}

fn tokens_contain_cloud(tokens: proc_macro2::TokenStream) -> bool {
    tokens.into_iter().any(|token| match token {
        TokenTree::Ident(ident) => ident.to_string().to_ascii_lowercase().contains("cloud"),
        TokenTree::Literal(literal) => literal.to_string().to_ascii_lowercase().contains("cloud"),
        TokenTree::Group(group) => tokens_contain_cloud(group.stream()),
        TokenTree::Punct(_) => false,
    })
}

fn tokens_contain_identifier(tokens: proc_macro2::TokenStream, expected: &str) -> bool {
    tokens.into_iter().any(|token| match token {
        TokenTree::Ident(ident) => ident == expected,
        TokenTree::Group(group) => tokens_contain_identifier(group.stream(), expected),
        TokenTree::Literal(_) | TokenTree::Punct(_) => false,
    })
}

fn item_is_test_only(item: &syn::Item) -> bool {
    let attributes = match item {
        syn::Item::Const(item) => &item.attrs,
        syn::Item::Enum(item) => &item.attrs,
        syn::Item::ExternCrate(item) => &item.attrs,
        syn::Item::Fn(item) => &item.attrs,
        syn::Item::ForeignMod(item) => &item.attrs,
        syn::Item::Impl(item) => &item.attrs,
        syn::Item::Macro(item) => &item.attrs,
        syn::Item::Mod(item) => &item.attrs,
        syn::Item::Static(item) => &item.attrs,
        syn::Item::Struct(item) => &item.attrs,
        syn::Item::Trait(item) => &item.attrs,
        syn::Item::TraitAlias(item) => &item.attrs,
        syn::Item::Type(item) => &item.attrs,
        syn::Item::Union(item) => &item.attrs,
        syn::Item::Use(item) => &item.attrs,
        syn::Item::Verbatim(_) | _ => return false,
    };
    attributes.iter().any(|attribute| {
        if attribute
            .path()
            .segments
            .last()
            .is_some_and(|segment| segment.ident == "test")
        {
            return true;
        }
        let syn::Meta::List(list) = &attribute.meta else {
            return false;
        };
        if !attribute.path().is_ident("cfg") {
            return false;
        }
        let mut tokens = list.tokens.clone().into_iter();
        matches!(tokens.next(), Some(TokenTree::Ident(ident)) if ident == "test")
            && tokens.next().is_none()
    })
}

fn projection_write(sql: &str) -> bool {
    let words: Vec<String> = sql
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|word| !word.is_empty())
        .map(|word| word.to_ascii_uppercase())
        .collect();
    words.iter().enumerate().any(|(index, word)| {
        if !matches!(word.as_str(), "INSERT" | "UPDATE" | "DELETE" | "REPLACE") {
            return false;
        }
        words
            .iter()
            .skip(index + 1)
            .take(7)
            .any(|candidate| candidate.starts_with("PROJ_"))
    })
}

#[cfg(test)]
fn public_builds_alone(evidence: BuildEvidence) -> Vec<BoundaryViolation> {
    let all_green = evidence.builds
        && evidence.tests
        && evidence.packages_desktop
        && evidence.packages_web
        && evidence.packages_cli
        && evidence.walking_skeleton
        && evidence.cloud_absent;
    violation_unless(
        "public-builds-alone",
        all_green,
        "build, test, three packages, walking skeleton, and cloud absence must all be proven",
    )
}

#[cfg(test)]
fn no_core_drift(evidence: CorePinEvidence) -> Vec<BoundaryViolation> {
    violation_unless(
        "no-core-drift",
        evidence.expected_matches_checkout && evidence.source_matches_tag,
        "cloud core pin or checked-out bytes differ from the upstream tag",
    )
}

#[cfg(test)]
fn capability_honesty(entries: &[CapabilityEvidence]) -> Vec<BoundaryViolation> {
    let honest = entries.iter().all(|entry| {
        entry.trait_present
            && entry.default_present
            && entry.ui_card_present
            && entry.unavailable_reason_required
    });
    violation_unless(
        "capability-honesty",
        honest,
        "every capability needs a trait, default, UI card, and required unavailable reason",
    )
}

#[cfg(test)]
fn no_crippleware(entries: &[OpenCapabilityEvidence]) -> Vec<BoundaryViolation> {
    let open = entries.iter().all(|entry| entry.functional && !entry.gated);
    violation_unless(
        "no-crippleware",
        open,
        "an open capability is non-functional or commercially gated",
    )
}

#[cfg(test)]
fn seam_freeze(evidence: SeamEvidence) -> Vec<BoundaryViolation> {
    let valid = !evidence.changed || evidence.version_bumped && evidence.adr_linked;
    violation_unless(
        "seam-freeze",
        valid,
        "a frozen seam changed without both a version bump and ADR link",
    )
}

#[cfg(test)]
fn submodule_pin(evidence: SubmoduleEvidence) -> Vec<BoundaryViolation> {
    let valid = evidence.core_is_gitlink
        && evidence.cloud_is_gitlink
        && evidence.core_tag_signed
        && evidence.cloud_tag_signed
        && !evidence.floating_branch
        && evidence.cloud_core_not_ahead;
    violation_unless(
        "submodule-pin",
        valid,
        "submodules must be gitlinks on signed compatible tags with no floating branch",
    )
}

#[cfg(test)]
fn docs_mirror_fresh(pairs: &[(&[u8], &[u8])]) -> Vec<BoundaryViolation> {
    violation_unless(
        "docs-mirror-fresh",
        pairs.iter().all(|(source, mirror)| source == mirror),
        "a public documentation mirror differs byte-for-byte from its source",
    )
}

#[cfg(test)]
fn violation_unless(
    gate: &'static str,
    condition: bool,
    detail: &'static str,
) -> Vec<BoundaryViolation> {
    if condition {
        Vec::new()
    } else {
        vec![BoundaryViolation {
            gate,
            detail: detail.to_owned(),
        }]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn public_builds_alone_has_clean_and_seeded_failure() {
        let clean = BuildEvidence {
            builds: true,
            tests: true,
            packages_desktop: true,
            packages_web: true,
            packages_cli: true,
            walking_skeleton: true,
            cloud_absent: true,
        };
        assert!(public_builds_alone(clean).is_empty());
        assert_eq!(
            public_builds_alone(BuildEvidence {
                cloud_absent: false,
                ..clean
            })
            .len(),
            1
        );
    }

    #[test]
    fn no_domain_in_cloud_has_clean_and_seeded_failure() {
        let clean = "pub struct HostedProvider;";
        let violation = ["pub enum Event", "Kind { Added }"].concat();
        assert!(source_violations(clean, RepositoryKind::Cloud).is_empty());
        assert_eq!(
            source_violations(&violation, RepositoryKind::Cloud).len(),
            1
        );
        let macro_violation = [
            "macro_rules! hidden { () => { pub enum Event",
            "Kind {} } }",
        ]
        .concat();
        assert_eq!(
            source_violations(&macro_violation, RepositoryKind::Cloud).len(),
            1
        );
    }

    #[test]
    fn cloud_dependencies_stop_at_the_public_capability_socket() {
        let root = Path::new("/checkout/cloud");
        let clean = dependency_metadata(&[(
            "projectos-cloud-stubs",
            &[("pos-capabilities", "/checkout/core/crates/pos-capabilities")],
        )]);
        assert!(
            dependency_violations_from_metadata(&clean, root, RepositoryKind::Cloud)
                .expect("fixture metadata is complete")
                .is_empty()
        );
        let violation = dependency_metadata(&[(
            "projectos-cloud-stubs",
            &[("pos-domain", "/checkout/core/crates/pos-domain")],
        )]);
        assert_eq!(
            dependency_violations_from_metadata(&violation, root, RepositoryKind::Cloud)
                .expect("fixture metadata is complete")
                .len(),
            1
        );
        let masquerading_socket = dependency_metadata(&[(
            "projectos-cloud-stubs",
            &[("pos-capabilities", "/checkout/private/pos-capabilities")],
        )]);
        assert_eq!(
            dependency_violations_from_metadata(&masquerading_socket, root, RepositoryKind::Cloud)
                .expect("fixture metadata is complete")
                .len(),
            1
        );
    }

    #[test]
    fn no_core_drift_has_clean_and_seeded_failure() {
        let clean = CorePinEvidence {
            expected_matches_checkout: true,
            source_matches_tag: true,
        };
        assert!(no_core_drift(clean).is_empty());
        assert_eq!(
            no_core_drift(CorePinEvidence {
                source_matches_tag: false,
                ..clean
            })
            .len(),
            1
        );
    }

    #[test]
    fn no_build_time_cloud_cfg_has_clean_and_seeded_failure() {
        let clean = "#[cfg(target_os = \"macos\")] pub fn platform() {}";
        let violation = ["#[cfg(feature = ", "\"cloud\")] pub fn fork() {}"].concat();
        assert!(source_violations(clean, RepositoryKind::Core).is_empty());
        assert_eq!(source_violations(&violation, RepositoryKind::Core).len(), 1);
    }

    #[test]
    fn capability_honesty_has_clean_and_seeded_failure() {
        let clean = CapabilityEvidence {
            trait_present: true,
            default_present: true,
            ui_card_present: true,
            unavailable_reason_required: true,
        };
        assert!(capability_honesty(&[clean]).is_empty());
        assert_eq!(
            capability_honesty(&[CapabilityEvidence {
                ui_card_present: false,
                ..clean
            }])
            .len(),
            1
        );
    }

    #[test]
    fn no_crippleware_has_clean_and_seeded_failure() {
        let clean = OpenCapabilityEvidence {
            functional: true,
            gated: false,
        };
        assert!(no_crippleware(&[clean]).is_empty());
        assert_eq!(
            no_crippleware(&[OpenCapabilityEvidence {
                gated: true,
                ..clean
            }])
            .len(),
            1
        );
    }

    #[test]
    fn seam_freeze_has_clean_and_seeded_failure() {
        let clean = SeamEvidence {
            changed: true,
            version_bumped: true,
            adr_linked: true,
        };
        assert!(seam_freeze(clean).is_empty());
        assert_eq!(
            seam_freeze(SeamEvidence {
                adr_linked: false,
                ..clean
            })
            .len(),
            1
        );
    }

    #[test]
    fn submodule_pin_has_clean_and_seeded_failure() {
        let clean = SubmoduleEvidence {
            core_is_gitlink: true,
            cloud_is_gitlink: true,
            core_tag_signed: true,
            cloud_tag_signed: true,
            floating_branch: false,
            cloud_core_not_ahead: true,
        };
        assert!(submodule_pin(clean).is_empty());
        assert_eq!(
            submodule_pin(SubmoduleEvidence {
                floating_branch: true,
                ..clean
            })
            .len(),
            1
        );
    }

    #[test]
    fn docs_mirror_fresh_has_clean_and_seeded_failure() {
        assert!(docs_mirror_fresh(&[(b"same", b"same")]).is_empty());
        assert_eq!(docs_mirror_fresh(&[(b"source", b"stale")]).len(), 1);
    }

    #[test]
    fn public_legal_files_have_clean_and_seeded_failures() {
        assert!(public_legal_violations(&legal_fixture(&[])).is_empty());
        let missing_license = public_legal_violations(&legal_fixture(&[("LICENSE", None)]));
        assert_eq!(missing_license.len(), 1);
        assert_eq!(missing_license[0].gate, "public-legal-files");

        let stub_notice = public_legal_violations(&legal_fixture(&[(
            "NOTICE",
            Some(PUBLIC_COPYRIGHT_HOLDER.to_owned()),
        )]));
        assert_eq!(stub_notice.len(), 1, "a short stub must not satisfy NOTICE");

        let wrong_intake = public_legal_violations(&legal_fixture(&[(
            "SECURITY.md",
            Some("x".repeat(LEGAL_FILE_SIZE_MIN)),
        )]));
        assert_eq!(
            wrong_intake.len(),
            1,
            "a long file without the accepted intake address must fail"
        );

        let unsigned_dco = public_legal_violations(&legal_fixture(&[(
            ".github/workflows/dco.yml",
            Some(format!("name: dco{}", "\n#".repeat(LEGAL_FILE_SIZE_MIN))),
        )]));
        assert_eq!(
            unsigned_dco.len(),
            1,
            "DCO enforcement must actually check the sign-off trailer"
        );
    }

    /// Builds an otherwise-clean set of legal files, applying the named
    /// overrides. `None` seeds a missing file.
    fn legal_fixture(overrides: &[(&str, Option<String>)]) -> Vec<(&'static str, Option<String>)> {
        REQUIRED_PUBLIC_LEGAL_FILES
            .iter()
            .map(|(name, phrase)| {
                let clean = format!("{phrase}{}", " padding".repeat(LEGAL_FILE_SIZE_MIN));
                let text = overrides
                    .iter()
                    .find(|(candidate, _)| candidate == name)
                    .map_or(Some(clean), |(_, replacement)| replacement.clone());
                (*name, text)
            })
            .collect()
    }

    fn source_violations(source: &str, repository_kind: RepositoryKind) -> Vec<BoundaryViolation> {
        let syntax = syn::parse_file(source).expect("fixture is valid Rust");
        let mut violations = Vec::new();
        let mut visitor = SourceBoundaryVisitor {
            repository_kind,
            path: Path::new("fixture.rs"),
            violations: &mut violations,
        };
        visitor.visit_file(&syntax);
        violations
    }

    fn dependency_metadata(packages: &[(&str, &[(&str, &str)])]) -> serde_json::Value {
        let packages: Vec<serde_json::Value> = packages
            .iter()
            .map(|(name, dependencies)| {
                let id = format!("path+file:///checkout/{name}#0.1.0");
                let dependencies: Vec<serde_json::Value> = dependencies
                    .iter()
                    .map(|(dependency_name, path)| {
                        serde_json::json!({ "name": dependency_name, "path": path })
                    })
                    .collect();
                serde_json::json!({
                    "id": id,
                    "name": name,
                    "dependencies": dependencies,
                })
            })
            .collect();
        let workspace_members: Vec<String> = packages
            .iter()
            .filter_map(|package| package.get("id").and_then(serde_json::Value::as_str))
            .map(str::to_owned)
            .collect();
        serde_json::json!({
            "packages": packages,
            "workspace_members": workspace_members,
        })
    }
}
