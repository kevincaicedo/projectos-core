//! Mechanical ProjectOS discipline checks (m0-s02).
//!
//! The checker turns workspace and state laws into one local/CI command: every
//! Cargo target has a charter and explicit unsafe policy, operational panic
//! calls need a same-line invariant justification, projection tables are
//! writable only from `pos-log` apply paths, and every direct Cargo or npm
//! dependency has an exact dependency-ledger row.

#![forbid(unsafe_code)]

use proc_macro2::{TokenStream, TokenTree};
use std::collections::BTreeSet;
use std::env;
use std::ffi::OsStr;
use std::fs;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, ExitCode};
use syn::spanned::Spanned;
use syn::visit::Visit;

/// Prevents a malformed checkout from turning a policy check into an unbounded walk.
const SOURCE_FILE_COUNT_MAX: usize = 50_000;
/// ProjectOS source nesting is shallow; a deeper tree is likely a symlink/vendor mistake.
const DIRECTORY_DEPTH_MAX: usize = 64;
const SOURCE_FILE_SIZE_MAX: u64 = 4 * 1_048_576;
const MANIFEST_FILE_COUNT_MAX: usize = 10_000;
const MANIFEST_FILE_SIZE_MAX: u64 = 1_048_576;
const LEDGER_FILE_SIZE_MAX: u64 = 1_048_576;
const TARGET_COUNT_MAX: usize = 10_000;
const INVARIANT_MARKER: &str = "// INVARIANT:";

/// Audited FFI leaves (STYLE unsafe policy, m0-s04): the only target roots
/// allowed to carry `#![deny(unsafe_code)]` instead of forbid, and for each,
/// the only module files allowed to `#![allow(unsafe_code)]`. Every entry
/// requires a SAFETY.md beside the crate manifest. Growing this table is a
/// deliberate, reviewed amendment — the same bar as a dependency edge.
const UNSAFE_FFI_LEAVES: [UnsafeFfiLeaf; 1] = [UnsafeFfiLeaf {
    target_root: "crates/pos-store/src/lib.rs",
    allowed_modules: &["crates/pos-store/src/extensions.rs"],
    safety_document: "crates/pos-store/SAFETY.md",
}];

struct UnsafeFfiLeaf {
    target_root: &'static str,
    allowed_modules: &'static [&'static str],
    safety_document: &'static str,
}

#[derive(Debug, Eq, Ord, PartialEq, PartialOrd)]
struct Violation {
    policy: &'static str,
    path: PathBuf,
    line: Option<usize>,
    message: String,
}

impl Violation {
    fn render(&self, root: &Path) -> String {
        let path = self.path.strip_prefix(root).unwrap_or(&self.path);
        let location = self.line.map_or_else(
            || path.display().to_string(),
            |line| format!("{}:{line}", path.display()),
        );
        format!("{}: {location}: {}", self.policy, self.message)
    }
}

fn main() -> ExitCode {
    let root = match parse_root(env::args_os().skip(1)) {
        Ok(root) => root,
        Err(error) => {
            eprintln!("check-discipline: {error}");
            return ExitCode::FAILURE;
        }
    };

    match check_workspace(&root) {
        Ok(violations) if violations.is_empty() => {
            println!(
                "check-discipline: ok — charter, unsafe, panic, projection, and dependency laws hold"
            );
            ExitCode::SUCCESS
        }
        Ok(violations) => {
            eprintln!("check-discipline: {} violation(s):", violations.len());
            for violation in violations {
                eprintln!("  {}", violation.render(&root));
            }
            ExitCode::FAILURE
        }
        Err(error) => {
            eprintln!("check-discipline: could not complete: {error}");
            ExitCode::FAILURE
        }
    }
}

fn parse_root(mut args: impl Iterator<Item = std::ffi::OsString>) -> Result<PathBuf, String> {
    let first = args.next();
    let root = match first.as_deref() {
        None => env::current_dir().map_err(|error| format!("read current directory: {error}"))?,
        Some(value) if value == OsStr::new("--root") => {
            let Some(path) = args.next() else {
                return Err("--root requires a path".to_owned());
            };
            PathBuf::from(path)
        }
        Some(value) => return Err(format!("unexpected argument: {}", value.to_string_lossy())),
    };
    if args.next().is_some() {
        return Err("too many arguments".to_owned());
    }
    root.canonicalize()
        .map_err(|error| format!("resolve workspace root {}: {error}", root.display()))
}

fn check_workspace(root: &Path) -> Result<Vec<Violation>, String> {
    let metadata = cargo_metadata(root)?;
    let files = source_files(root)?;
    let mut violations = Vec::new();
    for path in files {
        match path.extension().and_then(OsStr::to_str) {
            Some("rs") => check_rust_file(root, &path, &mut violations)?,
            Some("sql") => check_sql_file(root, &path, &mut violations)?,
            _ => {}
        }
    }
    violations.extend(check_target_roots(root, &metadata)?);
    violations.extend(check_dependency_ledger(root, &metadata)?);
    violations.sort();
    Ok(violations)
}

fn cargo_metadata(root: &Path) -> Result<serde_json::Value, String> {
    let output = Command::new("cargo")
        .args(["metadata", "--format-version", "1", "--no-deps"])
        .current_dir(root)
        .output()
        .map_err(|error| format!("run cargo metadata: {error}"))?;
    if !output.status.success() {
        return Err(format!(
            "cargo metadata failed: {}",
            String::from_utf8_lossy(&output.stderr)
        ));
    }
    serde_json::from_slice(&output.stdout).map_err(|error| format!("parse cargo metadata: {error}"))
}

fn check_target_roots(root: &Path, metadata: &serde_json::Value) -> Result<Vec<Violation>, String> {
    let packages = metadata
        .get("packages")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no packages array".to_owned())?;
    let workspace_members = metadata
        .get("workspace_members")
        .and_then(serde_json::Value::as_array)
        .ok_or_else(|| "cargo metadata has no workspace_members array".to_owned())?;
    let workspace_ids: BTreeSet<&str> = workspace_members
        .iter()
        .filter_map(serde_json::Value::as_str)
        .collect();
    let mut target_paths = BTreeSet::new();
    for package in packages {
        let Some(package_id) = package.get("id").and_then(serde_json::Value::as_str) else {
            return Err("cargo metadata package has no id".to_owned());
        };
        if !workspace_ids.contains(package_id) {
            continue;
        }
        let targets = package
            .get("targets")
            .and_then(serde_json::Value::as_array)
            .ok_or_else(|| format!("cargo metadata package {package_id} has no targets"))?;
        for target in targets {
            let source = target
                .get("src_path")
                .and_then(serde_json::Value::as_str)
                .ok_or_else(|| format!("cargo target in {package_id} has no src_path"))?;
            let path = PathBuf::from(source);
            if !path.starts_with(root) {
                return Err(format!(
                    "cargo target root {} escapes workspace {}",
                    path.display(),
                    root.display()
                ));
            }
            target_paths.insert(path);
            if target_paths.len() > TARGET_COUNT_MAX {
                return Err(format!("crate target count exceeds {TARGET_COUNT_MAX}"));
            }
        }
    }

    let mut violations = Vec::new();
    for path in target_paths {
        let source = read_bounded_text(&path, SOURCE_FILE_SIZE_MAX, "crate target root")?;
        let syntax = syn::parse_file(&source)
            .map_err(|error| format!("parse crate target root {}: {error}", path.display()))?;
        if !has_crate_charter(&source) {
            violations.push(Violation {
                policy: "crate-charter",
                path: path.clone(),
                line: Some(1),
                message: "crate target must begin with a `//!` charter naming its responsibility"
                    .to_owned(),
            });
        }
        if let Some(message) = unsafe_policy_violation(root, &path, &syntax) {
            violations.push(Violation {
                policy: "unsafe-policy",
                path,
                line: None,
                message,
            });
        }
    }
    Ok(violations)
}

/// Forbid everywhere, except the audited FFI leaves, which must deny at the
/// root (so their one allow-listed module can exist) and carry SAFETY.md.
fn unsafe_policy_violation(root: &Path, path: &Path, syntax: &syn::File) -> Option<String> {
    let relative = path.strip_prefix(root).unwrap_or(path);
    let Some(leaf) = UNSAFE_FFI_LEAVES
        .iter()
        .find(|leaf| Path::new(leaf.target_root) == relative)
    else {
        if explicitly_forbids_unsafe(syntax) {
            return None;
        }
        return Some(
            "crate target needs an explicit `#![forbid(unsafe_code)]`; an audited FFI \
             exception requires a checker amendment and SAFETY.md"
                .to_owned(),
        );
    };
    if !explicitly_denies_unsafe(syntax) {
        return Some(format!(
            "audited FFI leaf must carry `#![deny(unsafe_code)]` at its root \
             (see {})",
            leaf.safety_document
        ));
    }
    if !root.join(leaf.safety_document).is_file() {
        return Some(format!(
            "audited FFI leaf requires {} to exist",
            leaf.safety_document
        ));
    }
    None
}

fn explicitly_denies_unsafe(syntax: &syn::File) -> bool {
    lint_level_names_unsafe_code(syntax, "deny")
}

fn has_crate_charter(source: &str) -> bool {
    source
        .lines()
        .next()
        .is_some_and(|line| line.trim_start_matches('\u{feff}').starts_with("//!"))
}

fn explicitly_forbids_unsafe(syntax: &syn::File) -> bool {
    lint_level_names_unsafe_code(syntax, "forbid")
}

fn lint_level_names_unsafe_code(syntax: &syn::File, level: &str) -> bool {
    syntax.attrs.iter().any(|attribute| {
        if !attribute.path().is_ident(level) {
            return false;
        }
        let syn::Meta::List(list) = &attribute.meta else {
            return false;
        };
        let mut tokens = Vec::new();
        flatten_tokens(list.tokens.clone(), &mut tokens);
        tokens
            .into_iter()
            .any(|token| matches!(token, TokenTree::Ident(ident) if ident == "unsafe_code"))
    })
}

/// `allow(unsafe_code)` is legal only in the exact module files the FFI-leaf
/// table names; anywhere else it is an attempted bypass of the unsafe policy.
fn unsafe_allow_violation(root: &Path, path: &Path, syntax: &syn::File) -> Option<String> {
    let allows_unsafe = lint_level_names_unsafe_code(syntax, "allow");
    if !allows_unsafe {
        return None;
    }
    let relative = path.strip_prefix(root).unwrap_or(path);
    let allowed = UNSAFE_FFI_LEAVES.iter().any(|leaf| {
        leaf.allowed_modules
            .iter()
            .any(|module| Path::new(module) == relative)
    });
    if allowed {
        None
    } else {
        Some(
            "`allow(unsafe_code)` outside the audited FFI-leaf table is a policy bypass; \
             amend check-discipline and SAFETY.md deliberately or remove it"
                .to_owned(),
        )
    }
}

fn source_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut stack = vec![(root.to_path_buf(), 0_usize)];
    let mut files = Vec::new();
    while let Some((directory, depth)) = stack.pop() {
        if depth > DIRECTORY_DEPTH_MAX {
            return Err(format!(
                "directory nesting exceeds {DIRECTORY_DEPTH_MAX} at {}",
                directory.display()
            ));
        }
        let entries = fs::read_dir(&directory)
            .map_err(|error| format!("read directory {}: {error}", directory.display()))?;
        for entry in entries {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| format!("read file type {}: {error}", path.display()))?;
            if file_type.is_symlink() || ignored_path(&path) {
                continue;
            }
            if file_type.is_dir() {
                stack.push((path, depth + 1));
            } else if file_type.is_file()
                && matches!(path.extension().and_then(OsStr::to_str), Some("rs" | "sql"))
            {
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

fn ignored_path(path: &Path) -> bool {
    path.components().any(|component| {
        matches!(
            component.as_os_str().to_str(),
            Some(".git" | "target" | "node_modules" | "dist")
        )
    })
}

fn check_rust_file(
    root: &Path,
    path: &Path,
    violations: &mut Vec<Violation>,
) -> Result<(), String> {
    let source = read_bounded_text(path, SOURCE_FILE_SIZE_MAX, "Rust source")?;
    let syntax = syn::parse_file(&source)
        .map_err(|error| format!("parse Rust source {}: {error}", path.display()))?;
    if let Some(message) = unsafe_allow_violation(root, path, &syntax) {
        violations.push(Violation {
            policy: "unsafe-policy",
            path: path.to_path_buf(),
            line: Some(1),
            message,
        });
    }
    let lines: Vec<&str> = source.lines().collect();
    let mut visitor = PolicyVisitor {
        root,
        path,
        lines: &lines,
        projection_writes_allowed: projection_writes_allowed(root, path),
        panic_policy_enforced: !is_integration_test_path(root, path),
        violations,
    };
    visitor.visit_file(&syntax);
    Ok(())
}

/// Files under a crate's `tests/` directory compile only into test binaries
/// (Cargo's definition of integration tests), so the panic policy treats the
/// whole file as test code — the same standing `#[test]` items already have.
fn is_integration_test_path(root: &Path, path: &Path) -> bool {
    let relative = path.strip_prefix(root).unwrap_or(path);
    relative
        .components()
        .any(|component| component.as_os_str() == OsStr::new("tests"))
}

fn check_sql_file(root: &Path, path: &Path, violations: &mut Vec<Violation>) -> Result<(), String> {
    let sql = read_bounded_text(path, SOURCE_FILE_SIZE_MAX, "SQL source")?;
    if !projection_writes_allowed(root, path)
        && let Some(operation) = projection_write(&sql)
    {
        violations.push(Violation {
            policy: "projection-write",
            path: path.to_path_buf(),
            line: None,
            message: format!(
                "{operation} writes a proj_* table outside crates/pos-log/src/apply.rs or apply/"
            ),
        });
    }
    Ok(())
}

struct PolicyVisitor<'a> {
    root: &'a Path,
    path: &'a Path,
    lines: &'a [&'a str],
    projection_writes_allowed: bool,
    panic_policy_enforced: bool,
    violations: &'a mut Vec<Violation>,
}

impl PolicyVisitor<'_> {
    fn check_panic_call(&mut self, method: &str, call_end_line: usize, call_end_column: usize) {
        if !self.panic_policy_enforced {
            return;
        }
        let justified = self
            .lines
            .get(call_end_line.saturating_sub(1))
            .is_some_and(|source_line| has_invariant_justification(source_line, call_end_column));
        if !justified {
            self.violations.push(Violation {
                policy: "panic-policy",
                path: self.path.to_path_buf(),
                line: Some(call_end_line),
                message: format!(
                    ".{method}() outside test code requires a trailing `{INVARIANT_MARKER} <why>` comment on the same line"
                ),
            });
        }
    }

    fn check_projection_literal(&mut self, value: &str, line: usize) {
        if self.projection_writes_allowed {
            return;
        }
        if let Some(operation) = projection_write(value) {
            let apply_root = self.root.join("crates/pos-log/src/apply");
            self.violations.push(Violation {
                policy: "projection-write",
                path: self.path.to_path_buf(),
                line: Some(line),
                message: format!(
                    "{operation} writes a proj_* table outside {}.rs or {}/",
                    apply_root.display(),
                    apply_root.display()
                ),
            });
        }
    }
}

impl<'ast> Visit<'ast> for PolicyVisitor<'_> {
    fn visit_item(&mut self, item: &'ast syn::Item) {
        if item_is_test_only(item) {
            return;
        }
        syn::visit::visit_item(self, item);
    }

    fn visit_expr_method_call(&mut self, call: &'ast syn::ExprMethodCall) {
        let method = call.method.to_string();
        if matches!(method.as_str(), "unwrap" | "expect") {
            let end = call.span().end();
            self.check_panic_call(&method, end.line, end.column);
        }
        syn::visit::visit_expr_method_call(self, call);
    }

    fn visit_macro(&mut self, mac: &'ast syn::Macro) {
        check_macro_panic_calls(&mac.tokens, |method, line, column| {
            self.check_panic_call(method, line, column);
        });
        syn::visit::visit_macro(self, mac);
    }

    fn visit_lit_str(&mut self, literal: &'ast syn::LitStr) {
        self.check_projection_literal(&literal.value(), literal.span().start().line);
        syn::visit::visit_lit_str(self, literal);
    }
}

fn has_invariant_justification(source_line: &str, call_end_column: usize) -> bool {
    let Some(trailing) = source_line.get(call_end_column..) else {
        return false;
    };
    let trailing = trailing.trim_start_matches(|character: char| {
        character.is_ascii_whitespace() || matches!(character, ';' | ',' | '?' | ')' | ']' | '}')
    });
    trailing
        .strip_prefix(INVARIANT_MARKER)
        .is_some_and(|reason| !reason.trim().is_empty())
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
    attributes.iter().any(attribute_is_test_only)
}

fn attribute_is_test_only(attribute: &syn::Attribute) -> bool {
    let path = attribute.path();
    if path
        .segments
        .last()
        .is_some_and(|segment| segment.ident == "test")
    {
        return true;
    }
    if !path.is_ident("cfg") {
        return false;
    }
    let syn::Meta::List(list) = &attribute.meta else {
        return false;
    };
    let mut tokens = list.tokens.clone().into_iter();
    matches!(tokens.next(), Some(TokenTree::Ident(ident)) if ident == "test")
        && tokens.next().is_none()
}

fn check_macro_panic_calls(tokens: &TokenStream, mut report: impl FnMut(&str, usize, usize)) {
    let mut flattened = Vec::new();
    flatten_tokens(tokens.clone(), &mut flattened);
    for window in flattened.windows(3) {
        let [
            TokenTree::Punct(dot),
            TokenTree::Ident(method),
            TokenTree::Group(arguments),
        ] = window
        else {
            continue;
        };
        let method_name = method.to_string();
        if dot.as_char() == '.'
            && matches!(method_name.as_str(), "unwrap" | "expect")
            && arguments.delimiter() == proc_macro2::Delimiter::Parenthesis
        {
            let end = arguments.span().end();
            report(&method_name, end.line, end.column);
        }
    }
}

fn flatten_tokens(stream: TokenStream, output: &mut Vec<TokenTree>) {
    for token in stream {
        if let TokenTree::Group(group) = &token {
            flatten_tokens(group.stream(), output);
        }
        output.push(token);
    }
}

fn projection_writes_allowed(root: &Path, path: &Path) -> bool {
    let Ok(relative) = path.strip_prefix(root) else {
        return false;
    };
    let apply_file = Path::new("crates/pos-log/src/apply.rs");
    let apply_directory = Path::new("crates/pos-log/src/apply");
    relative == apply_file || relative.starts_with(apply_directory)
}

fn projection_write(sql: &str) -> Option<&'static str> {
    let tokens: Vec<String> = sql
        .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
        .filter(|token| !token.is_empty())
        .map(|token| token.to_ascii_uppercase())
        .collect();

    for (index, token) in tokens.iter().enumerate() {
        let targets_projection = match token.as_str() {
            "INSERT" | "REPLACE" => target_is_projection_after(&tokens, index, "INTO"),
            "UPDATE" => update_target_is_projection(&tokens, index),
            "DELETE" => target_is_projection_after(&tokens, index, "FROM"),
            _ => false,
        };
        if targets_projection {
            return Some(match token.as_str() {
                "INSERT" => "INSERT",
                "REPLACE" => "REPLACE",
                "UPDATE" => "UPDATE",
                "DELETE" => "DELETE",
                _ => unreachable!("operation is matched above"), // INVARIANT: all arms originate from the operation match.
            });
        }
    }
    None
}

fn target_is_projection_after(tokens: &[String], operation_index: usize, keyword: &str) -> bool {
    let Some(keyword_index) = tokens
        .iter()
        .enumerate()
        .skip(operation_index + 1)
        .take(5)
        .find_map(|(index, token)| (token == keyword).then_some(index))
    else {
        return false;
    };
    target_tokens_name_projection(tokens.get(keyword_index + 1), tokens.get(keyword_index + 2))
}

fn update_target_is_projection(tokens: &[String], operation_index: usize) -> bool {
    let mut candidates = tokens
        .iter()
        .skip(operation_index + 1)
        .take(7)
        .filter(|candidate| {
            !matches!(
                candidate.as_str(),
                "OR" | "ROLLBACK" | "ABORT" | "REPLACE" | "FAIL" | "IGNORE"
            )
        });
    target_tokens_name_projection(candidates.next(), candidates.next())
}

fn target_tokens_name_projection(first: Option<&String>, second: Option<&String>) -> bool {
    first.is_some_and(|table| table.starts_with("PROJ_"))
        || matches!(first.map(String::as_str), Some("MAIN" | "TEMP"))
            && second.is_some_and(|table| table.starts_with("PROJ_"))
}

fn check_dependency_ledger(
    root: &Path,
    metadata: &serde_json::Value,
) -> Result<Vec<Violation>, String> {
    let (cargo_dependencies, npm_dependencies) = manifest_dependencies(root, metadata)?;
    let ledger_path = root.join("DEPENDENCIES.md");
    let ledger = read_bounded_text(&ledger_path, LEDGER_FILE_SIZE_MAX, "dependency ledger")?;
    let (ledger_cargo, ledger_npm, malformed_rows) = ledger_dependencies(&ledger);
    let mut violations = Vec::new();

    for message in malformed_rows {
        violations.push(Violation {
            policy: "dependency-ledger",
            path: ledger_path.clone(),
            line: None,
            message,
        });
    }
    compare_dependency_sets(
        "Cargo",
        &cargo_dependencies,
        &ledger_cargo,
        &ledger_path,
        &mut violations,
    );
    compare_dependency_sets(
        "npm",
        &npm_dependencies,
        &ledger_npm,
        &ledger_path,
        &mut violations,
    );
    Ok(violations)
}

fn manifest_dependencies(
    root: &Path,
    metadata: &serde_json::Value,
) -> Result<(BTreeSet<String>, BTreeSet<String>), String> {
    let packages = metadata["packages"]
        .as_array()
        .ok_or_else(|| "cargo metadata has no packages array".to_owned())?;
    let workspace_names: BTreeSet<&str> = packages
        .iter()
        .filter_map(|package| package["name"].as_str())
        .collect();
    let cargo_dependencies = packages
        .iter()
        .flat_map(|package| package["dependencies"].as_array().into_iter().flatten())
        .filter_map(|dependency| dependency["name"].as_str())
        .filter(|name| !workspace_names.contains(name))
        .map(str::to_owned)
        .collect();

    let mut npm_dependencies = BTreeSet::new();
    for manifest in package_json_files(root)? {
        let source = read_bounded_text(&manifest, MANIFEST_FILE_SIZE_MAX, "npm manifest")?;
        let package: serde_json::Value = serde_json::from_str(&source)
            .map_err(|error| format!("parse npm manifest {}: {error}", manifest.display()))?;
        for key in [
            "dependencies",
            "devDependencies",
            "optionalDependencies",
            "peerDependencies",
        ] {
            if let Some(dependencies) = package[key].as_object() {
                npm_dependencies.extend(dependencies.keys().cloned());
            }
        }
    }
    Ok((cargo_dependencies, npm_dependencies))
}

fn package_json_files(root: &Path) -> Result<Vec<PathBuf>, String> {
    let mut stack = vec![(root.to_path_buf(), 0_usize)];
    let mut manifests = Vec::new();
    while let Some((directory, depth)) = stack.pop() {
        if depth > DIRECTORY_DEPTH_MAX {
            return Err(format!(
                "directory nesting exceeds {DIRECTORY_DEPTH_MAX} at {}",
                directory.display()
            ));
        }
        for entry in fs::read_dir(&directory)
            .map_err(|error| format!("read directory {}: {error}", directory.display()))?
        {
            let entry = entry.map_err(|error| format!("read directory entry: {error}"))?;
            let path = entry.path();
            let file_type = entry
                .file_type()
                .map_err(|error| format!("read file type {}: {error}", path.display()))?;
            if file_type.is_symlink() || ignored_path(&path) {
                continue;
            }
            if file_type.is_dir() {
                stack.push((path, depth + 1));
            } else if entry.file_name() == OsStr::new("package.json") {
                manifests.push(path);
                if manifests.len() > MANIFEST_FILE_COUNT_MAX {
                    return Err(format!(
                        "npm manifest count exceeds {MANIFEST_FILE_COUNT_MAX}"
                    ));
                }
            }
        }
    }
    manifests.sort();
    Ok(manifests)
}

fn read_bounded_text(path: &Path, limit: u64, description: &str) -> Result<String, String> {
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
    String::from_utf8(bytes)
        .map_err(|error| format!("{description} {} is not UTF-8: {error}", path.display()))
}

#[derive(Clone, Copy)]
enum LedgerSection {
    None,
    Cargo,
    Npm,
}

fn ledger_dependencies(ledger: &str) -> (BTreeSet<String>, BTreeSet<String>, Vec<String>) {
    let mut section = LedgerSection::None;
    let mut cargo = BTreeSet::new();
    let mut npm = BTreeSet::new();
    let mut malformed = Vec::new();
    for (line_index, line) in ledger.lines().enumerate() {
        if line.starts_with("## Rust") {
            section = LedgerSection::Cargo;
            continue;
        }
        if line.starts_with("## npm") {
            section = LedgerSection::Npm;
            continue;
        }
        let cells: Vec<&str> = line.split('|').map(str::trim).collect();
        if cells.len() < 8 || !cells.get(1).is_some_and(|cell| cell.starts_with('`')) {
            continue;
        }
        let name = cells[1].trim_matches('`');
        if name.is_empty() {
            malformed.push(format!(
                "line {} has an empty dependency name",
                line_index + 1
            ));
            continue;
        }
        if cells[2..7].iter().any(|cell| cell.is_empty()) {
            malformed.push(format!(
                "line {} ({name}) must state usage, failure surface, eject path, justification, and owner",
                line_index + 1
            ));
        }
        let inserted = match section {
            LedgerSection::Cargo => cargo.insert(name.to_owned()),
            LedgerSection::Npm => npm.insert(name.to_owned()),
            LedgerSection::None => {
                malformed.push(format!(
                    "line {} ({name}) is outside a Rust/npm ledger section",
                    line_index + 1
                ));
                true
            }
        };
        if !inserted {
            malformed.push(format!(
                "line {} duplicates dependency {name}",
                line_index + 1
            ));
        }
    }
    (cargo, npm, malformed)
}

fn compare_dependency_sets(
    ecosystem: &str,
    manifests: &BTreeSet<String>,
    ledger: &BTreeSet<String>,
    ledger_path: &Path,
    violations: &mut Vec<Violation>,
) {
    for missing in manifests.difference(ledger) {
        violations.push(Violation {
            policy: "dependency-ledger",
            path: ledger_path.to_path_buf(),
            line: None,
            message: format!(
                "{ecosystem} dependency `{missing}` is in a manifest but not the ledger"
            ),
        });
    }
    for stale in ledger.difference(manifests) {
        violations.push(Violation {
            policy: "dependency-ledger",
            path: ledger_path.to_path_buf(),
            line: None,
            message: format!(
                "{ecosystem} dependency `{stale}` is ledgered but absent from manifests"
            ),
        });
    }
}

#[cfg(test)]
mod tests {
    use super::{
        PolicyVisitor, Violation, compare_dependency_sets, explicitly_forbids_unsafe,
        has_crate_charter, item_is_test_only, ledger_dependencies, projection_write,
        unsafe_allow_violation, unsafe_policy_violation,
    };
    use std::collections::BTreeSet;
    use std::path::Path;
    use syn::visit::Visit;

    fn rust_violations(source: &str) -> Vec<Violation> {
        rust_violations_at(source, "/workspace/crates/pos-store/src/lib.rs")
    }

    fn rust_violations_at(source: &str, path: &str) -> Vec<Violation> {
        let syntax = syn::parse_file(source).expect("fixture is valid Rust");
        let lines: Vec<&str> = source.lines().collect();
        let mut violations = Vec::new();
        let root = Path::new("/workspace");
        let path = Path::new(path);
        let mut visitor = PolicyVisitor {
            root,
            path,
            lines: &lines,
            projection_writes_allowed: false,
            panic_policy_enforced: !super::is_integration_test_path(root, path),
            violations: &mut violations,
        };
        visitor.visit_file(&syntax);
        violations
    }

    /// Integration-test files are test code by Cargo's definition: the panic
    /// policy exempts them wholesale, and only them.
    #[test]
    fn integration_test_files_are_test_code_for_the_panic_policy() {
        let helper = "fn fixture_helper() { let _ = disk().unwrap(); }";
        assert!(rust_violations_at(helper, "/workspace/crates/pos-log/tests/props.rs").is_empty());
        assert_eq!(
            rust_violations_at(helper, "/workspace/crates/pos-log/src/lib.rs").len(),
            1,
            "the same helper under src/ stays a violation"
        );
        assert_eq!(
            rust_violations_at(helper, "/workspace/crates/pos-log/src/tests.rs").len(),
            1,
            "a module merely named tests.rs is not the tests directory"
        );
    }

    #[test]
    fn operational_unwrap_and_expect_fail_the_panic_policy() {
        let source = "fn load() { let _ = disk().unwrap(); let _ = db().expect(\"open\"); }";
        let violations = rust_violations(source);
        assert_eq!(violations.len(), 2, "both panic calls must be named");
        assert!(
            violations
                .iter()
                .all(|violation| violation.policy == "panic-policy")
        );
    }

    #[test]
    fn test_code_and_justified_invariants_pass_the_panic_policy() {
        let source = r#"
fn admitted() {
    let _ = value().expect("admission checked"); // INVARIANT: admission rejects an empty value.
}
#[cfg(test)]
mod tests {
    #[test]
    fn fixture() { value().unwrap(); }
}
"#;
        assert!(rust_violations(source).is_empty());
    }

    #[test]
    fn panic_call_inside_a_macro_argument_is_not_a_bypass() {
        let source = "fn load() { consume!(disk().unwrap()); }";
        let violations = rust_violations(source);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].policy, "panic-policy");
    }

    #[test]
    fn invariant_marker_must_be_a_non_empty_trailing_comment() {
        let source = r#"
fn string_is_not_a_comment() {
    let _ = disk().expect("// INVARIANT: attacker-controlled text");
}
fn empty_comment_is_not_a_reason() {
    let _ = disk().expect("checked"); // INVARIANT:
}
"#;
        let violations = rust_violations(source);
        assert_eq!(violations.len(), 2);
        assert!(
            violations
                .iter()
                .all(|violation| violation.policy == "panic-policy")
        );
    }

    #[test]
    fn projection_write_outside_pos_log_apply_fails() {
        let source = [
            "const SQL: &str = r#\"INSERT",
            " INTO proj_tasks (id) VALUES (1)\"#;",
        ]
        .concat();
        let violations = rust_violations(&source);
        assert_eq!(violations.len(), 1);
        assert_eq!(violations[0].policy, "projection-write");
    }

    #[test]
    fn projection_operations_are_recognized_case_and_whitespace_independently() {
        assert_eq!(
            projection_write("update OR FAIL main.proj_tasks set x=1"),
            Some("UPDATE")
        );
        assert_eq!(
            projection_write("delete\nfrom `proj_tasks`"),
            Some("DELETE")
        );
        assert_eq!(projection_write("select * from proj_tasks"), None);
    }

    #[test]
    fn unlisted_dependency_fixture_fails() {
        let manifests = BTreeSet::from(["serde_json".to_owned(), "syn".to_owned()]);
        let ledger = BTreeSet::from(["serde_json".to_owned()]);
        let mut violations = Vec::new();
        compare_dependency_sets(
            "Cargo",
            &manifests,
            &ledger,
            Path::new("DEPENDENCIES.md"),
            &mut violations,
        );
        assert_eq!(violations.len(), 1);
        assert!(violations[0].message.contains("`syn`"));
    }

    #[test]
    fn ledger_rows_require_all_five_decision_fields() {
        let ledger = "## Rust (crates.io)\n| `serde_json` | checker | parser | replace | justified | founders |\n";
        let (cargo, npm, malformed) = ledger_dependencies(ledger);
        assert_eq!(cargo, BTreeSet::from(["serde_json".to_owned()]));
        assert!(npm.is_empty());
        assert!(malformed.is_empty());

        let missing_owner =
            "## Rust (crates.io)\n| `serde_json` | checker | parser | replace | justified | |\n";
        let (_, _, malformed) = ledger_dependencies(missing_owner);
        assert_eq!(malformed.len(), 1);
        assert!(malformed[0].contains("must state usage"));
    }

    #[test]
    fn cfg_test_item_is_recognized() {
        let file = syn::parse_file("#[cfg(test)] mod tests {}").expect("fixture is valid Rust");
        assert!(item_is_test_only(&file.items[0]));
    }

    #[test]
    fn crate_roots_require_a_charter_and_real_unsafe_attribute() {
        let valid = "//! # fixture\n#![forbid(unsafe_code)]\n";
        let syntax = syn::parse_file(valid).expect("fixture is valid Rust");
        assert!(has_crate_charter(valid));
        assert!(explicitly_forbids_unsafe(&syntax));

        let comment_only = "//! # fixture\n// #![forbid(unsafe_code)]\n";
        let syntax = syn::parse_file(comment_only).expect("fixture is valid Rust");
        assert!(has_crate_charter(comment_only));
        assert!(!explicitly_forbids_unsafe(&syntax));
        assert!(!has_crate_charter("#![forbid(unsafe_code)]\n"));
    }

    #[test]
    fn audited_ffi_leaf_may_deny_but_every_other_target_must_forbid() {
        let root = Path::new("/workspace");
        let deny_root =
            syn::parse_file("//! # store\n#![deny(unsafe_code)]\n").expect("fixture is valid Rust");
        // The audited leaf accepts deny — but only with its SAFETY.md, which
        // this fixture workspace does not have, so the violation names it.
        let message = unsafe_policy_violation(
            root,
            Path::new("/workspace/crates/pos-store/src/lib.rs"),
            &deny_root,
        )
        .expect("missing SAFETY.md must be a violation");
        assert!(message.contains("SAFETY.md"));

        // A non-excepted crate carrying deny instead of forbid is a violation.
        let message = unsafe_policy_violation(
            root,
            Path::new("/workspace/crates/pos-log/src/lib.rs"),
            &deny_root,
        )
        .expect("deny outside the FFI-leaf table must be a violation");
        assert!(message.contains("forbid(unsafe_code)"));

        // The real workspace: the audited leaf with its SAFETY.md passes.
        let real_root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("../..")
            .canonicalize()
            .expect("workspace root resolves");
        assert_eq!(
            unsafe_policy_violation(
                &real_root,
                &real_root.join("crates/pos-store/src/lib.rs"),
                &deny_root,
            ),
            None
        );
    }

    #[test]
    fn allow_unsafe_outside_the_ffi_leaf_table_is_a_bypass() {
        let root = Path::new("/workspace");
        let allowing =
            syn::parse_file("//! # leaf\n#![allow(unsafe_code)]\n").expect("fixture is valid Rust");
        assert!(
            unsafe_allow_violation(
                root,
                Path::new("/workspace/crates/pos-store/src/extensions.rs"),
                &allowing,
            )
            .is_none(),
            "the audited extension module is allow-listed"
        );
        let message = unsafe_allow_violation(
            root,
            Path::new("/workspace/crates/pos-ingest/src/sneaky.rs"),
            &allowing,
        )
        .expect("allow(unsafe_code) elsewhere must be flagged");
        assert!(message.contains("policy bypass"));

        let clean = syn::parse_file("//! # ordinary module\n").expect("fixture is valid Rust");
        assert!(
            unsafe_allow_violation(root, Path::new("/workspace/crates/x/src/lib.rs"), &clean)
                .is_none()
        );
    }
}
