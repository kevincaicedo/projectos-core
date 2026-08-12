//! Deny-by-default authorization (m0-s08): every `/api/*` dispatch resolves
//! session → workspace → role BEFORE the registry sees the request.
//!
//! Server projects live at `<data_root>/workspaces/<workspace-hex>/<name>.pos`
//! — the server's placement discipline (§8: server-side project directories,
//! one per project). The middleware never rewrites inputs or results; it
//! validates that every path a request names parses into that shape and that
//! the session's account holds a sufficient role in the named workspace.
//! Enumerating ids therefore changes nothing: naming another workspace's
//! path fails the membership lookup, and a path outside the grammar never
//! reaches the filesystem at all (L6: client strings are data until proven
//! well-formed).
//!
//! Roles v0: `viewer` reads; `member`+ mutates. Mutating operations that
//! carry no workspace context yet (`run.*` until m0-s12 types their inputs)
//! gate on the account's highest role anywhere — deny-by-default, never
//! allow-by-omission.

use crate::control::{ControlDb, Role};
use pos_api::{
    ApiError, CommandName, CostRollupInput, ProjectCreateInput, ProjectExportInput,
    ProjectPathInput, ProjectSeedInput, QueryName, RunControlInput, RunResumeInput, RunStartInput,
    RunStepsInput, StreamName,
};
use std::path::Path;

/// Project directory names: one conservative charset, no dots, no
/// separators, bounded length — traversal is unrepresentable, not filtered.
const PROJECT_NAME_LEN_MAX: usize = 64;

/// What an operation does to state, for role mapping.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Access {
    Read,
    Mutate,
}

/// The audited actions (F44): fixed vocabulary, extended only by the story
/// that adds the operation.
#[must_use]
pub fn audited_action(command: CommandName) -> Option<&'static str> {
    match command {
        CommandName::ProjectCreate => Some("project.create"),
        CommandName::ProjectOpen => Some("project.open"),
        CommandName::RunStart => Some("run.start"),
        CommandName::RunCancel => Some("run.cancel"),
        _ => None,
    }
}

/// Authorizes a command dispatch for `account`, returning the typed
/// `forbidden`/`invalid_input` envelope on refusal.
///
/// # Errors
///
/// `invalid_input` when the input does not parse or a path leaves the
/// placement grammar; `forbidden` when membership or role is insufficient;
/// `control_failure` when the control database fails.
pub fn authorize_command(
    control: &ControlDb,
    data_root: &Path,
    account: [u8; 16],
    command: CommandName,
    input_json: &str,
) -> Result<(), ApiError> {
    match command {
        CommandName::ProjectCreate => {
            let input: ProjectCreateInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)
        }
        CommandName::ProjectSeedSynthetic => {
            let input: ProjectSeedInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)
        }
        CommandName::ProjectExport => {
            let input: ProjectExportInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)?;
            authorize_path(control, data_root, account, &input.out, Access::Mutate)
        }
        // Opening is how a viewer views: read access to the named workspace.
        CommandName::ProjectOpen => {
            let input: ProjectPathInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Read)
        }
        CommandName::RunStart => {
            let input: RunStartInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)
        }
        CommandName::RunCancel | CommandName::RunPause => {
            let input: RunControlInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)
        }
        CommandName::RunResume => {
            let input: RunResumeInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Mutate)
        }
        // A command name added to the registry without a policy arm must not
        // dispatch silently (deny-by-default at compile time via exhaustive
        // match; `#[non_exhaustive]` upstream keeps this wildcard honest).
        _ => Err(forbidden(
            "no authorization policy is registered for this command",
        )),
    }
}

/// Authorizes a query dispatch. All v0 queries are reads; the two
/// path-bearing ones resolve their workspace, the rest are account-scoped
/// (each account dispatches into its own runtime instance).
///
/// # Errors
///
/// Same envelope contract as [`authorize_command`].
pub fn authorize_query(
    control: &ControlDb,
    data_root: &Path,
    account: [u8; 16],
    query: QueryName,
    input_json: &str,
) -> Result<(), ApiError> {
    match query {
        QueryName::ProjectInspect | QueryName::ProjectVerify => {
            let input: ProjectPathInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Read)
        }
        QueryName::CostRollup => {
            let input: CostRollupInput = parse(input_json)?;
            match input.path {
                Some(path) => authorize_path(control, data_root, account, &path, Access::Read),
                None => Ok(()),
            }
        }
        QueryName::CapabilitySnapshot
        | QueryName::ProjectList
        | QueryName::JobList
        | QueryName::Health => Ok(()),
        _ => Err(forbidden(
            "no authorization policy is registered for this query",
        )),
    }
}

/// Authorizes a live stream before its feeder opens the project directory.
/// Every current stream is path-bearing, so no account-wide fallback exists.
pub fn authorize_stream(
    control: &ControlDb,
    data_root: &Path,
    account: [u8; 16],
    stream: StreamName,
    input_json: &str,
) -> Result<(), ApiError> {
    match stream {
        StreamName::RunSteps => {
            let input: RunStepsInput = parse(input_json)?;
            authorize_path(control, data_root, account, &input.path, Access::Read)
        }
        _ => Err(forbidden(
            "no authorization policy is registered for this stream",
        )),
    }
}

/// The projects root for a workspace — handed to clients at signup/login so
/// they can construct legal paths.
#[must_use]
pub fn workspace_projects_root(data_root: &Path, workspace_id: [u8; 16]) -> String {
    data_root
        .join("workspaces")
        .join(crate::control::hex(&workspace_id))
        .display()
        .to_string()
}

/// Validates the placement grammar and the caller's role in the named
/// workspace, creating the workspace directory on first (authorized) use.
fn authorize_path(
    control: &ControlDb,
    data_root: &Path,
    account: [u8; 16],
    path_text: &str,
    access: Access,
) -> Result<(), ApiError> {
    let workspace_id = parse_placement(data_root, path_text)?;
    let role = control.role_in_workspace(workspace_id, account)?;
    let required = match access {
        Access::Read => Role::Viewer,
        Access::Mutate => Role::Member,
    };
    match role {
        // Deny-by-default: no membership row is indistinguishable from a
        // nonexistent workspace — enumerating ids learns nothing.
        None => Err(forbidden(
            "this account has no membership in the named workspace",
        )),
        Some(held) if held < required => Err(forbidden(
            "this operation mutates the workspace; role viewer is read-only",
        )),
        Some(_) => {
            let directory = data_root
                .join("workspaces")
                .join(crate::control::hex(&workspace_id));
            std::fs::create_dir_all(&directory).map_err(|error| ApiError {
                code: "control_failure",
                message: format!("create {}: {error}", directory.display()),
                retriable: true,
            })
        }
    }
}

/// Parses `<data_root>/workspaces/<32 hex>/<name>.pos` exactly; anything
/// else — traversal, absolute smuggling, foreign roots, odd charsets — is a
/// typed refusal before any filesystem access.
fn parse_placement(data_root: &Path, path_text: &str) -> Result<[u8; 16], ApiError> {
    let root_text = data_root.join("workspaces").display().to_string();
    let remainder = path_text
        .strip_prefix(&root_text)
        .and_then(|rest| rest.strip_prefix('/'))
        .ok_or_else(|| {
            invalid_path("project paths must sit under this server's workspaces root")
        })?;
    let (workspace_hex, name_with_extension) = remainder
        .split_once('/')
        .ok_or_else(|| invalid_path("expected <workspace>/<name>.pos under the root"))?;
    let workspace_id = pos_foundation_hex(workspace_hex)
        .ok_or_else(|| invalid_path("the workspace segment must be 32 lowercase hex chars"))?;
    let name = name_with_extension
        .strip_suffix(".pos")
        .ok_or_else(|| invalid_path("project directories end in .pos"))?;
    let name_ok = !name.is_empty()
        && name.len() <= PROJECT_NAME_LEN_MAX
        && name
            .chars()
            .all(|c| c.is_ascii_alphanumeric() || c == '-' || c == '_');
    if !name_ok {
        return Err(invalid_path(
            "project names are 1-64 chars of [A-Za-z0-9_-]",
        ));
    }
    Ok(workspace_id)
}

/// 32 lowercase hex chars → 16 bytes, or `None`.
fn pos_foundation_hex(text: &str) -> Option<[u8; 16]> {
    if text.len() != 32 {
        return None;
    }
    let mut bytes = [0_u8; 16];
    for (index, byte) in bytes.iter_mut().enumerate() {
        let pair = text.get(2 * index..2 * index + 2)?;
        if pair.chars().any(|c| c.is_ascii_uppercase()) {
            return None;
        }
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(bytes)
}

fn parse<'de, T: serde::Deserialize<'de>>(input_json: &'de str) -> Result<T, ApiError> {
    serde_json::from_str(input_json).map_err(|error| ApiError {
        code: "invalid_input",
        message: error.to_string(),
        retriable: false,
    })
}

fn forbidden(message: &str) -> ApiError {
    ApiError {
        code: "forbidden",
        message: message.to_owned(),
        retriable: false,
    }
}

fn invalid_path(message: &str) -> ApiError {
    ApiError {
        code: "invalid_input",
        message: message.to_owned(),
        retriable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::parse_placement;
    use std::path::Path;

    #[test]
    fn the_placement_grammar_rejects_everything_but_its_own_shape() {
        let root = Path::new("/srv/pos-data");
        let ws = "ab".repeat(16);
        let good = format!("/srv/pos-data/workspaces/{ws}/alpha_1.pos");
        assert!(parse_placement(root, &good).is_ok());
        for bad in [
            "/etc/passwd",
            "/srv/pos-data/workspaces/nothex/alpha.pos",
            &format!("/srv/pos-data/workspaces/{ws}/alpha"),
            &format!("/srv/pos-data/workspaces/{ws}/../escape.pos"),
            &format!("/srv/pos-data/workspaces/{ws}/a/b.pos"),
            &format!("/srv/pos-data/workspaces/{ws}/.pos"),
            &format!("/srv/pos-data/workspaces/{}/alpha.pos", "AB".repeat(16)),
            &format!("/srv/pos-data-evil/workspaces/{ws}/alpha.pos"),
        ] {
            assert!(
                parse_placement(root, bad).is_err(),
                "{bad} must not authorize"
            );
        }
    }
}
