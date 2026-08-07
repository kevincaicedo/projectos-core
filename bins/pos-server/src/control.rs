//! `control.db` (m0-s08): the web shell's single-tenant control database —
//! accounts, sessions, workspaces, membership, and the audit log.
//!
//! This is deployment state, not project truth: nothing here is an event in
//! any project log, and losing this database loses accounts, never projects
//! (L1/L4). Deploy durability is Litestream replication of this one file —
//! documented as the deploy story per the milestone; not built in M0.
//!
//! Passwords are argon2id hashes; session tokens are stored only as BLAKE3
//! hashes, so a leaked control.db cannot be replayed into live sessions.
//! The audit log records `(actor, action, target, ts)` for every audited
//! authenticated action — secret values never enter any row (security-and-
//! taint: references and hashes only).

use pos_api::{ApiError, input_json};
use rusqlite::{Connection, OptionalExtension, params};
use serde::Serialize;
use std::path::Path;
use std::sync::Mutex;

/// Session lifetime. Two weeks balances "the founder demos weekly" against
/// unbounded token validity; there is no refresh flow in M0 (auth surface is
/// deliberately tiny — master plan §20).
pub const SESSION_TTL_MS: u64 = 14 * 24 * 60 * 60 * 1000;

/// Password bounds (L8): the argon2id cost makes unbounded input a DoS
/// vector, and single-digit passwords are not credentials.
pub const PASSWORD_LEN_MIN: usize = 10;
pub const PASSWORD_LEN_MAX: usize = 128;

/// Membership roles, v0 (m0-s08). Ordering is the privilege ladder.
#[derive(Clone, Copy, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum Role {
    Viewer,
    Member,
    Admin,
    Owner,
}

impl Role {
    #[must_use]
    pub fn parse(text: &str) -> Option<Self> {
        match text {
            "viewer" => Some(Self::Viewer),
            "member" => Some(Self::Member),
            "admin" => Some(Self::Admin),
            "owner" => Some(Self::Owner),
            _ => None,
        }
    }

    #[must_use]
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Viewer => "viewer",
            Self::Member => "member",
            Self::Admin => "admin",
            Self::Owner => "owner",
        }
    }
}

/// An authenticated session resolved from a presented token.
pub struct SessionIdentity {
    pub account_id: [u8; 16],
    pub email: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct AuditRow {
    pub actor: String,
    pub action: String,
    pub target: String,
    pub ts_ms: u64,
}

/// The one control-database handle. A single mutex-guarded connection is
/// deliberate at M0 scale (one team, one box): contention is bounded by the
/// tiny statement set, and one writer sidesteps SQLite busy handling.
pub struct ControlDb {
    connection: Mutex<Connection>,
}

impl ControlDb {
    /// Opens (creating on first use) the control database.
    ///
    /// # Errors
    ///
    /// Returns the typed envelope when the file cannot be opened or the
    /// schema cannot be installed.
    pub fn open(path: &Path) -> Result<Self, ApiError> {
        let connection = Connection::open(path).map_err(|error| control_failure(&error))?;
        // WAL + FULL mirrors the project-store durability posture: an audit
        // row that vanished on power loss would defeat its purpose.
        connection
            .pragma_update(None, "journal_mode", "WAL")
            .and_then(|()| connection.pragma_update(None, "synchronous", "FULL"))
            .and_then(|()| connection.pragma_update(None, "foreign_keys", "ON"))
            .map_err(|error| control_failure(&error))?;
        connection
            .execute_batch(
                "CREATE TABLE IF NOT EXISTS accounts (
                    id BLOB PRIMARY KEY,
                    email TEXT NOT NULL UNIQUE,
                    argon2id_hash TEXT NOT NULL,
                    created_ts_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS sessions (
                    token_hash BLOB PRIMARY KEY,
                    account BLOB NOT NULL REFERENCES accounts(id),
                    expires_ts_ms INTEGER NOT NULL,
                    created_device TEXT NOT NULL,
                    created_ts_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS workspaces (
                    id BLOB PRIMARY KEY,
                    name TEXT NOT NULL,
                    created_ts_ms INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS membership (
                    workspace BLOB NOT NULL REFERENCES workspaces(id),
                    account BLOB NOT NULL REFERENCES accounts(id),
                    role TEXT NOT NULL CHECK (role IN ('owner','admin','member','viewer')),
                    PRIMARY KEY (workspace, account)
                );
                CREATE TABLE IF NOT EXISTS audit_log (
                    seq INTEGER PRIMARY KEY AUTOINCREMENT,
                    actor BLOB NOT NULL,
                    action TEXT NOT NULL,
                    target TEXT NOT NULL,
                    ts_ms INTEGER NOT NULL
                );",
            )
            .map_err(|error| control_failure(&error))?;
        Ok(Self {
            connection: Mutex::new(connection),
        })
    }

    /// Creates the account plus its personal workspace and owner membership,
    /// atomically.
    ///
    /// # Errors
    ///
    /// `already_exists` for a taken email; `control_failure` otherwise.
    pub fn create_account(
        &self,
        account_id: [u8; 16],
        workspace_id: [u8; 16],
        email: &str,
        argon2id_hash: &str,
        now_ms: u64,
    ) -> Result<(), ApiError> {
        let mut connection = self.lock();
        let transaction = connection
            .transaction()
            .map_err(|error| control_failure(&error))?;
        let inserted = transaction
            .execute(
                "INSERT OR IGNORE INTO accounts (id, email, argon2id_hash, created_ts_ms)
                 VALUES (?1, ?2, ?3, ?4)",
                params![account_id, email, argon2id_hash, ms_to_sql(now_ms)],
            )
            .map_err(|error| control_failure(&error))?;
        if inserted == 0 {
            return Err(ApiError {
                code: "already_exists",
                message: "an account already exists for this email".to_owned(),
                retriable: false,
            });
        }
        transaction
            .execute(
                "INSERT INTO workspaces (id, name, created_ts_ms) VALUES (?1, 'Personal', ?2)",
                params![workspace_id, ms_to_sql(now_ms)],
            )
            .map_err(|error| control_failure(&error))?;
        transaction
            .execute(
                "INSERT INTO membership (workspace, account, role) VALUES (?1, ?2, 'owner')",
                params![workspace_id, account_id],
            )
            .map_err(|error| control_failure(&error))?;
        transaction
            .commit()
            .map_err(|error| control_failure(&error))
    }

    /// The stored hash for a login attempt, plus the account id.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure; `Ok(None)` for an unknown email
    /// (the caller folds that into the uniform bad-credentials envelope).
    pub fn account_by_email(&self, email: &str) -> Result<Option<([u8; 16], String)>, ApiError> {
        self.lock()
            .query_row(
                "SELECT id, argon2id_hash FROM accounts WHERE email = ?1",
                [email],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()
            .map_err(|error| control_failure(&error))
    }

    /// Stores a new session under the token's hash — the raw token is never
    /// persisted.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn insert_session(
        &self,
        token_hash: [u8; 32],
        account_id: [u8; 16],
        created_device: &str,
        now_ms: u64,
    ) -> Result<(), ApiError> {
        self.lock()
            .execute(
                "INSERT INTO sessions (token_hash, account, expires_ts_ms, created_device, created_ts_ms)
                 VALUES (?1, ?2, ?3, ?4, ?5)",
                params![
                    token_hash,
                    account_id,
                    ms_to_sql(now_ms.saturating_add(SESSION_TTL_MS)),
                    created_device,
                    ms_to_sql(now_ms)
                ],
            )
            .map(|_| ())
            .map_err(|error| control_failure(&error))
    }

    /// Resolves a presented token hash to a live session, sweeping it if
    /// expired. `Ok(None)` is "not authenticated", never an error.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn session_identity(
        &self,
        token_hash: [u8; 32],
        now_ms: u64,
    ) -> Result<Option<SessionIdentity>, ApiError> {
        let connection = self.lock();
        let row = connection
            .query_row(
                "SELECT sessions.account, sessions.expires_ts_ms, accounts.email
                 FROM sessions JOIN accounts ON accounts.id = sessions.account
                 WHERE sessions.token_hash = ?1",
                [token_hash],
                |row| {
                    Ok((
                        row.get::<_, [u8; 16]>(0)?,
                        row.get::<_, i64>(1)?,
                        row.get::<_, String>(2)?,
                    ))
                },
            )
            .optional()
            .map_err(|error| control_failure(&error))?;
        let Some((account_id, expires_sql, email)) = row else {
            return Ok(None);
        };
        if sql_to_ms(expires_sql) <= now_ms {
            connection
                .execute("DELETE FROM sessions WHERE token_hash = ?1", [token_hash])
                .map_err(|error| control_failure(&error))?;
            return Ok(None);
        }
        Ok(Some(SessionIdentity { account_id, email }))
    }

    /// Deletes the session (logout). Deleting an unknown token is a no-op —
    /// logout is idempotent.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn delete_session(&self, token_hash: [u8; 32]) -> Result<(), ApiError> {
        self.lock()
            .execute("DELETE FROM sessions WHERE token_hash = ?1", [token_hash])
            .map(|_| ())
            .map_err(|error| control_failure(&error))
    }

    /// The account's role in a workspace; `None` means no membership, which
    /// callers must treat as deny (deny-by-default).
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn role_in_workspace(
        &self,
        workspace_id: [u8; 16],
        account_id: [u8; 16],
    ) -> Result<Option<Role>, ApiError> {
        let text: Option<String> = self
            .lock()
            .query_row(
                "SELECT role FROM membership WHERE workspace = ?1 AND account = ?2",
                params![workspace_id, account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| control_failure(&error))?;
        Ok(text.as_deref().and_then(Role::parse))
    }

    /// The account's highest role across all memberships — the global gate
    /// for mutating operations that carry no workspace context yet (run.*
    /// until m0-s12 types their inputs).
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn max_role(&self, account_id: [u8; 16]) -> Result<Option<Role>, ApiError> {
        let connection = self.lock();
        let mut statement = connection
            .prepare("SELECT role FROM membership WHERE account = ?1")
            .map_err(|error| control_failure(&error))?;
        let roles = statement
            .query_map([account_id], |row| row.get::<_, String>(0))
            .map_err(|error| control_failure(&error))?;
        let mut max: Option<Role> = None;
        for role in roles {
            let role = role.map_err(|error| control_failure(&error))?;
            let Some(parsed) = Role::parse(&role) else {
                continue;
            };
            max = Some(max.map_or(parsed, |current| current.max(parsed)));
        }
        Ok(max)
    }

    /// One workspace the account owns (the personal workspace from signup).
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn owned_workspace(&self, account_id: [u8; 16]) -> Result<Option<[u8; 16]>, ApiError> {
        self.lock()
            .query_row(
                "SELECT workspace FROM membership WHERE account = ?1 AND role = 'owner'
                 ORDER BY workspace LIMIT 1",
                [account_id],
                |row| row.get(0),
            )
            .optional()
            .map_err(|error| control_failure(&error))
    }

    /// Appends an audit row. Callers pass fixed-vocabulary actions and
    /// non-secret targets (paths, names) only.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure — audit write failure fails the
    /// action, because an unaudited authenticated action is the defect F44
    /// exists to prevent.
    pub fn audit(
        &self,
        actor: [u8; 16],
        action: &str,
        target: &str,
        now_ms: u64,
    ) -> Result<(), ApiError> {
        debug_assert!(
            action.chars().all(|c| c.is_ascii_lowercase() || c == '.'),
            "audit actions are fixed vocabulary tokens"
        );
        self.lock()
            .execute(
                "INSERT INTO audit_log (actor, action, target, ts_ms) VALUES (?1, ?2, ?3, ?4)",
                params![actor, action, target, ms_to_sql(now_ms)],
            )
            .map(|_| ())
            .map_err(|error| control_failure(&error))
    }

    /// The audit trail, newest last, serialized as the `/auth/audit` body.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn audit_rows_json(&self, actor: [u8; 16]) -> Result<String, ApiError> {
        let connection = self.lock();
        let mut statement = connection
            .prepare(
                "SELECT actor, action, target, ts_ms FROM audit_log
                 WHERE actor = ?1 ORDER BY seq",
            )
            .map_err(|error| control_failure(&error))?;
        let rows = statement
            .query_map([actor], |row| {
                Ok(AuditRow {
                    actor: hex(&row.get::<_, [u8; 16]>(0)?),
                    action: row.get(1)?,
                    target: row.get(2)?,
                    ts_ms: sql_to_ms(row.get(3)?),
                })
            })
            .map_err(|error| control_failure(&error))?;
        let mut collected = Vec::new();
        for row in rows {
            collected.push(row.map_err(|error| control_failure(&error))?);
        }
        input_json(&AuditReport { rows: collected })
    }

    /// Test seam and future invite flow: grants a role directly. The v0
    /// registry has no invite route (auth surface stays tiny); the RBAC
    /// matrix suite uses this to place a viewer into a workspace.
    ///
    /// # Errors
    ///
    /// `control_failure` on database failure.
    pub fn grant_membership(
        &self,
        workspace_id: [u8; 16],
        account_id: [u8; 16],
        role: Role,
    ) -> Result<(), ApiError> {
        self.lock()
            .execute(
                "INSERT OR REPLACE INTO membership (workspace, account, role) VALUES (?1, ?2, ?3)",
                params![workspace_id, account_id, role.as_str()],
            )
            .map(|_| ())
            .map_err(|error| control_failure(&error))
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Connection> {
        match self.connection.lock() {
            Ok(guard) => guard,
            // A poisoned lock means a panic elsewhere; the connection itself
            // is still transactionally consistent (SQLite owns atomicity).
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct AuditReport {
    rows: Vec<AuditRow>,
}

/// SQLite INTEGER is i64; wall-clock milliseconds saturate rather than wrap
/// (u64 ms overflows i64 around year 292M — saturation is the documented
/// policy, mirroring `SystemWallClock`).
const fn ms_to_sql(ms: u64) -> i64 {
    if ms > i64::MAX as u64 {
        i64::MAX
    } else {
        ms as i64
    }
}

const fn sql_to_ms(value: i64) -> u64 {
    if value < 0 { 0 } else { value.cast_unsigned() }
}

pub fn hex(bytes: &[u8]) -> String {
    use std::fmt::Write as _;
    let mut text = String::with_capacity(bytes.len() * 2);
    for byte in bytes {
        write!(text, "{byte:02x}").expect("writing hex into a String cannot fail"); // INVARIANT: fmt::Write on String is infallible.
    }
    text
}

fn control_failure(error: &rusqlite::Error) -> ApiError {
    ApiError {
        code: "control_failure",
        message: format!("control.db: {error}"),
        retriable: false,
    }
}
