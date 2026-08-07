//! Credential classes and the secret seam (F51, security-and-taint skill).
//!
//! The absolute rule: secret *values* exist only inside [`SecretValue`],
//! which cannot be serialized, renders redacted in `Debug`/`Display`, and is
//! exposed exactly once — into a request header inside an adapter. Project
//! log, events, exports, preflight reports, and ledger rows carry
//! [`SecretRef`] ids and class labels only.
//!
//! Backing stores are a trait: the desktop OS-keychain provider and the
//! server KMS-envelope vault are the m1-s06 connector-secrets story's
//! implementations (visible debt, recorded in `docs/progress.md`);
//! [`MemorySecretStore`] is the in-process store tests and local development
//! use today. The seam — reference in, value out, revocation checked — is
//! frozen here so those stores plug in without touching a call site.

use pos_foundation::DeviceId;
use std::collections::BTreeMap;
use std::fmt;
use std::sync::Mutex;

/// Opaque reference to a secret in whatever store holds it. The string is an
/// id (`"byok/anthropic/primary"`), never key material.
#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub struct SecretRef(String);

impl SecretRef {
    #[must_use]
    pub fn new(id: impl Into<String>) -> Self {
        Self(id.into())
    }

    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// A resolved secret. No `Clone`, no serde, redacted rendering; the only way
/// to read it is [`Self::expose`], whose call sites are the audit surface.
pub struct SecretValue(String);

impl SecretValue {
    #[must_use]
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// The one intentional read. Callers put the value into an auth header
    /// and nowhere else — a new call site of this method is a review event.
    #[must_use]
    pub fn expose(&self) -> &str {
        &self.0
    }
}

impl fmt::Debug for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue(redacted)")
    }
}

// `Display` redacts exactly like `Debug`: there is no format specifier that
// reveals a secret value.
impl fmt::Display for SecretValue {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str("SecretValue(redacted)")
    }
}

/// Who pays and where the key lives (m0-s10): the three credential classes.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum CredentialClass {
    /// A ProjectOS-managed key. Provisioning arrives with billing (M6); the
    /// class is typed now so the ledger vocabulary never moves.
    Managed { secret_ref: SecretRef },
    /// The customer's own key: run-scoped release, revocable, and always
    /// `customer_billed` in the ledger — never fake ProjectOS model cost.
    Byok { secret_ref: SecretRef },
    /// A device-local provider session (an Ollama daemon, an LM Studio
    /// server). No key leaves the device; there may be no key at all.
    DeviceSession { adapter: String, device: DeviceId },
}

impl CredentialClass {
    /// The fixed ledger/UI label for this class.
    #[must_use]
    pub const fn label(&self) -> &'static str {
        match self {
            Self::Managed { .. } => "managed",
            Self::Byok { .. } => "byok",
            Self::DeviceSession { .. } => "device_session",
        }
    }
}

/// What an adapter receives after resolution: either nothing (device-local
/// endpoints) or one key value destined for one auth header.
pub enum CallAuth {
    None,
    ApiKey(SecretValue),
}

/// Typed resolution failure. Never contains the secret, obviously; also
/// never contains store internals a caller could mistake for one.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum SecretError {
    Unknown { secret_ref: String },
    Revoked { secret_ref: String },
    Store { reason: String },
}

impl fmt::Display for SecretError {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Unknown { secret_ref } => {
                write!(formatter, "no secret is stored under {secret_ref:?}")
            }
            Self::Revoked { secret_ref } => {
                write!(formatter, "the secret {secret_ref:?} is revoked")
            }
            Self::Store { reason } => write!(formatter, "secret store failure: {reason}"),
        }
    }
}

impl std::error::Error for SecretError {}

/// The store seam. Implementations own persistence and revocation state;
/// the gateway owns *when* resolution happens (after policy, before
/// transport) and the last-use audit.
pub trait SecretStore {
    /// # Errors
    ///
    /// Typed [`SecretError`]; a revoked reference fails here, which is what
    /// makes revocation block new dispatches immediately.
    fn resolve(&self, secret_ref: &SecretRef) -> Result<SecretValue, SecretError>;

    /// Marks a reference revoked. Idempotent.
    ///
    /// # Errors
    ///
    /// [`SecretError::Unknown`] when nothing is stored under the reference.
    fn revoke(&self, secret_ref: &SecretRef) -> Result<(), SecretError>;

    /// Records/returns last-use bookkeeping for the preflight surface.
    /// `None` means never used.
    fn last_used_ts_ms(&self, secret_ref: &SecretRef) -> Option<u64>;

    /// Called by the gateway when a resolved secret is actually dispatched.
    fn note_used(&self, secret_ref: &SecretRef, ts_ms: u64);
}

#[derive(Default)]
struct MemorySecretState {
    values: BTreeMap<String, String>,
    revoked: BTreeMap<String, bool>,
    last_used_ts_ms: BTreeMap<String, u64>,
}

/// In-process store for tests and local development. Not durable on
/// purpose: durability belongs to the keychain/vault implementations that
/// own it (module doc).
#[derive(Default)]
pub struct MemorySecretStore {
    state: Mutex<MemorySecretState>,
}

impl MemorySecretStore {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    pub fn insert(&self, secret_ref: &SecretRef, value: impl Into<String>) {
        let mut state = self
            .state
            .lock()
            .expect("secret store mutex is never poisoned: no panics while held"); // INVARIANT: every critical section below is panic-free straight-line code.
        state
            .values
            .insert(secret_ref.as_str().to_owned(), value.into());
    }
}

impl SecretStore for MemorySecretStore {
    fn resolve(&self, secret_ref: &SecretRef) -> Result<SecretValue, SecretError> {
        let state = self
            .state
            .lock()
            .expect("secret store mutex is never poisoned: no panics while held"); // INVARIANT: see insert.
        if state
            .revoked
            .get(secret_ref.as_str())
            .copied()
            .unwrap_or(false)
        {
            return Err(SecretError::Revoked {
                secret_ref: secret_ref.as_str().to_owned(),
            });
        }
        state
            .values
            .get(secret_ref.as_str())
            .map(|value| SecretValue::new(value.clone()))
            .ok_or_else(|| SecretError::Unknown {
                secret_ref: secret_ref.as_str().to_owned(),
            })
    }

    fn revoke(&self, secret_ref: &SecretRef) -> Result<(), SecretError> {
        let mut state = self
            .state
            .lock()
            .expect("secret store mutex is never poisoned: no panics while held"); // INVARIANT: see insert.
        if !state.values.contains_key(secret_ref.as_str()) {
            return Err(SecretError::Unknown {
                secret_ref: secret_ref.as_str().to_owned(),
            });
        }
        state.revoked.insert(secret_ref.as_str().to_owned(), true);
        Ok(())
    }

    fn last_used_ts_ms(&self, secret_ref: &SecretRef) -> Option<u64> {
        let state = self
            .state
            .lock()
            .expect("secret store mutex is never poisoned: no panics while held"); // INVARIANT: see insert.
        state.last_used_ts_ms.get(secret_ref.as_str()).copied()
    }

    fn note_used(&self, secret_ref: &SecretRef, ts_ms: u64) {
        let mut state = self
            .state
            .lock()
            .expect("secret store mutex is never poisoned: no panics while held"); // INVARIANT: see insert.
        state
            .last_used_ts_ms
            .insert(secret_ref.as_str().to_owned(), ts_ms);
    }
}

#[cfg(test)]
mod tests {
    use super::{MemorySecretStore, SecretError, SecretRef, SecretStore, SecretValue};

    #[test]
    fn secret_values_render_redacted_everywhere() {
        let value = SecretValue::new("sk-ant-EXTREMELY-SECRET");
        assert_eq!(format!("{value:?}"), "SecretValue(redacted)");
        assert_eq!(format!("{value}"), "SecretValue(redacted)");
        assert_eq!(value.expose(), "sk-ant-EXTREMELY-SECRET");
    }

    #[test]
    fn revocation_blocks_resolution_immediately_and_idempotently() {
        let store = MemorySecretStore::new();
        let secret_ref = SecretRef::new("byok/test/1");
        store.insert(&secret_ref, "sk-live");
        assert!(store.resolve(&secret_ref).is_ok());

        store.revoke(&secret_ref).expect("stored refs revoke");
        store.revoke(&secret_ref).expect("revoke is idempotent");
        let error = store
            .resolve(&secret_ref)
            .expect_err("a revoked ref must not resolve");
        assert!(matches!(error, SecretError::Revoked { .. }));

        let missing = store
            .revoke(&SecretRef::new("byok/none"))
            .expect_err("unknown refs are typed errors");
        assert!(matches!(missing, SecretError::Unknown { .. }));
    }

    #[test]
    fn last_use_is_recorded_for_the_preflight_audit() {
        let store = MemorySecretStore::new();
        let secret_ref = SecretRef::new("byok/test/2");
        store.insert(&secret_ref, "sk-live");
        assert_eq!(store.last_used_ts_ms(&secret_ref), None);
        store.note_used(&secret_ref, 1_700_000_000_123);
        assert_eq!(store.last_used_ts_ms(&secret_ref), Some(1_700_000_000_123));
    }
}
