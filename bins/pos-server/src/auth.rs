//! Auth v0 (m0-s08): argon2id + opaque session cookies, nothing else.
//! Every added auth feature is attack surface; OAuth, password reset, and
//! WebAuthn are deliberately absent (master plan §20, post-MVP).
//!
//! Cookie discipline (security-and-taint): `HttpOnly` keeps tokens out of
//! script reach, `SameSite=Lax` blocks cross-site POSTs, `Secure` pins the
//! cookie to TLS (browsers exempt localhost, which is the only non-TLS
//! deployment this shell supports before a reverse proxy). Tokens are 32
//! OS-entropy bytes; only their BLAKE3 hash is stored or compared.

use crate::control::{ControlDb, PASSWORD_LEN_MAX, PASSWORD_LEN_MIN, SessionIdentity};
use argon2::Argon2;
use argon2::password_hash::{PasswordHash, PasswordHasher, PasswordVerifier, SaltString};
use pos_api::ApiError;

/// The one session cookie. A fixed name keeps logout/replace simple.
pub const SESSION_COOKIE_NAME: &str = "pos_session";

/// Hashes a signup password with argon2id v19 default parameters
/// (m=19 MiB, t=2, p=1 — the OWASP-recommended interactive profile; the
/// crate defaults track it, and the PHC string records the exact parameters
/// per hash, so tuning later invalidates nothing).
///
/// # Errors
///
/// `invalid_input` for out-of-bounds passwords; `auth_failure` when hashing
/// itself fails.
pub fn hash_password(password: &str) -> Result<String, ApiError> {
    if password.len() < PASSWORD_LEN_MIN || password.len() > PASSWORD_LEN_MAX {
        return Err(ApiError {
            code: "invalid_input",
            message: format!(
                "password must be between {PASSWORD_LEN_MIN} and {PASSWORD_LEN_MAX} bytes"
            ),
            retriable: false,
        });
    }
    let mut salt_bytes = [0_u8; 16];
    fill_random(&mut salt_bytes)?;
    let salt = SaltString::encode_b64(&salt_bytes).map_err(auth_failure)?;
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|hash| hash.to_string())
        .map_err(auth_failure)
}

/// Verifies a login attempt against a stored PHC hash.
#[must_use]
pub fn password_matches(password: &str, stored_phc: &str) -> bool {
    let Ok(parsed) = PasswordHash::new(stored_phc) else {
        // A malformed stored hash is a defect, but the honest answer to a
        // login attempt against it is still "no".
        return false;
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// A freshly minted session token: the value for the cookie and the hash for
/// the database. The raw token exists only in the response.
pub struct MintedToken {
    pub cookie_value: String,
    pub token_hash: [u8; 32],
}

/// # Errors
///
/// `auth_failure` when the OS entropy source fails — refusing login beats
/// minting a predictable session.
pub fn mint_session_token() -> Result<MintedToken, ApiError> {
    let mut token = [0_u8; 32];
    fill_random(&mut token)?;
    Ok(MintedToken {
        cookie_value: crate::control::hex(&token),
        token_hash: *blake3::hash(&token).as_bytes(),
    })
}

/// Recomputes the storage hash for a presented cookie value. `None` for
/// anything that is not a 64-char lowercase-hex token — malformed cookies
/// are unauthenticated, not errors.
#[must_use]
pub fn presented_token_hash(cookie_value: &str) -> Option<[u8; 32]> {
    if cookie_value.len() != 64 {
        return None;
    }
    let mut token = [0_u8; 32];
    for (index, byte) in token.iter_mut().enumerate() {
        let pair = cookie_value.get(2 * index..2 * index + 2)?;
        *byte = u8::from_str_radix(pair, 16).ok()?;
    }
    Some(*blake3::hash(&token).as_bytes())
}

/// Resolves the session identity from a request's `Cookie` header value.
///
/// # Errors
///
/// `control_failure` on database failure; `Ok(None)` is "not authenticated".
pub fn identity_from_cookie_header(
    control: &ControlDb,
    cookie_header: Option<&str>,
    now_ms: u64,
) -> Result<Option<SessionIdentity>, ApiError> {
    let Some(header) = cookie_header else {
        return Ok(None);
    };
    let Some(value) = cookie_value(header, SESSION_COOKIE_NAME) else {
        return Ok(None);
    };
    let Some(token_hash) = presented_token_hash(&value) else {
        return Ok(None);
    };
    control.session_identity(token_hash, now_ms)
}

/// The `Set-Cookie` value that installs a session.
#[must_use]
pub fn session_cookie(value: &str) -> String {
    format!("{SESSION_COOKIE_NAME}={value}; Path=/; HttpOnly; SameSite=Lax; Secure")
}

/// The `Set-Cookie` value that clears the session on logout.
#[must_use]
pub fn clearing_cookie() -> String {
    format!("{SESSION_COOKIE_NAME}=; Path=/; HttpOnly; SameSite=Lax; Secure; Max-Age=0")
}

/// Minimal cookie-pair scan: no attributes appear in request `Cookie`
/// headers, so splitting on `;` and `=` is the whole grammar.
fn cookie_value(header: &str, name: &str) -> Option<String> {
    for pair in header.split(';') {
        let (key, value) = pair.split_once('=')?;
        if key.trim() == name {
            return Some(value.trim().to_owned());
        }
    }
    None
}

fn fill_random(buffer: &mut [u8]) -> Result<(), ApiError> {
    getrandom::fill(buffer).map_err(|error| ApiError {
        code: "auth_failure",
        message: format!("entropy source unavailable: {error}"),
        retriable: true,
    })
}

fn auth_failure(error: argon2::password_hash::Error) -> ApiError {
    ApiError {
        code: "auth_failure",
        message: format!("password hashing failed: {error}"),
        retriable: false,
    }
}

#[cfg(test)]
mod tests {
    use super::{
        clearing_cookie, hash_password, mint_session_token, password_matches, presented_token_hash,
        session_cookie,
    };

    #[test]
    fn passwords_hash_verify_and_reject() {
        let hash = hash_password("correct horse battery").expect("hashes");
        assert!(hash.starts_with("$argon2id$"));
        assert!(password_matches("correct horse battery", &hash));
        assert!(!password_matches("wrong horse", &hash));
        assert!(!password_matches("correct horse battery", "not-a-phc-hash"));
        let short = hash_password("short").expect_err("too short");
        assert_eq!(short.code, "invalid_input");
    }

    #[test]
    fn tokens_round_trip_through_their_hash_and_cookies_carry_the_attributes() {
        let minted = mint_session_token().expect("entropy available");
        assert_eq!(minted.cookie_value.len(), 64);
        assert_eq!(
            presented_token_hash(&minted.cookie_value),
            Some(minted.token_hash)
        );
        assert_eq!(presented_token_hash("zz"), None);
        let cookie = session_cookie(&minted.cookie_value);
        for attribute in ["HttpOnly", "SameSite=Lax", "Secure", "Path=/"] {
            assert!(cookie.contains(attribute), "missing {attribute}");
            assert!(clearing_cookie().contains(attribute));
        }
        assert!(clearing_cookie().contains("Max-Age=0"));
    }

    #[test]
    fn two_minted_tokens_differ() {
        let first = mint_session_token().expect("entropy");
        let second = mint_session_token().expect("entropy");
        assert_ne!(first.cookie_value, second.cookie_value);
    }
}
