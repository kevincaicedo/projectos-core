//! # pos-store
//!
//! The storage engine: SQLite (WAL) with FTS5 + sqlite-vec, and the BLAKE3 content-addressed blob store. One codepath for laptop and server (L12).
//!
//! NOTE: SQLite extension loading is the audited unsafe FFI leaf — when m0-s04 lands it, that module gets its own `[lints]` opt-out plus `// SAFETY:` arguments and a `SAFETY.md` entry (STYLE unsafe policy).
//!
//! Skeleton created by m0-s01; filled by m0-s04. Charter: master plan §19.

#![forbid(unsafe_code)]
