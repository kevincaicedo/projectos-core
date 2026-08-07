//! # pos-server (library half)
//!
//! The web shell's composition surface: control.db, auth v0, ACL, and the
//! assembled router (m0-s08). The binary in `main.rs` wires configuration
//! and signals around this; the integration suites drive the same router
//! in-process, so what CI proves is what the binary serves.
//!
//! Everything here is deployment/shell surface. Domain truth stays behind
//! `pos-api` (L12); this crate's own database (`control.db`) holds accounts,
//! sessions, workspaces, membership, and the audit log — state about *who
//! may reach* projects, never project state itself.

#![forbid(unsafe_code)]

pub mod acl;
pub mod auth;
pub mod control;
pub mod web;
