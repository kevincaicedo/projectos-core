# pos-store unsafe inventory (STYLE unsafe policy)

`pos-store` is one of the audited FFI leaves named by PROJECTOS_STYLE ("SQLite
extension loading"). Everything unsafe in this crate lives in exactly one
module and exists for exactly one reason.

| Module | Unsafe operation | Argument |
|---|---|---|
| `src/extensions.rs` | `rusqlite::ffi::sqlite3_auto_extension` + the entry-point cast for `sqlite_vec::sqlite3_vec_init` | The sqlite-vec crate statically links the `vec0` extension and documents this exact registration pattern. The entry point is a `'static` C function whose ABI is defined by SQLite's auto-extension contract; registration happens once per process behind `std::sync::Once`, before any connection opens. Every connection open then probes `vec_version()` — a misregistration is a typed `ExtensionMissing` open failure, never a silent absence. |

Rules for changing this file's scope:

- New unsafe code needs a row here, a `// SAFETY:` argument at the site, and a
  `check-discipline` amendment naming the target — in the same PR.
- If a safe API appears for extension registration at equal measured cost, the
  unsafe version is wrong (STYLE); delete the row and restore
  `#![forbid(unsafe_code)]`.
