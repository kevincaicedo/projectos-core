//! The audited unsafe FFI leaf (STYLE unsafe policy, SAFETY.md): static
//! registration of the sqlite-vec extension. This module is the only unsafe
//! code in the crate; `check-discipline` pins this exact file and fails if
//! unsafe appears anywhere else in `pos-store`.
#![allow(unsafe_code)]

use rusqlite::ffi;
use std::sync::Once;

/// Registers statically linked SQLite extensions for every connection this
/// process opens, exactly once, before the first open. FTS5 is compiled into
/// the bundled SQLite itself; sqlite-vec ships as a separate static object
/// with an auto-extension entry point.
pub(crate) fn register_static_extensions() {
    static REGISTER: Once = Once::new();
    REGISTER.call_once(|| {
        // SAFETY: `sqlite3_vec_init` is the extension entry point exported by
        // the statically linked sqlite-vec object; its real ABI is SQLite's
        // documented auto-extension signature `(db, pzErrMsg, pApi) -> int`,
        // and the crate declares it argumentless only because bindgen cannot
        // express the thunk — this cast is the crate's documented usage. The
        // function is `'static` (static linkage), so SQLite may retain and
        // call it for the process lifetime. Registration is serialized by
        // `Once` and happens before any connection exists. A silent
        // misregistration cannot pass: every open probes `vec_version()` and
        // fails typed (`ExtensionMissing`) if the module is absent.
        type AutoExtensionEntry = unsafe extern "C" fn(
            db: *mut ffi::sqlite3,
            error_message: *mut *mut std::os::raw::c_char,
            api: *const ffi::sqlite3_api_routines,
        ) -> std::os::raw::c_int;
        unsafe {
            ffi::sqlite3_auto_extension(Some(
                std::mem::transmute::<*const (), AutoExtensionEntry>(
                    sqlite_vec::sqlite3_vec_init as *const (),
                ),
            ));
        }
    });
}
