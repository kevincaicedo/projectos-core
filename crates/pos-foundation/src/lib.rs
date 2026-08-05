//! # pos-foundation
//!
//! Ids, injected clock, errors, config — near-zero deps. Everything sits on this crate; it depends on nothing internal.
//!
//! m0-s17 introduces the identity newtypes needed by the frozen capability
//! seam. m0-s03 adds event/log-specific ids and the injected clock. Charter:
//! master plan §19.

#![forbid(unsafe_code)]

use std::fmt;

macro_rules! opaque_id {
    ($name:ident, $label:literal) => {
        #[doc = concat!("Opaque, transport-neutral ", $label, " identifier.")]
        #[derive(Clone, Copy, Eq, Hash, Ord, PartialEq, PartialOrd)]
        pub struct $name([u8; 16]);

        impl $name {
            /// Builds an id from its stable 128-bit representation.
            #[must_use]
            pub const fn from_bytes(bytes: [u8; 16]) -> Self {
                Self(bytes)
            }

            /// Returns the stable representation without choosing a textual encoding.
            #[must_use]
            pub const fn into_bytes(self) -> [u8; 16] {
                self.0
            }
        }

        impl fmt::Debug for $name {
            fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(formatter, concat!(stringify!($name), "("))?;
                for byte in self.0 {
                    write!(formatter, "{byte:02x}")?;
                }
                formatter.write_str(")")
            }
        }
    };
}

opaque_id!(AccountId, "account");
opaque_id!(ProjectId, "project");
opaque_id!(RunId, "Run");
opaque_id!(WorkspaceId, "workspace");

#[cfg(test)]
mod tests {
    use super::ProjectId;

    #[test]
    fn debug_form_is_fixed_width_and_names_the_type() {
        let id = ProjectId::from_bytes([0xab; 16]);
        assert_eq!(
            format!("{id:?}"),
            "ProjectId(abababababababababababababababab)"
        );
    }
}
