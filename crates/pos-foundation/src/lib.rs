//! # pos-foundation
//!
//! Ids, injected clock, errors, config — near-zero deps. Everything sits on this crate; it depends on nothing internal.
//!
//! m0-s17 introduced the identity newtypes needed by the frozen capability
//! seam. m0-s03 adds the event/log id vocabulary (`EventSeq`, `DeviceId`,
//! `UserId`, `JobId`) and the injected wall clock. Charter: master plan §19.

#![forbid(unsafe_code)]

mod clock;
pub mod telemetry;

pub use clock::{ManualWallClock, SystemWallClock, WallClock};

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

            /// Lowercase 32-character hex — the one textual encoding manifests,
            /// exports, and CLI output share, so a rendered id always parses back.
            #[must_use]
            pub fn to_hex(self) -> String {
                let mut hex = String::with_capacity(32);
                for byte in self.0 {
                    use fmt::Write as _;
                    write!(hex, "{byte:02x}").expect("writing hex into a String cannot fail"); // INVARIANT: fmt::Write on String is infallible.
                }
                hex
            }

            /// Parses [`Self::to_hex`] output. `None` for anything else — a
            /// malformed id in a manifest is the caller's typed error, not a panic.
            #[must_use]
            pub fn from_hex(text: &str) -> Option<Self> {
                if text.len() != 32 || !text.is_ascii() {
                    return None;
                }
                let digits = text.as_bytes();
                let mut bytes = [0_u8; 16];
                for (index, byte) in bytes.iter_mut().enumerate() {
                    let high = hex_nibble(digits[2 * index])?;
                    let low = hex_nibble(digits[2 * index + 1])?;
                    *byte = (high << 4) | low;
                }
                Some(Self(bytes))
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

        // Ids serialize as 16-byte strings, not integer arrays: CBOR event
        // bodies stay compact and the wire shape cannot drift per format.
        impl serde::Serialize for $name {
            fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
                serializer.serialize_bytes(&self.0)
            }
        }

        impl<'de> serde::Deserialize<'de> for $name {
            fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
                deserialize_bytes16(deserializer).map(Self)
            }
        }
    };
}

/// Accepts the byte-string form (CBOR) and the numeric-array form (JSON),
/// rejecting any other shape or length with a typed serde error.
fn deserialize_bytes16<'de, D: serde::Deserializer<'de>>(
    deserializer: D,
) -> Result<[u8; 16], D::Error> {
    struct Bytes16Visitor;

    impl<'de> serde::de::Visitor<'de> for Bytes16Visitor {
        type Value = [u8; 16];

        fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter.write_str("exactly 16 id bytes")
        }

        fn visit_bytes<E: serde::de::Error>(self, bytes: &[u8]) -> Result<Self::Value, E> {
            Self::Value::try_from(bytes).map_err(|_| E::invalid_length(bytes.len(), &"16 id bytes"))
        }

        fn visit_seq<A: serde::de::SeqAccess<'de>>(
            self,
            mut sequence: A,
        ) -> Result<Self::Value, A::Error> {
            let mut bytes = [0_u8; 16];
            for (index, byte) in bytes.iter_mut().enumerate() {
                *byte = sequence
                    .next_element()?
                    .ok_or_else(|| serde::de::Error::invalid_length(index, &"16 id bytes"))?;
            }
            if sequence.next_element::<u8>()?.is_some() {
                return Err(serde::de::Error::invalid_length(17, &"16 id bytes"));
            }
            Ok(bytes)
        }
    }

    deserializer.deserialize_bytes(Bytes16Visitor)
}

const fn hex_nibble(digit: u8) -> Option<u8> {
    match digit {
        b'0'..=b'9' => Some(digit - b'0'),
        b'a'..=b'f' => Some(digit - b'a' + 10),
        _ => None,
    }
}

opaque_id!(AccountId, "account");
opaque_id!(ArtifactId, "artifact");
opaque_id!(CheckpointId, "Run checkpoint");
// Chunk and Evidence ids are BLAKE3 digests truncated to 128 bits rather than
// full 256-bit hashes (m1-s02). Two reasons, both structural: `EntityRef.id`
// is `[u8; 16]`, so an id wider than that could not participate in the L2
// why-chain at all; and a citation points at a chunk forever, so the id also
// has to stay cheap in every index and export. 128 bits keeps the collision
// probability below 10^-26 at the 1M-chunk §18 scale, which is far under the
// probability of the disk lying to us.
opaque_id!(ChunkId, "evidence chunk");
opaque_id!(CronId, "cron schedule");
opaque_id!(DeviceId, "origin device/server");
opaque_id!(EvidenceId, "Evidence item");
opaque_id!(ExecutionLeaseId, "execution lease");
opaque_id!(GateReceiptId, "human gate receipt");
opaque_id!(JobId, "scheduled job");
opaque_id!(ProjectId, "project");
opaque_id!(SourceId, "connected source");
opaque_id!(QuestionId, "Run question");
opaque_id!(RunId, "Run");
opaque_id!(ToolCallId, "tool call");
opaque_id!(UserId, "user");
opaque_id!(ValidationId, "Run validation");
opaque_id!(WorkspaceId, "workspace");

/// Per-project, contiguous event sequence number, assigned at append
/// (master plan §7.1). `ZERO` is the head of an empty log — the first
/// appended event is seq 1 — so "no events yet" needs no `Option`.
#[derive(Clone, Copy, Debug, Default, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct EventSeq(u64);

impl EventSeq {
    pub const ZERO: Self = Self(0);

    #[must_use]
    pub const fn new(value: u64) -> Self {
        Self(value)
    }

    #[must_use]
    pub const fn value(self) -> u64 {
        self.0
    }

    /// The seq the next append must receive (contiguity is asserted there).
    /// A u64 log head cannot overflow within the lifetime of any project;
    /// saturating keeps the arithmetic total without inventing a panic path.
    #[must_use]
    pub const fn next(self) -> Self {
        Self(self.0.saturating_add(1))
    }
}

impl fmt::Display for EventSeq {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", self.0)
    }
}

impl serde::Serialize for EventSeq {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        serializer.serialize_u64(self.0)
    }
}

impl<'de> serde::Deserialize<'de> for EventSeq {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        u64::deserialize(deserializer).map(Self)
    }
}

#[cfg(test)]
mod tests {
    use super::{EventSeq, ProjectId};

    #[test]
    fn debug_form_is_fixed_width_and_names_the_type() {
        let id = ProjectId::from_bytes([0xab; 16]);
        assert_eq!(
            format!("{id:?}"),
            "ProjectId(abababababababababababababababab)"
        );
    }

    #[test]
    fn hex_round_trips_and_rejects_malformed_text() {
        let id = ProjectId::from_bytes([
            0x00, 0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88, 0x99, 0xaa, 0xbb, 0xcc, 0xdd,
            0xee, 0xff,
        ]);
        let hex = id.to_hex();
        assert_eq!(hex, "00112233445566778899aabbccddeeff");
        assert_eq!(ProjectId::from_hex(&hex), Some(id));
        assert_eq!(
            ProjectId::from_hex("00112233445566778899AABBCCDDEEFF"),
            None
        );
        assert_eq!(ProjectId::from_hex("0011"), None);
        assert_eq!(
            ProjectId::from_hex("zz112233445566778899aabbccddeeff"),
            None
        );
    }

    #[test]
    fn seq_orders_and_advances_contiguously() {
        assert_eq!(EventSeq::ZERO.next(), EventSeq::new(1));
        assert!(EventSeq::new(1) < EventSeq::new(2));
        assert_eq!(EventSeq::new(41).next().value(), 42);
    }
}
