//! Derived identities (m1-s01, m1-s02).
//!
//! Every id in the pipeline is derived, never minted. Three consequences,
//! and they are the reason:
//!
//! 1. **Re-fetching is idempotent.** The same `(source, external ref)` always
//!    yields the same evidence id, so a connector replay collides on the
//!    projection's primary key instead of creating a twin.
//! 2. **Re-chunking preserves citations.** A chunk whose span and content are
//!    unchanged derives the same id, so "re-chunk with a better strategy in
//!    2027" is a reprocess rather than a citation apocalypse.
//! 3. **Nothing needs coordination.** Two devices ingesting the same Slack
//!    thread agree on ids without talking, which is what M5 sync will need.
//!
//! ## The chunk-id derivation, and why it carries the span start
//!
//! The milestone writes the derivation as
//! `BLAKE3(evidence_id ‖ normalized_content ‖ span_kind)`. Taken literally,
//! two turns of a transcript that both say "Yeah." derive the *same* id and
//! collide on `proj_chunks`' primary key — and real transcripts are full of
//! them. The obvious repair, an occurrence counter per `(kind, content)`,
//! needs a map over every distinct chunk in the item; on the 8 GB single-file
//! bench that map alone is over a hundred megabytes and blows the 64 MiB
//! per-stage RSS bound the same story sets.
//!
//! The span start is added instead. It is already part of what a chunk *is*
//! (`{id, evidence_id, span, kind}`), it makes uniqueness structural at zero
//! memory cost, and it preserves both properties the milestone actually
//! states: re-chunking with the same strategy churns nothing, and under a
//! changed window size every chunk whose span is unchanged keeps its id.
//! Recorded as a finding in `docs/progress.md`.
//!
//! Content-addressing that is *independent* of position still exists: it is
//! `content_hash`, the untruncated BLAKE3 of the normalized content alone,
//! which is what lets EMBED spend one embedding on content that arrived from
//! four sources (F6).

use pos_domain::ChunkKind;
use pos_foundation::{ChunkId, EvidenceId, SourceId};
use pos_store::blake3;

/// Domain separators. Distinct prefixes keep these id spaces from ever
/// colliding with each other or with `pos-sched`'s job ids.
const SOURCE_ID_DOMAIN: &[u8] = b"projectos/source-id/v1";
const EVIDENCE_ID_DOMAIN: &[u8] = b"projectos/evidence-id/v1";
const CHUNK_ID_DOMAIN: &[u8] = b"projectos/chunk-id/v1";
const CHUNK_CONTENT_DOMAIN: &[u8] = b"projectos/chunk-content/v1";

/// Length-prefixes every variable field so `("ab", "c")` and `("a", "bc")`
/// cannot hash alike — the same discipline `derive_job_id` uses.
fn update_framed(hasher: &mut blake3::Hasher, bytes: &[u8]) {
    hasher.update(&u32::try_from(bytes.len()).unwrap_or(u32::MAX).to_be_bytes());
    hasher.update(bytes);
}

fn truncate_to_id(digest: &blake3::Hash) -> [u8; 16] {
    let mut id = [0_u8; 16];
    id.copy_from_slice(&digest.as_bytes()[..16]);
    id
}

/// The id of a connected source. `scope` is the selection inside the
/// connector — a workspace/channel pair for Slack, the watched directory for
/// a watch folder, the minted address for email forwarding. m1-s06 keeps this
/// derivation and adds the `SourceBinding` record around it.
#[must_use]
pub fn derive_source_id(source_kind: &str, scope: &str) -> SourceId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(SOURCE_ID_DOMAIN);
    update_framed(&mut hasher, source_kind.as_bytes());
    update_framed(&mut hasher, scope.as_bytes());
    SourceId::from_bytes(truncate_to_id(&hasher.finalize()))
}

/// The id of one Evidence item: its source plus its identity *in* that
/// source. Uploads pass the content hash as the external id, which is why
/// re-dropping the same file is a visible no-op rather than a duplicate.
#[must_use]
pub fn derive_evidence_id(source_id: SourceId, external_id: &str) -> EvidenceId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(EVIDENCE_ID_DOMAIN);
    hasher.update(&source_id.into_bytes());
    update_framed(&mut hasher, external_id.as_bytes());
    EvidenceId::from_bytes(truncate_to_id(&hasher.finalize()))
}

/// The id a citation points at, forever. See the module docs for why the
/// span start participates.
#[must_use]
pub fn derive_chunk_id(
    evidence_id: EvidenceId,
    kind: ChunkKind,
    byte_start: u64,
    content_hash: &[u8; 32],
) -> ChunkId {
    let mut hasher = blake3::Hasher::new();
    hasher.update(CHUNK_ID_DOMAIN);
    hasher.update(&evidence_id.into_bytes());
    update_framed(&mut hasher, kind.as_str().as_bytes());
    hasher.update(&byte_start.to_be_bytes());
    hasher.update(content_hash);
    ChunkId::from_bytes(truncate_to_id(&hasher.finalize()))
}

/// Hashes chunk content in one streaming pass, normalizing whitespace as it
/// goes: runs of ASCII whitespace collapse to a single space, and leading and
/// trailing whitespace vanish.
///
/// This is what "normalized content" means in the id derivation, and it is
/// deliberately conservative. Collapsing whitespace makes a chunk survive
/// re-wrapping and indentation changes; anything more aggressive (case
/// folding, punctuation stripping) would make two genuinely different
/// sentences share an id, which is a wrong citation rather than a stable one.
pub struct ContentHasher {
    hasher: blake3::Hasher,
    pending_space: bool,
    started: bool,
    content_byte_count: u64,
}

impl Default for ContentHasher {
    fn default() -> Self {
        Self::new()
    }
}

impl ContentHasher {
    #[must_use]
    pub fn new() -> Self {
        let mut hasher = blake3::Hasher::new();
        hasher.update(CHUNK_CONTENT_DOMAIN);
        Self {
            hasher,
            pending_space: false,
            started: false,
            content_byte_count: 0,
        }
    }

    pub fn update(&mut self, bytes: &[u8]) {
        for byte in bytes {
            if byte.is_ascii_whitespace() {
                self.pending_space = self.started;
                continue;
            }
            if self.pending_space {
                self.hasher.update(b" ");
                self.content_byte_count += 1;
                self.pending_space = false;
            }
            self.hasher.update(std::slice::from_ref(byte));
            self.content_byte_count += 1;
            self.started = true;
        }
    }

    /// Normalized bytes hashed so far — the input to the token estimate, so
    /// the estimate describes what will actually be embedded rather than the
    /// raw span's whitespace.
    #[must_use]
    pub const fn content_byte_count(&self) -> u64 {
        self.content_byte_count
    }

    #[must_use]
    pub const fn is_empty(&self) -> bool {
        !self.started
    }

    #[must_use]
    pub fn finalize(self) -> [u8; 32] {
        *self.hasher.finalize().as_bytes()
    }
}

#[cfg(test)]
mod tests {
    use super::{ContentHasher, derive_chunk_id, derive_evidence_id, derive_source_id};
    use pos_domain::ChunkKind;
    use pos_foundation::EvidenceId;

    fn hash_of(text: &str) -> [u8; 32] {
        let mut hasher = ContentHasher::new();
        hasher.update(text.as_bytes());
        hasher.finalize()
    }

    #[test]
    fn whitespace_normalization_is_what_survives_reformatting() {
        assert_eq!(
            hash_of("the  quick\n\tbrown fox"),
            hash_of("the quick brown fox")
        );
        assert_eq!(hash_of("  padded  "), hash_of("padded"));
        // Conservative on purpose: different words are different content.
        assert_ne!(hash_of("brown fox"), hash_of("brownfox"));
        assert_ne!(hash_of("Brown"), hash_of("brown"));
    }

    #[test]
    fn feeding_the_hasher_in_pieces_matches_feeding_it_whole() {
        let mut piecewise = ContentHasher::new();
        for piece in ["the  quick", "\n\tbrown", " fox  "] {
            piecewise.update(piece.as_bytes());
        }
        assert_eq!(piecewise.finalize(), hash_of("the quick brown fox"));
    }

    #[test]
    fn identical_content_at_different_spans_gets_different_chunk_ids() {
        let evidence = EvidenceId::from_bytes([9; 16]);
        let content = hash_of("Yeah.");
        let first = derive_chunk_id(evidence, ChunkKind::TranscriptTurns, 120, &content);
        let second = derive_chunk_id(evidence, ChunkKind::TranscriptTurns, 4_096, &content);
        assert_ne!(
            first, second,
            "two identical turns must not collide on the chunk primary key"
        );
        // ...and the same span with the same content is the same chunk.
        assert_eq!(
            first,
            derive_chunk_id(evidence, ChunkKind::TranscriptTurns, 120, &content)
        );
    }

    #[test]
    fn the_chunk_kind_participates_in_the_id() {
        let evidence = EvidenceId::from_bytes([3; 16]);
        let content = hash_of("same text");
        assert_ne!(
            derive_chunk_id(evidence, ChunkKind::TranscriptTurns, 0, &content),
            derive_chunk_id(evidence, ChunkKind::DocumentSection, 0, &content)
        );
    }

    #[test]
    fn derived_ids_separate_their_framed_fields() {
        let ab_c = derive_source_id("ab", "c");
        assert_eq!(ab_c, derive_source_id("ab", "c"));
        assert_ne!(ab_c, derive_source_id("a", "bc"));

        let source = derive_source_id("upload", "~/Interviews");
        let other = derive_source_id("upload", "~/Recordings");
        assert_ne!(
            derive_evidence_id(source, "abc"),
            derive_evidence_id(other, "abc")
        );
        assert_ne!(
            derive_evidence_id(source, "ab"),
            derive_evidence_id(source, "abc")
        );
    }
}
