//! BERT WordPiece tokenization — **ours**, deliberately.
//!
//! ## Why this is not a dependency
//!
//! The obvious move is HuggingFace `tokenizers`. Measured against this
//! workspace it adds 29 crates including a C regex engine, and what we need
//! from it is a deterministic table lookup over a 30 522-entry vocabulary.
//! That fails the `DEPENDENCIES.md` question directly — 200 lines of ours do
//! it — and it fails a second one that matters more here.
//!
//! **A chunk's vector is a function of this tokenizer.** If the tokenizer
//! changes, every stored vector silently stops matching newly embedded text,
//! and nothing downstream can detect it: the dimensions still line up, the
//! cosine distances are still plausible, and retrieval just quietly gets
//! worse. A dependency we do not control cannot be allowed to move under a
//! stored index. This file is frozen the same way an event schema is frozen —
//! changing its output is an `enrichment_version` bump and a reprocess, not a
//! patch release.
//!
//! This is the same trade m1-s03 made in the other direction: take
//! `symphonia` for a decade of container edge cases, own the ~100-line
//! resampler beside it. Take ONNX Runtime for the inference kernels, own the
//! tokenizer.
//!
//! ## What "BERT WordPiece" means precisely
//!
//! The pipeline, in order, is the one `bert-base-uncased` and every bge/e5
//! model built on it use:
//!
//! 1. **Clean** — drop control characters, map every Unicode whitespace to a
//!    plain space.
//! 2. **Lowercase and strip accents** (the `uncased` half): NFD-decompose and
//!    drop combining marks.
//! 3. **Split on whitespace, then on punctuation and around CJK codepoints** —
//!    CJK characters are each their own token, which is why they are split
//!    rather than grouped.
//! 4. **Greedy longest-match-first subwording** per word: match the longest
//!    vocabulary entry from the left, then continue with `##` continuation
//!    entries. A word with no match at all becomes `[UNK]` **whole** — not
//!    per-character, which is the detail naive implementations get wrong.
//!
//! Then `[CLS] … [SEP]`, truncated to the caller's sequence cap.
//!
//! ## What we take, and why exactly that
//!
//! Steps 1–4 are ours. **NFD is not**: canonical decomposition is the Unicode
//! standard's own table, it is revised with each Unicode release, and
//! hand-maintaining 15 247 mappings is the kind of transcription error that
//! would corrupt an index silently. `unicode-normalization` (three crates,
//! from the `unicode-rs` maintainers) supplies it. The `P*` and `Mn` category
//! ranges below are *generated* from the same standard rather than typed, for
//! the same reason.
//!
//! Owning the algorithm and taking the character tables is the trade m1-s03
//! made in the other direction: `symphonia` for a decade of container edge
//! cases, our own ~100-line resampler beside it.
//!
//! ## How we know this is right
//!
//! A hand-written pre-tokenizer is easy to get *almost* right, and almost is
//! invisible — the ids still look plausible and retrieval just quietly gets
//! worse. So the first draft was differentially tested against HuggingFace
//! `tokenizers` over the real 30 522-entry bge vocabulary, and it found two
//! real defects that no unit test of ours had asked about:
//!
//! - `？` (U+FF1F, fullwidth question mark) was not punctuation, so
//!   `はどうですか？` stayed one word and became a single `[UNK]` instead of
//!   seven tokens.
//! - `ø` was accent-stripped to `o`. It has no canonical decomposition, so
//!   the reference leaves it alone — a hand-written table said otherwise.
//!
//! `tests/wordpiece_reference.rs` keeps that comparison as a fixture: the
//! reference ids are recorded, and the dependency is not.
//!
//! ## Bounds
//!
//! Every loop here is over a bounded input: [`WORDPIECE_WORD_CHARS_MAX`]
//! caps one word, and the caller caps the sequence. There is no recursion
//! (STYLE forbids it in this position) — the subword walk is an explicit
//! two-index scan.

use std::collections::HashMap;
use unicode_normalization::UnicodeNormalization;

/// Longest single "word" the subword walk will attempt. Past this a token is
/// not language — it is a base64 blob or a minified line — and the quadratic
/// worst case of longest-match-first is not worth paying for it. Such a word
/// becomes `[UNK]`, exactly as BERT's own reference implementation does.
pub const WORDPIECE_WORD_CHARS_MAX: usize = 100;

/// Vocabulary entries a `vocab.txt` may declare. bge-small has 30 522; the
/// cap is the L8 admission bound on a file the user could point anywhere.
pub const WORDPIECE_VOCAB_COUNT_MAX: usize = 1 << 20;

const CONTINUATION_PREFIX: &str = "##";
const TOKEN_UNKNOWN: &str = "[UNK]";
const TOKEN_CLASSIFY: &str = "[CLS]";
const TOKEN_SEPARATE: &str = "[SEP]";
const TOKEN_PAD: &str = "[PAD]";

/// Why a vocabulary could not be loaded. Operating errors, so they are typed
/// rather than asserted — a user can point the model manager at a bad file.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum VocabError {
    /// A required special token is absent, so `[CLS] … [SEP]` framing — which
    /// bge's CLS pooling depends on — could not be built.
    MissingSpecialToken {
        token: &'static str,
    },
    /// The file declared more entries than [`WORDPIECE_VOCAB_COUNT_MAX`].
    TooManyEntries {
        count: usize,
    },
    Empty,
}

impl std::fmt::Display for VocabError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::MissingSpecialToken { token } => {
                write!(formatter, "the vocabulary declares no {token} entry")
            }
            Self::TooManyEntries { count } => write!(
                formatter,
                "{count} vocabulary entries exceed the {WORDPIECE_VOCAB_COUNT_MAX} cap"
            ),
            Self::Empty => formatter.write_str("the vocabulary is empty"),
        }
    }
}

impl std::error::Error for VocabError {}

/// One encoded sequence, in the three parallel arrays every BERT-family ONNX
/// graph takes as input.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub struct Encoding {
    pub input_ids: Vec<i64>,
    pub attention_mask: Vec<i64>,
    pub token_type_ids: Vec<i64>,
    /// `true` when the text did not fit the sequence cap. The seam reports
    /// this rather than dropping it, because a truncated chunk embedded as if
    /// whole is the silent-truncation lie L8 forbids.
    pub truncated: bool,
}

impl Encoding {
    #[must_use]
    pub fn len(&self) -> usize {
        self.input_ids.len()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.input_ids.is_empty()
    }
}

/// A loaded WordPiece vocabulary.
pub struct WordPiece {
    ids: HashMap<String, i64>,
    id_unknown: i64,
    id_classify: i64,
    id_separate: i64,
    id_pad: i64,
}

impl std::fmt::Debug for WordPiece {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("WordPiece")
            .field("entry_count", &self.ids.len())
            .finish_non_exhaustive()
    }
}

impl WordPiece {
    /// Loads a `vocab.txt`: one entry per line, id = line number.
    ///
    /// # Errors
    ///
    /// [`VocabError`] when the file is empty, oversized, or missing one of
    /// the four special tokens the encoder needs.
    pub fn from_vocab_text(text: &str) -> Result<Self, VocabError> {
        let mut ids = HashMap::new();
        for (line_index, line) in text.lines().enumerate() {
            if line_index >= WORDPIECE_VOCAB_COUNT_MAX {
                return Err(VocabError::TooManyEntries {
                    count: line_index + 1,
                });
            }
            // `vocab.txt` entries are exact byte strings; only the line ending
            // is stripped. A trim would merge the several whitespace-ish
            // entries real vocabularies carry.
            let entry = line.strip_suffix('\r').unwrap_or(line);
            // First writer wins: duplicate lines exist in some published
            // vocabularies, and BERT's loader keeps the lower id.
            ids.entry(entry.to_owned())
                .or_insert_with(|| i64::try_from(line_index).unwrap_or(i64::MAX));
        }
        if ids.is_empty() {
            return Err(VocabError::Empty);
        }
        let required = |token: &'static str| {
            ids.get(token)
                .copied()
                .ok_or(VocabError::MissingSpecialToken { token })
        };
        Ok(Self {
            id_unknown: required(TOKEN_UNKNOWN)?,
            id_classify: required(TOKEN_CLASSIFY)?,
            id_separate: required(TOKEN_SEPARATE)?,
            id_pad: required(TOKEN_PAD)?,
            ids,
        })
    }

    #[must_use]
    pub fn entry_count(&self) -> usize {
        self.ids.len()
    }

    #[must_use]
    pub const fn id_pad(&self) -> i64 {
        self.id_pad
    }

    /// Encodes `text` as `[CLS] … [SEP]`, truncated to `sequence_tokens_max`
    /// total ids.
    ///
    /// `sequence_tokens_max` below 2 still produces `[CLS] [SEP]`: a sequence
    /// without its framing would pool the wrong position, and answering a
    /// nonsense cap with a wrong vector is worse than ignoring the cap.
    #[must_use]
    pub fn encode(&self, text: &str, sequence_tokens_max: usize) -> Encoding {
        let content_max = sequence_tokens_max.saturating_sub(2);
        let mut input_ids = Vec::with_capacity(sequence_tokens_max.min(64));
        input_ids.push(self.id_classify);
        let mut truncated = false;
        for word in pre_tokenize(text) {
            if input_ids.len() > content_max {
                truncated = true;
                break;
            }
            for id in self.subword_ids(&word) {
                if input_ids.len() > content_max {
                    truncated = true;
                    break;
                }
                input_ids.push(id);
            }
        }
        input_ids.push(self.id_separate);
        let len = input_ids.len();
        Encoding {
            input_ids,
            attention_mask: vec![1; len],
            token_type_ids: vec![0; len],
            truncated,
        }
    }

    /// Greedy longest-match-first over one pre-tokenized word.
    ///
    /// Iterative by construction (STYLE forbids recursion here): `start`
    /// walks forward, `end` walks back from the word's end looking for the
    /// longest entry. A word that fails anywhere is `[UNK]` *whole*, which is
    /// the behaviour that makes our ids match the reference implementation's.
    fn subword_ids(&self, word: &str) -> Vec<i64> {
        let chars: Vec<char> = word.chars().collect();
        if chars.is_empty() {
            return Vec::new();
        }
        if chars.len() > WORDPIECE_WORD_CHARS_MAX {
            return vec![self.id_unknown];
        }
        let mut ids = Vec::new();
        let mut start = 0usize;
        while start < chars.len() {
            let mut end = chars.len();
            let mut matched = None;
            while start < end {
                let piece: String = chars[start..end].iter().collect();
                let key = if start == 0 {
                    piece
                } else {
                    format!("{CONTINUATION_PREFIX}{piece}")
                };
                if let Some(id) = self.ids.get(&key) {
                    matched = Some((*id, end));
                    break;
                }
                end -= 1;
            }
            let Some((id, next)) = matched else {
                return vec![self.id_unknown];
            };
            ids.push(id);
            start = next;
        }
        ids
    }
}

/// Normalize, then split — in that order, because they are two phases and
/// merging them is a bug the reference comparison caught: `;` (U+037E, GREEK
/// QUESTION MARK) is punctuation whose canonical decomposition is `;`, so a
/// splitter that reads the raw character emits a token the vocabulary has
/// never seen.
fn pre_tokenize(text: &str) -> Vec<String> {
    split_words(&normalize(text))
}

/// Phase one: drop what BERT's cleaner drops, fold whitespace, lowercase,
/// decompose, and drop the combining marks.
fn normalize(text: &str) -> String {
    let mut normalized = String::with_capacity(text.len());
    for raw in text.chars() {
        match raw {
            '\t' | '\n' | '\r' => {
                normalized.push(' ');
                continue;
            }
            _ if is_dropped_category(raw) => continue,
            _ if raw.is_whitespace() => {
                normalized.push(' ');
                continue;
            }
            _ => {}
        }
        // Lowercase before decomposing, because `İ` (U+0130) lowercases to
        // `i` plus a combining dot that only accent stripping then removes.
        for lowered in raw.to_lowercase() {
            for decomposed in lowered.nfd() {
                if !is_nonspacing_mark(decomposed) {
                    normalized.push(decomposed);
                }
            }
        }
    }
    normalized
}

/// Phase two: split on whitespace, on punctuation, and around CJK.
fn split_words(text: &str) -> Vec<String> {
    let mut words = Vec::new();
    let mut current = String::new();
    for character in text.chars() {
        if character == ' ' {
            push_word(&mut words, &mut current);
            continue;
        }
        if is_punctuation(character) || is_cjk(character) {
            push_word(&mut words, &mut current);
            words.push(character.to_string());
            continue;
        }
        current.push(character);
    }
    push_word(&mut words, &mut current);
    words
}

fn push_word(words: &mut Vec<String>, current: &mut String) {
    if !current.is_empty() {
        words.push(std::mem::take(current));
    }
}

/// A codepoint BERT's cleaner removes outright.
fn is_dropped_category(character: char) -> bool {
    character == '\u{fffd}' || in_ranges(character, DROPPED_CATEGORY_RANGES)
}

/// Unicode `Cc`, `Cf`, `Co`, and `Cs` — the control, format, private-use, and
/// surrogate codepoints BERT's cleaner drops before anything else looks at them.
/// `Cn` (unassigned) is deliberately *not* here: it moves with every Unicode
/// release, and a codepoint that becomes assigned must not change how text
/// already in an index tokenizes. An unassigned codepoint reaches the
/// vocabulary and becomes `[UNK]`, which is stable.
/// 139751 codepoints in 26 ranges, generated from Unicode 16.0.0.
const DROPPED_CATEGORY_RANGES: &[(u32, u32)] = &[
    (0x0000, 0x001f),
    (0x007f, 0x009f),
    (0x00ad, 0x00ad),
    (0x0600, 0x0605),
    (0x061c, 0x061c),
    (0x06dd, 0x06dd),
    (0x070f, 0x070f),
    (0x0890, 0x0891),
    (0x08e2, 0x08e2),
    (0x180e, 0x180e),
    (0x200b, 0x200f),
    (0x202a, 0x202e),
    (0x2060, 0x2064),
    (0x2066, 0x206f),
    (0xd800, 0xf8ff),
    (0xfeff, 0xfeff),
    (0xfff9, 0xfffb),
    (0x110bd, 0x110bd),
    (0x110cd, 0x110cd),
    (0x13430, 0x1343f),
    (0x1bca0, 0x1bca3),
    (0x1d173, 0x1d17a),
    (0xe0001, 0xe0001),
    (0xe0020, 0xe007f),
    (0xf0000, 0xffffd),
    (0x100000, 0x10fffd),
];

/// BERT's punctuation rule, from the generated table below.
fn is_punctuation(character: char) -> bool {
    in_ranges(character, PUNCTUATION_RANGES)
}

/// A combining mark NFD separated out, which accent stripping drops.
fn is_nonspacing_mark(character: char) -> bool {
    in_ranges(character, NONSPACING_MARK_RANGES)
}

/// Binary search over a sorted, non-overlapping range table.
fn in_ranges(character: char, ranges: &[(u32, u32)]) -> bool {
    let point = u32::from(character);
    ranges
        .binary_search_by(|(low, high)| {
            if point < *low {
                std::cmp::Ordering::Greater
            } else if point > *high {
                std::cmp::Ordering::Less
            } else {
                std::cmp::Ordering::Equal
            }
        })
        .is_ok()
}

/// The CJK blocks BERT splits per character.
///
/// Hiragana and Katakana are deliberately absent — the reference
/// implementation's `_is_chinese_char` does not include them, so kana stay
/// grouped into words and reach WordPiece as subwordable text.
fn is_cjk(character: char) -> bool {
    matches!(u32::from(character),
        0x4e00..=0x9fff
        | 0x3400..=0x4dbf
        | 0x20000..=0x2a6df
        | 0x2a700..=0x2b73f
        | 0x2b740..=0x2b81f
        | 0x2b820..=0x2ceaf
        | 0xf900..=0xfaff
        | 0x2f800..=0x2fa1f)
}

/// BERT treats the four ASCII symbol blocks as punctuation in addition to every
/// Unicode `P*` category.
/// 864 codepoints in 193 ranges, generated from Unicode 16.0.0.
const PUNCTUATION_RANGES: &[(u32, u32)] = &[
    (0x0021, 0x002f),
    (0x003a, 0x0040),
    (0x005b, 0x0060),
    (0x007b, 0x007e),
    (0x00a1, 0x00a1),
    (0x00a7, 0x00a7),
    (0x00ab, 0x00ab),
    (0x00b6, 0x00b7),
    (0x00bb, 0x00bb),
    (0x00bf, 0x00bf),
    (0x037e, 0x037e),
    (0x0387, 0x0387),
    (0x055a, 0x055f),
    (0x0589, 0x058a),
    (0x05be, 0x05be),
    (0x05c0, 0x05c0),
    (0x05c3, 0x05c3),
    (0x05c6, 0x05c6),
    (0x05f3, 0x05f4),
    (0x0609, 0x060a),
    (0x060c, 0x060d),
    (0x061b, 0x061b),
    (0x061d, 0x061f),
    (0x066a, 0x066d),
    (0x06d4, 0x06d4),
    (0x0700, 0x070d),
    (0x07f7, 0x07f9),
    (0x0830, 0x083e),
    (0x085e, 0x085e),
    (0x0964, 0x0965),
    (0x0970, 0x0970),
    (0x09fd, 0x09fd),
    (0x0a76, 0x0a76),
    (0x0af0, 0x0af0),
    (0x0c77, 0x0c77),
    (0x0c84, 0x0c84),
    (0x0df4, 0x0df4),
    (0x0e4f, 0x0e4f),
    (0x0e5a, 0x0e5b),
    (0x0f04, 0x0f12),
    (0x0f14, 0x0f14),
    (0x0f3a, 0x0f3d),
    (0x0f85, 0x0f85),
    (0x0fd0, 0x0fd4),
    (0x0fd9, 0x0fda),
    (0x104a, 0x104f),
    (0x10fb, 0x10fb),
    (0x1360, 0x1368),
    (0x1400, 0x1400),
    (0x166e, 0x166e),
    (0x169b, 0x169c),
    (0x16eb, 0x16ed),
    (0x1735, 0x1736),
    (0x17d4, 0x17d6),
    (0x17d8, 0x17da),
    (0x1800, 0x180a),
    (0x1944, 0x1945),
    (0x1a1e, 0x1a1f),
    (0x1aa0, 0x1aa6),
    (0x1aa8, 0x1aad),
    (0x1b4e, 0x1b4f),
    (0x1b5a, 0x1b60),
    (0x1b7d, 0x1b7f),
    (0x1bfc, 0x1bff),
    (0x1c3b, 0x1c3f),
    (0x1c7e, 0x1c7f),
    (0x1cc0, 0x1cc7),
    (0x1cd3, 0x1cd3),
    (0x2010, 0x2027),
    (0x2030, 0x2043),
    (0x2045, 0x2051),
    (0x2053, 0x205e),
    (0x207d, 0x207e),
    (0x208d, 0x208e),
    (0x2308, 0x230b),
    (0x2329, 0x232a),
    (0x2768, 0x2775),
    (0x27c5, 0x27c6),
    (0x27e6, 0x27ef),
    (0x2983, 0x2998),
    (0x29d8, 0x29db),
    (0x29fc, 0x29fd),
    (0x2cf9, 0x2cfc),
    (0x2cfe, 0x2cff),
    (0x2d70, 0x2d70),
    (0x2e00, 0x2e2e),
    (0x2e30, 0x2e4f),
    (0x2e52, 0x2e5d),
    (0x3001, 0x3003),
    (0x3008, 0x3011),
    (0x3014, 0x301f),
    (0x3030, 0x3030),
    (0x303d, 0x303d),
    (0x30a0, 0x30a0),
    (0x30fb, 0x30fb),
    (0xa4fe, 0xa4ff),
    (0xa60d, 0xa60f),
    (0xa673, 0xa673),
    (0xa67e, 0xa67e),
    (0xa6f2, 0xa6f7),
    (0xa874, 0xa877),
    (0xa8ce, 0xa8cf),
    (0xa8f8, 0xa8fa),
    (0xa8fc, 0xa8fc),
    (0xa92e, 0xa92f),
    (0xa95f, 0xa95f),
    (0xa9c1, 0xa9cd),
    (0xa9de, 0xa9df),
    (0xaa5c, 0xaa5f),
    (0xaade, 0xaadf),
    (0xaaf0, 0xaaf1),
    (0xabeb, 0xabeb),
    (0xfd3e, 0xfd3f),
    (0xfe10, 0xfe19),
    (0xfe30, 0xfe52),
    (0xfe54, 0xfe61),
    (0xfe63, 0xfe63),
    (0xfe68, 0xfe68),
    (0xfe6a, 0xfe6b),
    (0xff01, 0xff03),
    (0xff05, 0xff0a),
    (0xff0c, 0xff0f),
    (0xff1a, 0xff1b),
    (0xff1f, 0xff20),
    (0xff3b, 0xff3d),
    (0xff3f, 0xff3f),
    (0xff5b, 0xff5b),
    (0xff5d, 0xff5d),
    (0xff5f, 0xff65),
    (0x10100, 0x10102),
    (0x1039f, 0x1039f),
    (0x103d0, 0x103d0),
    (0x1056f, 0x1056f),
    (0x10857, 0x10857),
    (0x1091f, 0x1091f),
    (0x1093f, 0x1093f),
    (0x10a50, 0x10a58),
    (0x10a7f, 0x10a7f),
    (0x10af0, 0x10af6),
    (0x10b39, 0x10b3f),
    (0x10b99, 0x10b9c),
    (0x10d6e, 0x10d6e),
    (0x10ead, 0x10ead),
    (0x10f55, 0x10f59),
    (0x10f86, 0x10f89),
    (0x11047, 0x1104d),
    (0x110bb, 0x110bc),
    (0x110be, 0x110c1),
    (0x11140, 0x11143),
    (0x11174, 0x11175),
    (0x111c5, 0x111c8),
    (0x111cd, 0x111cd),
    (0x111db, 0x111db),
    (0x111dd, 0x111df),
    (0x11238, 0x1123d),
    (0x112a9, 0x112a9),
    (0x113d4, 0x113d5),
    (0x113d7, 0x113d8),
    (0x1144b, 0x1144f),
    (0x1145a, 0x1145b),
    (0x1145d, 0x1145d),
    (0x114c6, 0x114c6),
    (0x115c1, 0x115d7),
    (0x11641, 0x11643),
    (0x11660, 0x1166c),
    (0x116b9, 0x116b9),
    (0x1173c, 0x1173e),
    (0x1183b, 0x1183b),
    (0x11944, 0x11946),
    (0x119e2, 0x119e2),
    (0x11a3f, 0x11a46),
    (0x11a9a, 0x11a9c),
    (0x11a9e, 0x11aa2),
    (0x11b00, 0x11b09),
    (0x11be1, 0x11be1),
    (0x11c41, 0x11c45),
    (0x11c70, 0x11c71),
    (0x11ef7, 0x11ef8),
    (0x11f43, 0x11f4f),
    (0x11fff, 0x11fff),
    (0x12470, 0x12474),
    (0x12ff1, 0x12ff2),
    (0x16a6e, 0x16a6f),
    (0x16af5, 0x16af5),
    (0x16b37, 0x16b3b),
    (0x16b44, 0x16b44),
    (0x16d6d, 0x16d6f),
    (0x16e97, 0x16e9a),
    (0x16fe2, 0x16fe2),
    (0x1bc9f, 0x1bc9f),
    (0x1da87, 0x1da8b),
    (0x1e5ff, 0x1e5ff),
    (0x1e95e, 0x1e95f),
];

/// Unicode `Mn` — the combining marks NFD separates out and accent stripping drops.
/// 2020 codepoints in 357 ranges, generated from Unicode 16.0.0.
const NONSPACING_MARK_RANGES: &[(u32, u32)] = &[
    (0x0300, 0x036f),
    (0x0483, 0x0487),
    (0x0591, 0x05bd),
    (0x05bf, 0x05bf),
    (0x05c1, 0x05c2),
    (0x05c4, 0x05c5),
    (0x05c7, 0x05c7),
    (0x0610, 0x061a),
    (0x064b, 0x065f),
    (0x0670, 0x0670),
    (0x06d6, 0x06dc),
    (0x06df, 0x06e4),
    (0x06e7, 0x06e8),
    (0x06ea, 0x06ed),
    (0x0711, 0x0711),
    (0x0730, 0x074a),
    (0x07a6, 0x07b0),
    (0x07eb, 0x07f3),
    (0x07fd, 0x07fd),
    (0x0816, 0x0819),
    (0x081b, 0x0823),
    (0x0825, 0x0827),
    (0x0829, 0x082d),
    (0x0859, 0x085b),
    (0x0897, 0x089f),
    (0x08ca, 0x08e1),
    (0x08e3, 0x0902),
    (0x093a, 0x093a),
    (0x093c, 0x093c),
    (0x0941, 0x0948),
    (0x094d, 0x094d),
    (0x0951, 0x0957),
    (0x0962, 0x0963),
    (0x0981, 0x0981),
    (0x09bc, 0x09bc),
    (0x09c1, 0x09c4),
    (0x09cd, 0x09cd),
    (0x09e2, 0x09e3),
    (0x09fe, 0x09fe),
    (0x0a01, 0x0a02),
    (0x0a3c, 0x0a3c),
    (0x0a41, 0x0a42),
    (0x0a47, 0x0a48),
    (0x0a4b, 0x0a4d),
    (0x0a51, 0x0a51),
    (0x0a70, 0x0a71),
    (0x0a75, 0x0a75),
    (0x0a81, 0x0a82),
    (0x0abc, 0x0abc),
    (0x0ac1, 0x0ac5),
    (0x0ac7, 0x0ac8),
    (0x0acd, 0x0acd),
    (0x0ae2, 0x0ae3),
    (0x0afa, 0x0aff),
    (0x0b01, 0x0b01),
    (0x0b3c, 0x0b3c),
    (0x0b3f, 0x0b3f),
    (0x0b41, 0x0b44),
    (0x0b4d, 0x0b4d),
    (0x0b55, 0x0b56),
    (0x0b62, 0x0b63),
    (0x0b82, 0x0b82),
    (0x0bc0, 0x0bc0),
    (0x0bcd, 0x0bcd),
    (0x0c00, 0x0c00),
    (0x0c04, 0x0c04),
    (0x0c3c, 0x0c3c),
    (0x0c3e, 0x0c40),
    (0x0c46, 0x0c48),
    (0x0c4a, 0x0c4d),
    (0x0c55, 0x0c56),
    (0x0c62, 0x0c63),
    (0x0c81, 0x0c81),
    (0x0cbc, 0x0cbc),
    (0x0cbf, 0x0cbf),
    (0x0cc6, 0x0cc6),
    (0x0ccc, 0x0ccd),
    (0x0ce2, 0x0ce3),
    (0x0d00, 0x0d01),
    (0x0d3b, 0x0d3c),
    (0x0d41, 0x0d44),
    (0x0d4d, 0x0d4d),
    (0x0d62, 0x0d63),
    (0x0d81, 0x0d81),
    (0x0dca, 0x0dca),
    (0x0dd2, 0x0dd4),
    (0x0dd6, 0x0dd6),
    (0x0e31, 0x0e31),
    (0x0e34, 0x0e3a),
    (0x0e47, 0x0e4e),
    (0x0eb1, 0x0eb1),
    (0x0eb4, 0x0ebc),
    (0x0ec8, 0x0ece),
    (0x0f18, 0x0f19),
    (0x0f35, 0x0f35),
    (0x0f37, 0x0f37),
    (0x0f39, 0x0f39),
    (0x0f71, 0x0f7e),
    (0x0f80, 0x0f84),
    (0x0f86, 0x0f87),
    (0x0f8d, 0x0f97),
    (0x0f99, 0x0fbc),
    (0x0fc6, 0x0fc6),
    (0x102d, 0x1030),
    (0x1032, 0x1037),
    (0x1039, 0x103a),
    (0x103d, 0x103e),
    (0x1058, 0x1059),
    (0x105e, 0x1060),
    (0x1071, 0x1074),
    (0x1082, 0x1082),
    (0x1085, 0x1086),
    (0x108d, 0x108d),
    (0x109d, 0x109d),
    (0x135d, 0x135f),
    (0x1712, 0x1714),
    (0x1732, 0x1733),
    (0x1752, 0x1753),
    (0x1772, 0x1773),
    (0x17b4, 0x17b5),
    (0x17b7, 0x17bd),
    (0x17c6, 0x17c6),
    (0x17c9, 0x17d3),
    (0x17dd, 0x17dd),
    (0x180b, 0x180d),
    (0x180f, 0x180f),
    (0x1885, 0x1886),
    (0x18a9, 0x18a9),
    (0x1920, 0x1922),
    (0x1927, 0x1928),
    (0x1932, 0x1932),
    (0x1939, 0x193b),
    (0x1a17, 0x1a18),
    (0x1a1b, 0x1a1b),
    (0x1a56, 0x1a56),
    (0x1a58, 0x1a5e),
    (0x1a60, 0x1a60),
    (0x1a62, 0x1a62),
    (0x1a65, 0x1a6c),
    (0x1a73, 0x1a7c),
    (0x1a7f, 0x1a7f),
    (0x1ab0, 0x1abd),
    (0x1abf, 0x1ace),
    (0x1b00, 0x1b03),
    (0x1b34, 0x1b34),
    (0x1b36, 0x1b3a),
    (0x1b3c, 0x1b3c),
    (0x1b42, 0x1b42),
    (0x1b6b, 0x1b73),
    (0x1b80, 0x1b81),
    (0x1ba2, 0x1ba5),
    (0x1ba8, 0x1ba9),
    (0x1bab, 0x1bad),
    (0x1be6, 0x1be6),
    (0x1be8, 0x1be9),
    (0x1bed, 0x1bed),
    (0x1bef, 0x1bf1),
    (0x1c2c, 0x1c33),
    (0x1c36, 0x1c37),
    (0x1cd0, 0x1cd2),
    (0x1cd4, 0x1ce0),
    (0x1ce2, 0x1ce8),
    (0x1ced, 0x1ced),
    (0x1cf4, 0x1cf4),
    (0x1cf8, 0x1cf9),
    (0x1dc0, 0x1dff),
    (0x20d0, 0x20dc),
    (0x20e1, 0x20e1),
    (0x20e5, 0x20f0),
    (0x2cef, 0x2cf1),
    (0x2d7f, 0x2d7f),
    (0x2de0, 0x2dff),
    (0x302a, 0x302d),
    (0x3099, 0x309a),
    (0xa66f, 0xa66f),
    (0xa674, 0xa67d),
    (0xa69e, 0xa69f),
    (0xa6f0, 0xa6f1),
    (0xa802, 0xa802),
    (0xa806, 0xa806),
    (0xa80b, 0xa80b),
    (0xa825, 0xa826),
    (0xa82c, 0xa82c),
    (0xa8c4, 0xa8c5),
    (0xa8e0, 0xa8f1),
    (0xa8ff, 0xa8ff),
    (0xa926, 0xa92d),
    (0xa947, 0xa951),
    (0xa980, 0xa982),
    (0xa9b3, 0xa9b3),
    (0xa9b6, 0xa9b9),
    (0xa9bc, 0xa9bd),
    (0xa9e5, 0xa9e5),
    (0xaa29, 0xaa2e),
    (0xaa31, 0xaa32),
    (0xaa35, 0xaa36),
    (0xaa43, 0xaa43),
    (0xaa4c, 0xaa4c),
    (0xaa7c, 0xaa7c),
    (0xaab0, 0xaab0),
    (0xaab2, 0xaab4),
    (0xaab7, 0xaab8),
    (0xaabe, 0xaabf),
    (0xaac1, 0xaac1),
    (0xaaec, 0xaaed),
    (0xaaf6, 0xaaf6),
    (0xabe5, 0xabe5),
    (0xabe8, 0xabe8),
    (0xabed, 0xabed),
    (0xfb1e, 0xfb1e),
    (0xfe00, 0xfe0f),
    (0xfe20, 0xfe2f),
    (0x101fd, 0x101fd),
    (0x102e0, 0x102e0),
    (0x10376, 0x1037a),
    (0x10a01, 0x10a03),
    (0x10a05, 0x10a06),
    (0x10a0c, 0x10a0f),
    (0x10a38, 0x10a3a),
    (0x10a3f, 0x10a3f),
    (0x10ae5, 0x10ae6),
    (0x10d24, 0x10d27),
    (0x10d69, 0x10d6d),
    (0x10eab, 0x10eac),
    (0x10efc, 0x10eff),
    (0x10f46, 0x10f50),
    (0x10f82, 0x10f85),
    (0x11001, 0x11001),
    (0x11038, 0x11046),
    (0x11070, 0x11070),
    (0x11073, 0x11074),
    (0x1107f, 0x11081),
    (0x110b3, 0x110b6),
    (0x110b9, 0x110ba),
    (0x110c2, 0x110c2),
    (0x11100, 0x11102),
    (0x11127, 0x1112b),
    (0x1112d, 0x11134),
    (0x11173, 0x11173),
    (0x11180, 0x11181),
    (0x111b6, 0x111be),
    (0x111c9, 0x111cc),
    (0x111cf, 0x111cf),
    (0x1122f, 0x11231),
    (0x11234, 0x11234),
    (0x11236, 0x11237),
    (0x1123e, 0x1123e),
    (0x11241, 0x11241),
    (0x112df, 0x112df),
    (0x112e3, 0x112ea),
    (0x11300, 0x11301),
    (0x1133b, 0x1133c),
    (0x11340, 0x11340),
    (0x11366, 0x1136c),
    (0x11370, 0x11374),
    (0x113bb, 0x113c0),
    (0x113ce, 0x113ce),
    (0x113d0, 0x113d0),
    (0x113d2, 0x113d2),
    (0x113e1, 0x113e2),
    (0x11438, 0x1143f),
    (0x11442, 0x11444),
    (0x11446, 0x11446),
    (0x1145e, 0x1145e),
    (0x114b3, 0x114b8),
    (0x114ba, 0x114ba),
    (0x114bf, 0x114c0),
    (0x114c2, 0x114c3),
    (0x115b2, 0x115b5),
    (0x115bc, 0x115bd),
    (0x115bf, 0x115c0),
    (0x115dc, 0x115dd),
    (0x11633, 0x1163a),
    (0x1163d, 0x1163d),
    (0x1163f, 0x11640),
    (0x116ab, 0x116ab),
    (0x116ad, 0x116ad),
    (0x116b0, 0x116b5),
    (0x116b7, 0x116b7),
    (0x1171d, 0x1171d),
    (0x1171f, 0x1171f),
    (0x11722, 0x11725),
    (0x11727, 0x1172b),
    (0x1182f, 0x11837),
    (0x11839, 0x1183a),
    (0x1193b, 0x1193c),
    (0x1193e, 0x1193e),
    (0x11943, 0x11943),
    (0x119d4, 0x119d7),
    (0x119da, 0x119db),
    (0x119e0, 0x119e0),
    (0x11a01, 0x11a0a),
    (0x11a33, 0x11a38),
    (0x11a3b, 0x11a3e),
    (0x11a47, 0x11a47),
    (0x11a51, 0x11a56),
    (0x11a59, 0x11a5b),
    (0x11a8a, 0x11a96),
    (0x11a98, 0x11a99),
    (0x11c30, 0x11c36),
    (0x11c38, 0x11c3d),
    (0x11c3f, 0x11c3f),
    (0x11c92, 0x11ca7),
    (0x11caa, 0x11cb0),
    (0x11cb2, 0x11cb3),
    (0x11cb5, 0x11cb6),
    (0x11d31, 0x11d36),
    (0x11d3a, 0x11d3a),
    (0x11d3c, 0x11d3d),
    (0x11d3f, 0x11d45),
    (0x11d47, 0x11d47),
    (0x11d90, 0x11d91),
    (0x11d95, 0x11d95),
    (0x11d97, 0x11d97),
    (0x11ef3, 0x11ef4),
    (0x11f00, 0x11f01),
    (0x11f36, 0x11f3a),
    (0x11f40, 0x11f40),
    (0x11f42, 0x11f42),
    (0x11f5a, 0x11f5a),
    (0x13440, 0x13440),
    (0x13447, 0x13455),
    (0x1611e, 0x16129),
    (0x1612d, 0x1612f),
    (0x16af0, 0x16af4),
    (0x16b30, 0x16b36),
    (0x16f4f, 0x16f4f),
    (0x16f8f, 0x16f92),
    (0x16fe4, 0x16fe4),
    (0x1bc9d, 0x1bc9e),
    (0x1cf00, 0x1cf2d),
    (0x1cf30, 0x1cf46),
    (0x1d167, 0x1d169),
    (0x1d17b, 0x1d182),
    (0x1d185, 0x1d18b),
    (0x1d1aa, 0x1d1ad),
    (0x1d242, 0x1d244),
    (0x1da00, 0x1da36),
    (0x1da3b, 0x1da6c),
    (0x1da75, 0x1da75),
    (0x1da84, 0x1da84),
    (0x1da9b, 0x1da9f),
    (0x1daa1, 0x1daaf),
    (0x1e000, 0x1e006),
    (0x1e008, 0x1e018),
    (0x1e01b, 0x1e021),
    (0x1e023, 0x1e024),
    (0x1e026, 0x1e02a),
    (0x1e08f, 0x1e08f),
    (0x1e130, 0x1e136),
    (0x1e2ae, 0x1e2ae),
    (0x1e2ec, 0x1e2ef),
    (0x1e4ec, 0x1e4ef),
    (0x1e5ee, 0x1e5ef),
    (0x1e8d0, 0x1e8d6),
    (0x1e944, 0x1e94a),
    (0xe0100, 0xe01ef),
];

#[cfg(test)]
mod tests {
    use super::{Encoding, VocabError, WORDPIECE_WORD_CHARS_MAX, WordPiece, pre_tokenize};

    /// A miniature vocabulary in `vocab.txt` order, so ids are line numbers
    /// exactly as a real file's are.
    fn vocab() -> WordPiece {
        let entries = [
            "[PAD]", "[UNK]", "[CLS]", "[SEP]", "the", "project", "##os", "log", "is", "un",
            "##aff", "##able", "e", "##m", "##b", "##ed", "北", "京", "a", "b", "c",
        ];
        WordPiece::from_vocab_text(&entries.join("\n")).expect("the fixture vocabulary loads")
    }

    #[test]
    fn a_vocabulary_without_its_framing_tokens_refuses() {
        assert_eq!(
            WordPiece::from_vocab_text("hello\nworld").err(),
            Some(VocabError::MissingSpecialToken { token: "[UNK]" })
        );
        assert_eq!(
            WordPiece::from_vocab_text("").err(),
            Some(VocabError::Empty)
        );
    }

    #[test]
    fn longest_match_first_produces_continuation_pieces() {
        let vocab = vocab();
        let encoded = vocab.encode("ProjectOS", 512);
        // [CLS] project ##os [SEP]
        assert_eq!(encoded.input_ids, vec![2, 5, 6, 3]);
        assert!(!encoded.truncated);
    }

    /// The detail naive implementations get wrong: a word that fails partway
    /// is `[UNK]` whole, never a mix of pieces and unknowns.
    #[test]
    fn a_word_that_fails_partway_is_one_unknown_token() {
        let vocab = vocab();
        // "unaffable" splits fully; "unaffordable" cannot, so the whole word
        // becomes one [UNK] rather than "un" + [UNK].
        assert_eq!(
            vocab.encode("unaffable", 512).input_ids,
            vec![2, 9, 10, 11, 3]
        );
        assert_eq!(vocab.encode("unaffordable", 512).input_ids, vec![2, 1, 3]);
    }

    #[test]
    fn casing_accents_and_punctuation_normalize_before_lookup() {
        assert_eq!(pre_tokenize("Héllo, wörld!"), ["hello", ",", "world", "!"]);
        assert_eq!(pre_tokenize("a\u{0}b\tc"), ["ab", "c"]);
    }

    #[test]
    fn cjk_characters_are_one_token_each() {
        let vocab = vocab();
        assert_eq!(pre_tokenize("北京"), ["北", "京"]);
        assert_eq!(vocab.encode("北京", 512).input_ids, vec![2, 16, 17, 3]);
    }

    #[test]
    fn truncation_is_reported_and_keeps_the_framing() {
        let vocab = vocab();
        let encoded = vocab.encode("a b c a b c a b c", 6);
        assert_eq!(encoded.len(), 6);
        assert_eq!(encoded.input_ids.first(), Some(&2));
        assert_eq!(encoded.input_ids.last(), Some(&3));
        assert!(encoded.truncated, "a cut sequence says so");
        let whole = vocab.encode("a b c", 512);
        assert!(!whole.truncated);
        assert_eq!(whole.attention_mask, vec![1; whole.len()]);
        assert_eq!(whole.token_type_ids, vec![0; whole.len()]);
    }

    /// A cap below the framing still frames: pooling position 0 of a sequence
    /// with no `[CLS]` would silently return the wrong vector.
    #[test]
    fn a_nonsense_cap_still_produces_a_framed_sequence() {
        let vocab = vocab();
        assert_eq!(vocab.encode("the log", 0).input_ids, vec![2, 3]);
        assert_eq!(vocab.encode("the log", 1).input_ids, vec![2, 3]);
    }

    /// Not language: a base64 blob must cost one lookup, not a quadratic walk.
    #[test]
    fn an_overlong_word_is_unknown_rather_than_a_quadratic_walk() {
        let vocab = vocab();
        let blob = "a".repeat(WORDPIECE_WORD_CHARS_MAX + 1);
        assert_eq!(vocab.encode(&blob, 512).input_ids, vec![2, 1, 3]);
    }

    #[test]
    fn an_empty_encoding_is_empty() {
        assert!(Encoding::default().is_empty());
        assert_eq!(vocab().encode("", 512).input_ids, vec![2, 3]);
    }
}
