// Palette matching (m0-s09): simple case-insensitive subsequence, per the
// pre-agreed cut line — ranking quality matters past ~50 commands (M2); at
// ~15 the match set is small enough to scan.

export function subsequenceMatches(query: string, candidate: string): boolean {
  const needle = query.toLowerCase();
  const haystack = candidate.toLowerCase();
  let position = 0;
  for (const character of needle) {
    if (character === " ") {
      continue;
    }
    position = haystack.indexOf(character, position);
    if (position === -1) {
      return false;
    }
    position += 1;
  }
  return true;
}
