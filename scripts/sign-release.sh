#!/usr/bin/env bash
# Signs a release bundle directory (m0-s16 slice).
#
# One trust root: the same SSH key that signs core/cloud tags signs releases,
# verified against the same allowed_signers file. A second key would mean a
# second revocation story, and revocation stories are where signing schemes fail.
#
# Written for bash 3.2 so it runs on a stock macOS as well as on CI. That rules
# out `mapfile` and GNU-only `sort -z`.
#
# Usage: sign-release.sh <bundle-dir> <private-key-path>
# Produces: <bundle-dir>/SHA256SUMS and <bundle-dir>/SHA256SUMS.sig
set -euo pipefail

readonly SIGNING_NAMESPACE="projectos-release"

bundle_dir="${1:-}"
key_path="${2:-}"
if [ -z "$bundle_dir" ] || [ -z "$key_path" ]; then
  echo "usage: sign-release.sh <bundle-dir> <private-key-path>" >&2
  exit 1
fi
if [ ! -d "$bundle_dir" ]; then
  echo "sign-release: ${bundle_dir} is not a directory" >&2
  exit 1
fi
if [ ! -f "$key_path" ]; then
  echo "sign-release: signing key ${key_path} not found" >&2
  exit 1
fi

checksum() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$@"
  else
    shasum --algorithm 256 "$@"
  fi
}

cd "$bundle_dir"
rm -f SHA256SUMS SHA256SUMS.sig

# Sorted and relative: a manifest whose byte order depends on directory
# iteration order cannot be compared between two builds of the same commit.
artifact_list="$(mktemp)"
trap 'rm -f "$artifact_list"' EXIT
find . -type f ! -name 'SHA256SUMS' ! -name 'SHA256SUMS.sig' | LC_ALL=C sort > "$artifact_list"

if [ ! -s "$artifact_list" ]; then
  echo "sign-release: ${bundle_dir} contains no artifacts to sign" >&2
  exit 1
fi
# A newline in an artifact name would split one manifest row into two, so it is
# rejected rather than escaped.
if [ "$(wc -l < "$artifact_list")" != "$(find . -type f ! -name 'SHA256SUMS' ! -name 'SHA256SUMS.sig' | wc -l)" ]; then
  echo "sign-release: an artifact name contains a newline" >&2
  exit 1
fi

artifact_count=0
: > SHA256SUMS
while IFS= read -r artifact; do
  checksum "$artifact" >> SHA256SUMS
  artifact_count=$(( artifact_count + 1 ))
done < "$artifact_list"

ssh-keygen -Y sign -f "$key_path" -n "$SIGNING_NAMESPACE" SHA256SUMS >/dev/null

echo "sign-release: signed ${artifact_count} artifact(s) in ${bundle_dir}"
cat SHA256SUMS
