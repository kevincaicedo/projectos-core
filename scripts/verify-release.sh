#!/usr/bin/env bash
# Verifies a signed release bundle (m0-s16 slice).
#
# Checks both halves, because either alone is worthless: the signature proves
# the manifest is ours, and the checksums prove the artifacts are the ones the
# manifest describes. A valid signature over a stale manifest is not a verified
# release.
#
# Usage: verify-release.sh <bundle-dir> <signer-identity> <allowed-signers-file>
set -euo pipefail

readonly SIGNING_NAMESPACE="projectos-release"

bundle_dir="${1:-}"
signer_identity="${2:-}"
allowed_signers="${3:-}"
if [[ -z "$bundle_dir" || -z "$signer_identity" || -z "$allowed_signers" ]]; then
  echo "usage: verify-release.sh <bundle-dir> <signer-identity> <allowed-signers-file>" >&2
  exit 1
fi
for required in "$bundle_dir/SHA256SUMS" "$bundle_dir/SHA256SUMS.sig" "$allowed_signers"; do
  if [[ ! -f "$required" ]]; then
    echo "verify-release: ${required} is missing" >&2
    exit 1
  fi
done

checksum_check() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum --check --strict "$@"
  else
    shasum --algorithm 256 --check "$@"
  fi
}

cd "$bundle_dir"

ssh-keygen -Y verify \
  -f "$allowed_signers" \
  -I "$signer_identity" \
  -n "$SIGNING_NAMESPACE" \
  -s SHA256SUMS.sig \
  < SHA256SUMS

checksum_check SHA256SUMS

echo "verify-release: signature and every artifact checksum are valid"
