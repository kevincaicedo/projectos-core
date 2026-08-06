#!/usr/bin/env bash
# Seeded fixture proving the release-signing gate actually fires (m0-s16 slice).
#
# The rule this repository keeps: a gate without a failing fixture is
# decoration. This runs in `just ci` with an ephemeral throwaway key, needs no
# secret, and proves four things in a couple of seconds:
#
#   1. a correctly signed bundle verifies;
#   2. a tampered artifact fails;
#   3. a tampered manifest fails the signature;
#   4. a signature from a key outside allowed_signers fails.
set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
workspace="$(mktemp -d)"
trap 'rm -rf "$workspace"' EXIT

readonly IDENTITY="release-fixture@projectos.invalid"
bundle="$workspace/bundle"
mkdir -p "$bundle"
printf 'pretend installer bytes\n' > "$bundle/ProjectOS.dmg"
printf 'pretend archive bytes\n' > "$bundle/ProjectOS.tar.gz"

ssh-keygen -t ed25519 -N '' -C "$IDENTITY" -f "$workspace/release" >/dev/null
ssh-keygen -t ed25519 -N '' -C "attacker" -f "$workspace/attacker" >/dev/null
printf '%s %s\n' "$IDENTITY" "$(cat "$workspace/release.pub")" > "$workspace/allowed_signers"

expect_failure() {
  local description="$1"
  shift
  if "$@" >/dev/null 2>&1; then
    echo "release-signing fixture: ${description} was accepted; the gate does not fire" >&2
    exit 1
  fi
}

# 1. The honest path.
bash "$script_dir/sign-release.sh" "$bundle" "$workspace/release" >/dev/null
bash "$script_dir/verify-release.sh" "$bundle" "$IDENTITY" "$workspace/allowed_signers" >/dev/null

# 2. A tampered artifact must fail the checksum half.
printf 'malicious payload\n' > "$bundle/ProjectOS.dmg"
expect_failure "a tampered artifact" \
  bash "$script_dir/verify-release.sh" "$bundle" "$IDENTITY" "$workspace/allowed_signers"

# 3. Re-listing the tampered artifact in the manifest must fail the signature.
printf 'pretend installer bytes\n' > "$bundle/ProjectOS.dmg"
bash "$script_dir/sign-release.sh" "$bundle" "$workspace/release" >/dev/null
printf 'malicious payload\n' > "$bundle/ProjectOS.dmg"
if command -v sha256sum >/dev/null 2>&1; then
  (cd "$bundle" && sha256sum ./ProjectOS.dmg ./ProjectOS.tar.gz > SHA256SUMS)
else
  (cd "$bundle" && shasum --algorithm 256 ./ProjectOS.dmg ./ProjectOS.tar.gz > SHA256SUMS)
fi
expect_failure "a re-listed tampered manifest" \
  bash "$script_dir/verify-release.sh" "$bundle" "$IDENTITY" "$workspace/allowed_signers"

# 4. A signature from an untrusted key must fail even when everything matches.
printf 'pretend installer bytes\n' > "$bundle/ProjectOS.dmg"
bash "$script_dir/sign-release.sh" "$bundle" "$workspace/attacker" >/dev/null
expect_failure "a signature from a key outside allowed_signers" \
  bash "$script_dir/verify-release.sh" "$bundle" "$IDENTITY" "$workspace/allowed_signers"

echo "release-signing fixture: signed bundle verifies; tampered artifact, tampered manifest, and untrusted key all fail"
