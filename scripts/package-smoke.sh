#!/usr/bin/env bash
# Proves the release-profile bundle boots and its packaged executable can open
# SQLite with FTS5 + sqlite-vec. The project path comes only from this script's
# private temporary directory; no ingested bytes reach the filesystem path.
set -euo pipefail

bundle_dir="${1:-}"
if [[ -z "$bundle_dir" || ! -d "$bundle_dir" ]]; then
  echo "usage: package-smoke.sh <bundle-dir>" >&2
  exit 1
fi

workspace="$(mktemp -d)"
trap 'rm -rf -- "$workspace"' EXIT
project_root="$workspace/packaging-smoke.pos"

case "$(uname -s)" in
  Darwin)
    executable="$bundle_dir/macos/ProjectOS.app/Contents/MacOS/pos-desktop"
    ;;
  Linux)
    executable="target/release/pos-desktop"
    ;;
  *)
    echo "package-smoke: unsupported platform $(uname -s)" >&2
    exit 1
    ;;
esac

if [[ ! -x "$executable" ]]; then
  echo "package-smoke: packaged executable is missing: $executable" >&2
  exit 1
fi

expected="packaging-smoke: native project create and verify are clean"
actual="$("$executable" --packaging-smoke "$project_root")"
if [[ "$actual" != "$expected" ]]; then
  echo "package-smoke: packaged core returned an unexpected report" >&2
  exit 1
fi

# A macOS acceptance run also starts the real Tauri event loop from the .app.
# Linux release CI is headless, so its packaged executable exercises the core
# path above while the native window compile stays covered by `desktop-check`.
if [[ "$(uname -s)" == "Darwin" ]]; then
  log_path="$workspace/native-boot.log"
  "$executable" >"$log_path" 2>&1 &
  app_pid=$!
  sleep 3
  if ! kill -0 "$app_pid" 2>/dev/null; then
    echo "package-smoke: the native bundle exited during boot" >&2
    sed -n '1,120p' "$log_path" >&2
    exit 1
  fi
  kill -TERM "$app_pid"
  wait "$app_pid" 2>/dev/null || true
fi

echo "package-smoke: release bundle boot and packaged extension path are clean"
