#!/usr/bin/env bash
# Emits the reference-machine registry fingerprint for the current host
# (docs/reference-machines.md §3). Deliberately excludes serial numbers,
# hardware UUIDs, user names, device names, and absolute home paths: an
# artifact that identifies a person instead of a machine is a leak, not
# evidence.
#
# Usage: just fingerprint-machine RM-LAPTOP-01
set -euo pipefail

machine_id="${1:-}"
if [[ -z "$machine_id" ]]; then
  echo "usage: fingerprint-machine.sh <machine-id>" >&2
  exit 1
fi
if [[ ! "$machine_id" =~ ^RM-[A-Z]+-[0-9]{2}$ ]]; then
  echo "machine id must look like RM-LAPTOP-01" >&2
  exit 1
fi

version_of() {
  if command -v "$1" >/dev/null 2>&1; then
    "$@"
  else
    echo "not-installed"
  fi
}

case "$(uname -s)" in
  Darwin)
    model="$(sysctl -n hw.model)"
    chip="$(sysctl -n machdep.cpu.brand_string)"
    cpu_total="$(sysctl -n hw.ncpu)"
    cpu_performance="$(sysctl -n hw.perflevel0.logicalcpu 2>/dev/null || echo not-applicable)"
    cpu_efficiency="$(sysctl -n hw.perflevel1.logicalcpu 2>/dev/null || echo not-applicable)"
    memory_bytes="$(sysctl -n hw.memsize)"
    os_name="macOS"
    os_version="$(sw_vers -productVersion) build $(sw_vers -buildVersion)"
    kernel="$(uname -r)"
    filesystem="$(diskutil info / 2>/dev/null | awk -F': *' '
      /File System Personality/ {
        value=$2
        sub(/^ +/, "", value)
        print value
        exit
      }
    ')"
    storage="$(system_profiler SPStorageDataType 2>/dev/null | awk -F': ' '
      /Device Name/ && device == "" {device=$2}
      /Medium Type/ && medium == "" {medium=$2}
      END {
        if (device != "" && medium != "") print device " (" medium ")"
        else if (device != "") print device
      }
    ')"
    ;;
  Linux)
    model="$(cat /sys/devices/virtual/dmi/id/product_name 2>/dev/null || echo unknown)"
    chip="$(awk -F': ' '/model name/ {print $2; exit}' /proc/cpuinfo)"
    cpu_total="$(nproc --all)"
    cpu_performance="see-lscpu-core-types"
    cpu_efficiency="see-lscpu-core-types"
    memory_bytes="$(( $(awk '/MemTotal/ {print $2}' /proc/meminfo) * 1024 ))"
    os_name="$(awk -F'=' '/^NAME=/ {gsub(/"/,"",$2); print $2}' /etc/os-release)"
    os_version="$(awk -F'=' '/^VERSION=/ {gsub(/"/,"",$2); print $2}' /etc/os-release)"
    kernel="$(uname -r)"
    filesystem="$(findmnt --noheadings --output FSTYPE --target / 2>/dev/null || echo unknown)"
    storage="$(lsblk --noheadings --output MODEL,ROTA 2>/dev/null | awk 'NF {print; exit}')"
    ;;
  *)
    echo "unsupported platform: $(uname -s)" >&2
    exit 1
    ;;
esac

memory_gib=$(( memory_bytes / 1024 / 1024 / 1024 ))
core_root="$(git rev-parse --show-toplevel)"
registry_root="$(git -C "$core_root/.." rev-parse --show-toplevel)"
if [[ ! -f "$registry_root/docs/reference-machines.md" ]]; then
  echo "reference-machine registry is missing beside the core checkout" >&2
  exit 1
fi
registry_revision="$(git -C "$registry_root" rev-parse HEAD)"
rust_version="$(version_of rustc --version)"
cargo_version="$(version_of cargo --version)"
node_version="$(version_of node --version)"
pnpm_version="$(version_of pnpm --version)"
just_version="$(version_of just --version)"

cat <<FINGERPRINT
schema_version: 1
machine_id: ${machine_id}
machine_registry_revision: ${registry_revision}
recorded_at_utc: $(date -u +%Y-%m-%dT%H:%M:%SZ)
hardware:
  model_identifier: ${model}
  cpu: ${chip}
  cpu_logical_cores: ${cpu_total}
  cpu_performance_cores: ${cpu_performance}
  cpu_efficiency_cores: ${cpu_efficiency}
  memory_gib: ${memory_gib}
  storage: ${storage:-unknown}
  filesystem: ${filesystem:-unknown}
os:
  name: ${os_name}
  version: ${os_version}
  kernel: ${kernel}
toolchain:
  rust: ${rust_version}
  cargo: ${cargo_version}
  node: ${node_version}
  pnpm: ${pnpm_version}
  just: ${just_version}
FINGERPRINT
