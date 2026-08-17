# ProjectOS core — task runner (m0-s01). `just ci` is the merge bar.

default:
    @just --list

# Everything CI runs. Green here before presenting any change as done (AGENTS.md).
ci: node-version-check fmt-check clippy test deny dep-dag discipline core-boundaries capability-catalog-check api-types-check snapshot-fixture-check release-signing-check public-build-bootstrap ui-check ui-build e2e desktop-check
    @echo "ci: all green"

node-version-check:
    node -e 'const fs = require("node:fs"); const expected = `v${fs.readFileSync(".node-version", "utf8").trim()}`; if (process.version !== expected) { console.error(`Node ${expected} is required; found ${process.version}`); process.exit(1); }'

fmt: node-version-check
    cargo fmt --all
    pnpm --dir apps/ui format

fmt-check:
    cargo fmt --all --check

clippy:
    cargo clippy --workspace --all-targets -- -D warnings

test:
    cargo test --workspace

deny:
    cargo deny check

# Crate-boundary checker (master plan §19; .agents/skills/crate-boundaries).
dep-dag:
    cargo run --quiet -p check-dep-dag --bin check-dep-dag

# L1/panic/dependency enforcement (m0-s02), with seeded violation unit fixtures.
discipline:
    cargo run --quiet -p check-dep-dag --bin check-discipline

core-boundaries:
    cargo run --quiet -p check-dep-dag --bin check-boundaries -- core --root .

# Requires the founder-created core/cloud gitlinks, signed tags, and imported
# release public key; the superproject workflow runs it after checkout.
umbrella-boundaries:
    cargo run --quiet -p check-dep-dag --bin check-boundaries -- umbrella --root ..

docs-mirror-check:
    cargo run --quiet -p check-dep-dag --bin check-boundaries -- docs --root ..

# The UI capability vocabulary is generated from the public Rust registry.
capability-catalog-check:
    cargo run --quiet -p pos-api --bin export-capabilities -- --check apps/ui/src/api/gen/capabilities.ts

# public-builds-alone: no cloud checkout, every target builds, the walking
# skeleton runs (`e2e`), and release signing is proven on a seeded fixture
# (`release-signing-check`). All four are wired into `ci` above.
public-build-bootstrap:
    test ! -e cloud
    cargo build --workspace --all-targets

generate-capabilities:
    cargo run --quiet -p pos-api --bin export-capabilities -- --write apps/ui/src/api/gen/capabilities.ts

# The UI API types are ts-rs-generated from the pos-api wire structs (m0-s06).
# `--check` regenerates into a scratch tree and diffs, so CI fails on drift
# without touching the checkout.
generate-api-types:
    cargo run --quiet -p pos-api --bin export-api-types -- --write apps/ui/src/api/gen/api

api-types-check:
    cargo run --quiet -p pos-api --bin export-api-types -- --check apps/ui/src/api/gen/api

# The e2e fixture is the real runtime's own stdout, so UI/runtime wire drift
# fails here instead of in front of a user.
generate-snapshot-fixture:
    cargo run --quiet -p pos -- capability-snapshot > apps/ui/e2e/fixtures/capability-snapshot.json

snapshot-fixture-check:
    cargo run --quiet -p pos -- capability-snapshot | diff --unified apps/ui/e2e/fixtures/capability-snapshot.json - \
      || { echo "snapshot fixture is stale; run \`just generate-snapshot-fixture\`" >&2; exit 1; }

# Proves the release-signing gate fires on a tampered artifact, a tampered
# manifest, and an untrusted key. Ephemeral key; no secret required.
release-signing-check:
    @bash scripts/release-signing-fixture.sh

# m0-s10 release-qualification lane (NOT in `ci`): live Ollama profile
# qualification against a running local daemon. LM Studio/vLLM run the same
# lane with their env vars; cloud smokes join when the TLS transport lands
# (visible debt: m1-s03).
qualify-gateway-local base="http://127.0.0.1:11434" model="qwen3:0.6b":
    POS_QUALIFY_OLLAMA_BASE={{base}} POS_QUALIFY_OLLAMA_MODEL={{model}} \
      cargo test -p pos-gateway --test live_qualification -- --ignored qualify_live_ollama --nocapture

# m0-s13 release-qualification lane (NOT in `ci`): the complete Echo path,
# including Run frames, validation, and its durable cost row, against the
# pinned local Ollama model used by EchoRuntimeOptions::default().
qualify-echo-local:
    cargo test -p pos-api --test echo_runtime identical_echo_path_passes_live_ollama_under_local_only -- --ignored --nocapture

qualify-gateway-lm-studio model base="http://127.0.0.1:1234":
    POS_QUALIFY_LMSTUDIO_BASE={{base}} POS_QUALIFY_LMSTUDIO_MODEL={{model}} \
      cargo test -p pos-gateway --test live_qualification -- --ignored qualify_live_lm_studio --nocapture

# m1-s03 cloud smoke (NOT in `ci`): one live completion over the reviewed TLS
# transport. Secret-gated — the key comes from the environment and is never a
# literal in the repository. With the repository-root `.env` holding
# OPEN_ROUTER_KEY:
#   POS_QUALIFY_CLOUD_KEY=$(grep OPEN_ROUTER_KEY ../.env | cut -d= -f2-) just qualify-gateway-cloud
qualify-gateway-cloud model="openai/gpt-4o-mini" base="https://openrouter.ai/api":
    POS_QUALIFY_CLOUD_BASE={{base}} POS_QUALIFY_CLOUD_MODEL={{model}} \
      cargo test -p pos-gateway --test live_qualification -- --ignored qualify_live_cloud --nocapture

qualify-gateway-vllm model base="http://127.0.0.1:8000":
    POS_QUALIFY_VLLM_BASE={{base}} POS_QUALIFY_VLLM_MODEL={{model}} \
      cargo test -p pos-gateway --test live_qualification -- --ignored qualify_live_vllm --nocapture

# m1-s03 qualification lane (NOT in `ci`): real whisper on a real recording,
# through the real pipeline. Needs `pos models pull whisper-small` and a
# recording; prints the §18 realtime row, which lands in docs/progress.md.
#   just qualify-transcribe-local ../tmp/interview.m4a 3
qualify-transcribe-local audio replicates="1" model="whisper-small" models_dir="models/pulled":
    POS_QUALIFY_MODELS_DIR={{models_dir}} POS_QUALIFY_WHISPER_MODEL={{model}} \
      POS_QUALIFY_AUDIO={{audio}} POS_QUALIFY_REPLICATES={{replicates}} \
      cargo test --release -p pos-ingest --test transcribe_qualification -- --ignored --nocapture

# Regenerates prompts/prompts.lock after adding a prompt version (m0-s11).
generate-prompt-lock:
    cargo run --quiet -p pos-gateway --bin generate-prompt-lock -- prompts

# Builds real desktop bundles and exercises the packaged executable. This is
# the public-builds-alone packaging row and needs no release secret.
package-unsigned:
    pnpm exec tauri build --config apps/desktop/tauri.conf.json --ci
    @bash scripts/package-smoke.sh target/release/bundle

# Signs an already-smoked bundle with the release trust root. Founder-run or
# release-workflow-run; the public PR lane uses `package-unsigned` above.
package key_path: package-unsigned
    @bash scripts/sign-release.sh target/release/bundle {{key_path}}

verify-package identity allowed_signers:
    @bash scripts/verify-release.sh target/release/bundle {{identity}} {{allowed_signers}}

# UI checks: typecheck + lint + format + the m0-s09 design/parity audits
# (each ships its own seeded violation fixture). Requires `pnpm install` once.
ui-check: node-version-check
    pnpm --dir apps/ui exec tsc --noEmit
    pnpm --dir apps/ui lint
    pnpm --dir apps/ui boundary:fixture
    pnpm --dir apps/ui design:audit
    pnpm --dir apps/ui parity:grep
    pnpm --dir apps/ui format:check

ui-build: node-version-check
    pnpm --dir apps/ui build

# Walking-skeleton e2e over the production bundle, with no server, no account,
# and no cloud checkout. Needs `just e2e-install` once per machine.
e2e: node-version-check
    pnpm --dir apps/ui exec playwright test

e2e-install: node-version-check
    pnpm --dir apps/ui exec playwright install --with-deps chromium

# Fast local native-shell compile through the actual Tauri CLI/config path;
# `package-unsigned` owns the release bundle and packaged-executable smoke.
desktop-check: node-version-check
    pnpm exec tauri build --config apps/desktop/tauri.conf.json --debug --no-bundle --ci

ui-install: node-version-check
    corepack enable pnpm
    pnpm install

# Shells (grow per story: web = m0-s08, native chrome = m0-s07).
dev-web:
    cargo run -p pos-server

dev-desktop: node-version-check
    pnpm exec tauri dev --config apps/desktop/tauri.conf.json

# Local model leg (m0-s10/m0-s13): assumes an Ollama daemon on the default port.
dev-ollama:
    @echo "gateway OpenAI-compatible adapter lands in m0-s10; start ollama separately (ollama serve)"

# The m0-s04 crash-point harness: every fault point × kill -9, restart,
# verify zero corruption. Runs inside `just test` per PR (it is fast at M0
# scale) and as the dedicated nightly lane the milestone names.
crash-matrix:
    cargo test -p pos-store --test crash_matrix
    cargo test -p pos-log --test log_crash

# The m1-s01 pipeline oracles at nightly weight. NOT a duplicate of the PR
# lane: release profile puts real corpus sizes in reach, and the wider
# property budget is what makes the segment decoder's totality claim mean
# something. The fault-point matrix inside `stage_framework` covers every
# stage this build registers — it grows as m1-s03/s04/s05/s11 land theirs,
# which is why the story's AC stays open until then.
ingest-matrix:
    PROPTEST_CASES=4096 cargo test --release -p pos-ingest

# The m0-s16 gate campaign. NOT in `ci`: §18 numbers are measured on a pinned
# reference machine under the docs/reference-machines.md §4 protocol, and the
# harness itself downgrades anything else to `early_warning`.
#
# Prerequisites for a binding run: a clean tree, release builds, AC power, and
# `just e2e` first (the in-page measurements are two of the three scenarios'
# inputs).
bench: bench-build
    ./target/release/pos-bench run --scenario ui-interaction-p95 --out ../docs/gates/m0
    ./target/release/pos-bench run --scenario project-open1m --out ../docs/gates/m0
    ./target/release/pos-bench run --scenario desktop-cold-start50 --out ../docs/gates/m0

bench-build: node-version-check
    pnpm --dir apps/ui build
    cargo build --release -p pos-bench -p pos-desktop

# One scenario, for iterating: `just bench-one project-open1m`.
bench-one scenario:
    ./target/release/pos-bench run --scenario {{scenario}}

# The M1 gate campaign (m1-s07 opened the intake path both rows drive through).
#
# `bench-m1-buffers` writes ~8.3 GiB of dataset into `target/bench-data` and
# ingests it, twice over: budget the disk before starting it.
# `bench-m1-transcribe` needs whisper-small pulled (`pos models pull`) and a
# real recording; the artifact records the derived evidence id and the
# duration, never the path, because the recording is gitignored.
bench-m1: bench-m1-buffers
    @echo "bench-m1: run `just bench-m1-transcribe <audio>` with a real recording"

bench-m1-buffers: bench-build
    ./target/release/pos-bench run --scenario ingest-buffers8gb --replicates 3

bench-m1-transcribe audio replicates="3": bench-build
    POS_MODELS_DIR=models/pulled \
      ./target/release/pos-bench run --scenario transcribe-realtime \
      --audio {{audio}} --replicates {{replicates}}

# Emits the docs/reference-machines.md fingerprint for the current host. Run on
# each binding machine and paste the output into its registry row (m0-s02).
fingerprint-machine machine_id: node-version-check
    @bash scripts/fingerprint-machine.sh {{machine_id}}
