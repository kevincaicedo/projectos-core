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

# Builds the desktop bundles and signs their checksum manifest with the release
# key. Founder-run or release-workflow-run; never part of the CI critical path,
# which is why `desktop-check` above passes --no-bundle instead of reusing this.
package key_path:
    pnpm exec tauri build --config apps/desktop/tauri.conf.json --ci
    @bash scripts/sign-release.sh target/release/bundle {{key_path}}

verify-package identity allowed_signers:
    @bash scripts/verify-release.sh target/release/bundle {{identity}} {{allowed_signers}}

# UI checks: typecheck + lint + format. Requires `pnpm install` once (corepack).
ui-check: node-version-check
    pnpm --dir apps/ui exec tsc --noEmit
    pnpm --dir apps/ui lint
    pnpm --dir apps/ui boundary:fixture
    pnpm --dir apps/ui format:check

ui-build: node-version-check
    pnpm --dir apps/ui build

# Walking-skeleton e2e over the production bundle, with no server, no account,
# and no cloud checkout. Needs `just e2e-install` once per machine.
e2e: node-version-check
    pnpm --dir apps/ui exec playwright test

e2e-install: node-version-check
    pnpm --dir apps/ui exec playwright install --with-deps chromium

# Compile the native shell through the actual Tauri CLI/config path without
# producing installers (packaging and signing remain m0-s07 work).
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

# pos-bench lands in m0-s16.
bench:
    @echo "pos-bench v0 lands in m0-s16 (cold-start, project-open, interaction scenarios)"

# Emits the docs/reference-machines.md fingerprint for the current host. Run on
# each binding machine and paste the output into its registry row (m0-s02).
fingerprint-machine machine_id:
    @bash scripts/fingerprint-machine.sh {{machine_id}}
