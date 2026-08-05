# ProjectOS core — task runner (m0-s01). `just ci` is the merge bar.

default:
    @just --list

# Everything CI runs. Green here before presenting any change as done (AGENTS.md).
ci: node-version-check fmt-check clippy test deny dep-dag discipline core-boundaries capability-catalog-check public-build-bootstrap ui-check ui-build desktop-check
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

# M0-E1 portion of public-builds-alone. m0-s16 extends this same gate with
# signed installers and the full walking-skeleton e2e before its AC can close.
public-build-bootstrap:
    test ! -e cloud
    cargo build --workspace --all-targets

generate-capabilities:
    cargo run --quiet -p pos-api --bin export-capabilities -- --write apps/ui/src/api/gen/capabilities.ts

# UI checks: typecheck + lint + format. Requires `pnpm install` once (corepack).
ui-check: node-version-check
    pnpm --dir apps/ui exec tsc --noEmit
    pnpm --dir apps/ui lint
    pnpm --dir apps/ui boundary:fixture
    pnpm --dir apps/ui format:check

ui-build: node-version-check
    pnpm --dir apps/ui build

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

# pos-bench lands in m0-s16.
bench:
    @echo "pos-bench v0 lands in m0-s16 (cold-start, project-open, interaction scenarios)"
