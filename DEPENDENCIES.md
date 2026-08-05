# Dependency ledger

Every dependency answers three questions before it enters (STYLE §Dependencies,
`.agents/skills/crate-boundaries`): what breaks when it breaks, what is the
eject path, and whether roughly 50 lines of ours could replace it. The
`check-discipline` binary compares these rows exactly with Cargo metadata and
every workspace `package.json`; missing, stale, duplicate, or incomplete rows
fail `just ci`.

## Rust (crates.io)

| Dependency | Used by | Failure surface | Eject path | Why ours cannot replace it | Owner |
|---|---|---|---|---|---|
| `proc-macro2` | `check-discipline` | Source spans become unavailable and discipline CI stops closed | Replace span lookup with a maintained lexer; roughly 100 lines plus adversarial tests | It supplies compiler-compatible token spans and is already in syn's supply chain | founders |
| `serde_json` | repository checkers | Cargo/npm metadata cannot be decoded and boundary CI stops closed | Replace metadata transport or write a bounded JSON decoder; at least several hundred security-sensitive lines | JSON correctness is commodity infrastructure and serde is the Rust ecosystem standard | founders |
| `syn` | `check-discipline` | Rust policy scanning cannot distinguish code, test items, macros, and literals, so discipline CI stops closed | Replace with rustc tooling or a maintained Rust lexer/parser; multiple engineer-days | A 50-line grep cannot parse Rust without false positives or bypasses | founders |
| `tauri` | `apps/desktop` | The native desktop window, webview, and event loop cannot boot | Replace the thin shell behind `pos-api`; domain crates and the shared UI remain unchanged, but native packaging is a planned migration | §20 chooses Tauri v2 for the small system-webview shell and Rust-core reuse; recreating a cross-platform shell is not product leverage | founders |
| `tauri-build` | `apps/desktop` build | Tauri configuration and capability metadata are not generated, so desktop compilation stops closed | Replace alongside Tauri or own equivalent platform build generation | It is the official build half of the selected Tauri v2 stack and must version with it | founders |

## npm (apps/ui)

| Dependency | Used by | Failure surface | Eject path | Why ours cannot replace it | Owner |
|---|---|---|---|---|---|
| `react` | `apps/ui` runtime | The shared desktop/web component tree cannot render | Migrate behind component and hook seams; a planned UI-framework migration | §20 chooses React for ecosystem depth and hiring leverage; recreating a renderer is outside product scope | founders |
| `react-dom` | `apps/ui` runtime | The React tree cannot mount in browser or Tauri webview | Move the mount adapter with any React replacement | Browser reconciliation and event plumbing are framework infrastructure, not 50 lines | founders |
| `typescript` | `apps/ui` build | Generated API and strict UI type checks stop closed | Pin/upgrade the compiler or migrate the UI language as a one-way-door ADR | A type checker cannot be replaced locally; strict TypeScript is a §20 decision | founders |
| `vite` | `apps/ui` build | Development server and production UI bundle stop | Replace the build adapter with another ESM bundler and update packaging tests | Bundling, HMR, and asset graphs are commodity tooling far beyond 50 lines | founders |
| `@vitejs/plugin-react` | `apps/ui` build | React transforms and fast refresh stop | Replace alongside Vite or configure an equivalent React transform | Correct JSX transforms and refresh integration belong to the selected build stack | founders |
| `eslint` | `apps/ui` CI | Mechanical UI source rules stop closed | Move enforced rules to another TypeScript-aware linter | The rule engine and editor ecosystem are commodity tooling, not product code | founders |
| `typescript-eslint` | `apps/ui` CI | TypeScript AST lint rules, including no-any, stop | Replace with another TypeScript-aware ESLint bridge or linter | Parsing TypeScript correctly is not a safe local utility | founders |
| `prettier` | `apps/ui` CI | Deterministic UI formatting stops closed | Replace with a pinned formatter and one mechanical rewrite | A mature TS/TSX formatter avoids maintaining syntax-sensitive style code | founders |
| `@types/react` | `apps/ui` build | React APIs lose static types and strict compilation fails | Replace with types shipped by a future framework/runtime | These declarations track React's public contract and must version with it | founders |
| `@types/react-dom` | `apps/ui` build | DOM mounting APIs lose static types and strict compilation fails | Replace with types shipped by a future mount adapter | These declarations track react-dom and cannot be safely hand-maintained | founders |
| `@tauri-apps/cli` | desktop development and packaging | `tauri dev` and later bundle commands stop; browser UI development remains available | Use the pinned Rust CLI or replace alongside the Tauri shell | The official CLI validates config, coordinates the UI build, and drives native packaging; reproducing it is outside the thin-shell boundary | founders |

Dev-machine tools (not vendored): `just`, `cargo-deny`, Node, and `pnpm` via
corepack — Node, pnpm, and Rust are exact-pinned by `.node-version`,
`package.json#packageManager`, and `rust-toolchain.toml`; hosted workflows pin
`just` 1.47.1; cargo-deny is selected by its CI action and enforced through the
checked policy file.

The pnpm install-script allowlist contains only `esbuild`: Vite needs its
platform binary, and esbuild's installer verifies/selects that package. pnpm 11
fails installation if any other direct or transitive dependency introduces an
unreviewed build script.
