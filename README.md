# ProjectOS Core

Local-first project workspace. Your projects are directories on your own disk
that you can inspect, copy, and export without an account, a server, or our
permission.

This repository is the complete product: the desktop app, the self-hosted
server, the CLI, the SDK and public contracts, and the plugin and pack
mechanisms. It is Apache-2.0. There is no disabled code path waiting for a
licence key — a mechanical `no-crippleware` gate asserts that every open
capability is functional.

> **Status: M0.** The foundation is real and enforced; the product is not built
> yet. The walking skeleton boots, resolves its capability registry, and tells
> you honestly what it cannot do. See `docs/` for the architecture and
> contracts.

## Build it

Requires the Rust, Node, and pnpm versions pinned by `rust-toolchain.toml`,
`.node-version`, and `package.json#packageManager`, plus
[`just`](https://github.com/casey/just).

Local transcription builds whisper.cpp from source, so a C++ toolchain,
**CMake**, and **libclang** (for the bindings generator) are build
prerequisites — on every platform that ships it. macOS gets all three from the
Xcode command-line tools plus `brew install cmake`; on Debian/Ubuntu:

```sh
sudo apt-get install build-essential clang cmake libclang-dev
```

**Shipping targets for local transcription at M1: macOS and Linux.** Windows
builds are not exercised; the capability is declared rather than silently
absent, and cloud transcription is the path there.

macOS 10.15 or newer, because whisper.cpp's ggml uses C++17 `std::filesystem`.

```sh
pnpm install
just ci            # the merge bar: build, test, policy, boundaries, UI, e2e
just dev-desktop   # the native shell
```

`just ci` runs everything CI runs, so a green local run is a real prediction
rather than a hope.

## How this repository is governed

Most of the rules here are checked by a machine, because a rule a machine
cannot check is a rule that dies at the first deadline. `just ci` rejects
`unsafe`, operational `.unwrap()`/`.expect()` without a stated invariant,
dependencies missing from the ledger, upward crate imports, hand-declared
server types in the UI, and a capability that claims availability it has not
demonstrated. Every gate ships with a seeded failing fixture proving it fires.

The commercial boundary is equally mechanical: the private `projectos-cloud`
repository may implement the public capability traits and sell hosted
operations, but it cannot hide a core mechanism or extend the seam privately.

- [CONTRIBUTING.md](CONTRIBUTING.md) — DCO sign-off, the enforced rules, and
  when a decision needs an ADR
- [SECURITY.md](SECURITY.md) — one private intake, with response targets
- [TRADEMARK.md](TRADEMARK.md) — what you may call your fork
- [DEPENDENCIES.md](DEPENDENCIES.md) — every direct dependency, and why it is
  here

## Licence

Apache-2.0 — see [LICENSE](LICENSE) and [NOTICE](NOTICE).
Copyright 2026 Private AI Inc. The code is licensed; the name is not.
