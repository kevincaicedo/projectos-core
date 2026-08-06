# Contributing to ProjectOS Core

ProjectOS is built on the premise that rules a machine can check are the only
rules that survive a deadline. Almost everything below is enforced by `just ci`
rather than by review taste, so the fastest path to a merged change is a green
local run.

## Developer Certificate of Origin

We take contributions under the
[Developer Certificate of Origin 1.1](https://developercertificate.org/). There
is no contributor licence agreement and no copyright assignment: you keep your
copyright, and your contribution is licensed under Apache-2.0 like the rest of
the repository.

Sign off every commit:

```
git commit --signoff
```

which appends:

```
Signed-off-by: Your Name <your.email@example.com>
```

The DCO check on every pull request fails without it. Use a real name and a
real address you can receive mail at. To fix a missed sign-off:

```
git rebase --signoff origin/main
git push --force-with-lease
```

## Before you open a pull request

```
just ci
```

`just ci` is the merge bar, not a suggestion. It runs formatting, Clippy with
`-D warnings`, the full test suite, the supply-chain policy, the crate
dependency DAG, the discipline checks, the boundary gates, the capability
catalog freshness check, the no-cloud public build, and the UI and native
builds. CI runs the same recipes, so a green local run is a real prediction.

If a check fails, fix the code. Do not weaken the check to merge — a relaxed
gate is a permanent change to the project's guarantees traded for a temporary
convenience.

## The rules the machine enforces

You will meet these as CI failures, so they are worth knowing first:

- **No `unsafe`.** Every crate carries `#![forbid(unsafe_code)]`. The only
  exceptions are audited FFI leaf modules, which opt out explicitly and carry a
  `SAFETY.md` entry with the argument.
- **No operational panics.** `.unwrap()` and `.expect()` outside tests are
  rejected unless the call carries a trailing same-line `// INVARIANT: …`
  comment stating why the case is impossible. Empty markers and markers hidden
  in string literals are detected and rejected.
- **Projections are written in one place.** Writes to `proj_*` tables outside
  the `pos-log` apply path are rejected. Append is the only write.
- **The dependency ledger is exact.** Every direct Cargo and npm dependency
  needs a row in `DEPENDENCIES.md` with its failure surface, eject path, why
  roughly 50 lines of ours cannot replace it, and an owner. An unlisted
  dependency fails CI; so does a stale row.
- **The crate DAG points one way.** `check-dep-dag` reads `cargo metadata` and
  rejects upward imports. Shells (`apps/desktop`, `bins/pos`, `bins/pos-server`)
  stop at `pos-api` and never reach into domain crates.
- **The UI does not hand-declare server types.** TypeScript types that cross
  the API boundary are generated into `apps/ui/src/api/gen/`. An ESLint rule
  rejects hand-written declarations, and a freshness check rejects stale
  generated files.
- **The open-core boundary is mechanical.** The boundary gates in
  `check-boundaries` implement ADR-0004 (open-core repository topology). Core
  never references cloud; cloud reaches core only through `pos-capabilities`.
- **Frozen seams need a version bump and an ADR.** Changing a frozen capability
  trait without both fails `seam-freeze`.

## Commits and pull requests

- Conventional-commit subjects (`feat:`, `fix:`, `docs:`, `refactor:`,
  `test:`, `chore:`), imperative mood, no trailing period.
- One logical change per pull request. A refactor and a behaviour change in one
  diff cannot be reviewed honestly.
- Describe what breaks if the change is wrong, and name the test that would
  catch it. "Added tests" is not that sentence.
- Every new boundary, budget, or policy needs a seeded failure fixture proving
  the check actually fires. A gate without a failing fixture is decoration.

## Decisions that need an ADR

Open an ADR from the ADR template before the code, for: a new
external dependency in a domain crate, a change to a frozen capability trait, a
storage-format change, a new process boundary, or anything the master plan
calls a one-way door. Record the alternatives you rejected and why — an ADR
whose alternatives section is empty is a decision nobody can revisit.

## Reporting security issues

Do not open a public issue. Follow [SECURITY.md](SECURITY.md).

## Using the name

Code contributions are Apache-2.0. The ProjectOS name and logo are not covered
by that licence; see [TRADEMARK.md](TRADEMARK.md).

## Conduct

Be straightforward, assume competence, and argue about the work rather than the
person. Reports of behaviour that makes this project worse to work on go to
ing.sys.kevincaicedo@gmail.com.
