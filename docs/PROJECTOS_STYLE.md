# ProjectOS Style

> Openly indebted to TigerBeetle's TIGER_STYLE and to InfinityDB's
> INFINITY_STYLE, adapted to ProjectOS's languages (Rust core, TypeScript UI),
> laws (L1–L12 in [master-plan.md](master-plan.md) Part II), and gates
> (master plan §18). Where this document and the master plan conflict, the
> master plan wins. Where this document is stricter than your habits, this
> document wins.
>
> Normative for all code in this repository. Reviewers affirm conformance per
> the milestone execution-discipline sections.

## Why Have Style?

Another word for style is design. Our design goals are **correctness,
efficiency, and developer experience — in that order**. All three matter; the
order settles arguments. A product that manages someone's entire product
history must never lose or corrupt it (correctness); a tool that claims
"any scale" must stay fast and small at gigabytes (efficiency); and a
two-person team shipping a platform can afford zero friction it didn't
choose (developer experience).

Style is not cosmetics: it is the set of decisions that make the next
thousand decisions cheaper and safer. Every rule below traces to a law, a
gate, or a category failure we refuse to repeat (master plan §5). When you
find a rule without a reason, fix the document — never obey mystery rules,
and never break reasoned ones casually.

## Simplicity and Technical Debt

Simplicity is the hardest revision, not the first draft. We spend design
energy up front — event schemas, seam freezes, napkin math — because an hour
of design is worth weeks of production archaeology, and because our STOP-gate
discipline means a wrong foundation halts the whole train.

**Zero technical debt, or visible debt.** We do it right the first time, or
we write the debt down where it cannot hide: an ADR marked `Proposed`, a
debt-forward entry in the milestone plan, a documented limitation in the
feature inventory. The one unforgivable form of debt is the silent kind. A
known limitation we ship is documentation; an unknown bug we could have
caught with an assertion is a process failure.

## Correctness (Rust core)

### Control flow

- **Simple, explicit control flow.** `?`, `let-else`, early returns, small
  helpers. Push `if`s up and `for`s down: parents own branching and state;
  leaf functions are straight-line. No recursion in parsers, ingestion
  stages, DAG walks, or projection code — iterative with explicit stacks and
  depth limits; every decoder ships its fuzz target in the same PR.
- **~70 lines per function.** If you scroll, you split — keeping control
  flow in the parent and moving straight-line work to helpers, never the
  reverse.
- **Events are the only writes (L1).** Domain state changes by appending a
  typed event and updating projections in the same transaction. Code that
  mutates a projection without an event is corruption with extra steps —
  there is a CI grep for direct projection writes outside `pos-log` apply
  paths.
- **Run at your own pace.** Pipeline stages, agent steps, and sync pull
  batches under budgets (L8); nothing does per-item syscalls, per-item
  transactions, or per-item model calls where a batch is possible.

### Put a limit on everything

Everything has a limit in reality; honest code states it. Every queue,
batch, retry loop, context assembly, embedding batch, SSE buffer, and
recursion-turned-iteration has a stated cap. Backpressure is budgets and
admission refusal, never an unbounded channel. When coverage is bounded
(top-N retrieval, sampled evidence, truncated context), the bound is
visible in the result metadata — silent truncation reads as completeness
and is therefore a lie (L3's engineering twin).

### Types

- **Make invalid states unrepresentable.** Newtypes for every id
  (`ProjectId`, `EventSeq`, `ChunkId`, `RunId` — never raw `u64`/`String`);
  enums for state machines (run status, job status, autonomy level);
  typestate where phase order matters (spec draft → review → approved).
- **Explicitly sized integers** in every event payload, wire format, and
  counter. `usize` never crosses a serialization boundary.
- Event payloads are `serde` structs with a version tag; **removing or
  re-typing a field is forbidden** — add a new version and a migration
  projection (L1 makes old events eternal; respect them).
- Distinguish `index`, `count`, and `size` in names. As a return type, `()`
  beats `bool`, `bool` beats `u64`, `Option` beats `Result` — every step
  down the ladder multiplies call-site branches.

### Assertions

Assertions detect programmer errors; operating errors get typed handling,
never assertions. The only correct response to corrupted internal state is
to crash — before it reaches the log.

- Assert preconditions, postconditions, and invariants; target two
  assertions per function in `pos-log`, `pos-store`, and `pos-domain`.
  `debug_assert!` default; release `assert!` reserved for invariants whose
  violation would corrupt durable state.
- **Pair assertions across boundaries:** assert before appending an event
  *and* after replaying it; before dispatching a pack *and* in the
  report-back handler. Boundaries are where the interesting bugs live.
- Every state machine keeps a written invariant inventory (module doc
  comment), kept current by the story that changes the machine.

### Panics and errors

- **Panics are for violated internal invariants only.** User input, model
  API failures, connector errors, disk-full, malformed ingested content —
  all typed errors, all handled, all tested. `unwrap()`/`expect()` on an
  operational `Result` is a review reject; `expect()` with an invariant
  justification is an assertion and is judged as one.
- **Model calls fail constantly — that is weather, not an incident.**
  Every gateway call site handles: timeout, rate-limit, refusal,
  malformed output, budget exhaustion. Degradation is visible (paused +
  notified, L8), never a silent empty answer.
- fsync/WAL failures in `pos-store` are fail-stop for the affected
  project. Never silently degrade durability.

### Unsafe Rust

`#![forbid(unsafe_code)]` everywhere except the audited FFI leaf modules
(whisper.cpp, ort, SQLite extension loading), each with `// SAFETY:`
arguments and an entry in the crate's `SAFETY.md`. Target: < 1% of LoC.
If you can express it safely at equal measured cost, the unsafe version is
wrong.

### Untrusted content (L6)

Everything ingested is hostile until proven boring. Ingested bytes are
**data**: they never touch `format!` into prompts outside delimited
evidence blocks, never reach shell/exec surfaces, never become file paths
without sanitization. The taint flag on agent runs is set by the harness,
not by the feature author — but the feature author must never launder
content across the boundary (review checklist item on every ingestion
and prompt-assembly PR).

## Correctness (TypeScript UI)

- `strict: true`, no `any` (lint-enforced; `unknown` + narrowing where
  genuinely dynamic). The UI consumes **generated** `pos-api` types only —
  hand-declaring a server type is a review reject (L12).
- The UI holds **view state only**. Domain state lives in the core and
  arrives by query/subscription; optimistic updates reconcile against
  events, never fork their own truth (L1 reaches the browser).
- Every user-visible async state has all four renderings: loading, empty,
  error (with retry), success. Uncited AI content renders as dropped —
  the citation-resolution component is the only way to render claims (L3).
- Components stay under ~150 lines; logic hooks separate from rendering;
  panels register through the panel registry (L10) — a hardcoded layout
  is a bug against morphability.

## Efficiency

> Napkin math before code. The best time for the 1000× win is design time.

- Sketch the four resources — disk, memory, model-API latency/cost, CPU —
  before building any pipeline or agent feature, and land within an order
  of magnitude of the §18 gate before writing code. Model calls are our
  fsync: the slowest, most expensive boundary — batch them, cache them,
  route them to the cheapest tier that passes evals (L9).
- **Stream, never slurp.** Files, transcripts, embeddings, exports, sync
  batches: bounded-memory streaming is the default shape; `read_to_end` on
  user content needs a size-capped justification. The GB-corpus CI job is
  the enforcement.
- **Measure, don't assume (the A/B rule).** Optimizations (SIMD-ish tricks,
  caching layers, clever indexes) ship with an artifact proving the win on
  the reference machine, or they don't ship. A losing A/B is a successful
  experiment: record it, don't merge it.
- **Memory attribution:** long-running processes report per-subsystem
  memory (ingest buffers, embedding batches, open projects, UI webview);
  RSS divergence from the sum is a bug to chase, not a mystery to accept.
- UI: virtualize every unbounded list; never block the main thread > 16 ms;
  measure interaction latency in CI (Playwright traces against the 100 ms
  gate).

## Developer Experience

### Naming

- Rust and TS conventions first; then: **get the nouns right** — the domain
  vocabulary (Evidence, Insight, Decision, Spec, Task, Milestone, Run,
  Memory) is fixed in master plan Part III; code, UI copy, events, and API
  use exactly these nouns. A synonym in code (`Finding`, `Story`, `Job` for
  a Task) is a review reject — vocabulary drift becomes user confusion.
- Units and qualifiers last, most significant first: `timeout_ms_default`,
  `budget_usd_run`. No abbreviations in identifiers (established domain
  terms — `id`, `seq`, `ttl`, `cas` — excepted). Long-form CLI flags.
- Event kinds are past-tense facts (`EvidenceAdded`, `DecisionApproved`,
  `RunStepCommitted`) — an event that reads like a command is modeling
  intent, not history, and belongs elsewhere.

### Comments and commits

- **Comments say why:** constraints, rejected alternatives, the invariant
  protected, the artifact justifying a trick. Comments narrating the next
  line are noise; delete them. Comments are prose — capitalized, full stops.
- Commit messages carry what and why, name the story id (`m2-s07`), link
  artifacts for evidence-bearing changes. A PR description is not stored in
  the repository and is therefore not a substitute.

### Tooling as law

`cargo fmt` + pinned toolchain; `-D warnings` everywhere; curated clippy
set; `cargo deny` (licenses, advisories, duplicates); `check-dep-dag`
(crate boundaries); panic-policy and projection-write greps; eslint +
prettier + `tsc --noEmit` for the UI; eval suite on prompt/retrieval
changes. Never weaken a check to merge — change the law first (ADR) or fix
the code. If a task needs a new tool, prefer a small Rust binary under
`bins/` over a shell script that works on one machine.

## Dependencies

Near-zero and always deliberate (master plan §20 records the big ones).
Every dependency is supply-chain surface, compile-time cost, and a
temptation to stop understanding our own stack. `DEPENDENCIES.md` carries
a one-line justification and an owner per entry; adding one requires
answering: what breaks when it breaks, what is the eject path, could 50
lines of ours do it? The usefulness of a dependency is inversely
proportional to the lifetime of the project — and we are building for the
long term. The UI gets no free pass: every npm package is the same
decision with worse odds.

## The Last Stage

These rules are seat belts, not ceremony. When a rule fights the work,
don't suffer silently and don't defect silently — change the rule in the
open (this file, an ADR, the master plan) so the next person inherits your
lesson instead of your workaround.

Keep trying things, measure everything, have fun. It's called ProjectOS
because, built this way, the project — any project, at any scale — finally
has an operating system.
