# ProjectOS core architecture and contracts

> Public architecture overview mirrored byte-for-byte into
> `projectos-core/docs/master-plan.md`. The private superproject owns the full
> product plan; this document contains every rule needed to build and review
> the public core without access to that repository.

## Product invariant

ProjectOS is a ledger-first project lifecycle system. A project is a portable
directory whose append-only event log is the source of truth. Projections,
search indexes, UI views, agent runs, and exports are derived from or linked to
that log. The desktop, web, CLI, and future mobile shells use the same Rust
domain and typed `pos-api` surface.

The public build is a complete local and self-hostable product. ProjectOS Cloud
may add hosted operations, managed credentials, fleet capacity, collaboration,
and paid content by implementing public capability traits; it may not add a
private domain rule or disable a public mechanism.

## Twelve engineering laws

1. Append facts to the log; never write projections around the apply path.
2. Preserve the why-chain and outcome link for every lifecycle artifact.
3. Render AI claims only when their evidence references resolve.
4. Keep projects portable, exportable, and capable of true zero-cloud mode.
5. Agents propose; capability gates authorize side effects.
6. Treat ingested content as untrusted data, never instructions or shell input.
7. Commit a run step before applying its effect; resume from committed steps.
8. Put an explicit cap and visible degradation at every resource boundary.
9. Route every model call through `pos-gateway` and project policy.
10. Treat UI layouts and views as projections over typed API state.
11. First-party features use the same public registries third parties use.
12. Keep domain logic in Rust and prove shell parity through shared contracts.

## Workspace direction

The internal dependency direction is:

```text
pos-foundation <- pos-store <- pos-log <- pos-domain <- feature crates <- pos-api
       ^               ^            ^             ^                    ^
       +--------- pos-capabilities  |        pos-sdk facade            |
       +--------- pos-gateway ------+                                 shells
```

- `pos-foundation` owns ids, injected time, errors, and low-level config.
- `pos-store` owns SQLite and blob storage; `pos-log` owns append/replay/apply.
- `pos-domain` owns domain nouns, events, invariants, and projections.
- `pos-capabilities` and `pos-gateway` sit beside the domain layer.
- Feature crates depend downward through explicitly allowed edges.
- `pos-api` is the only shell surface. Desktop, server, and CLI depend on it
  and nothing deeper. The UI imports generated server types only.
- `pos-sdk` exposes public plugin, pack, connector, and adapter contracts;
  product crates never depend back on that facade.

`bins/check-dep-dag` owns the executable allowed-edge map. A new crate or edge
requires an architecture decision and a deliberate checker change.

## Open-core capability socket

`pos-capabilities` v1 freezes ten runtime sockets:

| Capability id | Public trait | Local default |
|---|---|---|
| `control.plane` | `ControlPlane` | `LocalControlPlane` |
| `identity.broker` | `CredentialBroker` | `KeychainBroker` |
| `sync.transport` | `SyncTransport` | `DirectSync` |
| `realtime.bus` | `RealtimeBus` | `LocalBus` |
| `worker.fleet` | `WorkerFleet` | `LocalPool` |
| `pack.source` | `PackSource` | `FilePackSource` |
| `media.render` | `MediaRenderer` | `LocalRenderer` |
| `billing.meter` | `BillingMeter` | `NoopMeter` |
| `relay.ingress` | `IngressRelay` | `LocalIngress` |
| `connector.host` | `ConnectorHost` | `LocalConnectorHost` |

Every registry contains all ten entries. Each provider reports `local`,
`hosted`, or `unavailable(reason)`; the unavailable reason is non-empty,
bounded, and user-visible. Cloud consumes a signed core tag and may implement
these traits only. Core never uses build flags, hostnames, or cloud imports to
select behavior.

Changing a trait signature or capability id requires a capability version bump
and an ADR link. Request/response envelopes may evolve only additively.

## Mechanical merge bar

From the public repository root:

```text
just ci
```

This runs Rust formatting, clippy with warnings denied, tests, license/advisory
and duplicate-dependency policy, crate-DAG checks, panic/projection/dependency
discipline, capability and open-core source checks, generated-catalog freshness,
strict TypeScript/lint/format checks, the production UI build, and a Tauri
config/native compile smoke.

Repository-level CI additionally proves a clean public checkout builds with no
cloud access. The private cloud CI proves it declares no domain truth. The
superproject CI verifies signed gitlinks, the cloud core pin, frozen-seam change
evidence, and byte-current public documentation.

The portable project format specification is added here by `m0-s05`, when the
format exists in code. Performance claims are valid only when an artifact names
a pinned reference machine and records raw replicates.
