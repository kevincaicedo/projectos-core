# ProjectOS Project Directory Format — v0

> The documented contract behind "a project is a file" (F2, F45, L4).
> Governs every `.pos` directory ProjectOS creates, opens, verifies, and
> exports. Normative alongside [master-plan.md](master-plan.md) §7; the code
> constant `pos_store::FORMAT_VERSION` and this document move together — a CI
> check fails when they disagree.

**Format version:** `0`

## 1. Version policy

- `manifest.json#format_version` states the version a directory was written
  with. A build refuses to open a directory **newer** than it understands,
  with a typed error naming the version — never a best-effort read.
- Version bumps are **additive with a documented migration** (§3.2 of the M0
  plan). Removing or re-typing anything described here is forbidden; new
  tables, columns, and event kinds may be added.
- Projection tables (`proj_*`) are **derived state** and not part of the
  portable contract: any conforming build re-derives them from the log
  (`pos verify` proves it). Only §2–§6 below are load-bearing.

## 2. Directory layout

```text
acme-widgets.pos/
  project.db          # SQLite: event log + projections + FTS5 + sqlite-vec
  blobs/              # content-addressed blob store (BLAKE3)
    ab/cd/<hash>      # two-level fan-out, 64-char lowercase hex file names
    tmp/              # in-flight writes; disposable by definition
  manifest.json       # format version, project id, template, created
```

Copying the directory is a full backup. Deleting it deletes the project.
`manifest.json` is written **last** during creation and export: its presence
marks a complete directory, so an interrupted copy can never be mistaken for
a project.

## 3. `manifest.json`

UTF-8 JSON object; unknown fields must be preserved by tools that rewrite it.

| Field | Type | Meaning |
|---|---|---|
| `format_version` | integer | The version of this specification the directory conforms to (`0`). |
| `project_id` | string | 32-char lowercase hex — the project's stable 128-bit identity. |
| `template` | string | The creation template name (`generic` in v0). |
| `created_ts_ms` | integer | Wall-clock creation time, Unix milliseconds. Informational. |

## 4. `project.db` — the event log

SQLite database, WAL journal mode. The log tables are the durable truth (L1);
everything else in the database is rebuildable from them.

### 4.1 `events` — append-only, immutable

| Column | Type | Meaning |
|---|---|---|
| `seq` | INTEGER | Per-project contiguous sequence, starting at 1. Primary key; assigned at append. THE ordering. |
| `device` | BLOB(16) | Origin device/server identity (sync-ready from day one). |
| `lamport` | INTEGER | Per-device logical clock, strictly monotonic in seq order (M5 cross-device ordering). |
| `ts_ms` | INTEGER | Wall clock at append, Unix milliseconds. **Informational only** — never used for ordering or derived state. |
| `actor_kind` | INTEGER | `0` = user, `1` = agent run, `2` = system job. |
| `actor_id` | BLOB(16) | The user/run/job identity that caused the event. Never defaulted. |
| `kind` | TEXT | Past-tense fact tag (`ProjectCreated`, `RunStepCommitted`, …), ASCII alphanumeric, ≤ 64 bytes. |
| `body` | BLOB | Versioned CBOR payload (§5). ≤ 1 MiB — bulk content lives in `blobs/` with a ref. |
| `refs` | BLOB | CBOR array of `{entity: text, id: bytes(16)}` — the L2 links this event creates/touches. ≤ 64 entries. |

Rows are never updated or deleted. A tool that mutates an `events` row has
corrupted the project.

### 4.2 Log bookkeeping tables

- `log_devices(device BLOB PK, lamport INTEGER)` — the per-device lamport
  high-water mark.
- `log_state(key TEXT PK, value BLOB)` — `applied_seq` (8-byte big-endian:
  the seq projections are current through) and `schema_digest` (32 bytes:
  the projection-schema identity projections were built under).
- `log_snapshots(snapshot_seq INTEGER PK, schema_digest BLOB, body BLOB,
  created_ts_ms INTEGER)` — periodic CBOR projection snapshots that bound
  replay at open. Snapshots are an optimization: deleting every row only
  costs a full replay.

### 4.3 Projections (`proj_*` tables)

Derived, deterministic, rebuildable. Byte-identical replay of the same log
is a property-tested guarantee (§18 gate). Conforming tools must not write
them directly; ProjectOS enforces this mechanically in CI.

### 4.4 Node-local operational tables (`sched_*`)

`sched_leases(job_id BLOB PK, worker TEXT, attempt_index INTEGER,
claimed_ts_ms INTEGER, heartbeat_ts_ms INTEGER, lease_expires_ts_ms
INTEGER)` records which worker on **this machine** currently holds which
job (m0-s14).

It is deliberately outside the portable contract, in the opposite direction
from projections: a projection is derivable and therefore redundant, while a
lease is *only* meaningful on the node that took it. A conforming tool may
delete every row; the effect is that every unfinished job becomes claimable
again, which is exactly what a crash already does. `pos export` does not
carry it, and a copied project must not inherit another machine's leases.
The durable half of the queue — what work exists, what it attempted, how it
ended — lives in the log as `JobEnqueued` / `JobAttemptFailed` /
`JobCompleted` / `JobDead` facts and travels normally.

## 5. Event bodies — CBOR versioning rules

- Bodies are CBOR (RFC 8949) encodings of versioned structures: an
  externally tagged variant whose name is the version (`{"V1": {...}}`).
- Ids inside bodies are CBOR byte strings of length 16.
- **Old events are eternal:** a field, once shipped in some `V n`, is never
  removed or re-typed. Evolution adds `V(n+1)` beside it; readers match all
  shipped versions.
- A reader encountering an unknown `kind` must skip it without error (the
  raw event remains in the log); an unknown *version* of a known kind is an
  error naming the kind.

## 6. `blobs/` — content-addressed store

- Address = BLAKE3-256 of the content, rendered as 64-char lowercase hex.
- Path = `blobs/<hex[0..2]>/<hex[2..4]>/<hex>`.
- Identical content is stored once. A file whose re-hash does not equal its
  address is corrupt (`pos verify` names it).
- `blobs/tmp/` holds in-flight writes; its contents are disposable and are
  swept on open. Tools must ignore it.

- **Blob roles the ingestion pipeline writes (m1-s01).** Blobs are opaque
  bytes and the store does not type them; three roles are referenced from
  event bodies and are therefore part of the portable contract:
  - the **original content** of an Evidence item, byte-for-byte as it arrived
    (`EvidenceAdded.content_blob`);
  - the **normalized text**, UTF-8, `\n`-separated, BOM-stripped — the bytes a
    citation renders (`IngestStageFinished` → `Normalized.text_blob`);
  - the **segment index** over that text
    (`Normalized.segments_blob`), a sequence of fixed 40-byte
    little-endian records with no framing:

    | Offset | Size | Field |
    |---|---|---|
    | 0 | 8 | `byte_start` — inclusive offset into the normalized text |
    | 8 | 8 | `byte_end` — exclusive |
    | 16 | 1 | locator kind: `1` time range, `2` line range, `3` message range |
    | 17 | 1 | structural depth (markdown heading level, else 0) |
    | 18 | 6 | reserved, written as zero, ignored on read |
    | 24 | 8 | locator bound A (`start_ms` / `start`) |
    | 32 | 8 | locator bound B (`end_ms` / `end`) |

    An unknown locator kind, a short trailing record, or `byte_end <
    byte_start` is corruption, not a value to guess at. A conforming tool does
    **not** need this blob to render a citation — every chunk's locator is
    also a field of the `EvidenceChunked` fact — only to re-chunk.

## 7. Export (`pos export`)

An export is a directory that is **itself a valid v0 project** — it re-opens
and re-verifies with any conforming build — plus one additional file:

- `project.db` — a consistent single-file copy (no WAL sidecar).
- `blobs/` — every blob at its identical address.
- `events.jsonl` — the log rendered as UTF-8 JSON Lines, one event per line,
  in seq order, so the history is readable without SQLite:

| Field | Type | Meaning |
|---|---|---|
| `seq`, `lamport`, `tsMs` | number | As stored (§4.1). |
| `device` | string | 32-char lowercase hex. |
| `actorKind` | string | `"user"` \| `"agent"` \| `"system"`. |
| `actorId` | string | 32-char lowercase hex. |
| `kind` | string | The event kind tag. |
| `bodyCborHex` | string | The exact CBOR body bytes, lowercase hex. |
| `refs` | array | `{"entity": string, "id": hex string}` per ref. |

- `manifest.json` — written last, the completeness marker.

`events.jsonl` is a rendering, not a second truth: `project.db` remains the
authoritative log inside an export.
