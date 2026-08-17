// Typed reader over the intake wire types (m1-s07). Same rule as
// `evidence.ts`: an unrecognised payload becomes an error state rather than
// rendered content — a half-understood import summary is worse than an
// honest error, because the user's next move depends on believing it.

import type { IngestSubmitReport, IngestSubmitRow } from "./gen/api";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

export function asIngestSubmitReport(value: unknown): IngestSubmitReport | null {
  if (!isRecord(value) || !Array.isArray(value.items)) {
    return null;
  }
  const {
    sourceId,
    addedCount,
    duplicateCount,
    refusedCount,
    skippedCount,
    truncated,
    rowCountMax,
    backgroundWorkersRunning,
  } = value;
  if (
    typeof sourceId !== "string" ||
    typeof addedCount !== "number" ||
    typeof duplicateCount !== "number" ||
    typeof refusedCount !== "number" ||
    typeof skippedCount !== "number" ||
    typeof truncated !== "boolean" ||
    typeof rowCountMax !== "number" ||
    typeof backgroundWorkersRunning !== "boolean"
  ) {
    return null;
  }
  const items: IngestSubmitRow[] = [];
  for (const candidate of value.items) {
    const row = asIngestSubmitRow(candidate);
    if (row === null) {
      return null;
    }
    items.push(row);
  }
  return {
    sourceId,
    addedCount,
    duplicateCount,
    refusedCount,
    skippedCount,
    truncated,
    items,
    rowCountMax,
    backgroundWorkersRunning,
  };
}

function asIngestSubmitRow(value: unknown): IngestSubmitRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const { fileName, evidenceId, mediaKind, byteSize, outcome, refusedCode, refusedDetail } = value;
  if (
    typeof fileName !== "string" ||
    typeof byteSize !== "number" ||
    typeof outcome !== "string" ||
    !optionalString(evidenceId) ||
    !optionalString(mediaKind) ||
    !optionalString(refusedCode) ||
    !optionalString(refusedDetail)
  ) {
    return null;
  }
  return { fileName, evidenceId, mediaKind, byteSize, outcome, refusedCode, refusedDetail };
}

/// One line a person can act on. Written here rather than in the panel so the
/// wording of "you already had this" lives beside the shape it reads.
export function intakeSummary(report: IngestSubmitReport): string {
  const parts: string[] = [];
  if (report.addedCount > 0) {
    parts.push(`${report.addedCount} ingested`);
  }
  if (report.duplicateCount > 0) {
    parts.push(`${report.duplicateCount} already ingested`);
  }
  if (report.refusedCount > 0) {
    parts.push(`${report.refusedCount} refused`);
  }
  if (report.skippedCount > 0) {
    parts.push(`${report.skippedCount} skipped`);
  }
  if (parts.length === 0) {
    return "Nothing to ingest here.";
  }
  const summary = parts.join(" · ");
  return report.truncated ? `${summary} · more files than one import covers` : summary;
}

/// Bytes as something a person reads. Binary units, because that is what the
/// bounds this product states are written in.
export function byteSize(bytes: number): string {
  const units = ["B", "KiB", "MiB", "GiB"];
  let value = bytes;
  let unit = 0;
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024;
    unit += 1;
  }
  return `${unit === 0 ? value : value.toFixed(1)} ${units[unit]}`;
}
