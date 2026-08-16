// Typed readers over the generated ingestion wire types (m1-s01). Same rule
// as `projects.ts`: validation narrows runtime bytes into the ts-rs shapes,
// and an unrecognised payload becomes an error state rather than rendered
// content — a half-understood health card is worse than an honest error.

import type {
  EvidenceListReport,
  EvidenceRow,
  EvidenceStageRow,
  SourceHealthReport,
  SourceHealthRow,
} from "./gen/api";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function optionalString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function optionalNumber(value: unknown): value is number | null {
  return value === null || typeof value === "number";
}

export function asSourceHealthReport(value: unknown): SourceHealthReport | null {
  if (!isRecord(value) || !Array.isArray(value.sources)) {
    return null;
  }
  const sources: SourceHealthRow[] = [];
  for (const candidate of value.sources) {
    const row = asSourceHealthRow(candidate);
    if (row === null) {
      return null;
    }
    sources.push(row);
  }
  return { sources };
}

function asSourceHealthRow(value: unknown): SourceHealthRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const {
    sourceId,
    stage,
    okCount,
    failedCount,
    deadCount,
    itemCount,
    bytesTotal,
    wallMsTotal,
    lastSuccessTsMs,
    lastFailureTsMs,
    lastErrorCode,
    costFeature,
  } = value;
  if (
    typeof sourceId !== "string" ||
    typeof stage !== "string" ||
    typeof okCount !== "number" ||
    typeof failedCount !== "number" ||
    typeof deadCount !== "number" ||
    typeof itemCount !== "number" ||
    typeof bytesTotal !== "number" ||
    typeof wallMsTotal !== "number" ||
    typeof costFeature !== "string" ||
    !optionalNumber(lastSuccessTsMs) ||
    !optionalNumber(lastFailureTsMs) ||
    !optionalString(lastErrorCode)
  ) {
    return null;
  }
  return {
    sourceId,
    stage,
    okCount,
    failedCount,
    deadCount,
    itemCount,
    bytesTotal,
    wallMsTotal,
    lastSuccessTsMs,
    lastFailureTsMs,
    lastErrorCode,
    costFeature,
  };
}

export function asEvidenceListReport(value: unknown): EvidenceListReport | null {
  if (!isRecord(value) || !Array.isArray(value.evidence)) {
    return null;
  }
  if (typeof value.rowCountMax !== "number") {
    return null;
  }
  const evidence: EvidenceRow[] = [];
  for (const candidate of value.evidence) {
    const row = asEvidenceRow(candidate);
    if (row === null) {
      return null;
    }
    evidence.push(row);
  }
  return { evidence, rowCountMax: value.rowCountMax };
}

function asEvidenceRow(value: unknown): EvidenceRow | null {
  if (!isRecord(value) || !Array.isArray(value.stages)) {
    return null;
  }
  const stages: EvidenceStageRow[] = [];
  for (const candidate of value.stages) {
    const stage = asEvidenceStageRow(candidate);
    if (stage === null) {
      return null;
    }
    stages.push(stage);
  }
  const row = value as Record<string, unknown>;
  if (
    typeof row.evidenceId !== "string" ||
    typeof row.sourceId !== "string" ||
    typeof row.sourceKind !== "string" ||
    typeof row.externalId !== "string" ||
    typeof row.mediaKind !== "string" ||
    typeof row.shape !== "string" ||
    typeof row.status !== "string" ||
    typeof row.canaryLevel !== "string" ||
    typeof row.occurredTsMs !== "number" ||
    typeof row.byteSize !== "number" ||
    typeof row.chunkCount !== "number" ||
    typeof row.pass !== "number" ||
    typeof row.nextStageAvailable !== "boolean" ||
    !optionalString(row.externalUrl) ||
    !optionalString(row.title) ||
    !optionalString(row.author) ||
    !optionalString(row.nextStage) ||
    !optionalString(row.nextStageOwnerStory)
  ) {
    return null;
  }
  return {
    evidenceId: row.evidenceId,
    sourceId: row.sourceId,
    sourceKind: row.sourceKind,
    externalId: row.externalId,
    externalUrl: row.externalUrl,
    mediaKind: row.mediaKind,
    shape: row.shape,
    status: row.status,
    canaryLevel: row.canaryLevel,
    title: row.title,
    author: row.author,
    occurredTsMs: row.occurredTsMs,
    byteSize: row.byteSize,
    chunkCount: row.chunkCount,
    pass: row.pass,
    nextStage: row.nextStage,
    nextStageOwnerStory: row.nextStageOwnerStory,
    nextStageAvailable: row.nextStageAvailable,
    stages,
  };
}

function asEvidenceStageRow(value: unknown): EvidenceStageRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const {
    stage,
    state,
    pass,
    attemptIndex,
    wallMs,
    bytesRead,
    itemCount,
    lastErrorCode,
    lastErrorDetail,
  } = value;
  if (
    typeof stage !== "string" ||
    typeof state !== "string" ||
    typeof pass !== "number" ||
    typeof attemptIndex !== "number" ||
    !optionalNumber(wallMs) ||
    !optionalNumber(bytesRead) ||
    !optionalNumber(itemCount) ||
    !optionalString(lastErrorCode) ||
    !optionalString(lastErrorDetail)
  ) {
    return null;
  }
  return {
    stage,
    state,
    pass,
    attemptIndex,
    wallMs,
    bytesRead,
    itemCount,
    lastErrorCode,
    lastErrorDetail,
  };
}

/// The stage row a dead-lettered item is stuck on. `undefined` when the item
/// is not in the DLQ, which is what lets the card render "healthy" without
/// inventing a reason.
export function deadStage(row: EvidenceRow): EvidenceStageRow | undefined {
  return row.stages.find((stage) => stage.state === "dead");
}
