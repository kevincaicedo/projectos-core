// Runtime validators over generated Run and cost-ledger wire types. These
// functions narrow unknown transport bytes; they do not redeclare server
// shapes or manufacture missing fields.

import type {
  CostRollupReport,
  CostRollupRow,
  CostRollupTotals,
  RunBudgetWire,
  RunReport,
  RunStepFrame,
} from "./gen/api";

const BUDGET_KEYS = [
  "tokens",
  "usdMicros",
  "wallMs",
  "storageBytes",
  "toolCalls",
  "retries",
  "steps",
] as const;

export const ECHO_BUDGET: RunBudgetWire = {
  tokens: 4_096,
  usdMicros: 0,
  wallMs: 90_000,
  storageBytes: 64 * 1_024,
  toolCalls: 3,
  retries: 0,
  steps: 3,
};

export function asRunReport(value: unknown): RunReport | null {
  if (!isRecord(value)) {
    return null;
  }
  const stringFields = ["path", "runId", "worker", "runtimeId", "executor", "status"];
  const numberFields = [
    "autonomyLevel",
    "committedStepCount",
    "checkpointedStepCount",
    "lineageDepth",
  ];
  if (
    !stringFields.every((field) => typeof value[field] === "string") ||
    !numberFields.every((field) => isNonNegativeNumber(value[field])) ||
    !isNullableString(value.projectId) ||
    !isNullableString(value.parentRunId) ||
    !isNullableString(value.pendingControl) ||
    typeof value.tainted !== "boolean" ||
    !isBudget(value.budget) ||
    !isBudget(value.spent) ||
    !Array.isArray(value.toolGrants) ||
    !value.toolGrants.every(isToolGrant) ||
    !isPause(value.pause)
  ) {
    return null;
  }
  return value as RunReport;
}

export function asRunStepFrame(value: unknown): RunStepFrame | null {
  if (!isRecord(value)) {
    return null;
  }
  if (
    typeof value.runId !== "string" ||
    !isNullableString(value.projectId) ||
    !isPositiveNumber(value.streamSeq) ||
    !isNonNegativeNumber(value.stepIndex) ||
    typeof value.phase !== "string" ||
    typeof value.summary !== "string" ||
    !isNullableString(value.toolId) ||
    !isPositiveNumber(value.committedSeq) ||
    !isPositiveNumber(value.checkpointSeq) ||
    !isBudget(value.spent) ||
    typeof value.runStatus !== "string" ||
    typeof value.terminal !== "boolean" ||
    !isNullableString(value.validationStatus)
  ) {
    return null;
  }
  return value as RunStepFrame;
}

export function asCostRollupReport(value: unknown): CostRollupReport | null {
  if (
    !isRecord(value) ||
    typeof value.scope !== "string" ||
    !isNonNegativeNumber(value.projectCount) ||
    !Array.isArray(value.rows) ||
    !value.rows.every(isCostRow) ||
    !isCostTotals(value.totals)
  ) {
    return null;
  }
  return value as CostRollupReport;
}

function isBudget(value: unknown): value is RunBudgetWire {
  return isRecord(value) && BUDGET_KEYS.every((key) => isNonNegativeNumber(value[key]));
}

function isToolGrant(value: unknown): boolean {
  return (
    isRecord(value) &&
    typeof value.toolId === "string" &&
    (value.mode === "allow" || value.mode === "gate" || value.mode === "block")
  );
}

function isPause(value: unknown): boolean {
  if (value === null) {
    return true;
  }
  if (!isRecord(value) || typeof value.kind !== "string") {
    return false;
  }
  return ["budget", "requested", "toolWeather", "reservationExceeded"].includes(value.kind);
}

function isCostRow(value: unknown): value is CostRollupRow {
  if (!isRecord(value)) {
    return false;
  }
  const strings = [
    "projectId",
    "feature",
    "provider",
    "credentialClass",
    "model",
    "providerCostKind",
  ];
  const numbers = ["calls", "tokensIn", "tokensOut", "wallMsTotal", "usdMicros"];
  return (
    strings.every((field) => typeof value[field] === "string") &&
    numbers.every((field) => isNonNegativeNumber(value[field])) &&
    isNullableString(value.agent)
  );
}

function isCostTotals(value: unknown): value is CostRollupTotals {
  return (
    isRecord(value) &&
    ["calls", "tokensIn", "tokensOut", "usdMicros", "projectosUsdMicros"].every((field) =>
      isNonNegativeNumber(value[field]),
    )
  );
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string";
}

function isNonNegativeNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isSafeInteger(value) && value >= 0;
}

function isPositiveNumber(value: unknown): value is number {
  return isNonNegativeNumber(value) && value > 0;
}
