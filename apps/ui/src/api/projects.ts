// Typed readers over the generated wire types (m0-s09). Validation narrows
// runtime bytes into the ts-rs shapes; an unrecognised payload becomes an
// error state, never rendered content.

import type { HealthReport, OpenProjectRow, ProjectListReport } from "./gen/api";

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function asProjectListReport(value: unknown): ProjectListReport | null {
  if (!isRecord(value) || !Array.isArray(value.projects)) {
    return null;
  }
  if (typeof value.openProjectCountMax !== "number") {
    return null;
  }
  const projects: OpenProjectRow[] = [];
  for (const candidate of value.projects) {
    const row = asOpenProjectRow(candidate);
    if (row === null) {
      return null;
    }
    projects.push(row);
  }
  return { projects, openProjectCountMax: value.openProjectCountMax };
}

function asOpenProjectRow(value: unknown): OpenProjectRow | null {
  if (!isRecord(value)) {
    return null;
  }
  const { projectId, path, name, template, formatVersion, headSeq, openedTsMs } = value;
  if (
    typeof projectId !== "string" ||
    typeof path !== "string" ||
    typeof template !== "string" ||
    typeof formatVersion !== "number" ||
    typeof headSeq !== "number" ||
    typeof openedTsMs !== "number" ||
    (name !== null && typeof name !== "string")
  ) {
    return null;
  }
  return { projectId, path, name, template, formatVersion, headSeq, openedTsMs };
}

export function asHealthReport(value: unknown): HealthReport | null {
  if (!isRecord(value)) {
    return null;
  }
  const { status, apiSurfaceVersion, capabilityTraitVersion, formatVersion, openProjectCount } =
    value;
  if (
    typeof status !== "string" ||
    typeof apiSurfaceVersion !== "number" ||
    typeof capabilityTraitVersion !== "number" ||
    typeof formatVersion !== "number" ||
    typeof openProjectCount !== "number"
  ) {
    return null;
  }
  return { status, apiSurfaceVersion, capabilityTraitVersion, formatVersion, openProjectCount };
}
