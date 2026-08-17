// The intake hook (m1-s07): the front door, in one place.
//
// Two shells, two physical realities, one command. The desktop shell has file
// *paths* and hands them to the core, which streams each file itself — a
// four-gigabyte recording never enters the webview. A browser has only the
// bytes and puts them on the wire. Both end at `ingest.submit`, so the report
// a person reads is the same either way (L12).
//
// Reconciliation follows the m0-s09 rule: dispatch, then tell the caller to
// re-read. Nothing here keeps a local copy of what the project now holds.

import { useCallback, useEffect, useState } from "react";
import { asIngestSubmitReport } from "../api/intake";
import { isDesktopShell, onFilesDropped, pickFilesToIngest } from "../api/shell";
import { apiCommand, apiUpload } from "../api/transport";
import type { ApiErrorEnvelope, IngestSubmitReport } from "../api/gen/api";

export interface IntakeController {
  readonly supported: boolean;
  readonly desktop: boolean;
  readonly busy: boolean;
  readonly report: IngestSubmitReport | null;
  readonly lastError: ApiErrorEnvelope | null;
  /// Opens the native picker (desktop) and ingests what was chosen.
  readonly choose: () => void;
  /// Ingests files a browser drop or file input produced.
  readonly submitFiles: (files: readonly File[]) => void;
  readonly dismiss: () => void;
}

export function useIntake(projectPath: string | null, onChanged: () => void): IntakeController {
  const [busy, setBusy] = useState(false);
  const [report, setReport] = useState<IngestSubmitReport | null>(null);
  const [lastError, setLastError] = useState<ApiErrorEnvelope | null>(null);
  const desktop = isDesktopShell();

  const record = useCallback(
    (outcomes: readonly IngestSubmitReport[], failure: ApiErrorEnvelope | null) => {
      setLastError(failure);
      setReport(outcomes.length === 0 ? null : mergeReports(outcomes));
      setBusy(false);
      if (outcomes.length > 0) {
        onChanged();
      }
    },
    [onChanged],
  );

  const submitPaths = useCallback(
    (paths: readonly string[]) => {
      if (projectPath === null || paths.length === 0) {
        return;
      }
      setBusy(true);
      setLastError(null);
      void dispatchAll(paths, (filePath) =>
        apiCommand("ingest.submit", JSON.stringify({ path: projectPath, filePath })),
      ).then(({ reports, failure }) => {
        record(reports, failure);
      });
    },
    [projectPath, record],
  );

  const submitFiles = useCallback(
    (files: readonly File[]) => {
      if (projectPath === null || files.length === 0) {
        return;
      }
      setBusy(true);
      setLastError(null);
      void dispatchAll(files, (file) =>
        apiUpload(
          "ingest.submit",
          JSON.stringify({ path: projectPath, fileName: file.name }),
          file,
        ),
      ).then(({ reports, failure }) => {
        record(reports, failure);
      });
    },
    [projectPath, record],
  );

  const choose = useCallback(() => {
    void pickFilesToIngest().then(submitPaths);
  }, [submitPaths]);

  // A native window drop carries paths, and only the desktop shell has one.
  useEffect(() => onFilesDropped(submitPaths), [submitPaths]);

  return {
    supported: projectPath !== null,
    desktop,
    busy,
    report,
    lastError,
    choose,
    submitFiles,
    dismiss: () => {
      setReport(null);
      setLastError(null);
    },
  };
}

/// Dispatches one call per item, in order, and keeps every report.
///
/// Sequential rather than concurrent on purpose: each call streams a whole
/// file into the CAS, and firing ten of those at once would multiply the
/// resident cost of an import by ten for no wall-clock win on a disk.
async function dispatchAll<T>(
  items: readonly T[],
  dispatch: (
    item: T,
  ) => Promise<
    | { readonly kind: "ok"; readonly value: unknown }
    | { readonly kind: "failed"; readonly error: ApiErrorEnvelope }
  >,
): Promise<{ reports: IngestSubmitReport[]; failure: ApiErrorEnvelope | null }> {
  const reports: IngestSubmitReport[] = [];
  let failure: ApiErrorEnvelope | null = null;
  for (const item of items) {
    const outcome = await dispatch(item);
    if (outcome.kind === "failed") {
      failure = outcome.error;
      continue;
    }
    const parsed = asIngestSubmitReport(outcome.value);
    if (parsed === null) {
      failure = {
        code: "malformed_result",
        message: "The ingest.submit result had a shape this build does not recognise.",
        retriable: false,
      };
      continue;
    }
    reports.push(parsed);
  }
  return { reports, failure };
}

/// Folds several single-file reports into the one summary a person reads.
/// The counts add; the per-file rows concatenate up to the bound the runtime
/// stated, and `truncated` carries forward so a trimmed list still says so.
function mergeReports(reports: readonly IngestSubmitReport[]): IngestSubmitReport {
  const merged: IngestSubmitReport = {
    sourceId: reports[0]?.sourceId ?? "",
    addedCount: 0,
    duplicateCount: 0,
    refusedCount: 0,
    skippedCount: 0,
    truncated: false,
    items: [],
    rowCountMax: reports[0]?.rowCountMax ?? 0,
    backgroundWorkersRunning: reports[0]?.backgroundWorkersRunning ?? false,
  };
  for (const report of reports) {
    merged.addedCount += report.addedCount;
    merged.duplicateCount += report.duplicateCount;
    merged.refusedCount += report.refusedCount;
    merged.skippedCount += report.skippedCount;
    merged.truncated ||= report.truncated;
    for (const item of report.items) {
      if (merged.items.length < merged.rowCountMax) {
        merged.items.push(item);
      } else {
        merged.truncated = true;
      }
    }
  }
  return merged;
}
