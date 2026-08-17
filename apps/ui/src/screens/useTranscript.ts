// The transcript hook (m1-s03): one bounded read and three edits.
//
// Logic lives here rather than in the panel so the component stays a renderer
// (UI style §Components). Every edit follows the m0-s09 reconciliation rule —
// dispatch the command, then re-read — so an edit that the runtime refused
// can never linger on screen as if it had applied (L1 reaches the browser).

import { useCallback, useMemo, useState } from "react";
import { asEvidenceListReport } from "../api/evidence";
import { asTranscriptReport } from "../api/transcript";
import { useApiQuery, type QueryView } from "../api/query";
import { apiCommand } from "../api/transport";
import type { ApiErrorEnvelope, EvidenceRow, TranscriptReport } from "../api/gen/api";

/// Segments one page of the viewer holds. A ninety-minute interview is
/// thousands; the bound is visible in the panel rather than silently
/// truncating (L8). Paging through the rest is m1-s12's viewer.
export const TRANSCRIPT_ROW_COUNT_MAX = 200;

export interface TranscriptController {
  /// Recordings in this project, which is what the picker offers. Filtered to
  /// transcript-shaped items: a CSV has no turns to read.
  readonly recordings: QueryView<readonly EvidenceRow[]>;
  readonly selectedEvidenceId: string | null;
  readonly select: (evidenceId: string | null) => void;
  readonly view: QueryView<TranscriptReport>;
  readonly pending: boolean;
  readonly lastError: ApiErrorEnvelope | null;
  readonly refresh: () => void;
  readonly correct: (segmentIndex: number, text: string) => void;
  readonly nameSpeaker: (speakerIndex: number, name: string) => void;
  readonly assignSpeaker: (segmentIndex: number, speakerIndex: number) => void;
}

export function useTranscript(projectPath: string | null): TranscriptController {
  const [pending, setPending] = useState(false);
  const [lastError, setLastError] = useState<ApiErrorEnvelope | null>(null);
  const [evidenceId, setEvidenceId] = useState<string | null>(null);

  const recordingsInput = useMemo(
    () =>
      projectPath === null
        ? undefined
        : JSON.stringify({
            path: projectPath,
            rowCountMax: TRANSCRIPT_ROW_COUNT_MAX,
            withStages: false,
          }),
    [projectPath],
  );
  const recordings = useApiQuery(
    "evidence.list",
    recordingsInput,
    (value) =>
      asEvidenceListReport(value)?.evidence.filter((row) => row.shape === "transcript") ?? null,
    (rows) => rows.length === 0,
    recordingsInput === undefined,
  );

  const input = useMemo(
    () =>
      projectPath === null || evidenceId === null
        ? undefined
        : JSON.stringify({
            path: projectPath,
            evidenceId,
            rowCountMax: TRANSCRIPT_ROW_COUNT_MAX,
          }),
    [projectPath, evidenceId],
  );

  const transcript = useApiQuery(
    "transcript.get",
    input,
    (value) => asTranscriptReport(value),
    (report) => report.segments.length === 0,
    input === undefined,
  );
  const refetch = transcript.refetch;

  const dispatch = useCallback(
    (name: string, payload: Record<string, unknown>) => {
      if (projectPath === null || evidenceId === null) {
        return;
      }
      setPending(true);
      setLastError(null);
      void apiCommand(name, JSON.stringify({ path: projectPath, evidenceId, ...payload })).then(
        (outcome) => {
          setPending(false);
          if (outcome.kind === "failed") {
            setLastError(outcome.error);
            return;
          }
          // Re-read rather than patch: the runtime owns the transcript, and a
          // locally applied edit would be a second source of truth.
          refetch();
        },
      );
    },
    [projectPath, evidenceId, refetch],
  );

  const pass = transcript.view.state === "success" ? transcript.view.data.pass : 0;

  return {
    recordings: recordings.view,
    selectedEvidenceId: evidenceId,
    select: setEvidenceId,
    view: transcript.view,
    pending,
    lastError,
    refresh: useCallback(() => {
      recordings.refetch();
      refetch();
    }, [recordings, refetch]),
    correct: useCallback(
      (segmentIndex: number, text: string) => {
        dispatch("transcript.correct", { pass, segmentIndex, text });
      },
      [dispatch, pass],
    ),
    nameSpeaker: useCallback(
      (speakerIndex: number, name: string) => {
        dispatch("transcript.speaker-name", { speakerIndex, name });
      },
      [dispatch],
    ),
    assignSpeaker: useCallback(
      (segmentIndex: number, speakerIndex: number) => {
        dispatch("transcript.speaker-assign", { pass, segmentIndex, speakerIndex });
      },
      [dispatch, pass],
    ),
  };
}
