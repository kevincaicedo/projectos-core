// The source-health hook (m1-s01): two bounded reads behind one refresh.
//
// Logic lives here rather than in the panel so the component stays a
// renderer (UI style §Components). Both reads go through the shared query
// hook, which is what makes all four async states structural instead of
// remembered.

import { useCallback, useMemo } from "react";
import { asEvidenceListReport, asSourceHealthReport } from "../api/evidence";
import { useApiQuery, type QueryView } from "../api/query";
import type { EvidenceRow, SourceHealthRow } from "../api/gen/api";

/// Dead-lettered items one card shows. The DLQ is a work list, not a log:
/// past a handful the answer is "something is systematically wrong", and the
/// bound is visible in the card rather than silently truncating (L8).
export const DEAD_LETTER_ROW_COUNT_MAX = 20;

export interface SourceHealthController {
  readonly health: QueryView<readonly SourceHealthRow[]>;
  readonly deadLetters: QueryView<readonly EvidenceRow[]>;
  readonly rowCountMax: number;
  readonly refresh: () => void;
}

export function useSourceHealth(projectPath: string | null): SourceHealthController {
  // A null path means no project is selected; the queries are skipped by
  // passing a name the hook never dispatches would be a lie, so instead the
  // panel renders its own teaching state and these stay unmounted.
  const healthInput = useMemo(
    () => (projectPath === null ? undefined : JSON.stringify({ path: projectPath })),
    [projectPath],
  );
  const deadLetterInput = useMemo(
    () =>
      projectPath === null
        ? undefined
        : JSON.stringify({
            path: projectPath,
            status: "failed",
            rowCountMax: DEAD_LETTER_ROW_COUNT_MAX,
            withStages: true,
          }),
    [projectPath],
  );

  const health = useApiQuery(
    "source.health",
    healthInput,
    (value) => asSourceHealthReport(value)?.sources ?? null,
    (rows) => rows.length === 0,
  );
  const deadLetters = useApiQuery(
    "evidence.list",
    deadLetterInput,
    (value) => asEvidenceListReport(value)?.evidence ?? null,
    (rows) => rows.length === 0,
  );

  const refresh = useCallback(() => {
    health.refetch();
    deadLetters.refetch();
  }, [health, deadLetters]);

  return {
    health: health.view,
    deadLetters: deadLetters.view,
    rowCountMax: DEAD_LETTER_ROW_COUNT_MAX,
    refresh,
  };
}
