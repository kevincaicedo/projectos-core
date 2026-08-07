// The shared query hook (m0-s09): every user-visible async surface renders
// exactly four states — loading, empty, error-with-retry, success (STYLE).
// Components do not hand-roll fetch state; they pass a validator (unknown →
// T | null) and an emptiness predicate, and render the returned shape.
//
// Reconciliation rule: after any command, callers `refetch()` — the runtime
// is re-read rather than locally mutated, so the UI can never fork truth
// (L1 reaches the browser).

import { useCallback, useEffect, useRef, useState } from "react";
import type { ApiErrorEnvelope } from "./gen/capabilities";
import { apiQuery } from "./transport";

export type QueryView<T> =
  | { readonly state: "loading" }
  | { readonly state: "empty" }
  | { readonly state: "error"; readonly error: ApiErrorEnvelope }
  | { readonly state: "success"; readonly data: T };

export interface UseApiQueryResult<T> {
  readonly view: QueryView<T>;
  readonly refetch: () => void;
}

export function useApiQuery<T>(
  name: string,
  inputJson: string | undefined,
  validate: (value: unknown) => T | null,
  isEmpty: (data: T) => boolean,
): UseApiQueryResult<T> {
  const [view, setView] = useState<QueryView<T>>({ state: "loading" });
  // Monotonic request id: a slow first response must not overwrite a newer
  // retry's result (visible state follows the user's last ask, not timing).
  const latestRequest = useRef(0);
  const validateRef = useRef(validate);
  validateRef.current = validate;
  const isEmptyRef = useRef(isEmpty);
  isEmptyRef.current = isEmpty;

  const refetch = useCallback(() => {
    latestRequest.current += 1;
    const request = latestRequest.current;
    setView({ state: "loading" });
    void apiQuery(name, inputJson).then((outcome) => {
      if (latestRequest.current !== request) {
        return;
      }
      if (outcome.kind === "failed") {
        setView({ state: "error", error: outcome.error });
        return;
      }
      const data = validateRef.current(outcome.value);
      if (data === null) {
        setView({
          state: "error",
          error: {
            code: "malformed_result",
            message: `The ${name} result had a shape this build does not recognise.`,
            retriable: false,
          },
        });
        return;
      }
      setView(isEmptyRef.current(data) ? { state: "empty" } : { state: "success", data });
    });
  }, [name, inputJson]);

  useEffect(refetch, [refetch]);

  return { view, refetch };
}
