// Echo Run view controller. Durable frames and cost rows remain runtime
// truth; the hook stores only the current subscription, cursor, and rendered
// view state. Reconnect always resumes from the last validated stream id.

import { useCallback, useEffect, useRef, useState } from "react";
import type {
  ApiErrorEnvelope,
  CostRollupReport,
  RunStartInput,
  RunStepFrame,
} from "../api/gen/api";
import { ECHO_BUDGET, asCostRollupReport, asRunReport, asRunStepFrame } from "../api/runs";
import { apiCommand, apiQuery, apiStream, type StreamSubscription } from "../api/transport";
import type { QueryView } from "../api/query";

export interface RunFeedData {
  readonly runId: string;
  readonly frames: readonly RunStepFrame[];
}

export interface EchoRunController {
  readonly view: QueryView<RunFeedData>;
  readonly cost: QueryView<CostRollupReport>;
  readonly canceling: boolean;
  readonly controlError: ApiErrorEnvelope | null;
  readonly start: () => Promise<void>;
  readonly cancel: () => Promise<void>;
  readonly retry: () => void;
}

export function useEchoRun(path: string | null): EchoRunController {
  const [view, setView] = useState<QueryView<RunFeedData>>({ state: "empty" });
  const [cost, setCost] = useState<QueryView<CostRollupReport>>({ state: "empty" });
  const [canceling, setCanceling] = useState(false);
  const [controlError, setControlError] = useState<ApiErrorEnvelope | null>(null);
  const generation = useRef(0);
  const runId = useRef<string | null>(null);
  const frames = useRef<RunStepFrame[]>([]);
  const cursor = useRef<number | null>(null);
  const terminal = useRef(false);
  const costRequest = useRef(0);
  const subscription = useRef<StreamSubscription | null>(null);

  useEffect(() => {
    generation.current += 1;
    subscription.current?.close();
    subscription.current = null;
    runId.current = null;
    frames.current = [];
    cursor.current = null;
    terminal.current = false;
    costRequest.current += 1;
    setView({ state: "empty" });
    setCost({ state: "empty" });
    setCanceling(false);
    setControlError(null);
  }, [path]);

  const fail = useCallback((error: ApiErrorEnvelope, activeGeneration: number) => {
    if (generation.current !== activeGeneration) {
      return;
    }
    subscription.current?.close();
    subscription.current = null;
    setView({ state: "error", error });
  }, []);

  const refreshCost = useCallback(async (activePath: string, activeGeneration: number) => {
    const request = ++costRequest.current;
    setCost({ state: "loading" });
    const outcome = await apiQuery("cost.rollup", JSON.stringify({ path: activePath }));
    if (generation.current !== activeGeneration || costRequest.current !== request) {
      return;
    }
    if (outcome.kind === "failed") {
      setCost({ state: "error", error: outcome.error });
      return;
    }
    const report = asCostRollupReport(outcome.value);
    if (report === null) {
      setCost({ state: "error", error: malformed("cost.rollup") });
    } else {
      setCost(report.rows.length === 0 ? { state: "empty" } : { state: "success", data: report });
    }
  }, []);

  const subscribe = useCallback(
    (activePath: string, activeRunId: string, activeGeneration: number) => {
      subscription.current?.close();
      subscription.current = apiStream(
        "run.steps",
        JSON.stringify({ path: activePath, runId: activeRunId }),
        cursor.current,
        {
          onMessage: (message) => {
            if (generation.current !== activeGeneration) {
              return;
            }
            const parsed = parseFrame(message.data);
            const expected = (cursor.current ?? 0) + 1;
            if (
              message.event !== "run.step" ||
              message.id === null ||
              parsed === null ||
              parsed.runId !== activeRunId ||
              parsed.streamSeq !== message.id ||
              parsed.streamSeq !== expected
            ) {
              fail(malformed("run.steps"), activeGeneration);
              return;
            }
            cursor.current = parsed.streamSeq;
            frames.current = [...frames.current, parsed];
            terminal.current = parsed.terminal;
            setView({ state: "success", data: { runId: activeRunId, frames: frames.current } });
            void refreshCost(activePath, activeGeneration);
            if (parsed.terminal) {
              subscription.current?.close();
              subscription.current = null;
            }
          },
          onError: (error) => {
            fail(error, activeGeneration);
          },
          onEnd: () => {
            if (generation.current === activeGeneration && !terminal.current) {
              fail(
                {
                  code: "stream_ended",
                  message: "The Run feed ended before a terminal checkpoint. Resume to continue.",
                  retriable: true,
                },
                activeGeneration,
              );
            }
          },
        },
      );
    },
    [fail, refreshCost],
  );

  const start = useCallback(async () => {
    if (path === null) {
      setView({ state: "error", error: noProject() });
      return;
    }
    const activeGeneration = ++generation.current;
    subscription.current?.close();
    frames.current = [];
    cursor.current = null;
    terminal.current = false;
    runId.current = null;
    setControlError(null);
    setView({ state: "loading" });
    setCost({ state: "loading" });
    const input: RunStartInput = {
      path,
      worker: "echo",
      autonomyLevel: 2,
      budget: ECHO_BUDGET,
      toolGrants: [],
      parentRunId: null,
    };
    const outcome = await apiCommand("run.start", JSON.stringify(input));
    if (generation.current !== activeGeneration) {
      return;
    }
    if (outcome.kind === "failed") {
      fail(outcome.error, activeGeneration);
      return;
    }
    const report = asRunReport(outcome.value);
    if (report === null || report.worker !== "echo") {
      fail(malformed("run.start"), activeGeneration);
      return;
    }
    runId.current = report.runId;
    subscribe(path, report.runId, activeGeneration);
  }, [fail, path, subscribe]);

  const cancel = useCallback(async () => {
    const activeRunId = runId.current;
    if (path === null || activeRunId === null || terminal.current || canceling) {
      return;
    }
    const activeGeneration = generation.current;
    setCanceling(true);
    setControlError(null);
    const outcome = await apiCommand(
      "run.cancel",
      JSON.stringify({ path, runId: activeRunId, reason: "Canceled from the Run feed" }),
    );
    if (generation.current === activeGeneration) {
      setCanceling(false);
      if (outcome.kind === "failed") {
        setControlError(outcome.error);
      }
    }
  }, [canceling, path]);

  const retry = useCallback(() => {
    if (path === null || runId.current === null) {
      void start();
      return;
    }
    const activeGeneration = ++generation.current;
    terminal.current = false;
    setView({ state: "loading" });
    subscribe(path, runId.current, activeGeneration);
  }, [path, start, subscribe]);

  return { view, cost, canceling, controlError, start, cancel, retry };
}

function parseFrame(data: string): RunStepFrame | null {
  try {
    return asRunStepFrame(JSON.parse(data) as unknown);
  } catch {
    return null;
  }
}

function malformed(surface: string): ApiErrorEnvelope {
  return {
    code: "malformed_result",
    message: `The ${surface} result had a shape this build does not recognise.`,
    retriable: false,
  };
}

function noProject(): ApiErrorEnvelope {
  return {
    code: "project_required",
    message: "Select an open project before starting Echo.",
    retriable: false,
  };
}
