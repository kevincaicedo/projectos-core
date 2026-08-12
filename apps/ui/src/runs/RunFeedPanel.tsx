import type { OpenProjectRow } from "../api/gen/api";
import { CostTicker } from "./CostTicker";
import type { EchoRunController } from "./useEchoRun";

interface RunFeedPanelProps {
  readonly project: OpenProjectRow | null;
  readonly controller: EchoRunController;
}

export function RunFeedPanel({ project, controller }: RunFeedPanelProps) {
  const { view } = controller;
  if (view.state === "loading") {
    return (
      <section
        className="card run-feed"
        aria-labelledby="run-feed-title"
        data-run-feed-state="loading"
      >
        <Header />
        <p>Echo is entering the durable harness…</p>
      </section>
    );
  }
  if (view.state === "error") {
    return (
      <section
        className="card run-feed"
        aria-labelledby="run-feed-title"
        data-run-feed-state="error"
      >
        <Header />
        <p className="notice-clay">Run feed stopped: {view.error.message}</p>
        <button type="button" className="button-outline" onClick={controller.retry}>
          {view.error.retriable ? "Resume feed" : "Try again"}
        </button>
      </section>
    );
  }
  if (view.state === "empty") {
    return (
      <section
        className="card run-feed"
        aria-labelledby="run-feed-title"
        data-run-feed-state="empty"
      >
        <Header />
        <p>
          {project === null
            ? "Select a project to run Echo through the production harness."
            : "No active Run. Echo proves ledger-before-effect, local model policy, checkpoints, and replay."}
        </p>
        <button
          type="button"
          className="button-primary"
          disabled={project === null}
          onClick={() => void controller.start()}
        >
          Run Echo
        </button>
      </section>
    );
  }

  const terminal = view.data.frames.at(-1)?.terminal ?? false;
  return (
    <section
      className="card run-feed"
      aria-labelledby="run-feed-title"
      data-run-feed-state="success"
    >
      <Header />
      <div className="run-feed-meta">
        <span className="mono">Run {view.data.runId.slice(0, 12)}</span>
        <span className="live-chip" data-run-terminal={String(terminal)}>
          {terminal ? view.data.frames.at(-1)?.runStatus : "live"}
        </span>
      </div>
      <ol className="run-steps" aria-label="Durable Run steps">
        {view.data.frames.map((frame) => (
          <li key={frame.streamSeq} data-run-step={frame.streamSeq}>
            <span className="step-index mono">{frame.streamSeq}</span>
            <span>
              <strong>{frame.summary}</strong>
              <small className="mono">
                {frame.toolId ?? frame.phase} · checkpoint {frame.checkpointSeq}
              </small>
            </span>
          </li>
        ))}
      </ol>
      <CostTicker view={controller.cost} />
      {controller.controlError !== null && (
        <p className="notice-clay">Cancel was refused: {controller.controlError.message}</p>
      )}
      <button
        type="button"
        className="button-outline"
        disabled={terminal || controller.canceling}
        onClick={() => void controller.cancel()}
      >
        {controller.canceling ? "Cancel requested…" : terminal ? "Run ended" : "Cancel Run"}
      </button>
    </section>
  );
}

function Header() {
  return (
    <header className="run-feed-header">
      <div>
        <span className="micro-label">Agent activity</span>
        <h2 id="run-feed-title">Run feed</h2>
      </div>
      <span className="mono">durable · resumable</span>
    </header>
  );
}
