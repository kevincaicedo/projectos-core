// The source settings card (m1-s01): per-source, per-stage ingestion health,
// and the dead-letter list that names the stage, the attempt, and the typed
// reason for every item the pipeline could not finish.
//
// The DLQ half is the point (L8): a dead item is never a silent drop, so it
// has to be visible somewhere a human looks — with enough detail to act on
// rather than a count.

import type { EvidenceRow, SourceHealthRow } from "../api/gen/api";
import type { QueryView } from "../api/query";
import { deadStage } from "../api/evidence";
import type { SourceHealthController } from "./useSourceHealth";

interface SourceHealthPanelProps {
  readonly projectSelected: boolean;
  readonly controller: SourceHealthController;
}

export function SourceHealthPanel({ projectSelected, controller }: SourceHealthPanelProps) {
  if (!projectSelected) {
    return (
      <section className="card" data-source-health="no-project">
        <Header />
        <div className="teaching">
          <p>Select a project to see what its sources have ingested.</p>
        </div>
      </section>
    );
  }
  return (
    <section className="card" data-source-health={controller.health.state}>
      <Header />
      <StageHealth view={controller.health} onRetry={controller.refresh} />
      <DeadLetters
        view={controller.deadLetters}
        rowCountMax={controller.rowCountMax}
        onRetry={controller.refresh}
      />
    </section>
  );
}

function Header() {
  return (
    <div className="run-feed-header">
      <h2 id="source-health-title">Source health</h2>
      <span className="micro-label">per source · per stage</span>
    </div>
  );
}

function StageHealth({
  view,
  onRetry,
}: {
  readonly view: QueryView<readonly SourceHealthRow[]>;
  readonly onRetry: () => void;
}) {
  if (view.state === "loading") {
    return <p data-stage-health="loading">Reading ingestion health…</p>;
  }
  if (view.state === "error") {
    return (
      <div data-stage-health="error">
        <p className="notice-clay">Source health stopped: {view.error.message}</p>
        <button type="button" className="button-outline" onClick={onRetry}>
          Try again
        </button>
      </div>
    );
  }
  if (view.state === "empty") {
    return (
      <div className="teaching" data-stage-health="empty">
        <p>
          No Evidence has entered this project yet. Every item that arrives is streamed into the
          content store and then moves stage by stage — normalize, chunk, embed, extract, index —
          with its progress recorded here.
        </p>
      </div>
    );
  }
  return (
    <table className="stage-health" data-stage-health="success">
      <thead>
        <tr>
          <th scope="col">Source</th>
          <th scope="col">Stage</th>
          <th scope="col">Done</th>
          <th scope="col">Failed</th>
          <th scope="col">Dead</th>
          <th scope="col">Last error</th>
        </tr>
      </thead>
      <tbody>
        {view.data.map((row) => (
          <tr key={`${row.sourceId}:${row.stage}`} data-stage-row={row.stage}>
            <td className="mono">{row.sourceId.slice(0, 8)}</td>
            <td>{row.stage}</td>
            <td>{row.okCount}</td>
            <td>{row.failedCount}</td>
            <td data-dead-count={row.deadCount}>{row.deadCount}</td>
            <td className="mono">{row.lastErrorCode ?? "—"}</td>
          </tr>
        ))}
      </tbody>
    </table>
  );
}

function DeadLetters({
  view,
  rowCountMax,
  onRetry,
}: {
  readonly view: QueryView<readonly EvidenceRow[]>;
  readonly rowCountMax: number;
  readonly onRetry: () => void;
}) {
  if (view.state === "loading") {
    return <p data-dead-letters="loading">Checking for items the pipeline could not finish…</p>;
  }
  if (view.state === "error") {
    return (
      <div data-dead-letters="error">
        <p className="notice-clay">Dead-letter list stopped: {view.error.message}</p>
        <button type="button" className="button-outline" onClick={onRetry}>
          Try again
        </button>
      </div>
    );
  }
  if (view.state === "empty") {
    return (
      <p className="micro-label" data-dead-letters="empty">
        Nothing is dead-lettered.
      </p>
    );
  }
  return (
    <div data-dead-letters="success">
      <h3 className="micro-label">
        Dead-lettered ({view.data.length}
        {view.data.length >= rowCountMax ? ` of at least ${rowCountMax}` : ""})
      </h3>
      <ul className="dead-letters">
        {view.data.map((row) => (
          <DeadLetterItem key={row.evidenceId} row={row} />
        ))}
      </ul>
    </div>
  );
}

function DeadLetterItem({ row }: { readonly row: EvidenceRow }) {
  const stage = deadStage(row);
  return (
    <li className="notice-clay" data-dead-letter={row.evidenceId}>
      <strong>{row.title ?? row.externalId}</strong>{" "}
      <span className="mono">{row.evidenceId.slice(0, 8)}</span>
      {stage === undefined ? (
        <p>This item is failed but no stage is dead-lettered — rebuild projections to re-derive.</p>
      ) : (
        <p>
          Stopped at <strong data-dead-stage={stage.stage}>{stage.stage}</strong> after{" "}
          <span data-dead-attempts={stage.attemptIndex}>
            {stage.attemptIndex} {stage.attemptIndex === 1 ? "attempt" : "attempts"}
          </span>
          :{" "}
          <span className="mono" data-dead-reason={stage.lastErrorCode ?? "unknown"}>
            {stage.lastErrorCode ?? "unknown"}
          </span>
          {stage.lastErrorDetail !== null && ` — ${stage.lastErrorDetail}`}
        </p>
      )}
    </li>
  );
}
