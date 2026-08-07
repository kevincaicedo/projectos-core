// The context panel (doc 06 §3): the home screen's panel projection is the
// open-project list with a mono micro-label header, a count, and a live
// footer line reading real runtime health.

import type { HealthReport, OpenProjectRow } from "../api/gen/api";
import type { QueryView } from "../api/query";

interface ContextPanelProps {
  readonly projects: QueryView<readonly OpenProjectRow[]>;
  readonly health: QueryView<HealthReport>;
  readonly selectedProjectId: string | null;
  readonly onSelect: (projectId: string) => void;
  readonly onRetry: () => void;
}

export function ContextPanel({
  projects,
  health,
  selectedProjectId,
  onSelect,
  onRetry,
}: ContextPanelProps) {
  return (
    <aside className="context-panel" aria-label="Open projects">
      <span className="micro-label">
        Projects{projects.state === "success" ? ` · ${projects.data.length}` : ""}
      </span>
      <PanelBody
        projects={projects}
        selectedProjectId={selectedProjectId}
        onSelect={onSelect}
        onRetry={onRetry}
      />
      <footer className="panel-footer" data-panel-footer>
        {health.state === "success"
          ? `runtime ok · surface v${health.data.apiSurfaceVersion} · ${health.data.openProjectCount} open`
          : health.state === "error"
            ? "runtime unreachable"
            : "…"}
      </footer>
    </aside>
  );
}

function PanelBody({
  projects,
  selectedProjectId,
  onSelect,
  onRetry,
}: Omit<ContextPanelProps, "health">) {
  if (projects.state === "loading") {
    return <p data-projects-state="loading">Reading the session…</p>;
  }
  if (projects.state === "error") {
    return (
      <div data-projects-state="error">
        <p>{projects.error.message}</p>
        <button type="button" className="button-outline" onClick={onRetry}>
          Try again
        </button>
      </div>
    );
  }
  if (projects.state === "empty") {
    return (
      <p className="teaching" data-projects-state="empty">
        No project is open in this session. Create one from the stage, or press ⌘K and type
        “create”.
      </p>
    );
  }
  return (
    <div data-projects-state="ready">
      {projects.data.map((row) => (
        <button
          key={row.projectId}
          type="button"
          className="panel-row"
          data-active={row.projectId === selectedProjectId}
          data-project-row={row.projectId}
          onClick={() => {
            onSelect(row.projectId);
          }}
        >
          <div>{row.name ?? "Untitled Project"}</div>
          <div className="mono">
            {row.template} · seq {row.headSeq}
          </div>
        </button>
      ))}
    </div>
  );
}
