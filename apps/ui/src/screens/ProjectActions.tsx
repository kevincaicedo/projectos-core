// Create/open forms (m0-s09): the stage's project actions, dispatching the
// real registry commands and rendering their typed outcomes. On desktop the
// native dialogs (m0-s07) will feed the same commands; on web the path is a
// server placement path handed out at sign-in.

import { useRef, useState } from "react";
import { apiCommand, type DispatchOutcome } from "../api/transport";

interface ProjectActionsProps {
  /// Reconciliation: after a command, the shell re-reads the runtime.
  readonly onChanged: () => void;
  readonly focusToken: number;
}

export function ProjectActions({ onChanged, focusToken }: ProjectActionsProps) {
  const [path, setPath] = useState("");
  const [outcome, setOutcome] = useState<{ verb: string; result: DispatchOutcome } | null>(null);
  const inputRef = useRef<HTMLInputElement>(null);
  const lastFocusToken = useRef(focusToken);
  if (focusToken !== lastFocusToken.current) {
    lastFocusToken.current = focusToken;
    // The palette handed focus to this form; take it after paint.
    setTimeout(() => inputRef.current?.focus(), 0);
  }

  const dispatch = (verb: "project.create" | "project.open") => {
    const input = JSON.stringify({ path });
    void apiCommand(verb, input).then((result) => {
      setOutcome({ verb, result });
      if (result.kind === "ok") {
        onChanged();
      }
    });
  };

  return (
    <section className="card" aria-labelledby="project-actions-title">
      <h2 id="project-actions-title">Create or open a project</h2>
      <p className="mono">
        A project is a portable directory you own — copy it, back it up, leave.
      </p>
      <input
        ref={inputRef}
        className="text-input"
        data-project-path-input
        value={path}
        placeholder="Path to a .pos project directory"
        aria-label="Project directory path"
        onChange={(event) => {
          setPath(event.target.value);
        }}
      />
      <p>
        <button
          type="button"
          className="button-primary"
          disabled={path.length === 0}
          onClick={() => {
            dispatch("project.create");
          }}
        >
          Create
        </button>{" "}
        <button
          type="button"
          className="button-outline"
          disabled={path.length === 0}
          onClick={() => {
            dispatch("project.open");
          }}
        >
          Open
        </button>
      </p>
      {outcome !== null && <Outcome verb={outcome.verb} result={outcome.result} />}
    </section>
  );
}

function Outcome({ verb, result }: { verb: string; result: DispatchOutcome }) {
  if (result.kind === "failed") {
    return (
      <p className="notice-clay" data-project-action-outcome="error">
        {verb} was refused: {result.error.message}
      </p>
    );
  }
  return (
    <p className="notice-amber" data-project-action-outcome="ok">
      {verb} completed; the session list below is re-read from the runtime.
    </p>
  );
}
