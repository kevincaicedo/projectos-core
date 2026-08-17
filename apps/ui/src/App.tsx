// Shell composition (m0-s09): the doc 06 §3 grammar — rail, context panel,
// stage, dock placeholder — over the shared query hook and the command
// palette. View state only lives here (selection, palette, notices); domain
// state arrives by query and is reconciled by refetching, never forked.

import { useCallback, useEffect, useMemo, useRef, useState } from "react";
import { asHealthReport, asProjectListReport } from "./api/projects";
import { useApiQuery } from "./api/query";
import { onShellCommand, shellRecents } from "./api/shell";
import { apiCommand } from "./api/transport";
import { Palette } from "./palette/Palette";
import { paletteCommands, type PaletteActions } from "./palette/registry";
import { useEchoRun } from "./runs/useEchoRun";
import { useIntake } from "./screens/useIntake";
import { useSourceHealth } from "./screens/useSourceHealth";
import { useTranscript } from "./screens/useTranscript";
import { dispatchQueryNotice, runFeedNotice, type SeamNotice } from "./screens/seam";
import { HomeScreen } from "./screens/HomeScreen";
import { ContextPanel } from "./shell/ContextPanel";
import { Rail } from "./shell/Rail";
import { applyTheme, nextTheme, storedTheme, watchSystemTheme, type ThemeChoice } from "./theme";

export function App() {
  const [selectedProjectId, setSelectedProjectId] = useState<string | null>(null);
  const [paletteOpen, setPaletteOpen] = useState(false);
  const [notice, setNotice] = useState<SeamNotice | null>(null);
  const [focusToken, setFocusToken] = useState(0);
  const [theme, setTheme] = useState<ThemeChoice>(storedTheme);

  useEffect(() => {
    applyTheme(theme);
    return watchSystemTheme(() => {
      applyTheme(theme);
    });
  }, [theme]);

  const projects = useApiQuery(
    "project.list",
    undefined,
    (value) => asProjectListReport(value)?.projects ?? null,
    (rows) => rows.length === 0,
  );
  const health = useApiQuery("health", undefined, asHealthReport, () => false);

  const openRows = projects.view.state === "success" ? projects.view.data : [];
  const selected = openRows.find((row) => row.projectId === selectedProjectId) ?? null;
  const echoRun = useEchoRun(selected?.path ?? null);
  const sourceHealth = useSourceHealth(selected?.path ?? null);
  const transcript = useTranscript(selected?.path ?? null);

  const reconcile = useCallback(() => {
    projects.refetch();
    health.refetch();
  }, [projects, health]);
  // An import changes what the pipeline is working on, so the panels that
  // read that state re-read rather than keeping their own copy (L1).
  const reconcileIngest = useCallback(() => {
    sourceHealth.refresh();
    transcript.refresh();
    health.refetch();
  }, [sourceHealth, transcript, health]);
  const intake = useIntake(selected?.path ?? null, reconcileIngest);
  // The launch-restore effect runs once and must not re-subscribe whenever
  // the query hooks re-create their callbacks; a ref keeps it stable.
  const reconcileRef = useRef(reconcile);
  reconcileRef.current = reconcile;

  const actions: PaletteActions = useMemo(
    () => ({
      focusCreateProject: () => {
        setFocusToken((token) => token + 1);
      },
      focusOpenProject: () => {
        setFocusToken((token) => token + 1);
      },
      switchProject: setSelectedProjectId,
      runEchoAgent: () => {
        if (selected === null) {
          setNotice({
            kind: "refused",
            title: "Select a project",
            detail: "Echo needs one open project for its durable Run ledger.",
          });
          return;
        }
        setNotice(null);
        void echoRun.start();
      },
      cancelRun: () => {
        void echoRun.cancel();
      },
      openRunFeed: () => {
        setNotice(runFeedNotice());
      },
      openJobList: () => {
        void dispatchQueryNotice("job.list").then(setNotice);
      },
      toggleTheme: () => {
        setTheme(nextTheme);
      },
    }),
    [echoRun, selected],
  );

  const commands = useMemo(
    () =>
      paletteCommands(
        actions,
        openRows.map((row) => ({
          projectId: row.projectId,
          label: row.name ?? "Untitled Project",
        })),
      ),
    [actions, openRows],
  );

  useEffect(() => {
    const onKeyDown = (event: KeyboardEvent) => {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        setPaletteOpen((open) => !open);
      }
    };
    window.addEventListener("keydown", onKeyDown);
    return () => {
      window.removeEventListener("keydown", onKeyDown);
    };
  }, []);

  // Relaunch restore (m0-s07): the desktop shell remembers the last project
  // in its own app config; opening it goes through the same command any
  // other entry point uses, so a project that no longer opens surfaces its
  // typed error instead of a phantom selection. Web returns nothing here.
  useEffect(() => {
    void shellRecents().then((recents) => {
      if (recents.lastOpen === null) {
        return;
      }
      void apiCommand("project.open", JSON.stringify({ path: recents.lastOpen })).then(
        (outcome) => {
          if (outcome.kind === "ok") {
            reconcileRef.current();
          }
        },
      );
    });
  }, []);

  // Native menu selections drive the same handlers the palette does.
  useEffect(
    () =>
      onShellCommand((id) => {
        if (id === "shell.palette") {
          setPaletteOpen((open) => !open);
        } else if (id === "shell.theme") {
          setTheme(nextTheme);
        } else if (id === "project.create" || id === "project.open") {
          setFocusToken((token) => token + 1);
        }
      }),
    [],
  );

  return (
    <div className="shell">
      <Rail
        onHome={() => {
          setSelectedProjectId(null);
        }}
      />
      <ContextPanel
        projects={projects.view}
        health={health.view}
        selectedProjectId={selectedProjectId}
        onSelect={setSelectedProjectId}
        onRetry={reconcile}
      />
      <HomeScreen
        projects={projects.view}
        selected={selected}
        notice={notice}
        focusToken={focusToken}
        onChanged={reconcile}
        echoRun={echoRun}
        sourceHealth={sourceHealth}
        transcript={transcript}
        intake={intake}
      />
      <div className="dock-placeholder" title="The voice dock arrives with M2" aria-hidden="true">
        ◦
      </div>
      {paletteOpen && (
        <Palette
          commands={commands}
          onClose={() => {
            setPaletteOpen(false);
          }}
        />
      )}
    </div>
  );
}
