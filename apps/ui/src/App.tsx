// Shell composition (m0-s09): the doc 06 §3 grammar — rail, context panel,
// stage, dock placeholder — over the shared query hook and the command
// palette. View state only lives here (selection, palette, notices); domain
// state arrives by query and is reconciled by refetching, never forked.

import { useCallback, useEffect, useMemo, useState } from "react";
import { asHealthReport, asProjectListReport } from "./api/projects";
import { useApiQuery } from "./api/query";
import { Palette } from "./palette/Palette";
import { paletteCommands, type PaletteActions } from "./palette/registry";
import {
  dispatchCommandNotice,
  dispatchQueryNotice,
  runFeedNotice,
  type SeamNotice,
} from "./screens/seam";
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

  const reconcile = useCallback(() => {
    projects.refetch();
    health.refetch();
  }, [projects, health]);

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
        void dispatchCommandNotice("run.start").then(setNotice);
      },
      cancelRun: () => {
        void dispatchCommandNotice("run.cancel").then(setNotice);
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
    [],
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
