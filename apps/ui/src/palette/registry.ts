// The command palette registry (m0-s09, F41): the keyboard-first spine every
// later feature registers into. Commands are data — the palette renders and
// filters this list; handlers are supplied by the shell composition, so the
// registry itself holds no view or domain state.

export interface PaletteCommand {
  readonly id: string;
  readonly title: string;
  /// Fixed vocabulary scope shown as the mono chip: project | run | job |
  /// shell — the domain noun the command acts on.
  readonly scope: string;
  readonly keybinding?: string;
  readonly handler: () => void;
}

export interface PaletteActions {
  readonly focusCreateProject: () => void;
  readonly focusOpenProject: () => void;
  readonly switchProject: (projectId: string) => void;
  readonly runEchoAgent: () => void;
  readonly cancelRun: () => void;
  readonly openRunFeed: () => void;
  readonly openJobList: () => void;
  readonly toggleTheme: () => void;
}

export interface SwitchTarget {
  readonly projectId: string;
  readonly label: string;
}

/// The registered v0 commands (milestone list). Run entries use the durable
/// m0-s12/s13 engine; the job entry remains an honest future seam until m0-s14.
export function paletteCommands(
  actions: PaletteActions,
  switchTargets: readonly SwitchTarget[],
): readonly PaletteCommand[] {
  const fixed: PaletteCommand[] = [
    {
      id: "project.create",
      title: "Create project",
      scope: "project",
      handler: actions.focusCreateProject,
    },
    {
      id: "project.open",
      title: "Open project",
      scope: "project",
      handler: actions.focusOpenProject,
    },
    {
      id: "run.echo",
      title: "Run echo agent",
      scope: "run",
      handler: actions.runEchoAgent,
    },
    {
      id: "run.cancel",
      title: "Cancel run",
      scope: "run",
      handler: actions.cancelRun,
    },
    {
      id: "run.feed",
      title: "Open run feed",
      scope: "run",
      handler: actions.openRunFeed,
    },
    {
      id: "job.list",
      title: "Open job list",
      scope: "job",
      handler: actions.openJobList,
    },
    {
      id: "shell.theme",
      title: "Toggle theme",
      scope: "shell",
      handler: actions.toggleTheme,
    },
  ];
  const switches: PaletteCommand[] = switchTargets.map((target) => ({
    id: `project.switch.${target.projectId}`,
    title: `Switch to ${target.label}`,
    scope: "project",
    handler: () => {
      actions.switchProject(target.projectId);
    },
  }));
  return [...fixed, ...switches];
}
