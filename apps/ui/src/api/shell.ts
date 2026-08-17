// The desktop shell surface (m0-s07), read through the same allowed
// platform branch as transport selection. On web every function here is a
// no-op returning honest emptiness — the browser has no native dialog and no
// app-config recents list, and pretending otherwise would be the L12 bug.

import { activeTransport } from "./transport";

export interface ShellRecents {
  readonly lastOpen: string | null;
  readonly recents: readonly string[];
  readonly recentProjectCountMax: number;
}

const EMPTY: ShellRecents = { lastOpen: null, recents: [], recentProjectCountMax: 0 };

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

/// The project this shell restores on launch, plus the recents list the menu
/// renders. Web returns empty: recents are desktop app config, never project
/// data (L4).
export async function shellRecents(): Promise<ShellRecents> {
  const invoke = desktopInvoke();
  if (invoke === null) {
    return EMPTY;
  }
  try {
    const raw = await invoke("shell_recents");
    if (typeof raw !== "string") {
      return EMPTY;
    }
    const value: unknown = JSON.parse(raw);
    if (!isRecord(value) || !Array.isArray(value.recents)) {
      return EMPTY;
    }
    return {
      lastOpen: typeof value.lastOpen === "string" ? value.lastOpen : null,
      recents: value.recents.filter((entry): entry is string => typeof entry === "string"),
      recentProjectCountMax:
        typeof value.recentProjectCountMax === "number" ? value.recentProjectCountMax : 0,
    };
  } catch {
    return EMPTY;
  }
}

/// Opens the native directory picker. `null` means "no native dialog here"
/// (web) or "the user cancelled" — the caller treats both as no-op, so the
/// web path degrades to the typed path field rather than to a broken button.
export async function pickProjectDirectory(purpose: "create" | "open"): Promise<string | null> {
  const dialog = desktopDialog();
  if (dialog === null) {
    return null;
  }
  try {
    const selected = await dialog.open({
      directory: true,
      multiple: false,
      title: purpose === "create" ? "Choose where to create the project" : "Open a .pos project",
    });
    return typeof selected === "string" ? selected : null;
  } catch {
    return null;
  }
}

/// Opens the native file picker for ingestion (m1-s07). Returns the chosen
/// paths — the desktop shell hands the core a path and the core streams the
/// file, so a four-gigabyte recording never passes through the webview.
/// Empty on web, where the browser has bytes rather than paths.
export async function pickFilesToIngest(): Promise<readonly string[]> {
  const dialog = desktopDialog();
  if (dialog === null) {
    return [];
  }
  try {
    const selected = await dialog.open({
      directory: false,
      multiple: true,
      title: "Choose recordings, notes, or transcripts to ingest",
    });
    if (typeof selected === "string") {
      return [selected];
    }
    if (Array.isArray(selected)) {
      return selected.filter((entry): entry is string => typeof entry === "string");
    }
    return [];
  } catch {
    return [];
  }
}

/// Subscribes to the native window's file drops. Returns an unsubscribe
/// function; a no-op on web, where the browser's own drop event carries the
/// bytes and `apiUpload` sends them.
export function onFilesDropped(handler: (paths: readonly string[]) => void): () => void {
  const listen = desktopEvent();
  if (listen === null) {
    return () => undefined;
  }
  let disposed = false;
  let unlisten: (() => void) | null = null;
  void listen<unknown>("tauri://drag-drop", (event) => {
    const payload = event.payload;
    if (!isRecord(payload) || !Array.isArray(payload.paths)) {
      return;
    }
    handler(payload.paths.filter((entry): entry is string => typeof entry === "string"));
  }).then((dispose) => {
    if (disposed) {
      dispose();
    } else {
      unlisten = dispose;
    }
  });
  return () => {
    disposed = true;
    unlisten?.();
  };
}

/// Subscribes to native menu selections so the menu and the palette drive
/// one code path. Returns an unsubscribe function; a no-op on web.
export function onShellCommand(handler: (id: string) => void): () => void {
  const listen = desktopEvent();
  if (listen === null) {
    return () => undefined;
  }
  let disposed = false;
  let unlisten: (() => void) | null = null;
  void listen<string>("shell://command", (event) => {
    handler(event.payload);
  }).then((dispose) => {
    if (disposed) {
      dispose();
    } else {
      unlisten = dispose;
    }
  });
  return () => {
    disposed = true;
    unlisten?.();
  };
}

export function isDesktopShell(): boolean {
  return activeTransport() === "tauri-ipc";
}

function desktopInvoke() {
  return window.__TAURI__?.core?.invoke ?? null;
}

function desktopDialog() {
  return window.__TAURI__?.dialog ?? null;
}

function desktopEvent() {
  return window.__TAURI__?.event?.listen ?? null;
}
