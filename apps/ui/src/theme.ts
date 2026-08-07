// Theme selection (m0-s09): dark/light derive from the same token roles
// (doc 06 §2.1). `system` follows prefers-color-scheme; the manual choice
// persists per browser. This is view state — nothing here touches domain
// truth.

export type ThemeChoice = "system" | "light" | "dark";

const STORAGE_KEY = "pos.theme";

export function storedTheme(): ThemeChoice {
  const stored = window.localStorage.getItem(STORAGE_KEY);
  return stored === "light" || stored === "dark" ? stored : "system";
}

export function applyTheme(choice: ThemeChoice): void {
  const dark =
    choice === "dark" ||
    (choice === "system" && window.matchMedia("(prefers-color-scheme: dark)").matches);
  document.documentElement.setAttribute("data-theme", dark ? "dark" : "light");
}

/// Rotates system → dark → light → system, persisting the manual choice.
export function nextTheme(current: ThemeChoice): ThemeChoice {
  const next: ThemeChoice = current === "system" ? "dark" : current === "dark" ? "light" : "system";
  if (next === "system") {
    window.localStorage.removeItem(STORAGE_KEY);
  } else {
    window.localStorage.setItem(STORAGE_KEY, next);
  }
  return next;
}

export function watchSystemTheme(onChange: () => void): () => void {
  const media = window.matchMedia("(prefers-color-scheme: dark)");
  media.addEventListener("change", onChange);
  return () => {
    media.removeEventListener("change", onChange);
  };
}
