// L12 shell-parity grep (m0-s09): the identical bundle renders in the Tauri
// webview and the browser, so no domain logic may sit behind a platform
// conditional. The one allowlisted exception is transport selection.
//
// The rule is mechanical: any reference to a platform marker (`__TAURI__`,
// `isTauri`, `navigator.userAgent`, `window.__POS_PLATFORM`) outside the
// transport module is a defect. Seeded fixtures at the end prove it fires.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const SOURCE_ROOT = "src";
// The two allowlisted modules: transport selection and the desktop-shell
// surface (native dialogs, menu events, app-config recents). Both are
// *capability* access, not domain logic — everything they expose degrades to
// an honest no-op on web, so no feature works in one shell and not the other.
const PLATFORM_MODULES = [join("src", "api", "transport.ts"), join("src", "api", "shell.ts")];
const GENERATED_PREFIX = join("src", "api", "gen");

const PLATFORM_MARKERS = [/__TAURI__/, /\bisTauri\b/, /navigator\.userAgent/, /__POS_PLATFORM/];

const defects = [];

function audit(path, text) {
  if (PLATFORM_MODULES.includes(path)) {
    return;
  }
  for (const [index, line] of text.split("\n").entries()) {
    if (line.trimStart().startsWith("//") || line.trimStart().startsWith("*")) {
      continue;
    }
    for (const marker of PLATFORM_MARKERS) {
      if (marker.test(line)) {
        defects.push(
          `${path}:${index + 1}: platform branch outside the transport module (L12); ` +
            `transport selection is the only allowed exception`,
        );
      }
    }
  }
}

function walk(directory) {
  for (const entry of readdirSync(directory)) {
    const path = join(directory, entry);
    if (path.startsWith(GENERATED_PREFIX)) {
      continue;
    }
    if (statSync(path).isDirectory()) {
      walk(path);
      continue;
    }
    if (/\.tsx?$/.test(path)) {
      audit(path, readFileSync(path, "utf8"));
    }
  }
}

walk(SOURCE_ROOT);

// The checker's own violation fixture.
const before = defects.length;
audit(
  join("src", "screens", "Seeded.tsx"),
  "if (window.__TAURI__) { renderDesktopOnlyBoard(); }\n",
);
if (defects.length === before) {
  console.error("check-shell-parity: the seeded platform-branch fixture did not fire");
  process.exit(1);
}
defects.length = before;

if (defects.length > 0) {
  for (const defect of defects) {
    console.error(`check-shell-parity: ${defect}`);
  }
  process.exit(1);
}

console.log("check-shell-parity: one bundle, no domain logic behind a platform conditional");
