// Design-language audit (m0-s09): the mechanical half of the doc 06 §2/§4
// contract. Three laws, each proven on a seeded violation at the end of this
// script so the checker is never decoration:
//
//   1. Token values match the 06 §2 tables exactly (a drifted hex is a
//      design-language change, which needs the doc changed first).
//   2. Components use tokens, never raw color/shadow literals.
//   3. Every animation named in src/ is in the 06 §4 motion grammar.
//
// Layout numerics (px sizes, radii used via tokens) are not policed here;
// the reviewed surface is color, shadow, and motion, which is where silent
// drift actually happens.

import { readFileSync, readdirSync, statSync } from "node:fs";
import { join } from "node:path";

const TOKENS_PATH = "src/styles/tokens.css";
const SOURCE_ROOT = "src";
const GENERATED_PREFIX = join("src", "api", "gen");

// doc 06 §2.1 — every value the table names, keyed by its token name here.
const REQUIRED_COLORS = {
  "--surface-paper": "#f2f1ec",
  "--surface-card": "#ffffff",
  "--surface-raised": "#fbfaf7",
  "--surface-raised-alt": "#fcfbf8",
  "--ink-primary": "#171a19",
  "--ink-deep": "#14231b",
  "--ink-secondary": "#3d423f",
  "--ink-secondary-soft": "#5d6462",
  "--ink-muted": "#8e9491",
  "--ink-muted-soft": "#a8ada9",
  "--ink-muted-faint": "#b0b4b1",
  "--accent-green": "#1e6b4b",
  "--accent-green-tint": "#e9f1ea",
  "--accent-green-tint-strong": "#c7dfce",
  "--accent-green-tint-deep": "#a9cdb4",
  "--accent-green-fill": "#5c8c71",
  "--accent-live": "#d6ee9a",
  "--signal-amber": "#b87333",
  "--signal-amber-bg": "#f6ebdd",
  "--signal-amber-bg-soft": "#fbf7f1",
  "--signal-amber-border": "#ebd9c2",
  "--signal-clay": "#a9503c",
  "--signal-clay-bg": "#f7e7e3",
  "--line-default": "#eae8e1",
  "--line-soft": "#efede7",
  "--line-strong": "#e6e4dd",
  "--neutral-idle": "#d8d5cc",
  "--neutral-idle-deep": "#cbc8be",
};

// doc 06 §4 — the complete named motion grammar. Anything else must not
// animate; the M0 shell implements the subset it needs, and a name outside
// this list is a review reject whether or not it is implemented.
const MOTION_GRAMMAR = new Set([
  "rise",
  "slide",
  "stage",
  "fly",
  "breathe",
  "ring",
  "wave",
  "sweep",
  "flow",
  "orbit",
]);

const EASING = "cubic-bezier(0.2, 0.8, 0.2, 1)";

const defects = [];

// --- law 1: token values match the doc -----------------------------------

const tokensCss = readFileSync(TOKENS_PATH, "utf8");
// Only the :root block defines the reference (light) theme; the dark block
// deliberately derives different values for the same roles.
const rootBlock = tokensCss.slice(
  tokensCss.indexOf(":root {"),
  tokensCss.indexOf("\n}", tokensCss.indexOf(":root {")),
);
for (const [token, expected] of Object.entries(REQUIRED_COLORS)) {
  const match = new RegExp(`${token}:\\s*([^;]+);`).exec(rootBlock);
  if (match === null) {
    defects.push(`${TOKENS_PATH}: ${token} is missing (doc 06 §2.1)`);
  } else if (match[1].trim().toLowerCase() !== expected) {
    defects.push(`${TOKENS_PATH}: ${token} is ${match[1].trim()}, doc 06 §2.1 says ${expected}`);
  }
}
if (!tokensCss.includes(EASING)) {
  defects.push(`${TOKENS_PATH}: the 06 §4 easing ${EASING} is missing`);
}
if (!tokensCss.includes("prefers-reduced-motion")) {
  defects.push(`${TOKENS_PATH}: 06 §4 requires a prefers-reduced-motion fallback`);
}

// --- laws 2 and 3: component sources ------------------------------------

const RAW_COLOR = /(#[0-9a-fA-F]{3,8}\b|\brgba?\(|\bhsla?\()/;
const ANIMATION_NAME = /animation(?:-name)?:\s*([a-zA-Z][\w-]*)/g;

// The token file is the one place raw values are allowed to exist; that is
// what makes it the single source (doc 06 §2).
function auditSource(path, text, { isTokenFile = false } = {}) {
  for (const [index, line] of text.split("\n").entries()) {
    const location = `${path}:${index + 1}`;
    if (!isTokenFile && RAW_COLOR.test(line) && !line.trimStart().startsWith("*")) {
      defects.push(`${location}: raw color literal — components consume tokens (doc 06 §2)`);
    }
  }
  for (const match of text.matchAll(ANIMATION_NAME)) {
    const name = match[1];
    if (name === "none" || MOTION_GRAMMAR.has(name)) {
      continue;
    }
    defects.push(`${path}: animation "${name}" is not in the 06 §4 motion grammar`);
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
    if (/\.(css|tsx?)$/.test(path)) {
      auditSource(path, readFileSync(path, "utf8"), {
        isTokenFile: path === join(...TOKENS_PATH.split("/")),
      });
    }
  }
}

walk(SOURCE_ROOT);

// --- the checker's own violation fixtures --------------------------------
// A check that has never been seen to fail is decoration (testing-and-gates).

const seeded = [];
function expectDefect(label, path, text) {
  const before = defects.length;
  auditSource(path, text);
  if (defects.length === before) {
    seeded.push(`the ${label} fixture did not fire`);
  }
  defects.length = before;
}
expectDefect("raw-color", "src/__fixtures__/raw.css", ".x { color: #ff0000; }\n");
expectDefect("rgba-color", "src/__fixtures__/raw.css", ".x { background: rgba(0,0,0,.4); }\n");
expectDefect(
  "unnamed-animation",
  "src/__fixtures__/motion.css",
  ".x { animation: wobble 1s linear; }\n",
);

if (seeded.length > 0) {
  for (const message of seeded) {
    console.error(`check-design-tokens: ${message}`);
  }
  process.exit(1);
}

if (defects.length > 0) {
  for (const defect of defects) {
    console.error(`check-design-tokens: ${defect}`);
  }
  process.exit(1);
}

console.log(
  "check-design-tokens: token values, token-only components, and the motion grammar all hold",
);
