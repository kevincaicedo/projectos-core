import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

/// Where the in-page §18 measurements land for `pos-bench` to stamp into a
/// machine-identified artifact (m0-s16). They are written from inside the
/// page for a reason the m0-s09 finding records: a per-step Playwright call
/// measures the WebDriver channel, not the interaction.
const MEASUREMENTS_PATH = resolve(
  fileURLToPath(new URL("../e2e-artifacts/ui-measurements.json", import.meta.url)),
);

function recordMeasurements(entries: Record<string, readonly number[]>) {
  mkdirSync(dirname(MEASUREMENTS_PATH), { recursive: true });
  let existing: Record<string, unknown> = {};
  try {
    existing = JSON.parse(readFileSync(MEASUREMENTS_PATH, "utf8")) as Record<string, unknown>;
  } catch {
    existing = {};
  }
  writeFileSync(
    MEASUREMENTS_PATH,
    `${JSON.stringify({ ...existing, ...entries }, null, 2)}\n`,
    "utf8",
  );
}

/// A fifty-project workspace: the corpus the §18 cold-start row names.
const FIFTY_PROJECTS = JSON.stringify({
  projects: Array.from({ length: 50 }, (_unused, index) => ({
    projectId: index.toString(16).padStart(2, "0").repeat(16),
    path: `/tmp/cold-start-${index}.pos`,
    name: `Cold start ${index}`,
    template: "generic",
    formatVersion: 1,
    headSeq: 200,
    openedTsMs: 1_760_000_000_000 + index,
  })),
  openProjectCountMax: 64,
});

// The bytes below are produced by the real Rust runtime, not by this test:
// `just generate-snapshot-fixture` runs the `pos` shell and captures its
// stdout, and `just snapshot-fixture-check` fails CI when the committed copy
// drifts from what the runtime currently emits. So a UI that stops
// understanding the runtime's wire shape fails here rather than at a user.
const SNAPSHOT_FIXTURE = readFileSync(
  fileURLToPath(new URL("./fixtures/capability-snapshot.json", import.meta.url)),
  "utf8",
).trim();

const CAPABILITY_COUNT = 10;

/// Installs an IPC bridge that answers the registry from fixtures — the
/// desktop transport's shape, without a webview. `api_query` results are
/// keyed by query name so the shell's session surface has real bytes.
/// A query answer, or a sequence of them.
///
/// A sequence is what makes the m0-s09 reconciliation rule testable: after a
/// command the UI re-reads rather than patching local state, so a test that
/// wants to see an edit appear has to be able to answer the *second* read
/// differently. The last entry sticks, so a two-entry sequence means
/// "before, then ever after".
type QueryAnswer = string | readonly string[];

async function installIpc(
  page: import("@playwright/test").Page,
  queries: Record<string, QueryAnswer>,
  options: {
    readonly commands?: Record<string, string>;
    readonly streamFrames?: readonly string[];
  } = {},
) {
  await page.addInitScript(
    (fixtures) => {
      const reads = new Map<string, number>();
      let callbackId = 0;
      const callbacks = new Map<number, (message: unknown) => void>();
      Object.defineProperty(window, "__TAURI_INTERNALS__", {
        value: {
          transformCallback: (callback: (message: unknown) => void) => {
            callbackId += 1;
            callbacks.set(callbackId, callback);
            return callbackId;
          },
          unregisterCallback: (id: number) => callbacks.delete(id),
        },
      });
      Object.defineProperty(window, "__TAURI__", {
        value: {
          core: {
            invoke: (command: string, args?: Record<string, unknown>) => {
              if (command === "api_stream") {
                const channel = args?.channel as
                  { onmessage?: (message: string) => void } | undefined;
                if (channel?.onmessage === undefined) {
                  return Promise.reject(new Error("api_stream received no Channel"));
                }
                for (const [index, frame] of (fixtures.streamFrames ?? []).entries()) {
                  setTimeout(() => channel.onmessage?.(frame), 20 * (index + 1));
                }
                return Promise.resolve(null);
              }
              const name = String(args?.name ?? "");
              const answers: Record<string, string | readonly string[]> =
                command === "api_query" ? fixtures.queries : (fixtures.commands ?? {});
              const scripted = answers[name];
              let answer: string | undefined;
              if (Array.isArray(scripted)) {
                const seen = reads.get(name) ?? 0;
                answer = scripted[Math.min(seen, scripted.length - 1)];
                reads.set(name, seen + 1);
              } else if (typeof scripted === "string") {
                answer = scripted;
              }
              return answer === undefined
                ? Promise.reject(
                    JSON.stringify({
                      code: "unknown_query",
                      message: `no query is registered under the name "${name}"`,
                      retriable: false,
                    }),
                  )
                : Promise.resolve(answer);
            },
          },
        },
      });
    },
    { queries, ...options },
  );
}

const HEALTH = JSON.stringify({
  status: "ok",
  apiSurfaceVersion: 3,
  capabilityTraitVersion: 1,
  formatVersion: 1,
  openProjectCount: 1,
  backgroundWorkers: { running: true, registeredProjectCount: 1, lastError: null },
});

const EMPTY_LIST = JSON.stringify({ projects: [], openProjectCountMax: 64 });

/// One transcript-shaped recording, as `evidence.list` answers it (m1-s03).
const ONE_RECORDING = JSON.stringify({
  evidence: [
    {
      evidenceId: "a1b2c3d4e5f600000000000000000001",
      sourceId: "5c0a11e900000000000000000000beef",
      sourceKind: "upload",
      externalId: "interview-01.m4a",
      externalUrl: null,
      mediaKind: "audio",
      shape: "transcript",
      status: "chunked",
      canaryLevel: "clean",
      title: "Design partner interview",
      author: null,
      occurredTsMs: 1_772_946_000_000,
      byteSize: 43_844_726,
      chunkCount: 12,
      pass: 0,
      nextStage: "embed",
      nextStageOwnerStory: "m1-s04",
      nextStageAvailable: false,
      stages: [],
    },
  ],
  rowCountMax: 200,
});

function transcript(
  segment: { readonly speakerIndex: number; readonly text: string; readonly edited: boolean },
  speakers: readonly { readonly speakerIndex: number; readonly name: string }[],
): string {
  return JSON.stringify({
    evidenceId: "a1b2c3d4e5f600000000000000000001",
    pass: 0,
    segments: [
      {
        segmentIndex: 0,
        startMs: 0,
        endMs: 4_200,
        startsTurn: true,
        speakerIndex: segment.speakerIndex,
        text: segment.text,
        asrText: "the pricing page confused me",
        edited: segment.edited,
      },
      {
        segmentIndex: 1,
        startMs: 5_100,
        endMs: 8_000,
        startsTurn: true,
        speakerIndex: 0,
        text: "so I asked support twice",
        asrText: "so I asked support twice",
        edited: false,
      },
    ],
    speakers,
    rowCountMax: 200,
  });
}

const TRANSCRIPT_RAW = transcript(
  { speakerIndex: 0, text: "the pricing page confused me", edited: false },
  [],
);
const TRANSCRIPT_NAMED = transcript(
  { speakerIndex: 1, text: "the pricing page confused me", edited: false },
  [{ speakerIndex: 1, name: "Dana" }],
);
const TRANSCRIPT_CORRECTED = transcript(
  { speakerIndex: 1, text: "the pricing page confused me completely", edited: true },
  [{ speakerIndex: 1, name: "Dana" }],
);

const EDIT_OK = JSON.stringify({
  evidenceId: "a1b2c3d4e5f600000000000000000001",
  pass: 0,
  segmentIndex: 0,
});


const ONE_PROJECT = JSON.stringify({
  projects: [
    {
      projectId: "ab".repeat(16),
      path: "/tmp/demo.pos",
      name: "Demo Project",
      template: "generic",
      formatVersion: 1,
      headSeq: 7,
      openedTsMs: 1_760_000_000_000,
    },
  ],
  openProjectCountMax: 64,
});

const PROJECT_ID = "ab".repeat(16);
const RUN_ID = "cd".repeat(16);
const RUN_BUDGET = {
  tokens: 4_096,
  usdMicros: 0,
  wallMs: 90_000,
  storageBytes: 65_536,
  toolCalls: 3,
  retries: 0,
  steps: 3,
};
const RUN_REPORT = JSON.stringify({
  path: "/tmp/demo.pos",
  runId: RUN_ID,
  projectId: PROJECT_ID,
  worker: "echo",
  runtimeId: "projectos.native",
  executor: "device",
  status: "running",
  autonomyLevel: 2,
  committedStepCount: 0,
  checkpointedStepCount: 0,
  budget: RUN_BUDGET,
  spent: {
    tokens: 0,
    usdMicros: 0,
    wallMs: 0,
    storageBytes: 0,
    toolCalls: 0,
    retries: 0,
    steps: 0,
  },
  tainted: false,
  toolGrants: [
    { toolId: "echo.preflight", mode: "allow" },
    { toolId: "echo.complete", mode: "allow" },
    { toolId: "echo.report", mode: "allow" },
  ],
  parentRunId: null,
  lineageDepth: 0,
  pendingControl: null,
  pause: null,
});
const STEP_FRAMES = [
  stepFrame(1, "Local-only preflight passed", "echo.preflight", false),
  stepFrame(2, "Marker echoed by the fast model", "echo.complete", false),
  stepFrame(3, "Echo report stored and validated", "echo.report", true),
];
const SSE_FRAMES = STEP_FRAMES.map(
  (frame) => `id: ${frame.streamSeq}\nevent: run.step\ndata: ${JSON.stringify(frame)}\n\n`,
);
const COST_ROLLUP = JSON.stringify({
  scope: "project",
  projectCount: 1,
  rows: [
    {
      projectId: PROJECT_ID,
      feature: "echo",
      agent: "echo",
      provider: "openai_compatible",
      credentialClass: "device_session",
      model: "echo-fixture",
      providerCostKind: "customer_billed",
      calls: 1,
      tokensIn: 11,
      tokensOut: 4,
      wallMsTotal: 7,
      usdMicros: 0,
    },
  ],
  totals: { calls: 1, tokensIn: 11, tokensOut: 4, usdMicros: 0, projectosUsdMicros: 0 },
});

function stepFrame(streamSeq: number, summary: string, toolId: string, terminal: boolean) {
  return {
    runId: RUN_ID,
    projectId: PROJECT_ID,
    streamSeq,
    stepIndex: streamSeq - 1,
    phase: "checkpointed",
    summary,
    toolId,
    committedSeq: streamSeq * 3,
    checkpointSeq: streamSeq * 3 + 2,
    spent: { ...RUN_BUDGET, tokens: streamSeq === 1 ? 0 : 15, steps: streamSeq },
    runStatus: terminal ? "done" : "running",
    terminal,
    validationStatus: terminal ? "passed" : null,
  };
}

test.describe("walking skeleton without a server, an account, or the cloud repository", () => {
  test("the public bundle boots and reports an absent transport honestly", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "ProjectOS", level: 1 })).toBeVisible();

    // With no transport answering, both the panel and the capability view
    // render error-with-retry — never a card or a row carrying invented state.
    await expect(page.locator("[data-projects-state='error']")).toBeVisible();
    const error = page.locator("[data-registry-state='error']");
    await expect(error).toBeVisible();
    await expect(error.getByRole("button", { name: "Try again" })).toBeEnabled();
    await expect(page.locator("[data-capability-card]")).toHaveCount(0);
    await expect(page.locator("[data-project-row]")).toHaveCount(0);
  });

  test("live runtime bytes render as ten honest capability cards", async ({ page }) => {
    await installIpc(page, { "capability.snapshot": SNAPSHOT_FIXTURE, health: HEALTH });

    await page.goto("/");
    const ready = page.locator("[data-registry-state='ready']");
    await expect(ready).toBeVisible();
    await expect(page.locator("[data-capability-card]")).toHaveCount(CAPABILITY_COUNT);

    // The connector-host tick the runtime actually ran, rendered as the runtime
    // reported it (m0-s17 capability honesty).
    await expect(page.locator("[data-connector-host='available']")).toBeVisible();

    // Sockets the isolated bootstrap cannot offer say why, in the runtime's own
    // words. An empty or missing reason is a boundary-gate failure, not a
    // cosmetic one.
    const unavailable = page.locator("[data-capability-card='relay.ingress']");
    await expect(unavailable).toContainText("Unavailable:");
    await expect(unavailable).not.toContainText("Unavailable: .");

    await expect(page.locator("[data-capability-card='connector.host']")).toContainText(
      "LocalConnectorHost",
    );
  });

  test("a malformed runtime payload never renders as capability state", async ({ page }) => {
    await installIpc(page, {
      "capability.snapshot": '{"capabilities":[{"id":"not.a.capability"}]}',
    });

    await page.goto("/");
    await expect(page.locator("[data-registry-state='error']")).toBeVisible();
    await expect(page.locator("[data-capability-card]")).toHaveCount(0);
  });
});

/// Ingestion health with one dead-lettered item (m1-s01). Hand-built like
/// the project/health fixtures above — and drift-safe for the same reason
/// the validators exist: `asSourceHealthReport`/`asEvidenceListReport` narrow
/// into the *generated* ts-rs shapes, so a wire change the UI stops
/// understanding renders the error state and fails these assertions rather
/// than silently rendering a stale shape.
const SOURCE_HEALTH = JSON.stringify({
  sources: [
    {
      sourceId: "5c0a11e900000000000000000000beef",
      stage: "raw",
      okCount: 4,
      failedCount: 0,
      deadCount: 0,
      itemCount: 4,
      bytesTotal: 91_204,
      wallMsTotal: 0,
      lastSuccessTsMs: 1_760_000_000_000,
      lastFailureTsMs: null,
      lastErrorCode: null,
      costFeature: "ingest.raw",
    },
    {
      sourceId: "5c0a11e900000000000000000000beef",
      stage: "normalize",
      okCount: 3,
      failedCount: 4,
      deadCount: 1,
      itemCount: 3,
      bytesTotal: 68_403,
      wallMsTotal: 214,
      lastSuccessTsMs: 1_760_000_000_500,
      lastFailureTsMs: 1_760_000_001_000,
      lastErrorCode: "unreadable",
      costFeature: "ingest.normalize",
    },
  ],
});

const NO_SOURCE_HEALTH = JSON.stringify({ sources: [] });
const NO_DEAD_LETTERS = JSON.stringify({ evidence: [], rowCountMax: 20 });

const DEAD_LETTERS = JSON.stringify({
  evidence: [
    {
      evidenceId: "dead1e77e0000000000000000000f00d",
      sourceId: "5c0a11e900000000000000000000beef",
      sourceKind: "upload",
      externalId: "supplier-quote.pdf",
      externalUrl: null,
      mediaKind: "opaque",
      shape: "document",
      status: "failed",
      canaryLevel: "clean",
      title: "Supplier quote",
      author: "ops@example.test",
      occurredTsMs: 1_759_990_000_000,
      byteSize: 22_801,
      chunkCount: 0,
      pass: 0,
      nextStage: null,
      nextStageOwnerStory: null,
      nextStageAvailable: false,
      stages: [
        {
          stage: "normalize",
          state: "dead",
          pass: 0,
          attemptIndex: 4,
          wallMs: null,
          bytesRead: null,
          itemCount: null,
          lastErrorCode: "unreadable",
          lastErrorDetail: "content is not valid UTF-8 at byte offset 12",
        },
      ],
    },
  ],
  rowCountMax: 20,
});

test.describe("the M0 shell", () => {
  test("an empty session teaches instead of showing a blank panel", async ({ page }) => {
    await installIpc(page, {
      "project.list": EMPTY_LIST,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });
    await page.goto("/");

    const empty = page.locator("[data-projects-state='empty']");
    await expect(empty).toBeVisible();
    await expect(empty).toContainText("Create one from the stage");
    const teaching = page.locator("[data-teaching='workspace']");
    await expect(teaching).toBeVisible();
    // Teaching copy uses the fixed domain nouns, never synonyms.
    await expect(teaching).toContainText("Evidence");
    await expect(teaching).toContainText("Decision");
    await expect(page.locator("[data-panel-footer]")).toContainText("runtime ok");
  });

  test("project selection renders the project home from runtime bytes", async ({ page }) => {
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });
    await page.goto("/");

    const row = page.locator("[data-project-row]").first();
    await expect(row).toContainText("Demo Project");
    await row.click();
    const home = page.locator("[data-project-home]");
    await expect(home).toContainText("Demo Project");
    await expect(home).toContainText("head seq 7");
    await expect(page.locator("[data-teaching='project']")).toBeVisible();
  });

  /// m1-s01's DLQ criterion: a dead-lettered item shows its stage, its
  /// attempt count, and its typed reason — in the surface a human actually
  /// looks at. The L8 rule is that a dead item is never a silent drop, and a
  /// count alone is a silent drop with a number on it.
  test("a dead-lettered item names its stage, attempts, and typed reason", async ({ page }) => {
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
      "source.health": SOURCE_HEALTH,
      "evidence.list": DEAD_LETTERS,
    });
    await page.goto("/");
    await page.locator("[data-project-row]").first().click();

    // The per-source, per-stage table carries the counts...
    const normalize = page.locator("[data-stage-row='normalize']");
    await expect(normalize).toBeVisible();
    await expect(normalize.locator("[data-dead-count]")).toHaveAttribute("data-dead-count", "1");

    // ...and the dead-letter entry carries what a human needs to act.
    const item = page.locator("[data-dead-letter]").first();
    await expect(item).toBeVisible();
    await expect(item).toContainText("Supplier quote");
    await expect(item.locator("[data-dead-stage]")).toHaveAttribute("data-dead-stage", "normalize");
    await expect(item.locator("[data-dead-attempts]")).toHaveAttribute("data-dead-attempts", "4");
    await expect(item.locator("[data-dead-reason]")).toHaveAttribute(
      "data-dead-reason",
      "unreadable",
    );
    await expect(item).toContainText("4 attempts");
    await expect(item).toContainText("content is not valid UTF-8 at byte offset 12");
  });

  /// m1-s03's editable-transcript criterion, end to end: rename a speaker,
  /// fix a word, and the viewer re-renders from the runtime — with the model's
  /// original words still on screen beside the correction.
  ///
  /// The two reads are scripted as a sequence because the UI re-reads after
  /// every command rather than patching local state (m0-s09's reconciliation
  /// rule). A test that could pass against a locally mutated view would be
  /// testing the component, not the loop.
  test("a transcript edit is logged, re-rendered, and keeps the original ASR", async ({ page }) => {
    await installIpc(
      page,
      {
        "project.list": ONE_PROJECT,
        health: HEALTH,
        "capability.snapshot": SNAPSHOT_FIXTURE,
        "source.health": NO_SOURCE_HEALTH,
        "evidence.list": ONE_RECORDING,
        "transcript.get": [TRANSCRIPT_RAW, TRANSCRIPT_NAMED, TRANSCRIPT_CORRECTED],
      },
      { commands: { "transcript.speaker-name": EDIT_OK, "transcript.correct": EDIT_OK } },
    );
    await page.goto("/");
    await page.locator("[data-project-row]").first().click();
    await page.locator("[data-recording]").first().click();

    // The model's output, before anyone touches it. Nobody is attributed:
    // v1 detects the pause, not the person.
    const first = page.locator("[data-segment-index='0']");
    await expect(first).toBeVisible();
    await expect(first.locator("[data-segment-time]")).toHaveText("0:00");
    await expect(first.locator("[data-segment-speaker='0']")).toHaveText("Unattributed");
    await expect(first).toContainText("the pricing page confused me");
    await expect(first).toHaveAttribute("data-edited", "false");

    // Name the speaker, and the viewer shows the name the runtime now holds.
    await first.locator("[data-segment-speaker='0']").click();
    await first.locator("[data-speaker-input='0']").fill("Dana");
    await first.locator("[data-speaker-save='0']").click();
    await expect(first.locator("[data-segment-speaker='0']")).toHaveText("Dana");

    // Fix a word. The correction renders, and so does what the model heard —
    // "the raw ASR output stays immutable, edits project over it" is a thing
    // the user can see rather than a claim in a design record.
    await first.locator("[data-segment-text='0']").click();
    await first.locator("[data-segment-input='0']").fill("the pricing page confused me completely");
    await first.locator("[data-segment-save='0']").click();
    await expect(first).toHaveAttribute("data-edited", "true");
    await expect(first).toContainText("the pricing page confused me completely");
    await expect(first.locator("[data-segment-asr='0']")).toContainText(
      "model heard: the pricing page confused me",
    );
  });

  test("a project with no recordings teaches instead of showing an empty viewer", async ({
    page,
  }) => {
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
      "source.health": NO_SOURCE_HEALTH,
      "evidence.list": NO_DEAD_LETTERS,
    });
    await page.goto("/");
    await page.locator("[data-project-row]").first().click();
    await expect(page.locator("[data-transcript-recordings='empty']")).toContainText(
      "No recordings in this project yet",
    );
  });

  test("source health teaches when a project has ingested nothing", async ({ page }) => {
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
      "source.health": NO_SOURCE_HEALTH,
      "evidence.list": NO_DEAD_LETTERS,
    });
    await page.goto("/");
    await page.locator("[data-project-row]").first().click();

    const empty = page.locator("[data-stage-health='empty']");
    await expect(empty).toBeVisible();
    await expect(empty).toContainText("Evidence");
    await expect(page.locator("[data-dead-letters='empty']")).toContainText(
      "Nothing is dead-lettered",
    );
  });

  test("a refused source-health read renders the typed error with a retry", async ({ page }) => {
    // `source.health` is deliberately absent from the fixture registry, so
    // the IPC bridge answers exactly as the runtime does for an unknown
    // name: a typed envelope, which must reach the user rather than a blank.
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
      "evidence.list": NO_DEAD_LETTERS,
    });
    await page.goto("/");
    await page.locator("[data-project-row]").first().click();

    const failed = page.locator("[data-stage-health='error']");
    await expect(failed).toBeVisible();
    await expect(failed).toContainText("no query is registered");
    await expect(failed.getByRole("button", { name: "Try again" })).toBeVisible();
  });

  test("the palette opens, filters, switches projects, and stays under the p95 budget", async ({
    page,
  }) => {
    await installIpc(page, {
      "project.list": ONE_PROJECT,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });
    await page.goto("/");
    await expect(page.locator("[data-project-row]")).toHaveCount(1);

    // Behaviour first: the palette opens, and subsequence matching reaches
    // "Create project" from "cp".
    await page.keyboard.press("ControlOrMeta+k");
    await expect(page.locator("[data-palette]")).toBeVisible();
    await page.getByLabel("Search commands").fill("cp");
    await expect(page.locator("[data-command='project.create']")).toBeVisible();
    await page.keyboard.press("Escape");
    await expect(page.locator("[data-palette]")).toBeHidden();

    // §18 interaction gate: p95 < 100 ms for palette open and project
    // switch. The whole measurement runs INSIDE the page — one evaluate per
    // interaction kind — because a per-step Playwright round-trip would
    // measure the WebDriver channel rather than the interaction. Each sample
    // spans dispatching the real event to the DOM settling, so it is the
    // number a user experiences.
    const samples = await page.evaluate(async () => {
      const settled = (predicate: () => boolean) =>
        new Promise<number>((resolve) => {
          const started = performance.now();
          const check = () => {
            if (predicate()) {
              resolve(performance.now() - started);
            } else {
              requestAnimationFrame(check);
            }
          };
          check();
        });
      const paletteVisible = () => document.querySelector("[data-palette]") !== null;
      const pressPaletteKey = () => {
        window.dispatchEvent(
          new KeyboardEvent("keydown", { key: "k", metaKey: true, bubbles: true }),
        );
      };
      const open: number[] = [];
      const switched: number[] = [];

      for (let attempt = 0; attempt < 20; attempt += 1) {
        pressPaletteKey();
        open.push(await settled(paletteVisible));
        pressPaletteKey();
        await settled(() => !paletteVisible());
      }

      for (let attempt = 0; attempt < 20; attempt += 1) {
        pressPaletteKey();
        await settled(paletteVisible);
        const target = document.querySelector<HTMLElement>("[data-command^='project.switch.']");
        if (target === null) {
          throw new Error("the palette registered no project-switch command");
        }
        target.click();
        switched.push(await settled(() => document.querySelector("[data-project-home]") !== null));
        // Return to the workspace home so the next sample measures a real
        // switch rather than a no-op.
        document.querySelector<HTMLElement>(".rail-item[data-active='true']")?.click();
        await settled(() => document.querySelector("[data-project-home]") === null);
      }
      return { open, switched };
    });

    for (const [label, values] of [
      ["palette open", samples.open],
      ["project switch", samples.switched],
    ] as const) {
      const sorted = [...values].sort((left, right) => left - right);
      const p95 = sorted[Math.min(sorted.length - 1, Math.ceil(sorted.length * 0.95) - 1)] ?? 0;
      expect(p95, `${label} p95 (${p95.toFixed(1)}ms) exceeds the §18 100ms gate`).toBeLessThan(
        100,
      );
    }

    // The raw samples go to pos-bench, which stamps them with the machine
    // identity and protocol the §18 row requires. The gate assertion above
    // stays here so a regression fails CI whether or not a bench was run.
    recordMeasurements({ paletteOpenMs: samples.open, projectSwitchMs: samples.switched });
  });

  test("the app frame reaches interactive with fifty projects", async ({ page }) => {
    // "Interactive" is defined once, here: navigation start → the project
    // list is painted with resolved data. The observer is installed before
    // navigation, so the measurement spans the whole load rather than
    // whatever was left by the time a test could ask.
    await page.addInitScript(() => {
      const marked: { value: number | null } = { value: null };
      (window as unknown as { __posInteractiveMs: typeof marked }).__posInteractiveMs = marked;
      const ready = () => document.querySelector("[data-project-row]") !== null;
      const observer = new MutationObserver(() => {
        if (marked.value === null && ready()) {
          marked.value = performance.now();
          observer.disconnect();
        }
      });
      document.addEventListener("DOMContentLoaded", () => {
        observer.observe(document.body, { childList: true, subtree: true });
        if (ready()) {
          marked.value = performance.now();
          observer.disconnect();
        }
      });
    });
    await installIpc(page, {
      "project.list": FIFTY_PROJECTS,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });

    const samples: number[] = [];
    for (let attempt = 0; attempt < 5; attempt += 1) {
      await page.goto("/");
      await expect(page.locator("[data-project-row]")).toHaveCount(50);
      const interactive = await page.evaluate(
        () =>
          (window as unknown as { __posInteractiveMs: { value: number | null } }).__posInteractiveMs
            .value,
      );
      expect(interactive, "the interactive mark was never recorded").not.toBeNull();
      samples.push(interactive ?? 0);
    }
    recordMeasurements({ timeToInteractiveMs: samples });
  });

  test("Echo refuses visibly until a project is selected", async ({ page }) => {
    await installIpc(page, {
      "project.list": EMPTY_LIST,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });
    await page.goto("/");

    await page.keyboard.press("ControlOrMeta+k");
    await page.getByLabel("Search commands").fill("echo");
    await page.locator("[data-command='run.echo']").click();

    // A Run cannot float outside a project. The palette gives a visible
    // refusal without dispatching a malformed command.
    const notice = page.locator("[data-seam-notice='refused']");
    await expect(notice).toBeVisible();
    await expect(notice).toContainText("Select a project");
  });

  test("the Tauri Channel adapter renders durable Echo frames and ledger cost", async ({
    page,
  }) => {
    await installIpc(
      page,
      {
        "project.list": ONE_PROJECT,
        health: HEALTH,
        "capability.snapshot": SNAPSHOT_FIXTURE,
        "cost.rollup": COST_ROLLUP,
      },
      { commands: { "run.start": RUN_REPORT }, streamFrames: SSE_FRAMES },
    );
    await page.goto("/");
    await page.locator("[data-project-row]").click();
    await page.getByRole("button", { name: "Run Echo" }).click();

    await expect(page.locator("[data-run-feed-state='success']")).toBeVisible();
    await expect(page.locator("[data-run-step]")).toHaveCount(3);
    await expect(page.locator("[data-run-cost-state='success']")).toContainText(
      "echo@echo-fixture",
    );
    await expect(page.locator("[data-run-terminal='true']")).toContainText("done");
  });

  test("the HTTP stream adapter renders the same generated Run frames", async ({ page }) => {
    const calls: string[] = [];
    await page.route("**/api/**", async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      calls.push(`${request.method()} ${url.pathname}`);
      if (url.pathname === "/api/query/project.list") {
        await route.fulfill({ contentType: "application/json", body: ONE_PROJECT });
      } else if (url.pathname === "/api/query/health") {
        await route.fulfill({ contentType: "application/json", body: HEALTH });
      } else if (url.pathname === "/api/query/capability.snapshot") {
        await route.fulfill({ contentType: "application/json", body: SNAPSHOT_FIXTURE });
      } else if (url.pathname === "/api/query/cost.rollup") {
        await route.fulfill({ contentType: "application/json", body: COST_ROLLUP });
      } else if (url.pathname === "/api/cmd/run.start") {
        expect(request.postDataJSON()).toMatchObject({ path: "/tmp/demo.pos", worker: "echo" });
        await route.fulfill({ contentType: "application/json", body: RUN_REPORT });
      } else if (url.pathname === "/api/stream/run.steps") {
        expect(JSON.parse(url.searchParams.get("input") ?? "{}")).toEqual({
          path: "/tmp/demo.pos",
          runId: RUN_ID,
        });
        await route.fulfill({
          contentType: "text/event-stream",
          body: `retry: 2000\n\n${SSE_FRAMES.join("")}`,
        });
      } else {
        await route.fulfill({ status: 404, contentType: "application/json", body: "{}" });
      }
    });

    await page.goto("/");
    await page.locator("[data-project-row]").click();
    await page.getByRole("button", { name: "Run Echo" }).click();
    await expect(page.locator("[data-run-step]")).toHaveCount(3);
    await expect(page.locator("[data-run-cost-state='success']")).toContainText("1 model call");
    expect(calls).toContain("GET /api/stream/run.steps");
  });

  test("a truncated HTTP stream fails closed instead of inventing a clean end", async ({
    page,
  }) => {
    await page.route("**/api/**", async (route) => {
      const request = route.request();
      const url = new URL(request.url());
      if (url.pathname === "/api/query/project.list") {
        await route.fulfill({ contentType: "application/json", body: ONE_PROJECT });
      } else if (url.pathname === "/api/query/health") {
        await route.fulfill({ contentType: "application/json", body: HEALTH });
      } else if (url.pathname === "/api/query/capability.snapshot") {
        await route.fulfill({ contentType: "application/json", body: SNAPSHOT_FIXTURE });
      } else if (url.pathname === "/api/query/cost.rollup") {
        await route.fulfill({ contentType: "application/json", body: COST_ROLLUP });
      } else if (url.pathname === "/api/cmd/run.start") {
        await route.fulfill({ contentType: "application/json", body: RUN_REPORT });
      } else if (url.pathname === "/api/stream/run.steps") {
        await route.fulfill({
          contentType: "text/event-stream",
          body: SSE_FRAMES.join("").slice(0, -1),
        });
      } else {
        await route.fulfill({ status: 404, contentType: "application/json", body: "{}" });
      }
    });

    await page.goto("/");
    await page.locator("[data-project-row]").click();
    await page.getByRole("button", { name: "Run Echo" }).click();
    const error = page.locator("[data-run-feed-state='error']");
    await expect(error).toBeVisible();
    await expect(error).toContainText("ended inside an SSE frame");
  });

  test("the theme toggle swaps tokens without reloading", async ({ page }) => {
    await installIpc(page, {
      "project.list": EMPTY_LIST,
      health: HEALTH,
      "capability.snapshot": SNAPSHOT_FIXTURE,
    });
    await page.goto("/");

    const themeOf = () => page.evaluate(() => document.documentElement.dataset.theme);
    const before = await themeOf();
    await page.keyboard.press("ControlOrMeta+k");
    await page.getByLabel("Search commands").fill("theme");
    await page.locator("[data-command='shell.theme']").click();
    await expect.poll(themeOf).not.toBe(before);
  });
});
