import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { expect, test } from "@playwright/test";

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
async function installIpc(
  page: import("@playwright/test").Page,
  queries: Record<string, string>,
  options: {
    readonly commands?: Record<string, string>;
    readonly streamFrames?: readonly string[];
  } = {},
) {
  await page.addInitScript(
    (fixtures) => {
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
              const answers =
                command === "api_query" ? fixtures.queries : (fixtures.commands ?? {});
              const answer = answers[name];
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
});

const EMPTY_LIST = JSON.stringify({ projects: [], openProjectCountMax: 64 });

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
