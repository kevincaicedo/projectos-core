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

test.describe("walking skeleton without a server, an account, or the cloud repository", () => {
  test("the public bundle boots and reports an absent transport honestly", async ({ page }) => {
    await page.goto("/");
    await expect(page.getByRole("heading", { name: "ProjectOS", level: 1 })).toBeVisible();

    // No pos-server exists yet (m0-s08), so the honest rendering is the error
    // state with a retry — never a capability card carrying invented state.
    const error = page.locator("[data-registry-state='error']");
    await expect(error).toBeVisible();
    await expect(error.getByRole("button", { name: "Try again" })).toBeEnabled();
    await expect(page.locator("[data-capability-card]")).toHaveCount(0);
  });

  test("live runtime bytes render as ten honest capability cards", async ({ page }) => {
    await page.addInitScript((snapshot: string) => {
      Object.defineProperty(window, "__TAURI__", {
        value: {
          core: {
            invoke: (command: string) =>
              command === "api_query"
                ? Promise.resolve(snapshot)
                : Promise.reject(new Error(`unexpected command ${command}`)),
          },
        },
      });
    }, SNAPSHOT_FIXTURE);

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
    await page.addInitScript(() => {
      Object.defineProperty(window, "__TAURI__", {
        value: {
          core: {
            invoke: () => Promise.resolve('{"capabilities":[{"id":"not.a.capability"}]}'),
          },
        },
      });
    });

    await page.goto("/");
    await expect(page.locator("[data-registry-state='error']")).toBeVisible();
    await expect(page.locator("[data-capability-card]")).toHaveCount(0);
  });
});
