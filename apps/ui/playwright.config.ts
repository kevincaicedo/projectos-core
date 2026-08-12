import { defineConfig, devices } from "@playwright/test";
import { fileURLToPath } from "node:url";

// Walking-skeleton e2e (m0-s16 slice). It runs against the production bundle,
// on a machine with no ProjectOS server, no account, and no cloud submodule —
// which is precisely the `public-builds-alone` claim.
const PORT = 4173;
const VITE_BIN = fileURLToPath(new URL("./node_modules/vite/bin/vite.js", import.meta.url));
const VITE_PREVIEW = `${JSON.stringify(process.execPath)} ${JSON.stringify(VITE_BIN)}`;

export default defineConfig({
  testDir: "./e2e",
  fullyParallel: true,
  forbidOnly: Boolean(process.env.CI),
  retries: 0,
  reporter: process.env.CI === undefined ? "list" : "github",
  use: {
    baseURL: `http://127.0.0.1:${PORT}`,
    trace: "retain-on-failure",
  },
  projects: [{ name: "chromium", use: { ...devices["Desktop Chrome"] } }],
  webServer: {
    // The bind host is pinned to the polled URL's address: `localhost` can
    // resolve to ::1 only (Node ≥ 17 keeps OS ordering), which strands the
    // health check on 127.0.0.1 forever.
    // Reuse the Node executable that launched Playwright. This prevents a
    // login-shell PATH from silently selecting a different Node version for
    // the preview child.
    command: `${VITE_PREVIEW} preview --host 127.0.0.1 --port ${PORT} --strictPort`,
    url: `http://127.0.0.1:${PORT}`,
    reuseExistingServer: false,
    timeout: 60_000,
  },
});
