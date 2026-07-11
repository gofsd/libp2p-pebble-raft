import { defineConfig } from "@playwright/test";

// Real-browser end-to-end tests for the wasm client (tests/set_get.spec.js):
// needs SharedArrayBuffer (cross-origin isolation -- see vite.config.js) and
// a running kvnode to actually dial (see that file's doc comment for how to
// point this at one). `webServer` builds the wasm bundle and serves it the
// same way `npm run dev` does, so `npx playwright test` is the only command
// needed once a kvnode is up.
export default defineConfig({
  testDir: "tests",
  timeout: 60_000,
  webServer: {
    command: "npm run dev -- --port 5183 --strictPort",
    url: "http://localhost:5183",
    reuseExistingServer: !process.env.CI,
  },
  use: {
    baseURL: "http://localhost:5183",
  },
});
