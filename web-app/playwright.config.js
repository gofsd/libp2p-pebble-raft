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
    // Plain "chromium" launches "Chromium Headless Shell" (a stripped-down
    // binary) by default when headless. That variant doesn't forward
    // wasm-originated console.log calls (web_sys::console::log_1) to CDP's
    // Runtime.consoleAPICalled -- confirmed by a log_1 call at the very top
    // of app.rs's send_event never appearing even for rows that provably
    // executed it (their response came back). Plain JS console.log calls
    // (e.g. vite's own client script) forward fine either way. The
    // "chromium" channel forces the full Chrome-for-Testing binary instead.
    channel: "chromium",
  },
});
