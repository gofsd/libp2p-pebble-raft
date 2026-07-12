// Real per-row raw-event driver for pkg/e2erun's web execution path (see
// that package's web.go) -- unlike set_get.spec.js's generic connect/set/get
// smoke check, this is called by the e2e pipeline with one specific
// recorded pkg/e2edata.Node's identity and rows, so a browser tab actually
// exercises the exact test data recorded for it, not a throwaway random
// identity.
//
// Env vars (WEB_IDENTITY_SEED_HEX/WEB_ROWS_JSON required; skipped entirely
// if either is unset, so `npx playwright test` alone -- with no e2e run in
// progress -- still passes):
//
//   WEB_IDENTITY_SEED_HEX -- 128 hex chars, this node's recorded private
//                            key (see pkg/e2edata.Node.PrivateKey's format)
//   WEB_ROWS_JSON         -- JSON array of event JSON strings, each in the
//                            same human-readable shape pkg/e2edata.Event /
//                            kvctl-cli sendevent use (event name, plain-text
//                            value) -- an "add" row's value is expected to
//                            already be resolved to a real leader address,
//                            not the "BOOTSTRAP" placeholder (see
//                            pkg/e2erun.ResolveBootstrapPlaceholder)
//   WEB_RESULTS_PATH       -- where to write the JSON array of
//                            {"index":N,"pass":bool,"error":"..."} results
//                            (defaults to test-results/e2e_results.json)
import { test } from "@playwright/test";
import { writeFileSync, mkdirSync } from "node:fs";
import { dirname } from "node:path";

const seedHex = process.env.WEB_IDENTITY_SEED_HEX;
const rowsJson = process.env.WEB_ROWS_JSON;
const resultsPath = process.env.WEB_RESULTS_PATH || "test-results/e2e_results.json";

// See test.setTimeout's doc comment inside the test body for why an "add"
// row alone can legitimately need most of this.
const ROW_TIMEOUT_MS = 200_000;

test.skip(
  !seedHex || !rowsJson,
  "WEB_IDENTITY_SEED_HEX/WEB_ROWS_JSON not set -- see this file's doc comment"
);

test("run pkg/e2edata rows against a real browser tab with a deterministic identity", async ({ page }) => {
  // Generous: the first row is normally a real raft learner join (an "add"
  // event), which alone is up to 4 sequential real network round trips --
  // a relay reservation plus 3 client-protocol calls (GetPrivateKey
  // bootstrap, SetKey, Add), see p2p.rs's RELAY_RESERVATION_TIMEOUT_MS/
  // CLIENT_PROTOCOL_TIMEOUT_MS (45s each) -- so this and ROW_TIMEOUT_MS
  // below need real headroom past playwright.config.js's default 60s, not
  // just "a bit longer": a real deploy target with a slow link can
  // legitimately spend tens of seconds on *each* of those steps.
  test.setTimeout(300_000);

  const rows = JSON.parse(rowsJson);

  // Printed live (not just collected for the final failure message) so a
  // row that hangs instead of erroring cleanly still leaves a trail --
  // console.error specifically covers an uncaught panic/rejection inside
  // the wasm worker, which wouldn't otherwise surface as a JS exception at
  // this page.evaluate call site at all.
  const consoleErrors = [];
  page.on("console", (msg) => {
    console.log(`[browser:${msg.type()}] ${msg.text()}`);
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });
  page.on("pageerror", (err) => console.log(`[browser:pageerror] ${err}`));

  await page.goto(`/?seed=${encodeURIComponent(seedHex)}`);
  // page.goto only waits for the page's load event, not for main.js's own
  // top-level `await init()` (wasm init) plus Worker construction to
  // finish -- both genuinely asynchronous, so `window.__kvE2E` isn't
  // guaranteed to exist the instant goto() resolves. Caught directly: the
  // very first row consistently failed with "Cannot read properties of
  // undefined (reading 'sendEvent')" without this wait.
  await page.waitForFunction(() => window.__kvE2E !== undefined);

  const results = [];
  for (let i = 0; i < rows.length; i++) {
    const eventJson = rows[i];
    let pass = false;
    let error;
    try {
      // A JS-level backstop in addition to shmevent's own per-call
      // timeouts on the Rust side (see p2p.rs's RELAY_RESERVATION_TIMEOUT_MS/
      // CLIENT_PROTOCOL_TIMEOUT_MS): if those somehow don't fire, this still
      // bounds each row so one stuck row doesn't burn this whole test's
      // budget and hide every other row's real result behind a generic
      // Playwright test-timeout instead of a per-row one. An "add" row can
      // legitimately take up to ~4x CLIENT_PROTOCOL_TIMEOUT_MS worst case
      // (see test.setTimeout's doc comment above), so this needs to be
      // comfortably past that, not just past one single call's own bound.
      const respJson = await Promise.race([
        page.evaluate((json) => window.__kvE2E.sendEvent(json), eventJson),
        new Promise((_, reject) => setTimeout(() => reject(new Error(`row timed out after ${ROW_TIMEOUT_MS / 1000}s (JS-level backstop)`)), ROW_TIMEOUT_MS)),
      ]);
      const resp = JSON.parse(respJson);
      if (resp.event === "error") {
        error = resp.value;
      } else {
        pass = true;
      }
    } catch (e) {
      error = e instanceof Error ? e.message : String(e);
    }
    console.log(`[row ${i}] ${eventJson} -> pass=${pass}${error ? ` error=${error}` : ""}`);
    results.push({ index: i, pass, error });
  }

  mkdirSync(dirname(resultsPath), { recursive: true });
  writeFileSync(resultsPath, JSON.stringify(results));

  const failures = results.filter((r) => !r.pass);
  if (failures.length > 0) {
    throw new Error(
      `${failures.length} of ${rows.length} row(s) failed -- see ${resultsPath}` +
        (consoleErrors.length > 0 ? `\nconsole errors:\n${consoleErrors.join("\n")}` : "")
    );
  }
});
