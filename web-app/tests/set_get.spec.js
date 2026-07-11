// Real end-to-end coverage for this crate's whole reason for existing:
// Connect (join as a real raft non-voter), Set, and Get against a genuine
// running kvnode -- cross-checked against `mage get` on the Go side would
// additionally prove the raft replication itself, not just this tab's own
// round trip, but that's a manual step (see README.md's "Running it")
// rather than something this file can drive without shelling out to the
// Go toolchain.
//
// Needs a `kvnode` already running with a browser-reachable WebTransport
// listener (every node has one automatically -- see pkg/daemon.newHost)
// and a relay-capable node it can reserve a circuit through (a leader
// started with `-relay-service`, or any node with Config.RelayPeer set to
// one -- see pkg/daemon.Config.RelayPeer's doc comment). Point this test at
// it with:
//
//   KVNODE_MULTIADDR='/ip4/127.0.0.1/udp/4001/quic-v1/webtransport/certhash/.../p2p/12D3Koo...' \
//     npx playwright test
//
// (find the address via `cat ~/.libp2p-kv-raft/registry.json`, same as
// README.md's "Running it" section). Skipped entirely if that's unset, so
// `npx playwright test` still passes in CI/dev environments with no
// cluster running -- there is no way to fake a real raft learner join
// without one.
import { test, expect } from "@playwright/test";

const nodeMultiaddr = process.env.KVNODE_MULTIADDR;

test.skip(!nodeMultiaddr, "KVNODE_MULTIADDR not set -- see this file's doc comment");

test("connect, set, and get a key/value pair through a real raft learner join", async ({ page }) => {
  const consoleErrors = [];
  page.on("console", (msg) => {
    if (msg.type() === "error") consoleErrors.push(msg.text());
  });

  await page.goto("/");

  await page.fill("#nodeAddr", nodeMultiaddr);
  await page.click("#connectButton");
  await expect(page.locator("#status")).toHaveText(/^connected as 12D3Koo/, { timeout: 30_000 });

  const key = `pw-test-${Date.now()}`;
  const value = `value-${Math.random().toString(36).slice(2)}`;

  await page.fill("#keyInput", key);
  await page.fill("#valueInput", value);
  await page.click("#setButton");
  await expect(page.locator("#result")).toHaveText(`set ${key} = ${value}`, { timeout: 15_000 });

  // Get may briefly lag Set -- this tab's own raft learner has to actually
  // receive and apply the AppendEntries carrying it first (see
  // learner.rs's Get doc comment) -- so retry for a few seconds rather
  // than asserting on the first read.
  await expect(async () => {
    await page.click("#getButton");
    await expect(page.locator("#result")).toHaveText(`${key} = ${value}`);
  }).toPass({ timeout: 15_000 });

  expect(consoleErrors, `unexpected console errors: ${consoleErrors.join("\n")}`).toEqual([]);
});
