// worker.js is this project's in-tab counterpart to mobile/kvmobile.go: it
// holds all daemon-side state (the libp2p node, the raft learner, SQLite
// storage) inside app.rs's worker_main, which answers requests routed from
// the main thread over shmring_ipc::serve -- exactly like kvmobile's
// Start/Submit/Get answer pkg/ipc requests from Android's MainActivity.
//
// This loads its own, independent wasm module instance (shmring's
// SharedArrayBuffer-backed rings coordinate the two instances via real
// Atomics, not shared Rust-level state -- see shmring's README and
// shmring_ipc.rs's doc comment), so main.js's `init()` and this one are
// deliberately two separate calls, not a shared import.
import init, { worker_main, worker_main_with_seed } from "./pkg/kv_raft_web.js";

// Playwright's page.on("console") does not capture console messages from a
// dedicated Worker's own realm -- confirmed by a log_1 call at the very top
// of a Worker-reached function never appearing even when that function
// provably ran to completion (its response made it back to the main
// thread). __debugLog relays Worker-side diagnostics to the main thread
// (main.js forwards them to console.log there, which *is* captured) via
// plain postMessage -- distinct from shmring_ipc's own postMessage
// SharedArrayBuffer handoff, which only reacts to SharedArrayBuffer
// payloads and ignores anything else (see shmring_ipc.rs's onmessage).
self.__debugLog = (msg) => {
  try {
    self.postMessage({ __debug: true, msg });
  } catch {
    // best-effort diagnostics only
  }
};
await init();

// A "seed" query param on *this worker script's own URL* (set by main.js,
// itself reading it from the page's URL -- see main.js's doc comment)
// picks a deterministic identity instead of a freshly random one, the same
// property mobile/kvmobile's identitySeedHex ldflag and
// pkg/e2edata.WriteDesktopKeyFile give desktop/Android: the e2e test
// pipeline needs a browser tab to reliably come up as one specific
// recorded pkg/e2edata.Node, not a new random peer id every run. Absent
// (the normal case for a human using the demo page), behavior is
// unchanged: a fresh random identity every load.
const seed = new URLSearchParams(self.location.search).get("seed");
if (seed) {
  await worker_main_with_seed(seed);
} else {
  await worker_main();
}

// worker_main/worker_main_with_seed only resolve once run_worker has
// installed shmring_ipc::serve's onmessage handler -- before that, a
// message posted via MainChannel::call (main.js's `worker.postMessage`) is
// genuinely lost, not queued: confirmed directly by adding an "onmessage
// fired" log as the very first line of that handler and seeing it never
// appear for a request sent immediately after `new Worker(...)`, even
// though the exact same handler reliably received later requests sent
// after this point. main.js gates window.__kvE2E (and, in turn, every
// MainHandle call the e2e pipeline or UI might make) on this signal so no
// caller can race the handler's installation.
self.postMessage({ __ready: true });
