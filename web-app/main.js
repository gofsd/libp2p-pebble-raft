// main.js is this project's UI, playing the same role MainActivity.kt plays
// for the Android app: a thin wrapper that disables Set/Get until the
// underlying node is up, then drives it purely through MainHandle
// (app.rs's wasm-bindgen export of shmring_ipc.MainChannel). Unlike
// Android's build-time-baked leaderMultiaddr (a phone has no operator to
// type an address at run time -- see kvmobile.go's doc comment), the node
// multiaddr here is entered at run time, matching desktop's `mage
// addfollower <addr>` more than the phone build: a browser tab, like a
// desktop operator, can just be given the address directly.
//
// This is also this crate's worked example of driving it purely from JS:
// `new Worker("./worker.js", { type: "module" })` plus `new MainHandle(worker)`
// is the entire integration surface -- everything else (libp2p, the raft
// learner, SQLite storage, shmring IPC) lives in Rust and is reached only
// through MainHandle's connect/set/get.
import init, { MainHandle } from "./pkg/kv_raft_web.js";

await init();

// A "seed" query param on *this page's* URL (e.g. "/?seed=<128 hex chars>")
// is forwarded to worker.js's own URL, which uses it to pick a
// deterministic identity -- see worker.js's doc comment. Absent, behavior
// is unchanged from before this existed: worker.js falls back to a fresh
// random identity.
const seed = new URLSearchParams(window.location.search).get("seed");
const workerUrl = new URL("./worker.js", import.meta.url);
if (seed) workerUrl.searchParams.set("seed", seed);
const worker = new Worker(workerUrl, { type: "module" });
// Relayed Worker-side diagnostics (see worker.js's __debugLog doc comment)
// -- an addEventListener listener, so it coexists with MainChannel's own
// property-based worker.onmessage handler rather than replacing it.
worker.addEventListener("message", (e) => {
  if (e.data && e.data.__debug) console.log(`[worker] ${e.data.msg}`);
});
// An uncaught exception inside the Worker (e.g. a bad FFI call) fires here
// rather than bubbling to the page's own pageerror -- without this it would
// otherwise die silently with no trace in Playwright's captured output.
worker.addEventListener("error", (e) => {
  console.error(`[worker error] ${e.message} (${e.filename}:${e.lineno})`);
});
const handle = new MainHandle(worker);

// A message sent via MainChannel::call (this crate's Worker.postMessage
// request handoff) before worker.js's shmring_ipc::serve handler has
// actually been installed is lost outright, not queued -- confirmed
// directly while debugging the e2e pipeline's "add" row hanging forever:
// the very first request, sent immediately after `new Worker(...)`, never
// reached the Worker's onmessage handler at all, even though the identical
// handler reliably received every later request. worker.js posts
// `{__ready: true}` right after worker_main/worker_main_with_seed resolves
// (i.e. once that handler is installed), so anything that can call into
// `handle` -- window.__kvE2E below, or the UI's own button handlers if a
// human is fast enough to matter -- waits for it first.
const workerReady = new Promise((resolve) => {
  const onReady = (e) => {
    if (e.data && e.data.__ready) {
      worker.removeEventListener("message", onReady);
      resolve();
    }
  };
  worker.addEventListener("message", onReady);
});
await workerReady;

// Exposed for the e2e pipeline's Playwright driver (tests/e2e.spec.js) to
// call directly via page.evaluate, bypassing the UI entirely -- the same
// "drive the Go/Kotlin bindings directly, not through simulated clicks"
// approach android-app's E2ETest.kt uses. Not used by the UI below.
window.__kvE2E = { handle, sendEvent: (json) => handle.send_event(json) };

const nodeAddrInput = document.querySelector("#nodeAddr");
const connectButton = document.querySelector("#connectButton");
const statusEl = document.querySelector("#status");
const keyInput = document.querySelector("#keyInput");
const valueInput = document.querySelector("#valueInput");
const setButton = document.querySelector("#setButton");
const getButton = document.querySelector("#getButton");
const resultEl = document.querySelector("#result");

connectButton.addEventListener("click", () => {
  void (async () => {
    connectButton.disabled = true;
    statusEl.textContent = "connecting…";
    try {
      const peerId = await handle.connect(nodeAddrInput.value.trim());
      statusEl.textContent = `connected as ${peerId}`;
      setButton.disabled = false;
      getButton.disabled = false;
    } catch (err) {
      statusEl.textContent = `failed to connect: ${err}`;
    } finally {
      connectButton.disabled = false;
    }
  })();
});

setButton.addEventListener("click", () => {
  void (async () => {
    const key = keyInput.value;
    const value = valueInput.value;
    setButton.disabled = true;
    try {
      await handle.set(key, value);
      resultEl.textContent = `set ${key} = ${value}`;
    } catch (err) {
      resultEl.textContent = `set failed: ${err}`;
    } finally {
      setButton.disabled = false;
    }
  })();
});

getButton.addEventListener("click", () => {
  void (async () => {
    const key = keyInput.value;
    getButton.disabled = true;
    try {
      const value = await handle.get(key);
      resultEl.textContent = `${key} = ${value}`;
    } catch (err) {
      resultEl.textContent = `get failed: ${err}`;
    } finally {
      getButton.disabled = false;
    }
  })();
});
