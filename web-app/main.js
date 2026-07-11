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

const worker = new Worker(new URL("./worker.js", import.meta.url), { type: "module" });
const handle = new MainHandle(worker);

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
