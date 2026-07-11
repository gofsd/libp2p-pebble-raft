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
import init, { worker_main } from "./pkg/kv_raft_web.js";

await init();
await worker_main();
