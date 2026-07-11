//! `kv-raft-web`: a real (non-voting) hashicorp/raft learner that runs in a
//! browser tab, compiled to `wasm32-unknown-unknown`. See `web-app/README.md`
//! for the architecture; module-level doc comments below cover each piece.

mod app;
pub mod fsm;
pub mod ipcproto;
pub mod learner;
pub mod msgpack;
pub mod p2p;
pub mod raft_wire;
mod shmring_ipc;
pub mod sqlite_store;
