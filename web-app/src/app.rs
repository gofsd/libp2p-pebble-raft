//! Wires [`crate::p2p::Node`]/[`crate::p2p::Handle`] +
//! [`crate::learner::Learner`] + [`crate::shmring_ipc`] together into the
//! two `wasm-bindgen` entry points a page actually loads: [`worker_main`]
//! (run inside the Worker that owns this tab's daemon-equivalent state,
//! mirroring `mobile/kvmobile.go`'s Start/Submit/Get answering `pkg/ipc`
//! requests from Android's `MainActivity`) and [`MainHandle`] (run on the
//! main thread, driving the Worker exclusively through
//! [`shmring_ipc::MainChannel`], the same shape Android's UI drives its
//! in-process daemon through `pkg/ipc.Call`).
//!
//! `ActionAdd` is repurposed from "join the raft cluster as a voter" (what
//! it means everywhere else) to "connect to the target node and join as a
//! non-voting learner" -- this crate's whole reason for existing is that a
//! browser tab *can* be a real (if non-voting) raft member once it holds a
//! relay reservation (see `p2p.rs`'s doc comment), unlike the thin-client-
//! only design this replaces. `Key` carries the target node's multiaddr,
//! the same Key-carries-a-multiaddr convention `mobile/kvmobile.go`'s
//! `Start` already relies on for its own build-time-baked leader address.
//!
//! `WorkerState`'s `RefCell` is only ever borrowed for the instant it
//! takes to clone a [`crate::p2p::Handle`] (cheap: a peer id plus two
//! channel senders) or read/write `learner`/`leader` -- never held across
//! an `.await` -- so it can't contend with `Node::run`'s own event loop;
//! see `p2p.rs`'s "Task ownership" doc comment for why that distinction
//! matters here.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::rc::Rc;

use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};
use wasm_bindgen::prelude::*;
use web_sys::DedicatedWorkerGlobalScope;

use crate::ipcproto::{Action, Request, Response, Status};
use crate::learner::Learner;
use crate::p2p::{self, Node};
use crate::shmring_ipc;
use crate::sqlite_store::SqliteStore;

struct WorkerState {
    handle: p2p::Handle,
    learner: Option<Rc<Learner>>,
    leader: Option<PeerId>,
}

/// Runs forever inside the Worker script (see `web-app/README.md`'s
/// `worker.js`): brings up this tab's own `p2p::Node`, then answers every
/// request the main thread sends over the shmring channel.
#[wasm_bindgen]
pub async fn worker_main() {
    console_error_panic_hook::set_once();

    let global: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();
    let keypair = identity::Keypair::generate_ed25519();

    let (node, handle) = match Node::new(keypair) {
        Ok(v) => v,
        Err(e) => {
            web_sys::console::error_1(&format!("kv-raft-web: create node: {e}").into());
            return;
        }
    };
    // Node::run becomes this Node's sole, permanent owner -- see p2p.rs's
    // "Task ownership" doc comment.
    wasm_bindgen_futures::spawn_local(node.run());

    let state = Rc::new(RefCell::new(WorkerState {
        handle,
        learner: None,
        leader: None,
    }));

    shmring_ipc::serve(global, move |req: Request| {
        let state = state.clone();
        async move { handle_request(state, req).await }
    });
}

async fn handle_request(state: Rc<RefCell<WorkerState>>, req: Request) -> Response {
    match req.action {
        Action::Add => match do_connect(&state, &req.key).await {
            Ok(peer_id) => Response::new(Status::Ok, &peer_id.to_string()),
            Err(e) => Response::new(Status::Error, &e.0),
        },
        Action::Set => match do_set(&state, &req.key, &req.value).await {
            Ok(()) => Response::new(Status::Ok, ""),
            Err(e) => Response::new(Status::Error, &e.0),
        },
        Action::Get => match do_get(&state, &req.key).await {
            Ok(v) => Response::new(Status::Ok, &v),
            Err(e) => Response::new(Status::Error, &e.0),
        },
    }
}

/// Dials `target_addr` (any cluster member's WebTransport multiaddr, per
/// `pkg/daemon.newHost`), reserves a circuit-relay v2 slot through it, and
/// asks it (forwarding to the real leader if needed -- see the new
/// learner-join handling in `pkg/daemon`) to add this tab as a raft
/// non-voter at that reserved address. Returns this tab's own peer id,
/// mirroring `kvmobile.Start`'s return value.
async fn do_connect(
    state: &Rc<RefCell<WorkerState>>,
    target_addr: &str,
) -> Result<PeerId, p2p::Error> {
    let addr: Multiaddr = target_addr
        .parse()
        .map_err(|e: libp2p::multiaddr::Error| p2p::Error(e.to_string()))?;
    let target_peer = addr
        .iter()
        .find_map(|p| match p {
            Protocol::P2p(id) => Some(id),
            _ => None,
        })
        .ok_or_else(|| p2p::Error("multiaddr missing /p2p/<peer-id>".into()))?;

    let mut handle = state.borrow().handle.clone();
    let self_addr = handle.reserve_relay_slot(addr).await?;
    let self_id = handle.local_peer_id();

    let store = SqliteStore::open(&format!("kv-raft-web-{self_id}.sqlite3"), None)
        .map_err(|e| p2p::Error(e.to_string()))?;
    let learner = Rc::new(Learner::new(
        store,
        self_id.to_bytes(),
        self_addr.to_string().into_bytes(),
    ));

    {
        let mut guard = state.borrow_mut();
        guard.learner = Some(learner.clone());
        guard.leader = Some(target_peer);
    }

    // Accept inbound raft-protocol streams forever -- spawned once, using
    // its own cloned Handle (see this module's doc comment).
    {
        let mut handle = handle.clone();
        wasm_bindgen_futures::spawn_local(async move {
            if let Err(e) = handle.serve_raft(learner).await {
                web_sys::console::error_1(&format!("kv-raft-web: serve_raft: {e}").into());
            }
        });
    }

    // Ask target_peer (forwarding to the real leader server-side happens
    // on the Go side, mirroring ForwardJoinProtocolID) to AddNonvoter this
    // tab at `self_addr`.
    let add_req = Request::new(Action::Add, &self_id.to_string(), &self_addr.to_string());
    let resp = handle.call_client_protocol(target_peer, &add_req).await?;
    if resp.status != Status::Ok {
        return Err(p2p::Error(resp.value));
    }
    Ok(self_id)
}

async fn do_set(
    state: &Rc<RefCell<WorkerState>>,
    key: &str,
    value: &str,
) -> Result<(), p2p::Error> {
    let (mut handle, leader) = {
        let guard = state.borrow();
        let leader = guard
            .leader
            .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;
        (guard.handle.clone(), leader)
    };
    let req = Request::new(Action::Set, key, value);
    let resp = handle.call_client_protocol(leader, &req).await?;
    if resp.status != Status::Ok {
        return Err(p2p::Error(resp.value));
    }
    Ok(())
}

/// This tab's own locally replicated read (via the raft learner applied up
/// through its own `commit_index`) -- may lag a moment behind a Set that
/// just committed on the leader, same caveat any raft follower's local
/// read already carries.
async fn do_get(state: &Rc<RefCell<WorkerState>>, key: &str) -> Result<String, p2p::Error> {
    let learner = state
        .borrow()
        .learner
        .clone()
        .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;
    let value = learner
        .get(key.as_bytes())
        .map_err(|e| p2p::Error(e.to_string()))?
        .ok_or_else(|| p2p::Error(format!("key {key:?} not found")))?;
    String::from_utf8(value).map_err(|e| p2p::Error(e.to_string()))
}

/// Main-thread handle: the UI's only entry point (see `web-app/README.md`'s
/// `main.js`), wrapping [`shmring_ipc::MainChannel`] with the three
/// `ipcproto::Action`s a page actually needs, matching how
/// `MainActivity.kt` drives Android's in-process daemon through
/// `pkg/ipc.Call`.
#[wasm_bindgen]
pub struct MainHandle {
    channel: shmring_ipc::MainChannel,
}

#[wasm_bindgen]
impl MainHandle {
    #[wasm_bindgen(constructor)]
    pub fn new(worker: web_sys::Worker) -> MainHandle {
        MainHandle {
            channel: shmring_ipc::MainChannel::new(worker),
        }
    }

    /// Connects to `target_multiaddr` (any cluster member's WebTransport
    /// multiaddr) and joins this tab as a non-voting learner. Resolves to
    /// this tab's own peer id.
    pub async fn connect(&self, target_multiaddr: String) -> Result<String, JsValue> {
        let resp = self
            .channel
            .call(Request::new(Action::Add, &target_multiaddr, ""))
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        into_js_result(resp)
    }

    pub async fn set(&self, key: String, value: String) -> Result<String, JsValue> {
        let resp = self
            .channel
            .call(Request::new(Action::Set, &key, &value))
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        into_js_result(resp)
    }

    pub async fn get(&self, key: String) -> Result<String, JsValue> {
        let resp = self
            .channel
            .call(Request::new(Action::Get, &key, ""))
            .await
            .map_err(|e| JsValue::from_str(&e.to_string()))?;
        into_js_result(resp)
    }
}

fn into_js_result(resp: Response) -> Result<String, JsValue> {
    if resp.status == Status::Ok {
        Ok(resp.value)
    } else {
        Err(JsValue::from_str(&resp.value))
    }
}
