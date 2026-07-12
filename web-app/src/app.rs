//! Wires [`crate::p2p::Node`]/[`crate::p2p::Handle`] +
//! [`crate::learner::Learner`] + [`crate::shmring_ipc`] +
//! [`crate::shmevent`] together into the two `wasm-bindgen` entry points a
//! page actually loads: [`worker_main`] (run inside the Worker that owns
//! this tab's daemon-equivalent state, mirroring `mobile/kvmobile.go`'s
//! Start/Submit/Get answering `pkg/ipc` requests from Android's
//! `MainActivity`) and [`MainHandle`] (run on the main thread, driving the
//! Worker exclusively through [`shmring_ipc::MainChannel`], the same shape
//! Android's UI drives its in-process daemon through `pkg/ipc.Call`).
//!
//! # Two separate signing keys
//!
//! Every hop in this project speaks the same [`crate::shmevent`] struct,
//! and per that module's doc comment, a caller with no key of its own
//! bootstraps by fetching one from whichever "raft node instance" it's
//! talking to. That happens *twice* here, with two different keys:
//!
//!  1. **Main thread <-> Worker** (this file's `MainHandle`/`handle_request`):
//!     the Worker generates its own libp2p identity key in [`worker_main`]
//!     and doubles as this hop's signing key -- `MainHandle::ensure_key`
//!     fetches it via `EVENT_GET_PRIVATE_KEY` before signing anything else,
//!     mirroring `pkg/shmclient.Session`'s bootstrap. Same-origin/same-JS-realm
//!     as this relationship is, there's no realistic attacker who could
//!     forge a message here without already having arbitrary code execution
//!     in the page (at which point far worse is possible directly) -- this
//!     hop is signed anyway for protocol consistency (one struct, one
//!     verification path, everywhere) and because it costs little given the
//!     Worker already has a real key on hand.
//!  2. **This tab <-> the remote leader** (`do_connect`/`do_set`, over
//!     `p2p::Handle::call_client_protocol`): `do_connect` fetches the
//!     *leader's* key the same way, and every subsequent SetKey/SetField/Add
//!     to that leader is signed with it -- this is the hop
//!     `pkg/daemon.handleShmEvent` actually verifies against, and where
//!     unsigned/wrongly-signed requests are genuinely rejected server-side.
//!
//! `ActionAdd`'s old meaning ("connect to the target node and join as a
//! non-voting learner") is now `EVENT_ADD`, `SourceID` referencing a prior
//! `EVENT_SET_KEY` holding this tab's own peer id -- the exact sequence
//! `pkg/daemon.TestAddLearnerThroughRelay` exercises from the Go side.
#![cfg(target_arch = "wasm32")]

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use ed25519_dalek::{SigningKey, VerifyingKey};
use libp2p::{identity, multiaddr::Protocol, Multiaddr, PeerId};
use wasm_bindgen::prelude::*;
use web_sys::DedicatedWorkerGlobalScope;

use crate::learner::Learner;
use crate::p2p::{self, Node};
use crate::shmevent::{self, Msg};
use crate::shmring_ipc;
use crate::sqlite_store::SqliteStore;

/// Random non-zero id for a new message -- 0 is reserved meaning
/// "SourceID/DestinationID not used" (see `api/shmevent.capnp`), so a real
/// message's own id avoids it too. Mirrors `pkg/shmclient.newID`.
fn new_id() -> u16 {
    loop {
        let v = (js_sys::Math::random() * 65536.0) as u32 as u16;
        if v != 0 {
            return v;
        }
    }
}

struct WorkerState {
    handle: p2p::Handle,
    learner: Option<Rc<Learner>>,
    leader: Option<PeerId>,
    /// The leader's signing key, fetched once by `do_connect` -- see this
    /// module's doc comment, key relationship 2.
    remote_priv: Option<SigningKey>,
    /// Backs `EVENT_SET_KEY`/`EVENT_GET_KEY` for the main-thread<->Worker
    /// hop -- this Worker's own mirror of `pkg/shmevent.Registry`.
    registry: HashMap<u16, Vec<u8>>,
    /// This Worker's own identity key, doubling as the main-thread<->Worker
    /// hop's signing key -- see this module's doc comment, key
    /// relationship 1.
    signing_key: SigningKey,
    verifying_key: VerifyingKey,
}

/// Runs forever inside the Worker script (see `web-app/README.md`'s
/// `worker.js`): brings up this tab's own `p2p::Node`, then answers every
/// request the main thread sends over the shmring channel. Generates a
/// fresh random identity every time -- see `worker_main_with_seed` for a
/// deterministic-identity variant.
#[wasm_bindgen]
pub async fn worker_main() {
    run_worker(identity::Keypair::generate_ed25519()).await
}

/// Like `worker_main`, but with a deterministic identity instead of a
/// freshly random one: `seed_hex` is 128 hex chars decoding to the 64 raw
/// stdlib `crypto/ed25519` private key bytes (32-byte seed + 32-byte public
/// key) -- exactly `pkg/e2edata.Node.PrivateKey`'s own format, so a
/// recorded node's key can be passed straight through with no conversion.
/// This is what the e2e test pipeline needs so a build against a recorded
/// identity reliably comes up as that exact peer id (mirrors
/// `mobile/kvmobile`'s `identitySeedHex` ldflag and
/// `pkg/e2edata.WriteDesktopKeyFile`'s desktop equivalent). Returns an
/// error string via `Err` if seed_hex doesn't decode to a valid key,
/// instead of panicking -- a page driving this from JS can then report it
/// instead of silently hanging.
#[wasm_bindgen]
pub async fn worker_main_with_seed(seed_hex: String) -> Result<(), JsValue> {
    let raw = hex_decode(&seed_hex)
        .map_err(|e| JsValue::from_str(&format!("worker_main_with_seed: decode seed_hex: {e}")))?;
    if raw.len() != 64 {
        return Err(JsValue::from_str(&format!(
            "worker_main_with_seed: seed_hex must decode to 64 bytes (32-byte seed + 32-byte public key), got {}",
            raw.len()
        )));
    }
    let mut seed: [u8; 32] = raw[..32].try_into().expect("checked len above");
    let keypair = identity::Keypair::ed25519_from_bytes(&mut seed[..])
        .map_err(|e| JsValue::from_str(&format!("worker_main_with_seed: {e}")))?;
    run_worker(keypair).await;
    Ok(())
}

/// Shared body of `worker_main`/`worker_main_with_seed`: brings up this
/// tab's own `p2p::Node` under keypair, then answers every request the
/// main thread sends over the shmring channel.
async fn run_worker(keypair: identity::Keypair) {
    console_error_panic_hook::set_once();

    let global: DedicatedWorkerGlobalScope = js_sys::global().unchecked_into();

    // libp2p_identity::ed25519::Keypair wraps its own ed25519-dalek
    // SigningKey, not necessarily the same major version this crate
    // depends on directly -- to_bytes()/from_bytes() round-trips through
    // the portable raw byte format both understand identically (the same
    // "seed + public key" layout stdlib crypto/ed25519 and pkg/shmevent
    // use on the Go side), rather than trying to share the type itself.
    let ed25519_kp = keypair
        .clone()
        .try_into_ed25519()
        .expect("keypair is always constructed as ed25519 by worker_main/worker_main_with_seed");
    let kp_bytes = ed25519_kp.to_bytes();
    let seed: [u8; 32] = kp_bytes[..32]
        .try_into()
        .expect("ed25519 keypair bytes are 64 bytes: 32-byte seed + 32-byte public key");
    let signing_key = SigningKey::from_bytes(&seed);
    let verifying_key = signing_key.verifying_key();

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
        remote_priv: None,
        registry: HashMap::new(),
        signing_key,
        verifying_key,
    }));

    shmring_ipc::serve(global, move |req: Msg, crc: u32, sig: Vec<u8>| {
        let state = state.clone();
        async move { handle_request(state, req, crc, sig).await }
    });
}

/// Minimal hex decoder so `worker_main_with_seed` doesn't need to pull in
/// a whole `hex` crate dependency for one call site.
fn hex_decode(s: &str) -> Result<Vec<u8>, String> {
    if s.len() % 2 != 0 {
        return Err("odd-length hex string".to_string());
    }
    let mut out = Vec::with_capacity(s.len() / 2);
    let bytes = s.as_bytes();
    for chunk in bytes.chunks(2) {
        let hi = (chunk[0] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit {:?}", chunk[0] as char))?;
        let lo = (chunk[1] as char)
            .to_digit(16)
            .ok_or_else(|| format!("invalid hex digit {:?}", chunk[1] as char))?;
        out.push(((hi << 4) | lo) as u8);
    }
    Ok(out)
}

/// Dispatches one decoded main-thread request the same way
/// `pkg/daemon.handleShmEvent` dispatches a local `pkg/ipc` request --
/// using this Worker's own registry/key for the main-thread<->Worker hop
/// (see this module's doc comment) -- and returns the already-encoded,
/// already-signed response bytes `shmring_ipc::serve` expects.
async fn handle_request(
    state: Rc<RefCell<WorkerState>>,
    req: Msg,
    crc: u32,
    sig: Vec<u8>,
) -> Vec<u8> {
    let req_id = req.id;
    if shmevent::requires_signature(req.event_type) {
        let vk = state.borrow().verifying_key;
        if let Err(e) = shmevent::verify(&vk, &req, crc, &sig) {
            return encode_local_response(&state, Msg::error(req_id, e.to_string()));
        }
    }

    let resp = match req.event_type {
        shmevent::EVENT_SET_KEY => {
            state
                .borrow_mut()
                .registry
                .insert(req.id, req.value.clone());
            Msg {
                event_type: shmevent::EVENT_SET_KEY,
                id: req_id,
                value: req.value,
                ..Default::default()
            }
        }

        shmevent::EVENT_GET_KEY => match state.borrow().registry.get(&req.source_id).cloned() {
            Some(value) => Msg {
                event_type: shmevent::EVENT_GET_KEY,
                id: req_id,
                value,
                ..Default::default()
            },
            None => Msg::error(
                req_id,
                format!("no entry registered under id {}", req.source_id),
            ),
        },

        shmevent::EVENT_SET_FIELD => {
            let key = state.borrow().registry.get(&req.source_id).cloned();
            match key {
                Some(key) => {
                    let key = String::from_utf8_lossy(&key).into_owned();
                    let value = String::from_utf8_lossy(&req.value).into_owned();
                    match do_set(&state, &key, &value).await {
                        Ok(()) => Msg {
                            event_type: shmevent::EVENT_SET_FIELD,
                            id: req_id,
                            ..Default::default()
                        },
                        Err(e) => Msg::error(req_id, e.to_string()),
                    }
                }
                None => Msg::error(
                    req_id,
                    format!(
                        "no key registered under id {} -- send SetKey first",
                        req.source_id
                    ),
                ),
            }
        }

        shmevent::EVENT_GET_FIELD => {
            let key = if req.source_id != 0 {
                match state.borrow().registry.get(&req.source_id).cloned() {
                    Some(k) => k,
                    None => {
                        return encode_local_response(
                            &state,
                            Msg::error(
                                req_id,
                                format!(
                                    "no key registered under id {} -- send SetKey first",
                                    req.source_id
                                ),
                            ),
                        );
                    }
                }
            } else {
                req.value.clone()
            };
            let key = String::from_utf8_lossy(&key).into_owned();
            match do_get(&state, &key).await {
                Ok(value) => Msg {
                    event_type: shmevent::EVENT_GET_FIELD,
                    id: req_id,
                    value: value.into_bytes(),
                    ..Default::default()
                },
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        shmevent::EVENT_GET_PUBLIC_KEY => {
            let vk = state.borrow().verifying_key;
            Msg {
                event_type: shmevent::EVENT_GET_PUBLIC_KEY,
                id: req_id,
                value: vk.to_bytes().to_vec(),
                ..Default::default()
            }
        }

        shmevent::EVENT_GET_PRIVATE_KEY => {
            let sk_bytes = state.borrow().signing_key.to_bytes();
            Msg {
                event_type: shmevent::EVENT_GET_PRIVATE_KEY,
                id: req_id,
                value: sk_bytes.to_vec(),
                ..Default::default()
            }
        }

        shmevent::EVENT_ADD => {
            let target_addr = String::from_utf8_lossy(&req.value).into_owned();
            match do_connect(&state, &target_addr).await {
                Ok(peer_id) => Msg {
                    event_type: shmevent::EVENT_ADD,
                    id: req_id,
                    value: peer_id.to_string().into_bytes(),
                    ..Default::default()
                },
                Err(e) => Msg::error(req_id, e.to_string()),
            }
        }

        other => Msg::error(req_id, format!("unknown event {other}")),
    };

    encode_local_response(&state, resp)
}

/// Signs `resp` with this Worker's own identity key -- the main thread
/// already fetched the matching public key via `EVENT_GET_PRIVATE_KEY`
/// before sending anything else (see `MainHandle::ensure_key`), mirroring
/// `pkg/ipc.Serve`'s responses always being signed with the daemon's real
/// key (never the `None`-key bootstrap exception, which only ever applies
/// to requests).
fn encode_local_response(state: &Rc<RefCell<WorkerState>>, resp: Msg) -> Vec<u8> {
    let signing_key = state.borrow().signing_key.clone();
    shmevent::encode(&resp, Some(&signing_key)).unwrap_or_default()
}

/// Dials `target_addr` (any cluster member's WebTransport multiaddr, per
/// `pkg/daemon.newHost`), reserves a circuit-relay v2 slot through it,
/// fetches its signing key (unsigned `EVENT_GET_PRIVATE_KEY` bootstrap --
/// see this module's doc comment, key relationship 2), and asks it
/// (forwarding to the real leader server-side if needed -- see the
/// learner-join handling in `pkg/daemon`) to add this tab as a raft
/// non-voter at that reserved address via a signed `EVENT_SET_KEY` +
/// `EVENT_ADD` pair -- the same sequence
/// `pkg/daemon.TestAddLearnerThroughRelay` exercises from the Go side.
/// Returns this tab's own peer id, mirroring `kvmobile.Start`'s return
/// value.
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

    let remote_priv = fetch_remote_signing_key(&mut handle, target_peer).await?;

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
        guard.remote_priv = Some(remote_priv.clone());
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

    let set_key_id = new_id();
    let set_key_resp = call_remote(
        &mut handle,
        target_peer,
        Msg {
            event_type: shmevent::EVENT_SET_KEY,
            value: self_id.to_string().into_bytes(),
            id: set_key_id,
            ..Default::default()
        },
        Some(&remote_priv),
    )
    .await?;
    reject_if_error(&set_key_resp)?;

    let add_resp = call_remote(
        &mut handle,
        target_peer,
        Msg {
            event_type: shmevent::EVENT_ADD,
            source_id: set_key_id,
            value: self_addr.to_string().into_bytes(),
            id: new_id(),
            ..Default::default()
        },
        Some(&remote_priv),
    )
    .await?;
    reject_if_error(&add_resp)?;

    Ok(self_id)
}

async fn fetch_remote_signing_key(
    handle: &mut p2p::Handle,
    target_peer: PeerId,
) -> Result<SigningKey, p2p::Error> {
    let resp = call_remote(
        handle,
        target_peer,
        Msg {
            event_type: shmevent::EVENT_GET_PRIVATE_KEY,
            id: new_id(),
            ..Default::default()
        },
        None,
    )
    .await?;
    reject_if_error(&resp)?;
    let seed: [u8; 32] = resp
        .value
        .get(..32)
        .and_then(|s| s.try_into().ok())
        .ok_or_else(|| p2p::Error("invalid private key length in response".into()))?;
    Ok(SigningKey::from_bytes(&seed))
}

async fn call_remote(
    handle: &mut p2p::Handle,
    target_peer: PeerId,
    req: Msg,
    priv_key: Option<&SigningKey>,
) -> Result<Msg, p2p::Error> {
    handle
        .call_client_protocol(target_peer, &req, priv_key)
        .await
}

fn reject_if_error(resp: &Msg) -> Result<(), p2p::Error> {
    if resp.event_type == shmevent::EVENT_ERROR {
        return Err(p2p::Error(
            String::from_utf8_lossy(&resp.value).into_owned(),
        ));
    }
    Ok(())
}

/// Applies key=value through raft on the connected leader: `EVENT_SET_KEY`
/// registers key, then `EVENT_SET_FIELD` (referencing it via `SourceID`)
/// applies the value -- see `pkg/shmevent`'s doc comment for why a Set
/// needs two linked messages rather than one.
async fn do_set(
    state: &Rc<RefCell<WorkerState>>,
    key: &str,
    value: &str,
) -> Result<(), p2p::Error> {
    let (mut handle, leader, remote_priv) = {
        let guard = state.borrow();
        let leader = guard
            .leader
            .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;
        let remote_priv = guard
            .remote_priv
            .clone()
            .ok_or_else(|| p2p::Error("do_connect has not completed yet".into()))?;
        (guard.handle.clone(), leader, remote_priv)
    };

    let set_key_id = new_id();
    let set_key_resp = call_remote(
        &mut handle,
        leader,
        Msg {
            event_type: shmevent::EVENT_SET_KEY,
            value: key.as_bytes().to_vec(),
            id: set_key_id,
            ..Default::default()
        },
        Some(&remote_priv),
    )
    .await?;
    reject_if_error(&set_key_resp)?;

    let set_field_resp = call_remote(
        &mut handle,
        leader,
        Msg {
            event_type: shmevent::EVENT_SET_FIELD,
            source_id: set_key_id,
            value: value.as_bytes().to_vec(),
            id: new_id(),
            ..Default::default()
        },
        Some(&remote_priv),
    )
    .await?;
    reject_if_error(&set_field_resp)
}

/// This tab's own locally replicated read (via the raft learner applied up
/// through its own `commit_index`) -- may lag a moment behind a Set that
/// just committed on the leader, same caveat any raft follower's local
/// read already carries. Purely local: no round trip to the leader at all.
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
/// operations a page actually needs, matching how `MainActivity.kt` drives
/// Android's in-process daemon through `pkg/ipc.Call`. `ensure_key` fetches
/// and caches the Worker's signing key on first use (see this module's doc
/// comment, key relationship 1) -- mirroring `pkg/shmclient.Session`.
#[wasm_bindgen]
pub struct MainHandle {
    channel: shmring_ipc::MainChannel,
    signing_key: RefCell<Option<SigningKey>>,
}

#[wasm_bindgen]
impl MainHandle {
    #[wasm_bindgen(constructor)]
    pub fn new(worker: web_sys::Worker) -> MainHandle {
        MainHandle {
            channel: shmring_ipc::MainChannel::new(worker),
            signing_key: RefCell::new(None),
        }
    }

    /// Connects to `target_multiaddr` (any cluster member's WebTransport
    /// multiaddr) and joins this tab as a non-voting learner. Resolves to
    /// this tab's own peer id.
    pub async fn connect(&self, target_multiaddr: String) -> Result<String, JsValue> {
        let key = self.ensure_key().await?;
        let resp = self
            .channel
            .call(
                &Msg {
                    event_type: shmevent::EVENT_ADD,
                    value: target_multiaddr.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&key),
            )
            .await
            .map_err(js_err)?;
        into_js_result(resp)
    }

    pub async fn set(&self, key: String, value: String) -> Result<String, JsValue> {
        let signing_key = self.ensure_key().await?;
        let set_key_id = new_id();
        let set_key_resp = self
            .channel
            .call(
                &Msg {
                    event_type: shmevent::EVENT_SET_KEY,
                    value: key.into_bytes(),
                    id: set_key_id,
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await
            .map_err(js_err)?;
        into_js_result(set_key_resp)?;

        let set_field_resp = self
            .channel
            .call(
                &Msg {
                    event_type: shmevent::EVENT_SET_FIELD,
                    source_id: set_key_id,
                    value: value.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await
            .map_err(js_err)?;
        into_js_result(set_field_resp)
    }

    pub async fn get(&self, key: String) -> Result<String, JsValue> {
        let signing_key = self.ensure_key().await?;
        let resp = self
            .channel
            .call(
                &Msg {
                    event_type: shmevent::EVENT_GET_FIELD,
                    value: key.into_bytes(),
                    id: new_id(),
                    ..Default::default()
                },
                Some(&signing_key),
            )
            .await
            .map_err(js_err)?;
        into_js_result(resp)
    }

    async fn ensure_key(&self) -> Result<SigningKey, JsValue> {
        if let Some(k) = self.signing_key.borrow().clone() {
            return Ok(k);
        }
        let resp = self
            .channel
            .call(
                &Msg {
                    event_type: shmevent::EVENT_GET_PRIVATE_KEY,
                    id: new_id(),
                    ..Default::default()
                },
                None,
            )
            .await
            .map_err(js_err)?;
        if resp.event_type == shmevent::EVENT_ERROR {
            // Not routed through into_js_result: that lossy-UTF8-decodes
            // `value`, which is correct for an actual error message but
            // would corrupt the raw key bytes on the success path below.
            return Err(JsValue::from_str(&String::from_utf8_lossy(&resp.value)));
        }
        let seed: [u8; 32] = resp
            .value
            .get(..32)
            .and_then(|s| s.try_into().ok())
            .ok_or_else(|| JsValue::from_str("invalid private key length in response"))?;
        let key = SigningKey::from_bytes(&seed);
        *self.signing_key.borrow_mut() = Some(key.clone());
        Ok(key)
    }
}

fn js_err(e: shmring_ipc::Error) -> JsValue {
    JsValue::from_str(&e.to_string())
}

fn into_js_result(resp: Msg) -> Result<String, JsValue> {
    if resp.event_type == shmevent::EVENT_ERROR {
        Err(JsValue::from_str(&String::from_utf8_lossy(&resp.value)))
    } else {
        Ok(String::from_utf8_lossy(&resp.value).into_owned())
    }
}
