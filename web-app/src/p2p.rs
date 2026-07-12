//! rust-libp2p `Swarm` for the browser tab: dials the relay/leader over
//! WebTransport (`pkg/daemon.newHost`'s WebTransport listener), reserves a
//! circuit-relay v2 slot so a Go leader can `Dial()` back to this tab the
//! same way it already reaches an Android device behind carrier-grade NAT
//! (see `pkg/daemon.Config.RelayPeer`'s doc comment) -- a browser can never
//! accept a raw inbound connection, but *can* be dialed through a relay it
//! already holds an outbound reservation on, which is what makes a real
//! (non-voting) raft membership possible here at all. Serves two
//! protocols: `rafttransport::ProtocolID`'s raw RPC stream (dispatched to
//! [`crate::learner::Learner`]) and `pkg/daemon.ClientProtocolID`'s
//! initial AddNonvoter join handshake.
//!
//! # Task ownership
//!
//! [`Swarm`] is not `Sync`/shareable in any useful way for our purposes --
//! `dial`/`listen_on` are synchronous calls that must interleave correctly
//! with `select_next_some().await`'s indefinite wait for the *next* event,
//! which is a problem the moment two different async tasks want to touch
//! it: if one task holds a naive lock across `select_next_some().await`
//! (which can legitimately hang until, say, a dial elsewhere produces a
//! connection event), any other task blocked on that same lock to *call*
//! `dial` in the first place deadlocks permanently. So [`Node`] is not
//! shared at all: [`Node::new`] returns it alongside a cheaply-cloneable
//! [`Handle`], the *only* thing calling code should keep -- [`Node`]
//! itself is moved into exactly one [`Node::run`] task (see `app.rs`) that
//! becomes its sole, permanent owner. Stream operations
//! (`Handle::open_stream`/`accept`, via `libp2p_stream::Control`) don't
//! need this at all: they work through `Control`'s own internal channels,
//! serviced by `Node::run`'s ambient `select_next_some()` polling, so
//! `Handle` can call them directly with no synchronization.
#![cfg(target_arch = "wasm32")]

use futures::channel::{mpsc, oneshot};
use futures::{AsyncReadExt, AsyncWriteExt, FutureExt, SinkExt, StreamExt};
use libp2p::{
    identity::Keypair, noise, relay, swarm::NetworkBehaviour, webtransport_websys, Multiaddr,
    PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use libp2p_stream as stream;
use wasm_bindgen::prelude::wasm_bindgen;

#[wasm_bindgen]
extern "C" {
    // See worker.js's __debugLog doc comment: this code runs inside the
    // Worker, whose console messages Playwright's page.on("console") never
    // sees, so diagnostics here go through this relay instead of
    // web_sys::console::log_1.
    #[wasm_bindgen(js_namespace = globalThis, js_name = __debugLog)]
    pub(crate) fn debug_log(msg: &str);
}

use crate::raft_wire;

/// Matches `pkg/rafttransport.ProtocolID` exactly.
pub const RAFT_PROTOCOL: StreamProtocol = StreamProtocol::new("/libp2p-kv-raft/raft/1.0.0");
/// Matches `pkg/daemon.ClientProtocolID` exactly.
pub const CLIENT_PROTOCOL: StreamProtocol = StreamProtocol::new("/libp2p-kv-raft/client/1.0.0");

/// Bounds [`Node::do_reserve`]'s whole dial-then-reserve flow: neither of
/// its two swarm-event-waiting loops has any timeout of its own, so a
/// reservation that never completes (the relay target dials fine but the
/// v2 reservation handshake itself stalls -- observed directly against
/// this project's own real deploy target, whose link can be well under
/// 1 Mbps) hangs [`Handle::reserve_relay_slot`] forever with no way for a
/// caller to notice, let alone recover. Mirrors
/// `pkg/daemon.join`'s `awaitRelayAddr(45 * time.Second)` fix for the
/// exact same failure mode on the Go side -- same 45s budget, same
/// reasoning: a slow reservation isn't a bug, an *unbounded* wait for one
/// is.
const RELAY_RESERVATION_TIMEOUT_MS: u32 = 45_000;

/// Bounds each [`Handle::call_client_protocol`] round trip -- see that
/// method's doc comment for why it needs one at all.
const CLIENT_PROTOCOL_TIMEOUT_MS: u32 = 45_000;

#[derive(NetworkBehaviour)]
struct Behaviour {
    relay_client: relay::client::Behaviour,
    stream: stream::Behaviour,
}

#[derive(Debug, Clone)]
pub struct Error(pub String);

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "p2p: {}", self.0)
    }
}
impl std::error::Error for Error {}

impl From<std::io::Error> for Error {
    fn from(e: std::io::Error) -> Self {
        Error(e.to_string())
    }
}
impl From<libp2p::multiaddr::Error> for Error {
    fn from(e: libp2p::multiaddr::Error) -> Self {
        Error(e.to_string())
    }
}
impl From<std::convert::Infallible> for Error {
    fn from(e: std::convert::Infallible) -> Self {
        match e {}
    }
}
impl From<noise::Error> for Error {
    fn from(e: noise::Error) -> Self {
        Error(e.to_string())
    }
}

enum Command {
    /// Dials `Multiaddr` directly and reserves a circuit-relay v2 slot
    /// through it; replies with this node's own now-dialable circuit
    /// address. Only ever sent once per `Node` lifetime in practice (a tab
    /// connects to one target), but nothing here assumes that.
    ReserveRelaySlot(Multiaddr, oneshot::Sender<Result<Multiaddr, Error>>),
}

/// The sole owner of the `Swarm`; see the module doc comment's "Task
/// ownership" section. Not `Clone` -- exists only to be consumed by
/// [`Node::run`].
pub struct Node {
    swarm: Swarm<Behaviour>,
    commands: mpsc::UnboundedReceiver<Command>,
}

/// Cheaply-cloneable handle to a running [`Node::run`] task -- everything
/// calling code actually holds onto (see `app.rs`).
#[derive(Clone)]
pub struct Handle {
    control: stream::Control,
    local_peer_id: PeerId,
    commands: mpsc::UnboundedSender<Command>,
}

impl Node {
    pub fn new(keypair: Keypair) -> Result<(Self, Handle), Error> {
        let swarm = SwarmBuilder::with_existing_identity(keypair)
            .with_wasm_bindgen()
            .with_other_transport(|kp| {
                webtransport_websys::Transport::new(webtransport_websys::Config::new(kp))
            })
            .map_err(Error::from)?
            .with_relay_client(noise::Config::new, libp2p::yamux::Config::default)
            .map_err(Error::from)?
            .with_behaviour(|_key, relay_client| Behaviour {
                relay_client,
                stream: stream::Behaviour::new(),
            })
            .map_err(Error::from)?
            .build();

        let control = swarm.behaviour().stream.new_control();
        let local_peer_id = *swarm.local_peer_id();
        let (tx, rx) = mpsc::unbounded();

        Ok((
            Node {
                swarm,
                commands: rx,
            },
            Handle {
                control,
                local_peer_id,
                commands: tx,
            },
        ))
    }

    /// Runs forever: services [`Command`]s and drains swarm events (which
    /// is also what makes `Handle`'s `Control`-based stream operations
    /// make progress -- see the module doc comment). Spawn exactly once,
    /// immediately after [`Node::new`] (e.g. via
    /// `wasm_bindgen_futures::spawn_local`); nothing on the corresponding
    /// [`Handle`] does anything useful before this is running.
    pub async fn run(mut self) {
        loop {
            futures::select! {
                event = self.swarm.select_next_some() => {
                    let _ = event;
                }
                cmd = self.commands.next() => {
                    match cmd {
                        Some(Command::ReserveRelaySlot(addr, reply)) => {
                            let result = futures::select! {
                                result = self.do_reserve(addr).fuse() => result,
                                _ = gloo_timers::future::TimeoutFuture::new(RELAY_RESERVATION_TIMEOUT_MS).fuse() => {
                                    Err(Error(format!(
                                        "relay reservation timed out after {}s -- the reservation handshake itself never completed, most likely a slow/unreachable path to the relay target rather than a bug here (see RELAY_RESERVATION_TIMEOUT_MS's doc comment)",
                                        RELAY_RESERVATION_TIMEOUT_MS / 1000
                                    )))
                                }
                            };
                            let _ = reply.send(result);
                        }
                        None => {}
                    }
                }
            }
        }
    }

    async fn do_reserve(&mut self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        debug_log(&format!("kv-raft-web: do_reserve: dialing {addr}"));
        self.swarm
            .dial(addr.clone())
            .map_err(|e| Error(e.to_string()))?;

        let relay_peer = loop {
            let event = self.swarm.select_next_some().await;
            match event {
                libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    break peer_id;
                }
                libp2p::swarm::SwarmEvent::OutgoingConnectionError { error, .. } => {
                    return Err(Error(format!("dial relay: {error}")));
                }
                _ => {}
            }
        };
        debug_log(&format!("kv-raft-web: do_reserve: connected to relay {relay_peer}"));

        // Must carry the relay's actual dialable address, not just its bare
        // peer id (`/p2p/<relay_peer>/p2p-circuit` alone) -- the relay
        // client transport needs the full path to know which relay to
        // reserve a slot through, and rejects a bare-peer-id circuit
        // address with `MissingRelayAddr` (confirmed directly: this is
        // exactly the error `listen_on` returned before this fix). `addr`
        // is the full target multiaddr `do_connect` was given, already
        // ending in `/p2p/<relay_peer>`, so appending `/p2p-circuit`
        // directly onto it is both correct and simpler than rebuilding an
        // address from scratch.
        let circuit_addr: Multiaddr = addr.with(libp2p::multiaddr::Protocol::P2pCircuit);
        self.swarm
            .listen_on(circuit_addr.clone())
            .map_err(|e| Error(format!("listen_on circuit addr: {e:?}")))?;
        debug_log(&format!("kv-raft-web: do_reserve: listen_on({circuit_addr}) called"));

        loop {
            let event = self.swarm.select_next_some().await;
            match event {
                libp2p::swarm::SwarmEvent::NewListenAddr { address, .. }
                    if address.to_string().contains("p2p-circuit") =>
                {
                    // `address` already ends in `/p2p/<local_peer_id>` --
                    // the relay client behaviour reports circuit listen
                    // addresses fully qualified with the local peer id, not
                    // just the relay's. Appending it again here used to
                    // produce a malformed, doubled-up address
                    // (`.../p2p-circuit/p2p/<id>/p2p/<id>`) that this tab
                    // sent the leader as its own dial-back address for
                    // AppendEntries -- confirmed directly: the join/set
                    // calls all succeeded, but the leader could never push
                    // any raft entries back, so a local Get right after a
                    // remote Set always came up empty.
                    return Ok(address);
                }
                libp2p::swarm::SwarmEvent::ListenerError { error, .. } => {
                    return Err(Error(format!("relay reservation: {error}")));
                }
                _ => {}
            }
        }
    }
}

impl Handle {
    pub fn local_peer_id(&self) -> PeerId {
        self.local_peer_id
    }

    /// See [`Command::ReserveRelaySlot`].
    pub async fn reserve_relay_slot(&self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        let (tx, rx) = oneshot::channel();
        self.commands
            .clone()
            .send(Command::ReserveRelaySlot(addr, tx))
            .await
            .map_err(|_| Error("node task no longer running".into()))?;
        rx.await
            .map_err(|_| Error("node task no longer running".into()))?
    }

    /// Accepts inbound `RAFT_PROTOCOL` streams forever, running each
    /// through [`serve_raft_stream`]. Spawn this once `learner` is ready
    /// (typically right after a successful [`Handle::reserve_relay_slot`]).
    pub async fn serve_raft(
        &mut self,
        learner: std::rc::Rc<crate::learner::Learner>,
    ) -> Result<(), Error> {
        let mut incoming = self
            .control
            .accept(RAFT_PROTOCOL)
            .map_err(|e| Error(e.to_string()))?;
        debug_log("kv-raft-web: serve_raft: accepting inbound raft streams");
        while let Some((peer, stream)) = incoming.next().await {
            debug_log(&format!("kv-raft-web: serve_raft: inbound stream from {peer}"));
            let learner = learner.clone();
            wasm_bindgen_futures::spawn_local(async move {
                if let Err(e) = serve_raft_stream(stream, learner).await {
                    debug_log(&format!("kv-raft-web: raft stream: {e}"));
                }
            });
        }
        debug_log("kv-raft-web: serve_raft: incoming stream iterator ended");
        Ok(())
    }

    /// Speaks `CLIENT_PROTOCOL` to `target` for one request/response round
    /// trip -- used for the join handshake and for forwarded Set/Get,
    /// exactly like the desktop CLI and the Android app's relationship to
    /// their own daemon (see `pkg/daemon.ClientProtocolID`), just
    /// re-encoded with this crate's own [`crate::shmevent`] port. A capnp
    /// message has no fixed size (unlike the ipcproto.Request/Response
    /// this replaced), so the response is read until the daemon closes
    /// its write side rather than a known byte count.
    ///
    /// Bounded by [`CLIENT_PROTOCOL_TIMEOUT_MS`]: neither `open_stream` nor
    /// `read_to_end` below has any timeout of its own, so a stream that
    /// never opens (e.g. dialing back through a relay circuit that looked
    /// reserved but isn't actually usable yet) or a remote that never
    /// closes its write side hangs this -- and every caller through
    /// `do_connect`/`do_set`/`fetch_remote_signing_key` -- forever, the
    /// same failure mode [`RELAY_RESERVATION_TIMEOUT_MS`] fixes for the
    /// reservation step itself. Caught by exactly that happening against a
    /// real relay-mediated leader: the reservation completed but a
    /// following `call_client_protocol` still hung the full test timeout.
    pub async fn call_client_protocol(
        &mut self,
        target: PeerId,
        req: &crate::shmevent::Msg,
        priv_key: Option<&ed25519_dalek::SigningKey>,
    ) -> Result<crate::shmevent::Msg, Error> {
        futures::select! {
            result = self.call_client_protocol_inner(target, req, priv_key).fuse() => result,
            _ = gloo_timers::future::TimeoutFuture::new(CLIENT_PROTOCOL_TIMEOUT_MS).fuse() => {
                Err(Error(format!(
                    "client protocol call timed out after {}s",
                    CLIENT_PROTOCOL_TIMEOUT_MS / 1000
                )))
            }
        }
    }

    async fn call_client_protocol_inner(
        &mut self,
        target: PeerId,
        req: &crate::shmevent::Msg,
        priv_key: Option<&ed25519_dalek::SigningKey>,
    ) -> Result<crate::shmevent::Msg, Error> {
        debug_log(&format!("kv-raft-web: call_client_protocol: opening stream to {target}"));
        let mut s = self
            .control
            .open_stream(target, CLIENT_PROTOCOL.clone())
            .await
            .map_err(|e| Error(e.to_string()))?;
        debug_log("kv-raft-web: call_client_protocol: stream open, writing request");
        let buf = crate::shmevent::encode(req, priv_key).map_err(|e| Error(e.to_string()))?;
        s.write_all(&buf).await?;
        s.close().await?;
        debug_log("kv-raft-web: call_client_protocol: request written, reading response");

        let mut resp_buf = Vec::new();
        s.read_to_end(&mut resp_buf).await?;
        debug_log(&format!("kv-raft-web: call_client_protocol: got {} response bytes", resp_buf.len()));
        let (resp, _, _) = crate::shmevent::decode(&resp_buf).map_err(|e| Error(e.to_string()))?;
        Ok(resp)
    }
}

/// One inbound raft RPC stream's lifetime, mirroring
/// `net_transport.go`'s `handleConn`/`handleCommand`: loop reading a
/// one-byte RPC type then its msgpack request, dispatch to `learner`,
/// write back the framed reply, and keep going until the leader closes the
/// stream (its `NetworkTransport` connection pool reuses one stream for
/// many sequential RPCs, so this must not stop after the first one).
async fn serve_raft_stream(
    mut s: libp2p::Stream,
    learner: std::rc::Rc<crate::learner::Learner>,
) -> Result<(), Error> {
    loop {
        let mut type_byte = [0u8; 1];
        if s.read_exact(&mut type_byte).await.is_err() {
            return Ok(()); // leader closed the stream; not an error
        }
        let rpc_type = raft_wire::RpcType::from_byte(type_byte[0])
            .ok_or_else(|| Error(format!("unknown rpc type {}", type_byte[0])))?;
        debug_log(&format!("kv-raft-web: serve_raft_stream: rpc {rpc_type:?}"));

        // Every request struct decodes from one msgpack value with no
        // length prefix, so read incrementally: grow a buffer and retry
        // the decode until it succeeds, matching how a streaming
        // `codec.Decoder` on the Go side consumes exactly one value's
        // worth of bytes off the wire without a separate framing layer.
        let reply_bytes = match rpc_type {
            raft_wire::RpcType::AppendEntries => {
                let (req, _leftover) =
                    decode_one(&mut s, raft_wire::decode_append_entries_request).await?;
                let resp = learner
                    .handle_append_entries(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_append_entries_response(&resp))
            }
            raft_wire::RpcType::RequestVote => {
                let (req, _leftover) =
                    decode_one(&mut s, raft_wire::decode_request_vote_request).await?;
                let resp = learner
                    .handle_request_vote(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_request_vote_response(&resp))
            }
            raft_wire::RpcType::RequestPreVote => {
                let (req, _leftover) =
                    decode_one(&mut s, raft_wire::decode_request_pre_vote_request).await?;
                let resp = learner
                    .handle_request_pre_vote(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_request_pre_vote_response(&resp))
            }
            raft_wire::RpcType::InstallSnapshot => {
                let (req, leftover) =
                    decode_one(&mut s, raft_wire::decode_install_snapshot_request).await?;
                // The raw snapshot body immediately follows the msgpack
                // header on the same stream (hashicorp/raft's
                // NetworkTransport.InstallSnapshot writes both to the same
                // buffered writer before one flush -- see net_transport.go),
                // so a single underlying read can legitimately return header
                // bytes *and* some/all of the body bytes together. decode_one
                // already consumed that whole chunk into its own buffer to
                // find the header; `leftover` is whatever of it came after
                // the header ended, per its own doc comment. Without
                // prepending it here, those bytes were silently discarded and
                // the subsequent read_exact came up short -- confirmed
                // directly: every InstallSnapshot attempt failed with
                // "unexpected end of file" until this fix, which is why this
                // RPC type was the only one actually broken by the bug (every
                // other RPC here has nothing following its own header on the
                // wire, so the same silent discard was always harmless there).
                let want = req.size.max(0) as usize;
                let mut body = leftover;
                body.truncate(want); // leftover can't legitimately exceed the announced size
                if body.len() < want {
                    let mut rest = vec![0u8; want - body.len()];
                    s.read_exact(&mut rest).await?;
                    body.extend_from_slice(&rest);
                }
                let resp = learner
                    .handle_install_snapshot(&req, &body)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_install_snapshot_response(&resp))
            }
            raft_wire::RpcType::TimeoutNow => {
                let (req, _leftover) =
                    decode_one(&mut s, raft_wire::decode_timeout_now_request).await?;
                let resp = learner
                    .handle_timeout_now(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_timeout_now_response(&resp))
            }
        };
        s.write_all(&reply_bytes).await?;
    }
}

/// Reads exactly one msgpack value off `s` and decodes it with `decode`,
/// growing the read buffer a chunk at a time until decoding stops failing
/// on truncation. `AppendEntriesRequest`'s `Entries` can be arbitrarily
/// large, so there is no fixed size to read up front the way
/// `ipcproto`/`raft_wire`'s own fixed-size reply framing allows.
///
/// Returns any bytes read past the end of the decoded value alongside it,
/// rather than discarding them: a single `s.read()` has no reason to stop
/// exactly at a msgpack value's boundary, so a chunk can come back carrying
/// the start of whatever follows on the wire too. Every RPC here except
/// `InstallSnapshot` has nothing following its own header on the same
/// stream, so an empty leftover is the normal case for those -- but
/// `InstallSnapshot`'s raw snapshot body comes right after its header (see
/// that match arm's doc comment), and discarding a chunk's leftover bytes
/// used to silently drop the start of that body, always coming up short on
/// the subsequent fixed-size read.
async fn decode_one<T>(
    s: &mut libp2p::Stream,
    decode: impl Fn(&mut crate::msgpack::Reader) -> crate::msgpack::Result<T>,
) -> Result<(T, Vec<u8>), Error> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let mut r = crate::msgpack::Reader::new(&buf);
        match decode(&mut r) {
            Ok(v) => {
                let consumed = r.pos();
                return Ok((v, buf[consumed..].to_vec()));
            }
            Err(crate::msgpack::Error::UnexpectedEof) => {
                let n = s.read(&mut chunk).await.map_err(|e| Error(e.to_string()))?;
                if n == 0 {
                    return Err(Error("stream closed mid-message".into()));
                }
                buf.extend_from_slice(&chunk[..n]);
            }
            Err(e) => return Err(Error(format!("{e:?}"))),
        }
    }
}
