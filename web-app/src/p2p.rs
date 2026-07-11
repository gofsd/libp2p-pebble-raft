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
use futures::{AsyncReadExt, AsyncWriteExt, SinkExt, StreamExt};
use libp2p::{
    identity::Keypair, noise, relay, swarm::NetworkBehaviour, webtransport_websys, Multiaddr,
    PeerId, StreamProtocol, Swarm, SwarmBuilder,
};
use libp2p_stream as stream;

use crate::raft_wire;

/// Matches `pkg/rafttransport.ProtocolID` exactly.
pub const RAFT_PROTOCOL: StreamProtocol = StreamProtocol::new("/libp2p-kv-raft/raft/1.0.0");
/// Matches `pkg/daemon.ClientProtocolID` exactly.
pub const CLIENT_PROTOCOL: StreamProtocol = StreamProtocol::new("/libp2p-kv-raft/client/1.0.0");

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
                            let result = self.do_reserve(addr).await;
                            let _ = reply.send(result);
                        }
                        None => {}
                    }
                }
            }
        }
    }

    async fn do_reserve(&mut self, addr: Multiaddr) -> Result<Multiaddr, Error> {
        self.swarm
            .dial(addr.clone())
            .map_err(|e| Error(e.to_string()))?;

        let relay_peer = loop {
            match self.swarm.select_next_some().await {
                libp2p::swarm::SwarmEvent::ConnectionEstablished { peer_id, .. } => {
                    break peer_id;
                }
                libp2p::swarm::SwarmEvent::OutgoingConnectionError { error, .. } => {
                    return Err(Error(format!("dial relay: {error}")));
                }
                _ => {}
            }
        };

        let circuit_addr: Multiaddr = format!("/p2p/{relay_peer}/p2p-circuit").parse()?;
        self.swarm
            .listen_on(circuit_addr.clone())
            .map_err(|e| Error(e.to_string()))?;

        loop {
            match self.swarm.select_next_some().await {
                libp2p::swarm::SwarmEvent::NewListenAddr { address, .. }
                    if address.to_string().contains("p2p-circuit") =>
                {
                    return Ok(address.with(libp2p::multiaddr::Protocol::P2p(
                        *self.swarm.local_peer_id(),
                    )));
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
        while let Some((_peer, stream)) = incoming.next().await {
            let learner = learner.clone();
            wasm_bindgen_futures::spawn_local(async move {
                if let Err(e) = serve_raft_stream(stream, learner).await {
                    web_sys::console::log_1(&format!("kv-raft-web: raft stream: {e}").into());
                }
            });
        }
        Ok(())
    }

    /// Speaks `CLIENT_PROTOCOL` to `target` for one request/response round
    /// trip -- used for the `ActionAdd` join handshake and for forwarded
    /// Set/Get, exactly like the desktop CLI and the Android app's
    /// relationship to their own daemon (see `pkg/daemon.ClientProtocolID`),
    /// just re-encoded with this crate's own [`crate::ipcproto`] port.
    pub async fn call_client_protocol(
        &mut self,
        target: PeerId,
        req: &crate::ipcproto::Request,
    ) -> Result<crate::ipcproto::Response, Error> {
        let mut s = self
            .control
            .open_stream(target, CLIENT_PROTOCOL.clone())
            .await
            .map_err(|e| Error(e.to_string()))?;
        let buf = req.encode();
        s.write_all(&buf).await?;
        s.close().await?;

        let mut resp_buf = vec![0u8; crate::ipcproto::RESPONSE_SIZE];
        s.read_exact(&mut resp_buf).await?;
        crate::ipcproto::Response::decode(&resp_buf).map_err(|e| Error(e.to_string()))
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

        // Every request struct decodes from one msgpack value with no
        // length prefix, so read incrementally: grow a buffer and retry
        // the decode until it succeeds, matching how a streaming
        // `codec.Decoder` on the Go side consumes exactly one value's
        // worth of bytes off the wire without a separate framing layer.
        let reply_bytes = match rpc_type {
            raft_wire::RpcType::AppendEntries => {
                let req = decode_one(&mut s, raft_wire::decode_append_entries_request).await?;
                let resp = learner
                    .handle_append_entries(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_append_entries_response(&resp))
            }
            raft_wire::RpcType::RequestVote => {
                let req = decode_one(&mut s, raft_wire::decode_request_vote_request).await?;
                let resp = learner
                    .handle_request_vote(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_request_vote_response(&resp))
            }
            raft_wire::RpcType::RequestPreVote => {
                let req = decode_one(&mut s, raft_wire::decode_request_pre_vote_request).await?;
                let resp = learner
                    .handle_request_pre_vote(&req)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_request_pre_vote_response(&resp))
            }
            raft_wire::RpcType::InstallSnapshot => {
                let req = decode_one(&mut s, raft_wire::decode_install_snapshot_request).await?;
                let mut body = vec![0u8; req.size.max(0) as usize];
                s.read_exact(&mut body).await?;
                let resp = learner
                    .handle_install_snapshot(&req, &body)
                    .map_err(|e| Error(e.to_string()))?;
                raft_wire::encode_reply(None, &raft_wire::encode_install_snapshot_response(&resp))
            }
            raft_wire::RpcType::TimeoutNow => {
                let req = decode_one(&mut s, raft_wire::decode_timeout_now_request).await?;
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
async fn decode_one<T>(
    s: &mut libp2p::Stream,
    decode: impl Fn(&mut crate::msgpack::Reader) -> crate::msgpack::Result<T>,
) -> Result<T, Error> {
    let mut buf = Vec::new();
    let mut chunk = [0u8; 4096];
    loop {
        let mut r = crate::msgpack::Reader::new(&buf);
        match decode(&mut r) {
            Ok(v) => return Ok(v),
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
