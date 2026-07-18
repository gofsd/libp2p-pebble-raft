// Package daemon implements the long-running kvnode process: a libp2p host,
// a hashicorp/raft node backed by pkg/kvfsm/pkg/store, and a pkg/ipc server
// that a local mage CLI invocation drives with add/set/get requests.
//
// A daemon always starts "unconfigured": it has an identity and a raft
// instance, but no cluster role until it receives a pkg/shmevent EventAdd
// request telling it whether to bootstrap as the cluster's sole leader or
// to join an existing leader. This lets the same binary serve every
// `mage addnode` case (new leader, new follower, or rejoining after a
// restart) with identical startup code.
package daemon

import (
	"bufio"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/hashicorp/raft"
	raftboltdb "github.com/hashicorp/raft-boltdb/v2"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/protocol"
	v2relay "github.com/libp2p/go-libp2p/p2p/protocol/circuitv2/relay"
	"github.com/multiformats/go-multiaddr"
	manet "github.com/multiformats/go-multiaddr/net"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/kvfsm"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
	"github.com/gofsd/libp2p-kv-raft/pkg/store"
)

// JoinProtocolID is the libp2p protocol a joining node uses to ask an
// existing leader to add it as a voter.
const JoinProtocolID = protocol.ID("/libp2p-kv-raft/join/1.0.0")

// ForwardProtocolID is the libp2p protocol a non-leader node uses to relay
// a Set to the current raft leader on the caller's behalf. Needed because
// pkg/ipc is a same-machine-only protocol -- a node has no other way to
// reach whichever peer is actually the raft leader -- and raft itself
// deliberately has no client-request forwarding built in; that's left to
// the application. In particular, the Android build of this project runs
// as a follower with no separate "find the leader" step available to its
// UI, so every Set it issues needs this to reach the leader at all.
const ForwardProtocolID = protocol.ID("/libp2p-kv-raft/forward-set/1.0.0")

// ForwardConfirmProtocolID is the libp2p protocol a non-leader node uses
// to relay an EventPermitConfirm to the current raft leader, mirroring
// ForwardProtocolID's role for Set. It's a separate protocol (rather than
// overloading ForwardProtocolID's existing OpSet-only handling) because
// its handler, unlike handleForwardSetStream, must check the identity of
// whoever actually opened the stream against the leader's live raft
// configuration before applying -- see handleForwardConfirmStream's doc
// comment.
const ForwardConfirmProtocolID = protocol.ID("/libp2p-kv-raft/forward-confirm/1.0.0")

// ForwardJoinProtocolID is the libp2p protocol a non-leader node uses to
// relay a Join request to the current raft leader on the joining node's
// behalf, mirroring ForwardProtocolID's role for Set and for the same
// reason: raft leadership can move to any voter at any time -- this
// project watched it happen mid-session -- so whichever cluster member a
// new node was told to join through (e.g. a leader address baked into an
// Android build at compile time) may no longer be the leader by the time
// it actually tries.
const ForwardJoinProtocolID = protocol.ID("/libp2p-kv-raft/forward-join/1.0.0")

// ClientProtocolID is the libp2p protocol a remote client with no local
// pkg/ipc channel of its own speaks directly to any cluster node to issue
// Add/Set/Get requests -- namely the browser build in web-app/ (rust-libp2p
// compiled to wasm), which has no shared process with this daemon the way
// the Android build's in-process kvmobile does, and dials in over the
// WebTransport listener newHost adds for exactly this. ActionAdd here means
// something different than it does over pkg/ipc: a browser tab can never
// accept a raw inbound connection, so it can never be a raft *voter* (a
// voter's transport must be independently dialable by any other voter at
// any time -- see rafttransport's doc comment), but it *can* be dialed
// through a circuit-relay v2 reservation it already holds (the same
// mechanism an Android device behind carrier-grade NAT already relies on --
// see Config.RelayPeer), which makes it a real raft *non-voter* (learner):
// Key carries the browser's own peer id, Value its reserved
// /p2p-circuit multiaddr -- see handleAddLearner.
const ClientProtocolID = protocol.ID("/libp2p-kv-raft/client/1.0.0")

// ExecuteProtocolID is the libp2p protocol one raft node uses to deliver an
// EventExecute notification directly to another, peer-to-peer -- see that
// event's doc comment in pkg/shmevent. Unlike every other protocol in this
// file, the message it carries never touches raft or the store at either
// end: handleExecuteStream just verifies it and queues it in the
// receiving Node's executeInbox for a local caller to drain via
// EventPollExecute.
const ExecuteProtocolID = protocol.ID("/libp2p-kv-raft/execute/1.0.0")

// ReadyFileName is written to Config.DataDir once the daemon's host and IPC
// server are up, so the spawning `mage addnode` can learn the node's peer id
// and listen addresses without parsing stdout.
const ReadyFileName = "ready.json"

// Config configures a single node process.
type Config struct {
	DataDir string // root directory for this node's identity, sqlite, and raft data
	KeyPath string // libp2p identity key (already generated by the caller)

	// ListenPort is the TCP/QUIC port to listen on. 0 (the default) picks an
	// ephemeral port, fine for same-machine/LAN use. A publicly reachable
	// deployment should pin this to a known port so it can be opened in a
	// firewall/security group.
	ListenPort int

	// RelayService makes this node act as a circuit-relay v2 point for
	// other nodes that can't be dialed directly (the "worst case" NAT
	// fallback) and forces it to advertise itself as publicly reachable.
	// Only enable this on a node known to actually have a public,
	// unfiltered address -- e.g. the leader deployed on a public VPS.
	// Every node, regardless of this flag, can still *use* a relay
	// (EnableRelay/EnableHolePunching are always on); this flag only
	// controls whether it also serves as one for others.
	RelayService bool

	// RelayPeer is a known circuit-relay v2 server's multiaddr (a node
	// running with RelayService=true) this node should proactively reserve
	// a relay slot through, so it ends up with a /p2p-circuit address that
	// someone can dial it on even though it has no directly-dialable
	// address of its own -- e.g. a phone on a cellular connection behind
	// carrier-grade NAT, which blocks *inbound* connections entirely.
	// EnableRelay/EnableHolePunching (always on regardless of this field)
	// only let a node *use* a relay connection someone else already set
	// up; without a static relay target here, a node that nothing can dial
	// directly has no way to make its own reservation, and ends up
	// advertising only addresses that raft's leader can never use to send
	// it AppendEntries -- leaving it permanently stuck as a voter that
	// never learns who the leader is. Leave empty for a node with a real
	// public or otherwise directly-dialable address, where a reservation
	// would just be wasted overhead.
	RelayPeer string

	// Raft timing knobs. Zero means "use hashicorp/raft's own default"
	// (1s heartbeat/election, 50ms commit, 500ms leader lease) -- values
	// the raft project itself considers safe for real networks, not just
	// a fast LAN/loopback. Override with smaller values only where the
	// network genuinely warrants it (e.g. same-machine integration
	// tests): tightening these for a real WAN deployment is what causes
	// spurious "leadership lost" elections when latency/jitter occasionally
	// exceeds an aggressive timeout, especially in a small cluster where
	// every node's vote is required.
	HeartbeatTimeout   time.Duration
	ElectionTimeout    time.Duration
	CommitTimeout      time.Duration
	LeaderLeaseTimeout time.Duration

	// SnapshotThreshold/SnapshotInterval bound how large a long-lived
	// leader's raft log is allowed to grow between compactions. Zero means
	// "use hashicorp/raft's own default" (8192 entries / 120s), which is
	// tuned for a cluster that mostly just needs *a* periodic snapshot, not
	// one where a brand new non-voter might join long after the log has
	// grown into the thousands: every fresh join replays the entire log
	// from index 1 up to the most recent snapshot, one entry at a time, so
	// a leader that's been running a long time without ever snapshotting
	// makes every subsequent join progressively slower -- observed directly
	// against this project's own long-lived e2e deploy target, where a
	// newly-joined browser tab had to replay well over a thousand mostly-
	// empty heartbeat entries before reaching the one write it actually
	// needed. A lower threshold trades more frequent (cheap, incremental)
	// snapshotting for a join's replay being bounded by TrailingLogs
	// instead of the leader's entire lifetime log.
	SnapshotThreshold uint64
	SnapshotInterval  time.Duration

	// TrailingLogs is how many of the most recent log entries a snapshot
	// leaves in place instead of compacting away. Zero means hashicorp/
	// raft's own default (10240), which -- combined with a lowered
	// SnapshotThreshold above -- can still leave a snapshot compacting
	// nothing at all: a log under 10240 entries total has nothing eligible
	// for removal regardless of how often it snapshots, so a fresh non-
	// voter join still replays the whole thing. Set this alongside
	// SnapshotThreshold, not instead of it, for a snapshot to actually
	// shrink what a new join has to replay.
	TrailingLogs uint64

	// RequirePermitForRemote gates every remote (ClientProtocolID) request
	// other than EventPermitRequest/EventPermitConfirm on the caller
	// having a confirmed KindPermitPeer record (see pkg/shmevent's
	// SystemKey/EventPermitRequest doc comments) for its own
	// libp2p-authenticated peer id. Defaults to false: today's behavior,
	// where any remote caller that signs with its own key (see
	// callerIdentity) is honored with no separate allow-listing. Turning
	// this on requires an operator to RequestPermit+ConfirmPermit every
	// remote peer first -- including test/e2e/web-app callers -- via
	// pkg/kvctl's RequestPermit/ConfirmPermit (mage requestpermit/
	// confirmpermit, kvctl-cli requestpermit/confirmpermit).
	RequirePermitForRemote bool

	// RequirePermitForRelay gates this node's circuit-relay v2 service
	// (only meaningful alongside RelayService) on the reserving/connecting
	// peer having a confirmed KindPermitPeer record -- see that kind's doc
	// comment ("permission for a peer to join/use the cluster's relay").
	// Defaults to false: today's behavior, where any peer can reserve a
	// relay slot or open a relayed circuit through this node with no
	// allow-listing at all. Independent of RequirePermitForRemote, which
	// gates the unrelated shmevent/ClientProtocolID RPC layer -- a node
	// can restrict one without the other. Turning this on requires an
	// operator to RequestPermit+ConfirmPermit every peer that needs relay
	// access first, same as RequirePermitForRemote.
	RequirePermitForRelay bool
}

// Node is a running daemon instance. Its raft/transport fields are nil
// until the first EventAdd request initializes them (see
// initRaft): constructing raft.NewRaft starts its election-timeout loop
// immediately, so doing that unconditionally at process startup -- before
// mage has had a chance to deliver the Add request that decides whether
// this node bootstraps or joins -- creates a race where the node times out
// waiting for a leader, becomes a candidate, and bumps its persisted term.
// That alone makes raft.HasExistingState true and BootstrapCluster then
// fails with "bootstrap only works on new clusters". Deferring
// raft.NewRaft to initRaft, invoked synchronously inside the Add handler,
// closes that window.
type Node struct {
	cfg    Config
	host   lp2phost.Host
	store  *store.Store
	peerID string

	// ed25519Priv/ed25519Pub are this node's libp2p identity key, in
	// stdlib crypto/ed25519's portable raw form -- what
	// EventGetPrivateKey/EventGetPublicKey hand out, and what every
	// pkg/shmevent message this node sends/verifies is signed/checked
	// against. See pkg/shmevent's doc comment on why local callers share
	// this same key rather than provisioning one of their own.
	ed25519Priv shmevent.PrivateKey
	ed25519Pub  shmevent.PublicKey

	// registry backs pkg/shmevent's EventSetKey/EventGetKey and the
	// SourceID-addressed forms of EventSetField/EventGetField/EventAdd.
	registry *shmevent.Registry

	// executeInbox queues EventExecute notifications (see that event's doc
	// comment) delivered to this node over ExecuteProtocolID, for
	// EventPollExecute to drain. Purely in-memory and never persisted --
	// unlike everything else this daemon handles, a queued notification
	// that's lost on restart is an accepted trade-off, not a correctness
	// bug (see executeInbox's own doc comment).
	executeInbox *executeInbox

	logStore  *raftboltdb.BoltStore
	snapStore raft.SnapshotStore

	mu              sync.RWMutex
	raft            *raft.Raft
	transport       *raft.NetworkTransport
	electionTimeout time.Duration // the effective value raft is actually using, set by initRaft

	// leadershipObserver/leadershipObsCh back watchLeadership -- see
	// initRaft's registration of them. Torn down in shutdown so that
	// goroutine doesn't leak past this Node's lifetime (relevant mainly
	// for tests, which construct many short-lived Nodes in one process).
	leadershipObserver *raft.Observer
	leadershipObsCh    chan raft.Observation
}

// maxExecuteInbox bounds executeInbox: a queue nothing ever drains (no
// local caller ever polls) would otherwise grow without limit as long as
// other nodes keep sending EventExecute notifications. Past this many
// pending entries, the oldest is dropped to make room for the newest --
// same trade-off a best-effort notification queue with no persistence
// already implies (see executeInbox's doc comment).
const maxExecuteInbox = 256

// executeNotification is one queued EventExecute delivery: senderPeerID is
// the string pkg/shmevent.DecodeExecuteNotification returned (the sending
// node's own peer id, already signature-verified against it by
// handleExecuteStream before queuing), payload is that same call's payload.
type executeNotification struct {
	senderPeerID []byte
	payload      []byte
}

// executeInbox is a bounded FIFO queue of executeNotification, guarded by
// a mutex -- deliberately the simplest thing that could work rather than
// a channel, since EventPollExecute needs a non-blocking "is anything
// there" drain (a closed/empty channel read blocks or needs a select,
// where a plain slice-under-a-mutex just returns ok=false).
type executeInbox struct {
	mu      sync.Mutex
	entries []executeNotification
}

func newExecuteInbox() *executeInbox {
	return &executeInbox{}
}

func (q *executeInbox) push(senderPeerID, payload []byte) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) >= maxExecuteInbox {
		q.entries = q.entries[1:]
	}
	q.entries = append(q.entries, executeNotification{senderPeerID: senderPeerID, payload: payload})
}

func (q *executeInbox) pop() (executeNotification, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.entries) == 0 {
		return executeNotification{}, false
	}
	n := q.entries[0]
	q.entries = q.entries[1:]
	return n, true
}

// Run starts a node and blocks, serving IPC requests, until ctx is
// cancelled. It always returns a non-nil error except on clean shutdown via
// ctx cancellation.
func Run(ctx context.Context, cfg Config) error {
	n, err := start(cfg)
	if err != nil {
		return err
	}
	defer n.shutdown()

	// A node that already has persisted raft state -- because it was
	// bootstrapped or joined before, e.g. across a restart -- doesn't need
	// (and, since BootstrapCluster now correctly refuses a non-empty log,
	// can't use) an EventAdd to become operational again: raft.NewRaft
	// recovers the last known configuration and log from disk on its own.
	// Resume immediately so Set/Get work right away, with no coordination
	// step at all, as long as this node is still reachable at whatever
	// address that recovered configuration expects (true if -listen-port
	// is pinned across restarts). A caller that also needs to re-announce
	// a changed address to the current leader still sends an EventAdd
	// with a leader address; handleAdd's join path works whether or not
	// this already ran, since initRaft is idempotent.
	hasState, err := raft.HasExistingState(n.logStore, n.logStore, n.snapStore)
	if err != nil {
		return fmt.Errorf("daemon: check existing raft state: %w", err)
	}
	if hasState {
		if _, err := n.initRaft(); err != nil {
			return fmt.Errorf("daemon: resume raft: %w", err)
		}
	}

	if err := n.writeReadyFile(); err != nil {
		return fmt.Errorf("daemon: write ready file: %w", err)
	}

	return ipc.Serve(ctx, n.peerID, n.ed25519Priv, func(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte) shmevent.Msg {
		return n.handleShmEvent(ctx, m, crc, sig, n.localCaller())
	})
}

func start(cfg Config) (*Node, error) {
	if err := os.MkdirAll(cfg.DataDir, 0o755); err != nil {
		return nil, fmt.Errorf("daemon: create data dir: %w", err)
	}

	priv, err := loadKey(cfg.KeyPath)
	if err != nil {
		return nil, fmt.Errorf("daemon: load identity: %w", err)
	}
	peerID, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("daemon: derive peer id: %w", err)
	}

	sqliteDir := filepath.Join(cfg.DataDir, "sqlite")
	st, err := store.Open(sqliteDir)
	if err != nil {
		return nil, fmt.Errorf("daemon: open store: %w", err)
	}

	h, err := newHost(priv, cfg, st)
	if err != nil {
		st.Close()
		return nil, fmt.Errorf("daemon: create libp2p host: %w", err)
	}

	raftDir := filepath.Join(cfg.DataDir, "raft")
	if err := os.MkdirAll(raftDir, 0o755); err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: create raft dir: %w", err)
	}

	logStore, err := raftboltdb.NewBoltStore(filepath.Join(raftDir, "raft.db"))
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: open raft log store: %w", err)
	}

	snapDir := filepath.Join(raftDir, "snapshots")
	if err := os.MkdirAll(snapDir, 0o755); err != nil {
		st.Close()
		h.Close()
		return nil, err
	}
	snapStore, err := raft.NewFileSnapshotStore(snapDir, 2, io.Discard)
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: open snapshot store: %w", err)
	}

	ed25519Priv, err := priv.Raw()
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: raw identity private key: %w", err)
	}
	ed25519Pub, err := priv.GetPublic().Raw()
	if err != nil {
		st.Close()
		h.Close()
		return nil, fmt.Errorf("daemon: raw identity public key: %w", err)
	}

	n := &Node{
		cfg:          cfg,
		host:         h,
		store:        st,
		peerID:       peerID.String(),
		ed25519Priv:  ed25519Priv,
		ed25519Pub:   ed25519Pub,
		registry:     shmevent.NewRegistry(),
		executeInbox: newExecuteInbox(),
		logStore:     logStore,
		snapStore:    snapStore,
	}
	h.SetStreamHandler(JoinProtocolID, n.handleJoinStream)
	h.SetStreamHandler(ForwardProtocolID, n.handleForwardSetStream)
	h.SetStreamHandler(ForwardConfirmProtocolID, n.handleForwardConfirmStream)
	h.SetStreamHandler(ExecuteProtocolID, n.handleExecuteStream)
	h.SetStreamHandler(ForwardJoinProtocolID, n.handleForwardJoinStream)
	h.SetStreamHandler(ClientProtocolID, n.handleClientStream)
	return n, nil
}

// newHost builds this node's libp2p host. Every node gets relay-client and
// hole-punching capability unconditionally, so it can be dialed through
// (or dial through) a circuit relay when a direct connection isn't
// possible -- the "worst case" NAT fallback. A node only advertises itself
// as a relay *for others* (RelayService) and forces public reachability
// when the caller knows it actually has one, e.g. the leader on a public
// VPS; the resource limits mirror the standalone relay in
// pkg/raft/node.go's StartRelayNode. st is only consulted when
// cfg.RequirePermitForRelay is set (see relayACL); it's threaded in here,
// ahead of any *Node existing, because the ACL closure needs to read
// confirmed KindPermitPeer records live -- one already-open *store.Store,
// not a snapshot taken at host-construction time.
func newHost(priv crypto.PrivKey, cfg Config, st *store.Store) (lp2phost.Host, error) {
	opts := []libp2p.Option{
		libp2p.Identity(priv),
		libp2p.ListenAddrStrings(
			fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", cfg.ListenPort),
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1", cfg.ListenPort),
			// Shares the quic-v1 UDP port above (WebTransport is a session
			// layered on the same QUIC socket, not a separate listener).
			// The webtransport transport module itself is already part of
			// go-libp2p's default transport set (this Config never calls
			// libp2p.Transport, so DefaultTransports applies); only the
			// listen address was missing, which is why every other node
			// this project has run so far -- none of them reachable from a
			// browser -- never noticed. n.host.Addrs() will report the
			// resulting address with its /certhash component appended
			// automatically, so advertisedAddrs()/ready.json need no
			// change to start including it. See web-app/ for the browser
			// client (js-libp2p, since go-libp2p itself has no usable
			// browser-sandbox transport) that dials this.
			fmt.Sprintf("/ip4/0.0.0.0/udp/%d/quic-v1/webtransport", cfg.ListenPort),
		),
		libp2p.EnableRelay(),
		libp2p.EnableHolePunching(),
	}

	if cfg.RelayService {
		rc := v2relay.DefaultResources()
		rc.Limit = &v2relay.RelayLimit{
			Duration: time.Hour,
			Data:     1 << 30, // 1 GB
		}
		rc.ReservationTTL = time.Hour
		rc.MaxReservations = 256
		rc.MaxCircuits = 256
		rc.BufferSize = 4096

		relayOpts := []v2relay.Option{v2relay.WithResources(rc)}
		if cfg.RequirePermitForRelay {
			relayOpts = append(relayOpts, v2relay.WithACL(relayACL{store: st}))
		}
		opts = append(opts,
			libp2p.EnableRelayService(relayOpts...),
			libp2p.ForceReachabilityPublic(),
		)
	}

	if cfg.RelayPeer != "" {
		maddr, err := multiaddr.NewMultiaddr(cfg.RelayPeer)
		if err != nil {
			return nil, fmt.Errorf("invalid relay peer address %q: %w", cfg.RelayPeer, err)
		}
		info, err := peer.AddrInfoFromP2pAddr(maddr)
		if err != nil {
			return nil, fmt.Errorf("relay peer address %q missing peer id: %w", cfg.RelayPeer, err)
		}
		// AutoRelay only actively reserves a relay slot once it believes
		// this host is privately reachable, a judgment it otherwise leaves
		// to AutoNAT -- which can be slow, or simply wrong on a network
		// (like this project's own test environment) that looks publicly
		// dialable but isn't actually reachable by the specific peer that
		// matters (the raft leader). RelayPeer is only ever set by a caller
		// who already knows this node needs a relay to be reached at all
		// (see the field's doc comment), so force that judgment instead of
		// leaving the reservation -- and therefore the /p2p-circuit address
		// join()'s awaitRelayAddr waits for -- contingent on AutoNAT.
		opts = append(opts,
			libp2p.ForceReachabilityPrivate(),
			libp2p.EnableAutoRelayWithStaticRelays([]peer.AddrInfo{*info}),
		)
	}

	return libp2p.New(opts...)
}

// initRaft lazily constructs the raft transport and raft.Raft instance. It
// must be called at most once, synchronously, from the EventAdd handler.
func (n *Node) initRaft() (*raft.Raft, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.raft != nil {
		return n.raft, nil
	}

	transport := rafttransport.NewTransport(n.host, 10*time.Second)

	raftConf := raft.DefaultConfig()
	raftConf.LocalID = raft.ServerID(n.peerID)
	if n.cfg.HeartbeatTimeout > 0 {
		raftConf.HeartbeatTimeout = n.cfg.HeartbeatTimeout
	}
	if n.cfg.ElectionTimeout > 0 {
		raftConf.ElectionTimeout = n.cfg.ElectionTimeout
	}
	if n.cfg.CommitTimeout > 0 {
		raftConf.CommitTimeout = n.cfg.CommitTimeout
	}
	if n.cfg.LeaderLeaseTimeout > 0 {
		raftConf.LeaderLeaseTimeout = n.cfg.LeaderLeaseTimeout
	}
	if n.cfg.SnapshotThreshold > 0 {
		raftConf.SnapshotThreshold = n.cfg.SnapshotThreshold
	}
	if n.cfg.SnapshotInterval > 0 {
		raftConf.SnapshotInterval = n.cfg.SnapshotInterval
	}
	if n.cfg.TrailingLogs > 0 {
		raftConf.TrailingLogs = n.cfg.TrailingLogs
	}
	if logFile, err := os.OpenFile(filepath.Join(n.cfg.DataDir, "raft.log"), os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644); err == nil {
		raftConf.LogOutput = logFile
	}

	fsm := kvfsm.New(n.store)
	rf, err := raft.NewRaft(raftConf, fsm, n.logStore, n.logStore, n.snapStore, transport)
	if err != nil {
		transport.Close()
		return nil, fmt.Errorf("daemon: create raft node: %w", err)
	}

	n.raft = rf
	n.transport = transport
	n.electionTimeout = raftConf.ElectionTimeout

	// Registered before this function returns to whichever caller then
	// calls BootstrapCluster (handleAdd's bootstrap branch) -- so this
	// node's very first self-election, not just later re-elections, is
	// caught too.
	obsCh := make(chan raft.Observation, 8)
	observer := raft.NewObserver(obsCh, false, func(o *raft.Observation) bool {
		_, ok := o.Data.(raft.LeaderObservation)
		return ok
	})
	rf.RegisterObserver(observer)
	n.leadershipObserver = observer
	n.leadershipObsCh = obsCh
	go n.watchLeadership(rf, obsCh)

	return rf, nil
}

// watchLeadership reacts to every leadership-change notification (see
// initRaft's Observer registration) by re-asserting this node's own
// current truth in its KindClusterMember record -- not tracking "who used
// to be leader": deliberately stateless/idempotent, so a redundant
// identical write is harmless and a missed one self-corrects on the next
// transition. Returns once ch is closed (shutdown).
func (n *Node) watchLeadership(rf *raft.Raft, ch chan raft.Observation) {
	for range ch {
		role, ok := n.ownCurrentRole(rf)
		if !ok {
			continue
		}
		if err := n.recordClusterMember(context.Background(), n.peerID, role); err != nil {
			fmt.Fprintf(os.Stderr, "daemon: record own cluster member status: %v\n", err)
		}
	}
}

// ownCurrentRole determines this node's own current role: RoleLeader if
// it's currently raft.Leader, else RoleVoter/RoleLearner per its own
// suffrage in the current configuration. ok is false if this node isn't
// (yet) present in the configuration at all.
func (n *Node) ownCurrentRole(rf *raft.Raft) (role byte, ok bool) {
	if rf.State() == raft.Leader {
		return shmevent.RoleLeader, true
	}
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		return 0, false
	}
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(n.peerID) {
			if srv.Suffrage == raft.Nonvoter {
				return shmevent.RoleLearner, true
			}
			return shmevent.RoleVoter, true
		}
	}
	return 0, false
}

// getRaft returns the raft instance if initRaft has already run.
func (n *Node) getRaft() *raft.Raft {
	n.mu.RLock()
	defer n.mu.RUnlock()
	return n.raft
}

func loadKey(keyPath string) (crypto.PrivKey, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return nil, fmt.Errorf("read key file %s: %w", keyPath, err)
	}
	raw, err := hex.DecodeString(string(data))
	if err != nil {
		return nil, fmt.Errorf("decode key file %s: %w", keyPath, err)
	}
	return crypto.UnmarshalPrivateKey(raw)
}

func (n *Node) shutdown() {
	n.mu.Lock()
	rf, transport := n.raft, n.transport
	observer, obsCh := n.leadershipObserver, n.leadershipObsCh
	n.mu.Unlock()

	if rf != nil {
		if observer != nil {
			// Deregister before closing: RegisterObserver/DeregisterObserver
			// and observe() share a lock, so once this returns, raft can no
			// longer be mid-send on obsCh, and closing it is safe -- stops
			// watchLeadership's goroutine instead of leaking it past this
			// Node's lifetime (relevant mainly for tests, which construct
			// many short-lived Nodes in one process).
			rf.DeregisterObserver(observer)
			close(obsCh)
		}
		rf.Shutdown()
	}
	if transport != nil {
		transport.Close()
	}
	if n.store != nil {
		n.store.Close()
	}
	if n.host != nil {
		n.host.Close()
	}
}

// advertisedAddrs returns this node's dialable multiaddrs, each including a
// trailing /p2p/<peer-id> component.
// advertisedAddrs returns this node's own dialable addresses, best first:
// this is what gets baked into raft's persisted cluster configuration
// (index [0], for both a bootstrapping leader's own address and a joining
// follower's self-reported address) and what a peer that has never
// connected to this node before -- e.g. after a restart tears down any
// existing connection -- has to work with to dial it fresh.
//
// n.host.Addrs() carries no ordering guarantee that favors reachability, so
// a multi-homed host (a VPS with both a public IP and a private/VPN
// interface, like the one this project targets for a real remote
// deployment) can easily end up with a private address in [0] purely by
// interface enumeration order. That address is often not just suboptimal
// but entirely undialable by anyone outside that private network -- and
// unlike the peerstore, which can accumulate additional observed addresses
// from a successful connection, raft's configuration stores exactly one
// address per voter, so getting it wrong here is not recoverable by any
// other layer once persisted. Sort public addresses first, then
// private/unspecified, then loopback last (only useful for same-machine
// setups, and worse than everything else for a real multi-host one).
func (n *Node) advertisedAddrs() []string {
	hostAddrs := n.host.Addrs()

	const (
		scorePublic = iota
		scoreRelay
		scoreOther
		scoreLoopback
	)
	score := func(a multiaddr.Multiaddr) int {
		switch {
		case manet.IsPublicAddr(a):
			return scorePublic
		// A /p2p-circuit address is a relay reservation (see Config.RelayPeer):
		// unlike a raw private/NAT address, it's actually dialable by
		// whoever needs to reach this node, so it belongs ahead of the
		// "other" tier even though it isn't a direct address either.
		case strings.Contains(a.String(), "/p2p-circuit"):
			return scoreRelay
		case manet.IsIPLoopback(a):
			return scoreLoopback
		default:
			return scoreOther
		}
	}
	sorted := make([]multiaddr.Multiaddr, len(hostAddrs))
	copy(sorted, hostAddrs)
	sort.SliceStable(sorted, func(i, j int) bool { return score(sorted[i]) < score(sorted[j]) })

	addrs := make([]string, 0, len(sorted))
	for _, a := range sorted {
		addrs = append(addrs, fmt.Sprintf("%s/p2p/%s", a, n.peerID))
	}
	return addrs
}

// awaitRelayAddr waits up to timeout for a /p2p-circuit address -- proof
// the Config.RelayPeer reservation configured in newHost has completed --
// to appear in n.host.Addrs(). A no-op that returns immediately when
// RelayPeer isn't set, since there's then nothing to wait for.
func (n *Node) awaitRelayAddr(timeout time.Duration) {
	if n.cfg.RelayPeer == "" {
		return
	}
	deadline := time.After(timeout)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		for _, a := range n.host.Addrs() {
			if strings.Contains(a.String(), "/p2p-circuit") {
				return
			}
		}
		select {
		case <-deadline:
			return
		case <-ticker.C:
		}
	}
}

// ReadyInfo is the content of ReadyFileName: what the spawning `mage
// addnode` needs to learn about a freshly started node before it can
// register it and trigger its EventAdd bootstrap.
type ReadyInfo struct {
	PeerID      string   `json:"peer_id"`
	ListenAddrs []string `json:"listen_addrs"`
}

// ReadReadyFile reads and parses the ReadyFileName written by a node in
// dataDir. It returns an error (wrapping fs.ErrNotExist) if the node hasn't
// written it yet.
func ReadReadyFile(dataDir string) (ReadyInfo, error) {
	var info ReadyInfo
	data, err := os.ReadFile(filepath.Join(dataDir, ReadyFileName))
	if err != nil {
		return info, err
	}
	if err := json.Unmarshal(data, &info); err != nil {
		return info, fmt.Errorf("daemon: parse ready file: %w", err)
	}
	return info, nil
}

func (n *Node) writeReadyFile() error {
	info := ReadyInfo{PeerID: n.peerID, ListenAddrs: n.advertisedAddrs()}
	data, err := json.Marshal(info)
	if err != nil {
		return err
	}
	path := filepath.Join(n.cfg.DataDir, ReadyFileName)
	tmp := path + ".tmp"
	if err := os.WriteFile(tmp, data, 0o644); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// handleShmEvent dispatches one decoded pkg/shmevent.Msg to the appropriate
// callerIdentity is who handleShmEvent should treat as the sender of a
// request: the public key its signature must verify against, and -- for a
// genuinely remote, libp2p-authenticated caller -- the peer id that
// identity resolves to. The zero value means "local pkg/ipc caller":
// verify against this node's own shared key, exactly as before (see
// pkg/shmevent's doc comment on the shmring same-machine trust boundary).
// remotePeer being non-empty is also what lets handleShmEvent tell a
// genuinely remote caller apart from a local one for the
// remote-only restrictions below (no key-fetch bootstrap, optional permit
// gate) -- it is never derived from anything the message itself claims.
type callerIdentity struct {
	verifyPub  shmevent.PublicKey
	remotePeer peer.ID // "" for a local caller
}

// localCaller is every pkg/ipc (shmring) request's identity: this node's
// own shared key, the same one it hands out via EventGetPrivateKey to a
// same-machine caller with no key yet -- see pkg/shmevent's doc comment.
func (n *Node) localCaller() callerIdentity {
	return callerIdentity{verifyPub: n.ed25519Pub}
}

// remoteCaller derives a callerIdentity from s's own libp2p-authenticated
// identity (the Noise/TLS handshake's remote public key/peer id) rather
// than anything the message itself claims -- mirroring the RemotePeer()
// pattern handleForwardConfirmStream already uses for its voter-only
// confirm check.
func remoteCaller(s network.Stream) (callerIdentity, error) {
	pub := s.Conn().RemotePublicKey()
	if pub == nil {
		return callerIdentity{}, fmt.Errorf("remote caller: connection has no authenticated public key")
	}
	raw, err := pub.Raw()
	if err != nil {
		return callerIdentity{}, fmt.Errorf("remote caller: %w", err)
	}
	return callerIdentity{verifyPub: raw, remotePeer: s.Conn().RemotePeer()}, nil
}

// isPermittedPeer reports whether id has a confirmed KindPermitPeer
// record. Only consulted when Config.RequirePermitForRemote is true -- see
// that field's doc comment.
func (n *Node) isPermittedPeer(id peer.ID) bool {
	return isPermittedPeer(n.store, id)
}

// isPermittedPeer is the package-level form of the check above, taking a
// *store.Store directly rather than a *Node -- needed by relayACL, which
// is constructed inside newHost before a *Node exists yet (see start).
func isPermittedPeer(st *store.Store, id peer.ID) bool {
	_, err := st.Get(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(id.String())))
	return err == nil
}

// relayACL is the v2relay.ACLFilter wired into newHost via
// v2relay.WithACL when Config.RequirePermitForRelay is set -- it's what
// actually makes a confirmed KindPermitPeer record mean "permission for a
// peer to join/use the cluster's relay" (that kind's doc comment) rather
// than just gating the unrelated shmevent RPC layer. Both reservation
// (AllowReserve) and outgoing-connect (AllowConnect) check the peer
// that's trying to use *this* node's relay service -- the destination in
// AllowConnect is who src is dialing through the relay, not itself
// requesting anything, so it's never checked here.
type relayACL struct {
	store *store.Store
}

func (a relayACL) AllowReserve(p peer.ID, _ multiaddr.Multiaddr) bool {
	return isPermittedPeer(a.store, p)
}

func (a relayACL) AllowConnect(src peer.ID, _ multiaddr.Multiaddr, _ peer.ID) bool {
	return isPermittedPeer(a.store, src)
}

// raft/store/registry operation and returns the Msg to send back -- the
// single entry point both pkg/ipc.Serve (local shared memory) and
// handleClientStream (ClientProtocolID, the remote equivalent for a
// browser learner) call into. See pkg/shmevent's doc comment for the
// overall protocol design and api/shmevent.capnp for the wire struct.
//
// caller identifies who's asking (see callerIdentity) -- a remote caller
// gets two restrictions a local one doesn't: EventGetPrivateKey/
// EventGetPublicKey are refused outright (a remote caller always has its
// own key already -- see web-app's do_connect -- so the bootstrap
// exception that exists for a same-machine caller with no key yet has no
// legitimate remote use, and serving it remotely would just hand out this
// node's own private key to anyone able to dial it), and, if
// Config.RequirePermitForRemote is set, every event but
// EventPermitRequest/EventPermitConfirm additionally requires a confirmed
// KindPermitPeer record for the caller's own authenticated peer id.
func (n *Node) handleShmEvent(ctx context.Context, m shmevent.Msg, crc uint32, sig []byte, caller callerIdentity) shmevent.Msg {
	if caller.remotePeer != "" && (m.EventType == shmevent.EventGetPrivateKey || m.EventType == shmevent.EventGetPublicKey) {
		return errorMsg(m.ID, fmt.Errorf("%s: not available to a remote caller -- bring your own key", shmevent.EventName(m.EventType)))
	}
	if shmevent.RequiresSignature(m.EventType) {
		if err := shmevent.Verify(caller.verifyPub, m, crc, sig); err != nil {
			return errorMsg(m.ID, err)
		}
	}
	if caller.remotePeer != "" && n.cfg.RequirePermitForRemote &&
		m.EventType != shmevent.EventPermitRequest && m.EventType != shmevent.EventPermitConfirm {
		if !n.isPermittedPeer(caller.remotePeer) {
			return errorMsg(m.ID, fmt.Errorf("%s not permitted -- send permit_request and have a raft voter confirm it first", caller.remotePeer))
		}
	}

	switch m.EventType {
	case shmevent.EventSetKey:
		n.registry.Register(m.ID, m.Value)
		return shmevent.Msg{EventType: shmevent.EventSetKey, ID: m.ID, Value: m.Value}

	case shmevent.EventGetKey:
		v, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return errorMsg(m.ID, fmt.Errorf("no entry registered under id %d", m.SourceID))
		}
		return shmevent.Msg{EventType: shmevent.EventGetKey, ID: m.ID, Value: v}

	case shmevent.EventSetField:
		key, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return errorMsg(m.ID, fmt.Errorf("no key registered under id %d -- send SetKey first", m.SourceID))
		}
		if len(key) > 0 && key[0] == shmevent.SystemKeyPrefix {
			return errorMsg(m.ID, fmt.Errorf("key namespace starting with 0x00 is reserved for system use"))
		}
		if err := n.handleSetForward(ctx, key, m.Value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventSetField, ID: m.ID}

	case shmevent.EventSet:
		key, value, err := shmevent.DecodeSetPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		if len(key) > 0 && key[0] == shmevent.SystemKeyPrefix {
			return errorMsg(m.ID, fmt.Errorf("key namespace starting with 0x00 is reserved for system use"))
		}
		if err := n.handleSetForward(ctx, key, value, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventSet, ID: m.ID}

	case shmevent.EventPermitRequest:
		kind, peerID, metadata, err := shmevent.DecodePermitRequestPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		key := shmevent.SystemKey(kind, shmevent.StatusPending, peerID)
		if err := n.handleSetForward(ctx, key, metadata, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPermitRequest, ID: m.ID}

	case shmevent.EventPermitConfirm:
		// The only place that actually enforces "only a raft voter may
		// confirm" (see shmevent.EventPermitConfirm's doc comment): check
		// the *original* caller's own identity here, once, before doing
		// anything else -- not handleForwardConfirmStream's check, which
		// only ever authenticates whichever node relayed the request one
		// hop closer to the leader (using *its own* libp2p identity), not
		// who actually asked. A remote caller with no standing at all
		// could otherwise dial any legitimate voter follower and have it
		// unwittingly relay the confirm on its behalf, or dial the leader
		// directly (handleConfirmForward's isLeader branch used to apply
		// with no identity check at all -- that reasoning only held when
		// caller and node were the same actor, true for a local pkg/ipc
		// operator but not for a remote caller.Conn()-authenticated key).
		if caller.remotePeer != "" {
			rf := n.getRaft()
			if rf == nil || !isVoter(rf, raft.ServerID(caller.remotePeer.String())) {
				return errorMsg(m.ID, fmt.Errorf("%s is not a current raft voter", caller.remotePeer))
			}
		}
		kind, peerID, err := shmevent.DecodePermitConfirmPayload(m.Value)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		pendingKey := shmevent.SystemKey(kind, shmevent.StatusPending, peerID)
		confirmedKey := shmevent.SystemKey(kind, shmevent.StatusConfirmed, peerID)
		if err := n.handleConfirmForward(ctx, pendingKey, confirmedKey, true); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPermitConfirm, ID: m.ID}

	case shmevent.EventExecute:
		if err := n.dispatchExecute(ctx, m); err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventExecute, ID: m.ID}

	case shmevent.EventPollExecute:
		notif, ok := n.executeInbox.pop()
		if !ok {
			return shmevent.Msg{EventType: shmevent.EventPollExecute, ID: m.ID}
		}
		value, err := shmevent.EncodeExecuteNotification(notif.senderPeerID, notif.payload)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventPollExecute, Value: value, ID: m.ID}

	case shmevent.EventGetField:
		key := m.Value
		if m.SourceID != 0 {
			k, ok := n.registry.Lookup(m.SourceID)
			if !ok {
				return errorMsg(m.ID, fmt.Errorf("no key registered under id %d -- send SetKey first", m.SourceID))
			}
			key = k
		}
		value, err := n.handleGet(key)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventGetField, ID: m.ID, Value: value}

	case shmevent.EventGetPublicKey:
		return shmevent.Msg{EventType: shmevent.EventGetPublicKey, ID: m.ID, Value: n.ed25519Pub}

	case shmevent.EventGetPrivateKey:
		return shmevent.Msg{EventType: shmevent.EventGetPrivateKey, ID: m.ID, Value: n.ed25519Priv}

	case shmevent.EventAdd:
		peerID, err := n.handleAddDispatch(ctx, m, caller.remotePeer)
		if err != nil {
			return errorMsg(m.ID, err)
		}
		return shmevent.Msg{EventType: shmevent.EventAdd, ID: m.ID, Value: []byte(peerID)}

	default:
		return errorMsg(m.ID, fmt.Errorf("unknown event %d", m.EventType))
	}
}

// errorMsg builds the response for a failed request -- see
// shmevent.EventError's doc comment for why this event exists even though
// it isn't part of api/shmevent.capnp's originally specified field set.
func errorMsg(id uint16, err error) shmevent.Msg {
	msg := err.Error()
	if len(msg) > shmevent.ValueSize {
		msg = msg[:shmevent.ValueSize]
	}
	return shmevent.Msg{EventType: shmevent.EventError, ID: id, Value: []byte(msg)}
}

// handleAddDispatch implements EventAdd's three shapes -- see
// EventAdd's doc comment in pkg/shmevent -- and returns this node's own
// peer id on success, mirroring the pre-shmevent ipcproto.ActionAdd
// response. remotePeer is the caller's own libp2p-authenticated identity
// ("" for a local pkg/ipc caller, see callerIdentity) -- checked against
// the learner-join branch's claimed peer id below.
func (n *Node) handleAddDispatch(ctx context.Context, m shmevent.Msg, remotePeer peer.ID) (string, error) {
	// Learner join (remote browser caller, via ClientProtocolID): SourceID
	// references a prior EventSetKey holding the caller's own peer id,
	// Value is the caller's own reachable address.
	if m.SourceID != 0 {
		joinPeerID, ok := n.registry.Lookup(m.SourceID)
		if !ok {
			return "", fmt.Errorf("no peer id registered under id %d -- send SetKey first", m.SourceID)
		}
		// A remote caller could otherwise register any peer id string it
		// likes via EventSetKey -- not necessarily its own -- and get it
		// added to the raft configuration at an address of its choosing.
		// Binding the claim to the stream's own authenticated identity
		// (unforgeable -- established by the libp2p handshake, not
		// anything the message itself carries) closes that.
		if remotePeer != "" && string(joinPeerID) != remotePeer.String() {
			return "", fmt.Errorf("add: claimed peer id %q does not match the authenticated connection identity %s", joinPeerID, remotePeer)
		}
		return n.handleAddLearner(ctx, string(joinPeerID), string(m.Value))
	}
	// Bootstrap (Value empty) or voter join (Value = leader peer id/multiaddr).
	return n.handleAdd(ctx, string(m.Value))
}

func (n *Node) handleAdd(ctx context.Context, leaderPeerID string) (string, error) {
	rf, err := n.initRaft()
	if err != nil {
		return "", fmt.Errorf("init raft: %w", err)
	}

	if leaderPeerID == "" {
		cfg := raft.Configuration{
			Servers: []raft.Server{{
				Suffrage: raft.Voter,
				ID:       raft.ServerID(n.peerID),
				Address:  raft.ServerAddress(n.advertisedAddrs()[0]),
			}},
		}
		if err := rf.BootstrapCluster(cfg).Error(); err != nil {
			return "", fmt.Errorf("bootstrap: %w", err)
		}
		// BootstrapCluster only guarantees the configuration was persisted,
		// not that self-election has completed yet. A follower's join
		// request (handleJoinStream) requires State() == Leader, so make
		// the Add response wait for that instead of racing a subsequent
		// `mage addnode <leaderPeerID>` against this node's own election.
		// Scale the wait off the actual configured election timeout rather
		// than a fixed constant, so a longer WAN-tuned timeout still gets a
		// comfortable margin instead of being raced against a hardcoded window.
		if _, err := n.awaitLeader(10 * n.electionTimeout); err != nil {
			return "", fmt.Errorf("await self-election: %w", err)
		}
		return n.peerID, nil
	}

	// leaderPeerID is either a full multiaddr (a leader on another machine,
	// e.g. a remote deployment -- there's no shared registry to resolve it
	// from) or a bare peer id created on this same machine, resolved
	// through the local registry.
	leaderAddr := leaderPeerID
	if !registry.IsMultiaddr(leaderPeerID) {
		reg, err := registry.Open()
		if err != nil {
			return "", err
		}
		leaderAddr, err = reg.ResolveAddress(leaderPeerID)
		if err != nil {
			return "", err
		}
	}

	if err := n.join(ctx, leaderAddr); err != nil {
		return "", fmt.Errorf("join: %w", err)
	}
	return n.peerID, nil
}

// join asks the leader reachable at leaderAddr to add this node as a voter.
func (n *Node) join(ctx context.Context, leaderAddr string) error {
	// If this node needs a relay reservation to be reachable at all (see
	// Config.RelayPeer), give it a moment to complete before doing anything
	// else: AutoRelay's reservation happens asynchronously in the background
	// from newHost, and raft's configuration stores whatever address we send
	// below permanently -- getting it before the /p2p-circuit address exists
	// means the leader can never actually reach this node.
	//
	// This has to happen before opening the join stream, not just before
	// sending on it: host.NewStream returns as soon as Identify has told us
	// the remote supports JoinProtocolID, deferring the actual
	// multistream-select handshake to the stream's first Write (see
	// msmux.NewMSSelect) -- but the remote's own DefaultNegotiationTimeout
	// (10s) starts ticking the moment it sees the raw stream open, not when
	// bytes first arrive on it. Waiting here, before NewStream is ever
	// called, sidesteps that negotiation-timeout risk entirely (an earlier
	// version awaited between NewStream and the first Write instead, and
	// joins reliably failed with StreamProtocolNegotiationFailed whenever
	// RelayPeer was set as a result) -- so this wait is free to be as long
	// as a reservation genuinely needs, not bounded by that 10s window.
	//
	// A reservation that doesn't complete within this wait isn't just a
	// slower join: awaitRelayAddr gives up silently either way, and
	// whatever address n.host.Addrs() has *then* -- a real /p2p-circuit
	// address, or, if the reservation lost the race, only this node's raw
	// (often NAT'd, undialable) addresses -- is what gets sent below and
	// stored in raft's persisted configuration permanently. Get that
	// wrong and no amount of retrying a later read fixes it: the leader
	// keeps trying to deliver AppendEntries to an address that was never
	// reachable, until this node rejoins with a corrected one. Observed
	// directly against a real relay this project's own deploy target
	// (measured well under 1 Mbps to it): 15s was not consistently enough
	// for the reservation handshake itself to complete over a link that
	// slow, and every subsequent read from that follower failed
	// indefinitely as a result -- not a timing issue a retry budget on
	// the read side could ever paper over.
	n.awaitRelayAddr(45 * time.Second)

	maddr, err := multiaddr.NewMultiaddr(leaderAddr)
	if err != nil {
		return fmt.Errorf("invalid leader address %q: %w", leaderAddr, err)
	}
	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return fmt.Errorf("leader address %q missing peer id: %w", leaderAddr, err)
	}
	if err := n.host.Connect(ctx, *info); err != nil {
		return fmt.Errorf("connect to leader %s: %w", info.ID, err)
	}

	s, err := n.host.NewStream(ctx, info.ID, JoinProtocolID)
	if err != nil {
		return fmt.Errorf("open join stream to leader %s: %w", info.ID, err)
	}
	defer s.Close()

	selfAddr := n.advertisedAddrs()[0]
	if _, err := fmt.Fprintf(s, "%s %s voter\n", n.peerID, selfAddr); err != nil {
		return fmt.Errorf("send join request: %w", err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("close join request: %w", err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return fmt.Errorf("read join response: %w", err)
		}
		return fmt.Errorf("no response from leader %s", info.ID)
	}
	line := scanner.Text()
	if line == "OK" {
		return nil
	}
	return fmt.Errorf("leader rejected join: %s", line)
}

// handleJoinStream is the leader-side handler for JoinProtocolID: it parses
// "<peer-id> <multiaddr> <voter|learner>" from the requester and adds it to
// the raft configuration with the requested suffrage if this node is the
// leader, or forwards the request to whoever currently is (one hop only,
// over ForwardJoinProtocolID -- see handleForwardJoinStream) since the
// joining node has no other way to learn the real leader.
func (n *Node) handleJoinStream(s network.Stream) {
	defer s.Close()

	joinPeerID, joinAddr, suffrage, err := parseJoinRequest(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: malformed join request: %v\n", err)
		return
	}

	rf := n.getRaft()
	if rf != nil && rf.State() == raft.Leader {
		fmt.Fprintf(s, "%s\n", n.addServerLine(context.Background(), rf, joinPeerID, joinAddr, suffrage))
		return
	}

	var leaderID raft.ServerID
	if rf != nil {
		_, leaderID = rf.LeaderWithID()
	}
	if leaderID == "" {
		fmt.Fprintf(s, "ERR: not leader\n")
		return
	}

	line, err := n.forwardJoin(context.Background(), leaderID, joinPeerID, joinAddr, suffrage)
	if err != nil {
		fmt.Fprintf(s, "ERR: forward join: %v\n", err)
		return
	}
	fmt.Fprintf(s, "%s\n", line)
}

// handleForwardJoinStream is the leader-side handler for
// ForwardJoinProtocolID: it adds the requester to the raft configuration
// with the requested suffrage if this node is actually the leader, or
// reports the current leader without forwarding again -- mirroring
// handleForwardSetStream's single-hop guarantee, which rules out a
// forwarding cycle regardless of how leadership bounces around.
func (n *Node) handleForwardJoinStream(s network.Stream) {
	defer s.Close()

	joinPeerID, joinAddr, suffrage, err := parseJoinRequest(s)
	if err != nil {
		fmt.Fprintf(s, "ERR: malformed join request: %v\n", err)
		return
	}

	rf := n.getRaft()
	if rf == nil || rf.State() != raft.Leader {
		var leaderID raft.ServerID
		if rf != nil {
			_, leaderID = rf.LeaderWithID()
		}
		fmt.Fprintf(s, "ERR: not leader; current leader is %s (already forwarded once)\n", leaderID)
		return
	}

	fmt.Fprintf(s, "%s\n", n.addServerLine(context.Background(), rf, joinPeerID, joinAddr, suffrage))
}

// parseJoinRequest reads and parses the single
// "<peer-id> <multiaddr> <voter|learner>" line that is the wire format
// shared by JoinProtocolID and ForwardJoinProtocolID. The suffrage token
// defaults to "voter" if absent, so a line written by an older build of
// this same code (before ClientProtocolID's browser-learner join existed)
// still parses the same way it always has.
func parseJoinRequest(s network.Stream) (peerID, addr string, suffrage raft.ServerSuffrage, err error) {
	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", "", raft.Voter, err
		}
		return "", "", raft.Voter, fmt.Errorf("empty join request")
	}
	var suffrageWord string
	fields := strings.Fields(scanner.Text())
	switch len(fields) {
	case 2:
		peerID, addr = fields[0], fields[1]
		suffrageWord = "voter"
	case 3:
		peerID, addr, suffrageWord = fields[0], fields[1], fields[2]
	default:
		return "", "", raft.Voter, fmt.Errorf("expected \"<peer-id> <multiaddr> [voter|learner]\", got %q", scanner.Text())
	}
	switch suffrageWord {
	case "voter":
		suffrage = raft.Voter
	case "learner":
		suffrage = raft.Nonvoter
	default:
		return "", "", raft.Voter, fmt.Errorf("unknown suffrage %q", suffrageWord)
	}
	return peerID, addr, suffrage, nil
}

// addServerLine runs raft.AddVoter or raft.AddNonvoter (per suffrage) for
// joinPeerID/joinAddr and returns the response line to send back over the
// wire: "OK" or "ERR: <reason>". Every call site already only reaches this
// once rf.State()==Leader is confirmed, so on success it also records
// joinPeerID's KindClusterMember system record (pubkey + role) --
// see pkg/shmevent.KindClusterMember's doc comment.
func (n *Node) addServerLine(ctx context.Context, rf *raft.Raft, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage) string {
	var future raft.IndexFuture
	switch suffrage {
	case raft.Nonvoter:
		future = rf.AddNonvoter(raft.ServerID(joinPeerID), raft.ServerAddress(joinAddr), 0, 10*time.Second)
	default:
		future = rf.AddVoter(raft.ServerID(joinPeerID), raft.ServerAddress(joinAddr), 0, 10*time.Second)
	}
	if err := future.Error(); err != nil {
		return fmt.Sprintf("ERR: %v", err)
	}

	role := shmevent.RoleVoter
	if suffrage == raft.Nonvoter {
		role = shmevent.RoleLearner
	}
	if err := n.recordClusterMember(ctx, joinPeerID, role); err != nil {
		// The join itself already succeeded and is committed to the raft
		// configuration -- this registry is a queryable convenience
		// mirror, not something anything else depends on for
		// correctness, so a failure recording it shouldn't fail the join
		// response back to the caller.
		fmt.Fprintf(os.Stderr, "daemon: record cluster member %s: %v\n", joinPeerID, err)
	}
	return "OK"
}

// recordClusterMember extracts peerIDStr's own public key -- embedded in
// the peer id itself for this project's Ed25519 identities, see
// pkg/shmevent.KindClusterMember's doc comment -- and writes its
// KindClusterMember record with the given role.
func (n *Node) recordClusterMember(ctx context.Context, peerIDStr string, role byte) error {
	pid, err := peer.Decode(peerIDStr)
	if err != nil {
		return fmt.Errorf("decode peer id: %w", err)
	}
	pub, err := pid.ExtractPublicKey()
	if err != nil {
		return fmt.Errorf("extract public key: %w", err)
	}
	raw, err := pub.Raw()
	if err != nil {
		return fmt.Errorf("public key raw bytes: %w", err)
	}
	key := shmevent.ClusterMemberKey([]byte(peerIDStr))
	value := shmevent.EncodeClusterMemberPayload(raw, role)
	return n.handleSetForward(ctx, key, value, true)
}

// forwardJoin relays a join request (joinPeerID, joinAddr, suffrage) to
// leaderID over ForwardJoinProtocolID and returns its response line
// verbatim (without the trailing newline). Mirrors forwardSet's reasoning:
// the libp2p host already knows how to reach leaderID via this node's own
// raft transport, so no address resolution beyond the peer id is needed.
func (n *Node) forwardJoin(ctx context.Context, leaderID raft.ServerID, joinPeerID, joinAddr string, suffrage raft.ServerSuffrage) (string, error) {
	pid, err := peer.Decode(string(leaderID))
	if err != nil {
		return "", fmt.Errorf("invalid leader id %s: %w", leaderID, err)
	}
	s, err := n.host.NewStream(ctx, pid, ForwardJoinProtocolID)
	if err != nil {
		return "", fmt.Errorf("open forward-join stream to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	suffrageWord := "voter"
	if suffrage == raft.Nonvoter {
		suffrageWord = "learner"
	}
	if _, err := fmt.Fprintf(s, "%s %s %s\n", joinPeerID, joinAddr, suffrageWord); err != nil {
		return "", fmt.Errorf("write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return "", fmt.Errorf("close write to leader %s: %w", leaderID, err)
	}

	scanner := bufio.NewScanner(s)
	if !scanner.Scan() {
		if err := scanner.Err(); err != nil {
			return "", fmt.Errorf("read response from leader %s: %w", leaderID, err)
		}
		return "", fmt.Errorf("no response from leader %s", leaderID)
	}
	return scanner.Text(), nil
}

// handleSetForward is the entry point for a Set (EventSetField, or
// handleForwardSetStream's leader-side answer to an already-forwarded
// request): it applies directly if this node is the leader, or forwards
// to the leader (one hop only) if not. allowForward is false on the
// already-forwarded path so a request can be forwarded at most once: if
// the node it lands on then *also* turns out not to be leader (a
// leadership change mid-flight), it fails outward with a clear error
// instead of forwarding again, which rules out any forwarding cycle
// regardless of how leadership bounces around.
func (n *Node) handleSetForward(ctx context.Context, key, value []byte, allowForward bool) error {
	// Both wait windows below scale off the actual configured election
	// timeout (not a fixed constant) so a WAN-tuned longer timeout still
	// gets a comfortable margin: Apply itself can legitimately take a full
	// election cycle if the leader steps down and a new one is elected
	// mid-call.
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		return n.applySet(rf, key, value)
	}
	if !allowForward {
		return fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	return n.forwardSet(ctx, leaderID, key, value)
}

func (n *Node) applySet(rf *raft.Raft, key, value []byte) error {
	cmd := kvfsm.EncodeCommand(kvfsm.OpSet, key, value)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return res.Err
	}
	return nil
}

// forwardSet relays a Set(key, value) to leaderID over ForwardProtocolID
// and returns its outcome. This is purely internal node-to-node machinery
// (not something a "user" ever speaks -- see pkg/shmevent's doc comment),
// so it reuses kvfsm's own log-command framing directly rather than
// pkg/shmevent's user-facing relational protocol: write the encoded
// command, close the write side, then read until EOF -- an empty response
// means success, a non-empty one is the leader's error message. The
// libp2p host already has an open connection/known address for leaderID
// -- it's the peer this node's own raft transport talks to for
// AppendEntries -- so no address resolution is needed beyond the peer id
// itself.
func (n *Node) forwardSet(ctx context.Context, leaderID raft.ServerID, key, value []byte) error {
	pid, err := peer.Decode(string(leaderID))
	if err != nil {
		return fmt.Errorf("forward set: invalid leader id %s: %w", leaderID, err)
	}
	s, err := n.host.NewStream(ctx, pid, ForwardProtocolID)
	if err != nil {
		return fmt.Errorf("forward set to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(kvfsm.OpSet, key, value)
	if _, err := s.Write(cmd); err != nil {
		return fmt.Errorf("forward set: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward set: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward set: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward set: %s", respBuf)
	}
	return nil
}

// handleForwardSetStream is the leader-side handler for ForwardProtocolID:
// it decodes a kvfsm-framed Set command and answers it exactly like a
// local Set would, with forwarding disabled (see
// handleSetForward's allowForward doc). See forwardSet's doc comment for
// the wire format (kvfsm's own command framing, not pkg/shmevent) --
// forwardSet treats *any* empty response as success, so every early return
// here must write a non-empty error instead of silently closing the
// stream: an empty response from a read/decode failure would otherwise be
// indistinguishable from genuine success, silently dropping the write
// while the forwarding follower reports it as applied. Found exactly this
// way -- a follower over a relay-adjacent connection (a phone) reporting
// every Set as successful while nothing was ever persisted, anywhere.
func (n *Node) handleForwardSetStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward set: read command: %v", err)
		return
	}
	op, key, value, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		fmt.Fprintf(s, "forward set: decode command: %v", err)
		return
	}
	if op != kvfsm.OpSet {
		fmt.Fprintf(s, "forward set: expected OpSet, got op %d", op)
		return
	}

	if err := n.handleSetForward(context.Background(), key, value, false); err != nil {
		s.Write([]byte(err.Error()))
	}
}

// handleConfirmForward is EventPermitConfirm's counterpart to
// handleSetForward: applies directly if this node is the leader, or
// forwards to the leader (one hop only, same allowForward-guarded
// pattern) if not. When this node *is* the leader, no separate voter
// check is needed here -- hashicorp/raft guarantees only a Voter can ever
// hold leader state, so isLeader==true already implies the confirming
// node is a voter. The forwarded path's voter check happens in
// handleForwardConfirmStream instead, against the authenticated identity
// of whichever node actually opened the stream.
func (n *Node) handleConfirmForward(ctx context.Context, pendingKey, confirmedKey []byte, allowForward bool) error {
	rf, isLeader, leaderID, err := n.resolveWriteTarget(5 * n.electionTimeout)
	if err != nil {
		return err
	}
	if isLeader {
		return n.applyConfirm(rf, pendingKey, confirmedKey)
	}
	if !allowForward {
		return fmt.Errorf("not leader; current leader is %s (already forwarded once)", leaderID)
	}
	return n.forwardConfirm(ctx, leaderID, pendingKey, confirmedKey)
}

func (n *Node) applyConfirm(rf *raft.Raft, pendingKey, confirmedKey []byte) error {
	cmd := kvfsm.EncodeCommand(kvfsm.OpConfirm, pendingKey, confirmedKey)
	future := rf.Apply(cmd, 10*n.electionTimeout)
	if err := future.Error(); err != nil {
		return err
	}
	if res, ok := future.Response().(kvfsm.ApplyResult); ok && res.Err != nil {
		return res.Err
	}
	return nil
}

// forwardConfirm relays an OpConfirm(pendingKey, confirmedKey) to
// leaderID over ForwardConfirmProtocolID, mirroring forwardSet's wire
// convention exactly (kvfsm's own command framing; empty response =
// success, non-empty = the leader's error message).
func (n *Node) forwardConfirm(ctx context.Context, leaderID raft.ServerID, pendingKey, confirmedKey []byte) error {
	pid, err := peer.Decode(string(leaderID))
	if err != nil {
		return fmt.Errorf("forward confirm: invalid leader id %s: %w", leaderID, err)
	}
	s, err := n.host.NewStream(ctx, pid, ForwardConfirmProtocolID)
	if err != nil {
		return fmt.Errorf("forward confirm to leader %s: %w", leaderID, err)
	}
	defer s.Close()

	cmd := kvfsm.EncodeCommand(kvfsm.OpConfirm, pendingKey, confirmedKey)
	if _, err := s.Write(cmd); err != nil {
		return fmt.Errorf("forward confirm: write to leader %s: %w", leaderID, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("forward confirm: close write to leader %s: %w", leaderID, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("forward confirm: read response from leader %s: %w", leaderID, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("forward confirm: %s", respBuf)
	}
	return nil
}

// handleForwardConfirmStream is the leader-side handler for
// ForwardConfirmProtocolID. Unlike handleForwardSetStream, it checks the
// stream's libp2p-authenticated remote peer -- s.Conn().RemotePeer(),
// established by the connection's own handshake and so unforgeable by
// whatever a caller puts in the message itself -- against the leader's
// live raft configuration before applying anything, rejecting unless
// that peer is currently a Voter. This is the actual enforcement of
// EventPermitConfirm's "only a raft voter may confirm" rule: the
// generic per-message Ed25519 signature check every event type already
// gets (see handleShmEvent) only proves the message wasn't corrupted and
// was signed with whoever's key it was checked against -- for local
// same-machine shmring IPC that's inherently this same node's own key
// (see pkg/shmevent's doc comment), which doesn't by itself say anything
// about cluster membership. The RemotePeer check here is what does.
func (n *Node) handleForwardConfirmStream(s network.Stream) {
	defer s.Close()

	remote := s.Conn().RemotePeer()
	rf := n.getRaft()
	if rf == nil || !isVoter(rf, raft.ServerID(remote.String())) {
		fmt.Fprintf(s, "forward confirm: %s is not a current raft voter", remote)
		return
	}

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: read command: %v", err)
		return
	}
	op, pendingKey, confirmedKey, err := kvfsm.DecodeCommand(buf)
	if err != nil {
		fmt.Fprintf(s, "forward confirm: decode command: %v", err)
		return
	}
	if op != kvfsm.OpConfirm {
		fmt.Fprintf(s, "forward confirm: expected OpConfirm, got op %d", op)
		return
	}

	if err := n.handleConfirmForward(context.Background(), pendingKey, confirmedKey, false); err != nil {
		s.Write([]byte(err.Error()))
	}
}

// isVoter reports whether id is currently a Voter in rf's configuration.
func isVoter(rf *raft.Raft, id raft.ServerID) bool {
	cfg := rf.GetConfiguration()
	if err := cfg.Error(); err != nil {
		return false
	}
	for _, srv := range cfg.Configuration().Servers {
		if srv.ID == id && srv.Suffrage == raft.Voter {
			return true
		}
	}
	return false
}

// dispatchExecute implements EventExecute (see that event's doc comment
// in pkg/shmevent): resolves SourceID/DestinationID against this node's
// own registry, confirms the caller isn't claiming some other node as the
// sender (this node can only ever relay the peer-to-peer hop below under
// its own identity, since that's the key it signs with), then delivers
// it. Never touches n.store or raft.
func (n *Node) dispatchExecute(ctx context.Context, m shmevent.Msg) error {
	senderKey, ok := n.registry.Lookup(m.SourceID)
	if !ok {
		return fmt.Errorf("execute: no peer id registered under source id %d -- send SetKey first", m.SourceID)
	}
	if string(senderKey) != n.peerID {
		return fmt.Errorf("execute: source %q is not this node's own peer id (%s)", senderKey, n.peerID)
	}
	destKey, ok := n.registry.Lookup(m.DestinationID)
	if !ok {
		return fmt.Errorf("execute: no peer id registered under destination id %d -- send SetKey first", m.DestinationID)
	}
	destPeerID, err := peer.Decode(string(destKey))
	if err != nil {
		return fmt.Errorf("execute: invalid destination peer id %q: %w", destKey, err)
	}
	return n.sendExecute(ctx, destPeerID, m.Value)
}

// sendExecute dials dest directly over ExecuteProtocolID -- a fresh
// peer-to-peer libp2p stream between two raft node processes, entirely
// outside raft consensus -- and hands it an EventExecute message carrying
// EncodeExecuteNotification(this node's own peer id, payload), signed
// with this node's own key. See handleExecuteStream for the receiving
// side.
func (n *Node) sendExecute(ctx context.Context, dest peer.ID, payload []byte) error {
	s, err := n.host.NewStream(ctx, dest, ExecuteProtocolID)
	if err != nil {
		return fmt.Errorf("execute: open stream to %s: %w", dest, err)
	}
	defer s.Close()

	value, err := shmevent.EncodeExecuteNotification([]byte(n.peerID), payload)
	if err != nil {
		return fmt.Errorf("execute: encode notification: %w", err)
	}
	buf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventExecute, Value: value}, n.ed25519Priv)
	if err != nil {
		return fmt.Errorf("execute: encode message: %w", err)
	}
	if _, err := s.Write(buf); err != nil {
		return fmt.Errorf("execute: write to %s: %w", dest, err)
	}
	if err := s.CloseWrite(); err != nil {
		return fmt.Errorf("execute: close write to %s: %w", dest, err)
	}

	respBuf, err := io.ReadAll(s)
	if err != nil {
		return fmt.Errorf("execute: read response from %s: %w", dest, err)
	}
	if len(respBuf) > 0 {
		return fmt.Errorf("execute: %s", respBuf)
	}
	return nil
}

// handleExecuteStream is the receiving side of ExecuteProtocolID: it
// decodes the message (Decode itself checks crc32), extracts the claimed
// sender peer id from the notification payload (see
// EncodeExecuteNotification), and verifies the signature against *that*
// peer id's own Ed25519 public key -- embedded in the peer id itself for
// this project's identities, the same extraction
// pkg/daemon.recordClusterMember uses -- rather than trusting whichever
// address dialed in. That's what makes the signature self-contained
// (matches EventExecute's doc comment: authenticity doesn't depend on the
// stream's own connection identity), unlike handleForwardConfirmStream's
// check, which deliberately does the opposite for a different reason (see
// its own doc comment). On success, queues the notification for
// EventPollExecute; never touches n.store or raft either way.
func (n *Node) handleExecuteStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		fmt.Fprintf(s, "execute: read: %v", err)
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		fmt.Fprintf(s, "execute: decode: %v", err)
		return
	}
	if m.EventType != shmevent.EventExecute {
		fmt.Fprintf(s, "execute: expected EventExecute, got %s", shmevent.EventName(m.EventType))
		return
	}
	senderPeerID, payload, err := shmevent.DecodeExecuteNotification(m.Value)
	if err != nil {
		fmt.Fprintf(s, "execute: decode notification: %v", err)
		return
	}
	senderPeer, err := peer.Decode(string(senderPeerID))
	if err != nil {
		fmt.Fprintf(s, "execute: invalid sender peer id %q: %v", senderPeerID, err)
		return
	}
	senderPub, err := senderPeer.ExtractPublicKey()
	if err != nil {
		fmt.Fprintf(s, "execute: extract sender public key: %v", err)
		return
	}
	rawSenderPub, err := senderPub.Raw()
	if err != nil {
		fmt.Fprintf(s, "execute: sender public key raw bytes: %v", err)
		return
	}
	if err := shmevent.Verify(shmevent.PublicKey(rawSenderPub), m, crc, sig); err != nil {
		fmt.Fprintf(s, "execute: %v", err)
		return
	}
	n.executeInbox.push(senderPeerID, payload)
}

// handleClientStream is the leader-or-follower-side handler for
// ClientProtocolID: the remote counterpart of pkg/ipc's local shared
// memory, speaking the exact same pkg/shmevent capnp wire struct -- see
// that package's doc comment and ClientProtocolID's for why a browser
// learner's join (EventAdd) looks the way it does here specifically. A
// capnp message has no fixed size (unlike the ipcproto.Request this
// replaced), so this reads the whole request off the stream before
// decoding, the same way handleForwardSetStream already did for
// kvfsm's variable-length command framing.
//
// Every request here is treated as a remoteCaller (see callerIdentity):
// its signature is checked against its own libp2p-authenticated identity,
// not this node's key -- there is no shared-key bootstrap over this
// protocol (see handleShmEvent's doc comment).
func (n *Node) handleClientStream(s network.Stream) {
	defer s.Close()

	buf, err := io.ReadAll(s)
	if err != nil {
		return
	}
	m, crc, sig, err := shmevent.Decode(buf)
	if err != nil {
		return
	}

	caller, err := remoteCaller(s)
	if err != nil {
		respBuf, encErr := shmevent.Encode(errorMsg(m.ID, err), n.ed25519Priv)
		if encErr == nil {
			s.Write(respBuf)
		}
		return
	}

	resp := n.handleShmEvent(context.Background(), m, crc, sig, caller)
	respBuf, err := shmevent.Encode(resp, n.ed25519Priv)
	if err != nil {
		return
	}
	s.Write(respBuf)
}

// handleAddLearner adds joinPeerID as a raft non-voter at joinAddr directly
// if this node is the leader, or forwards to whoever currently is (one hop
// only, over ForwardJoinProtocolID, reusing the exact same wire path a
// voter join already forwards through -- see handleJoinStream) since the
// caller has no cheaper way to learn the real leader than any other
// joining node does. Returns this node's own peer id on success, mirroring
// handleAdd's return value.
func (n *Node) handleAddLearner(ctx context.Context, joinPeerID, joinAddr string) (string, error) {
	if joinPeerID == "" || joinAddr == "" {
		return "", fmt.Errorf("client add: missing peer id or multiaddr")
	}

	rf := n.getRaft()
	if rf != nil && rf.State() == raft.Leader {
		if line := n.addServerLine(ctx, rf, joinPeerID, joinAddr, raft.Nonvoter); strings.HasPrefix(line, "ERR: ") {
			return "", fmt.Errorf("%s", strings.TrimPrefix(line, "ERR: "))
		}
		return n.peerID, nil
	}

	var leaderID raft.ServerID
	if rf != nil {
		_, leaderID = rf.LeaderWithID()
	}
	if leaderID == "" {
		return "", fmt.Errorf("client add: not leader and no leader known")
	}

	line, err := n.forwardJoin(ctx, leaderID, joinPeerID, joinAddr, raft.Nonvoter)
	if err != nil {
		return "", fmt.Errorf("client add: forward: %w", err)
	}
	if reason, isErr := strings.CutPrefix(line, "ERR: "); isErr {
		return "", fmt.Errorf("%s", reason)
	}
	return n.peerID, nil
}

func (n *Node) handleGet(key []byte) ([]byte, error) {
	value, err := n.store.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// awaitLeader waits up to timeout for this node to become raft leader and
// returns the raft instance once it has. A freshly bootstrapped
// single-voter cluster elects itself almost immediately; this absorbs that
// startup race instead of failing the first Set issued right after
// `mage addnode`.
func (n *Node) awaitLeader(timeout time.Duration) (*raft.Raft, error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if rf := n.getRaft(); rf != nil && rf.State() == raft.Leader {
			return rf, nil
		}
		select {
		case <-deadline:
			rf := n.getRaft()
			if rf == nil {
				return nil, fmt.Errorf("node has not been added to a cluster yet")
			}
			if _, leaderID := rf.LeaderWithID(); leaderID != "" {
				return nil, fmt.Errorf("not leader; current leader is %s", leaderID)
			}
			return nil, fmt.Errorf("not leader and no leader known")
		case <-ticker.C:
		}
	}
}

// resolveWriteTarget waits (up to timeout) for this node to either become
// raft leader itself or learn who currently is, and reports which. In
// steady state this returns on its very first check, with no waiting at
// all -- the timeout only matters right after bootstrap/join, before
// raft's first election has completed and LeaderWithID is still empty.
func (n *Node) resolveWriteTarget(timeout time.Duration) (rf *raft.Raft, isLeader bool, leaderID raft.ServerID, err error) {
	deadline := time.After(timeout)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for {
		if rf := n.getRaft(); rf != nil {
			if rf.State() == raft.Leader {
				return rf, true, "", nil
			}
			if _, id := rf.LeaderWithID(); id != "" {
				return rf, false, id, nil
			}
		}
		select {
		case <-deadline:
			if n.getRaft() == nil {
				return nil, false, "", fmt.Errorf("node has not been added to a cluster yet")
			}
			return nil, false, "", fmt.Errorf("not leader and no leader known")
		case <-ticker.C:
		}
	}
}
