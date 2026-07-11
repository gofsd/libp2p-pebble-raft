package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/multiformats/go-multiaddr"

	"github.com/gofsd/libp2p-kv-raft/pkg/ipcproto"
	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/rafttransport"
)

// TestAddLearnerThroughRelay is a real-cluster test for handleAddLearner
// (ClientProtocolID's ActionAdd): it spins up a genuine circuit-relay v2
// server, a real leader daemon.Node, and a plain go-libp2p host standing in
// for what web-app/'s rust-libp2p-in-wasm build would be -- a peer with no
// directly-dialable address of its own, reachable only through a relay
// reservation, exactly like an Android device behind carrier-grade NAT (see
// Config.RelayPeer's doc comment). It doesn't exercise the Rust wire codec
// itself (that's covered byte-for-byte by web-app's own raft_wire tests
// against real hashicorp/raft fixtures) -- what it proves is the Go-side
// half: that AddNonvoter over a relay-reserved address actually lands in
// the leader's raft configuration, and that the leader's own
// rafttransport.NetworkTransport can subsequently Dial() the "browser"
// through that reservation to deliver a real AppendEntries stream, the same
// way it already does for a relay-joined voter (see TestJoinThroughRelay).
func TestAddLearnerThroughRelay(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	relay, err := p2praft.StartRelayNode(ctx, filepath.Join(tmpDir, "relay.key"), 0)
	if err != nil {
		t.Fatalf("start relay: %v", err)
	}
	defer relay.Host.Close()
	if len(relay.Addrs) == 0 {
		t.Fatal("relay has no addresses")
	}
	relayAddr := relay.Addrs[0]
	t.Logf("relay addr: %s", relayAddr)

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

	leaderKey := filepath.Join(tmpDir, "leader.key")
	if _, err := p2praft.LoadOrGenerateKey(leaderKey); err != nil {
		t.Fatalf("generate leader key: %v", err)
	}
	leaderCfg := fastRaft
	leaderCfg.DataDir = filepath.Join(tmpDir, "leader")
	leaderCfg.KeyPath = leaderKey
	leader, err := start(leaderCfg)
	if err != nil {
		t.Fatalf("start leader: %v", err)
	}
	defer leader.shutdown()

	bootstrapResp := leader.handleAdd(ctx, ipcproto.NewRequest(ipcproto.ActionAdd, "", ""))
	if bootstrapResp.Status != ipcproto.StatusOK {
		t.Fatalf("bootstrap leader: %s", bootstrapResp.ValueString())
	}
	leaderAddr := leader.advertisedAddrs()[0]
	t.Logf("leader addr: %s", leaderAddr)

	// browser stands in for web-app/'s rust-libp2p-in-wasm build: a peer
	// with no directly-dialable address, reachable only through the relay
	// reservation NewP2PNode already sets up (EnableRelay +
	// ListenAddrStrings("/p2p-circuit") + relayclient.Reserve), exactly
	// the mechanism p2p.rs's reserve_relay_slot performs from inside wasm.
	browser, err := p2praft.NewP2PNode(ctx, relayAddr, filepath.Join(tmpDir, "browser.key"))
	if err != nil {
		t.Fatalf("start browser stand-in: %v", err)
	}
	defer browser.Host.Close()

	// Proves the leader's raft transport can really reach the browser
	// through its relay reservation, not just that AddNonvoter accepted
	// the address: a real AppendEntries (heartbeat) stream should arrive
	// on rafttransport.ProtocolID shortly after it joins the configuration.
	raftStreamCh := make(chan struct{}, 1)
	browser.Host.SetStreamHandler(rafttransport.ProtocolID, func(s network.Stream) {
		defer s.Reset()
		select {
		case raftStreamCh <- struct{}{}:
		default:
		}
	})

	// Same caveat TestJoinThroughRelay documents at length: on this
	// same-machine test topology, go-libp2p's own reachability tracker
	// correctly determines the browser's direct address is in fact
	// dialable (it is, by every other node here), so GetAddress() may
	// return that instead of a /p2p-circuit address. That doesn't weaken
	// what this test actually proves -- handleAddLearner correctly
	// AddNonvoters whatever address it's given, and the leader's raft
	// transport really dials it -- it just means this test alone can't
	// distinguish "dialed directly" from "dialed through the relay
	// circuit" the way a genuinely NATed deployment would force.
	browserAddr := browser.GetAddress()
	t.Logf("browser addr: %s (relay circuit address: %v)", browserAddr, strings.Contains(browserAddr, "/p2p-circuit"))
	if _, err := multiaddr.NewMultiaddr(browserAddr); err != nil {
		t.Fatalf("browser address %q is not a valid multiaddr: %v", browserAddr, err)
	}

	leaderPeerID, err := peer.Decode(leader.peerID)
	if err != nil {
		t.Fatalf("decode leader peer id: %v", err)
	}
	leaderMaddr, err := multiaddr.NewMultiaddr(leaderAddr)
	if err != nil {
		t.Fatalf("parse leader addr: %v", err)
	}
	leaderInfo, err := peer.AddrInfoFromP2pAddr(leaderMaddr)
	if err != nil {
		t.Fatalf("leader addr info: %v", err)
	}
	if err := browser.Host.Connect(ctx, *leaderInfo); err != nil {
		t.Fatalf("browser connect to leader: %v", err)
	}

	addStream, err := browser.Host.NewStream(ctx, leaderPeerID, ClientProtocolID)
	if err != nil {
		t.Fatalf("open client-protocol stream to leader: %v", err)
	}
	addReq := ipcproto.NewRequest(ipcproto.ActionAdd, browser.Host.ID().String(), browserAddr)
	addReqBuf := addReq.Encode()
	if _, err := addStream.Write(addReqBuf[:]); err != nil {
		t.Fatalf("write add-learner request: %v", err)
	}
	addRespBuf := make([]byte, ipcproto.ResponseSize)
	if _, err := readFullTest(addStream, addRespBuf); err != nil {
		t.Fatalf("read add-learner response: %v", err)
	}
	addStream.Close()
	addResp, err := ipcproto.DecodeResponse(addRespBuf)
	if err != nil {
		t.Fatalf("decode add-learner response: %v", err)
	}
	if addResp.Status != ipcproto.StatusOK {
		t.Fatalf("add-learner rejected: %s", addResp.ValueString())
	}

	rf := leader.getRaft()
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	var found bool
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(browser.Host.ID().String()) {
			found = true
			if srv.Suffrage != raft.Nonvoter {
				t.Fatalf("browser added with suffrage %v, want Nonvoter", srv.Suffrage)
			}
			if string(srv.Address) != browserAddr {
				t.Fatalf("browser address in configuration = %q, want %q", srv.Address, browserAddr)
			}
		}
	}
	if !found {
		t.Fatal("browser not present in leader's raft configuration after AddNonvoter")
	}

	select {
	case <-raftStreamCh:
	case <-time.After(20 * time.Second):
		t.Fatal("leader never dialed the browser's relay-reserved address with a raft RPC stream")
	}
}

func readFullTest(s network.Stream, buf []byte) (int, error) {
	total := 0
	for total < len(buf) {
		n, err := s.Read(buf[total:])
		total += n
		if err != nil {
			return total, err
		}
	}
	return total, nil
}
