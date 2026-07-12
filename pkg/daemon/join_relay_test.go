package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
)

// TestJoinThroughRelay is a real-cluster regression test for the join()
// ordering fix (awaitRelayAddr now runs before host.NewStream, not between
// NewStream and the stream's first Write -- see join's doc comment). It
// spins up a genuine circuit-relay v2 server and two real daemon.Node
// instances -- no mocks -- and drives them exactly the way `mage addnode` /
// `mage addfollower` would over IPC, except calling handleAdd/handleSet/
// handleGet directly since this file lives in package daemon.
//
// Topology:
//
//	leader   -- bootstraps itself, no RelayPeer (has a normal dialable addr)
//	follower -- RelayPeer set to the relay, forcing every join() call through
//	            the exact awaitRelayAddr path the fix targets
//
// On this same-machine test topology, go-libp2p's address-reachability
// tracker (autonatv2) correctly determines the follower's direct address is
// in fact dialable -- it is, by every other node here -- so it never
// surfaces a /p2p-circuit address in host.Addrs(), and awaitRelayAddr
// reliably burns its full ~45s timeout waiting for one that will never
// appear. That is exactly the scenario the fix targets, just for a
// different underlying reason than a real NATed deployment: a ~45s wait
// now happens to elapse, for real, before join() ever opens the stream to
// the leader. Before the fix (see the reordering this test would catch if
// reverted: awaitRelayAddr moved back between NewStream and the request
// Write), that same wait would instead elapse *after* the stream was
// already open, blowing well past the leader's 10s multistream-select
// negotiation timeout and getting the stream reset before the join request
// ever reached it -- so the join would fail every time, not just under a
// real relay-required deployment. Asserting a wall-clock floor well into
// that 45s window (see minJoinWait below) is what makes this test actually
// exercise the fixed ordering rather than passing trivially fast.
//
// It asserts not just that the join RPC returns OK, but that it took long
// enough to prove the wait genuinely happened first, that the leader's raft
// configuration actually gained the follower as a voter, and that a Set on
// the leader is visible via Get on the follower -- proving the join, the
// raft replication, and the new SQLite-backed store all work together
// end-to-end.
func TestJoinThroughRelay(t *testing.T) {
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

	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]
	t.Logf("leader addr: %s", leaderAddr)

	followerKey := filepath.Join(tmpDir, "follower.key")
	if _, err := p2praft.LoadOrGenerateKey(followerKey); err != nil {
		t.Fatalf("generate follower key: %v", err)
	}
	followerCfg := fastRaft
	followerCfg.DataDir = filepath.Join(tmpDir, "follower")
	followerCfg.KeyPath = followerKey
	followerCfg.RelayPeer = relayAddr
	follower, err := start(followerCfg)
	if err != nil {
		t.Fatalf("start follower: %v", err)
	}
	defer follower.shutdown()

	// minJoinWait is comfortably below awaitRelayAddr's own 45s ceiling (so
	// a slightly-early return doesn't flake the test) but well past the
	// leader's 10s multistream-select negotiation timeout -- the exact
	// window the pre-fix ordering would have lost the race against.
	const minJoinWait = 40 * time.Second
	joinStart := time.Now()
	_, err = follower.handleAdd(ctx, leaderAddr)
	joinElapsed := time.Since(joinStart)
	if err != nil {
		t.Fatalf("follower join: %v", err)
	}
	t.Logf("join took %s", joinElapsed)
	if joinElapsed < minJoinWait {
		t.Fatalf("join returned after only %s, too fast for awaitRelayAddr's wait to have actually elapsed first (want >= %s) -- this test is not exercising the fixed ordering", joinElapsed, minJoinWait)
	}

	rf := leader.getRaft()
	cfgFuture := rf.GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	var followerAddr raft.ServerAddress
	for _, srv := range cfgFuture.Configuration().Servers {
		if srv.ID == raft.ServerID(follower.peerID) {
			followerAddr = srv.Address
		}
	}
	if followerAddr == "" {
		t.Fatal("follower not present in leader's raft configuration after join")
	}
	t.Logf("follower's raft address: %s (relay circuit address: %v)", followerAddr, strings.Contains(string(followerAddr), "/p2p-circuit"))

	if err := leader.handleSetForward(ctx, []byte("k1"), []byte("v1"), true); err != nil {
		t.Fatalf("set on leader: %v", err)
	}

	deadline := time.Now().Add(20 * time.Second)
	for {
		value, err := follower.handleGet([]byte("k1"))
		if err == nil && string(value) == "v1" {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("follower never observed replicated value: err=%v value=%q", err, value)
		}
		time.Sleep(50 * time.Millisecond)
	}
}
