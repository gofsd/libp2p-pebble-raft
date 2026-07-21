package daemon

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestConfirmGatedJoinRequiresVoterConfirmation is a real-cluster test (no
// mocks) for Config.RequireConfirmForJoin: it proves a join request
// against a leader with that flag set only lodges a pending
// shmevent.KindClusterJoin record (join() gets back "pending", not "ok",
// and the joiner is NOT yet in the leader's raft configuration), and that
// the joiner is only actually admitted (raft.AddVoter, via applyConfirm's
// KindClusterJoin special case) once a *different, non-leader* raft voter
// confirms it -- exercising "any voter, not just the leader" specifically,
// since a leader confirming its own pending record would be the less
// interesting case.
func TestConfirmGatedJoinRequiresVoterConfirmation(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:      200 * time.Millisecond,
		ElectionTimeout:       200 * time.Millisecond,
		CommitTimeout:         20 * time.Millisecond,
		LeaderLeaseTimeout:    100 * time.Millisecond,
		RequireConfirmForJoin: true,
	}

	startNode := func(cfg Config, name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode(fastRaft, "leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	// Admit a second, real voter directly (bypassing the join wire
	// protocol) purely as test setup, so there's a non-leader voter
	// available to exercise "any voter, not just the leader" below --
	// the point of this test is confirm's authorization, not join's.
	voter := startNode(fastRaft, "voter")
	defer voter.shutdown()
	if _, err := voter.initRaft(); err != nil {
		t.Fatalf("init voter raft: %v", err)
	}
	if line := leader.addServerLine(ctx, leader.getRaft(), voter.peerID, voter.advertisedAddrs()[0], raft.Voter); line != "OK" {
		t.Fatalf("admit voter directly: %s", line)
	}

	joiner := startNode(fastRaft, "joiner")
	defer joiner.shutdown()

	status, err := joiner.handleAdd(ctx, leaderAddr)
	if err != nil {
		t.Fatalf("joiner handleAdd: %v", err)
	}
	if status != joiner.peerID+" pending" {
		t.Fatalf("handleAdd status = %q, want %q", status, joiner.peerID+" pending")
	}

	isMember := func(rf *raft.Raft, id string) bool {
		cfgFuture := rf.GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			t.Fatalf("get configuration: %v", err)
		}
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(id) {
				return true
			}
		}
		return false
	}

	if isMember(leader.getRaft(), joiner.peerID) {
		t.Fatal("joiner already in leader's raft configuration before any confirmation")
	}

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	confirmPayload := shmevent.EncodePermitConfirmPayload(shmevent.KindClusterJoin, []byte(joiner.peerID))
	resp := call(voter, shmevent.Msg{EventType: shmevent.EventPermitConfirm, Value: confirmPayload, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter confirm rejected: %s", resp.Value)
	}

	if !isMember(leader.getRaft(), joiner.peerID) {
		t.Fatal("joiner not in leader's raft configuration after confirmation")
	}
}

// TestLeaveShrinksClusterGracefully is a real-cluster test for
// shmevent.EventLeave/raft.RemoveServer: a joined voter leaving is
// removed from the leader's raft configuration (and its KindClusterMember
// record deleted), while the leader itself keeps operating normally
// afterward -- the "shrink, don't break" guarantee EventLeave/Rm depend
// on.
func TestLeaveShrinksClusterGracefully(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()

	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}

	startNode := func(name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg := fastRaft
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode("leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	leaderAddr := leader.advertisedAddrs()[0]

	voter := startNode("voter")
	defer voter.shutdown()
	if _, err := voter.handleAdd(ctx, leaderAddr); err != nil {
		t.Fatalf("join voter: %v", err)
	}

	isMember := func(rf *raft.Raft, id string) bool {
		cfgFuture := rf.GetConfiguration()
		if err := cfgFuture.Error(); err != nil {
			t.Fatalf("get configuration: %v", err)
		}
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == raft.ServerID(id) {
				return true
			}
		}
		return false
	}
	if !isMember(leader.getRaft(), voter.peerID) {
		t.Fatal("voter not in leader's raft configuration after joining")
	}

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	// voter is not the leader, so this exercises leaveCluster's forwarded
	// (ForwardLeaveProtocolID) path, not just the direct-apply one.
	resp := call(voter, shmevent.Msg{EventType: shmevent.EventLeave, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("leave rejected: %s", resp.Value)
	}

	if isMember(leader.getRaft(), voter.peerID) {
		t.Fatal("voter still in leader's raft configuration after leaving")
	}

	memberKey := shmevent.ClusterMemberKey([]byte(voter.peerID))
	getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: memberKey, ID: 2})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("voter's KindClusterMember record still present after leaving -- should have been deleted")
	}

	// The remaining cluster (just the leader, now the sole voter again)
	// must keep operating normally after the shrink.
	setPayload, err := shmevent.EncodeSetPayload([]byte("k"), []byte("v"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	resp = call(leader, shmevent.Msg{EventType: shmevent.EventSet, Value: setPayload, ID: 3})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("set after leave rejected: %s", resp.Value)
	}
}
