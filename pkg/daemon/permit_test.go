package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/raft"

	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// TestPermitRequestConfirmWorkflow is a real-cluster test (a leader, a
// joined voter, and a joined nonvoter/learner, no mocks) for
// EventPermitRequest/EventPermitConfirm: it proves the two-stage
// pending-then-confirmed record actually gets replicated through raft
// like any other Set, and specifically that EventPermitConfirm's "only a
// raft voter may confirm" rule is enforced against the real,
// libp2p-authenticated identity of whichever node originates the
// confirm -- not a client-supplied claim -- by exercising the rejection
// path against a genuine joined learner, not a hand-constructed error.
func TestPermitRequestConfirmWorkflow(t *testing.T) {
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

	learner := startNode("learner")
	defer learner.shutdown()
	if _, err := learner.initRaft(); err != nil {
		t.Fatalf("init learner raft: %v", err)
	}
	if _, err := leader.handleAddLearner(ctx, learner.peerID, learner.advertisedAddrs()[0]); err != nil {
		t.Fatalf("join learner: %v", err)
	}

	// Sanity check both landed in the leader's configuration with the
	// suffrage this test's rejection/success expectations depend on.
	cfgFuture := leader.getRaft().GetConfiguration()
	if err := cfgFuture.Error(); err != nil {
		t.Fatalf("get leader configuration: %v", err)
	}
	wantSuffrage := map[raft.ServerID]raft.ServerSuffrage{
		raft.ServerID(voter.peerID):   raft.Voter,
		raft.ServerID(learner.peerID): raft.Nonvoter,
	}
	for id, want := range wantSuffrage {
		var found bool
		for _, srv := range cfgFuture.Configuration().Servers {
			if srv.ID == id {
				found = true
				if srv.Suffrage != want {
					t.Fatalf("%s joined with suffrage %v, want %v", id, srv.Suffrage, want)
				}
			}
		}
		if !found {
			t.Fatalf("%s not present in leader's raft configuration", id)
		}
	}

	// call mirrors what pkg/ipc.Serve does for a local caller: sign with
	// n's own key, decode back into the (Msg, crc, sig) triple
	// handleShmEvent expects, and dispatch.
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
		return n.handleShmEvent(ctx, decoded, crc, sig)
	}

	targetPeerID := []byte("some-new-node-peer-id")

	reqPayload, err := shmevent.EncodePermitRequestPayload(shmevent.KindPermitPeer, targetPeerID, nil)
	if err != nil {
		t.Fatalf("EncodePermitRequestPayload: %v", err)
	}
	resp := call(leader, shmevent.Msg{EventType: shmevent.EventPermitRequest, Value: reqPayload, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("permit_request rejected: %s", resp.Value)
	}

	confirmPayload := shmevent.EncodePermitConfirmPayload(shmevent.KindPermitPeer, targetPeerID)

	// A learner (nonvoter) confirming must be rejected, and specifically
	// for the voter-only reason -- not because e.g. it couldn't find a
	// leader at all.
	resp = call(learner, shmevent.Msg{EventType: shmevent.EventPermitConfirm, Value: confirmPayload, ID: 2})
	if resp.EventType != shmevent.EventError {
		t.Fatalf("learner permit_confirm unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not a current raft voter") {
		t.Fatalf("learner permit_confirm rejected for the wrong reason: %s", resp.Value)
	}

	// The pending record must still be there -- the rejected confirm must
	// not have consumed or altered it.
	pendingKey := shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusPending, targetPeerID)
	getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: pendingKey, ID: 3})
	if getResp.EventType == shmevent.EventError {
		t.Fatalf("pending record missing after rejected learner confirm: %s", getResp.Value)
	}

	// A real voter confirming must succeed.
	resp = call(voter, shmevent.Msg{EventType: shmevent.EventPermitConfirm, Value: confirmPayload, ID: 4})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter permit_confirm rejected: %s", resp.Value)
	}

	confirmedKey := shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, targetPeerID)
	getResp = call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: confirmedKey, ID: 5})
	if getResp.EventType == shmevent.EventError {
		t.Fatalf("confirmed record missing after voter confirm: %s", getResp.Value)
	}

	getResp = call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: pendingKey, ID: 6})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("pending record still present after successful confirm -- should have been consumed")
	}
}
