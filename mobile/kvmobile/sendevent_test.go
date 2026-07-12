package kvmobile

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// spawnTestLeader starts a real pkg/daemon node in dataDir, bootstrapped
// as the cluster's sole leader, and returns its dialable multiaddr -- a
// real leader for kvmobile.Start to join against, the same way
// pkg/daemon's own relay/learner-join tests spin one up.
func spawnTestLeader(t *testing.T, dataDir string) (multiaddr string) {
	t.Helper()
	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if err := e2edata.WriteDesktopKeyFile(e2edata.Node{PrivateKey: hex.EncodeToString(priv)}, keyPath); err != nil {
		t.Fatalf("WriteDesktopKeyFile: %v", err)
	}
	peerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}

	ctx := context.Background()
	go func() {
		_ = daemon.Run(ctx, daemon.Config{
			DataDir:            dataDir,
			KeyPath:            keyPath,
			HeartbeatTimeout:   200 * time.Millisecond,
			ElectionTimeout:    200 * time.Millisecond,
			CommitTimeout:      50 * time.Millisecond,
			LeaderLeaseTimeout: 100 * time.Millisecond,
		})
	}()

	deadline := time.Now().Add(10 * time.Second)
	var ready daemon.ReadyInfo
	for time.Now().Before(deadline) {
		if info, err := daemon.ReadReadyFile(dataDir); err == nil {
			ready = info
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if ready.PeerID == "" {
		t.Fatal("leader daemon never became ready")
	}

	bootstrapCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := shmclient.Add(bootstrapCtx, peerID, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}
	if len(ready.ListenAddrs) == 0 {
		t.Fatal("leader daemon reported no listen addresses")
	}
	return ready.ListenAddrs[0]
}

// TestSendEventAgainstRealDaemon exercises kvmobile.SendEvent's raw-event
// path end to end against a real local leader: a bootstrap unsigned
// get_public_key, then a signed set_key/set_field/get_field round trip --
// the same fidelity kvctl-cli sendevent already has on desktop/remote,
// now proven on the same in-process daemon Android actually runs (this
// test never touches Android/gomobile itself, but kvmobile.go has no
// build-tag restriction and pkg/ipc.Call has an identical signature on
// both platforms -- see pkg/ipc/ipc_android.go's doc comment -- so this is
// real evidence SendEvent works, not just that it compiles).
func TestSendEventAgainstRealDaemon(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		mu.Lock()
		started, peerID, session = false, "", nil
		mu.Unlock()
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resp := mustSendEvent(t, `{"event":"get_public_key"}`)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("get_public_key returned an error event: value=%q", resp.Value())
	}
	if len(resp.Value()) != shmevent.PublicKeySize {
		t.Fatalf("get_public_key value len = %d, want %d", len(resp.Value()), shmevent.PublicKeySize)
	}

	if resp := mustSendEvent(t, `{"event":"set_key","value":"hello","id":100}`); resp.EventType == shmevent.EventError {
		t.Fatalf("set_key returned an error event: value=%q", resp.Value())
	}
	if resp := mustSendEvent(t, `{"event":"set_field","source_id":100,"value":"world"}`); resp.EventType == shmevent.EventError {
		t.Fatalf("set_field returned an error event: value=%q", resp.Value())
	}

	// A SetField forwarded to the leader can commit slightly before this
	// follower's own local read catches up (the same documented caveat
	// pkg/e2erun.retryReadsIfNeeded works around for desktop/remote rows,
	// and web-app/README.md's "Running it" section documents for the
	// browser client) -- a bounded retry here, not a hard requirement that
	// replication has already caught up in zero time.
	deadline := time.Now().Add(3 * time.Second)
	for {
		resp = mustSendEvent(t, `{"event":"get_field","value":"hello"}`)
		if resp.EventType != shmevent.EventError {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("get_field returned an error event: value=%q", resp.Value())
		}
		time.Sleep(50 * time.Millisecond)
	}
	if string(resp.Value()) != "world" {
		t.Fatalf("get_field value = %q, want %q", resp.Value(), "world")
	}
}

func mustSendEvent(t *testing.T, eventJSON string) e2edata.Event {
	t.Helper()
	respJSON, err := SendEvent(eventJSON)
	if err != nil {
		t.Fatalf("SendEvent(%s): %v", eventJSON, err)
	}
	var resp e2edata.Event
	if err := json.Unmarshal([]byte(respJSON), &resp); err != nil {
		t.Fatalf("parse SendEvent response %q: %v", respJSON, err)
	}
	return resp
}
