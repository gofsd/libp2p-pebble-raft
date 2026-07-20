package kvmobile

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// peerIDFromMultiaddr extracts the trailing /p2p/<peerID> component from a
// multiaddr string, e.g. what spawnTestLeader returns -- the tests below
// need the leader's own peer id (not just its dialable address) to target
// Execute at it or query its store directly via pkg/shmclient.
func peerIDFromMultiaddr(t *testing.T, addr string) string {
	t.Helper()
	i := strings.LastIndex(addr, "/p2p/")
	if i < 0 {
		t.Fatalf("multiaddr %q has no /p2p/ component", addr)
	}
	return addr[i+len("/p2p/"):]
}

// pollGet retries kvmobile.Get(key) until it stops erroring (the key has
// replicated to this device) or deadline passes, returning the last error
// seen. Mirrors the retry loop pkg/kvctl's own cross-node tests use for the
// same reason: a Set/permit record committed on the leader reaches a
// follower's local store asynchronously.
func pollGet(t *testing.T, key string, timeout time.Duration) (string, error) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	var (
		got     string
		lastErr error
	)
	for time.Now().Before(deadline) {
		got, lastErr = Get(key)
		if lastErr == nil {
			return got, nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return got, lastErr
}

// TestRequestConfirmRevokePermitThroughKvmobile drives kvmobile's
// RequestPermit/ConfirmPermit/RevokePermit bindings end to end against a
// real leader. The kvmobile-run follower device joins as a full raft voter
// (see CLAUDE.md's "Node connectivity policy" note that only the browser
// client joins as a non-voter learner), so unlike desktop's cross-node
// permit test this can drive request *and* confirm/revoke from the single
// follower session -- no separate voter node needed.
func TestRequestConfirmRevokePermitThroughKvmobile(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const targetPeerID = "some-target-peer-id"

	if err := RequestPermit("peer", targetPeerID, ""); err != nil {
		t.Fatalf("RequestPermit: %v", err)
	}
	pendingKey := string(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusPending, []byte(targetPeerID)))
	if _, err := pollGet(t, pendingKey, 10*time.Second); err != nil {
		t.Fatalf("Get(pendingKey) after RequestPermit: %v", err)
	}

	if err := ConfirmPermit("peer", targetPeerID); err != nil {
		t.Fatalf("ConfirmPermit: %v", err)
	}
	confirmedKey := string(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(targetPeerID)))
	if _, err := pollGet(t, confirmedKey, 10*time.Second); err != nil {
		t.Fatalf("Get(confirmedKey) after ConfirmPermit: %v", err)
	}

	if err := RevokePermit("peer", targetPeerID); err != nil {
		t.Fatalf("RevokePermit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var stillPresent bool
	for time.Now().Before(deadline) {
		if _, err := Get(confirmedKey); err != nil {
			stillPresent = false
			break
		}
		stillPresent = true
		time.Sleep(50 * time.Millisecond)
	}
	if stillPresent {
		t.Fatalf("Get(confirmedKey) after RevokePermit: still present, want deleted")
	}
}

// TestRequestConfirmRevokeLogPermitThroughKvmobile is
// TestRequestConfirmRevokePermitThroughKvmobile's counterpart for the
// RequestLogPermit/ConfirmLogPermit/RevokeLogPermit bindings.
func TestRequestConfirmRevokeLogPermitThroughKvmobile(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	const (
		logKind      = "audit"
		targetPeerID = "some-target-peer-id"
	)

	if err := RequestLogPermit(logKind, targetPeerID, ""); err != nil {
		t.Fatalf("RequestLogPermit: %v", err)
	}
	pendingKey, err := shmevent.LogPermitKey(shmevent.StatusPending, logKind, []byte(targetPeerID))
	if err != nil {
		t.Fatalf("LogPermitKey(pending): %v", err)
	}
	if _, err := pollGet(t, string(pendingKey), 10*time.Second); err != nil {
		t.Fatalf("Get(pendingKey) after RequestLogPermit: %v", err)
	}

	if err := ConfirmLogPermit(logKind, targetPeerID); err != nil {
		t.Fatalf("ConfirmLogPermit: %v", err)
	}
	confirmedKey, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, logKind, []byte(targetPeerID))
	if err != nil {
		t.Fatalf("LogPermitKey(confirmed): %v", err)
	}
	if _, err := pollGet(t, string(confirmedKey), 10*time.Second); err != nil {
		t.Fatalf("Get(confirmedKey) after ConfirmLogPermit: %v", err)
	}

	if err := RevokeLogPermit(logKind, targetPeerID); err != nil {
		t.Fatalf("RevokeLogPermit: %v", err)
	}
	deadline := time.Now().Add(10 * time.Second)
	var stillPresent bool
	for time.Now().Before(deadline) {
		if _, err := Get(string(confirmedKey)); err != nil {
			stillPresent = false
			break
		}
		stillPresent = true
		time.Sleep(50 * time.Millisecond)
	}
	if stillPresent {
		t.Fatalf("Get(confirmedKey) after RevokeLogPermit: still present, want deleted")
	}
}

// TestExecuteAndPollExecuteThroughKvmobile drives both directions of the
// EventExecute peer-to-peer notification path through kvmobile: Execute
// sent from the kvmobile-run follower and drained on the (non-kvmobile)
// leader via a raw pkg/shmclient.PollExecute call, then the reverse --
// sent from the leader via a raw pkg/shmclient.Execute call and drained
// through kvmobile.PollExecute's JSON-envelope return.
func TestExecuteAndPollExecuteThroughKvmobile(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := Execute(leaderPeerID, "hello-from-follower"); err != nil {
		t.Fatalf("Execute: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var (
		sender  string
		payload []byte
		ok      bool
	)
	for time.Now().Before(deadline) {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		sender, payload, ok, err = shmclient.PollExecute(ctx, leaderPeerID)
		cancel()
		if err != nil {
			t.Fatalf("shmclient.PollExecute(leader): %v", err)
		}
		if ok {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if !ok {
		t.Fatalf("shmclient.PollExecute(leader): no notification arrived within %s", 10*time.Second)
	}
	if sender != followerID {
		t.Fatalf("PollExecute(leader) sender = %s, want %s", sender, followerID)
	}
	if string(payload) != "hello-from-follower" {
		t.Fatalf("PollExecute(leader) value = %q, want %q", payload, "hello-from-follower")
	}

	sendCtx, sendCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer sendCancel()
	if err := shmclient.Execute(sendCtx, leaderPeerID, followerID, []byte("hello-from-leader")); err != nil {
		t.Fatalf("shmclient.Execute(leader -> follower): %v", err)
	}

	deadline = time.Now().Add(10 * time.Second)
	var resultJSON string
	for time.Now().Before(deadline) {
		resultJSON, err = PollExecute()
		if err != nil {
			t.Fatalf("PollExecute: %v", err)
		}
		var result pollExecuteResult
		if err := json.Unmarshal([]byte(resultJSON), &result); err != nil {
			t.Fatalf("parse PollExecute result %q: %v", resultJSON, err)
		}
		if result.Pending {
			if result.SenderPeerID != leaderPeerID {
				t.Fatalf("PollExecute sender_peer_id = %s, want %s", result.SenderPeerID, leaderPeerID)
			}
			if result.Value != "hello-from-leader" {
				t.Fatalf("PollExecute value = %q, want %q", result.Value, "hello-from-leader")
			}
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("PollExecute: no notification arrived within %s", 10*time.Second)
}
