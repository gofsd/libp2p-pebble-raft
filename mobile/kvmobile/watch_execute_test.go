package kvmobile

import (
	"context"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// notification is one ExecuteCallback.OnNotification call recorded by
// recordingCallback.
type notification struct {
	sender string
	value  string
}

// recordingCallback is a Go-side ExecuteCallback implementation for tests
// -- Kotlin would implement the same interface via gomobile's reverse
// binding, but from Go the interface is just an ordinary type to satisfy.
// Notifications are delivered on OnNotification's channel so a test
// goroutine can wait for them without racing WatchExecute's own goroutine.
type recordingCallback struct {
	ch chan notification
}

func newRecordingCallback() *recordingCallback {
	return &recordingCallback{ch: make(chan notification, 16)}
}

func (r *recordingCallback) OnNotification(senderPeerID, value string) {
	r.ch <- notification{sender: senderPeerID, value: value}
}

func (r *recordingCallback) next(t *testing.T, timeout time.Duration) notification {
	t.Helper()
	select {
	case n := <-r.ch:
		return n
	case <-time.After(timeout):
		t.Fatal("timed out waiting for a WatchExecute notification")
		return notification{}
	}
}

// expectNone fails if a notification arrives within window -- used to
// confirm delivery actually stopped (StopWatchExecute) or was redirected
// (a replaced watcher), not just that it hasn't happened yet.
func (r *recordingCallback) expectNone(t *testing.T, window time.Duration) {
	t.Helper()
	select {
	case n := <-r.ch:
		t.Fatalf("unexpected notification: sender=%s value=%s", n.sender, n.value)
	case <-time.After(window):
	}
}

// TestWatchExecuteDeliversNotifications drives WatchExecute end to end: a
// raw pkg/shmclient.Execute call from a real (non-kvmobile) leader must
// reach the registered callback with the right sender and value.
func TestWatchExecuteDeliversNotifications(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		StopWatchExecute()
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb := newRecordingCallback()
	if err := WatchExecute(cb); err != nil {
		t.Fatalf("WatchExecute: %v", err)
	}

	sendCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shmclient.Execute(sendCtx, leaderPeerID, PeerID(), []byte("hello")); err != nil {
		t.Fatalf("shmclient.Execute: %v", err)
	}

	n := cb.next(t, 10*time.Second)
	if n.sender != leaderPeerID {
		t.Fatalf("notification sender = %s, want %s", n.sender, leaderPeerID)
	}
	if n.value != "hello" {
		t.Fatalf("notification value = %q, want %q", n.value, "hello")
	}
}

// TestStopWatchExecuteStopsDelivery confirms StopWatchExecute actually
// stops callbacks from firing, not just that nothing new happened to
// arrive yet.
func TestStopWatchExecuteStopsDelivery(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		StopWatchExecute()
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb := newRecordingCallback()
	if err := WatchExecute(cb); err != nil {
		t.Fatalf("WatchExecute: %v", err)
	}

	send := func(value string) {
		t.Helper()
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := shmclient.Execute(ctx, leaderPeerID, PeerID(), []byte(value)); err != nil {
			t.Fatalf("shmclient.Execute: %v", err)
		}
	}

	send("before-stop")
	if n := cb.next(t, 10*time.Second); n.value != "before-stop" {
		t.Fatalf("notification value = %q, want %q", n.value, "before-stop")
	}

	StopWatchExecute()
	send("after-stop")
	cb.expectNone(t, 2*time.Second)
}

// TestWatchExecuteReplacesPreviousWatcher confirms a second WatchExecute
// call redirects delivery to the new callback instead of running both.
func TestWatchExecuteReplacesPreviousWatcher(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID := peerIDFromMultiaddr(t, leaderAddr)

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		StopWatchExecute()
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	cb1 := newRecordingCallback()
	if err := WatchExecute(cb1); err != nil {
		t.Fatalf("WatchExecute (first): %v", err)
	}
	cb2 := newRecordingCallback()
	if err := WatchExecute(cb2); err != nil {
		t.Fatalf("WatchExecute (second): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shmclient.Execute(ctx, leaderPeerID, PeerID(), []byte("only-for-second")); err != nil {
		t.Fatalf("shmclient.Execute: %v", err)
	}

	n := cb2.next(t, 10*time.Second)
	if n.value != "only-for-second" {
		t.Fatalf("cb2 notification value = %q, want %q", n.value, "only-for-second")
	}
	cb1.expectNone(t, 2*time.Second)
}

// TestWatchExecuteSurvivesStopStart confirms a single WatchExecute
// registration keeps working across a Stop/Start identity switch, with no
// need to re-register -- the property WatchExecute's doc comment promises.
// Uses two independent leaders for the same reason
// TestStopThenStartSwitchesIdentity does: a killed voter follower strands
// its old leader below quorum, unrelated to what this test is checking.
func TestWatchExecuteSurvivesStopStart(t *testing.T) {
	firstLeaderAddr := spawnTestLeader(t, t.TempDir())
	secondLeaderAddr := spawnTestLeader(t, t.TempDir())
	secondLeaderPeerID := peerIDFromMultiaddr(t, secondLeaderAddr)

	prevLeader := leaderMultiaddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		StopWatchExecute()
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	cb := newRecordingCallback()
	if err := WatchExecute(cb); err != nil {
		t.Fatalf("WatchExecute: %v", err)
	}

	leaderMultiaddr = firstLeaderAddr
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start (first): %v", err)
	}
	if err := Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	leaderMultiaddr = secondLeaderAddr
	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start (second): %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := shmclient.Execute(ctx, secondLeaderPeerID, PeerID(), []byte("after-switch")); err != nil {
		t.Fatalf("shmclient.Execute: %v", err)
	}

	n := cb.next(t, 10*time.Second)
	if n.value != "after-switch" {
		t.Fatalf("notification value = %q, want %q", n.value, "after-switch")
	}
}
