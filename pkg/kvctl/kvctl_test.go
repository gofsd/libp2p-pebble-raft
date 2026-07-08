package kvctl_test

import (
	"os"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-pebble-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-pebble-raft/pkg/registry"
)

// repoRoot walks up from this test file's location to the module root
// (which contains go.mod and cmd/kvnode), so kvctl can `go build
// ./cmd/kvnode` regardless of the working directory `go test` uses.
func repoRoot(t *testing.T) string {
	t.Helper()
	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("cannot determine test file path")
	}
	return filepath.Join(filepath.Dir(file), "..", "..")
}

// killAllRegistered terminates every OS process the registry currently
// knows about. It is registered as a single up-front t.Cleanup (rather than
// one per node after each successful AddNode) because AddNode can spawn and
// register a daemon and still return an error afterward (e.g. its bootstrap
// IPC round trip fails) -- in which case the test would never learn that
// node's peer id to clean it up individually, but the registry already has
// its PID.
func killAllRegistered(t *testing.T, reg *registry.Registry) {
	t.Helper()
	nodes, err := reg.List()
	if err != nil {
		return
	}
	var wg sync.WaitGroup
	for _, info := range nodes {
		if info.PID == 0 {
			continue
		}
		proc, err := os.FindProcess(info.PID)
		if err != nil {
			continue
		}
		wg.Add(1)
		go func(proc *os.Process) {
			defer wg.Done()
			_ = proc.Signal(syscall.SIGTERM)
			done := make(chan struct{})
			go func() {
				proc.Wait()
				close(done)
			}()
			select {
			case <-done:
			case <-time.After(3 * time.Second):
				_ = proc.Kill()
			}
		}(proc)
	}
	wg.Wait()
}

// TestAddSetGetAcrossNodes drives the whole stack through its public,
// mage-target-facing API: it creates a leader node, joins a follower to it,
// writes a key through raft on the leader, and confirms the value shows up
// on the follower's own locally replicated state -- the scenario the task
// asked to be covered by a test ("adding nodes, set value for one node and
// read this value from other db node").
func TestAddSetGetAcrossNodes(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	leaderID, err := kvctl.AddNode(root)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	t.Logf("leader peer id: %s", leaderID)

	followerID, err := kvctl.AddNode(root, leaderID)
	if err != nil {
		t.Fatalf("AddNode (follower): %v", err)
	}
	t.Logf("follower peer id: %s", followerID)

	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}
	if err := kvctl.Set("foo", "bar"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	// Raft replication to the follower, and the follower's own apply of the
	// committed entry, are asynchronous; poll instead of assuming it's
	// instantaneous.
	deadline := time.Now().Add(10 * time.Second)
	var (
		got     string
		lastErr error
	)
	for time.Now().Before(deadline) {
		got, lastErr = kvctl.GetFrom(followerID, "foo")
		if lastErr == nil && got == "bar" {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("GetFrom(follower, foo): %v", lastErr)
	}
	if got != "bar" {
		t.Fatalf("GetFrom(follower, foo) = %q, want %q", got, "bar")
	}

	// Sanity check: the leader also has the value locally.
	leaderGot, err := kvctl.GetFrom(leaderID, "foo")
	if err != nil {
		t.Fatalf("GetFrom(leader, foo): %v", err)
	}
	if leaderGot != "bar" {
		t.Fatalf("GetFrom(leader, foo) = %q, want %q", leaderGot, "bar")
	}
}
