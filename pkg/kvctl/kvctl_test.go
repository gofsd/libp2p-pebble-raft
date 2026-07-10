package kvctl_test

import (
	"net"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/kvctl"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
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

	// The daemon's raft timeouts default to hashicorp/raft's own
	// WAN-appropriate values (1s election/heartbeat) so a real deployment
	// isn't tuned for loopback speed. This is a same-machine test with
	// near-zero latency, so ask for a much faster cycle explicitly rather
	// than paying for a full 1s+ election on every run.
	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	t.Logf("leader peer id: %s", leaderID)

	followerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs, leaderID)
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

// freePort finds a TCP port that's free at the moment of the call, for
// pinning a node's -listen-port so it can be reliably resumed on the same
// address later. Racy in principle (another process could grab it before
// the caller does), but standard practice for tests and fine here.
func freePort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("find free port: %v", err)
	}
	defer l.Close()
	return l.Addr().(*net.TCPAddr).Port
}

// killProcess sends SIGTERM to pid and blocks until it's actually gone, so
// a subsequent ResumeNode's ensureNotAlreadyRunning check doesn't race a
// still-exiting process.
func killProcess(t *testing.T, pid int) {
	t.Helper()
	proc, err := os.FindProcess(pid)
	if err != nil {
		t.Fatalf("FindProcess(%d): %v", pid, err)
	}
	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("signal SIGTERM to pid %d: %v", pid, err)
	}
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if proc.Signal(syscall.Signal(0)) != nil {
			return
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("process %d did not exit after SIGTERM", pid)
}

// TestResumeNode drives kvctl.ResumeNode's restart-in-place path -- a
// separate code path from TestAddSetGetAcrossNodes's AddNode/join flow: the
// node's OS process is killed outright (not gracefully stopped) and then
// resumed on the same pinned port, with no leader coordination at all, and
// must come back up serving the data it had before and still accept new
// writes.
func TestResumeNode(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	// Pin the listen port so the resumed node comes back on the same
	// address it went down on -- see ResumeNode's doc comment on why that
	// matters -- and use fast raft timeouts so self-election after resume
	// doesn't pay the full WAN-appropriate 1s+ default on every test run.
	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
		"-listen-port", strconv.Itoa(freePort(t)),
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	t.Logf("leader peer id: %s", leaderID)

	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use: %v", err)
	}
	if err := kvctl.Set("foo", "bar"); err != nil {
		t.Fatalf("Set: %v", err)
	}

	info, ok, err := reg.Get(leaderID)
	if err != nil || !ok {
		t.Fatalf("registry.Get(%s): ok=%v err=%v", leaderID, ok, err)
	}
	killProcess(t, info.PID)

	if _, err := kvctl.ResumeNodeWithArgs(root, fastRaftArgs, leaderID); err != nil {
		t.Fatalf("ResumeNode: %v", err)
	}

	// The pre-restart value is read from the persistent Pebble store, which
	// doesn't require raft leadership -- it should be there immediately.
	got, err := kvctl.GetFrom(leaderID, "foo")
	if err != nil {
		t.Fatalf("GetFrom(foo) after resume: %v", err)
	}
	if got != "bar" {
		t.Fatalf("GetFrom(foo) after resume = %q, want %q", got, "bar")
	}

	// A write after resume goes through raft and so needs the node to have
	// re-elected itself leader; handleSet's awaitLeader absorbs that wait.
	if err := kvctl.Set("baz", "qux"); err != nil {
		t.Fatalf("Set after resume: %v", err)
	}
	got, err = kvctl.GetFrom(leaderID, "baz")
	if err != nil {
		t.Fatalf("GetFrom(baz) after resume: %v", err)
	}
	if got != "qux" {
		t.Fatalf("GetFrom(baz) after resume = %q, want %q", got, "qux")
	}
}
