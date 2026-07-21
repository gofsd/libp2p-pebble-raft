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
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
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

// TestRangeScan drives kvctl.RangeScan/RangeScanFrom against a real
// (spawned) node: writes a handful of keys, some sharing a prefix and one
// deliberately outside it, then checks a scan over just that prefix's
// range returns exactly the matching keys in ascending byte order, that
// limit caps the result count, and that RangeScanFrom targets an explicit
// peer id without disturbing "current".
func TestRangeScan(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}

	for _, kv := range [][2]string{
		{"scan:a", "1"},
		{"scan:b", "2"},
		{"scan:c", "3"},
		{"zzz-outside-the-range", "should not appear"},
	} {
		if err := kvctl.Set(kv[0], kv[1]); err != nil {
			t.Fatalf("Set(%s): %v", kv[0], err)
		}
	}

	results, err := kvctl.RangeScan("scan:", "scan:\xff", 0)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	want := []kvctl.KV{
		{Key: "scan:a", Value: "1"},
		{Key: "scan:b", Value: "2"},
		{Key: "scan:c", Value: "3"},
	}
	if len(results) != len(want) {
		t.Fatalf("RangeScan returned %d results, want %d: %+v", len(results), len(want), results)
	}
	for i, w := range want {
		if results[i] != w {
			t.Fatalf("RangeScan result[%d] = %+v, want %+v", i, results[i], w)
		}
	}

	limited, err := kvctl.RangeScan("scan:", "scan:\xff", 2)
	if err != nil {
		t.Fatalf("RangeScan (limit=2): %v", err)
	}
	if len(limited) != 2 {
		t.Fatalf("RangeScan (limit=2) returned %d results, want 2: %+v", len(limited), limited)
	}
	if limited[0] != want[0] || limited[1] != want[1] {
		t.Fatalf("RangeScan (limit=2) = %+v, want first 2 of %+v", limited, want)
	}

	// RangeScanFrom targets leaderID explicitly, whether or not it's
	// "current" -- switch current away first to prove that.
	otherID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (other): %v", err)
	}
	if err := kvctl.Use(otherID); err != nil {
		t.Fatalf("Use(other): %v", err)
	}
	fromLeader, err := kvctl.RangeScanFrom(leaderID, "scan:", "scan:\xff", 0)
	if err != nil {
		t.Fatalf("RangeScanFrom(leader): %v", err)
	}
	if len(fromLeader) != len(want) {
		t.Fatalf("RangeScanFrom(leader) returned %d results, want %d: %+v", len(fromLeader), len(want), fromLeader)
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

// TestAddNodeWithKeyReusesIdentity drives kvctl.AddNodeWithKey: it creates a
// node normally (minting a fresh identity), saves a copy of its
// identity.key, deletes the node entirely, then re-provisions a brand new
// data directory from that saved key via AddNodeWithKey. The resulting peer
// id must match the original -- proving the peer id really is derived from
// (and stable across a fresh import of) the supplied key, the scenario
// AddNodeWithKey exists for (e.g. restoring a node after its data dir was
// lost, as long as the key itself was backed up).
func TestAddNodeWithKeyReusesIdentity(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	originalID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	info, ok, err := reg.Get(originalID)
	if err != nil || !ok {
		t.Fatalf("registry.Get(%s): ok=%v err=%v", originalID, ok, err)
	}

	keyBytes, err := os.ReadFile(info.KeyPath)
	if err != nil {
		t.Fatalf("read identity.key: %v", err)
	}
	savedKeyPath := filepath.Join(t.TempDir(), "saved-identity.key")
	if err := os.WriteFile(savedKeyPath, keyBytes, 0o600); err != nil {
		t.Fatalf("save identity.key copy: %v", err)
	}

	killProcess(t, info.PID)
	if err := kvctl.DeleteNode(originalID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}

	restoredID, err := kvctl.AddNodeWithKeyAndArgs(root, savedKeyPath, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNodeWithKey: %v", err)
	}
	if restoredID != originalID {
		t.Fatalf("AddNodeWithKey peer id = %s, want %s (re-imported the same identity)", restoredID, originalID)
	}
}

// TestDeleteNode drives kvctl.DeleteNode: it must refuse while the node's
// daemon is still running, and once stopped must remove both the registry
// entry and the on-disk data directory.
func TestDeleteNode(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	peerID, err := kvctl.AddNode(root)
	if err != nil {
		t.Fatalf("AddNode: %v", err)
	}
	info, ok, err := reg.Get(peerID)
	if err != nil || !ok {
		t.Fatalf("registry.Get(%s): ok=%v err=%v", peerID, ok, err)
	}

	if err := kvctl.DeleteNode(peerID); err == nil {
		t.Fatalf("DeleteNode while running: want error, got none")
	}

	killProcess(t, info.PID)

	if err := kvctl.DeleteNode(peerID); err != nil {
		t.Fatalf("DeleteNode: %v", err)
	}
	if _, ok, err := reg.Get(peerID); err != nil {
		t.Fatalf("registry.Get after DeleteNode: %v", err)
	} else if ok {
		t.Fatalf("registry.Get after DeleteNode: entry still present")
	}
	if _, err := os.Stat(info.DataDir); !os.IsNotExist(err) {
		t.Fatalf("data dir %s still exists after DeleteNode (stat err: %v)", info.DataDir, err)
	}
}

// TestRequestConfirmPermitAcrossNodes drives kvctl.RequestPermit/
// ConfirmPermit (the CLI-facing plumbing behind `mage requestpermit`/
// `confirmpermit`) end to end through two real spawned nodes and real
// pkg/shmclient/pkg/ipc round trips -- not just the daemon-internal
// dispatch pkg/daemon's own permit_test.go/caller_identity_test.go
// already cover. The follower lodges a request for an arbitrary target
// peer id, the leader (a real raft voter) confirms it, and the confirmed
// system record shows up under GetFrom on both nodes once raft replicates
// it -- proving the CLI path reaches the exact same shmevent.SystemKey
// pkg/daemon's own handling reads and writes.
func TestRequestConfirmPermitAcrossNodes(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	followerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs, leaderID)
	if err != nil {
		t.Fatalf("AddNode (follower): %v", err)
	}

	const targetPeerID = "some-target-peer-id"

	if err := kvctl.Use(followerID); err != nil {
		t.Fatalf("Use(follower): %v", err)
	}
	if err := kvctl.RequestPermit(shmevent.KindPermitPeer, []byte(targetPeerID), nil); err != nil {
		t.Fatalf("RequestPermit: %v", err)
	}

	pendingKey := string(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusPending, []byte(targetPeerID)))
	deadline := time.Now().Add(10 * time.Second)
	var (
		got     string
		lastErr error
	)
	for time.Now().Before(deadline) {
		got, lastErr = kvctl.GetFrom(leaderID, pendingKey)
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("GetFrom(leader, pendingKey) after RequestPermit: %v", lastErr)
	}
	_ = got // metadata is empty for KindPermitPeer; just needs to exist

	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}
	if err := kvctl.ConfirmPermit(shmevent.KindPermitPeer, []byte(targetPeerID)); err != nil {
		t.Fatalf("ConfirmPermit: %v", err)
	}

	confirmedKey := string(shmevent.SystemKey(shmevent.KindPermitPeer, shmevent.StatusConfirmed, []byte(targetPeerID)))
	deadline = time.Now().Add(10 * time.Second)
	for time.Now().Before(deadline) {
		_, lastErr = kvctl.GetFrom(followerID, confirmedKey)
		if lastErr == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if lastErr != nil {
		t.Fatalf("GetFrom(follower, confirmedKey) after ConfirmPermit: %v", lastErr)
	}
}

// TestJoinConfirmLeaveRejoinRm drives kvctl.Join/Leave/Rm end to end
// through two real spawned nodes, with the leader's -require-confirm-for-join
// flag on -- the CLI-facing path behind `mage join/leave/rejoinnode/rm`. It
// walks the whole cluster-membership lifecycle: a solo identity joins the
// leader's cluster (pending until confirmed), gets confirmed, replicates
// data, leaves gracefully (the leader's cluster keeps its own data intact
// and the joiner resumes its own solo db), rejoins the same cluster
// (reusing the composite dir Leave left on disk), and finally Rm's out of
// it -- proving the composite dir is gone and a further join attempt
// needs a *fresh* confirmation, not one silently reused from before.
func TestJoinConfirmLeaveRejoinRm(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}
	gatedArgs := append(append([]string{}, fastRaftArgs...), "-require-confirm-for-join")

	leaderID, err := kvctl.AddNodeWithArgs(root, gatedArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	joinerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (joiner, solo): %v", err)
	}

	memberKey := string(shmevent.ClusterMemberKey([]byte(joinerID)))

	waitFor := func(cond func() bool, what string) {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			if cond() {
				return
			}
			time.Sleep(100 * time.Millisecond)
		}
		t.Fatalf("timed out waiting for: %s", what)
	}
	isClusterMember := func() bool {
		_, err := kvctl.GetFrom(leaderID, memberKey)
		return err == nil
	}

	// --- join (pending until confirmed) ---
	if err := kvctl.Use(joinerID); err != nil {
		t.Fatalf("Use(joiner): %v", err)
	}
	returned, err := kvctl.Join(root, leaderID)
	if err != nil {
		t.Fatalf("Join: %v", err)
	}
	if returned != joinerID {
		t.Fatalf("Join returned %q, want %q", returned, joinerID)
	}
	if isClusterMember() {
		t.Fatal("joiner already a cluster member before confirmation")
	}

	// --- confirm (from the leader, itself a real voter) ---
	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader): %v", err)
	}
	if err := kvctl.ConfirmPermit(shmevent.KindClusterJoin, []byte(joinerID)); err != nil {
		t.Fatalf("ConfirmPermit(cluster-join): %v", err)
	}
	waitFor(isClusterMember, "joiner admitted as cluster member")

	// --- data replicates once admitted ---
	if err := kvctl.Set("foo", "bar"); err != nil {
		t.Fatalf("Set: %v", err)
	}
	waitFor(func() bool {
		got, err := kvctl.GetFrom(joinerID, "foo")
		return err == nil && got == "bar"
	}, "joiner replicated foo=bar")

	// --- leave: graceful shrink, joiner resumes its own solo db ---
	if err := kvctl.Leave(root, joinerID); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	info, ok, err := reg.Get(joinerID)
	if err != nil || !ok {
		t.Fatalf("registry.Get(joiner) after Leave: ok=%v err=%v", ok, err)
	}
	if info.ClusterPeerID != "" {
		t.Fatalf("joiner still shows ClusterPeerID=%q after Leave", info.ClusterPeerID)
	}
	if info.DataDir != reg.NodeDataDir(joinerID) {
		t.Fatalf("joiner DataDir = %s after Leave, want its solo dir %s", info.DataDir, reg.NodeDataDir(joinerID))
	}
	if isClusterMember() {
		t.Fatal("joiner still a cluster member after Leave")
	}

	// --- rejoin: same cluster, composite dir preserved, needs a fresh
	// confirmation again since Leave's RemoveServer already dropped it
	// from the raft configuration ---
	if err := kvctl.Use(joinerID); err != nil {
		t.Fatalf("Use(joiner) before rejoin: %v", err)
	}
	if _, err := kvctl.Join(root, leaderID); err != nil {
		t.Fatalf("Join (rejoin after Leave): %v", err)
	}
	if isClusterMember() {
		t.Fatal("joiner auto-admitted on rejoin without a fresh confirmation")
	}
	if err := kvctl.Use(leaderID); err != nil {
		t.Fatalf("Use(leader) for rejoin confirm: %v", err)
	}
	if err := kvctl.ConfirmPermit(shmevent.KindClusterJoin, []byte(joinerID)); err != nil {
		t.Fatalf("ConfirmPermit(cluster-join) on rejoin: %v", err)
	}
	waitFor(isClusterMember, "joiner re-admitted as cluster member after rejoin")

	// --- rm: leave + revoke standing + wipe the composite dir ---
	if err := kvctl.Rm(root, joinerID); err != nil {
		t.Fatalf("Rm: %v", err)
	}
	info2, ok2, err := reg.Get(joinerID)
	if err != nil || !ok2 {
		t.Fatalf("registry.Get(joiner) after Rm: ok=%v err=%v", ok2, err)
	}
	if info2.ClusterPeerID != "" {
		t.Fatalf("joiner still shows ClusterPeerID=%q after Rm", info2.ClusterPeerID)
	}
	compositeDir := reg.ClusterDataDir(joinerID, leaderID)
	if _, err := os.Stat(compositeDir); !os.IsNotExist(err) {
		t.Fatalf("composite dir %s still exists after Rm (stat err: %v)", compositeDir, err)
	}
	if isClusterMember() {
		t.Fatal("joiner still a cluster member after Rm")
	}

	// --- a further join attempt must require confirmation again, not be
	// silently re-admitted by any standing left over from before ---
	if err := kvctl.Use(joinerID); err != nil {
		t.Fatalf("Use(joiner) after Rm: %v", err)
	}
	if _, err := kvctl.Join(root, leaderID); err != nil {
		t.Fatalf("Join (after Rm): %v", err)
	}
	if isClusterMember() {
		t.Fatal("joiner auto-admitted after Rm -- confirmation should be required again")
	}
}

// TestListClustersAndListClusterMembers drives ListClusters (a pure
// registry read -- "show all available raft clusters") and
// ListClusterMembers (a live query against one already-running node --
// "return peer ids of this cluster"), covering the pairing the task asked
// for: an item ListClusters names should be exactly what ListClusterMembers
// accepts.
func TestListClustersAndListClusterMembers(t *testing.T) {
	root := repoRoot(t)
	home := t.TempDir()
	t.Setenv(registry.EnvHome, home)

	reg, err := registry.Open()
	if err != nil {
		t.Fatalf("registry.Open: %v", err)
	}
	t.Cleanup(func() { killAllRegistered(t, reg) })

	fastRaftArgs := []string{
		"-raft-heartbeat-timeout", "300ms",
		"-raft-election-timeout", "300ms",
		"-raft-leader-lease-timeout", "250ms",
	}

	leaderID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs)
	if err != nil {
		t.Fatalf("AddNode (leader): %v", err)
	}
	followerID, err := kvctl.AddNodeWithArgs(root, fastRaftArgs, leaderID)
	if err != nil {
		t.Fatalf("AddNode (follower): %v", err)
	}

	// --- ListClusters: a pure registry read, grouping both local
	// identities under one cluster keyed by the leader's peer id ---
	clusters, err := kvctl.ListClusters()
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	if len(clusters) != 1 {
		t.Fatalf("ListClusters returned %d clusters, want 1: %+v", len(clusters), clusters)
	}
	if clusters[0].ClusterID != leaderID {
		t.Fatalf("ListClusters cluster id = %s, want %s", clusters[0].ClusterID, leaderID)
	}
	if len(clusters[0].Members) != 2 {
		t.Fatalf("ListClusters member count = %d, want 2: %+v", len(clusters[0].Members), clusters[0].Members)
	}
	for _, m := range clusters[0].Members {
		if !m.Running {
			t.Fatalf("ListClusters member %s reported not running, both nodes are still up", m.PeerID)
		}
	}

	// --- ListClusterMembers: a live query against the leader (any listed
	// member's peer id works -- checked below for the follower too),
	// polled since the follower's join record replicating to whichever
	// node answers is asynchronous ---
	deadline := time.Now().Add(10 * time.Second)
	var members []kvctl.ClusterMember
	for time.Now().Before(deadline) {
		members, err = kvctl.ListClusterMembers(leaderID)
		if err == nil && len(members) == 2 {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if err != nil {
		t.Fatalf("ListClusterMembers(leader): %v", err)
	}
	byPeerID := make(map[string]string, len(members))
	for _, m := range members {
		byPeerID[m.PeerID] = m.Role
	}
	if byPeerID[leaderID] != "leader" {
		t.Fatalf("ListClusterMembers(leader): leader role = %q, want %q (members: %+v)", byPeerID[leaderID], "leader", members)
	}
	if byPeerID[followerID] != "voter" {
		t.Fatalf("ListClusterMembers(leader): follower role = %q, want %q (members: %+v)", byPeerID[followerID], "voter", members)
	}

	// --- asking the follower instead must agree: it's the same live raft
	// configuration, replicated to every member ---
	followerView, err := kvctl.ListClusterMembers(followerID)
	if err != nil {
		t.Fatalf("ListClusterMembers(follower): %v", err)
	}
	if len(followerView) != 2 {
		t.Fatalf("ListClusterMembers(follower) returned %d members, want 2: %+v", len(followerView), followerView)
	}

	// --- an unknown peer id is refused outright, and a known-but-stopped
	// one is refused with a clear reason rather than hanging ---
	if _, err := kvctl.ListClusterMembers("not-a-real-peer-id"); err == nil {
		t.Fatal("ListClusterMembers(unknown peer id): want error, got none")
	}
}
