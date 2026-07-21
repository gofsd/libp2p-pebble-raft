package kvmobile

import (
	"context"
	"encoding/hex"
	"os"
	"path/filepath"
	"testing"

	lp2pcrypto "github.com/libp2p/go-libp2p/core/crypto"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// marshaledKeyHex converts a raw stdlib ed25519 key (e2edata.GenerateIdentity's
// format) to the hex-encoded-marshaled-libp2p-key format StartWithKey/
// importIdentity expect -- the same conversion e2edata.WriteDesktopKeyFile
// does for a desktop key file, just returned as a string instead of
// written to disk.
func marshaledKeyHex(t *testing.T, priv shmevent.PrivateKey) string {
	t.Helper()
	lp2pPriv, err := lp2pcrypto.UnmarshalEd25519PrivateKey(priv)
	if err != nil {
		t.Fatalf("UnmarshalEd25519PrivateKey: %v", err)
	}
	marshaled, err := lp2pcrypto.MarshalPrivateKey(lp2pPriv)
	if err != nil {
		t.Fatalf("MarshalPrivateKey: %v", err)
	}
	return hex.EncodeToString(marshaled)
}

// TestImportIdentityMismatch drives importIdentity directly (no daemon
// needed, it's a pure filesystem+crypto function): importing the same key
// twice into the same dataDir is idempotent, but importing a *different*
// key into a dataDir that already holds one must be refused rather than
// silently overwriting an identity that may have already-replicated raft
// state under it.
func TestImportIdentityMismatch(t *testing.T) {
	_, priv1, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	_, priv2, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	wantPeerID, err := e2edata.PeerIDFromPrivateKey(priv1)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}
	keyHex1 := marshaledKeyHex(t, priv1)
	keyHex2 := marshaledKeyHex(t, priv2)

	dir := t.TempDir()
	_, gotPeerID, err := importIdentity(dir, keyHex1)
	if err != nil {
		t.Fatalf("importIdentity (first): %v", err)
	}
	if gotPeerID != wantPeerID {
		t.Fatalf("importIdentity peer id = %s, want %s", gotPeerID, wantPeerID)
	}

	// Same key again: idempotent, no error.
	if _, gotPeerID, err := importIdentity(dir, keyHex1); err != nil {
		t.Fatalf("importIdentity (repeat, same key): %v", err)
	} else if gotPeerID != wantPeerID {
		t.Fatalf("importIdentity (repeat) peer id = %s, want %s", gotPeerID, wantPeerID)
	}

	// Different key: must refuse, not overwrite.
	if _, _, err := importIdentity(dir, keyHex2); err == nil {
		t.Fatalf("importIdentity (different key over existing identity): want error, got none")
	}
}

// TestStartWithKeyDerivesGivenIdentity drives StartWithKey against a real
// (in-process) leader and checks the follower comes up as exactly the peer
// id the supplied key derives to -- proving the caller, not ensureIdentity,
// controls the identity when StartWithKey is used.
func TestStartWithKeyDerivesGivenIdentity(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	_, priv, err := e2edata.GenerateIdentity()
	if err != nil {
		t.Fatalf("GenerateIdentity: %v", err)
	}
	wantPeerID, err := e2edata.PeerIDFromPrivateKey(priv)
	if err != nil {
		t.Fatalf("PeerIDFromPrivateKey: %v", err)
	}

	gotPeerID, err := StartWithKey(t.TempDir(), marshaledKeyHex(t, priv))
	if err != nil {
		t.Fatalf("StartWithKey: %v", err)
	}
	if gotPeerID != wantPeerID {
		t.Fatalf("StartWithKey peer id = %s, want %s", gotPeerID, wantPeerID)
	}
	if got := PeerID(); got != wantPeerID {
		t.Fatalf("PeerID() = %s, want %s", got, wantPeerID)
	}
}

// TestStopThenStartSwitchesIdentity drives the Stop-then-Start pattern that
// stands in for desktop's `mage use <peerID>` on Android: kvmobile only
// ever runs one daemon per process, so "switching" means stopping whatever
// is currently running and starting a different dataDir/identity in its
// place. Confirms the two starts really do come up as different peer ids
// and PeerID() tracks whichever is currently running.
//
// Each Start joins its leader as a full raft voter (see CLAUDE.md's "Node
// connectivity policy" / README's 2-voter-cluster note), so this uses two
// independent leaders rather than reusing one across both Starts: killing
// the first follower via Stop -- not Leave, which performs a graceful
// raft.RemoveServer first, see TestLeaveShrinksClusterAndStops below --
// would otherwise strand that leader's cluster at 1-alive-of-2-voters --
// unable to commit the second join's own AddVoter -- which is a real,
// documented limitation of a 2-voter raft cluster, not something
// Stop/Start can or should paper over.
func TestStopThenStartSwitchesIdentity(t *testing.T) {
	firstLeaderAddr := spawnTestLeader(t, t.TempDir())
	secondLeaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	leaderMultiaddr = firstLeaderAddr
	firstID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start (first): %v", err)
	}
	if got := PeerID(); got != firstID {
		t.Fatalf("PeerID() after first Start = %s, want %s", got, firstID)
	}

	if err := Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if got := PeerID(); got != "" {
		t.Fatalf("PeerID() after Stop = %s, want \"\"", got)
	}

	leaderMultiaddr = secondLeaderAddr
	secondID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start (second, different dataDir/leader): %v", err)
	}
	if secondID == firstID {
		t.Fatalf("Start (second) peer id = %s, same as first -- expected a distinct fresh identity in a fresh dataDir", secondID)
	}
	if got := PeerID(); got != secondID {
		t.Fatalf("PeerID() after second Start = %s, want %s", got, secondID)
	}
}

// TestDeleteRefusesWhileRunning drives Delete: it must refuse while a
// daemon is running against the target dataDir, and once Stopped must
// remove the directory outright.
func TestDeleteRefusesWhileRunning(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() { leaderMultiaddr = prevLeader })

	dir := filepath.Join(t.TempDir(), "node")
	if _, err := Start(dir); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if err := Delete(dir); err == nil {
		t.Fatalf("Delete while running: want error, got none")
	}

	if err := Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}

	if err := Delete(dir); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	if _, err := os.Stat(dir); !os.IsNotExist(err) {
		t.Fatalf("dataDir %s still exists after Delete (stat err: %v)", dir, err)
	}
}

// TestLeaveShrinksClusterAndStops drives Leave against a real (in-process)
// leader: it must actually remove this device from the leader's raft
// configuration (raft.RemoveServer, not just stop the local daemon) and
// then stop it, mirroring desktop's pkg/kvctl.Leave -- see that package's
// own TestJoinConfirmLeaveRejoinRm for the fuller lifecycle this only
// needs to prove once here, since the underlying shmevent.EventLeave
// mechanics are already covered end to end in pkg/daemon.
func TestLeaveShrinksClusterAndStops(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID, err := registry.ExtractPeerID(leaderAddr)
	if err != nil {
		t.Fatalf("ExtractPeerID: %v", err)
	}

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		_ = Stop()
	})

	id, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	ctx := context.Background()
	memberKey := string(shmevent.ClusterMemberKey([]byte(id)))
	if _, err := shmclient.Get(ctx, leaderPeerID, memberKey); err != nil {
		t.Fatalf("follower not a cluster member after Start: %v", err)
	}

	if err := Leave(); err != nil {
		t.Fatalf("Leave: %v", err)
	}
	if got := PeerID(); got != "" {
		t.Fatalf("PeerID() after Leave = %s, want \"\" (daemon should be stopped)", got)
	}
	if _, err := shmclient.Get(ctx, leaderPeerID, memberKey); err == nil {
		t.Fatalf("follower still a cluster member after Leave")
	}
}

// TestRmDeletesClusterDirAndStops drives Rm: everything
// TestLeaveShrinksClusterAndStops proves, plus the joined cluster's local
// data subdirectory must actually be gone afterward -- mirroring
// desktop's pkg/kvctl.Rm.
func TestRmDeletesClusterDirAndStops(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())
	leaderPeerID, err := registry.ExtractPeerID(leaderAddr)
	if err != nil {
		t.Fatalf("ExtractPeerID: %v", err)
	}

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		_ = Stop()
	})

	dataDirRoot := t.TempDir()
	id, err := Start(dataDirRoot)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	clusterDir := filepath.Join(dataDirRoot, registry.ClusterDirName(id, leaderPeerID))
	if _, err := os.Stat(clusterDir); err != nil {
		t.Fatalf("cluster dir %s missing after Start: %v", clusterDir, err)
	}

	if err := Rm(); err != nil {
		t.Fatalf("Rm: %v", err)
	}
	if got := PeerID(); got != "" {
		t.Fatalf("PeerID() after Rm = %s, want \"\"", got)
	}
	if _, err := os.Stat(clusterDir); !os.IsNotExist(err) {
		t.Fatalf("cluster dir %s still exists after Rm (stat err: %v)", clusterDir, err)
	}

	ctx := context.Background()
	memberKey := string(shmevent.ClusterMemberKey([]byte(id)))
	if _, err := shmclient.Get(ctx, leaderPeerID, memberKey); err == nil {
		t.Fatalf("follower still a cluster member after Rm")
	}
}
