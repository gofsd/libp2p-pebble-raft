package kvmobile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// TestListClustersAndListClusterMembers drives ListClusters (0 or 1
// entries -- whichever cluster this device is currently joined to, see
// ClusterInfo's doc comment for why kvmobile can't report more than
// desktop's multi-cluster registry can) and ListClusterMembers (a live
// query against this device's own already-running daemon) against a real
// (in-process) leader.
func TestListClustersAndListClusterMembers(t *testing.T) {
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

	// Before Start, there's nothing to report.
	before, err := ListClusters()
	if err != nil {
		t.Fatalf("ListClusters (before Start): %v", err)
	}
	if before != "[]" {
		t.Fatalf("ListClusters (before Start) = %s, want []", before)
	}

	id, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	clustersJSON, err := ListClusters()
	if err != nil {
		t.Fatalf("ListClusters: %v", err)
	}
	var clusters []ClusterInfo
	if err := json.Unmarshal([]byte(clustersJSON), &clusters); err != nil {
		t.Fatalf("unmarshal ListClusters result %s: %v", clustersJSON, err)
	}
	if len(clusters) != 1 {
		t.Fatalf("ListClusters returned %d entries, want 1: %s", len(clusters), clustersJSON)
	}
	if clusters[0].ClusterID != leaderPeerID {
		t.Fatalf("ListClusters cluster id = %s, want %s", clusters[0].ClusterID, leaderPeerID)
	}
	if clusters[0].PeerID != id {
		t.Fatalf("ListClusters peer id = %s, want %s", clusters[0].PeerID, id)
	}

	// ListClusterMembers: polled, since the leader's own KindClusterMember
	// record replicating to this brand-new follower is asynchronous.
	deadline := time.Now().Add(10 * time.Second)
	var (
		members    []ClusterMember
		membersErr error
	)
	for time.Now().Before(deadline) {
		var membersJSON string
		membersJSON, membersErr = ListClusterMembers()
		if membersErr == nil {
			if err := json.Unmarshal([]byte(membersJSON), &members); err != nil {
				t.Fatalf("unmarshal ListClusterMembers result %s: %v", membersJSON, err)
			}
			if len(members) == 2 {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
	if membersErr != nil {
		t.Fatalf("ListClusterMembers: %v", membersErr)
	}
	byPeerID := make(map[string]string, len(members))
	for _, m := range members {
		byPeerID[m.PeerID] = m.Role
	}
	if len(members) != 2 {
		t.Fatalf("ListClusterMembers returned %d members, want 2: %+v", len(members), members)
	}
	if byPeerID[leaderPeerID] != "leader" {
		t.Fatalf("ListClusterMembers: leader role = %q, want %q (members: %+v)", byPeerID[leaderPeerID], "leader", members)
	}
	if byPeerID[id] != "voter" {
		t.Fatalf("ListClusterMembers: this device's role = %q, want %q (members: %+v)", byPeerID[id], "voter", members)
	}

	// After Stop, ListClusters reports nothing again, and ListClusterMembers
	// refuses outright (no session to query).
	if err := Stop(); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	after, err := ListClusters()
	if err != nil {
		t.Fatalf("ListClusters (after Stop): %v", err)
	}
	if after != "[]" {
		t.Fatalf("ListClusters (after Stop) = %s, want []", after)
	}
	if _, err := ListClusterMembers(); err == nil {
		t.Fatal("ListClusterMembers (after Stop): want error, got none")
	}
}
