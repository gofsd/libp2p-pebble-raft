package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// ClusterInfo mirrors pkg/kvctl.ClusterInfo's identifying field (the peer
// id of whoever originally bootstrapped a cluster) for JSON marshaling
// across the gomobile boundary -- see ListClusters. Unlike desktop, which
// tracks every cluster identity it has ever created/joined in a
// persistent registry (pkg/registry), kvmobile only ever runs one
// identity/cluster at a time and keeps no such history (see this
// package's doc comment on leaderMultiaddr always being build-time-only,
// now overridable at runtime via Join), so ListClusters can only ever
// report 0 or 1 entries: whichever cluster, if any, this device is
// currently joined to.
type ClusterInfo struct {
	ClusterID string `json:"cluster_id"`
	PeerID    string `json:"peer_id"`
}

// ListClusters reports whichever cluster this device is currently joined
// to, as a JSON array with 0 or 1 entries (`"[]"` if Start/StartWithKey/
// Join hasn't completed successfully yet) -- see ClusterInfo's doc comment
// for why kvmobile can't report more than that, unlike desktop's
// kvctl.ListClusters, which can list every cluster a whole machine's
// registry knows about. Feed the one entry's PeerID (this device's own)
// into ListClusterMembers for that cluster's full live raft membership.
func ListClusters() (string, error) {
	mu.Lock()
	clusters := []ClusterInfo{}
	if started {
		clusters = append(clusters, ClusterInfo{ClusterID: curClusterID, PeerID: peerID})
	}
	mu.Unlock()

	out, err := json.Marshal(clusters)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode clusters: %w", err)
	}
	return string(out), nil
}

// ClusterMember mirrors pkg/kvctl.ClusterMember (one entry in a raft
// cluster's live membership) for JSON marshaling -- see
// ListClusterMembers.
type ClusterMember struct {
	PeerID string `json:"peer_id"`
	Role   string `json:"role"` // "leader", "voter", or "learner" -- see shmevent.RoleName
}

// ListClusterMembers returns every peer id currently in the raft cluster
// this device is joined to, as a JSON array (`"[]"` if none, though a live
// cluster always has at least this device's own record once Start has
// completed) -- read from this device's own locally-replicated
// shmevent.KindClusterMember records, kept current on every member
// (leader and follower alike) whenever a peer joins/leaves or this
// device's own leadership status changes. The kvmobile counterpart to
// desktop's kvctl.ListClusterMembers, minus the "which locally running
// identity" parameter: kvmobile only ever runs one at a time (see
// currentSession), so there's nothing to select between.
func ListClusterMembers() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	lo, hi := shmevent.ClusterMemberKeyBounds()
	members := []ClusterMember{}
	for {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: list cluster members: %w", err)
		}
		if !ok {
			break
		}
		if len(key) < 3 {
			return "", fmt.Errorf("kvmobile: malformed cluster-member key %x", key)
		}
		peerID := string(key[3:])
		_, role, err := shmevent.DecodeClusterMemberPayload(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: decode member record for %s: %w", peerID, err)
		}
		members = append(members, ClusterMember{PeerID: peerID, Role: shmevent.RoleName(role)})
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(members)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode cluster members: %w", err)
	}
	return string(out), nil
}
