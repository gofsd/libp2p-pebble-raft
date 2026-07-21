package kvctl

import (
	"context"
	"fmt"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
)

// KV is one key/value pair, as returned by RangeScan/RangeScanFrom.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RangeScan implements `mage rangescan <start> <end> [limit]`: returns
// every key/value pair in [start, end] (both inclusive, lexicographic byte
// order over the raw key bytes -- not numeric or any other ordering) on
// the current node, up to limit results (0 = unlimited). This is a thin,
// generic counterpart to every internal caller already built on the same
// shmevent.EventListRange primitive (listUnitIDs, ListClusterMembers,
// logrecord.ScanBounds) -- those all narrow to one fixed namespace, this
// one doesn't: start/end are whatever the caller passes, covering ordinary
// Set/Get keys or anything else in the store, reserved namespaces
// (shmevent.SystemKeyPrefix, pkg/logrecord's own prefix) included. That's
// not a new privilege: every kvctl call only ever reaches the local
// daemon over shmring IPC, the same same-machine trust boundary Set/Get
// already operate under (see pkg/shmevent's package doc comment) -- a
// local caller already has unrestricted read access to its own node's
// entire store; this just exposes it conveniently instead of requiring a
// raw `sendevent` call.
func RangeScan(start, end string, limit int) ([]KV, error) {
	reg, err := registry.Open()
	if err != nil {
		return nil, err
	}
	peerID, err := reg.Current()
	if err != nil {
		return nil, err
	}
	return RangeScanFrom(peerID, start, end, limit)
}

// RangeScanFrom is RangeScan against an explicit peerID, regardless of
// which node is currently selected -- the RangeScan equivalent of
// GetFrom, and how ListClusterMembers-style callers that already know
// which node they want reach this without disturbing "current".
func RangeScanFrom(peerID, start, end string, limit int) ([]KV, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()

	lo := []byte(start)
	hi := []byte(end)
	var results []KV
	for limit <= 0 || len(results) < limit {
		key, value, ok, err := shmclient.ListRange(ctx, peerID, lo, hi)
		if err != nil {
			return nil, fmt.Errorf("rangescan: %w", err)
		}
		if !ok {
			break
		}
		results = append(results, KV{Key: string(key), Value: string(value)})
		lo = append(append([]byte{}, key...), 0x00)
	}
	return results, nil
}
