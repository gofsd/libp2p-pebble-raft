package kvmobile

import (
	"context"
	"encoding/json"
	"fmt"
)

// KV mirrors pkg/kvctl.KV (one key/value pair) for JSON marshaling across
// the gomobile boundary -- see RangeScan.
type KV struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// RangeScan returns every key/value pair in [start, end] (both inclusive,
// lexicographic byte order over the raw key bytes) on this device's own
// locally replicated state, as a JSON array (`"[]"` if none), up to limit
// results (0 = unlimited) -- the kvmobile counterpart to desktop's
// kvctl.RangeScan, a generic complement to Submit/Get for inspecting a
// whole range of keys at once. Like Submit/Get it isn't restricted to any
// particular namespace -- see kvctl.RangeScan's doc comment for why that's
// not a new privilege: this device's own daemon is no more (and no less)
// trusted than it already is for Submit/Get.
func RangeScan(start, end string, limit int) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	lo := []byte(start)
	hi := []byte(end)
	results := []KV{}
	for limit <= 0 || len(results) < limit {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: range scan: %w", err)
		}
		if !ok {
			break
		}
		results = append(results, KV{Key: string(key), Value: string(value)})
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(results)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode range scan results: %w", err)
	}
	return string(out), nil
}
