package kvmobile

import (
	"encoding/json"
	"testing"
)

// TestRangeScan drives RangeScan against a real (in-process) leader: sets
// a handful of keys sharing a prefix plus one deliberately outside it,
// then checks a scan over just that prefix's range returns exactly the
// matching keys in ascending order, and that limit caps the result count.
func TestRangeScan(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		_ = Stop()
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	for _, kv := range [][2]string{
		{"scan:a", "1"},
		{"scan:b", "2"},
		{"scan:c", "3"},
		{"zzz-outside-the-range", "should not appear"},
	} {
		if err := Submit(kv[0], kv[1]); err != nil {
			t.Fatalf("Submit(%s): %v", kv[0], err)
		}
	}

	resultsJSON, err := RangeScan("scan:", "scan:\xff", 0)
	if err != nil {
		t.Fatalf("RangeScan: %v", err)
	}
	var results []KV
	if err := json.Unmarshal([]byte(resultsJSON), &results); err != nil {
		t.Fatalf("unmarshal RangeScan result %s: %v", resultsJSON, err)
	}
	want := []KV{
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

	limitedJSON, err := RangeScan("scan:", "scan:\xff", 2)
	if err != nil {
		t.Fatalf("RangeScan (limit=2): %v", err)
	}
	var limited []KV
	if err := json.Unmarshal([]byte(limitedJSON), &limited); err != nil {
		t.Fatalf("unmarshal RangeScan (limit=2) result %s: %v", limitedJSON, err)
	}
	if len(limited) != 2 {
		t.Fatalf("RangeScan (limit=2) returned %d results, want 2: %+v", len(limited), limited)
	}
	if limited[0] != want[0] || limited[1] != want[1] {
		t.Fatalf("RangeScan (limit=2) = %+v, want first 2 of %+v", limited, want)
	}
}

// TestRangeScanBeforeStartRefuses drives RangeScan with no daemon running
// -- it must refuse outright, same as Submit/Get do (currentSession's
// guard), rather than hang or panic.
func TestRangeScanBeforeStartRefuses(t *testing.T) {
	if _, err := RangeScan("a", "z", 0); err == nil {
		t.Fatal("RangeScan before Start: want error, got none")
	}
}
