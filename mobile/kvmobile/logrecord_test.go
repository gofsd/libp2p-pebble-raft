package kvmobile

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
)

// TestLogAppendAndLogQueryThroughKvmobile drives kvmobile's LogAppend/
// LogQuery bindings end to end against a real leader: append one record,
// then query it back and check every field round-tripped, including
// AuthorPeerID (attributed to this device) and the structured Fields map.
func TestLogAppendAndLogQueryThroughKvmobile(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	followerID, err := Start(t.TempDir())
	if err != nil {
		t.Fatalf("Start: %v", err)
	}

	const (
		kind   = "audit"
		unitID = "unit-1"
	)
	if err := LogAppend(kind, unitID, `{"severity":"high"}`, "narrative text"); err != nil {
		t.Fatalf("LogAppend: %v", err)
	}

	deadline := time.Now().Add(10 * time.Second)
	var records []logrecord.Record
	for time.Now().Before(deadline) {
		resultJSON, err := LogQuery(kind, unitID, "", "", "")
		if err != nil {
			t.Fatalf("LogQuery: %v", err)
		}
		if err := json.Unmarshal([]byte(resultJSON), &records); err != nil {
			t.Fatalf("parse LogQuery result %q: %v", resultJSON, err)
		}
		if len(records) > 0 {
			break
		}
		time.Sleep(50 * time.Millisecond)
	}
	if len(records) != 1 {
		t.Fatalf("LogQuery returned %d records, want 1", len(records))
	}

	rec := records[0]
	if rec.Kind != kind {
		t.Errorf("Kind = %q, want %q", rec.Kind, kind)
	}
	if rec.UnitID != unitID {
		t.Errorf("UnitID = %q, want %q", rec.UnitID, unitID)
	}
	if rec.AuthorPeerID != followerID {
		t.Errorf("AuthorPeerID = %q, want %q", rec.AuthorPeerID, followerID)
	}
	if rec.Narrative != "narrative text" {
		t.Errorf("Narrative = %q, want %q", rec.Narrative, "narrative text")
	}
	if rec.Fields["severity"] != "high" {
		t.Errorf("Fields[severity] = %q, want %q", rec.Fields["severity"], "high")
	}
}

// TestLogQueryEmptyResultIsEmptyArray checks LogQuery returns "[]", not
// "null", when nothing matches -- so Kotlin's JSON decoder always gets a
// well-formed array to iterate, never a null it has to special-case.
func TestLogQueryEmptyResultIsEmptyArray(t *testing.T) {
	leaderAddr := spawnTestLeader(t, t.TempDir())

	prevLeader := leaderMultiaddr
	leaderMultiaddr = leaderAddr
	t.Cleanup(func() {
		leaderMultiaddr = prevLeader
		if err := Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}
	})

	if _, err := Start(t.TempDir()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	resultJSON, err := LogQuery("nonexistent-kind", "nonexistent-unit", "", "", "")
	if err != nil {
		t.Fatalf("LogQuery: %v", err)
	}
	if resultJSON != "[]" {
		t.Fatalf("LogQuery result = %q, want %q", resultJSON, "[]")
	}
}
