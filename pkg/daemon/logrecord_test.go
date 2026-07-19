package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// reportKindConfig is sample kind config data, not code constants --
// pkg/logrecord treats "kind" as an arbitrary caller-chosen string (see
// its doc comment), so this table exists purely to exercise that
// genericness with a realistic, varied set of names rather than to
// declare these as the package's supported types.
var reportKindConfig = []string{
	"journal", "opslog", "wardiary", "decklog",
	"sitrep", "spotrep", "lace", "logstat",
	"tacrep", "casrep", "intrep", "aar",
}

// appendRecord builds a pkg/logrecord key/value for kind/unitID/ts and
// sends it as EventLogAppend against n, failing the test on any error.
func appendRecord(t *testing.T, ctx context.Context, n *Node, kind, unitID string, ts time.Time, narrative string) {
	t.Helper()
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: kind, UnitID: unitID, Timestamp: ts, AuthorPeerID: n.peerID, Narrative: narrative}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventLogAppend, Value: payload, ID: 1}, n.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("log_append rejected: %s", resp.Value)
	}
}

// queryRecords drives EventListRange in a loop against n exactly the way
// pkg/kvctl.LogQuery does, returning every matching Record in order.
func queryRecords(t *testing.T, ctx context.Context, n *Node, kind, unitID string, start, end time.Time) []logrecord.Record {
	t.Helper()
	lo, hi := logrecord.ScanBounds(kind, unitID, start, end)
	var records []logrecord.Record
	for i := uint16(1); i <= 1000; i++ {
		query, err := shmevent.EncodeListRangeQuery(lo, hi)
		if err != nil {
			t.Fatalf("EncodeListRangeQuery: %v", err)
		}
		resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventListRange, Value: query, ID: i}, n.ed25519Priv)
		if resp.EventType == shmevent.EventError {
			t.Fatalf("list_range rejected: %s", resp.Value)
		}
		if len(resp.Value) == 0 {
			break
		}
		key, value, err := shmevent.DecodeListRangeQuery(resp.Value)
		if err != nil {
			t.Fatalf("DecodeListRangeQuery: %v", err)
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			t.Fatalf("logrecord.Decode: %v", err)
		}
		records = append(records, rec)
		lo = append(append([]byte{}, key...), 0x00)
	}
	return records
}

// TestLogAppendQueryConfigDriven is a real-cluster test (leader +
// follower, no mocks) proving pkg/logrecord's append+query path works
// end to end: records written for every kind in reportKindConfig --
// loaded as test data, not hardcoded per-kind logic -- replicate from
// leader to follower and come back out via EventListRange in
// chronological order, correctly scoped to the kind/unit/time window
// queried.
func TestLogAppendQueryConfigDriven(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	fastRaft := Config{
		HeartbeatTimeout:   200 * time.Millisecond,
		ElectionTimeout:    200 * time.Millisecond,
		CommitTimeout:      20 * time.Millisecond,
		LeaderLeaseTimeout: 100 * time.Millisecond,
	}
	startNode := func(name string) *Node {
		t.Helper()
		key := filepath.Join(tmpDir, name+".key")
		if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
			t.Fatalf("generate %s key: %v", name, err)
		}
		cfg := fastRaft
		cfg.DataDir = filepath.Join(tmpDir, name)
		cfg.KeyPath = key
		n, err := start(cfg)
		if err != nil {
			t.Fatalf("start %s: %v", name, err)
		}
		return n
	}

	leader := startNode("leader")
	defer leader.shutdown()
	if _, err := leader.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap leader: %v", err)
	}

	follower := startNode("follower")
	defer follower.shutdown()
	if _, err := follower.handleAdd(ctx, leader.advertisedAddrs()[0]); err != nil {
		t.Fatalf("join follower: %v", err)
	}

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	const unitA, unitB = "1BCT", "2BCT"

	// One record per kind under unitA, timestamps strictly increasing so
	// chronological order is unambiguous; every write goes through the
	// leader.
	for i, kind := range reportKindConfig {
		appendRecord(t, ctx, leader, kind, unitA, base.Add(time.Duration(i)*time.Minute), kind+" entry for "+unitA)
	}
	// A second unit, and a second record for the first kind, to prove
	// queries stay scoped to exactly the kind+unit asked for.
	appendRecord(t, ctx, leader, reportKindConfig[0], unitB, base, reportKindConfig[0]+" entry for "+unitB)
	appendRecord(t, ctx, leader, reportKindConfig[0], unitA, base.Add(time.Hour), reportKindConfig[0]+" second entry for "+unitA)

	// Replication: query from the follower, not the leader.
	deadline := time.After(10 * time.Second)
	for {
		got := queryRecords(t, ctx, follower, reportKindConfig[0], unitA, base.Add(-time.Hour), base.Add(2*time.Hour))
		if len(got) == 2 {
			if got[0].Narrative != reportKindConfig[0]+" entry for "+unitA {
				t.Fatalf("first record = %+v, want the base-timestamp entry first (chronological order)", got[0])
			}
			if got[1].Narrative != reportKindConfig[0]+" second entry for "+unitA {
				t.Fatalf("second record = %+v, want the +1h entry second", got[1])
			}
			break
		}
		select {
		case <-deadline:
			t.Fatalf("follower never saw both %s/%s records replicate, got %d", reportKindConfig[0], unitA, len(got))
		case <-time.After(50 * time.Millisecond):
		}
	}

	// Every other kind under unitA must have exactly its own one record,
	// scoped correctly -- proves kind isolation across the whole config
	// table, not just the one kind exercised above.
	for i, kind := range reportKindConfig[1:] {
		got := queryRecords(t, ctx, follower, kind, unitA, base, base.Add(time.Duration(i+2)*time.Minute))
		if len(got) != 1 {
			t.Fatalf("kind %q under %s: got %d records, want 1: %+v", kind, unitA, len(got), got)
		}
		if got[0].Kind != kind {
			t.Fatalf("kind %q under %s: record kind = %q", kind, unitA, got[0].Kind)
		}
	}

	// unitB must see only its own record for the shared kind, not
	// unitA's.
	got := queryRecords(t, ctx, follower, reportKindConfig[0], unitB, base.Add(-time.Hour), base.Add(2*time.Hour))
	if len(got) != 1 || got[0].UnitID != unitB {
		t.Fatalf("unitB query = %+v, want exactly one record scoped to unitB", got)
	}
}

// TestLogAppendRejectsWrongPrefix confirms EventLogAppend refuses a key
// that doesn't actually start with logrecord.LogKeyPrefix -- it's a
// dedicated way into that one reserved namespace, not a general Set
// bypass.
func TestLogAppendRejectsWrongPrefix(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: tmpDir, KeyPath: key})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()
	if _, err := n.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	payload, err := shmevent.EncodeSetPayload([]byte("ordinary-key"), []byte("value"))
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventLogAppend, Value: payload, ID: 1}, n.ed25519Priv)
	if resp.EventType != shmevent.EventError {
		t.Fatal("log_append with a non-LogKeyPrefix key unexpectedly succeeded")
	}
}

// TestOrdinarySetRejectsReservedPrefixes confirms EventSet/EventSetField
// refuse both shmevent.SystemKeyPrefix and logrecord.LogKeyPrefix --
// an ordinary caller can never collide with either reserved namespace,
// on purpose or by accident.
func TestOrdinarySetRejectsReservedPrefixes(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	tmpDir := t.TempDir()
	key := filepath.Join(tmpDir, "n.key")
	if _, err := p2praft.LoadOrGenerateKey(key); err != nil {
		t.Fatalf("generate key: %v", err)
	}
	n, err := start(Config{DataDir: tmpDir, KeyPath: key})
	if err != nil {
		t.Fatalf("start: %v", err)
	}
	defer n.shutdown()
	if _, err := n.handleAdd(ctx, ""); err != nil {
		t.Fatalf("bootstrap: %v", err)
	}

	for _, tc := range []struct {
		name string
		key  []byte
	}{
		{"system", []byte{shmevent.SystemKeyPrefix, 'x'}},
		{"logrecord", []byte{logrecord.LogKeyPrefix, 'x'}},
	} {
		payload, err := shmevent.EncodeSetPayload(tc.key, []byte("value"))
		if err != nil {
			t.Fatalf("[%s] EncodeSetPayload: %v", tc.name, err)
		}
		resp := callLocal(t, ctx, n, shmevent.Msg{EventType: shmevent.EventSet, Value: payload, ID: 1}, n.ed25519Priv)
		if resp.EventType != shmevent.EventError {
			t.Fatalf("[%s] ordinary Set with a reserved-prefix key unexpectedly succeeded", tc.name)
		}
		if !strings.Contains(string(resp.Value), "reserved") {
			t.Fatalf("[%s] rejected for the wrong reason: %s", tc.name, resp.Value)
		}
	}
}
