package daemon

import (
	"context"
	"path/filepath"
	"strings"
	"testing"
	"time"

	lp2phost "github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	p2praft "github.com/gofsd/libp2p-kv-raft/pkg/raft"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// remoteAppend builds a pkg/logrecord key/value for kind/unitID/ts and
// sends it as EventLogAppend from remote, exactly the way a genuine
// non-local caller would over ClientProtocolID.
func remoteAppend(t *testing.T, ctx context.Context, remote lp2phost.Host, remotePriv shmevent.PrivateKey, target peer.ID, kind, unitID string, ts time.Time) shmevent.Msg {
	t.Helper()
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey(kind, unitID, ts, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: kind, UnitID: unitID, Timestamp: ts, Narrative: "x"}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	resp, err := callClientProtocol(ctx, remote, target, shmevent.Msg{
		EventType: shmevent.EventLogAppend,
		Value:     payload,
		ID:        1,
	}, remotePriv)
	if err != nil {
		t.Fatalf("log_append: %v", err)
	}
	return resp
}

// remoteQuery drives EventListRange once (not in a loop -- these tests
// only need to know whether the first match is granted or rejected) from
// remote against target for [start, end].
func remoteQuery(t *testing.T, ctx context.Context, remote lp2phost.Host, remotePriv shmevent.PrivateKey, target peer.ID, start, end []byte) shmevent.Msg {
	t.Helper()
	query, err := shmevent.EncodeListRangeQuery(start, end)
	if err != nil {
		t.Fatalf("EncodeListRangeQuery: %v", err)
	}
	resp, err := callClientProtocol(ctx, remote, target, shmevent.Msg{
		EventType: shmevent.EventListRange,
		Value:     query,
		ID:        2,
	}, remotePriv)
	if err != nil {
		t.Fatalf("list_range: %v", err)
	}
	return resp
}

// TestRequirePermitForLogGateRejectsUnpermitted proves Config.RequirePermitForLog
// rejects a remote caller with no log-kind grant at all, on both
// EventLogAppend and EventListRange.
func TestRequirePermitForLogGateRejectsUnpermitted(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{RequirePermitForLog: true})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	resp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, "sitrep", "1BCT", base)
	if resp.EventType != shmevent.EventError {
		t.Fatal("log_append from an unpermitted remote peer unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not permitted") {
		t.Fatalf("log_append rejection = %q, want it to mention not being permitted", resp.Value)
	}

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	qresp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi)
	if qresp.EventType != shmevent.EventError {
		t.Fatal("list_range from an unpermitted remote peer unexpectedly succeeded")
	}
	if !strings.Contains(string(qresp.Value), "not permitted") {
		t.Fatalf("list_range rejection = %q, want it to mention not being permitted", qresp.Value)
	}
}

// TestLogPermitKindIsolation proves a confirmed grant for one kind does
// NOT extend to another, and that EventListRange's per-range check
// covers both the start and end bounds -- a query whose start lands in a
// permitted kind's range but whose end reaches into a different kind's
// range must be rejected outright, not silently answered.
func TestLogPermitKindIsolation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{RequirePermitForLog: true})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)
	remoteIDStr := remote.ID().String()

	// leader is the sole raft voter, so it can confirm its own pending
	// record locally, same pattern TestRequirePermitForRemoteGate uses.
	reqPayload, err := shmevent.EncodeLogPermitRequestPayload("sitrep", []byte(remoteIDStr), nil)
	if err != nil {
		t.Fatalf("EncodeLogPermitRequestPayload: %v", err)
	}
	reqResp, err := callClientProtocol(ctx, remote, leaderPeerID, shmevent.Msg{
		EventType: shmevent.EventLogPermitRequest,
		Value:     reqPayload,
		ID:        1,
	}, remotePriv)
	if err != nil {
		t.Fatalf("log_permit_request: %v", err)
	}
	if reqResp.EventType == shmevent.EventError {
		t.Fatalf("log_permit_request rejected: %s", reqResp.Value)
	}

	confirmPayload, err := shmevent.EncodeLogPermitConfirmPayload("sitrep", []byte(remoteIDStr))
	if err != nil {
		t.Fatalf("EncodeLogPermitConfirmPayload: %v", err)
	}
	confirmBuf, err := shmevent.Encode(shmevent.Msg{EventType: shmevent.EventLogPermitConfirm, Value: confirmPayload, ID: 2}, leader.ed25519Priv)
	if err != nil {
		t.Fatalf("encode confirm: %v", err)
	}
	decoded, crc, sig, err := shmevent.Decode(confirmBuf)
	if err != nil {
		t.Fatalf("decode confirm: %v", err)
	}
	confirmResp := leader.handleShmEvent(ctx, decoded, crc, sig, leader.localCaller())
	if confirmResp.EventType == shmevent.EventError {
		t.Fatalf("log_permit_confirm rejected: %s", confirmResp.Value)
	}

	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)

	// Permitted kind must succeed.
	if resp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, "sitrep", "1BCT", base); resp.EventType == shmevent.EventError {
		t.Fatalf("log_append for the permitted kind rejected: %s", resp.Value)
	}

	// A different, ungranted kind must still be rejected.
	if resp := remoteAppend(t, ctx, remote, remotePriv, leaderPeerID, "casrep", "1BCT", base); resp.EventType != shmevent.EventError {
		t.Fatal("log_append for an ungranted kind unexpectedly succeeded")
	}

	// Query for the permitted kind must succeed.
	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi); resp.EventType == shmevent.EventError {
		t.Fatalf("list_range for the permitted kind rejected: %s", resp.Value)
	}

	// Query entirely inside the ungranted kind must be rejected.
	lo2, hi2 := logrecord.ScanBounds("casrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	if resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo2, hi2); resp.EventType != shmevent.EventError {
		t.Fatal("list_range for an ungranted kind unexpectedly succeeded")
	}

	// The bypass this feature specifically closes: start lands in the
	// permitted "sitrep" range, but end is crafted to reach into
	// "casrep"'s range (a raw byte range has no logical confinement to
	// "sitrep" just because start does).
	bypassResp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, lo, hi2)
	if bypassResp.EventType != shmevent.EventError {
		t.Fatal("list_range with start in a permitted kind but end reaching into an ungranted kind unexpectedly succeeded")
	}
}

// TestRequirePermitForLogGateCoversRangeOutsideNamespace proves the
// RequirePermitForLog gate on EventListRange can't be evaded by choosing
// a start that doesn't literally begin with logrecord.LogKeyPrefix: a
// range scan whose start comes from entirely outside the logrecord
// namespace (here, the empty string) but whose end reaches into it must
// still be rejected for an unpermitted remote caller, exactly like a
// query that starts squarely inside the namespace already is (see
// TestRequirePermitForLogGateRejectsUnpermitted) -- pkg/store.ScanRange
// has no concept of namespaces, so without overlapsLogNamespace's check
// such a query would otherwise return logrecord data the caller was
// never granted.
func TestRequirePermitForLogGateCoversRangeOutsideNamespace(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{RequirePermitForLog: true})
	remote, remotePriv, leaderPeerID := newTestRemoteHost(t, ctx, leader)

	// Plant a real record under a kind the remote caller has no grant for
	// at all, appended locally -- RequirePermitForLog never gates a local
	// caller (see TestRequirePermitForLogLocalCallerNeverGated), so this
	// is just test setup, not itself part of what's being proven.
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	key, err := logrecord.BuildKey("secret", "1BCT", base, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: "secret", UnitID: "1BCT", Timestamp: base, Narrative: "classified"}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}
	appendResp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventLogAppend, Value: payload, ID: 1}, leader.ed25519Priv)
	if appendResp.EventType == shmevent.EventError {
		t.Fatalf("local plant append rejected: %s", appendResp.Value)
	}

	// A query starting well before the logrecord namespace (the empty
	// string, the smallest possible key) with an end reaching just past
	// the namespace's own end must still be rejected, even though
	// start[0] doesn't equal logrecord.LogKeyPrefix -- the exact shape the
	// old start[0]-only check let through unchecked.
	resp := remoteQuery(t, ctx, remote, remotePriv, leaderPeerID, []byte{}, []byte{logrecord.LogKeyPrefix + 1})
	if resp.EventType != shmevent.EventError {
		t.Fatalf("list_range with start outside the logrecord namespace but end reaching into it unexpectedly succeeded: %+v", resp)
	}
	if !strings.Contains(string(resp.Value), "must be scoped") {
		t.Fatalf("rejection = %q, want it to mention the single-kind scope requirement", resp.Value)
	}
}

// TestLogPermitConfirmRevokeVoterOnly mirrors
// TestPermitRequestConfirmWorkflow/TestPermitRevokeWorkflow's real
// leader/voter/learner topology (permit_test.go), substituting
// EventLogPermitConfirm/EventLogPermitRevoke: only a genuine raft voter
// may confirm or revoke a log-kind permit, and revoke actually deletes
// the confirmed record.
func TestLogPermitConfirmRevokeVoterOnly(t *testing.T) {
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
	leaderAddr := leader.advertisedAddrs()[0]

	voter := startNode("voter")
	defer voter.shutdown()
	if _, err := voter.handleAdd(ctx, leaderAddr); err != nil {
		t.Fatalf("join voter: %v", err)
	}

	learner := startNode("learner")
	defer learner.shutdown()
	if _, err := learner.initRaft(); err != nil {
		t.Fatalf("init learner raft: %v", err)
	}
	if _, err := leader.handleAddLearner(ctx, learner.peerID, learner.advertisedAddrs()[0]); err != nil {
		t.Fatalf("join learner: %v", err)
	}

	call := func(n *Node, m shmevent.Msg) shmevent.Msg {
		t.Helper()
		buf, err := shmevent.Encode(m, n.ed25519Priv)
		if err != nil {
			t.Fatalf("encode: %v", err)
		}
		decoded, crc, sig, err := shmevent.Decode(buf)
		if err != nil {
			t.Fatalf("decode: %v", err)
		}
		return n.handleShmEvent(ctx, decoded, crc, sig, n.localCaller())
	}

	targetPeerID := []byte("some-log-permitted-peer-id")

	reqPayload, err := shmevent.EncodeLogPermitRequestPayload("sitrep", targetPeerID, nil)
	if err != nil {
		t.Fatalf("EncodeLogPermitRequestPayload: %v", err)
	}
	resp := call(leader, shmevent.Msg{EventType: shmevent.EventLogPermitRequest, Value: reqPayload, ID: 1})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("log_permit_request rejected: %s", resp.Value)
	}

	confirmPayload, err := shmevent.EncodeLogPermitConfirmPayload("sitrep", targetPeerID)
	if err != nil {
		t.Fatalf("EncodeLogPermitConfirmPayload: %v", err)
	}

	// A learner (nonvoter) confirming must be rejected.
	resp = call(learner, shmevent.Msg{EventType: shmevent.EventLogPermitConfirm, Value: confirmPayload, ID: 2})
	if resp.EventType != shmevent.EventError {
		t.Fatal("learner log_permit_confirm unexpectedly succeeded")
	}
	if !strings.Contains(string(resp.Value), "not a current raft voter") {
		t.Fatalf("learner log_permit_confirm rejected for the wrong reason: %s", resp.Value)
	}

	// A real voter confirming must succeed.
	resp = call(voter, shmevent.Msg{EventType: shmevent.EventLogPermitConfirm, Value: confirmPayload, ID: 3})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter log_permit_confirm rejected: %s", resp.Value)
	}

	confirmedKey, err := shmevent.LogPermitKey(shmevent.StatusConfirmed, "sitrep", targetPeerID)
	if err != nil {
		t.Fatalf("LogPermitKey: %v", err)
	}
	getResp := call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: confirmedKey, ID: 4})
	if getResp.EventType == shmevent.EventError {
		t.Fatalf("confirmed log-permit record missing after voter confirm: %s", getResp.Value)
	}

	// A learner revoking must likewise be rejected.
	resp = call(learner, shmevent.Msg{EventType: shmevent.EventLogPermitRevoke, Value: confirmPayload, ID: 5})
	if resp.EventType != shmevent.EventError {
		t.Fatal("learner log_permit_revoke unexpectedly succeeded")
	}
	getResp = call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: confirmedKey, ID: 6})
	if getResp.EventType == shmevent.EventError {
		t.Fatalf("confirmed record missing after rejected learner revoke: %s", getResp.Value)
	}

	// A real voter revoking must succeed and delete the record.
	resp = call(voter, shmevent.Msg{EventType: shmevent.EventLogPermitRevoke, Value: confirmPayload, ID: 7})
	if resp.EventType == shmevent.EventError {
		t.Fatalf("voter log_permit_revoke rejected: %s", resp.Value)
	}
	getResp = call(leader, shmevent.Msg{EventType: shmevent.EventGetField, Value: confirmedKey, ID: 8})
	if getResp.EventType != shmevent.EventError {
		t.Fatal("confirmed log-permit record still present after successful revoke -- should have been deleted")
	}
}

// TestRequirePermitForLogLocalCallerNeverGated proves a local caller (no
// caller.remotePeer at all) is never restricted by
// Config.RequirePermitForLog, regardless of raft membership or any
// granted permit -- mirrors the same guarantee
// TestRequirePermitForRemoteGate documents for the general remote gate.
func TestRequirePermitForLogLocalCallerNeverGated(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	leader := startTestLeader(t, ctx, Config{RequirePermitForLog: true})

	rnd, err := logrecord.NewRand()
	if err != nil {
		t.Fatalf("NewRand: %v", err)
	}
	base := time.Date(2026, 7, 19, 0, 0, 0, 0, time.UTC)
	key, err := logrecord.BuildKey("sitrep", "1BCT", base, rnd)
	if err != nil {
		t.Fatalf("BuildKey: %v", err)
	}
	rec := logrecord.Record{Kind: "sitrep", UnitID: "1BCT", Timestamp: base, Narrative: "x"}
	value, err := rec.Encode()
	if err != nil {
		t.Fatalf("Record.Encode: %v", err)
	}
	payload, err := shmevent.EncodeSetPayload(key, value)
	if err != nil {
		t.Fatalf("EncodeSetPayload: %v", err)
	}

	resp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventLogAppend, Value: payload, ID: 1}, leader.ed25519Priv)
	if resp.EventType == shmevent.EventError {
		t.Fatalf("local log_append rejected despite RequirePermitForLog and no grant: %s", resp.Value)
	}

	lo, hi := logrecord.ScanBounds("sitrep", "1BCT", base.Add(-time.Hour), base.Add(time.Hour))
	query, err := shmevent.EncodeListRangeQuery(lo, hi)
	if err != nil {
		t.Fatalf("EncodeListRangeQuery: %v", err)
	}
	qresp := callLocal(t, ctx, leader, shmevent.Msg{EventType: shmevent.EventListRange, Value: query, ID: 2}, leader.ed25519Priv)
	if qresp.EventType == shmevent.EventError {
		t.Fatalf("local list_range rejected despite RequirePermitForLog and no grant: %s", qresp.Value)
	}
	if len(qresp.Value) == 0 {
		t.Fatal("local list_range found nothing -- append above must not have landed")
	}
}
