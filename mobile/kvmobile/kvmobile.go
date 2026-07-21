// Package kvmobile is the gomobile-bindable entry point for running this
// project's kvnode follower daemon inside an Android app process, and
// driving it from the UI over pkg/ipc's Android transport (see
// pkg/ipc/ipc_android.go).
//
// Unlike the desktop mage CLI, a mobile app has no operator to type
// `mage addnode <leaderPeerID>`: the leader to join has to be known before
// the app is ever run. leaderMultiaddr is therefore a build-time constant,
// set via `gomobile bind -ldflags "-X .../mobile/kvmobile.leaderMultiaddr=<multiaddr>"`
// rather than a runtime parameter -- by the time Start returns, this
// device is already a member of the raft cluster, with no further setup
// step for the UI to drive.
package kvmobile

import (
	"context"
	"crypto/rand"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/logrecord"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// leaderMultiaddr is baked in at build time -- see package doc comment.
// It's a full multiaddr (not a bare peer id) because there is no shared
// registry between this device and wherever the leader runs, exactly the
// same "leader on another machine" case pkg/registry.IsMultiaddr exists
// for on desktop.
var leaderMultiaddr string

// identitySeedHex, when set, is baked in at build time the same way as
// leaderMultiaddr (via `gomobile bind -ldflags
// "-X .../mobile/kvmobile.identitySeedHex=<128 hex chars>"`) to make this
// device's identity deterministic instead of freshly random on first run --
// what the e2e test pipeline needs so a build against a recorded
// pkg/e2edata.Node reliably comes up as that exact peer id. The expected
// format is 128 hex chars decoding to the 64 raw stdlib crypto/ed25519
// private key bytes (32-byte seed + 32-byte public key) -- exactly
// pkg/e2edata.Node.PrivateKey's own format, so a recorded node's key can be
// pasted straight into the ldflag with no conversion. Left empty (the
// default), ensureIdentity's existing random-on-first-run behavior is
// unchanged.
var identitySeedHex string

// relayMultiaddr is baked in at build time the same way as leaderMultiaddr
// (via `gomobile bind -ldflags "-X .../mobile/kvmobile.relayMultiaddr=..."`),
// and is normally just the leader's own multiaddr, since the leader is
// what's deployed with -relay-service. It's threaded into
// daemon.Config.RelayPeer so this device -- typically a phone on a
// cellular connection behind carrier-grade NAT -- proactively reserves a
// relay slot instead of ending up stuck advertising only addresses the
// leader can never dial back; see Config.RelayPeer's doc comment in
// pkg/daemon. Leave unset at build time for a device with its own
// directly-dialable address (e.g. same LAN as the leader).
var relayMultiaddr string

// callTimeout bounds a single Start/Submit/Get round trip. Comfortably
// exceeds 5*raftElectionTimeout, the longest wait handleSetForward can do
// internally while resolving the current leader (see pkg/daemon).
const callTimeout = 25 * time.Second

// Raft timing knobs for this node's own participation in the cluster.
// Library defaults (1s heartbeat/election, 500ms leader lease) are tuned
// for a LAN and are not safe here: a phone on cellular data, reached only
// through a circuit relay (see newHost's doc comment in pkg/daemon), sees
// meaningfully higher and jitterier round trips than even the WAN link
// this project's VPS deployment already needed to tune for. Without this,
// the phone's own raft.Raft loses track of the leader between heartbeats
// and every Submit fails with "not leader and no leader known" -- observed
// directly when this app was first brought up against a real leader.
const (
	raftHeartbeatTimeout   = 4 * time.Second
	raftElectionTimeout    = 4 * time.Second
	raftCommitTimeout      = 200 * time.Millisecond
	raftLeaderLeaseTimeout = 2500 * time.Millisecond
)

var (
	mu sync.Mutex
	// started, peerID, curDataDir, curDataDirRoot, session, runErrC,
	// cancelRun track the single in-process daemon this package runs at a
	// time. curDataDir is the cluster-paired subdirectory of
	// curDataDirRoot (the dataDir a caller passed to Start/StartWithKey)
	// that actually holds the sqlite/raft state -- identity.key lives at
	// curDataDirRoot itself (see ensureIdentity/importIdentity) -- see
	// registry.ClusterDirName. Delete compares against curDataDirRoot
	// since that's the directory callers pass in, not the computed
	// subdirectory.
	started        bool
	peerID         string
	curDataDir     string
	curDataDirRoot string
	runErrC        chan error
	session        *shmclient.Session
	cancelRun      context.CancelFunc
)

// Start brings up the follower daemon in-process under dataDir (an
// Android app-private directory the Kotlin side already has, e.g.
// Context.getFilesDir()) and joins it to the build-time-configured leader.
// It's safe to call more than once (e.g. from Application.onCreate on
// every launch); after the first successful call it just returns the
// already-running node's peer id.
//
// The identity persisted under dataDir is reused across calls/app
// restarts, and joining is re-sent every time regardless of whether this
// is a fresh identity or a resumed one: hashicorp/raft's AddVoter is a
// no-op-ish update (not an error) for a peer id already in the
// configuration, so this uniformly handles "first run" and "resumed after
// the app was killed and this device's address changed" without needing
// to tell the two cases apart -- see pkg/kvctl's RejoinNode for the same
// reasoning on desktop.
func Start(dataDir string) (string, error) {
	return start(dataDir, ensureIdentity)
}

// StartWithKey is like Start but provisions dataDir's identity from keyHex
// (hex-encoded, identity.key's own on-disk format -- e.g. a key file
// exported from another device, or the string pkg/kvctl.AddNodeWithKey/
// `mage addnodewithkey` reads from a file on desktop) instead of always
// falling back to ensureIdentity's persisted-or-generated-or-build-seeded
// key. This is the runtime equivalent of desktop's AddNodeWithKey/
// `mage addnodewithkey`, letting the caller pick which identity a data
// directory comes up as instead of only ever a fresh or build-time one.
//
// If dataDir already holds a *different* identity, it refuses rather than
// silently abandoning that identity's already-replicated raft state --
// call Delete(dataDir) first if that's really what's wanted.
func StartWithKey(dataDir, keyHex string) (string, error) {
	return start(dataDir, func(dataDir string) (keyPath, peerID string, err error) {
		return importIdentity(dataDir, keyHex)
	})
}

// start brings up the daemon under dataDirRoot and joins it to the
// build-time-configured leader.
func start(dataDirRoot string, resolveIdentity func(dataDir string) (keyPath, peerID string, err error)) (string, error) {
	mu.Lock()
	defer mu.Unlock()
	if started {
		return peerID, nil
	}
	if leaderMultiaddr == "" {
		return "", fmt.Errorf("kvmobile: no leader multiaddr baked in at build time")
	}

	keyPath, id, err := resolveIdentity(dataDirRoot)
	if err != nil {
		return "", err
	}

	// clusterDir is a subdirectory of dataDirRoot dedicated to whichever
	// cluster this identity is joining -- identity.key stays at
	// dataDirRoot itself (resolveIdentity above), unaffected by which
	// cluster is active. This mirrors desktop's
	// registry.NodeDataDir/ClusterDataDir split exactly (see that
	// package's doc comments) so the same identity can hold separate,
	// non-colliding state per cluster it has ever joined.
	remotePID, err := registry.ExtractPeerID(leaderMultiaddr)
	if err != nil {
		return "", fmt.Errorf("kvmobile: resolve leader peer id: %w", err)
	}
	clusterDir := filepath.Join(dataDirRoot, registry.ClusterDirName(id, remotePID))

	// A stale ready file left behind by an earlier Start/Stop cycle against
	// this same directory would let waitForReady below return early --
	// before this run has written its own -- since pkg/daemon.Run's
	// writeReadyFile only overwrites it once the new daemon is actually
	// up, not on the way in. Harmless on a directory Start has never
	// touched before; only relevant once Stop() makes restarting the same
	// directory in one process possible.
	_ = os.Remove(filepath.Join(clusterDir, daemon.ReadyFileName))

	ctx, cancel := context.WithCancel(context.Background())
	errC := make(chan error, 1)
	go func() {
		errC <- daemon.Run(ctx, daemon.Config{
			DataDir:            clusterDir,
			KeyPath:            keyPath,
			RelayPeer:          relayMultiaddr,
			HeartbeatTimeout:   raftHeartbeatTimeout,
			ElectionTimeout:    raftElectionTimeout,
			CommitTimeout:      raftCommitTimeout,
			LeaderLeaseTimeout: raftLeaderLeaseTimeout,
		})
	}()

	if err := waitForReady(clusterDir, errC, callTimeout); err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: start follower: %w", err)
	}

	addCtx, addCancel := context.WithTimeout(ctx, callTimeout)
	defer addCancel()
	sess, err := shmclient.Open(addCtx, id)
	if err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: fetch signing key: %w", err)
	}
	if _, err := sess.Add(addCtx, leaderMultiaddr); err != nil {
		cancel()
		return "", fmt.Errorf("kvmobile: join cluster: %w", err)
	}

	session = sess
	peerID = id
	curDataDir = clusterDir
	curDataDirRoot = dataDirRoot
	runErrC = errC
	cancelRun = cancel
	started = true
	return peerID, nil
}

// Stop shuts down the currently running in-process daemon, if any, and
// waits for it to actually release its resources (sqlite/raft files,
// libp2p host) before returning. kvmobile runs exactly one daemon per
// process at a time, so this is the enabling step for the same thing
// desktop's `mage use <peerID>` picks between multiple already-running
// nodes: to switch to a different identity here, Stop the current one,
// then Start/StartWithKey against the dataDir for the identity to switch
// to. Safe to call when nothing is running (a no-op).
func Stop() error {
	mu.Lock()
	if !started {
		mu.Unlock()
		return nil
	}
	cancel := cancelRun
	errC := runErrC
	mu.Unlock()

	cancel()
	select {
	case <-errC:
	case <-time.After(callTimeout):
		return fmt.Errorf("kvmobile: daemon did not stop within %s", callTimeout)
	}

	mu.Lock()
	started = false
	peerID = ""
	curDataDir = ""
	curDataDirRoot = ""
	session = nil
	runErrC = nil
	cancelRun = nil
	mu.Unlock()
	return nil
}

// Delete permanently removes dataDir's persisted node state (identity key,
// sqlite store, raft log/snapshots -- the whole directory), mirroring
// desktop's `mage deletenode`/pkg/kvctl.DeleteNode. It refuses while a
// daemon is currently running against that same dataDir -- call Stop
// first -- since removing files out from under a live process would
// corrupt them.
func Delete(dataDir string) error {
	mu.Lock()
	running := started && curDataDirRoot == dataDir
	mu.Unlock()
	if running {
		return fmt.Errorf("kvmobile: node at %s is currently running; call Stop first", dataDir)
	}
	if err := os.RemoveAll(dataDir); err != nil {
		return fmt.Errorf("kvmobile: delete %s: %w", dataDir, err)
	}
	return nil
}

// Leave asks the raft cluster this device is currently joined to, to
// remove it (raft.RemoveServer -- see shmevent.EventLeave's doc comment: a
// graceful shrink, the remaining voters keep operating normally), then
// stops the daemon. Unlike desktop's pkg/kvctl.Leave, there is no solo/
// default cluster to fall back to here -- kvmobile's daemon always joins
// leaderMultiaddr, baked in at build time (see this package's doc
// comment) -- so a later Start/StartWithKey would simply attempt to
// rejoin the very same cluster. Call Rm instead if that later rejoin
// attempt should require fresh confirmation rather than being silently
// re-admitted.
func Leave() error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.Leave(ctx); err != nil {
		return fmt.Errorf("kvmobile: leave: %w", err)
	}
	return Stop()
}

// Rm does everything Leave does, plus revokes this device's peer id's
// cluster-join standing (shmevent.KindClusterJoin) so a later
// Start/StartWithKey attempt against the same cluster starts genuinely
// pending again -- not silently re-admitted by a stale confirmed record
// -- and deletes the joined cluster's local data subdirectory. Unlike
// Delete, it never touches dataDirRoot itself (where identity.key lives):
// mirrors desktop's pkg/kvctl.Rm, which likewise only ever wipes the
// composite cluster dir, never the solo identity dir.
func Rm() error {
	mu.Lock()
	ownPeerID := peerID
	clusterDir := curDataDir
	mu.Unlock()
	if ownPeerID == "" {
		return fmt.Errorf("kvmobile: not currently running")
	}

	sess, err := currentSession()
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokePermit(ctx, shmevent.KindClusterJoin, []byte(ownPeerID)); err != nil {
		return fmt.Errorf("kvmobile: rm: revoke cluster-join standing: %w", err)
	}
	if err := sess.Leave(ctx); err != nil {
		return fmt.Errorf("kvmobile: rm: %w", err)
	}
	if err := Stop(); err != nil {
		return err
	}
	if err := os.RemoveAll(clusterDir); err != nil {
		return fmt.Errorf("kvmobile: rm: remove cluster data dir %s: %w", clusterDir, err)
	}
	return nil
}

// currentSession returns the *shmclient.Session for the currently running
// daemon, or an error if Start/StartWithKey hasn't completed successfully
// yet -- the shared guard every call that needs a live daemon (Submit,
// Get, the permit/execute bindings below) starts with.
func currentSession() (*shmclient.Session, error) {
	mu.Lock()
	sess := session
	ok := started
	mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("kvmobile: Start has not completed successfully yet")
	}
	return sess, nil
}

// Submit sets key=value through raft, forwarding to the current leader if
// this device isn't it (see pkg/daemon's ForwardProtocolID) -- which, as a
// follower, it never is, so every Submit takes that path.
func Submit(key, value string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.Set(ctx, key, value); err != nil {
		return fmt.Errorf("kvmobile: set: %w", err)
	}
	return nil
}

// Get reads key from this device's own locally replicated state (which,
// like any raft follower's local read, may lag a moment behind a Submit
// that just committed on the leader).
func Get(key string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	value, err := sess.Get(ctx, key)
	if err != nil {
		return "", fmt.Errorf("kvmobile: get: %w", err)
	}
	return value, nil
}

// PeerID returns this device's peer id, or "" if Start hasn't completed
// successfully yet.
func PeerID() string {
	mu.Lock()
	defer mu.Unlock()
	return peerID
}

// permitKindFromName converts kind ("peer" or "bootstrap") to
// shmevent.KindPermitPeer/KindBootstrapNode -- the same mapping desktop's
// `mage requestpermit`/`confirmpermit`/`revokepermit` apply via
// shmevent.KindFromName before reaching pkg/kvctl.
func permitKindFromName(kind string) (byte, error) {
	k, ok := shmevent.KindFromName(kind)
	if !ok {
		return 0, fmt.Errorf("kvmobile: unknown permit kind %q (want \"peer\" or \"bootstrap\")", kind)
	}
	return k, nil
}

// RequestPermit lodges a pending permit record for targetPeerID (kind is
// "peer" or "bootstrap") on this device, forwarded to the leader like any
// other Set -- see shmevent.EventPermitRequest's doc comment. Any raft
// node may originate one, so this needs no special standing of its own.
// metadata may be "" (only meaningful for kind "bootstrap": the dialable
// multiaddr being vouched for).
func RequestPermit(kind, targetPeerID, metadata string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	k, err := permitKindFromName(kind)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RequestPermit(ctx, k, []byte(targetPeerID), []byte(metadata)); err != nil {
		return fmt.Errorf("kvmobile: request permit: %w", err)
	}
	return nil
}

// ConfirmPermit promotes a pending permit record for targetPeerID to
// confirmed. Only takes effect if this device is itself a raft voter --
// see shmevent.EventPermitConfirm's doc comment.
func ConfirmPermit(kind, targetPeerID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	k, err := permitKindFromName(kind)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.ConfirmPermit(ctx, k, []byte(targetPeerID)); err != nil {
		return fmt.Errorf("kvmobile: confirm permit: %w", err)
	}
	return nil
}

// RevokePermit deletes a confirmed permit record for targetPeerID
// outright. Only takes effect if this device is itself a raft voter --
// see shmevent.EventPermitRevoke's doc comment.
func RevokePermit(kind, targetPeerID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}
	k, err := permitKindFromName(kind)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokePermit(ctx, k, []byte(targetPeerID)); err != nil {
		return fmt.Errorf("kvmobile: revoke permit: %w", err)
	}
	return nil
}

// RequestLogPermit lodges a pending permission for targetPeerID to
// append/query pkg/logrecord records of logKind, forwarded to the leader
// like any other Set -- see shmevent.EventLogPermitRequest's doc comment.
// metadata may be "".
func RequestLogPermit(logKind, targetPeerID, metadata string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RequestLogPermit(ctx, logKind, []byte(targetPeerID), []byte(metadata)); err != nil {
		return fmt.Errorf("kvmobile: request log permit: %w", err)
	}
	return nil
}

// ConfirmLogPermit promotes a pending log-kind permit record for
// targetPeerID to confirmed. Only takes effect if this device is itself a
// raft voter -- see shmevent.EventLogPermitConfirm's doc comment.
func ConfirmLogPermit(logKind, targetPeerID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.ConfirmLogPermit(ctx, logKind, []byte(targetPeerID)); err != nil {
		return fmt.Errorf("kvmobile: confirm log permit: %w", err)
	}
	return nil
}

// RevokeLogPermit deletes a confirmed log-kind permit record for
// targetPeerID outright. Only takes effect if this device is itself a raft
// voter -- see shmevent.EventLogPermitRevoke's doc comment.
func RevokeLogPermit(logKind, targetPeerID string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.RevokeLogPermit(ctx, logKind, []byte(targetPeerID)); err != nil {
		return fmt.Errorf("kvmobile: revoke log permit: %w", err)
	}
	return nil
}

// LogAppend writes a new pkg/logrecord.Record of the given kind/unitID,
// timestamped now and attributed to this device's own peer id -- the
// mobile equivalent of `mage logappend <kind> <unitID> <fieldsJSON>
// <narrative>`/pkg/kvctl.LogAppend. kind and unitID are entirely
// caller-chosen strings, not a fixed set -- see pkg/logrecord's doc
// comment for the generic key/record scheme this builds on. fieldsJSON is
// a JSON object of string fields, or "" for none.
func LogAppend(kind, unitID, fieldsJSON, narrative string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	var fields map[string]string
	if fieldsJSON != "" {
		if err := json.Unmarshal([]byte(fieldsJSON), &fields); err != nil {
			return fmt.Errorf("kvmobile: decode fieldsJSON: %w", err)
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := appendRecord(ctx, sess, kind, unitID, fields, narrative); err != nil {
		return fmt.Errorf("kvmobile: log append: %w", err)
	}
	return nil
}

// LogQuery lists every pkg/logrecord.Record of the given kind/unitID whose
// timestamp falls in [since, until], oldest first, up to limit records --
// the mobile equivalent of `mage logquery <kind> <unitID> <since|"">
// <until|""> <limit|"">`/pkg/kvctl.LogQuery. since/until are RFC3339 or ""
// (since "" = unbounded, until "" = now); limit is a count or "" (no
// limit). The result is a JSON array of records, e.g. `[{"kind":"...",
// "unit_id":"...","timestamp":"...","author_peer_id":"...","narrative":
// "..."}]` -- gomobile bindings only support one non-error return value,
// so this returns the whole array as one string rather than
// pkg/kvctl.LogQuery's []logrecord.Record.
func LogQuery(kind, unitID, since, until, limit string) (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	start := time.Unix(0, 0)
	if since != "" {
		t, err := time.Parse(time.RFC3339, since)
		if err != nil {
			return "", fmt.Errorf("kvmobile: since: %w", err)
		}
		start = t
	}
	end := time.Now()
	if until != "" {
		t, err := time.Parse(time.RFC3339, until)
		if err != nil {
			return "", fmt.Errorf("kvmobile: until: %w", err)
		}
		end = t
	}
	n := 0
	if limit != "" {
		v, err := strconv.Atoi(limit)
		if err != nil {
			return "", fmt.Errorf("kvmobile: limit: %w", err)
		}
		n = v
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	lo, hi := logrecord.ScanBounds(kind, unitID, start, end)
	records := []logrecord.Record{}
	for n <= 0 || len(records) < n {
		key, value, ok, err := sess.ListRange(ctx, lo, hi)
		if err != nil {
			return "", fmt.Errorf("kvmobile: log query: %w", err)
		}
		if !ok {
			break
		}
		rec, err := logrecord.Decode(value)
		if err != nil {
			return "", fmt.Errorf("kvmobile: log query: decode record: %w", err)
		}
		records = append(records, rec)
		lo = append(append([]byte{}, key...), 0x00)
	}

	out, err := json.Marshal(records)
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode log query result: %w", err)
	}
	return string(out), nil
}

// Execute sends value as a direct peer-to-peer EventExecute notification
// from this device to destPeerID, bypassing raft and the store entirely --
// see shmevent.EventExecute's doc comment.
func Execute(destPeerID, value string) error {
	sess, err := currentSession()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	if err := sess.Execute(ctx, destPeerID, []byte(value)); err != nil {
		return fmt.Errorf("kvmobile: execute: %w", err)
	}
	return nil
}

// pollExecuteResult is PollExecute's JSON return shape -- gomobile
// bindings only support one non-error return value, so this mirrors
// SendEvent's own JSON-envelope convention rather than
// pkg/kvctl.PollExecute's 4-value Go signature.
type pollExecuteResult struct {
	Pending      bool   `json:"pending"`
	SenderPeerID string `json:"sender_peer_id,omitempty"`
	Value        string `json:"value,omitempty"`
}

// PollExecute drains one queued EventExecute notification delivered to
// this device, if any -- see shmevent.EventPollExecute's doc comment. The
// result is JSON encoded, e.g. `{"pending":true,"sender_peer_id":"...",
// "value":"..."}`, or `{"pending":false}` if nothing was queued.
func PollExecute() (string, error) {
	sess, err := currentSession()
	if err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()
	sender, payload, ok, err := sess.PollExecute(ctx)
	if err != nil {
		return "", fmt.Errorf("kvmobile: poll execute: %w", err)
	}

	out, err := json.Marshal(pollExecuteResult{
		Pending:      ok,
		SenderPeerID: sender,
		Value:        string(payload),
	})
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode poll execute result: %w", err)
	}
	return string(out), nil
}

// ExecuteCallback is a gomobile-bindable interface Kotlin implements to
// receive WatchExecute's push notifications -- this package's first use of
// gomobile's reverse-binding pattern (a Go interface Kotlin implements,
// with Go calling into it), unlike every other kvmobile function's plain
// string-in/string-out shape.
type ExecuteCallback interface {
	// OnNotification is called once per EventExecute notification drained
	// from this device's own queue, in delivery order, on a goroutine
	// WatchExecute owns -- never the caller's own thread. gomobile's
	// generated Kotlin binding already hops this call onto a JNI-managed
	// thread, but the implementation is still responsible for its own
	// thread-safety (e.g. runOnUiThread before touching views).
	OnNotification(senderPeerID, value string)
}

// watchExecutePollInterval bounds how long runExecuteWatch sleeps after
// finding nothing queued (or no daemon currently running) before checking
// again. PollExecute is a local, in-memory, non-blocking check (see
// executeInbox's doc comment in pkg/daemon), not a store read, so this can
// be short without meaningful cost -- unlike a LogQuery-based poll loop,
// which would mean a real replicated-store read every tick.
const watchExecutePollInterval = 200 * time.Millisecond

// watchExecutePollCallTimeout bounds each individual PollExecute call
// runExecuteWatch makes -- deliberately much shorter than callTimeout
// (which exists for genuinely slow cross-cluster operations like a
// forwarded Set/Get awaiting a leader election). PollExecute never leaves
// this device, so under normal operation it returns almost immediately;
// the only time this bound actually matters is a call that's in flight
// exactly when Stop() tears down the daemon out from under it, which
// would otherwise leave the watch loop stuck for the rest of a full
// callTimeout before it notices the session is gone and can resume
// waiting for the next Start.
const watchExecutePollCallTimeout = 2 * time.Second

var (
	watchMu     sync.Mutex
	watchCancel context.CancelFunc
	watchDone   chan struct{}
)

// WatchExecute starts a background loop that drains this device's
// EventExecute queue (see PollExecute) and invokes cb.OnNotification for
// each notification, in delivery order, until StopWatchExecute is called.
// Calling WatchExecute again replaces any previously running watcher
// (stopping and waiting for it first) rather than running two at once.
//
// If no daemon is currently running -- before the first Start, or between
// a Stop and the next Start/StartWithKey -- the loop just keeps checking
// rather than exiting, so a single WatchExecute registration survives a
// Stop/Start identity switch with no need to re-register.
func WatchExecute(cb ExecuteCallback) error {
	if cb == nil {
		return fmt.Errorf("kvmobile: WatchExecute: cb must not be nil")
	}

	watchMu.Lock()
	defer watchMu.Unlock()

	stopWatchExecuteLocked()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	watchCancel = cancel
	watchDone = done

	go runExecuteWatch(ctx, done, cb)
	return nil
}

// StopWatchExecute stops a running WatchExecute loop, if any, and waits
// for it to actually exit before returning. Safe to call when nothing is
// running (a no-op).
func StopWatchExecute() {
	watchMu.Lock()
	defer watchMu.Unlock()
	stopWatchExecuteLocked()
}

// stopWatchExecuteLocked requires watchMu already held.
func stopWatchExecuteLocked() {
	if watchCancel == nil {
		return
	}
	watchCancel()
	<-watchDone
	watchCancel = nil
	watchDone = nil
}

// runExecuteWatch is WatchExecute's background loop body. It always
// terminates by closing done, whether via ctx cancellation (the normal
// StopWatchExecute/replaced-by-a-new-WatchExecute path) or, in principle,
// never on its own -- there's no other exit.
func runExecuteWatch(ctx context.Context, done chan struct{}, cb ExecuteCallback) {
	defer close(done)

	wait := func() (stop bool) {
		select {
		case <-ctx.Done():
			return true
		case <-time.After(watchExecutePollInterval):
			return false
		}
	}

	for {
		if ctx.Err() != nil {
			return
		}

		sess, err := currentSession()
		if err != nil {
			// No daemon running right now -- keep waiting rather than
			// exiting, so a later Start/StartWithKey picks this watcher
			// back up with no action needed from the caller.
			if wait() {
				return
			}
			continue
		}

		pollCtx, cancel := context.WithTimeout(ctx, watchExecutePollCallTimeout)
		sender, payload, ok, err := sess.PollExecute(pollCtx)
		cancel()
		if err != nil || !ok {
			if wait() {
				return
			}
			continue
		}

		cb.OnNotification(sender, string(payload))
		// Loop again immediately (no wait) to drain any backlog quickly.
	}
}

// SendEvent sends one raw pkg/shmevent event to this device's own
// in-process daemon and returns the JSON response -- the same
// human-readable JSON shape pkg/e2edata.Event and kvctl-cli sendevent use
// (e.g. `{"event":"get_field","value":"hello"}`, see that type's doc
// comment for the exact field names and how binary values are
// represented). This gives the e2e test pipeline the same raw-event
// fidelity on Android it already has on desktop/remote via kvctl-cli
// sendevent, instead of only Submit/Get's higher-level Set/Get. Requires
// Start to have completed successfully.
//
// Unlike Submit/Get, this dials pkg/ipc.Call directly rather than going
// through the cached *shmclient.Session -- Session only ever signs with
// the one key it fetched at Open time, but a raw event caller may
// legitimately want e.g. an unsigned EventGetPublicKey, so the signing
// decision has to be made per call, the same way kvctl-cli's cmdSendEvent
// does it.
func SendEvent(eventJSON string) (string, error) {
	mu.Lock()
	id := peerID
	ok := started
	mu.Unlock()
	if !ok {
		return "", fmt.Errorf("kvmobile: Start has not completed successfully yet")
	}

	var ev e2edata.Event
	if err := json.Unmarshal([]byte(eventJSON), &ev); err != nil {
		return "", fmt.Errorf("kvmobile: parse event json: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), callTimeout)
	defer cancel()

	var priv shmevent.PrivateKey
	if shmevent.RequiresSignature(ev.EventType) {
		keyResp, err := ipc.Call(ctx, id, shmevent.Msg{EventType: shmevent.EventGetPrivateKey, ID: randomID()}, nil)
		if err != nil {
			return "", fmt.Errorf("kvmobile: fetch signing key: %w", err)
		}
		if keyResp.EventType == shmevent.EventError {
			return "", fmt.Errorf("kvmobile: fetch signing key: %s", keyResp.Value)
		}
		priv = shmevent.PrivateKey(keyResp.Value)
	}

	msg := ev.ToMsg()
	if msg.ID == 0 {
		msg.ID = randomID()
	}
	resp, err := ipc.Call(ctx, id, msg, priv)
	if err != nil {
		return "", fmt.Errorf("kvmobile: send event: %w", err)
	}

	out, err := json.Marshal(e2edata.EventFromMsg(resp))
	if err != nil {
		return "", fmt.Errorf("kvmobile: encode response: %w", err)
	}
	return string(out), nil
}

// randomID returns a random non-zero id -- 0 is reserved meaning
// "SourceID/DestinationID not used" (see api/shmevent.capnp), so a real
// message's own id avoids it too. Mirrors pkg/shmclient.newID and
// cmd/kvctl-cli's randomID.
func randomID() uint16 {
	for {
		var b [2]byte
		if _, err := rand.Read(b[:]); err != nil {
			return 1
		}
		if id := binary.BigEndian.Uint16(b[:]); id != 0 {
			return id
		}
	}
}

// ensureIdentity loads the Ed25519 identity persisted under dataDir from a
// previous run, or generates and persists a new one -- same on-disk format
// as pkg/daemon's loadKey (hex-encoded marshaled key) and pkg/kvctl's
// generateIdentity, so a data dir produced by either is interchangeable
// with this one.
func ensureIdentity(dataDir string) (keyPath, peerID string, err error) {
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", fmt.Errorf("kvmobile: create data dir: %w", err)
	}
	keyPath = filepath.Join(dataDir, "identity.key")

	if pid, err := readIdentityFile(keyPath); err == nil {
		return keyPath, pid.String(), nil
	}

	priv, err := generateOrSeededKeyPair()
	if err != nil {
		return "", "", err
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", "", fmt.Errorf("kvmobile: derive peer id: %w", err)
	}
	raw, err := crypto.MarshalPrivateKey(priv)
	if err != nil {
		return "", "", fmt.Errorf("kvmobile: marshal key: %w", err)
	}
	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return "", "", fmt.Errorf("kvmobile: write key file: %w", err)
	}
	return keyPath, pid.String(), nil
}

// importIdentity persists keyHex (hex-encoded, identity.key's own format)
// as dataDir's identity, the same on-disk shape ensureIdentity itself
// writes, so a later plain Start against the same dataDir picks it back up
// unchanged. If dataDir already holds a *different* identity, it refuses
// rather than silently abandoning that identity's already-replicated raft
// state -- see StartWithKey's doc comment.
func importIdentity(dataDir, keyHex string) (keyPath, peerID string, err error) {
	raw, err := hex.DecodeString(strings.TrimSpace(keyHex))
	if err != nil {
		return "", "", fmt.Errorf("kvmobile: decode key: %w", err)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return "", "", fmt.Errorf("kvmobile: unmarshal key: %w", err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", "", fmt.Errorf("kvmobile: derive peer id: %w", err)
	}

	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return "", "", fmt.Errorf("kvmobile: create data dir: %w", err)
	}
	keyPath = filepath.Join(dataDir, "identity.key")
	if existingPid, err := readIdentityFile(keyPath); err == nil {
		if existingPid != pid {
			return "", "", fmt.Errorf("kvmobile: %s already holds a different identity (%s); call Delete(%s) first", dataDir, existingPid, dataDir)
		}
		return keyPath, pid.String(), nil
	}

	if err := os.WriteFile(keyPath, []byte(hex.EncodeToString(raw)), 0o600); err != nil {
		return "", "", fmt.Errorf("kvmobile: write key file: %w", err)
	}
	return keyPath, pid.String(), nil
}

// readIdentityFile loads and decodes the identity persisted at keyPath, if
// any -- the shared read side of ensureIdentity/importIdentity's
// hex-encoded-marshaled-key format.
func readIdentityFile(keyPath string) (peer.ID, error) {
	data, err := os.ReadFile(keyPath)
	if err != nil {
		return "", err
	}
	raw, err := hex.DecodeString(string(data))
	if err != nil {
		return "", fmt.Errorf("kvmobile: decode key file %s: %w", keyPath, err)
	}
	priv, err := crypto.UnmarshalPrivateKey(raw)
	if err != nil {
		return "", fmt.Errorf("kvmobile: unmarshal key file %s: %w", keyPath, err)
	}
	pid, err := peer.IDFromPrivateKey(priv)
	if err != nil {
		return "", fmt.Errorf("kvmobile: derive peer id: %w", err)
	}
	return pid, nil
}

// generateOrSeededKeyPair returns a deterministic key derived from
// identitySeedHex if set, or a freshly random one otherwise (this
// package's original, still-default behavior).
func generateOrSeededKeyPair() (crypto.PrivKey, error) {
	if identitySeedHex == "" {
		priv, _, err := crypto.GenerateKeyPair(crypto.Ed25519, -1)
		if err != nil {
			return nil, fmt.Errorf("kvmobile: generate key pair: %w", err)
		}
		return priv, nil
	}
	raw, err := hex.DecodeString(identitySeedHex)
	if err != nil {
		return nil, fmt.Errorf("kvmobile: decode identitySeedHex: %w", err)
	}
	priv, err := crypto.UnmarshalEd25519PrivateKey(raw)
	if err != nil {
		return nil, fmt.Errorf("kvmobile: unmarshal identitySeedHex: %w", err)
	}
	return priv, nil
}

// waitForReady polls for dataDir's ready file (written once the daemon's
// host/store/IPC server are up), failing fast if the daemon goroutine
// exits before that happens instead of waiting out the full timeout.
func waitForReady(dataDir string, runErrC <-chan error, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		if _, err := daemon.ReadReadyFile(dataDir); err == nil {
			return nil
		} else {
			lastErr = err
		}
		select {
		case err := <-runErrC:
			return fmt.Errorf("daemon exited during startup: %w", err)
		case <-time.After(50 * time.Millisecond):
		}
	}
	return lastErr
}
