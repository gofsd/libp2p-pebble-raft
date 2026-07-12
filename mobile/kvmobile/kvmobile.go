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
	"sync"
	"time"

	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/peer"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
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
	mu      sync.Mutex
	started bool
	peerID  string
	runErrC chan error
	session *shmclient.Session
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
	mu.Lock()
	defer mu.Unlock()
	if started {
		return peerID, nil
	}
	if leaderMultiaddr == "" {
		return "", fmt.Errorf("kvmobile: no leader multiaddr baked in at build time")
	}

	keyPath, id, err := ensureIdentity(dataDir)
	if err != nil {
		return "", err
	}

	ctx := context.Background()
	runErrC = make(chan error, 1)
	go func() {
		runErrC <- daemon.Run(ctx, daemon.Config{
			DataDir:            dataDir,
			KeyPath:            keyPath,
			RelayPeer:          relayMultiaddr,
			HeartbeatTimeout:   raftHeartbeatTimeout,
			ElectionTimeout:    raftElectionTimeout,
			CommitTimeout:      raftCommitTimeout,
			LeaderLeaseTimeout: raftLeaderLeaseTimeout,
		})
	}()

	if err := waitForReady(dataDir, runErrC, callTimeout); err != nil {
		return "", fmt.Errorf("kvmobile: start follower: %w", err)
	}

	addCtx, cancel := context.WithTimeout(ctx, callTimeout)
	defer cancel()
	sess, err := shmclient.Open(addCtx, id)
	if err != nil {
		return "", fmt.Errorf("kvmobile: fetch signing key: %w", err)
	}
	if _, err := sess.Add(addCtx, leaderMultiaddr); err != nil {
		return "", fmt.Errorf("kvmobile: join cluster: %w", err)
	}

	session = sess
	peerID = id
	started = true
	return peerID, nil
}

// Submit sets key=value through raft, forwarding to the current leader if
// this device isn't it (see pkg/daemon's ForwardProtocolID) -- which, as a
// follower, it never is, so every Submit takes that path.
func Submit(key, value string) error {
	mu.Lock()
	sess := session
	ok := started
	mu.Unlock()
	if !ok {
		return fmt.Errorf("kvmobile: Start has not completed successfully yet")
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
	mu.Lock()
	sess := session
	ok := started
	mu.Unlock()
	if !ok {
		return "", fmt.Errorf("kvmobile: Start has not completed successfully yet")
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

	if data, err := os.ReadFile(keyPath); err == nil {
		raw, err := hex.DecodeString(string(data))
		if err != nil {
			return "", "", fmt.Errorf("kvmobile: decode key file %s: %w", keyPath, err)
		}
		priv, err := crypto.UnmarshalPrivateKey(raw)
		if err != nil {
			return "", "", fmt.Errorf("kvmobile: unmarshal key file %s: %w", keyPath, err)
		}
		pid, err := peer.IDFromPrivateKey(priv)
		if err != nil {
			return "", "", fmt.Errorf("kvmobile: derive peer id: %w", err)
		}
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
