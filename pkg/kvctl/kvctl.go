// Package kvctl implements the client-side logic behind the mage
// addnode/use/set/get targets: spawning and bootstrapping kvnode daemon
// processes and round-tripping set/get requests to them over pkg/ipc. It is
// a plain importable package (not a magefile) so both the mage targets and
// tests can drive it directly.
package kvctl

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipc"
	"github.com/gofsd/libp2p-kv-raft/pkg/ipcproto"
	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
)

// readyTimeout bounds how long AddNode waits for a freshly spawned daemon to
// write its ready file before giving up.
const readyTimeout = 15 * time.Second

// ipcTimeout bounds how long a single shmring request/response round trip
// may take.
const ipcTimeout = 10 * time.Second

// AddNode implements `mage addnode [leaderPeerID] [ownPeerID]`, building the
// kvnode daemon binary from source. It requires a Go toolchain and this
// repo's source tree on the machine it runs on -- see AddNodeWithBinary for
// deploying to a machine that has neither (e.g. over SSH to a bare VPS).
//
//   - 0 args: create a brand new node and bootstrap it as the cluster's
//     sole leader.
//   - 1 arg (leaderPeerID): create a brand new node and join it to the
//     cluster led by leaderPeerID. leaderPeerID may be a bare peer id
//     created on this machine (resolved through the local registry) or a
//     full multiaddr, e.g. "/ip4/1.2.3.4/tcp/4001/p2p/12D3Koo...", for a
//     leader on another machine.
//   - 2 args (leaderPeerID, ownPeerID): restart the existing node
//     identified by ownPeerID (reusing its data dir/identity) and (re)join
//     it to leaderPeerID -- used to bring a node back after it went down,
//     possibly under a new leader.
//
// It returns the peer id of the node that was created or restarted, and
// leaves it selected as the "current" node for Set/Get.
func AddNode(repoRoot string, peerIDs ...string) (string, error) {
	return AddNodeWithArgs(repoRoot, nil, peerIDs...)
}

// AddNodeWithArgs is like AddNode but appends extraDaemonArgs to the
// spawned kvnode's command line, e.g. []string{"-raft-election-timeout",
// "300ms"} to shorten raft's WAN-appropriate default timeouts for a fast
// same-machine test.
func AddNodeWithArgs(repoRoot string, extraDaemonArgs []string, peerIDs ...string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return "", err
	}
	return addNode(reg, binPath, extraDaemonArgs, peerIDs...)
}

// AddNodeWithBinary is like AddNode but uses an already-built kvnode binary
// at binPath instead of compiling one from source, so it works on a
// machine with no Go toolchain -- the case for a remote deployment target
// reached over SSH, where the binary was cross-compiled elsewhere and
// copied over. extraDaemonArgs are appended to the spawned kvnode's
// command line, e.g. []string{"-listen-port", "4001", "-relay-service"}
// for a publicly reachable leader that needs a fixed port and to act as a
// relay for other nodes.
func AddNodeWithBinary(binPath string, extraDaemonArgs []string, peerIDs ...string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return addNode(reg, binPath, extraDaemonArgs, peerIDs...)
}

func addNode(reg *registry.Registry, binPath string, extraDaemonArgs []string, peerIDs ...string) (string, error) {
	switch len(peerIDs) {
	case 0:
		return addNew(reg, binPath, extraDaemonArgs, registry.RoleLeader, "")
	case 1:
		leaderPeerID := peerIDs[0]
		if err := checkLeaderReachable(reg, leaderPeerID); err != nil {
			return "", err
		}
		return addNew(reg, binPath, extraDaemonArgs, registry.RoleFollower, leaderPeerID)
	case 2:
		return rejoin(reg, binPath, extraDaemonArgs, peerIDs[0], peerIDs[1])
	default:
		return "", fmt.Errorf("addnode: expected 0, 1, or 2 arguments (leaderPeerID, ownPeerID), got %d", len(peerIDs))
	}
}

func addNew(reg *registry.Registry, binPath string, extraDaemonArgs []string, role registry.Role, leaderPeerID string) (string, error) {
	peerID, dataDir, keyPath, err := generateIdentity(reg.NodeDataDir)
	if err != nil {
		return "", err
	}
	return bootUp(reg, binPath, extraDaemonArgs, peerID, dataDir, keyPath, role, leaderPeerID)
}

func rejoin(reg *registry.Registry, binPath string, extraDaemonArgs []string, leaderPeerID, ownPeerID string) (string, error) {
	info, ok, err := reg.Get(ownPeerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("addnode: unknown peer id %s; nothing to rejoin", ownPeerID)
	}
	if err := ensureNotAlreadyRunning(info); err != nil {
		return "", err
	}
	if err := checkLeaderReachable(reg, leaderPeerID); err != nil {
		return "", err
	}
	return bootUp(reg, binPath, extraDaemonArgs, ownPeerID, info.DataDir, info.KeyPath, info.Role, leaderPeerID)
}

// ResumeNode restarts the existing node ownPeerID in place -- reusing its
// data dir and identity -- with no leader coordination at all: the daemon
// recognizes it already has persisted raft state (see pkg/daemon.Run's
// auto-resume check) and resumes operating on it directly, no ActionAdd
// needed. Use this when the node's address hasn't changed since it went
// down (a pinned -listen-port makes that reliable) and no leader needs to
// be told about it; if the address changed, or a different/new leader
// needs to know, use AddNode's 2-arg rejoin form instead, which still
// works after a resume since initRaft is idempotent.
//
// It builds the kvnode daemon binary from source; see ResumeNodeWithBinary
// for a machine with no Go toolchain.
func ResumeNode(repoRoot, ownPeerID string) (string, error) {
	return ResumeNodeWithArgs(repoRoot, nil, ownPeerID)
}

// ResumeNodeWithArgs is like ResumeNode but appends extraDaemonArgs to the
// spawned kvnode's command line, e.g. []string{"-raft-election-timeout",
// "300ms"} to shorten raft's WAN-appropriate default timeouts for a fast
// same-machine test -- the ResumeNode equivalent of AddNodeWithArgs.
func ResumeNodeWithArgs(repoRoot string, extraDaemonArgs []string, ownPeerID string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return "", err
	}
	return resumeNode(reg, binPath, extraDaemonArgs, ownPeerID)
}

// ResumeNodeWithBinary is the AddNodeWithBinary-equivalent of ResumeNode,
// for a machine with no Go toolchain (a remote deployment target).
func ResumeNodeWithBinary(binPath, ownPeerID string, extraDaemonArgs []string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return resumeNode(reg, binPath, extraDaemonArgs, ownPeerID)
}

func resumeNode(reg *registry.Registry, binPath string, extraDaemonArgs []string, ownPeerID string) (string, error) {
	info, ok, err := reg.Get(ownPeerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("resumenode: unknown peer id %s; nothing to resume", ownPeerID)
	}
	if err := ensureNotAlreadyRunning(info); err != nil {
		return "", err
	}

	proc, err := spawnDaemon(binPath, info.DataDir, info.KeyPath, extraDaemonArgs)
	if err != nil {
		return "", err
	}
	ready, err := waitForReady(info.DataDir, readyTimeout)
	if err != nil {
		return "", fmt.Errorf("resumenode: waiting for node %s to start: %w", ownPeerID, err)
	}

	info.ListenAddrs = ready.ListenAddrs
	info.PID = proc.Pid
	if err := reg.Put(info); err != nil {
		return "", err
	}
	if err := reg.SetCurrent(ownPeerID); err != nil {
		return "", err
	}
	return ownPeerID, nil
}

// ensureNotAlreadyRunning refuses to spawn a second daemon against the same
// data dir if the registry's last known PID for it is still alive -- two
// processes writing to the same Pebble/BoltDB files concurrently would
// corrupt them.
func ensureNotAlreadyRunning(info registry.NodeInfo) error {
	if info.PID != 0 && isAlive(info.PID) {
		return fmt.Errorf("node %s appears to already be running (pid %d); stop it first", info.PeerID, info.PID)
	}
	return nil
}

// checkLeaderReachable validates leaderPeerID as early as possible. A full
// multiaddr (a leader on another machine, e.g. a remote deployment) can't
// be checked locally at all -- there's no shared registry -- so it's
// accepted as-is and only a real dial attempt during the daemon's Add
// bootstrap can confirm it. A bare peer id is expected to have been created
// on this machine, so it's worth rejecting immediately if it's not.
func checkLeaderReachable(reg *registry.Registry, leaderPeerID string) error {
	if registry.IsMultiaddr(leaderPeerID) {
		return nil
	}
	if _, ok, err := reg.Get(leaderPeerID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("addnode: unknown leader peer id %s (not created on this machine)", leaderPeerID)
	}
	return nil
}

func bootUp(reg *registry.Registry, binPath string, extraDaemonArgs []string, peerID, dataDir, keyPath string, role registry.Role, leaderPeerID string) (string, error) {
	proc, err := spawnDaemon(binPath, dataDir, keyPath, extraDaemonArgs)
	if err != nil {
		return "", err
	}

	ready, err := waitForReady(dataDir, readyTimeout)
	if err != nil {
		return "", fmt.Errorf("addnode: waiting for node %s to start: %w", peerID, err)
	}

	if err := reg.Put(registry.NodeInfo{
		PeerID:       peerID,
		Role:         role,
		DataDir:      dataDir,
		KeyPath:      keyPath,
		ListenAddrs:  ready.ListenAddrs,
		LeaderPeerID: leaderPeerID,
		PID:          proc.Pid,
	}); err != nil {
		return "", err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	resp, err := ipc.Call(ctx, peerID, ipcproto.NewRequest(ipcproto.ActionAdd, leaderPeerID, ""))
	if err != nil {
		return "", fmt.Errorf("addnode: bootstrap request to %s: %w", peerID, err)
	}
	if resp.Status != ipcproto.StatusOK {
		return "", fmt.Errorf("addnode: node %s rejected bootstrap: %s", peerID, resp.ValueString())
	}

	if err := reg.SetCurrent(peerID); err != nil {
		return "", err
	}
	return peerID, nil
}

// Use implements `mage use <peerID>`: selects peerID as the node Set/Get
// target.
func Use(peerID string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	if _, ok, err := reg.Get(peerID); err != nil {
		return err
	} else if !ok {
		return fmt.Errorf("use: unknown peer id %s (not created on this machine)", peerID)
	}
	return reg.SetCurrent(peerID)
}

// Set implements `mage set <key> <value>`: applies key=value through raft on
// the current node.
func Set(key, value string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	peerID, err := reg.Current()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	resp, err := ipc.Call(ctx, peerID, ipcproto.NewRequest(ipcproto.ActionSet, key, value))
	if err != nil {
		return fmt.Errorf("set: %w", err)
	}
	if resp.Status != ipcproto.StatusOK {
		return fmt.Errorf("set: %s", resp.ValueString())
	}
	return nil
}

// Get implements `mage get <key>`: reads key from the current node's local
// state (which may be a follower serving a possibly-eventually-consistent
// replicated read).
func Get(key string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	peerID, err := reg.Current()
	if err != nil {
		return "", err
	}
	return GetFrom(peerID, key)
}

// GetFrom reads key from the node identified by peerID, regardless of which
// node is currently selected. Used by tests that need to target a specific
// node without disturbing the "current" selection.
func GetFrom(peerID, key string) (string, error) {
	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	resp, err := ipc.Call(ctx, peerID, ipcproto.NewRequest(ipcproto.ActionGet, key, ""))
	if err != nil {
		return "", fmt.Errorf("get: %w", err)
	}
	if resp.Status != ipcproto.StatusOK {
		return "", fmt.Errorf("get: %s", resp.ValueString())
	}
	return resp.ValueString(), nil
}

func ensureDaemonBinary(reg *registry.Registry, repoRoot string) (string, error) {
	binDir := filepath.Join(reg.Dir, "bin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	name := "kvnode"
	if runtime.GOOS == "windows" {
		name += ".exe"
	}
	binPath := filepath.Join(binDir, name)

	cmd := exec.Command("go", "build", "-o", binPath, "./cmd/kvnode")
	cmd.Dir = repoRoot
	cmd.Stdout = os.Stderr
	cmd.Stderr = os.Stderr
	if err := cmd.Run(); err != nil {
		return "", fmt.Errorf("kvctl: build kvnode binary: %w", err)
	}
	return binPath, nil
}

func spawnDaemon(binPath, dataDir, keyPath string, extraArgs []string) (*os.Process, error) {
	logPath := filepath.Join(dataDir, "daemon.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kvctl: open daemon log: %w", err)
	}
	defer logFile.Close()

	args := append([]string{"-data-dir", dataDir, "-key-path", keyPath}, extraArgs...)
	cmd := exec.Command(binPath, args...)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	detach(cmd)

	if err := cmd.Start(); err != nil {
		return nil, fmt.Errorf("kvctl: start kvnode: %w", err)
	}
	// Reap the process's exit status in the background so it doesn't become
	// a zombie; we don't otherwise wait on it since it's meant to outlive us.
	go cmd.Wait()

	return cmd.Process, nil
}

func waitForReady(dataDir string, timeout time.Duration) (daemon.ReadyInfo, error) {
	deadline := time.Now().Add(timeout)
	var lastErr error
	for time.Now().Before(deadline) {
		info, err := daemon.ReadReadyFile(dataDir)
		if err == nil {
			return info, nil
		}
		lastErr = err
		time.Sleep(50 * time.Millisecond)
	}
	return daemon.ReadyInfo{}, lastErr
}
