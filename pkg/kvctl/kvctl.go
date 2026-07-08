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

	"github.com/gofsd/libp2p-pebble-raft/pkg/daemon"
	"github.com/gofsd/libp2p-pebble-raft/pkg/ipc"
	"github.com/gofsd/libp2p-pebble-raft/pkg/ipcproto"
	"github.com/gofsd/libp2p-pebble-raft/pkg/registry"
)

// readyTimeout bounds how long AddNode waits for a freshly spawned daemon to
// write its ready file before giving up.
const readyTimeout = 15 * time.Second

// ipcTimeout bounds how long a single shmring request/response round trip
// may take.
const ipcTimeout = 10 * time.Second

// AddNode implements `mage addnode [leaderPeerID] [ownPeerID]`:
//
//   - 0 args: create a brand new node and bootstrap it as the cluster's
//     sole leader.
//   - 1 arg (leaderPeerID): create a brand new node and join it to the
//     cluster led by leaderPeerID.
//   - 2 args (leaderPeerID, ownPeerID): restart the existing node
//     identified by ownPeerID (reusing its data dir/identity) and (re)join
//     it to leaderPeerID -- used to bring a node back after it went down,
//     possibly under a new leader.
//
// It returns the peer id of the node that was created or restarted, and
// leaves it selected as the "current" node for Set/Get.
func AddNode(repoRoot string, peerIDs ...string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}

	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return "", err
	}

	switch len(peerIDs) {
	case 0:
		return addNew(reg, binPath, registry.RoleLeader, "")
	case 1:
		leaderPeerID := peerIDs[0]
		if _, ok, err := reg.Get(leaderPeerID); err != nil {
			return "", err
		} else if !ok {
			return "", fmt.Errorf("addnode: unknown leader peer id %s (not created on this machine)", leaderPeerID)
		}
		return addNew(reg, binPath, registry.RoleFollower, leaderPeerID)
	case 2:
		return rejoin(reg, binPath, peerIDs[0], peerIDs[1])
	default:
		return "", fmt.Errorf("addnode: expected 0, 1, or 2 arguments (leaderPeerID, ownPeerID), got %d", len(peerIDs))
	}
}

func addNew(reg *registry.Registry, binPath string, role registry.Role, leaderPeerID string) (string, error) {
	peerID, dataDir, keyPath, err := generateIdentity(reg.NodeDataDir)
	if err != nil {
		return "", err
	}
	return bootUp(reg, binPath, peerID, dataDir, keyPath, role, leaderPeerID)
}

func rejoin(reg *registry.Registry, binPath, leaderPeerID, ownPeerID string) (string, error) {
	info, ok, err := reg.Get(ownPeerID)
	if err != nil {
		return "", err
	}
	if !ok {
		return "", fmt.Errorf("addnode: unknown peer id %s; nothing to rejoin", ownPeerID)
	}
	if _, ok, err := reg.Get(leaderPeerID); err != nil {
		return "", err
	} else if !ok {
		return "", fmt.Errorf("addnode: unknown leader peer id %s (not created on this machine)", leaderPeerID)
	}
	return bootUp(reg, binPath, ownPeerID, info.DataDir, info.KeyPath, info.Role, leaderPeerID)
}

func bootUp(reg *registry.Registry, binPath, peerID, dataDir, keyPath string, role registry.Role, leaderPeerID string) (string, error) {
	proc, err := spawnDaemon(binPath, dataDir, keyPath)
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

func spawnDaemon(binPath, dataDir, keyPath string) (*os.Process, error) {
	logPath := filepath.Join(dataDir, "daemon.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, fmt.Errorf("kvctl: open daemon log: %w", err)
	}
	defer logFile.Close()

	cmd := exec.Command(binPath, "-data-dir", dataDir, "-key-path", keyPath)
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
