package e2erun

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
)

// localE2EHome is where locally-run (non-bootstrap) desktop test nodes keep
// their data -- deliberately separate from the operator's normal
// ~/.libp2p-kv-raft (pkg/registry.EnvHome default) so e2e runs never
// collide with or disturb nodes created by `mage addnode` for manual use.
func localE2EHome() (string, error) {
	home, err := os.UserHomeDir()
	if err != nil {
		return "", err
	}
	dir := filepath.Join(home, ".libp2p-kv-raft-e2e")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", err
	}
	return dir, nil
}

// desktopNodeDataDir returns the fixed data directory for node's identity
// under localE2EHome -- fixed (keyed by peer id), not ephemeral, so this
// node's identity/data survives across e2e runs the same way a normal
// `mage addnode`-created node does.
func desktopNodeDataDir(e2eHome string, node e2edata.Node) string {
	return filepath.Join(e2eHome, "nodes", node.PeerID)
}

// desktopNodePort deterministically derives a fixed listen port from
// nodeID so repeated e2e runs bind the same port every time ("predictable
// deploy") without a shared port registry.
func desktopNodePort(nodeID int) int {
	return 15600 + nodeID
}

// EnsureLocalDesktopNode makes sure a kvnode process for node (a
// PlatformDesktop identity that is not the bootstrap node) is running
// locally, spawning it if it isn't already (idempotent: a live pidfile
// means it's reused as-is, matching "predictable deploy"). It does not
// join the node to any cluster -- that happens through the row sequence's
// own EventAdd, same as any other recorded event.
func EnsureLocalDesktopNode(kvnodeBin string, nodeID int, node e2edata.Node) error {
	e2eHome, err := localE2EHome()
	if err != nil {
		return err
	}
	dataDir := desktopNodeDataDir(e2eHome, node)
	if err := os.MkdirAll(dataDir, 0o755); err != nil {
		return err
	}
	keyPath := filepath.Join(dataDir, "identity.key")
	if _, err := os.Stat(keyPath); os.IsNotExist(err) {
		if err := e2edata.WriteDesktopKeyFile(node, keyPath); err != nil {
			return err
		}
	}

	pidPath := filepath.Join(dataDir, "e2e.pid")
	if isPidfileAlive(pidPath) {
		return nil
	}

	logPath := filepath.Join(dataDir, "daemon.log")
	logFile, err := os.OpenFile(logPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	defer logFile.Close()

	cmd := exec.Command(kvnodeBin,
		"-data-dir", dataDir,
		"-key-path", keyPath,
		"-listen-port", strconv.Itoa(desktopNodePort(nodeID)),
	)
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	if err := cmd.Start(); err != nil {
		return fmt.Errorf("e2erun: start local desktop node %s: %w", node.PeerID, err)
	}
	go cmd.Wait()

	if err := os.WriteFile(pidPath, []byte(strconv.Itoa(cmd.Process.Pid)), 0o644); err != nil {
		return err
	}

	if _, err := waitLocalReady(dataDir, readyTimeout); err != nil {
		return fmt.Errorf("e2erun: local desktop node %s never became ready: %w", node.PeerID, err)
	}
	return nil
}

func waitLocalReady(dataDir string, timeout time.Duration) (daemon.ReadyInfo, error) {
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

func isPidfileAlive(pidPath string) bool {
	data, err := os.ReadFile(pidPath)
	if err != nil {
		return false
	}
	pid, err := strconv.Atoi(string(data))
	if err != nil {
		return false
	}
	return isAlive(pid)
}
