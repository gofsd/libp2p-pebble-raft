package kvctl

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/registry"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmclient"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// stopTimeout bounds how long Leave/Rm wait for a running daemon to exit
// after being asked to terminate, before giving up on restarting it
// against the solo data dir.
const stopTimeout = 15 * time.Second

// Join implements `mage join <targetPeerID>`: asks the cluster reachable
// through targetPeerID to admit the *current* node's own identity (see
// registry.Registry.Current) -- reusing AddNode's existing 2-arg "join
// under an existing identity" (rejoin) shape verbatim, since this is the
// same running instance changing which cluster it belongs to, not
// spinning up a new one. The resulting composite data dir is named
// exactly registry.ClusterDataDir(ownPeerID, remotePeerID) would produce,
// since that's what AddNode's rejoin path already uses. If the current
// identity's daemon is already running (e.g. still operating its own solo
// db), it's stopped first -- like Leave/Rm, and unlike rejoin's own
// direct callers elsewhere, Join doesn't require the operator to do that
// manually first, since switching which cluster this instance belongs to
// is exactly the point.
//
// Whether this is admitted immediately or first requires a separate
// confirmation from a raft voter (mage confirmpermit cluster-join
// <peerID>) depends entirely on targetPeerID's own daemon's
// Config.RequireConfirmForJoin setting -- bootUp already prints a
// message when the join comes back pending instead of ok.
//
// It builds the kvnode daemon binary from source; see JoinWithBinary for
// a machine with no Go toolchain.
func Join(repoRoot, targetPeerID string) (string, error) {
	return JoinWithArgs(repoRoot, nil, targetPeerID)
}

// JoinWithArgs is like Join but appends extraDaemonArgs to the spawned
// kvnode's command line -- the Join equivalent of AddNodeWithArgs.
func JoinWithArgs(repoRoot string, extraDaemonArgs []string, targetPeerID string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return "", err
	}
	return join(reg, binPath, extraDaemonArgs, targetPeerID)
}

// JoinWithBinary is the AddNodeWithBinary-equivalent of Join, for a
// machine with no Go toolchain (a remote deployment target).
func JoinWithBinary(binPath string, extraDaemonArgs []string, targetPeerID string) (string, error) {
	reg, err := registry.Open()
	if err != nil {
		return "", err
	}
	return join(reg, binPath, extraDaemonArgs, targetPeerID)
}

func join(reg *registry.Registry, binPath string, extraDaemonArgs []string, targetPeerID string) (string, error) {
	ownPeerID, err := reg.Current()
	if err != nil {
		return "", fmt.Errorf("join: %w", err)
	}
	info, ok, err := reg.Get(ownPeerID)
	if err != nil {
		return "", fmt.Errorf("join: %w", err)
	}
	if ok {
		if err := stopAndWait(info, stopTimeout); err != nil {
			return "", fmt.Errorf("join: stop node %s: %w", ownPeerID, err)
		}
	}
	return rejoin(reg, binPath, extraDaemonArgs, targetPeerID, ownPeerID)
}

// Leave implements `mage leave <peerID>`: asks the raft cluster peerID is
// currently joined to, over its already-running daemon's local IPC, to
// remove it (raft.RemoveServer -- see shmevent.EventLeave's doc comment),
// then restarts that same identity against its own solo data dir
// (registry.NodeDataDir) -- switching this instance back to operating its
// default single-node cluster, exactly like it did before it ever joined
// anywhere. The remote cluster is never torn down: removing one member is
// a graceful shrink, and the remaining voters keep operating normally.
// The composite cluster data dir peerID was using is left on disk
// untouched, so a later `mage join`/`mage rejoinnode` back to the same
// cluster can pick its local state back up -- see Rm for the variant that
// wipes it instead.
//
// It builds the kvnode daemon binary from source; see LeaveWithBinary for
// a machine with no Go toolchain.
func Leave(repoRoot, ownPeerID string) error {
	return LeaveWithArgs(repoRoot, nil, ownPeerID)
}

// LeaveWithArgs is like Leave but appends extraDaemonArgs to the restarted
// solo daemon's command line -- the Leave equivalent of AddNodeWithArgs.
func LeaveWithArgs(repoRoot string, extraDaemonArgs []string, ownPeerID string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return err
	}
	return leave(reg, binPath, extraDaemonArgs, ownPeerID)
}

// LeaveWithBinary is the AddNodeWithBinary-equivalent of Leave, for a
// machine with no Go toolchain (a remote deployment target).
func LeaveWithBinary(binPath string, extraDaemonArgs []string, ownPeerID string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	return leave(reg, binPath, extraDaemonArgs, ownPeerID)
}

func leave(reg *registry.Registry, binPath string, extraDaemonArgs []string, ownPeerID string) error {
	info, err := requireJoined(reg, "leave", ownPeerID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	if err := shmclient.Leave(ctx, ownPeerID); err != nil {
		return fmt.Errorf("leave: %w", err)
	}

	if err := stopAndWait(info, stopTimeout); err != nil {
		return fmt.Errorf("leave: stop node %s: %w", ownPeerID, err)
	}

	return resumeSolo(reg, binPath, extraDaemonArgs, info)
}

// Rm implements `mage rm <peerID>`: everything Leave does (raft.RemoveServer
// shrink, switch back to the solo db), plus it revokes peerID's
// shmevent.KindClusterJoin standing with the cluster it's leaving -- so a
// later `mage join` attempt against the same cluster starts genuinely
// pending again, not auto-approved by a stale confirmed record -- and
// deletes the composite cluster data dir outright (mirrors DeleteNode,
// applied to the composite dir specifically, never the solo one).
//
// Order matters: the revoke happens first, while peerID is still a
// confirmed raft voter -- shmevent.EventPermitRevoke's "only a raft voter
// may revoke" check (see pkg/daemon's isVoter) would otherwise reject it
// once RemoveServer has already taken effect.
//
// It builds the kvnode daemon binary from source; see RmWithBinary for a
// machine with no Go toolchain.
func Rm(repoRoot, ownPeerID string) error {
	return RmWithArgs(repoRoot, nil, ownPeerID)
}

// RmWithArgs is like Rm but appends extraDaemonArgs to the restarted solo
// daemon's command line -- the Rm equivalent of AddNodeWithArgs.
func RmWithArgs(repoRoot string, extraDaemonArgs []string, ownPeerID string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	binPath, err := ensureDaemonBinary(reg, repoRoot)
	if err != nil {
		return err
	}
	return rm(reg, binPath, extraDaemonArgs, ownPeerID)
}

// RmWithBinary is the AddNodeWithBinary-equivalent of Rm, for a machine
// with no Go toolchain (a remote deployment target).
func RmWithBinary(binPath string, extraDaemonArgs []string, ownPeerID string) error {
	reg, err := registry.Open()
	if err != nil {
		return err
	}
	return rm(reg, binPath, extraDaemonArgs, ownPeerID)
}

func rm(reg *registry.Registry, binPath string, extraDaemonArgs []string, ownPeerID string) error {
	info, err := requireJoined(reg, "rm", ownPeerID)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), ipcTimeout)
	defer cancel()
	if err := shmclient.RevokePermit(ctx, ownPeerID, shmevent.KindClusterJoin, []byte(ownPeerID)); err != nil {
		return fmt.Errorf("rm: revoke cluster-join standing: %w", err)
	}
	if err := shmclient.Leave(ctx, ownPeerID); err != nil {
		return fmt.Errorf("rm: %w", err)
	}

	clusterDir := info.DataDir
	if err := stopAndWait(info, stopTimeout); err != nil {
		return fmt.Errorf("rm: stop node %s: %w", ownPeerID, err)
	}
	if err := os.RemoveAll(clusterDir); err != nil {
		return fmt.Errorf("rm: remove cluster data dir %s: %w", clusterDir, err)
	}

	return resumeSolo(reg, binPath, extraDaemonArgs, info)
}

// requireJoined validates that ownPeerID is a known, currently-joined
// (registry.NodeInfo.ClusterPeerID != "") node that is actually running --
// Leave/Rm both need to reach its live daemon over local IPC before they
// can do anything else, unlike every other kvctl operation (rejoin,
// ResumeNode, DeleteNode), which all require the opposite: the node
// stopped first.
func requireJoined(reg *registry.Registry, op, ownPeerID string) (registry.NodeInfo, error) {
	info, ok, err := reg.Get(ownPeerID)
	if err != nil {
		return registry.NodeInfo{}, err
	}
	if !ok {
		return registry.NodeInfo{}, fmt.Errorf("%s: unknown peer id %s (not created on this machine)", op, ownPeerID)
	}
	if info.ClusterPeerID == "" {
		return registry.NodeInfo{}, fmt.Errorf("%s: %s is not currently joined to any cluster (already on its own solo db)", op, ownPeerID)
	}
	if info.PID == 0 || !isAlive(info.PID) {
		return registry.NodeInfo{}, fmt.Errorf("%s: node %s does not appear to be running; start it first", op, ownPeerID)
	}
	return info, nil
}

// resumeSolo restarts ownPeerID's identity against its own solo data dir
// (registry.NodeDataDir) -- the same dir it was bootstrapped into
// originally, so raft.NewRaft recovers its last known configuration and
// log from disk on its own, exactly like ResumeNode's own no-coordination
// resume (this identity's solo cluster was never touched while it was
// away, whether or not other nodes had joined into it in the meantime --
// raft's own recovery reconnects with them automatically as long as
// addresses didn't change). Shared by leave/rm, the last step of both.
func resumeSolo(reg *registry.Registry, binPath string, extraDaemonArgs []string, info registry.NodeInfo) error {
	soloDir := reg.NodeDataDir(info.PeerID)

	proc, err := spawnDaemon(binPath, soloDir, info.KeyPath, extraDaemonArgs)
	if err != nil {
		return fmt.Errorf("resume solo db: %w", err)
	}
	ready, err := waitForReady(soloDir, readyTimeout)
	if err != nil {
		return fmt.Errorf("waiting for node %s to resume its solo db: %w", info.PeerID, err)
	}

	info.DataDir = soloDir
	info.ClusterPeerID = ""
	info.LeaderPeerID = ""
	info.ListenAddrs = ready.ListenAddrs
	info.PID = proc.Pid
	if err := reg.Put(info); err != nil {
		return err
	}
	return reg.SetCurrent(info.PeerID)
}

// stopAndWait asks info's live daemon process to terminate (see terminate,
// spawn_unix.go/spawn_windows.go) and polls isAlive until it's gone or
// stopTimeout elapses -- Leave/Rm's counterpart to ensureNotAlreadyRunning's
// refusal everywhere else in this package: unlike rejoin/ResumeNode/
// DeleteNode, which all require an operator to have stopped the node
// themselves first, Leave/Rm already know exactly what should happen next
// (resume the solo db) and so can safely do the stop/restart in one motion.
func stopAndWait(info registry.NodeInfo, timeout time.Duration) error {
	if info.PID == 0 || !isAlive(info.PID) {
		return nil
	}
	if err := terminate(info.PID); err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if !isAlive(info.PID) {
			return nil
		}
		time.Sleep(50 * time.Millisecond)
	}
	return fmt.Errorf("pid %d still running after %s", info.PID, timeout)
}
