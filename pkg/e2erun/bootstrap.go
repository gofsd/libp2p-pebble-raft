package e2erun

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/daemon"
	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// BootstrapHost is the one SSH server this project currently deploys the
// shared e2e bootstrap/leader node to.
const BootstrapHost = "root@63.250.47.155"

// BootstrapRemoteDir is deliberately its own tree, not /root/kvstore --
// that directory already holds a separately, manually managed live kvnode
// leader from earlier work on this same server; sharing a data dir or port
// with it would either corrupt its raft state or fail to bind. Isolating
// the e2e pipeline's own bootstrap node here means `mage e2e:bootstrap` can
// never disturb it.
const BootstrapRemoteDir = "/root/kvstore-e2e"

// BootstrapRemotePort is fixed (not ephemeral) so the deployment is
// idempotent across runs and the leader's multiaddr is stable enough for a
// human to hardcode in a phone/browser client between e2e runs, matching
// the existing leader's own fixed -listen-port 4001 convention.
const BootstrapRemotePort = 4101

// BootstrapToken is the sentinel EventAdd rows use in place of a literal
// leader multiaddr -- see ResolveBootstrapPlaceholder. The bootstrap
// server's address is fixed but its multiaddr embeds a QUIC/WebTransport
// certhash that isn't predictable ahead of a real deploy, so rows are
// authored against this token instead of a frozen (and possibly stale)
// address.
const BootstrapToken = "BOOTSTRAP"

// EnsureBootstrap makes sure f has a bootstrap node identity (provisioning
// one and mutating f if it doesn't yet), cross-compiles and deploys kvnode
// + kvctl-cli to BootstrapHost:BootstrapRemoteDir if not already present,
// and starts (or confirms already running) the daemon there. It returns
// the bootstrap's live, dialable multiaddr (read back from its own
// ready.json over ssh) and its peer id.
//
// Deployment is idempotent: re-running this after the daemon is already up
// just confirms it's alive and returns its current address -- safe to call
// at the start of every e2e:current/e2e:all invocation. path is saved to
// immediately after provisioning f.BootstrapNode (if it needed
// provisioning), before any remote action is taken -- a newly generated
// identity that dies with this process before ever reaching disk would
// otherwise get silently regenerated (and so *changed*) on the next
// attempt, even though the first attempt may have already deployed the
// original identity's key file remotely; caught by hitting exactly that
// after a first deploy attempt failed partway through for an unrelated
// reason.
func EnsureBootstrap(repoRoot, path string, f *e2edata.File) (multiaddr, peerID string, err error) {
	if f.BootstrapNode == 0 {
		id, _, err := f.AddNode(e2edata.PlatformDesktop)
		if err != nil {
			return "", "", fmt.Errorf("e2erun: provision bootstrap identity: %w", err)
		}
		f.BootstrapNode = id
		if err := f.Save(path); err != nil {
			return "", "", fmt.Errorf("e2erun: save provisioned bootstrap identity: %w", err)
		}
	}
	node, ok := f.Nodes[f.BootstrapNode]
	if !ok {
		return "", "", fmt.Errorf("e2erun: bootstrap_node %d not found in nodes", f.BootstrapNode)
	}

	binDir, err := buildLinuxAmd64Binaries(repoRoot)
	if err != nil {
		return "", "", err
	}

	if err := deployIfMissing(binDir); err != nil {
		return "", "", err
	}
	if err := deployKeyIfMissing(node); err != nil {
		return "", "", err
	}
	justStarted, err := startIfNotRunning()
	if err != nil {
		return "", "", err
	}

	ready, err := waitRemoteReady(BootstrapRemoteDir+"/data", readyTimeout)
	if err != nil {
		return "", "", fmt.Errorf("e2erun: bootstrap not ready: %w", err)
	}
	if ready.PeerID != node.PeerID {
		return "", "", fmt.Errorf("e2erun: bootstrap peer id mismatch: deployed identity is %s but running daemon reports %s (stale /root/kvstore-e2e data from a different identity?)", node.PeerID, ready.PeerID)
	}
	if len(ready.ListenAddrs) == 0 {
		return "", "", fmt.Errorf("e2erun: bootstrap reported no listen addresses")
	}

	if justStarted {
		// A freshly spawned kvnode has no raft configuration at all --
		// EventAdd with SourceID 0 and an empty Value is what
		// pkg/daemon.handleAdd reads as "bootstrap as the cluster's sole
		// leader" (see pkg/shmevent.EventAdd's doc comment). Without this,
		// the daemon just sits there correctly rejecting every join
		// attempt with "not leader" -- caught by a real join row failing
		// with exactly that error against a freshly deployed bootstrap.
		status, errMsg := sendEventRemote(node.PeerID, e2edata.NewEvent(shmevent.EventAdd, 0, 0, nil, 0))
		if status != e2edata.StatusPass {
			return "", "", fmt.Errorf("e2erun: bootstrap self-leader EventAdd: %s", errMsg)
		}
	}

	return ready.ListenAddrs[0], node.PeerID, nil
}

// ResolveBootstrapPlaceholder replaces BootstrapToken with the live
// bootstrap multiaddr inside an EventAdd row's value, leaving any other
// value untouched. Rows are authored against the token (see BootstrapToken)
// since the real address isn't known until deploy time.
func ResolveBootstrapPlaceholder(value, bootstrapMultiaddr string) string {
	if value == BootstrapToken {
		return bootstrapMultiaddr
	}
	return value
}

func buildLinuxAmd64Binaries(repoRoot string) (string, error) {
	binDir := filepath.Join(repoRoot, ".e2e-build", "linux_amd64")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", err
	}
	for _, pkg := range []string{"./cmd/kvnode", "./cmd/kvctl-cli"} {
		name := filepath.Base(pkg)
		out := filepath.Join(binDir, name)
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = repoRoot
		cmd.Env = append(os.Environ(), "GOOS=linux", "GOARCH=amd64")
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", fmt.Errorf("e2erun: cross-compile %s for linux/amd64: %w", pkg, err)
		}
	}
	return binDir, nil
}

// deployIfMissing scp's kvnode/kvctl-cli to the bootstrap host only if
// they're not already there with a matching size -- avoids re-uploading on
// every single e2e run while still self-healing a partial/missing deploy.
func deployIfMissing(binDir string) error {
	if err := sshRun(BootstrapHost, fmt.Sprintf("mkdir -p %s/bin %s/data", BootstrapRemoteDir, BootstrapRemoteDir)); err != nil {
		return err
	}
	for _, name := range []string{"kvnode", "kvctl-cli"} {
		local := filepath.Join(binDir, name)
		remote := BootstrapRemoteDir + "/bin/" + name
		localInfo, err := os.Stat(local)
		if err != nil {
			return err
		}
		out, err := sshOutput(BootstrapHost, fmt.Sprintf("stat -c%%s %s 2>/dev/null || true", remote))
		if err == nil && fmt.Sprintf("%d\n", localInfo.Size()) == out {
			continue // already deployed, same size
		}
		if err := scp(local, BootstrapHost, remote); err != nil {
			return err
		}
		if err := sshRun(BootstrapHost, "chmod +x "+remote); err != nil {
			return err
		}
	}
	return nil
}

func deployKeyIfMissing(node e2edata.Node) error {
	remoteKey := BootstrapRemoteDir + "/data/identity.key"
	out, _ := sshOutput(BootstrapHost, "cat "+remoteKey+" 2>/dev/null || true")
	if out != "" {
		return nil // already deployed
	}
	tmp, err := os.CreateTemp("", "kvstore-e2e-bootstrap-key-*")
	if err != nil {
		return err
	}
	defer os.Remove(tmp.Name())
	tmp.Close()
	if err := e2edata.WriteDesktopKeyFile(node, tmp.Name()); err != nil {
		return err
	}
	return scp(tmp.Name(), BootstrapHost, remoteKey)
}

// bootstrapPidFile is where startIfNotRunning records the daemon's pid, so
// a later run can check liveness precisely (`kill -0 <pid>`) instead of a
// `pgrep -f`-style command-line pattern match -- which, run over ssh as
// `ssh host "pgrep -f '<pattern containing the remote dir path>'"`, would
// false-positive-match the very shell process ssh itself spawns to run
// that command (its argv literally contains the search pattern), since
// pgrep only excludes itself, not its parent -- caught by testing this
// against the real server before trusting it.
const bootstrapPidFile = BootstrapRemoteDir + "/data/e2e.pid"

// startIfNotRunning starts the bootstrap kvnode over ssh unless
// bootstrapPidFile names a still-live pid, in which case it's a no-op.
// justStarted reports which happened -- EnsureBootstrap only needs to send
// the self-leader EventAdd bootstrap request the first time a daemon is
// actually started, not on every idempotent "already running" check.
func startIfNotRunning() (justStarted bool, err error) {
	out, _ := sshOutput(BootstrapHost, fmt.Sprintf("kill -0 \"$(cat %s 2>/dev/null)\" 2>/dev/null && echo alive || true", bootstrapPidFile))
	if strings.TrimSpace(out) == "alive" {
		return false, nil // already running
	}
	// setsid fully detaches the daemon into its own session, immune to
	// the SIGHUP the ssh server sends the command's process group when
	// the channel closes -- verified directly against this server that
	// the more common `nohup cmd & disown` idiom does *not* survive here:
	// disown isn't a dash builtin (this host's /bin/sh), so it silently
	// no-ops/errors and the backgrounded job dies with the ssh session
	// anyway. `< /dev/null` matters too: leaving stdin attached to the
	// ssh channel is enough on its own to keep the session from being
	// considered closed.
	remoteCmd := fmt.Sprintf(
		"setsid %[1]s/bin/kvnode -data-dir %[1]s/data -key-path %[1]s/data/identity.key -listen-port %[2]d -relay-service >%[1]s/daemon.log 2>&1 </dev/null & echo $! >%[3]s",
		BootstrapRemoteDir, BootstrapRemotePort, bootstrapPidFile,
	)
	if err := sshRun(BootstrapHost, remoteCmd); err != nil {
		return false, err
	}
	return true, nil
}

const readyTimeout = 30 * time.Second

// waitRemoteReady polls for remoteDataDir/ready.json over ssh, returning a
// real timeout error if it never appears -- earlier versions folded "cat
// succeeded but the file doesn't exist yet" (via `|| true`, needed so a
// merely-not-ready-yet poll doesn't count as an ssh failure) into a nil
// lastErr, so a daemon that never started at all was reported as "ready"
// with a zero-value ReadyInfo instead of a clear timeout -- caught by
// actually hitting it against the real server, not by inspection.
func waitRemoteReady(remoteDataDir string, timeout time.Duration) (daemon.ReadyInfo, error) {
	deadline := time.Now().Add(timeout)
	lastErr := fmt.Errorf("no attempt made")
	for time.Now().Before(deadline) {
		out, err := sshOutput(BootstrapHost, "cat "+remoteDataDir+"/"+daemon.ReadyFileName+" 2>/dev/null || true")
		if err != nil {
			lastErr = err
		} else if out == "" {
			lastErr = fmt.Errorf("ready file not written yet")
		} else {
			var info daemon.ReadyInfo
			if jsonErr := json.Unmarshal([]byte(out), &info); jsonErr == nil {
				return info, nil
			} else {
				lastErr = fmt.Errorf("parse ready file: %s: %w", out, jsonErr)
			}
		}
		time.Sleep(500 * time.Millisecond)
	}
	return daemon.ReadyInfo{}, fmt.Errorf("timed out after %s waiting for %s/%s: %w", timeout, remoteDataDir, daemon.ReadyFileName, lastErr)
}

func shellQuote(s string) string {
	return "'" + strings.ReplaceAll(s, "'", `'\''`) + "'"
}
