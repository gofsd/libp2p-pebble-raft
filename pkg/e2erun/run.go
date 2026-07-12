// Package e2erun implements the deploy-and-run engine behind the mage
// e2e:* targets: it turns pkg/e2edata's recorded rows into real actions --
// a real SSH-deployed bootstrap/leader node, real locally-spawned desktop
// kvnode processes, a real Playwright-driven browser check for web rows --
// and writes each row's outcome back into the file.
package e2erun

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// Run executes every row in rowIndices (indices into f.Rows -- see
// e2edata.File.PendingRows/AllRowIndices) in order, updating each row's
// Status/Error in place and saving f to path after every single row, so a
// crash or Ctrl-C mid-run still leaves already-recorded outcomes on disk
// instead of losing the whole batch.
//
// It always provisions/confirms the SSH bootstrap leader first (see
// EnsureBootstrap) since EventAdd rows join against it via
// BootstrapToken. It returns an error if any non-skipped row ends up
// failing; the caller should still treat f/path as having the real,
// saved results even when this returns an error.
func Run(repoRoot, path string, f *e2edata.File, rowIndices []int) error {
	bootstrapMultiaddr, bootstrapPeerID, err := EnsureBootstrap(repoRoot, path, f)
	if err != nil {
		return fmt.Errorf("e2erun: ensure bootstrap: %w", err)
	}
	fmt.Fprintf(os.Stderr, "e2erun: bootstrap %s ready at %s\n", bootstrapPeerID, bootstrapMultiaddr)

	kvnodeBin, kvctlBin, err := buildNativeBinaries(repoRoot)
	if err != nil {
		return err
	}

	web := &webRunResult{}
	failures := 0
	for _, idx := range rowIndices {
		row := &f.Rows[idx]
		node, ok := f.Nodes[row.Node]
		if !ok {
			row.Status = e2edata.StatusFail
			row.Error = fmt.Sprintf("unknown node id %d", row.Node)
			failures++
		} else {
			status, errMsg := runRow(repoRoot, kvnodeBin, kvctlBin, f, row.Node, node, bootstrapMultiaddr, row.Event, web)
			row.Status = status
			row.Error = errMsg
			if status == e2edata.StatusFail {
				failures++
			}
		}
		fmt.Fprintf(os.Stderr, "e2erun: row %d (version %d, node %d, event %s): %s\n",
			idx, row.Version, row.Node, shmevent.EventName(row.Event.EventType), statusName(row.Status))
		if err := f.Save(path); err != nil {
			return err
		}
	}

	if failures > 0 {
		return fmt.Errorf("e2erun: %d row(s) failed", failures)
	}
	return nil
}

func statusName(status int) string {
	switch status {
	case e2edata.StatusPass:
		return "PASS"
	case e2edata.StatusSkipped:
		return "SKIP"
	default:
		return "FAIL"
	}
}

// runRow dispatches one row to the right execution path: the SSH bootstrap
// node itself (nodeID == f.BootstrapNode) goes over ssh; other desktop
// identities get a real local kvnode process and a real sendevent call;
// web rows share one Playwright-driven verdict per Run; android rows are
// always reported skipped (see web.go/runWebSuite and this package's doc
// comment for the reasoning).
func runRow(repoRoot, kvnodeBin, kvctlBin string, f *e2edata.File, nodeID int, node e2edata.Node, bootstrapMultiaddr string, ev e2edata.Event, web *webRunResult) (status int, errMsg string) {
	if ev.EventType == shmevent.EventAdd {
		if v, err := ev.Value(); err == nil {
			resolved := ResolveBootstrapPlaceholder(string(v), bootstrapMultiaddr)
			ev = e2edata.NewEvent(ev.EventType, ev.SourceID, ev.DestinationID, []byte(resolved), ev.ID)
		}
	}

	switch {
	case nodeID == f.BootstrapNode:
		return retryReadsIfNeeded(ev, func() (int, string) { return sendEventRemote(node.PeerID, ev) })
	case node.Platform == e2edata.PlatformDesktop:
		if err := EnsureLocalDesktopNode(kvnodeBin, nodeID, node); err != nil {
			return e2edata.StatusFail, err.Error()
		}
		return retryReadsIfNeeded(ev, func() (int, string) { return sendEventLocal(kvctlBin, node.PeerID, ev) })
	case node.Platform == e2edata.PlatformWeb:
		return web.resultFor(repoRoot, bootstrapMultiaddr)
	case node.Platform == e2edata.PlatformAndroid:
		return e2edata.StatusSkipped, "android e2e execution not implemented yet -- no on-device/emulator build automation exists; see mobile/kvmobile"
	default:
		return e2edata.StatusFail, fmt.Sprintf("unknown platform %q", node.Platform)
	}
}

// readRetryAttempts/readRetryDelay bound retryReadsIfNeeded's total wait to
// a few seconds -- generous relative to hashicorp/raft's own commit/apply
// latency on a real WAN link, but still a hard bound so a genuinely broken
// read fails the row instead of hanging.
const (
	readRetryAttempts = 10
	readRetryDelay    = 300 * time.Millisecond
)

// retryReadsIfNeeded retries dispatch a few times, with a short delay
// between attempts, only for EventGetField/EventGetKey -- a raft
// follower's local read can briefly lag just behind a Set that only just
// committed on the leader (see e.g. web-app/README.md's "Running it"
// section, which documents this same caveat for the browser client's own
// Get), so a GetField row placed right after the SetField row that wrote
// what it reads needs a little slack, not a hard requirement that
// replication has already caught up in zero time. Every other event type
// dispatches exactly once, unretried -- a real failure there (a bad
// signature, a rejected join, a genuinely missing key) shouldn't be masked
// by blindly retrying. Caught by a real GetField row immediately following
// its SetField failing intermittently against a live deployed cluster.
func retryReadsIfNeeded(ev e2edata.Event, dispatch func() (int, string)) (int, string) {
	if ev.EventType != shmevent.EventGetField && ev.EventType != shmevent.EventGetKey {
		return dispatch()
	}
	var status int
	var errMsg string
	for range readRetryAttempts {
		status, errMsg = dispatch()
		if status == e2edata.StatusPass {
			return status, errMsg
		}
		time.Sleep(readRetryDelay)
	}
	return status, errMsg
}

func sendEventLocal(kvctlBin, peerID string, ev e2edata.Event) (int, string) {
	argJSON, err := json.Marshal(ev)
	if err != nil {
		return e2edata.StatusFail, err.Error()
	}
	cmd := exec.Command(kvctlBin, "sendevent", peerID, string(argJSON))
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	runErr := cmd.Run()
	// kvctl-cli sendevent always prints the response JSON to stdout before
	// exiting -- including the EventError case, where it exits 1 *after*
	// printing (see cmd/kvctl-cli's cmdSendEvent) -- so a nonzero exit
	// isn't itself a reason to discard stdout; only fall back to the raw
	// process error when there's nothing on stdout to parse. Caught by
	// this producing a useless "exit status 1: " error (stderr always
	// empty on that path) against a real deployed node before this fix.
	return interpretSendEventResult(stdout.String(), stderr.String(), runErr)
}

func sendEventRemote(peerID string, ev e2edata.Event) (int, string) {
	argJSON, err := json.Marshal(ev)
	if err != nil {
		return e2edata.StatusFail, err.Error()
	}
	remoteCmd := fmt.Sprintf("%s/bin/kvctl-cli sendevent %s %s", BootstrapRemoteDir, peerID, shellQuote(string(argJSON)))
	stdout, stderr, runErr := sshOutputAnyExit(BootstrapHost, remoteCmd)
	return interpretSendEventResult(stdout, stderr, runErr)
}

func interpretSendEventResult(stdout, stderr string, runErr error) (int, string) {
	stdout = strings.TrimSpace(stdout)
	var resp e2edata.Event
	if err := json.Unmarshal([]byte(stdout), &resp); err != nil {
		if runErr != nil {
			return e2edata.StatusFail, fmt.Sprintf("%v: %s", runErr, stderr)
		}
		return e2edata.StatusFail, fmt.Sprintf("parse sendevent output %q: %v", stdout, err)
	}
	if resp.EventType == shmevent.EventError {
		v, _ := resp.Value()
		return e2edata.StatusFail, string(v)
	}
	return e2edata.StatusPass, ""
}

// buildNativeBinaries compiles kvnode/kvctl-cli for the host's own
// OS/ARCH (unlike EnsureBootstrap's linux/amd64 cross-compile), used to
// drive local desktop test nodes and local sendevent calls.
func buildNativeBinaries(repoRoot string) (kvnodeBin, kvctlBin string, err error) {
	binDir := filepath.Join(repoRoot, ".e2e-build", "native")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		return "", "", err
	}
	for _, pkg := range []string{"./cmd/kvnode", "./cmd/kvctl-cli"} {
		name := filepath.Base(pkg)
		out := filepath.Join(binDir, name)
		cmd := exec.Command("go", "build", "-o", out, pkg)
		cmd.Dir = repoRoot
		cmd.Stdout = os.Stderr
		cmd.Stderr = os.Stderr
		if err := cmd.Run(); err != nil {
			return "", "", fmt.Errorf("e2erun: build %s: %w", pkg, err)
		}
	}
	return filepath.Join(binDir, "kvnode"), filepath.Join(binDir, "kvctl-cli"), nil
}
