package e2erun

import (
	"encoding/json"
	"fmt"
	"maps"
	"os"
	"os/exec"
	"path/filepath"

	"github.com/gofsd/libp2p-kv-raft/pkg/e2edata"
	"github.com/gofsd/libp2p-kv-raft/pkg/shmevent"
)

// webUnavailable checks that the web-app crate's dev dependencies are all
// present, returning a human-readable reason if not -- so a pipeline run
// on a machine without them degrades to a clear Skipped status instead of
// failing hard.
func webUnavailable(webDir string) string {
	if _, err := exec.LookPath("npx"); err != nil {
		return "npx not found on PATH (see web-app/README.md's Building section)"
	}
	if _, err := os.Stat(filepath.Join(webDir, "node_modules")); err != nil {
		return "web-app/node_modules missing (run `npm install` in web-app/)"
	}
	if _, err := os.Stat(filepath.Join(webDir, "pkg", "kv_raft_web.js")); err != nil {
		return "web-app/pkg wasm bundle missing (run `npm run build:wasm` in web-app/)"
	}
	return ""
}

// runWebRows runs every PlatformWeb row in rowIndices, grouped by node: one
// real Playwright browser run per distinct web node, with that node's own
// recorded identity (see e2edata.Node.PrivateKey) baked into the page via
// worker.js's "seed" query param -- so, like desktop/remote/android, a
// browser tab in this run reliably becomes the exact peer id recorded for
// it, and tests/e2e.spec.js drives real per-row raw shmevent dispatch
// through MainHandle::send_event rather than a single generic smoke check.
// Returns a result for every web row index in rowIndices.
//
// bootstrapWebTransportAddr must be the bootstrap's WebTransport-specific
// multiaddr (see EnsureBootstrap's doc comment on why that's a different
// address than the plain TCP/QUIC one desktop/remote/Android use) -- a
// browser sandbox can never dial anything else. Empty (no WebTransport
// listener was ever advertised) fails every web row with a clear reason
// rather than silently trying to connect a tab to nothing.
func runWebRows(repoRoot string, f *e2edata.File, rowIndices []int, bootstrapWebTransportAddr string) map[int]rowOutcome {
	results := map[int]rowOutcome{}

	byNode := map[int][]int{}
	for _, idx := range rowIndices {
		row := f.Rows[idx]
		node, ok := f.Nodes[row.Node]
		if !ok || node.Platform != e2edata.PlatformWeb {
			continue
		}
		byNode[row.Node] = append(byNode[row.Node], idx)
	}
	if len(byNode) == 0 {
		return results
	}

	if bootstrapWebTransportAddr == "" {
		for _, idxs := range byNode {
			for _, idx := range idxs {
				results[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: "bootstrap advertised no WebTransport address; a browser tab can't dial anything else"}
			}
		}
		return results
	}

	webDir := filepath.Join(repoRoot, "web-app")
	if reason := webUnavailable(webDir); reason != "" {
		for _, idxs := range byNode {
			for _, idx := range idxs {
				results[idx] = rowOutcome{status: e2edata.StatusSkipped, errMsg: "web e2e skipped: " + reason}
			}
		}
		return results
	}

	for nodeID, idxs := range byNode {
		node := f.Nodes[nodeID]
		maps.Copy(results, runWebNode(webDir, node, bootstrapWebTransportAddr, f, idxs))
	}
	return results
}

// runWebNode runs tests/e2e.spec.js once against node's identity, covering
// every row in rowIdxs (in order) in one Playwright test invocation, and
// maps the per-row results back to rowIdxs. A failure before the results
// file is ever written (a real browser/Playwright launch problem, not an
// individual row's own event failing) marks every row in this batch
// failed with that same error, since none of them actually ran.
func runWebNode(webDir string, node e2edata.Node, bootstrapWebTransportAddr string, f *e2edata.File, rowIdxs []int) map[int]rowOutcome {
	fail := func(err error) map[int]rowOutcome {
		out := make(map[int]rowOutcome, len(rowIdxs))
		for _, idx := range rowIdxs {
			out[idx] = rowOutcome{status: e2edata.StatusFail, errMsg: err.Error()}
		}
		return out
	}

	eventJSONs := make([]string, len(rowIdxs))
	for i, idx := range rowIdxs {
		ev := f.Rows[idx].Event
		if ev.EventType == shmevent.EventAdd {
			resolved := ResolveBootstrapPlaceholder(string(ev.Value()), bootstrapWebTransportAddr)
			ev = e2edata.NewEvent(ev.EventType, ev.SourceID, ev.DestinationID, []byte(resolved), ev.ID)
		}
		data, err := json.Marshal(ev)
		if err != nil {
			return fail(fmt.Errorf("e2erun: encode web row event: %w", err))
		}
		eventJSONs[i] = string(data)
	}
	rowsArg, err := json.Marshal(eventJSONs)
	if err != nil {
		return fail(fmt.Errorf("e2erun: encode web rows argument: %w", err))
	}

	resultsPath := filepath.Join(webDir, "test-results", fmt.Sprintf("e2e_results_%s.json", node.PeerID))
	defer os.Remove(resultsPath)

	cmd := exec.Command("npx", "playwright", "test", "tests/e2e.spec.js")
	cmd.Dir = webDir
	cmd.Env = append(os.Environ(),
		"WEB_IDENTITY_SEED_HEX="+node.PrivateKey,
		"WEB_ROWS_JSON="+string(rowsArg),
		"WEB_RESULTS_PATH="+resultsPath,
	)
	// Playwright reports a failing row as a failing test (nonzero exit),
	// exactly like `adb shell am instrument`'s JUnit failure for
	// E2ETest.kt -- the results file, not this exit status, is
	// authoritative for per-row pass/fail (see runCaptured's use here:
	// still worth capturing output for the all-rows-failed case where the
	// results file was never written at all, e.g. the browser/wasm bundle
	// itself never loaded).
	if err := runCaptured(cmd, "playwright test tests/e2e.spec.js"); err != nil {
		if _, statErr := os.Stat(resultsPath); statErr != nil {
			return fail(err)
		}
		// Results file exists despite the nonzero exit: at least one row
		// really did fail (not a launch-level problem) -- fall through to
		// parse it for real per-row detail instead of failing every row
		// with the same generic message.
	}

	resultsJSON, err := os.ReadFile(resultsPath)
	if err != nil {
		return fail(fmt.Errorf("e2erun: read web results %s: %w", resultsPath, err))
	}
	out, err := parseRowResults(resultsJSON, rowIdxs, "web e2e.spec.js")
	if err != nil {
		return fail(err)
	}
	return out
}
